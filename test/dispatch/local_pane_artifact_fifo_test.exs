defmodule AiOrchestrator.Dispatch.LocalPaneArtifactFifoTest do
  @moduledoc """
  NS-20.D.001 / NS-35.D.001 invariant: an `expected_artifact` that is NOT a regular file is
  refused at the snapshot boundary and never opened. `File.read/1` on a named pipe with no writer
  blocks in open(2) until a writer appears, and it does so INSIDE `file_server_2`, the node-wide
  file server every non-raw `File.*` call is routed through: measured at af3309f8, one writerless
  FIFO named as an artifact wedged not only the snapshot caller but every later `File.stat`,
  `File.write` and `File.rm_rf` in the VM (crash dump: `file_server_2` last scheduled in
  `prim_file:read_file_nif/1`, 17 queued callers). `File.stat/2` already precedes the read; the
  invariant is that its `type` is consulted before any byte is requested. The refusal answers the
  EXISTING closed classes `artifact_baseline_failed` (snapshot) and `artifact_read_failed`
  (observe); no new class, event or detail arm.

  Safety of the suite itself: every call into the site runs under `Task.async` with a bounded
  `Task.yield`. A regression is released, not waited out: the FIFO is opened read-write from a
  Bash port (never blocks on a FIFO), which supplies the writer the blocked open is waiting for
  and hands it EOF. The Bash path is resolved BEFORE the read starts and passed absolutely,
  because `System.cmd/3` with a bare name resolves it through the same file server that is
  wedged; only then can the task be shut down and the failure reported as a timeout.

  Ownership of the fixture's own directory (successor 2026-09-20, after independent review):
  the base every row builds under is created EXCLUSIVELY with `File.mkdir/1`, never
  `mkdir_p`, which adopts whatever already exists at the path together with its contents.
  Its name is unique across OS processes (OS pid, VM unique integer, random bytes; the VM
  integer alone is unique only inside one VM). A colliding path is retried under a fresh name
  a bounded number of times and is never entered, chmodded or removed; a namespace that never
  yields a fresh name is refused. The cleanup that recursively removes the base is registered
  the moment the base exists and before any later setup step, so nothing this suite removes
  can be a directory it did not create. A control row plants a foreign directory with a
  sentinel at exactly the colliding path and proves it survives.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane

  @yield_ms 2_000

  setup do
    bash = System.find_executable("bash") || flunk("bash is not on PATH")

    base = claim_base!()
    fifo = Path.join(base, "repo/lib/out.org")

    # Ownership is established: register the whole cleanup NOW, before any later step can
    # fail. Releasing a FIFO that was never created is a no-op (see release_fifo/2), and the
    # release comes first so a blocked reader cannot wedge the removal.
    on_exit(fn ->
      release_fifo(bash, fifo)
      File.rm_rf!(base)
    end)

    repo = Path.join(base, "repo")
    File.mkdir!(repo)
    File.mkdir!(Path.join(repo, "lib"))
    {_, 0} = System.cmd("mkfifo", [fifo], stderr_to_stdout: true)
    {:ok, %File.Stat{type: :other}} = File.stat(fifo)

    {:ok, base: base, repo: repo, fifo: fifo, bash: bash}
  end

  # Opening a FIFO O_RDWR never blocks (Linux and macOS): it momentarily supplies the writer a
  # blocked O_RDONLY open is waiting for, and closing it hands that reader EOF. `bash` is an
  # absolute path so `System.cmd/3` goes straight to `Port.open/2` without consulting the file
  # server. Idempotent when nothing is blocked, and a no-op when the path is not a FIFO (the
  # `-p` test runs inside Bash, so it never consults the file server either).
  defp release_fifo(bash, fifo) do
    {_, 0} =
      System.cmd(bash, ["-c", ~S([ -p "$1" ] || exit 0; exec 3<>"$1"; exec 3>&-), "bash", fifo], stderr_to_stdout: true)

    :ok
  end

  # A name no other OS process can be building at the same time: the OS pid, this VM's unique
  # integer and three random bytes. The VM integer alone is unique only inside one VM.
  defp unique_token do
    "#{System.pid()}-#{System.unique_integer([:positive])}-#{Base.encode16(:crypto.strong_rand_bytes(3), case: :lower)}"
  end

  defp base_path(token), do: Path.join(System.tmp_dir!(), "orris-artifact-fifo-#{token}")

  @claim_attempts 8

  defp claim_base! do
    case claim_base(&unique_token/0, @claim_attempts) do
      {:ok, base} -> base
      {:error, reason} -> flunk("no fresh artifact base claimed in #{@claim_attempts} attempts: #{inspect(reason)}")
    end
  end

  # Exclusive ownership. `File.mkdir/1` fails with :eexist on a path that already exists and
  # then NOTHING is done to that path: no chmod, no read, no removal; the next token is tried.
  # Only a directory this call created is chmodded and returned. The bound turns a namespace
  # that never yields a fresh name into a refusal instead of an adoption.
  defp claim_base(_token_fun, 0), do: {:error, :exhausted}

  defp claim_base(token_fun, attempts) do
    base = base_path(token_fun.())

    case File.mkdir(base) do
      :ok ->
        File.chmod!(base, 0o700)
        {:ok, base}

      {:error, :eexist} ->
        claim_base(token_fun, attempts - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The token sequence a control test feeds to claim_base/2, popped from the test process.
  defp scripted_tokens(tokens) do
    Process.put(:scripted_tokens, tokens)

    fn ->
      [token | rest] = Process.get(:scripted_tokens)
      Process.put(:scripted_tokens, rest)
      token
    end
  end

  defp bounded(%{bash: bash, fifo: fifo}, fun) do
    task = Task.async(fun)

    case Task.yield(task, @yield_ms) do
      {:ok, answer} ->
        answer

      nil ->
        release_fifo(bash, fifo)
        Task.shutdown(task, 5_000)
        flunk("the artifact read blocked on a writerless FIFO for #{@yield_ms} ms: no refusal was returned")

      {:exit, reason} ->
        flunk("the artifact read exited: #{inspect(reason)}")
    end
  end

  defp snapshot_command(repo, artifact) do
    %{
      "assignment_id" => "as_0001",
      "repo_root" => repo,
      "expected_artifact" => artifact,
      "allowed_roots" => ["lib"]
    }
  end

  defp observe_command(repo, artifact) do
    %{
      "assignment_id" => "as_0001",
      "artifact_id" => "art_0001",
      "pane_ref" => "pane-fixture-1",
      "repo_root" => repo,
      "expected_artifact" => artifact,
      "artifact_baseline" => %{"exists" => false}
    }
  end

  defmodule IdlePane do
    @moduledoc false
    def status(_pane_ref, _opts), do: {:ok, %{"state" => "idle", "pending_count" => 0}}
  end

  test "snapshot refuses a writerless FIFO artifact instead of blocking in the read", %{repo: repo} = ctx do
    answer = bounded(ctx, fn -> LocalPane.snapshot(snapshot_command(repo, "lib/out.org")) end)

    assert answer == {:error, %{"reason" => "artifact_baseline_failed", "detail" => ":not_regular_file"}}
    refute inspect(answer, limit: :infinity) =~ "sha256"
  end

  test "observe refuses a writerless FIFO artifact instead of blocking the worker", %{repo: repo} = ctx do
    opts = [pane_client: IdlePane, observe_timeout_ms: 50, poll_interval_ms: 1]

    answer = bounded(ctx, fn -> LocalPane.observe(observe_command(repo, "lib/out.org"), opts) end)

    assert answer == {:error, %{"reason" => "artifact_read_failed", "detail" => ":not_regular_file"}}
  end

  test "the type judgement is general: a directory artifact is refused by type, not by the read",
       %{repo: repo} = ctx do
    File.mkdir_p!(Path.join(repo, "lib/dir.org"))

    answer = bounded(ctx, fn -> LocalPane.snapshot(snapshot_command(repo, "lib/dir.org")) end)

    assert answer == {:error, %{"reason" => "artifact_baseline_failed", "detail" => ":not_regular_file"}}
  end

  test "a regular artifact beside the FIFO is still baselined with its fingerprint", %{repo: repo} = ctx do
    File.write!(Path.join(repo, "lib/real.org"), "in scope\n")

    answer = bounded(ctx, fn -> LocalPane.snapshot(snapshot_command(repo, "lib/real.org")) end)

    assert {:ok, %{"exists" => true, "bytes" => 9, "sha256" => "sha256:" <> _digest}} = answer
  end

  test "a pre-existing directory at a colliding base path is never adopted, chmodded or removed", _ctx do
    # Plant what another VM or an earlier run could have left: a directory at exactly the path
    # the first token names, mode 0755, with a file inside that is not ours and no repo/lib/out.org.
    planted_token = unique_token()
    planted = base_path(planted_token)
    File.mkdir!(planted)
    File.chmod!(planted, 0o755)
    sentinel = Path.join(planted, "sentinel")
    File.write!(sentinel, "not yours\n")
    on_exit(fn -> File.rm_rf!(planted) end)

    fresh_token = unique_token()
    assert {:ok, base} = claim_base(scripted_tokens([planted_token, fresh_token]), 2)
    on_exit(fn -> File.rm_rf!(base) end)

    refute base == planted, "the fixture adopted a directory it did not create"
    assert base == base_path(fresh_token)
    assert Bitwise.band(File.stat!(base).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(planted).mode, 0o777) == 0o755, "the planted directory was chmodded"
    assert File.read!(sentinel) == "not yours\n"
    assert File.ls!(planted) == ["sentinel"]

    # A namespace that never yields a fresh name is refused after the bound, and the planted
    # directory is still untouched.
    assert claim_base(fn -> planted_token end, 3) == {:error, :exhausted}
    assert Bitwise.band(File.stat!(planted).mode, 0o777) == 0o755
    assert File.read!(sentinel) == "not yours\n"
    assert File.ls!(planted) == ["sentinel"]
  end
end
