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
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane

  @yield_ms 2_000

  setup do
    bash = System.find_executable("bash") || flunk("bash is not on PATH")

    base =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_local_pane_artifact_fifo",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    repo = Path.join(base, "repo")
    File.mkdir_p!(Path.join(repo, "lib"))
    fifo = Path.join(repo, "lib/out.org")
    {_, 0} = System.cmd("mkfifo", [fifo], stderr_to_stdout: true)
    {:ok, %File.Stat{type: :other}} = File.stat(fifo)

    on_exit(fn ->
      release_fifo(bash, fifo)
      File.rm_rf!(base)
    end)

    {:ok, base: base, repo: repo, fifo: fifo, bash: bash}
  end

  # Opening a FIFO O_RDWR never blocks (Linux and macOS): it momentarily supplies the writer a
  # blocked O_RDONLY open is waiting for, and closing it hands that reader EOF. `bash` is an
  # absolute path so `System.cmd/3` goes straight to `Port.open/2` without consulting the file
  # server. Idempotent when nothing is blocked.
  defp release_fifo(bash, fifo) do
    {_, 0} = System.cmd(bash, ["-c", ~S(exec 3<>"$1"; exec 3>&-), "bash", fifo], stderr_to_stdout: true)
    :ok
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
end
