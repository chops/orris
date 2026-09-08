defmodule AiOrchestrator.Gate.ProcessControlSpikeTest do
  @moduledoc """
  Measured process-control spike for the gate-ownership unit (rulings
  `m_1788577643895_gate_ownership_rulings`, R4). Real OS, no production code, no
  dependency: it pins what one candidate mechanism can and cannot prove, so the lifecycle
  RED that follows is written against measured facts rather than assumptions.

  Mechanism under measurement -- a GUARDIAN launched as a Port with :spawn_executable (no
  shell string): `test/support/gate_spike/helper.pl` (perl + POSIX, present on macOS and
  Linux base systems, so the spike needs no packaging; production packaging is named in
  docs/spikes/gate-process-control.org). A Port-spawned child is already a process-group
  leader (measured: setsid fails there), and a cold pid lookup is not authority to signal
  (Codex caution m_1788577949713647250_0b72426a: the check-then-kill reuse window), so:

    1. the guardian stays alive holding the control channel and forks the WORKER, which
       calls setsid and therefore leads its own group with every descendant of the gate;
    2. the guardian publishes `IDENT guardian=<pid> worker=<pid> pgid=<pgid>` on stdout
       BEFORE anything runs (prepare);
    3. the worker reports READY on a status pipe only after a successful setsid (the
       guardian fails closed with SETUP_FAILED otherwise), then blocks on a private pipe for
       the one-time go-token the guardian forwards on `GO` (release); a guardian that loses
       its parent (stdin EOF) settles the group it owns by force -- an unreleased command
       never runs;
    4. completion is the whole owned group settled, never leader exit: after the worker
       exits the guardian waits (bounded) for the group to empty, terminates and counts
       leftovers, and only then reports `EXIT status=<n>|signal=<NAME> group_settled=1
       leftovers=<k>` (WIFSIGNALED decoded as a typed fact, never a right-shifted zero);
       `TERM` settles by force and reports `DEAD pgid=<pgid> proven=<1|0|unknown>`. The
       exact child is NOT reaped until the group is settled and the final signal sent --
       authority ends at that reap, and reuse safety is claimed only up to it.

  Kernel identity is read back from the OS as well, never only from the helper's word:
  `ps -o pid=,pgid=,lstart=` here (second resolution; the microsecond source needs
  compiled code on macOS). Linux /proc/<pid>/stat identity is UNMEASURED: the branch is
  unpushed and the spike is excluded by default, so no Linux run has happened. Group death
  is what the guardian proves; the test cross-checks ESRCH for the group plus zero members
  by pgid, and a `ps` failure is a failed check, never an empty set. A descendant that
  escapes the session (setsid again) is outside any process-GROUP proof by definition;
  group death is not arbitrary descendant death.

  Run with `mix test --include spike`; excluded by default because it forks real processes.
  """

  use ExUnit.Case, async: false

  @moduletag :spike

  @helper Path.expand("../support/gate_spike/helper.pl", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "gate-spike-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, dir: dir}
  end

  test "prepare publishes identity before the command runs; release lets it run; the guardian proves its group dead", %{
    dir: dir
  } do
    marker = Path.join(dir, "ran")
    port = prepare(["/bin/sh", "-c", "touch #{marker}; sleep 30 & sleep 30; wait"])
    %{worker: worker, pgid: pgid} = identity(port)

    # prepare: the worker leads its own group, the kernel agrees, nothing has run.
    assert {^worker, ^pgid, lstart} = ps_identity(worker)
    assert lstart != ""
    assert pgid == worker, "the worker leads its own group"
    refute File.exists?(marker), "the command ran before release"

    # release: exactly one go-token; the command and its descendants share the group.
    assert "RELEASED" = release(port)
    wait_until(fn -> File.exists?(marker) end)
    wait_until(fn -> length(members(pgid)) >= 3 end)
    assert Enum.all?(members(pgid), fn {_p, g} -> g == pgid end)

    # terminate: the guardian signals its own group and proves it dead; the OS agrees.
    assert {"DEAD", ^pgid, "1"} = terminate(port)
    assert members(pgid) == []
    assert group_gone?(pgid)
  end

  test "an unreleased command never runs when the guardian loses its parent (fails closed)", %{dir: dir} do
    marker = Path.join(dir, "ran")
    port = prepare(["/bin/sh", "-c", "touch #{marker}"])
    %{pgid: pgid, worker: worker} = identity(port)

    Port.close(port)
    wait_until(fn -> group_gone?(pgid) end)
    refute File.exists?(marker), "a command ran that was never released"
    assert members(pgid) == []
    assert ps_identity(worker) == :gone
  end

  test "the worker's exit status is reported by the guardian with the group settled, never invented", %{dir: _dir} do
    port = prepare(["/bin/sh", "-c", "exit 7"])
    %{} = identity(port)
    assert "RELEASED" = release(port)
    assert "EXIT status=7 group_settled=1 leftovers=0 proven=1" = line(port)
  end

  # Codex probes (m_1788578468382_gate_spike_findings, M1 and M2), verbatim in substance.
  test "ordinary leader exit does not certify completion while a descendant remains" do
    port = prepare(["/bin/sh", "-c", "sleep 2 & exit 0"])
    %{pgid: pgid} = identity(port)
    "RELEASED" = release(port)

    # The leader exits at once; the guardian must not report completion until the group
    # has settled -- here by waiting the descendant out -- and the OS must agree.
    exit_line = line(port, 6_000)
    assert exit_line =~ ~r/^EXIT status=0 group_settled=1 leftovers=\d+ proven=1$/, exit_line
    assert group_gone?(pgid), "the guardian certified completion while its group remained live"
  end

  test "signal termination is a typed signal fact, never a successful exit code" do
    port = prepare(["/bin/sh", "-c", "kill -TERM $$"])
    %{} = identity(port)
    "RELEASED" = release(port)
    exit_line = line(port)
    refute exit_line =~ ~r/^EXIT status=0/, "WIFSIGNALED must not decode to exit zero"
    assert exit_line =~ ~r/^EXIT signal=TERM group_settled=1/, exit_line
  end

  test "identity is worker pid + kernel start: a later process at the same pid with another start is a stranger" do
    port = prepare(["/bin/sh", "-c", "sleep 30"])
    %{worker: worker, pgid: pgid} = identity(port)
    {^worker, ^pgid, lstart} = ps_identity(worker)
    "RELEASED" = release(port)
    {"DEAD", ^pgid, "1"} = terminate(port)

    # The recorded identity is (pid, start); anything found later at this pid is ours only
    # if the start matches, and even then only the guardian -- not a cold lookup -- may
    # signal it. After proven death nothing at this pid is ours.
    case ps_identity(worker) do
      :gone -> :ok
      {^worker, _g, other_start} -> refute other_start == lstart, "a reused pid must not read as the gate's process"
    end
  end

  # Codex probe (m_1788579010516_gate_spike_r3_probe), verbatim in substance: a lone leader
  # that ignores TERM must be escalated to KILL by the guardian without a blocking wait.
  test "a lone TERM-resistant worker is escalated without blocking the guardian" do
    port =
      Port.open({:spawn_executable, @helper}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:args, ["/usr/bin/perl", "-e", "$|=1; $SIG{TERM}='IGNORE'; print qq(GATE_READY\\n); sleep 30"]},
        {:line, 1024}
      ])

    "IDENT guardian=" <> rest = line(port)
    [_, "worker=" <> worker, _] = String.split(rest, " ")

    on_exit(fn ->
      System.cmd("kill", ["-KILL", worker], stderr_to_stdout: true)
      Process.sleep(300)
    end)

    Port.command(port, "GO\n")
    assert "RELEASED" = line(port)
    assert "GATE_READY" = line(port)
    Port.command(port, "TERM\n")
    result = line(port, 5_000)
    assert is_binary(result) and String.starts_with?(result, "DEAD "), "lone worker was not escalated within 5 seconds"
    assert result =~ ~r/proven=1/, result
  end

  # The ps contract, proven with a deterministic fake ps on PATH: only exit 0 with numeric
  # lines, or exactly exit 1 with empty output, mean anything; everything else is unknown.
  for {name, script, expected} <- [
        {"exit 0 empty", "exit 0", "settled"},
        {"exit 1 empty", "exit 1", "settled"},
        {"exit 0 foreign live pid", "printf '%s\\n' 1", "unsettled"},
        {"exit 1 with text", "echo 'ps: not permitted' >&2; echo 'ps: not permitted'; exit 1", "unknown"},
        {"exit 2 empty", "exit 2", "unknown"},
        {"exit 0 garbage", "echo 'total 3'; exit 0", "unknown"}
      ] do
    test "ps contract: #{name} -> #{expected}", %{dir: dir} do
      bin = Path.join(dir, "bin")
      File.mkdir_p!(bin)
      fake = Path.join(bin, "ps")

      File.write!(
        fake,
        "#!/bin/sh\nif [ \"$1\" = \"-o\" ] && [ \"$2\" = \"stat=\" ]; then exec /bin/ps \"$@\"; fi\n#{unquote(script)}\n"
      )

      File.chmod!(fake, 0o755)

      port =
        Port.open({:spawn_executable, @helper}, [
          :binary,
          :exit_status,
          :use_stdio,
          {:args, ["/bin/sh", "-c", "exit 0"]},
          {:env, [{~c"PATH", String.to_charlist(bin <> ":" <> System.get_env("PATH"))}]},
          {:line, 1024}
        ])

      %{} = identity(port)
      "RELEASED" = release(port)
      # The guardian's escalation is two bounded rounds (~3 s each) before it gives up.
      exit_line = line(port, 12_000)

      case unquote(expected) do
        "settled" -> assert exit_line =~ ~r/group_settled=1/, exit_line
        # A live member the guardian cannot end is honestly unsettled, never certified.
        "unsettled" -> assert exit_line =~ ~r/group_settled=0/, exit_line
        "unknown" -> assert exit_line =~ ~r/group_settled=0/, exit_line
      end
    end
  end

  # ----- the mechanism, as a Port with :spawn_executable and no shell string -----

  defp prepare(argv) do
    Port.open({:spawn_executable, @helper}, [:binary, :exit_status, :use_stdio, {:args, argv}, {:line, 1024}])
  end

  defp identity(port) do
    case line(port) do
      "IDENT guardian=" <> rest ->
        [guardian, "worker=" <> worker, "pgid=" <> pgid] = String.split(rest, " ")
        %{guardian: String.to_integer(guardian), worker: String.to_integer(worker), pgid: String.to_integer(pgid)}

      other ->
        flunk("the guardian did not publish its identity first: #{inspect(other)}")
    end
  end

  defp release(port) do
    true = Port.command(port, "GO\n")
    line(port)
  end

  defp terminate(port) do
    true = Port.command(port, "TERM\n")

    case line(port) do
      "DEAD pgid=" <> rest ->
        [pgid, "proven=" <> proven | _] = String.split(rest, " ")
        {"DEAD", String.to_integer(pgid), proven}

      other ->
        flunk("the guardian did not report the group dead: #{inspect(other)}")
    end
  end

  defp line(port, timeout_ms \\ 3_000) do
    receive do
      {^port, {:data, {:eol, text}}} -> text
      {^port, {:exit_status, status}} -> "guardian exited #{status}"
    after
      timeout_ms -> flunk("no line from the guardian within #{timeout_ms} ms")
    end
  end

  # Kernel-derived identity: pid, pgid, and the kernel's start time for that pid.
  defp ps_identity(pid) do
    case System.cmd("ps", ["-p", Integer.to_string(pid), "-o", "pid=,pgid=,lstart="], stderr_to_stdout: true) do
      {out, 0} ->
        case out |> String.trim() |> String.split(~r/\s+/, parts: 3) do
          [p, g, lstart] -> {String.to_integer(p), String.to_integer(g), lstart}
          _ -> :gone
        end

      _ ->
        :gone
    end
  end

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=,pgid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} ->
        out
        |> String.split("\n", trim: true)
        |> Enum.map(fn line -> line |> String.trim() |> String.split(~r/\s+/) end)
        |> Enum.filter(&match?([_, _], &1))
        |> Enum.map(fn [p, g] -> {String.to_integer(p), String.to_integer(g)} end)
        |> Enum.filter(fn {_p, g} -> g == pgid end)

      # ps contract, exact: exit 0 with every line numeric is the member list (above);
      # exactly exit 1 with empty output is no match (an empty group); anything else is a
      # failed check, and a membership check that cannot run is never an empty group.
      {"", 1} ->
        []

      {out, status} ->
        flunk("ps failed (#{status}); a membership check that cannot run is not an empty group: #{inspect(out)}")
    end
  end

  # ESRCH for the whole group is the proof; a live member anywhere makes kill -0 succeed.
  defp group_gone?(pgid) do
    {_out, status} = System.cmd("kill", ["-0", "--", "-#{pgid}"], stderr_to_stdout: true)
    status != 0 and members(pgid) == []
  end

  # A bounded await that fails closed: the condition must hold before the deadline, or
  # the test fails here rather than passing on a timeout.
  defp wait_until(fun, timeout_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    assert fun.(), "condition did not hold within #{timeout_ms} ms"
  end
end
