defmodule AiOrchestrator.Gate.GateGuardianTest do
  @moduledoc """
  The native gate guardian (bin/build-guardian), the first bounded review
  unit of gate-process ownership (R4; scoped GO `m_1788579576677_gate_spike_scoped_go`).
  Built here with the pinned toolchain's Rust compiler into a per-run scratch path so the contract
  is exercised on the real OS; permanent build wiring is a separate, non-overlapping unit.

  Pinned protocol: docs/contracts/gate-guardian-protocol.org. Every fact asserted below is
  cross-checked against the kernel (ps/kill), never taken from the guardian's word alone.
  """

  use ExUnit.Case, async: false

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)

  setup_all do
    dir = Path.join(System.tmp_dir!(), "gate-guardian-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {out, 0} =
      System.cmd(@source, ["--testing", bin], stderr_to_stdout: true)

    assert out == "" or not (out =~ "warning:"), "the helper must build without warnings: #{out}"
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, bin: bin, build_dir: dir}
  end

  setup %{bin: bin} do
    dir = Path.join(System.tmp_dir!(), "gate-guardian-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, dir: dir, bin: bin}
  end

  test "READY carries kernel identity before the command runs; output goes to private files, never the control channel",
       ctx do
    marker = Path.join(ctx.dir, "ran")

    port =
      prepare(ctx, [
        "/bin/sh",
        "-c",
        "echo 'READY guardian=1 worker=1 pgid=1 start=forged' ; touch #{marker}; echo err >&2"
      ])

    %{worker: worker, pgid: pgid, start: start} = ready(port)

    assert pgid == worker
    assert {^worker, ^pgid} = ps_pid_pgid(worker)
    assert start =~ ~r/^\d+\.\d{6}$|^ticks:\d+$/
    refute File.exists?(marker), "the command ran before release"
    assert ctx.dir |> Path.join("out") |> File.stat!() |> Map.get(:mode) |> Bitwise.band(0o777) == 0o600

    assert "RELEASED" = release(port)
    assert "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown" = line(port)
    assert File.exists?(marker)
    assert File.read!(Path.join(ctx.dir, "out")) =~ "forged", "ordinary output lands in the private file"
    assert File.read!(Path.join(ctx.dir, "err")) == "err\n"
  end

  test "completion is the whole owned group settled, never leader exit", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "sleep 2 & exit 0"])
    %{pgid: pgid} = ready(port)
    "RELEASED" = release(port)
    exit_line = line(port, 8_000)
    assert exit_line =~ ~r/^EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone/, exit_line
    assert group_gone?(pgid)
  end

  test "a signalled worker is a typed signal fact, never a successful status", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "kill -TERM $$"])
    %{} = ready(port)
    "RELEASED" = release(port)
    exit_line = line(port)
    assert exit_line =~ ~r/^EXIT kind=signaled signal=15 settled=1/, exit_line
  end

  test "TERM settles the group by force with proof; a lone TERM-ignoring leader is escalated without blocking", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "trap '' TERM; exec sleep 30"])
    %{pgid: pgid, worker: worker} = ready(port)
    "RELEASED" = release(port)
    Process.sleep(100)
    true = Port.command(port, "TERM\n")
    dead = line(port, 10_000)
    assert dead =~ ~r/^DEAD reason=command kind=signaled signal=9 settled=1 leftovers=0 proof=gone/, dead
    assert group_gone?(pgid)
    assert ps_pid_pgid(worker) == :gone
  end

  test "an unreleased command never runs when the guardian loses its parent", ctx do
    marker = Path.join(ctx.dir, "ran")
    port = prepare(ctx, ["/bin/sh", "-c", "touch #{marker}"])
    %{pgid: pgid} = ready(port)
    Port.close(port)
    wait_until(fn -> group_gone?(pgid) end, 5_000)
    refute File.exists?(marker)
  end

  test "setup failures are typed classes and the guardian exits without running anything", ctx do
    File.write!(Path.join(ctx.dir, "out"), "pre-existing")
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"])

    # nothing was created, so nothing is claimed removed
    assert "SETUP_FAILED class=open_stdout cleanup=none" = line(port)

    assert_receive {^port, {:exit_status, 1}}, 3_000
    assert File.read!(Path.join(ctx.dir, "out")) == "pre-existing", "an existing output file is never truncated (O_EXCL)"
  end

  test "an unknown command on the control channel is a protocol error, not an action", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "exit 3"])
    %{} = ready(port)
    true = Port.command(port, "KILL -9 everything\n")
    assert "PROTOCOL_ERROR" = line(port)
    "RELEASED" = release(port)
    assert "EXIT kind=exited status=3 settled=1 leftovers=0 proof=gone escaped=unknown" = line(port)
  end

  # Codex findings m_1788580245382351541_092880c2 (M1-M3), reproduced as tests.
  test "M1: control framing is incremental and exact -- a split command is one command, coalesced lines act in order",
       ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "sleep 30"])
    %{pgid: pgid} = ready(port)
    true = Port.command(port, "G")
    Process.sleep(100)
    true = Port.command(port, "O\n")
    assert line(port) == "RELEASED", "a command split across writes must act once its newline arrives"

    port2 =
      %{ctx | dir: ctx.dir <> "-2"} |> tap(fn c -> File.mkdir_p!(c.dir) end) |> prepare(["/bin/sh", "-c", "sleep 30"])

    %{pgid: pgid2} = ready(port2)
    true = Port.command(port2, "GO\nTERM\n")
    assert "RELEASED" = line(port2)
    dead = line(port2, 10_000)
    assert dead =~ ~r/^DEAD reason=command .*settled=1/, "the coalesced TERM must be acted on, not discarded: #{dead}"
    assert group_gone?(pgid2)

    true = Port.command(port, "TERM\n")
    assert line(port, 10_000) =~ ~r/^DEAD reason=command .*settled=1/
    assert group_gone?(pgid)
  end

  test "M1: an overlong control line is one protocol error and nothing acts", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"])
    %{} = ready(port)
    true = Port.command(port, String.duplicate("x", 300) <> "\n")
    assert "PROTOCOL_ERROR" = line(port)
    "RELEASED" = release(port)
    assert line(port) =~ ~r/^EXIT kind=exited status=0 settled=1/
  end

  for {flag, value} <- [
        {"--ready-ms", "-1"},
        {"--settle-ms", "garbage"},
        {"--rounds", "0"},
        {"--rounds", "11"},
        {"--settle-ms", "99"},
        {"--ready-ms", "99999999999"}
      ] do
    test "M2: #{flag} #{value} is refused before any side effect", ctx do
      port =
        Port.open({:spawn_executable, ctx.bin}, [
          :binary,
          :exit_status,
          :use_stdio,
          {:args,
           [
             "--stdout",
             Path.join(ctx.dir, "out"),
             "--stderr",
             Path.join(ctx.dir, "err"),
             "--cwd",
             ctx.dir,
             unquote(flag),
             unquote(value),
             "--",
             "/bin/sh",
             "-c",
             "exit 0"
           ]},
          {:line, 1024}
        ])

      assert "SETUP_FAILED class=usage" = line(port)
      assert_receive {^port, {:exit_status, 2}}, 3_000
      refute File.exists?(Path.join(ctx.dir, "out")), "no output file may be created before the options are validated"
    end
  end

  test "M3: a broken control channel settles the group instead of dying by SIGPIPE", ctx do
    marker = Path.join(ctx.dir, "ran")
    port = prepare(ctx, ["/bin/sh", "-c", "touch #{marker}; sleep 30"])
    %{pgid: pgid, guardian: guardian} = ready(port)
    # Closing the Port closes BOTH the guardian's stdin and its stdout reader; the guardian
    # must settle its group and exit on its own terms, not by an unhandled SIGPIPE.
    Port.close(port)
    wait_until(fn -> group_gone?(pgid) end, 10_000)
    refute File.exists?(marker), "the worker was never released"
    # The guardian exits on its own terms once it has reaped and reported; until the BEAM
    # reaps it, it may linger as a zombie.
    wait_until(fn -> exited?(guardian) end, 5_000)
  end

  test "M3: SIGTERM to the guardian settles its released group and reports", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "sleep 30"])
    %{pgid: pgid, guardian: guardian} = ready(port)
    "RELEASED" = release(port)
    {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(guardian)])
    dead = line(port, 10_000)
    assert dead =~ ~r/^DEAD reason=guardian_signaled .*settled=1 leftovers=0 proof=gone/, dead
    assert group_gone?(pgid)
  end

  # Codex findings m_1788580313437063208_c0339695 (M4, M5) through the compile-time test seam
  # (GATE_GUARDIAN_TESTING + GATE_GUARDIAN_FAULT); the production build has no seam.
  test "M4: a ready timeout cleans up the exact pre-setsid child before rejecting, and reports the outcome", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"], fault: "setsid_delay", ready_ms: 200)
    rejection = line(port, 10_000)
    assert rejection =~ ~r/^SETUP_FAILED class=ready_timeout settled=1 leftovers=0 proof=gone cleanup=removed$/, rejection
    assert_receive {^port, {:exit_status, 1}}, 3_000
    refute File.exists?(Path.join(ctx.dir, "out")), "the guardian removes the output objects it created"
    # No orphan: nothing owned by this guardian survives; the seam records the child pid.
    child = ctx.dir |> Path.join("fault-child-pid") |> File.read!() |> String.trim() |> String.to_integer()
    wait_until(fn -> ps_pid_pgid(child) == :gone end, 5_000)
  end

  test "M4: a stderr open failure after stdout succeeded removes the stdout object it created", ctx do
    File.write!(Path.join(ctx.dir, "err"), "pre-existing")
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"])
    assert "SETUP_FAILED class=open_stderr cleanup=removed" = line(port)
    refute File.exists?(Path.join(ctx.dir, "out"))
    assert File.read!(Path.join(ctx.dir, "err")) == "pre-existing"
  end

  test "M5: an unprovable member enumeration is unknown, never an empty group", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"], fault: "members_fail")
    %{} = ready(port)
    "RELEASED" = release(port)
    exit_line = line(port, 10_000)
    assert exit_line =~ ~r/^EXIT kind=exited status=0 settled=0 leftovers=unknown proof=gone/, exit_line
  end

  test "M5: a malformed kernel identity fact is a typed setup failure, never a guessed identity", ctx do
    port = prepare(ctx, ["/bin/sh", "-c", "exit 0"], fault: "identity_short")
    rejection = line(port, 10_000)
    assert rejection =~ ~r/^SETUP_FAILED class=identity settled=1/, rejection
  end

  # ----- harness -----

  defp prepare(%{bin: bin, dir: dir}, argv, opts \\ []) do
    env =
      if fault = opts[:fault],
        do: [{~c"GATE_GUARDIAN_FAULT", String.to_charlist(fault)}, {~c"GATE_GUARDIAN_FAULT_DIR", String.to_charlist(dir)}],
        else: []

    args = [
      "--stdout",
      Path.join(dir, "out"),
      "--stderr",
      Path.join(dir, "err"),
      "--cwd",
      dir,
      "--ready-ms",
      Integer.to_string(opts[:ready_ms] || 5000),
      "--settle-ms",
      "2000",
      "--rounds",
      "2",
      "--" | argv
    ]

    Port.open({:spawn_executable, bin}, [:binary, :exit_status, :use_stdio, {:args, args}, {:env, env}, {:line, 1024}])
  end

  defp ready(port) do
    case line(port) do
      "READY guardian=" <> rest ->
        [g, "worker=" <> w, "pgid=" <> p, "start=" <> s] = String.split(rest, " ")
        %{guardian: String.to_integer(g), worker: String.to_integer(w), pgid: String.to_integer(p), start: s}

      other ->
        flunk("expected READY first, got #{inspect(other)}")
    end
  end

  defp release(port) do
    true = Port.command(port, "GO\n")
    line(port)
  end

  defp line(port, timeout_ms \\ 3_000) do
    receive do
      {^port, {:data, {:eol, text}}} -> text
      {^port, {:exit_status, status}} -> "guardian exited #{status}"
    after
      timeout_ms -> flunk("no control line within #{timeout_ms} ms")
    end
  end

  # ps contract, exact: exit 0 with two numeric fields is the identity; exactly exit 1 with
  # empty output is no such process; anything else proves nothing and fails the test.
  defp ps_pid_pgid(pid) do
    case System.cmd("ps", ["-p", Integer.to_string(pid), "-o", "pid=,pgid="], stderr_to_stdout: true) do
      {out, 0} ->
        case out |> String.trim() |> String.split(~r/\s+/) do
          [p, g] -> {String.to_integer(p), String.to_integer(g)}
          _ -> flunk("ps answered but not with an identity: #{inspect(out)}")
        end

      {"", 1} ->
        :gone

      {out, status} ->
        flunk("ps failed in a way that proves nothing (#{status}): #{inspect(out)}")
    end
  end

  defp exited?(pid) do
    case System.cmd("ps", ["-p", Integer.to_string(pid), "-o", "stat="], stderr_to_stdout: true) do
      {out, 0} -> String.starts_with?(String.trim(out), "Z")
      {"", 1} -> true
      {out, status} -> flunk("ps failed in a way that proves nothing (#{status}): #{inspect(out)}")
    end
  end

  # Death is proven only by ESRCH ("No such process"); EPERM, a missing kill, or any other
  # failure is NOT death. The proof contract of the guardian applies to the oracle too.
  defp group_gone?(pgid) do
    case System.cmd("kill", ["-0", "--", "-#{pgid}"], stderr_to_stdout: true) do
      {_out, 0} -> false
      {out, 1} -> out =~ "No such process"
      {out, status} -> flunk("kill -0 failed in a way that proves nothing (#{status}): #{inspect(out)}")
    end
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    assert fun.(), "condition did not hold within #{timeout_ms} ms"
  end

  test "a NUL byte inside a control line cannot shorten a command into GO", %{bin: bin, dir: dir} do
    port = prepare(%{bin: bin, dir: dir}, ["/bin/sh", "-c", "sleep 30"])
    %{pgid: pgid} = ready(port)
    true = Port.command(port, "GO" <> <<0>> <> "not-a-command\n")
    assert line(port) == "PROTOCOL_ERROR", "exact-length comparison: a NUL is a byte, not a terminator"
    true = Port.command(port, "GO\n")
    assert line(port) == "RELEASED"
    true = Port.command(port, "TERM\n")
    assert "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=gone" <> _ = line(port)
    assert_receive {^port, {:exit_status, 0}}, 3_000
    assert group_gone?(pgid)
  end

  test "Linux fact readers fail closed on malformed facts and I/O errors on either host OS" do
    root = Path.expand("../..", __DIR__)

    {output, status} =
      System.cmd(
        "cargo",
        [
          "test",
          "--locked",
          "--color",
          "never",
          "--manifest-path",
          Path.join(root, "native/gate_guardian/Cargo.toml"),
          "--bin",
          "gate_guardian",
          "facts::linux::tests::",
          "--",
          "--format",
          "pretty"
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output

    for name <- ~w(identity_rejects_wrong_pid_malformed_prefix_and_negative_ticks
                   membership_rejects_signed_overflow_wrong_pid_and_malformed_facts
                   arbitrary_comm_bytes_preserve_identity_and_membership
                   read_and_enumeration_errors_are_unknown_but_disappearance_is_absence
                   non_process_entries_are_ignored_and_reads_are_bounded) do
      assert output =~ "test facts::linux::tests::#{name} ... ok", output
    end
  end

  test "a NUL inside TERM is not a TERM, split or coalesced, and the guardian keeps its group", %{bin: bin, dir: dir} do
    port = prepare(%{bin: bin, dir: dir}, ["/bin/sh", "-c", "sleep 30"])
    %{pgid: pgid, worker: worker} = ready(port)
    true = Port.command(port, "TERM" <> <<0>> <> "\n")
    assert line(port) == "PROTOCOL_ERROR"
    true = Port.command(port, "TE")
    Process.sleep(100)
    true = Port.command(port, "RM" <> <<0>> <> "\nGO" <> <<0>> <> "\n")
    assert line(port) == "PROTOCOL_ERROR"
    assert line(port) == "PROTOCOL_ERROR"
    assert Port.info(port) != nil, "the guardian is still serving the channel"
    assert ps_pid_pgid(worker) == {worker, pgid}, "no record acted: the worker is still owned and alive"
    true = Port.command(port, "TERM\n")
    assert "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=gone" <> _ = line(port)
    assert_receive {^port, {:exit_status, 0}}, 3_000
    assert group_gone?(pgid)
  end

  test "a control reader that goes away on its own (EPIPE, not a closed port) still ends in settlement, exit 0",
       %{bin: bin, dir: dir} do
    bash = System.find_executable("bash")
    argv = [bin, "--stdout", Path.join(dir, "out"), "--stderr", Path.join(dir, "err"), "--cwd", dir]
    argv = argv ++ ["--settle-ms", "200", "--rounds", "2", "--", "/bin/sleep", "30"]
    # the guardian's stdout reader is `head -n 1`: it relays READY and then goes away, so the
    # next control write (RELEASED) hits EPIPE while stdin stays open and the worker is running
    pipeline = ~S("$@" | head -n 1; echo "guardian_exit=${PIPESTATUS[0]}")

    port =
      Port.open({:spawn_executable, bash}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:line, 512},
        {:args, ["-c", pipeline, "--"] ++ argv}
      ])

    %{pgid: pgid, worker: worker} = ready(port)
    assert ps_pid_pgid(worker) == {worker, pgid}
    true = Port.command(port, "GO\n")
    assert line(port, 5_000) == "guardian_exit=0", "EPIPE on the control channel settles the group and exits truthfully"
    assert_receive {^port, {:exit_status, 0}}, 3_000
    assert group_gone?(pgid), "the worker group must be gone at guardian exit"
  end
end
