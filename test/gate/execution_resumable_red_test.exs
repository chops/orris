defmodule AiOrchestrator.Gate.ExecutionResumableRedTest do
  @moduledoc """
  RED / interface rows for the FIRST bounded primitive unit (docs/contracts/gate-ownership.org, revision 4; RED GO
  m_1788803100000, corrections m_1788805650000 GP-M1..M5 and m_1788806600000 GP-M6..M8):
  `Gate.Execution.begin_await/2` and `resume_await/2`. Same-process Port / parser / memo / descriptor authority;
  `await/2` and `expire/1` unchanged.

  "RB-" rows are BASELINES on the EXISTING API (green today): they measure what the primitives must reproduce.
  "R-" rows are the primitive interface and fail today with UndefinedFunctionError (no lib change yet).
  Real native guardian, real prepare/ack/release; the TEST PROCESS is the Port owner.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FixedClock

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)
  @run_id "run_resumable"
  @now 1_700_000_000
  @far @now + 600

  # VALID complete guardian records (grammar: execution.ex settlement_grammar / dead_grammar?)
  @exit_ok "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"
  @dead_dup "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=gone escaped=unknown"

  # GP-M8: each row changes exactly ONE grammar property of @exit_ok; the expected closed diagnostic is the
  # existing parser's (describe_record: "malformed <HEAD>" for a known head, "malformed" for an unknown head)
  @malformed [
    {"unknown_head", "BOGUS kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown", "malformed"},
    {"missing_field", "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone", "malformed EXIT"},
    {"extra_key", "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown extra=1", "malformed EXIT"},
    {"duplicate_key", "EXIT kind=exited status=0 kind=exited settled=1 leftovers=0 proof=gone escaped=unknown",
     "malformed EXIT"},
    {"invalid_enum", "EXIT kind=exited status=0 settled=1 leftovers=0 proof=CANARY escaped=unknown", "malformed EXIT"},
    {"dead_invalid_reason", "DEAD reason=zzz kind=signaled signal=15 settled=1 leftovers=0 proof=gone escaped=unknown",
     "malformed DEAD"}
  ]

  # a clock whose wall reads are COUNTED in the owner's dictionary once activated (GP-M4: "no clock decision")
  defmodule Clock do
    @moduledoc false
    def unix_now do
      if Process.get(:clock_counting), do: Process.put(:clock_reads, Process.get(:clock_reads, 0) + 1)
      Process.get(:resumable_now, 1_700_000_000)
    end

    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "resumable-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    # ---- driver (owner = this process) ----
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    Process.put(:resumable_now, @now)
    Process.delete(:clock_counting)
    Process.put(:clock_reads, 0)
    run_dir = Path.join(System.tmp_dir!(), "resumable-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf(run_dir) end)
    {:ok, run_dir: run_dir, opts: [helper: helper, settle_ms: 200, rounds: 2, clock: Clock]}
  end

  defp request(run_dir, argv, deadline) do
    %{
      run_id: @run_id,
      gate_run_id: "gr_0001",
      attempt: 1,
      command_argv: argv,
      repo_root: run_dir,
      run_dir: run_dir,
      deadline_unix: deadline,
      supervisor_instance: "sup_0001"
    }
  end

  defp persist!(run_dir, prepared) do
    lock = [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
    {:ok, w, _} = Writer.open(run_dir, create: true, clock: FixedClock, lock: lock)

    event = %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 2,
      "seq" => 1,
      "event_id" => "ev_0001",
      "type" => "gate_started",
      "ts" => "2026-01-01T00:00:01Z",
      "run_id" => @run_id,
      "actor" => "run_supervisor",
      "data" => Execution.started_data(prepared)
    }

    result = Writer.append(w, event)
    :ok = Writer.close(w)
    assert {:ok, persisted} = result
    persisted
  end

  defp running!(run_dir, argv, deadline, opts) do
    assert {:ok, prepared} = Execution.prepare({SystemFs, nil}, request(run_dir, argv, deadline), opts)
    identity = Execution.identity(prepared)
    track(identity)
    assert {:ok, ack} = Execution.ack(prepared, persist!(run_dir, prepared))
    assert {:ok, running} = Execution.release(prepared, ack, opts)
    {running, identity}
  end

  # forced order (GP-M2): the command completes only when the test creates `go`
  defp gated(run_dir, then_sh),
    do: ["/bin/sh", "-c", "while [ ! -f '#{Path.join(run_dir, "go")}' ]; do sleep 0.02; done; #{then_sh}"]

  defp release_gate!(run_dir), do: File.write!(Path.join(run_dir, "go"), "")

  defp port_of(%{port: port}), do: port

  # a second gate directory under the test's run_dir (prepare requires an existing directory)
  defp sub!(run_dir, name) do
    dir = Path.join(run_dir, name)
    File.mkdir_p!(dir)
    dir
  end

  # non-consuming mailbox witness (GP-M2/M3): the exact record for THIS port is queued, nothing is dequeued
  defp queued_record?(port, prefix) do
    {:messages, msgs} = Process.info(self(), :messages)
    Enum.any?(msgs, &match?({^port, {:data, {:eol, line}}} when binary_part(line, 0, byte_size(prefix)) == prefix, &1))
  end

  defp queued_record?(port, prefix, timeout_ms), do: wait_until(fn -> queued_record?(port, prefix) end, timeout_ms)

  defp counting!, do: Process.put(:clock_counting, true)
  defp reads, do: Process.get(:clock_reads, 0)
  defp canary, do: "CANARY" <> Base.encode16(:crypto.strong_rand_bytes(6))
  defp with_canary(line, c), do: String.replace(line, "CANARY", c)

  # ---- OS oracles ----
  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n", trim: true) |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
      {"", 1} -> []
      {out, status} -> flunk("ps proved nothing (#{status}): #{inspect(out)}")
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do:
      signal_zero(Integer.to_string(pid)) == :gone and signal_zero("-" <> Integer.to_string(pgid)) == :gone and
        members(pgid) == []

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    fun.()
  end

  # ================= RB: baselines on the EXISTING API (green today) =================

  defp track(identity),
    do: on_exit(fn -> wait_until(fn -> dead?(identity) end, 15_000) || raise("owned group still present") end)

  # cleanup: expire the handle past its deadline, then restore the test clock for any later prepare
  defp expire_far!(running) do
    Process.put(:resumable_now, @far + 1)
    assert {:timeout, _} = Execution.expire(running)
    Process.put(:resumable_now, @now)
  end

  # hand the next dequeued record for THIS port to resume_await exactly as an owner's handle_info would; every
  # resume answers {:done, _} (a non-terminal record is the closed await_failed, as in await/2)
  defp drain_until_done(waiting, port) do
    receive do
      {^port, _} = msg -> Execution.resume_await(waiting, msg)
    after
      10_000 -> flunk("no Port record arrived")
    end
  end

  describe "RB baselines" do
    test "RB-1 (GP-M1 control) group TERM from outside: the leader dies by signal, the guardian reports EXIT kind=signaled signal=15",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      {_, 0} = System.cmd("kill", ["-TERM", "-" <> Integer.to_string(identity.pgid)], stderr_to_stdout: true)
      assert queued_record?(port, "EXIT ", 10_000), "the EXIT record is queued for the owner"
      assert {:exit, outcome} = Execution.await(running, opts)
      assert outcome["kind"] == "signaled" and to_string(outcome["signal"]) == "15"
      assert outcome["settled"] == true and outcome["proof"] == "gone"
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "RB-2 (GP-M1 positive) TERM on the OWNER's control channel: the guardian settles by force and reports DEAD reason=command",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert Port.info(port, :connected) == {:connected, self()}
      assert Port.command(port, "TERM\n")
      assert queued_record?(port, "DEAD reason=command", 10_000), "the DEAD record is queued for the owner"
      assert {:timeout, termination} = Execution.await(running, opts)
      assert %{kind: "timeout", settled: true, proof: "gone"} = termination
      refute Map.has_key?(termination, :backstop)
      assert Process.get({Execution, port, :termination}) == termination
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "RB-2b (GP-M7 positive reference) the declared duplicate DEAD line, injected ahead of the unchanged await, is a valid DEAD: timeout memo",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      send(self(), {port, {:data, {:eol, @dead_dup}}})
      assert {:timeout, %{kind: "timeout", settled: true, proof: "gone"} = termination} = Execution.await(running, opts)
      assert Process.get({Execution, port, :termination}) == termination
      # the live guardian was never told: drain_exit closed the Port, the guardian settles on control EOF
      assert wait_until(fn -> dead?(identity) end, 15_000)
    end

    test "RB-3 (GP-M8 parser reference) one grammar defect per row from a complete valid EXIT: the EXACT closed diagnostic; the valid neighbour is accepted",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(sub!(run_dir, "valid"), ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      send(self(), {port, {:data, {:eol, @exit_ok}}})
      assert {:exit, %{"kind" => "exited", "exit_status" => 0}} = Execution.await(running, opts)
      assert wait_until(fn -> dead?(identity) end, 15_000)

      for {name, line, description} <- @malformed do
        {running, _} = running!(sub!(run_dir, name), ["/bin/sleep", "30"], @far, opts)
        port = port_of(running)
        c = canary()
        send(self(), {port, {:data, {:eol, with_canary(line, c)}}})
        answer = Execution.await(running, opts)
        assert answer == {:error, %{clause: "await_failed", record: description}}, name
        refute inspect(answer) =~ c, name
        expire_far!(running)
      end
    end
  end

  describe "R-1 pending" do
    test "begin_await on a running far-deadline handle returns {:pending, waiting} promptly with ZERO clock reads; duplicate begin while pending reuses the same descriptor",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      counting!()
      t0 = System.monotonic_time(:millisecond)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      # ================= R: the primitive interface (RED today) =================
      assert System.monotonic_time(:millisecond) - t0 < 200
      assert reads() == 0, "pending is not a clock decision"
      assert waiting.port == port
      assert Port.info(port, :connected) == {:connected, self()}
      assert {:pending, ^waiting} = Execution.begin_await(running, opts)
      assert reads() == 0
      expire_far!(running)
    end

    test "past-due with NO queued terminal is still {:pending} without a clock read (the deadline is the owner harness's, not begin's)",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, ["/bin/sleep", "30"], @now + 1, opts)
      Process.put(:resumable_now, @now + 50)
      counting!()
      assert {:pending, _} = Execution.begin_await(running, opts)
      assert reads() == 0
      assert {:timeout, _} = Execution.expire(running)
    end
  end

  describe "R-2 memo" do
    test "after expire/1 (Port drained and closed by the owner) begin_await answers {:done, {:timeout, same}} from the owner-local memo; live Port info is nil and irrelevant",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:timeout, termination} = Execution.expire(running)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert wait_until(fn -> Port.info(port) == nil end, 3_000)
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
      assert Execution.await(running, opts) == {:timeout, termination}
    end
  end

  describe "R-3 queued" do
    test "an EXIT record WITNESSED queued (not consumed) is answered by begin_await before any clock decision, even past the deadline; that begin finalizes the active descriptor (GP-M6)",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, gated(run_dir, "exit 0"), @now + 1, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      release_gate!(run_dir)
      assert queued_record?(port, "EXIT ", 10_000)
      Process.put(:resumable_now, @now + 50)
      counting!()
      assert {:done, {:exit, outcome}} = Execution.begin_await(running, opts)
      assert reads() == 0
      assert Execution.pass?(outcome)
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, {port, {:data, {:eol, @exit_ok}}}) end
    end

    test "a terminal record received BEFORE the Port closed is valid evidence: begin_await answers it although Port.info is nil (G-N3)",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, ["/bin/sh", "-c", "exit 0"], @far, opts)
      port = port_of(running)
      assert queued_record?(port, "EXIT ", 10_000)
      assert wait_until(fn -> Port.info(port) == nil end, 5_000), "the guardian exited; the Port closed"
      assert {:done, {:exit, outcome}} = Execution.begin_await(running, opts)
      assert Execution.pass?(outcome)
    end
  end

  describe "R-4 resume" do
    test "EXIT via one dequeued record, released only after {:pending}: exit_status 3, kind exited, and field parity with an unchanged await/2 baseline (duration normalised)",
         %{run_dir: run_dir, opts: opts} do
      {b_running, b_identity} = running!(sub!(run_dir, "baseline"), ["/bin/sh", "-c", "exit 3"], @far, opts)
      assert {:exit, baseline} = Execution.await(b_running, opts)
      assert wait_until(fn -> dead?(b_identity) end, 10_000)

      {running, identity} = running!(run_dir, gated(run_dir, "exit 3"), @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      release_gate!(run_dir)
      assert {:done, {:exit, outcome}} = drain_until_done(waiting, port)
      assert outcome["exit_status"] == 3 and outcome["kind"] == "exited"
      assert outcome["settled"] == true and outcome["proof"] == "gone" and outcome["leftovers"] == "0"
      assert Map.delete(outcome, "duration_ms") == Map.delete(baseline, "duration_ms")
      assert is_integer(outcome["duration_ms"]) and outcome["duration_ms"] >= 0
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "DEAD via the owner's control channel (GP-M1): TERM on the retained Port after {:pending}, the DEAD record handed to resume_await, memoised HERE; replay: begin reads, reuse raises (GP-M6)",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      assert Port.command(port, "TERM\n")
      assert queued_record?(port, "DEAD reason=command", 10_000)
      assert {:done, {:timeout, termination}} = drain_until_done(waiting, port)
      assert %{kind: "timeout", settled: true, proof: "gone"} = termination
      refute Map.has_key?(termination, :backstop)
      assert Process.get({Execution, port, :termination}) == termination
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, {port, {:data, {:eol, @dead_dup}}}) end
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end
  end

  describe "R-5 grammar" do
    test "malformed :eol matrix through resume_await equals the WHOLE closed diagnostic of the unchanged await on a sibling gate; canary never echoed; valid neighbour accepted",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(sub!(run_dir, "valid"), ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)

      assert {:done, {:exit, %{"kind" => "exited", "exit_status" => 0}}} =
               Execution.resume_await(waiting, {port, {:data, {:eol, @exit_ok}}})

      assert wait_until(fn -> dead?(identity) end, 15_000)

      for {name, line, description} <- @malformed do
        c = canary()
        {ref_running, _} = running!(sub!(run_dir, name <> "_ref"), ["/bin/sleep", "30"], @far, opts)
        send(self(), {port_of(ref_running), {:data, {:eol, with_canary(line, c)}}})
        reference = Execution.await(ref_running, opts)
        assert reference == {:error, %{clause: "await_failed", record: description}}, name
        expire_far!(ref_running)

        {running, _} = running!(sub!(run_dir, name), ["/bin/sleep", "30"], @far, opts)
        port = port_of(running)
        assert {:pending, waiting} = Execution.begin_await(running, opts)
        msg = {port, {:data, {:eol, with_canary(line, c)}}}
        answer = Execution.resume_await(waiting, msg)
        assert answer == {:done, reference}, name
        refute inspect(answer) =~ c, name
        assert_raise ArgumentError, fn -> Execution.resume_await(waiting, msg) end
        expire_far!(running)
      end
    end

    test ":noeol (overlong) and a premature :exit_status answer closed await_failed with no raw bytes", %{
      run_dir: run_dir,
      opts: opts
    } do
      {running, _} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      c = canary()
      assert {:pending, w1} = Execution.begin_await(running, opts)
      answer = Execution.resume_await(w1, {port, {:data, {:noeol, c}}})
      assert answer == {:done, {:error, %{clause: "await_failed", record: "malformed"}}}
      refute inspect(answer) =~ c
      {running2, _} = running!(sub!(run_dir, "b"), ["/bin/sleep", "30"], @far, opts)
      port2 = port_of(running2)
      assert {:pending, w2} = Execution.begin_await(running2, opts)

      assert {:done, {:error, %{clause: "await_failed", record: "exited 0"}}} =
               Execution.resume_await(w2, {port2, {:exit_status, 0}})

      expire_far!(running)
      expire_far!(running2)
    end
  end

  describe "R-6 ownership" do
    test "a message for a different Port raises ArgumentError (never pending); wrong-caller begin AND resume raise; the owner's handle is untouched",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      other = Port.open({:spawn_executable, "/bin/cat"}, [:binary])
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, {other, {:data, {:eol, @exit_ok}}}) end
      Port.close(other)
      owner = self()

      caller = fn fun ->
        fn ->
          try do
            {:returned, fun.()}
          rescue
            e in ArgumentError -> {:raised, e.__struct__}
          end
        end
        |> Task.async()
        |> Task.await(5_000)
      end

      assert {:raised, ArgumentError} = caller.(fn -> Execution.begin_await(running, opts) end)

      assert {:raised, ArgumentError} =
               caller.(fn -> Execution.resume_await(waiting, {port, {:data, {:eol, @exit_ok}}}) end)

      assert Port.info(port, :connected) == {:connected, owner}
      assert {:pending, ^waiting} = Execution.begin_await(running, opts)
      expire_far!(running)
    end

    test "closed WITHOUT memo and without a queued terminal (owner closed the Port): {:done, {:error, guardian_gone}} on the ORIGINAL handle, no raw data",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      Port.close(port)
      assert Port.info(port) == nil
      assert {:done, {:error, %{clause: "guardian_gone"}}} = Execution.begin_await(running, opts)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end
  end

  describe "R-6b lifecycle (reviews m_1788807761787369000, m_1788807900000)" do
    # imported verbatim from the reviewer's runner (logs/review-3fdee64/probes.exs)
    test "PG-1 a terminal descriptor never becomes active again through begin", %{run_dir: dir, opts: opts} do
      {running, _} = running!(dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, old} = Execution.begin_await(running, opts)
      message = {port, {:data, {:eol, "BOGUS x=1"}}}
      assert {:done, {:error, %{clause: "await_failed"}}} = Execution.resume_await(old, message)
      assert_raise ArgumentError, fn -> Execution.resume_await(old, message) end

      try do
        # Either a refused restart or a fresh lifecycle may be specified; neither revives old.
        try do
          Execution.begin_await(running, opts)
        rescue
          ArgumentError -> :refused
        end

        assert_raise ArgumentError, fn -> Execution.resume_await(old, message) end
      after
        expire_far!(running)
      end
    end

    test "PG-2 closed Port does not authorize a foreign caller", %{run_dir: dir, opts: opts} do
      {running, identity} = running!(dir, ["/bin/sleep", "30"], @far, opts)
      assert {:timeout, termination} = Execution.expire(running)
      assert wait_until(fn -> Port.info(port_of(running)) == nil end, 3_000)
      assert wait_until(fn -> dead?(identity) end, 10_000)

      result =
        fn ->
          try do
            {:returned, Execution.begin_await(running, opts)}
          rescue
            ArgumentError -> :refused
          end
        end
        |> Task.async()
        |> Task.await(5_000)

      assert result == :refused
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
    end

    # the accepted design (precision m_1788807970000): a fresh wait after a terminal error carries a fresh internal
    # ref; the old descriptor never matches; a duplicate begin of the active wait returns the SAME stored value;
    # no fresh native GO/release happens (only the existing retained handle is awaited)
    test "R-6c after a terminal error a fresh begin is a NEW distinct descriptor; the old one stays consumed; duplicate begin reuses the fresh one; no native re-release",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      released_at = Process.get({Execution, port, :released_at_ms})
      assert {:pending, old} = Execution.begin_await(running, opts)
      msg = {port, {:data, {:eol, "BOGUS x=1"}}}
      assert {:done, {:error, %{clause: "await_failed"}}} = Execution.resume_await(old, msg)
      assert {:pending, fresh} = Execution.begin_await(running, opts)
      refute fresh == old, "a recreated descriptor must never equal a consumed one"
      assert_raise ArgumentError, fn -> Execution.resume_await(old, msg) end
      assert {:pending, ^fresh} = Execution.begin_await(running, opts)
      # only the existing retained handle is awaited: same Port, the release memo untouched, no second GO
      assert fresh.port == port and fresh.running == running
      assert Process.get({Execution, port, :released}) == true
      assert Process.get({Execution, port, :released_at_ms}) == released_at
      assert Port.info(port, :connected) == {:connected, self()}
      expire_far!(running)
    end

    test "control: after a finalized descriptor, cleanup through the original handle then begin READS the memo (no refusal)",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, old} = Execution.begin_await(running, opts)
      assert {:done, {:error, _}} = Execution.resume_await(old, {port, {:data, {:eol, "BOGUS x=1"}}})
      assert {:timeout, termination} = Execution.expire(running)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
    end

    test "control: after a finalized descriptor, a LATER queued terminal is still consumed by begin (queued proof precedence, no refusal)",
         %{run_dir: run_dir, opts: opts} do
      {running, _} = running!(run_dir, gated(run_dir, "exit 0"), @far, opts)
      port = port_of(running)
      assert {:pending, old} = Execution.begin_await(running, opts)
      assert {:done, {:error, _}} = Execution.resume_await(old, {port, {:data, {:eol, "BOGUS x=1"}}})
      release_gate!(run_dir)
      assert queued_record?(port, "EXIT ", 10_000)
      assert {:done, {:exit, outcome}} = Execution.begin_await(running, opts)
      assert Execution.pass?(outcome)
      assert_raise ArgumentError, fn -> Execution.resume_await(old, {port, {:data, {:eol, @exit_ok}}}) end
    end

    test "control: foreign begin after the owner closed the Port WITHOUT memo raises; the owner itself gets guardian_gone",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      Port.close(port)
      assert Port.info(port) == nil

      result =
        fn ->
          try do
            {:returned, Execution.begin_await(running, opts)}
          rescue
            ArgumentError -> :refused
          end
        end
        |> Task.async()
        |> Task.await(5_000)

      assert result == :refused
      assert {:done, {:error, %{clause: "guardian_gone"}}} = Execution.begin_await(running, opts)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end
  end

  describe "R-7 consumer" do
    test "a waiting value is consumed by its terminal EXIT (released only after pending): resuming it again raises ArgumentError",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, gated(run_dir, "exit 0"), @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      release_gate!(run_dir)
      assert {:done, {:exit, _}} = drain_until_done(waiting, port)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, {port, {:data, {:eol, @exit_ok}}}) end
    end
  end

  describe "R-8 expiry" do
    test "external expiry during pending: foreign Port refused BEFORE memo lookup; the FIRST valid resume returns the exact memo and finalizes; the second raises; begin still reads (GP-M6/M7)",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      assert {:timeout, termination} = Execution.expire(running)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
      other = Port.open({:spawn_executable, "/bin/cat"}, [:binary])
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, {other, {:data, {:eol, @dead_dup}}}) end
      Port.close(other)
      # INJECTION (declared duplicate of a VALID guardian DEAD line, RB-2b reference), bound and matched exactly
      tuple = {port, {:data, {:eol, @dead_dup}}}
      send(self(), tuple)
      assert_receive ^tuple, 1_000
      assert Execution.resume_await(waiting, tuple) == {:done, {:timeout, termination}}
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, tuple) end
      assert Execution.begin_await(running, opts) == {:done, {:timeout, termination}}
    end

    test "a malformed late record for the RIGHT Port after external expiry passes ownership/Port validation and is answered from the memo, finalizing the descriptor",
         %{run_dir: run_dir, opts: opts} do
      {running, identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
      port = port_of(running)
      assert {:pending, waiting} = Execution.begin_await(running, opts)
      assert {:timeout, termination} = Execution.expire(running)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      tuple = {port, {:data, {:eol, "BOGUS x=1"}}}
      send(self(), tuple)
      assert_receive ^tuple, 1_000
      assert Execution.resume_await(waiting, tuple) == {:done, {:timeout, termination}}
      assert_raise ArgumentError, fn -> Execution.resume_await(waiting, tuple) end
    end
  end
end
