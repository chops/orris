defmodule AiOrchestrator.Gate.RecordRetentionRedTest do
  @moduledoc """
  RED/interface + controls for owned gate record retention (docs/contracts/gate-record-retention-proposal.org,
  revision 4; scope m_1788822780000, corrections m_1788823400000 + m_1788824040000). `RT-` rows name the proposed
  `Execution.stage/2`, the retention-aware `next_record/2` / `drain_exit/1`, the Worker routing clause and the
  `:retention_observer` boot seam; they fail today for interface absence. Rows titled "control" measure CURRENT
  behaviour and pass today. Synthetic EXIT/DEAD lines prove parser/consumer behaviour only, never real-world
  settlement; rows on the real guardian's own records say so. Execution-level rows own their Port in this test
  process; Worker-level rows use the real `Run.Worker` and a bounded `:sys.get_state/2` as the ordered loop barrier.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.ScenarioHarness
  alias AiOrchestrator.Test.StepClock

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)
  @run_id "run_retention"
  @gate "gr_0001"
  @now 1_700_000_000
  @far @now + 600
  @exit_ok "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"
  @dead_ok "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=gone escaped=unknown"
  @dead_bad "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=CANARY escaped=unknown"
  @bogus "BOGUS kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"
  @loop 5_000

  # counts BOTH clock methods once activated (RT-7)
  defmodule Clock do
    @moduledoc false
    def unix_now do
      count(:unix)
      Process.get(:retention_now, 1_700_000_000)
    end

    def monotonic_ms do
      count(:mono)
      System.monotonic_time(:millisecond)
    end

    defp count(which) do
      if Process.get(:clock_counting), do: Process.put({:clock_reads, which}, Process.get({:clock_reads, which}, 0) + 1)
    end
  end

  # RT-11: a delegating executor that deliberately has NO stage/2
  defmodule NoStageExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate await(running, opts), to: Execution
    defdelegate abandon(handle), to: Execution
    defdelegate identity(handle), to: Execution
    defdelegate started_data(prepared), to: Execution
  end

  # RT-27..RT-29b: stage/2 whose behaviour is configured OWNER-SIDE through the handle's own opts (seams gate_opts)
  defmodule ModeExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate await(running, opts), to: Execution
    defdelegate abandon(handle), to: Execution
    defdelegate identity(handle), to: Execution
    defdelegate started_data(prepared), to: Execution

    # the selected mode is witnessed to the OWNER-SIDE recipient named in the handle opts, from the executing pid
    def stage(%{opts: opts, port: port}, _message) do
      mode = Keyword.fetch!(opts, :stage_mode)
      send(Keyword.fetch!(opts, :stage_witness), {:stage_mode_selected, mode, self()})

      case mode do
        :raise ->
          raise("stage escaped")

        :throw ->
          throw(:stage_thrown)

        :exit ->
          exit(:stage_exited)

        :invalid ->
          :not_a_disposition

        :write_then_raise ->
          Process.put({Execution, port, :staged}, [{:custom, :written}])
          raise("after write")
      end
    end
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "retention-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    Process.put(:retention_now, @now)
    Process.delete(:clock_counting)
    StepClock.set(@now, 0)
    on_exit(fn -> StepClock.clear() end)
    run_dir = Path.join(System.tmp_dir!(), "retention-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf(run_dir) end)
    test = self()
    # barrier oracle: every barrier crossing is reported with its monotonic instant (RT-7, RT-1e, RT-18)
    # barrier oracle: every crossing is reported with the CALLER's probe token (nil during setup) and its instant
    barrier = fn name, _info ->
      send(test, {:barrier, Process.get(:probe_token), name, System.monotonic_time(:millisecond)})
      true
    end

    opts = [helper: helper, settle_ms: 200, rounds: 2, clock: Clock, barrier: barrier]
    {:ok, run_dir: run_dir, helper: helper, opts: opts}
  end

  # ---- in-process owner driver ----

  defp request(run_dir, argv, deadline) do
    %{
      run_id: @run_id,
      gate_run_id: @gate,
      attempt: 1,
      command_argv: argv,
      repo_root: run_dir,
      run_dir: run_dir,
      deadline_unix: deadline,
      supervisor_instance: "sup_0001"
    }
  end

  defp persist!(run_dir, data) do
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
      "data" => data
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
    assert {:ok, ack} = Execution.ack(prepared, persist!(run_dir, Execution.started_data(prepared)))
    assert {:ok, running} = Execution.release(prepared, ack, opts)
    {running, identity}
  end

  # the REAL guardian's own EXIT and exit_status, dequeued into the caller (real records, not synthetic)
  defp exited!(run_dir, opts) do
    {running, identity} = running!(run_dir, ["/bin/sh", "-c", "exit 0"], @far, opts)
    port = running.port
    assert wait_until(fn -> dead?(identity) end, 10_000)
    {running, port, dequeue!(port, "EXIT "), dequeue_status!(port)}
  end

  defp sleeping!(run_dir, opts) do
    {running, _identity} = running!(run_dir, ["/bin/sleep", "30"], @far, opts)
    {running, running.port}
  end

  defp dequeue!(port, prefix) do
    receive do
      {^port, {:data, {:eol, line}}} = msg when binary_part(line, 0, byte_size(prefix)) == prefix -> msg
    after
      10_000 -> flunk("no #{prefix} record for the port")
    end
  end

  defp dequeue_status!(port) do
    receive do
      {^port, {:exit_status, _}} = msg -> msg
    after
      10_000 -> flunk("no exit_status for the port")
    end
  end

  defp line(port, text), do: {port, {:data, {:eol, text}}}
  # RED: the primitive does not exist yet; a runtime module value keeps --warnings-as-errors honest
  defp execution, do: Module.concat(["AiOrchestrator", "Gate", "Execution"])
  defp staged(port), do: Process.get({Execution, port, :staged}, [])
  defp memos(port), do: for(k <- [:released, :termination, :descriptor], do: {k, Process.get({Execution, port, k})})
  defp counting!, do: Process.put(:clock_counting, true)
  defp reads(which), do: Process.get({:clock_reads, which}, 0)

  # purity checker (RT-M13): runs `fun` under a UNIQUE probe token armed AFTER setup and reports every violation:
  # a clock read (either method), a barrier crossing carrying this token, a consumed native canary on `port`, or
  # a closed Port; setup's own barrier events carry a nil token and never count
  defp purity_violations(fun, port, canary) do
    token = make_ref()
    Process.put(:probe_token, token)
    Process.put({:clock_reads, :unix}, 0)
    Process.put({:clock_reads, :mono}, 0)
    send(self(), canary)
    counting!()
    _ = fun.()
    Process.delete(:clock_counting)
    Process.delete(:probe_token)

    crossed =
      receive do
        {:barrier, ^token, _name, _t} -> [:barrier_crossed]
      after
        0 -> []
      end

    eaten =
      receive do
        ^canary -> []
      after
        0 -> [:canary_consumed]
      end

    clocks = Enum.filter([:unix_clock_read, :mono_clock_read], fn v -> reads(clock_of(v)) > 0 end)
    clocks ++ crossed ++ eaten ++ [if(Port.info(port) == nil, do: :port_closed, else: :port_open)]
  end

  defp clock_of(:unix_clock_read), do: :unix
  defp clock_of(:mono_clock_read), do: :mono

  defp sub!(run_dir, name) do
    dir = Path.join(run_dir, name)
    File.mkdir_p!(dir)
    dir
  end

  defp expire_far!(running) do
    Process.put(:retention_now, @far + 1)
    _ = Execution.expire(running)
    Process.put(:retention_now, @now)
  end

  # the drain boundary: exit_outcome crosses :after_exit BEFORE drain_exit, so return minus that instant bounds
  # the drain; a legacy fallback drain costs >= 1000 ms (RT-M10)
  defp drain_bound!(returned_at) do
    assert_receive {:barrier, _token, :after_exit, t1}, 0
    assert returned_at - t1 < 500, "no fallback 1000 ms drain after the staged/mailbox exit_status was available"
  end

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

  defp track(identity),
    do: on_exit(fn -> wait_until(fn -> dead?(identity) end, 15_000) || raise("owned group still present") end)

  # ================= Execution-level rows (owner = this process) =================

  describe "RT staging (RED: Execution.stage/2)" do
    test "RT-1e real EXIT + exit_status staged: await answers at once; drain consumed the status slot",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, exit_msg)
      assert :staged = execution().stage(running, status_msg)
      assert [{"EXIT", _}, {:exited, 0}] = staged(port)
      assert {:exit, %{"kind" => "exited", "exit_status" => 0}} = Execution.await(running, o)
      drain_bound!(System.monotonic_time(:millisecond))
      assert [] == staged(port), "EXIT consumed by next_record, exited by drain_exit"
    end

    test "RT-2 staged exit_status before a later EXIT: await_failed exited (mailbox parity)", %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, status_msg)
      assert :staged = execution().stage(running, exit_msg)
      assert {:error, %{clause: "await_failed", record: "exited 0"}} = Execution.await(running, o)
      assert match?([{"EXIT", _}], staged(port)), "the later EXIT is not promoted"
    end

    test "RT-2c control: the same sequence through the mailbox gives the same answer", %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      send(self(), status_msg)
      send(self(), exit_msg)
      assert {:error, %{clause: "await_failed", record: "exited 0"}} = Execution.await(running, o)
      assert_receive {^port, {:data, {:eol, "EXIT " <> _}}}, 0
    end

    test "RT-3 staged malformed line (synthetic) before a later real EXIT: await_failed malformed", %{run_dir: d, opts: o} do
      {running, port, exit_msg, _status} = exited!(d, o)
      assert :staged = execution().stage(running, line(port, @bogus))
      assert :staged = execution().stage(running, exit_msg)
      assert {:error, %{clause: "await_failed", record: "malformed"}} = Execution.await(running, o)
      assert [{"EXIT", _}] = staged(port)
    end

    test "RT-16 unsupported Port payload: :unsupported, unstored; malformed native line: classified slot",
         %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert :unsupported = execution().stage(running, {port, :connected})
      assert :unsupported = execution().stage(running, {port, {:data, "not a line tuple"}})
      assert [] == staged(port)
      assert :staged = execution().stage(running, line(port, @bogus))
      assert [{:malformed, "unknown_head"}] = staged(port)
      expire_far!(running)
    end

    test "RT-5 foreign Port (another owned handle's): :foreign, no key, no tombstone", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      {other, other_port} = sleeping!(sub!(d, "b"), o)
      assert :foreign = execution().stage(running, line(other_port, @exit_ok))
      assert nil == Process.get({Execution, other_port, :staged})
      assert [] == staged(port)
      expire_far!(running)
      expire_far!(other)
    end

    test "RT-6 finalized by termination memo: :finalized, nothing stored, memos byte-identical", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      expire_far!(running)
      before = memos(port)
      assert {:termination, %{kind: "timeout"}} = List.keyfind(before, :termination, 0)
      assert :finalized = execution().stage(running, line(port, @exit_ok))
      assert [] == staged(port)
      assert before == memos(port)
    end

    test "RT-6b finalized DESCRIPTOR (no termination memo): :finalized, nothing stored", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      # a synthetic malformed record finalizes the descriptor through the closed await_failed (no exit_outcome I/O)
      assert {:done, {:error, %{clause: "await_failed"}}} = Execution.resume_await(waiting, line(port, @bogus))
      assert :finalized == Process.get({Execution, port, :descriptor})
      assert nil == Process.get({Execution, port, :termination})
      assert :finalized = execution().stage(running, line(port, @exit_ok))
      assert [] == staged(port)
      expire_far!(running)
    end

    test "RT-7 stage/2 reads neither clock, crosses no barrier, closes no Port, consumes no native message",
         %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      canary = line(port, "EXIT kind=exited status=7 settled=1 leftovers=0 proof=gone escaped=unknown")
      staged_answer = fn -> send(self(), {:answer, execution().stage(running, line(port, @exit_ok))}) end
      assert [:port_open] == purity_violations(staged_answer, port, canary)
      assert_receive {:answer, :staged}, 0
      assert [{"EXIT", _}] = staged(port)
      expire_far!(running)
    end

    test "RT-7c control: the purity checker passes a no-op and rejects each injected impurity", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      canary = line(port, @exit_ok)
      assert_receive {:barrier, nil, :after_ready, _}, 0
      assert [:port_open] == purity_violations(fn -> :noop end, port, canary)
      assert [:unix_clock_read, :port_open] == purity_violations(fn -> Clock.unix_now() end, port, canary)
      assert [:mono_clock_read, :port_open] == purity_violations(fn -> Clock.monotonic_ms() end, port, canary)
      barrier = Keyword.fetch!(o, :barrier)
      assert [:barrier_crossed, :port_open] == purity_violations(fn -> barrier.(:probe, nil) end, port, canary)
      consume = fn -> receive(do: (^canary -> :eaten)) end
      assert [:canary_consumed, :port_open] == purity_violations(consume, port, canary)
      expire_far!(running)
    end

    test "RT-8 existing memos untouched by staging", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      before = memos(port)
      assert :staged = execution().stage(running, line(port, @bogus))
      assert before == memos(port)
      expire_far!(running)
    end

    test "RT-9 begin_await consumes a staged real terminal first and finalizes as its scan does", %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, exit_msg)
      assert :staged = execution().stage(running, status_msg)
      assert {:done, {:exit, %{"kind" => "exited"}}} = Execution.begin_await(running, o)
      assert [] == staged(port)
    end

    test "RT-12 closed Port with retained evidence stages; closed Port without evidence: :not_owner",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, _status} = exited!(d, o)
      assert wait_until(fn -> Port.info(port) == nil end, 3_000)
      assert :staged = execution().stage(running, exit_msg)
      stranger = %{running | port: Port.open({:spawn, "/bin/true"}, [:binary])}
      assert wait_until(fn -> Port.info(stranger.port) == nil end, 3_000)
      assert :not_owner = execution().stage(stranger, line(stranger.port, @exit_ok))
      assert nil == Process.get({Execution, stranger.port, :staged})
      assert {:exit, _} = Execution.await(running, o)
    end

    test "RT-12b a LIVE Port owned by another process: :not_owner, nothing stored", %{run_dir: d, opts: o} do
      {running, _port} = sleeping!(d, o)
      test = self()

      {holder, monitor} =
        spawn_monitor(fn ->
          port = Port.open({:spawn, "/bin/sleep 5"}, [:binary])
          send(test, {:held_port, port})
          receive do: (:release -> Port.close(port))
        end)

      on_exit(fn ->
        if Process.alive?(holder) do
          ref = Process.monitor(holder)
          Process.exit(holder, :kill)
          receive do: ({:DOWN, ^ref, :process, ^holder, _} -> :ok)
        end
      end)

      assert_receive {:held_port, foreign}, 2_000
      assert {:connected, ^holder} = Port.info(foreign, :connected)
      assert :not_owner = execution().stage(%{running | port: foreign}, line(foreign, @exit_ok))
      assert nil == Process.get({Execution, foreign, :staged})
      send(holder, :release)
      assert_receive {:DOWN, ^monitor, :process, ^holder, :normal}, 2_000
      expire_far!(running)
    end
  end

  describe "RT drain (RED: retention-aware drain_exit/1)" do
    test "RT-18 real EXIT staged, exit_status still in the mailbox: drain receives it, no fallback drain",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, exit_msg)
      send(self(), status_msg)
      assert {:exit, _} = Execution.await(running, o)
      drain_bound!(System.monotonic_time(:millisecond))
      refute_receive {^port, {:exit_status, _}}, 0
      assert [] == staged(port)
    end

    test "RT-19 an intervening non-status slot between EXIT and exit_status is preserved in place",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, exit_msg)
      assert :staged = execution().stage(running, line(port, @bogus))
      assert :staged = execution().stage(running, status_msg)
      assert {:exit, _} = Execution.await(running, o)
      drain_bound!(System.monotonic_time(:millisecond))
      assert [{:malformed, "unknown_head"}] == staged(port), "drain popped only the exited slot"
    end
  end

  describe "RT settle (RED: staged input to terminate/2; synthetic records = parser/consumer proof)" do
    test "RT-20 staged EXIT before DEAD: abandon answers settle_unproven record EXIT; DEAD not skipped to",
         %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert :staged = execution().stage(running, line(port, @exit_ok))
      assert :staged = execution().stage(running, line(port, @dead_ok))
      assert {:error, %{clause: "settle_unproven", record: "EXIT"}} = Execution.abandon(running)
      assert [{"DEAD", _}] = staged(port)
      _ = Execution.expire(running)
    end

    test "RT-20c control: the same EXIT-then-DEAD through the mailbox gives the same abandon answer",
         %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      send(self(), line(port, @exit_ok))
      send(self(), line(port, @dead_ok))
      assert {:error, %{clause: "settle_unproven", record: "EXIT"}} = Execution.abandon(running)
      assert_receive {^port, {:data, {:eol, "DEAD " <> _}}}, 0
    end

    test "RT-21 staged valid DEAD: abandon :ok (proof from the record)", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert :staged = execution().stage(running, line(port, @dead_ok))
      assert :ok = Execution.abandon(running)
      assert [] == staged(port)
    end

    test "RT-21b staged DEAD with a rejected proof field: settle_unproven 'malformed DEAD'", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert :staged = execution().stage(running, line(port, @dead_bad))
      assert {:error, %{clause: "settle_unproven", record: "malformed DEAD"}} = Execution.abandon(running)
    end

    test "RT-21c control: the same rejected DEAD through the mailbox: settle_unproven 'malformed DEAD'",
         %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      send(self(), line(port, @dead_bad))
      assert {:error, %{clause: "settle_unproven", record: "malformed DEAD"}} = Execution.abandon(running)
    end

    test "RT-22 closed Port + staged EXIT/status/DEAD: guardian_gone; drain removes only the status slot",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert wait_until(fn -> Port.info(port) == nil end, 3_000)
      assert :staged = execution().stage(running, exit_msg)
      assert :staged = execution().stage(running, status_msg)
      assert :staged = execution().stage(running, line(port, @dead_ok))
      assert {:error, %{clause: "guardian_gone"}} = Execution.abandon(running)

      assert match?([{"EXIT", _}, {"DEAD", _}], staged(port)),
             "next_record not consulted; unconditional drain took the status"
    end

    test "RT-22b expire with staged EXIT before DEAD: error and NO termination memo", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      assert :staged = execution().stage(running, line(port, @exit_ok))
      assert :staged = execution().stage(running, line(port, @dead_ok))
      assert {:error, %{clause: "settle_unproven", record: "EXIT"}} = Execution.expire(running)
      assert nil == Process.get({Execution, port, :termination})
      _ = Execution.expire(running)
    end
  end

  describe "RT overflow (RED: bounded tail marker)" do
    test "RT-23/25 fifth enqueue adds one sentinel; repeated overflow adds nothing", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      for _ <- 1..4, do: assert(:staged = execution().stage(running, line(port, @bogus)))
      assert :overflow = execution().stage(running, line(port, @exit_ok))
      assert :overflow = execution().stage(running, line(port, @exit_ok))
      assert length(staged(port)) == 5
      assert List.last(staged(port)) == {:malformed, "staged_overflow"}
      expire_far!(running)
    end

    test "RT-24 enqueue after a partial drain is stored normally, no sentinel", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      for _ <- 1..4, do: assert(:staged = execution().stage(running, line(port, @bogus)))
      assert {:error, %{clause: "await_failed"}} = Execution.await(running, o)
      assert length(staged(port)) == 3
      assert :staged = execution().stage(running, line(port, @bogus))
      assert length(staged(port)) == 4
      refute {:malformed, "staged_overflow"} in staged(port)
      expire_far!(running)
    end

    test "RT-25b the sentinel is consumed FIFO like any slot; staging recovers afterwards", %{run_dir: d, opts: o} do
      {running, port} = sleeping!(d, o)
      for _ <- 1..4, do: assert(:staged = execution().stage(running, line(port, @bogus)))
      assert :overflow = execution().stage(running, line(port, @bogus))
      for _ <- 1..4, do: assert({:error, %{clause: "await_failed", record: "malformed"}} = Execution.await(running, o))
      assert [{:malformed, "staged_overflow"}] = staged(port)
      assert {:error, %{clause: "await_failed", record: "malformed"}} = Execution.await(running, o)
      assert [] == staged(port)
      assert :staged = execution().stage(running, line(port, @bogus))
      expire_far!(running)
    end

    test "RT-26 an earlier staged real terminal finalizes first; the sentinel is never observed by that await",
         %{run_dir: d, opts: o} do
      {running, port, exit_msg, status_msg} = exited!(d, o)
      assert :staged = execution().stage(running, exit_msg)
      assert :staged = execution().stage(running, status_msg)
      for _ <- 1..2, do: assert(:staged = execution().stage(running, line(port, @bogus)))
      assert :overflow = execution().stage(running, line(port, @bogus))
      assert {:exit, _} = Execution.await(running, o)
      assert List.last(staged(port)) == {:malformed, "staged_overflow"}
    end
  end

  # ================= Worker-level rows (real Run.Worker) =================

  defp native_seams(ctx, executor \\ Execution, gate_extra \\ [], extra \\ []) do
    [
      gate_executor: executor,
      gate_helper: ctx.helper,
      gate_opts: [settle_ms: 200, rounds: 2, clock: StepClock, barrier: identity_barrier(self())] ++ gate_extra,
      run_id: @run_id,
      supervisor_instance: "sup_retention",
      clock: StepClock,
      fs: {SystemFs, nil},
      run_dir: ctx.run_dir
    ] ++ extra
  end

  defp identity_barrier(parent) do
    fn
      :after_ready, identity ->
        send(parent, {:identity, identity})
        true

      _name, _info ->
        true
    end
  end

  defp worker(seams, bootstrap \\ nil) do
    spec = if bootstrap, do: {Worker, {self(), bootstrap}}, else: {Worker, self()}
    pid = start_supervised!(Supervisor.child_spec(spec, restart: :temporary))
    cap = make_ref()
    send(pid, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    {pid, cap}
  end

  defp prepare_effect(run_dir, argv, deadline, gate) do
    %Effect.PrepareGate{
      gate_run_id: gate,
      attempt: 1,
      requested: %{"command_argv" => argv},
      deadline_unix: deadline,
      repo_root: run_dir,
      run_dir: run_dir
    }
  end

  defp execute!(pid, cap, effect, receipt \\ nil) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, effect, receipt})

    receive do
      {:effect_result, ^cap, 1, ^ref, ^pid, observation} -> {:ok, observation}
      {:effect_failed, ^cap, 1, ^ref, ^pid, closed} -> {:failed, closed}
    after
      20_000 -> flunk("no result for #{inspect(effect.__struct__)}")
    end
  end

  # prepare + durable start + release through the Worker; returns the identity and the exact owned Port
  defp released!(pid, cap, run_dir, argv, deadline, gate \\ @gate) do
    before = owned_ports(pid)
    prepare = prepare_effect(run_dir, argv, deadline, gate)
    {:ok, %Observation.GatePrepared{started: started}} = execute!(pid, cap, prepare)
    assert_receive {:identity, identity}, 10_000
    track(identity)
    persisted = persist!(run_dir, started)
    release = %Effect.ReleaseGate{gate_run_id: gate, attempt: 1, started_seq: 1}
    assert {:ok, %Observation.GateReleased{}} = execute!(pid, cap, release, persisted)
    [port] = owned_ports(pid) -- before
    {identity, port}
  end

  # permit-gated command: exits only once the test writes the permit (RT-M8 forced order)
  defp gated(run_dir), do: ["/bin/sh", "-c", "while [ ! -f '#{Path.join(run_dir, "go")}' ]; do sleep 0.02; done; exit 0"]
  defp permit!(run_dir), do: File.write!(Path.join(run_dir, "go"), "")

  defp owned_ports(pid) do
    {:links, links} = Process.info(pid, :links)
    Enum.filter(links, &is_port/1)
  end

  defp dictionary(pid) do
    {:dictionary, dict} = Process.info(pid, :dictionary)
    dict
  end

  defp staged_in(pid, port), do: List.keyfind(dictionary(pid), {Execution, port, :staged}, 0)
  # ordered loop barrier: a bounded call is served only after the pending handle_info completed
  defp loop!(pid), do: :sys.get_state(pid, @loop)
  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)

  # forced order: trace armed BEFORE the permit; the guardian's EXIT is DEQUEUED by the idle Worker, then the loop
  # barrier proves handle_info completed before any dictionary assertion
  defp idle_dequeued!(pid, port, run_dir, identity) do
    :erlang.trace(pid, true, [:receive])
    permit!(run_dir)
    assert wait_until(fn -> dead?(identity) end, 10_000)
    assert_receive {:trace, ^pid, :receive, {^port, {:data, {:eol, "EXIT " <> _}}}}, 5_000
    :erlang.trace(pid, false, [:receive])
    loop!(pid)
  end

  describe "RT Worker (RED unless titled control)" do
    test "RT-1 idle Worker stages the dequeued real EXIT; the later await answers the exit (inverts GB-8b)", ctx do
      {pid, cap} = worker(native_seams(ctx))
      {identity, port} = released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @now + 1)
      idle_dequeued!(pid, port, ctx.run_dir, identity)
      assert {{Execution, ^port, :staged}, [{"EXIT", _} | _]} = staged_in(pid, port)
      StepClock.set_unix(@now + 50)
      await = %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @now + 1}
      assert {:ok, %Observation.GateFinished{}} = execute!(pid, cap, await)
    end

    test "RT-11 control: a delegating executor WITHOUT stage/2 keeps the catch-all drop (GB-8b behaviour)", ctx do
      {:module, _} = Code.ensure_loaded(NoStageExecutor)
      refute function_exported?(NoStageExecutor, :stage, 2)
      {pid, cap} = worker(native_seams(ctx, NoStageExecutor))
      {identity, port} = released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @now + 1)
      idle_dequeued!(pid, port, ctx.run_dir, identity)
      assert nil == staged_in(pid, port)
      assert wait_until(fn -> owned_ports(pid) == [] end, 5_000)
      StepClock.set_unix(@now + 50)
      await = %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @now + 1}
      assert {:ok, %Observation.GateError{reason: %{"clause" => "guardian_gone"}}} = execute!(pid, cap, await)
    end

    test "RT-10 fenced Observe held: an owned gate record stays in the mailbox, is staged at loop return", ctx do
      seams =
        native_seams(ctx, Execution, [], dispatch: ScenarioHarness.OkDispatch, observe_fence_hold: %{before_go: self()})

      {pid, cap} = worker(seams)
      {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      ref = make_ref()
      observe = %Effect.Observe{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}, deadline_unix: @far}
      send(pid, {:execute, cap, 1, ref, observe, nil})
      assert_receive {:observe_fence_held, ^pid, token, %{stage: :before_go}}, 5_000
      record = line(port, @bogus)
      send(pid, record)
      assert wait_until(fn -> record in mailbox(pid) end, 2_000), "retained in the mailbox during the nested receive"
      assert nil == staged_in(pid, port)
      send(pid, {:observe_fence_proceed, token})
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.ArtifactObserved{}}, 5_000
      loop!(pid)
      refute record in mailbox(pid)
      assert {_, [{:malformed, "unknown_head"}]} = staged_in(pid, port)
    end

    test "RT-13 two retained handles: the target Port is staged in EACH enumeration position", ctx do
      {pid, cap} = worker(native_seams(ctx))
      {_, pa} = released!(pid, cap, sub!(ctx.run_dir, "a"), ["/bin/sleep", "30"], @far, "gr_a")
      {_, pb} = released!(pid, cap, sub!(ctx.run_dir, "b"), ["/bin/sleep", "30"], @far, "gr_b")

      for target <- [pa, pb] do
        send(pid, line(target, @bogus))
        loop!(pid)
        assert {_, [{:malformed, "unknown_head"}]} = staged_in(pid, target)
      end

      assert nil == staged_in(pid, make_ref())
    end

    test "RT-14 control: foreign Port with two handles: nothing stored, no key, loop serves", ctx do
      {pid, cap} = worker(native_seams(ctx))
      {_, pa} = released!(pid, cap, sub!(ctx.run_dir, "a"), ["/bin/sleep", "30"], @far, "gr_a")
      {_, pb} = released!(pid, cap, sub!(ctx.run_dir, "b"), ["/bin/sleep", "30"], @far, "gr_b")
      foreign = Port.open({:spawn, "/bin/sleep 5"}, [:binary])
      send(pid, line(foreign, @exit_ok))
      loop!(pid)
      assert nil == staged_in(pid, foreign) and nil == staged_in(pid, pa) and nil == staged_in(pid, pb)
      Port.close(foreign)
    end

    test "RT-15a control: PRE-admission (runtime nil): a Port message is dropped, loop serves", ctx do
      pid = start_supervised!(Supervisor.child_spec({Worker, self()}, restart: :temporary))
      assert %{runtime: nil} = loop!(pid)
      stray = Port.open({:spawn, "/bin/sleep 5"}, [:binary])
      send(pid, line(stray, @exit_ok))
      assert %{runtime: nil} = loop!(pid)
      assert nil == staged_in(pid, stray)
      _ = ctx
      Port.close(stray)
    end

    test "RT-15b control: admitted EMPTY runtime: a Port message is dropped, loop serves", ctx do
      {pid, _cap} = worker(native_seams(ctx))
      stray = Port.open({:spawn, "/bin/sleep 5"}, [:binary])
      send(pid, line(stray, @exit_ok))
      send(pid, :unrelated)
      loop!(pid)
      assert nil == staged_in(pid, stray)
      Port.close(stray)
    end

    test "RT-C1 control: a non-Port message still reaches the catch-all; the loop keeps serving", ctx do
      {pid, _cap} = worker(native_seams(ctx))
      send(pid, {:not_a_port, :whatever})
      send(pid, {make_ref(), {:data, {:eol, @exit_ok}}})
      assert is_map(loop!(pid))
    end
  end

  describe "RT callback boundary (RED: :stage_failed + :retention_observer)" do
    # observer identity is the payload-free Runtime key {gate_run_id, attempt}; the RECIPIENT is captured by the
    # test OUTSIDE the bootstrap closure, because the bootstrap executes inside Worker.init (RT-M12)
    defp observing(parent), do: fn {:retention, key, disposition} -> send(parent, {:retention, key, disposition}) end

    test "RT-B control: the bootstrap closure executes in the Worker pid, not in the test", ctx do
      parent = self()

      {pid, _cap} =
        worker(native_seams(ctx), fn ->
          send(parent, {:bootstrap_ran_in, self()})
          %{}
        end)

      assert_receive {:bootstrap_ran_in, ran_in}, 2_000
      assert ran_in == pid and ran_in != parent
    end

    for mode <- [:raise, :throw, :exit, :invalid] do
      test "RT-27 pre-write #{mode}: :stage_failed to the observer, nothing stored, loop serves", ctx do
        parent = self()
        mode = unquote(mode)
        seams = native_seams(ctx, ModeExecutor, stage_mode: mode, stage_witness: parent)
        {pid, cap} = worker(seams, fn -> %{retention_observer: observing(parent)} end)
        {_, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
        send(pid, line(port, @bogus))
        assert_receive {:stage_mode_selected, ^mode, ^pid}, 2_000
        assert_receive {:retention, {@gate, 1}, :stage_failed}, 2_000
        loop!(pid)
        assert nil == staged_in(pid, port)
      end
    end

    test "RT-27b write-then-raise: :stage_failed AND the custom slot remains (side effects unknown, no rollback)", ctx do
      parent = self()
      seams = native_seams(ctx, ModeExecutor, stage_mode: :write_then_raise, stage_witness: parent)
      {pid, cap} = worker(seams, fn -> %{retention_observer: observing(parent)} end)
      {_, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      send(pid, line(port, @bogus))
      assert_receive {:stage_mode_selected, :write_then_raise, ^pid}, 2_000
      assert_receive {:retention, {@gate, 1}, :stage_failed}, 2_000
      loop!(pid)
      assert {_, [{:custom, :written}]} = staged_in(pid, port)
    end

    test "RT-28 observer raises AFTER a real :staged verdict: slot kept, verdict unchanged, loop serves", ctx do
      parent = self()

      observer = fn {:retention, key, disposition} ->
        send(parent, {:retention, key, disposition})
        raise("observer escaped")
      end

      {pid, cap} = worker(native_seams(ctx), fn -> %{retention_observer: observer} end)
      {_, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      send(pid, line(port, @bogus))
      assert_receive {:retention, {@gate, 1}, :staged}, 2_000
      assert is_map(loop!(pid))
      assert {_, [{:malformed, "unknown_head"}]} = staged_in(pid, port)
    end

    test "RT-28b observer throws AFTER a :stage_failed verdict: loop serves, nothing stored", ctx do
      parent = self()

      observer = fn {:retention, key, disposition} ->
        send(parent, {:retention, key, disposition})
        throw(:observer_thrown)
      end

      {pid, cap} =
        worker(native_seams(ctx, ModeExecutor, stage_mode: :raise, stage_witness: parent), fn ->
          %{retention_observer: observer}
        end)

      {_, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      send(pid, line(port, @bogus))
      assert_receive {:retention, {@gate, 1}, :stage_failed}, 2_000
      assert is_map(loop!(pid))
      assert nil == staged_in(pid, port)
    end
  end
end
