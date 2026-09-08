defmodule AiOrchestrator.Run.GateOwnershipBaselineTest do
  @moduledoc """
  Gate ownership MEASURED baselines (docs/contracts/gate-ownership.org, revision 1, scope m_1788801060000).

  Every row pins CURRENT behaviour of the orchestrated gate route on the REAL product `Run.Worker` with the REAL
  `Gate.Execution` and the native guardian. Rows that measure the ownership gap (GB-6) are the evidence a later unit
  changes; they are not desired assertions. OS oracles and the driver helpers are copied from
  `test/run/worker_spike_native_test.exs` (disclosed) so the witness is the same one the spike used.

  Owner-resident await (docs/contracts/gate-async-await-proposal.org, ruling m_1788890296406): with the REAL executor
  the Worker no longer blocks inside `Execution.next_record/2` and no longer enforces the deadline inside the await, so
  the six native rows GB-1/4/5/4n/8c/7 witness the PENDING operation through the acknowledged correlated
  `Effects.pending/1` / loop-state facts instead of a stack sample, and GB-4/GB-8c drive the owner's own expiry through
  an INJECTED correlated deadline wake (the real Server scheduling is the AW-S* rows). Every ownership guarantee they
  pinned before (Port connected to the owner, release/termination memos, exactly-once command and output, the normal
  exit result, retained-handle settlement, native OS death/EOF) is retained; the non-opt-in BlockingExecutor controls
  are unchanged.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.StepClock

  @moduletag :native

  @source Path.expand("../../native/gate_guardian/gate_guardian.c", __DIR__)
  @run_id "run_gate_ownership"
  @gate "gr_0001"
  @far 1_700_000_600
  @soon 1_700_000_001
  @empty_sha "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  defmodule Clock do
    @moduledoc false
    def unix_now, do: 1_700_000_000
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  # GB-6: an executor whose await blocks in the CALLER until released (the Worker is that caller today)
  defmodule BlockingExecutor do
    @moduledoc false
    def await(running, opts) do
      send(Keyword.fetch!(opts, :collector), {:await_entered, self(), running, Keyword.get(opts, :deadline_unix)})

      receive do
        {:release, answer} -> answer
      end
    end
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "gate-ownership-build-#{System.unique_integer([:positive])}")
    # ---- the REAL product Worker admitted with the REAL executor ----
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd("cc", ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-o", bin, @source], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    run_dir = Path.join(System.tmp_dir!(), "gate-ownership-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf(run_dir) end)
    {:ok, run_dir: run_dir, helper: helper}
  end

  defp native_seams(ctx) do
    [
      gate_executor: Execution,
      gate_helper: ctx.helper,
      gate_opts: [settle_ms: 200, rounds: 2, clock: Clock, barrier: identity_barrier(self())],
      run_id: @run_id,
      supervisor_instance: "sup_gate_ownership",
      clock: Clock,
      fs: {SystemFs, nil},
      run_dir: ctx.run_dir
    ]
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

  defp worker(seams) do
    pid = start_supervised!(Supervisor.child_spec({Worker, self()}, restart: :temporary))
    cap = make_ref()
    send(pid, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    {pid, cap}
  end

  defp prepare_effect(run_dir, argv, deadline) do
    %Effect.PrepareGate{
      gate_run_id: @gate,
      attempt: 1,
      requested: %{"command_argv" => argv},
      deadline_unix: deadline,
      repo_root: run_dir,
      run_dir: run_dir
    }
  end

  defp prepare!(pid, cap, effect) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, effect, nil})
    assert_receive {:identity, identity}, 10_000
    track(identity)
    {await!(pid, cap, ref, effect), identity}
  end

  defp execute!(pid, cap, effect, receipt \\ nil) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, effect, receipt})
    await!(pid, cap, ref, effect)
  end

  defp await!(pid, cap, ref, effect) do
    receive do
      {:effect_result, ^cap, 1, ^ref, ^pid, observation} -> {:ok, observation}
      {:effect_failed, ^cap, 1, ^ref, ^pid, closed} -> {:failed, closed}
    after
      20_000 -> flunk("no result for #{inspect(effect.__struct__)}")
    end
  end

  # the driver persists gate_started through a REAL Writer (the durable receipt boundary), seq 1
  defp persist_started!(run_dir, started) do
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
      "data" => started
    }

    result = Writer.append(w, event)
    :ok = Writer.close(w)
    assert {:ok, persisted} = result
    persisted
  end

  # prepare + durable start + release on the given command; returns {pid, cap, identity, started}
  defp released!(ctx, argv, deadline) do
    {pid, cap} = worker(native_seams(ctx))

    {{:ok, %Observation.GatePrepared{started: started}}, identity} =
      prepare!(pid, cap, prepare_effect(ctx.run_dir, argv, deadline))

    persisted = persist_started!(ctx.run_dir, started)

    assert {:ok, %Observation.GateReleased{gate_run_id: @gate, attempt: 1}} =
             execute!(pid, cap, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

    {pid, cap, identity, started}
  end

  # forced-order positive control (addendum m_1788803880000): the command completes only after the test has
  # witnessed the Worker's PENDING await for this op, so the terminal record can never arrive while the Worker is idle
  defp gated_argv(run_dir, then_sh),
    do: ["/bin/sh", "-c", "while [ ! -f '#{Path.join(run_dir, "go")}' ]; do sleep 0.02; done; #{then_sh}"]

  defp await_gated!(pid, cap, run_dir, deadline) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: deadline}, nil})
    pending!(pid, ref)
    File.write!(Path.join(run_dir, "go"), "")
    await!(pid, cap, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: deadline})
  end

  defp owned_ports(pid) do
    {:links, links} = Process.info(pid, :links)
    Enum.filter(links, &is_port/1)
  end

  defp dictionary(pid) do
    {:dictionary, d} = Process.info(pid, :dictionary)
    d
  end

  defp memo(pid, port, key), do: List.keyfind(dictionary(pid), {Execution, port, key}, 0)

  # entry witness (owner-resident await): the loop serves a bounded call and its state carries the correlated pending
  # op for THIS ref on the owned Port, agreeing with the opaque accessor over the latest runtime; returns the Port
  defp pending!(pid, ref) do
    state = :sys.get_state(pid, 5_000)
    assert %{pending: %{ref: ^ref, key: {@gate, 1}, port: port}} = state
    assert {:ok, %{key: {@gate, 1}, port: ^port}} = Effects.pending(state.runtime)
    assert Port.info(port, :connected) == {:connected, pid}
    port
  end

  # the owner's own expiry is driven by the correlated deadline wake the Server would send (INJECTED here; the real
  # Server scheduling and send are the AW-S* rows of run_server_foundation_red_test.exs)
  defp inject_deadline_wake!(pid, cap, ref), do: send(pid, {:gate_deadline, cap, 1, ref})

  # ---- OS oracles (as in gate_execution_test / worker_spike_native_test) ----
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

  @probe_ms 300

  describe "GB-1/GB-2 ownership" do
    test "GB-1 after PrepareGate the ONLY owner of the guardian Port is the Worker; GB-2 release memos are Worker-local; the recorded deadline is the effect's",
         ctx do
      {pid, cap, identity, started} = released!(ctx, gated_argv(ctx.run_dir, "exit 0"), @far)

      ports = owned_ports(pid)
      assert length(ports) == 1, "exactly one owned Port after prepare/release"
      [port] = ports
      assert Port.info(port, :connected) == {:connected, pid}
      assert {:os_pid, os_pid} = Port.info(port, :os_pid)
      assert os_pid == identity.worker or signal_zero(Integer.to_string(os_pid)) == :alive

      # GB-2: the release memos are process-dictionary entries of the OWNER keyed by the Port
      assert {{Execution, ^port, :released}, true} = memo(pid, port, :released)
      assert {{Execution, ^port, :released_at_ms}, at} = memo(pid, port, :released_at_ms)
      assert is_integer(at)
      assert is_nil(memo(pid, port, :termination)), "no termination memo before the await"

      # GB-3 (deadline record): started_data carries the effect's absolute deadline (the full journal pin is
      # test/lifecycle/gate_wiring_test.exs:288-293, not duplicated here)
      assert started["deadline_unix"] == @far

      # forced order: the command exits only after the Worker's pending await for this op is witnessed
      assert {:ok, %Observation.GateFinished{}} = await_gated!(pid, cap, ctx.run_dir, @far)

      assert wait_until(fn -> dead?(identity) end, 10_000)
      settle = make_ref()
      send(pid, {:settle, cap, 1, settle})
      assert_receive {:settled, ^cap, 1, ^settle, ^pid, [%{"settle" => _}]}, 5_000
    end
  end

  describe "GB-4/GB-5 owner-resident await (wake INJECTED; real scheduling is AW-S*)" do
    test "GB-4 a gate at its deadline is terminated by the owner's own settle on the correlated wake (TERM through the retained handle); memo Worker-local; a later await agrees",
         ctx do
      {pid, cap, identity, _started} = released!(ctx, ["/bin/sleep", "30"], @soon)
      [port] = owned_ports(pid)
      ref = make_ref()
      intent = %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @soon}
      send(pid, {:execute, cap, 1, ref, intent, nil})
      # the await is PENDING in the owner (no wait, no clock read); the owner answers nothing until the wake
      assert ^port = pending!(pid, ref)
      refute_received {:effect_result, ^cap, 1, ^ref, ^pid, _}
      t0 = System.monotonic_time(:millisecond)
      inject_deadline_wake!(pid, cap, ref)
      {:ok, observation} = await!(pid, cap, ref, intent)
      elapsed = System.monotonic_time(:millisecond) - t0

      # provenance: the OWNER's expiry on the wake (TERM through the retained handle, DEAD reason=command, no
      # :backstop) answers within the settle budget, well before the guardian's own +2000 ms backstop grace after the
      # deadline; a guardian-backstop termination would carry reason "deadline" / backstop: true instead
      assert elapsed >= 0 and elapsed < 2_900, "owner expiry on the wake within the window (#{elapsed} ms)"

      # MEASURED class and result: GateFailed carrying the guardian's settled timeout termination, null exit status,
      # empty-output hashes, the duration measured from the owner's release; the memo has NO :backstop key (owner TERM),
      # a guardian-backstop termination would carry backstop: true
      assert %Observation.GateFailed{gate_run_id: @gate, result: result} = observation
      assert result["termination"] == %{"kind" => "timeout", "leftovers" => "0", "proof" => "gone", "settled" => true}
      assert result["exit_status"] == nil
      assert is_integer(result["duration_ms"]) and result["duration_ms"] >= 0 and result["duration_ms"] < 2_900
      assert result["stdout_hash"] == "sha256:" <> @empty_sha
      assert {{Execution, ^port, :termination}, %{kind: "timeout"} = termination} = memo(pid, port, :termination)
      assert termination.settled == true and termination.proof == "gone"
      refute Map.has_key?(termination, :backstop), "owner TERM, not the guardian's deadline backstop"
      assert wait_until(fn -> dead?(identity) end, 10_000)

      # a later await on the same handle reports the SAME termination from the memo, never a fresh run or a second
      # pending wait (memo idempotence): the retained handle answers :done at once
      {:ok, again} = execute!(pid, cap, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @soon})
      assert again == observation
      assert %{pending: nil} = :sys.get_state(pid, 5_000)
    end

    test "GB-5 a normal exit is observed GateFinished with the parsed exit record; the group is gone; settle proves one handle",
         ctx do
      {pid, cap, identity, _} = released!(ctx, gated_argv(ctx.run_dir, "echo ran >> ran; exit 0"), @far)

      # forced order (addendum): the exit happens only after the Worker's pending await for this op is witnessed
      assert {:ok, %Observation.GateFinished{gate_run_id: @gate, result: result}} =
               await_gated!(pid, cap, ctx.run_dir, @far)

      assert is_map(result)

      assert wait_until(fn -> File.read(Path.join(ctx.run_dir, "ran")) == {:ok, "ran\n"} end, 5_000),
             "ran written exactly once"

      assert wait_until(fn -> dead?(identity) end, 10_000)
      settle = make_ref()
      send(pid, {:settle, cap, 1, settle})
      assert_receive {:settled, ^cap, 1, ^settle, ^pid, [_one]}, 5_000
    end
  end

  describe "GB-6 responsiveness gap" do
    test "the await blocks INSIDE the Worker process; a correlated settle probe is not answered until the executor returns" do
      {worker, cap} = worker(clock: FixedClock, gate_executor: BlockingExecutor, gate_opts: [collector: self()])
      ref = make_ref()
      deadline = FixedClock.base_unix() + 600

      send(
        worker,
        {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: "g1", attempt: 1, deadline_unix: deadline}, nil}
      )

      assert_receive {:await_entered, caller, running, ^deadline}, 1_000
      assert caller == worker, "the executor's await runs in the Worker itself (no task)"
      assert is_nil(running), "no PrepareGate retained a handle here"

      settle = make_ref()
      send(worker, {:settle, cap, 1, settle})
      refute_receive {:settled, ^cap, 1, ^settle, ^worker, _}, @probe_ms
      assert Process.alive?(worker)

      # release: the effect result arrives first, then the queued settle answer (mailbox order preserved)
      send(caller, {:release, {:error, %{clause: "probe_released"}}})
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.GateError{}}, 1_000
      assert_receive {:settled, ^cap, 1, ^settle, ^worker, []}, 1_000
    end

    test "control: with no effect in flight the same settle probe is answered within the probe window" do
      {worker, cap} = worker(clock: FixedClock, gate_executor: BlockingExecutor, gate_opts: [collector: self()])
      settle = make_ref()
      send(worker, {:settle, cap, 1, settle})
      assert_receive {:settled, ^cap, 1, ^settle, ^worker, _}, @probe_ms
    end

    test "GB-4n native INVERTED: the owner ANSWERS a correlated settle probe within the probe window while the REAL await is pending; the probe settles the handle, the original ref gets no result",
         ctx do
      {pid, cap, identity, _} = released!(ctx, ["/bin/sleep", "30"], @soon)
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @soon}, nil})
      pending!(pid, ref)
      settle = make_ref()
      send(pid, {:settle, cap, 1, settle})
      # the measured gap (GB-6) is closed: the probe is answered promptly with the REAL cleanup of the one handle
      assert_receive {:settled, ^cap, 1, ^settle, ^pid,
                      [%{"gate_run_id" => @gate, "attempt" => 1, "settle" => %{"settled" => true, "proof" => "gone"}}]},
                     @probe_ms

      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      assert %{pending: nil} = :sys.get_state(pid, 5_000)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end
  end

  describe "GB-8 stage baselines (G-N3)" do
    setup ctx do
      StepClock.set(1_700_000_000, 0)
      on_exit(fn -> StepClock.clear() end)

      seams =
        Keyword.put(native_seams(ctx), :gate_opts,
          settle_ms: 200,
          rounds: 2,
          clock: StepClock,
          barrier: identity_barrier(self())
        )

      {:ok, seams: Keyword.put(seams, :clock, StepClock)}
    end

    test "GB-8a pre-GO: a prepared handle whose deadline passed before ReleaseGate is refused deadline_expired and abandoned (settle carried); no GO",
         ctx do
      {pid, cap} = worker(ctx.seams)

      {{:ok, %Observation.GatePrepared{started: started}}, identity} =
        prepare!(pid, cap, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran"], 1_700_000_001))

      persisted = persist_started!(ctx.run_dir, started)
      StepClock.set_unix(1_700_000_005)

      assert {:ok, %Observation.GateReleaseFailed{reason: reason}} =
               execute!(pid, cap, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

      assert reason["clause"] == "deadline_expired"
      assert is_map(reason["settle"])
      assert wait_until(fn -> dead?(identity) end, 10_000)
      refute File.exists?(Path.join(ctx.run_dir, "ran")), "no GO was ever sent"
      assert [] == owned_ports(pid)
    end

    test "GB-8b-neg the old witness order is invalid: an immediate command's Port is already closed before a trace could arm",
         ctx do
      {pid, cap} = worker(ctx.seams)

      {{:ok, %Observation.GatePrepared{started: started}}, identity} =
        prepare!(pid, cap, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran; exit 0"], 1_700_000_001))

      persisted = persist_started!(ctx.run_dir, started)

      assert {:ok, %Observation.GateReleased{}} =
               execute!(pid, cap, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

      # forced schedule (review m_1788826770000 RG-M2): give the guardian its natural head start; the Port can be
      # gone before any later trace, so "[port] = owned_ports(pid) after release" is not a valid witness order
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert wait_until(fn -> owned_ports(pid) == [] end, 5_000)
      :sys.get_state(pid, 5_000)
      assert [] == owned_ports(pid), "nothing left to trace: the old order could only race"
    end

    test "GB-8b MEASURED (C-4 retired by retention): a terminal record arriving while the Worker is idle is STAGED; the later await answers it",
         ctx do
      {pid, cap} = worker(ctx.seams)

      {{:ok, %Observation.GatePrepared{started: started}}, identity} =
        prepare!(pid, cap, prepare_effect(ctx.run_dir, gated_argv(ctx.run_dir, "exit 0"), 1_700_000_001))

      persisted = persist_started!(ctx.run_dir, started)

      assert {:ok, %Observation.GateReleased{}} =
               execute!(pid, cap, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

      # forced order (RG-M2): the exact Port is captured and the receive trace armed BEFORE the permit lets the
      # command exit, so the guardian's EXIT can only be dequeued by the IDLE Worker after the trace is live
      [port] = owned_ports(pid)
      :erlang.trace(pid, true, [:receive])
      File.write!(Path.join(ctx.run_dir, "go"), "")
      assert wait_until(fn -> dead?(identity) end, 10_000), "the command exited and the group settled before any await"
      assert_receive {:trace, ^pid, :receive, {^port, {:data, {:eol, "EXIT " <> _}}}}, 5_000
      :erlang.trace(pid, false, [:receive])
      # bounded owner-loop barrier: the dequeue's handle_info has completed before the dictionary is read
      :sys.get_state(pid, 5_000)
      # the guardian's EXIT record reached the IDLE Worker's mailbox and is now STAGED by its Port clause
      # (docs/contracts/gate-record-retention-proposal.org; C-4 retired): the later await answers the staged
      # exit, even though the Port has closed since; queued evidence precedes the past deadline
      assert wait_until(fn -> owned_ports(pid) == [] end, 5_000), "the guardian exited; the Port closed with it"

      assert {{Execution, ^port, :staged}, [{"EXIT", _} | _]} =
               List.keyfind(dictionary(pid), {Execution, port, :staged}, 0)

      StepClock.set_unix(1_700_000_050)

      assert {:ok, %Observation.GateFinished{result: %{"exit_status" => 0}}} =
               execute!(pid, cap, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: 1_700_000_001})
    end

    test "GB-8c released handle past its deadline with NO queued terminal: the await is PENDING (no clock decision); the correlated wake expires it at once, owner TERM",
         ctx do
      {pid, cap} = worker(ctx.seams)

      {{:ok, %Observation.GatePrepared{started: started}}, identity} =
        prepare!(pid, cap, prepare_effect(ctx.run_dir, ["/bin/sleep", "30"], 1_700_000_001))

      persisted = persist_started!(ctx.run_dir, started)

      assert {:ok, %Observation.GateReleased{}} =
               execute!(pid, cap, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

      [port] = owned_ports(pid)
      StepClock.set_unix(1_700_000_050)
      ref = make_ref()

      send(
        pid,
        {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: 1_700_000_001}, nil}
      )

      # queued evidence precedes any clock decision: none is queued, so the await is pending, not expired
      assert ^port = pending!(pid, ref)
      refute_received {:effect_result, ^cap, 1, ^ref, ^pid, _}
      assert is_nil(memo(pid, port, :termination)), "no termination memo before the wake"
      t0 = System.monotonic_time(:millisecond)
      inject_deadline_wake!(pid, cap, ref)

      {:ok, observation} =
        await!(pid, cap, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: 1_700_000_001})

      assert System.monotonic_time(:millisecond) - t0 < 2_900, "no wait on the wake: expired at once"
      assert observation.__struct__ in [Observation.GateFailed, Observation.GateError]
      assert {{Execution, ^port, :termination}, %{kind: "timeout"} = termination} = memo(pid, port, :termination)
      refute Map.has_key?(termination, :backstop), "owner expiry, not the guardian backstop"
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end
  end

  describe "GB-7 owner hard-death" do
    test "the Worker's death closes the control channel; settlement is the guardian's own control-EOF path, proven only by the OS oracle",
         ctx do
      {pid, cap, identity, _} = released!(ctx, ["/bin/sleep", "30"], @far)
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @far}, nil})
      assert wait_until(fn -> signal_zero(Integer.to_string(identity.worker)) == :alive end, 2_000)
      pending!(pid, ref)

      mon = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 2_000
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, 0

      # the Worker being dead proves nothing about the group; the guardian's EOF settlement does
      assert wait_until(fn -> dead?(identity) end, 10_000), "guardian settled the group on control EOF"
    end
  end
end
