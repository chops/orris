defmodule AiOrchestrator.Run.WorkerSpikeNativeTest do
  @moduledoc """
  TEST-ONLY native crash-window proof for the effect-runtime owner (ruling m_1788677511000 constraint 3 and
  m_1788678466000 authorization): the owner runs the REAL `Gate.Execution` through `Effects.execute` with the native
  guardian, and is killed by a barrier at exact points. Boundaries are distinguished by durable prefixes and the
  gate's own barriers, never by timing: (A) before gate_started is durable, (B) after the durable receipt before GO,
  (C) after GO. One GO only on a proven successful execution; the existing cold-recovery outcome is preserved and
  proven from OS evidence; cleanup truth is never claimed from a dead process.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Work
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.WorkerSpike.Owner
  alias AiOrchestrator.Test.WorkerSpike.Registry
  alias AiOrchestrator.Test.WorkerSpike.Seams
  alias AiOrchestrator.Test.WorkerSpike.Teardown

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)
  @run_id "run_spike_native"
  @deadline 1_700_000_600
  @gate "gr_0001"

  defmodule Clock do
    @moduledoc false
    def unix_now, do: 1_700_000_000
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "worker-spike-native-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    Process.flag(:trap_exit, true)
    {:ok, work} = DynamicSupervisor.start_link(Work.Supervisor, [])
    {:ok, holder} = Seams.start()
    {:ok, registry} = Registry.start()
    run_dir = Path.join(System.tmp_dir!(), "worker-spike-native-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)

    on_exit(fn ->
      if Process.alive?(work), do: Teardown.reap(work, registry, total_budget: 5_000)
      if Process.alive?(holder), do: Agent.stop(holder)
      if Process.alive?(registry), do: Agent.stop(registry)
      File.rm_rf(run_dir)
    end)

    {:ok, work: work, holder: holder, registry: registry, run_dir: run_dir, helper: helper}
  end

  # ---- the owner with the REAL executor; barriers run INSIDE the owner (the executor runs there) ----

  defp seams(helper, run_dir, barrier) do
    [
      gate_executor: Execution,
      gate_helper: helper,
      gate_opts: [settle_ms: 200, rounds: 2, clock: Clock, barrier: barrier],
      run_id: @run_id,
      supervisor_instance: "sup_spike",
      clock: Clock,
      fs: {SystemFs, nil},
      run_dir: run_dir
    ]
  end

  defp born!(ctx, barrier, gen) do
    cap = make_ref()
    Seams.put(ctx.holder, cap, seams(ctx.helper, ctx.run_dir, barrier))
    {:ok, pid} = Registry.birth(ctx.registry, ctx.work, {Owner, %{server: self(), cap: cap, seams: ctx.holder}})
    mon = Process.monitor(pid)
    send(pid, {:admit, cap, gen})
    assert_receive {:admitted, ^cap, ^gen}, 2_000
    {pid, cap, mon}
  end

  # a barrier that reports the READY identity and the published claim to the test, then kills the owner at `at`
  defp reporting_barrier(parent, at) do
    fn
      :after_ready, identity ->
        send(parent, {:identity, identity})
        true

      :after_claim, started ->
        send(parent, {:claim, started})
        if at == :after_claim, do: Process.exit(self(), :kill)
        true

      name, _info when name == at ->
        Process.exit(self(), :kill)

      _name, _info ->
        true
    end
  end

  defp prepare_effect(run_dir, argv, attempt \\ 1) do
    %Effect.PrepareGate{
      gate_run_id: @gate,
      attempt: attempt,
      requested: %{"command_argv" => argv},
      deadline_unix: @deadline,
      repo_root: run_dir,
      run_dir: run_dir
    }
  end

  # a prepare whose READY identity is tracked (OS absence oracle registered) BEFORE the result is awaited, so a failure
  # between READY and the result still has its oracle
  defp prepare!(pid, cap, gen, effect) do
    ref = make_ref()
    send(pid, {:execute, cap, gen, ref, effect, [receipt: nil]})
    assert_receive {:identity, identity}, 10_000
    track(identity)
    {await!(cap, gen, ref, effect), identity}
  end

  defp execute!(pid, cap, gen, effect, receipt \\ nil) do
    ref = make_ref()
    send(pid, {:execute, cap, gen, ref, effect, [receipt: receipt]})
    await!(cap, gen, ref, effect)
  end

  defp await!(cap, gen, ref, effect) do
    receive do
      {:effect_result, ^cap, ^gen, ^ref, observation} -> {:ok, observation}
      {:effect_failed, ^cap, ^gen, ^ref, closed} -> {:failed, closed}
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

  defp expected(started, journaled?), do: Map.merge(started, %{"run_id" => @run_id, "journaled" => journaled?})

  defp ran_lines(run_dir),
    do:
      run_dir
      |> Path.join("ran")
      |> File.read()
      |> then(fn
        {:ok, s} -> length(String.split(s, "\n", trim: true))
        _ -> 0
      end)

  # ---- OS oracles (as in gate_execution_test): leader absence AND an empty group ----
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

  # ---- N-0 control: prepare -> durable start -> release -> await, one GO, through the owner ----
  test "N-0 control: the owner drives the real gate end to end; exactly one GO; the exit is observed", ctx do
    {pid, cap, _mon} = born!(ctx, reporting_barrier(self(), :none), 1)

    {{:ok, %Observation.GatePrepared{started: started}}, identity} =
      prepare!(pid, cap, 1, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran"]))

    persisted = persist_started!(ctx.run_dir, started)

    assert {:ok, %Observation.GateReleased{gate_run_id: @gate, attempt: 1}} =
             execute!(pid, cap, 1, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted)

    assert {:ok, %Observation.GateFinished{gate_run_id: @gate} = finished} =
             execute!(pid, cap, 1, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @deadline})

    assert is_map(finished.result)
    assert wait_until(fn -> ran_lines(ctx.run_dir) == 1 end, 5_000), "exactly one GO"
    assert wait_until(fn -> dead?(identity) end, 10_000)
    send(pid, {:settle, cap, 1})
    assert_receive {:settled, ^cap, 1, %{attempts: 1}}, 5_000
  end

  # ---- N-A: killed at :after_claim - the claim is published but gate_started is NOT durable ----
  test "N-A killed before gate_started is durable: no GO, the claim outlives the owner as orphan, a retry of the same attempt is refused (no forced GO)",
       ctx do
    {pid, cap, mon} = born!(ctx, reporting_barrier(self(), :after_claim), 1)
    ref = make_ref()

    send(
      pid,
      {:execute, cap, 1, ref, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran; sleep 30"]), [receipt: nil]}
    )

    assert_receive {:identity, identity}, 10_000
    track(identity)
    assert_receive {:claim, claim}, 10_000
    assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 10_000
    refute_received {:effect_result, ^cap, _, _, _}
    refute File.exists?(Path.join(ctx.run_dir, "events.jsonl")), "nothing durable: gate_started was never persisted"
    assert wait_until(fn -> dead?(identity) end, 10_000), "the guardian settles the group on control EOF"
    assert ran_lines(ctx.run_dir) == 0, "no GO"
    # cold recovery from the durable prefix (none) and the OS: the published claim is an orphan
    assert {:orphan_claim, _} =
             Execution.reconcile(
               {SystemFs, nil},
               ctx.run_dir,
               expected(claim, false),
               seams(ctx.helper, ctx.run_dir, nil)[:gate_opts]
             )

    # a NEW owner retrying the same attempt is refused by the existing claim: the delivered outcome is closed, not a
    # forced GO
    {pid2, cap2, _} = born!(ctx, reporting_barrier(self(), :none), 2)
    assert {:ok, observation} = execute!(pid2, cap2, 2, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran"]))
    assert %Observation.GatePrepareFailed{} = observation
    assert ran_lines(ctx.run_dir) == 0
  end

  # ---- N-B: killed at :after_ack - gate_started durable with its receipt, GO never sent ----
  test "N-B killed after the durable receipt before GO: no GO; cold recovery reconciles dead from the journaled start; no duplicate GO",
       ctx do
    {pid, cap, mon} = born!(ctx, reporting_barrier(self(), :after_ack), 1)

    {{:ok, %Observation.GatePrepared{started: started}}, identity} =
      prepare!(pid, cap, 1, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran; sleep 30"]))

    persisted = persist_started!(ctx.run_dir, started)
    ref = make_ref()

    send(
      pid,
      {:execute, cap, 1, ref, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, [receipt: persisted]}
    )

    assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 10_000
    refute_received {:effect_result, ^cap, _, _, _}
    assert wait_until(fn -> dead?(identity) end, 10_000)
    assert ran_lines(ctx.run_dir) == 0, "GO was never sent"
    # cold recovery: a NEW owner reconciles the journaled start (existing recovery path), never re-releases
    {pid2, cap2, _} = born!(ctx, reporting_barrier(self(), :none), 2)

    assert {:ok, %Observation.GateReconciled{verdict: :dead, attempt: 1}} =
             execute!(pid2, cap2, 2, %Effect.ReconcileGate{
               gate_run_id: @gate,
               attempt: 1,
               expected: expected(started, true)
             })

    assert ran_lines(ctx.run_dir) == 0, "no duplicate GO on recovery"
  end

  # ---- N-C: killed at :after_go - GO happened once; the guardian settles the released group ----
  test "N-C killed after GO: exactly one GO, the guardian settles the group, cold recovery reconciles dead from OS evidence",
       ctx do
    {pid, cap, mon} = born!(ctx, reporting_barrier(self(), :after_go), 1)

    {{:ok, %Observation.GatePrepared{started: started}}, identity} =
      prepare!(pid, cap, 1, prepare_effect(ctx.run_dir, ["/bin/sh", "-c", "echo ran >> ran; sleep 30"]))

    persisted = persist_started!(ctx.run_dir, started)
    ref = make_ref()

    send(
      pid,
      {:execute, cap, 1, ref, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, [receipt: persisted]}
    )

    assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 10_000
    # MEASURED: the kill right after GO closes the Port, so the guardian settles the group on control EOF and RACES
    # the worker's first instruction - the marker may or may not have been written. GO was written exactly once
    # (the :after_go barrier fired once, in the process that owned the Port); the marker count is at most one and
    # must not increase on recovery, which is what proves "no duplicate GO".
    assert wait_until(fn -> dead?(identity) end, 10_000), "the guardian settles the released group on control EOF"
    after_kill = ran_lines(ctx.run_dir)
    assert after_kill <= 1
    {pid2, cap2, _} = born!(ctx, reporting_barrier(self(), :none), 2)

    assert {:ok, %Observation.GateReconciled{verdict: :dead}} =
             execute!(pid2, cap2, 2, %Effect.ReconcileGate{
               gate_run_id: @gate,
               attempt: 1,
               expected: expected(started, true)
             })

    Process.sleep(200)
    assert ran_lines(ctx.run_dir) == after_kill, "recovery never re-releases: the count does not grow"
  end
end
