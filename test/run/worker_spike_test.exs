defmodule AiOrchestrator.Run.WorkerSpikeTest do
  @moduledoc """
  TEST-ONLY mechanism spike for the effect-runtime owner (ruling m_1788677511000; design rev 2 in
  m_1788677371000; contract note docs/contracts/worker-ownership-spike.org). The test process plays the driving
  Server: it mints the run capability, births the owner under a REAL `Run.Work.Supervisor`, records the identity
  BEFORE any message, admits it, and drives real `Effects.execute` through a controllable executor double.
  Nothing here touches product code.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Run.Work
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.WorkerSpike.ControlledExecutor
  alias AiOrchestrator.Test.WorkerSpike.Owner
  alias AiOrchestrator.Test.WorkerSpike.Registry
  alias AiOrchestrator.Test.WorkerSpike.Seams
  alias AiOrchestrator.Test.WorkerSpike.Teardown

  @secret "UNIQUE-WORKER-SPIKE-SENTINEL-9c1e"

  # MEASURED under the full suite (disclosed): a GenServer child terminates when its parent supervisor is killed even
  # with trap_exit, because gen_server handles the parent's EXIT itself; the review probes' GenServer TrapWorker only
  # "survived" when the assertion raced ahead of that termination. A raw linked process that traps and ignores the
  # EXIT survives deterministically - that is the unknown survivor the closure proof must refuse.
  defmodule TrapWorker do
    @moduledoc false
    def start_link(_) do
      pid =
        spawn_link(fn ->
          Process.flag(:trap_exit, true)
          ignore_forever()
        end)

      {:ok, pid}
    end

    defp ignore_forever do
      receive do
        _ -> ignore_forever()
      end
    end
  end

  @far 4_102_444_800

  setup do
    Process.flag(:trap_exit, true)
    {:ok, work} = DynamicSupervisor.start_link(Work.Supervisor, [])
    {:ok, holder} = Seams.start()
    dir = Path.join(System.tmp_dir!(), "worker-spike-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      # bounded cleanup of whatever the test left: never a hang
      if Process.alive?(work), do: Teardown.reap(work, [], total_budget: 5_000)
      if Process.alive?(holder), do: Agent.stop(holder)
      File.rm_rf!(dir)
    end)

    {:ok, work: work, holder: holder, dir: dir}
  end

  defp seams(_dir, abandon \\ :ok) do
    [
      gate_executor: ControlledExecutor,
      gate_helper: System.find_executable("true"),
      gate_opts: [spike_control: self(), spike_secret: @secret, spike_abandon: abandon],
      run_id: "run_spike",
      supervisor_instance: "sup_spike",
      clock: FixedClock,
      fs: SystemFs.new()
    ]
  end

  # birth: the seams go to the holder under the cap; the child spec carries pid/cap/holder only; the identity is
  # recorded (monitored) BEFORE any message reaches the owner
  defp born!(work, holder, dir, opts \\ []) do
    cap = make_ref()
    Seams.put(holder, cap, seams(dir, Keyword.get(opts, :abandon, :ok)))
    spec = {Owner, %{server: self(), cap: cap, seams: holder}}

    {:ok, pid} =
      case Keyword.get(opts, :registry) do
        nil -> DynamicSupervisor.start_child(work, spec)
        registry -> Registry.birth(registry, work, spec)
      end

    mon = Process.monitor(pid)
    {pid, cap, mon}
  end

  defp registry! do
    {:ok, registry} = Registry.start()
    on_exit(fn -> if Process.alive?(registry), do: Agent.stop(registry) end)
    registry
  end

  defp admit!(pid, cap, gen) do
    send(pid, {:admit, cap, gen})
    assert_receive {:admitted, ^cap, ^gen}, 2_000
  end

  defp prepare(dir, attempt \\ 1) do
    %Effect.PrepareGate{
      gate_run_id: "gr_0001",
      attempt: attempt,
      requested: %{"command_argv" => ["true"]},
      deadline_unix: @far,
      repo_root: dir,
      run_dir: dir
    }
  end

  # the Server-side result gate: only the outstanding (gen, ref) is accepted; anything else is refused, never applied
  defp await_result!(cap, gen, ref) do
    receive do
      {:effect_result, ^cap, ^gen, ^ref, observation} -> {:ok, observation}
      {:effect_failed, ^cap, ^gen, ^ref, closed} -> {:failed, closed}
      {:effect_result, ^cap, _other_gen, _other_ref, _} = stale -> {:refused_stale, stale}
    after
      5_000 -> flunk("no result for the outstanding op")
    end
  end

  # ---- S-1 registration/admission before any effect ----
  test "S-1 the owner refuses work before admission; after the recorded identity is admitted it executes exactly one op",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, prepare(dir), [receipt: nil]})
    assert_receive {:refused, ^cap, 1, ^ref, "not_admitted"}, 2_000
    refute_received {:prepare_entered, _, _}
    admit!(pid, cap, 1)
    send(pid, {:execute, cap, 1, ref, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, "gr_0001"}, 2_000
    send(pid, :proceed)
    assert {:ok, %Observation.GatePrepared{gate_run_id: "gr_0001", attempt: 1}} = await_result!(cap, 1, ref)
    assert Process.alive?(pid)
  end

  # ---- S-2 the driver never blocks; the owner is busy inside the effect and cannot serve anything meanwhile ----
  test "S-2 the driving process stays free while the owner is inside a blocking effect; the owner itself is not responsive",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    # the driver is free: it keeps working (here: it asks the owner a question and observes that the owner is blocked)
    send(pid, {:dropped?, cap, self()})
    refute_receive {:dropped, ^cap, _}, 200
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, ref)
    assert_receive {:dropped, ^cap, 0}, 2_000
  end

  # ---- S-3 exactly one current op; same-generation duplicates and stale generations on dequeue ----
  test "S-3 a queued duplicate of the same (gen, ref) is refused and never re-executed; an older generation is refused; a new ref executes",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    a = make_ref()
    b = make_ref()
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    # queued behind the blocked op: a duplicate of A, a stale-generation op, then a genuinely new op B
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    send(pid, {:execute, cap, 0, make_ref(), prepare(dir), [receipt: nil]})
    send(pid, {:execute, cap, 1, b, prepare(dir, 2), [receipt: nil]})
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, a)
    assert_receive {:refused, ^cap, 1, ^a, "effect_duplicate"}, 2_000
    assert_receive {:refused, ^cap, 0, _, "effect_generation_stale"}, 2_000
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed)
    assert {:ok, %Observation.GatePrepared{attempt: 2}} = await_result!(cap, 1, b)
    refute_received {:prepare_entered, _, _}, "exactly two executions for two distinct refs"
    # the Server-side gate: a result that is not the outstanding op is refused, never applied
    send(self(), {:effect_result, cap, 1, make_ref(), :forged})
    assert {:refused_stale, _} = await_result!(cap, 1, make_ref())
  end

  # ---- S-4 the capability correlates; a PID in a message is a claim, not identity ----
  test "S-4 messages without the run capability are dropped and counted, never executed", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    send(pid, {:execute, make_ref(), 1, make_ref(), prepare(dir), [receipt: nil]})
    send(pid, {:admit, make_ref(), 7})
    refute_receive {:prepare_entered, _, _}, 200
    refute_received {:admitted, _, 7}
    send(pid, {:dropped?, cap, self()})
    assert_receive {:dropped, ^cap, 2}, 2_000
  end

  # ---- S-5 trappable failure: same-invocation settlement of the LATEST runtime, closed reply, owner alive ----
  test "S-5 a trappable failure inside an effect settles the latest runtime in the same invocation and answers closed without the sentinel",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    # a retained handle first, so the settle has something to report
    a = make_ref()
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, a)
    b = make_ref()
    send(pid, {:execute, cap, 1, b, prepare(dir, 2), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :raise)
    assert {:failed, closed} = await_result!(cap, 1, b)
    assert %{clause: "effect_failed", kind: :error, cleanup: %{attempts: 1, settled: 1, unproven: 0}} = closed
    assert_receive {:abandoned, "gr_0001", 1, :ok}, 1_000
    assert is_binary(closed.class) and String.starts_with?(closed.digest, "sha256:")
    refute inspect(closed, limit: :infinity) =~ @secret
    assert Process.alive?(pid)
    send(pid, {:settle, cap, 1})
    assert_receive {:settled, ^cap, 1, %{attempts: 0}}, 2_000
  end

  # ---- WM-4: cleanup truth - attempts vs proven vs unproven; the transfer boundary ----
  for {mode, expect} <- [
        {:error, %{attempts: 1, settled: 0, unproven: 1}},
        {:raise, %{attempts: 1, settled: 0, unproven: 1}},
        {:unsettled, %{attempts: 1, settled: 0, unproven: 1}}
      ] do
    test "W-4 an abandon that answers #{mode} is reported unproven, never settled", %{
      work: work,
      holder: holder,
      dir: dir
    } do
      {pid, cap, _mon} = born!(work, holder, dir, abandon: unquote(mode))
      admit!(pid, cap, 1)
      a = make_ref()
      send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
      assert_receive {:prepare_entered, ^pid, _}, 2_000
      send(pid, :proceed)
      assert {:ok, _} = await_result!(cap, 1, a)
      send(pid, {:settle, cap, 1})
      assert_receive {:settled, ^cap, 1, summary}, 2_000
      assert summary == unquote(Macro.escape(expect))
      assert_receive {:abandoned, "gr_0001", 1, unquote(mode)}, 1_000
      assert Process.alive?(pid)
    end
  end

  test "W-4t transfer boundary: a handle transferred into the runtime and then failing at started_data is settled by the same invocation",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    a = make_ref()
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, a)
    b = make_ref()
    send(pid, {:execute, cap, 1, b, prepare(dir, 2), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed_then_fail_started)
    assert {:failed, closed} = await_result!(cap, 1, b)
    assert %{clause: "effect_failed", cleanup: %{attempts: 2, settled: 2, unproven: 0}} = closed
    # BOTH the old handle and the NEWLY transferred one were abandoned
    assert_receive {:abandoned, "gr_0001", 1, :ok}, 1_000
    assert_receive {:abandoned, "gr_0001", 2, :ok}, 1_000
    refute inspect(closed, limit: :infinity) =~ @secret
    assert Process.alive?(pid)
  end

  # ---- S-6 blocked callback + supervisor shutdown: MEASURED - the owner does not trap exits, so the supervisor's
  # :shutdown signal ends it immediately (reason :shutdown), terminate/2 never runs, nothing is settled or claimed;
  # the 1 s shutdown bound is what would contain a TRAPPING owner (killed at the bound) ----
  test "S-6 a blocked owner ends at the supervisor's shutdown signal at once (no trap), within the bound; no settlement is claimed",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    send(pid, {:execute, cap, 1, make_ref(), prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :hang)
    t0 = System.monotonic_time(:millisecond)
    assert :ok = DynamicSupervisor.terminate_child(work, pid)
    assert_receive {:DOWN, ^mon, :process, ^pid, reason}, 5_000
    assert reason in [:shutdown, :killed]
    assert System.monotonic_time(:millisecond) - t0 < 2_000, "within the shutdown bound"
    refute_received {:settled, ^cap, _, _}
    refute_received {:effect_result, ^cap, _, _, _}
  end

  # ---- S-7 hard death mid-effect: no terminate/2, no settlement claim ----
  test "S-7 a killed owner leaves the op unproven: no result, no settle, DOWN :killed", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    {pid, cap, mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    send(pid, {:execute, cap, 1, make_ref(), prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 2_000
    refute_receive {:effect_result, ^cap, _, _, _}, 200
    refute_received {:settled, ^cap, _, _}
  end

  # ---- S-8 no disclosure: start failure, status, mailbox, supervisor view, logs, DOWN ----
  test "S-8 no surface discloses the seams' sentinel: start failure, sys status, queued message, child listing, crash log, DOWN reason",
       %{work: work, holder: holder, dir: dir} do
    # start failure with no seams under the cap: a closed clause, nothing else
    missing = make_ref()

    assert {:error, {:shutdown, %{clause: "owner_seams_missing"}} = failure} =
             DynamicSupervisor.start_child(work, {Owner, %{server: self(), cap: missing, seams: holder}})

    refute inspect(failure) =~ @secret
    {pid, cap, mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    # the IDLE owner's system status is redacted (a blocked owner cannot answer system messages at all: measured)
    refute inspect(:sys.get_status(pid, 1_000), limit: :infinity) =~ @secret
    send(pid, {:execute, cap, 1, make_ref(), prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    # a queued execute while blocked: the mailbox carries effect + receipt only
    send(pid, {:execute, cap, 1, make_ref(), prepare(dir, 2), [receipt: nil]})
    {:messages, queued} = Process.info(pid, :messages)
    refute inspect(queued, limit: :infinity) =~ @secret
    refute inspect(DynamicSupervisor.which_children(work), limit: :infinity) =~ @secret

    assert catch_exit(:sys.get_status(pid, 200)) != nil, "a blocked owner does not answer system messages"

    crash =
      capture_log(fn ->
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^mon, :process, ^pid, reason}, 2_000
        refute inspect(reason) =~ @secret
        Process.sleep(50)
      end)

    refute crash =~ @secret
  end

  # ---- WM-3: closed boundary on every stage; nothing reflected ----
  test "W-3a malformed execute inputs are refused closed without reflecting them; the owner stays alive and no log carries the sentinel",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    sentinel = "WORKER-REVIEW-MESSAGE-SENTINEL-52b4"
    ref = make_ref()

    log =
      capture_log(fn ->
        send(pid, {:execute, cap, 1, ref, :probe, %{payload: sentinel}})
        assert_receive {:refused, ^cap, 1, ^ref, "execute_shape_invalid"}, 2_000
        send(pid, {:execute, cap, 1, ref, prepare(dir), [receipt: nil, opts: [leak: sentinel]]})
        assert_receive {:refused, ^cap, 1, ^ref, "execute_shape_invalid"}, 2_000
        send(pid, {:release_terminal, cap, 1, ref, sentinel})
        assert_receive {:refused, ^cap, 1, ^ref, "release_shape_invalid"}, 2_000
        refute_receive {:DOWN, ^mon, :process, ^pid, _}, 100
      end)

    refute log =~ sentinel
    assert Process.alive?(pid)
  end

  test "W-3b an init-stage failure (seams not a keyword) stops with a closed clause and no seams in the failure", %{
    work: work,
    holder: holder
  } do
    cap = make_ref()
    Agent.update(holder, &Map.put(&1, cap, %{secret: @secret}))

    assert {:error, {:shutdown, %{clause: "owner_seams_invalid"}} = failure} =
             DynamicSupervisor.start_child(work, {Owner, %{server: self(), cap: cap, seams: holder}})

    refute inspect(failure, limit: :infinity) =~ @secret
  end

  test "W-3c a raise inside the executor after admission is trapped and answered closed; the crash log surface stays empty of the sentinel",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir, abandon: :raise)
    admit!(pid, cap, 1)
    a = make_ref()
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, a)

    log =
      capture_log(fn ->
        # a raise in the executor's abandon while settling: reported unproven, no crash, no sentinel
        send(pid, {:settle, cap, 1})
        assert_receive {:settled, ^cap, 1, %{attempts: 1, unproven: 1}}, 2_000
        b = make_ref()
        send(pid, {:execute, cap, 1, b, prepare(dir, 2), [receipt: nil]})
        assert_receive {:prepare_entered, ^pid, _}, 2_000
        send(pid, :raise)
        assert {:failed, _} = await_result!(cap, 1, b)
      end)

    refute log =~ @secret
    assert Process.alive?(pid)
  end

  # ---- Server-side failure while the owner is responsive vs blocked (constraint 4) ----
  test "D-1 a Server-side commit/observer/reducer failure after a result: the driver settles through the responsive owner and gets the truthful summary",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, _mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    a = make_ref()
    send(pid, {:execute, cap, 1, a, prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :proceed)
    assert {:ok, _} = await_result!(cap, 1, a)
    # the driver's own stage fails (a refused commit, an observer raise, a reducer error): it settles, then closes
    send(pid, {:settle, cap, 1})
    assert_receive {:settled, ^cap, 1, %{attempts: 1, settled: 1, unproven: 0}}, 2_000
    assert_receive {:abandoned, "gr_0001", 1, :ok}, 1_000
  end

  test "D-2 a Server-side failure while the owner is BLOCKED: the driver does not hang on settle; a bounded wait then a bounded stop; the outcome stays unproven",
       %{work: work, holder: holder, dir: dir} do
    {pid, cap, mon} = born!(work, holder, dir)
    admit!(pid, cap, 1)
    send(pid, {:execute, cap, 1, make_ref(), prepare(dir), [receipt: nil]})
    assert_receive {:prepare_entered, ^pid, _}, 2_000
    send(pid, :hang)
    t0 = System.monotonic_time(:millisecond)
    send(pid, {:settle, cap, 1})
    refute_receive {:settled, ^cap, 1, _}, 300
    assert :ok = DynamicSupervisor.terminate_child(work, pid)
    assert_receive {:DOWN, ^mon, :process, ^pid, reason}, 2_000
    assert reason in [:shutdown, :killed]
    assert System.monotonic_time(:millisecond) - t0 < 2_500, "a finite bound is containment, not deadline delivery"
    refute_received {:settled, ^cap, 1, _}
    refute_received {:abandoned, _, _, _}, "no cleanup is claimed for a process that never ran it"
  end

  # ---- WM residuals (m_1788679223000): freeze inside the budget; a dead supervisor never enumerates; birth at the
  # boundary ----
  test "P-4 (imported) a dead supervisor cannot enumerate a surviving unknown child", %{work: work} do
    {:ok, child} =
      DynamicSupervisor.start_child(work, %{
        id: :trap,
        start: {__MODULE__.TrapWorker, :start_link, [nil]},
        restart: :temporary
      })

    on_exit(fn -> if Process.alive?(child), do: Process.exit(child, :kill) end)
    mon = Process.monitor(work)
    Process.exit(work, :kill)
    assert_receive {:DOWN, ^mon, :process, ^work, :killed}
    assert Process.alive?(child), "control: trapping child survives parent death"

    assert {:error, %{clause: "teardown_incomplete", discovery: :absent, cause: "closure_unproven"}} =
             Teardown.reap(work, [])

    Process.exit(child, :kill)
  end

  test "P-5 (imported) registry freeze is inside the same total cleanup budget", %{work: work} do
    registry = registry!()
    caller = self()

    resumer =
      spawn(fn ->
        :erlang.suspend_process(registry)
        send(caller, :registry_suspended)
        Process.sleep(300)
        if Process.alive?(registry), do: :erlang.resume_process(registry)
      end)

    on_exit(fn -> if Process.alive?(resumer), do: Process.exit(resumer, :kill) end)
    assert_receive :registry_suspended
    start = System.monotonic_time(:millisecond)
    result = Teardown.reap(work, registry, total_budget: 20, op_timeout: 20)
    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed < 150, "freeze exceeded the 20 ms total budget: #{elapsed} ms; #{inspect(result)}"
    assert {:error, %{clause: "teardown_incomplete", cause: "registry_unavailable", ops: %{freeze_ms: f}}} = result
    assert f <= 40
  end

  # the clamp forbids any single operation from outrunning the deadline, so the crossing cannot be staged by timing;
  # the boundary itself is pinned on the pure decision the reap consults before every success
  test "W-1x completion crossing the boundary: all joined and closed but the deadline passed is budget_exhausted, never a late success" do
    assert Teardown.decision(true, true, 0) == :budget_exhausted
    assert Teardown.decision(true, true, -1) == :budget_exhausted
    assert Teardown.decision(true, true, 1) == :success
    assert Teardown.decision(true, false, 1) == :closure_unproven
    assert Teardown.decision(false, true, 1) == :continue
    assert Teardown.decision(false, true, 0) == :budget_exhausted
  end

  test "W-2x a birth in flight at the freeze boundary is an INCOMPLETE set: closure unproven now, proven once the intent resolves",
       %{work: work, holder: holder, dir: dir} do
    registry = registry!()
    {p1, c1, _} = born!(work, holder, dir, registry: registry)
    admit!(p1, c1, 1)
    cap = make_ref()
    Seams.put(holder, cap, seams(dir))
    :erlang.suspend_process(work)
    parent = self()
    # the birth records its INTENT, then blocks inside the suspended supervisor's start_child
    birther =
      spawn(fn ->
        send(parent, {:birth, Registry.birth(registry, work, {Owner, %{server: parent, cap: cap, seams: holder}})})
      end)

    on_exit(fn -> if Process.alive?(birther), do: Process.exit(birther, :kill) end)
    Process.sleep(50)

    assert {:error, %{clause: "teardown_incomplete", cause: "closure_unproven", survivors: 0}} =
             Teardown.reap(work, registry, op_timeout: 200)

    # the reap killed the suspended supervisor: the blocked start exits, the intent resolves without a pid
    assert_receive {:birth, {:error, _}}, 3_000
    refute Process.alive?(work)
    # the earlier registered owner is admitted and observed DOWN (joined 1); the in-flight birth resolved without a pid
    assert {:ok, %{joined: 1} = second} = Teardown.reap(work, registry, op_timeout: 200)
    assert second.closure == "complete_registry", inspect(second)
  end

  # ---- WM-5 (m_1788680880000): a pre-stop enumeration plus a forced kill is not closure ----
  defmodule LateChild do
    @moduledoc false
    def start_link(parent) do
      pid =
        spawn_link(fn ->
          Process.flag(:trap_exit, true)
          send(parent, {:late_child, self()})

          receive do
            :finish -> :ok
          end
        end)

      send(parent, {:start_blocked, self(), pid})

      receive do
        :return_start -> {:ok, pid}
      end
    end
  end

  defp queued!(pid, count, attempts \\ 200)
  defp queued!(_pid, _count, 0), do: flunk("messages were not queued")

  defp queued!(pid, count, attempts) do
    {:messages, messages} = Process.info(pid, :messages)

    if length(messages) < count do
      Process.sleep(5)
      queued!(pid, count, attempts - 1)
    end
  end

  # imported verbatim in substance from /tmp/worker_enumeration_boundary_review_test.exs (sha 36979378...)
  test "W-5 (imported) enumeration before an in-flight birth is not closure after forced supervisor death" do
    {:ok, work} = DynamicSupervisor.start_link(strategy: :one_for_one)
    Process.unlink(work)
    parent = self()
    :erlang.suspend_process(work)
    reaper = spawn(fn -> send(parent, {:reap_result, Teardown.reap(work, [], op_timeout: 100, total_budget: 2_000)}) end)
    queued!(work, 1)

    birther =
      spawn(fn ->
        try do
          late = %{id: :late, start: {LateChild, :start_link, [parent]}, restart: :temporary}
          DynamicSupervisor.start_child(work, late)
        catch
          :exit, _ -> :ok
        end
      end)

    queued!(work, 2)
    :erlang.resume_process(work)
    assert_receive {:late_child, child}, 1_000

    on_exit(fn ->
      for pid <- [child, work, reaper, birther], Process.alive?(pid) do
        mon = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^mon, :process, ^pid, _} -> :ok
        after
          1_000 -> raise "probe teardown incomplete"
        end
      end
    end)

    assert_receive {:start_blocked, ^work, ^child}, 1_000
    assert_receive {:reap_result, result}, 2_000
    assert Process.alive?(child), "control: raw child survives forced parent death"
    assert {:error, %{clause: "teardown_incomplete", stop: :forced, cause: "closure_unproven"}} = result
  end

  test "W-5c positive control: an ACKNOWLEDGED orderly stop of a live supervisor with a normal child is enumerated closure",
       %{work: work, holder: holder, dir: dir} do
    {p1, c1, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)
    assert {:ok, %{closure: "enumerated", joined: 1}} = Teardown.reap(work, [p1])
    refute Process.alive?(work)
    refute Process.alive?(p1)
  end

  # ---- S-9 bounded teardown: registered identities only, births, suspended supervisor, bystander, exhaustion ----
  test "S-9a registered and merely-discovered children are all reaped; every admitted pid observed DOWN", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    {p1, c1, _} = born!(work, holder, dir)
    {p2, _c2, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)
    # p2 is a child the driver never registered: the supervisor's own child listing admits it
    assert {:ok, %{joined: 2, rounds: 1, closure: "enumerated"}} = Teardown.reap(work, [p1])
    refute Process.alive?(p1)
    refute Process.alive?(p2)
    refute Process.alive?(work)
  end

  test "S-9b suspended work supervisor + bare pid list: registered identities are reaped but closure is NOT proven -> incomplete",
       %{work: work, holder: holder, dir: dir} do
    {p1, c1, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)
    :erlang.suspend_process(work)

    assert {:error, %{clause: "teardown_incomplete", cause: "closure_unproven", discovery: :unknown, survivors: 0}} =
             Teardown.reap(work, [p1], op_timeout: 300)

    refute Process.alive?(p1)
  end

  test "S-9b' suspended work supervisor + COMPLETE frozen registry: every registered identity joined proves closure", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    registry = registry!()
    {p1, c1, _} = born!(work, holder, dir, registry: registry)
    {p2, _c2, _} = born!(work, holder, dir, registry: registry)
    admit!(p1, c1, 1)
    :erlang.suspend_process(work)
    assert {:ok, %{joined: 2, closure: "complete_registry"}} = Teardown.reap(work, registry, op_timeout: 300)
    refute Process.alive?(p1)
    refute Process.alive?(p2)
    refute Process.alive?(work)

    assert {:error, :admission_frozen} =
             Registry.birth(registry, work, {Owner, %{server: self(), cap: make_ref(), seams: holder}})
  end

  # imported review probe (WM-2), verbatim in substance: an UNREGISTERED trapping child under a suspended supervisor
  test "P-2 (imported) unknown discovery plus supervisor death is not proof all children were reaped", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    registry = registry!()
    {p1, c1, _} = born!(work, holder, dir, registry: registry)
    admit!(p1, c1, 1)

    {:ok, child} =
      DynamicSupervisor.start_child(work, %{
        id: :trap,
        start: {__MODULE__.TrapWorker, :start_link, [nil]},
        restart: :temporary
      })

    on_exit(fn -> if Process.alive?(child), do: Process.exit(child, :kill) end)
    :erlang.suspend_process(work)
    # the registry is frozen but INCOMPLETE relative to the subtree: the trapping child was born outside it
    result = Teardown.reap(work, [p1], op_timeout: 20)
    assert Process.alive?(child), "control: the unregistered trapping child survives the supervisor kill"
    assert {:error, %{clause: "teardown_incomplete"}} = result
    Process.exit(child, :kill)
  end

  test "P-1 (imported) the total budget bounds the operations, not only entry to a round", %{work: work} do
    :erlang.suspend_process(work)
    start = System.monotonic_time(:millisecond)
    result = Teardown.reap(work, [], total_budget: 20, op_timeout: 200)
    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed < 120, "20 ms total budget overran to #{elapsed} ms with result #{inspect(result)}"
    assert {:error, %{clause: "teardown_incomplete", ops: %{discover_ms: _, stop_ms: _, join_ms: _}}} = result
  end

  test "P-1s the budget scales over MANY joins: ten blocked owners, a 300 ms budget, bounded end-to-end", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    owners = for _ <- 1..10, do: born!(work, holder, dir)
    for {pid, cap, _} <- owners, do: admit!(pid, cap, 1)
    :erlang.suspend_process(work)
    for {pid, _, _} <- owners, do: :erlang.suspend_process(pid)
    start = System.monotonic_time(:millisecond)
    result = Teardown.reap(work, Enum.map(owners, &elem(&1, 0)), total_budget: 300, op_timeout: 100, join_timeout: 100)
    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed < 700, "budget 300 ms overran to #{elapsed} ms"
    # suspended owners make the orderly stop time out: a forced kill after a snapshot is never enumerated closure
    assert {:error, %{clause: "teardown_incomplete", stop: :forced}} = result
    for {pid, _, _} <- owners, Process.alive?(pid), do: :erlang.resume_process(pid)
  end

  test "S-9c a bystander linked to the supervisor is never killed by the reap (links are not ownership)", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    {p1, c1, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)
    parent = self()

    bystander =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        Process.link(work)
        send(parent, :linked)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :linked, 1_000
    assert {:ok, _} = Teardown.reap(work, [p1])
    assert Process.alive?(bystander), "an unrelated linked process is not an owned identity"
    send(bystander, :stop)
  end

  test "S-9d a birth racing the teardown is admitted by re-discovery; nothing survives", %{
    work: work,
    holder: holder,
    dir: dir
  } do
    {p1, c1, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)
    cap = make_ref()
    Seams.put(holder, cap, seams(dir))
    parent = self()

    breeder =
      spawn(fn ->
        results =
          for _ <- 1..20 do
            try do
              case DynamicSupervisor.start_child(work, {Owner, %{server: parent, cap: cap, seams: holder}}) do
                {:ok, pid} -> pid
                _ -> nil
              end
            catch
              :exit, _ -> nil
            end
          end

        send(parent, {:bred, Enum.reject(results, &is_nil/1)})
      end)

    assert {:ok, _} = Teardown.reap(work, [p1])
    assert_receive {:bred, born}, 5_000
    Process.sleep(50)
    refute Enum.any?(born, &Process.alive?/1), "every child born before the supervisor stopped is gone"
    refute Process.alive?(breeder)
  end

  test "S-9e exhausted rounds or budget is the closed incomplete, never success", %{work: work, holder: holder, dir: dir} do
    {p1, c1, _} = born!(work, holder, dir)
    admit!(p1, c1, 1)

    assert {:error, %{clause: "teardown_incomplete", survivors: 1, cause: "rounds_exhausted"}} =
             Teardown.reap(work, [p1], max_rounds: 0)

    assert Process.alive?(p1)
    assert {:error, %{clause: "teardown_incomplete"}} = Teardown.reap(work, [p1], total_budget: -1)
  end
end
