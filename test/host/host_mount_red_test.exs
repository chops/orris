# A discovery stand-in for MR-15: forwards every call to the real host supervisor after a fixed delay.
defmodule AiOrchestrator.Host.MountRedTest.DelayingProxy do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
  @impl true
  def init(state), do: {:ok, state}
  @impl true
  def handle_call(request, _from, %{target: target, delay: delay} = state) do
    Process.sleep(delay)
    {:reply, GenServer.call(target, request, 15_000), state}
  end
end

defmodule AiOrchestrator.Host.MountRedTest do
  @moduledoc """
  RED acceptance rows for host-mounted runs (docs/contracts/host-mounted-runs.org).

  Two kinds of RED, attributed per row: (1) rows guarded by `require_mount!/0` fail on the ABSENT interface
  (`Host.Supervisor`, `Host.RunOwner`, `Host.mount/ready/await/stop/mounted/census`), each missing function
  named; (2) the RP rows (MR-16) are unguarded and fail on BEHAVIOUR: today the run subtree registers with the
  global arbiter although `opts[:ownership]` names a private one (baseline MB-6 pins the fact).

  Every host row runs under an ISOLATED supervised root (a private arbiter, host supervisor and monitor with
  injected names), never the Application's; ownership AND disk-lock observations are asserted exactly, and
  a failed observation stays a failure. Budgets are seams (`budgets:`, `child_shutdown_ms`, `retention_ms`).
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host.MountRedTest.DelayingProxy
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @deadline 15_000
  @slack 300
  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @budgets %{close: 3_000, stop: 8_000, join: 1_000, handoff: 5_000, helper_join: 500}

  # ---- late-bound receivers ----
  defp host, do: Module.concat(["AiOrchestrator", "Host"])
  defp host_sup, do: Module.concat(["AiOrchestrator", "Host", "Supervisor"])
  defp run_owner, do: Module.concat(["AiOrchestrator", "Host", "RunOwner"])
  defp monitor_mod, do: Module.concat(["AiOrchestrator", "Host", "Monitor"])

  defp require_mount! do
    for {mod, fun, arity} <- [
          {host_sup(), :start_link, 1},
          {run_owner(), :child_spec, 1},
          {host(), :mount, 3},
          {host(), :ready, 2},
          {host(), :await, 2},
          {host(), :stop, 2},
          {host(), :mounted, 2},
          {host(), :census, 1},
          {run_owner(), :inspect, 1}
        ] do
      assert Code.ensure_loaded?(mod) and function_exported?(mod, fun, arity),
             "#{inspect(mod)}.#{fun}/#{arity} does not exist"
    end
  end

  # the exact identities under the TRUSTED supervisor by child id (the H-6a lesson): every-DOWN checks use this map,
  # never the subset the implementation chose to put in a payload
  defp trusted_map!(supervisor) do
    children = Supervisor.which_children(supervisor)
    {_, server, _, _} = List.keyfind(children, Run.Server, 0)
    {_, work, :supervisor, _} = List.keyfind(children, Run.Work.Supervisor, 0)
    {_, writer, _, _} = Enum.find(children, &match?({{Writer, _}, pid, _, _} when is_pid(pid), &1))
    [{_, worker, _, _}] = Supervisor.which_children(work)
    %{supervisor: supervisor, server: server, writer: writer, work: work, worker: worker}
  end

  defp assert_payload_exact!(payload, owner) do
    expected = payload.supervisor |> trusted_map!() |> Map.put(:owner, owner)
    assert Enum.sort(Map.keys(payload)) == Enum.sort(Map.keys(expected)), "payload key set differs"
    for {role, pid} <- expected, do: assert(payload[role] == pid, "#{role} identity differs")
    expected
  end

  # a trapping test-owned process running `fun`, joined afterwards (Recovery.acquire requires a trapping caller)
  defp trapping!(fun) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, {ref, fun.()})
      end)

    mon = Process.monitor(pid)

    result =
      receive do
        {^ref, r} -> r
      after
        @deadline -> flunk("trapping helper never answered")
      end

    assert_receive {:DOWN, ^mon, :process, ^pid, _}, @deadline
    result
  end

  # ---- the isolated host root: private arbiter, host supervisor, monitor (Monitor LAST) ----
  defp start_host!(opts \\ []) do
    require_mount!()
    n = System.unique_integer([:positive])
    arb = :"mount_arb_#{n}"
    hsup = :"mount_hsup_#{n}"
    mon = :"mount_mon_#{n}"
    child_shutdown = Keyword.get(opts, :child_shutdown_ms, 20_000)

    children = [
      {Ownership, name: arb},
      {host_sup(), name: hsup, child_shutdown_ms: child_shutdown},
      {monitor_mod(), name: mon, host_supervisor: hsup, ownership: arb}
    ]

    root =
      start_supervised!(%{
        id: :"mount_root_#{n}",
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    %{root: root, arb: arb, hsup: hsup, mon: mon, host: %{supervisor: hsup, monitor: mon, ownership: arb}}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "mount_red_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  # ---- command + context (built in the test process) ----
  defp command_ctx(dir, barrier, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    ctx =
      opts_fun.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: dir,
        spec: spec,
        plan: plan,
        spec_hash: hash(spec),
        plan_hash: hash(plan),
        supervisor_instance: "sup_mount_0001",
        trace: self(),
        barrier: barrier
      )
      |> Keyword.merge(extra)

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_mount_0001",
        command_id: "cmd_mount_red_000000001",
        now: @now
      )

    {command, ctx}
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  # a barrier that holds the mounted run at `hold` (inside the helper) until the test releases it
  defp holding(test_pid, hold) do
    fn label, payload ->
      if label == hold do
        ref = make_ref()
        send(test_pid, {:held, label, ref, payload, self()})

        receive do
          {:release, ^ref} -> :ok
        after
          @deadline -> exit(:never_released)
        end
      else
        :ok
      end
    end
  end

  defp mount!(h, dir, barrier, extra \\ []) do
    H.reset_seams()
    {command, ctx} = command_ctx(dir, barrier, extra)
    {:ok, handle} = host().mount(command, ctx, host: h.host, budgets: @budgets)
    handle
  end

  defp await_held!(hold) do
    assert_receive {:held, ^hold, ref, payload, helper}, @deadline
    {ref, payload, helper}
  end

  defp owned_pids(payload), do: payload |> Map.take([:supervisor, :server, :work, :writer, :worker]) |> Map.values()
  defp monitor_all(pids), do: for(pid <- pids, do: {pid, Process.monitor(pid)})

  defp assert_all_down!(mons) do
    for {pid, mon} <- mons, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, @deadline)
  end

  defp assert_released!(dir, arb) do
    assert :none == Ownership.status(dir, server: arb), "arbiter record not retired"
    assert :none == RunLock.owner(SystemFs.new(), dir), "disk lock still present"
  end

  defp kill_join!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, @deadline
  end

  defp lock_opts,
    do: [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]

  # a registration-visibility witness: the cast is asynchronous, so visibility is awaited, never assumed
  defp await_registered!(h, dir) do
    deadline = System.monotonic_time(:millisecond) + 2_000

    fn -> host().status(dir, monitor: h.mon, ownership: h.arb) end
    |> Stream.repeatedly()
    |> Enum.find(fn
      {:ok, %{registered: true}} -> true
      _ -> System.monotonic_time(:millisecond) > deadline and flunk("registration never became visible")
    end)
  end

  # ================================================================= rows
  describe "layout and readiness" do
    test "MR-1b the Application root order is [Ownership, Host.Supervisor, Host.Monitor] (behavioural row)" do
      assert [Ownership, host_sup(), monitor_mod()] == AiOrchestrator.Application.children()
    end

    test "MR-1a a mounted tree is a descendant with the exact trusted identities in its barrier payload", %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {ref, payload, helper} = await_held!(:subtree_started)
      assert handle.owner == payload.owner
      assert_payload_exact!(payload, handle.owner)
      assert handle.owner in Enum.map(DynamicSupervisor.which_children(h.hsup), &elem(&1, 1))
      # Run.Supervisor's parent link is the owner itself (single owner, no protocol process)
      {:links, links} = Process.info(payload.supervisor, :links)
      assert handle.owner in links
      send(helper, {:release, ref})
      assert match?({:ok, %{}}, host().await(handle, @deadline))
    end

    test "MR-2 start acknowledgment is not readiness: a held mount never blocks another mount", %{dir: dir} do
      h = start_host!()
      other = dir <> "_other"
      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf!(other) end)
      a = mount!(h, dir, holding(self(), :handoff_received))
      {ref_a, _payload_a, helper_a} = await_held!(:handoff_received)
      assert {:error, %{clause: "ready_timeout"}} == host().ready(a, 300)
      assert Process.alive?(helper_a), "ready_timeout must leave the held tree untouched"
      b = mount!(h, other, fn _, _ -> :ok end)
      assert {:ok, %{generation: g}} = host().ready(b, @deadline)
      assert is_integer(g) and g >= 1
      assert match?({:ok, %{}}, host().await(b, @deadline))
      assert {:ok, :stopped} == host().stop(a, @deadline)
      refute Process.alive?(helper_a)
      assert_released!(dir, h.arb)
      send(helper_a, {:release, ref_a})
    end

    test "MR-3 ready follows the :subtree_started barrier RESULT, not the phase name", %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {ref, _payload, helper} = await_held!(:subtree_started)
      waiter = Task.async(fn -> host().ready(handle, @deadline) end)
      assert nil == Task.yield(waiter, 300), "ready answered while the barrier had not returned"
      send(helper, {:release, ref})
      assert {:ok, %{supervisor: _, server: _, writer: _, worker: _}} = Task.await(waiter, @deadline)
      assert match?({:ok, %{}}, host().await(handle, @deadline))
    end

    test "MR-4 mounted-route barrier identity and registry owner (visibility awaited with a witness)", %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {ref, payload, helper} = await_held!(:subtree_started)
      assert helper != payload.owner, "the mounted barrier runs in a helper, not in the owner"
      assert payload.owner == handle.owner
      expected = assert_payload_exact!(payload, handle.owner)
      # registration precedes the user barrier inside the same helper: it is proven WHILE the run is held
      {:ok, %{owner: registered, supervisor: sup, server: srv, writer: wr, worker: wk}} = await_registered!(h, dir)

      assert {registered, sup, srv, wr, wk} ==
               {handle.owner, expected.supervisor, expected.server, expected.writer, expected.worker}

      send(helper, {:release, ref})
      assert match?({:ok, %{}}, host().await(handle, @deadline))
    end
  end

  describe "parent shutdown and owner death" do
    test "MR-5 parent shutdown at each held barrier and while awaiting: teardown over the owner's own identities", %{
      dir: dir
    } do
      for {tag, hold, suspend_server?} <- [
            {"handoff", :handoff_received, false},
            {"subtree", :subtree_started, false},
            {"await", :subtree_started, true}
          ] do
        d = dir <> "_" <> tag
        File.mkdir_p!(d)
        on_exit(fn -> File.rm_rf!(d) end)
        h = start_host!()
        handle = mount!(h, d, holding(self(), hold))
        {ref, payload, helper} = await_held!(hold)
        mons = payload.supervisor |> trusted_map!() |> Map.values() |> monitor_all()
        hmon = Process.monitor(helper)

        if suspend_server? do
          :ok = :sys.suspend(payload.server)
          send(helper, {:release, ref})
          assert {:ok, _} = host().ready(handle, @deadline)
        end

        waiter = Task.async(fn -> host().await(handle, @deadline) end)
        assert_waiters!(handle.owner, 1)
        :ok = Supervisor.terminate_child(h.root, host_sup())
        assert {:error, %{clause: "run_host_stopped"}} == Task.await(waiter, @deadline), tag
        assert_receive {:DOWN, ^hmon, :process, ^helper, _}, @deadline
        assert_all_down!(mons)
        assert_released!(d, h.arb)
        assert {:error, %{clause: "run_host_owner_down"}} == host().await(handle, 500)
        if !suspend_server?, do: send(helper, {:release, ref})
      end
    end

    test "MR-6 owner :kill at each barrier with a suspended worker, Writer and Run.Supervisor: exact lock and arbiter observations",
         %{dir: dir} do
      for hold <- [:handoff_received, :subtree_started], role <- [:worker, :writer, :supervisor] do
        d = dir <> "_#{hold}_#{role}"
        File.mkdir_p!(d)
        on_exit(fn -> File.rm_rf!(d) end)
        h = start_host!()
        handle = mount!(h, d, holding(self(), hold))
        {_ref, payload, helper} = await_held!(hold)
        trusted = trusted_map!(payload.supervisor)
        :ok = :sys.suspend(Map.fetch!(trusted, role))
        mons = trusted |> Map.values() |> monitor_all()
        hmon = Process.monitor(helper)
        kill_join!(handle.owner)
        assert_receive {:DOWN, ^hmon, :process, ^helper, _}, @deadline
        assert_all_down!(mons)
        # the observation is exact, never relaxed: a suspended descendant that terminates on its parent's exit
        # releases; a failed release would be a failure of this row, not a redefinition of it
        assert_released!(d, h.arb)
        assert {:error, %{clause: "run_host_owner_down"}} == host().await(handle, 500)
      end
    end

    test "MR-7 a never-released startup under a REAL isolated DynamicSupervisor: stop within the deadline, fail-closed unproven",
         %{dir: dir} do
      h = start_host!(child_shutdown_ms: 1_000)
      fs = FaultFs.new()
      observer = self()

      :ok =
        FaultFs.inject(
          fs,
          :open,
          1,
          {:hook,
           fn ->
             send(observer, {:blocking, self()})

             receive do
               :unblock -> true
             after
               60_000 -> true
             end
           end}
        )

      handle = mount!(h, dir, nil, fs: fs)
      assert_receive {:blocking, blocked}, @deadline

      try do
        # the hook blocks the acquire INSIDE the private arbiter's call path: that arbiter is unavailable, not :none
        assert match?(
                 {:error, %{clause: "ownership_unavailable"}},
                 Ownership.status(dir, server: h.arb, acquire_timeout: 300)
               )

        started = System.monotonic_time(:millisecond)
        assert {:error, %{clause: "run_host_stop_unproven"}} == host().stop(handle, 2_000)
        assert System.monotonic_time(:millisecond) - started < 1_000 + 2_000 + @slack
        refute handle.owner in Enum.map(DynamicSupervisor.which_children(h.hsup), &elem(&1, 1))
        # while the hook is still held nothing about the lock is claimed: unproven stays unproven
      after
        send(blocked, :unblock)
      end

      # EVENTUAL outcome after the controlled release: the caller observes lock and arbiter itself
      bmon = Process.monitor(blocked)
      assert_receive {:DOWN, ^bmon, :process, ^blocked, _}, @deadline
      wait_until(fn -> Ownership.status(dir, server: h.arb, acquire_timeout: 500) == :none end)
      assert :none == RunLock.owner(SystemFs.new(), dir)
      # responsive control on a SEPARATE isolated host: the hook is released before stop
      h2 = start_host!()
      other = dir <> "_released"
      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf!(other) end)
      fs2 = FaultFs.new()

      :ok =
        FaultFs.inject(
          fs2,
          :open,
          1,
          {:hook,
           fn ->
             send(observer, {:blocking2, self()})
             receive(do: (:unblock -> true))
           end}
        )

      handle2 = mount!(h2, other, holding(self(), :subtree_started), fs: fs2)
      assert_receive {:blocking2, blocked2}, @deadline
      send(blocked2, :unblock)
      {ref, _payload, helper} = await_held!(:subtree_started)
      assert {:ok, :stopped} == host().stop(handle2, @deadline)
      assert_released!(other, h2.arb)
      refute Process.alive?(helper)
      send(helper, {:release, ref})
    end

    test "MR-8 unobserved helper join and unobserved descendant join are survivors, never silently :ok", %{dir: dir} do
      for target <- [:helper, :worker] do
        d = dir <> "_#{target}"
        File.mkdir_p!(d)
        on_exit(fn -> File.rm_rf!(d) end)
        h = start_host!()

        seam = fn pid, mon, timeout ->
          observed =
            receive do
              {:DOWN, ^mon, :process, ^pid, _} -> true
            after
              timeout -> false
            end

          if Process.get(:unobserved) == pid, do: false, else: observed
        end

        handle = mount!(h, d, holding(self(), :subtree_started), join: seam)
        {_ref, payload, helper} = await_held!(:subtree_started)
        victim = if target == :helper, do: helper, else: payload.worker

        :sys.replace_state(handle.owner, fn state ->
          Process.put(:unobserved, victim)
          state
        end)

        assert {:error, %{clause: "run_executor_teardown_incomplete", survivors: 1}} == host().stop(handle, @deadline)
        # the kill itself succeeded (control): the processes are gone even though the join was reported unobserved
        refute Process.alive?(victim)
      end

      # (c) the caller's stop budget bounds the whole call and never extends the child shutdown
      d = dir <> "_stop_timeout"
      File.mkdir_p!(d)
      on_exit(fn -> File.rm_rf!(d) end)
      h = start_host!(child_shutdown_ms: 5_000)

      slow = fn pid, mon, timeout ->
        Process.sleep(400)
        receive(do: ({:DOWN, ^mon, :process, ^pid, _} -> true), after: (timeout -> false))
      end

      handle = mount!(h, d, holding(self(), :subtree_started), join: slow)
      {_ref, _payload, _helper} = await_held!(:subtree_started)
      {result, elapsed} = timed(fn -> host().stop(handle, 100) end)
      assert {:error, %{clause: "run_host_stop_timeout"}} == result
      assert elapsed < 100 + @slack
      # the teardown continued to its own outcome
      wait_until(fn -> not Process.alive?(handle.owner) end)
      assert_released!(d, h.arb)
    end

    test "MR-9 late identity: the producer (Run.Server) is joined before the sweep; a synthetic identity with the real ref is swept",
         %{dir: dir} do
      # (a) REAL producer cut point: the Server is suspended the moment it starts (trace from Run.Supervisor's init),
      # so the owner sits in :handoff with no identity; stop tears down; the Server's DOWN is joined; no identity ever
      # reaches the owner (Server and Worker do not trap exits, so a stopped producer cannot send later)
      h = start_host!()
      handle = mount!(h, dir, nil)
      assert_receive {:run_child_started, sup, :server, server}, @deadline
      :ok = :sys.suspend(server)
      smon = Process.monitor(server)
      wait_until(fn -> run_owner().inspect(handle.owner).phase == :handoff end)
      assert {:ok, :stopped} == host().stop(handle, @deadline)
      assert_receive {:DOWN, ^smon, :process, ^server, _}, @deadline
      refute Process.alive?(sup)
      assert_released!(dir, h.arb)
      # (b) SYNTHETIC witness (labelled): the sweep after the joins kills an identity carrying the REAL handoff ref
      d2 = dir <> "_synthetic"
      File.mkdir_p!(d2)
      on_exit(fn -> File.rm_rf!(d2) end)

      slow = fn pid, mon, timeout ->
        Process.sleep(150)
        receive(do: ({:DOWN, ^mon, :process, ^pid, _} -> true), after: (timeout -> false))
      end

      handle2 = mount!(h, d2, holding(self(), :subtree_started), join: slow)
      {_ref, payload, _helper} = await_held!(:subtree_started)
      %{handoff_ref: href} = run_owner().inspect(handle2.owner)
      stray = spawn(fn -> receive(do: (:never -> :ok)) end)
      on_exit(fn -> Process.exit(stray, :kill) end)
      smon2 = Process.monitor(stray)
      stopper = Task.async(fn -> host().stop(handle2, @deadline) end)
      wait_until(fn -> run_owner().inspect(handle2.owner).phase == :tearing_down end)
      send(handle2.owner, {:run_worker_registered, href, stray, payload.server})
      assert_receive {:DOWN, ^smon2, :process, ^stray, :killed}, @deadline
      assert {:ok, :stopped} == Task.await(stopper, @deadline)
    end
  end

  describe "waiters, stop lifecycle, results" do
    test "MR-10 waiters: a live timed-out caller expires by timer, a dead caller by DOWN, a valid waiter is answered",
         %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {ref, _payload, helper} = await_held!(:subtree_started)
      # (a) live caller, call timed out: the entry expires within its own deadline; observed through :sys (no event)
      assert {:error, %{clause: "await_timeout"}} == host().await(handle, 100)
      # inspection is a system message, never an owner event: once the entry's own deadline (the caller's 100 ms)
      # has passed it is gone WITHOUT any other message reaching the owner (a lazy, event-driven expiry would
      # still show it here, because :sys.get_state is not an event)
      Process.sleep(150)
      assert 0 == run_owner().inspect(handle.owner).waiters
      # (b) dead caller: DOWN cleanup
      dead = spawn(fn -> host().await(handle, @deadline) end)
      assert_waiters!(handle.owner, 1)
      kill_join!(dead)
      assert_waiters!(handle.owner, 0)
      # (c) valid waiter answered
      waiter = Task.async(fn -> host().await(handle, @deadline) end)
      assert_waiters!(handle.owner, 1)
      send(helper, {:release, ref})
      assert match?({:ok, %{}}, Task.await(waiter, @deadline))
    end

    test "MR-11 stop lifecycle: active stop completes, second active stop owner_down, terminal stop idempotent, expiry",
         %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {_ref, payload, _helper} = await_held!(:subtree_started)
      mons = monitor_all(owned_pids(payload))
      assert {:ok, :stopped} == host().stop(handle, @deadline)
      assert_all_down!(mons)
      assert {:error, %{clause: "run_host_owner_down"}} == host().stop(handle, @deadline)
      # a retained terminal owner: stop is idempotent and the cached result survives; then expiry
      other = dir <> "_terminal"
      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf!(other) end)
      H.reset_seams()
      {command, ctx} = command_ctx(other, fn _, _ -> :ok end, [])
      {:ok, h2} = host().mount(command, ctx, host: h.host, budgets: @budgets, retention_ms: 800)
      assert {:ok, %{} = result} = host().await(h2, @deadline)
      assert :ok == host().stop(h2, @deadline)
      assert {:ok, ^result} = host().await(h2, @deadline)
      Process.sleep(1_000)
      assert {:error, %{clause: "run_host_owner_down"}} == host().await(h2, 500)
    end

    test "MR-12 lock probes with real Writer/Recovery.acquire: responsive release, failed release, killed Writer", %{
      dir: dir
    } do
      h = start_host!()
      # G-lock-1: after stop the directory is reacquirable by a real Writer and by Recovery.acquire (trapping caller)
      handle = mount!(h, dir, holding(self(), :handoff_received))
      {_ref, _payload, _helper} = await_held!(:handoff_received)
      assert {:ok, :stopped} == host().stop(handle, @deadline)
      assert_released!(dir, h.arb)
      assert {:ok, writer, _} = Writer.open(dir, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: h.arb])
      assert :ok == Writer.close(writer)

      outcome =
        trapping!(fn ->
          with {:ok, acquired} <- Run.Recovery.acquire(dir, lock: lock_opts(), ownership: [server: h.arb]) do
            Run.Recovery.release(acquired)
          end
        end)

      assert :ok == outcome
      assert_released!(dir, h.arb)
      # G-lock-2a: a failed tombstone link on release -> close_failed naming the lock leg; the record is retained :down
      d2 = dir <> "_failrelease"
      File.mkdir_p!(d2)
      on_exit(fn -> File.rm_rf!(d2) end)
      fs = FaultFs.new()
      File.write!(Path.join(d2, "events.jsonl"), "", [:exclusive])
      {:ok, w2, _} = Writer.open(d2, fs: fs, lock: lock_opts(), ownership: [server: h.arb])
      :ok = FaultFs.inject(fs, :link, fn _ -> true end, {:error, :eacces})
      assert {:error, %{clause: "close_failed", failures: failures}} = Writer.close(w2)
      assert Enum.any?(failures, &(&1.leg == "lock"))
      wait_until(fn -> match?({:ok, %{state: :down}}, Ownership.status(d2, server: h.arb)) end)
      assert {:ok, %{}} = RunLock.owner(SystemFs.new(), d2)
      # deterministic per path: (i) responsive FS under the same arbiter reclaims -> a real Writer opens
      assert {:ok, w3, _} = Writer.open(d2, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: h.arb])
      assert :ok == Writer.close(w3)
      assert_released!(d2, h.arb)
      # (ii) injected failure on the reclaim itself -> the arbiter's closed reclaim_failed (no path bytes)
      d4 = dir <> "_reclaimfail"
      File.mkdir_p!(d4)
      on_exit(fn -> File.rm_rf!(d4) end)
      fs4 = FaultFs.new()
      File.write!(Path.join(d4, "events.jsonl"), "", [:exclusive])
      {:ok, w5, _} = Writer.open(d4, fs: fs4, lock: lock_opts(), ownership: [server: h.arb])
      :ok = FaultFs.inject(fs4, :link, fn _ -> true end, {:error, :eacces})
      assert {:error, %{clause: "close_failed"}} = Writer.close(w5)
      fs5 = FaultFs.new()
      :ok = FaultFs.inject(fs5, :link, fn _ -> true end, {:error, :eacces})

      assert {:error, %{clause: "reclaim_failed"} = rejection} =
               Writer.open(d4, fs: fs5, lock: lock_opts(), ownership: [server: h.arb])

      refute Map.has_key?(rejection, :path)
      # G-lock-2b: a killed Writer strands the lock; a fresh arbiter refuses the live same-OS holder as run_locked
      d3 = dir <> "_killed"
      File.mkdir_p!(d3)
      on_exit(fn -> File.rm_rf!(d3) end)
      File.write!(Path.join(d3, "events.jsonl"), "", [:exclusive])
      {:ok, w4, _} = Writer.open(d3, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: h.arb])
      Process.unlink(w4)
      kill_join!(w4)
      wait_until(fn -> match?({:ok, %{state: :down}}, Ownership.status(d3, server: h.arb)) end)
      fresh = start_supervised!(%{id: :fresh_arb, start: {Ownership, :start_link, [[name: nil]]}})

      assert {:error, %{clause: "run_locked"}} =
               Writer.open(d3, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: fresh])
    end

    test "MR-13 stop journals nothing; cancel refused while held, admitted after stop", %{dir: dir} do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :handoff_received))
      {_ref, _payload, _helper} = await_held!(:handoff_received)
      {:ok, before} = File.read(Path.join(dir, "events.jsonl"))
      # same-BEAM comparison through the SAME private arbiter (the passthrough) -> second_live_writer
      {command, ctx} = cancel_ctx(dir, ownership: [server: h.arb])
      assert {:error, %{clause: "second_live_writer"}} = Run.Executor.execute(command, ctx)
      assert {:ok, ^before} = File.read(Path.join(dir, "events.jsonl"))
      # cross-arbiter control (default admission against a lock held under a private arbiter) -> run_locked
      {command, ctx} = cancel_ctx(dir, [])
      assert {:error, %{clause: "run_locked"}} = Run.Executor.execute(command, ctx)
      assert {:ok, ^before} = File.read(Path.join(dir, "events.jsonl"))
      assert {:ok, :stopped} == host().stop(handle, @deadline)
      assert {:ok, ^before} = File.read(Path.join(dir, "events.jsonl"))
      {command, ctx} = cancel_ctx(dir, ownership: [server: h.arb])
      assert match?({:ok, %{}}, Run.Executor.execute(command, ctx))
    end
  end

  describe "census, discovery, arbiter passthrough, privacy, parity" do
    test "MR-14 census: live held mounts rebuilt, stalled owner skipped, concurrent mount, late reply vs replacement, terminal not resurrected, stalled supervisor",
         %{dir: dir} do
      h = start_host!()
      dirs = for tag <- ~w(two done during), do: dir <> "_" <> tag
      Enum.each(dirs, &File.mkdir_p!/1)
      on_exit(fn -> Enum.each(dirs, &File.rm_rf!/1) end)
      [d2, d_done, d_during] = dirs
      a = mount!(h, dir, holding(self(), :subtree_started))
      {ra, pa, ha} = await_held!(:subtree_started)
      b = mount!(h, d2, holding(self(), :subtree_started))
      {_rb, _pb, _hb} = await_held!(:subtree_started)
      # a completed run whose owner is RETAINED (terminal) and already unregistered
      H.reset_seams()
      {command, ctx} = command_ctx(d_done, fn _, _ -> :ok end, [])
      {:ok, c} = host().mount(command, ctx, host: h.host, budgets: @budgets, retention_ms: 30_000)
      assert {:ok, %{}} = host().await(c, @deadline)
      assert match?({:ok, %{registered: false}}, host().status(d_done, monitor: h.mon, ownership: h.arb))
      assert match?(%{phase: :terminal}, run_owner().inspect(c.owner))
      :ok = :sys.suspend(b.owner)
      kill_join!(Process.whereis(h.mon))
      # concurrent activity DURING the census: a new mount registers through the live path
      e = mount!(h, d_during, holding(self(), :subtree_started))
      {re, _pe, he} = await_held!(:subtree_started)
      assert {:ok, %{census: :complete, skipped: 1}} = await_census!(h)

      assert {:ok, %{registered: true, owner: owner_a, supervisor: sup_a}} =
               host().status(dir, monitor: h.mon, ownership: h.arb)

      assert sup_a == pa.supervisor and owner_a == a.owner
      assert {:ok, %{registered: true, owner: owner_e}} = host().status(d_during, monitor: h.mon, ownership: h.arb)
      assert owner_e == e.owner

      assert match?({:ok, %{registered: false}}, host().status(d2, monitor: h.mon, ownership: h.arb)),
             "a skipped owner is partial knowledge"

      assert match?({:ok, %{registered: false}}, host().status(d_done, monitor: h.mon, ownership: h.arb)),
             "a retained terminal owner must not be resurrected"

      # a synthetic census reply naming the terminal owner (alive) is confirmed with the owner and dropped
      send(Process.whereis(h.mon), {:census_reply, make_ref(), full_record(d_done, c.owner, 1), :awaiting})
      Process.sleep(200)
      assert match?({:ok, %{registered: false}}, host().status(d_done, monitor: h.mon, ownership: h.arb))

      # late reply versus replacement: stop b (owner gone), mount b2 on the same directory,
      # then a late reply for the old owner
      :ok = :sys.resume(b.owner)
      old_owner = b.owner
      assert {:ok, :stopped} == host().stop(b, @deadline)
      b2 = mount!(h, d2, holding(self(), :subtree_started))
      {rb2, _pb2, hb2} = await_held!(:subtree_started)
      assert {:ok, %{registered: true, owner: owner_b2}} = await_registered!(h, d2)
      assert owner_b2 == b2.owner
      send(Process.whereis(h.mon), {:census_reply, make_ref(), full_record(d2, old_owner, 1), :awaiting})
      Process.sleep(200)
      assert {:ok, %{registered: true, owner: ^owner_b2}} = host().status(d2, monitor: h.mon, ownership: h.arb)
      # stalled host supervisor: the census completes with nothing learned
      :ok = :sys.suspend(Process.whereis(h.hsup))
      kill_join!(Process.whereis(h.mon))
      assert {:ok, %{census: :complete}} = await_census!(h)
      :ok = :sys.resume(Process.whereis(h.hsup))
      for {r, hp} <- [{ra, ha}, {re, he}, {rb2, hb2}], do: send(hp, {:release, r})
      for x <- [a, e, b2], do: assert(match?({:ok, %{}}, host().await(x, @deadline)))
    end

    test "MR-15 Host.mounted: delayed discovery then stalled owners under ONE total budget (batch seam rejects per-leg resets)",
         %{dir: dir} do
      h = start_host!()
      dirs = for tag <- ~w(one two three), do: dir <> "_" <> tag
      Enum.each(dirs, &File.mkdir_p!/1)
      on_exit(fn -> Enum.each(dirs, &File.rm_rf!/1) end)
      handles = for d <- dirs, do: mount!(h, d, holding(self(), :subtree_started))
      held = for _ <- dirs, do: await_held!(:subtree_started)
      [h1, h2, _h3] = handles
      :ok = :sys.suspend(h1.owner)
      :ok = :sys.suspend(h2.owner)
      # discovery answers only after 200 ms (a forwarding proxy in front of the host supervisor)
      proxy = start_supervised!({DelayingProxy, target: Process.whereis(h.hsup), delay: 200})
      {result, elapsed} = timed(fn -> host().mounted(%{h.host | supervisor: proxy}, 600, batch: 1) end)
      assert {:ok, listed} = result
      assert Enum.count(listed, &(&1.phase == :unknown)) == 2
      # a fresh budget per discovery leg / per batch would need 200 + 2 x 600: rejected by the single budget
      assert elapsed < 600 + @slack, "mounted took #{elapsed} ms"
      :ok = :sys.suspend(Process.whereis(h.hsup))
      {result, elapsed} = timed(fn -> host().mounted(h.host, 300) end)
      assert {:error, %{clause: "host_supervisor_unavailable"}} == result
      assert elapsed < 300 + @slack
      :ok = :sys.resume(Process.whereis(h.hsup))
      :ok = :sys.resume(h1.owner)
      :ok = :sys.resume(h2.owner)
      for {r, _p, hp} <- held, do: send(hp, {:release, r})
      for x <- handles, do: assert(match?({:ok, %{}}, host().await(x, @deadline)))
    end

    # ---- RP rows: UNGUARDED; they fail today on behaviour (baseline MB-6), not on a missing module ----
    test "MR-16/RP-1 Run.Supervisor registers its Writer with the injected arbiter, not the global one", %{dir: dir} do
      Process.flag(:trap_exit, true)
      arb = start_supervised!({Ownership, name: nil})
      config = run_config(dir, ownership: [server: arb])
      {:ok, sup} = Run.Supervisor.start_link(config)
      assert {:ok, %{state: :live}} = Ownership.status(dir, server: arb)
      assert :none == Ownership.status(dir)
      :ok = Supervisor.stop(sup, :shutdown, @deadline)
      assert :none == Ownership.status(dir, server: arb)
    end

    test "MR-16/RP-2 Run.Server discovery confirms the Writer against the injected arbiter", %{dir: dir} do
      Process.flag(:trap_exit, true)
      arb = start_supervised!({Ownership, name: nil})
      config = dir |> run_config(ownership: [server: arb]) |> Map.put(:trace, self())
      {:ok, sup} = Run.Supervisor.start_link(config)
      assert_receive {:run_server_driving, _server, %{ownership: {:ok, %{writer: writer}}}}, @deadline
      assert {:ok, %{writer: ^writer}} = Ownership.status(dir, server: arb)
      :ok = Supervisor.stop(sup, :shutdown, @deadline)
    end

    test "MR-16/RP-3 a mounted run under the private arbiter: registration, status and the arbiter-loss cascade", %{
      dir: dir
    } do
      h = start_host!()
      handle = mount!(h, dir, holding(self(), :subtree_started))
      {_ref, payload, _helper} = await_held!(:subtree_started)
      # registration is proven WHILE the run is held; the interruption (arbiter kill) happens with the run live
      assert {:ok, %{registered: true, live: true}} = await_registered!(h, dir)
      assert :none == Ownership.status(dir), "the global arbiter must not see a run mounted under a private one"
      mons = payload.supervisor |> trusted_map!() |> Map.values() |> monitor_all()
      omon = Process.monitor(handle.owner)
      kill_join!(Process.whereis(h.arb))
      assert_receive {:DOWN, ^omon, :process, _, _}, @deadline
      assert_all_down!(mons)
      wait_until(fn -> is_pid(Process.whereis(h.arb)) end)
      assert_released!(dir, h.arb)
      assert {:ok, w, _} = Writer.open(dir, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: h.arb])
      assert :ok == Writer.close(w)
    end

    test "MR-17 privacy: the owner's format_status and a helper crash report carry no run directory or config", %{
      dir: dir
    } do
      h = start_host!()
      canary = "canary_#{System.unique_integer([:positive])}"
      d = Path.join(dir, canary)
      File.mkdir_p!(d)
      handle = mount!(h, d, holding(self(), :subtree_started))
      {ref, _payload, helper} = await_held!(:subtree_started)
      status_bytes = handle.owner |> :sys.get_status() |> inspect(limit: :infinity, printable_limit: :infinity)
      refute String.contains?(status_bytes, canary)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(helper, {:release, ref})
          assert match?({:ok, %{}}, host().await(handle, @deadline))
          # a crashing helper on a second mount
          d2 = Path.join(dir, canary <> "_crash")
          File.mkdir_p!(d2)
          h2 = mount!(h, d2, fn _, _ -> raise "helper boom " <> canary end)
          assert {:error, %{clause: "run_executor_down"} = closed} = host().await(h2, @deadline)
          refute String.contains?(inspect(closed, limit: :infinity), canary)
          wait_until(fn -> not Process.alive?(h2.owner) or match?(%{phase: :terminal}, run_owner().inspect(h2.owner)) end)
          Process.sleep(100)
        end)

      refute String.contains?(log, canary), "the helper escape report leaked the raised reason or the run directory"
    end

    test "MR-18 slice-1 status, lookup and collision shapes are unchanged (parity control)", %{dir: dir} do
      h = start_host!()
      assert {:ok, %{registered: false} = absent} = host().status(dir, monitor: h.mon, ownership: h.arb)
      assert Map.keys(absent) == [:registered]
      assert {:ok, []} == host().lookup_run_id("run_mount_0001", monitor: h.mon)
      assert {:ok, %{clause: "host_registry_unique", count: 0}} == host().collision("run_mount_0001", monitor: h.mon)
      assert {:ok, %{census: census, skipped: skipped}} = host().census(monitor: h.mon)
      assert census in [:pending, :complete] and is_integer(skipped)
    end
  end

  # ---- helpers ----
  defp assert_waiters!(owner, n) do
    deadline = System.monotonic_time(:millisecond) + 2_000

    fn -> run_owner().inspect(owner).waiters end
    |> Stream.repeatedly()
    |> Enum.find(fn
      ^n -> true
      _ -> System.monotonic_time(:millisecond) > deadline and flunk("waiter count never reached #{n}")
    end)
  end

  defp await_census!(h) do
    deadline = System.monotonic_time(:millisecond) + 3_000

    fn -> host().census(monitor: h.mon) end
    |> Stream.repeatedly()
    |> Enum.find(fn
      {:ok, %{census: :complete}} -> true
      _ -> System.monotonic_time(:millisecond) > deadline and flunk("census never completed")
    end)
  end

  defp wait_until(fun, tries \\ 100) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never held")
      true -> Process.sleep(20) && wait_until(fun, tries - 1)
    end
  end

  defp full_record(dir, owner, generation) do
    %{
      run_dir: Path.expand(dir),
      run_id: "run_mount_0001",
      owner: owner,
      supervisor: owner,
      server: owner,
      writer: owner,
      worker: owner,
      generation: generation
    }
  end

  defp timed(fun) do
    started = System.monotonic_time(:millisecond)
    result = fun.()
    {result, System.monotonic_time(:millisecond) - started}
  end

  defp cancel_ctx(dir, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    H.reset_seams()

    ctx =
      opts_fun.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: dir,
        spec: H.spec(scenario),
        plan: H.plan(scenario),
        supervisor_instance: "sup_mount_0001",
        trace: self(),
        barrier: fn _, _ -> :ok end
      )
      |> Keyword.merge(extra)

    {:ok, command} =
      Commands.build(@operator, "cancel", %{"reason" => "operator_cancel"},
        run_id: "run_mount_0001",
        command_id: "cmd_mount_cancel_0000001",
        now: @now
      )

    {command, ctx}
  end

  defp run_config(dir, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    H.reset_seams()
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    ctx =
      opts_fun.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: dir,
        spec: spec,
        plan: plan,
        spec_hash: hash(spec),
        plan_hash: hash(plan),
        supervisor_instance: "sup_mount_0001",
        trace: nil
      )
      |> Keyword.merge(extra)

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_mount_0001",
        command_id: "cmd_mount_rp_000000001",
        now: @now
      )

    %{
      run_dir: dir,
      mode: :run,
      spec: spec,
      plan: plan,
      opts: ctx |> Keyword.drop([:run_dir, :spec, :plan, :trace, :barrier, :restart_empty]) |> Keyword.drop(@owned),
      trace: nil,
      command: command
    }
  end
end
