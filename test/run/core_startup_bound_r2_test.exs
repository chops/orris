defmodule AiOrchestrator.Run.CoreStartupBoundR2Test do
  @moduledoc """
  Discriminating regressions for the four lifecycle findings of the independent review of 4096e5c (R1-R4). Each row
  fails on the exact pre-correction source and names the defect in its message.

  R1 forwarded the supervisor EXIT with the startup generation while the mounted owner matched its worker-handoff
  reference, so a real supervisor death was ignored and a held barrier could wait forever. R2 let an already-dead
  reaper certify teardown: its death establishes nothing, because the starter traps that exit and goes on holding a
  live supervisor. R3 blocked the owner in `begin/2` on a helper message before either owner armed its expiry. R4
  observed ownership and announced the abort BEFORE killing the Writers, so `:reclaimable` was a prediction rather
  than an observation.

  Ordering is forced causally, never sampled: the ORIGINAL regression rows for the responsive `:starting` state: the mounted
  owner's `:starting` state is RESPONSIVE (section 6), so it processes its mailbox while the subtree is still being
  live in core_startup_bound_green_test.exs and are unchanged. C-4 is honest coverage, not a discriminator: R3's
  defect was an unbounded receive that no longer exists in the source, and holding a helper before its own first
  instruction is not something a test can win deterministically.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Run.Executor
  alias AiOrchestrator.Run.Executor.Startup
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.StartupCleanup, as: Cleanup

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @budgets %{close: 3_000, stop: 3_000, join: 1_000, handoff: 3_000, helper_join: 500, startup: 30_000, ack: 5_000}
  @window 5_000

  setup do
    dir = Path.join(System.tmp_dir!(), "csb_r2_#{System.unique_integer([:positive])}")
    cleanup = Cleanup.setup_row(dir)
    %{dir: dir, cleanup: cleanup}
  end

  defp own(%{cleanup: c}, pids), do: Cleanup.own(c, pids)
  defp gate(%{cleanup: c}, pid), do: Cleanup.gate(c, pid)
  defp row(%{cleanup: c}, body), do: Cleanup.row(c, body)

  # ---- forcing the ordering ----

  # the seam's helper: the only path a completion can take to the owner, so a suspended helper pins :starting
  defp startup_helper!(owner) do
    helper =
      wait_until(fn ->
        Enum.find(links(owner), &match?({Startup, :helper_init, _}, initial_call(&1)))
      end)

    assert is_pid(helper), "the startup helper must be linked to the owner"
    helper
  end

  defp wait_until(fun, tries \\ 500) do
    Enum.find_value(1..tries, fn _ ->
      case fun.() do
        nil ->
          :timer.sleep(10)
          nil

        false ->
          :timer.sleep(10)
          nil

        value ->
          value
      end
    end)
  end

  defp links(pid) do
    case Process.info(pid, :links) do
      {:links, links} -> Enum.filter(links, &is_pid/1)
      nil -> []
    end
  end

  defp initial_call(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} -> Keyword.get(dictionary, :"$initial_call")
      nil -> :dead
    end
  end

  defp resume(pid) do
    :erlang.resume_process(pid)
  catch
    :error, _ -> :dead
  end

  # ---- rows ----

  describe "R1 the forwarded supervisor exit" do
    test "C-1 a supervisor killed at a held barrier ends the run with the retained run_server_down, never an await that cannot finish",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        {:ok, handle} = mount(ctx.dir, FaultFs.new(), h, @budgets, barrier: holding(self(), :subtree_started))
        own(ctx, handle.owner)
        assert_receive {:held, :subtree_started, release, payload, barrier}, @window
        own(ctx, [payload.supervisor, payload.writer, barrier])
        supervisor = payload.supervisor

        monitor = Process.monitor(supervisor)
        Process.exit(supervisor, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^supervisor, _}, @window

        # the owner must LEARN of that death: the forwarded exit carries the startup generation, and an owner that
        # matches its own worker-handoff reference instead sits at the barrier until the caller gives up
        assert {:error, %{clause: "run_server_down"}} == Host.await(handle, @window),
               "the forwarded supervisor exit was not matched: the owner never left its barrier"

        send(barrier, {:release, release})
      end)
    end
  end

  describe "R2 the reaper's own death" do
    test "C-2 a startup helper killed after the startup never certifies closure: the identities are reaped, and no stop answers stopped over a live subtree",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        {:ok, handle} = mount(ctx.dir, FaultFs.new(), h, @budgets, barrier: holding(self(), :subtree_started))
        own(ctx, handle.owner)
        assert_receive {:held, :subtree_started, release, payload, barrier}, @window
        own(ctx, [payload.supervisor, payload.writer, payload.server, payload.work, payload.worker, barrier])
        helper = startup_helper!(handle.owner)
        starter = Enum.find(links(helper), &match?({Startup, :starter_init, _}, initial_call(&1)))
        own(ctx, [helper, starter])
        identities = [payload.supervisor, payload.writer, payload.server, payload.work, payload.worker, starter]
        monitors = for pid <- identities, is_pid(pid), do: {pid, Process.monitor(pid)}

        Process.exit(helper, :kill)

        for {pid, monitor} <- monitors do
          assert_receive {:DOWN, ^monitor, :process, ^pid, _},
                         @window,
                         "#{inspect(initial_call(pid))} outlived the reaper's death: a dead helper proves nothing " <>
                           "about the subtree, because the starter traps its exit and keeps holding the supervisor"
        end

        # the owner's COMPLETE cleanup path must run, not a bare reap: the barrier helper it holds is an identity
        # like any other, and the registration must be gone before it retains a terminal result. Nothing here
        # releases the barrier by hand - a row that does cannot tell product cleanup from fixture cleanup.
        barrier_monitor = Process.monitor(barrier)

        assert_receive {:DOWN, ^barrier_monitor, :process, ^barrier, _},
                       @window,
                       "the barrier helper outlived the owner's terminal: the reaper's death took the short path " <>
                         "and skipped kill_helper, the late sweep, the link release and the unregister"

        # the unregister is a cast, so its visibility is AWAITED, never assumed; what the row proves is that it
        # happens at all, which the short path never does
        assert wait_until(fn ->
                 match?({:ok, %{registered: false}}, Host.status(ctx.dir, monitor: h.mon, ownership: h.arb))
               end),
               "the owner retained a terminal result while the Monitor still held its registration"

        stop = Host.stop(handle, @window)

        assert stop == :ok or match?({:error, %{clause: "run_executor_teardown_incomplete"}}, stop),
               "a stop after the reaper's death answered #{inspect(stop)}"

        _ = release
      end)
    end
  end

  describe "R4 the ownership diagnostic" do
    test "C-3 the abort trace is emitted only after the reaped Writer is observed dead, and its diagnostic names that identity",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        context = context(ctx.dir, fs, [])
        caller = spawn(fn -> Executor.execute(command!(context), context) end)
        own(ctx, caller)
        writer = await_blocked!(ctx)
        monitor = Process.monitor(writer)
        Process.exit(caller, :kill)

        # the ORDER these two reach this process is the whole point: the reap kills the Writer and JOINS its death
        # before it announces anything, so the DOWN is enqueued first. An implementation that observes ownership
        # and announces the abort BEFORE the kill announces a Writer that still owns the lock.
        order =
          for _ <- 1..2 do
            receive do
              {:DOWN, ^monitor, :process, ^writer, _} -> :writer_down
              {:run_startup_aborted, _owner, diagnostic} -> {:trace, diagnostic.ownership}
            after
              @window -> :nothing
            end
          end

        assert [:writer_down, {:trace, :reclaimable}] == order,
               "expected the reaped Writer's death to be observed BEFORE the abort trace, got #{inspect(order)}: " <>
                 ":reclaimable was predicted, not observed"
      end)
    end
  end

  describe "R3 responsiveness across the seam's birth" do
    # COVERAGE ONLY, and deliberately not described as more: this owner has already passed the seam's birth, and
    # its stop runs AFTER the helper is resumed. It is not a pre-adoption test, and it does not claim that a stop
    # succeeds while the reaper is suspended. The pre-adoption states are covered directly by the P rows below.
    test "C-4 coverage: an owner whose startup has not completed answers inspect while its reaper is suspended, and stops once it is resumed",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        fs = blocking_open_fs(self())
        {:ok, handle} = mount(ctx.dir, fs, h, @budgets, [])
        own(ctx, handle.owner)
        writer = await_blocked!(ctx)
        own(ctx, writer)
        helper = startup_helper!(handle.owner)
        own(ctx, [helper | links(helper)])

        try do
          true = :erlang.suspend_process(helper)
          assert RunOwner.inspect(handle.owner, @window).phase == :starting
        after
          resume(helper)
        end

        assert {:ok, :stopped} == Host.stop(handle, @window)
      end)
    end
  end

  describe "R3 the adoption gap (direct pre-adoption states; no scheduler race)" do
    # starter_init/1 is driven directly, so each of these states is constructed rather than raced for
    test "P-1 an owner that dies before adoption ends its unlinked starter at once, with nothing acquired", ctx do
      row(ctx, fn ->
        test = self()
        owner = spawn(fn -> receive(do: (:never -> :ok)) end)
        own(ctx, owner)
        {starter, monitor} = starter!(owner, System.monotonic_time(:millisecond) + 45_000)
        own(ctx, starter)
        Process.exit(owner, :kill)

        assert_receive {:DOWN, ^monitor, :process, ^starter, :normal},
                       @window,
                       "a starter spawned before its keeper existed waited out the startup budget after its owner died"

        assert {:none, :none} == {arbiter(ctx.dir), disk(ctx.dir)}
        refute_received {:blocked_open, _}
        _ = test
      end)
    end

    test "P-2 a starter that is never adopted expires on the owner's own deadline and says so, with nothing acquired",
         ctx do
      row(ctx, fn ->
        {starter, monitor} = starter!(self(), System.monotonic_time(:millisecond) + 250)
        own(ctx, starter)
        assert_receive {:startup_note, _ref, :deadline_before_adoption}, @window
        assert_receive {:DOWN, ^monitor, :process, ^starter, :normal}, @window
        assert {:none, :none} == {arbiter(ctx.dir), disk(ctx.dir)}
      end)
    end

    test "P-3 an adoption that arrives after the deadline finds no starter and acquires nothing", ctx do
      row(ctx, fn ->
        ref = make_ref()
        {starter, monitor} = starter!(self(), System.monotonic_time(:millisecond) + 200, ref)
        own(ctx, starter)
        assert_receive {:DOWN, ^monitor, :process, ^starter, :normal}, @window
        send(starter, {:startup_config, ref, self(), %{run_dir: ctx.dir, opts: []}})
        refute_receive {:startup_identity, ^ref, _}, 300
        assert {:none, :none} == {arbiter(ctx.dir), disk(ctx.dir)}
      end)
    end

    test "P-4 a release before adoption ends the starter normally, with nothing acquired", ctx do
      row(ctx, fn ->
        ref = make_ref()
        {starter, monitor} = starter!(self(), System.monotonic_time(:millisecond) + 45_000, ref)
        own(ctx, starter)
        send(starter, {:release, ref})
        assert_receive {:DOWN, ^monitor, :process, ^starter, :normal}, @window
        assert {:none, :none} == {arbiter(ctx.dir), disk(ctx.dir)}
      end)
    end
  end

  # the seam's starter, driven directly in its pre-adoption state
  defp starter!(owner, deadline, ref \\ make_ref()) do
    starter = :proc_lib.spawn(Startup, :starter_init, [%{owner: owner, ref: ref, deadline: deadline}])
    {starter, Process.monitor(starter)}
  end

  defp arbiter(dir) do
    case Ownership.status(dir, acquire_timeout: 1_000) do
      {:ok, registration} -> registration.state
      other -> other
    end
  end

  defp disk(dir) do
    case RunLock.owner(SystemFs.new(), dir) do
      :none -> :none
      {:ok, _held} -> :held
      other -> other
    end
  end

  # a barrier that reports and holds until released, so a row can act on a fully started tree
  defp holding(test, label) do
    fn seen, payload ->
      if seen == label do
        release = make_ref()
        send(test, {:held, seen, release, payload, self()})
        receive(do: ({:release, ^release} -> :ok), after: (30_000 -> exit(:never_released)))
      else
        :ok
      end
    end
  end

  # ---- setup helpers (an isolated host per row; the same scenario the contract rows use) ----

  defp isolated_host do
    n = System.unique_integer([:positive])
    arb = :"csb_r2_arb_#{n}"
    hsup = :"csb_r2_hsup_#{n}"
    mon = :"csb_r2_mon_#{n}"

    children = [
      {Ownership, name: arb},
      {Host.Supervisor, name: hsup, child_shutdown_ms: 20_000},
      {Monitor, name: mon, host_supervisor: hsup, ownership: arb, census_timeout: 2_000}
    ]

    start_supervised!(%{
      id: :"csb_r2_root_#{n}",
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
      type: :supervisor
    })

    %{arb: arb, hsup: hsup, mon: mon, host: %{supervisor: hsup, monitor: mon, ownership: arb}}
  end

  defp mount(dir, fs, h, budgets, extra) do
    context = context(dir, fs, extra)
    Host.mount(command!(context), context, host: h.host, budgets: budgets)
  end

  defp context(dir, fs, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    H.reset_seams()
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    opts_fun.()
    |> Keyword.drop(@owned)
    |> Keyword.merge(
      run_dir: dir,
      spec: spec,
      plan: plan,
      spec_hash: hash(spec),
      plan_hash: hash(plan),
      supervisor_instance: "sup_csb_r2",
      trace: self(),
      barrier: fn _, _ -> :ok end,
      fs: fs
    )
    |> Keyword.merge(extra)
  end

  defp command!(context) do
    n = System.unique_integer([:positive])

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => context[:spec_hash], "plan_hash" => context[:plan_hash]},
        run_id: "run_csb_r2_#{n}",
        command_id: "cmd_csb_r2_#{String.pad_leading("#{n}", 6, "0")}",
        now: @now
      )

    command
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  # the Writer's append-open is held so the startup cannot complete before the row has taken its positions
  defp blocking_open_fs(test) do
    fs = FaultFs.new()
    matcher = fn args -> match?(["events.jsonl", modes] when is_list(modes), args) and :append in Enum.at(args, 1) end

    :ok =
      FaultFs.inject(
        fs,
        :open,
        matcher,
        {:hook,
         fn ->
           send(test, {:blocked_open, self()})
           receive(do: (:unblock -> true), after: (60_000 -> true))
         end}
      )

    fs
  end

  defp await_blocked!(ctx) do
    assert_receive {:blocked_open, writer}, 10_000
    gate(ctx, writer)
    own(ctx, writer)
    writer
  end
end
