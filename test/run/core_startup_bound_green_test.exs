defmodule AiOrchestrator.Run.CoreStartupBoundGreenTest do
  @moduledoc """
  Regression rows for the ordering the GREEN unit of docs/contracts/core-startup-bound.org introduced: the mounted
  owner's `:starting` state is RESPONSIVE (section 6), so it processes its mailbox while the subtree is still being
  started. The Server produces the worker handoff during that window, and the pre-contract owner could only ever see
  that message once it was already in `:handoff`.

  Every row forces the ordering CAUSALLY instead of waiting for it: the seam's helper is suspended, so no completion
  can be accepted, and the owner itself is suspended while the real handoff is emitted, so the message is provably in
  its mailbox before it can run. The owner is then resumed alone: whatever it does with that message, it does in
  `:starting`. The rows discriminate - with the two `:starting` postpone clauses removed, G-1 leaves the worker in
  `owned.late` with no registration and no acknowledgment, and G-2 loses the event entirely and stalls until the
  handoff budget expires.

  G-3 covers the other end: an identity that reached a `:starting` owner and was postponed is still collected when the
  startup is aborted instead of accepted, because the postponed event is re-delivered in `:terminal`.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Executor.Startup
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.StartupCleanup, as: Cleanup

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @budgets %{close: 3_000, stop: 3_000, join: 1_000, handoff: 3_000, helper_join: 500, startup: 30_000, ack: 5_000}
  # the forced-abort row waits out the seam's own bound for a helper that cannot answer, so its budgets are small
  @abort_budgets %{close: 500, stop: 500, join: 200, handoff: 3_000, helper_join: 200, startup: 30_000, ack: 500}
  @window 5_000

  defmodule Relay do
    @moduledoc false
    # holds the Server's REAL handoff so the owner never receives a registration of its own (the absent branch is
    # then the only handoff-shaped event that reaches it)
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state), do: {:ok, Map.merge(%{held: [], owner: nil}, state)}

    @impl true
    def handle_info({:relay_owner, owner}, state), do: {:noreply, %{state | owner: owner}}

    def handle_info({:run_worker_registered, _, _, _} = message, state) do
      send(state.notify, {:relay_held, self(), message})
      {:noreply, %{state | held: state.held ++ [message]}}
    end

    def handle_info(_other, state), do: {:noreply, state}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "csb_green_#{System.unique_integer([:positive])}")
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

  defp queued?(pid, tag) do
    case Process.info(pid, :messages) do
      {:messages, messages} -> Enum.any?(messages, &(is_tuple(&1) and elem(&1, 0) == tag))
      nil -> false
    end
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

  describe "the worker handoff reaching a RESPONSIVE :starting owner" do
    test "G-1 the real handoff processed in :starting is postponed, not classed late: the identity is acknowledged, the record registered and the run completes",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        fs = blocking_open_fs(self())
        {:ok, handle} = mount(ctx.dir, fs, h, @budgets, [])
        own(ctx, handle.owner)
        writer = await_blocked!(ctx)
        helper = startup_helper!(handle.owner)
        own(ctx, [helper | links(helper)])

        try do
          # no completion can reach the owner, and the owner cannot run while the handoff is produced
          true = :erlang.suspend_process(helper)
          true = :erlang.suspend_process(handle.owner)
          send(writer, :unblock)

          # the Server records the worker before it hands the identity over, so this trace proves the send happened
          assert_receive {:run_child_started, _work, :worker, worker}, @window
          own(ctx, worker)

          assert wait_until(fn -> queued?(handle.owner, :run_worker_registered) end),
                 "setup: the real handoff must be queued at the owner before it runs"
        after
          resume(handle.owner)
        end

        # the owner now runs ALONE: it can only process that message in :starting
        assert wait_until(fn -> not queued?(handle.owner, :run_worker_registered) end),
               "the :starting owner never consumed the queued handoff"

        view = RunOwner.inspect(handle.owner, @window)
        assert view.phase == :starting

        refute :late in view.owned,
               "the ordinary handoff was classed as a LATE identity in :starting: it is the same event arriving " <>
                 "before acceptance and must be postponed to :handoff (owned: #{inspect(view.owned)})"

        resume(helper)

        assert {:ok, ready} = Host.ready(handle, @window)
        assert is_integer(ready.generation) and ready.generation >= 1
        assert is_pid(ready.worker)

        assert {:ok, %{registered: true, live: true, generation: generation, worker: registered}} =
                 Host.status(ctx.dir, monitor: h.mon, ownership: h.arb, timeout: @window)

        assert generation == ready.generation and registered == ready.worker

        # the acknowledgment itself: the Server admits no effect before it, so a completed run proves it happened
        assert {:ok, %{}} = Host.await(handle, 30_000)
        assert :ok == Host.stop(handle, @window)
      end)
    end

    test "G-2 an absent-handoff notice processed in :starting is postponed, not dropped: the owner proceeds at once instead of waiting out its handoff budget",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        fs = blocking_open_fs(self())
        relay = start_supervised!({Relay, notify: self()})
        {:ok, handle} = mount(ctx.dir, fs, h, @budgets, handoff_relay: relay)
        own(ctx, handle.owner)
        writer = await_blocked!(ctx)
        helper = startup_helper!(handle.owner)
        own(ctx, [helper | links(helper)])
        ref = RunOwner.inspect(handle.owner, @window).handoff_ref

        try do
          true = :erlang.suspend_process(helper)
          true = :erlang.suspend_process(handle.owner)
          send(writer, :unblock)
          # the Server's real registration is held by the relay, so the owner's only handoff-shaped event is the
          # absent notice injected here: nothing in lib/ emits it today, and this is the ordering clause it feeds
          assert_receive {:relay_held, ^relay, {:run_worker_registered, _, worker, _}}, @window
          own(ctx, worker)
          send(handle.owner, {:run_worker_absent, ref, "relay_held"})
          assert queued?(handle.owner, :run_worker_absent)
        after
          resume(handle.owner)
        end

        assert wait_until(fn -> not queued?(handle.owner, :run_worker_absent) end),
               "the :starting owner never consumed the queued absent notice"

        assert RunOwner.inspect(handle.owner, @window).phase == :starting
        resume(helper)

        # a postponed notice is consumed the moment the owner enters :handoff; a dropped one leaves it waiting for
        # the whole handoff budget (3 s), which this bound is deliberately far below
        assert {:ok, _ready} = Host.ready(handle, 1_000)
        assert {:ok, :stopped} == Host.stop(handle, @window)
      end)
    end

    test "G-3 an identity postponed in :starting is still collected when the startup is ABORTED instead of accepted",
         ctx do
      row(ctx, fn ->
        h = isolated_host()
        fs = blocking_open_fs(self())
        {:ok, handle} = mount(ctx.dir, fs, h, @abort_budgets, [])
        own(ctx, handle.owner)
        writer = await_blocked!(ctx)
        helper = startup_helper!(handle.owner)
        own(ctx, [helper | links(helper)])

        worker =
          try do
            true = :erlang.suspend_process(helper)
            true = :erlang.suspend_process(handle.owner)
            send(writer, :unblock)
            assert_receive {:run_child_started, _work, :worker, worker}, @window
            own(ctx, worker)
            assert wait_until(fn -> queued?(handle.owner, :run_worker_registered) end)
            worker
          after
            resume(handle.owner)
          end

        assert wait_until(fn -> not queued?(handle.owner, :run_worker_registered) end)
        assert RunOwner.inspect(handle.owner, @window).phase == :starting
        monitor = Process.monitor(worker)

        # the helper stays suspended: the abort falls back to the owner's own mirror duty through the starter
        assert {:ok, :stopped} == Host.stop(handle, 20_000)
        resume(helper)

        assert_receive {:DOWN, ^monitor, :process, ^worker, _}, @window
        refute Process.alive?(handle.owner)
      end)
    end
  end

  # ---- setup helpers (an isolated host per row; the same scenario the contract rows use) ----

  defp isolated_host do
    n = System.unique_integer([:positive])
    arb = :"csb_green_arb_#{n}"
    hsup = :"csb_green_hsup_#{n}"
    mon = :"csb_green_mon_#{n}"

    children = [
      {Ownership, name: arb},
      {Host.Supervisor, name: hsup, child_shutdown_ms: 20_000},
      {Monitor, name: mon, host_supervisor: hsup, ownership: arb, census_timeout: 2_000}
    ]

    start_supervised!(%{
      id: :"csb_green_root_#{n}",
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
      supervisor_instance: "sup_csb_green",
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
        run_id: "run_csb_green_#{n}",
        command_id: "cmd_csb_green_#{String.pad_leading("#{n}", 6, "0")}",
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
