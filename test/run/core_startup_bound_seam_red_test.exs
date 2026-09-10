defmodule AiOrchestrator.Run.CoreStartupBoundSeamRedTest do
  @moduledoc """
  Rows of docs/contracts/core-startup-bound.org that need the startup seam (`AiOrchestrator.Run.Executor.Startup`,
  section 1) to execute: S-5, S-10, S-12, S-17, S-21 and the budgets row. Each row has a real setup/action/assertion
  through dynamic calls into the seam, plus cleanup, so it compiles while the seam is absent and can become GREEN
  with a correct implementation. While P-1 (core_startup_bound_red_test.exs) holds, the whole module is SKIPPED.
  Every row runs under the shared cleanup owner (AiOrchestrator.Test.StartupCleanup.row/2) with the test process
  trapping exits while it acts as the owner (the helper is linked to its owner by contract); S-10 and the timely
  controls use a bounded OWNER DRIVER (test-only process implementing the declared owner protocol of section 1).
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Executor
  alias AiOrchestrator.Run.Executor.Owner
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.StartupCleanup, as: Cleanup

  @seam Module.concat(["AiOrchestrator", "Run", "Executor", "Startup"])
  @seam_present Code.ensure_loaded?(@seam) and function_exported?(@seam, :begin, 2)
  if not @seam_present,
    do:
      @moduletag(skip: "prerequisite absent: Run.Executor.Startup (reported once by core_startup_bound_red_test.exs P-1)")

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @short 300
  @window 3_000
  @mount_budgets %{close: 3_000, stop: 3_000, join: 1_000, handoff: 2_000, helper_join: 500, startup: 5_000, ack: 500}

  # dynamic calls keep this file compiling while the seam is absent
  defp seam(fun, args), do: apply(@seam, fun, args)

  setup do
    dir = Path.join(System.tmp_dir!(), "csb_seam_#{System.unique_integer([:positive])}")
    cleanup = Cleanup.setup_row(dir)
    %{dir: dir, cleanup: cleanup, baseline: cleanup.baseline}
  end

  defp own(%{cleanup: c}, pids), do: Cleanup.own(c, pids)
  defp gate(%{cleanup: c}, pid), do: Cleanup.gate(c, pid)
  # The test process acts as the OWNER in these rows and the contract links the helper to its owner: killing the helper
  # sends this process an exit signal, so the row traps exits for its duration and restores the previous flag. This is a
  # test-owner convention only: production begin/2 never mutates a caller's flags and never removes the required link.
  defp row(%{cleanup: c}, body) do
    previous = Process.flag(:trap_exit, true)

    try do
      Cleanup.row(c, body)
    after
      Process.flag(:trap_exit, previous)
      flush_exits()
    end
  end

  defp flush_exits do
    receive do
      {:EXIT, _, _} -> flush_exits()
    after
      0 -> :ok
    end
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  defp config!(dir, fs, n) do
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
        supervisor_instance: "sup_csb_seam",
        trace: self(),
        barrier: fn _, _ -> :ok end,
        fs: fs
      )

    {:ok, c} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_csb_seam_#{n}",
        command_id: "cmd_csb_seam_#{String.pad_leading("#{n}", 6, "0")}",
        now: @now
      )

    {:ok, %{config: config}} = Executor.prepare(c, ctx)
    Map.put(config, :owner_handoff, {self(), make_ref()})
  end

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

  defp status(dir),
    do:
      (case Ownership.status(dir, acquire_timeout: 1_000) do
         {:ok, reg} -> {:ok, reg.state}
         o -> o
       end)

  defp disk(dir),
    do:
      (case RunLock.owner(SystemFs.new(), dir) do
         :none -> :none
         {:ok, _} -> :held
         o -> o
       end)

  defp initial_call(pid),
    do:
      (case Process.info(pid, :dictionary) do
         {:dictionary, d} -> Keyword.get(d, :"$initial_call")
         nil -> :dead
       end)

  defp links(pid),
    do:
      (case Process.info(pid, :links) do
         {:links, l} -> Enum.filter(l, &is_pid/1)
         nil -> []
       end)

  defp sup_of(pid), do: Enum.find(links(pid), &match?({:supervisor, Run.Supervisor, _}, initial_call(&1)))

  defp all_down?(pids, window) do
    mons = for p <- pids, is_pid(p), do: {p, Process.monitor(p)}
    t0 = System.monotonic_time(:millisecond)

    Enum.all?(mons, fn {p, m} ->
      receive(
        do: ({:DOWN, ^m, :process, ^p, _} -> true),
        after: (max(window - (System.monotonic_time(:millisecond) - t0), 0) -> false)
      )
    end)
  end

  # the test acts as the owner: begin, then (optionally) permit when the identity arrives
  defp begin!(ctx, fs, n, budgets) do
    {:ok, startup} = seam(:begin, [config!(ctx.dir, fs, n), Map.merge(%{startup: @short, ack: 500}, budgets)])
    own(ctx, [startup.helper, startup.starter])
    startup
  end

  defp identity!(startup) do
    ref = startup.ref
    assert_receive {:startup_identity, ^ref, starter}, 2_000
    starter
  end

  describe "S-21 identity permit" do
    test "S-21a late identity: permit/1 after the deadline denies, aborts and acquires nothing", ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 1, %{})
        _starter = identity!(startup)
        :timer.sleep(@short + 100)
        denied = seam(:permit, [startup])
        ref = startup.ref
        assert_receive {:startup_aborted, ^ref, %{why: :late_identity, ownership: :none}}, @window
        refute_received {:blocked_open, _}
        assert all_down?([startup.starter, startup.helper], @window)
        assert {:error, :late_identity} == denied
        assert {:none, :none} == {status(ctx.dir), disk(ctx.dir)}
      end)
    end

    test "S-21b timely permit consumed late: the starter held past the deadline refuses with :permit_expired and acquires nothing",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 2, %{})
        starter = identity!(startup)
        true = :erlang.suspend_process(starter)
        assert :ok == seam(:permit, [startup])
        :timer.sleep(@short + 100)
        true = :erlang.resume_process(starter)
        ref = startup.ref
        assert_receive {:startup_aborted, ^ref, %{why: :permit_expired, ownership: :none}}, @window
        refute_received {:blocked_open, _}
        assert all_down?([starter], @window)
        assert {:none, :none} == {status(ctx.dir), disk(ctx.dir)}
      end)
    end
  end

  describe "S-5 late completions under the one clock" do
    test "S-5a a success accepted after the deadline loses (late_result_after_deadline) and its born tree is collected",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 3, %{})
        _ = identity!(startup)
        :ok = seam(:permit, [startup])
        assert_receive {:blocked_open, writer}, 5_000
        gate(ctx, writer)
        own(ctx, [writer, sup_of(writer)])
        :timer.sleep(@short + 100)
        send(writer, :unblock)
        ref = startup.ref
        assert_receive {:startup_started, ^ref, {:ok, sup, _facts}} = completion, 5_000
        own(ctx, sup)
        result = seam(:accept, [startup, completion])
        assert match?({:error, %{clause: "run_startup_timeout", why: :late_result_after_deadline}}, result)
        assert all_down?([sup, writer, startup.starter, startup.helper], @window)
      end)
    end

    test "S-5b an error accepted after the deadline loses (late_error_after_deadline) with the late class recorded",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 4, %{})
        _ = identity!(startup)
        :ok = seam(:permit, [startup])
        assert_receive {:blocked_open, writer}, 5_000
        gate(ctx, writer)
        own(ctx, [writer, sup_of(writer)])
        :timer.sleep(@short + 100)
        Process.exit(writer, :kill)
        ref = startup.ref
        assert_receive {:startup_started, ^ref, {:error, _}} = completion, 5_000
        result = seam(:accept, [startup, completion])

        assert match?(
                 {:error,
                  %{clause: "run_startup_timeout", why: :late_error_after_deadline, late_class: "writer_start_failed"}},
                 result
               )

        assert all_down?([startup.starter, startup.helper], @window)
      end)
    end
  end

  describe "S-17 helper death" do
    test "S-17a helper death before the permit: the responsive starter exits with nothing acquired; report ownership :none, no starter",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 6, %{})
        starter = identity!(startup)
        Process.exit(startup.helper, :kill)
        report = seam(:reap, [startup])
        assert match?(%{why: {:helper_down, _}, ownership: :none}, report) and not Map.has_key?(report, :starter_reaped)
        assert all_down?([starter], @window)
        refute_received {:blocked_open, _}
        assert {:none, :none} == {status(ctx.dir), disk(ctx.dir)}
      end)
    end

    test "S-17b helper death after the permit with the starter blocked in the open: reap/1 joins writer/supervisor/starter, ownership :reclaimable",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 7, %{})
        _ = identity!(startup)
        :ok = seam(:permit, [startup])
        assert_receive {:blocked_open, writer}, 5_000
        gate(ctx, writer)
        sup = sup_of(writer)
        own(ctx, [writer, sup])
        Process.exit(startup.helper, :kill)
        report = seam(:reap, [startup])
        assert match?(%{ownership: :reclaimable, survivors: 0}, report)
        assert all_down?([writer, sup, startup.starter], @window)
        assert {:ok, :down} == status(ctx.dir)
      end)
    end
  end

  describe "S-12 three-hop Host.stop witness and retained terminal" do
    test "owner -> helper -> starter -> Run.Supervisor is linked while blocked and while running; no links after teardown",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        startup = begin!(ctx, fs, 8, %{startup: 5_000})
        _ = identity!(startup)
        :ok = seam(:permit, [startup])
        assert_receive {:blocked_open, writer}, 5_000
        gate(ctx, writer)
        sup = sup_of(writer)
        own(ctx, [writer, sup])

        chain = fn ->
          [
            startup.helper in links(self()),
            startup.starter in links(startup.helper),
            sup in links(startup.starter),
            sup in links(self())
          ]
        end

        assert [true, true, true, false] == chain.(), "while blocked"
        send(writer, :unblock)
        ref = startup.ref
        assert_receive {:startup_started, ^ref, {:ok, ^sup, _}} = c, 5_000
        {:ok, ^sup, _facts} = seam(:accept, [startup, c])
        assert [true, true, true, false] == chain.(), "while running"
        assert :ok == seam(:teardown, [startup, %{stop: 3_000, join: 1_000}])
        assert all_down?([startup.helper, startup.starter, sup], @window)
        assert [false, false, false, false] == chain.(), "after teardown"
      end)
    end

    test "a retained mounted owner holds no link to any owned identity and stop answers :ok (retained)", ctx do
      row(ctx, fn ->
        n = System.unique_integer([:positive])
        arb = :"csb_seam_arb_#{n}"
        hsup = :"csb_seam_hsup_#{n}"
        mon = :"csb_seam_mon_#{n}"

        root =
          start_supervised!(%{
            id: :"csb_seam_root_#{n}",
            start:
              {Supervisor, :start_link,
               [
                 [
                   {Ownership, name: arb},
                   {Host.Supervisor, name: hsup, child_shutdown_ms: 5_000},
                   {Host.Monitor, name: mon, host_supervisor: hsup, ownership: arb, census_timeout: 1_000}
                 ],
                 [strategy: :rest_for_one]
               ]},
            type: :supervisor
          })

        _ = root
        config = config!(ctx.dir, FaultFs.new(), 9)

        {:ok, command} =
          Commands.build(
            @operator,
            "start",
            %{"spec_hash" => config.opts[:spec_hash], "plan_hash" => config.opts[:plan_hash]},
            run_id: "run_csb_seam_9",
            command_id: "cmd_csb_seam_000009",
            now: @now
          )

        {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

        ctxk =
          opts_fun.()
          |> Keyword.drop(@owned)
          |> Keyword.merge(
            run_dir: ctx.dir,
            spec: H.spec(scenario),
            plan: H.plan(scenario),
            spec_hash: config.opts[:spec_hash],
            plan_hash: config.opts[:plan_hash],
            supervisor_instance: "sup_csb_seam",
            trace: self(),
            barrier: fn _, _ -> :ok end,
            fs: FaultFs.new()
          )

        {:ok, handle} =
          Host.mount(command, ctxk,
            host: %{supervisor: hsup, monitor: mon, ownership: arb},
            budgets: @mount_budgets,
            retention_ms: 30_000
          )

        own(ctx, handle.owner)
        assert {:ok, _} = Host.await(handle, 30_000)

        assert links(handle.owner) -- [Process.whereis(hsup)] == [],
               "a retained owner must hold no link to helper/starter/supervisor"

        assert :ok == Host.stop(handle, 5_000)
      end)
    end
  end

  # ---- bounded OWNER DRIVER: a test-only process implementing the declared owner protocol of contract section 1 ----
  # birth -> begin -> arm ONE timer from startup.deadline -> permit on identity (via the seam) -> accept on completion
  # (via the seam) -> abort(startup, :deadline) on expiry. It reports every step to the test; it never re-arms.
  defp driver(test, config, budgets) do
    spawn(fn ->
      Process.flag(:trap_exit, true)
      birth = System.monotonic_time(:millisecond)
      {:ok, startup} = seam(:begin, [config, Map.put(budgets, :birth, birth)])
      send(test, {:driver_begun, self(), startup, birth})
      Process.send_after(self(), {:expire, startup.ref}, max(startup.deadline - System.monotonic_time(:millisecond), 0))
      driver_loop(test, startup, birth)
    end)
  end

  defp driver_loop(test, startup, birth) do
    ref = startup.ref
    now = fn -> System.monotonic_time(:millisecond) - birth end

    receive do
      {:startup_identity, ^ref, _starter} ->
        send(test, {:driver_permit, ref, seam(:permit, [startup]), now.()})
        driver_loop(test, startup, birth)

      {:startup_started, ^ref, _} = completion ->
        send(test, {:driver_accepted, ref, seam(:accept, [startup, completion]), now.()})
        driver_loop(test, startup, birth)

      {:expire, ^ref} ->
        # the absolute deadline: the driver decides by the clock, not by the timer message alone
        if System.monotonic_time(:millisecond) >= startup.deadline do
          send(test, {:driver_aborted, ref, seam(:abort, [startup, :deadline]), now.()})
        else
          driver_loop(test, startup, birth)
        end

      {:startup_aborted, ^ref, report} ->
        send(test, {:driver_helper_report, ref, report, now.()})
        driver_loop(test, startup, birth)

      {:teardown, from} ->
        send(from, {:torn, seam(:teardown, [startup, %{stop: 3_000, join: 1_000}])})

      _ ->
        driver_loop(test, startup, birth)
    end
  end

  describe "S-10 retry under ONE owner clock (owner-driven)" do
    test "the first attempt consumes 150 ms of the clock, the retry's open blocks, and the owner aborts at the ABSOLUTE deadline",
         ctx do
      row(ctx, fn ->
        # seed: a run COMPLETED through the real executor (worker acknowledged, awaited, closed)
        {ctxk, command} = executor_ctx!(ctx.dir, FaultFs.new(), 5)
        assert {:ok, %{}} = Executor.execute(command, ctxk)
        assert 32 == length(String.split(File.read!(Path.join(ctx.dir, "events.jsonl")), "\n", trim: true))
        drain_trace()

        # the second start (same command id): attempt 1 spends 150 ms inside its [:exclusive] create before eexist;
        # attempt 2 (:existing / :retry_only) blocks in its append-open
        fs = FaultFs.new()
        test = self()

        :ok =
          FaultFs.inject(
            fs,
            :open,
            fn args -> match?(["events.jsonl", m] when is_list(m), args) and :exclusive in Enum.at(args, 1) end,
            {:hook,
             fn ->
               :timer.sleep(150)
               true
             end}
          )

        :ok =
          FaultFs.inject(
            fs,
            :open,
            fn args -> match?(["events.jsonl", m] when is_list(m), args) and :append in Enum.at(args, 1) end,
            {:hook,
             fn ->
               send(test, {:blocked_open, self()})
               receive(do: (:unblock -> true), after: (60_000 -> true))
             end}
          )

        d = driver(self(), config!(ctx.dir, fs, 5), %{startup: @short, ack: 500})
        own(ctx, d)
        assert_receive {:driver_begun, ^d, startup, _birth}, 2_000
        own(ctx, [startup.helper, startup.starter])
        ref = startup.ref
        assert_receive {:driver_permit, ^ref, :ok, _}, 2_000
        assert_receive {:blocked_open, writer}, 5_000
        gate(ctx, writer)
        own(ctx, [writer, sup_of(writer)])
        opens = fs |> FaultFs.trace() |> Enum.filter(&match?({:open, "events.jsonl", _}, &1))
        assert [{:open, "events.jsonl", [:exclusive]}, {:open, "events.jsonl", [:append]}] == opens

        # ONE absolute clock: abort in [300, 300 + slack] from the driver's birth;
        # a reset-at-retry mutant would abort >= 450
        assert_receive {:driver_aborted, ^ref, report, at}, @short + @window

        assert at >= @short and at < @short + 150,
               "abort at #{at} ms: expected the absolute deadline (#{@short}), not a clock reset at the retry (>= #{@short + 150})"

        assert match?(%{why: :deadline, ownership: :reclaimable}, report)
        assert all_down?([writer, startup.starter, startup.helper], @window)
      end)
    end
  end

  describe "S-5c/S-5d timely completions (controls for the late-case rows)" do
    test "S-5c a success accepted BEFORE the deadline is {:ok, sup, facts} and the run is torn down normally", ctx do
      row(ctx, fn ->
        d = driver(self(), config!(ctx.dir, FaultFs.new(), 10), %{startup: 5_000, ack: 500})
        own(ctx, d)
        assert_receive {:driver_begun, ^d, startup, _}, 2_000
        own(ctx, [startup.helper, startup.starter])
        ref = startup.ref
        assert_receive {:driver_permit, ^ref, :ok, _}, 2_000
        assert_receive {:driver_accepted, ^ref, {:ok, sup, facts}, at}, 5_000
        own(ctx, [sup | Map.values(facts)])
        assert at < 5_000 and is_pid(facts.writer)
        send(d, {:teardown, self()})
        assert_receive {:torn, :ok}, 8_000
        assert all_down?([sup, startup.helper, startup.starter], @window)
      end)
    end

    test "S-5d a timely INNER error keeps its own class: a blocked private-arbiter acquire (acquire_timeout 200 ms) accepted before the deadline -> ownership_unavailable",
         ctx do
      row(ctx, fn ->
        n = System.unique_integer([:positive])
        arb = :"csb_seam_arb_#{n}"
        {:ok, arb_pid} = Ownership.start_link(name: arb)
        Process.unlink(arb_pid)
        own(ctx, arb_pid)
        fs = FaultFs.new()
        test = self()

        :ok =
          FaultFs.inject(
            fs,
            :open,
            1,
            {:hook,
             fn ->
               send(test, {:blocked_acquire, self()})
               receive(do: (:unblock -> true), after: (60_000 -> true))
             end}
          )

        config = config!(ctx.dir, fs, 11)
        config = %{config | opts: Keyword.put(config.opts, :ownership, server: arb, acquire_timeout: 200)}
        d = driver(self(), config, %{startup: 5_000, ack: 500})
        own(ctx, d)
        assert_receive {:driver_begun, ^d, startup, _}, 2_000
        own(ctx, [startup.helper, startup.starter])
        ref = startup.ref
        assert_receive {:driver_permit, ^ref, :ok, _}, 2_000
        assert_receive {:blocked_acquire, blocked}, 5_000
        gate(ctx, blocked)
        assert_receive {:driver_accepted, ^ref, {:error, %{clause: "ownership_unavailable"}}, at}, 5_000
        assert at < 5_000
        assert all_down?([startup.starter, startup.helper], @window)
      end)
    end
  end

  defp drain_trace do
    receive do
      {:run_child_started, _, _, _} -> drain_trace()
      {:run_executor_started, _, _} -> drain_trace()
    after
      0 -> :ok
    end
  end

  # an executor context + command for the real foreground path (the S-10 seed)
  defp executor_ctx!(dir, fs, n) do
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
        supervisor_instance: "sup_csb_seam",
        trace: self(),
        barrier: fn _, _ -> :ok end,
        fs: fs
      )

    {:ok, c} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_csb_seam_#{n}",
        command_id: "cmd_csb_seam_#{String.pad_leading("#{n}", 6, "0")}",
        now: @now
      )

    {ctx, c}
  end

  test "budgets: Owner.default_budgets/0 and Startup.default_budgets/0 carry :startup 45_000 and :ack 5_000" do
    assert %{startup: 45_000, ack: 5_000} = Map.take(Owner.default_budgets(), [:startup, :ack])
    assert %{startup: 45_000, ack: 5_000} = Map.take(seam(:default_budgets, []), [:startup, :ack])
  end
end
