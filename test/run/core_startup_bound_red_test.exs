defmodule AiOrchestrator.Run.CoreStartupBoundRedTest do
  @moduledoc """
  Expected-RED rows for docs/contracts/core-startup-bound.org (section 9 is the coverage matrix), asserted against
  the REAL current owners and Writer. The one prerequisite gap (the startup seam) is reported exactly once by P-1;
  rows that cannot execute without it live in core_startup_bound_seam_red_test.exs and are skipped, not failed.

  Every row runs under `AiOrchestrator.Test.StartupCleanup.row/2` (shared with the seam file): the body's outcome is
  CAUGHT (rescue and catch of exits/throws), `cleanup!/1` then releases every tracked filesystem gate, kills and joins
  every owned identity with recorded survivors, takes a SCOPED census of run-tree processes born since the row's
  baseline (helper/starter/server/work/worker included) and removes the row's directories only when nothing survived;
  the original failure is re-raised annotated with the cleanup result. A body that cannot be caught (its process is
  killed or times out) is covered by the idempotent on_exit fallback, which runs the same `cleanup!/1`. CO-1 (failing
  body), CO-2 (body process killed externally, fallback by the surviving owner) and CO-2a (caught exit) demonstrate it.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Executor
  alias AiOrchestrator.Run.Executor.Owner
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.StartupCleanup, as: Cleanup

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @budgets %{close: 3_000, stop: 3_000, join: 1_000, handoff: 2_000, helper_join: 500}
  # the contract's teardown bound for a blocked-startup abort (joins + stop); rows observe a window of this size
  @window 3_000
  @short_startup 300

  # ---- per-row failure-safe ownership: the shared cleanup owner (test/support/startup_cleanup.ex) ----
  setup do
    dir = Path.join(System.tmp_dir!(), "csb_#{System.unique_integer([:positive])}")
    ctx = Cleanup.setup_row(dir)
    %{dir: dir, tracker: ctx.tracker, baseline: ctx.baseline, cleanup: ctx}
  end

  defp own(%{cleanup: c}, pids), do: Cleanup.own(c, pids)
  defp gate(%{cleanup: c}, pid), do: Cleanup.gate(c, pid)
  defp row(%{cleanup: c}, body), do: Cleanup.row(c, body)
  defp initial_call(pid), do: Cleanup.initial_call(pid)
  defp census_pids, do: Cleanup.census_pids()

  defp links(pid) do
    case Process.info(pid, :links) do
      {:links, l} -> Enum.filter(l, &is_pid/1)
      nil -> []
    end
  end

  # verified chain traversal from a root process: the first Run.Supervisor reachable through links within `hops`,
  # its Writer child, and the linked parent of that supervisor (labelled by role, never assumed to be the owner)
  defp chain_from(root, hops \\ 3) do
    sup = find_linked(root, &match?({:supervisor, Run.Supervisor, _}, initial_call(&1)), hops, MapSet.new([root]))
    writer = sup && Enum.find(links(sup), &match?({Writer, :init, _}, initial_call(&1)))
    parent = sup && Enum.find(links(sup), &(&1 != writer and not match?({Writer, :init, _}, initial_call(&1))))
    %{supervisor: sup, writer: writer, parent: parent, parent_call: parent && initial_call(parent)}
  end

  defp find_linked(_pid, _pred, 0, _seen), do: nil

  defp find_linked(pid, pred, hops, seen) do
    next = Enum.reject(links(pid), &MapSet.member?(seen, &1))

    case Enum.find(next, pred) do
      nil -> Enum.find_value(next, fn n -> find_linked(n, pred, hops - 1, MapSet.put(seen, n)) end)
      found -> found
    end
  end

  defp all_down?(pids, window) do
    mons = for {_, p} <- pids, is_pid(p), do: {p, Process.monitor(p)}
    t0 = System.monotonic_time(:millisecond)

    Enum.all?(mons, fn {p, m} ->
      receive do
        {:DOWN, ^m, :process, ^p, _} -> true
      after
        max(window - (System.monotonic_time(:millisecond) - t0), 0) -> false
      end
    end)
  end

  defp alive?(pids), do: Map.new(pids, fn {k, p} -> {k, is_pid(p) and Process.alive?(p)} end)

  defp status(dir, opts \\ []) do
    case Ownership.status(dir, Keyword.put_new(opts, :acquire_timeout, 1_000)) do
      {:ok, reg} -> {:ok, reg.state}
      other -> other
    end
  end

  defp disk(dir) do
    case RunLock.owner(SystemFs.new(), dir) do
      :none -> :none
      {:ok, _} -> :held
      other -> other
    end
  end

  defp lines(dir) do
    case File.read(Path.join(dir, "events.jsonl")) do
      {:ok, b} -> length(String.split(b, "\n", trim: true))
      _ -> :absent
    end
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  defp ctx(dir, fs, extra) do
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
      supervisor_instance: "sup_csb_0001",
      trace: self(),
      barrier: fn _, _ -> :ok end,
      fs: fs
    )
    |> Keyword.merge(extra)
  end

  defp start_command(ctx, n) do
    {:ok, c} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_csb_#{n}",
        command_id: "cmd_csb_start_#{String.pad_leading("#{n}", 6, "0")}",
        now: @now
      )

    c
  end

  defp cancel_command(n, m) do
    {:ok, c} =
      Commands.build(@operator, "cancel", %{"reason" => "operator_cancel"},
        run_id: "run_csb_#{n}",
        command_id: "cmd_csb_cancel_#{String.pad_leading("#{m}", 5, "0")}",
        now: @now
      )

    c
  end

  # a FaultFs hook that blocks the Writer inside an operation until :unblock; the blocked pid is reported to the test
  defp hook(test, tag) do
    {:hook,
     fn ->
       send(test, {tag, self()})
       receive(do: (:unblock -> true), after: (60_000 -> true))
     end}
  end

  defp blocking_open_fs(test) do
    fs = FaultFs.new()
    matcher = fn args -> match?(["events.jsonl", modes] when is_list(modes), args) and :append in Enum.at(args, 1) end
    :ok = FaultFs.inject(fs, :open, matcher, hook(test, :blocked_open))
    fs
  end

  # waits for the gate report, tracks the gated pid and the identities reachable from it
  defp await_gate!(ctx, tag) do
    pid =
      receive do
        {^tag, p} -> p
      after
        10_000 -> flunk("gate #{tag} never reached")
      end

    gate(ctx, pid)
    own(ctx, pid)
    pid
  end

  defp caller(test, command, ctx),
    do: spawn(fn -> send(test, {:caller_result, self(), Executor.execute(command, ctx)}) end)

  defp isolated_host(child_shutdown_ms) do
    n = System.unique_integer([:positive])
    arb = :"csb_arb_#{n}"
    hsup = :"csb_hsup_#{n}"
    mon = :"csb_mon_#{n}"

    children = [
      {Ownership, name: arb},
      {Host.Supervisor, name: hsup, child_shutdown_ms: child_shutdown_ms},
      {Host.Monitor, name: mon, host_supervisor: hsup, ownership: arb, census_timeout: 1_000}
    ]

    root =
      start_supervised!(%{
        id: :"csb_root_#{n}",
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    %{root: root, arb: arb, hsup: hsup, mon: mon, host: %{supervisor: hsup, monitor: mon, ownership: arb}}
  end

  defp drain_trace do
    receive do
      {:run_child_started, _, _, _} -> drain_trace()
      {:run_executor_started, _, _} -> drain_trace()
    after
      0 -> :ok
    end
  end

  describe "prerequisite (reported once)" do
    test "P-1 the startup seam and the owner budgets exist" do
      seam = Module.concat(["AiOrchestrator", "Run", "Executor", "Startup"])

      missing =
        for {f, a} <- [begin: 2, permit: 1, accept: 2, abort: 2, reap: 1, teardown: 2, default_budgets: 0],
            not (Code.ensure_loaded?(seam) and function_exported?(seam, f, a)),
            do: "#{inspect(seam)}.#{f}/#{a}"

      budgets = Owner.default_budgets()
      gaps = missing ++ for(key <- [:startup, :ack], not is_map_key(budgets, key), do: "Owner.default_budgets/0 #{key}")

      assert Enum.empty?(gaps),
             "prerequisite gap (reported once; seam rows are skipped while it holds): #{inspect(gaps)}"
    end
  end

  describe "expected RED (real owners and Writer; the assertion names the unmet behaviour)" do
    test "S-1 foreground blocked open + caller death: subtree torn down with the gate held; caller-gone trace carries the diagnostic",
         ctx do
      row(ctx, fn ->
        dir = ctx.dir

        command = start_command(ctx(dir, blocking_open_fs(self()), []), 1)
        c = caller(self(), command, ctx(dir, blocking_open_fs(self()), []))

        own(ctx, c)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])

        assert is_pid(chain.supervisor) and is_pid(chain.parent),
               "setup: supervisor/parent not reachable from the blocked Writer"

        cmon = Process.monitor(c)
        Process.exit(c, :kill)
        assert_receive {:DOWN, ^cmon, :process, ^c, _}, 2_000

        down = all_down?([writer: writer, supervisor: chain.supervisor, parent: chain.parent], @window)

        trace =
          receive do
            {:run_startup_aborted, _owner, %{ownership: o}} -> {:trace, o}
          after
            0 -> :no_trace
          end

        drain_trace()

        assert down and trace == {:trace, :reclaimable},
               "S-1 unmet: after the caller's death, with the append-open still gated, the supervisor's parent (#{inspect(chain.parent_call) <> ", the plain-spawned owner"}), " <>
                 "Run.Supervisor and the Writer were #{inspect(alive?(writer: writer, supervisor: chain.supervisor, parent: chain.parent))} " <>
                 "after #{@window} ms and no {:run_startup_aborted, owner, %{ownership: :reclaimable}} trace was emitted (#{inspect(trace)})"
      end)
    end

    test "S-2/S-20 foreground blocked open, caller alive, startup_budgets 300 ms: exact timeout result, elapsed bound, joins with the gate held",
         ctx do
      row(ctx, fn ->
        dir = ctx.dir
        fs = blocking_open_fs(self())
        command = start_command(ctx(dir, fs, []), 2)
        c = caller(self(), command, ctx(dir, fs, startup_budgets: %{startup: @short_startup}))
        own(ctx, c)
        t0 = System.monotonic_time(:millisecond)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])

        result =
          receive do
            {:caller_result, ^c, r} -> {r, System.monotonic_time(:millisecond) - t0}
          after
            @short_startup + @window -> :no_result
          end

        joined = all_down?([writer: writer, supervisor: chain.supervisor, parent: chain.parent], 500)
        drain_trace()

        assert match?(
                 {{:error, %{clause: "run_startup_timeout", why: :deadline, ownership: :reclaimable}}, ms}
                 when ms >= @short_startup and ms <= @short_startup + @window,
                 result
               ) and joined,
               "S-2/S-20 unmet: with startup_budgets %{startup: #{@short_startup}} the caller received #{inspect(result)} " <>
                 "(expected {:error, %{clause: \"run_startup_timeout\", why: :deadline, ownership: :reclaimable}} within " <>
                 "[#{@short_startup}, #{@short_startup + @window}] ms) and joins=#{joined} with the gate held; today no startup " <>
                 "budget seam exists (P-1) and the owner blocks inside Run.Supervisor.start_link"
      end)
    end

    test "S-4a birth abort: a Writer with birth: announces before acquire and stops on abort with nothing acquired",
         ctx do
      row(ctx, fn -> birth_case(ctx, :abort) end)
    end

    test "S-4b birth silence: a silent reaper stops the Writer at the ack budget with nothing acquired", ctx do
      row(ctx, fn -> birth_case(ctx, :silent) end)
    end

    test "S-4c birth ack: an acknowledged Writer acquires and reaches its open", ctx do
      row(ctx, fn -> birth_case(ctx, :ack) end)
    end

    test "S-6/S-12 mounted stop during a blocked startup: {:ok, :stopped} within child_shutdown, tree down with the gate held",
         ctx do
      row(ctx, fn ->
        h = isolated_host(2_000)
        fs = blocking_open_fs(self())

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 3), ctx(ctx.dir, fs, []), host: h.host, budgets: @budgets)

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])
        stop = Host.stop(handle, 4_000)
        gone = all_down?([supervisor: chain.supervisor, writer: writer], 1_000)
        drain_trace()

        assert stop == {:ok, :stopped} and gone,
               "S-6/S-12 unmet: Host.stop on an owner blocked in :starting answered #{inspect(stop)} and left " <>
                 "#{inspect(alive?(supervisor: chain.supervisor, writer: writer))} with the gate held"
      end)
    end

    test "S-7 host root shutdown during a blocked startup: no run tree survives the root's stop", ctx do
      row(ctx, fn ->
        h = isolated_host(1_000)
        fs = blocking_open_fs(self())

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 4), ctx(ctx.dir, fs, []), host: h.host, budgets: @budgets)

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])
        :ok = Supervisor.stop(h.root, :shutdown, 15_000)
        survivors = alive?(supervisor: chain.supervisor, writer: writer)
        drain_trace()

        assert Enum.all?(Map.values(survivors), &(&1 == false)),
               "S-7 unmet: after the host root stopped, #{inspect(survivors)} outlived the root and its arbiter"
      end)
    end

    test "S-9a mounted :starting is responsive: inspect/2 answers while the Writer is blocked", ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = blocking_open_fs(self())

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 5), ctx(ctx.dir, fs, []), host: h.host, budgets: @budgets)

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        own(ctx, chain_from(writer).supervisor)

        phase =
          try do
            RunOwner.inspect(handle.owner, 500).phase
          catch
            :exit, _ -> :unresponsive
          end

        assert phase == :starting, "S-9a unmet: RunOwner.inspect/2 answered #{inspect(phase)} during a blocked startup"
      end)
    end

    test "S-9b mounted expiry (budgets startup 300 ms): retained {:error, run_startup_timeout}; await answers it; stop -> :ok",
         ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = blocking_open_fs(self())
        budgets = Map.merge(@budgets, %{startup: @short_startup, ack: 500})

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 6), ctx(ctx.dir, fs, []), host: h.host, budgets: budgets)

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])
        t0 = System.monotonic_time(:millisecond)

        awaited =
          try do
            Host.await(handle, @short_startup + @window)
          catch
            :exit, _ -> :await_unanswered
          end

        elapsed = System.monotonic_time(:millisecond) - t0
        stop = if Process.alive?(handle.owner), do: Host.stop(handle, 2_000), else: :owner_gone
        gone = all_down?([supervisor: chain.supervisor, writer: writer], 500)
        drain_trace()

        assert match?({:error, %{clause: "run_startup_timeout", why: :deadline}}, awaited) and
                 elapsed <= @short_startup + @window and stop == :ok and gone,
               "S-9b unmet: with budgets.startup #{@short_startup} the mounted owner answered await #{inspect(awaited)} after #{elapsed} ms, " <>
                 "stop #{inspect(stop)}, tree #{inspect(alive?(supervisor: chain.supervisor, writer: writer))}; expected a retained " <>
                 "{:error, run_startup_timeout} with stop :ok (retained) and the tree reaped with the gate held"
      end)
    end

    test "S-15a owner (reaper) dies BEFORE acknowledging the Writer's birth: the Writer stops with nothing acquired and its DOWN is collected",
         ctx do
      row(ctx, fn ->
        dir = ctx.dir
        ref = make_ref()
        fs = blocking_open_fs(self())
        test = self()
        # the reaper the Writer announces to: it RELAYS the announcement to the test (a bounded causal witness) and
        # dies before any acknowledgment
        reaper =
          spawn(fn ->
            # relays a birth if one arrives; dies on request in either phase (today no birth arrives at all)
            receive do
              {:run_writer_born, _ref, _writer} = born ->
                send(test, {:relayed, born})
                receive(do: (:die -> exit(:normal)))

              :die ->
                exit(:normal)
            end
          end)

        own(ctx, reaper)

        starter =
          spawn(fn ->
            Process.flag(:trap_exit, true)

            send(
              test,
              {:writer_start,
               Writer.start_link(dir,
                 fs: fs,
                 create: true,
                 lock: [supervisor_instance: "sup_csb_0001"],
                 birth: {reaper, ref},
                 ack: 2_000
               )}
            )

            receive(do: (:release -> :ok))
          end)

        own(ctx, starter)

        # the announced Writer identity is retained, tracked and monitored BEFORE the reaper dies
        announced =
          receive do
            {:relayed, {:run_writer_born, ^ref, writer}} when is_pid(writer) ->
              own(ctx, writer)
              {:ok, writer, Process.monitor(writer)}
          after
            1_500 -> :no_announcement
          end

        rmon = Process.monitor(reaper)
        send(reaper, :die)
        assert_receive {:DOWN, ^rmon, :process, ^reaper, _}, 1_000

        started =
          receive do
            {:writer_start, r} -> r
          after
            2_500 -> :start_unanswered
          end

        # the Writer's OWN DOWN is required (bounded) before the starter is released or cleanup runs
        writer_down =
          case announced do
            {:ok, w, m} ->
              receive do
                {:DOWN, ^m, :process, ^w, reason} -> {:down, reason}
              after
                2_500 -> {:alive, Process.alive?(w)}
              end

            _ ->
              :unobservable
          end

        reached_open =
          receive do
            {:blocked_open, w} ->
              gate(ctx, w)
              own(ctx, w)
              true
          after
            200 -> false
          end

        after_case = {status(dir), disk(dir)}
        send(starter, :release)

        assert match?({:ok, _, _}, announced) and match?({:down, _}, writer_down) and
                 match?({:error, %{clause: "writer_birth_aborted"}}, started) and not reached_open and
                 after_case == {:none, :none},
               "S-15a unmet: announcement #{inspect(announced)}, Writer DOWN #{inspect(writer_down)}, start result " <>
                 "#{inspect(started)}, append-open reached #{reached_open}, after-case #{inspect(after_case)}; the contract " <>
                 "requires a Writer whose reaper dies before the ack to stop (writer_birth_aborted) with no registration and " <>
                 "no lock, its own DOWN observed; today Writer.init ignores birth: and acquires"
      end)
    end

    test "S-15b mounted owner killed after acquire while the open blocks: Writer and supervisor reaped with the gate held",
         ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = blocking_open_fs(self())

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 7), ctx(ctx.dir, fs, []), host: h.host, budgets: @budgets)

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])

        assert {:ok, :live} == status(ctx.dir, server: h.arb),
               "setup: the Writer must hold a live registration (after acquire)"

        Process.exit(handle.owner, :kill)
        gone = all_down?([supervisor: chain.supervisor, writer: writer], @window)
        drain_trace()

        assert gone,
               "S-15b unmet: after the owner's death the acquired Writer and Run.Supervisor were " <>
                 "#{inspect(alive?(supervisor: chain.supervisor, writer: writer))} after #{@window} ms with the gate held"
      end)
    end

    test "S-15c foreground owner killed after startup with the Writer blocked in an append: tree reaped with the gate held",
         ctx do
      row(ctx, fn ->
        fs = FaultFs.new()
        :ok = FaultFs.inject(fs, :write, 12, hook(self(), :blocked_write))
        c = caller(self(), start_command(ctx(ctx.dir, fs, []), 8), ctx(ctx.dir, fs, []))
        own(ctx, c)
        assert_receive {:run_executor_started, owner, sup}, 10_000
        own(ctx, [owner, sup])
        writer = await_gate!(ctx, :blocked_write)
        Process.exit(owner, :kill)
        assert_receive {:caller_result, ^c, {:error, %{clause: "run_executor_down"}}}, 5_000
        gone = all_down?([supervisor: sup, writer: writer], @window)
        drain_trace()

        assert gone,
               "S-15c unmet: after the owner's death the responsive supervisor waited on the Writer blocked in its append; " <>
                 "#{inspect(alive?(supervisor: sup, writer: writer))} after #{@window} ms with the gate held (the contract " <>
                 "requires the reaper to kill the Writer identity first)"
      end)
    end

    test "S-16 mounted owner killed while the Writer is blocked inside the arbiter acquire: roles verified, descendants reaped, diagnostic :unknown",
         ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = FaultFs.new()
        :ok = FaultFs.inject(fs, :open, 1, hook(self(), :blocked_acquire))

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 9), ctx(ctx.dir, fs, []), host: h.host, budgets: @budgets)

        own(ctx, handle.owner)
        blocked = await_gate!(ctx, :blocked_acquire)
        assert blocked == Process.whereis(h.arb), "setup: the blocked pid must be the private arbiter (acquire leg)"
        :timer.sleep(100)
        chain = chain_from(handle.owner)
        own(ctx, [chain.supervisor, chain.writer])

        assert is_pid(chain.supervisor) and is_pid(chain.writer) and
                 match?({Writer, :init, _}, initial_call(chain.writer)),
               "setup: Run.Supervisor and its Writer must be reachable from the owner by chain traversal before the kill"

        phase =
          try do
            RunOwner.inspect(handle.owner, 300).phase
          catch
            :exit, _ -> :unresponsive
          end

        Process.exit(handle.owner, :kill)
        gone = all_down?([supervisor: chain.supervisor, writer: chain.writer], @window)

        diag =
          receive do
            {:run_startup_aborted, _o, %{ownership: o}} -> o
          after
            0 -> :no_report
          end

        drain_trace()

        assert phase == :starting and gone and diag == :unknown,
               "S-16 unmet: owner phase #{inspect(phase)}; after its death #{inspect(alive?(supervisor: chain.supervisor, writer: chain.writer))}; " <>
                 "ownership diagnostic #{inspect(diag)} (expected :unknown while the arbiter is unavailable)"
      end)
    end

    test "S-19 caller death while discovery is blocked: the queued which_children is witnessed, then the tree is reaped",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        c = caller(self(), start_command(ctx(ctx.dir, fs, []), 10), ctx(ctx.dir, fs, []))
        own(ctx, c)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])
        # let the start complete but hold the supervisor before discovery can be answered
        true = :erlang.suspend_process(chain.parent)
        send(writer, :unblock)
        :timer.sleep(150)
        true = :erlang.suspend_process(chain.supervisor)
        true = :erlang.resume_process(chain.parent)

        witnessed =
          Enum.find_value(1..40, fn _ ->
            cf =
              case Process.info(chain.parent, :current_function) do
                {:current_function, f} -> f
                _ -> :dead
              end

            q =
              case Process.info(chain.supervisor, :messages) do
                {:messages, m} -> m
                _ -> []
              end

            queued = Enum.any?(q, &match?({:"$gen_call", _, :which_children}, &1))

            if queued or cf == {:gen, :do_call, 4},
              do: %{parent_current_function: cf, which_children_queued: queued},
              else:
                (
                  :timer.sleep(25)
                  nil
                )
          end)

        Process.exit(c, :kill)
        gone = all_down?([supervisor: chain.supervisor, writer: writer, parent: chain.parent], @window)

        _ =
          try do
            :erlang.resume_process(chain.supervisor)
          catch
            :error, _ -> :dead
          end

        drain_trace()

        assert is_map(witnessed) and witnessed.which_children_queued and gone,
               "S-19 unmet: discovery witness #{inspect(witnessed)}; after the caller's death " <>
                 "#{inspect(alive?(supervisor: chain.supervisor, writer: writer, parent: chain.parent))}"
      end)
    end

    test "S-18b survivor rejection: a join seam that never observes a DOWN yields run_executor_teardown_incomplete on a blocked-startup stop",
         ctx do
      row(ctx, fn ->
        h = isolated_host(2_000)
        fs = blocking_open_fs(self())
        never = fn _pid, _mon, _timeout -> false end

        {:ok, handle} =
          Host.mount(start_command(ctx(ctx.dir, fs, []), 11), ctx(ctx.dir, fs, join: never),
            host: h.host,
            budgets: @budgets
          )

        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        chain = chain_from(writer)
        own(ctx, [chain.supervisor, chain.parent])
        stop = Host.stop(handle, 4_000)
        drain_trace()

        assert match?({:error, %{clause: "run_executor_teardown_incomplete", survivors: n}} when n > 0, stop),
               "S-18b unmet: with a join seam that never observes a DOWN the stop answered #{inspect(stop)}; the contract " <>
                 "reports survivors as run_executor_teardown_incomplete, never success (today the blocked :starting owner is unresponsive)"
      end)
    end
  end

  # S-4: the real Writer under a test-owned trapping starter; the test is the reaper named in birth:
  defp birth_case(ctx, mode) do
    dir = ctx.dir
    ref = make_ref()
    fs = blocking_open_fs(self())
    test = self()

    starter =
      spawn(fn ->
        Process.flag(:trap_exit, true)

        send(
          test,
          {:writer_start,
           Writer.start_link(dir,
             fs: fs,
             create: true,
             lock: [supervisor_instance: "sup_csb_0001"],
             birth: {test, ref},
             ack: 400
           )}
        )

        receive(do: (:release -> :ok))
      end)

    own(ctx, starter)

    born =
      receive do
        {:run_writer_born, ^ref, writer} ->
          own(ctx, writer)
          {:ok, writer}
      after
        1_500 -> :no_announcement
      end

    registration_before_ack = status(dir)

    case {mode, born} do
      {:abort, {:ok, w}} -> send(w, {:run_writer_abort, ref})
      {:ack, {:ok, w}} -> send(w, {:run_writer_ack, ref})
      _ -> :ok
    end

    started =
      receive do
        {:writer_start, r} -> r
      after
        2_000 -> :start_unanswered
      end

    reached_open =
      receive do
        {:blocked_open, w} ->
          gate(ctx, w)
          own(ctx, w)
          true
      after
        300 -> false
      end

    after_case = {status(dir), disk(dir)}
    send(starter, :release)

    expected = birth_expected(mode, started, after_case, reached_open)

    assert match?({:ok, _}, born) and registration_before_ack == :none and expected,
           "S-4#{mode} unmet: announcement #{inspect(born)}, registration before any ack #{inspect(registration_before_ack)} " <>
             "(expected :none), start result #{inspect(started)}, append-open reached #{reached_open}, after-case " <>
             "#{inspect(after_case)}; today Writer.init ignores birth:/ack: and acquires immediately"
  end

  defp birth_expected(:abort, started, after_case, reached_open) do
    match?({:error, %{clause: "writer_birth_aborted"}}, started) and after_case == {:none, :none} and not reached_open
  end

  defp birth_expected(:silent, started, after_case, reached_open) do
    match?({:error, %{clause: "writer_birth_unacknowledged"}}, started) and after_case == {:none, :none} and
      not reached_open
  end

  defp birth_expected(:ack, _started, after_case, reached_open),
    do: reached_open and match?({:ok, :live}, elem(after_case, 0))

  describe "known-green controls (pinned facts; must stay green)" do
    test "S-3 raw-tree control (no reaper): a trapping parent that starts Run.Supervisor directly and is killed leaves the blocked Writer alive and live-registered",
         ctx do
      row(ctx, fn ->
        fs = blocking_open_fs(self())
        {:ok, %{config: config}} = Executor.prepare(start_command(ctx(ctx.dir, fs, []), 12), ctx(ctx.dir, fs, []))
        test = self()

        parent =
          spawn(fn ->
            Process.flag(:trap_exit, true)
            send(test, {:raw_start, Run.Supervisor.start_link(config)})
            receive(do: (:release -> :ok))
          end)

        own(ctx, parent)
        writer = await_gate!(ctx, :blocked_open)
        sup = Enum.find(links(writer), &match?({:supervisor, Run.Supervisor, _}, initial_call(&1)))
        own(ctx, sup)
        assert sup in links(parent), "setup: the raw parent must be the supervisor's linked parent"
        assert {:ok, :live} == status(ctx.dir)
        Process.exit(parent, :kill)
        :timer.sleep(300)
        # links alone do not reap a trapping Writer blocked in a filesystem call: this is the fact the reaper exists for
        assert Process.alive?(writer) and Process.alive?(sup)
        assert {:ok, :live} == status(ctx.dir)
        send(writer, :unblock)
        assert all_down?([writer: writer, supervisor: sup], 5_000)
        assert :none == status(ctx.dir)
      end)
    end

    test "S-8 mounted requester death leaves the run to its result", ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = blocking_open_fs(self())
        test = self()
        c = ctx(ctx.dir, fs, [])

        requester =
          spawn(fn ->
            {:ok, handle} = Host.mount(start_command(c, 13), c, host: h.host, budgets: @budgets)
            send(test, {:mounted, handle})
            receive(do: (:never -> :ok))
          end)

        own(ctx, requester)
        assert_receive {:mounted, handle}, 5_000
        own(ctx, handle.owner)
        writer = await_gate!(ctx, :blocked_open)
        own(ctx, chain_from(writer).supervisor)
        Process.exit(requester, :kill)
        send(writer, :unblock)
        assert {:ok, _} = Host.await(handle, 30_000)
        assert :ok == Host.stop(handle, 5_000)
        drain_trace()
      end)
    end

    test "S-11 the inner acquire seam bounds a blocked acquire with ownership_unavailable", ctx do
      row(ctx, fn ->
        h = isolated_host(5_000)
        fs = FaultFs.new()
        :ok = FaultFs.inject(fs, :open, 1, hook(self(), :blocked_acquire))

        c =
          caller(
            self(),
            start_command(ctx(ctx.dir, fs, []), 14),
            ctx(ctx.dir, fs, ownership: [server: h.arb, acquire_timeout: 500])
          )

        own(ctx, c)
        _blocked = await_gate!(ctx, :blocked_acquire)
        assert_receive {:caller_result, ^c, {:error, %{clause: "ownership_unavailable"}}}, 5_000
        drain_trace()
      end)
    end

    test "S-13a empty journal: a Writer killed in its blocked open; a fresh cancel reclaims and answers command_run_mismatch",
         ctx do
      row(ctx, fn -> recovery_case(ctx, :empty) end)
    end

    test "S-13b partial preamble: a Writer killed inside write #4 (first line on disk); a fresh cancel reclaims and answers preamble_violation",
         ctx do
      row(ctx, fn -> recovery_case(ctx, :preamble) end)
    end

    test "S-13c admitted run: a Writer killed inside write #12; a fresh cancel reclaims and cancels", ctx do
      row(ctx, fn -> recovery_case(ctx, :admitted) end)
    end

    test "CO-1 cleanup oracle: a deliberately failing body over a blocked tree is cleaned by the shared wrapper (processes AND directories)",
         ctx do
      inner_dir = ctx.dir <> "_co1_inner"
      inner = Cleanup.setup_row(inner_dir)
      Cleanup.dir(inner, ctx.dir)
      fs = blocking_open_fs(self())
      c = caller(self(), start_command(ctx(inner_dir, fs, []), 18), ctx(inner_dir, fs, []))
      Cleanup.own(inner, c)

      writer =
        receive do
          {:blocked_open, w} -> w
        after
          10_000 -> flunk("gate never reached")
        end

      Cleanup.gate(inner, writer)
      Cleanup.own(inner, writer)
      chain = chain_from(writer)
      Cleanup.own(inner, [chain.supervisor, chain.parent])

      failed =
        try do
          Cleanup.row(inner, fn -> flunk("deliberate inner failure over a blocked tree") end)
          :no_failure
        rescue
          e in ExUnit.AssertionError -> e.message
        end

      assert failed =~ "deliberate inner failure" and failed =~ "cleanup: %{survivors: [], leaked: [], dirs_left: []}",
             "CO-1: the wrapper must re-raise the inner failure annotated with an empty cleanup result, got #{inspect(failed)}"

      assert Enum.all?([writer, chain.supervisor, chain.parent, c], &(not Process.alive?(&1)))
      assert Enum.reject(census_pids(), &MapSet.member?(ctx.baseline, &1)) == []
      refute File.exists?(inner_dir), "CO-1: the inner directory must be removed by the wrapper"
      refute File.exists?(ctx.dir), "CO-1: the outer setup directory tracked by the inner row must be removed too"
      assert :already == Cleanup.cleanup!(inner)
      drain_trace()
    end

    test "CO-2 cleanup oracle: a body process KILLED externally (its after-path cannot run) is cleaned by the surviving owner's idempotent fallback",
         ctx do
      inner_dir = ctx.dir <> "_co2_inner"
      inner = Cleanup.setup_row(inner_dir)
      test = self()

      # the scenario context is built here (the harness registers on_exit, which only the test process may call);
      # a disposable body process then starts a blocked tree under the shared tracker and hangs inside row/2
      fs = blocking_open_fs(test)
      body_ctx = ctx(inner_dir, fs, [])
      command = start_command(body_ctx, 19)

      body =
        spawn(fn ->
          c = caller(test, command, body_ctx)
          Cleanup.own(inner, c)
          Cleanup.row(inner, fn -> receive(do: (:never -> :ok)) end)
        end)

      # the disposable body is owned IMMEDIATELY by the inner tracker and by this row's own tracker: a failed gate
      # wait or precondition below leaves it to the on_exit fallback, never to a :never receive
      Cleanup.own(inner, body)
      own(ctx, body)

      writer =
        receive do
          {:blocked_open, w} -> w
        after
          10_000 -> flunk("gate never reached")
        end

      Cleanup.gate(inner, writer)
      Cleanup.own(inner, writer)
      chain = chain_from(writer)
      Cleanup.own(inner, [chain.supervisor, chain.parent])
      bmon = Process.monitor(body)
      Process.exit(body, :kill)
      assert_receive {:DOWN, ^bmon, :process, ^body, :killed}, 1_000
      # the body's after-path never ran: the tree is still there
      assert Process.alive?(writer) and Process.alive?(chain.supervisor)
      # the surviving owner executes the SAME idempotent fallback the on_exit hook would run
      assert %{survivors: [], leaked: [], dirs_left: []} == Cleanup.cleanup!(inner)
      assert :already == Cleanup.cleanup!(inner)
      assert Enum.all?([writer, chain.supervisor, chain.parent], &(not Process.alive?(&1)))
      assert Enum.reject(census_pids(), &MapSet.member?(ctx.baseline, &1)) == []
      refute File.exists?(inner_dir)
      drain_trace()
    end

    test "CO-2a cleanup oracle: a caught interrupted body (exit) is cleaned by the wrapper's after-path", ctx do
      inner_dir = ctx.dir <> "_co2a_inner"
      inner = Cleanup.setup_row(inner_dir)
      fs = blocking_open_fs(self())
      c = caller(self(), start_command(ctx(inner_dir, fs, []), 20), ctx(inner_dir, fs, []))
      Cleanup.own(inner, c)

      writer =
        receive do
          {:blocked_open, w} -> w
        after
          10_000 -> flunk("gate never reached")
        end

      Cleanup.gate(inner, writer)
      Cleanup.own(inner, writer)
      chain = chain_from(writer)
      Cleanup.own(inner, [chain.supervisor, chain.parent])

      interrupted =
        try do
          Cleanup.row(inner, fn -> exit(:interrupted_body) end)
          :no_exit
        catch
          :exit, reason -> reason
        end

      assert interrupted == :interrupted_body
      assert Enum.all?([writer, chain.supervisor, chain.parent, c], &(not Process.alive?(&1)))
      refute File.exists?(inner_dir)
      drain_trace()
    end
  end

  defp recovery_expected(:empty, recovery, lines_before),
    do: match?({:error, %{clause: "command_run_mismatch"}}, recovery) and lines_before == 0

  defp recovery_expected(:preamble, recovery, lines_before) do
    match?({:error, %{clause: "preamble_violation"}}, recovery) and is_integer(lines_before) and lines_before <= 2
  end

  defp recovery_expected(:admitted, recovery, lines_before),
    do: match?({:ok, %{}}, recovery) and is_integer(lines_before) and lines_before > 2

  defp recovery_case(ctx, mode) do
    dir = ctx.dir

    {fs, tag, n} =
      case mode do
        :empty ->
          {blocking_open_fs(self()), :blocked_open, 15}

        :preamble ->
          fs = FaultFs.new()
          :ok = FaultFs.inject(fs, :write, 4, hook(self(), :blocked_write))
          {fs, :blocked_write, 16}

        :admitted ->
          fs = FaultFs.new()
          :ok = FaultFs.inject(fs, :write, 12, hook(self(), :blocked_write))
          {fs, :blocked_write, 17}
      end

    c = caller(self(), start_command(ctx(dir, fs, []), n), ctx(dir, fs, []))
    own(ctx, c)
    writer = await_gate!(ctx, tag)
    chain = chain_from(writer)
    own(ctx, [chain.supervisor, chain.parent])
    lines_before = lines(dir)
    Process.exit(writer, :kill)
    assert_receive {:caller_result, ^c, {:error, %{clause: clause}}}, 10_000
    assert clause in ["writer_start_failed", "run_server_down"]
    assert {:ok, :down} == status(dir)
    assert :held == disk(dir)
    c2 = caller(self(), cancel_command(n, 1), ctx(dir, FaultFs.new(), []))
    own(ctx, c2)
    assert_receive {:caller_result, ^c2, recovery}, 10_000
    observed = {status(dir), disk(dir)}
    drain_trace()

    expected = recovery_expected(mode, recovery, lines_before)

    assert expected and observed == {:none, :none},
           "S-13#{mode}: lines before the kill #{inspect(lines_before)}, recovery #{inspect(recovery)}, post-reclaim/close observation #{inspect(observed)}"
  end
end
