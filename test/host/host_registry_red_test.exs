defmodule AiOrchestrator.Host.RegistryRedTest do
  @moduledoc """
  RED rows for the in-VM observational host slice (docs/contracts/host-observational-registry.org).

  The interface under test does not exist yet: `AiOrchestrator.Host.Monitor` (an injectable
  observational monitor process), `AiOrchestrator.Host.Executor` (the `Commands.Executor` wrapper
  that composes the owner barrier fail-soft) and `AiOrchestrator.Host.status/2`,
  `AiOrchestrator.Host.lookup_run_id/2`. Every row is guarded by `require_host!/0`, which fails on
  the unchanged source with exactly "AiOrchestrator.Host does not exist"; the row bodies then
  exercise real held commands so that a non-functional stub (no registration, empty lookups,
  a pass-through executor) is rejected by the registration rows, while the noninterference
  rows compare the host-routed command against `Run.Executor` under identical seams.

  Test seams (in-VM only, pinned by the contract): the executor context key `:host_monitor`
  (a monitor pid or name, stripped before delegation) and the status/lookup options
  `monitor:` and `timeout:` (default 1_000 ms). Nothing here changes admission: the monitor is a
  hint, and the clauses the rows compare are the existing authorities.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Executor, as: RunExecutor
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @deadline 15_000

  # ---- late-bound receivers: the interface is absent on the unchanged source ----
  defp host, do: Module.concat(["AiOrchestrator", "Host"])
  defp monitor_mod, do: Module.concat(["AiOrchestrator", "Host", "Monitor"])
  defp host_executor, do: Module.concat(["AiOrchestrator", "Host", "Executor"])

  defp require_host! do
    for mod <- [host(), monitor_mod(), host_executor()] do
      assert Code.ensure_loaded?(mod), "#{inspect(mod)} does not exist"
    end

    assert function_exported?(host(), :status, 2), "AiOrchestrator.Host.status/2 does not exist"
    assert function_exported?(host(), :lookup_run_id, 2), "AiOrchestrator.Host.lookup_run_id/2 does not exist"
    assert function_exported?(host_executor(), :execute, 2), "AiOrchestrator.Host.Executor.execute/2 does not exist"
    assert function_exported?(monitor_mod(), :start_link, 1), "AiOrchestrator.Host.Monitor.start_link/1 does not exist"
  end

  # a test-owned monitor instance (never the Application child), named so a pid or name can be injected
  defp start_monitor! do
    require_host!()
    name = :"host_monitor_#{System.unique_integer([:positive])}"
    pid = start_supervised!({monitor_mod(), [name: name]})
    {pid, name}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "host_red_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp lock_opts(overrides \\ []) do
    Keyword.merge(
      [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
      overrides
    )
  end

  defp seed_journal!(dir), do: File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
  defp open(dir, overrides \\ []), do: Writer.open(dir, Keyword.merge([fs: SystemFs.new(), lock: lock_opts()], overrides))
  defp journal(dir), do: File.read(Path.join(dir, "events.jsonl"))

  # ---- deterministic held barrier: the owner blocks at :subtree_started until the test releases it ----
  defp holding_barrier(test_pid, tag) do
    fn
      :subtree_started, owned ->
        ref = make_ref()
        send(test_pid, {:held, tag, ref, owned, self()})

        receive do
          {:release, ^ref} -> :ok
        after
          @deadline -> exit({:hold_never_released, tag})
        end

      label, owned ->
        send(test_pid, {:barrier, tag, label, owned})
        :ok
    end
  end

  defp await_held!(tag) do
    assert_receive {:held, ^tag, ref, owned, owner}, @deadline
    {ref, owned, owner}
  end

  defp release!(owner, ref), do: send(owner, {:release, ref})

  # ---- one real command with fixed seams; `executor` selects the route under comparison ----
  defp command_ctx(dir, barrier, extra) do
    index = Enum.find_index(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    {_name, _kind, scenario, [], opts_fun} = Enum.at(H.cases(), index)
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
      supervisor_instance: "sup_host_0001",
      trace: self(),
      barrier: barrier
    )
    |> Keyword.merge(extra)
  end

  defp invoke(verb, dir, executor, barrier, extra, command_id \\ "cmd_host_red_00000001") do
    invoke_ctx(verb, command_ctx(dir, barrier, extra), executor, command_id)
  end

  # the context is built in the test process (the harness registers on_exit cleanups), never in a task
  defp invoke_ctx(verb, ctx, executor, command_id) do
    args =
      case verb do
        "start" -> %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]}
        "resume" -> %{"recovery_reason" => "operator_resume"}
        "cancel" -> %{"reason" => "operator_cancel"}
      end

    Commands.invoke(@operator, verb, args,
      run_id: "run_host_0001",
      command_id: command_id,
      now: @now,
      executor: executor,
      executor_opts: ctx
    )
  end

  # runs the command in a task so the test can drive the held barrier; the seams are reset ONCE before
  # a comparison so that identical inputs produce identical journals on both routes
  defp start_async(verb, dir, executor, barrier, extra) do
    ctx = command_ctx(dir, barrier, extra)
    Task.async(fn -> invoke_ctx(verb, ctx, executor, "cmd_host_red_00000001") end)
  end

  defp finish!(task), do: Task.await(task, @deadline)

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  # ---- rows ----

  describe "registration lifecycle (R5, F1)" do
    test "H-1 a held run is registered with its exact identities and generation; the entry is gone after completion",
         %{dir: dir} do
      {monitor, _name} = start_monitor!()
      H.reset_seams()
      task = start_async("start", dir, host_executor(), holding_barrier(self(), :a), host_monitor: monitor)
      {ref, owned, owner} = await_held!(:a)

      assert {:ok, %{registered: true, live: true, generation: generation} = status} =
               host().status(dir, monitor: monitor)

      assert is_integer(generation) and generation >= 1
      assert {:ok, %{state: :live, generation: ^generation}} = Ownership.status(dir)

      for key <- [:owner, :supervisor, :server, :writer] do
        assert status[key] == Map.fetch!(Map.put(owned, :owner, owner), key), "#{key} identity differs"
      end

      release!(owner, ref)
      assert {:ok, %{}} = finish!(task)
      owner_ref = Process.monitor(owner)
      assert_receive {:DOWN, ^owner_ref, :process, ^owner, _}, @deadline
      assert {:ok, %{registered: false} = after_status} = host().status(dir, monitor: monitor)
      refute Map.has_key?(after_status, :live), "absence is unknown, never a not-live claim"
      assert :none = Ownership.status(dir)
    end

    test "H-1b a delayed registration for an owner that already died never persists", %{dir: dir} do
      {monitor, _} = start_monitor!()
      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, :process, ^dead, _}, @deadline
      :ok = monitor_mod().register(monitor, full_record(dir, dead, 1))
      assert {:ok, %{registered: false}} = host().status(dir, monitor: monitor)
    end

    test "H-1d a stale DOWN or unregister from an older owner never erases the replacement for the same directory",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      old = spawn(fn -> receive do: (:stop -> :ok) end)
      new = spawn(fn -> receive do: (:stop -> :ok) end)
      :ok = monitor_mod().register(monitor, full_record(dir, old, 1))
      :ok = monitor_mod().register(monitor, full_record(dir, new, 2))
      :ok = monitor_mod().unregister(monitor, full_record(dir, old, 1))
      send(old, :stop)
      ref = Process.monitor(old)
      assert_receive {:DOWN, ^ref, :process, ^old, _}, @deadline
      # the entry is the replacement's; whether status reports it live depends on Ownership (no writer here)
      assert {:ok, entries} = host().lookup_run_id("run_host_0001", monitor: monitor)
      assert [%{owner: ^new, generation: 2}] = Enum.filter(entries, &(&1.run_dir == Path.expand(dir)))
      send(new, :stop)
    end
  end

  describe "the monitor is a hint, never an admission authority (R4/R7, F1)" do
    test "H-2 duplicate start/resume/cancel against a held live run: Host route == Run.Executor route", %{dir: dir} do
      {monitor, _} = start_monitor!()
      H.reset_seams()
      task = start_async("start", dir, host_executor(), holding_barrier(self(), :live), host_monitor: monitor)
      {ref, _owned, owner} = await_held!(:live)
      assert {:ok, %{registered: true, generation: gen}} = host().status(dir, monitor: monitor)
      {:ok, before} = journal(dir)

      for verb <- ["start", "resume", "cancel"] do
        via_host =
          invoke(verb, dir, host_executor(), fn _, _ -> :ok end, [host_monitor: monitor], "cmd_host_red_dup_" <> verb)

        via_run = invoke(verb, dir, RunExecutor, fn _, _ -> :ok end, [], "cmd_host_red_dup_" <> verb)
        assert match?({:error, %{clause: _}}, via_host), "#{verb} while live must be refused"
        assert via_host == via_run, "#{verb}: host route diverged from Run.Executor"
      end

      assert {:ok, ^before} = journal(dir)
      assert {:ok, %{state: :live, generation: ^gen}} = Ownership.status(dir)
      release!(owner, ref)
      assert {:ok, %{}} = finish!(task)
    end

    test "H-2b absence of a monitor entry never proves absence of a live run", %{dir: dir} do
      {monitor, _} = start_monitor!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      assert {:ok, %{registered: false} = status} = host().status(dir, monitor: monitor)
      refute Map.has_key?(status, :live)
      assert {:ok, %{state: :live}} = Ownership.status(dir)
      :ok = Writer.close(writer)
    end

    test "H-5 two held concurrent runs under one monitor: two entries, exact identities, run-id collision diagnosed",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      other = dir <> "_other"
      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf!(other) end)
      H.reset_seams()
      t1 = start_async("start", dir, host_executor(), holding_barrier(self(), :one), host_monitor: monitor)
      {r1, o1, own1} = await_held!(:one)
      t2 = start_async("start", other, host_executor(), holding_barrier(self(), :two), host_monitor: monitor)
      {r2, o2, own2} = await_held!(:two)

      assert {:ok, %{registered: true, owner: ^own1, supervisor: sup1}} = host().status(dir, monitor: monitor)
      assert {:ok, %{registered: true, owner: ^own2, supervisor: sup2}} = host().status(other, monitor: monitor)
      assert sup1 == o1[:supervisor] and sup2 == o2[:supervisor] and sup1 != sup2

      assert {:ok, entries} = host().lookup_run_id("run_host_0001", monitor: monitor)
      assert length(entries) == 2
      assert Enum.sort(Enum.map(entries, & &1.run_dir)) == Enum.sort([Path.expand(dir), Path.expand(other)])
      assert {:ok, %{clause: "host_registry_collision", count: 2}} = host().collision("run_host_0001", monitor: monitor)

      release!(own1, r1)
      release!(own2, r2)
      assert {:ok, %{}} = finish!(t1)
      assert {:ok, %{}} = finish!(t2)
      assert :none = Ownership.status(dir)
      assert :none = Ownership.status(other)
    end

    test "H-7 a complete record whose generation disagrees with Journal.Ownership is reported inconsistent; a matching one is live",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      holder = spawn(fn -> receive do: (:stop -> :ok) end)
      :ok = monitor_mod().register(monitor, full_record(dir, holder, 1, writer))
      assert {:ok, %{registered: true, live: true, generation: 1}} = host().status(dir, monitor: monitor)
      :ok = monitor_mod().register(monitor, full_record(dir, holder, 99, writer))
      assert {:ok, %{clause: "host_registry_inconsistent", generation: 99} = diag} = host().status(dir, monitor: monitor)
      refute Enum.any?(Map.values(diag), &(is_binary(&1) and String.contains?(&1, dir))), "no path bytes in diagnostics"
      send(holder, :stop)
      :ok = Writer.close(writer)
    end
  end

  describe "noninterference: Host route versus Run.Executor under monitor faults (R6, F4)" do
    test "H-6a absent monitor (unregistered name): identical result, journal, barrier sequence and cleanup", %{dir: dir} do
      require_host!()
      compare_routes!(dir, "absent", host_monitor: :host_monitor_never_started)
      assert {:error, %{clause: "host_monitor_unavailable"}} = host().status(dir, monitor: :host_monitor_never_started)
    end

    test "H-6d stalled monitor: the command is not blocked and status is bounded by the outer deadline", %{dir: dir} do
      require_host!()
      stalled = start_supervised!(%{id: :stalled, start: {Task, :start_link, [fn -> Process.sleep(:infinity) end]}})
      started = System.monotonic_time(:millisecond)
      compare_routes!(dir, "stalled", host_monitor: stalled)
      task = Task.async(fn -> host().status(dir, monitor: stalled, timeout: 200) end)
      assert {:error, %{clause: "host_monitor_unavailable"}} = Task.await(task, 2_000)
      assert System.monotonic_time(:millisecond) - started < @deadline
    end

    test "H-6b/H-6c the user barrier keeps its return and escape semantics on both routes", %{dir: dir} do
      {monitor, _} = start_monitor!()

      for {tag, barrier} <- [
            {"non_ok", fn _, _ -> :not_ok end},
            {"raise", fn _, _ -> raise "boom" end},
            {"throw", fn _, _ -> throw(:boom) end},
            {"exit", fn _, _ -> exit(:boom) end}
          ] do
        d1 = dir <> "_r_" <> tag
        d2 = dir <> "_h_" <> tag
        File.mkdir_p!(d1)
        File.mkdir_p!(d2)

        on_exit(fn ->
          File.rm_rf!(d1)
          File.rm_rf!(d2)
        end)

        H.reset_seams()
        via_run = invoke("start", d1, RunExecutor, barrier, [])
        H.reset_seams()
        via_host = invoke("start", d2, host_executor(), barrier, host_monitor: monitor)
        assert {:error, %{clause: "run_executor_down"}} = via_run
        assert via_run == via_host, "#{tag}: escape/return semantics diverged"
        assert {:ok, %{registered: false}} = host().status(d2, monitor: monitor)
      end
    end
  end

  describe "crashes (R3/R7, F1/F2)" do
    test "H-9a the monitor is live and registered, then killed mid-run: the run completes and status falls back",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      H.reset_seams()
      task = start_async("start", dir, host_executor(), holding_barrier(self(), :m), host_monitor: monitor)
      {ref, _owned, owner} = await_held!(:m)
      assert {:ok, %{registered: true}} = host().status(dir, monitor: monitor)
      mref = Process.monitor(monitor)
      Process.exit(monitor, :kill)
      assert_receive {:DOWN, ^mref, :process, ^monitor, :killed}, @deadline
      release!(owner, ref)
      assert {:ok, %{}} = finish!(task)
      assert {:error, %{clause: "host_monitor_unavailable"}} = host().status(dir, monitor: monitor)
      assert :none = Ownership.status(dir)
    end

    test "H-9c the Server dies mid-run: the owner's closed result is unchanged and the entry is removed on owner DOWN",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      H.reset_seams()
      task = start_async("start", dir, host_executor(), holding_barrier(self(), :crash), host_monitor: monitor)
      {ref, owned, owner} = await_held!(:crash)
      assert {:ok, %{registered: true}} = host().status(dir, monitor: monitor)
      oref = Process.monitor(owner)
      Process.exit(owned[:server], :kill)
      release!(owner, ref)
      assert {:error, %{clause: "run_server_down"}} = finish!(task)
      assert_receive {:DOWN, ^oref, :process, ^owner, _}, @deadline
      assert {:ok, %{registered: false}} = host().status(dir, monitor: monitor)
    end
  end

  # every variant runs the same start through both routes at the SAME directory path (the journal embeds
  # the run directory, so a different path would change every chained line hash): the first route's
  # result and journal bytes are captured, the directory is recreated empty, then the second route runs
  # with the seams reset; result term, journal bytes, barrier payloads and observed cleanup must match
  defp compare_routes!(base_dir, variant, extra_for_host) do
    dir = base_dir <> "_" <> variant
    on_exit(fn -> File.rm_rf!(dir) end)
    test_pid = self()

    capture = fn tag ->
      fn label, owned ->
        send(test_pid, {:cap, tag, label, Map.keys(owned)})
        :ok
      end
    end

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    H.reset_seams()
    via_run = invoke("start", dir, RunExecutor, capture.(:run), [])
    journal_run = journal(dir)
    assert :none = Ownership.status(dir)

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    H.reset_seams()
    via_host = invoke("start", dir, host_executor(), capture.(:host), extra_for_host)
    journal_host = journal(dir)
    assert :none = Ownership.status(dir)

    assert via_run == via_host, "#{variant}: result differs"
    assert journal_run == journal_host, "#{variant}: journal bytes differ"
    assert captured(:run) == captured(:host), "#{variant}: barrier labels/payload differ"
    refute_receive {:cap, _, _, _}, 100
  end

  # the two barrier invocations of one route, in order: {label, payload keys}
  defp captured(tag) do
    for _ <- 1..2 do
      receive do
        {:cap, ^tag, label, keys} -> {label, keys}
      after
        @deadline -> :timeout
      end
    end
  end

  # a complete, otherwise valid identity record (all identities present) for direct monitor rows
  defp full_record(dir, owner, generation, writer \\ nil) do
    %{
      run_dir: Path.expand(dir),
      run_id: "run_host_0001",
      owner: owner,
      supervisor: owner,
      server: owner,
      writer: writer || owner,
      worker: owner,
      generation: generation
    }
  end
end
