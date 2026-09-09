# A test-owned monitor stand-in: holds every call until the test releases it, then forwards it verbatim
# to the real monitor and replies with the real answer; casts and other messages are forwarded at once.
# It lets a row measure a slow monitor leg without touching any global state or assuming the lookup
# message shape.
defmodule AiOrchestrator.Host.RegistryRedTest.ForwardingProxy do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(request, from, %{target: target, notify: notify} = state) do
    ref = make_ref()
    send(notify, {:proxy_held, self(), ref})

    receive do
      {:proxy_release, ^ref} -> :ok
    after
      15_000 -> exit({:proxy_never_released, ref})
    end

    GenServer.reply(from, GenServer.call(target, request, 15_000))
    {:noreply, state}
  end

  @impl true
  def handle_cast(request, %{target: target} = state) do
    GenServer.cast(target, request)
    {:noreply, state}
  end

  @impl true
  def handle_info(message, %{target: target} = state) do
    send(target, message)
    {:noreply, state}
  end
end

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
  `monitor:`, `ownership:` (the `Journal.Ownership` server consulted, default the Application
  arbiter, which is never suspended here) and `timeout:` (the TOTAL budget for the monitor
  lookup plus the ownership lookup, default 1_000 ms). Nothing here changes admission: the monitor is a
  hint, and the clauses the rows compare are the existing authorities.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host.RegistryRedTest.ForwardingProxy
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Executor, as: RunExecutor
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @deadline 15_000
  # scheduler slack allowed on top of a requested status budget (small enough that a fresh timeout per
  # leg cannot hide inside it: @monitor_delay + @budget > @budget + @slack)
  @slack 200
  @budget 1_000
  @monitor_delay 700

  # ---- late-bound receivers: the interface is absent on the unchanged source ----
  defp host, do: Module.concat(["AiOrchestrator", "Host"])
  defp monitor_mod, do: Module.concat(["AiOrchestrator", "Host", "Monitor"])
  defp host_executor, do: Module.concat(["AiOrchestrator", "Host", "Executor"])

  defp require_host! do
    for mod <- [host(), monitor_mod(), host_executor()] do
      assert Code.ensure_loaded?(mod), "#{inspect(mod)} does not exist"
    end

    for {mod, fun, arity} <- [
          {host(), :status, 2},
          {host(), :lookup_run_id, 2},
          {host(), :collision, 2},
          {host_executor(), :execute, 2},
          {monitor_mod(), :start_link, 1},
          {monitor_mod(), :register, 2},
          {monitor_mod(), :unregister, 2}
        ] do
      assert function_exported?(mod, fun, arity), "#{inspect(mod)}.#{fun}/#{arity} does not exist"
    end
  end

  # a caller-owned holder process that ExUnit stops whether or not the row's assertions succeed
  # (temporary, so an intentional kill is not restarted under a fresh pid)
  defp holder! do
    start_supervised!(%{
      id: make_ref(),
      restart: :temporary,
      start: {Task, :start_link, [fn -> Process.sleep(:infinity) end]}
    })
  end

  defp kill_join!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, @deadline
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

  # ---- rows ----

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
      old = holder!()
      new = holder!()
      :ok = monitor_mod().register(monitor, full_record(dir, old, 1))
      :ok = monitor_mod().register(monitor, full_record(dir, new, 2))
      :ok = monitor_mod().unregister(monitor, full_record(dir, old, 1))
      kill_join!(old)
      # the entry is the replacement's; whether status reports it live depends on Ownership (no writer here)
      assert {:ok, entries} = host().lookup_run_id("run_host_0001", monitor: monitor)
      assert [%{owner: ^new, generation: 2}] = Enum.filter(entries, &(&1.run_dir == Path.expand(dir)))
    end
  end

  describe "the monitor is a hint, never an admission authority (R4/R7, F1)" do
    test "H-2 duplicate start/resume/cancel against a held live run: Host route == Run.Executor route", %{dir: dir} do
      {monitor, _} = start_monitor!()
      H.reset_seams()
      task = start_async("start", dir, host_executor(), holding_barrier(self(), :live), host_monitor: monitor)
      {ref, _owned, owner} = await_held!(:live)
      assert {:ok, %{registered: true, generation: gen}} = host().status(dir, monitor: monitor)
      # the held run's own worker keeps journaling behind the barrier; the duplicates are measured
      # against the journal once that run has nothing more to append
      before = quiescent_journal!(dir)

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
      holder = holder!()
      :ok = monitor_mod().register(monitor, full_record(dir, holder, 1, writer))
      assert {:ok, %{registered: true, live: true, generation: 1}} = host().status(dir, monitor: monitor)
      :ok = monitor_mod().register(monitor, full_record(dir, holder, 99, writer))
      assert {:ok, %{clause: "host_registry_inconsistent", generation: 99} = diag} = host().status(dir, monitor: monitor)
      refute Enum.any?(Map.values(diag), &(is_binary(&1) and String.contains?(&1, dir))), "no path bytes in diagnostics"
      :ok = Writer.close(writer)
    end
  end

  describe "noninterference: Host route versus Run.Executor under monitor faults (R6, F4)" do
    test "H-6a absent monitor (unregistered name): identical result, journal, barrier sequence and cleanup", %{dir: dir} do
      require_host!()
      compare_routes!(dir, "absent", host_monitor: :host_monitor_never_started)
      assert {:error, %{clause: "host_monitor_unavailable"}} = host().status(dir, monitor: :host_monitor_never_started)
    end

    test "H-6d stalled monitor: the command is not blocked and status is bounded by the requested budget", %{dir: dir} do
      require_host!()
      stalled = holder!()
      started = System.monotonic_time(:millisecond)
      compare_routes!(dir, "stalled", host_monitor: stalled)
      assert System.monotonic_time(:millisecond) - started < @deadline, "the held command was blocked by the monitor"

      assert {{:error, %{clause: "host_monitor_unavailable"}}, elapsed} =
               timed(fn -> host().status(dir, monitor: stalled, timeout: 300) end)

      assert elapsed < 300 + @slack, "status exceeded its total budget: #{elapsed} ms"
      # the pinned default budget: no timeout option means 1_000 ms, neither shorter nor unbounded
      assert {{:error, %{clause: "host_monitor_unavailable"}}, elapsed} =
               timed(fn -> host().status(dir, monitor: stalled) end)

      assert elapsed >= 1_000 and elapsed < 1_000 + @slack, "default budget is 1_000 ms, measured #{elapsed} ms"
      # timeout validity is closed, never a crash or an unbounded wait
      for bad <- [0, -1, :infinity, "300", 1.5] do
        assert {:error, %{clause: "host_status_timeout_invalid"}} = host().status(dir, monitor: stalled, timeout: bad)
      end
    end

    test "H-6e one TOTAL status budget: a slow monitor leg then a stalled ownership leg must close within it",
         %{dir: dir} do
      {monitor, _} = start_monitor!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      holder = holder!()
      :ok = monitor_mod().register(monitor, full_record(dir, holder, 1, writer))
      # a test-owned forwarding proxy stands in front of the real monitor: every call is held until the
      # test releases it (so the delay is measured while status is actually waiting on the monitor leg),
      # then forwarded verbatim, so the lookup message shape is irrelevant
      proxy = start_supervised!({ForwardingProxy, target: monitor, notify: self()})

      # responsive control (released immediately): the record is confirmed live within the budget
      task = Task.async(fn -> host().status(dir, monitor: proxy, ownership: Ownership, timeout: @budget) end)
      assert_receive {:proxy_held, ^proxy, hold_ref}, @deadline
      send(proxy, {:proxy_release, hold_ref})
      assert {:ok, %{registered: true, live: true, generation: 1}} = Task.await(task, @budget + @deadline)

      # the witness: the monitor leg consumes ~@monitor_delay of the budget, then the ownership leg
      # stalls (a private process that never answers; the global arbiter is untouched); a closed
      # host_ownership_unavailable must arrive within the SINGLE budget plus scheduler slack, which a
      # fresh-per-leg implementation (~@monitor_delay + @budget) cannot meet
      stalled = holder!()
      started = System.monotonic_time(:millisecond)
      task = Task.async(fn -> host().status(dir, monitor: proxy, ownership: stalled, timeout: @budget) end)
      assert_receive {:proxy_held, ^proxy, hold_ref}, @deadline
      Process.sleep(max(@monitor_delay - (System.monotonic_time(:millisecond) - started), 0))
      send(proxy, {:proxy_release, hold_ref})
      assert {:error, %{clause: "host_ownership_unavailable"}} = Task.await(task, @budget + @deadline)
      elapsed = System.monotonic_time(:millisecond) - started
      assert elapsed >= @monitor_delay, "the monitor leg did not consume the measured delay"
      assert elapsed < @budget + @slack, "monitor plus ownership exceeded the single budget: #{elapsed} ms"
      assert {:ok, %{state: :live, generation: 1}} = Ownership.status(dir), "the global arbiter kept answering"
      :ok = Writer.close(writer)
    end

    test "H-6b/H-6c the user barrier keeps its return and escape semantics on both routes: result, journal, invocations",
         %{dir: dir} do
      {monitor, _} = start_monitor!()

      for {tag, outcome} <- [{"non_ok", :not_ok}, {"raise", :raise}, {"throw", :throw}, {"exit", :exit}] do
        d = dir <> "_" <> tag
        on_exit(fn -> File.rm_rf!(d) end)
        test_pid = self()

        escape = fn route ->
          fn label, _owned ->
            send(test_pid, {:inv, route, label})

            case outcome do
              :not_ok -> :not_ok
              :raise -> raise "boom"
              :throw -> throw(:boom)
              :exit -> exit(:boom)
            end
          end
        end

        {via_run, journal_run} = escape_route(d, RunExecutor, escape.(:run), [])
        {via_host, journal_host} = escape_route(d, host_executor(), escape.(:host), host_monitor: monitor)
        assert {:error, %{clause: "run_executor_down"}} = via_run
        assert via_run == via_host, "#{tag}: escape/return semantics diverged"
        assert journal_run == journal_host, "#{tag}: journal bytes diverged"
        {inv_run, inv_host} = {invocations(:run), invocations(:host)}
        assert inv_run == inv_host, "#{tag}: invocation sequence diverged"
        assert [:handoff_received] == inv_run, "#{tag}: the user barrier is invoked once and the escape ends the run"

        assert {:ok, %{registered: false}} = host().status(d, monitor: monitor)
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
  # the run directory, so a different path would change every chained line hash): each route runs in a
  # task, the barrier callback ships its FULL payload plus in-callback evidence (self(), the supervisor's
  # descendants) and waits for the test's ack, the test validates every identity against the owner's
  # own trace message and the supervision tree, monitors every owned process, and after the command
  # returns joins the DOWN of each of them; the two routes must then agree on the result term, the
  # journal bytes and the normalized per-label payload sequence (pids differ between runs by nature)
  defp compare_routes!(base_dir, variant, extra_for_host) do
    dir = base_dir <> "_" <> variant
    on_exit(fn -> File.rm_rf!(dir) end)
    run = route!(dir, :run, RunExecutor, [])
    host = route!(dir, :host, host_executor(), extra_for_host)
    assert run.result == host.result, "#{variant}: result differs"
    assert run.journal == host.journal, "#{variant}: journal bytes differ"
    assert run.payloads == host.payloads, "#{variant}: normalized barrier label/payload sequence differs"
  end

  @labels [:handoff_received, :subtree_started]

  defp route!(dir, tag, executor, extra) do
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    H.reset_seams()
    test_pid = self()

    capture = fn label, owned ->
      ref = make_ref()
      send(test_pid, {:cap, tag, label, owned, %{caller: self(), ref: ref}})

      receive do
        {:cap_ack, ^ref} -> :ok
      after
        @deadline -> exit({:capture_never_acknowledged, tag, label})
      end
    end

    task = start_async("start", dir, executor, capture, extra)
    assert_receive {:run_executor_started, owner, supervisor}, @deadline

    {payloads, monitored} =
      Enum.map_reduce(@labels, %{}, fn label, acc -> capture!(tag, label, owner, supervisor, acc) end)

    result = finish!(task)
    for {pid, ref} <- monitored, do: assert_receive({:DOWN, ^ref, :process, ^pid, _}, @deadline)
    refute_receive {:cap, ^tag, _, _, _}, 100
    assert :none = Ownership.status(dir)
    %{result: result, journal: journal(dir), payloads: payloads}
  end

  # one expected label, in order: while the owner is blocked in the callback, the test reads the run tree
  # from the TRUSTED supervisor (the pid the owner traced in :run_executor_started, never the payload's
  # own supervisor value), builds the ROLE -> PID map by child id (Journal.Writer, Run.Server,
  # Run.Work.Supervisor and its single worker) and requires the payload to equal that map exactly: same
  # key set, same pid per role (a swap of two live pids is a corrupted payload); every owned pid is then
  # monitored so its DOWN can be joined after the command; the cross-route value is the label with the
  # verified role set (pids legitimately differ between runs)
  defp capture!(tag, label, owner, supervisor, monitored) do
    assert_receive {:cap, ^tag, ^label, owned, evidence}, @deadline
    assert evidence.caller == owner, "#{label}: the barrier must run in the owner"
    expected = Map.put(role_map!(supervisor), :owner, owner)
    assert Enum.sort(Map.keys(owned)) == Enum.sort(Map.keys(expected)), "#{label}: payload key set differs"

    for {role, pid} <- expected do
      assert owned[role] == pid,
             "#{label}: #{role} identity corrupted (payload #{inspect(owned[role])}, tree #{inspect(pid)})"
    end

    send(evidence.caller, {:cap_ack, evidence.ref})

    monitored =
      Enum.reduce(Map.values(owned), monitored, fn pid, acc ->
        Map.put_new_lazy(acc, pid, fn -> Process.monitor(pid) end)
      end)

    {{label, expected |> Map.keys() |> Enum.sort() |> Map.new(&{&1, :verified_role})}, monitored}
  end

  # the exact identities under the trusted run supervisor, by child id; only the Work.Supervisor (a
  # supervisor-typed child) is walked further, so no worker gen_statem receives an unexpected call
  defp role_map!(supervisor) do
    children = Supervisor.which_children(supervisor)
    {_, server, _, _} = List.keyfind(children, AiOrchestrator.Run.Server, 0)
    {_, work, :supervisor, _} = List.keyfind(children, AiOrchestrator.Run.Work.Supervisor, 0)
    {_, writer, _, _} = Enum.find(children, &match?({{Writer, _}, pid, _, _} when is_pid(pid), &1))
    [{_, worker, _, _}] = Supervisor.which_children(work)
    map = %{supervisor: supervisor, server: server, writer: writer, work: work, worker: worker}
    assert Enum.all?(Map.values(map), &(is_pid(&1) and Process.alive?(&1))), "trusted tree not fully live"
    map
  end

  # an escape row runs one route at the path (recreated empty) and returns the result with the journal bytes
  defp escape_route(dir, executor, barrier, extra) do
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    H.reset_seams()
    result = invoke("start", dir, executor, barrier, extra)
    {result, journal(dir)}
  end

  defp invocations(route) do
    receive do
      {:inv, ^route, label} -> [label | invocations(route)]
    after
      100 -> []
    end
  end

  # the journal bytes once they have stopped changing for a settle window (bounded by @deadline)
  defp quiescent_journal!(dir, previous \\ nil, since \\ System.monotonic_time(:millisecond)) do
    {:ok, current} = journal(dir)

    cond do
      current == previous ->
        current

      System.monotonic_time(:millisecond) - since > @deadline ->
        flunk("the held run's journal never settled")

      true ->
        Process.sleep(250)
        quiescent_journal!(dir, current, since)
    end
  end

  defp timed(fun) do
    started = System.monotonic_time(:millisecond)
    result = fun.()
    {result, System.monotonic_time(:millisecond) - started}
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
