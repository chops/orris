defmodule AiOrchestrator.Run.CommandExecutorRedTest do
  @moduledoc """
  RED/interface rev 5 for the NS-43 caller migration: one `Commands.Executor` over the integrated
  foreground foundation (docs/contracts/command-executor-migration.org; rulings m_1788650460000 a-d;
  reviews m_1788652000000 Q1-Q4 + E-M1..E-M6, m_1788653141000 E-M7..E-M11, m_1788654330000 E-M12..E-M16, m_1788655396000 E-M17/E-M18). `AiOrchestrator.Run.Executor`
  does not exist yet: every call into it is late-bound so the tree compiles warning-free. Tests named
  "control:" are baseline-green and never depend on the missing module. Unit A = executor/admission/
  lifetime; unit B = stamping/retry/CLI migration. Every receiver-bound value is built in the test
  process; every pid a test learns of is reaped by the test harness itself.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Commands.Arguments
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Commands.Idempotency
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Schemas.RequestedBy
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Run.CommandExecutorRedTest.CountingDispatch
  alias AiOrchestrator.Run.CommandExecutorRedTest.HeldGate
  alias AiOrchestrator.Run.CommandExecutorRedTest.Seam
  alias AiOrchestrator.Run.CommandExecutorRedTest.UnsettledExecutor
  alias AiOrchestrator.Test.DerivedLive
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.OwnerOracle
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @cli_src Path.expand("../../lib/ai_orchestrator/cli.ex", __DIR__)
  @trusted_src Path.expand("../../lib/ai_orchestrator/prepare/trusted.ex", __DIR__)
  @run_fsm_src Path.expand("../../lib/ai_orchestrator/lifecycle/run_fsm.ex", __DIR__)
  @executor_src Path.expand("../../lib/ai_orchestrator/run/executor.ex", __DIR__)
  @stamp_fixture Path.expand("../fixtures/contracts/journals/valid_requested_by", __DIR__)
  @kill9 Path.expand("../fixtures/contracts/scenarios/kill9_resume", __DIR__)
  @operator %{"class" => "operator", "id" => "local_operator"}
  @console %{"class" => "console", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-05T20:00:00Z", unix: 1_788_638_400}
  @zero "sha256:" <> String.duplicate("0", 64)
  @instance "sup_exec_0001"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]

  # ---- late-bound receivers ----
  defp executor, do: Module.concat(["AiOrchestrator", "Run", "Executor"])
  defp run_sup, do: Module.concat(["AiOrchestrator", "Run", "Supervisor"])
  defp run_server, do: Module.concat(["AiOrchestrator", "Run", "Server"])
  defp require_executor!, do: assert(Code.ensure_loaded?(executor()), "AiOrchestrator.Run.Executor does not exist")

  # ---- cross-process seam (unlinked; read after the test exits) ----
  defmodule Seam do
    @moduledoc false
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
    def put(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))
    def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))
    def push(key, value), do: Agent.update(__MODULE__, &Map.update(&1, key, [value], fn l -> [value | l] end))

    def bump(key) do
      Agent.get_and_update(__MODULE__, fn m -> {Map.get(m, key, 0) + 1, Map.update(m, key, 1, &(&1 + 1))} end)
    end
  end

  setup do
    case Seam.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> Seam.reset()
    end

    Seam.put(:abandon_subscriber, self())
    # E-M10: every pid a test learns of (owner, supervisor, children, callers) is reaped by THIS harness in
    # cleanup with fresh monitors in the cleanup process, never by the behaviour under test
    key = {:tracked, self()}
    Seam.put(key, [])
    collector = start_collector!(self(), key)
    Seam.put({:collector, self()}, collector)
    Seam.put({:dirs, self()}, [])
    on_exit(fn -> teardown_tracked!(key, collector) end)
    :ok
  end

  # E-M17: teardown is a protocol, not a snapshot. Reap the owned producers/callers registered so far
  # (quiescing them), flush the collector so every identity they published before dying is registered,
  # reap the newly registered pids, repeat until the set is closed, stop the collector, then remove the
  # temporary directories (never before the processes that may still hold them are gone).
  defp teardown_tracked!({:tracked, test} = key, collector) do
    teardown_round!(key, collector, MapSet.new([collector]), 0)
  after
    # a protocol failure still gets bounded final cleanup: stop the collector, then remove the directories
    _ = reap_all!([collector | Enum.filter(Seam.get(key) || [], &Process.alive?/1)])
    for dir <- Seam.get({:dirs, test}) || [], do: File.rm_rf(dir)
  end

  defp teardown_round!(_key, _collector, _done, 6), do: raise("owned set did not close after six teardown rounds")

  defp teardown_round!(key, collector, done, round) do
    known = Enum.reject(Seam.get(key) || [], &MapSet.member?(done, &1))
    _ = reap_all!(known)
    done = Enum.reduce(known, done, &MapSet.put(&2, &1))
    :ok = flush_collector!(collector, 5_000)
    remaining = (Seam.get(key) || []) |> Enum.reject(&MapSet.member?(done, &1)) |> Enum.filter(&Process.alive?/1)
    if remaining == [], do: MapSet.to_list(done), else: teardown_round!(key, collector, done, round + 1)
  end

  # the collector answers a flush marker only after every message queued before it was registered and forwarded
  defp flush_collector!(collector, timeout) do
    if Process.alive?(collector) do
      ref = make_ref()
      send(collector, {:flush, ref, self()})

      receive do
        {:flushed, ^ref} -> :ok
      after
        timeout -> raise("the collector did not flush within #{timeout} ms")
      end
    else
      :ok
    end
  end

  defp collector_flush!, do: flush_collector!(collector(), 5_000)

  # a test-owned COLLECTOR receives every trace message, registers each pid it names under the test's key BEFORE
  # forwarding the message to the test, so a partial start or a missing final notification still leaves the
  # already-started pids registered for the reaper (E-M15)
  defp start_collector!(test, key) do
    collector = spawn(fn -> collector_loop(test, key) end)
    Seam.push(key, collector)
    collector
  end

  defp collector_loop(test, key) do
    receive do
      {:flush, ref, from} ->
        send(from, {:flushed, ref})
        collector_loop(test, key)

      message ->
        for pid <- pids_in(message), do: Seam.push(key, pid)
        send(test, message)
        collector_loop(test, key)
    end
  end

  defp pids_in({:run_child_started, sup, _id, pid}), do: [sup, pid]
  defp pids_in({:run_executor_started, owner, sup}), do: [owner, sup]
  defp pids_in({:run_server_driving, server, _facts}), do: [server]
  defp pids_in({:subtree_started, facts, _ref}) when is_map(facts), do: facts |> Map.values() |> Enum.filter(&is_pid/1)
  defp pids_in(_other), do: []

  defp collector, do: Seam.get({:collector, self()})

  defp track!(pids) when is_list(pids), do: Enum.each(pids, &Seam.push({:tracked, self()}, &1))
  defp track!(pid), do: track!([pid])

  # kill + reap every still-alive tracked pid (most recently learned first), bounded; survivors are a failure
  defp reap_all!(pids) do
    alive = Enum.filter(pids, &(is_pid(&1) and Process.alive?(&1)))
    refs = for pid <- alive, do: {pid, Process.monitor(pid)}
    for pid <- alive, do: Process.exit(pid, :kill)

    survivors =
      for {pid, ref} <- refs,
          (receive do
             {:DOWN, ^ref, :process, ^pid, _} -> false
           after
             5_000 -> true
           end),
          do: pid

    if survivors != [], do: raise("tracked processes survived the reaper: #{inspect(survivors)}")
    alive
  end

  # ---- doubles ----
  defmodule HeldGate do
    @moduledoc false
    # bounded: a held gate that is never released ends the executing process instead of hanging a suite
    def runner(parent) do
      fn _gate ->
        send(parent, {:gate_entered, self()})

        receive do
          :release_gate ->
            {:ok,
             %{
               "exit_status" => 0,
               "duration_ms" => 1,
               "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
               "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
             }}
        after
          30_000 -> exit(:held_gate_never_released)
        end
      end
    end
  end

  defmodule UnsettledExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate release(handle, ack, opts), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate reconcile(fs, dir, expected, opts), to: GateDouble
    defdelegate await(handle, opts), to: GateDouble

    def abandon(handle) do
      if pid = Seam.get(:abandon_subscriber), do: send(pid, {:abandoned, handle})
      :ok
    end
  end

  # counts real deliveries through the pane adapter (a re-send after recovery would be a second count)
  defmodule CountingDispatch do
    @moduledoc false
    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def deliver(command, opts) do
      Seam.bump({:delivered, command["assignment_id"]})
      LocalPane.deliver(command, opts)
    end
  end

  # unit L (L-M1): at deliver ENTRY this double folds the DURABLE prefix the test made readable through the Seam
  # (:durable_prefix -> a 0-arity reader: the journal file on the executor path, the recording sink on the plain
  # path) and records what was durable for that assignment BEFORE any delegation; then it counts and delegates.
  defmodule PrefixCheckingDispatch do
    @moduledoc false
    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def deliver(command, opts) do
      assignment_id = command["assignment_id"]
      Seam.put({:durable_at_delivery, assignment_id}, durable_facts(Seam.get(:durable_prefix).(), assignment_id))
      Seam.bump({:delivered, assignment_id})
      LocalPane.deliver(command, opts)
    end

    # the guard itself: what the durable prefix says about the assignment's leases (nil when it does not fold)
    def durable_facts(lines, assignment_id) do
      case Fold.fold_lines(lines) do
        {:ok, fold} ->
          a = fold.assignments[assignment_id] || %{}
          %{folds: true, pane: a[:pane_lease?] == true, workspace: a[:workspace_lease?] == true}

        {:error, _} ->
          %{folds: false, pane: false, workspace: false}
      end
    end
  end

  # ---- fixtures ----
  defp tmp_run_dir do
    dir = Path.join(System.tmp_dir!(), "command-executor-red-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    # removed by the tracked teardown AFTER every owned process is reaped (E-M17)
    Seam.push({:dirs, self()}, dir)
    dir
  end

  defp seed_legacy!(run_dir, lines) do
    File.write!(Path.join(run_dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    run_dir
  end

  defp seed_v2!(run_dir, lines) do
    File.write!(Path.join(run_dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    last = List.last(lines)

    receipt =
      Chain.encode_receipt(%{
        seq: length(lines),
        line_sha256: Chain.line_sha256(last <> "\n"),
        updated_at: FixedClock.wall_ts()
      })

    File.write!(Path.join(run_dir, "events.head"), receipt)
    run_dir
  end

  # a legacy seed whose run_created carries the given structured stamp (a stamped, interrupted or complete start)
  defp stamp_first(lines, stamp),
    do:
      List.update_at(lines, 0, fn raw ->
        raw |> Jason.decode!() |> put_in(["data", "requested_by"], stamp) |> Jason.encode!()
      end)

  # Host results carry the harness sink's chain fields; a seeded journal must be a PURE legacy (v1) journal, so the
  # chain fields are dropped (a re-encoded v2 line would not verify against its own prev_line_sha256)
  defp legacy_lines(events) do
    Enum.map(events, fn e -> e |> Map.delete("prev_line_sha256") |> Map.put("schema_version", 1) |> Jason.encode!() end)
  end

  defp kill9(file), do: @kill9 |> Path.join(file) |> File.read!() |> String.split("\n", trim: true)

  # derived-live (m_1788751885000 / m_1788752018000): the historical prefix with ONLY its assignment deadline fields
  # moved ahead of FixedClock (exact delta verified by the helper); ids, phases, leases and every other byte untouched
  defp live(lines), do: lines |> DerivedLive.shift_deadlines() |> elem(0)

  # the fresh owner's arm must witness the retained deadline still ahead (fence observer fact on the resumed run)
  defp assert_live_arm! do
    live = DerivedLive.deadline()
    assert_receive {:observe_fence, _worker, %{fact: {:armed, %{deadline_unix: ^live, unix_now: arm_unix}}}}, 30_000
    assert live > arm_unix, "the retained deadline is ahead of the owner's OWN arm read (#{arm_unix})"
  end

  # the Writer's head receipt names exactly the last journaled line: real receipt evidence, not envelope syntax
  defp assert_head_receipt!(run_dir) do
    lines = run_dir |> journal_bytes() |> String.split("\n", trim: true)
    assert {:ok, %{seq: seq, line_sha256: hash}} = Chain.decode_receipt(File.read!(Path.join(run_dir, "events.head")))
    assert seq == length(lines) and hash == Chain.line_sha256(List.last(lines) <> "\n")
  end

  # D1 on the UNCHANGED expired kill9 prefix (m_1788751607000): the due Observe answers the exact expiry
  @expired_resume {:error, %{"reason" => "observation_timeout", "deadline_unix" => 1_767_225_600}}

  defp budget_lines,
    do:
      @stamp_fixture
      |> Path.dirname()
      |> Path.join("fold_budget_exhausted/events.jsonl")
      |> File.read!()
      |> String.split("
", trim: true)

  # a GENUINELY re-chained envelope-v2 journal: every line stamped schema_version 2 with prev_line_sha256 of the
  # previous encoded line (the anchor first), exactly as the Writer stamps; verified by Chain before use
  defp rechain_v2(legacy_lines) do
    {lines, _prev} =
      Enum.map_reduce(legacy_lines, Chain.anchor(), fn raw, prev ->
        line =
          raw |> Jason.decode!() |> Map.put("schema_version", 2) |> Map.put("prev_line_sha256", prev) |> Jason.encode!()

        {line, Chain.line_sha256(line <> "\n")}
      end)

    {:ok, verified} = Chain.verify(Enum.join(lines, "\n") <> "\n")
    assert verified.envelope_version == 2 and verified.count == length(lines)
    lines
  end

  defp corrupt_link(v2_lines, at) do
    List.update_at(v2_lines, at, fn line ->
      line |> Jason.decode!() |> Map.put("prev_line_sha256", "sha256:" <> String.duplicate("9", 64)) |> Jason.encode!()
    end)
  end

  defp bad_receipt!(run_dir, seq, bytes) do
    receipt = Chain.encode_receipt(%{seq: seq, line_sha256: Chain.line_sha256(bytes), updated_at: FixedClock.wall_ts()})
    File.write!(Path.join(run_dir, "events.head"), receipt)
    run_dir
  end

  # the Writer's own rejection for a directory; a successful open is closed instead of leaked
  defp writer_rejection!(run_dir) do
    case Writer.open(run_dir, lock: [supervisor_instance: @instance]) do
      {:error, rejection} ->
        rejection

      {:ok, writer, opened} ->
        :ok = Writer.close(writer)
        flunk("expected a Writer rejection, the journal was accepted: #{inspect(Map.take(opened, [:last_seq, :repair]))}")
    end
  end

  defp writer_accepts!(run_dir) do
    {:ok, writer, opened} = Writer.open(run_dir, lock: [supervisor_instance: @instance])
    :ok = Writer.close(writer)
    opened
  end

  defp journal(run_dir),
    do:
      run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp journal_bytes(run_dir), do: File.read!(Path.join(run_dir, "events.jsonl"))

  # the leases a cancel must release, from the seed's own Fold: never a guessed key, never an empty expectation
  defp expected_lease_identities(lines) do
    {:ok, fold} = Fold.fold_lines(lines)
    workspace_ids = fold.active_workspace_leases |> Map.keys() |> Enum.sort()
    pane_refs = for {_, a} <- fold.assignments, a[:pane_lease?], is_binary(a[:pane_ref]), do: a[:pane_ref]
    assert workspace_ids != [] and pane_refs != []
    {workspace_ids, Enum.sort(pane_refs)}
  end

  defp fresh_opts(index) do
    {_name, _kind, _scenario, _prior, opts_fun} = Enum.at(H.cases(), index)
    H.reset_seams()
    opts_fun.()
  end

  defp gated_index, do: Enum.find_index(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
  defp pre_dispatch_index, do: Enum.find_index(H.cases(), &match?({"kill9 resume pre_dispatch", _, _, _, _}, &1))
  defp blocked_index, do: Enum.find_index(H.cases(), &match?({"auth_blocked_pane run", _, _, _, _}, &1))

  defp sha256(bytes), do: "sha256:" <> (:sha256 |> :crypto.hash(bytes) |> Base.encode16(case: :lower))
  defp spec_hash(spec), do: spec |> Jason.encode!() |> sha256()

  # executor CONTEXT (never arguments): Host options minus every Server-owned binding, plus the run directory,
  # the inputs the caller read, their hashes, a fixed explicit instance and the trace
  defp context(run_dir, scenario, index, extra \\ []) do
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    index
    |> fresh_opts()
    |> Keyword.drop(@owned)
    |> Keyword.merge(
      run_dir: run_dir,
      spec: spec,
      plan: plan,
      spec_hash: spec_hash(spec),
      plan_hash: spec_hash(plan),
      supervisor_instance: @instance,
      trace: collector()
    )
    |> Keyword.merge(extra)
  end

  defp start_args(ctx), do: %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]}
  defp args_for(:run, ctx), do: {"start", start_args(ctx)}
  defp args_for(:resume, _ctx), do: {"resume", %{"recovery_reason" => "operator_resume"}}
  defp args_for(:cancel, _ctx), do: {"cancel", %{"reason" => "operator_cancel"}}

  defp invoke(verb, args, run_id, command_id, ctx, actor \\ @operator) do
    Commands.invoke(actor, verb, args,
      run_id: run_id,
      command_id: command_id,
      now: @now,
      executor: executor(),
      executor_opts: ctx
    )
  end

  # the independent expected stamp: computed from actor/verb/args, never read from an event
  defp expected_stamp(actor, verb, args, command_id) do
    actor
    |> Map.put("command_id", command_id)
    |> Map.put("verb", verb)
    |> Map.put("args_hash", Arguments.hash(verb, args))
  end

  # the bare Host oracle keeps the harness receipt sink (a gate releases only against a persisted receipt; the
  # Server path replaces that sink with the Writer) and receives the same executor bindings
  defp oracle_opts(ctx, index, extra) do
    index
    |> fresh_opts()
    |> Keyword.merge(Keyword.take(ctx, [:run_dir, :spec_hash, :plan_hash, :supervisor_instance]))
    |> Keyword.merge(extra)
  end

  defp oracle_run(ctx, index, run_id) do
    {:ok, %{events: events}} = OwnerOracle.run(ctx[:spec], ctx[:plan], oracle_opts(ctx, index, run_id: run_id))
    events
  end

  defp seq_of(events, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("seq")
  defp run_id_of(run_dir), do: run_dir |> journal() |> hd() |> Map.fetch!("run_id")
  defp fs_trace(fs), do: FaultFs.trace(fs)
  defp observer_to(parent), do: fn e, _ -> send(parent, {:effect_ran, e.__struct__}) end
  defp ordered_observer(key), do: fn e, _ -> Seam.push(key, e.__struct__) end
  defp ordered_effects(key), do: Enum.reverse(Seam.get(key) || [])
  @gate_family [Effect.ReconcileGate, Effect.PrepareGate, Effect.ReleaseGate, Effect.AwaitGate]

  # ---- the named executor traces: consumed exactly and CORRELATED, never drained ----
  defp drain_protocol_traces!(acc \\ %{requested: 0, applied: 0, dropped: 0}) do
    receive do
      {:run_effect_requested, _, _} -> drain_protocol_traces!(%{acc | requested: acc.requested + 1})
      {:run_effect_applied, _, _} -> drain_protocol_traces!(%{acc | applied: acc.applied + 1})
      {:run_effect_reply_dropped, _, _} -> drain_protocol_traces!(%{acc | dropped: acc.dropped + 1})
    after
      0 -> acc
    end
  end

  defp consume_start_traces!(timeout) do
    traces =
      for _ <- 1..3 do
        receive do
          {:run_child_started, sup, id, pid} ->
            track!([sup, pid])
            {sup, id, pid}
        after
          timeout -> flunk("child start trace missing")
        end
      end

    [sup] = traces |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
    started = Enum.map(traces, fn {_sup, id, pid} -> {id, pid} end)
    assert Keyword.keys(started) == [:writer, :server, :work]

    # the ratified effect owner: the Server births ONE worker under Work after discovery (traced under the Work
    # supervisor); when it was born it is part of the owned set
    work = Keyword.fetch!(started, :work)

    started =
      receive do
        {:run_child_started, ^work, :worker, worker} ->
          track!(worker)
          started ++ [worker: worker]
      after
        timeout -> started
      end

    server = Keyword.fetch!(started, :server)
    assert_receive {:run_server_driving, ^server, %{writer: writer, work: work}}, timeout

    assert writer == Keyword.fetch!(started, :writer) and work == Keyword.fetch!(started, :work),
           "driving facts name the siblings"

    assert_receive {:run_executor_started, owner, ^sup}, timeout
    track!(owner)
    {started, owner, sup}
  end

  # E-M9: the two-way :subtree_started barrier - the OWNER blocks before awaiting until the test acks, so the
  # sibling Writer is provably live while its opening is captured; nothing is read from produced events
  defp opening_barrier(collector) do
    # the barrier contract is TOTAL over its two names; the handoff is observed by other suites
    fn
      :handoff_received, _facts ->
        :ok

      :subtree_started, %{writer: _} = facts ->
        ref = make_ref()
        send(collector, {:subtree_started, facts, ref})

        receive do
          {:barrier_ack, ^ref} -> :ok
        after
          30_000 -> exit(:opening_barrier_not_acknowledged)
        end
    end
  end

  defp capture_opening!(timeout) do
    assert_receive {:subtree_started, %{writer: writer} = facts, ref}, timeout
    %{owner: owner, supervisor: sup, server: server, work: work} = facts

    track!([owner, sup, writer, server, work])
    assert Process.alive?(writer), "the sibling Writer is live while its opening is captured"
    opened = Writer.opened(writer)
    send(owner, {:barrier_ack, ref})
    opened
  end

  defp assert_all_down!(pids, timeout) do
    for pid <- pids do
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, timeout
    end
  end

  defp assert_released!(run_dir) do
    refute match?({:ok, %{state: :live}}, Ownership.status(run_dir)), "ownership not live after return"
  end

  defp assert_no_activity!(fs, run_dir) do
    assert fs_trace(fs) == [], "no filesystem operation was dispatched"
    refute File.exists?(Path.join(run_dir, "events.jsonl"))
    refute_received {:run_child_started, _, _, _}
    refute_received {:run_executor_started, _, _}
    refute_received {:effect_ran, _}
  end

  # the Writer is killed inside its own process right after the n-th receipt of this invocation is durable
  # (a legacy seed has no receipts, so the first receipt is the acceptance event's)
  defp kill_after_receipt(fs, parent, n) do
    FaultFs.inject(
      fs,
      :dir_sync,
      fn _ -> true end,
      {:after,
       fn trace ->
         receipts = Enum.count(trace, &match?({:rename, "events.head.tmp", "events.head"}, &1))

         if match?([{:dir_sync, _}, {:rename, "events.head.tmp", "events.head"} | _], trace) and receipts == n do
           send(parent, {:durable, n})
           Process.exit(self(), :kill)
         end
       end}
    )
  end

  # =================================================================================================
  describe "controls: the facts the REDs rely on (baseline" do
    test "control: the stamp fixture recomputes and compares (match; id, class and args_hash conflicts)" do
      [created | _] = @stamp_fixture |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)
      %{"data" => %{"requested_by" => stamp}} = Jason.decode!(created)

      %{"verb" => verb, "args" => args} =
        @stamp_fixture |> Path.join("args/seq_0001.json") |> File.read!() |> Jason.decode!()

      assert Arguments.hash(verb, args) == stamp["args_hash"], "ARGS-CANON-1 recomputation"
      assert {:ok, _} = RequestedBy.parse(stamp)
      actor = %{"class" => stamp["class"], "id" => stamp["id"]}

      build = fn a, v, ar ->
        {:ok, command} = Commands.build(a, v, ar, run_id: "run_fixture_0004", command_id: stamp["command_id"], now: @now)
        command
      end

      assert Idempotency.compare(stamp, build.(actor, verb, args)) == :match
      assert Idempotency.compare(stamp, build.(%{actor | "id" => "someone_else"}, verb, args)) == {:conflict, "id"}
      assert Idempotency.compare(stamp, build.(%{actor | "class" => "console"}, verb, args)) == {:conflict, "class"}
      changed = %{args | "plan_hash" => "sha256:" <> String.duplicate("f", 64)}
      assert Idempotency.compare(stamp, build.(actor, verb, changed)) == {:conflict, "args_hash"}
    end

    test "control: policy and argument documents are closed; context keys are never arguments" do
      agent = %{"class" => "agent", "id" => "writer", "run_id" => "r", "assignment_id" => "a"}
      assert {:error, %{clause: "command_not_authorized"}} = Policy.authorize(agent, "start")
      bad_start = %{"spec_hash" => @zero, "plan_hash" => @zero, "run_dir" => "/tmp/x"}
      assert {:error, %{clause: "command_argument_fields"}} = Arguments.validate("start", bad_start)

      assert {:error, %{clause: "command_argument_fields"}} =
               Arguments.validate("cancel", %{"reason" => "x", "fs" => "y"})

      assert {:ok, _} = Arguments.validate("pause", %{"reason" => "hold"})
      assert "pause" in Policy.verbs()["operator"] and "start" in Policy.verbs()["console"]
    end

    test "control: today a :run on an existing journal is refused by the Writer's exclusive create before any Server" do
      run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))

      assert {:error, %{clause: "journal_exists"}} =
               Writer.open(run_dir, create: true, lock: [supervisor_instance: "sup_ctl"])

      opts = gated_index() |> fresh_opts() |> Keyword.delete(:event_sink)

      config = %{
        run_dir: run_dir,
        mode: :run,
        spec: H.spec("kill9_resume"),
        plan: H.plan("kill9_resume"),
        opts: opts,
        trace: self()
      }

      parent = self()
      # the supervisor links to its starter: a trapping owner receives the failed start as a return value
      {owner, ref} =
        spawn_monitor(fn ->
          Process.flag(:trap_exit, true)
          send(parent, {:started, run_sup().start_link(config)})
        end)

      track!(owner)
      assert_receive {:started, {:error, %{clause: "journal_exists"}}}, 5_000
      assert_receive {:DOWN, ^ref, :process, ^owner, :normal}, 5_000
      refute_received {:run_child_started, _, :server, _}, "no Server exists when the create is refused"
    end

    test "control: receipt/chain corpus - each fixture produces exactly its intended Writer verdict" do
      legacy = kill9("events_pre_dispatch.jsonl")
      v2 = rechain_v2(legacy)
      # legacy without a receipt is ACCEPTED (the baseline fact the rev 2 fixtures got wrong)
      assert %{last_seq: 9, repair: nil} = writer_accepts!(seed_legacy!(tmp_run_dir(), legacy))

      assert %{clause: "receipt_on_legacy_journal"} =
               tmp_run_dir() |> seed_legacy!(legacy) |> bad_receipt!(9, "x\n") |> writer_rejection!()

      # a genuinely re-chained v2 journal with its matching receipt is accepted (positive control)
      assert %{last_seq: 9, repair: nil} = writer_accepts!(seed_v2!(tmp_run_dir(), v2))

      assert %{clause: "receipt_hash_mismatch"} =
               tmp_run_dir() |> seed_v2!(v2) |> bad_receipt!(9, "t\n") |> writer_rejection!()

      assert %{clause: "receipt_missing"} =
               tmp_run_dir() |> seed_v2!(v2) |> tap(&File.rm!(Path.join(&1, "events.head"))) |> writer_rejection!()

      assert %{clause: "chain_mismatch", at_seq: 3} = writer_rejection!(seed_v2!(tmp_run_dir(), corrupt_link(v2, 2)))
      # torn tail: the run id is taken from the ACCEPTED prefix before the tail is appended; the Writer repairs 18 bytes
      run_id = legacy |> hd() |> Jason.decode!() |> Map.fetch!("run_id")
      torn = seed_legacy!(tmp_run_dir(), legacy)
      File.write!(Path.join(torn, "events.jsonl"), File.read!(Path.join(torn, "events.jsonl")) <> ~s({"schema":"ai-orch))
      assert_raise Jason.DecodeError, fn -> journal(torn) end
      assert %{repair: %{action: :truncate_tail, truncate_bytes: 18}} = writer_accepts!(torn)
      assert run_id == "run_scenario_0001"
    end

    test "control: a second live Writer on the same directory inside one BEAM is the arbiter's second_live_writer" do
      run_dir = tmp_run_dir()
      {:ok, holder, _} = Writer.open(run_dir, create: true, lock: [supervisor_instance: "sup_holder"])
      track!(holder)

      try do
        # the in-BEAM Ownership arbiter refuses before any lock file is consulted; run_locked is the cross-process
        # clause
        assert {:error, %{clause: "second_live_writer"}} = Writer.open(run_dir, lock: [supervisor_instance: @instance])
      after
        if Process.alive?(holder), do: Writer.close(holder)
      end
    end

    test "control: an EMPTY existing journal and an unstamped legacy journal refuse an exclusive create yet open as existing" do
      empty = tmp_run_dir()
      File.write!(Path.join(empty, "events.jsonl"), "")

      assert {:error, %{clause: "journal_exists"}} =
               Writer.open(empty, create: true, lock: [supervisor_instance: "sup_ctl"])

      assert %{last_seq: 0, lines: []} = writer_accepts!(empty)
      legacy = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))

      assert {:error, %{clause: "journal_exists"}} =
               Writer.open(legacy, create: true, lock: [supervisor_instance: "sup_ctl"])

      refute Enum.any?(journal(legacy), &Map.has_key?(&1["data"], "requested_by")), "no stamp anywhere in the legacy seed"
    end

    test "control: delayed observer - a sibling Writer stays live until its OWNER tears the subtree down, not by scheduling" do
      run_dir = tmp_run_dir()
      parent = self()
      opts = gated_index() |> fresh_opts() |> Keyword.delete(:event_sink)

      config = %{
        run_dir: run_dir,
        mode: :run,
        spec: H.spec("gated_run_seed"),
        plan: H.plan("gated_run_seed"),
        opts: opts,
        trace: parent
      }

      owner =
        spawn(fn ->
          Process.flag(:trap_exit, true)
          {:ok, sup} = run_sup().start_link(config)
          send(parent, {:sup, sup})

          receive do
            {:stop, from} ->
              Supervisor.stop(sup, :shutdown, 10_000)
              send(from, :stopped)
          end
        end)

      track!(owner)
      assert_receive {:sup, sup}, 5_000
      track!(sup)

      traces =
        for _ <- 1..3 do
          receive do
            {:run_child_started, ^sup, id, pid} -> {id, pid}
          after
            5_000 -> flunk("trace")
          end
        end

      writer = Keyword.fetch!(traces, :writer)
      server = Keyword.fetch!(traces, :server)
      track!(Keyword.values(traces))
      assert_receive {:run_server_driving, ^server, _}, 5_000
      assert {:ok, _} = run_server().await(server, 30_000)
      # the Server has FINISHED; the Writer is still live because only the owner's teardown ends it
      assert Process.alive?(writer)
      assert %{last_seq: 0, lines: []} = Writer.opened(writer)
      ref = Process.monitor(writer)
      send(owner, {:stop, self()})
      assert_receive :stopped, 15_000
      assert_receive {:DOWN, ^ref, :process, ^writer, _}, 5_000
    end

    test "control: the tracked reaper kills an owner that refuses cooperative cleanup together with its trapping dependent" do
      parent = self()

      stuck =
        spawn(fn ->
          Process.flag(:trap_exit, true)

          dependent =
            spawn_link(fn ->
              Process.flag(:trap_exit, true)

              receive do
                :never -> :ok
              end
            end)

          send(parent, {:dependent, dependent})

          receive do
            :never -> :ok
          end
        end)

      assert_receive {:dependent, dependent}, 1_000
      # reaped from ANOTHER process, as on_exit does
      killed = fn -> reap_all!([dependent, stuck]) end |> Task.async() |> Task.await(10_000)
      assert Enum.sort(killed) == Enum.sort([stuck, dependent])
      refute Process.alive?(stuck) or Process.alive?(dependent)
      assert fn -> reap_all!([dependent, stuck]) end |> Task.async() |> Task.await(5_000) == []
    end

    test "control: the no-activity witness passes on an untouched FaultFs and FAILS on any dispatched operation" do
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      assert FaultFs.trace(fs) == []
      assert_no_activity!(fs, run_dir)
      :ok = Fs.mkdir_p(fs, Path.join(run_dir, "gates"))
      assert [{:mkdir_p, _}] = FaultFs.trace(fs)
      assert_raise ExUnit.AssertionError, fn -> assert_no_activity!(fs, run_dir) end
    end

    test "control: a FaultFs receipt ordinal is invocation-relative (that adapter's own receipts), never the journal seq" do
      lines = kill9("events_pre_dispatch.jsonl")
      run_dir = seed_legacy!(tmp_run_dir(), Enum.take(lines, 4))
      fs = FaultFs.new()
      kill_after_receipt(fs, self(), 5)
      {:ok, writer, %{last_seq: 4}} = Writer.open(run_dir, fs: fs, lock: [supervisor_instance: @instance])
      track!(writer)
      assert {:ok, %{"seq" => 5}} = Writer.append(writer, Jason.decode!(Enum.at(lines, 4)))
      assert Enum.count(FaultFs.trace(fs), &match?({:rename, "events.head.tmp", "events.head"}, &1)) == 1
      refute_received {:durable, _}, "ordinal 5 never fires for this adapter's FIRST receipt"
      :ok = Writer.close(writer)
      # the correct ordinal for 'this invocation's first receipt' is 1; Writer.open links the writer to its opener,
      # so the kill is observed from a trapping owner, never from the test process
      run_dir2 = seed_legacy!(tmp_run_dir(), Enum.take(lines, 4))
      fs2 = FaultFs.new()
      parent = self()
      kill_after_receipt(fs2, parent, 1)
      line5 = Jason.decode!(Enum.at(lines, 4))

      {owner, mon} =
        spawn_monitor(fn ->
          Process.flag(:trap_exit, true)
          {:ok, writer2, _} = Writer.open(run_dir2, fs: fs2, lock: [supervisor_instance: @instance])
          send(parent, {:writer2, writer2})
          result = catch_exit(Writer.append(writer2, line5))

          receive do
            {:EXIT, ^writer2, reason} -> send(parent, {:writer2_exit, reason, result})
          after
            5_000 -> send(parent, {:writer2_exit, :no_exit_seen, result})
          end
        end)

      track!(owner)
      assert_receive {:writer2, writer2}, 5_000
      track!(writer2)
      assert_receive {:durable, 1}, 5_000
      assert_receive {:writer2_exit, :killed, _}, 5_000
      assert_receive {:DOWN, ^mon, :process, ^owner, :normal}, 5_000
      assert length(journal(run_dir2)) == 5, "the line and its receipt were durable before the kill"
    end

    test "control: registration precedes assertion - a partial start with the final trace missing still registers every pid" do
      key = {:tracked, self()}

      pids =
        for _ <- 1..4,
            do:
              spawn(fn ->
                receive do
                  :never -> :ok
                end
              end)

      [sup, writer, server, work] = pids
      col = collector()
      for {id, pid} <- [writer: writer, server: server, work: work], do: send(col, {:run_child_started, sup, id, pid})
      send(col, {:run_server_driving, server, %{writer: writer, work: work}})
      # no {:run_executor_started, _, _}: the consumer must fail, and everything already seen must be registered
      assert_raise ExUnit.AssertionError, fn -> consume_start_traces!(200) end
      tracked = Seam.get(key)
      for pid <- pids, do: assert(pid in tracked, "#{inspect(pid)} registered before the failing assertion")
      assert Enum.all?(pids, &Process.alive?/1)
      killed = fn -> reap_all!(tracked -- [col]) end |> Task.async() |> Task.await(10_000)
      for pid <- pids, do: assert(pid in killed)
      refute Enum.any?(pids, &Process.alive?/1)
    end

    test "control: terminal no-ops - resume/cancel on a completed prefix append nothing; run_budget_exhausted is terminal" do
      for index <- [1, 2] do
        {_, kind, scenario, prior, _} = Enum.at(H.cases(), index)
        opts = fresh_opts(index)

        result =
          case kind do
            :resume -> OwnerOracle.resume(H.spec(scenario), H.plan(scenario), prior, opts)
            :cancel -> OwnerOracle.cancel(prior, opts)
          end

        assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} = result
      end

      {:ok, state} = Fold.fold_lines(budget_lines())
      assert state.terminal? and state.status == "budget_exhausted"
    end

    test "control: delayed collector delivery - a zero-timeout consume can fail after every trace was sent; a flush closes the hop" do
      col = collector()

      pids =
        for _ <- 1..5,
            do:
              spawn(fn ->
                receive do
                  :never -> :ok
                end
              end)

      [sup, writer, server, work, owner] = pids
      track!(pids)
      :erlang.suspend_process(col)

      try do
        for {id, pid} <- [writer: writer, server: server, work: work], do: send(col, {:run_child_started, sup, id, pid})
        send(col, {:run_server_driving, server, %{writer: writer, work: work}})
        send(col, {:run_executor_started, owner, sup})
        # every producer send has completed, yet nothing has crossed the hop: the old zero-timeout assumption fails
        assert_raise ExUnit.AssertionError, fn -> consume_start_traces!(0) end
      after
        :erlang.resume_process(col)
      end

      collector_flush!()
      assert {_children, ^owner, ^sup} = consume_start_traces!(0)
    end

    test "control: identities still queued in the collector are reaped by the teardown protocol, lost by a snapshot" do
      key = {:tracked, self()}
      col = collector()

      sup =
        spawn(fn ->
          receive do
            :never -> :ok
          end
        end)

      child =
        spawn(fn ->
          receive do
            :never -> :ok
          end
        end)

      :erlang.suspend_process(col)

      try do
        send(col, {:run_child_started, sup, :writer, child})
        # the OLD behaviour (a one-time snapshot): only what is already registered is seen; sup/child would survive
        snapshot = Seam.get(key) || []
        refute sup in snapshot or child in snapshot, "queued identities are not in a snapshot"
      after
        :erlang.resume_process(col)
      end

      # the protocol: reap known, flush, reap the newly registered, repeat until closed (run from another process,
      # as on_exit does); the collector itself is stopped last and the queued identities are reaped
      closed = fn -> teardown_round!(key, col, MapSet.new([col]), 0) end |> Task.async() |> Task.await(10_000)
      assert sup in closed and child in closed
      refute Process.alive?(sup) or Process.alive?(child)
      assert Process.alive?(col), "the collector outlives the rounds and is stopped last by the teardown"
    end

    test "control: a Writer-accepted prefix may hold a stamped start and then an UNSTAMPED lifecycle acceptance" do
      legacy = kill9("events_pre_dispatch.jsonl")
      stamp = expected_stamp(@operator, "start", %{"spec_hash" => @zero, "plan_hash" => @zero}, CommandId.generate())
      prior = stamp_first(legacy, stamp)
      {_, :cancel, _, _, _} = Enum.at(H.cases(), 10)
      assert {:ok, %{events: events, appended_events: [acceptance | _]}} = Host.cancel(prior, fresh_opts(10))
      assert acceptance["type"] == "run_cancel_requested"
      refute Map.has_key?(acceptance["data"], "requested_by"), "the unchanged Host emits an UNSTAMPED acceptance"
      assert List.last(events)["type"] == "run_cancelled"
      assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1, "one stamp, two lifecycle acceptances"
      run_dir = seed_legacy!(tmp_run_dir(), legacy_lines(events))
      assert %{repair: nil} = writer_accepts!(run_dir)
    end

    test "control: the Host oracle with executor bindings and the harness receipt sink completes; a bare Host stamps nothing" do
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      [created | _] = events = oracle_run(ctx, gated_index(), "run_ctl_0001")
      assert created["data"]["spec_hash"] == ctx[:spec_hash]
      refute Map.has_key?(created["data"], "requested_by")
      # the emission order the decision table is built on: run_started precedes the work it starts
      assert seq_of(events, "run_started") < seq_of(events, "assignment_dispatch_sent")
      assert Enum.any?(events, &(&1["type"] == "run_completed"))
    end
  end

  # =================================================================================================
  describe "E-1 (A) interface and verb scope" do
    test "Run.Executor implements Commands.Executor and accepts exactly start, resume and cancel" do
      require_executor!()
      assert function_exported?(executor(), :execute, 2)
      behaviours = :attributes |> executor().module_info() |> Keyword.get_values(:behaviour) |> List.flatten()
      assert AiOrchestrator.Commands.Executor in behaviours
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      run_id = "run_e1_0001"

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               invoke("start", start_args(ctx), run_id, CommandId.generate(), ctx)

      assert_released!(run_dir)
      before = journal_bytes(run_dir)
      # terminal no-ops (existing reducer semantics preserved): a fresh resume/cancel on a completed run appends nothing
      assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} =
               invoke("resume", %{"recovery_reason" => "operator_resume"}, run_id, CommandId.generate(), ctx)

      assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, run_id, CommandId.generate(), ctx)

      assert journal_bytes(run_dir) == before
      refute Enum.any?(journal(run_dir), &(&1["type"] in ["run_resumed", "run_cancel_requested"]))
    end

    for {verb, args} <- [
          {"pause", %{"reason" => "hold"}},
          {"repair", %{"kind" => "tail_truncate", "detail_hash" => @zero}},
          {"resolve_attention", %{"attention_ids" => "att_0001"}},
          {"ratify_plan", %{"plan_hash" => @zero}}
        ] do
      test "a policy-valid but out-of-scope verb (#{verb}) is command_verb_unsupported with no activity at all" do
        require_executor!()
        run_dir = tmp_run_dir()
        fs = FaultFs.new()
        ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, effect_observer: observer_to(self()))

        assert {:error, %{clause: "command_verb_unsupported"}} =
                 invoke(unquote(verb), unquote(Macro.escape(args)), "run_e1_0002", CommandId.generate(), ctx)

        assert_no_activity!(fs, run_dir)
      end
    end
  end

  # =================================================================================================
  describe "E-2 (A) context, not arguments; stamp revalidation; unsupported options" do
    test "a hand-built Command with a tampered args_hash or verb is command_stamp_invalid with no activity" do
      require_executor!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)

      {:ok, %Command{} = command} =
        Commands.build(@operator, "start", start_args(ctx),
          run_id: "run_e2_0001",
          command_id: CommandId.generate(),
          now: @now
        )

      tampered_hash = %{
        command
        | requested_by: Map.put(command.requested_by, "args_hash", "sha256:" <> String.duplicate("e", 64))
      }

      tampered_verb = %{command | requested_by: Map.put(command.requested_by, "verb", "cancel")}

      for bad <- [tampered_hash, tampered_verb],
          do: assert({:error, %{clause: "command_stamp_invalid"}} = executor().execute(bad, ctx))

      assert_no_activity!(fs, run_dir)
    end

    test "start whose argument hashes differ from the consumed spec/plan hashes is command_inputs_mismatch" do
      require_executor!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      args = %{"spec_hash" => ctx[:spec_hash], "plan_hash" => "sha256:" <> String.duplicate("d", 64)}

      assert {:error, %{clause: "command_inputs_mismatch"}} =
               invoke("start", args, "run_e2_0002", CommandId.generate(), ctx)

      assert_no_activity!(fs, run_dir)
    end

    # EA-M1: the COMPLETE structured stamp is validated through the existing closed schema, not an approximation
    for {label, mutate} <- [
          {"an unknown stamp field", :extra_field},
          {"a stamp that is not a map", :not_map},
          {"a missing command_id", :missing_command_id},
          {"an unknown actor class", :bad_class},
          {"an oversize value", :oversize},
          {"a verb the class may not invoke", :agent_verb}
        ] do
      test "#{label} is command_stamp_invalid with no activity, whatever else is valid" do
        require_executor!()
        run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
        fs = FaultFs.new()
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), fs: fs, effect_observer: observer_to(self()))

        {:ok, %Command{} = command} =
          Commands.build(@operator, "cancel", %{"reason" => "review"},
            run_id: run_id_of(run_dir),
            command_id: CommandId.generate(),
            now: @now
          )

        stamp =
          case unquote(mutate) do
            :extra_field ->
              Map.put(command.requested_by, "unexpected", "PRIVATE_STAMP_EXTRA")

            :not_map ->
              "operator"

            :missing_command_id ->
              Map.delete(command.requested_by, "command_id")

            :bad_class ->
              Map.put(command.requested_by, "class", "root")

            :oversize ->
              Map.put(command.requested_by, "id", String.duplicate("a", 5_000))

            :agent_verb ->
              command.requested_by |> Map.put("class", "agent") |> Map.put("run_id", "r") |> Map.put("assignment_id", "a")
          end

        assert {:error, %{clause: "command_stamp_invalid"}} = executor().execute(%{command | requested_by: stamp}, ctx)
        assert FaultFs.trace(fs) == [], "no filesystem operation before the stamp is proven"
        refute_received {:run_child_started, _, _, _}
        refute_received {:run_executor_started, _, _}
        refute_received {:effect_ran, _}
        assert journal_bytes(run_dir) == Enum.join(kill9("events_pre_dispatch.jsonl"), "\n") <> "\n"
      end
    end

    test "an invalid barrier value in the context is command_context_invalid before any subtree exists" do
      require_executor!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, barrier: :not_a_function)

      assert {:error, %{clause: "command_context_invalid", field: "barrier"}} =
               invoke("start", start_args(ctx), "run_e2_0005", CommandId.generate(), ctx)

      assert_no_activity!(fs, run_dir)
    end

    test "a context without a run directory, or with a wait-timeout option, is command_context_invalid before any work" do
      require_executor!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)

      assert {:error, %{clause: "command_context_invalid"}} =
               invoke("start", start_args(ctx), "run_e2_0003", CommandId.generate(), Keyword.delete(ctx, :run_dir))

      # E-M1: there is no caller wait-timeout in this synchronous unit; a supplied one is refused, never honoured
      assert {:error, %{clause: "command_context_invalid", field: "await_timeout"}} =
               invoke(
                 "start",
                 start_args(ctx),
                 "run_e2_0004",
                 CommandId.generate(),
                 Keyword.put(ctx, :await_timeout, 200)
               )

      assert_no_activity!(fs, run_dir)
    end
  end

  # =================================================================================================
  describe "E-3 (B) acceptance stamp and parity for every case; E-4 (A) run id binding" do
    for {{name, _kind, _scenario, _prior, _}, index} <- Enum.with_index(H.cases()) do
      test "case #{index + 1} #{name}: invoke == oracle + the independent stamp on the acceptance event only" do
        require_executor!()
        {_, kind, scenario, prior, _} = Enum.at(H.cases(), unquote(index))
        run_dir = tmp_run_dir()
        if prior != [], do: seed_legacy!(run_dir, prior)
        parent = self()
        ctx = context(run_dir, scenario, unquote(index), barrier: opening_barrier(collector()))
        {verb, args} = args_for(kind, ctx)
        run_id = if kind == :run, do: "run_parity_#{unquote(index)}", else: run_id_of(run_dir)
        command_id = CommandId.generate()
        # the oracle's bindings come from the LIVE sibling Writer's opening, captured under the two-way barrier (the
        # owner blocks before awaiting until acked); the oracle runs only after the invocation returned
        {caller, mon} =
          spawn_monitor(fn -> send(parent, {:invoke_result, invoke(verb, args, run_id, command_id, ctx)}) end)

        track!(caller)
        opened = capture_opening!(30_000)
        {_started, _owner, _sup} = consume_start_traces!(30_000)
        assert_receive {:invoke_result, actual}, 60_000
        assert_receive {:DOWN, ^mon, :process, ^caller, :normal}, 5_000
        stamp = expected_stamp(@operator, verb, args, command_id)
        oracle = oracle_opts(ctx, unquote(index), run_id: run_id, run_lock_path: opened.lock_path)

        oracle =
          if opened.repair, do: Keyword.put(oracle, :tail_repair, Writer.tail_repair_data(opened.repair)), else: oracle

        oracle =
          case kind do
            :run -> oracle
            :resume -> Keyword.put(oracle, :recovery_reason, args["recovery_reason"])
            :cancel -> Keyword.put(oracle, :cancel_reason, args["reason"])
          end

        expected =
          case kind do
            :run -> OwnerOracle.run(ctx[:spec], ctx[:plan], oracle)
            :resume -> OwnerOracle.resume(ctx[:spec], ctx[:plan], prior, oracle)
            :cancel -> OwnerOracle.cancel(prior, oracle)
          end

        assert {:ok, %{summary: summary_e, events: events_e, appended_events: appended_e}} = expected
        # D1 transition pin (m_1788751607000 / m_1788752018000) for the two EXPIRED kill9 resume cases: the independent
        # oracle (direct Effects, unchanged) still completes; the Worker path answers the exact expiry for the due
        # Observe; the acceptance stamp, Writer v2 chain and prefix bytes are asserted on the ACTUAL journal
        expired? = unquote(name) in ["kill9 resume pre_dispatch", "kill9 resume awaiting_artifact"]

        if expired? do
          assert summary_e["status"] == "completed", "the independent oracle still completes: " <> unquote(name)
          # U2b GREEN transition (recorded): pre_dispatch expires at the DISPATCH (blocked, attention-only);
          # awaiting_artifact still expires at the Observe
          if unquote(name) == "kill9 resume pre_dispatch" do
            assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} = actual
            last = run_dir |> journal() |> List.last()
            assert last["type"] == "human_attention_required" and last["data"]["reason"] == "dispatch_deadline_exceeded"
          else
            assert actual == @expired_resume, unquote(name)
          end

          assert String.starts_with?(journal_bytes(run_dir), Enum.join(prior, "\n") <> "\n"), "prefix bytes preserved"
          [first | rest] = run_dir |> journal() |> Enum.drop(length(prior))
          assert first["type"] == "run_resumed" and first["data"]["requested_by"] == stamp, unquote(name)
          refute Enum.any?(rest, &Map.has_key?(&1["data"], "requested_by")), "no derived event carries the stamp"
          refute inspect([first | rest]) =~ @now.wall_ts, "command arrival time is not journal data"

          for e <- [first | rest],
              do: assert(e["schema_version"] == 2 and e["prev_line_sha256"] =~ ~r/\Asha256:[0-9a-f]{64}\z/)

          assert_head_receipt!(run_dir)
        else
          assert {:ok, %{summary: summary_a, events: events_a, appended_events: appended}} = actual
          assert summary_a == summary_e, unquote(name)
          assert length(appended) == length(appended_e), "the oracle and the executor append the same number of events"
          strip = fn e -> Map.drop(e, ["prev_line_sha256", "schema_version"]) end

          if appended_e == [] do
            # a terminal no-op (E-M16): nothing appended, bytes exactly as seeded, no stamp anywhere
            assert Enum.map(events_a, strip) == Enum.map(events_e, strip), unquote(name)
            assert journal_bytes(run_dir) == Enum.join(prior, "\n") <> "\n"
            refute Enum.any?(events_a, &Map.has_key?(&1["data"], "requested_by"))
          else
            acceptance = %{run: "run_created", resume: "run_resumed", cancel: "run_cancel_requested"}[kind]
            [first | _] = appended
            assert first["type"] == acceptance, unquote(name)
            assert first["data"]["requested_by"] == stamp, "the acceptance event carries exactly the independent stamp"
            refute Enum.any?(tl(appended), &Map.has_key?(&1["data"], "requested_by")), "no derived event carries it"
            refute inspect(appended) =~ @now.wall_ts, "command arrival time is not journal data"
            # Writer envelope facts are asserted on their own, never dropped blindly: v2 with a chained hash
            for e <- appended,
                do: assert(e["schema_version"] == 2 and e["prev_line_sha256"] =~ ~r/\Asha256:[0-9a-f]{64}\z/)

            at = length(events_e) - length(appended)
            expected_events = List.update_at(Enum.map(events_e, strip), at, &put_in(&1, ["data", "requested_by"], stamp))
            assert Enum.map(events_a, strip) == expected_events, unquote(name)
          end
        end
      end
    end

    for verb <- ["resume", "cancel"] do
      test "#{verb} with a command.run_id foreign to the locked accepted prefix is command_run_mismatch, bytes unchanged" do
        require_executor!()
        run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
        before = journal_bytes(run_dir)
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), effect_observer: observer_to(self()))
        args = if unquote(verb) == "resume", do: %{"recovery_reason" => "x"}, else: %{"reason" => "x"}

        assert {:error, %{clause: "command_run_mismatch"}} =
                 invoke(unquote(verb), args, "run_foreign_0001", CommandId.generate(), ctx)

        assert journal_bytes(run_dir) == before
        refute_received {:effect_ran, _}
        assert_released!(run_dir)
      end
    end

    test "a retried START at a foreign command.run_id is command_run_mismatch before any replay or execution" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:ok, _} = invoke("start", args, "run_e4_0001", command_id, ctx)
      before = journal_bytes(run_dir)
      retry_ctx = Keyword.put(ctx, :effect_observer, observer_to(self()))

      assert {:error, %{clause: "command_run_mismatch"}} =
               invoke("start", args, "run_foreign_0002", command_id, retry_ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end
  end

  # =================================================================================================
  describe "E-5 (B) idempotency under Writer ownership (the durable-prefix decision table)" do
    for kind <- [:run, :resume, :cancel] do
      test "completed #{kind} retried with the same stamp replays: same summary, nothing appended, zero effects" do
        require_executor!()

        run_dir =
          if unquote(kind) == :run,
            do: tmp_run_dir(),
            else: seed_legacy!(tmp_run_dir(), live(kill9("events_pre_dispatch.jsonl")))

        {scenario, index} =
          if unquote(kind) == :run, do: {"gated_run_seed", gated_index()}, else: {"kill9_resume", pre_dispatch_index()}

        # derived-live input for the resume kind (labelled): the fresh owner must arm with the deadline ahead
        ctx = context(run_dir, scenario, index, observe_fence_observer: self())
        {verb, args} = args_for(unquote(kind), ctx)
        run_id = if unquote(kind) == :run, do: "run_e5_replay", else: run_id_of(run_dir)
        command_id = CommandId.generate()
        assert {:ok, %{summary: summary}} = invoke(verb, args, run_id, command_id, ctx)
        if unquote(kind) == :resume, do: assert_live_arm!()
        before = journal_bytes(run_dir)
        retry_ctx = Keyword.put(ctx, :effect_observer, observer_to(self()))
        assert {:ok, %{summary: ^summary, appended_events: []}} = invoke(verb, args, run_id, command_id, retry_ctx)
        assert journal_bytes(run_dir) == before
        refute_received {:effect_ran, _}
        assert_released!(run_dir)
      end
    end

    test "a run blocked on attention is a completion boundary: the start retry REPLAYS the blocked result" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "auth_blocked_pane", blocked_index())
      command_id = CommandId.generate()
      args = start_args(ctx)

      assert {:ok, %{summary: %{"status" => "blocked"} = summary}} =
               invoke("start", args, "run_e5_blocked", command_id, ctx)

      before = journal_bytes(run_dir)
      retry_ctx = context(run_dir, "auth_blocked_pane", blocked_index(), effect_observer: observer_to(self()))

      assert {:ok, %{summary: ^summary, appended_events: []}} =
               invoke("start", args, "run_e5_blocked", command_id, retry_ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "same command_id with a different actor id, actor class, verb or args is idempotency_conflict naming the field" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:ok, _} = invoke("start", args, "run_e5_0002", command_id, ctx)
      before = journal_bytes(run_dir)

      assert {:error, %{clause: "idempotency_conflict", field: "id"}} =
               invoke("start", args, "run_e5_0002", command_id, ctx, %{@operator | "id" => "other_operator"})

      assert {:error, %{clause: "idempotency_conflict", field: "class"}} =
               invoke("start", args, "run_e5_0002", command_id, ctx, @console)

      assert {:error, %{clause: "idempotency_conflict", field: "verb"}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, "run_e5_0002", command_id, ctx)

      changed = %{args | "plan_hash" => "sha256:" <> String.duplicate("c", 64)}

      assert {:error, %{clause: "idempotency_conflict", field: "args_hash"}} =
               invoke("start", changed, "run_e5_0002", command_id, Keyword.put(ctx, :plan_hash, changed["plan_hash"]))

      assert journal_bytes(run_dir) == before
    end

    for {label, seed} <- [{"an EMPTY existing journal", :empty}, {"a legacy journal without a stamp", :legacy}] do
      test "start on #{label} takes the second attempt and stays journal_exists: zero matching rows never execute" do
        require_executor!()
        run_dir = tmp_run_dir()

        if unquote(seed) == :empty,
          do: File.write!(Path.join(run_dir, "events.jsonl"), ""),
          else: seed_legacy!(run_dir, kill9("events_pre_dispatch.jsonl"))

        before = journal_bytes(run_dir)
        ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))

        assert {:error, %{clause: "journal_exists"}} =
                 invoke("start", start_args(ctx), "run_e5_zero", CommandId.generate(), ctx)

        assert journal_bytes(run_dir) == before
        refute_received {:effect_ran, _}
        assert_released!(run_dir)
      end
    end

    test "a different command_id start on an existing journal is the Writer's journal_exists (second attempt admits only a match)" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      assert {:ok, _} = invoke("start", start_args(ctx), "run_e5_0003", CommandId.generate(), ctx)
      before = journal_bytes(run_dir)

      assert {:error, %{clause: "journal_exists"}} =
               invoke("start", start_args(ctx), "run_e5_0003", CommandId.generate(), ctx)

      assert journal_bytes(run_dir) == before
    end

    for kind <- [:run, :resume, :cancel] do
      test "#{kind}: acceptance durable before the reply -> run_server_down; the retry continues with ONE acceptance" do
        require_executor!()

        run_dir =
          if unquote(kind) == :run,
            do: tmp_run_dir(),
            else: seed_legacy!(tmp_run_dir(), live(kill9("events_pre_dispatch.jsonl")))

        {scenario, index} =
          if unquote(kind) == :run, do: {"gated_run_seed", gated_index()}, else: {"kill9_resume", pre_dispatch_index()}

        prior_count = if unquote(kind) == :run, do: 0, else: length(journal(run_dir))
        fs = FaultFs.new()
        kill_after_receipt(fs, self(), 1)
        ctx = context(run_dir, scenario, index, fs: fs)
        {verb, args} = args_for(unquote(kind), ctx)
        run_id = if unquote(kind) == :run, do: "run_e5_durable", else: run_id_of(run_dir)
        command_id = CommandId.generate()
        assert {:error, %{clause: "run_server_down"}} = invoke(verb, args, run_id, command_id, ctx)
        assert_received {:durable, 1}
        events = journal(run_dir)
        acceptance = %{run: "run_created", resume: "run_resumed", cancel: "run_cancel_requested"}[unquote(kind)]
        assert length(events) == prior_count + 1 and List.last(events)["type"] == acceptance
        assert List.last(events)["data"]["requested_by"]["command_id"] == command_id
        assert_released!(run_dir)

        retry_ctx = context(run_dir, scenario, index, observe_fence_observer: self())
        assert {:ok, %{summary: summary, events: after_events}} = invoke(verb, args, run_id, command_id, retry_ctx)
        if unquote(kind) == :resume, do: assert_live_arm!()

        stamped =
          Enum.count(after_events, &(&1["type"] == acceptance and &1["data"]["requested_by"]["command_id"] == command_id))

        assert stamped == 1, "no second acceptance"
        assert Enum.all?(after_events, &(&1["run_id"] == run_id))

        case unquote(kind) do
          :run ->
            assert(summary["status"] == "completed" and Enum.count(after_events, &(&1["type"] == "run_started")) == 1)

          :resume ->
            assert(summary["status"] == "completed")

          :cancel ->
            assert(summary["status"] == "cancelled" and Enum.count(after_events, &(&1["type"] == "run_cancelled")) == 1)
        end
      end
    end

    test "cut after run_started, before any dispatch: the retry CONTINUES (one run_started, one dispatch, completed)" do
      require_executor!()
      run_dir = tmp_run_dir()

      started_seq =
        seq_of(oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "run_started")

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, dispatch: CountingDispatch)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_e5_started", command_id, ctx)
      assert_received {:durable, ^started_seq}
      assert length(journal(run_dir)) == started_seq and List.last(journal(run_dir))["type"] == "run_started"
      assert hd(journal(run_dir))["data"]["requested_by"]["command_id"] == command_id
      assert Seam.get({:delivered, "as_0001"}) == nil, "no dispatch happened before the cut"
      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("start", args, "run_e5_started", command_id, retry_ctx)

      assert Enum.count(events, &(&1["type"] == "run_started")) == 1 and
               Enum.count(events, &(&1["type"] == "run_created")) == 1

      assert Seam.get({:delivered, "as_0001"}) == 1
    end

    test "an interrupted dispatch consequence is re-observed on retry: one assignment_dispatch_sent, one adapter delivery" do
      require_executor!()
      run_dir = tmp_run_dir()

      dispatch_seq =
        seq_of(
          oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"),
          "assignment_dispatch_sent"
        )

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), dispatch_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, dispatch: CountingDispatch)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_e5_dispatch", command_id, ctx)
      assert_received {:durable, ^dispatch_seq}
      assert List.last(journal(run_dir))["type"] == "assignment_dispatch_sent"
      assert Seam.get({:delivered, "as_0001"}) == 1
      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("start", args, "run_e5_dispatch", command_id, retry_ctx)

      assert Enum.count(events, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == "as_0001")) ==
               1

      assert Seam.get({:delivered, "as_0001"}) == 1, "the interrupted delivery is re-observed, never re-sent"
    end

    # MEASURED during C GREEN (disclosed; prose corrected per C-M1): on this cut prefix the EXISTING recovery (plain
    # Host.resume) releases the stale leases, runs exactly ONE ReconcileGate, and only then - on the reconcile
    # verdict's clock read, earlier than the journaled gate start under the harness FixedClock - blocks with
    # human_attention_required gate_recovery_clock_skew; no fresh PrepareGate/ReleaseGate/AwaitGate runs. The RED
    # wording ("completed") assumed a clock this harness does not provide. The row pins: the continuation IS the
    # existing gate recovery (same appended types and status as plain resume on the same prefix, minus the run_resumed
    # acceptance), one run_created, no run_resumed, and the strict one-reconcile witness.
    test "cut after gate_started: the retry continues through the EXISTING gate recovery (parity with plain resume, no run_resumed)" do
      require_executor!()
      run_dir = tmp_run_dir()

      gate_seq =
        seq_of(oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "gate_started")

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), gate_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_e5_gate", command_id, ctx)
      assert_received {:durable, ^gate_seq}
      assert length(journal(run_dir)) == gate_seq and List.last(journal(run_dir))["type"] == "gate_started"
      assert hd(journal(run_dir))["data"]["requested_by"]["command_id"] == command_id

      # the oracle: the existing recovery on the same prefix (harness sink, never the journal file)
      cut_lines = run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)

      assert {:ok, %{summary: plain_summary, appended_events: plain_appended}} =
               Host.resume(ctx[:spec], ctx[:plan], cut_lines, oracle_opts(ctx, gated_index(), run_id: "run_e5_gate"))

      assert hd(plain_appended)["type"] == "run_resumed"
      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: ordered_observer(:gate_effects))
      assert {:ok, %{summary: summary, events: events}} = invoke("start", args, "run_e5_gate", command_id, retry_ctx)
      continued = Enum.drop(events, gate_seq)
      assert Enum.map(continued, & &1["type"]) == Enum.map(tl(plain_appended), & &1["type"])
      assert summary["status"] == plain_summary["status"]
      gate_effects = Enum.filter(ordered_effects(:gate_effects), &(&1 in @gate_family))

      assert gate_effects == [Effect.ReconcileGate],
             "the clock-skew recovery must reconcile once, with no fresh prepare/release/await"

      assert Enum.count(events, &(&1["type"] == "run_created")) == 1
      refute Enum.any?(events, &(&1["type"] == "run_resumed")), "continuation is not a new acceptance"
    end

    test "a later acceptance before completion: the start retry is command_superseded while the resume retry continues" do
      require_executor!()
      run_dir = tmp_run_dir()

      started_seq =
        seq_of(oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "run_started")

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      start_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_e5_later", start_id, ctx)
      assert_received {:durable, ^started_seq}
      assert length(journal(run_dir)) == started_seq and List.last(journal(run_dir))["type"] == "run_started"
      # a resume is accepted (durable) and interrupted right after its own acceptance: for THIS invocation's FaultFs the
      # acceptance is its FIRST receipt (ordinals are invocation-relative, never journal seqs)
      fs2 = FaultFs.new()
      kill_after_receipt(fs2, self(), 1)
      resume_id = CommandId.generate()
      resume_args = %{"recovery_reason" => "operator_resume"}

      assert {:error, %{clause: "run_server_down"}} =
               invoke(
                 "resume",
                 resume_args,
                 "run_e5_later",
                 resume_id,
                 context(run_dir, "gated_run_seed", gated_index(), fs: fs2)
               )

      assert_received {:durable, 1}
      assert length(journal(run_dir)) == started_seq + 1
      assert List.last(journal(run_dir))["type"] == "run_resumed"
      assert List.last(journal(run_dir))["data"]["requested_by"]["command_id"] == resume_id
      before = journal_bytes(run_dir)
      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))
      assert {:error, %{clause: "command_superseded"}} = invoke("start", args, "run_e5_later", start_id, retry_ctx)
      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("resume", resume_args, "run_e5_later", resume_id, context(run_dir, "gated_run_seed", gated_index()))

      assert Enum.count(events, &(&1["type"] == "run_resumed")) == 1 and
               Enum.count(events, &(&1["type"] == "run_created")) == 1
    end

    test "completion BEFORE a later acceptance: a blocked start then an accepted cancel - both retries replay the current fold" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "auth_blocked_pane", blocked_index())
      start_id = CommandId.generate()
      args = start_args(ctx)
      assert {:ok, %{summary: %{"status" => "blocked"}}} = invoke("start", args, "run_e5_iv", start_id, ctx)
      cancel_id = CommandId.generate()
      cancel_args = %{"reason" => "operator_cancel"}

      assert {:ok, %{summary: %{"status" => "cancelled"} = cancelled, appended_events: [requested | _]}} =
               invoke(
                 "cancel",
                 cancel_args,
                 "run_e5_iv",
                 cancel_id,
                 context(run_dir, "auth_blocked_pane", blocked_index())
               )

      assert requested["type"] == "run_cancel_requested" and requested["data"]["requested_by"]["command_id"] == cancel_id
      before = journal_bytes(run_dir)
      # the start's OWN interval (before the cancel acceptance) ends in the blocking attention: complete -> replay
      retry_ctx = context(run_dir, "auth_blocked_pane", blocked_index(), effect_observer: observer_to(self()))
      assert {:ok, %{summary: ^cancelled, appended_events: []}} = invoke("start", args, "run_e5_iv", start_id, retry_ctx)

      assert {:ok, %{summary: ^cancelled, appended_events: []}} =
               invoke("cancel", cancel_args, "run_e5_iv", cancel_id, retry_ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "run_budget_exhausted is a terminal: fresh resume and cancel on it are no-ops (nothing appended, no stamp)" do
      require_executor!()
      run_dir = seed_legacy!(tmp_run_dir(), budget_lines())
      run_id = run_id_of(run_dir)
      before = journal_bytes(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))

      assert {:ok, %{summary: %{"status" => "budget_exhausted"}, appended_events: []}} =
               invoke("resume", %{"recovery_reason" => "x"}, run_id, CommandId.generate(), ctx)

      assert {:ok, %{summary: %{"status" => "budget_exhausted"}, appended_events: []}} =
               invoke("cancel", %{"reason" => "x"}, run_id, CommandId.generate(), ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    # ---- U1a-R (docs/contracts/recovery-reservation.org): history injected between a durable stamped start and its
    # retry. The injected line is CHAIN-STAMPED on the Writer's real v2 prefix and the receipt is advanced, so the chain
    # and the Reader accept the prefix before any command decision; the baseline-known control proves the mechanics on
    # the unchanged lib with a typed reserved type the Reader already knows; the RED rows use the new type.
    defp inject_history!(run_dir, event_fields, advance_receipt?) do
      raw = journal_bytes(run_dir)
      last_line = raw |> String.split("\n", trim: true) |> List.last()
      seq = Jason.decode!(last_line)["seq"] + 1

      line =
        event_fields
        |> Map.merge(%{
          "schema" => "ai-orchestrator/journal-event",
          "schema_version" => 2,
          "event_version" => 1,
          "seq" => seq,
          "event_id" => "ev_injected_#{seq}",
          "ts" => "2026-09-05T20:00:01Z",
          "run_id" => Jason.decode!(last_line)["run_id"],
          "actor" => "run_supervisor",
          "prev_line_sha256" => Chain.line_sha256(last_line <> "\n")
        })
        |> Jason.encode!()

      File.write!(Path.join(run_dir, "events.jsonl"), line <> "\n", [:append])

      if advance_receipt? do
        receipt =
          Chain.encode_receipt(%{
            seq: seq,
            line_sha256: Chain.line_sha256(line <> "\n"),
            updated_at: FixedClock.wall_ts()
          })

        File.write!(Path.join(run_dir, "events.head"), receipt)
      end

      seq
    end

    defp durable_start!(run_dir, run_id) do
      started_seq =
        seq_of(oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "run_started")

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      # the counting adapter is wired from the interrupted start on, so a delivery can never escape the count
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, dispatch: CountingDispatch)
      start_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, run_id, start_id, ctx)
      assert_received {:durable, ^started_seq}
      assert Seam.get({:delivered, "as_0001"}) == nil, "no dispatch happened before the cut"
      {started_seq, start_id, args}
    end

    test "C-8 control: chain-stamped KNOWN history after a durable start is accepted, stale receipt repaired, retry completes once" do
      require_executor!()
      run_dir = tmp_run_dir()
      {started_seq, start_id, args} = durable_start!(run_dir, "run_e5_known_history")

      known = %{
        "type" => "stop_policy_evaluated",
        "data" => %{"policy_id" => "sp_0001", "scope" => "run", "reason" => "checkpoint", "decision" => "continue"}
      }

      # stale receipt first: the Reader plans the repair (advance_receipt) and the chain accepts the prefix
      seq = inject_history!(run_dir, known, false)
      assert seq == started_seq + 1
      assert {:ok, %{envelope_version: 2, count: ^seq}} = Chain.verify(journal_bytes(run_dir))
      assert {:ok, %{pending_repair: %{action: :advance_receipt}}} = Reader.load(run_dir)
      # then advance the receipt as the Writer would: no repair pending
      receipt =
        Chain.encode_receipt(%{
          seq: seq,
          line_sha256:
            Chain.line_sha256((run_dir |> journal_bytes() |> String.split("\n", trim: true) |> List.last()) <> "\n"),
          updated_at: FixedClock.wall_ts()
        })

      File.write!(Path.join(run_dir, "events.head"), receipt)
      assert {:ok, %{pending_repair: nil}} = Reader.load(run_dir)
      before = journal_bytes(run_dir)

      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("start", args, "run_e5_known_history", start_id, retry_ctx)

      assert Seam.get({:delivered, "as_0001"}) == 1, "exactly one adapter delivery on the continuation"

      assert Enum.count(events, &(&1["type"] == "run_created")) == 1 and
               Enum.count(events, &(&1["type"] == "run_started")) == 1

      assert hd(events)["data"]["requested_by"]["command_id"] == start_id
      assert Enum.count(events, &(&1["type"] == "stop_policy_evaluated")) == 1
      assert String.starts_with?(journal_bytes(run_dir), before)
      # the completed run replays on an identical retry: nothing appended
      assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} =
               invoke(
                 "start",
                 args,
                 "run_e5_known_history",
                 start_id,
                 context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)
               )

      assert Seam.get({:delivered, "as_0001"}) == 1, "a replay delivers nothing"
    end

    test "R-7 a chain-stamped reservation between a stamped start and its retry: the retry CONTINUES once, then replays" do
      require_executor!()
      run_dir = tmp_run_dir()
      {started_seq, start_id, args} = durable_start!(run_dir, "run_e5_reserved")

      reservation = %{
        "type" => "run_recovery_reserved",
        "data" => %{
          "attempt" => 1,
          "limit" => 3,
          "cause_class" => "writer_exit",
          "lost_generation" => 1,
          "authority" => "executor"
        }
      }

      seq = inject_history!(run_dir, reservation, true)
      assert seq == started_seq + 1

      # chain-level acceptance is type-agnostic and holds today; the Reader/fold acceptance is the intended missing
      # behaviour
      assert {:ok, %{envelope_version: 2, count: ^seq}} = Chain.verify(journal_bytes(run_dir))
      assert {:ok, %{pending_repair: nil}} = Reader.load(run_dir)
      before = journal_bytes(run_dir)
      assert {:ok, %{status: "in_flight"} = folded} = Fold.fold_lines(String.split(before, "\n", trim: true))
      assert Map.get(Map.from_struct(folded), :recovery_reservations) == 1

      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("start", args, "run_e5_reserved", start_id, retry_ctx)

      assert Seam.get({:delivered, "as_0001"}) == 1, "exactly one adapter delivery on the continuation"

      assert Enum.count(events, &(&1["type"] == "run_created")) == 1 and
               Enum.count(events, &(&1["type"] == "run_started")) == 1

      assert hd(events)["data"]["requested_by"]["command_id"] == start_id
      assert Enum.count(events, &(&1["type"] == "run_recovery_reserved")) == 1
      assert String.starts_with?(journal_bytes(run_dir), before), "the reservation line survives the retry"
      assert {:ok, final} = Fold.fold_lines(Enum.map(events, &Jason.encode!/1))
      assert Map.get(Map.from_struct(final), :recovery_reservations) == 1
      # COMPLETE counterpart: an identical retry replays with nothing appended
      assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} =
               invoke(
                 "start",
                 args,
                 "run_e5_reserved",
                 start_id,
                 context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)
               )

      assert Seam.get({:delivered, "as_0001"}) == 1, "a replay delivers nothing"
    end

    test "R-7b a reservation does not soften the later-acceptance rule: a later stamped resume still supersedes the start retry" do
      require_executor!()
      run_dir = tmp_run_dir()
      {started_seq, start_id, args} = durable_start!(run_dir, "run_e5_reserved_super")

      reservation = %{
        "type" => "run_recovery_reserved",
        "data" => %{
          "attempt" => 1,
          "limit" => 3,
          "cause_class" => "writer_timeout",
          "lost_generation" => 1,
          "authority" => "host"
        }
      }

      seq = inject_history!(run_dir, reservation, true)
      assert seq == started_seq + 1
      fs2 = FaultFs.new()
      kill_after_receipt(fs2, self(), 1)
      resume_id = CommandId.generate()

      assert {:error, %{clause: "run_server_down"}} =
               invoke(
                 "resume",
                 %{"recovery_reason" => "operator_resume"},
                 "run_e5_reserved_super",
                 resume_id,
                 context(run_dir, "gated_run_seed", gated_index(), fs: fs2)
               )

      assert_received {:durable, 1}
      assert List.last(journal(run_dir))["type"] == "run_resumed"
      before = journal_bytes(run_dir)

      assert {:error, %{clause: "command_superseded"}} =
               invoke("start", args, "run_e5_reserved_super", start_id, context(run_dir, "gated_run_seed", gated_index()))

      assert journal_bytes(run_dir) == before
    end

    test "an incomplete acceptance whose run ended otherwise (cancelled meanwhile) is command_superseded, never continued" do
      require_executor!()
      run_dir = tmp_run_dir()
      # measured while turning GREEN: a cancel right after run_created is a preamble_violation (the journal requires
      # run_spec_loaded/plan_recorded/run_started first), so the interruption is placed after run_started
      started_seq =
        seq_of(oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "run_started")

      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      start_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_e5_super", start_id, ctx)
      assert_received {:durable, ^started_seq}
      assert length(journal(run_dir)) == started_seq and List.last(journal(run_dir))["type"] == "run_started"
      assert hd(journal(run_dir))["data"]["requested_by"]["command_id"] == start_id
      cancel_ctx = context(run_dir, "gated_run_seed", gated_index())

      assert {:ok, %{summary: %{"status" => "cancelled"}}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, "run_e5_super", CommandId.generate(), cancel_ctx)

      before = journal_bytes(run_dir)
      # the start's OWN interval (before the cancel acceptance) holds no terminal and no block: incomplete + later
      # acceptance -> superseded, even though the later command terminated the run
      retry_ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))
      assert {:error, %{clause: "command_superseded"}} = invoke("start", args, "run_e5_super", start_id, retry_ctx)
      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
      assert Enum.count(journal(run_dir), &(&1["type"] == "run_started")) == 1, "no consequence appended after a terminal"
    end

    for {label, boundary} <- [{"an unstamped later CANCEL", :cancel}, {"an unstamped later RESUME", :resume}] do
      test "#{label} bounds the stamped start's interval: the start retry is command_superseded even though the run ended" do
        require_executor!()
        legacy = kill9("events_pre_dispatch.jsonl")
        start_id = CommandId.generate()
        args = %{"spec_hash" => @zero, "plan_hash" => @zero}
        prior = stamp_first(legacy, expected_stamp(@operator, "start", args, start_id))
        index = if unquote(boundary) == :cancel, do: 10, else: pre_dispatch_index()

        {:ok, %{events: events}} =
          case unquote(boundary) do
            :cancel -> Host.cancel(prior, fresh_opts(index))
            :resume -> Host.resume(H.spec("kill9_resume"), H.plan("kill9_resume"), prior, fresh_opts(index))
          end

        boundary_type = %{cancel: "run_cancel_requested", resume: "run_resumed"}[unquote(boundary)]
        assert Enum.any?(events, &(&1["type"] == boundary_type and not Map.has_key?(&1["data"], "requested_by")))

        assert List.last(events)["type"] in ["run_cancelled", "run_completed"],
               "the LATER unstamped command ended the run"

        run_dir = seed_legacy!(tmp_run_dir(), legacy_lines(events))
        run_id = run_id_of(run_dir)
        before = journal_bytes(run_dir)

        ctx =
          context(run_dir, "kill9_resume", pre_dispatch_index(),
            spec_hash: @zero,
            plan_hash: @zero,
            effect_observer: observer_to(self())
          )

        # the stamped start's own interval ends at the unstamped boundary and holds no terminal or block
        assert {:error, %{clause: "command_superseded"}} = invoke("start", args, run_id, start_id, ctx)
        assert journal_bytes(run_dir) == before
        refute_received {:effect_ran, _}
      end
    end

    test "a stamped start BLOCKED before an unstamped cancel is complete in its own interval: the retry replays" do
      require_executor!()
      blocked_ctx = context(tmp_run_dir(), "auth_blocked_pane", blocked_index())
      start_id = CommandId.generate()
      args = %{"spec_hash" => @zero, "plan_hash" => @zero}
      blocked = oracle_run(blocked_ctx, blocked_index(), "run_iv_0001")
      assert Enum.any?(blocked, &(&1["type"] == "human_attention_required"))
      prior = stamp_first(legacy_lines(blocked), expected_stamp(@operator, "start", args, start_id))
      {:ok, %{events: events}} = Host.cancel(prior, fresh_opts(10))
      assert List.last(events)["type"] == "run_cancelled"
      run_dir = seed_legacy!(tmp_run_dir(), legacy_lines(events))
      before = journal_bytes(run_dir)

      ctx =
        context(run_dir, "auth_blocked_pane", blocked_index(),
          spec_hash: @zero,
          plan_hash: @zero,
          effect_observer: observer_to(self())
        )

      assert {:ok, %{summary: %{"status" => "cancelled"}, appended_events: []}} =
               invoke("start", args, "run_iv_0001", start_id, ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "a stamped start whose own interval ends in run_budget_exhausted is complete: the retry replays" do
      require_executor!()
      start_id = CommandId.generate()
      args = %{"spec_hash" => @zero, "plan_hash" => @zero}

      run_dir =
        seed_legacy!(tmp_run_dir(), stamp_first(budget_lines(), expected_stamp(@operator, "start", args, start_id)))

      before = journal_bytes(run_dir)

      ctx =
        context(run_dir, "gated_run_seed", gated_index(),
          spec_hash: @zero,
          plan_hash: @zero,
          effect_observer: observer_to(self())
        )

      assert {:ok, %{summary: %{"status" => "budget_exhausted"}, appended_events: []}} =
               invoke("start", args, run_id_of(run_dir), start_id, ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "duplicate acceptance rows for one command_id are acceptance_ambiguous (fail closed), bytes unchanged" do
      require_executor!()
      prior = kill9("events_pre_dispatch.jsonl")
      %{"run_id" => run_id, "seq" => last_seq} = prior |> List.last() |> Jason.decode!()
      command_id = CommandId.generate()
      stamp = expected_stamp(@operator, "resume", %{"recovery_reason" => "operator_resume"}, command_id)

      resumed = fn seq ->
        Jason.encode!(%{
          "schema" => "ai-orchestrator/journal-event",
          "schema_version" => 1,
          "event_version" => 1,
          "seq" => seq,
          "event_id" => "ev_" <> String.pad_leading(Integer.to_string(seq), 4, "0"),
          "type" => "run_resumed",
          "ts" => "2026-09-01T12:00:00Z",
          "run_id" => run_id,
          "actor" => "run_supervisor",
          "data" => %{
            "supervisor_instance" => @instance,
            "last_seen_seq" => seq - 1,
            "recovery_reason" => "operator_resume",
            "requested_by" => stamp
          }
        })
      end

      run_dir = seed_legacy!(tmp_run_dir(), prior ++ [resumed.(last_seq + 1), resumed.(last_seq + 2)])
      before = journal_bytes(run_dir)
      ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), effect_observer: observer_to(self()))

      assert {:error, %{clause: "acceptance_ambiguous"}} =
               invoke("resume", %{"recovery_reason" => "operator_resume"}, run_id, command_id, ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}

      # D-M1 (review m_1788669010000): under the EXPLICIT restart the same duplicate-stamped NONEMPTY prefix is
      # journal_exists - the verified-nonempty refusal precedes stamp classification; the ordinary control above stays
      assert {:error, %{clause: "journal_exists"}} =
               invoke("start", start_args(ctx), run_id, command_id, Keyword.put(ctx, :restart_empty, true))

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
      assert_released!(run_dir)
    end
  end

  # =================================================================================================
  describe "C (continuation) RED/interface: the bounded internal continuation arm (ruling R-B1 = c)" do
    defp preamble_types, do: ~w(run_created run_spec_loaded plan_recorded run_started)

    for {label, cut_type} <- [
          {"run_created", "run_created"},
          {"run_spec_loaded", "run_spec_loaded"},
          {"plan_recorded", "plan_recorded"},
          {"run_started", "run_started"}
        ] do
      test "C-1 start cut after #{label}: the retry emits only the missing preamble suffix, no run_resumed, completed" do
        require_executor!()
        run_dir = tmp_run_dir()

        cut_seq =
          seq_of(
            oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"),
            unquote(cut_type)
          )

        fs = FaultFs.new()
        kill_after_receipt(fs, self(), cut_seq)
        ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
        command_id = CommandId.generate()
        args = start_args(ctx)
        assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_c1", command_id, ctx)
        assert_received {:durable, ^cut_seq}
        assert length(journal(run_dir)) == cut_seq and List.last(journal(run_dir))["type"] == unquote(cut_type)
        before = journal_bytes(run_dir)
        retry_ctx = context(run_dir, "gated_run_seed", gated_index())

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 invoke("start", args, "run_c1", command_id, retry_ctx)

        for type <- preamble_types(), do: assert(Enum.count(events, &(&1["type"] == type)) == 1, type)
        refute Enum.any?(events, &(&1["type"] == "run_resumed")), "continuation is not a new acceptance"
        assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1, "one acceptance row, ever"
        assert String.starts_with?(journal_bytes(run_dir), before), "the accepted prefix is byte-exact preserved"
      end
    end

    # the continuation's own receipts are invocation-relative: receipt n = the nth line it appends after run_started
    test "C-2 a crash DURING the continuation (at its dispatch receipt): the next retry continues once more; one acceptance ever" do
      require_executor!()
      run_dir = tmp_run_dir()
      oracle = oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle")
      started_seq = seq_of(oracle, "run_started")
      dispatch_rel = seq_of(oracle, "assignment_dispatch_sent") - started_seq
      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, dispatch: CountingDispatch)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_c2", command_id, ctx)
      assert_received {:durable, ^started_seq}
      fs2 = FaultFs.new()
      kill_after_receipt(fs2, self(), dispatch_rel)

      assert {:error, %{clause: "run_server_down"}} =
               invoke(
                 "start",
                 args,
                 "run_c2",
                 command_id,
                 context(run_dir, "gated_run_seed", gated_index(), fs: fs2, dispatch: CountingDispatch)
               )

      assert_received {:durable, ^dispatch_rel}
      assert length(journal(run_dir)) == started_seq + dispatch_rel
      assert List.last(journal(run_dir))["type"] == "assignment_dispatch_sent"
      assert Seam.get({:delivered, "as_0001"}) == 1
      before = journal_bytes(run_dir)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke(
                 "start",
                 args,
                 "run_c2",
                 command_id,
                 context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)
               )

      assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1
      refute Enum.any?(events, &(&1["type"] == "run_resumed"))

      assert Enum.count(events, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == "as_0001")) ==
               1

      assert Seam.get({:delivered, "as_0001"}) == 1, "the interrupted delivery is re-observed, never re-sent"
      assert String.starts_with?(journal_bytes(run_dir), before)
    end

    # Baseline control (imported from the review fact probe): on this seed Host.cancel appends request/release PAIRS,
    # workspace first, then pane, then run_cancelled; the identities are workspace_lease_id / pane_ref, never lease_id.
    test "C-3 baseline: cancel on the awaiting_artifact seed is request/release pairs then run_cancelled (measured)" do
      lines = kill9("events_awaiting_artifact.jsonl")
      {workspace_ids, pane_refs} = expected_lease_identities(lines)
      assert workspace_ids == ["wsl_as_0001"] and pane_refs == ["pane_writer"]
      H.reset_seams()
      {_, :cancel, _, _, opts_fun} = Enum.at(H.cases(), 2)
      {:ok, %{appended_events: suffix}} = Host.cancel(lines, opts_fun.())

      assert Enum.map(suffix, & &1["type"]) == [
               "run_cancel_requested",
               "workspace_lease_release_requested",
               "workspace_lease_released",
               "pane_lease_release_requested",
               "pane_lease_released",
               "run_cancelled"
             ]

      [_, ws_req, ws_rel, pane_req, pane_rel, _] = suffix
      assert ws_req["data"]["release_request_id"] == ws_rel["data"]["release_request_id"]
      assert ws_rel["data"]["workspace_lease_id"] == "wsl_as_0001"
      assert pane_req["data"]["release_request_id"] == pane_rel["data"]["release_request_id"]
      assert pane_rel["data"]["pane_ref"] == "pane_writer"
      assert ws_rel["data"]["lease_id"] == nil and ws_rel["data"]["workspace_id"] == nil
    end

    # receipt n of the cancel invocation = the nth appended line: 2 = pending workspace REQUEST, 3 = completed workspace
    # release, 4 = pending pane REQUEST
    for {label, cut, check} <- [
          {"pending workspace request (receipt 2)", 2, :workspace_pending},
          {"completed workspace release (receipt 3)", 3, :workspace_released},
          {"pending pane request (receipt 4)", 4, :pane_pending}
        ] do
      test "C-3 cancel cut at a #{label}: the retry completes the pairs exactly once, one acceptance, one run_cancelled" do
        require_executor!()
        lines = kill9("events_awaiting_artifact.jsonl")
        {[workspace_id], [pane_ref]} = expected_lease_identities(lines)
        run_dir = seed_legacy!(tmp_run_dir(), lines)
        run_id = run_id_of(run_dir)
        prior = length(lines)
        fs = FaultFs.new()
        kill_after_receipt(fs, self(), unquote(cut))
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), fs: fs)
        command_id = CommandId.generate()
        args = %{"reason" => "operator_cancel"}
        assert {:error, %{clause: "run_server_down"}} = invoke("cancel", args, run_id, command_id, ctx)
        assert_received {:durable, unquote(cut)}
        cut_events = journal(run_dir)
        assert length(cut_events) == prior + unquote(cut)
        suffix = Enum.drop(cut_events, prior)
        assert hd(suffix)["type"] == "run_cancel_requested"
        {:ok, fold} = Fold.fold_lines(Enum.map(cut_events, &Jason.encode!/1))

        # positive controls on the cut prefix: what the continuation must NOT redo and what it must still do
        case unquote(check) do
          :workspace_pending ->
            assert List.last(suffix)["type"] == "workspace_lease_release_requested"
            assert Map.keys(fold.active_workspace_leases) == [workspace_id], "the workspace lease is still held"

          :workspace_released ->
            assert List.last(suffix)["type"] == "workspace_lease_released"
            assert fold.active_workspace_leases == %{}, "the workspace lease is gone"

            held = for {_, a} <- fold.assignments, a[:pane_lease?], do: a[:pane_ref]
            assert held == [pane_ref], "the pane lease remains"

          :pane_pending ->
            assert List.last(suffix)["type"] == "pane_lease_release_requested"
            assert fold.active_workspace_leases == %{}
        end

        before = journal_bytes(run_dir)

        pending_ids =
          suffix
          |> Enum.filter(&String.ends_with?(&1["type"], "_release_requested"))
          |> Enum.map(& &1["data"]["release_request_id"])

        assert {:ok, %{summary: %{"status" => "cancelled"}, events: events}} =
                 invoke("cancel", args, run_id, command_id, context(run_dir, "kill9_resume", pre_dispatch_index()))

        assert String.starts_with?(journal_bytes(run_dir), before), "the accepted prefix is byte-exact preserved"
        assert Enum.count(events, &(&1["type"] == "run_cancel_requested")) == 1, "one acceptance ever"
        assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1
        assert Enum.count(events, &(&1["type"] == "run_cancelled")) == 1
        ws_reqs = Enum.filter(events, &(&1["type"] == "workspace_lease_release_requested"))
        ws_rels = Enum.filter(events, &(&1["type"] == "workspace_lease_released"))
        pane_reqs = Enum.filter(events, &(&1["type"] == "pane_lease_release_requested"))
        pane_rels = Enum.filter(events, &(&1["type"] == "pane_lease_released"))
        assert Enum.map(ws_reqs, & &1["data"]["workspace_lease_id"]) == [workspace_id], "exactly one workspace request"

        assert Enum.map(ws_rels, & &1["data"]["workspace_lease_id"]) == [workspace_id],
               "exactly one completed workspace release"

        assert Enum.map(pane_reqs, & &1["data"]["pane_ref"]) == [pane_ref], "exactly one pane request"
        assert Enum.map(pane_rels, & &1["data"]["pane_ref"]) == [pane_ref], "exactly one completed pane release"
        assert hd(ws_rels)["data"]["release_request_id"] == hd(ws_reqs)["data"]["release_request_id"]
        assert hd(pane_rels)["data"]["release_request_id"] == hd(pane_reqs)["data"]["release_request_id"]
        # a request left pending by the cut is REUSED by its completed release, never re-requested
        for id <- pending_ids,
            do:
              assert(
                id in Enum.map(ws_rels ++ pane_rels, & &1["data"]["release_request_id"]),
                "pending request #{id} reused"
              )

        assert (ws_reqs ++ pane_reqs) |> Enum.map(& &1["data"]["release_request_id"]) |> Enum.uniq() |> length() == 2
      end
    end

    test "C-5 a caller-supplied continuation selector is refused before any I/O (only Server admission selects it)" do
      require_executor!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()

      for {key, value} <- [{:acceptance, %{seq: 1, verb: "start"}}, {:continuation, true}, {:mode, :continue}] do
        ctx = run_dir |> context("gated_run_seed", gated_index(), fs: fs) |> Keyword.put(key, value)

        assert {:error, %{clause: "command_context_invalid", field: field}} =
                 invoke("start", start_args(ctx), "run_c5", CommandId.generate(), ctx)

        assert field == Atom.to_string(key)
      end

      assert_no_activity!(fs, run_dir)
    end
  end

  # =================================================================================================
  describe "L (lease-prefix recovery) RED/interface" do
    @lease_cuts ~w(assignment_requested pane_lease_requested pane_lease_acquired workspace_lease_requested workspace_lease_acquired)
    @ambiguous "lease_ownership_ambiguous"

    # a durable prefix ending at `cut_type` for as_0001: the original start is cut at run_started, the continuation is
    # cut at its own (invocation-relative) receipt of `cut_type`. Returns the stamped command's identity for retries.
    defp lease_cut!(run_dir, run_id, cut_type) do
      oracle = oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle")
      started_seq = seq_of(oracle, "run_started")
      rel = seq_of(oracle, cut_type) - started_seq
      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, run_id, command_id, ctx)
      assert_received {:durable, ^started_seq}
      fs2 = FaultFs.new()
      kill_after_receipt(fs2, self(), rel)

      assert {:error, %{clause: "run_server_down"}} =
               invoke("start", args, run_id, command_id, context(run_dir, "gated_run_seed", gated_index(), fs: fs2))

      assert_received {:durable, ^rel}
      assert List.last(journal(run_dir))["type"] == cut_type
      {command_id, args, ctx}
    end

    defp cut_lines(run_dir), do: run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)

    # the executor path: the durable prefix IS the journal file (the Writer fsyncs every line before the reducer
    # moves on)
    defp read_journal_prefix(run_dir), do: Seam.put(:durable_prefix, fn -> cut_lines(run_dir) end)

    # the plain path: a recording receipt sink; the durable prefix = the seed lines + what the sink accepted so far
    defp recording_sink!(prefix_lines) do
      {:ok, agent} = Agent.start_link(fn -> [] end)
      track!(agent)
      Seam.put(:durable_prefix, fn -> prefix_lines ++ Enum.map(Agent.get(agent, &Enum.reverse/1), &Jason.encode!/1) end)
      GateDouble.receipt(fn event -> Agent.update(agent, &[event | &1]) && :ok end)
    end

    defp assert_durable_at_delivery!(assignment_id) do
      facts = Seam.get({:durable_at_delivery, assignment_id})
      assert facts != nil, "the adapter was entered for #{assignment_id}"
      assert facts.folds, "the prefix durable at delivery entry folds valid"
      assert facts.pane, "the pane lease was durable BEFORE delivery"
      assert facts.workspace, "the role-required workspace lease was durable BEFORE delivery"
    end

    # an acquisition of THIS lease before `seq` that no release of THIS lease undoes before `seq`
    defp held_before?(all_events, acquisitions, released_type, key, seq) do
      case Enum.filter(acquisitions, &(&1["seq"] < seq)) do
        [] ->
          false

        before ->
          last = List.last(before)
          identity = last["data"][key]

          released? =
            &(&1["type"] == released_type and &1["data"][key] == identity and &1["seq"] > last["seq"] and
                &1["seq"] < seq)

          not Enum.any?(all_events, released?)
      end
    end

    # the recovery pins shared by plain resume and the continuation: the existing assignment and its lease/request
    # identities are recovered (acquisitions name JOURNALED requests - no pre-filtering by expected id), only missing
    # lawful prerequisites are appended, one dispatch row preceded by held leases, every durable prefix folds valid
    defp assert_lease_recovery!(all_events, assignment_id \\ "as_0001") do
      as = &(&1["data"]["assignment_id"] == assignment_id)
      assert Enum.count(all_events, &(&1["type"] == "assignment_requested" and as.(&1))) == 1, "not re-requested"
      pane_reqs = Enum.filter(all_events, &(&1["type"] == "pane_lease_requested" and as.(&1)))
      assert match?([_], pane_reqs), "one pane request, ever"
      [pane_req] = pane_reqs

      journaled_pane_ids =
        all_events |> Enum.filter(&(&1["type"] == "pane_lease_requested")) |> MapSet.new(& &1["data"]["lease_request_id"])

      all_pane_acqs = Enum.filter(all_events, &(&1["type"] == "pane_lease_acquired"))

      for acq <- all_pane_acqs do
        assert acq["data"]["lease_request_id"] in journaled_pane_ids, "every acquisition names a JOURNALED request"
      end

      # a request id and a pane must describe the SAME journaled request (LG-M2)
      requests_by_id =
        Map.new(
          Enum.filter(all_events, &(&1["type"] == "pane_lease_requested")),
          &{&1["data"]["lease_request_id"], &1["data"]["pane_ref"]}
        )

      for acq <- all_pane_acqs do
        assert acq["data"]["pane_ref"] == requests_by_id[acq["data"]["lease_request_id"]],
               "acquisition binds its request's pane"
      end

      pane_acqs = Enum.filter(all_pane_acqs, &(&1["data"]["lease_request_id"] == pane_req["data"]["lease_request_id"]))
      assert pane_acqs != [], "the pane lease is acquired under the journaled request id"
      assert Enum.all?(pane_acqs, &(&1["data"]["pane_ref"] == pane_req["data"]["pane_ref"])), "pane_ref binding kept"
      ws_reqs = Enum.filter(all_events, &(&1["type"] == "workspace_lease_requested" and as.(&1)))
      assert match?([_], ws_reqs), "one workspace request, ever"
      [ws_req] = ws_reqs
      ws_id = ws_req["data"]["workspace_lease_id"]

      journaled_ws_ids =
        all_events
        |> Enum.filter(&(&1["type"] == "workspace_lease_requested"))
        |> MapSet.new(& &1["data"]["workspace_lease_id"])

      all_ws_acqs = Enum.filter(all_events, &(&1["type"] == "workspace_lease_acquired"))

      for acq <- all_ws_acqs do
        assert acq["data"]["workspace_lease_id"] in journaled_ws_ids, "every acquisition names a JOURNALED lease"
      end

      ws_acqs = Enum.filter(all_ws_acqs, &(&1["data"]["workspace_lease_id"] == ws_id))
      assert ws_acqs != [], "the workspace lease is acquired under the journaled id"
      assert Enum.all?(ws_acqs, &(&1["data"]["mode"] == ws_req["data"]["mode"])), "mode binding kept"
      dispatches = Enum.filter(all_events, &(&1["type"] == "assignment_dispatch_sent" and as.(&1)))
      assert match?([_], dispatches), "exactly one dispatch row"
      dispatch_seq = hd(dispatches)["seq"]
      assert held_before?(all_events, pane_acqs, "pane_lease_released", "pane_ref", dispatch_seq), "held pane lease"

      assert held_before?(all_events, ws_acqs, "workspace_lease_released", "workspace_lease_id", dispatch_seq),
             "held workspace"

      lines = Enum.map(all_events, &Jason.encode!/1)

      for n <- 1..length(lines) do
        assert match?({:ok, _}, Fold.fold_lines(Enum.take(lines, n))), "every durable prefix (#{n}) folds valid"
      end
    end

    # ---- L-M1 guard controls: the delivery-entry guard demonstrably distinguishes early delivery ----
    test "L-G positive control and out-of-order control: the durable-prefix guard flags a delivery before its leases" do
      complete = kill9("events_pre_dispatch.jsonl")
      assert %{folds: true, pane: true, workspace: true} = PrefixCheckingDispatch.durable_facts(complete, "as_0001")
      early = Enum.take(complete, 6)
      assert %{folds: true, pane: false} = PrefixCheckingDispatch.durable_facts(early, "as_0001")

      assert %{folds: true, pane: true, workspace: false} =
               PrefixCheckingDispatch.durable_facts(Enum.take(complete, 7), "as_0001")

      assert %{folds: false} = PrefixCheckingDispatch.durable_facts(complete ++ [~s({"not":"an event"})], "as_0001")
    end

    # L-0 (INVERTED at GREEN, authorized m_1788670628000; the pre-fix fact is kept in the doc as history): on 82cf7eb a
    # cut between pane_lease_requested and pane_lease_acquired let the continuation deliver once and persist a
    # lease-less assignment_dispatch_sent, after which Fold rejected the journal and every retry failed closed. Now the
    # first retry recovers lawfully and completes with ONE delivery; a repeat retry replays the completed run.
    test "L-0 regression: a pane_lease_requested cut recovers lawfully with one delivery; the repeat replays" do
      require_executor!()
      run_dir = tmp_run_dir()
      {command_id, args, _} = lease_cut!(run_dir, "run_l0", "pane_lease_requested")
      read_journal_prefix(run_dir)

      for _attempt <- 1..2 do
        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 invoke(
                   "start",
                   args,
                   "run_l0",
                   command_id,
                   context(run_dir, "gated_run_seed", gated_index(), dispatch: PrefixCheckingDispatch)
                 )

        assert Seam.get({:delivered, "as_0001"}) == 1, "one delivery across the repeat"
        assert_durable_at_delivery!("as_0001")
        assert_lease_recovery!(events)
      end
    end

    for cut <- @lease_cuts do
      test "L-1 plain resume after a #{cut} cut: identities recovered, leases durable at delivery, valid prefixes" do
        require_executor!()
        run_dir = tmp_run_dir()
        {_command_id, _args, ctx} = lease_cut!(run_dir, "run_l1", unquote(cut))
        lines = cut_lines(run_dir)
        sink = recording_sink!(lines)

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 Host.resume(
                   ctx[:spec],
                   ctx[:plan],
                   lines,
                   oracle_opts(ctx, gated_index(), run_id: "run_l1", dispatch: PrefixCheckingDispatch, event_sink: sink)
                 )

        assert Seam.get({:delivered, "as_0001"}) == 1, "one delivery for the recovered assignment"
        assert_durable_at_delivery!("as_0001")
        assert_lease_recovery!(events)
      end

      test "L-2 continuation after a #{cut} cut: identities recovered, one acceptance, leases durable at delivery" do
        require_executor!()
        run_dir = tmp_run_dir()
        {command_id, args, _} = lease_cut!(run_dir, "run_l2", unquote(cut))
        lines = cut_lines(run_dir)
        read_journal_prefix(run_dir)

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 invoke(
                   "start",
                   args,
                   "run_l2",
                   command_id,
                   context(run_dir, "gated_run_seed", gated_index(), dispatch: PrefixCheckingDispatch)
                 )

        assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1, "one acceptance ever"
        assert Seam.get({:delivered, "as_0001"}) == 1, "one delivery, never a duplicate send"
        assert_durable_at_delivery!("as_0001")
        assert_lease_recovery!(events)
        assert String.starts_with?(journal_bytes(run_dir), Enum.join(lines, "\n") <> "\n")
      end
    end

    # ---- L-M2: the interrupted stale-lease repair itself, cut at each measured repair row ----
    # Measured resume suffix on the pre_dispatch seed (Host.resume): 1 run_resumed, 2 workspace_lease_release_requested
    # (wslr_0001), 3 workspace_lease_released, 4 workspace_lease_acquired, 5 pane_lease_release_requested (plrr_0001),
    # 6 pane_lease_released, 7 pane_lease_acquired (plr_as_0001), 8 assignment_prompt_projected,
    # 9 assignment_dispatch_sent.
    for {label, cut} <- [
          {"workspace release requested", 2},
          {"workspace released (flag cleared)", 3},
          {"workspace reacquired", 4},
          {"pane release requested", 5},
          {"pane released (flag cleared)", 6},
          {"pane reacquired", 7}
        ] do
      test "L-5 resume repair cut after #{label} (receipt #{cut}): same command completes, ids reused, one delivery" do
        require_executor!()
        # derived-live input (labelled): the historical pre_dispatch prefix with only its deadline moved ahead
        lines = live(kill9("events_pre_dispatch.jsonl"))
        run_dir = seed_legacy!(tmp_run_dir(), lines)
        run_id = run_id_of(run_dir)
        prior = length(lines)
        fs = FaultFs.new()
        kill_after_receipt(fs, self(), unquote(cut))
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), fs: fs, dispatch: CountingDispatch)
        command_id = CommandId.generate()
        args = %{"recovery_reason" => "crash_recovery"}
        assert {:error, %{clause: "run_server_down"}} = invoke("resume", args, run_id, command_id, ctx)
        assert_received {:durable, unquote(cut)}
        cut_events = journal(run_dir)
        assert length(cut_events) == prior + unquote(cut)

        expected_last =
          Enum.at(
            ~w(run_resumed workspace_lease_release_requested workspace_lease_released workspace_lease_acquired pane_lease_release_requested pane_lease_released pane_lease_acquired),
            unquote(cut) - 1
          )

        assert List.last(cut_events)["type"] == expected_last, "the cut identity is the measured repair row"
        assert Seam.get({:delivered, "as_0001"}) == nil, "nothing delivered before the cut"

        pending_ids =
          cut_events
          |> Enum.drop(prior)
          |> Enum.filter(&String.ends_with?(&1["type"], "_release_requested"))
          |> Enum.map(& &1["data"]["release_request_id"])

        before = journal_bytes(run_dir)
        read_journal_prefix(run_dir)

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 invoke(
                   "resume",
                   args,
                   run_id,
                   command_id,
                   context(run_dir, "kill9_resume", pre_dispatch_index(),
                     dispatch: PrefixCheckingDispatch,
                     observe_fence_observer: self()
                   )
                 )

        assert_live_arm!()
        assert String.starts_with?(journal_bytes(run_dir), before)
        assert Enum.count(events, &(&1["type"] == "run_resumed")) == 1, "no extra acceptance"
        assert Seam.get({:delivered, "as_0001"}) == 1, "exactly one delivery across the repeat"
        assert_durable_at_delivery!("as_0001")

        # MEASURED (disclosed): a lease the crashed instance had already reacquired is stale AGAIN for the retry's
        # instance, so the existing repair may release/reacquire it a second time: bounded by the two resume attempts,
        # never a third, and every release request is completed exactly once under its own id
        stale = &(&1["data"]["reason"] == "resume_stale_repair")
        assert Enum.count(events, &(&1["type"] == "workspace_lease_release_requested" and stale.(&1))) in 1..2
        assert Enum.count(events, &(&1["type"] == "pane_lease_release_requested" and stale.(&1))) in 1..2

        requested_ids =
          events
          |> Enum.filter(&String.ends_with?(&1["type"], "_release_requested"))
          |> Enum.map(& &1["data"]["release_request_id"])

        released_ids =
          events
          |> Enum.filter(&String.ends_with?(&1["type"], "_lease_released"))
          |> Enum.map(& &1["data"]["release_request_id"])

        assert Enum.sort(requested_ids) == Enum.sort(released_ids), "every release request completed once, under its id"

        for id <- pending_ids,
            do: assert(Enum.count(released_ids, &(&1 == id)) == 1, "pending #{id} completed once under its id")

        assert_lease_recovery!(events)
      end
    end

    test "L-6 a crash DURING the missing-prerequisite repair: the durable cut is a valid lease row; the next retry completes once" do
      require_executor!()
      run_dir = tmp_run_dir()
      {command_id, args, _} = lease_cut!(run_dir, "run_l6", "pane_lease_requested")
      fs = FaultFs.new()
      # the continuation's FIRST receipt must be the missing prerequisite (the acquisition), never a projection/dispatch
      kill_after_receipt(fs, self(), 1)

      assert {:error, %{clause: "run_server_down"}} =
               invoke(
                 "start",
                 args,
                 "run_l6",
                 command_id,
                 context(run_dir, "gated_run_seed", gated_index(), fs: fs, dispatch: CountingDispatch)
               )

      assert_received {:durable, 1}
      assert Seam.get({:delivered, "as_0001"}) == nil, "no delivery before the prerequisite is durable"
      cut = journal(run_dir)
      assert match?({:ok, _}, Fold.fold_lines(cut_lines(run_dir))), "the durable cut folds valid"

      assert List.last(cut)["type"] == "pane_lease_acquired" and
               List.last(cut)["data"]["lease_request_id"] == "plr_as_0001"

      read_journal_prefix(run_dir)

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke(
                 "start",
                 args,
                 "run_l6",
                 command_id,
                 context(run_dir, "gated_run_seed", gated_index(), dispatch: PrefixCheckingDispatch)
               )

      assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1
      assert Seam.get({:delivered, "as_0001"}) == 1
      assert_durable_at_delivery!("as_0001")
      assert_lease_recovery!(events)
    end

    test "L-3 interrupted stale-lease repair on resume (awaiting artifact): pending release completed under its id, no adapter delivery" do
      require_executor!()
      # derived-live input (labelled): the historical awaiting prefix with only its deadlines moved ahead
      lines = live(kill9("events_awaiting_artifact.jsonl"))
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      prior = length(lines)
      fs = FaultFs.new()
      # receipt 1 = run_resumed (acceptance), receipt 2 = the first stale-repair release request
      kill_after_receipt(fs, self(), 2)
      ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), fs: fs, dispatch: CountingDispatch)
      command_id = CommandId.generate()
      args = %{"recovery_reason" => "crash_recovery"}
      assert {:error, %{clause: "run_server_down"}} = invoke("resume", args, run_id, command_id, ctx)
      assert_received {:durable, 2}
      cut = journal(run_dir)
      assert Enum.at(cut, prior)["type"] == "run_resumed"
      pending = Enum.at(cut, prior + 1)
      assert pending["type"] == "workspace_lease_release_requested"
      pending_id = pending["data"]["release_request_id"]
      before = journal_bytes(run_dir)

      assert {:ok, %{summary: summary, events: events}} =
               invoke(
                 "resume",
                 args,
                 run_id,
                 command_id,
                 context(run_dir, "kill9_resume", pre_dispatch_index(),
                   dispatch: CountingDispatch,
                   observe_fence_observer: self()
                 )
               )

      assert_live_arm!()
      assert summary["status"] in ["completed", "blocked"]
      assert Enum.count(events, &(&1["type"] == "run_resumed")) == 1

      releases =
        Enum.filter(
          events,
          &(&1["type"] == "workspace_lease_released" and &1["data"]["release_request_id"] == pending_id)
        )

      assert length(releases) == 1, "the pending release request is completed under its own id"

      assert Enum.count(
               events,
               &(&1["type"] == "workspace_lease_release_requested" and &1["data"]["reason"] == "resume_stale_repair")
             ) == 1

      refute Enum.any?(
               Enum.drop(events, prior),
               &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == "as_0001")
             )

      assert Seam.get({:delivered, "as_0001"}) == nil, "the adapter was never entered for the awaiting assignment"
      assert String.starts_with?(journal_bytes(run_dir), before)
      lines_all = Enum.map(events, &Jason.encode!/1)

      for n <- (prior + 1)..length(lines_all),
          do: assert(match?({:ok, _}, Fold.fold_lines(Enum.take(lines_all, n))), "prefix #{n} folds valid")
    end

    for {label, cut, forbidden} <- [
          {"pending pane request", "pane_lease_requested",
           ~w(pane_lease_acquired pane_lease_released assignment_dispatch_sent)},
          {"pending workspace request", "workspace_lease_requested",
           ~w(workspace_lease_acquired workspace_lease_released assignment_dispatch_sent)}
        ] do
      test "L-4 cancel after a #{label} does not invent an acquisition merely to release it" do
        require_executor!()
        run_dir = tmp_run_dir()
        {_command_id, _args, _} = lease_cut!(run_dir, "run_l4", unquote(cut))
        prior = length(journal(run_dir))
        ctx = context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)

        assert {:ok, %{summary: %{"status" => "cancelled"}, events: events}} =
                 invoke("cancel", %{"reason" => "operator_cancel"}, "run_l4", CommandId.generate(), ctx)

        appended = Enum.drop(events, prior)
        refute Enum.any?(appended, &(&1["type"] in unquote(forbidden)))
        assert Enum.count(appended, &(&1["type"] == "run_cancelled")) == 1
        assert Seam.get({:delivered, "as_0001"}) == nil
        assert match?({:ok, _}, Fold.fold_lines(cut_lines(run_dir)))
      end
    end

    # ---- L-M3: ambiguous pending ownership is admissible and must be REFUSED before any dispatch ----
    # a legacy prefix with TWO pending pane requests for as_0001 naming different panes (review probe, imported)
    defp ambiguous_lines do
      lines = Enum.take(kill9("events_pre_dispatch.jsonl"), 6)

      second =
        lines
        |> List.last()
        |> Jason.decode!()
        |> Map.merge(%{"seq" => 7, "event_id" => "ev_0007"})
        |> put_in(["data", "lease_request_id"], "plr_conflict")
        |> put_in(["data", "pane_ref"], "pane_other")
        |> Jason.encode!()

      lines ++ [second]
    end

    test "L-7 fact (imported): two conflicting pending pane requests survive Reader, Writer and Fold" do
      lines = ambiguous_lines()
      assert {:ok, fold} = Fold.fold_lines(lines)
      assert map_size(fold.pane_lease_requests) == 2
      assert fold.pane_lease_requests["plr_as_0001"].pane_ref == "pane_writer"
      assert fold.pane_lease_requests["plr_conflict"].pane_ref == "pane_other"
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      assert {:ok, %{lines: ^lines}} = Reader.load(run_dir)
      assert {:ok, writer, %{lines: ^lines}} = Writer.open(run_dir, lock: [supervisor_instance: "sup_review"])
      track!(writer)
      assert {:ok, _} = Fold.fold_lines(Writer.opened(writer).lines)
      Writer.close(writer)
    end

    test "L-7 plain resume with ambiguous pending ownership: closed refusal before any acquisition or delivery" do
      lines = ambiguous_lines()
      ctx = context(tmp_run_dir(), "kill9_resume", pre_dispatch_index())
      sink = recording_sink!(lines)
      run_id = lines |> hd() |> Jason.decode!() |> Map.fetch!("run_id")

      assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
               Host.resume(
                 ctx[:spec],
                 ctx[:plan],
                 lines,
                 oracle_opts(ctx, pre_dispatch_index(),
                   run_id: run_id,
                   dispatch: PrefixCheckingDispatch,
                   event_sink: sink
                 )
               )

      assert Seam.get({:delivered, "as_0001"}) == nil, "never delivered"
      accepted = Seam.get(:durable_prefix).() |> Enum.drop(length(lines)) |> Enum.map(&Jason.decode!/1)

      assert accepted == [], "pre-effect refusal commits no acceptance or ownership fact"
    end

    test "L-7 executor resume with ambiguous pending ownership: closed pre-effect refusal, no append" do
      require_executor!()
      lines = ambiguous_lines()
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), dispatch: CountingDispatch)

      assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
               invoke("resume", %{"recovery_reason" => "crash_recovery"}, run_id, CommandId.generate(), ctx)

      assert Seam.get({:delivered, "as_0001"}) == nil
      appended = Enum.drop(journal(run_dir), length(lines))
      assert appended == [], "pre-effect refusal commits no acceptance or ownership fact"
      assert match?({:ok, _}, Fold.fold_lines(cut_lines(run_dir)))
    end

    test "L-7 continuation with ambiguous pending ownership: closed refusal, no acquisition, no delivery, prefix exact" do
      require_executor!()
      run_dir = tmp_run_dir()
      {command_id, args, _} = lease_cut!(run_dir, "run_l7", "pane_lease_requested")
      # a second, conflicting pending request appended as a chained continuation of the durable prefix
      events = journal(run_dir)
      last = List.last(events)

      conflict =
        last
        |> Map.merge(%{"seq" => last["seq"] + 1, "event_id" => "ev_conflict"})
        |> put_in(["data", "lease_request_id"], "plr_conflict")
        |> put_in(["data", "pane_ref"], "pane_other")

      seed_v2!(run_dir, rechain_v2(legacy_lines(events ++ [conflict])))
      assert {:ok, fold} = Fold.fold_lines(cut_lines(run_dir))
      assert map_size(fold.pane_lease_requests) == 2
      before = journal_bytes(run_dir)

      assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
               invoke(
                 "start",
                 args,
                 "run_l7",
                 command_id,
                 context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)
               )

      assert Seam.get({:delivered, "as_0001"}) == nil
      assert journal_bytes(run_dir) == before, "nothing appended: no invented acquisition, no dispatch"
    end

    # ---- LG-M1 (review m_1788672226000): a reviewer's recovery keeps the absence of a workspace lease ----
    defp reviewer_cut(oracle) do
      reviewer = Enum.find(oracle, &(&1["type"] == "assignment_requested" and &1["data"]["role"] == "reviewer"))
      id = reviewer["data"]["assignment_id"]
      sent = Enum.find(oracle, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == id))
      {id, sent["seq"]}
    end

    defp assert_reviewer_without_workspace!(events, reviewer_id) do
      refute Enum.any?(
               events,
               &(&1["type"] == "workspace_lease_requested" and &1["data"]["assignment_id"] == reviewer_id)
             ),
             "a reviewer must not acquire the implementation item's workspace lease"

      assert {:ok, fold} = Fold.fold_lines(Enum.map(events, &Jason.encode!/1))
      refute Enum.any?(fold.active_workspace_leases, fn {_lease, facts} -> facts.assignment_id == reviewer_id end)
    end

    test "L-9 plain resume of an interrupted REVIEWER assignment: no invented writer workspace, valid completion" do
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      oracle = oracle_run(ctx, gated_index(), "run_l9")
      {reviewer_id, sent_seq} = reviewer_cut(oracle)
      lines = oracle |> Enum.take(sent_seq) |> Enum.map(&Jason.encode!/1)
      assert match?({:ok, _}, Fold.fold_lines(lines))

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               Host.resume(ctx[:spec], ctx[:plan], lines, oracle_opts(ctx, gated_index(), run_id: "run_l9"))

      assert_reviewer_without_workspace!(events, reviewer_id)
    end

    test "L-9 continuation of an interrupted REVIEWER assignment: no invented writer workspace, one acceptance, completed" do
      require_executor!()
      run_dir = tmp_run_dir()
      oracle = oracle_run(context(run_dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle")
      {reviewer_id, sent_seq} = reviewer_cut(oracle)
      started_seq = seq_of(oracle, "run_started")
      fs = FaultFs.new()
      kill_after_receipt(fs, self(), started_seq)
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs)
      command_id = CommandId.generate()
      args = start_args(ctx)
      assert {:error, %{clause: "run_server_down"}} = invoke("start", args, "run_l9c", command_id, ctx)
      assert_received {:durable, ^started_seq}
      fs2 = FaultFs.new()
      kill_after_receipt(fs2, self(), sent_seq - started_seq)

      assert {:error, %{clause: "run_server_down"}} =
               invoke("start", args, "run_l9c", command_id, context(run_dir, "gated_run_seed", gated_index(), fs: fs2))

      assert List.last(journal(run_dir))["type"] == "assignment_dispatch_sent"
      assert List.last(journal(run_dir))["data"]["assignment_id"] == reviewer_id

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("start", args, "run_l9c", command_id, context(run_dir, "gated_run_seed", gated_index()))

      assert Enum.count(events, &Map.has_key?(&1["data"], "requested_by")) == 1
      assert_reviewer_without_workspace!(events, reviewer_id)
    end

    # ---- LG-M2: a held pane plus a second pending request naming another pane: the repair reuses the PROVEN pair ----
    defp mixed_held_pending_lines do
      lines = kill9("events_pre_dispatch.jsonl")
      request = lines |> Enum.at(5) |> Jason.decode!()

      conflict =
        request
        |> Map.merge(%{"seq" => length(lines) + 1, "event_id" => "ev_conflict"})
        |> put_in(["data", "lease_request_id"], "plr_0000")
        |> put_in(["data", "pane_ref"], "pane_other")

      lines ++ [Jason.encode!(conflict)]
    end

    # the mixed fixture journals TWO requests for as_0001 on purpose, so the single-request pin does not apply; the
    # coherence pins do: every acquisition binds its own request's pane, one dispatch preceded by held leases,
    # prefixes valid
    defp assert_coherent_recovery!(all_events) do
      requests_by_id =
        Map.new(
          Enum.filter(all_events, &(&1["type"] == "pane_lease_requested")),
          &{&1["data"]["lease_request_id"], &1["data"]["pane_ref"]}
        )

      for acq <- Enum.filter(all_events, &(&1["type"] == "pane_lease_acquired")) do
        assert acq["data"]["pane_ref"] == requests_by_id[acq["data"]["lease_request_id"]],
               "acquisition binds its request's pane"
      end

      dispatches =
        Enum.filter(all_events, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == "as_0001"))

      assert match?([_], dispatches), "exactly one dispatch row"
      dispatch_seq = hd(dispatches)["seq"]

      pane_acqs =
        Enum.filter(all_events, &(&1["type"] == "pane_lease_acquired" and &1["data"]["pane_ref"] == "pane_writer"))

      ws_acqs =
        Enum.filter(
          all_events,
          &(&1["type"] == "workspace_lease_acquired" and &1["data"]["workspace_lease_id"] == "wsl_as_0001")
        )

      assert held_before?(all_events, pane_acqs, "pane_lease_released", "pane_ref", dispatch_seq), "held pane lease"

      assert held_before?(all_events, ws_acqs, "workspace_lease_released", "workspace_lease_id", dispatch_seq),
             "held workspace"

      lines = Enum.map(all_events, &Jason.encode!/1)

      for n <- 1..length(lines) do
        assert match?({:ok, _}, Fold.fold_lines(Enum.take(lines, n))), "every durable prefix (#{n}) folds valid"
      end
    end

    defp assert_proven_pair_reused!(events, prior) do
      # only as_0001's own requests: the reviewer's later acquisition (plr_as_0002) is lawful and unrelated
      reacquired =
        Enum.filter(
          events,
          &(&1["type"] == "pane_lease_acquired" and &1["seq"] > prior and
              &1["data"]["lease_request_id"] in ["plr_as_0001", "plr_0000"])
        )

      assert reacquired != [], "the stale repair reacquired the held pane"

      for acq <- reacquired do
        assert Map.take(acq["data"], ["lease_request_id", "pane_ref"]) == %{
                 "lease_request_id" => "plr_as_0001",
                 "pane_ref" => "pane_writer"
               },
               "the reacquisition reuses the proven pair, never plr_0000 with pane_writer"
      end
    end

    test "L-10 plain resume with a held pane and a conflicting pending request: repair reuses the proven pair, one delivery" do
      lines = mixed_held_pending_lines()
      assert {:ok, fold} = Fold.fold_lines(lines)
      assert fold.assignments["as_0001"].pane_ref == "pane_writer" and map_size(fold.pane_lease_requests) == 2
      ctx = context(tmp_run_dir(), "kill9_resume", pre_dispatch_index())
      sink = recording_sink!(lines)
      run_id = lines |> hd() |> Jason.decode!() |> Map.fetch!("run_id")

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               Host.resume(
                 ctx[:spec],
                 ctx[:plan],
                 lines,
                 oracle_opts(ctx, pre_dispatch_index(),
                   run_id: run_id,
                   dispatch: PrefixCheckingDispatch,
                   event_sink: sink
                 )
               )

      assert_proven_pair_reused!(events, length(lines))
      assert Seam.get({:delivered, "as_0001"}) == 1
      assert_durable_at_delivery!("as_0001")
      assert_coherent_recovery!(events)
    end

    test "L-10 executor resume with a held pane and a conflicting pending request: repair reuses the proven pair, one delivery" do
      require_executor!()
      # derived-live input (labelled)
      lines = live(mixed_held_pending_lines())
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      read_journal_prefix(run_dir)

      ctx =
        context(run_dir, "kill9_resume", pre_dispatch_index(),
          dispatch: PrefixCheckingDispatch,
          observe_fence_observer: self()
        )

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               invoke("resume", %{"recovery_reason" => "crash_recovery"}, run_id, CommandId.generate(), ctx)

      assert_live_arm!()
      assert_proven_pair_reused!(events, length(lines))
      assert Seam.get({:delivered, "as_0001"}) == 1
      assert_durable_at_delivery!("as_0001")
      assert_coherent_recovery!(events)
    end

    # ---- LG-M3 / LG-M4 (review m_1788673581000): residual ownership-selection ambiguities fail CLOSED ----
    defp append_probe(lines, event) do
      seq = length(lines) + 1
      lines ++ [Jason.encode!(Map.merge(event, %{"seq" => seq, "event_id" => "ev_probe_#{seq}"}))]
    end

    # two held DISJOINT workspaces for as_0001 (lib and other): Fold accepts both; recovery must not pick one
    defp two_held_workspaces_lines do
      lines = kill9("events_pre_dispatch.jsonl")

      request =
        lines
        |> Enum.at(7)
        |> Jason.decode!()
        |> put_in(["data", "workspace_lease_id"], "wsl_other")
        |> put_in(["data", "allowed_paths"], ["other"])

      acquired = lines |> Enum.at(8) |> Jason.decode!() |> put_in(["data", "workspace_lease_id"], "wsl_other")
      lines |> append_probe(request) |> append_probe(acquired)
    end

    # a held plr_as_0001/pane_writer, then a request plr_other -> pane_other ACQUIRED with pane_writer: contradictory
    # later ownership evidence (Fold records pane_other); an older coherent pair must never be resurrected
    defp mismatched_later_acquisition_lines do
      lines = kill9("events_pre_dispatch.jsonl")

      request =
        lines
        |> Enum.at(5)
        |> Jason.decode!()
        |> put_in(["data", "lease_request_id"], "plr_other")
        |> put_in(["data", "pane_ref"], "pane_other")

      acquired = lines |> Enum.at(6) |> Jason.decode!() |> put_in(["data", "lease_request_id"], "plr_other")
      lines |> append_probe(request) |> append_probe(acquired)
    end

    defp probe_lines(:two_held_workspaces_lines), do: two_held_workspaces_lines()
    defp probe_lines(:mismatched_later_acquisition_lines), do: mismatched_later_acquisition_lines()

    for {label, builder, fact} <- [
          {"L-11 two held disjoint workspaces", :two_held_workspaces_lines, :two_workspaces},
          {"L-12 a later mismatched pane acquisition", :mismatched_later_acquisition_lines, :pane_other}
        ] do
      test "#{label}: plain resume refuses closed before any effect (imported probe, ruled fail-closed)" do
        lines = probe_lines(unquote(builder))
        assert {:ok, fold} = Fold.fold_lines(lines)

        case unquote(fact) do
          :two_workspaces -> assert map_size(fold.active_workspace_leases) == 2
          :pane_other -> assert fold.assignments["as_0001"].pane_ref == "pane_other"
        end

        ctx = context(tmp_run_dir(), "kill9_resume", pre_dispatch_index())
        sink = recording_sink!(lines)
        run_id = lines |> hd() |> Jason.decode!() |> Map.fetch!("run_id")

        assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
                 Host.resume(
                   ctx[:spec],
                   ctx[:plan],
                   lines,
                   oracle_opts(ctx, pre_dispatch_index(),
                     run_id: run_id,
                     dispatch: PrefixCheckingDispatch,
                     event_sink: sink
                   )
                 )

        assert Seam.get({:delivered, "as_0001"}) == nil
        assert Seam.get(:durable_prefix).() == lines, "nothing accepted by the sink"
      end

      test "#{label}: executor resume refuses closed, zero appended bytes, no delivery" do
        require_executor!()
        lines = probe_lines(unquote(builder))
        run_dir = seed_legacy!(tmp_run_dir(), lines)
        before = journal_bytes(run_dir)
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), dispatch: CountingDispatch)

        assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
                 invoke("resume", %{"recovery_reason" => "crash_recovery"}, run_id_of(run_dir), CommandId.generate(), ctx)

        assert journal_bytes(run_dir) == before
        assert Seam.get({:delivered, "as_0001"}) == nil
      end
    end

    for {label, cut, extra} <- [
          {"L-11 two held disjoint workspaces", "workspace_lease_acquired", :two_workspaces},
          {"L-12 a later mismatched pane acquisition", "pane_lease_acquired", :pane_other}
        ] do
      test "#{label}: the continuation refuses closed, prefix bytes exact, no delivery" do
        require_executor!()
        run_dir = tmp_run_dir()
        {command_id, args, _} = lease_cut!(run_dir, "run_l1112", unquote(cut))
        events = journal(run_dir)

        rows =
          case unquote(extra) do
            :two_workspaces ->
              req = Enum.find(events, &(&1["type"] == "workspace_lease_requested"))
              acq = Enum.find(events, &(&1["type"] == "workspace_lease_acquired"))

              [
                req
                |> put_in(["data", "workspace_lease_id"], "wsl_other")
                |> put_in(["data", "allowed_paths"], ["other"]),
                put_in(acq, ["data", "workspace_lease_id"], "wsl_other")
              ]

            :pane_other ->
              req = Enum.find(events, &(&1["type"] == "pane_lease_requested"))
              acq = Enum.find(events, &(&1["type"] == "pane_lease_acquired"))

              [
                req |> put_in(["data", "lease_request_id"], "plr_other") |> put_in(["data", "pane_ref"], "pane_other"),
                put_in(acq, ["data", "lease_request_id"], "plr_other")
              ]
          end

        {rows, _} =
          Enum.map_reduce(rows, length(events), fn row, seq ->
            {Map.merge(row, %{"seq" => seq + 1, "event_id" => "ev_probe_#{seq + 1}"}), seq + 1}
          end)

        seed_v2!(run_dir, rechain_v2(legacy_lines(events ++ rows)))
        assert match?({:ok, _}, Fold.fold_lines(cut_lines(run_dir)))
        before = journal_bytes(run_dir)

        assert {:error, %{"reason" => @ambiguous, "assignment_id" => "as_0001"}} =
                 invoke(
                   "start",
                   args,
                   "run_l1112",
                   command_id,
                   context(run_dir, "gated_run_seed", gated_index(), dispatch: CountingDispatch)
                 )

        assert journal_bytes(run_dir) == before
        assert Seam.get({:delivered, "as_0001"}) == nil
      end
    end

    # ---- L-M4: identities must be READ from the prefix, never regenerated (non-default ids) ----
    # structured rewrite (never string surgery on JSON): the pane request id and the workspace lease id of as_0001
    @custom_ids %{"plr_as_0001" => "plr_custom_7", "wsl_as_0001" => "wsl_custom_9"}

    defp custom_id_lines(take) do
      "events_pre_dispatch.jsonl"
      |> kill9()
      |> Enum.take(take)
      |> Enum.map(fn line ->
        event = Jason.decode!(line)
        Jason.encode!(Map.put(event, "data", rename_lease_ids(event["data"])))
      end)
    end

    defp rename_lease_ids(data) do
      Enum.reduce(["lease_request_id", "workspace_lease_id"], data, fn key, d ->
        case Map.fetch(d, key) do
          {:ok, id} when is_map_key(@custom_ids, id) -> Map.put(d, key, Map.fetch!(@custom_ids, id))
          _ -> d
        end
      end)
    end

    for {label, take, expect} <- [
          {"pending pane request plr_custom_7", 6,
           [{"pane_lease_acquired", "lease_request_id", "plr_custom_7", "pane_ref", "pane_writer"}]},
          {"pending workspace request wsl_custom_9", 8,
           [
             {"pane_lease_acquired", "lease_request_id", "plr_custom_7", "pane_ref", "pane_writer"},
             {"workspace_lease_acquired", "workspace_lease_id", "wsl_custom_9", "mode", "shared_repo"}
           ]}
        ] do
      test "L-8 non-default ids (#{label}): plain resume acquires under the EXACT journaled ids and bindings" do
        lines = custom_id_lines(unquote(take))
        assert match?({:ok, _}, Fold.fold_lines(lines)), "the custom-id prefix is Fold-accepted"
        run_dir = seed_legacy!(tmp_run_dir(), lines)
        assert {:ok, _} = Reader.load(run_dir)
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index())
        sink = recording_sink!(lines)
        run_id = run_id_of(run_dir)

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 Host.resume(
                   ctx[:spec],
                   ctx[:plan],
                   lines,
                   oracle_opts(ctx, pre_dispatch_index(),
                     run_id: run_id,
                     dispatch: PrefixCheckingDispatch,
                     event_sink: sink
                   )
                 )

        for {type, id_key, id, bind_key, bind} <- unquote(Macro.escape(expect)) do
          rows = Enum.filter(events, &(&1["type"] == type))
          assert Enum.any?(rows, &(&1["data"][id_key] == id and &1["data"][bind_key] == bind)), "#{type} under #{id}"
          refute Enum.any?(rows, &(&1["data"][id_key] in ["plr_as_0001", "wsl_as_0001"])), "no regenerated default id"
        end

        assert Seam.get({:delivered, "as_0001"}) == 1
        assert_durable_at_delivery!("as_0001")
        assert_lease_recovery!(events)
      end

      test "L-8 non-default ids (#{label}): executor resume acquires under the EXACT journaled ids and bindings" do
        require_executor!()
        # derived-live input (labelled)
        lines = live(custom_id_lines(unquote(take)))
        run_dir = seed_legacy!(tmp_run_dir(), lines)
        run_id = run_id_of(run_dir)
        read_journal_prefix(run_dir)

        ctx =
          context(run_dir, "kill9_resume", pre_dispatch_index(),
            dispatch: PrefixCheckingDispatch,
            observe_fence_observer: self()
          )

        assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
                 invoke("resume", %{"recovery_reason" => "crash_recovery"}, run_id, CommandId.generate(), ctx)

        assert_live_arm!()

        for {type, id_key, id, bind_key, bind} <- unquote(Macro.escape(expect)) do
          rows = Enum.filter(events, &(&1["type"] == type))
          assert Enum.any?(rows, &(&1["data"][id_key] == id and &1["data"][bind_key] == bind)), "#{type} under #{id}"
          refute Enum.any?(rows, &(&1["data"][id_key] in ["plr_as_0001", "wsl_as_0001"])), "no regenerated default id"
        end

        assert Seam.get({:delivered, "as_0001"}) == 1
        assert_durable_at_delivery!("as_0001")
        assert_lease_recovery!(events)
      end
    end
  end

  # =================================================================================================
  describe "D (restart-empty) RED/interface: explicit locked-empty admission (ruling R-B2 = a)" do
    test "D-2 explicit restart-empty on an existing EMPTY journal: one stamped run_created, completed, lock path bound" do
      require_executor!()
      run_dir = tmp_run_dir()
      File.write!(Path.join(run_dir, "events.jsonl"), "")
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true)
      command_id = CommandId.generate()
      args = start_args(ctx)

      assert {:ok, %{summary: %{"status" => "completed"}, events: [created | _] = events}} =
               invoke("start", args, "run_d2", command_id, ctx)

      assert created["type"] == "run_created" and
               created["data"]["requested_by"] == expected_stamp(@operator, "start", args, command_id)

      assert Enum.count(events, &(&1["type"] == "run_created")) == 1
      assert created["run_id"] == "run_d2"

      assert is_binary(created["data"]["run_lock_path"]) or
               is_binary(Enum.find(events, &(&1["type"] == "run_started"))["data"]["run_lock_path"])

      assert_released!(run_dir)
    end

    test "D-3a a NONEMPTY prefix under the explicit selector (admission control): journal_exists, bytes unchanged, no effect" do
      require_executor!()
      run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
      before = journal_bytes(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))
      assert {:error, %{clause: "journal_exists"}} = invoke("start", start_args(ctx), "run_d3", CommandId.generate(), ctx)
      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
      assert_released!(run_dir)
    end

    # the race itself (preflight saw empty, the prefix is nonempty when the Writer takes the lock) is witnessed at the
    # CLI entry in test/cli/cli_test.exs ("run --resume: a journal that fills between preflight and the lock ...")

    # Repaired authority (CD-M3): D admission follows the Writer-VERIFIED prefix, not raw byte emptiness. Every
    # fixture's Reader/Writer verdict is measured FIRST so GREEN is never asked to discover whether the corpus is valid.
    test "D-R1 a torn first unaccepted line verifies to EMPTY (truncate repair): explicit restart-empty admits one stamped start" do
      require_executor!()
      run_dir = tmp_run_dir()
      [first | _] = kill9("events_pre_dispatch.jsonl")
      File.write!(Path.join(run_dir, "events.jsonl"), String.slice(first, 0, 40))
      assert {:ok, %{lines: [], pending_repair: %{action: :truncate_tail, receipt_seq_after: 0}}} = Reader.load(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true)
      command_id = CommandId.generate()
      args = start_args(ctx)

      assert {:ok, %{summary: %{"status" => "completed"}, events: [created | _] = events}} =
               invoke("start", args, "run_dr1", command_id, ctx)

      assert created["type"] == "run_created" and created["seq"] == 1
      assert created["data"]["requested_by"] == expected_stamp(@operator, "start", args, command_id)
      assert Enum.count(events, &(&1["type"] == "run_created")) == 1
      refute journal_bytes(run_dir) =~ String.slice(first, 0, 40) <> "{", "the torn bytes are gone, not accepted"
      assert_released!(run_dir)
    end

    # CD-M4 (m_1788663540000): receipt absence is version-specific. A LEGACY v1 first line with no receipt file plans no
    # repair and creates no receipt (Chain.reconcile's envelope_version 1 + nil receipt arm); a v2 first line with no
    # receipt file plans advance_receipt 0 -> 1 and the Writer writes events.head. Both are pinned; both are NONEMPTY.
    test "D-R2 (legacy v1) a complete v1 first line with NO receipt file verifies NONEMPTY (no repair, no receipt): journal_exists" do
      require_executor!()
      run_dir = tmp_run_dir()
      [first | _] = kill9("events_pre_dispatch.jsonl")
      assert Jason.decode!(first)["schema_version"] == 1
      File.write!(Path.join(run_dir, "events.jsonl"), first <> "\n")
      assert {:ok, %{lines: [_], pending_repair: nil}} = Reader.load(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))

      assert {:error, %{clause: "journal_exists"}} =
               invoke("start", start_args(ctx), "run_dr2", CommandId.generate(), ctx)

      assert journal_bytes(run_dir) == first <> "\n", "the accepted line is exact and no command event was appended"
      refute_received {:effect_ran, _}
      assert_released!(run_dir)
    end

    test "D-R2 (v2) a complete v2 first line with NO receipt file: advance_receipt 0->1 is planned and performed; journal_exists" do
      require_executor!()

      [line | _] =
        "test/fixtures/contracts/journals/valid_envelope_version_2_minimal/events.jsonl"
        |> File.read!()
        |> String.split("\n", trim: true)

      assert Jason.decode!(line)["schema_version"] == 2
      bytes = line <> "\n"
      # the three ruled keys are pinned as a SUBSET of the plan (the plan carries more)
      expected_repair = %{action: :advance_receipt, receipt_seq_before: 0, receipt_seq_after: 1}

      # baseline control on its OWN directory (imported from the review probe): the Reader plans, the Writer performs
      # and writes the receipt
      control = tmp_run_dir()
      File.write!(Path.join(control, "events.jsonl"), bytes)
      assert {:ok, %{lines: [^line], pending_repair: control_plan}} = Reader.load(control)
      assert Map.take(control_plan, Map.keys(expected_repair)) == expected_repair, "the Reader plans the receipt advance"
      refute File.exists?(Path.join(control, "events.head"))
      {:ok, control_writer, opened} = Writer.open(control, lock: [supervisor_instance: "sup_control"])
      track!(control_writer)

      try do
        assert opened.lines == [line] and Map.take(opened.repair, Map.keys(expected_repair)) == expected_repair
        assert File.read!(Path.join(control, "events.jsonl")) == bytes
        assert {:ok, %{seq: 1, line_sha256: hash}} = Chain.decode_receipt(File.read!(Path.join(control, "events.head")))
        assert hash == Chain.line_sha256(bytes)
      after
        if Process.alive?(control_writer), do: Writer.close(control_writer)
      end

      # the feature case: NOT repaired beforehand (Reader is read-only); the executor's own Writer performs the repair,
      # and the explicit restart is refused against the repaired NONEMPTY prefix: accepted line exact, receipt present
      run_dir = tmp_run_dir()
      File.write!(Path.join(run_dir, "events.jsonl"), bytes)
      assert {:ok, %{lines: [^line], pending_repair: feature_plan}} = Reader.load(run_dir)
      assert Map.take(feature_plan, Map.keys(expected_repair)) == expected_repair
      refute File.exists?(Path.join(run_dir, "events.head"))
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))

      assert {:error, %{clause: "journal_exists"}} =
               invoke("start", start_args(ctx), "run_dr2v2", CommandId.generate(), ctx)

      assert File.read!(Path.join(run_dir, "events.jsonl")) == bytes,
             "the accepted line is exact; no command event appended"

      assert {:ok, %{seq: 1, line_sha256: repaired_hash}} =
               Chain.decode_receipt(File.read!(Path.join(run_dir, "events.head")))

      assert repaired_hash == Chain.line_sha256(bytes), "the executor's Writer performed the receipt repair"
      refute_received {:effect_ran, _}
      assert_released!(run_dir)
    end

    test "D-R3 a corrupt receipt keeps the Writer's exact rejection under the explicit selector" do
      require_executor!()
      run_dir = tmp_run_dir() |> seed_v2!(rechain_v2(kill9("events_pre_dispatch.jsonl"))) |> bad_receipt!(9, "t\n")
      expected = writer_rejection!(run_dir)
      assert %{clause: clause} = expected
      refute clause in ["journal_exists", "journal_missing"], "the corpus is refused for its receipt, measured first"
      before = journal_bytes(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))
      assert invoke("start", start_args(ctx), "run_dr3", CommandId.generate(), ctx) == {:error, expected}
      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "D-4a a MISSING journal under the explicit selector is journal_missing and is never recreated" do
      require_executor!()
      run_dir = tmp_run_dir()
      assert {:error, %{clause: "journal_missing"}} = Reader.load(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))

      assert {:error, %{clause: "journal_missing"}} =
               invoke("start", start_args(ctx), "run_d4a", CommandId.generate(), ctx)

      refute File.exists?(Path.join(run_dir, "events.jsonl")), "a missing journal is never recreated by the explicit mode"
      refute_received {:effect_ran, _}
    end

    test "D-4b a LOCKED empty journal under the explicit selector keeps the Writer's exact lock rejection" do
      require_executor!()
      run_dir = tmp_run_dir()
      File.write!(Path.join(run_dir, "events.jsonl"), "")
      {:ok, holder, _} = Writer.open(run_dir, lock: [supervisor_instance: "sup_holder"])
      track!(holder)

      try do
        {:error, expected} = Writer.open(run_dir, lock: [supervisor_instance: @instance])
        assert is_binary(expected.clause)
        ctx = context(run_dir, "gated_run_seed", gated_index(), restart_empty: true, effect_observer: observer_to(self()))
        assert invoke("start", start_args(ctx), "run_d4b", CommandId.generate(), ctx) == {:error, expected}
        assert journal_bytes(run_dir) == ""
      after
        if Process.alive?(holder), do: Writer.close(holder)
      end

      refute_received {:effect_ran, _}
    end

    for {label, value} <- [{"a non-boolean", "yes"}, {"an explicit false", false}, {"an explicit nil", nil}] do
      test "D-5 #{label} restart_empty value is command_context_invalid before any I/O (absence alone means the ordinary path)" do
        require_executor!()
        run_dir = tmp_run_dir()
        File.write!(Path.join(run_dir, "events.jsonl"), "")
        fs = FaultFs.new()
        bad = run_dir |> context("gated_run_seed", gated_index(), fs: fs) |> Keyword.put(:restart_empty, unquote(value))

        assert {:error, %{clause: "command_context_invalid", field: "restart_empty"}} =
                 invoke("start", start_args(bad), "run_d5", CommandId.generate(), bad)

        assert FaultFs.trace(fs) == []
        refute_received {:run_child_started, _, _, _}
      end
    end

    test "D-5 restart_empty with a non-start verb is command_context_invalid before any I/O" do
      require_executor!()
      run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
      fs = FaultFs.new()
      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, restart_empty: true)

      assert {:error, %{clause: "command_context_invalid", field: "restart_empty"}} =
               invoke("resume", %{"recovery_reason" => "x"}, run_id_of(run_dir), CommandId.generate(), ctx)

      assert {:error, %{clause: "command_context_invalid", field: "restart_empty"}} =
               invoke("cancel", %{"reason" => "x"}, run_id_of(run_dir), CommandId.generate(), ctx)

      assert FaultFs.trace(fs) == []
      refute_received {:run_child_started, _, _, _}
    end
  end

  # =================================================================================================
  describe "E-6 (A) lifetime of the owned foreground subtree" do
    test "after return: exactly the named, correlated traces; owner and supervisor DOWN; ownership released; nothing else in the mailbox" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      send(self(), :unrelated_caller_message)
      flag_before = Process.info(self(), :trap_exit)
      assert {:ok, _} = invoke("start", start_args(ctx), "run_e6_0001", CommandId.generate(), ctx)
      assert Process.info(self(), :trap_exit) == flag_before, "the caller's trap_exit flag is never changed"
      # the collector is an extra hop: flush it (bounded ack), THEN the named set is provably in this mailbox
      collector_flush!()
      {started, owner, sup} = consume_start_traces!(0)
      assert Keyword.has_key?(started, :worker), "the worker's birth is one of the named traces"
      assert_all_down!([owner, sup | Keyword.values(started)], 5_000)
      assert_released!(run_dir)
      assert_received :unrelated_caller_message
      # the correlated protocol traces of the run (requested / applied; never a drop) are the only other messages
      drained = drain_protocol_traces!()
      assert drained.requested > 0 and drained.requested == drained.applied and drained.dropped == 0
      assert {:message_queue_len, 0} = Process.info(self(), :message_queue_len)
    end

    test "a TRAPPING caller keeps its pre-existing EXIT message and receives no EXIT from the executor (monitored, not linked)" do
      require_executor!()
      run_dir = tmp_run_dir()
      parent = self()
      ctx = context(run_dir, "gated_run_seed", gated_index())

      {caller, ref} =
        spawn_monitor(fn ->
          Process.flag(:trap_exit, true)
          send(self(), {:EXIT, self(), :unrelated_exit})
          result = invoke("start", start_args(ctx), "run_e6_0002", CommandId.generate(), ctx)
          {:messages, left} = Process.info(self(), :messages)
          send(parent, {:caller_done, result, left, Process.info(self(), :trap_exit)})
        end)

      track!(caller)
      assert_receive {:caller_done, {:ok, _}, left, {:trap_exit, true}}, 60_000

      assert left == [{:EXIT, caller, :unrelated_exit}],
             "only the caller's own EXIT message remains; none arrived from the executor"

      assert_receive {:DOWN, ^ref, :process, ^caller, :normal}, 5_000
      {_started, owner, sup} = consume_start_traces!(5_000)
      assert_all_down!([owner, sup], 5_000)
      assert_released!(run_dir)
    end

    test "caller death while a gate is HELD (never released) stops owner, supervisor and children; no terminal" do
      require_executor!()
      run_dir = tmp_run_dir()
      parent = self()
      ctx = context(run_dir, "gated_run_seed", gated_index(), gate_opts: [runner: HeldGate.runner(parent)])

      {caller, ref} =
        spawn_monitor(fn ->
          send(parent, {:result, invoke("start", start_args(ctx), "run_e6_0003", CommandId.generate(), ctx)})
        end)

      track!(caller)
      {started, owner, sup} = consume_start_traces!(30_000)
      assert_receive {:gate_entered, executing}, 30_000
      assert executing == Keyword.fetch!(started, :worker), "the effect executes in the run's worker"
      Process.exit(caller, :kill)
      assert_receive {:DOWN, ^ref, :process, ^caller, :killed}, 5_000
      assert_all_down!([owner, sup | Keyword.values(started)], 15_000)
      assert_released!(run_dir)
      refute_received {:result, _}

      refute Enum.any?(
               journal(run_dir),
               &(&1["type"] in ["run_completed", "run_failed", "run_cancelled", "gate_passed"])
             ),
             "no terminal fabricated"
    end

    test "a concurrent invoke on the same run directory meets the Writer's same-BEAM rejection while the first completes" do
      require_executor!()
      run_dir = tmp_run_dir()
      parent = self()
      # both contexts are built BEFORE the first live invocation: building one resets the shared harness seams
      ctx = context(run_dir, "gated_run_seed", gated_index(), gate_opts: [runner: HeldGate.runner(parent)])
      second_ctx = context(run_dir, "gated_run_seed", gated_index())
      # the first caller is an UNLINKED monitored process: its death can never take the test down
      {first, mon} =
        spawn_monitor(fn ->
          send(parent, {:first_result, invoke("start", start_args(ctx), "run_e6_0004", CommandId.generate(), ctx)})
        end)

      track!(first)
      {started, _owner, _sup} = consume_start_traces!(30_000)
      assert_receive {:gate_entered, executing}, 30_000
      assert executing == Keyword.fetch!(started, :worker), "the effect executes in the run's worker"
      # measured while turning GREEN: two owners in ONE BEAM meet the Ownership arbiter (second_live_writer), not the
      # cross-process lock-file clause (run_locked) the RED had assumed; either way it is the Writer's own rejection
      assert {:error, %{clause: "second_live_writer"}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, "run_e6_0004", CommandId.generate(), second_ctx)

      send(executing, :release_gate)
      assert_receive {:first_result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
      assert_receive {:DOWN, ^mon, :process, ^first, :normal}, 5_000
      assert_released!(run_dir)
    end
  end

  # =================================================================================================
  describe "E-7 (A) closed failures through invoke" do
    test "a Server crash is run_server_down as-is, payload-free; the abandon ran once" do
      require_executor!()
      run_dir = tmp_run_dir()
      canary = "PRIVATE_EXECUTOR_CANARY_#{System.unique_integer([:positive])}"

      observer = fn
        %Effect.ReleaseGate{}, _ -> exit({:private, canary})
        _, _ -> :ok
      end

      ctx = context(run_dir, "gated_run_seed", gated_index(), gate_executor: UnsettledExecutor, effect_observer: observer)
      result = invoke("start", start_args(ctx), "run_e7_0001", CommandId.generate(), ctx)
      assert result == {:error, %{clause: "run_server_down"}}
      assert_received {:abandoned, %{released: true}}
      refute_received {:abandoned, _}
      assert_released!(run_dir)
    end

    test "a locked run directory is the Writer's exact run_locked rejection with zero effects" do
      require_executor!()
      run_dir = tmp_run_dir()
      {:ok, holder, _} = Writer.open(run_dir, create: true, lock: [supervisor_instance: "sup_holder"])
      track!(holder)

      try do
        {:error, expected} = Writer.open(run_dir, lock: [supervisor_instance: @instance])
        ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))

        assert invoke("resume", %{"recovery_reason" => "x"}, "run_e7_0002", CommandId.generate(), ctx) ==
                 {:error, expected}

        refute_received {:effect_ran, _}
      after
        if Process.alive?(holder), do: Writer.close(holder)
      end
    end

    # EB-M1: a close is proven only by an observed acknowledgement; a suspended Writer is close-unproven, never
    # success, even though teardown kills every owned process afterwards. Imported from the review probe with its
    # false-success assertions INVERTED (disclosed). The close call to a suspended Writer can only end one way, so
    # the cause is pinned exactly: timeout.
    @tag timeout: 90_000
    test "a Writer that never answers the close: close_unproven timeout; the lock is not claimed released; all pids joined" do
      require_executor!()
      {_, :cancel, _, lines, _} = Enum.at(H.cases(), 2)
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      parent = self()

      barrier = fn
        :handoff_received, _facts ->
          :ok

        :subtree_started, facts ->
          send(parent, {:owned, facts})

          receive do
            :close_now -> :ok
          after
            30_000 -> exit(:barrier_never_released)
          end
      end

      ctx = context(run_dir, "gated_run_seed", gated_index(), barrier: barrier)

      {caller, caller_mon} =
        spawn_monitor(fn ->
          send(parent, {:result, invoke("cancel", %{"reason" => "review"}, run_id, CommandId.generate(), ctx)})
        end)

      track!(caller)
      assert_receive {:owned, facts}, 30_000
      track!(Map.values(facts))
      {:ok, _} = run_server().await(facts.server, :infinity)
      opened = Writer.opened(facts.writer)
      lock_path = Path.join(run_dir, opened.lock_path)
      assert File.exists?(lock_path)
      :erlang.suspend_process(facts.writer)

      try do
        send(facts.owner, :close_now)
        assert_receive {:result, {:ok, result}}, 80_000

        assert Map.get(result, :close) == {:error, %{clause: "close_unproven", cause: "timeout"}},
               "no acknowledgement is not proof of release"

        assert result.summary["status"] == "completed", "the command's own result is preserved"
        refute inspect(result, limit: :infinity) =~ ~r/#PID|Elixir\.GenServer/, "no raw exit payload"
        for {name, pid} <- Map.delete(facts, :owner), do: refute(Process.alive?(pid), "#{name} joined by teardown")
        assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000
        assert File.exists?(lock_path), "an unproven close leaves the lock file it could not release"
      after
        for pid <- Map.values(facts), Process.alive?(pid), do: :erlang.resume_process(pid)
      end
    end

    # CI 34020770508 (ruling m_1788682741000): the former "dies before the close" row killed the Writer while the OWNER
    # was still parked at :subtree_started, BEFORE its own await, so the rest_for_one teardown raced the owner's
    # request to the Server: whenever the teardown reached the Server first the closed outcome was run_server_down,
    # not close_unproven, and the row waited 80 s for a shape that had already been ruled out. That order is now
    # pinned DETERMINISTICALLY - the Server's and the root's DOWN are observed before the barrier is released - and
    # the exact closed outcome is asserted. close_unproven coverage lives in the suspended-close row above and the
    # death-DURING-close row below; a death-before-close witness with the owner's result already acquired needs a
    # post-await barrier the owner does not expose (no lib change on this lane).
    @tag timeout: 90_000
    test "a Writer that dies BEFORE the owner asks for the result: the teardown reaches the Server first; closed run_server_down, nothing claimed closed; lock left; all pids joined" do
      require_executor!()
      {_, :cancel, _, lines, _} = Enum.at(H.cases(), 2)
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      parent = self()

      barrier = fn
        :handoff_received, _facts ->
          :ok

        :subtree_started, facts ->
          send(parent, {:owned, facts})

          receive do
            :close_now -> :ok
          after
            30_000 -> exit(:barrier_never_released)
          end
      end

      ctx = context(run_dir, "gated_run_seed", gated_index(), barrier: barrier)

      {caller, caller_mon} =
        spawn_monitor(fn ->
          send(parent, {:result, invoke("cancel", %{"reason" => "review"}, run_id, CommandId.generate(), ctx)})
        end)

      track!(caller)
      assert_receive {:owned, facts}, 30_000
      track!(Map.values(facts))
      # the Server holds a completed result the owner never gets to collect
      {:ok, _} = run_server().await(facts.server, :infinity)
      opened = Writer.opened(facts.writer)
      lock_path = Path.join(run_dir, opened.lock_path)
      assert File.exists?(lock_path)
      server_mon = Process.monitor(facts.server)
      root_mon = Process.monitor(facts.supervisor)
      Process.exit(facts.writer, :kill)
      assert_receive {:DOWN, ^server_mon, :process, _, _}, 30_000, "rest_for_one took the Server down first"
      assert_receive {:DOWN, ^root_mon, :process, _, _}, 30_000, "max_restarts 0: the subtree root is gone too"
      assert Process.alive?(facts.owner), "the owner is still parked at the barrier"
      send(facts.owner, :close_now)
      assert_receive {:result, result}, 30_000

      assert result == {:error, %{clause: "run_server_down"}},
             "a Server gone before the owner's await is the closed run_server_down, never a claimed close"

      refute inspect(result, limit: :infinity) =~ ~r/#PID|Elixir\.GenServer|killed/, "no raw exit payload"
      for {name, pid} <- Map.delete(facts, :owner), do: refute(Process.alive?(pid), "#{name} joined by teardown")
      assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000
      assert File.exists?(lock_path), "the killed Writer never released its lock; nothing claims it released"
    end

    # R3 (m_1788661922000): a DETERMINISTIC death-during-close witness. The Writer is killed from inside its own close
    # (the FaultFs :close hook runs in the Writer process while it tears down the descriptor), so the owner's close call
    # is answered by the Writer's death, never by an acknowledgement: close_unproven cause writer_down, lock left.
    @tag timeout: 90_000
    test "a Writer that dies DURING the close (killed inside its close): close_unproven writer_down; all pids joined" do
      require_executor!()
      {_, :cancel, _, lines, _} = Enum.at(H.cases(), 2)
      run_dir = seed_legacy!(tmp_run_dir(), lines)
      run_id = run_id_of(run_dir)
      parent = self()
      fs = FaultFs.new()

      FaultFs.inject(
        fs,
        :close,
        fn _ -> true end,
        {:hook,
         fn ->
           if Seam.get(:kill_writer_in_close), do: Process.exit(self(), :kill)
           true
         end}
      )

      barrier = fn
        :handoff_received, _facts ->
          :ok

        :subtree_started, facts ->
          send(parent, {:owned, facts})

          receive do
            :close_now -> :ok
          after
            30_000 -> exit(:barrier_never_released)
          end
      end

      ctx = context(run_dir, "gated_run_seed", gated_index(), fs: fs, barrier: barrier)

      {caller, caller_mon} =
        spawn_monitor(fn ->
          send(parent, {:result, invoke("cancel", %{"reason" => "review"}, run_id, CommandId.generate(), ctx)})
        end)

      track!(caller)
      assert_receive {:owned, facts}, 30_000
      track!(Map.values(facts))
      {:ok, _} = run_server().await(facts.server, :infinity)
      opened = Writer.opened(facts.writer)
      lock_path = Path.join(run_dir, opened.lock_path)
      assert File.exists?(lock_path)
      writer_mon = Process.monitor(facts.writer)
      Seam.put(:kill_writer_in_close, true)
      send(facts.owner, :close_now)
      assert_receive {:DOWN, ^writer_mon, :process, _, :killed}, 30_000, "the Writer died inside its close"
      assert_receive {:result, {:ok, result}}, 80_000
      close = Map.get(result, :close)

      assert match?({:error, %{clause: "close_unproven", cause: "writer_down"}}, close),
             "death during close is not an ack"

      assert result.summary["status"] == "completed"
      refute inspect(result, limit: :infinity) =~ ~r/#PID|Elixir\.GenServer|killed/, "no raw exit payload"
      for {name, pid} <- Map.delete(facts, :owner), do: refute(Process.alive?(pid), "#{name} joined by teardown")
      assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000
      assert File.exists?(lock_path), "the lock the dying Writer never released is left behind, not claimed released"
    end

    test "a normally closed Writer proves its close: no close key on the result and the lock is released" do
      require_executor!()
      run_dir = tmp_run_dir()
      ctx = context(run_dir, "gated_run_seed", gated_index())
      assert {:ok, result} = invoke("start", start_args(ctx), "run_e7_close_ok", CommandId.generate(), ctx)
      refute Map.has_key?(result, :close)
      assert_released!(run_dir)

      # a released lock leaves a tombstone, never a held owner
      assert RunLock.owner(SystemFs.new(), run_dir) == :none
    end

    # EA-M2: attribution shapes on the accepted prefix - only a structured stamp names an identity
    test "a read-valid legacy literal attribution is a non-matching identity: the terminal no-op stands, no crash" do
      require_executor!()
      {_, :cancel, _, lines, _} = Enum.at(H.cases(), 2)

      literal =
        List.update_at(lines, 0, fn raw ->
          raw |> Jason.decode!() |> put_in(["data", "requested_by"], "operator") |> Jason.encode!()
        end)

      run_dir = seed_legacy!(tmp_run_dir(), literal)
      assert match?(%{repair: nil}, writer_accepts!(run_dir)), "the legacy literal is read-valid"
      before = journal_bytes(run_dir)
      ctx = context(run_dir, "gated_run_seed", gated_index(), effect_observer: observer_to(self()))

      assert {:ok, %{summary: %{"status" => "completed"}, appended_events: []}} =
               invoke("cancel", %{"reason" => "review"}, run_id_of(run_dir), CommandId.generate(), ctx)

      assert journal_bytes(run_dir) == before
      refute_received {:effect_ran, _}
    end

    test "absent attribution and a structured stamp of ANOTHER command are non-matching: the command executes normally" do
      require_executor!()
      # absent attribution (plain legacy seed): a fresh cancel executes
      absent = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
      ctx = context(absent, "kill9_resume", pre_dispatch_index())

      assert {:ok, %{summary: %{"status" => "cancelled"}, appended_events: [%{"type" => "run_cancel_requested"} | _]}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, run_id_of(absent), CommandId.generate(), ctx)

      # a structured stamp naming a DIFFERENT command on run_created: still no match for this cancel; it executes
      stamped =
        seed_legacy!(
          tmp_run_dir(),
          stamp_first(
            kill9("events_pre_dispatch.jsonl"),
            expected_stamp(@operator, "start", %{"spec_hash" => @zero, "plan_hash" => @zero}, CommandId.generate())
          )
        )

      assert %{repair: nil} = writer_accepts!(stamped)
      ctx2 = context(stamped, "kill9_resume", pre_dispatch_index())

      assert {:ok, %{summary: %{"status" => "cancelled"}, appended_events: [%{"type" => "run_cancel_requested"} | _]}} =
               invoke("cancel", %{"reason" => "operator_cancel"}, run_id_of(stamped), CommandId.generate(), ctx2)
    end

    # EA-M3: owner-local failures run under a closed boundary AFTER teardown; nothing raw leaves
    for {label, failure} <- [{"raise", :raise}, {"throw", :throw}, {"exit", :exit}, {"invalid result", :invalid}] do
      test "an owner-local barrier #{label} is closed run_executor_down after full teardown; no canary in reply, DOWN or logs" do
        require_executor!()
        run_dir = tmp_run_dir()
        parent = self()
        canary = "PRIVATE_OWNER_FAILURE_CANARY_#{System.unique_integer([:positive])}"

        barrier = fn
          :handoff_received, _facts ->
            :ok

          :subtree_started, facts ->
            send(parent, {:owned, facts})

            receive do
              :fail_now -> :ok
            after
              30_000 -> exit(:barrier_never_released)
            end

            case unquote(failure) do
              :raise -> raise(canary)
              :throw -> throw({:private, canary})
              :exit -> exit({:private, canary})
              :invalid -> {:not_ok, canary}
            end
        end

        ctx = context(run_dir, "gated_run_seed", gated_index(), barrier: barrier)

        {caller, caller_mon} =
          spawn_monitor(fn ->
            send(parent, {:owner_result, invoke("start", start_args(ctx), "run_e7_owner", CommandId.generate(), ctx)})
          end)

        track!(caller)

        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert_receive {:owned, facts}, 30_000
            track!(Map.values(facts))
            owner_mon = Process.monitor(facts.owner)
            send(facts.owner, :fail_now)
            assert_receive {:DOWN, ^owner_mon, :process, _owner, owner_reason}, 60_000
            assert_receive {:owner_result, result}, 5_000
            assert {:error, %{clause: "run_executor_down", kind: kind, class: class, digest: digest}} = result
            assert kind in [:error, :throw, :exit] and is_binary(class) and digest =~ ~r/\Asha256:[0-9a-f]{64}\z/
            refute inspect(result, limit: :infinity) =~ canary
            refute inspect(owner_reason, limit: :infinity) =~ canary
            assert owner_reason == :normal, "the owner completes normally after the closed boundary"

            for pid <- Map.values(facts),
                do: refute(Process.alive?(pid), "owned process #{inspect(pid)} reaped before the reply")

            assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000
            Logger.flush()
          end)

        refute log =~ canary
        assert_released!(run_dir)
      end
    end

    # EA-M5: an owner-local failure COMBINED with an externally forced stop - the catch scope must still hold every
    # captured identity. Adapted from the review probe with its survivor assertion INVERTED to the required
    # postcondition (disclosed): the Writer must be dead, not alive.
    for {label, failure} <- [{"raise", :raise}, {"throw", :throw}, {"exit", :exit}, {"invalid result", :invalid}] do
      @tag timeout: 120_000
      test "owner #{label} while root and Writer are suspended: every captured pid is reaped before the closed reply" do
        require_executor!()
        run_dir = tmp_run_dir()
        parent = self()
        canary = "PRIVATE_COMBINED_OWNER_FAILURE_#{System.unique_integer([:positive])}"

        barrier = fn
          :handoff_received, _facts ->
            :ok

          :subtree_started, facts ->
            send(parent, {:owned, facts})

            receive do
              :fail_now -> :ok
            after
              30_000 -> exit(:barrier_never_released)
            end

            case unquote(failure) do
              :raise -> raise(canary)
              :throw -> throw({:private, canary})
              :exit -> exit({:private, canary})
              :invalid -> {:not_ok, canary}
            end
        end

        ctx = context(run_dir, "gated_run_seed", gated_index(), barrier: barrier)

        {caller, caller_mon} =
          spawn_monitor(fn ->
            send(parent, {:owner_result, invoke("start", start_args(ctx), "run_e7_combined", CommandId.generate(), ctx)})
          end)

        track!(caller)
        assert_receive {:owned, facts}, 30_000
        track!(Map.values(facts))
        owner_mon = Process.monitor(facts.owner)

        # the TEST owns the suspension (root and Writer), so the orderly stop cannot succeed and a root-only map
        # would leak
        :erlang.suspend_process(facts.supervisor)
        :erlang.suspend_process(facts.writer)

        try do
          send(facts.owner, :fail_now)
          assert_receive {:DOWN, ^owner_mon, :process, _owner, owner_reason}, 90_000
          assert owner_reason == :normal
          assert_receive {:owner_result, result}, 5_000
          assert {:error, %{clause: "run_executor_down", kind: kind, class: class, digest: digest}} = result
          assert kind in [:error, :throw, :exit] and is_binary(class) and digest =~ ~r/\Asha256:[0-9a-f]{64}\z/
          refute inspect(result, limit: :infinity) =~ canary
          for {name, pid} <- Map.delete(facts, :owner), do: refute(Process.alive?(pid), "#{name} reaped before the reply")
          assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000
          assert_released!(run_dir)
        after
          for pid <- Map.values(facts), Process.alive?(pid), do: :erlang.resume_process(pid)
        end
      end
    end

    # EA-M4: a forced root stop must reap still-live descendants under bounds; root death is never subtree closure
    @tag timeout: 120_000
    test "forced stop with a suspended supervisor AND Writer: the Writer is reaped too, ownership released, no live owned pid" do
      require_executor!()
      run_dir = tmp_run_dir()
      parent = self()

      barrier = fn
        :handoff_received, _facts ->
          :ok

        :subtree_started, facts ->
          send(parent, {:owned_for_stop, facts})

          receive do
            :drive_now -> :ok
          after
            30_000 -> exit(:barrier_never_released)
          end
      end

      ctx = context(run_dir, "gated_run_seed", gated_index(), barrier: barrier)

      {caller, caller_mon} =
        spawn_monitor(fn -> invoke("start", start_args(ctx), "run_e7_forced", CommandId.generate(), ctx) end)

      track!(caller)
      assert_receive {:owned_for_stop, facts}, 30_000
      track!(Map.values(facts))
      owner_mon = Process.monitor(facts.owner)
      # the TEST suspends the root and the Writer: the orderly stop cannot complete, the Writer cannot be shut down
      # by it
      :erlang.suspend_process(facts.supervisor)
      :erlang.suspend_process(facts.writer)

      try do
        send(facts.owner, :drive_now)
        Process.exit(caller, :kill)
        assert_receive {:DOWN, ^caller_mon, :process, ^caller, :killed}, 5_000
        # the owner escalates past the root to every owned identity and joins each under a bound
        assert_receive {:DOWN, ^owner_mon, :process, _owner, :normal}, 90_000
        refute Process.alive?(facts.supervisor)
        refute Process.alive?(facts.writer), "a still-live descendant is reaped, not left behind root death"
        refute Process.alive?(facts.server)
        refute Process.alive?(facts.work)
        assert_released!(run_dir)
      after
        for pid <- Map.values(facts), Process.alive?(pid), do: :erlang.resume_process(pid)
      end
    end

    test "the receipt/chain corpus through invoke: the Writer's exact rejections, bytes unchanged, no effects; a torn tail is repaired" do
      require_executor!()
      legacy = kill9("events_pre_dispatch.jsonl")
      v2 = rechain_v2(legacy)
      run_id = legacy |> hd() |> Jason.decode!() |> Map.fetch!("run_id")

      cases = [
        {"receipt on legacy", tmp_run_dir() |> seed_legacy!(legacy) |> bad_receipt!(9, "x\n"),
         "receipt_on_legacy_journal"},
        {"receipt hash mismatch", tmp_run_dir() |> seed_v2!(v2) |> bad_receipt!(9, "t\n"), "receipt_hash_mismatch"},
        {"receipt missing", tmp_run_dir() |> seed_v2!(v2) |> tap(&File.rm!(Path.join(&1, "events.head"))),
         "receipt_missing"},
        {"one corrupted link", seed_v2!(tmp_run_dir(), corrupt_link(v2, 2)), "chain_mismatch"}
      ]

      for {label, run_dir, clause} <- cases do
        expected = writer_rejection!(run_dir)
        assert expected.clause == clause, label
        before = journal_bytes(run_dir)
        ctx = context(run_dir, "kill9_resume", pre_dispatch_index(), effect_observer: observer_to(self()))

        assert invoke("resume", %{"recovery_reason" => "x"}, run_id, CommandId.generate(), ctx) == {:error, expected},
               label

        assert journal_bytes(run_dir) == before, label
        refute_received {:effect_ran, _}
      end

      # torn tail: the run id is known from the accepted prefix BEFORE the tail is appended
      # (a torn journal cannot be decoded, so nothing may be read from it afterwards)
      # derived-live input (labelled) so the repaired resume can complete under D1
      torn = seed_legacy!(tmp_run_dir(), live(legacy))
      File.write!(Path.join(torn, "events.jsonl"), File.read!(Path.join(torn, "events.jsonl")) <> ~s({"schema":"ai-orch))
      ctx = context(torn, "kill9_resume", pre_dispatch_index(), observe_fence_observer: self())

      assert {:ok, %{appended_events: [resumed | _]}} =
               invoke("resume", %{"recovery_reason" => "x"}, run_id, CommandId.generate(), ctx)

      assert_live_arm!()

      assert match?(%{"action" => "truncate_tail", "truncated_bytes" => 18}, resumed["data"]["tail_repair"]),
             "the Writer's repair, journaled by the Writer"
    end
  end

  # =================================================================================================
  describe "E-8 (B) sole entry and E-9 (B) operator path" do
    test "the CLI has no direct lifecycle path; RunFSM is an uncalled, documented-unsupported seam; the executor holds no Host logic" do
      require_executor!()
      cli = File.read!(@cli_src)

      for forbidden <- ["RunFSM.", "Writer.open(", "with_writer", "run_then_close", "event_sink"],
          do: refute(cli =~ forbidden, forbidden)

      # the sole lifecycle entry is Commands.invoke/4, reached through the SHARED trusted preparation
      # (docs/contracts/public-console-seam.org): the CLI routes every verb through Prepare.Trusted.invoke and
      # never calls Commands.invoke or any lifecycle seam itself; Trusted holds exactly that one entry
      assert cli =~ "Trusted.invoke("
      refute cli =~ "Commands.invoke("
      trusted = File.read!(@trusted_src)
      assert trusted =~ "Commands.invoke("

      for forbidden <- ["RunFSM.", "Writer.open(", "with_writer", "run_then_close", "event_sink"],
          do: refute(trusted =~ forbidden, forbidden)

      assert File.read!(@run_fsm_src) =~ ~r/not a supported public lifecycle (surface|entry)/i
      lib = Path.expand("../../lib", __DIR__)

      callers =
        lib
        |> Path.join("**/*.ex")
        |> Path.wildcard()
        |> Enum.filter(&(File.read!(&1) =~ ~r/\bRunFSM\./))
        |> Enum.reject(&String.ends_with?(&1, "run_fsm.ex"))

      assert callers == [], "RunFSM callers in lib: #{inspect(callers)}"
      src = File.read!(@executor_src)

      for forbidden <- ["defp commit(", "Effects.settle(", "Reducer.step(", "Writer.append(", "Process.flag(:trap_exit"],
          do: refute(src =~ forbidden, forbidden)
    end

    test "CLI run, resume and cancel journal operator stamps on NONTERMINAL runs; terminal resume/cancel are no-ops" do
      require_executor!()
      spec_bytes = Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json"))
      plan_bytes = Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json"))
      registry_root = Path.join(System.tmp_dir!(), "command-executor-registry-#{System.unique_integer([:positive])}")

      cli_opts =
        gated_index()
        |> fresh_opts()
        |> Keyword.drop([:event_sink, :prompt_root])
        |> Keyword.merge(pane_registry_root: registry_root, operator: "local_operator")

      write_inputs = fn dir ->
        File.write!(Path.join(dir, "spec.json"), spec_bytes)
        File.write!(Path.join(dir, "plan.json"), plan_bytes)
        dir
      end

      # start: the stamp binds the REAL consumed bytes
      run_dir = write_inputs.(tmp_run_dir())
      assert %{status: 0, stdout: stdout} = AiOrchestrator.CLI.run(["run", run_dir], cli_opts)
      assert stdout =~ "* Status: completed"
      [created | _] = journal(run_dir)

      assert %{
               "class" => "operator",
               "id" => "local_operator",
               "verb" => "start",
               "command_id" => command_id,
               "args_hash" => args_hash
             } = created["data"]["requested_by"]

      assert {:ok, ^command_id} = CommandId.validate(command_id)

      consumed = %{
        "spec_hash" => sha256(File.read!(Path.join(run_dir, "spec.json"))),
        "plan_hash" => sha256(File.read!(Path.join(run_dir, "plan.json")))
      }

      assert args_hash == Arguments.hash("start", consumed), "ARGS-CANON-1 over sha256 of the bytes the CLI actually read"
      assert created["data"]["spec_hash"] == consumed["spec_hash"]
      # terminal no-ops through the CLI: status 0, bytes unchanged, no stamp added (existing reducer semantics)
      before = journal_bytes(run_dir)
      assert %{status: 0} = AiOrchestrator.CLI.run(["run", "--resume", run_dir], cli_opts)
      assert %{status: 0} = AiOrchestrator.CLI.run(["cancel", run_dir], cli_opts)
      assert journal_bytes(run_dir) == before

      # NONTERMINAL fixtures: a run interrupted right after run_started (real machinery), then CLI resume / cancel
      nonterminal = fn ->
        dir = write_inputs.(tmp_run_dir())

        started_seq =
          seq_of(oracle_run(context(dir, "gated_run_seed", gated_index()), gated_index(), "run_oracle"), "run_started")

        fs = FaultFs.new()
        kill_after_receipt(fs, self(), started_seq)

        kill_ctx =
          context(dir, "gated_run_seed", gated_index(),
            fs: fs,
            spec_hash: consumed["spec_hash"],
            plan_hash: consumed["plan_hash"]
          )

        assert {:error, %{clause: "run_server_down"}} =
                 invoke(
                   "start",
                   consumed,
                   "run_cli_#{System.unique_integer([:positive])}",
                   CommandId.generate(),
                   kill_ctx
                 )

        assert_received {:durable, ^started_seq}
        assert List.last(journal(dir))["type"] == "run_started"
        dir
      end

      resume_dir = nonterminal.()
      assert %{status: 0} = AiOrchestrator.CLI.run(["run", "--resume", resume_dir], cli_opts)

      assert %{"verb" => "resume", "class" => "operator", "id" => "local_operator"} =
               resume_dir |> journal() |> Enum.find(&(&1["type"] == "run_resumed")) |> get_in(["data", "requested_by"])

      cancel_dir = nonterminal.()
      assert %{status: 0} = AiOrchestrator.CLI.run(["cancel", cancel_dir], cli_opts)

      assert %{"verb" => "cancel", "class" => "operator", "id" => "local_operator"} =
               cancel_dir
               |> journal()
               |> Enum.find(&(&1["type"] == "run_cancel_requested"))
               |> get_in(["data", "requested_by"])

      assert Enum.any?(journal(cancel_dir), &(&1["type"] == "run_cancelled"))
    end
  end
end
