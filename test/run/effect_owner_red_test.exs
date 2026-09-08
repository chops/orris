defmodule AiOrchestrator.Run.EffectOwnerRedTest do
  @moduledoc """
  RED/interface for the effect owner lifecycle through the EXECUTOR path (contract revision 3). Controls are
  INVARIANT: they hold today and under the intended implementation; the before-state measurements that contradict
  the intended behavior (who executes, a blocked Server, run_server_down for an executing-process kill, the
  four-key diagnostic, the root disclosure) are recorded as historical evidence in the contract, not as tests.
  Ownership: every pid a row learns of goes through the tracked harness and is reaped on any failure.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1, spawn_caller!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.OwnerDoubles.AbandonGate
  alias AiOrchestrator.Test.OwnerDoubles.HoldingDispatch
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-06T08:00:00Z", unix: 1_788_681_600}
  @instance "sup_owner_red"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @sentinel "OWNER-RED-SENTINEL-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @unknown_cleanup %{attempts: :unknown, settled: 0, unproven: :unknown}

  defp worker, do: Module.concat(["AiOrchestrator", "Run", "Worker"])
  defp require_worker!, do: assert(Code.ensure_loaded?(worker()), "AiOrchestrator.Run.Worker does not exist")
  defp run_server, do: AiOrchestrator.Run.Server

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "effect-owner-red-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  defp gated_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp sha(term), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, Jason.encode!(term)), case: :lower)

  defp context(dir, extra) do
    spec = H.spec("gated_run_seed")
    plan = H.plan("gated_run_seed")

    gated_opts()
    |> Keyword.drop(@owned)
    |> Keyword.merge(
      run_dir: dir,
      spec: spec,
      plan: plan,
      spec_hash: sha(spec),
      plan_hash: sha(plan),
      supervisor_instance: @instance,
      trace: collector(),
      barrier: facts_barrier(collector()),
      effect_observer: observer(collector())
    )
    |> Keyword.merge(extra)
  end

  defp held(dir, extra \\ []), do: context(dir, [gate_opts: [runner: OwnerDoubles.held_gate(collector())]] ++ extra)

  defp holding_dispatch(dir, extra \\ []),
    do: context(dir, [dispatch: HoldingDispatch, dispatch_opts: [collector: collector(), sentinel: @sentinel]] ++ extra)

  defp abandon_gate(dir, mode, extra) do
    AbandonGate.control(collector(), mode)
    context(dir, [gate_executor: AbandonGate] ++ extra)
  end

  defp start!(ctx) do
    args = %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]}

    spawn_caller!(fn ->
      Commands.invoke(@operator, "start", args,
        run_id: "run_owner_red",
        command_id: CommandId.generate(),
        now: @now,
        executor: AiOrchestrator.Run.Executor,
        executor_opts: ctx
      )
    end)
  end

  defp facts! do
    receive do
      {:facts, facts} -> facts
    after
      30_000 -> flunk("no subtree facts")
    end
  end

  # the Owner's barrier: :subtree_started hands the facts to the COLLECTOR and returns at once; a held name parks
  # the Owner until the test releases it (two-way, acknowledged) - used for the birth/handoff boundaries
  defp facts_barrier(collector, hold \\ nil) do
    fn name, info ->
      cond do
        name == :subtree_started ->
          send(collector, {:facts, info})
          :ok

        name == hold ->
          ref = make_ref()
          send(collector, {:held, name, self(), ref})

          receive do
            {:release, ^ref} -> :ok
          after
            30_000 -> exit(:barrier_never_released)
          end

        true ->
          :ok
      end
    end
  end

  # the Server-side birth barrier (opts[:birth_barrier], test-only): :before_birth / :registered in the Server
  defp birth_barrier(collector, hold) do
    fn name, info ->
      if name == hold do
        ref = make_ref()
        send(collector, {:held, name, self(), ref, info})

        receive do
          {:release, ^ref} -> :ok
        after
          30_000 -> exit(:birth_barrier_never_released)
        end
      else
        :ok
      end
    end
  end

  defp observer(collector) do
    fn effect, _observation ->
      send(collector, {:observed, effect.__struct__})
      :ok
    end
  end

  defp journal(dir) do
    case File.read(Path.join(dir, "events.jsonl")) do
      {:ok, s} -> s |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
      _ -> []
    end
  end

  defp messages do
    {:messages, messages} = Process.info(self(), :messages)
    messages
  end

  defp count(tag), do: Enum.count(messages(), &(is_tuple(&1) and elem(&1, 0) == tag))

  defp applied_executes(server), do: for({:run_effect_applied, ^server, {:execute, _, _, _} = key} <- messages(), do: key)

  # the counting oracle, pure over a message list: every applied execute produced exactly one observer call
  def observer_matches_applied?(mailbox, server) do
    applied = Enum.count(mailbox, &match?({:run_effect_applied, ^server, {:execute, _, _, _}}, &1))
    observed = Enum.count(mailbox, &match?({:observed, _}, &1))
    applied > 0 and observed == applied
  end

  # the run's own consistency, no cross-run counts: contiguous seqs, one gate start/pass, completed
  defp run_consistent!(dir) do
    events = journal(dir)
    assert Enum.map(events, & &1["seq"]) == Enum.to_list(1..length(events))
    assert Enum.count(events, &(&1["type"] == "gate_started")) == 1
    assert Enum.count(events, &(&1["type"] == "gate_passed")) == 1
    assert List.last(events)["type"] == "run_completed"
    events
  end

  defp status_sample(named) do
    for {name, pid} <- named, is_pid(pid) do
      try do
        {name, :live, inspect(:sys.get_status(pid, 500), limit: :infinity)}
      catch
        :exit, _ -> {name, :blocked, ""}
      end
    end
  end

  defp sentinel_free!(sample, names) do
    for {name, :live, status} <- sample,
        name in names,
        do: refute(status =~ @sentinel, "sentinel in the status of #{name}")
  end

  defp expected_digest(term), do: Diagnostic.describe(term)["digest"]

  # =================================================================================================
  describe "R-1 topology" do
    test "control (invariant): the held effect executes in exactly ONE owned process, gone after the result",
         %{dir: dir} do
      {_caller, cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000

      assert executing in Map.values(facts) or
               Enum.any?(messages(), &match?({:run_child_started, _, :worker, ^executing}, &1))

      refute_receive {:gate_entered, _}, 100, "one entry"
      send(executing, :release_gate)
      assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
      assert_receive {:DOWN, ^cmon, :process, _, :normal}, 5_000
      refute Process.alive?(executing)
    end

    test "R-1 one worker born under Work after discovery, traced, handed to the Owner, executes, gone after",
         %{dir: dir} do
      require_worker!()
      {_caller, cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:run_child_started, work_sup, :worker, worker_pid}, 10_000
      assert work_sup == facts.work and is_pid(worker_pid)
      assert facts[:worker] == worker_pid, "the Owner's facts carry the worker BEFORE any effect (handoff)"
      assert_receive {:gate_entered, executing}, 10_000
      assert executing == worker_pid, "the effect executes inside the worker, not the Server"
      assert [{_, ^worker_pid, :worker, _}] = DynamicSupervisor.which_children(facts.work)
      send(executing, :release_gate)
      assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
      assert_receive {:DOWN, ^cmon, :process, _, :normal}, 5_000
      refute_receive {:run_child_started, _, :worker, _}, 100, "one birth only"
      refute Process.alive?(worker_pid)
    end
  end

  describe "R-2 responsiveness" do
    test "R-2 Run.Server.status/1 answers :driving within 1 s while a gate is held inside the worker", %{dir: dir} do
      require_worker!()
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000
      assert_receive {:run_child_started, _, :worker, ^executing}, 10_000
      task = Task.async(fn -> run_server().status(facts.server) end)
      track!(task.pid)
      assert Task.yield(task, 1_000) == {:ok, :driving}, "the Server must not be blocked by the held effect"
      send(executing, :release_gate)
      assert_receive {:result, {:ok, _}}, 30_000
    end
  end

  describe "R-3 executing-process death mid-effect" do
    test "control (invariant): killing the executing process mid-effect closes; no second gate entry; all joined",
         %{dir: dir} do
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000
      Process.exit(executing, :kill)
      assert_receive {:result, {:error, %{clause: clause}}}, 30_000
      assert is_binary(clause)
      refute_receive {:gate_entered, _}, 200

      for {name, pid} <- Map.take(facts, [:supervisor, :server, :writer, :work]),
          do: refute(Process.alive?(pid), "#{name}")
    end

    test "R-3 kill the worker mid-effect: the SERVER closes run_effect_owner_down, the OWNER tears down; no rebirth",
         %{dir: dir} do
      require_worker!()
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:run_child_started, _work, :worker, worker_pid}, 10_000
      assert_receive {:gate_entered, ^worker_pid}, 10_000
      smon = Process.monitor(facts.server)
      Process.exit(worker_pid, :kill)
      assert_receive {:result, {:error, %{clause: "run_effect_owner_down"}}}, 30_000
      refute_receive {:run_child_started, _, :worker, _}, 200
      refute_receive {:gate_entered, _}, 200

      server = facts.server
      assert_receive {:DOWN, ^smon, :process, ^server, server_reason}, 5_000
      assert server_reason == :shutdown

      for {name, pid} <- Map.take(facts, [:supervisor, :server, :writer, :work]),
          do: refute(Process.alive?(pid), "#{name}")
    end
  end

  describe "R-4 counting oracle (pure self-test)" do
    test "control: complete / duplicate / missing observer calls are told apart" do
      server = self()
      key = {:execute, make_ref(), 1, make_ref()}

      complete = [
        {:run_effect_applied, server, key},
        {:observed, Effect.AwaitGate},
        {:run_effect_reply_dropped, server, :x}
      ]

      assert observer_matches_applied?(complete, server)
      refute observer_matches_applied?(complete ++ [{:observed, Effect.AwaitGate}], server), "a duplicate observer call"
      refute observer_matches_applied?([{:run_effect_applied, server, key}], server), "a missing observer call"
      refute observer_matches_applied?([{:observed, Effect.AwaitGate}], server), "nothing applied"
      other = spawn(fn -> :ok end)
      refute observer_matches_applied?([{:run_effect_applied, other, key}, {:observed, Effect.AwaitGate}], server)
    end
  end

  describe "R-4 correlation vs the current outstanding request" do
    # the current run: the held gate is the AwaitGate execute of the Server in THIS run's facts; after the flush the
    # LAST requested execute of that kind by that Server is the outstanding one, and it has not been applied
    defp outstanding!(facts) do
      OwnedHarness.flush!()
      server = facts.server

      requested =
        for {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate} = record} <- messages(), do: record

      assert [record] = Enum.take(requested, -1)
      %{cap: cap, gen: gen, ref: ref} = record
      refute_received {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^ref}}, "still outstanding (baseline oracle)"
      record
    end

    defp release_record!(facts) do
      server = facts.server
      OwnedHarness.flush!()
      records = for {:run_effect_requested, ^server, %{op: :release} = record} <- messages(), do: record
      assert [record] = Enum.take(records, -1)
      record
    end

    test "control (invariant): random forgeries into the Server mid-effect are ignored; run consistent",
         %{dir: dir} do
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000
      send(facts.server, {:effect_result, make_ref(), 1, make_ref(), self(), :forged})
      send(facts.server, {:effect_failed, make_ref(), 1, make_ref(), self(), %{clause: "forged"}})
      send(facts.server, {:released, make_ref(), 1, make_ref(), self()})
      send(facts.server, {:settled, make_ref(), 1, make_ref(), self(), []})
      send(executing, :release_gate)
      assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
      run_consistent!(dir)
      assert count(:gate_entered) == 0, "one gate entry, already consumed"
    end

    for {label, field, reason} <- [
          {"R-4a a matching (cap, gen, ref) from the WRONG claimed sender", :sender, :sender_mismatch},
          {"R-4b a matching cap/ref with a STALE generation", :generation, :generation_stale},
          {"R-4c a matching cap/gen with the WRONG ref", :ref, :ref_mismatch},
          {"R-4d the WRONG capability with matching gen/ref", :cap, :cap_mismatch},
          {"R-4e a reply of the WRONG op (a duplicate released for this stage)", :op, :op_mismatch}
        ] do
      test "#{label}: dropped with its reason; nothing applied", %{dir: dir} do
        require_worker!()
        {_caller, _cmon} = start!(held(dir))
        facts = facts!()
        assert_receive {:gate_entered, executing}, 10_000
        %{cap: cap, gen: gen, ref: ref, worker: ^executing} = outstanding!(facts)
        server = facts.server
        forged = %{"forged" => true}

        message =
          case unquote(field) do
            :sender -> {:effect_result, cap, gen, ref, self(), forged}
            :generation -> {:effect_result, cap, gen + 1, ref, executing, forged}
            :ref -> {:effect_result, cap, gen, make_ref(), executing, forged}
            :cap -> {:effect_result, make_ref(), gen, ref, executing, forged}
            :op -> {:released, cap, gen, release_record!(facts).ref, executing}
          end

        send(server, message)
        assert_receive {:run_effect_reply_dropped, ^server, unquote(reason)}, 5_000
        assert run_server().status(server) == :driving
        refute_received {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^ref}}
        send(executing, :release_gate)
        assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
        OwnedHarness.flush!()
        run_consistent!(dir)
        # the FULL correlated record stays in the mailbox (nothing consumed): the counting oracle sees every apply
        assert {:execute, cap, gen, ref} in applied_executes(server), "the held stage was applied after the release"
        assert observer_matches_applied?(messages(), server), "one observer call per applied execute, none for a forgery"
        assert count(:gate_entered) == 0
      end
    end

    test "R-4f positive control: each op requested and applied once under a unique {op, ref}; nothing dropped",
         %{dir: dir} do
      require_worker!()
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000
      send(executing, :release_gate)
      assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
      OwnedHarness.flush!()
      server = facts.server
      requested = for {:run_effect_requested, ^server, %{op: op, ref: ref}} <- messages(), do: {op, ref}
      applied = for {:run_effect_applied, ^server, {op, _cap, _gen, ref}} <- messages(), do: {op, ref}
      assert requested != [] and Enum.sort(requested) == Enum.sort(applied)
      assert length(Enum.uniq(applied)) == length(applied), "unique full operation keys"
      assert Enum.any?(requested, &match?({:release, _}, &1)) and Enum.any?(requested, &match?({:settle, _}, &1))
      refute_received {:run_effect_reply_dropped, _, _}
    end
  end

  describe "R-5 origin canary (F-1 subunit)" do
    test "control (invariant): held deliver: Writer/Work live and sentinel-free; the raise closes run_server_down; no sentinel in result or log",
         %{dir: dir} do
      log =
        capture_log(fn ->
          {_caller, _cmon} = start!(holding_dispatch(dir))
          facts = facts!()
          assert_receive {:deliver_entered, executing}, 10_000
          sample = status_sample(Map.take(facts, [:writer, :work]))
          assert Enum.count(sample, &match?({_, :live, _}, &1)) == 2
          sentinel_free!(sample, [:writer, :work])
          send(executing, :raise)
          assert_receive {:result, {:error, %{clause: "run_server_down"} = rejection}}, 30_000
          refute inspect(rejection, limit: :infinity) =~ @sentinel
        end)

      refute log =~ @sentinel
    end

    test "F-1 the ROOT's printable sys status carries no sentinel while the adapter holds it (direct)",
         %{dir: dir} do
      {_caller, _cmon} = start!(holding_dispatch(dir))
      facts = facts!()
      assert_receive {:deliver_entered, executing}, 10_000
      [{:supervisor, :live, status}] = status_sample(Map.take(facts, [:supervisor]))
      refute status =~ @sentinel, "the root supervisor's status must not print the run config"
      send(executing, :proceed)
      assert_receive {:result, _}, 30_000
    end

    test "control (invariant): the child-termination report prints closures, not the config: no sentinel",
         %{dir: dir} do
      log =
        capture_log(fn ->
          {_caller, _cmon} = start!(holding_dispatch(dir))
          _facts = facts!()
          assert_receive {:deliver_entered, executing}, 10_000
          send(executing, :raise)
          assert_receive {:result, {:error, _}}, 30_000
        end)

      assert log =~ "terminating", "a termination report was captured"
      refute log =~ @sentinel
    end

    test "R-5 worker blocked; Server/Writer/Work/root live and sentinel-free; executor parity; sentinel nowhere",
         %{dir: dir} do
      require_worker!()

      log =
        capture_log(fn ->
          {_caller, _cmon} = start!(holding_dispatch(dir))
          facts = facts!()
          assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
          # GREEN transition (U2b delivery deadline, recorded): the deliver runs in the Worker's TASK; the Worker
          # itself is blocked in the runner receive, so the status sample below is unchanged
          assert_receive {:deliver_entered, executing}, 10_000
          refute executing == worker_pid
          wmon = Process.monitor(worker_pid)
          smon = Process.monitor(facts.server)
          sample = status_sample(Map.take(facts, [:worker, :server, :writer, :work, :supervisor]))
          assert Enum.count(sample, &match?({_, :live, _}, &1)) == 4, "the four owners answer while the worker works"
          assert {:worker, :blocked, ""} in sample
          sentinel_free!(sample, [:server, :writer, :work, :supervisor])
          send(executing, :raise)
          assert_receive {:result, {:error, %{clause: "run_server_down"} = rejection}}, 30_000
          refute inspect(rejection, limit: :infinity) =~ @sentinel
          assert_receive {:DOWN, ^wmon, :process, _, wreason}, 5_000
          refute inspect(wreason, limit: :infinity) =~ @sentinel, "the worker's exit reason is closed"
          assert_receive {:DOWN, ^smon, :process, _, sreason}, 5_000
          assert {:run_step_failed, %{kind: :error, class: "map", digest: _, frames: _}} = sreason
          refute inspect(sreason, limit: :infinity) =~ @sentinel
        end)

      refute log =~ @sentinel
    end
  end

  describe "R-6 Writer loss while the effect is held" do
    test "control (invariant): killing the Writer while a gate is held closes the run; held effect never completes",
         %{dir: dir} do
      {_caller, _cmon} = start!(held(dir))
      facts = facts!()
      assert_receive {:gate_entered, executing}, 10_000
      emon = Process.monitor(executing)
      Process.exit(facts.writer, :kill)
      assert_receive {:result, {:error, %{clause: _closed}}}, 30_000
      assert_receive {:DOWN, ^emon, :process, ^executing, _}, 10_000, "the executing process died with the subtree"

      for {name, pid} <- Map.take(facts, [:supervisor, :server, :writer, :work]),
          do: refute(Process.alive?(pid), "#{name}")
    end
  end

  describe "R-7 error / throw / exit inside the effect" do
    for {kind, instruction, class} <- [{:error, :raise, "map"}, {:throw, :throw, "tuple"}, {:exit, :exit, "tuple"}] do
      test "control (invariant): a #{kind} inside deliver ends the Server closed (class #{class}); executor run_server_down",
           %{dir: dir} do
        {_caller, _cmon} = start!(holding_dispatch(dir))
        facts = facts!()
        assert_receive {:deliver_entered, executing}, 10_000
        smon = Process.monitor(facts.server)
        send(executing, unquote(instruction))
        assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
        assert_receive {:DOWN, ^smon, :process, _, reason}, 5_000
        assert {:run_step_failed, %{kind: unquote(kind), class: unquote(class), digest: "sha256:" <> _}} = reason
        refute inspect(reason, limit: :infinity) =~ @sentinel
      end

      test "R-7 a #{kind} inside the WORKER's effect: effect_failed closes the Server with the closed diagnostic; parity",
           %{dir: dir} do
        require_worker!()
        {_caller, _cmon} = start!(holding_dispatch(dir))
        facts = facts!()
        assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
        # GREEN transition (U2b delivery deadline, recorded): the raw failure happens in the Worker's TASK and reaches
        # the same closed boundary (kind/class/digest as before, frames = the task-side depth)
        assert_receive {:deliver_entered, executing}, 10_000
        refute executing == worker_pid
        smon = Process.monitor(facts.server)
        wmon = Process.monitor(worker_pid)
        send(executing, unquote(instruction))
        assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
        assert_receive {:DOWN, ^smon, :process, _, reason}, 5_000
        assert {:run_step_failed, %{kind: unquote(kind), class: unquote(class), digest: "sha256:" <> _}} = reason
        refute inspect(reason, limit: :infinity) =~ @sentinel
        assert_receive {:DOWN, ^wmon, :process, _, wreason}, 5_000
        refute inspect(wreason, limit: :infinity) =~ @sentinel
        refute_receive {:run_child_started, _, :worker, _}, 100
      end
    end
  end

  describe "R-8/9/10 Server-stage failure with a live handle" do
    # the observer fails on the PrepareGate observation: the prepared handle is live in the runtime at that moment
    defp failing_on_prepare(collector, kind) do
      fn effect, _observation ->
        send(collector, {:observed, effect.__struct__})
        if match?(%Effect.PrepareGate{}, effect), do: fail_as(kind), else: :ok
      end
    end

    defp fail_as(:error), do: raise(@sentinel)
    defp fail_as(:throw), do: throw({:observer_throw, @sentinel})
    defp fail_as(:exit), do: exit({:observer_exit, @sentinel})

    defp raising_on_prepare(collector), do: failing_on_prepare(collector, :error)

    defp primary(:error), do: expected_digest(%RuntimeError{message: @sentinel})
    defp primary(:throw), do: expected_digest({:observer_throw, @sentinel})
    defp primary(:exit), do: expected_digest({:observer_exit, @sentinel})
    defp expected_primary, do: primary(:error)

    for kind <- [:error, :throw, :exit] do
      test "control (invariant): observer #{kind} with a live handle: settled exactly once; the Server exits closed with that origin's digest",
           %{dir: dir} do
        ctx = abandon_gate(dir, :ok, effect_observer: failing_on_prepare(collector(), unquote(kind)))
        {_caller, _cmon} = start!(ctx)
        facts = facts!()
        smon = Process.monitor(facts.server)
        assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
        assert_receive {:abandoned, _abandoner, :ok}, 5_000
        refute_receive {:abandoned, _, _}, 100
        assert_receive {:DOWN, ^smon, :process, _, {:run_step_failed, diagnostic}}, 5_000
        assert diagnostic.kind == unquote(kind) and diagnostic.digest == primary(unquote(kind))
        refute inspect(diagnostic, limit: :infinity) =~ @sentinel
      end

      test "R-8 observer #{kind}: the Server requests the WORKER's settlement before exiting: one worker-side abandon; cleanup 1/1/0; digest unchanged",
           %{dir: dir} do
        require_worker!()
        ctx = abandon_gate(dir, :ok, effect_observer: failing_on_prepare(collector(), unquote(kind)))
        {_caller, _cmon} = start!(ctx)
        facts = facts!()
        assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
        smon = Process.monitor(facts.server)
        assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
        assert_receive {:abandoned, ^worker_pid, :ok}, 5_000, "the handle is abandoned where it lives: in the worker"
        refute_receive {:abandoned, _, _}, 100
        assert_receive {:DOWN, ^smon, :process, _, {:run_step_failed, diagnostic}}, 5_000
        assert diagnostic.cleanup == %{attempts: 1, settled: 1, unproven: 0}
        assert diagnostic.kind == unquote(kind) and diagnostic.digest == primary(unquote(kind))
        refute inspect(diagnostic, limit: :infinity) =~ @sentinel
      end
    end

    test "R-9 an abandon that FAILS is reported unproven; the primary error is preserved", %{dir: dir} do
      require_worker!()
      ctx = abandon_gate(dir, :error, effect_observer: raising_on_prepare(collector()))
      {_caller, _cmon} = start!(ctx)
      facts = facts!()
      assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
      smon = Process.monitor(facts.server)
      assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
      assert_receive {:abandoned, ^worker_pid, :error}, 5_000
      assert_receive {:DOWN, ^smon, :process, _, {:run_step_failed, diagnostic}}, 5_000
      assert diagnostic.cleanup == %{attempts: 1, settled: 0, unproven: 1}
      assert diagnostic.digest == expected_primary()
    end

    test "R-10 a BLOCKED owner (abandon hangs): settle budget -> cleanup UNKNOWN; the worker leaves on :shutdown at once",
         %{dir: dir} do
      require_worker!()
      ctx = abandon_gate(dir, :hang, effect_observer: raising_on_prepare(collector()))
      {_caller, _cmon} = start!(ctx)
      facts = facts!()
      assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
      smon = Process.monitor(facts.server)
      wmon = Process.monitor(worker_pid)
      assert_receive {:abandoned, ^worker_pid, :hang}, 10_000
      assert_receive {:DOWN, ^smon, :process, _, {:run_step_failed, diagnostic}}, 15_000
      assert diagnostic.cleanup == @unknown_cleanup, "no reply: nothing is known about the worker's handles"
      assert diagnostic.digest == expected_primary()
      # spike S-6 measurement: a non-trapping owner blocked in a sleep terminates on the :shutdown signal immediately
      assert_receive {:DOWN, ^wmon, :process, _, :shutdown}, 5_000
      assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
    end
  end

  describe "R-11 birth and handoff boundaries" do
    test "R-11a Server killed at :before_birth: no worker is ever born; run_server_down; no survivor",
         %{dir: dir} do
      require_worker!()
      ctx = held(dir, birth_barrier: birth_barrier(collector(), :before_birth))
      {_caller, _cmon} = start!(ctx)
      assert_receive {:held, :before_birth, server, _ref, _info}, 10_000
      Process.exit(server, :kill)
      assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
      refute_receive {:run_child_started, _, :worker, _}, 200
      refute_received {:gate_entered, _}
    end

    test "R-11b Server killed at :registered (unacked): the worker dies with Work; no effect ran; run_server_down",
         %{dir: dir} do
      require_worker!()
      ctx = held(dir, birth_barrier: birth_barrier(collector(), :registered))
      {_caller, _cmon} = start!(ctx)
      assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
      wmon = Process.monitor(worker_pid)
      assert_receive {:held, :registered, server, _ref, _info}, 10_000
      Process.exit(server, :kill)
      assert_receive {:result, {:error, %{clause: "run_server_down"}}}, 30_000
      assert_receive {:DOWN, ^wmon, :process, _, _}, 5_000
      refute_received {:gate_entered, _}
      refute_received {:run_effect_requested, _, _}
    end

    test "R-11c Owner parked at :handoff_received: no effect until the reaper acknowledged; release runs it",
         %{dir: dir} do
      require_worker!()
      ctx = held(dir, barrier: facts_barrier(collector(), :handoff_received))
      {_caller, _cmon} = start!(ctx)
      assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
      assert_receive {:held, :handoff_received, owner, ref}, 10_000
      refute_receive {:gate_entered, _}, 300, "nothing executes before the reaper acknowledged the worker"
      refute_received {:run_effect_requested, _, _}
      assert Process.alive?(worker_pid)
      send(owner, {:release, ref})
      facts = facts!()
      assert facts[:worker] == worker_pid
      assert_receive {:gate_entered, ^worker_pid}, 10_000
      send(worker_pid, :release_gate)
      assert_receive {:result, {:ok, %{summary: %{"status" => "completed"}}}}, 30_000
    end

    test "R-11d Owner dies at :handoff_received: the linked subtree (worker included) is gone; closed exit",
         %{dir: dir} do
      require_worker!()
      ctx = held(dir, barrier: facts_barrier(collector(), :handoff_received))
      {_caller, cmon} = start!(ctx)
      assert_receive {:run_child_started, _, :worker, worker_pid}, 10_000
      assert_receive {:run_child_started, root, :server, _server}, 10_000
      assert_receive {:held, :handoff_received, owner, _ref}, 10_000
      wmon = Process.monitor(worker_pid)
      rmon = Process.monitor(root)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^rmon, :process, _, _}, 5_000
      assert_receive {:DOWN, ^wmon, :process, _, _}, 5_000
      assert_receive {:result, {:error, %{clause: clause}}}, 30_000
      assert clause in ["run_executor_down", "run_server_down"]
      assert_receive {:DOWN, ^cmon, :process, _, :normal}, 5_000
      refute_received {:gate_entered, _}
    end
  end
end
