defmodule AiOrchestrator.Run.DeliveryDeadlineRedTest do
  @moduledoc """
  RED/interface for the delivery-deadline lane (docs/contracts/delivery-deadline.org, revision 4). Released rows:
  mechanics (D-1, D-2, DA, D-7, DO, DL; m_1788758714013) and durable expiry (D-3..D-6; m_1788759787549) on the
  corroborated attention-only interface: the existing DispatchFailed / SendReconcileFailed observation classes with
  the stable reasons dispatch_deadline_exceeded / dispatch_reconcile_timeout, and a narrow pure-reducer branch that
  emits ONLY human_attention_required through the existing builder. Every row fails today because no
  Dispatch/ReconcileSend runner seam, fence, admission or reducer branch exists; the deadline rides on the intent as
  an EXTRA map key (`Map.put/3`) until the struct field exists (compile-time struct keys cannot be written in RED).
  Controls (green today): DA-control (direct no-runner legacy path).
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_delivery_deadline_red"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @canary "DELIVERY-DEADLINE-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @expired_reason %{"reason" => "dispatch_deadline_exceeded", "detector" => "dispatch_deadline"}
  @timeout_reason %{"reason" => "dispatch_reconcile_timeout", "detector" => "dispatch_reconcile"}

  # a NON-cooperative deliver: reports entry with the command's identity, blocks until released
  defmodule BlockingDeliver do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane

    # DD-M15: the reconcile method is WITNESSED before delegating, so "no reconcile entry" is an observed fact
    def reconcile(command, opts) do
      send(Keyword.fetch!(opts, :collector), {:reconcile_entered, self(), command["send_message_id"]})
      LocalPane.reconcile(command, Keyword.drop(opts, [:collector, :canary]))
    end

    def deliver(command, opts) do
      send(
        Keyword.fetch!(opts, :collector),
        {:deliver_entered, self(), command["send_message_id"], command["payload_hash"]}
      )

      receive do
        :release_ok ->
          {:ok,
           %{
             "assignment_id" => command["assignment_id"],
             "backend" => "local_pane",
             "pane_ref" => command["pane_ref"],
             "send_status" => "ok",
             "send_message_id" => command["send_message_id"],
             "prompt_hash" => command["payload_hash"],
             "replayed" => false
           }}

        :release_error ->
          {:error, %{"reason" => "adapter_error_for_parity"}}
      end
    end
  end

  # a real queued deliver (LocalPane base, receipt says queued) then a NON-cooperative reconcile
  defmodule QueuedThenBlockingReconcile do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane

    # DD-M15: the deliver method is WITNESSED before delegating
    def deliver(command, opts) do
      send(
        Keyword.fetch!(opts, :collector),
        {:deliver_entered, self(), command["send_message_id"], command["payload_hash"]}
      )

      {:ok, base} = LocalPane.deliver(command, Keyword.drop(opts, [:collector, :canary]))
      {:ok, Map.put(base, "send_status", "queued")}
    end

    def reconcile(command, opts) do
      send(Keyword.fetch!(opts, :collector), {:reconcile_entered, self(), command["send_message_id"]})

      receive do
        :never -> :ok
      end
    end
  end

  # a real queued deliver then a scripted reconcile (proven-absent retry witness)
  defmodule QueuedThenScriptedReconcile do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane

    def deliver(command, opts) do
      {:ok, base} = LocalPane.deliver(command, Keyword.drop(opts, [:reconcile_result, :collector, :canary]))
      {:ok, Map.put(base, "send_status", "queued")}
    end

    def reconcile(_command, opts), do: Keyword.fetch!(opts, :reconcile_result)
  end

  # counts every adapter call (zero-entry proofs) while delegating to LocalPane
  defmodule CountingDispatch do
    @moduledoc false
    for name <- [:deliver, :snapshot, :observe, :reconcile] do
      def unquote(name)(command, opts) do
        send(Keyword.fetch!(opts, :collector), {:dispatch_called, unquote(name), command["assignment_id"], self()})
        apply(LocalPane, unquote(name), [command, Keyword.delete(opts, :collector)])
      end
    end
  end

  defmodule PortGate do
    @moduledoc false
    alias AiOrchestrator.Test.GateDouble

    def prepare(fs, request, opts) do
      {:ok, handle} = GateDouble.prepare(fs, request, opts)
      port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary, :stream, :exit_status])
      memo = make_ref()
      Process.put({__MODULE__, memo}, :live)
      {:ok, Map.merge(handle, %{owner: self(), port: port, memo: memo, witness: Keyword.fetch!(opts, :witness)})}
    end

    def started_data(handle) do
      witness!(handle, :prepared)
      GateDouble.started_data(handle)
    end

    def abandon(handle) do
      witness!(handle, :settling)
      true = Port.close(handle.port)
      nil = Port.info(handle.port)
      :live = Process.delete({__MODULE__, handle.memo})
      send(handle.witness, {:gate_closed, self(), handle.port, handle.memo})
      :ok
    end

    def witness!(handle, stage) do
      if !(handle.owner == self() and Process.get({__MODULE__, handle.memo}) == :live and
             Port.info(handle.port, :connected) == {:connected, self()}) do
        raise "gate owner or memo changed"
      end

      port = handle.port
      bytes = Atom.to_string(stage) <> "\n"
      true = Port.command(port, bytes)

      receive do
        {^port, {:data, ^bytes}} -> :ok
      after
        2_000 -> raise "owner Port echo did not arrive"
      end

      send(handle.witness, {:gate_owned, stage, self(), port, handle.memo})
      :ok
    end
  end

  # a NON-Observe adapter that raises the public carrier with whatever diagnostic the test supplies (OG-M1)
  defmodule ForgedSnapshot do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate deliver(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def snapshot(_command, opts),
      do: raise(AiOrchestrator.Effects.AdapterFailure, diagnostic: Keyword.fetch!(opts, :diagnostic))
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "delivery-deadline-red-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  # ---- standalone Worker harness ----
  defp scenario_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp standalone_seams(extra \\ []) do
    base = scenario_opts()
    test = self()

    base
    |> Keyword.drop(@owned)
    |> Keyword.put(:dispatch, BlockingDeliver)
    |> Keyword.put(:dispatch_opts, Keyword.merge(Keyword.get(base, :dispatch_opts, []), collector: test, canary: @canary))
    |> Keyword.merge(
      supervisor_instance: @instance,
      run_id: "run_fixture_0001",
      observe_fence_observer: test,
      observe_fence_kinds: [Effect.Dispatch, Effect.ReconcileSend, Effect.Observe],
      clock: FixedClock
    )
    |> Keyword.merge(extra)
  end

  defp port_gate_seams(extra), do: standalone_seams([gate_executor: PortGate, gate_opts: [witness: self()]] ++ extra)

  defp standalone_worker!(seams) do
    {:ok, worker} = Run.Worker.start_link(self())
    track!(worker)
    cap = make_ref()
    send(worker, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^worker}, 5_000
    {worker, cap}
  end

  # the command carries a REAL prompt identity: payload_hash is the prompt's SensitiveBytes.hash
  defp command(id) do
    prompt = SensitiveBytes.new("hello " <> id, :prompt)

    %{
      "assignment_id" => id,
      "pane_ref" => "pane_writer",
      "send_message_id" => "snd_" <> Base.encode16(:crypto.hash(:sha256, id), case: :lower),
      "payload_hash" => SensitiveBytes.hash(prompt),
      "prompt" => prompt,
      "repo_root" => "/tmp/example-repo",
      "expected_artifact" => "out.txt"
    }
  end

  # the deadline rides on the intent as an extra key until the struct field exists (see @moduledoc); :missing = no key
  defp with_deadline(intent, :missing), do: intent
  defp with_deadline(intent, deadline), do: Map.put(intent, :deadline_unix, deadline)

  defp dispatch(deadline, id \\ "as_0001") do
    c = command(id)
    with_deadline(%Effect.Dispatch{assignment_id: id, command: c, message_id: c["send_message_id"]}, deadline)
  end

  defp reconcile_send(deadline, id \\ "as_0001"),
    do: with_deadline(%Effect.ReconcileSend{assignment_id: id, command: command(id)}, deadline)

  defp prepare_gate(dir) do
    %Effect.PrepareGate{
      gate_run_id: "gr_0001",
      attempt: 1,
      requested: %{"command_argv" => ["true"], "timeout_s" => 600},
      deadline_unix: FixedClock.unix_now() + 600,
      repo_root: dir,
      run_dir: dir
    }
  end

  defp closed_missing?(closed) do
    closed.kind == :error and closed.class == "atom" and
      closed.digest == Diagnostic.describe(:dispatch_deadline_missing)["digest"]
  end

  # ---- integrated run harness (real reducer -> Server -> Worker), coherent REAL clock ----
  defp config(dir, dispatch, extra_dispatch_opts, extra_opts, mode \\ :run) do
    base = scenario_opts()

    dispatch_opts =
      base
      |> Keyword.get(:dispatch_opts, [])
      |> Keyword.put(:collector, collector())
      |> Keyword.merge(extra_dispatch_opts)

    opts =
      base
      |> Keyword.drop(@owned)
      |> Keyword.put(:supervisor_instance, @instance)
      |> Keyword.put(:dispatch, dispatch)
      |> Keyword.put(:dispatch_opts, dispatch_opts)
      |> Keyword.put(:clock, SystemClock)
      |> Keyword.put(:observe_fence_observer, self())
      |> Keyword.put(:observe_fence_kinds, [Effect.Dispatch, Effect.ReconcileSend, Effect.Observe])
      |> Keyword.merge(extra_opts)

    opts = if mode == :resume, do: Keyword.put(opts, :recovery_reason, "crash_recovery"), else: opts

    %{
      run_dir: dir,
      mode: mode,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: opts,
      trace: collector()
    }
  end

  defp start!(config) do
    assert {:ok, root} = Run.Supervisor.start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, server: server, work: work}
  end

  defp stop!(root) do
    if Process.alive?(root), do: Supervisor.stop(root, :shutdown, 10_000)
    :ok
  end

  defp journal(dir),
    do: dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp types(events), do: Enum.map(events, & &1["type"])
  defp durable_deadline!(events), do: Enum.find(events, &(&1["type"] == "assignment_requested"))["data"]["deadline_unix"]

  defp wait_for(fun, timeout_ms), do: wait(fun, System.monotonic_time(:millisecond) + timeout_ms)

  defp wait(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(20) && wait(fun, deadline)
    end
  end

  defp kill9!(dir, root) do
    Process.exit(root, :kill)
    assert wait_for(fn -> not Process.alive?(root) end, 5_000)
    assert wait_for(fn -> Ownership.status(dir) == :none end, 5_000)
  end

  defp dispatch_calls, do: for({:dispatch_called, name, id, _} <- mailbox(), do: {name, id})
  defp mailbox, do: elem(Process.info(self(), :messages), 1)

  # the exact attention-only payload (existing builder, no wedge, no detail)
  defp attention_only(reason, attention_id \\ "att_0001", assignment_id \\ "as_0001") do
    %{
      "attention_id" => attention_id,
      "blocking_entity" => assignment_id,
      "reason" => reason,
      "resume_command" => "run --resume RUN_DIR",
      "summary_hash" => "sha256:" <> String.duplicate("0", 64),
      "summary_path" => "attention/#{attention_id}.org"
    }
  end

  defp assert_attention_only!(dir, reason) do
    events = journal(dir)
    refute Enum.any?(events, &(&1["type"] == "agent_wedge_detected")), "no synthetic wedge for a deadline"
    [last] = Enum.take(events, -1)
    assert {last["type"], last["data"]} == {"human_attention_required", attention_only(reason)}
    events
  end

  # ---- Host (real reducer, direct effects) witnesses: capture the intents the reducer actually emits ----
  defp host_opts(extra) do
    base = scenario_opts()
    test = self()
    observer = fn effect, _observation -> send(test, {:intent, effect}) end
    dispatch_opts = Keyword.merge(Keyword.get(base, :dispatch_opts, []), Keyword.get(extra, :dispatch_opts, []))

    base
    |> Keyword.put(:effect_observer, observer)
    |> Keyword.merge(Keyword.delete(extra, :dispatch_opts))
    |> Keyword.put(:dispatch_opts, dispatch_opts)
  end

  defp drain_intents do
    receive do
      {:intent, _} -> drain_intents()
    after
      0 -> :ok
    end
  end

  defp intents(module), do: for({:intent, %{__struct__: ^module} = i} <- mailbox(), do: i)

  describe "RED: propagation seam and arming (D-1, D-2)" do
    test "D-1 direct Effects: the runner is invoked for Dispatch and ReconcileSend with the deadline metadata; never for a snapshot" do
      test = self()

      runner = fn closure, %{deadline_unix: d} ->
        send(test, {:runner_called, d})
        {:ok, closure.()}
      end

      opts = [dispatch: H.OkDispatch, dispatch_opts: [], adapter_runner: runner]
      {observation, _} = Effects.execute(dispatch(FixedClock.unix_now() + 60), Runtime.new([]), opts: opts)
      assert %Observation.Dispatched{} = observation
      assert_receive {:runner_called, d1}, 1_000
      assert d1 == FixedClock.unix_now() + 60
      # the reconcile leg needs a dispatch with reconcile/2 (OkDispatch has none): the witnessing double + fake pane
      ropts = [
        dispatch: BlockingDeliver,
        dispatch_opts: [collector: test, canary: @canary, pane_client: H.FakePaneClient],
        adapter_runner: runner
      ]

      {%Observation.SendReconciled{}, _} =
        Effects.execute(reconcile_send(FixedClock.unix_now() + 60), Runtime.new([]), opts: ropts)

      assert_receive {:runner_called, _}, 1_000
      assert_receive {:reconcile_entered, _, _}, 1_000
      snapshot = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}
      Effects.execute(snapshot, Runtime.new([]), opts: opts)
      refute_receive {:runner_called, _}, 200
    end

    test "D-2 the Worker arms ONCE at the Dispatch dequeue from the propagated deadline; the adapter runs in a task" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      deadline = FixedClock.unix_now() + 600
      send(worker, {:execute, cap, 1, ref, dispatch(deadline), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{cap: ^cap, gen: 1, ref: ^ref}, fact: :arming}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref}, fact: {:armed, %{deadline_unix: ^deadline}}}}, 5_000
      refute_receive {:observe_fence, ^worker, %{fact: {:armed, _}}}, 200, "armed exactly once"
      assert_receive {:deliver_entered, task, _, _}, 5_000
      refute task == worker
      send(task, :release_ok)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.Dispatched{}}, 5_000
    end
  end

  describe "RED: production propagation witnesses through the real reducer (DP-1..DP-4, DD-M10)" do
    test "DP-1 fresh dispatch: the emitted Dispatch intent carries EXACTLY the durable assignment_requested deadline" do
      {:ok, %{events: events}} = Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), host_opts([]))
      [first | _] = intents(Effect.Dispatch)
      assert first.assignment_id == "as_0001"
      assert Map.fetch(first, :deadline_unix) == {:ok, durable_deadline!(events)}
    end

    test "DP-2 fresh-no-receipt resume: the re-emitted Dispatch intent carries the prior journal's durable deadline" do
      # ONE opts for run and resume: the retained prompt must be reachable (same prompt_root), see DC-7b
      opts = host_opts([])
      {:ok, %{events: events}} = Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
      # the loss-window prefix: everything up to the first prompt projection, nothing dispatched yet
      prior = events |> Enum.take_while(&(&1["type"] != "assignment_dispatch_sent")) |> Enum.map(&Jason.encode!/1)
      assert List.last(prior) =~ "assignment_prompt_projected"
      drain_intents()

      {:ok, _} =
        Host.resume(
          H.spec("gated_run_seed"),
          H.plan("gated_run_seed"),
          prior,
          Keyword.put(opts, :recovery_reason, "crash_recovery")
        )

      [resumed | _] = intents(Effect.Dispatch)
      assert resumed.assignment_id == "as_0001"
      assert Map.fetch(resumed, :deadline_unix) == {:ok, durable_deadline!(Enum.map(prior, &Jason.decode!/1))}
    end

    test "DP-3/DP-4 queued receipt then proven absent: the ReconcileSend intent and the retry Dispatch intent both carry the durable deadline" do
      opts =
        host_opts(
          dispatch: QueuedThenScriptedReconcile,
          dispatch_opts: [reconcile_result: {:ok, %{"outcome" => "absent", "delivery_attempt" => 1}}]
        )

      {:ok, %{events: events}} = Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
      d = durable_deadline!(events)
      [reconcile | _] = intents(Effect.ReconcileSend)
      assert Map.fetch(reconcile, :deadline_unix) == {:ok, d}
      dispatches = intents(Effect.Dispatch)
      assert length(dispatches) >= 2, "the proven-absent retry re-dispatched"
      [_first, retry | _] = dispatches
      assert Map.fetch(retry, :deadline_unix) == {:ok, d}
    end
  end

  describe "RED: deadline admission matrix for BOTH effects (DA, DD-M10)" do
    for {effect, builder} <- [{"Dispatch", :dispatch}, {"ReconcileSend", :reconcile_send}],
        {label, value} <- [
          {"missing", :missing},
          {"nil", nil},
          {"a struct", %URI{}},
          {"a string", "123"},
          {"a float", 1.5},
          {"negative", -1}
        ] do
      test "DA #{effect} with #{label} deadline is refused closed: dispatch_deadline_missing, no adapter entry" do
        {worker, cap} = standalone_worker!(standalone_seams(dispatch: BlockingDeliver))
        ref = make_ref()
        value = unquote(Macro.escape(value))
        intent = if unquote(builder) == :dispatch, do: dispatch(value), else: reconcile_send(value)
        send(worker, {:execute, cap, 1, ref, intent, nil})
        assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, closed}, 5_000
        assert closed_missing?(closed)
        refute_receive {:deliver_entered, _, _, _}, 100
        refute_receive {:reconcile_entered, _, _}, 100
        refute_receive {:observe_fence, ^worker, %{fact: {:task_allocated, _}}}, 100
        assert Process.alive?(worker)
      end
    end

    test "DA valid future deadline is admitted and armed" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, dispatch(FixedClock.unix_now() + 600), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref}, fact: {:armed, _}}}, 5_000
      assert_receive {:deliver_entered, task, _, _}, 5_000
      send(task, :release_ok)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.Dispatched{}}, 5_000
    end

    test "DA valid 0 is a due deadline: admitted, armed, answered as expired with zero adapter entry (D-3 shape)" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, dispatch(0), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref}, fact: {:armed, %{deadline_unix: 0}}}}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.DispatchFailed{} = expired}, 5_000
      assert expired.reason == @expired_reason
      refute_receive {:deliver_entered, _, _, _}, 100
    end

    test "DA-control the direct owner-local Effects path WITHOUT a runner stays unfenced (legacy direct callers)" do
      test = self()
      opts = [dispatch: BlockingDeliver, dispatch_opts: [collector: test, canary: @canary]]
      intent = dispatch(:missing)
      caller = spawn_link(fn -> send(test, {:direct, Effects.execute(intent, Runtime.new([]), opts: opts)}) end)
      track!(caller)
      assert_receive {:deliver_entered, ^caller, _, _}, 5_000
      send(caller, :release_ok)
      assert_receive {:direct, {%Observation.Dispatched{}, _}}, 5_000
      # and the reconcile method is positively witnessed on the same direct path (DD-M15)
      opts2 = [
        dispatch: BlockingDeliver,
        dispatch_opts: [collector: test, canary: @canary, pane_client: H.FakePaneClient]
      ]

      {%Observation.SendReconciled{outcome: "absent"}, _} =
        Effects.execute(reconcile_send(:missing), Runtime.new([]), opts: opts2)

      assert_receive {:reconcile_entered, _, _}, 1_000
    end
  end

  describe "RED: durable expiry (D-3..D-6) on the corroborated attention-only interface" do
    test "D-3 already due at the Worker (both effects): the exact expiry observation, zero adapter entries, no task" do
      {worker, cap} = standalone_worker!(standalone_seams(dispatch: QueuedThenBlockingReconcile))
      due = FixedClock.unix_now() - 1
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, dispatch(due), nil})

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker,
                      %Observation.DispatchFailed{assignment_id: "as_0001", reason: @expired_reason}},
                     5_000

      ref2 = make_ref()
      send(worker, {:execute, cap, 1, ref2, reconcile_send(due), nil})

      assert_receive {:effect_result, ^cap, 1, ^ref2, ^worker,
                      %Observation.SendReconcileFailed{assignment_id: "as_0001", reason: @timeout_reason}},
                     5_000

      refute_receive {:deliver_entered, _, _, _}, 100
      refute_receive {:reconcile_entered, _, _}, 100
      refute_receive {:observe_fence, ^worker, %{fact: {:task_allocated, _}}}, 100
      # positive witness of the same doubles (DD-M15): a live deadline enters BOTH methods
      ref3 = make_ref()
      send(worker, {:execute, cap, 1, ref3, dispatch(FixedClock.unix_now() + 600), nil})
      assert_receive {:deliver_entered, task, _, _}, 5_000
      send(task, :release_ok)
      assert_receive {:effect_result, ^cap, 1, ^ref3, ^worker, %Observation.Dispatched{}}, 5_000
      ref4 = make_ref()
      send(worker, {:execute, cap, 1, ref4, reconcile_send(FixedClock.unix_now() + 600), nil})
      assert_receive {:reconcile_entered, _, _}, 5_000
    end

    test "D-3i integrated: a zero timeout makes the first dispatch due at dequeue: attention-only, blocked, zero adapter calls",
         %{dir: dir} do
      facts = start!(config(dir, CountingDispatch, [], default_assignment_timeout_s: 0))

      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} =
               Server.await(facts.server, 30_000)

      OwnedHarness.flush!()
      events = assert_attention_only!(dir, "dispatch_deadline_exceeded")
      assert Enum.at(events, -2)["type"] == "assignment_prompt_projected"
      # DD-M12: the ONE prior SnapshotArtifact (before the projection) is legitimate and pinned separately; then
      # ZERO deliver / reconcile / observe entries for the due assignment
      assert dispatch_calls() == [{:snapshot, "as_0001"}], "exactly the prior snapshot, nothing else"
      stop!(facts.root)
    end

    test "D-4 integrated: a deliver interrupted after GO (2 s, real clock): task killed and joined, attention-only committed, blocked",
         %{dir: dir} do
      facts = start!(config(dir, BlockingDeliver, [], default_assignment_timeout_s: 2))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      # future-at-arm and GO are WITNESSED, never assumed from a short delta on a slow scheduler
      assert_receive {:observe_fence, ^worker, %{op: op, fact: {:armed, %{deadline_unix: d}}}}, 10_000
      assert SystemClock.unix_now() < d, "future at arm"
      assert_receive {:observe_fence, ^worker, %{op: ^op, task: %{pid: task}, fact: {:task_started, task}}}, 10_000
      assert_receive {:deliver_entered, ^task, _, _}, 5_000
      mon = Process.monitor(task)
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 15_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, :killed}}}, 5_000
      assert_receive {:DOWN, ^mon, :process, ^task, _}, 5_000

      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} =
               Server.await(facts.server, 30_000)

      assert_attention_only!(dir, "dispatch_deadline_exceeded")
      assert Process.alive?(worker)
      stop!(facts.root)
    end

    test "D-4p retained Port survives an interrupted deliver and settles exactly once (existing owner lifetime)", %{
      dir: dir
    } do
      {worker, cap} = standalone_worker!(port_gate_seams(clock: SystemClock, observe_fence_cap_ms: 400))
      gate_ref = make_ref()
      send(worker, {:execute, cap, 1, gate_ref, prepare_gate(dir), nil})
      assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
      %{runtime: runtime} = :sys.get_state(worker, 2_000)
      handle = Runtime.handle(runtime, {"gr_0001", 1})
      ref = make_ref()
      d = SystemClock.unix_now() + 1
      send(worker, {:execute, cap, 1, ref, dispatch(d), nil})
      # future-or-now at arm and GO are witnessed (a slow scheduler never turns this into an already-due row)
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref}, fact: {:armed, %{deadline_unix: ^d}}}}, 5_000
      assert SystemClock.unix_now() <= d
      assert_receive {:deliver_entered, task, _, _}, 5_000

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.DispatchFailed{} = expired}, 15_000
      assert expired.reason == @expired_reason

      refute Process.alive?(task)
      %{runtime: retained} = :sys.get_state(worker, 2_000)
      assert Runtime.handle(retained, {"gr_0001", 1}) == handle and Port.info(port, :connected) == {:connected, worker}
      settle_ref = make_ref()
      send(worker, {:settle, cap, 1, settle_ref})
      assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000

      assert_receive {:settled, ^cap, 1, ^settle_ref, ^worker,
                      [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true}}]},
                     5_000
    end

    test "D-4r a reply queued after expiry selection is stale: the expiry answer stands (held race)" do
      {worker, cap} =
        standalone_worker!(
          standalone_seams(clock: SystemClock, observe_fence_hold: %{after_expiry: self()}, observe_fence_cap_ms: 400)
        )

      ref = make_ref()
      d = SystemClock.unix_now() + 1
      send(worker, {:execute, cap, 1, ref, dispatch(d), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref} = op, fact: {:armed, %{deadline_unix: ^d}}}}, 5_000
      assert SystemClock.unix_now() <= d, "future-or-now at arm"

      assert_receive {:observe_fence, ^worker,
                      %{op: ^op, task: %{ref: task_ref, pid: task}, fact: {:task_started, task}}},
                     5_000

      assert_receive {:deliver_entered, ^task, _, _}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 15_000

      assert_receive {:observe_fence_held, ^worker, token, %{op: ^op, task: %{ref: ^task_ref}, stage: :after_expiry}},
                     5_000

      send(task, :release_ok)

      assert wait_for(
               fn -> Enum.any?(elem(Process.info(worker, :messages), 1), &match?({^task_ref, {:ok, _}}, &1)) end,
               5_000
             )

      send(worker, {:observe_fence_proceed, token})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:shutdown_return, :ok_reply}}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:late_result, :stale}}}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.DispatchFailed{} = expired}, 5_000
      assert expired.reason == @expired_reason
    end

    test "D-5i integrated: a reconciliation query that never answers (2 s, real clock): attention-only dispatch_reconcile_timeout, blocked",
         %{dir: dir} do
      facts = start!(config(dir, QueuedThenBlockingReconcile, [], default_assignment_timeout_s: 2))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      assert_receive {:deliver_entered, _, _, _}, 30_000
      # the ReconcileSend is armed in the future and GO is witnessed before the reconcile blocks (the Dispatch's own
      # armed fact came earlier; the kind pins this one); the arm's OWN wall read is the oracle, not a later sample
      assert_receive {:observe_fence, ^worker,
                      %{kind: Effect.ReconcileSend, fact: {:armed, %{deadline_unix: d, unix_now: at_arm}}}},
                     10_000

      assert at_arm < d, "future at arm"
      assert_receive {:reconcile_entered, task, _}, 30_000
      mon = Process.monitor(task)
      assert_receive {:DOWN, ^mon, :process, ^task, _}, 15_000

      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} =
               Server.await(facts.server, 30_000)

      events = assert_attention_only!(dir, "dispatch_reconcile_timeout")

      assert "assignment_dispatch_sent" in types(events),
             "the queued receipt is journaled; the timeout is the reconcile's"

      stop!(facts.root)
    end

    test "D-6a committed timeout attention: the resume is refused through the existing API, zero pane calls, nothing appended",
         %{dir: dir} do
      facts = start!(config(dir, CountingDispatch, [], default_assignment_timeout_s: 0))
      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts.server, 30_000)
      stop!(facts.root)
      before = journal(dir)
      OwnedHarness.flush!()
      for {:dispatch_called, _, _, _} <- mailbox(), do: receive(do: ({:dispatch_called, _, _, _} -> :ok))

      refused =
        case Run.Supervisor.start_link(config(dir, CountingDispatch, [], [], :resume)) do
          {:error, refusal} ->
            refusal

          {:ok, root} ->
            track!(root)
            assert_receive {:run_child_started, ^root, :server, server}, 10_000
            {:error, refusal} = Server.await(server, 30_000)
            stop!(root)
            refusal
        end

      assert refused == %{"reason" => "attention_required", "open_attention_ids" => ["att_0001"]}
      assert dispatch_calls() == [] and journal(dir) == before
    end

    test "D-6c pre-attention crash with an EXPIRED retained deadline: the resume follows strict D1: zero adapter entry, attention-only",
         %{dir: dir} do
      cfg = config(dir, BlockingDeliver, [], default_assignment_timeout_s: 2)
      prompt_root = Keyword.fetch!(cfg.opts, :prompt_root)
      facts = start!(cfg)
      assert_receive {:deliver_entered, _task, mid, hash}, 30_000
      events = journal(dir)
      deadline = durable_deadline!(events)
      # bind the original assignment/key/deadline and prove the retained prompt is accessible BEFORE any expiry claim
      projected = Enum.find(events, &(&1["type"] == "assignment_prompt_projected"))
      assert projected["data"]["assignment_id"] == "as_0001" and projected["data"]["prompt_hash"] == hash
      assert String.starts_with?(mid, "snd_") and is_integer(deadline)
      assert File.exists?(Path.join(prompt_root, projected["data"]["prompt_path"])), "the retained prompt is reachable"
      kill9!(dir, facts.root)
      assert wait_for(fn -> SystemClock.unix_now() > deadline end, 10_000), "the retained deadline is now in the past"
      before = length(journal(dir))
      OwnedHarness.flush!()
      # the SAME prompt_root on resume (DD-M13; a fresh root fails on prompt_fetch_failed before any dispatch, DC-7b)
      facts2 =
        start!(config(dir, CountingDispatch, [], [default_assignment_timeout_s: 2, prompt_root: prompt_root], :resume))

      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} =
               Server.await(facts2.server, 30_000)

      OwnedHarness.flush!()
      assert dispatch_calls() == [], "strict D1: no adapter entry for the expired retained deadline"
      events = assert_attention_only!(dir, "dispatch_deadline_exceeded")
      assert length(events) == before + 8, "acceptance + lease repair + the attention, nothing else"
      stop!(facts2.root)
    end
  end

  describe "RED: identity, forced orders and task lifetime (D-7, DO-1, DO-2, DL-1)" do
    test "D-7 the task's adapter call carries the intent's message id and REAL payload hash; the reply keeps them" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      intent = dispatch(FixedClock.unix_now() + 600)
      send(worker, {:execute, cap, 1, ref, intent, nil})

      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref}, task: %{pid: task}, fact: {:task_started, task}}},
                     5_000

      mid = intent.message_id
      hash = SensitiveBytes.hash(intent.command["prompt"])
      assert hash == intent.command["payload_hash"]
      assert_receive {:deliver_entered, ^task, ^mid, ^hash}, 5_000
      send(task, :release_ok)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.Dispatched{result: result}}, 5_000
      assert result["send_message_id"] == mid and result["prompt_hash"] == hash
    end

    test "DO-1 reply before due (+600 s, forced): the reply wins, no kill, a later wake for that op is stale" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, dispatch(FixedClock.unix_now() + 600), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref} = op, fact: {:armed, _}}}, 5_000
      assert_receive {:deliver_entered, task, _, _}, 5_000
      send(task, :release_error)

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker,
                      %Observation.DispatchFailed{reason: %{"reason" => "adapter_error_for_parity"}}},
                     5_000

      refute_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 100
      send(worker, {:observe_fence_wake, op})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:stale, :retired}}}, 5_000
    end

    test "DO-2 wake before reply (+1 s on the COHERENT real clock): first wait, chunks, expiry selected, held, killed" do
      {worker, cap} =
        standalone_worker!(
          standalone_seams(clock: SystemClock, observe_fence_hold: %{after_expiry: self()}, observe_fence_cap_ms: 400)
        )

      ref = make_ref()
      deadline = SystemClock.unix_now() + 1
      send(worker, {:execute, cap, 1, ref, dispatch(deadline), nil})

      assert_receive {:observe_fence, ^worker,
                      %{op: %{ref: ^ref} = op, fact: {:armed, %{deadline_unix: ^deadline, due_ms: due}}}},
                     5_000

      assert_receive {:deliver_entered, task, _, _}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:early, %{due_ms: ^due, wait_ms: w1}}}}, 5_000
      assert w1 <= 400
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :due}}, 10_000
      assert SystemClock.unix_now() >= deadline, "due was selected at or after the deadline on the same clock"
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 5_000
      assert_receive {:observe_fence_held, ^worker, token, %{op: ^op, task: %{pid: ^task}, stage: :after_expiry}}, 5_000
      send(worker, {:observe_fence_proceed, token})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, task: %{pid: ^task}, fact: {:death, :killed}}}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.DispatchFailed{} = expired}, 5_000
      assert expired.reason == @expired_reason
      assert Process.alive?(worker)
    end

    test "DL-1 a hard Worker kill while the deliver task is blocked joins the task" do
      {worker, cap} = standalone_worker!(standalone_seams())
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, dispatch(FixedClock.unix_now() + 600), nil})
      assert_receive {:deliver_entered, task, _, _}, 5_000
      refute task == worker
      mon = Process.monitor(task)
      Process.exit(worker, :kill)
      assert_receive {:DOWN, ^mon, :process, ^task, _}, 5_000
    end
  end
end
