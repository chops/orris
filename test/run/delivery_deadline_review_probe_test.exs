defmodule AiOrchestrator.Run.DeliveryDeadlineReviewProbeTest do
  @moduledoc """
  Codex on-head review probes for 9a2bb18 (m_1788799760000, DGM-1/DGM-2), imported verbatim modulo module name and
  formatting, plus the two converse companions the review asked for.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.FixedClock

  defmodule Adapter do
    @moduledoc false
    def deliver(_, opts) do
      send(Keyword.fetch!(opts, :collector), :unexpected_deliver)
      {:error, %{"reason" => "unexpected"}}
    end

    # companion B (Claude): the Observe adapter is the same module; it blocks until released
    def observe(_, opts) do
      send(Keyword.fetch!(opts, :collector), {:observing, self()})

      receive do
        :release -> {:ok, %{"artifact_ref" => "art_1"}}
      end
    end

    def reconcile(_, opts) do
      send(Keyword.fetch!(opts, :collector), {:reconciling, self()})

      receive do
        :release -> {:ok, %{"outcome" => "absent", "delivery_attempt" => 0}}
      end
    end
  end

  defp worker(extra \\ []) do
    pid = start_supervised!({Worker, self()})
    cap = make_ref()

    seams = [
      clock: FixedClock,
      dispatch: Adapter,
      dispatch_opts: [collector: self()],
      observe_fence_observer: self()
    ]

    send(pid, {:admit, cap, 1, Keyword.merge(seams, extra)})
    assert_receive {:admitted, ^cap, 1, ^pid}, 1_000
    {pid, cap}
  end

  defp dispatch do
    %Effect.Dispatch{assignment_id: "a", command: %{}, message_id: "m", deadline_unix: 0}
  end

  defp execute(pid, cap, intent) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, intent, nil})
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, observation}, 1_000
    {%{cap: cap, gen: 1, ref: ref}, observation}
  end

  for {effect, observation} <- [
        {Effect.Dispatch, Observation.DispatchFailed},
        {Effect.ReconcileSend, Observation.SendReconcileFailed}
      ] do
    test "default settings fence #{inspect(effect)} with no diagnostic opt-in" do
      {pid, cap} = worker()
      attrs = [assignment_id: "a", command: %{}, deadline_unix: 0]
      attrs = if unquote(effect) == Effect.Dispatch, do: Keyword.put(attrs, :message_id, "m"), else: attrs
      effect = struct!(unquote(effect), attrs)
      {_op, result} = execute(pid, cap, effect)
      assert result.__struct__ == unquote(observation)
      refute_receive :unexpected_deliver, 0
      refute_receive {:reconciling, _}, 0
    end
  end

  test "Observe-only default excludes retired Dispatch wakes as well as its arm facts" do
    {pid, cap} = worker()
    {op, _} = execute(pid, cap, dispatch())
    send(pid, {:observe_fence_wake, op})
    settle = make_ref()
    send(pid, {:settle, cap, 1, settle})
    assert_receive {:settled, ^cap, 1, ^settle, ^pid, []}, 1_000
    refute_receive {:observe_fence, ^pid, _}, 0
  end

  test "retired Dispatch wake after Observe retains Dispatch kind in the idle owner" do
    {pid, cap} = worker(observe_fence_kinds: [Effect.Dispatch, Effect.Observe])
    {op, _} = execute(pid, cap, dispatch())
    execute(pid, cap, %Effect.Observe{assignment_id: "a", command: %{}, deadline_unix: 0})
    send(pid, {:observe_fence_wake, op})
    assert_receive {:observe_fence, ^pid, %{op: ^op, fact: {:stale, :retired}} = fact}, 1_000
    assert fact.kind == Effect.Dispatch
  end

  test "retired Dispatch wake during Reconcile retains Dispatch kind" do
    {pid, cap} = worker(observe_fence_kinds: [Effect.Dispatch, Effect.ReconcileSend])
    {op, _} = execute(pid, cap, dispatch())
    ref = make_ref()
    intent = %Effect.ReconcileSend{assignment_id: "a", command: %{}, deadline_unix: FixedClock.base_unix() + 60}
    send(pid, {:execute, cap, 1, ref, intent, nil})
    assert_receive {:reconciling, task}, 1_000
    send(pid, {:observe_fence_wake, op})
    assert_receive {:observe_fence, ^pid, %{op: ^op, fact: {:stale, :retired}} = fact}, 1_000
    send(task, :release)
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.SendReconciled{}}, 1_000
    assert fact.kind == Effect.Dispatch
  end

  # companions (m_1788799760000 DGM-2): the selected-kind policy follows the fact's OWN operation
  test "companion A: allowed retired Dispatch wake during an EXCLUDED live ReconcileSend is reported as Dispatch; no ReconcileSend fact leaks" do
    {pid, cap} = worker(observe_fence_kinds: [Effect.Dispatch])
    {op, _} = execute(pid, cap, dispatch())
    assert_receive {:observe_fence, ^pid, %{op: ^op, kind: Effect.Dispatch, fact: :arming}}, 1_000
    ref = make_ref()
    intent = %Effect.ReconcileSend{assignment_id: "a", command: %{}, deadline_unix: FixedClock.base_unix() + 60}
    send(pid, {:execute, cap, 1, ref, intent, nil})
    assert_receive {:reconciling, task}, 1_000
    refute_received {:observe_fence, ^pid, %{kind: Effect.ReconcileSend}}
    send(pid, {:observe_fence_wake, op})
    assert_receive {:observe_fence, ^pid, %{op: ^op, fact: {:stale, :retired}, kind: Effect.Dispatch}}, 1_000
    send(task, :release)
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.SendReconciled{}}, 1_000
    refute_receive {:observe_fence, ^pid, %{kind: Effect.ReconcileSend}}, 0
  end

  test "companion B: EXCLUDED retired Dispatch wake during an allowed live Observe does not leak; the Observe's own facts still flow" do
    {pid, cap} = worker(observe_fence_kinds: [Effect.Observe])
    {op, _} = execute(pid, cap, dispatch())
    refute_received {:observe_fence, ^pid, %{op: ^op}}
    ref = make_ref()
    intent = %Effect.Observe{assignment_id: "a", command: %{}, deadline_unix: FixedClock.base_unix() + 60}
    send(pid, {:execute, cap, 1, ref, intent, nil})
    assert_receive {:observing, task}, 1_000
    assert_receive {:observe_fence, ^pid, %{fact: {:armed, _}, kind: Effect.Observe} = live}, 1_000
    send(pid, {:observe_fence_wake, op})
    send(task, :release)
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.ArtifactObserved{}}, 1_000
    refute_receive {:observe_fence, ^pid, %{op: ^op}}, 0
    refute_receive {:observe_fence, ^pid, %{kind: Effect.Dispatch}}, 0
    assert live.op.ref == ref
  end
end
