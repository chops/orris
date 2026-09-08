defmodule AiOrchestrator.Run.GateOwnershipPrototypeTest do
  @moduledoc """
  Prototype rows for the Server-side timer authority double (docs/contracts/gate-ownership.org, GP rows; GO-M1/M2).
  TEST SUPPORT ONLY: proves the authority's arming/validation/classification/lifecycle grammar, not the product.
  The clock is a StepClock the test sets explicitly: no wall/real-timer conflation, no sleep-only expiry.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Test.GateDeadlineAuthority, as: Authority
  alias AiOrchestrator.Test.StepClock

  @unix 1_700_000_000

  defp op, do: %{cap: make_ref(), gen: 1, ref: make_ref()}

  setup do
    StepClock.set(@unix, 0)
    on_exit(fn -> StepClock.clear() end)
    pid = start_supervised!({Authority, clock: StepClock, cap_ms: 1_000})
    {:ok, authority: pid}
  end

  test "GP-2 positive future due: armed 60 s ahead, an early wake is :early and fires nothing; a wake at the due time fires exactly once",
       %{authority: a} do
    id = op()
    assert {:armed, 60_000} = Authority.arm(a, id, @unix + 60, self())
    send(a, {:fire, id})
    assert Authority.last_wake(a, id) == :early
    assert Authority.classify(a, id) == :armed
    refute_receive {:gate_deadline, ^id}, 50

    StepClock.set_mono(59_990)
    send(a, {:fire, id})
    assert Authority.last_wake(a, id) == :early
    refute_receive {:gate_deadline, ^id}, 50

    StepClock.set_mono(60_000)
    send(a, {:fire, id})
    assert_receive {:gate_deadline, ^id}, 1_000
    assert Authority.classify(a, id) == :fired
    send(a, {:fire, id})
    refute_receive {:gate_deadline, ^id}, 50
    assert Authority.last_wake(a, id) == :stale
    assert Authority.complete(a, id) == {:stale, :fired}
  end

  test "GP-2b a completion before the due time cancels; a later wake for it is stale, nothing fires", %{authority: a} do
    id = op()
    assert {:armed, 1_000} = Authority.arm(a, id, @unix + 1, self())
    assert Authority.complete(a, id) == :cancelled
    StepClock.set_mono(5_000)
    send(a, {:fire, id})
    refute_receive {:gate_deadline, ^id}, 50
    assert Authority.classify(a, id) == :cancelled
    assert Authority.last_wake(a, id) == :stale
  end

  test "GP-2c timer chunks are bounded by cap_ms (measured remaining <= 1000 for a 3 s due); the due is the fence's, not the chunk's",
       %{authority: a} do
    id = op()
    assert {:armed, 3_000} = Authority.arm(a, id, @unix + 3, self())
    remaining = Authority.timer_remaining_ms(a, id)
    assert is_integer(remaining) and remaining <= 1_000, "chunk bounded by cap_ms, not the full 3000 ms due"
    # an early chunk wake re-schedules another bounded chunk; the step clock has not moved, so it is early
    send(a, {:fire, id})
    assert Authority.last_wake(a, id) == :early
    assert Authority.classify(a, id) == :armed
    again = Authority.timer_remaining_ms(a, id)
    assert is_integer(again) and again <= 1_000
    assert Authority.complete(a, id) == :cancelled
    assert Authority.timer_remaining_ms(a, id) == nil
  end

  test "GP-3 duplicate arm refused, foreign identity classified, nothing fires for an unknown", %{authority: a} do
    id = op()
    assert {:armed, _} = Authority.arm(a, id, @unix + 60, self())
    assert Authority.arm(a, id, @unix + 60, self()) == {:refused, :duplicate}
    other = op()
    assert Authority.classify(a, other) == :foreign
    assert Authority.complete(a, other) == {:stale, :foreign}
    send(a, {:fire, other})
    refute_receive {:gate_deadline, ^other}, 50
    assert Authority.last_wake(a, other) == nil
    assert Authority.complete(a, id) == :cancelled
  end

  test "GP-3b already due at arm: the fence says :due, one message, no actuation beyond it", %{authority: a} do
    id = op()
    assert {:armed, 0} = Authority.arm(a, id, @unix - 5, self())
    assert_receive {:gate_deadline, ^id}, 1_000
    assert Authority.classify(a, id) == :fired
    assert Process.alive?(a)
  end

  test "GP-L lifecycle (GO-M2): the authority is supervised by the test; stopping it is witnessed and leaves no timer",
       %{authority: a} do
    id = op()
    assert {:armed, _} = Authority.arm(a, id, @unix + 60, self())
    mon = Process.monitor(a)
    :ok = stop_supervised!(Authority)
    assert_receive {:DOWN, ^mon, :process, ^a, :shutdown}, 1_000
    refute Process.alive?(a)
    refute_receive {:gate_deadline, ^id}, 50
  end
end
