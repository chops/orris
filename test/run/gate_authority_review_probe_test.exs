defmodule AiOrchestrator.Run.GateAuthorityReviewProbeTest do
  @moduledoc """
  Codex review probes for 800853d (m_1788803100000, GO-M1/GO-M2). Row 1 imported verbatim (module name, alias).
  Row 2 is ADAPTED as the review instructed: the original reproduced the old unsupervised setup (an ordinary
  start_link survives its starter's NORMAL exit); the corrected harness starts the authority supervised and the
  witness is its supervised stop.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Test.GateDeadlineAuthority, as: Authority
  alias AiOrchestrator.Test.StepClock

  setup do
    StepClock.set(1_700_000_000, 0)
    on_exit(fn -> StepClock.clear() end)
    :ok
  end

  test "an early wake does not fire a future deadline" do
    a = start_supervised!({Authority, clock: StepClock})
    op = %{cap: make_ref(), gen: 1, ref: make_ref()}
    assert {:armed, 60_000} = Authority.arm(a, op, StepClock.unix_now() + 60, self())
    send(a, {:fire, op})
    assert Authority.classify(a, op) == :armed
    refute_receive {:gate_deadline, ^op}, 0
  end

  test "corrected harness: a supervised authority does not outlive the test's supervisor (adapted row 2)" do
    a = start_supervised!({Authority, clock: StepClock})
    ref = Process.monitor(a)
    :ok = stop_supervised!(Authority)
    assert_receive {:DOWN, ^ref, :process, ^a, _}, 300
    refute Process.alive?(a)
  end
end
