defmodule AiOrchestrator.Test.OwnedHarnessControlTest do
  @moduledoc """
  Controls for the tracked-ownership harness itself (EO-M10): registration precedes any caller work, an early failure
  still reaps everything learned so far, OS oracles run after the BEAM reaper and before directory removal.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Test.OwnedHarness

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    :ok
  end

  test "control: a caller is owned BEFORE its work starts (the permit follows registration)" do
    test_pid = self()

    {pid, _mon} =
      OwnedHarness.spawn_caller!(fn ->
        send(test_pid, {:owned_at_start, pid_owned?(test_pid, self())})
        :done
      end)

    assert_receive {:owned_at_start, true}, 1_000
    assert pid in OwnedHarness.owned()
    assert_receive {:result, :done}, 1_000
  end

  test "control: a forced early failure window - the caller dies at once, yet it was registered" do
    {pid, mon} = OwnedHarness.spawn_caller!(fn -> exit(:early) end)
    assert_receive {:DOWN, ^mon, :process, ^pid, :early}, 1_000
    assert pid in OwnedHarness.owned()
  end

  test "control: teardown order - reap every owner, then the OS oracle, then the directories" do
    {:ok, order} = Agent.start(fn -> [] end)
    dir = Path.join(System.tmp_dir!(), "owned-harness-ctl-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    OwnedHarness.track_dir!(dir)
    sleeper = spawn(fn -> Process.sleep(:infinity) end)
    OwnedHarness.track!(sleeper)

    OwnedHarness.os_oracle!(fn ->
      Agent.update(order, &[{:oracle, Process.alive?(sleeper), File.exists?(dir)} | &1])
      true
    end)

    OwnedHarness.close!()
    refute Process.alive?(sleeper)
    refute File.exists?(dir)
    assert Agent.get(order, & &1) == [{:oracle, false, true}], "oracle ran after the reaper and before rm_rf"
    Agent.stop(order)
  end

  test "control: an OS oracle that never settles is a teardown failure, never success" do
    OwnedHarness.os_oracle!(fn -> false end)
    assert_raise RuntimeError, ~r/still present/, fn -> OwnedHarness.close!() end
  end

  defp pid_owned?(test_pid, pid), do: pid in (OwnedHarness.Seam.get({:tracked, test_pid}) || [])
end
