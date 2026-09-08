defmodule AiOrchestrator.Effects.ExtractionOwnershipReviewTest do
  # Imported from Codex's baseline probes (/tmp/effects-extraction-ownership-review_test.exs),
  # assertion-preserving for cases 1-5; case 6 is a disclosed baseline pin (see its comment).
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  defmodule Clock do
    @moduledoc false
    defdelegate wall_ts(), to: FixedClock

    def unix_now do
      if Process.delete(:review_clock_raise), do: raise("review clock")
      FixedClock.unix_now()
    end
  end

  defmodule Executor do
    @moduledoc false
    def prepare(fs, request, opts) do
      {:ok, handle} = GateDouble.prepare(fs, request, opts)
      remember(handle, :prepare)
      {:ok, handle}
    end

    def release(prepared, ack, opts) do
      {:ok, handle} = GateDouble.release(prepared, ack, opts)
      remember(handle, :release)
      {:ok, handle}
    end

    defp remember(handle, stage) do
      Process.put(:review_latest_handle, handle)
      if Process.get(:review_fail_stage) == stage, do: Process.put(:review_clock_raise, true)
    end

    def abandon(handle) do
      Process.put(:review_abandoned, [handle | Process.get(:review_abandoned, [])])
      :ok
    end

    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate await(handle, opts), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
  end

  setup do
    H.reset_seams()
    :ok
  end

  defp run(extra \\ []) do
    {_, :run, "gated_run_seed", [], make_opts} =
      Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

    opts = make_opts.() |> Keyword.merge(clock: Clock, gate_executor: Executor) |> Keyword.merge(extra)
    Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
  end

  defp assert_latest_abandoned do
    assert [abandoned] = Process.get(:review_abandoned)
    assert abandoned == Process.get(:review_latest_handle)
  end

  for stage <- [:prepare, :release] do
    test "#{stage} handle survives a clock exception before observation return" do
      Process.put(:review_fail_stage, unquote(stage))
      assert_raise RuntimeError, "review clock", fn -> run() end
      assert_latest_abandoned()
    end
  end

  for effect <- [Effect.PrepareGate, Effect.ReleaseGate] do
    test "#{inspect(effect)} observer exception cleans the latest handle" do
      observer = fn intent, _observation ->
        if intent.__struct__ == unquote(effect), do: raise("review observer")
      end

      assert_raise RuntimeError, "review observer", fn -> run(effect_observer: observer) end
      assert_latest_abandoned()
    end
  end

  test "a refused gate terminal still owns the released handle" do
    sink =
      GateDouble.receipt(fn event ->
        if event["type"] == "gate_passed", do: {:error, %{clause: "review_sink"}}, else: :ok
      end)

    assert {:error, _rejection} = run(event_sink: sink)
    assert_latest_abandoned()
    assert Process.get(:review_latest_handle).released
  end

  # BASELINE DISCREPANCY, disclosed (docs/contracts/effects-extraction.org, "Suffix-commit boundary",
  # ruling R-4). Codex's original sixth probe asserted:
  #     assert Process.get(:review_abandoned, []) == []
  # i.e. "a later sink exception does not abandon an already committed gate terminal". At 61a16ac
  # that FAILS: commit/3 commits the suffix as a whole and terminal release runs only afterwards,
  # so a sink raise on run_completed (after the gate_passed line was persisted) leaves the handle
  # registered and the raise exit abandons it - conservative extra cleanup of an already-exited
  # worker. This test pins THAT baseline so the extraction cannot change it silently; per-line
  # terminal release is a behavioural change for a separate ruling.
  test "BASELINE PIN (R-4): a sink exception on a later line of the suffix that persisted gate_passed still abandons the released handle" do
    sink =
      GateDouble.receipt(fn event ->
        if event["type"] == "run_completed", do: raise("review after terminal")
        :ok
      end)

    assert_raise RuntimeError, "review after terminal", fn -> run(event_sink: sink) end
    assert Process.get(:review_latest_handle).released
    assert_latest_abandoned()
  end
end
