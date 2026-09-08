defmodule AiOrchestrator.Lifecycle.RunFSMGateRetryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule OkDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    @impl true
    def deliver(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "backend" => "local_pane",
         "pane_ref" => command["pane_ref"],
         "send_status" => "ok",
         "send_message_id" => command["send_message_id"],
         "replayed" => false
       }}
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => LocalPane.zero_hash(),
         "stable_for_ms" => 5000,
         "modified_after_dispatch" => true
       }}
    end
  end

  # G-3 (review of 910c51b): a failing gate must be JOURNALED — gate_failed with its
  # bounded failure_summary, then work_item_retry_scheduled within budget, then the
  # retry attempt — not surfaced as {:error, ...} aborting the run. Mirrors the
  # gate_failure_summary_feedback scenario fixture at runtime.
  test "gate failure journals gate_failed + retry and the second attempt completes the run" do
    spec = F.json("scenarios", "gate_failure_summary_feedback", "spec.json")
    plan = F.json("scenarios", "gate_failure_summary_feedback", "plan.json")
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    flaky_gate = fn _gate ->
      case Agent.get_and_update(counter, &{&1, &1 + 1}) do
        0 ->
          {:failed,
           %{
             "exit_status" => 1,
             "duration_ms" => 100,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash(),
             "failure_summary" => %{
               "headline" => "1 test failed",
               "failures" => [%{"line" => "test/item_a_test.exs:12"}],
               "suggestion" => "fix the failing check"
             }
           }}

        _later ->
          {:ok,
           %{
             "exit_status" => 0,
             "duration_ms" => 100,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash()
           }}
      end
    end

    assert {:ok, %{events: events, summary: summary}} =
             RunFSM.run(spec, plan,
               dispatch: OkDispatch,
               prompt_root: ScenarioHarness.prompt_root(),
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: flaky_gate],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
             )

    types = Enum.map(events, & &1["type"])
    assert "gate_failed" in types
    assert "work_item_retry_scheduled" in types

    gate_failed = Enum.find(events, &(&1["type"] == "gate_failed"))
    assert %{"failure_summary" => %{"headline" => _headline}} = gate_failed["data"]

    assert summary["status"] == "completed"
    assert summary["completed_work_item_ids"] == ["item_a"]
  end
end
