defmodule AiOrchestrator.Lifecycle.RunFSMBlockedTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule BlockedDispatch do
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
      {:blocked,
       %{
         "reason" => "agent_auth_blocked",
         "pane_ref" => command["pane_ref"],
         "pane_state" => "blocked",
         "pending_count" => 0
       }}
    end
  end

  # R-4 (review of 4663f63): a blocked pane must be JOURNALED — wedge observation,
  # assignment failure, durable attention, no duplicate send — not surfaced as a
  # bare error tuple. Mirrors the auth_blocked_pane scenario fixture at runtime.
  test "blocked observation journals wedge + attention and returns a blocked summary" do
    spec = F.json("scenarios", "auth_blocked_pane", "spec.json")
    plan = F.json("scenarios", "auth_blocked_pane", "plan.json")
    fixture_events = Enum.map(F.lines("scenarios", "auth_blocked_pane"), &Jason.decode!/1)

    assert {:ok, %{events: events, summary: summary}} =
             RunFSM.run(spec, plan,
               dispatch: BlockedDispatch,
               prompt_root: ScenarioHarness.prompt_root(),
               run_id: "run_scenario_0002"
             )

    types = Enum.map(events, & &1["type"])
    fixture_types = Enum.map(fixture_events, & &1["type"])

    assert "agent_wedge_detected" in types
    assert "human_attention_required" in types
    assert Enum.count(types, &(&1 == "assignment_dispatch_sent")) == 1
    assert summary["status"] == "blocked"
    assert [_attention_id] = summary["open_attention_ids"]
    assert types == fixture_types

    prompt = Enum.find(events, &(&1["type"] == "assignment_prompt_projected"))
    assert prompt["data"]["prompt_hash"] != "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    assert prompt["data"]["prompt_bytes"] > 512
  end
end
