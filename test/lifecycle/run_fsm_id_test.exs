defmodule AiOrchestrator.Lifecycle.RunFSMIdTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Id.SystemId
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.FixedId
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule OkDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

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

  defp stub_gate(_gate) do
    {:ok,
     %{
       "exit_status" => 0,
       "duration_ms" => 100,
       "stdout_hash" => LocalPane.zero_hash(),
       "stderr_hash" => LocalPane.zero_hash()
     }}
  end

  defp seed_inputs do
    {F.json("scenarios", "gated_run_seed", "spec.json"), F.json("scenarios", "gated_run_seed", "plan.json")}
  end

  defp run_opts,
    do: [
      dispatch: OkDispatch,
      prompt_root: ScenarioHarness.prompt_root(),
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: &stub_gate/1],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]

  test "SystemId run ids match the grammar and are unique" do
    id_a = SystemId.run_id()
    id_b = SystemId.run_id()

    assert id_a =~ ~r/^run_\d{8}T\d{6}Z_[0-9a-f]{24}$/
    assert id_b =~ ~r/^run_\d{8}T\d{6}Z_[0-9a-f]{24}$/
    refute id_a == id_b
    assert SystemId.supervisor_instance() =~ ~r/^sup_[0-9a-f]{24}$/
  end

  test "fresh runs get seam-generated identity and truthful derived provenance" do
    {spec, plan} = seed_inputs()

    assert {:ok, %{events: [created | _rest]}} = RunFSM.run(spec, plan, run_opts())

    assert created["run_id"] =~ ~r/^run_\d{8}T\d{6}Z_[0-9a-f]{24}$/
    assert created["data"]["project"] == "example-repo"
    refute created["run_id"] == "run_scenario_0001"

    assert {:ok, %{events: [created_b | _]}} = RunFSM.run(spec, plan, run_opts())
    refute created_b["run_id"] == created["run_id"]
  end

  test "roster hash is content-derived and order-independent; context hash is canonical-empty" do
    {spec, plan} = seed_inputs()
    shuffled = Map.update!(spec, "agents", &Enum.reverse/1)

    assert {:ok, %{events: events_a}} = RunFSM.run(spec, plan, run_opts())
    assert {:ok, %{events: events_b}} = RunFSM.run(shuffled, plan, run_opts())

    roster_a = Enum.find(events_a, &(&1["type"] == "run_spec_loaded"))["data"]["agent_roster_hash"]
    roster_b = Enum.find(events_b, &(&1["type"] == "run_spec_loaded"))["data"]["agent_roster_hash"]

    assert roster_a == roster_b
    refute roster_a == LocalPane.zero_hash()

    context_hash = Enum.find(events_a, &(&1["type"] == "plan_recorded"))["data"]["context_initial_hash"]
    empty_hash = "sha256:" <> Base.encode16(:crypto.hash(:sha256, ""), case: :lower)
    assert context_hash == empty_hash
  end

  test "resume preserves the recorded run_id and generates a fresh supervisor instance" do
    {spec, plan} = seed_inputs()

    prior_lines =
      "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
      |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
      |> File.read!()
      |> String.split("\n", trim: true)

    prior_instance =
      prior_lines
      |> Enum.map(&Jason.decode!/1)
      |> Enum.find(&(&1["type"] == "run_started"))
      |> get_in(["data", "supervisor_instance"])

    assert {:ok, %{events: events}} = RunFSM.resume(spec, plan, prior_lines, run_opts())

    resumed = Enum.find(events, &(&1["type"] == "run_resumed"))
    assert resumed["run_id"] == "run_scenario_0001"
    assert resumed["data"]["supervisor_instance"] =~ ~r/^sup_[0-9a-f]{24}$/
    refute resumed["data"]["supervisor_instance"] == prior_instance
  end

  test "FixedId double stays deterministic for fixture-grade tests" do
    FixedId.reset()
    assert FixedId.run_id() == "run_fixture_0001"
    assert FixedId.run_id() == "run_fixture_0002"
    assert FixedId.supervisor_instance() == "sup_0001"
  end
end
