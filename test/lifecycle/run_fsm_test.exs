defmodule AiOrchestrator.Lifecycle.RunFSMTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule FakePaneClient do
    @moduledoc false
    # Every stand-in answers a reconcile: the adapter asks before every send.
    def reconcile(pane_ref, message_id, _opts),
      do:
        {:ok,
         %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, prompt, opts) do
      if test_pid = Keyword.get(opts, :test_pid) do
        # The stub is the adapter that would paste, so it is the one place entitled to reveal.
        Process.send(test_pid, {:send_called, pane_ref, SensitiveBytes.reveal(prompt)}, [])
      end

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  test "gated_run_seed executes through local pane dispatch and folds to the expected summary" do
    spec = F.json("scenarios", "gated_run_seed", "spec.json")
    plan = F.json("scenarios", "gated_run_seed", "plan.json")
    expected = F.json("scenarios", "gated_run_seed", "expected.json")
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    artifact_reader = fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end

    gate_runner = fn _gate, gate_opts ->
      Process.send(self(), {:gate_opts, gate_opts[:repo_root], gate_opts[:run_dir]}, [])
      {:ok, gate_pass}
    end

    assert {:ok, result} =
             RunFSM.run(spec, plan,
               dispatch: LocalPane,
               prompt_root: ScenarioHarness.prompt_root(),
               dispatch_opts: [artifact_reader: artifact_reader, pane_client: FakePaneClient, test_pid: self()],
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: gate_runner],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end,
               run_id: "run_scenario_0001"
             )

    assert Enum.map(result.events, & &1["type"]) == Enum.map(fixture_events, & &1["type"])
    assert result.summary == expected
    assert_receive {:gate_opts, "/tmp/example-repo", "/tmp/example-run"}

    sent = sent_prompts()
    assert %{"pane_writer" => writer_prompt, "pane_reviewer" => reviewer_prompt} = sent
    assert writer_prompt =~ "Scenario seed run"
    assert writer_prompt =~ "Work item item_a"
    assert writer_prompt =~ "- expected_artifact :: lib/item_a.ex"
    assert writer_prompt =~ "* Current run context"
    assert writer_prompt =~ "#+title: Run context"

    assert reviewer_prompt =~ "* Review subject"
    assert reviewer_prompt =~ "- subject_assignment_id :: as_0001"
    assert reviewer_prompt =~ "- subject_artifact :: art_as_0001 at lib/item_a.ex"
    assert reviewer_prompt =~ "* Allowed paths\n- review/item_a.org\n\n* Acceptance gates"
    assert reviewer_prompt =~ "- expected_artifact :: review/item_a.org"

    metadata = prompt_metadata(result.events, "as_0001")
    assert metadata["prompt_hash"] == sha256(writer_prompt)
    assert metadata["prompt_bytes"] == byte_size(writer_prompt)
    assert metadata["context_hash"] != LocalPane.zero_hash()
  end

  test "agent pane_hint pane_ref is the runtime local-pane binding" do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> put_in(["agents", Access.at(0), "pane_hint"], %{"pane_ref" => "scratch_writer"})
      |> put_in(["agents", Access.at(1), "pane_hint"], %{"pane_ref" => "scratch_reviewer"})

    plan = F.json("scenarios", "gated_run_seed", "plan.json")
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    artifact_reader = fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end
    gate_runner = fn _gate, _gate_opts -> {:ok, gate_pass} end

    assert {:ok, result} =
             RunFSM.run(spec, plan,
               dispatch: LocalPane,
               prompt_root: ScenarioHarness.prompt_root(),
               dispatch_opts: [artifact_reader: artifact_reader, pane_client: FakePaneClient, test_pid: self()],
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: gate_runner],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
             )

    assert result.summary["status"] == "completed"
    assert sent_prompts() |> Map.keys() |> Enum.sort() == ["scratch_reviewer", "scratch_writer"]
  end

  defp fixture_data(events, type) do
    events
    |> Enum.find(&(&1["type"] == type))
    |> Map.fetch!("data")
  end

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(fn event ->
      data = Map.fetch!(event, "data")
      {Map.fetch!(data, "assignment_id"), data}
    end)
  end

  defp prompt_metadata(events, assignment_id) do
    events
    |> fixture_data_by_assignment("assignment_prompt_projected")
    |> Map.fetch!(assignment_id)
  end

  defp sent_prompts(acc \\ %{}) do
    receive do
      {:send_called, pane_ref, prompt} -> sent_prompts(Map.put(acc, pane_ref, prompt))
    after
      0 -> acc
    end
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end
end
