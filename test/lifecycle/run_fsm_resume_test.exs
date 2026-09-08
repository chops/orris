defmodule AiOrchestrator.Lifecycle.RunFSMResumeTest do
  use ExUnit.Case, async: true

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
      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, prompt}, [])

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def status(pane_ref, opts) do
      {:ok, Keyword.get(opts, :pane_status, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0})}
    end
  end

  test "resume before dispatch repairs stale leases and continues through completion" do
    assert {:ok, result} = resume_from("events_pre_dispatch.jsonl")

    assert result.summary["status"] == "completed"
    assert result.summary["completed_work_item_ids"] == ["item_a"]

    assert result.appended_events |> Enum.map(& &1["type"]) |> Enum.take(7) == [
             "run_resumed",
             "workspace_lease_release_requested",
             "workspace_lease_released",
             "workspace_lease_acquired",
             "pane_lease_release_requested",
             "pane_lease_released",
             "pane_lease_acquired"
           ]

    assert "pane_writer" in sent_panes()
  end

  test "resume while awaiting an artifact observes without resending the writer prompt" do
    assert {:ok, result} = resume_from("events_awaiting_artifact.jsonl")

    assert result.summary["status"] == "completed"
    assert result.summary["completed_work_item_ids"] == ["item_a"]
    panes = sent_panes()
    refute "pane_writer" in panes
    assert "pane_reviewer" in panes
  end

  # a legacy (version 1) gate_started records no process identity, so the resumed owner cannot know
  # whether that gate still runs: the lawful outcome is attention, never a silent rerun
  test "resume before a gate result of a legacy start is attention gate_start_unresolved: no rerun, no prompts resent" do
    assert {:ok, result} = resume_from("events_pre_gate.jsonl")

    assert result.summary["status"] == "blocked"
    assert Map.get(result.summary, "completed_work_item_ids", []) == []

    assert [%{"data" => %{"reason" => "gate_start_unresolved"}}] =
             Enum.filter(result.appended_events, &(&1["type"] == "human_attention_required"))

    assert result.appended_events |> Enum.map(& &1["type"]) |> List.last() == "human_attention_required"
    assert sent_panes() == []
    refute_received {:gate_called, _}
  end

  test "resume journals attention when the resumed pane is auth blocked" do
    opts =
      fsm_opts(pane_status: %{"state" => "blocked", "pane_ref" => "pane_writer", "pending_count" => 1})

    assert {:ok, result} = resume_from("events_awaiting_artifact.jsonl", opts)

    assert result.summary["status"] == "blocked"
    assert result.summary["open_attention_ids"] == ["att_0001"]
    assert result.appended_events |> Enum.map(& &1["type"]) |> List.last() == "human_attention_required"
  end

  test "resume of a terminal journal is a no-op" do
    spec = F.json("scenarios", "gated_run_seed", "spec.json")
    plan = F.json("scenarios", "gated_run_seed", "plan.json")

    assert {:ok, %{appended_events: [], summary: %{"status" => "completed"}}} =
             RunFSM.resume(spec, plan, F.lines("scenarios", "gated_run_seed"), fsm_opts())
  end

  test "cancel appends a terminal cancellation and releases active leases" do
    assert {:ok, result} = RunFSM.cancel(kill9_lines("events_pre_dispatch.jsonl"))

    appended_types = Enum.map(result.appended_events, & &1["type"])
    assert result.summary["status"] == "cancelled"
    assert "run_cancel_requested" in appended_types
    assert "workspace_lease_released" in appended_types
    assert "pane_lease_released" in appended_types
    assert List.last(appended_types) == "run_cancelled"
  end

  test "cancel of a terminal journal is a no-op" do
    assert {:ok, %{appended_events: [], summary: %{"status" => "completed"}}} =
             RunFSM.cancel(F.lines("scenarios", "gated_run_seed"))
  end

  defp resume_from(file, opts \\ fsm_opts()) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.resume(spec, plan, kill9_lines(file), opts)
  end

  defp kill9_lines(file) do
    [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", file]
    |> Path.join()
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  defp fsm_opts(extra_dispatch_opts \\ []) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")
    parent = self()

    artifact_reader = fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end

    gate_runner = fn gate, _gate_opts ->
      Process.send(parent, {:gate_called, gate["gate_run_id"]}, [])
      {:ok, gate_pass}
    end

    [
      dispatch: LocalPane,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts:
        [artifact_reader: artifact_reader, pane_client: FakePaneClient, test_pid: parent] ++ extra_dispatch_opts,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: gate_runner],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
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

  defp sent_panes do
    receive do
      {:send_called, pane_ref, _prompt} -> [pane_ref | sent_panes()]
    after
      0 -> []
    end
  end
end
