defmodule AiOrchestrator.Lifecycle.RunFSMMultiItemTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule CapturingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    @impl true
    def deliver(command, opts) do
      if pid = Keyword.get(opts, :test_pid) do
        # The double stands where the pane adapter stands, so it is entitled to reveal.
        send(pid, {:prompt, command["assignment_id"], SensitiveBytes.reveal(command["prompt"])})
      end

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
         "stable_for_ms" => 5_000,
         "modified_after_dispatch" => true
       }}
    end
  end

  test "three dependent writer items release overlapping workspace leases and inherit completed context" do
    {spec, plan} = multi_item_inputs()

    assert {:ok, %{events: events, summary: summary}} = RunFSM.run(spec, plan, fsm_opts())

    assert summary["status"] == "completed"
    assert summary["completed_work_item_ids"] == ["item_a", "item_b", "item_c"]

    assert events |> types() |> Enum.count(&(&1 == "workspace_lease_release_requested")) == 3
    assert events |> types() |> Enum.count(&(&1 == "workspace_lease_released")) == 3
    assert events |> types() |> Enum.count(&(&1 == "gate_passed")) == 3

    assert plan_recorded(events)["dag_edges"] == [
             %{"item" => "item_b", "depends_on" => "item_a"},
             %{"item" => "item_c", "depends_on" => "item_a"},
             %{"item" => "item_c", "depends_on" => "item_b"}
           ]

    assert release_precedes_next_item?(events, "item_a", "item_b")
    assert release_precedes_next_item?(events, "item_b", "item_c")

    prompts = collect_prompts(%{})
    item_b_prompt = writer_prompt(events, prompts, "item_b")
    item_c_prompt = writer_prompt(events, prompts, "item_c")

    assert item_b_prompt =~ "- item_a :: DONE"
    assert item_b_prompt =~ "- item_b :: OPEN"
    assert item_c_prompt =~ "- item_a :: DONE"
    assert item_c_prompt =~ "- item_b :: DONE"

    writer_assignment_ids =
      events
      |> Enum.filter(&match?(%{"type" => "assignment_requested", "data" => %{"role" => "writer"}}, &1))
      |> MapSet.new(& &1["data"]["assignment_id"])

    leased_assignment_ids =
      events
      |> Enum.filter(&(&1["type"] == "workspace_lease_requested"))
      |> MapSet.new(& &1["data"]["assignment_id"])

    assert leased_assignment_ids == writer_assignment_ids
  end

  test "resume releases a completed item's active workspace lease before the next item" do
    {spec, plan} = resume_inputs()
    prior_lines = "scenarios" |> F.lines("concurrency_cap") |> Enum.take(18)

    assert {:ok, result} = RunFSM.resume(spec, plan, prior_lines, fsm_opts())

    assert result.summary["status"] == "completed"
    assert result.summary["completed_work_item_ids"] == ["item_a", "item_b"]

    assert result.appended_events |> types() |> Enum.take(3) == [
             "run_resumed",
             "workspace_lease_release_requested",
             "workspace_lease_released"
           ]

    assert dispatch_count(result.events, "as_0001") == 1
  end

  test "resume completes a dangling workspace release instead of opening another bracket" do
    {spec, plan} = resume_inputs()
    prior_lines = "scenarios" |> F.lines("concurrency_cap") |> Enum.take(19)

    assert {:ok, result} = RunFSM.resume(spec, plan, prior_lines, fsm_opts())

    assert result.summary["status"] == "completed"

    workspace_events =
      Enum.filter(
        result.appended_events,
        &(&1["type"] in [
            "workspace_lease_release_requested",
            "workspace_lease_released"
          ])
      )

    assert [%{"type" => "workspace_lease_released", "data" => data} | _rest] = workspace_events
    assert data["release_request_id"] == "wslr_0001"

    refute Enum.any?(
             workspace_events,
             &(&1["type"] == "workspace_lease_release_requested" and
                 &1["data"]["workspace_lease_id"] == "wsl_as_0001")
           )

    release_request_ids =
      result.events
      |> Enum.filter(&(&1["type"] == "workspace_lease_release_requested"))
      |> Enum.map(& &1["data"]["release_request_id"])

    assert release_request_ids == ["wslr_0001", "wslr_0002"]
    assert Enum.uniq(release_request_ids) == release_request_ids
    assert dispatch_count(result.events, "as_0001") == 1
  end

  defp multi_item_inputs do
    spec = F.json("plans", "valid_diamond", "spec.json")

    plan =
      "plans"
      |> F.json("valid_diamond", "plan.json")
      |> Map.update!("work_items", fn items ->
        items
        |> Enum.take(3)
        |> List.update_at(2, &Map.put(&1, "deps", ["item_b", "item_a"]))
      end)

    {spec, plan}
  end

  defp resume_inputs do
    spec = F.json("scenarios", "concurrency_cap", "spec.json")

    plan =
      "scenarios"
      |> F.json("concurrency_cap", "plan.json")
      |> update_in(["work_items", Access.at(1), "deps"], fn _deps -> ["item_a"] end)

    {spec, plan}
  end

  defp fsm_opts do
    [
      dispatch: CapturingDispatch,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts: [test_pid: self()],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [
        runner: fn _gate ->
          {:ok,
           %{
             "exit_status" => 0,
             "duration_ms" => 10,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash()
           }}
        end
      ],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  defp release_precedes_next_item?(events, completed_item_id, next_item_id) do
    completed_seq = event_seq(events, "work_item_completed", "work_item_id", completed_item_id)
    assignment_id = writer_assignment_id(events, completed_item_id)
    workspace_lease_id = "wsl_" <> assignment_id
    requested_seq = event_seq(events, "workspace_lease_release_requested", "workspace_lease_id", workspace_lease_id)
    released_seq = event_seq(events, "workspace_lease_released", "workspace_lease_id", workspace_lease_id)
    next_seq = event_seq(events, "assignment_requested", "work_item_id", next_item_id)

    completed_seq < requested_seq and requested_seq < released_seq and released_seq < next_seq
  end

  defp writer_prompt(events, prompts, work_item_id) do
    events
    |> writer_assignment_id(work_item_id)
    |> then(&Map.fetch!(prompts, &1))
  end

  defp writer_assignment_id(events, work_item_id) do
    events
    |> Enum.find(fn event ->
      event["type"] == "assignment_requested" and event["data"]["work_item_id"] == work_item_id and
        event["data"]["role"] == "writer"
    end)
    |> get_in(["data", "assignment_id"])
  end

  defp event_seq(events, type, field, value) do
    events
    |> Enum.find(&(&1["type"] == type and &1["data"][field] == value))
    |> Map.fetch!("seq")
  end

  defp dispatch_count(events, assignment_id) do
    Enum.count(events, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == assignment_id))
  end

  defp plan_recorded(events) do
    events
    |> Enum.find(&(&1["type"] == "plan_recorded"))
    |> Map.fetch!("data")
  end

  defp types(events), do: Enum.map(events, & &1["type"])

  defp collect_prompts(acc) do
    receive do
      {:prompt, assignment_id, prompt} -> collect_prompts(Map.put(acc, assignment_id, prompt))
    after
      0 -> acc
    end
  end
end
