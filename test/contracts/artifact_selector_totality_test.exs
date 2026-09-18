defmodule AiOrchestrator.Contracts.ArtifactSelectorTotalityTest do
  @moduledoc """
  NS-35.D.000 / NS-35.D.001 subclaim 1 -- the half the row's three delivered Evidence lines do not
  cover. The row's failure control is "First-artifact-only selection or inert accepted field fails".

  Two delivered facts sit in two different files and nothing joins them. Admission refuses a work
  item that does not declare EXACTLY ONE non-empty artifact (`Spec.Plan.validate_expected_artifacts/1`,
  plan.ex:161-175, clause `expected_artifact_cardinality`). The pure reducer then selects the FIRST
  declared artifact (`expected_artifact/2`, reducer.ex:3149-3150) through a two-clause function with
  no catch-all. Neither file can state the join, so the row can acquire a false verdict in either
  direction: weaken admission and the selector becomes PARTIAL -- a work item with no artifact raises
  `FunctionClauseError` inside the pure core -- and widen it and the selector becomes silently LOSSY,
  which is the loss plan.ex:171 names as the reason the rule exists ("The reducer observes one
  artifact").

  These rows state the join as an implication over admission itself: every plan `Plan.validate/2`
  admits carries a singleton artifact list on every work item, so `[artifact | _rest]` has
  `_rest == []` and "the first" is "the only one". They do NOT claim the reducer observes several
  artifacts. It observes one. What is pinned here is that the one it observes is the one that was
  declared, and that a declaration the reducer could not honour is refused at ADMISSION rather than
  discovered at observation time.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  @admitted_plan_fixtures ["valid_linear", "valid_diamond"]
  @scenario_plan_fixtures ~w(auth_blocked_pane concurrency_cap fingerprint_drift
                             gate_failure_summary_feedback gated_run_seed kill9_resume)

  defmodule CapturingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, opts) do
      if pid = Keyword.get(opts, :test_pid) do
        send(pid, {:dispatched, command["assignment_id"], command["expected_artifact"]})
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

  # ---- the implication itself ----

  test "T4-1 every plan fixture admission admits carries exactly one non-empty artifact per work item" do
    admitted =
      for {class, name} <-
            Enum.map(@admitted_plan_fixtures, &{"plans", &1}) ++
              Enum.map(@scenario_plan_fixtures, &{"scenarios", &1}) do
        plan = F.json(class, name, "plan.json")
        spec = F.json(class, name, "spec.json")

        result = Plan.validate(plan, spec)
        assert match?({:ok, _validated}, result), "#{class}/#{name} no longer validates: #{inspect(result)}"
        {:ok, validated} = result

        for work_item <- validated["work_items"] do
          assert singleton_artifact(work_item) != :not_singleton,
                 "#{class}/#{name} admitted #{inspect(work_item["id"])} with " <>
                   "#{inspect(work_item["expected_artifacts"])}, which the reducer's first-selector " <>
                   "(reducer.ex:3149-3150) cannot honour"
        end

        {class, name}
      end

    # the sweep is not empty, so an empty fixture directory cannot pass it
    assert length(admitted) == length(@admitted_plan_fixtures) + length(@scenario_plan_fixtures)
    assert length(admitted) >= 8
  end

  test "T4-2 the implication holds over a family admission must split, and the family really splits" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")

    candidates = [
      {[], :refused},
      {[""], :refused},
      {["lib/item_a.ex", "lib/second.ex"], :refused},
      {["lib/item_a.ex", "lib/item_a.ex"], :refused},
      {["lib/item_a.ex"], :admitted},
      {["lib/nested/item_a.ex"], :admitted}
    ]

    outcomes =
      for {artifacts, expected} <- candidates do
        case Plan.validate(with_writer_artifacts(plan, artifacts), spec) do
          {:ok, validated} ->
            assert expected == :admitted, "#{inspect(artifacts)} was admitted"

            for work_item <- validated["work_items"] do
              assert singleton_artifact(work_item) != :not_singleton,
                     "admitted #{inspect(artifacts)} left the first-selector partial or lossy"
            end

            :admitted

          {:error, rejection} ->
            assert expected == :refused, "#{inspect(artifacts)} was refused: #{inspect(rejection)}"

            # the refusal is the one that makes the selector total, and it names the item
            assert rejection == %{clause: "expected_artifact_cardinality", field: "item_a"},
                   "#{inspect(artifacts)} was refused by some other rule: #{inspect(rejection)}"

            :refused
        end
      end

    # neither half may be empty: an admission that admitted everything, or nothing, would satisfy the
    # implication above without saying anything at all
    assert :admitted in outcomes and :refused in outcomes
    assert Enum.count(outcomes, &(&1 == :refused)) == 4
    assert Enum.count(outcomes, &(&1 == :admitted)) == 2
  end

  # ---- the selector, in the delivered path ----

  test "T4-3 every assignment the reducer projects names the sole artifact its work item declared" do
    {spec, plan} = multi_item_inputs()

    assert {:ok, %{events: events, summary: summary}} = RunFSM.run(spec, plan, fsm_opts())
    assert summary["status"] == "completed"

    declared = Map.new(plan["work_items"], &{&1["id"], &1["expected_artifacts"]})

    projected =
      for event <- events, event["type"] == "assignment_prompt_projected" do
        {event["data"]["assignment_id"], event["data"]["expected_artifact"]}
      end

    assert projected != [], "no prompt was projected, so the selector was never reached"

    roles =
      Map.new(
        for event <- events, event["type"] == "assignment_requested" do
          {event["data"]["assignment_id"], {event["data"]["role"], event["data"]["work_item_id"]}}
        end
      )

    for {assignment_id, expected_artifact} <- projected do
      assert is_binary(expected_artifact) and expected_artifact != "",
             "#{assignment_id} projected #{inspect(expected_artifact)}"

      {role, work_item_id} = Map.fetch!(roles, assignment_id)

      case role do
        "writer" ->
          assert declared[work_item_id] == [expected_artifact],
                 "#{work_item_id}: the reducer selected #{inspect(expected_artifact)} out of " <>
                   "#{inspect(declared[work_item_id])}, so first and only are not the same artifact"

        "reviewer" ->
          # the review work item is synthesised by the reducer itself (reducer.ex:3007, 3016) and it
          # declares exactly one artifact too, so the same selector is total over it
          assert expected_artifact == "review/#{work_item_id}.org"
      end
    end

    # the artifact the ADAPTER was handed is the same one, so nothing re-selects downstream
    for {assignment_id, expected_artifact} <- projected do
      assert_received {:dispatched, ^assignment_id, ^expected_artifact}
    end
  end

  test "T4-4 an artifact the reducer could not honour is refused at admission, before any event exists" do
    {spec, plan} = multi_item_inputs()

    # a writer artifact outside the spec's allowed roots: refused by FORM and ROOTS at admission
    # (plan.ex:177-207), never carried into an observation the host would have to make
    outside = with_writer_artifacts(plan, ["docs/item_a.ex"])
    assert Plan.validate(outside, spec) == {:error, %{clause: "work_item_paths_outside_roots", field: "docs/item_a.ex"}}

    assert RunFSM.run(spec, outside, fsm_opts()) ==
             {:error, %{clause: "work_item_paths_outside_roots", field: "docs/item_a.ex"}}

    # a plural declaration the first-selector would silently truncate: same treatment
    plural = with_writer_artifacts(plan, ["lib/item_a.ex", "lib/also_mine.ex"])
    assert Plan.validate(plural, spec) == {:error, %{clause: "expected_artifact_cardinality", field: "item_a"}}
    assert RunFSM.run(spec, plural, fsm_opts()) == {:error, %{clause: "expected_artifact_cardinality", field: "item_a"}}

    # neither reached the selector: no assignment was ever projected and no adapter was ever called
    refute_received {:dispatched, _assignment_id, _artifact}
  end

  # ---- helpers ----

  defp singleton_artifact(%{"expected_artifacts" => [artifact]}) when is_binary(artifact) and artifact != "", do: artifact

  defp singleton_artifact(_work_item), do: :not_singleton

  defp with_writer_artifacts(plan, artifacts) do
    [writer | rest] = plan["work_items"]
    Map.put(plan, "work_items", [Map.put(writer, "expected_artifacts", artifacts) | rest])
  end

  # the three-item shape `run_fsm_multi_item_test.exs` drives: item_c made to depend on both of its
  # predecessors, so the run is a straight line and this file measures the selector, not the scheduler
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
end
