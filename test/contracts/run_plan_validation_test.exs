defmodule AiOrchestrator.Contracts.RunPlanValidationTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Spec.Plan

  for name <- ["valid_linear", "valid_diamond"] do
    test "#{name}: plan validates against its spec" do
      plan = F.json("plans", unquote(name), "plan.json")
      spec = F.json("plans", unquote(name), "spec.json")
      assert {:ok, validated} = Plan.validate(plan, spec)
      assert is_map(validated)
    end
  end

  for name <- [
        "invalid_cycle",
        "invalid_duplicate_ids",
        "invalid_missing_dep",
        "invalid_undeclared_agent",
        "invalid_effort_hint_value",
        "invalid_empty_expected_artifacts",
        "invalid_multiple_expected_artifacts",
        "invalid_stretch_overlap",
        "invalid_paths_outside_roots",
        "invalid_nonpositive_timeout"
      ] do
    test "#{name}: plan rejected with the named clause" do
      name = unquote(name)
      plan = F.json("plans", name, "plan.json")
      spec = F.json("plans", name, "spec.json")
      expected = F.json("plans", name, "expected_rejection.json")
      assert {:error, rejection} = Plan.validate(plan, spec)
      F.assert_rejection_matches(rejection, expected)
    end
  end

  test "a sole empty artifact string is rejected with the work item id" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    [item | rest] = plan["work_items"]
    plan = Map.put(plan, "work_items", [Map.put(item, "expected_artifacts", [""]) | rest])

    assert {:error, %{clause: "expected_artifact_cardinality", field: "item_a"}} = Plan.validate(plan, spec)
  end

  test "malformed artifact fields retain the existing shape rejection" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    [item | rest] = plan["work_items"]

    malformed =
      [Map.delete(item, "expected_artifacts")] ++
        Enum.map([nil, "lib/item_a.ex", %{}, [1], [nil], ["lib/item_a.ex", 1]], fn artifacts ->
          Map.put(item, "expected_artifacts", artifacts)
        end)

    for invalid_item <- malformed do
      invalid_plan = Map.put(plan, "work_items", [invalid_item | rest])
      assert {:error, %{clause: "invalid_run_plan_shape"}} = Plan.validate(invalid_plan, spec)
    end
  end

  test "valid singleton artifact declarations are preserved for every work item" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    assert {:ok, validated} = Plan.validate(plan, spec)

    assert Enum.map(validated["work_items"], & &1["expected_artifacts"]) ==
             Enum.map(plan["work_items"], & &1["expected_artifacts"])
  end

  test "a later review work item is checked and named in the rejection" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    [writer, reviewer] = plan["work_items"]
    plan = Map.put(plan, "work_items", [writer, Map.put(reviewer, "expected_artifacts", [])])

    assert {:error, %{clause: "expected_artifact_cardinality", field: "item_b"}} = Plan.validate(plan, spec)
  end

  test "earlier timeout, duplicate-id and missing-dependency rejections keep precedence" do
    for name <- ["invalid_nonpositive_timeout", "invalid_duplicate_ids", "invalid_missing_dep"] do
      plan = F.json("plans", name, "plan.json")
      spec = F.json("plans", name, "spec.json")
      expected = F.json("plans", name, "expected_rejection.json")
      [item | rest] = plan["work_items"]
      plan = Map.put(plan, "work_items", [Map.put(item, "expected_artifacts", []) | rest])

      assert {:error, rejection} = Plan.validate(plan, spec)
      F.assert_rejection_matches(rejection, expected)
    end
  end
end
