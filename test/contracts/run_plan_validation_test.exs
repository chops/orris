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
        "invalid_artifact_absolute",
        "invalid_artifact_outside_roots",
        "invalid_artifact_traversal",
        "invalid_cycle",
        "invalid_duplicate_ids",
        "invalid_missing_dep",
        "invalid_undeclared_agent",
        "invalid_effort_hint_value",
        "invalid_empty_expected_artifacts",
        "invalid_multiple_expected_artifacts",
        "invalid_empty_acceptance",
        "invalid_multiple_acceptance",
        "invalid_unknown_acceptance_gate",
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

  test "malformed acceptance fields retain the existing shape rejection" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    [item | rest] = plan["work_items"]

    malformed =
      [Map.delete(item, "acceptance")] ++
        Enum.map([nil, "tests", %{}, [1], [nil], ["tests", 1], [%{"tests" => true}]], fn acceptance ->
          Map.put(item, "acceptance", acceptance)
        end)

    for invalid_item <- malformed do
      invalid_plan = Map.put(plan, "work_items", [invalid_item | rest])
      assert {:error, %{clause: "invalid_run_plan_shape"}} = Plan.validate(invalid_plan, spec)
    end
  end

  test "valid singleton acceptance declarations are preserved for every work item" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    assert {:ok, validated} = Plan.validate(plan, spec)

    assert Enum.map(validated["work_items"], & &1["acceptance"]) == Enum.map(plan["work_items"], & &1["acceptance"])
  end

  test "a later review work item's acceptance is checked and named in the rejection" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    [writer, reviewer] = plan["work_items"]

    empty = Map.put(plan, "work_items", [writer, Map.put(reviewer, "acceptance", [])])
    assert {:error, %{clause: "acceptance_gate_cardinality", field: "item_b"}} = Plan.validate(empty, spec)

    unknown = Map.put(plan, "work_items", [writer, Map.put(reviewer, "acceptance", ["not_a_gate"])])
    assert {:error, %{clause: "unknown_acceptance_gate", field: "not_a_gate"}} = Plan.validate(unknown, spec)
  end

  test "an acceptance entry naming any declared gate is accepted, not only the first declared" do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    spec = Map.put(spec, "gates", %{"tests" => ["mix", "test"], "format" => ["mix", "format", "--check-formatted"]})
    [writer, reviewer] = plan["work_items"]
    plan = Map.put(plan, "work_items", [writer, Map.put(reviewer, "acceptance", ["format"])])

    assert {:ok, _validated} = Plan.validate(plan, spec)
  end

  test "earlier timeout, duplicate-id, missing-dependency and artifact rejections keep precedence over acceptance" do
    for name <- [
          "invalid_nonpositive_timeout",
          "invalid_duplicate_ids",
          "invalid_missing_dep",
          "invalid_empty_expected_artifacts",
          "invalid_multiple_expected_artifacts"
        ] do
      plan = F.json("plans", name, "plan.json")
      spec = F.json("plans", name, "spec.json")
      expected = F.json("plans", name, "expected_rejection.json")
      [item | rest] = plan["work_items"]

      for acceptance <- [[], ["tests", "tests2"], ["not_a_gate"]] do
        plan = Map.put(plan, "work_items", [Map.put(item, "acceptance", acceptance) | rest])

        assert {:error, rejection} = Plan.validate(plan, spec)
        F.assert_rejection_matches(rejection, expected)
      end
    end
  end

  test "acceptance rejections keep precedence over the later agent-role and path checks" do
    for name <- ["invalid_undeclared_agent", "invalid_paths_outside_roots"] do
      plan = F.json("plans", name, "plan.json")
      spec = F.json("plans", name, "spec.json")
      [%{"id" => id} = item | rest] = plan["work_items"]
      plan = Map.put(plan, "work_items", [Map.put(item, "acceptance", []) | rest])

      assert {:error, %{clause: "acceptance_gate_cardinality", field: ^id}} = Plan.validate(plan, spec)
    end
  end
end
