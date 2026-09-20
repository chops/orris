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
        "invalid_reviewer_not_independent",
        "invalid_nonpositive_timeout",
        "invalid_integration_kind"
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

  # D-09 (2026-09-20): `kind: "integration"` is refused at admission while no merge policy exists
  # (OPEN-09). These two halves are one control. The negative alone would also pass if admission
  # refused EVERY plan, so the positive half re-admits the same fixture bytes with the one token
  # changed: `invalid_integration_kind/plan.json` and `valid_diamond/plan.json` differ only in
  # item_d's `kind`, and their spec.json bytes are identical.
  describe "D-09 integration admission refusal" do
    setup do
      %{
        plan: F.json("plans", "invalid_integration_kind", "plan.json"),
        spec: F.json("plans", "invalid_integration_kind", "spec.json")
      }
    end

    # fails if validate/2 returns {:ok, _}, or returns any other clause, or names any other item
    test "the integration item is refused and named", %{plan: plan, spec: spec} do
      assert Plan.validate(plan, spec) ==
               {:error, %{clause: "integration_unsupported", field: "item_d"}}
    end

    # fails if admission refuses this plan for ANY reason -- which is what a blanket refusal, or a
    # fixture invalid on some unrelated ground, would do. This is what makes the negative half mean
    # "the kind was refused" rather than "something was refused".
    test "the same plan with item_d as implement is still admitted", %{plan: plan, spec: spec} do
      admissible =
        update_in(plan, ["work_items", Access.at(3)], &Map.put(&1, "kind", "implement"))

      assert admissible != plan
      assert {:ok, _validated} = Plan.validate(admissible, spec)
    end

    # fails if `field` is hardcoded to item_d rather than tracking the offending work item
    test "the refusal names whichever item carries the kind", %{plan: plan, spec: spec} do
      moved =
        plan
        |> update_in(["work_items", Access.at(3)], &Map.put(&1, "kind", "implement"))
        |> update_in(["work_items", Access.at(0)], &Map.put(&1, "kind", "integration"))

      assert Plan.validate(moved, spec) ==
               {:error, %{clause: "integration_unsupported", field: "item_a"}}
    end

    # Masking controls, added 2026-09-20 after review found the ordering defect in 3b5473f.
    # `integration_unsupported` is a CAPABILITY refusal and must never stand in for a structural
    # diagnosis. Every plan below carries the integration item AND a genuine shape error, so every
    # one must come back `invalid_run_plan_shape`. Each of the three fails -- returning
    # `integration_unsupported` instead -- if the kind check runs before `Zoi.parse/2`, which is
    # precisely what the first cut of this slice did: it ran last among the VALIDATORS, and every
    # validator runs before the parse. These are the controls that would have caught that.
    test "an unknown top-level key is diagnosed, not masked by the kind refusal", %{plan: plan, spec: spec} do
      assert Plan.validate(Map.put(plan, "unexpected_key", true), spec) ==
               {:error, %{clause: "invalid_run_plan_shape"}}
    end

    test "an integration item missing its title is diagnosed, not masked", %{plan: plan, spec: spec} do
      malformed = update_in(plan, ["work_items", Access.at(3)], &Map.delete(&1, "title"))

      assert malformed != plan
      assert Plan.validate(malformed, spec) == {:error, %{clause: "invalid_run_plan_shape"}}
    end

    test "a non-string plan_id is diagnosed, not masked", %{plan: plan, spec: spec} do
      assert Plan.validate(Map.put(plan, "plan_id", 3), spec) ==
               {:error, %{clause: "invalid_run_plan_shape"}}
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

  # S3 (NS-25.D.001): the one identity fact the source can decide at admission
  describe "reviewer independence" do
    setup do
      {:ok, plan: F.json("plans", "valid_linear", "plan.json"), spec: F.json("plans", "valid_linear", "spec.json")}
    end

    defp with_agents(spec, agents), do: Map.put(spec, "agents", agents)

    test "distinct writer and reviewer agents are admitted", %{plan: plan, spec: spec} do
      assert {:ok, _validated} = Plan.validate(plan, spec)
    end

    test "the same agent name under both roles is refused with that name", %{plan: plan, spec: spec} do
      spec =
        with_agents(spec, [
          %{"name" => "writer_agent", "role" => "writer"},
          %{"name" => "writer_agent", "role" => "reviewer"}
        ])

      assert Plan.validate(plan, spec) ==
               {:error, %{clause: "reviewer_agent_not_independent", field: "writer_agent"}}
    end

    test "the same agent_id under two names is refused with the writer's name", %{plan: plan, spec: spec} do
      spec =
        with_agents(spec, [
          %{"name" => "writer_agent", "role" => "writer", "agent_id" => "codex_cli_1"},
          %{"name" => "reviewer_agent", "role" => "reviewer", "agent_id" => "codex_cli_1"}
        ])

      assert Plan.validate(plan, spec) ==
               {:error, %{clause: "reviewer_agent_not_independent", field: "writer_agent"}}
    end

    test "distinct agent_ids under distinct names are admitted", %{plan: plan, spec: spec} do
      spec =
        with_agents(spec, [
          %{"name" => "writer_agent", "role" => "writer", "agent_id" => "codex_cli_1"},
          %{"name" => "reviewer_agent", "role" => "reviewer", "agent_id" => "claude_cli_1"}
        ])

      assert {:ok, _validated} = Plan.validate(plan, spec)
    end

    test "a spec with no reviewer role is unaffected", %{plan: plan, spec: spec} do
      [writer | _rest] = plan["work_items"]
      plan = Map.put(plan, "work_items", [writer])
      spec = with_agents(spec, [%{"name" => "writer_agent", "role" => "writer"}])

      assert {:ok, _validated} = Plan.validate(plan, spec)
    end

    test "an integration item is judged too, and a review item's own role is not a writer", %{plan: plan, spec: spec} do
      [writer, reviewer] = plan["work_items"]

      spec =
        with_agents(spec, [%{"name" => "one_agent", "role" => "writer"}, %{"name" => "one_agent", "role" => "reviewer"}])

      integration = Map.put(writer, "kind", "integration")

      assert {:error, %{clause: "reviewer_agent_not_independent", field: "one_agent"}} =
               Plan.validate(Map.put(plan, "work_items", [integration, reviewer]), spec)

      # only the review item remains: nothing writes, so nothing is reviewed by its own writer
      sole_reviewer = Map.put(reviewer, "deps", [])
      assert {:ok, _validated} = Plan.validate(Map.put(plan, "work_items", [sole_reviewer]), spec)
    end

    test "a writer item declared under the reviewer role is refused", %{plan: plan, spec: spec} do
      [writer, reviewer] = plan["work_items"]
      writer = Map.put(writer, "role", "reviewer")

      assert Plan.validate(Map.put(plan, "work_items", [writer, reviewer]), spec) ==
               {:error, %{clause: "reviewer_agent_not_independent", field: "reviewer_agent"}}
    end

    test "the undeclared-role rejection keeps precedence", %{plan: plan, spec: spec} do
      [writer, reviewer] = plan["work_items"]

      spec =
        with_agents(spec, [
          %{"name" => "writer_agent", "role" => "writer"},
          %{"name" => "writer_agent", "role" => "reviewer"}
        ])

      plan = Map.put(plan, "work_items", [Map.put(writer, "role", "nobody"), reviewer])

      assert Plan.validate(plan, spec) == {:error, %{clause: "undeclared_agent_role", field: "nobody"}}
    end

    test "the independence rejection takes precedence over the later allowed-path check", %{plan: plan, spec: spec} do
      [writer, reviewer] = plan["work_items"]

      spec =
        with_agents(spec, [
          %{"name" => "writer_agent", "role" => "writer"},
          %{"name" => "writer_agent", "role" => "reviewer"}
        ])

      plan = Map.put(plan, "work_items", [Map.put(writer, "allowed_paths", ["src"]), reviewer])

      assert Plan.validate(plan, spec) ==
               {:error, %{clause: "reviewer_agent_not_independent", field: "writer_agent"}}
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
