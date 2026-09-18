defmodule AiOrchestrator.Spec.PlanPathBoundaryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Spec.Plan

  # plan admission is the existing check point for allowed paths: the pure layer of the containment
  # rule (NS-20.D.001) runs there with the plan's rejection vocabulary
  setup do
    plan = F.json("plans", "valid_linear", "plan.json")
    spec = F.json("plans", "valid_linear", "spec.json")
    {:ok, plan: plan, spec: spec}
  end

  defp with_writer_paths(plan, paths) do
    [writer | rest] = plan["work_items"]
    Map.put(plan, "work_items", [Map.put(writer, "allowed_paths", paths) | rest])
  end

  test "a .. segment in a writer's allowed path is refused with the path named", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_paths(plan, ["lib", "lib/../test/x"]), spec) ==
             {:error, %{clause: "work_item_path_traversal", field: "lib/../test/x"}}
  end

  test "an absolute allowed path is refused even under an allowed root's name", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_paths(plan, ["/lib/x"]), spec) ==
             {:error, %{clause: "work_item_path_absolute", field: "/lib/x"}}
  end

  test "a path outside every allowed root keeps the historical clause", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_paths(plan, ["lib", "src"]), spec) ==
             {:error, %{clause: "work_item_paths_outside_roots", field: "src"}}
  end

  test "dotted and slashed spellings of an in-root path are admitted", %{plan: plan, spec: spec} do
    assert {:ok, _validated} = Plan.validate(with_writer_paths(plan, ["./lib/x", "lib//y/", "test"]), spec)
  end

  test "the writer paths the rule judges are the writer kinds' string entries only", %{plan: plan} do
    assert Plan.writer_allowed_paths(plan) == ["lib"]
    assert Plan.writer_allowed_paths(with_writer_paths(plan, ["lib", 7, "test"])) == ["lib", "test"]
    assert Plan.writer_allowed_paths(%{"work_items" => "not a list"}) == []
  end

  defp with_writer_artifacts(plan, artifacts) do
    [writer | rest] = plan["work_items"]
    Map.put(plan, "work_items", [Map.put(writer, "expected_artifacts", artifacts) | rest])
  end

  defp with_review_artifacts(plan, artifacts) do
    [writer, reviewer] = plan["work_items"]
    Map.put(plan, "work_items", [writer, Map.put(reviewer, "expected_artifacts", artifacts)])
  end

  # S1 (NS-20.D.001): the declared artifact is the path observation joins to repo_root and reads,
  # so it is judged by the same pure layer, with the same vocabulary, as allowed_paths
  test "an absolute expected artifact is refused with the path named", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_artifacts(plan, ["/etc/item_a.ex"]), spec) ==
             {:error, %{clause: "work_item_path_absolute", field: "/etc/item_a.ex"}}
  end

  test "a .. segment in an expected artifact is refused", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_artifacts(plan, ["lib/../../outside/x.ex"]), spec) ==
             {:error, %{clause: "work_item_path_traversal", field: "lib/../../outside/x.ex"}}
  end

  test "a writer's expected artifact outside every allowed root is refused", %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_artifacts(plan, ["docs/item_a.ex"]), spec) ==
             {:error, %{clause: "work_item_paths_outside_roots", field: "docs/item_a.ex"}}
  end

  test "an integration item's expected artifact is judged by the writer rule too", %{plan: plan, spec: spec} do
    [writer | rest] = plan["work_items"]
    writer = writer |> Map.put("kind", "integration") |> Map.put("expected_artifacts", ["docs/merged.ex"])

    assert Plan.validate(Map.put(plan, "work_items", [writer | rest]), spec) ==
             {:error, %{clause: "work_item_paths_outside_roots", field: "docs/merged.ex"}}
  end

  # the roots rule is the writer's; a declared review item names its own document, exactly as the
  # reducer-synthesised review item does, and that document need not lie under a writer's root
  test "a review item's expected artifact outside the allowed roots is admitted", %{plan: plan, spec: spec} do
    assert {:ok, _validated} = Plan.validate(with_review_artifacts(plan, ["docs/deep/review.org"]), spec)
  end

  test "a review item's expected artifact is still judged by form", %{plan: plan, spec: spec} do
    assert Plan.validate(with_review_artifacts(plan, ["/etc/review.org"]), spec) ==
             {:error, %{clause: "work_item_path_absolute", field: "/etc/review.org"}}

    assert Plan.validate(with_review_artifacts(plan, ["../review.org"]), spec) ==
             {:error, %{clause: "work_item_path_traversal", field: "../review.org"}}
  end

  test "dotted and slashed spellings of an in-root artifact are admitted", %{plan: plan, spec: spec} do
    assert {:ok, _validated} = Plan.validate(with_writer_artifacts(plan, ["./lib/item_a.ex"]), spec)
    assert {:ok, _validated} = Plan.validate(with_writer_artifacts(plan, ["lib//nested/item_a.ex"]), spec)
    assert {:ok, _validated} = Plan.validate(with_writer_artifacts(plan, ["test"]), spec)
  end

  test "form is judged before the roots rule, so an absolute writer artifact is never outside_roots",
       %{plan: plan, spec: spec} do
    assert Plan.validate(with_writer_artifacts(plan, ["/lib/item_a.ex"]), spec) ==
             {:error, %{clause: "work_item_path_absolute", field: "/lib/item_a.ex"}}
  end

  test "the artifact cardinality pass answers before the artifact path pass", %{plan: plan, spec: spec} do
    assert {:error, %{clause: "expected_artifact_cardinality", field: "item_a"}} =
             Plan.validate(with_writer_artifacts(plan, ["/etc/x", "/etc/y"]), spec)

    assert {:error, %{clause: "expected_artifact_cardinality", field: "item_a"}} =
             Plan.validate(with_writer_artifacts(plan, []), spec)
  end

  test "malformed artifact entries still defer to the existing shape check", %{plan: plan, spec: spec} do
    [writer | rest] = plan["work_items"]

    for artifacts <- [nil, "lib/item_a.ex", %{}, [1], [nil]] do
      invalid = Map.put(plan, "work_items", [Map.put(writer, "expected_artifacts", artifacts) | rest])
      assert {:error, %{clause: "invalid_run_plan_shape"}} = Plan.validate(invalid, spec)
    end
  end

  test "earlier timeout, duplicate-id and missing-dependency rejections keep precedence over the artifact path" do
    for name <- ["invalid_nonpositive_timeout", "invalid_duplicate_ids", "invalid_missing_dep"] do
      plan = F.json("plans", name, "plan.json")
      spec = F.json("plans", name, "spec.json")
      expected = F.json("plans", name, "expected_rejection.json")
      plan = with_writer_artifacts(plan, ["/etc/escape.ex"])

      assert {:error, rejection} = Plan.validate(plan, spec)
      F.assert_rejection_matches(rejection, expected)
    end
  end

  test "the artifact path rejection takes precedence over the later gate, role and allowed-path checks",
       %{plan: plan} do
    spec = F.json("plans", "valid_linear", "spec.json")
    [writer | rest] = plan["work_items"]

    writer =
      writer
      |> Map.put("expected_artifacts", ["/etc/escape.ex"])
      |> Map.put("acceptance", ["not_a_gate"])
      |> Map.put("role", "undeclared_role")
      |> Map.put("allowed_paths", ["src"])

    assert Plan.validate(Map.put(plan, "work_items", [writer | rest]), spec) ==
             {:error, %{clause: "work_item_path_absolute", field: "/etc/escape.ex"}}
  end

  test "the mapping to the plan vocabulary is total over the boundary's clauses" do
    assert Plan.path_rejection(%{clause: "path_outside_roots", path: "p"}) ==
             %{clause: "work_item_paths_outside_roots", field: "p"}

    assert Plan.path_rejection(%{clause: "path_symlink_escape", path: "p"}) ==
             %{clause: "work_item_path_symlink_escape", field: "p"}
  end
end
