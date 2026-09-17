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

  test "the mapping to the plan vocabulary is total over the boundary's clauses" do
    assert Plan.path_rejection(%{clause: "path_outside_roots", path: "p"}) ==
             %{clause: "work_item_paths_outside_roots", field: "p"}

    assert Plan.path_rejection(%{clause: "path_symlink_escape", path: "p"}) ==
             %{clause: "work_item_path_symlink_escape", field: "p"}
  end
end
