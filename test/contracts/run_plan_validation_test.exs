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
end
