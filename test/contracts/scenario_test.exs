defmodule AiOrchestrator.Contracts.ScenarioTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Spec.RunSpec

  @scenarios [
    "gated_run_seed",
    "auth_blocked_pane",
    "gate_failure_summary_feedback",
    "fingerprint_drift",
    "concurrency_cap"
  ]

  for name <- @scenarios do
    test "#{name}: spec and plan validate, journal folds to expected outcome" do
      name = unquote(name)
      spec = F.json("scenarios", name, "spec.json")
      plan = F.json("scenarios", name, "plan.json")
      expected = F.json("scenarios", name, "expected.json")

      assert {:ok, validated_spec} = RunSpec.validate(spec)
      assert {:ok, _validated_plan} = Plan.validate(plan, validated_spec)
      assert {:ok, state} = Fold.fold_lines(F.lines("scenarios", name))
      assert Fold.summary(state) == expected
    end
  end

  for window <- ["pre_dispatch", "awaiting_artifact", "pre_gate"] do
    test "kill9_resume/#{window}: truncated journal folds to a resumable state" do
      window = unquote(window)
      spec = F.json("scenarios", "kill9_resume", "spec.json")
      plan = F.json("scenarios", "kill9_resume", "plan.json")
      expected = F.json("scenarios", "kill9_resume", "expected_#{window}.json")

      lines =
        "scenarios/kill9_resume/events_#{window}.jsonl"
        |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
        |> File.read!()
        |> String.split("\n", trim: true)

      assert {:ok, validated_spec} = RunSpec.validate(spec)
      assert {:ok, _validated_plan} = Plan.validate(plan, validated_spec)
      assert {:ok, state} = Fold.fold_lines(lines)
      assert Fold.summary(state) == expected
    end
  end
end
