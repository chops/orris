defmodule AiOrchestrator.Spec.TypedBudgetsGreenReviewTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Spec.Budgets
  alias AiOrchestrator.Spec.RunSpec

  @canary "TYPED-BUDGET-STRUCT-PRIVATE-CANARY"
  @refusal {:error, %{clause: "budget_invalid", field: "budgets", reason: "unknown_key"}}
  @v2 "test/fixtures/contracts/run_specs/valid_minimal/spec.json"
      |> File.read!()
      |> Jason.decode!()
      |> Map.put("schema_version", 2)

  test "BG-1 non-enumerable struct is refused without invoking its protocol" do
    assert Budgets.validate(%URI{path: @canary}) == @refusal
  end

  test "BG-2 enumerable struct cannot masquerade as an empty budget map" do
    assert Budgets.validate(MapSet.new()) == @refusal
  end

  test "BG-3 RunSpec validation preserves the standalone closed struct refusal" do
    assert RunSpec.validate(Map.put(@v2, "budgets", %URI{path: @canary})) == @refusal
  end

  test "BG-4 accessor never labels an enumerable struct typed" do
    assert RunSpec.budgets(%{"schema_version" => 2, "budgets" => MapSet.new()}) == @refusal
  end
end
