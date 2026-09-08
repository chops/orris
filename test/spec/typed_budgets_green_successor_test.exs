defmodule AiOrchestrator.Spec.TypedBudgetsGreenSuccessorTest do
  @moduledoc """
  Companions to Codex's green-review probes (BG-M1): the section's ACTUAL keys are inspected, never a struct's
  Enumerable implementation, so a forged `__struct__` map, a non-empty enumerable struct, and a struct nested in
  a run spec are all the closed unknown-key refusal - with no protocol error and no canary echo.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Spec.Budgets
  alias AiOrchestrator.Spec.RunSpec

  @canary "TYPED-BUDGETS-STRUCT-SUCCESSOR-CANARY"
  @refusal {:error, %{clause: "budget_invalid", field: "budgets", reason: "unknown_key"}}
  @v2 "test/fixtures/contracts/run_specs/valid_minimal/spec.json"
      |> File.read!()
      |> Jason.decode!()
      |> Map.put("schema_version", 2)

  test "a forged __struct__ map (no real struct, no Enumerable) is unknown_key" do
    assert Budgets.validate(%{"restart_attempts" => 0, __struct__: Module.concat(["Forged", "Budgets"])}) == @refusal
  end

  test "a NON-empty enumerable struct is unknown_key, and its contents are never echoed" do
    actual = Budgets.validate(MapSet.new([@canary]))
    assert actual == @refusal
    refute inspect(actual, limit: :infinity) =~ @canary
  end

  test "a struct with only atom keys and a struct-shaped map with a canary VALUE both refuse without echo" do
    for section <- [%Version{major: 1, minor: 0, patch: 0}, %{__struct__: URI, path: @canary}] do
      actual = Budgets.validate(section)
      assert actual == @refusal
      refute inspect(actual, limit: :infinity) =~ @canary
    end
  end

  test "through RunSpec and the accessor: an enumerable struct with contents is refused, never typed" do
    assert RunSpec.validate(Map.put(@v2, "budgets", MapSet.new([1]))) == @refusal
    assert RunSpec.budgets(%{"schema_version" => 2, "budgets" => MapSet.new([1])}) == @refusal
    assert RunSpec.budgets(%{"schema_version" => 2, "budgets" => %URI{path: @canary}}) == @refusal
  end

  test "a plain map with the four known fields is still typed (the fix admits nothing new)" do
    budgets = %{"max_attempts_default" => 1, "restart_attempts" => 0, "max_wall_clock_s" => 1, "gate_attempts" => 1}
    assert Budgets.validate(budgets) == {:ok, budgets}
    assert RunSpec.budgets(%{"schema_version" => 2, "budgets" => budgets}) == {:typed, budgets}
  end
end
