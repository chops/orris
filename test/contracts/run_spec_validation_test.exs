defmodule AiOrchestrator.Contracts.RunSpecValidationTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Spec.RunSpec

  for name <- ["valid_minimal", "valid_full"] do
    test "#{name}: spec validates" do
      spec = F.json("run_specs", unquote(name), "spec.json")
      assert {:ok, validated} = RunSpec.validate(spec)
      assert is_map(validated)
    end
  end

  for name <- [
        "invalid_missing_gate",
        "invalid_agent_grammar",
        "invalid_reserved_agent",
        "invalid_oracle_freeform",
        "invalid_schema_version",
        "invalid_allowed_roots_escape"
      ] do
    test "#{name}: spec rejected with the named clause" do
      name = unquote(name)
      spec = F.json("run_specs", name, "spec.json")
      expected = F.json("run_specs", name, "expected_rejection.json")
      assert {:error, rejection} = RunSpec.validate(spec)
      F.assert_rejection_matches(rejection, expected)
    end
  end

  test "pane_hint pane_ref is explicit and nonblank when pane_hint is present" do
    spec =
      "run_specs"
      |> F.json("valid_minimal", "spec.json")
      |> put_in(["agents", Access.at(0), "pane_hint"], %{"pane_ref" => " "})

    assert {:error, %{clause: "pane_ref_blank", field: "pane_hint.pane_ref"}} = RunSpec.validate(spec)
  end
end
