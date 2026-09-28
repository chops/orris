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

  # NS-07.A.001: each closed RunSpec map refuses one unknown key, independently, with the shape
  # clause; the unmutated spec at the same version validates first, so each refusal is the key's.
  for version <- [1, 2] do
    test "version #{version}: an unknown key is refused at every closed RunSpec map" do
      spec = full_spec(unquote(version))
      assert {:ok, _validated} = RunSpec.validate(spec)

      for {site, mutated} <- with_unknown_key(spec) do
        assert RunSpec.validate(mutated) == {:error, %{clause: "invalid_run_spec_shape"}},
               "#{site}: an unknown key was not refused"
      end
    end
  end

  defp full_spec(1), do: F.json("run_specs", "valid_full", "spec.json")
  defp full_spec(2), do: 1 |> full_spec() |> Map.put("schema_version", 2) |> Map.delete("budgets")

  defp with_unknown_key(spec) do
    [
      {"top level", Map.put(spec, "unexpected", true)},
      {"agent", put_in(spec, ["agents", Access.at(0), "unexpected"], true)},
      {"pane_hint", put_in(spec, ["agents", Access.at(0), "pane_hint", "unexpected"], true)}
    ]
  end
end
