defmodule AiOrchestrator.Spec.CanonicalHashTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Spec.RunSpec

  defp sha256(bytes), do: "sha256:" <> (:sha256 |> :crypto.hash(bytes) |> Base.encode16(case: :lower))

  test "context_initial_hash is sha256 over the JSON encoding, or over the empty string when absent" do
    context = %{"b" => [1, 2], "a" => "x"}
    assert Plan.context_initial_hash(%{"context_initial" => context}) == sha256(Jason.encode!(context))
    assert Plan.context_initial_hash(%{}) == sha256("")
    assert Plan.context_initial_hash(%{}) == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  end

  test "gate_json embeds the gate definition and [] for an unknown gate" do
    spec = %{"gates" => %{"test" => %{"command_argv" => ["mix", "test"], "timeout_s" => 60}}}
    assert RunSpec.gate_json(spec, "test") == Jason.encode!(spec["gates"]["test"])
    assert RunSpec.gate_json(spec, "missing") == "[]"
    assert RunSpec.gate_json(%{}, "test") == "[]"
  end
end
