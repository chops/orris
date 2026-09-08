defmodule AiOrchestrator.Config.GateGuardianResolutionTest do
  @moduledoc """
  C4 RED (M6): native helper resolution precedence through Config.Runtime: flag, then the
  environment, then the local config file, else nil; a nil or non-executable helper is refused
  at PrepareGate (tested in the wiring suite), never a fallback to Gate.Runner.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Config.Runtime

  defp resolve(opts), do: Runtime.resolve(Keyword.merge([env: %{}, file_reader: fn _ -> {:error, :enoent} end], opts))

  test "absent everywhere resolves to nil, never a default path" do
    assert {:ok, %{gate_guardian: nil}} = resolve([])
  end

  test "the flag wins over the environment, which wins over the file" do
    env = %{"AI_ORCHESTRATOR_GATE_GUARDIAN" => "/from/env"}
    file = fn _ -> {:ok, Jason.encode!(%{"gate_guardian" => "/from/file"})} end
    assert {:ok, %{gate_guardian: "/from/flag"}} = resolve(gate_guardian: "/from/flag", env: env, file_reader: file)
    assert {:ok, %{gate_guardian: "/from/env"}} = resolve(env: env, file_reader: file)
    assert {:ok, %{gate_guardian: "/from/file"}} = resolve(file_reader: file)
  end

  test "a relative or empty helper path is refused at resolution" do
    assert {:error, _} = resolve(gate_guardian: "relative/gate_guardian")
    assert {:error, _} = resolve(env: %{"AI_ORCHESTRATOR_GATE_GUARDIAN" => ""})
  end
end
