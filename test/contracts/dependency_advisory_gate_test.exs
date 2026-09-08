defmodule AiOrchestrator.DependencyAdvisoryGateTest do
  use ExUnit.Case, async: true

  @verify Path.expand("../../bin/verify", __DIR__)

  test "verification fails closed through both dependency advisory sources" do
    lines =
      @verify
      |> File.read!()
      |> String.split("\n")
      |> Enum.map(&String.trim/1)

    deps_get = command_index!(lines, "mix deps.get --check-locked")
    hex_audit = command_index!(lines, "mix hex.audit")
    compile = command_index!(lines, "mix compile --warnings-as-errors")
    deps_audit = command_index!(lines, "mix deps.audit")

    assert deps_get < hex_audit
    assert hex_audit < compile
    assert compile < deps_audit
  end

  defp command_index!(lines, command) do
    assert Enum.count(lines, &(&1 == command)) == 1
    Enum.find_index(lines, &(&1 == command))
  end
end
