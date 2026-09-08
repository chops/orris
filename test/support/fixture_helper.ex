defmodule AiOrchestrator.Contracts.FixtureHelper do
  @moduledoc false

  import ExUnit.Assertions

  @root Path.expand("../fixtures/contracts", __DIR__)

  @doc "Exact persisted bytes of a journal fixture, newline included."
  def raw(class, name), do: @root |> Path.join("#{class}/#{name}/events.jsonl") |> File.read!()

  def lines(class, name) do
    @root |> Path.join("#{class}/#{name}/events.jsonl") |> File.read!() |> String.split("\n", trim: true)
  end

  def json(class, name, file) do
    @root |> Path.join("#{class}/#{name}/#{file}") |> File.read!() |> Jason.decode!()
  end

  def assert_rejection_matches(actual, expected) when is_map(actual) and is_map(expected) do
    actual = stringify_keys(actual)

    assert is_binary(actual["clause"])

    for {key, expected_value} <- expected do
      assert Map.fetch(actual, key) == {:ok, expected_value}
    end
  end

  defp stringify_keys(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end
end
