defmodule AiOrchestrator.Journal.SchemaCacheWriteRegressionTest do
  @moduledoc """
  Static regression guard for direct persistent_term writes in schema modules.

  This is not runtime or transitive purity evidence, and it does not prohibit reads.
  It detects literal remote put/2 and erase/1 calls, including nested calls; it does
  not resolve apply, computed module atoms, imports, aliases or captured functions.
  Independent source review remains required for those forms and downstream calls.
  """

  use ExUnit.Case, async: true

  @sources [
    {"../../lib/ai_orchestrator/journal/event.ex", AiOrchestrator.Journal.Event},
    {"../../lib/ai_orchestrator/journal/schemas/event_data.ex", AiOrchestrator.Journal.Schemas.EventData},
    {"../../lib/ai_orchestrator/journal/schemas/event_data_schemas.ex", AiOrchestrator.Journal.Schemas.EventDataSchemas},
    {"../../lib/ai_orchestrator/journal/schemas/literal_schema.ex", AiOrchestrator.Journal.Schemas.LiteralSchema}
  ]

  test "schema modules contain no direct persistent_term cache writes" do
    for {relative, module} <- @sources do
      path = Path.expand(relative, __DIR__)
      ast = path |> File.read!() |> Code.string_to_quoted!()
      {:defmodule, _, [{:__aliases__, _, parts}, _body]} = ast
      assert Module.concat(parts) == module, path
      assert direct_writes(ast) == [], path
    end
  end

  test "the detector reports put and erase through the same nested AST traversal" do
    assert direct_writes(quote(do: :persistent_term.put(:key, :value))) == [{:put, 2}]
    assert direct_writes(quote(do: :persistent_term.erase(:key))) == [{:erase, 1}]

    nested =
      quote do
        case :miss do
          :miss -> {:ok, :persistent_term.put(:key, :value)}
          :hit -> :persistent_term.erase(:key)
        end
      end

    assert Enum.sort(direct_writes(nested)) == [{:erase, 1}, {:put, 2}]
  end

  test "reads and text mentioning writes do not count as direct writes" do
    assert direct_writes(quote(do: :persistent_term.get(:key, nil))) == []
    assert direct_writes(quote(do: ":persistent_term.put(:key, :value)")) == []
  end

  defp direct_writes(ast) do
    {_ast, writes} =
      Macro.prewalk(ast, [], fn
        {{:., _, [:persistent_term, function]}, _, arguments} = node, writes
        when is_list(arguments) ->
          call = {function, length(arguments)}
          if call in [{:put, 2}, {:erase, 1}], do: {node, [call | writes]}, else: {node, writes}

        node, writes ->
          {node, writes}
      end)

    Enum.reverse(writes)
  end
end
