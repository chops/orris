defmodule AiOrchestrator.Contracts.FoldTypeCollector do
  @moduledoc """
  Test-support helper: the event types a fold source names semantically.

  Collected positions are exactly three: the value of a `"type"` key inside a map
  pattern or literal; the right-hand side of `==` whose left-hand side is the
  variable `type`; and the members of a list on the right of `in` whose left-hand
  side is the variable `type`. Comparisons of other variables (`status`,
  `disposition`, `role`, ...) against event-name-looking strings are ignored, as are
  doc strings, module attributes, and free literals.
  """

  @spec named_types_from_source(Path.t()) :: MapSet.t(String.t())
  def named_types_from_source(path) do
    path
    |> File.read!()
    |> Code.string_to_quoted!(file: path)
    |> named_types_from_ast()
  end

  @spec named_types_from_string(String.t()) :: MapSet.t(String.t())
  def named_types_from_string(source) when is_binary(source) do
    source |> Code.string_to_quoted!() |> named_types_from_ast()
  end

  @spec named_types_from_ast(Macro.t()) :: MapSet.t(String.t())
  def named_types_from_ast(ast) do
    ast
    |> Macro.prewalk(MapSet.new(), fn
      {:%{}, _meta, pairs} = node, acc when is_list(pairs) ->
        {node,
         Enum.reduce(pairs, acc, fn
           {"type", value}, acc when is_binary(value) -> MapSet.put(acc, value)
           _pair, acc -> acc
         end)}

      {:==, _meta, [{:type, _, ctx}, rhs]} = node, acc when is_atom(ctx) and is_binary(rhs) ->
        {node, MapSet.put(acc, rhs)}

      {:in, _meta, [{:type, _, ctx}, list]} = node, acc when is_atom(ctx) and is_list(list) ->
        {node, Enum.reduce(list, acc, fn v, acc -> if is_binary(v), do: MapSet.put(acc, v), else: acc end)}

      node, acc ->
        {node, acc}
    end)
    |> elem(1)
  end
end
