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

  @doc """
  The same namings, grouped by the enclosing top-level `def`/`defp` name.

  Gate A's fold check can only ask WHETHER a fold source names a type. Where it names it
  decides what the naming means: an `apply_type_update/2` clause returns a new `Fold.State`,
  while a membership predicate such as `blocks_on_attention?/1` or `cleanup_event?/1`
  classifies an event without transitioning anything. A type named only in the second kind
  satisfies the naming check with no disposition at all.
  """
  @spec named_types_by_function(Path.t()) :: %{String.t() => MapSet.t(atom())}
  def named_types_by_function(path) do
    path |> File.read!() |> Code.string_to_quoted!(file: path) |> participation_from_ast()
  end

  @spec participation_from_string(String.t()) :: %{String.t() => MapSet.t(atom())}
  def participation_from_string(source) when is_binary(source) do
    source |> Code.string_to_quoted!() |> participation_from_ast()
  end

  @spec participation_from_ast(Macro.t()) :: %{String.t() => MapSet.t(atom())}
  def participation_from_ast(ast) do
    ast
    |> Macro.prewalker()
    |> Enum.reduce(%{}, &merge_definition/2)
  end

  defp merge_definition(node, acc) do
    case definition_name(node) do
      nil -> acc
      name -> Enum.reduce(named_types_from_ast(node), acc, &record_naming(&1, &2, name))
    end
  end

  defp record_naming(type, acc, name), do: Map.update(acc, type, MapSet.new([name]), &MapSet.put(&1, name))

  defp definition_name({kind, _meta, [head | _rest]}) when kind in [:def, :defp], do: head_name(head)
  defp definition_name(_node), do: nil

  defp head_name({:when, _meta, [head | _guards]}), do: head_name(head)
  defp head_name({name, _meta, args}) when is_atom(name) and is_list(args), do: name
  defp head_name({name, _meta, nil}) when is_atom(name), do: name
  defp head_name(_other), do: nil

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
