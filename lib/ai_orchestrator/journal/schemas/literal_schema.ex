defmodule AiOrchestrator.Journal.Schemas.LiteralSchema do
  @moduledoc false

  # Compilation checks the actual term before it becomes a module attribute.
  def check!(schema) do
    scan!(schema, [:schema])
    schema
  end

  defp scan!(term, path) when is_function(term) or is_pid(term) or is_reference(term) or is_port(term) do
    raise ArgumentError, "nonliteral schema term at #{inspect(Enum.reverse(path))}"
  end

  defp scan!(%Regex{}, path) do
    raise ArgumentError, "nested Regex violates schema premise at #{inspect(Enum.reverse(path))}"
  end

  defp scan!(term, path) when is_map(term) do
    for {key, value} <- :maps.to_list(term) do
      scan!(key, [:map_key | path])
      scan!(value, [key | path])
    end

    :ok
  end

  defp scan!(term, path) when is_tuple(term) do
    term
    |> Tuple.to_list()
    |> scan_list!(path, 0)
  end

  defp scan!([], _path), do: :ok
  defp scan!([_head | _tail] = list, path), do: scan_list!(list, path, 0)
  defp scan!(_term, _path), do: :ok

  defp scan_list!([], _path, _index), do: :ok

  defp scan_list!([head | tail], path, index) do
    scan!(head, [index | path])
    scan_list!(tail, path, index + 1)
  end

  defp scan_list!(tail, path, index), do: scan!(tail, [{:tail, index} | path])
end
