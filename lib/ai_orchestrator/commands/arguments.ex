defmodule AiOrchestrator.Commands.Arguments do
  @moduledoc "Validation and ARGS-CANON-1 encoding for command argument documents."

  @fields %{
    "start" => ~w(spec_hash plan_hash),
    "resume" => ~w(recovery_reason),
    "resolve_attention" => ~w(attention_ids),
    "repair" => ~w(kind detail_hash),
    "cancel" => ~w(reason),
    "pause" => ~w(reason),
    "update_context" => ~w(patch_hash),
    "propose_context_change" => ~w(patch_hash),
    "propose_plan" => ~w(plan_hash),
    "ratify_plan" => ~w(plan_hash)
  }

  @hash_pattern ~r/^sha256:[0-9a-f]{64}$/
  @attention_pattern ~r/^[A-Za-z0-9_-]+(?:,[A-Za-z0-9_-]+)*$/
  @repair_kinds ~w(tail_truncate claim_reconcile)
  @max_value_bytes 4096

  @spec validate(String.t(), term()) :: {:ok, map()} | {:error, map()}
  def validate(verb, args) when is_binary(verb) and is_map(args) do
    with {:ok, fields} <- fields(verb),
         :ok <- exact_fields(args, fields),
         :ok <- string_values(args),
         :ok <- semantic_values(verb, args) do
      {:ok, args}
    end
  end

  def validate(_verb, _args), do: {:error, %{clause: "invalid_command_arguments"}}

  @spec bytes(String.t(), map()) :: binary()
  def bytes(verb, args) when is_binary(verb) and is_map(args) do
    pairs =
      args |> Enum.sort_by(fn {key, _value} -> key end) |> Enum.map(fn {key, value} -> field(key) <> field(value) end)

    IO.iodata_to_binary(["ARGS-CANON-1\n", field(verb) | pairs])
  end

  @spec hash(String.t(), map()) :: String.t()
  def hash(verb, args) do
    "sha256:" <> (:sha256 |> :crypto.hash(bytes(verb, args)) |> Base.encode16(case: :lower))
  end

  @spec fields() :: %{String.t() => [String.t()]}
  def fields, do: @fields

  defp fields(verb) do
    case Map.fetch(@fields, verb) do
      {:ok, fields} -> {:ok, fields}
      :error -> {:error, %{clause: "unknown_command_verb", verb: verb}}
    end
  end

  defp exact_fields(args, fields) do
    actual = args |> Map.keys() |> Enum.sort()
    expected = Enum.sort(fields)

    if actual == expected do
      :ok
    else
      {:error, %{clause: "command_argument_fields", expected: expected, actual: actual}}
    end
  end

  defp string_values(args) do
    case Enum.find(args, fn {_key, value} ->
           not is_binary(value) or value == "" or not String.valid?(value) or byte_size(value) > @max_value_bytes
         end) do
      nil -> :ok
      {key, _value} -> {:error, %{clause: "command_argument_value", field: key}}
    end
  end

  defp semantic_values("repair", %{"kind" => kind}) when kind not in @repair_kinds do
    {:error, %{clause: "repair_kind", field: "kind"}}
  end

  defp semantic_values("resolve_attention", %{"attention_ids" => ids}) do
    if Regex.match?(@attention_pattern, ids) do
      :ok
    else
      {:error, %{clause: "attention_ids", field: "attention_ids"}}
    end
  end

  defp semantic_values(_verb, args) do
    case Enum.find(args, fn {key, value} -> String.ends_with?(key, "_hash") and not Regex.match?(@hash_pattern, value) end) do
      nil -> :ok
      {key, _value} -> {:error, %{clause: "command_argument_hash", field: key}}
    end
  end

  defp field(value), do: <<byte_size(value)::32-big>> <> value
end
