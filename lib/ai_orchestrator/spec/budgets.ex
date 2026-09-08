defmodule AiOrchestrator.Spec.Budgets do
  @moduledoc """
  The typed run-spec budgets section (docs/contracts/typed-budgets-v2.org): a STANDALONE strict schema, used by
  run-spec `schema_version 2` and never by the immutable version 1.

  Every field is optional and, when present, a bounded integer; omission means NOT CONFIGURED and nothing is ever
  injected. An unknown key is arbitrary user content: it is refused as `field "budgets"` without being echoed, and
  no atom is created from input. The section itself must be a map (JSON null included is `not_map`).
  """

  @type typed :: %{optional(String.t()) => non_neg_integer()}
  @type rejection :: %{clause: String.t(), field: String.t(), reason: String.t()}

  # declared order = precedence among known-field violations
  @fields [
    {"max_attempts_default", 1},
    {"restart_attempts", 0},
    {"max_wall_clock_s", 1},
    {"gate_attempts", 1}
  ]
  @known Enum.map(@fields, &elem(&1, 0))

  @doc "The literal field names, in precedence order."
  @spec fields() :: [String.t()]
  def fields, do: @known

  @spec validate(term()) :: {:ok, typed()} | {:error, rejection()}
  def validate(section) when is_map(section) do
    with :ok <- known_keys(section),
         :ok <- bounded_fields(section) do
      {:ok, section}
    end
  end

  def validate(_section), do: {:error, refusal("budgets", "not_map")}

  # any unknown key refuses first; the key is never named (user content) and never coerced. The ACTUAL map keys
  # are inspected through the Map API - never through Enumerable, which a struct could implement (or not) -
  # so a struct or struct-shaped map (atom keys, `__struct__`) is simply a map with unknown keys.
  defp known_keys(section) do
    if section |> Map.keys() |> Enum.all?(&(is_binary(&1) and &1 in @known)),
      do: :ok,
      else: {:error, refusal("budgets", "unknown_key")}
  end

  defp bounded_fields(section) do
    Enum.reduce_while(@fields, :ok, fn {field, minimum}, :ok ->
      case Map.fetch(section, field) do
        :error -> {:cont, :ok}
        {:ok, value} -> bounded(field, minimum, value)
      end
    end)
  end

  defp bounded(_field, minimum, value) when is_integer(value) and value >= minimum, do: {:cont, :ok}
  defp bounded(field, _minimum, value) when is_integer(value), do: {:halt, {:error, refusal(field, "below_minimum")}}
  defp bounded(field, _minimum, nil), do: {:halt, {:error, refusal(field, "null")}}
  defp bounded(field, _minimum, _value), do: {:halt, {:error, refusal(field, "not_integer")}}

  defp refusal(field, reason), do: %{clause: "budget_invalid", field: field, reason: reason}
end
