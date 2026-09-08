defmodule AiOrchestrator.Journal.Schemas.RequestedBy do
  @moduledoc """
  Typed command-authorship stamp shared by every journal event producer.

  The envelope actor remains `run_supervisor`; this schema records the command
  authority that caused a command-originated event.
  """

  @operator_verbs ~w(start resume resolve_attention repair cancel pause update_context ratify_plan)
  @agent_verbs ~w(propose_plan propose_context_change)
  @system_verbs ~w(repair)

  @command_id_pattern ~r/^[A-Za-z0-9_-]{16,64}$/
  @hash_pattern ~r/^sha256:[0-9a-f]{64}$/
  @actor_id_pattern ~r/^[A-Za-z0-9_.-]{1,64}$/
  @agent_id_pattern ~r/^[a-z][a-z0-9_-]{0,31}$/
  @max_value_bytes 4_096

  @spec schema() :: Zoi.schema()
  def schema do
    Zoi.discriminated_union("class", [
      variant("operator", @operator_verbs, %{}),
      variant("console", @operator_verbs, %{}),
      variant("agent", @agent_verbs, %{
        "run_id" => nonempty_string(),
        "assignment_id" => nonempty_string()
      }),
      variant("system", @system_verbs, %{"reason" => nonempty_string()})
    ])
  end

  @spec parse(term()) :: Zoi.result()
  def parse(value), do: Zoi.parse(schema(), value)

  @doc "Validates append-only bounds without changing the historical read schema."
  @spec validate_append_bounds(map()) :: :ok | {:error, map()}
  def validate_append_bounds(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {field, _value} -> field end)
    |> Enum.reduce_while(:ok, fn
      {field, string}, :ok when is_binary(string) ->
        cond do
          not String.valid?(string) ->
            {:halt, {:error, %{clause: "requested_by_invalid_utf8", field: field}}}

          byte_size(string) > @max_value_bytes ->
            {:halt,
             {:error,
              %{
                clause: "requested_by_value_too_large",
                field: field,
                bytes: byte_size(string),
                max_bytes: @max_value_bytes
              }}}

          true ->
            {:cont, :ok}
        end

      _field_and_value, :ok ->
        {:cont, :ok}
    end)
  end

  @doc "Returns the command verbs admitted by each actor-class variant."
  @spec verbs() :: %{String.t() => [String.t()]}
  def verbs do
    %{
      "operator" => @operator_verbs,
      "console" => @operator_verbs,
      "agent" => @agent_verbs,
      "system" => @system_verbs
    }
  end

  defp variant(class, verbs, extra_fields) do
    id_pattern = if class == "agent", do: @agent_id_pattern, else: @actor_id_pattern

    Zoi.map(
      Map.merge(
        %{
          "class" => Zoi.literal(class),
          "id" => Zoi.regex(Zoi.string(), id_pattern),
          "command_id" => Zoi.regex(Zoi.string(), @command_id_pattern),
          "verb" => Zoi.enum(verbs),
          "args_hash" => Zoi.regex(Zoi.string(), @hash_pattern)
        },
        extra_fields
      ),
      unrecognized_keys: :error
    )
  end

  defp nonempty_string, do: Zoi.min(Zoi.string(), 1)
end
