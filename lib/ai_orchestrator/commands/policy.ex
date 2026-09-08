defmodule AiOrchestrator.Commands.Policy do
  @moduledoc "Closed actor-class policy table for the command API."

  @verbs %{
    "operator" => ~w(start resume resolve_attention repair cancel pause update_context ratify_plan),
    "console" => ~w(start resume resolve_attention repair cancel pause update_context ratify_plan),
    "agent" => ~w(propose_plan propose_context_change),
    "system" => ~w(repair)
  }

  @actor_id_pattern ~r/^[A-Za-z0-9_.-]{1,64}$/
  @agent_id_pattern ~r/^[a-z][a-z0-9_-]{0,31}$/
  @max_value_bytes 4096

  @spec authorize(term(), term()) :: {:ok, map()} | {:error, map()}
  def authorize(%{"class" => class} = actor, verb) when is_binary(class) and is_binary(verb) do
    with :ok <- known_class(class),
         :ok <- actor_shape(actor),
         :ok <- allowed(class, verb) do
      {:ok, actor}
    end
  end

  def authorize(_actor, _verb), do: {:error, %{clause: "invalid_command_actor"}}

  @spec verbs() :: %{String.t() => [String.t()]}
  def verbs, do: @verbs

  defp known_class(class) do
    if Map.has_key?(@verbs, class) do
      :ok
    else
      {:error, %{clause: "unknown_actor_class", class: class}}
    end
  end

  defp actor_shape(%{"class" => class, "id" => id} = actor) when class in ["operator", "console"] do
    exact_actor(actor, ~w(class id), id, @actor_id_pattern)
  end

  defp actor_shape(%{"class" => "agent", "id" => id} = actor) do
    exact_actor(actor, ~w(class id run_id assignment_id), id, @agent_id_pattern)
  end

  defp actor_shape(%{"class" => "system", "id" => id} = actor) do
    exact_actor(actor, ~w(class id reason), id, @actor_id_pattern)
  end

  defp actor_shape(_actor), do: {:error, %{clause: "invalid_command_actor"}}

  defp exact_actor(actor, keys, id, id_pattern) do
    values = Map.take(actor, keys)

    cond do
      Enum.sort(Map.keys(actor)) != Enum.sort(keys) ->
        {:error, %{clause: "command_actor_fields"}}

      not (is_binary(id) and Regex.match?(id_pattern, id)) ->
        {:error, %{clause: "command_actor_id"}}

      Enum.any?(values, fn {_key, value} ->
        not is_binary(value) or value == "" or not String.valid?(value) or byte_size(value) > @max_value_bytes
      end) ->
        {:error, %{clause: "command_actor_value"}}

      true ->
        :ok
    end
  end

  defp allowed(class, verb) do
    case Map.fetch(@verbs, class) do
      {:ok, verbs} ->
        if verb in verbs do
          :ok
        else
          {:error, %{clause: "command_not_authorized", class: class, verb: verb}}
        end

      :error ->
        {:error, %{clause: "unknown_actor_class", class: class}}
    end
  end
end
