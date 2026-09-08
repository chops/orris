defmodule AiOrchestrator.Commands.Idempotency do
  @moduledoc "Compares a retried command with a durable acceptance stamp."

  alias AiOrchestrator.Contract.Command

  @compared_fields ~w(class id verb args_hash)
  @required_fields ["command_id" | @compared_fields]

  @spec compare(map(), Command.t()) :: :match | {:conflict, String.t()} | {:error, map()}
  def compare(%{} = accepted, %Command{requested_by: requested}) do
    cond do
      Enum.any?(@required_fields, &(not is_binary(accepted[&1]))) ->
        {:error, %{clause: "invalid_accepted_command_stamp"}}

      Enum.any?(@required_fields, &(not is_binary(requested[&1]))) ->
        {:error, %{clause: "invalid_requested_command_stamp"}}

      accepted["command_id"] != requested["command_id"] ->
        {:error, %{clause: "command_id_scope_mismatch"}}

      field = Enum.find(@compared_fields, &(accepted[&1] != requested[&1])) ->
        {:conflict, field}

      true ->
        :match
    end
  end

  def compare(_accepted, _command), do: {:error, %{clause: "invalid_idempotency_comparison"}}
end
