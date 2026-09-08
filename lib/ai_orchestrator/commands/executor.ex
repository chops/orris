defmodule AiOrchestrator.Commands.Executor do
  @moduledoc "Execution port implemented by a live or locked-offline run host."

  alias AiOrchestrator.Contract.Command

  @callback execute(Command.t(), keyword()) :: {:ok, map()} | {:error, map()}
end
