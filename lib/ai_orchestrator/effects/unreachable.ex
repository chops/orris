defmodule AiOrchestrator.Effects.Unreachable do
  @moduledoc "Raised for a contract effect that has no executable path (today: `Effect.Notify`)."

  defexception [:effect]

  @impl true
  def message(%__MODULE__{effect: effect}), do: "effect #{inspect(effect)} has no executable path"
end
