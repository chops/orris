defmodule AiOrchestrator do
  @moduledoc """
  Local-first run supervisor for ai-pair backed agent workflows.
  """

  use Boundary, deps: [], exports: []

  @doc """
  Returns the application version.
  """
  @spec version() :: String.t()
  def version do
    :ai_orchestrator |> Application.spec(:vsn) |> to_string()
  end
end
