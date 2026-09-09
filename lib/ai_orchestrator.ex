defmodule AiOrchestrator do
  @moduledoc """
  Local-first run supervisor for ai-pair backed agent workflows.
  """

  # the public seam an external application may use (docs/contracts/public-console-seam.org)
  use Boundary, deps: [], exports: [Commands, Prepare, Query]

  @doc """
  Returns the application version.
  """
  @spec version() :: String.t()
  def version do
    :ai_orchestrator |> Application.spec(:vsn) |> to_string()
  end
end
