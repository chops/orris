defmodule AiOrchestrator.Projection.RunContext do
  @moduledoc false

  alias AiOrchestrator.Journal.Fold

  @doc """
  Renders the shared-context worldview as an org-mode document.

  The bytes are produced by `Journal.Fold.Context`, the same pure function the
  execution core hashes into prompts, so the read model can never diverge from
  what a run actually saw. The journal remains the only truth; this projection
  is deletable and rebuilt by folding (EC-9 / EJ-10).
  """
  @spec render(Fold.State.t()) :: String.t()
  defdelegate render(state), to: Fold.Context
end
