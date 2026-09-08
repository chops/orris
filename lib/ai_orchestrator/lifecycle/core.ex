defmodule AiOrchestrator.Lifecycle.Core do
  @moduledoc """
  The pure execution core (Gate C): a strict sub-boundary that may depend only on
  the journal, the spec contracts, and the pure projection renderer. No effect
  adapter, clock, id seam, process, or file is reachable from here; the reducer
  returns events and intents, and the host executes them.
  """

  use Boundary,
    type: :strict,
    deps: [AiOrchestrator.Contract, AiOrchestrator.Journal, AiOrchestrator.Spec],
    exports: [Diagnostic, Reducer]
end
