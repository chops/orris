defmodule AiOrchestrator.Lifecycle.Core.Diagnostic do
  @moduledoc """
  Compatibility entry point for the payload-free diagnostic normalizer. The implementation lives
  in `AiOrchestrator.Contract.Diagnostic` (Effects extraction: the host-side executors and the
  reducer share one normalizer without an `Effects -> Lifecycle` dependency); both functions
  delegate byte for byte.
  """

  alias AiOrchestrator.Contract.Diagnostic

  defdelegate describe(term), to: Diagnostic
  defdelegate describe_rejection(term), to: Diagnostic
end
