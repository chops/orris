defmodule AiOrchestrator.Lifecycle.Host.Loop do
  @moduledoc """
  One in-flight Host loop: the reducer's pending outcome, the committed journal so far, the
  effect runtime and the resolved options. Opaque to callers: `AiOrchestrator.Lifecycle.Host.advance/1`
  is the only way to move it.
  """

  alias AiOrchestrator.Effects.Runtime

  @enforce_keys [:step, :committed, :runtime, :opts]
  defstruct [:step, :committed, :runtime, :opts]

  @type t :: %__MODULE__{step: term(), committed: [map()], runtime: Runtime.t(), opts: keyword()}
end
