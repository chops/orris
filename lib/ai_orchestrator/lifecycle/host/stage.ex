defmodule AiOrchestrator.Lifecycle.Host.Stage do
  @moduledoc """
  One committed Host stage, handed to whoever owns the effect runtime (the run's worker). The record carries the
  loop AFTER the commit (never a runtime), the pending intent, the IMMUTABLE persisted suffix this commit produced
  (the `Effects.release_terminal/2` input), the receipt the Host selected for a `ReleaseGate`, and the stage's own
  identity. Per-operation correlation refs are minted by the driver, not carried here.
  """

  alias AiOrchestrator.Lifecycle.Host.Loop

  @enforce_keys [:kind, :loop, :suffix, :ref]
  defstruct [:kind, :loop, :intent, :suffix, :receipt, :ref]

  @type t :: %__MODULE__{
          kind: :effect | :done,
          loop: Loop.t(),
          intent: struct() | nil,
          suffix: [map()],
          receipt: map() | nil,
          ref: reference()
        }
end
