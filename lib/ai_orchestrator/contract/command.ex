defmodule AiOrchestrator.Contract.Command do
  @moduledoc """
  An authorized command presented to the lifecycle reducer.

  `requested_by` is the durable NS-41 acceptance stamp. `Commands` constructs
  and validates it before this value crosses into the lifecycle core. `now` is
  command-arrival evidence; journal-visible time comes from clock observations.
  """

  alias AiOrchestrator.Contract.Moment

  @enforce_keys [:requested_by, :run_id, :args, :now]
  defstruct [:requested_by, :run_id, :args, :now]

  @type t :: %__MODULE__{
          requested_by: map(),
          run_id: String.t(),
          args: %{optional(String.t()) => String.t()},
          now: Moment.t()
        }
end
