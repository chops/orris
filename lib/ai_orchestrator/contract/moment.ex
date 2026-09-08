defmodule AiOrchestrator.Contract.Moment do
  @moduledoc """
  Wall-clock facts supplied to the reducer with an input.

  Monotonic time remains private to effect executors because it is used for
  durations and must never become journal truth.
  """

  @enforce_keys [:wall_ts, :unix]
  defstruct [:wall_ts, :unix]

  @type t :: %__MODULE__{wall_ts: String.t(), unix: integer()}
end
