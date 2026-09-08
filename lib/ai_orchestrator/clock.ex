defmodule AiOrchestrator.Clock do
  @moduledoc """
  Determinism seam for time (MC-12): wall-clock facts for the journal,
  monotonic time for durations. Domain code never reads the system clock
  directly — it receives an implementation of this behaviour.
  """

  use Boundary, deps: [], exports: [SystemClock]

  @doc "RFC3339 UTC timestamp with Z suffix, second precision."
  @callback wall_ts() :: String.t()

  @doc "Unix seconds."
  @callback unix_now() :: integer()

  @doc "Monotonic milliseconds — durations only, never journaled as wall time."
  @callback monotonic_ms() :: integer()

  defmodule SystemClock do
    @moduledoc false

    @behaviour AiOrchestrator.Clock

    @impl true
    def wall_ts, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    @impl true
    def unix_now, do: System.os_time(:second)

    @impl true
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end
end
