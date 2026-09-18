defmodule AiOrchestrator.Test.LiveMonotonicClock do
  @moduledoc """
  TEST-ONLY clock: `FixedClock`'s scripted WALL stream (the reducer journals exactly what the scenario's FixedClock
  would) with REAL monotonic time. For subtree rows whose Worker-owned fence must actually FIRE with nobody driving
  the clock (a queued-send `Effect.Timer`, R08 G4): FixedClock's own monotonic counter advances 10 ms per READ and
  can never reach a fence's due instant on its own, so a Timer armed against it would wait one real chunk per read.
  """
  @behaviour AiOrchestrator.Clock

  alias AiOrchestrator.Test.FixedClock

  @impl true
  def unix_now, do: FixedClock.unix_now()
  @impl true
  def wall_ts, do: FixedClock.wall_ts()
  @impl true
  def monotonic_ms, do: System.monotonic_time(:millisecond)
end
