defmodule AiOrchestrator.Test.FixedClock do
  @moduledoc """
  Deterministic Clock for tests. Wall time: `wall_ts` CONSUMES one wall tick per read (one second), `unix_now`
  PEEKS without consuming, `advance/1` adds whole seconds. Monotonic: `monotonic_ms` advances 10 ms per read on
  its OWN counter and never touches the wall ticks (decoupled 2026-09-07 under m_1788751885000: a fence or poll
  reading monotonic time must not move the wall clock the reducer's Clock effects journal). Both streams are
  process-local; `reset/0` clears both. Base 2026-09-01T12:00:00Z.
  """

  @behaviour AiOrchestrator.Clock

  @base ~U[2026-09-01 12:00:00Z]

  def reset do
    Process.put({__MODULE__, :ticks}, 0)
    Process.put({__MODULE__, :mono}, 0)
  end

  @doc "Advance the clock by whole seconds without consuming a tick read."
  def advance(seconds) when is_integer(seconds) and seconds >= 0 do
    Process.put({__MODULE__, :ticks}, Process.get({__MODULE__, :ticks}, 0) + seconds)
  end

  @impl true
  def wall_ts do
    @base |> DateTime.shift(second: tick()) |> DateTime.to_iso8601()
  end

  @impl true
  def unix_now, do: DateTime.to_unix(@base) + peek()

  @impl true
  # monotonic reads advance their OWN counter (10 ms per read) and never consume a wall tick: a fence or
  # poll reading monotonic time must not move the wall clock the reducer's Clock effects journal
  def monotonic_ms do
    n = Process.get({__MODULE__, :mono}, 0)
    Process.put({__MODULE__, :mono}, n + 1)
    n * 10
  end

  def base_unix, do: DateTime.to_unix(@base)

  defp tick do
    n = Process.get({__MODULE__, :ticks}, 0)
    Process.put({__MODULE__, :ticks}, n + 1)
    n
  end

  defp peek, do: Process.get({__MODULE__, :ticks}, 0)
end
