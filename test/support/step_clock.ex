defmodule AiOrchestrator.Test.StepClock do
  @moduledoc """
  TEST-ONLY clock whose wall and monotonic readings are set explicitly by the test through `:persistent_term`, so a
  process OTHER than the test (an authority, a Worker) reads exactly what the test decided. No real timer, no tick.
  """
  @behaviour AiOrchestrator.Clock

  def set(unix, mono_ms), do: :persistent_term.put({__MODULE__, self_key()}, {unix, mono_ms})
  def set_unix(unix), do: set(unix, elem(get(), 1))
  def set_mono(mono_ms), do: set(elem(get(), 0), mono_ms)
  def clear, do: :persistent_term.erase({__MODULE__, self_key()})

  @impl true
  def unix_now, do: elem(get(), 0)
  @impl true
  def monotonic_ms, do: elem(get(), 1)
  @impl true
  def wall_ts, do: unix_now() |> DateTime.from_unix!() |> DateTime.to_iso8601()

  # one clock per test module run: the key is fixed so any process reads the same value
  defp self_key, do: :global
  defp get, do: :persistent_term.get({__MODULE__, self_key()}, {1_700_000_000, 0})
end
