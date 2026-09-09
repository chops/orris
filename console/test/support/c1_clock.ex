defmodule C1.Clock do
  @moduledoc "TEST SUPPORT ONLY: a settable monotonic millisecond clock for exact expiry and limiter rows."
  def start!, do: Agent.start_link(fn -> 1_000_000 end) |> elem(1)
  def fun(agent), do: fn -> Agent.get(agent, & &1) end
  def advance(agent, ms), do: Agent.update(agent, &(&1 + ms))
  def now(agent), do: Agent.get(agent, & &1)
end
