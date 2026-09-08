defmodule AiOrchestrator.Test.WorkerSpike.Seams do
  @moduledoc """
  TEST-ONLY holder for an owner's seams (adapter modules and closures), keyed by the run capability. The child
  spec carries the holder's pid and the cap, never the seams themselves, so supervisor reports, start MFAs and
  crash reports cannot print a closure environment.
  """

  def start, do: Agent.start(fn -> %{} end)
  def put(holder, cap, opts) when is_reference(cap) and is_list(opts), do: Agent.update(holder, &Map.put(&1, cap, opts))
  def fetch(holder, cap), do: Agent.get(holder, &Map.fetch(&1, cap))
end
