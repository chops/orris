defmodule AiOrchestrator.Test.FixedId do
  @moduledoc "Deterministic Id seam for tests: predictable, process-local counters."

  @behaviour AiOrchestrator.Id

  def reset do
    Process.put({__MODULE__, :run}, 0)
    Process.put({__MODULE__, :sup}, 0)
  end

  @impl true
  def run_id, do: "run_fixture_#{pad(bump(:run))}"

  @impl true
  def supervisor_instance, do: "sup_#{pad(bump(:sup))}"

  defp bump(key) do
    n = Process.get({__MODULE__, key}, 0) + 1
    Process.put({__MODULE__, key}, n)
    n
  end

  defp pad(n), do: String.pad_leading(Integer.to_string(n), 4, "0")
end
