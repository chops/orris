defmodule OrrisConsole.MutationRegistry do
  @moduledoc """
  Unique keys {:operation, op_ref} registered by every MutationOperation in its init: the authority's census (never
  which_children). Start wrapper: a killed Registry leaves its partition alive for a moment and an immediate restart
  answers already_started (measured); the wrapper waits a bounded time for the previous generation to be gone.
  """
  @wait_ms 2_000
  @step_ms 20

  def child_spec(_opts \\ []), do: %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}

  def start_link do
    wait_previous(System.monotonic_time(:millisecond) + @wait_ms)
    Registry.start_link(keys: :unique, name: __MODULE__)
  end

  defp wait_previous(deadline) do
    stale = [__MODULE__, Module.concat(__MODULE__, "PIDPartition0")] |> Enum.any?(&is_pid(Process.whereis(&1)))

    if stale and System.monotonic_time(:millisecond) < deadline do
      Process.sleep(@step_ms)
      wait_previous(deadline)
    else
      :ok
    end
  end
end
