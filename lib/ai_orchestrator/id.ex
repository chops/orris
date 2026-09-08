defmodule AiOrchestrator.Id do
  @moduledoc """
  Determinism seam for identifiers (MC-12): run and supervisor-instance
  identities are journaled facts generated at run time, never defaults.
  """

  use Boundary, deps: [], exports: [SystemId]

  @doc "Unique run identifier: `run_<utc-compact>_<hex24>` — path-safe, sortable, collision-resistant."
  @callback run_id() :: String.t()

  @doc "Unique supervisor incarnation identifier: `sup_<hex24>`."
  @callback supervisor_instance() :: String.t()

  defmodule SystemId do
    @moduledoc false

    @behaviour AiOrchestrator.Id

    @impl true
    def run_id do
      stamp =
        DateTime.utc_now()
        |> DateTime.truncate(:second)
        |> Calendar.strftime("%Y%m%dT%H%M%SZ")

      "run_#{stamp}_#{random_hex()}"
    end

    @impl true
    def supervisor_instance, do: "sup_#{random_hex()}"

    defp random_hex, do: 12 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
  end
end
