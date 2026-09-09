defmodule AiOrchestrator.Application do
  @moduledoc false

  use Boundary, deps: [AiOrchestrator, AiOrchestrator.Host, AiOrchestrator.Journal]
  use Application

  @impl true
  def start(_type, _args), do: Supervisor.start_link(children(), options())

  @doc """
  The host root's supervision options.

  `rest_for_one` is the whole point of the root: see `children/0`.
  """
  @spec options() :: keyword()
  def options, do: [strategy: :rest_for_one, name: AiOrchestrator.Supervisor]

  @doc """
  The host root's children, in the order their ownership depends on.

  The strategy is `rest_for_one` because that order is a dependency, not a
  preference. `AiOrchestrator.Journal.Ownership` arbitrates which process may
  hold a run directory's lock inside this BEAM, so every run tree mounted
  after it depends on it; if the arbiter is lost, those trees hold locks no
  arbiter has recorded, and the only honest recovery is to take them down
  with it. Each writer releases its own token-safe lock as it shuts down, and
  the restarted arbiter reconstructs its registrations as the run trees
  restart above their own locks.

  `AiOrchestrator.Host.Supervisor` mounts after the arbiter: its run trees hold
  locks the arbiter recorded, so losing the arbiter must take them down through
  their owners' teardown. `AiOrchestrator.Host.Monitor` mounts LAST: it only
  observes, so its own restart must restart no run tree; it reconciles from the
  tree instead (docs/contracts/host-mounted-runs.org).
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()} | module()]
  def children, do: [AiOrchestrator.Journal.Ownership, AiOrchestrator.Host.Supervisor, AiOrchestrator.Host.Monitor]
end
