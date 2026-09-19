defmodule AiOrchestrator.Application do
  @moduledoc false

  use Boundary, deps: [AiOrchestrator, AiOrchestrator.Host, AiOrchestrator.Journal, AiOrchestrator.Telemetry]
  use Application

  alias AiOrchestrator.Telemetry.Handler

  @doc """
  Starts the host root, with the application-boundary span handler attached first.

  The handler is attached here rather than mounted as a child because it IS NOT A PROCESS: a
  `:telemetry` handler is a function in that library's own ETS table, invoked in whichever process
  emits, and it keeps its per-process span state in that process's dictionary. A child holding
  nothing would be supervision theatre -- it could not restart the handler's state, because the
  handler has none of its own. north-star-architecture.org:132 draws `Telemetry.Handler` in the
  target tree; this is the same handler at the same boundary, attached rather than spawned, and
  `stop/1` detaches it when the application stops.

  Attaching BEFORE the supervisor starts is deliberate: a run tree that mounts during boot is then
  already observable, and a failure to attach is loud here rather than silently partial later.
  """
  @impl true
  def start(_type, _args) do
    # `:already_exists` is the same end state as `:ok` here: the handler is attached under the
    # default id. It happens when an application is stopped and restarted inside one VM.
    case Handler.attach() do
      {:ok, _id} -> :ok
      {:error, :already_exists} -> :ok
    end

    Supervisor.start_link(children(), options())
  end

  @impl true
  def stop(_state) do
    _detached = Handler.detach()
    :ok
  end

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

  The Monitor is started WITH the supervisor it reconciles from. That argument is
  the census: `AiOrchestrator.Host.Monitor.start_census/2` discovers nothing and
  marks itself `:complete` when no `:host_supervisor` is given, so a bare-module
  child spec would make the reconciliation of section 5 inert in every shipped
  binary (row MR-1c).
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()} | module()]
  def children,
    do: [
      AiOrchestrator.Journal.Ownership,
      AiOrchestrator.Host.Supervisor,
      {AiOrchestrator.Host.Monitor, [host_supervisor: AiOrchestrator.Host.Supervisor]}
    ]
end
