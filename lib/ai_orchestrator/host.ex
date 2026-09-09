defmodule AiOrchestrator.Host do
  @moduledoc """
  The in-VM observational host boundary (docs/contracts/host-observational-registry.org).

  It sits ABOVE `AiOrchestrator.Run`: `AiOrchestrator.Host.Monitor` records which run directories are
  live under this BEAM's run subtrees, `AiOrchestrator.Host.Executor` wraps `Run.Executor` so an owned
  subtree registers itself at its barrier, and this module answers bounded lookups. Nothing here admits
  or refuses a command: admission stays with `Journal.Ownership`, `RunLock` and the run's own journal
  decisions (R4). Registry presence authorizes nothing; registry absence proves nothing.

  `status/2` consults the monitor and then `Journal.Ownership.status/2` under ONE total budget
  (`timeout:`, default #{1_000} ms) and answers closed maps that name only clause, generation and this
  run's own identities: never a run directory, a lock path or another run's pids.
  """

  use Boundary,
    deps: [AiOrchestrator.Run, AiOrchestrator.Commands, AiOrchestrator.Contract, AiOrchestrator.Journal],
    exports: [Executor, Monitor]

  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Journal.Ownership

  @default_timeout 1_000
  @monitor_unavailable %{clause: "host_monitor_unavailable"}
  @ownership_unavailable %{clause: "host_ownership_unavailable"}
  @timeout_invalid %{clause: "host_status_timeout_invalid"}
  @identity_keys [:owner, :supervisor, :server, :writer, :worker]

  @type status ::
          {:ok,
           %{
             registered: true,
             live: true,
             generation: pos_integer(),
             owner: pid(),
             supervisor: pid(),
             server: pid(),
             writer: pid(),
             worker: pid()
           }}
          | {:ok, %{registered: false}}
          | {:ok, %{clause: String.t(), generation: pos_integer()}}
          | {:error, %{clause: String.t()}}

  @doc """
  The live-run view of `run_dir`: the monitor's entry confirmed against `Journal.Ownership`.

  Options: `monitor:` (pid or name, default the Application instance), `ownership:` (the arbiter
  consulted, default `Journal.Ownership`), `timeout:` (positive integer ms, the total budget for both
  lookups, default #{@default_timeout}).
  """
  @spec status(Path.t(), keyword()) :: status()
  def status(run_dir, opts \\ []) when is_binary(run_dir) and is_list(opts) do
    with {:ok, timeout} <- budget(opts),
         started = System.monotonic_time(:millisecond),
         {:ok, entry} <- ask(monitor(opts), {:lookup, Path.expand(run_dir)}, timeout) do
      confirm(entry, run_dir, opts, remaining(timeout, started))
    end
  end

  @doc "Every monitor entry carrying `run_id` (a diagnostic list, never an admission input)."
  @spec lookup_run_id(String.t(), keyword()) :: {:ok, [map()]} | {:error, %{clause: String.t()}}
  def lookup_run_id(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with {:ok, timeout} <- budget(opts), do: ask(monitor(opts), {:lookup_run_id, run_id}, timeout)
  end

  @doc "Whether more than one live directory carries `run_id`; diagnosed, never acted on (R4)."
  @spec collision(String.t(), keyword()) ::
          {:ok, %{clause: String.t(), count: non_neg_integer()}} | {:error, %{clause: String.t()}}
  def collision(run_id, opts \\ []) do
    with {:ok, entries} <- lookup_run_id(run_id, opts) do
      count = length(entries)
      clause = if count > 1, do: "host_registry_collision", else: "host_registry_unique"
      {:ok, %{clause: clause, count: count}}
    end
  end

  defp budget(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      timeout when is_integer(timeout) and timeout > 0 -> {:ok, timeout}
      _other -> {:error, @timeout_invalid}
    end
  end

  defp monitor(opts), do: Keyword.get(opts, :monitor, Monitor)

  # the monitor answers ordinary GenServer calls (so a forwarding proxy is a valid monitor); an absent,
  # dead, renamed or silent monitor is one closed clause, whatever the exit reason
  defp ask(monitor, request, timeout) do
    {:ok, GenServer.call(monitor, request, timeout)}
  catch
    :exit, _reason -> {:error, @monitor_unavailable}
  end

  defp remaining(timeout, started), do: max(timeout - (System.monotonic_time(:millisecond) - started), 0)

  defp confirm(nil, _run_dir, _opts, _remaining), do: {:ok, %{registered: false}}

  defp confirm(entry, run_dir, opts, remaining) do
    ownership = Keyword.get(opts, :ownership, Ownership)

    case Ownership.status(run_dir, server: ownership, acquire_timeout: remaining) do
      {:ok, %{state: :live, generation: generation}} when generation == entry.generation ->
        {:ok, entry |> Map.take(@identity_keys) |> Map.merge(%{registered: true, live: true, generation: generation})}

      {:error, _unavailable} ->
        {:error, @ownership_unavailable}

      _disagreement ->
        {:ok, %{clause: "host_registry_inconsistent", generation: entry.generation}}
    end
  end
end
