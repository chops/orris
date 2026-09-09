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
    exports: [Executor, Monitor, RunOwner, Supervisor]

  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Host.Supervisor, as: HostSupervisor
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Executor, as: RunExecutor

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

  # ---- mounted runs (docs/contracts/host-mounted-runs.org) ----

  @default_mount_timeout 1_000
  @default_batch 64
  @relay_keys [:join, :handoff_relay]

  @doc """
  Mounts a run tree under the host supervisor. Validation is `Run.Executor.prepare/2` (never duplicated);
  the call returns as soon as the owner child is started (supervisor acknowledgment), before readiness.
  Options: `host:` seams `%{supervisor, monitor, ownership}` (default the Application instances), `budgets:`,
  `retention_ms:`. The context may carry the test seams `:join` and `:handoff_relay`, which are stripped.
  """
  @spec mount(AiOrchestrator.Contract.Command.t(), keyword(), keyword()) :: {:ok, map()} | {:error, map()}
  def mount(command, context, opts \\ []) when is_list(context) and is_list(opts) do
    seams = Keyword.take(context, @relay_keys)

    host =
      Map.merge(
        %{supervisor: HostSupervisor, monitor: Monitor, ownership: Ownership},
        Keyword.get(opts, :host, %{})
      )

    with {:ok, %{config: config, barrier: barrier}} <-
           RunExecutor.prepare(command, Keyword.drop(context, @relay_keys)) do
      # the host's arbiter reaches the Writer and the Server through the Run passthrough (contract section 6)
      config =
        Map.update!(config, :opts, fn run_opts ->
          run_opts |> Keyword.drop(@relay_keys) |> Keyword.put_new(:ownership, server: host.ownership)
        end)

      args =
        %{
          config: config,
          barrier: barrier,
          host: host,
          child_shutdown_ms: HostSupervisor.child_shutdown_ms(host.supervisor)
        }
        |> put_opt(:budgets, Keyword.get(opts, :budgets))
        |> put_opt(:retention_ms, Keyword.get(opts, :retention_ms))
        |> put_opt(:join, seams[:join])
        |> put_opt(:handoff_relay, seams[:handoff_relay])

      case DynamicSupervisor.start_child(host.supervisor, {RunOwner, args}) do
        {:ok, owner} ->
          handle = %{run_dir: config.run_dir, run_id: command.run_id, owner: owner, ref: make_ref()}
          {:ok, Map.put(handle, :supervisor, host.supervisor)}

        {:error, _reason} ->
          {:error, %{clause: "host_mount_failed"}}
      end
    end
  end

  defp put_opt(map, _key, nil), do: map
  defp put_opt(map, key, value), do: Map.put(map, key, value)

  @doc "Wait-only readiness: answers once the :subtree_started barrier returned :ok, or an earlier closed result."
  @spec ready(map(), timeout()) :: {:ok, map()} | {:error, map()}
  def ready(%{owner: owner}, timeout) do
    :gen_statem.call(owner, {:ready, timeout}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, %{clause: "ready_timeout"}}
    :exit, _ -> {:error, %{clause: "run_host_owner_down"}}
  end

  @doc "The cached run result; a wait timeout never cancels; a gone owner answers run_host_owner_down."
  @spec await(map(), timeout()) :: {:ok, map()} | {:error, map()}
  def await(%{owner: owner}, timeout) do
    :gen_statem.call(owner, {:await, timeout}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, %{clause: "await_timeout"}}
    :exit, _ -> {:error, %{clause: "run_host_owner_down"}}
  end

  @doc """
  Synchronous stop bounded by the caller's `timeout` (the whole call); the child shutdown bounds the owner's
  teardown and is never extended. Answers {:ok, :stopped}, the closed teardown_incomplete, :ok for a retained
  terminal owner, run_host_owner_down when the owner is gone, run_host_stop_timeout when the caller's budget
  elapsed first (teardown continues), run_host_stop_unproven when the supervisor had to kill the owner.
  """
  @spec stop(map(), timeout()) :: :ok | {:ok, :stopped} | {:error, map()}
  def stop(%{owner: owner} = handle, timeout) do
    if Process.alive?(owner) do
      ref = make_ref()
      mon = Process.monitor(owner)
      # the stopper is recorded by a cast (answered from any responsive state, including :terminal, where the
      # owner keeps its retention); an owner blocked in its start phase never processes it and is stopped by
      # the supervisor within the child shutdown, which this call reports as unproven
      :gen_statem.cast(owner, {:stop_waiter, {self(), ref}})
      supervisor = Map.get(handle, :supervisor, HostSupervisor)
      stopper = Task.async(fn -> stop_child(supervisor, owner, ref) end)

      receive do
        {:stop_outcome, ^ref, :retained} ->
          Task.shutdown(stopper, :brutal_kill)
          Process.demonitor(mon, [:flush])
          :ok

        {:stop_outcome, ^ref, outcome} ->
          Task.shutdown(stopper, :brutal_kill)
          Process.demonitor(mon, [:flush])
          outcome

        {:DOWN, ^mon, :process, ^owner, :killed} ->
          Task.shutdown(stopper, :brutal_kill)
          {:error, %{clause: "run_host_stop_unproven"}}

        {:DOWN, ^mon, :process, ^owner, _reason} ->
          Task.shutdown(stopper, :brutal_kill)

          receive do
            {:stop_outcome, ^ref, outcome} -> outcome
          after
            0 -> {:error, %{clause: "run_host_owner_down"}}
          end
      after
        timeout ->
          Task.shutdown(stopper, :brutal_kill)
          Process.demonitor(mon, [:flush])
          {:error, %{clause: "run_host_stop_timeout"}}
      end
    else
      {:error, %{clause: "run_host_owner_down"}}
    end
  end

  # a retained (terminal) owner answers the stop cast itself and must not be terminated; every other owner is
  # terminated through its supervisor, bounded by the child shutdown
  defp stop_child(supervisor, owner, ref) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
    after
      50 -> DynamicSupervisor.terminate_child(supervisor, owner)
    end
  end

  @doc """
  The mounted trees as the supervisor knows them: discovery and every per-owner phase query run inside ONE
  monotonic deadline (`timeout`); the active batch is queried concurrently with the remaining time; owners
  that do not answer are listed with phase :unknown; a silent supervisor answers host_supervisor_unavailable.
  """
  @spec mounted(map(), timeout(), keyword()) :: {:ok, [map()]} | {:error, map()}
  def mounted(host, timeout \\ @default_mount_timeout, opts \\ []) do
    supervisor = Map.get(host, :supervisor, HostSupervisor)
    deadline = System.monotonic_time(:millisecond) + timeout
    remaining = fn -> max(deadline - System.monotonic_time(:millisecond), 0) end
    discovery = Task.async(fn -> DynamicSupervisor.which_children(supervisor) end)

    case Task.yield(discovery, remaining.()) || Task.shutdown(discovery, :brutal_kill) do
      {:ok, children} ->
        owners = for {_, pid, _, _} <- children, is_pid(pid), do: pid
        batch = Keyword.get(opts, :batch, @default_batch)

        listed = owners |> Enum.chunk_every(batch) |> Enum.flat_map(&query_batch(&1, remaining))

        {:ok, listed}

      _ ->
        {:error, %{clause: "host_supervisor_unavailable"}}
    end
  end

  # the active batch is queried concurrently, every query bounded by what remains of the single deadline
  defp query_batch(chunk, remaining) do
    budget = remaining.()

    chunk
    |> Task.async_stream(&phase_of(&1, budget),
      timeout: budget + 50,
      on_timeout: :kill_task,
      ordered: false,
      max_concurrency: max(length(chunk), 1)
    )
    |> Enum.map(fn
      {:ok, view} -> view
      _ -> %{run_dir: nil, run_id: nil, owner: nil, phase: :unknown}
    end)
  end

  defp phase_of(owner, budget) do
    phase = :gen_statem.call(owner, :phase, budget)
    {_state, data} = :sys.get_state(owner, budget)
    %{run_dir: data.config.run_dir, run_id: data.config.command.run_id, owner: owner, phase: phase}
  catch
    :exit, _ -> %{run_dir: nil, run_id: nil, owner: owner, phase: :unknown}
  end

  @doc "The Monitor's census state: {:ok, %{census: :pending | :complete, skipped: n}} (bounded call)."
  @spec census(keyword()) ::
          {:ok, %{census: :pending | :complete, skipped: non_neg_integer()}} | {:error, %{clause: String.t()}}
  def census(opts \\ []) do
    with {:ok, timeout} <- budget(opts), do: ask(monitor(opts), :census, timeout)
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
