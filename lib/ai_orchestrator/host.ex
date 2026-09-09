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
      caller = self()
      supervisor = Map.get(handle, :supervisor, HostSupervisor)
      child_shutdown = HostSupervisor.child_shutdown_ms(supervisor)
      # the stop agent is neither linked to the caller nor bounded by the caller's wait: it carries the
      # obligation to obtain an acknowledgment or, failing that, the supervisor's termination
      {agent, amon} = spawn_monitor(fn -> stop_agent(owner, supervisor, child_shutdown, caller, ref, timeout) end)
      outcome = stop_wait(owner, mon, agent, amon, ref, timeout)
      Process.demonitor(mon, [:flush])
      Process.demonitor(amon, [:flush])
      outcome
    else
      {:error, %{clause: "run_host_owner_down"}}
    end
  end

  defp stop_wait(owner, mon, _agent, amon, ref, timeout) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
      {:stop_outcome, ^ref, outcome} -> outcome
      {:DOWN, ^mon, :process, ^owner, :killed} -> {:error, %{clause: "run_host_stop_unproven"}}
      {:DOWN, ^mon, :process, ^owner, _reason} -> stop_outcome_or_down(ref)
      {:DOWN, ^amon, :process, _agent, _reason} -> stop_wait_owner_only(owner, mon, ref, timeout)
    after
      timeout -> {:error, %{clause: "run_host_stop_timeout"}}
    end
  end

  defp stop_wait_owner_only(owner, mon, ref, timeout) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
      {:stop_outcome, ^ref, outcome} -> outcome
      {:DOWN, ^mon, :process, ^owner, :killed} -> {:error, %{clause: "run_host_stop_unproven"}}
      {:DOWN, ^mon, :process, ^owner, _reason} -> stop_outcome_or_down(ref)
    after
      timeout -> {:error, %{clause: "run_host_stop_timeout"}}
    end
  end

  defp stop_outcome_or_down(ref) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
      {:stop_outcome, ^ref, outcome} -> outcome
    after
      0 -> {:error, %{clause: "run_host_owner_down"}}
    end
  end

  # ONE shared bound: acknowledgment wait, the :sys inspection, and the teardown itself all live inside the
  # declared child shutdown measured from the stop; the caller's wait neither resets nor cancels it. A
  # retained terminal owner is never touched. An owner still alive at the bound is killed (reported unproven).
  defp stop_agent(owner, supervisor, child_shutdown, caller, ref, timeout) do
    started = System.monotonic_time(:millisecond)
    remaining = fn -> max(child_shutdown - (System.monotonic_time(:millisecond) - started), 0) end
    mon = Process.monitor(owner)
    ack_budget = max(min(div(child_shutdown, 2), div(max(timeout, 1), 2)), 50)

    ack =
      try do
        :gen_statem.call(owner, {:stop_request, {caller, ref, max(timeout, child_shutdown)}}, ack_budget)
      catch
        :exit, _ -> :no_ack
      end

    case ack do
      {:ack, :retained} ->
        :ok

      {:ack, :stopping} ->
        enforce_bound(owner, mon, remaining)

      :no_ack ->
        phase =
          try do
            RunOwner.inspect(owner, min(200, max(remaining.(), 1))).phase
          catch
            :exit, _ -> :unknown
          end

        if phase == :terminal do
          send(caller, {:stop_outcome, ref, :retained})
        else
          spawn(fn -> DynamicSupervisor.terminate_child(supervisor, owner) end)
          enforce_bound(owner, mon, remaining)
        end
    end
  end

  defp enforce_bound(owner, mon, remaining) do
    receive do
      {:DOWN, ^mon, :process, ^owner, _reason} -> :ok
    after
      remaining.() -> Process.exit(owner, :kill)
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
    me = self()
    dref = make_ref()
    {_pid, dmon} = spawn_monitor(fn -> send(me, {:discovered, dref, DynamicSupervisor.which_children(supervisor)}) end)

    receive do
      {:discovered, ^dref, children} ->
        Process.demonitor(dmon, [:flush])
        owners = for {_, pid, _, _} <- children, is_pid(pid), do: pid
        batch = Keyword.get(opts, :batch, :all)
        chunks = if batch == :all, do: [owners], else: Enum.chunk_every(owners, batch)
        {:ok, Enum.flat_map(chunks, &query_batch(&1, remaining))}

      {:DOWN, ^dmon, :process, _pid, _reason} ->
        {:error, %{clause: "host_supervisor_unavailable"}}
    after
      remaining.() ->
        {:error, %{clause: "host_supervisor_unavailable"}}
    end
  end

  # the active batch is queried concurrently by unlinked monitored workers, ONE :sys leg per owner, every
  # worker bounded by what remains of the single deadline; an expired batch starts no work; every unknown
  # result keeps the owner it names
  defp query_batch([], _remaining), do: []

  defp query_batch(chunk, remaining) do
    budget = remaining.()

    if budget == 0 do
      Enum.map(chunk, &unknown/1)
    else
      me = self()
      tag = make_ref()

      workers =
        for owner <- chunk, into: %{} do
          {pid, mon} = spawn_monitor(fn -> send(me, {tag, owner, phase_of(owner, budget)}) end)
          {owner, {pid, mon}}
        end

      collect_views(workers, tag, remaining, %{})
    end
  end

  defp collect_views(workers, _tag, _remaining, views) when map_size(workers) == 0, do: Map.values(views)

  defp collect_views(workers, tag, remaining, views) do
    receive do
      {^tag, owner, view} when is_map_key(workers, owner) ->
        {{_pid, mon}, rest} = Map.pop(workers, owner)
        Process.demonitor(mon, [:flush])
        collect_views(rest, tag, remaining, Map.put(views, owner, view))

      {:DOWN, mon, :process, _pid, _reason} ->
        case Enum.find(workers, fn {_owner, {_p, m}} -> m == mon end) do
          {owner, _} ->
            collect_views(Map.delete(workers, owner), tag, remaining, Map.put_new(views, owner, unknown(owner)))

          nil ->
            collect_views(workers, tag, remaining, views)
        end
    after
      remaining.() ->
        for {owner, {pid, mon}} <- workers do
          Process.demonitor(mon, [:flush])
          Process.exit(pid, :kill)
          _ = owner
        end

        Map.values(Map.merge(Map.new(workers, fn {owner, _} -> {owner, unknown(owner)} end), views))
    end
  end

  defp unknown(owner), do: %{run_dir: nil, run_id: nil, owner: owner, phase: :unknown}

  defp phase_of(owner, budget) do
    {state, data} =
      case :sys.get_state(owner, budget) do
        {state, %{} = data} -> {state, data}
        %{} = data -> {Map.get(data, :phase, :unknown), data}
      end

    config = Map.get(data, :config) || %{}
    command = Map.get(config, :command)
    run_id = if is_map(command), do: Map.get(command, :run_id)
    %{run_dir: Map.get(config, :run_dir), run_id: run_id, owner: owner, phase: Map.get(data, :phase, state)}
  catch
    :exit, _ -> unknown(owner)
    :error, _ -> unknown(owner)
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
