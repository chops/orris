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
  alias AiOrchestrator.Run.Executor.Startup
  alias AiOrchestrator.Run.Supervisor, as: RunSupervisor

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

  @infinite_caller_ms 60_000
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
  Synchronous stop bounded by the caller's `timeout` (the whole call, one absolute deadline); the child shutdown
  bounds the owner's teardown and is never extended. Answers {:ok, :stopped}, the closed teardown_incomplete, :ok
  for a retained terminal owner, run_host_owner_down when the owner is gone, run_host_stop_timeout when the
  caller's budget elapsed first (the agent continues), run_host_stop_unproven when the bound elapsed without a
  proven stop (an owner proven active was killed; an owner that gave no evidence is left untouched).
  """
  @spec stop(map(), timeout()) :: :ok | {:ok, :stopped} | {:error, map()}
  def stop(%{owner: owner} = handle, timeout) do
    if Process.alive?(owner) do
      ref = make_ref()
      mon = Process.monitor(owner)
      caller = self()
      supervisor = Map.get(handle, :supervisor, HostSupervisor)
      child_shutdown = HostSupervisor.child_shutdown_ms(supervisor)
      deadline = absolute(timeout)
      # the stop agent is neither linked to the caller nor bounded by the caller's wait: it carries the
      # obligation to obtain an acknowledgment or, failing that, a lifecycle proof before any termination
      {agent, amon} = spawn_monitor(fn -> stop_agent(owner, supervisor, child_shutdown, caller, ref, timeout) end)
      outcome = stop_wait(owner, mon, agent, amon, ref, deadline)
      Process.demonitor(mon, [:flush])
      Process.demonitor(amon, [:flush])
      outcome
    else
      {:error, %{clause: "run_host_owner_down"}}
    end
  end

  defp absolute(:infinity), do: :infinity
  defp absolute(timeout) when is_integer(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp left(:infinity), do: :infinity
  defp left(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp stop_wait(owner, mon, _agent, amon, ref, deadline) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
      {:stop_outcome, ^ref, outcome} -> outcome
      {:DOWN, ^mon, :process, ^owner, :killed} -> {:error, %{clause: "run_host_stop_unproven"}}
      {:DOWN, ^mon, :process, ^owner, _reason} -> stop_outcome_or_down(ref)
      {:DOWN, ^amon, :process, _agent, _reason} -> stop_wait_owner_only(owner, mon, ref, deadline)
    after
      left(deadline) -> {:error, %{clause: "run_host_stop_timeout"}}
    end
  end

  # the agent is gone: the same absolute caller deadline continues, never a fresh one
  defp stop_wait_owner_only(owner, mon, ref, deadline) do
    receive do
      {:stop_outcome, ^ref, :retained} -> :ok
      {:stop_outcome, ^ref, outcome} -> outcome
      {:DOWN, ^mon, :process, ^owner, :killed} -> {:error, %{clause: "run_host_stop_unproven"}}
      {:DOWN, ^mon, :process, ^owner, _reason} -> stop_outcome_or_down(ref)
    after
      left(deadline) -> {:error, %{clause: "run_host_stop_timeout"}}
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

  # ONE shared bound: the acknowledgment wait, the evidence legs and the teardown all live inside the declared
  # child shutdown measured from the stop; the caller's wait neither resets nor cancels it. The request is sent
  # once and its reply awaited for the whole bound: no probe budget exceeds the bound (no floor).
  #
  # Lifecycle ARBITRATION (docs/contracts/host-mounted-runs.org, stop): the owner's own word (:retained /
  # :stopping) always wins and is awaited for the whole bound. Evidence gathered while the owner is silent (its
  # :sys phase; a live Run.Supervisor still linked to it, which proves the subtree is not torn down and so no
  # result is retained) NEVER authorises destruction before the bound: it only decides what happens to an owner
  # that is STILL silent at the bound. At the bound the owner is frozen (scheduler-level suspension, so it can
  # neither answer nor transition while the decision is taken), its reply is checked first (it replies before it
  # informs the caller, so no reply means the caller was told nothing), the link evidence is re-read under the
  # freeze, and only a still-linked, still-silent owner that is a child of this host's supervisor is killed.
  # Everything else is resumed untouched and reported unproven: a wait can never authorise the loss of a cached
  # result. A sampled phase can therefore never be acted upon after it became obsolete.
  defp stop_agent(owner, supervisor, child_shutdown, caller, ref, timeout) do
    started = System.monotonic_time(:millisecond)
    remaining = fn -> max(child_shutdown - (System.monotonic_time(:millisecond) - started), 0) end
    mon = Process.monitor(owner)
    request = :gen_statem.send_request(owner, {:stop_request, {caller, ref, max(timeout, child_shutdown)}})
    probe = min(min(div(child_shutdown, 2), div(caller_ms(timeout), 2)), remaining.())

    case :gen_statem.wait_response(request, probe) do
      :timeout -> stop_unacknowledged(owner, supervisor, caller, ref, request, mon, remaining)
      answer -> acknowledged(answer, owner, mon, remaining)
    end
  end

  defp caller_ms(:infinity), do: @infinite_caller_ms
  defp caller_ms(timeout), do: timeout

  defp acknowledged({:reply, {:ack, :retained}}, _owner, _mon, _remaining), do: :ok
  defp acknowledged({:reply, {:ack, :stopping}}, owner, mon, remaining), do: enforce_bound(owner, mon, remaining)
  defp acknowledged({:error, _owner_down}, _owner, _mon, _remaining), do: :ok

  defp stop_unacknowledged(owner, supervisor, caller, ref, request, mon, remaining) do
    case lifecycle_evidence(owner, supervisor, remaining) do
      :retained ->
        send(caller, {:stop_outcome, ref, :retained})
        :ok

      :active ->
        # the host kills only what it hosts: membership is confirmed with the supervisor (bounded) before any
        # decision at the bound; the evidence itself is re-read under the freeze, never trusted from here
        arbitrated = if child_of?(supervisor, owner, remaining), do: :active, else: :unknown
        await_word(arbitrated, owner, caller, ref, request, mon, remaining)

      :unknown ->
        await_word(:unknown, owner, caller, ref, request, mon, remaining)
    end
  end

  # the owner's word is awaited for the whole remaining bound; only then is the bound arbitrated
  defp await_word(evidence, owner, caller, ref, request, mon, remaining) do
    case :gen_statem.wait_response(request, remaining.()) do
      :timeout -> arbitrate_at_bound(evidence, owner, caller, ref, request, mon, remaining)
      answer -> acknowledged(answer, owner, mon, remaining)
    end
  end

  defp arbitrate_at_bound(evidence, owner, caller, ref, request, mon, remaining) do
    frozen = freeze(owner)

    case :gen_statem.receive_response(request, 0) do
      :timeout ->
        if evidence == :active and frozen and subtree_linked?(owner) do
          Process.exit(owner, :kill)
          thaw(owner)
          enforce_bound(owner, mon, remaining)
        else
          thaw(owner)
          send(caller, {:stop_outcome, ref, {:error, %{clause: "run_host_stop_unproven"}}})
        end

      answer ->
        thaw(owner)
        acknowledged(answer, owner, mon, remaining)
    end
  end

  # scheduler-level freeze: the only primitive that makes "check the owner's word, then kill" atomic against the
  # owner's own transitions; a dead owner is simply not frozen
  defp freeze(owner) do
    :erlang.suspend_process(owner, [])
  catch
    :error, _ -> false
  end

  defp thaw(owner) do
    :erlang.resume_process(owner)
  catch
    :error, _ -> false
  end

  defp child_of?(supervisor, owner, remaining) do
    children = GenServer.call(supervisor, :which_children, max(remaining.(), 1))
    Enum.any?(children, fn {_id, pid, _type, _mods} -> pid == owner end)
  catch
    :exit, _ -> false
  end

  defp lifecycle_evidence(owner, _supervisor, remaining) do
    case inspected_phase(owner, min(200, remaining.())) do
      :terminal -> :retained
      :unknown -> if subtree_linked?(owner), do: :active, else: :unknown
      _active_or_starting -> :active
    end
  end

  defp inspected_phase(_owner, 0), do: :unknown

  defp inspected_phase(owner, budget) do
    RunOwner.inspect(owner, budget).phase
  catch
    :exit, _ -> :unknown
  end

  # runtime evidence: the startup chain owner -> helper -> starter -> Run.Supervisor, traversed by links and
  # witnessed by each process's `$initial_call` (set by proc_lib at spawn, before any init, so a Writer blocked
  # in acquire still counts). The chain is a subtree that has not been torn down. It is FALSE at a terminal
  # owner, which unlinks and joins helper and starter before retaining its result
  # (docs/contracts/core-startup-bound.org, section 6).
  defp subtree_linked?(owner) do
    Enum.any?(linked(owner), fn helper ->
      seam?(helper, :helper_init) and
        Enum.any?(linked(helper), fn starter ->
          seam?(starter, :starter_init) and Enum.any?(linked(starter), &run_supervisor?/1)
        end)
    end)
  end

  defp linked(pid) do
    case Process.info(pid, :links) do
      {:links, links} -> Enum.filter(links, &is_pid/1)
      nil -> []
    end
  end

  defp initial_call(pid) when is_pid(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} -> Keyword.get(dictionary, :"$initial_call")
      nil -> nil
    end
  end

  defp seam?(pid, function), do: match?({Startup, ^function, _}, initial_call(pid))

  defp run_supervisor?(pid) when is_pid(pid), do: match?({:supervisor, RunSupervisor, _}, initial_call(pid))

  defp run_supervisor?(_port), do: false

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

    case discover(supervisor, remaining) do
      {:ok, children} ->
        owners = for {_, pid, _, _} <- children, is_pid(pid), do: pid
        batch = Keyword.get(opts, :batch, :all)
        chunks = if batch == :all, do: [owners], else: Enum.chunk_every(owners, batch)
        {:ok, Enum.flat_map(chunks, &query_batch(&1, remaining))}

      :unavailable ->
        {:error, %{clause: "host_supervisor_unavailable"}}
    end
  end

  # discovery runs in a worker THIS process owns: linked (the caller's death ends it), its call bounded by what
  # remains of the deadline (it never outlives the query), joined on timeout with its monitor and any late reply
  # cleaned up; it exits normally on every path so the link never reaches the caller
  defp discover(supervisor, remaining) do
    me = self()
    dref = make_ref()
    budget = remaining.()
    worker = spawn_link(fn -> send(me, {:discovered, dref, which_children(supervisor, budget)}) end)
    dmon = Process.monitor(worker)

    receive do
      {:discovered, ^dref, answer} ->
        Process.demonitor(dmon, [:flush])
        answer

      {:DOWN, ^dmon, :process, ^worker, _reason} ->
        drain_discovery(dref)
        :unavailable
    after
      remaining.() ->
        Process.unlink(worker)
        Process.exit(worker, :kill)
        receive(do: ({:DOWN, ^dmon, :process, ^worker, _reason} -> :ok))
        drain_discovery(dref)
        :unavailable
    end
  end

  defp which_children(supervisor, budget) do
    {:ok, GenServer.call(supervisor, :which_children, budget)}
  catch
    :exit, _ -> :unavailable
  end

  defp drain_discovery(dref), do: receive(do: ({:discovered, ^dref, _} -> :ok), after: (0 -> :ok))

  # the active batch is queried concurrently by unlinked monitored workers, ONE :sys leg per owner, every
  # worker bounded by what remains of the single deadline; an expired batch starts no work; every unknown
  # result keeps the owner it names; only THIS batch's worker monitors and replies are ever received
  defp query_batch([], _remaining), do: []

  defp query_batch(chunk, remaining) do
    budget = remaining.()

    if budget == 0 do
      Enum.map(chunk, &unknown/1)
    else
      tag = make_ref()
      workers = Map.new(chunk, &{&1, query_worker(&1, tag, budget)})
      monitors = Map.new(workers, fn {owner, {_pid, mon}} -> {mon, owner} end)
      collect_views(workers, monitors, tag, remaining, %{})
    end
  end

  defp query_worker(owner, tag, budget) do
    me = self()
    spawn_monitor(fn -> send(me, {tag, owner, phase_of(owner, budget)}) end)
  end

  defp collect_views(workers, _monitors, _tag, _remaining, views) when map_size(workers) == 0, do: Map.values(views)

  defp collect_views(workers, monitors, tag, remaining, views) do
    receive do
      {^tag, owner, view} when is_map_key(workers, owner) ->
        {{_pid, mon}, rest} = Map.pop(workers, owner)
        Process.demonitor(mon, [:flush])
        collect_views(rest, Map.delete(monitors, mon), tag, remaining, Map.put(views, owner, view))

      {:DOWN, mon, :process, _pid, _reason} when is_map_key(monitors, mon) ->
        owner = Map.fetch!(monitors, mon)
        views = Map.put_new(views, owner, unknown(owner))
        collect_views(Map.delete(workers, owner), Map.delete(monitors, mon), tag, remaining, views)
    after
      remaining.() ->
        for {_owner, {pid, mon}} <- workers do
          Process.demonitor(mon, [:flush])
          Process.exit(pid, :kill)
        end

        drain_views(tag)
        Map.values(Map.merge(Map.new(workers, fn {owner, _} -> {owner, unknown(owner)} end), views))
    end
  end

  defp drain_views(tag), do: receive(do: ({^tag, _owner, _view} -> drain_views(tag)), after: (0 -> :ok))

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
