defmodule AiOrchestrator.Host.RunOwner do
  @moduledoc """
  The hosted harness owner of one mounted run (docs/contracts/host-mounted-runs.org): ONE responsive
  `:gen_statem` that is the linked parent of `AiOrchestrator.Run.Supervisor`, holds every owned identity in
  its own data, runs the user barrier in a linked+monitored helper (mounted-route semantic), receives the
  parent's EXIT in `terminate/3`, and runs the ONE teardown implementation shared with the foreground owner
  (`AiOrchestrator.Run.Executor.Owner`).

  States: `:starting` (the only synchronous phase: `Run.Supervisor.start_link` blocks this process),
  `:handoff`, `:barrier_handoff`, `:barrier_subtree`, `:awaiting`, `:terminal`. Teardown runs inside the
  handler or `terminate/3` that enters it (never observable as a state); `inspect/1` reports the last phase.

  Waiters (`await`, `ready`, `stop`) are recorded with a monitor on the caller and a timer-driven deadline.
  A late identity message carrying this owner's handoff reference is retained under `:late` and swept at
  teardown. Registration with `AiOrchestrator.Host.Monitor` happens inside the `:subtree_started` helper
  before the user barrier; the record is unregistered explicitly at teardown.
  """

  @behaviour :gen_statem

  alias AiOrchestrator.Host.Executor, as: HostExecutor
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Executor.Owner

  @default_budgets %{close: 15_000, stop: 15_000, join: 5_000, handoff: 5_000, helper_join: 1_000}
  @default_retention_ms 60_000
  @identity_roles [:supervisor, :server, :work, :writer, :worker]

  @type args :: %{
          required(:config) => map(),
          required(:barrier) => (atom(), map() -> term()) | nil,
          required(:host) => %{supervisor: term(), monitor: term(), ownership: term()},
          optional(:budgets) => map(),
          optional(:retention_ms) => pos_integer(),
          optional(:join) => Owner.join_fn(),
          optional(:handoff_relay) => pid(),
          optional(:child_shutdown_ms) => pos_integer()
        }

  @spec child_spec(args()) :: Supervisor.child_spec()
  def child_spec(args) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: Map.get(args, :child_shutdown_ms, 60_000)
    }
  end

  @spec start_link(args()) :: :gen_statem.start_ret()
  def start_link(args), do: :gen_statem.start_link(__MODULE__, args, [])

  @doc "Test-only observation seam through `:sys.get_state` (a system message, never an owner event)."
  @spec inspect(pid()) :: %{phase: atom(), waiters: non_neg_integer(), handoff_ref: reference(), owned: [atom()]}
  def inspect(owner) do
    {_state, data} = :sys.get_state(owner)

    %{
      phase: data.phase,
      waiters: map_size(data.waiters),
      handoff_ref: data.ref,
      owned: data.owned |> Map.keys() |> Enum.sort()
    }
  end

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)
    ref = make_ref()
    relay = Map.get(args, :handoff_relay)
    handoff_target = if is_pid(relay), do: relay, else: self()
    if is_pid(relay), do: send(relay, {:relay_owner, self()})
    config = Map.put(args.config, :owner_handoff, {handoff_target, ref})

    data = %{
      config: config,
      barrier: args.barrier,
      host: args.host,
      ref: ref,
      owned: %{},
      task: nil,
      registered: nil,
      request: nil,
      waiters: %{},
      ready_waiters: %{},
      result: nil,
      record: nil,
      budgets: Map.merge(@default_budgets, Map.get(args, :budgets, %{})),
      join: Map.get(args, :join, &Owner.joined?/3),
      retention_ms: Map.get(args, :retention_ms, @default_retention_ms),
      phase: :starting
    }

    {:ok, :starting, data, [{:next_event, :internal, :start}]}
  end

  # ---- starting: the only synchronous phase; a :kill here collapses the subtree by links ----
  @impl true
  def handle_event(:internal, :start, :starting, data) do
    case Owner.start_subtree(data.config) do
      {:ok, sup} ->
        case Owner.facts(sup) do
          {:ok, facts} ->
            data = %{data | owned: Map.put(facts, :supervisor, sup), phase: :handoff}
            {:next_state, :handoff, data, [{:state_timeout, data.budgets.handoff, :handoff_budget}]}

          :error ->
            teardown_to(%{data | owned: %{supervisor: sup}}, {:error, %{clause: "run_server_down"}})
        end

      {:error, rejection} ->
        terminal(data, {:error, rejection})
    end
  end

  # ---- the subtree's EXIT (linked parent) in any live state ----
  def handle_event(:info, {:EXIT, sup, _reason}, state, %{owned: %{supervisor: sup}} = data)
      when state not in [:terminal, :starting], do: teardown_to(data, {:error, %{clause: "run_server_down"}})

  # ---- waiter bookkeeping: caller DOWN and timer-driven deadlines ----
  def handle_event(:info, {:DOWN, mon, :process, _pid, _}, _state, %{waiters: waiters} = data)
      when is_map_key(waiters, mon), do: {:keep_state, %{data | waiters: Map.delete(waiters, mon)}}

  def handle_event(:info, {:DOWN, mon, :process, _pid, _}, _state, %{ready_waiters: ready} = data)
      when is_map_key(ready, mon), do: {:keep_state, %{data | ready_waiters: Map.delete(ready, mon)}}

  def handle_event({:timeout, {:waiter, mon}}, :expire, _state, data) do
    Process.demonitor(mon, [:flush])
    {:keep_state, %{data | waiters: Map.delete(data.waiters, mon), ready_waiters: Map.delete(data.ready_waiters, mon)}}
  end

  # ---- handoff: the worker identity is an EVENT stored by this process itself ----
  def handle_event(:info, {:run_worker_registered, ref, worker, server}, :handoff, %{ref: ref} = data) do
    data = %{data | owned: Map.put(data.owned, :worker, worker), registered: {ref, server}, phase: :barrier_handoff}
    {:next_state, :barrier_handoff, run_barrier(data, :handoff_received)}
  end

  def handle_event(:info, {:run_worker_absent, ref, _clause}, :handoff, %{ref: ref} = data),
    do: {:next_state, :barrier_subtree, run_barrier(%{data | phase: :barrier_subtree}, :subtree_started)}

  def handle_event(:state_timeout, :handoff_budget, :handoff, data),
    do: {:next_state, :barrier_subtree, run_barrier(%{data | phase: :barrier_subtree}, :subtree_started)}

  # a late identity (any other state) is retained, never dropped, never acted on before teardown
  def handle_event(:info, {:run_worker_registered, ref, worker, _server}, state, %{ref: ref} = data)
      when state != :handoff, do: {:keep_state, %{data | owned: Map.update(data.owned, :late, [worker], &[worker | &1])}}

  # ---- barriers run in a helper; results and escapes are events ----
  def handle_event(
        :info,
        {:barrier_result, tref, :ok},
        :barrier_handoff,
        %{task: {_, _, tref}, registered: {ref, server}} = data
      ) do
    send(server, {:run_worker_acknowledged, ref})
    {:next_state, :barrier_subtree, run_barrier(%{data | task: nil, phase: :barrier_subtree}, :subtree_started)}
  end

  def handle_event(:info, {:barrier_result, tref, :ok}, :barrier_subtree, %{task: {_, _, tref}} = data) do
    request = :gen_statem.send_request(data.owned.server, :await)
    data = %{data | task: nil, request: request, phase: :awaiting}
    ready = ready_view(data)
    for {mon, from} <- data.ready_waiters, do: reply_waiter(mon, from, {:ok, ready})
    {:next_state, :awaiting, %{data | ready_waiters: %{}}}
  end

  def handle_event(:info, {:barrier_result, tref, _other}, state, %{task: {_, _, tref}} = data)
      when state in [:barrier_handoff, :barrier_subtree],
      do: teardown_to(%{data | task: nil}, {:error, %{clause: "run_executor_down"}})

  def handle_event(:info, {:DOWN, tmon, :process, _tpid, _reason}, state, %{task: {_, tmon, _}} = data)
      when state in [:barrier_handoff, :barrier_subtree],
      do: teardown_to(%{data | task: nil}, {:error, %{clause: "run_executor_down"}})

  def handle_event(:info, {:EXIT, tpid, _reason}, _state, %{task: {tpid, _, _}}), do: :keep_state_and_data

  # ---- awaiting: the Server's reply is an event ----
  def handle_event(:info, message, :awaiting, %{request: request} = data) when request != nil do
    case :gen_statem.check_response(message, request) do
      {:reply, result} -> finish(data, result)
      {:error, _reason} -> teardown_to(data, {:error, %{clause: "run_server_down"}})
      :no_reply -> census_or_ignore(message, :awaiting, data)
    end
  end

  # ---- calls ----
  def handle_event({:call, from}, {:await, _timeout}, :terminal, %{result: result}),
    do: {:keep_state_and_data, [{:reply, from, result}]}

  def handle_event({:call, {pid, _} = from}, {:await, timeout}, _state, data) do
    mon = Process.monitor(pid)
    {:keep_state, %{data | waiters: Map.put(data.waiters, mon, from)}, [{{:timeout, {:waiter, mon}}, timeout, :expire}]}
  end

  def handle_event({:call, from}, {:ready, _timeout}, :awaiting, data),
    do: {:keep_state_and_data, [{:reply, from, {:ok, ready_view(data)}}]}

  def handle_event({:call, from}, {:ready, _timeout}, :terminal, %{result: result}),
    do: {:keep_state_and_data, [{:reply, from, result}]}

  def handle_event({:call, {pid, _} = from}, {:ready, timeout}, _state, data) do
    mon = Process.monitor(pid)

    {:keep_state, %{data | ready_waiters: Map.put(data.ready_waiters, mon, from)},
     [{{:timeout, {:waiter, mon}}, timeout, :expire}]}
  end

  # stop: a terminal owner tells the stopper it is retained (idempotent, retention kept); an active owner
  # records the stopper, whose answer is the completed outcome of the teardown that the supervisor's
  # termination (terminate/3) then runs
  def handle_event(:cast, {:stop_waiter, {pid, ref}}, :terminal, _data) do
    send(pid, {:stop_outcome, ref, :retained})
    :keep_state_and_data
  end

  def handle_event(:cast, {:stop_waiter, {pid, ref}}, _state, data),
    do: {:keep_state, %{data | waiters: Map.put(data.waiters, {:stopper, ref}, pid)}}

  def handle_event({:call, from}, :phase, state, _data), do: {:keep_state_and_data, [{:reply, from, state}]}

  def handle_event(:state_timeout, :expire_retention, :terminal, data), do: {:stop, :normal, data}

  # ---- census request from the Monitor: answered only while active and registered ----
  def handle_event(:info, {:census, ref, monitor}, state, %{record: record}) when state != :terminal and record != nil do
    send(monitor, {:census_reply, ref, record, state})
    :keep_state_and_data
  end

  def handle_event(:info, _other, _state, _data), do: :keep_state_and_data

  defp census_or_ignore({:census, ref, monitor}, state, %{record: record}) when record != nil do
    send(monitor, {:census_reply, ref, record, state})
    :keep_state_and_data
  end

  defp census_or_ignore({:run_worker_registered, ref, worker, _server}, _state, %{ref: ref} = data),
    do: {:keep_state, %{data | owned: Map.update(data.owned, :late, [worker], &[worker | &1])}}

  defp census_or_ignore(_message, _state, _data), do: :keep_state_and_data

  # ---- parent shutdown: OTP delivers the parent's EXIT here, in every state ----
  @impl true
  def terminate(_reason, :terminal, _data), do: :ok

  def terminate(_reason, _state, data) do
    _ = teardown_to(data, {:error, %{clause: "run_host_stopped"}})
    :ok
  end

  @impl true
  def format_status(status), do: Map.merge(status, %{data: :redacted, state: Map.get(status, :state)})

  # ---- helpers ----
  defp run_barrier(data, label) do
    owner = self()
    tref = make_ref()
    payload = Map.put(Map.take(data.owned, @identity_roles), :owner, owner)
    %{barrier: barrier, host: host, config: config} = data

    record =
      if label == :subtree_started do
        case Ownership.status(config.run_dir, ownership_opts(config)) do
          {:ok, %{generation: generation}} ->
            HostExecutor.registration(config.run_dir, config.command.run_id, payload, generation)

          _ ->
            nil
        end
      else
        data.record
      end

    pid = spawn_link(fn -> helper(owner, tref, label, payload, barrier, host, record) end)

    %{data | task: {pid, Process.monitor(pid), tref}, record: record}
  end

  # the helper registers (guarded) before the user barrier and reports the barrier's value or its escape as a
  # closed digest, never crashing with a raw reason
  defp helper(owner, tref, label, payload, barrier, host, record) do
    if is_map(record), do: guarded(fn -> Monitor.register(host.monitor, record) end)

    result =
      try do
        Owner.call_barrier(barrier, label, payload)
      catch
        kind, reason -> {:barrier_escape, Owner.owner_failure(kind, reason)}
      end

    send(owner, {:barrier_result, tref, result})
  end

  defp ownership_opts(config), do: config |> Map.get(:opts, []) |> Keyword.get(:ownership, [])

  defp ready_view(data) do
    data.owned
    |> Map.take([:supervisor, :server, :writer, :worker])
    |> Map.put(:generation, if(is_map(data.record), do: data.record.generation))
  end

  defp finish(data, {:ok, %{} = result}) do
    outcome =
      case Owner.observed_close(data.owned.writer, data.budgets.close) do
        :ok -> {:ok, result}
        {:error, rejection} -> {:ok, Map.put(result, :close, {:error, rejection})}
      end

    teardown_to(data, outcome)
  end

  defp finish(data, result), do: teardown_to(data, result)

  # ONE cleanup owner: kill the helper, run the shared teardown over the held identities (late ones included),
  # unregister explicitly, answer every recorded waiter, enter :terminal with retention
  defp teardown_to(data, result) do
    data = %{data | phase: :tearing_down}
    data = kill_helper(data)
    owned = Map.take(data.owned, @identity_roles ++ [:late])
    outcome = Owner.teardown(owned, data.budgets, data.join)
    if is_map(data.record), do: guarded(fn -> Monitor.unregister(data.host.monitor, data.record) end)

    result =
      case outcome do
        :ok -> result
        {:error, incomplete} -> {:error, incomplete}
      end

    terminal(data, result)
  end

  defp kill_helper(%{task: nil} = data), do: data

  defp kill_helper(%{task: {pid, mon, _}} = data) do
    Process.exit(pid, :kill)

    if data.join.(pid, mon, data.budgets.helper_join),
      do: %{data | task: nil},
      else: %{data | task: nil, owned: Map.update(data.owned, :late, [pid], &[pid | &1])}
  end

  defp terminal(data, result) do
    for {mon, from} <- data.waiters do
      case {mon, result} do
        {{:stopper, ref}, {:error, %{clause: "run_host_stopped"}}} -> send(from, {:stop_outcome, ref, {:ok, :stopped}})
        {{:stopper, ref}, other} -> send(from, {:stop_outcome, ref, other})
        _ -> reply_waiter(mon, from, result)
      end
    end

    for {mon, from} <- data.ready_waiters, do: reply_waiter(mon, from, result)
    data = %{data | result: result, waiters: %{}, ready_waiters: %{}, task: nil, request: nil, phase: :terminal}
    {:next_state, :terminal, data, [{:state_timeout, data.retention_ms, :expire_retention}]}
  end

  defp reply_waiter(mon, from, answer) do
    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    :gen_statem.reply(from, answer)
  end

  defp guarded(fun) do
    fun.()
    :ok
  catch
    _kind, _reason -> :ok
  end
end
