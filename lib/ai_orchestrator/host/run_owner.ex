defmodule AiOrchestrator.Host.RunOwner do
  @moduledoc """
  The hosted harness owner of one mounted run (docs/contracts/host-mounted-runs.org): ONE responsive
  `:gen_statem` that is the linked parent of `AiOrchestrator.Run.Supervisor`, holds every owned identity in
  its own data, runs the user barrier in a linked+monitored helper (mounted-route semantic), receives the
  parent's EXIT in `terminate/3`, and runs the ONE teardown implementation shared with the foreground owner
  (`AiOrchestrator.Run.Executor.Owner`).

  States: `:starting` (RESPONSIVE: the blocking start runs in the `AiOrchestrator.Run.Executor.Startup` seam's
  starter, under ONE absolute deadline this owner fixes at its own birth), `:handoff`, `:barrier_handoff`,
  `:barrier_subtree`, `:awaiting`, `:terminal`. Teardown runs inside the handler or `terminate/3` that enters it
  (never observable as a state); `inspect/1` reports the last phase. An expiry with no completion, a completion
  accepted after the deadline and a denied or late-consumed permit all end in a RETAINED terminal
  `{:error, run_startup_timeout}` (docs/contracts/core-startup-bound.org).

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
  alias AiOrchestrator.Run.Executor.Startup

  @default_budgets %{
    close: 15_000,
    stop: 15_000,
    join: 5_000,
    handoff: 5_000,
    helper_join: 1_000,
    startup: 45_000,
    ack: 5_000
  }
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
  @spec inspect(pid(), timeout()) :: %{
          phase: atom(),
          waiters: non_neg_integer(),
          handoff_ref: reference(),
          owned: [atom()]
        }
  def inspect(owner, timeout \\ 5_000) do
    {_state, data} = :sys.get_state(owner, timeout)

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
    # the ONE absolute startup deadline is fixed at this process's birth, before any other process exists
    birth = System.monotonic_time(:millisecond)
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
      birth: birth,
      startup: nil,
      owned: %{},
      task: nil,
      registered: nil,
      request: nil,
      waiters: %{},
      ready_waiters: %{},
      stoppers: %{},
      result: nil,
      record: nil,
      budgets: Map.merge(@default_budgets, Map.get(args, :budgets, %{})),
      join: Map.get(args, :join, &Owner.joined?/3),
      retention_ms: Map.get(args, :retention_ms, @default_retention_ms),
      phase: :starting
    }

    {:ok, :starting, data, [{:next_event, :internal, :start}]}
  end

  # ---- starting: RESPONSIVE; the blocking start lives in the seam's starter under the owner's own deadline ----
  @impl true
  def handle_event(:internal, :start, :starting, data) do
    {:ok, startup} = Startup.begin(data.config, startup_budgets(data))

    {:keep_state, %{data | startup: startup},
     [{:state_timeout, max(startup.deadline - System.monotonic_time(:millisecond), 0), :startup_deadline}]}
  end

  # identity before work: the owner already holds the starter (begin/2); the permit is granted only inside the deadline
  # the identity confirms the starter begin/2 already handed this owner; the permit is granted only inside the
  # absolute deadline, and the owner holds that identity for cleanup either way
  def handle_event(:info, {:startup_identity, ref, starter}, :starting, %{startup: %{ref: ref, starter: starter}} = data) do
    case Startup.permit(data.startup) do
      :ok -> :keep_state_and_data
      {:error, :late_identity} -> expired(data, Startup.await_report(data.startup))
    end
  end

  # every completion is compared with the absolute deadline HERE, at acceptance
  def handle_event(:info, {:startup_started, ref, _completion} = completion, :starting, %{startup: %{ref: ref}} = data) do
    case Startup.accept(data.startup, completion) do
      {:ok, sup, facts} ->
        data = %{data | owned: Map.put(facts, :supervisor, sup), phase: :handoff}
        {:next_state, :handoff, data, [{:state_timeout, data.budgets.handoff, :handoff_budget}]}

      {:error, %{clause: "run_startup_timeout"} = result} ->
        terminal(%{data | startup: nil}, {:error, result})

      {:error, rejection} ->
        _ = Startup.teardown(data.startup, teardown_budgets(data))
        terminal(%{data | startup: nil}, {:error, rejection})
    end
  end

  # a reap the helper performed on its own (a starter that refused an expired permit)
  def handle_event(:info, {:startup_aborted, ref, report}, :starting, %{startup: %{ref: ref}} = data) do
    :ok = Startup.join(data.startup)
    terminal(%{data | startup: nil}, {:error, Startup.timeout_result(report)})
  end

  # a real expiry: no completion at all reached this owner inside its own clock
  def handle_event(:state_timeout, :startup_deadline, :starting, %{startup: startup} = data) when startup != nil,
    do: expired(data, Startup.abort(startup, :deadline))

  # The reaper died. Its death establishes nothing about the subtree - the starter traps that exit and goes on
  # holding a live Run.Supervisor - so the owner performs its own mirror duty through the starter, in EVERY live
  # phase and not merely during the startup leg (review R2). A helper that exits at the end of an orderly teardown
  # never reaches here: that DOWN is consumed and its link dropped inside the seam's own join.
  def handle_event(:info, {:DOWN, mon, :process, _helper, _reason}, state, %{startup: %{helper_monitor: mon}} = data)
      when state != :terminal, do: teardown_to(data, {:error, %{clause: "run_executor_down"}})

  # ---- the subtree's exit, forwarded by the starter that parents it, in any live state ----
  # The reference carried here is the STARTUP generation, not the worker-handoff reference this owner minted in
  # init/1; matching the wrong one silently ignored a real supervisor death and left a held barrier waiting for a
  # tree that no longer existed (review R1). Both the generation and the supervisor identity must match, so a late
  # or foreign forward can never tear down a live run.
  def handle_event(
        :info,
        {:startup_subtree_exit, ref, sup, _reason},
        state,
        %{startup: %{ref: ref}, owned: %{supervisor: sup}} = data
      )
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
  # The Server produces the handoff while the subtree is starting, so a RESPONSIVE :starting owner can see it
  # before it accepts the completion. That is the ORDINARY handoff arriving early, never a late identity: it is
  # postponed and processed in :handoff exactly as it was when this state blocked (a startup that ends in
  # :terminal instead re-delivers it there, where the identity is collected).
  def handle_event(:info, {:run_worker_registered, ref, _worker, _server}, :starting, %{ref: ref}),
    do: {:keep_state_and_data, [:postpone]}

  def handle_event(:info, {:run_worker_absent, ref, _clause}, :starting, %{ref: ref}),
    do: {:keep_state_and_data, [:postpone]}

  def handle_event(:info, {:run_worker_registered, ref, worker, server}, :handoff, %{ref: ref} = data) do
    data = %{data | owned: Map.put(data.owned, :worker, worker), registered: {ref, server}, phase: :barrier_handoff}
    {:next_state, :barrier_handoff, run_barrier(data, :handoff_received)}
  end

  def handle_event(:info, {:run_worker_absent, ref, _clause}, :handoff, %{ref: ref} = data),
    do: {:next_state, :barrier_subtree, run_barrier(%{data | phase: :barrier_subtree}, :subtree_started)}

  def handle_event(:state_timeout, :handoff_budget, :handoff, data),
    do: {:next_state, :barrier_subtree, run_barrier(%{data | phase: :barrier_subtree}, :subtree_started)}

  # a matched identity reaching a RETAINED owner (its tree is gone) is collected at once, never left uncollected
  def handle_event(:info, {:run_worker_registered, ref, worker, _server}, :terminal, %{ref: ref} = data) do
    _ = collect([worker], data.budgets, data.join)
    :keep_state_and_data
  end

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

  # stop protocol: the owner ACKNOWLEDGES from any responsive state. A terminal owner answers :retained and
  # keeps its retention. An active owner records the stopper (caller monitor + deadline), answers :stopping
  # and arms its OWN teardown as the next internal event: the obligation is in this process, independent of
  # whether the caller keeps waiting. The completed outcome reaches every live recorded stopper.
  # the reply reaches the stop agent BEFORE the caller is informed: an agent that sees no reply under its freeze
  # knows the caller has not been told :retained either (docs/contracts/host-mounted-runs.org, stop arbitration)
  def handle_event({:call, from}, {:stop_request, {pid, ref, _deadline_ms}}, :terminal, _data) do
    :ok = :gen_statem.reply(from, {:ack, :retained})
    send(pid, {:stop_outcome, ref, :retained})
    :keep_state_and_data
  end

  def handle_event({:call, from}, {:stop_request, {pid, ref, deadline_ms}}, _state, data) do
    mon = Process.monitor(pid)
    stoppers = Map.put(data.stoppers, ref, %{pid: pid, mon: mon})

    actions = [
      {:reply, from, {:ack, :stopping}},
      {{:timeout, {:stopper, ref}}, deadline_ms, :expire},
      {:next_event, :internal, :stop_now}
    ]

    {:keep_state, %{data | stoppers: stoppers}, actions}
  end

  def handle_event(:internal, :stop_now, :terminal, _data), do: :keep_state_and_data

  def handle_event(:internal, :stop_now, _state, data) do
    {:next_state, :terminal, data, _actions} = teardown_to(data, {:error, %{clause: "run_host_stopped"}})
    {:stop, :normal, data}
  end

  def handle_event({:timeout, {:stopper, ref}}, :expire, _state, data), do: {:keep_state, drop_stopper(data, ref)}

  def handle_event(:info, {:DOWN, mon, :process, _pid, _}, _state, %{stoppers: stoppers} = data)
      when map_size(stoppers) > 0 do
    case Enum.find(stoppers, fn {_ref, %{mon: m}} -> m == mon end) do
      {ref, _} -> {:keep_state, drop_stopper(data, ref)}
      nil -> :keep_state_and_data
    end
  end

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
  defp expired(data, report), do: terminal(%{data | startup: nil}, {:error, Startup.timeout_result(report)})

  defp startup_budgets(%{budgets: budgets, birth: birth, join: join}) do
    budgets
    |> Map.take([:startup, :ack, :stop, :join])
    |> Map.merge(%{birth: birth, join_fn: join})
  end

  defp teardown_budgets(%{budgets: budgets, join: join}),
    do: budgets |> Map.take([:stop, :join]) |> Map.put(:join_fn, join)

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

  defp release_links(owned) do
    for pid <- owned |> Map.values() |> List.flatten(), is_pid(pid), do: Process.unlink(pid)
    :ok
  end

  # ONE cleanup owner: kill the helper, run the shared teardown over the held identities (late ones included),
  # unregister explicitly, answer every recorded waiter, enter :terminal with retention
  defp teardown_to(data, result) do
    data = %{data | phase: :tearing_down}
    data = kill_helper(data)
    owned = Map.take(data.owned, @identity_roles ++ [:late])
    {outcome, data} = collapse(data, owned, result)
    # by construction, never by scheduling: a terminal owner holds no link to any owned identity, survivors
    # included (a survivor is a pid whose DOWN the join did not observe in time, never a process that can be
    # kept alive), so the stop arbitration's "linked run supervisor => not terminal" evidence is exact
    release_links(owned)
    # the producer (Server) is dead after the teardown above, so one sweep of the identities matching this
    # owner's reference that reached the mailbox (including during the teardown) is complete and finite
    swept = sweep(data.ref, data.budgets, data.join, 0)
    if is_map(data.record), do: guarded(fn -> Monitor.unregister(data.host.monitor, data.record) end)

    result =
      case {outcome, swept} do
        {:ok, 0} -> result
        {:ok, n} -> {:error, %{clause: "run_executor_teardown_incomplete", survivors: n}}
        {{:error, %{survivors: n} = incomplete}, m} -> {:error, %{incomplete | survivors: n + m}}
      end

    terminal(data, result)
  end

  # the ONE cleanup path over the startup seam: while the tree exists the helper runs the shared orderly
  # teardown and releases the starter; before it exists (a stop or a parent EXIT during the startup leg) the
  # helper reaps what was born and the starter is reaped with it
  defp collapse(%{startup: nil} = data, owned, _result), do: {Owner.teardown(owned, data.budgets, data.join), data}

  defp collapse(%{startup: startup} = data, owned, result) do
    outcome =
      if is_pid(owned[:supervisor]) do
        :ok = Startup.own(startup, owned)
        Startup.teardown(startup, teardown_budgets(data))
      else
        startup |> Startup.abort(abort_why(result)) |> incomplete()
      end

    {outcome, %{data | startup: nil}}
  end

  defp abort_why({:error, %{clause: "run_host_stopped"}}), do: :stop
  defp abort_why({:error, %{clause: clause}}), do: {:owner_failure, clause}
  defp abort_why(_result), do: :stop

  defp incomplete(%{survivors: 0}), do: :ok
  defp incomplete(%{survivors: n}), do: {:error, %{clause: "run_executor_teardown_incomplete", survivors: n}}

  # drain every queued identity matching this owner's reference, kill and join each; unobserved joins count
  defp sweep(ref, budgets, join, unobserved) do
    receive do
      {:run_worker_registered, ^ref, worker, _server} ->
        sweep(ref, budgets, join, unobserved + collect([worker], budgets, join))
    after
      0 -> unobserved
    end
  end

  defp collect(pids, budgets, join) do
    monitors = for pid <- pids, is_pid(pid), do: {pid, Process.monitor(pid)}
    for {pid, _} <- monitors, Process.alive?(pid), do: Process.exit(pid, :kill)
    Enum.count(monitors, fn {pid, mon} -> not join.(pid, mon, Map.get(budgets, :join, 5_000)) end)
  end

  defp drop_stopper(data, ref) do
    case Map.pop(data.stoppers, ref) do
      {nil, _} ->
        data

      {%{mon: mon}, rest} ->
        Process.demonitor(mon, [:flush])
        %{data | stoppers: rest}
    end
  end

  defp kill_helper(%{task: nil} = data), do: data

  defp kill_helper(%{task: {pid, mon, _}} = data) do
    Process.exit(pid, :kill)

    if data.join.(pid, mon, data.budgets.helper_join),
      do: %{data | task: nil},
      else: %{data | task: nil, owned: Map.update(data.owned, :late, [pid], &[pid | &1])}
  end

  defp terminal(data, result) do
    for {mon, from} <- data.waiters, do: reply_waiter(mon, from, result)

    for {ref, %{pid: pid, mon: mon}} <- data.stoppers do
      Process.demonitor(mon, [:flush])
      answer = if match?({:error, %{clause: "run_host_stopped"}}, result), do: {:ok, :stopped}, else: result
      if Process.alive?(pid), do: send(pid, {:stop_outcome, ref, answer})
    end

    for {mon, from} <- data.ready_waiters, do: reply_waiter(mon, from, result)

    data = %{
      data
      | result: result,
        waiters: %{},
        ready_waiters: %{},
        stoppers: %{},
        task: nil,
        request: nil,
        phase: :terminal
    }

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
