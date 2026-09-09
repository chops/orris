defmodule OrrisConsole.SessionStore do
  @moduledoc """
  The console's in-memory session authority (C1-05/08/09): loads the credential digest at start (fails closed),
  serializes login (bounded token bucket, secure digest comparison, capacity 128, fresh random ids), validation
  (:observe never renews idle, :action does; expiry is exact against the configured clock), view registration
  (at most 8 per session, monitored) and revocation (views notified BEFORE revoke returns). Only digests of ids are
  stored. The formatted status and crash reports never carry digests or ids (format_status).

  U1 (docs/contracts/console-mutations.org): THE mutation authority as well. Intents (one pending per session,
  digest only, bound to session + root + run_ref + TTL), acceptance (authorization inside ONE serialized callback:
  session valid for :action → intent → scope → no non-terminal operation → not fenced → reservation), the
  starter/barrier/grant protocol (the starter only submits; the supervisor's own start callback reports the attempt;
  ownership is installed in the same callback as the report; a starter DOWN without a report runs a correlated
  barrier owned here; a report after the visible start budget counts the child until its DOWN without a grant),
  occupancy by monitored identities (slots free only on observed DOWN), the orphan census from MutationRegistry at
  every (re)start with fenced admission, waiters (one per operation, woken by every terminal transition), and the
  session's ONE retained outcome (dropped at retention or session end).
  """
  use GenServer
  alias OrrisConsole.{Config, Credential, MutationOperation, MutationRegistry, MutationWorkers, Session}

  @secret_bytes 32
  @intent_bytes 32
  @barrier_retries 5
  @barrier_retry_ms 100
  @nonterminal [:starting, :running]

  def start_link(%Config{} = config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, restart: :permanent, shutdown: 5_000}

  # ---- C1 API ----
  @spec login(GenServer.server(), term()) ::
          {:ok, binary()} | {:error, :invalid | :rate_limited | :capacity | :unavailable}
  def login(store, submitted) when is_binary(submitted) and byte_size(submitted) == @secret_bytes,
    do: call(store, {:login, submitted})

  # a malformed submission still consumes a login token (the limiter counts attempts) and allocates nothing
  def login(store, _other), do: call(store, {:login, :invalid})

  @spec validate(GenServer.server(), term(), :observe | :action) ::
          {:ok, Session.t()} | {:error, :invalid | :expired | :unavailable}
  def validate(store, id, mode) when is_binary(id) and byte_size(id) in 16..128 and mode in [:observe, :action],
    do: call(store, {:validate, id, mode})

  def validate(_store, _id, _mode), do: {:error, :invalid}

  @spec register_view(GenServer.server(), term(), pid()) ::
          :ok | {:error, :invalid | :expired | :view_capacity | :unavailable}
  def register_view(store, id, pid) when is_binary(id) and is_pid(pid), do: call(store, {:register_view, id, pid})
  def register_view(_store, _id, _pid), do: {:error, :invalid}

  @spec revoke(GenServer.server(), term()) :: :ok
  def revoke(store, id) when is_binary(id), do: call(store, {:revoke, id}) |> then(fn _ -> :ok end)
  def revoke(_store, _id), do: :ok

  @spec revoke_all(GenServer.server()) :: :ok
  def revoke_all(store), do: call(store, :revoke_all) |> then(fn _ -> :ok end)

  @spec counts(GenServer.server()) :: %{sessions: non_neg_integer(), views: non_neg_integer()}
  def counts(store), do: GenServer.call(store, :counts)

  # ---- U1 mutation API ----
  @spec issue_intent(GenServer.server(), term(), term(), term()) ::
          {:ok, binary()} | {:error, :invalid | :expired | :in_progress | :fenced | :unavailable}
  def issue_intent(store, id, root_id, run_ref)
      when is_binary(id) and byte_size(id) in 16..128 and is_binary(root_id) and is_binary(run_ref),
      do: call(store, {:issue_intent, id, root_id, run_ref})

  def issue_intent(_store, _id, _root_id, _run_ref), do: {:error, :invalid}

  @spec accept(GenServer.server(), term(), term(), term(), term()) ::
          {:accepted, reference()}
          | {:error,
             :invalid
             | :expired
             | :intent_invalid
             | :intent_expired
             | :scope
             | :in_progress
             | :busy
             | :fenced
             | :unavailable}
  def accept(store, id, intent, root_id, run_ref)
      when is_binary(id) and byte_size(id) in 16..128 and is_binary(intent) and byte_size(intent) in 16..128 and
             is_binary(root_id) and is_binary(run_ref),
      do: call(store, {:accept, id, intent, root_id, run_ref})

  def accept(_store, id, _intent, _root_id, _run_ref) when is_binary(id) and byte_size(id) in 16..128,
    do: {:error, :intent_invalid}

  def accept(_store, _id, _intent, _root_id, _run_ref), do: {:error, :invalid}

  @spec await(GenServer.server(), reference(), non_neg_integer()) ::
          {:finished, map()}
          | {:unknown, map()}
          | {:failed_to_start, term()}
          | {:refused_at_grant, :session_revoked}
          | :pending
  def await(store, op_ref, timeout_ms) when is_reference(op_ref) and is_integer(timeout_ms) and timeout_ms >= 0 do
    GenServer.call(store, {:await, op_ref, timeout_ms}, timeout_ms + 5_000)
  catch
    :exit, _ -> :pending
  end

  @spec outcome(GenServer.server(), term()) :: {:ok, map()} | :none
  def outcome(store, id) when is_binary(id) and byte_size(id) in 16..128 do
    case call(store, {:outcome, id}) do
      {:ok, record} -> {:ok, record}
      _ -> :none
    end
  end

  def outcome(_store, _id), do: :none

  @spec mutation_status(GenServer.server()) :: %{fenced: boolean(), occupied: non_neg_integer(), operations: map()}
  def mutation_status(store), do: GenServer.call(store, :mutation_status)

  defp call(store, request) do
    GenServer.call(store, request)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  # ---- server ----
  @impl true
  def init(%Config{} = config) do
    case Credential.load(config.credential_path) do
      {:ok, digest} ->
        Process.send_after(self(), :sweep, config.sweep_ms)
        orphans = census()

        {:ok,
         %{
           config: config,
           digest: digest,
           clock: config.clock || fn -> System.monotonic_time(:millisecond) end,
           sessions: %{},
           views: %{},
           bucket: %{tokens: config.login_capacity, last_ms: nil},
           intents: %{},
           ops: %{},
           retained: %{},
           waiters: %{},
           waiter_refs: %{},
           orphans: orphans,
           fenced: map_size(orphans) > 0
         }}

      {:error, reason} ->
        {:stop, {:credential, reason}}
    end
  end

  # the census: every live operation registered in MutationRegistry (never which_children) is monitored as an orphan
  defp census do
    MutationRegistry
    |> Registry.select([{{{:operation, :"$1"}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {_op_ref, pid} -> Process.alive?(pid) end)
    |> Map.new(fn {op_ref, pid} -> {Process.monitor(pid), {op_ref, pid}} end)
  rescue
    ArgumentError -> %{}
  end

  @impl true
  def handle_call({:login, submitted}, _from, state) do
    now = state.clock.()
    {allowed?, bucket} = take_token(state.bucket, state.config, now)

    cond do
      not allowed? ->
        {:reply, {:error, :rate_limited}, %{state | bucket: bucket}}

      submitted == :invalid or not Plug.Crypto.secure_compare(Credential.digest(submitted), state.digest) ->
        {:reply, {:error, :invalid}, %{state | bucket: bucket}}

      map_size(state.sessions) >= state.config.session_capacity ->
        {:reply, {:error, :capacity}, %{state | bucket: bucket}}

      true ->
        id = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

        session = %Session{
          actor_id: state.config.operator.id,
          root_ids: state.config.operator.root_ids,
          issued_ms: now,
          idle_deadline_ms: now + state.config.idle_ms,
          absolute_deadline_ms: now + state.config.absolute_ms
        }

        {:reply, {:ok, id}, %{state | bucket: bucket, sessions: Map.put(state.sessions, key(id), session)}}
    end
  end

  def handle_call({:validate, id, mode}, _from, state) do
    case lookup(state, id) do
      {:ok, k, session} ->
        session =
          if mode == :action, do: %{session | idle_deadline_ms: state.clock.() + state.config.idle_ms}, else: session

        {:reply, {:ok, session}, %{state | sessions: Map.put(state.sessions, k, session)}}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:register_view, id, pid}, _from, state) do
    case lookup(state, id) do
      {:ok, k, _session} ->
        if Enum.count(state.views, fn {_ref, {vk, _pid}} -> vk == k end) >= state.config.views_per_session do
          {:reply, {:error, :view_capacity}, state}
        else
          ref = Process.monitor(pid)
          {:reply, :ok, %{state | views: Map.put(state.views, ref, {k, pid})}}
        end

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:revoke, id}, _from, state), do: {:reply, :ok, drop(state, key(id), id)}

  def handle_call(:revoke_all, _from, state) do
    for {ref, {_k, pid}} <- state.views do
      Process.demonitor(ref, [:flush])
      send(pid, {:session_revoked, :all})
    end

    {:reply, :ok, %{state | sessions: %{}, views: %{}, intents: %{}, retained: %{}}}
  end

  def handle_call(:counts, _from, state),
    do: {:reply, %{sessions: map_size(state.sessions), views: map_size(state.views)}, state}

  # ---- intents ----
  def handle_call({:issue_intent, id, root_id, run_ref}, _from, state) do
    case lookup(state, id) do
      {:ok, k, _session} ->
        state = drop_expired_intent(state, k)

        cond do
          state.fenced ->
            {:reply, {:error, :fenced}, state}

          Map.has_key?(state.intents, k) or nonterminal?(state, k) ->
            {:reply, {:error, :in_progress}, state}

          true ->
            token = Base.url_encode64(:crypto.strong_rand_bytes(@intent_bytes), padding: false)

            intent = %{
              digest: key(token),
              root_id: root_id,
              run_ref: run_ref,
              expires_ms: state.clock.() + state.config.intent_ttl_ms
            }

            {:reply, {:ok, token}, %{state | intents: Map.put(state.intents, k, intent)}}
        end

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  # ---- AUTHORIZATION = this one serialized callback ----
  def handle_call({:accept, id, intent, root_id, run_ref}, _from, state) do
    with {:ok, k, session, state} <- validate_action(state, id),
         {:ok, state} <- check_intent(state, k, intent, root_id, run_ref),
         :ok <- scope(state, session, root_id),
         :ok <- if(nonterminal?(state, k), do: {:error, :in_progress}, else: :ok),
         :ok <- if(state.fenced, do: {:error, :fenced}, else: :ok),
         :ok <- if(occupied(state) >= state.config.mutation_capacity, do: {:error, :busy}, else: :ok) do
      state = %{state | intents: Map.delete(state.intents, k)}
      {op_ref, state} = start_operation(state, k, root_id, run_ref)
      {:reply, {:accepted, op_ref}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:await, op_ref, timeout_ms}, from, state) do
    case Map.get(state.ops, op_ref) do
      nil ->
        {:reply, :pending, state}

      %{state: s} = rec when s in @nonterminal or s == :failed_to_start_visible ->
        _ = rec
        state = reply_waiter(state, op_ref, :pending)
        {pid, _} = from
        mon = Process.monitor(pid)
        timer = Process.send_after(self(), {:wait_timeout, op_ref, from}, timeout_ms)

        {:noreply,
         %{
           state
           | waiters: Map.put(state.waiters, op_ref, %{from: from, mon: mon, timer: timer}),
             waiter_refs: Map.put(state.waiter_refs, mon, op_ref)
         }}

      rec ->
        {:reply, terminal_reply(rec), state}
    end
  end

  def handle_call({:outcome, id}, _from, state) do
    case lookup(state, id) do
      {:ok, k, _session} ->
        case Map.get(state.retained, k) do
          nil ->
            {:reply, :none, state}

          op_ref ->
            case Map.get(state.ops, op_ref) do
              nil ->
                {:reply, :none, state}

              rec ->
                {:reply,
                 {:ok,
                  %{
                    op_ref: op_ref,
                    state: visible_state(rec),
                    outcome: rec.outcome,
                    root_id: rec.root_id,
                    run_ref: rec.run_ref
                  }}, state}
            end
        end

      {:error, _reason, state} ->
        {:reply, :none, state}
    end
  end

  def handle_call(:mutation_status, _from, state) do
    {:reply,
     %{
       fenced: state.fenced,
       occupied: occupied(state),
       operations: Map.new(state.ops, fn {ref, rec} -> {ref, visible_state(rec)} end)
     }, state}
  end

  # ---- start ownership: the supervisor's own report, the visible budget, the barrier ----
  @impl true
  def handle_info({:child_start_attempt, op_ref, result}, state) do
    case Map.get(state.ops, op_ref) do
      nil ->
        {:noreply, state}

      %{attempt_reported: true} ->
        {:noreply, state}

      rec ->
        rec = rec |> Map.put(:attempt_reported, true) |> stop_helper()
        state = put_op(state, rec)

        case result do
          {:ok, pid} ->
            # ownership BEFORE yielding: pid + monitor in this same callback
            rec = %{rec | pid: pid, mon: Process.monitor(pid)}
            witness(state, :owned, op_ref, pid)

            case rec.state do
              :starting -> {:noreply, grant_or_refuse(state, rec)}
              _late -> {:noreply, put_op(state, rec) |> tap(fn s -> witness(s, :counted_late, op_ref, pid) end)}
            end

          {:error, reason} ->
            rec = %{rec | reserved: false, state: :failed_to_start, reason: {:start_error, sanitize(reason)}}

            {:noreply,
             state |> put_op(rec) |> wake(rec) |> collect(op_ref) |> tap(fn s -> witness(s, :released, op_ref) end)}
        end
    end
  end

  def handle_info({:start_timeout, op_ref}, state) do
    case Map.get(state.ops, op_ref) do
      %{state: :starting} = rec ->
        # the VISIBLE budget answers the waiter only; the reservation stays until a disposition
        rec = %{rec | state: :failed_to_start, reason: :start_timeout, visible_expired: true}
        {:noreply, state |> put_op(rec) |> wake(rec)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_barrier, op_ref}, state) do
    case Map.get(state.ops, op_ref) do
      %{attempt_reported: false, helper: nil, reserved: true} = rec -> {:noreply, start_barrier(state, rec)}
      _ -> {:noreply, state}
    end
  end

  def handle_info({:barrier_result, op_ref, attempt, :answered}, state) do
    case Map.get(state.ops, op_ref) do
      %{helper: %{attempt: ^attempt}} = rec ->
        rec = stop_helper(rec)

        if rec.attempt_reported do
          {:noreply, put_op(state, rec)}
        else
          # the supervisor answered after the dead starter's request would have been processed: no request exists
          rec = %{rec | reserved: false, state: :failed_to_start, reason: :no_start_request}

          {:noreply,
           state |> put_op(rec) |> wake(rec) |> collect(op_ref) |> tap(fn s -> witness(s, :released, op_ref) end)}
        end

      _stale_or_unknown ->
        {:noreply, state}
    end
  end

  def handle_info({:finished, op_ref, outcome}, state) do
    case Map.get(state.ops, op_ref) do
      %{state: s} = rec when s in @nonterminal ->
        new_state = if Map.get(outcome, :phase) == :unknown, do: :unknown, else: :finished
        rec = %{rec | state: new_state, outcome: outcome}
        {:noreply, state |> put_op(rec) |> wake(rec) |> tap(fn s -> witness(s, :finished, op_ref) end)}

      _ ->
        # a late finish for an unknown or already terminal operation is dropped
        {:noreply, state}
    end
  end

  # retention expiry: the outcome becomes unreadable NOW (even while the operation's slot is still occupied);
  # the record itself is collected once nothing owned is alive
  def handle_info({:retention_expired, op_ref}, state) do
    case Map.get(state.ops, op_ref) do
      nil ->
        {:noreply, state}

      rec ->
        retained = state.retained |> Enum.reject(fn {_k, r} -> r == op_ref end) |> Map.new()
        {:noreply, %{state | retained: retained} |> put_op(%{rec | expired: true}) |> collect(op_ref)}
    end
  end

  def handle_info({:wait_timeout, op_ref, from}, state) do
    case Map.get(state.waiters, op_ref) do
      %{from: ^from} -> {:noreply, reply_waiter(state, op_ref, :pending)}
      _ -> {:noreply, state}
    end
  end

  def handle_info(:sweep, state) do
    now = state.clock.()

    expired = for {k, s} <- state.sessions, now >= s.idle_deadline_ms or now >= s.absolute_deadline_ms, do: k
    state = Enum.reduce(expired, state, fn k, acc -> drop(acc, k, nil) end)
    Process.send_after(self(), :sweep, state.config.sweep_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    cond do
      Map.has_key?(state.views, ref) ->
        {:noreply, %{state | views: Map.delete(state.views, ref)}}

      Map.has_key?(state.orphans, ref) ->
        orphans = Map.delete(state.orphans, ref)
        {:noreply, %{state | orphans: orphans, fenced: map_size(orphans) > 0}}

      Map.has_key?(state.waiter_refs, ref) ->
        {:noreply, waiter_down(state, ref)}

      true ->
        {:noreply, operation_down(state, ref, pid)}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # the formatted status (:sys.get_status, crash reports) carries counts only: no digests, ids, tokens, paths
  @impl true
  def format_status(status) do
    summary = fn
      %{sessions: s, views: v} = st ->
        %{sessions: map_size(s), views: map_size(v), operations: map_size(Map.get(st, :ops, %{}))}

      other ->
        other
    end

    status |> Map.update(:state, nil, summary) |> Map.put(:message, :redacted) |> Map.put(:queue, :redacted)
  end

  # ---- acceptance helpers ----
  defp validate_action(state, id) do
    case lookup(state, id) do
      {:ok, k, session} ->
        session = %{session | idle_deadline_ms: state.clock.() + state.config.idle_ms}
        {:ok, k, session, %{state | sessions: Map.put(state.sessions, k, session)}}

      {:error, reason, state} ->
        {:error, reason, state}
    end
  end

  defp check_intent(state, k, intent, root_id, run_ref) do
    case Map.get(state.intents, k) do
      nil ->
        {:error, :intent_invalid}

      %{digest: digest, root_id: ir, run_ref: rr, expires_ms: expires} ->
        cond do
          not Plug.Crypto.secure_compare(key(intent), digest) or ir != root_id or rr != run_ref ->
            {:error, :intent_invalid}

          state.clock.() >= expires ->
            {:error, :intent_expired, %{state | intents: Map.delete(state.intents, k)}}

          true ->
            {:ok, state}
        end
    end
  end

  defp scope(state, session, root_id) do
    if root_id in session.root_ids and Map.has_key?(state.config.roots, root_id), do: :ok, else: {:error, :scope}
  end

  defp drop_expired_intent(state, k) do
    case Map.get(state.intents, k) do
      %{expires_ms: expires} ->
        if state.clock.() >= expires, do: %{state | intents: Map.delete(state.intents, k)}, else: state

      nil ->
        state
    end
  end

  defp nonterminal?(state, k),
    do: Enum.any?(state.ops, fn {_ref, rec} -> rec.key == k and rec.state in @nonterminal end)

  defp occupied(state), do: Enum.count(state.ops, fn {_ref, rec} -> rec.reserved end)

  # the store spawns a monitored STARTER that only submits; a start budget is armed; the reservation is recorded
  defp start_operation(state, k, root_id, run_ref) do
    op_ref = make_ref()
    config = state.config
    dir = Map.fetch!(config.roots, root_id)

    args = %{
      op_ref: op_ref,
      authority: self(),
      root_id: root_id,
      run_ref: run_ref,
      dir: dir,
      config: config,
      actor: %{"class" => "console", "id" => config.operator.id}
    }

    {starter, smon} = Process.spawn(fn -> starter(args) end, [:monitor])
    Process.send_after(self(), {:start_timeout, op_ref}, config.mutation_start_ms)

    rec = %{
      op_ref: op_ref,
      key: k,
      root_id: root_id,
      run_ref: run_ref,
      state: :starting,
      reserved: true,
      starter: starter,
      starter_mon: smon,
      attempt_reported: false,
      pid: nil,
      mon: nil,
      helper: nil,
      retries: 0,
      visible_expired: false,
      outcome: nil,
      reason: nil,
      retention_timer: nil,
      expired: false
    }

    # the session's ONE retained record: the previous one is replaced
    state = forget_retained(state, k)
    state = %{state | ops: Map.put(state.ops, op_ref, rec), retained: Map.put(state.retained, k, op_ref)}
    witness(state, :starter, op_ref, starter)
    {op_ref, state}
  end

  defp starter(args) do
    case args.config.starter_gate do
      gate when is_pid(gate) ->
        send(gate, {:mutation, :starter_ready, args.op_ref, self()})

        if is_pid(args.config.mutation_witness) and args.config.mutation_witness != gate,
          do: send(args.config.mutation_witness, {:mutation, :starter_ready, args.op_ref, self()})

        receive do
          :proceed -> :ok
        end

      _ ->
        :ok
    end

    _ = DynamicSupervisor.start_child(MutationWorkers, {MutationOperation, args})
    :ok
  end

  # GRANT = the work-release barrier: revalidate the session and scope in the same callback that installed ownership
  defp grant_or_refuse(state, rec) do
    valid? =
      case Map.fetch(state.sessions, rec.key) do
        {:ok, session} ->
          now = state.clock.()

          now < session.idle_deadline_ms and now < session.absolute_deadline_ms and
            scope(state, session, rec.root_id) == :ok

        :error ->
          false
      end

    if valid? do
      # revalidation only: acceptance's :action validation is the ONLY idle renewal on this path
      rec = %{rec | state: :running}
      send(rec.pid, {:grant, rec.op_ref})
      state = put_op(state, rec)
      witness(state, :granted, rec.op_ref)
      state
    else
      rec = %{rec | state: :refused_at_grant, reason: :session_revoked}
      state = state |> put_op(rec) |> wake(rec)
      witness(state, :refused_at_grant, rec.op_ref)
      state
    end
  end

  # the starter's DOWN without a report: a correlated barrier owned here; child DOWN: the slot frees; helper DOWN
  # without its result: a bounded retry
  defp operation_down(state, ref, _pid) do
    case Enum.find(state.ops, fn {_op, rec} -> rec.starter_mon == ref or rec.mon == ref or helper_mon(rec) == ref end) do
      nil ->
        state

      {_op_ref, %{starter_mon: ^ref} = rec} ->
        rec = %{rec | starter: nil, starter_mon: nil}
        state = put_op(state, rec)
        if rec.attempt_reported or rec.pid != nil or not rec.reserved, do: state, else: start_barrier(state, rec)

      {op_ref, %{mon: ^ref} = rec} ->
        rec = %{rec | reserved: false, pid: nil, mon: nil}

        rec =
          if rec.state in @nonterminal do
            %{
              rec
              | state: :unknown,
                outcome: %{
                  phase: :unknown,
                  invoke: :unknown,
                  observed: nil,
                  message: "Cancel outcome uncertain (unknown): Current journal state unavailable (operation_lost)"
                }
            }
          else
            rec
          end

        state = state |> put_op(rec) |> wake(rec) |> collect(op_ref)
        witness(state, :released, op_ref)
        state

      {_op_ref, rec} ->
        # the barrier helper died without its result: retain the reservation, retry (bounded), never block
        rec = %{rec | helper: nil}

        cond do
          rec.attempt_reported ->
            put_op(state, rec)

          rec.retries < @barrier_retries ->
            Process.send_after(self(), {:retry_barrier, rec.op_ref}, @barrier_retry_ms)
            put_op(state, %{rec | retries: rec.retries + 1})

          true ->
            rec = %{rec | state: :failed_to_start, reason: :barrier_exhausted}
            state |> put_op(rec) |> wake(rec)
        end
    end
  end

  defp helper_mon(%{helper: %{mon: mon}}), do: mon
  defp helper_mon(_), do: nil

  defp start_barrier(state, rec) do
    attempt = make_ref()
    authority = self()
    op_ref = rec.op_ref

    {helper, hmon} =
      Process.spawn(
        fn ->
          _ = DynamicSupervisor.count_children(MutationWorkers)
          send(authority, {:barrier_result, op_ref, attempt, :answered})
        end,
        [:monitor]
      )

    rec = %{rec | helper: %{attempt: attempt, pid: helper, mon: hmon}}
    state = put_op(state, rec)
    witness(state, :barrier_helper, op_ref, {attempt, helper})
    state
  end

  defp stop_helper(%{helper: %{pid: pid, mon: mon}} = rec) do
    Process.demonitor(mon, [:flush])
    if Process.alive?(pid), do: Process.exit(pid, :kill)
    %{rec | helper: nil}
  end

  defp stop_helper(rec), do: rec

  defp put_op(state, rec), do: %{state | ops: Map.put(state.ops, rec.op_ref, rec)}

  defp terminal_reply(%{state: :finished, outcome: outcome}), do: {:finished, outcome}
  defp terminal_reply(%{state: :unknown, outcome: outcome}), do: {:unknown, outcome}
  defp terminal_reply(%{state: :failed_to_start, reason: reason}), do: {:failed_to_start, reason}
  defp terminal_reply(%{state: :refused_at_grant}), do: {:refused_at_grant, :session_revoked}
  defp terminal_reply(_), do: :pending

  defp visible_state(%{state: s}), do: s

  # every terminal transition wakes the operation's waiter; the FIRST terminal transition is the retention origin
  # (repeated reports, wakes or DOWNs never extend it)
  defp wake(state, rec) do
    case rec.state do
      s when s in @nonterminal ->
        state

      _ ->
        state = reply_waiter(state, rec.op_ref, terminal_reply(rec))
        arm_retention(state, Map.get(state.ops, rec.op_ref) || rec)
    end
  end

  defp arm_retention(state, %{retention_timer: nil} = rec) do
    timer = Process.send_after(self(), {:retention_expired, rec.op_ref}, state.config.mutation_retention_ms)
    put_op(state, %{rec | retention_timer: timer})
  end

  defp arm_retention(state, _rec), do: state

  # a record is collected once nothing owned is alive (reserved == false) AND it is no longer presented: expired,
  # replaced by a newer acceptance, or its session ended
  defp collect(state, op_ref) do
    case Map.get(state.ops, op_ref) do
      %{reserved: false} = rec ->
        presented? = Enum.any?(state.retained, fn {_k, r} -> r == op_ref end)

        if rec.expired or not presented? do
          if rec.retention_timer, do: Process.cancel_timer(rec.retention_timer)
          %{state | ops: Map.delete(state.ops, op_ref)}
        else
          state
        end

      _ ->
        state
    end
  end

  defp waiter_down(state, ref) do
    {op_ref, waiter_refs} = Map.pop(state.waiter_refs, ref)
    state = %{state | waiter_refs: waiter_refs}

    case Map.get(state.waiters, op_ref) do
      # the exact dead waiter: cancel its timer and drop it; a stale ref never touches a replacement waiter
      %{mon: ^ref, timer: timer} ->
        Process.cancel_timer(timer)
        %{state | waiters: Map.delete(state.waiters, op_ref)}

      _ ->
        state
    end
  end

  defp reply_waiter(state, op_ref, reply) do
    case Map.pop(state.waiters, op_ref) do
      {nil, _} ->
        state

      {%{from: from, mon: mon, timer: timer}, waiters} ->
        Process.demonitor(mon, [:flush])
        Process.cancel_timer(timer)
        GenServer.reply(from, reply)
        %{state | waiters: waiters, waiter_refs: Map.delete(state.waiter_refs, mon)}
    end
  end

  # the session's previous record is no longer presented; it is collected now if nothing owned is alive, else at
  # its last owned DOWN
  defp forget_retained(state, k) do
    case Map.pop(state.retained, k) do
      {nil, _} -> state
      {op_ref, retained} -> collect(%{state | retained: retained}, op_ref)
    end
  end

  defp witness(%{config: %{mutation_witness: pid}}, kind, op_ref, payload) when is_pid(pid),
    do: send(pid, {:mutation, kind, op_ref, payload})

  defp witness(_state, _kind, _op_ref, _payload), do: :ok

  defp witness(%{config: %{mutation_witness: pid}}, kind, op_ref) when is_pid(pid),
    do: send(pid, {:mutation, kind, op_ref})

  defp witness(_state, _kind, _op_ref), do: :ok

  defp sanitize(reason) when is_atom(reason), do: reason
  defp sanitize(_reason), do: :start_failed

  # ---- C1 helpers ----
  defp key(id), do: :crypto.hash(:sha256, id)

  defp lookup(state, id) do
    k = key(id)

    case Map.fetch(state.sessions, k) do
      :error ->
        {:error, :invalid, state}

      {:ok, session} ->
        now = state.clock.()

        if now >= session.idle_deadline_ms or now >= session.absolute_deadline_ms,
          do: {:error, :expired, drop(state, k, nil)},
          else: {:ok, k, session}
    end
  end

  # removes a session, its views, its pending intent and its retained outcome; each view receives the revocation
  # notice BEFORE the caller is answered; accepted operations of the session complete on their own
  defp drop(state, k, id) do
    {gone, kept} = Enum.split_with(state.views, fn {_ref, {vk, _pid}} -> vk == k end)

    for {ref, {_k, pid}} <- gone do
      Process.demonitor(ref, [:flush])
      send(pid, {:session_revoked, id || :expired})
    end

    state = %{
      state
      | sessions: Map.delete(state.sessions, k),
        views: Map.new(kept),
        intents: Map.delete(state.intents, k),
        retained: Map.delete(state.retained, k)
    }

    # the session's records are no longer presented: collect the unreserved ones now (reserved ones at their DOWN)
    state.ops
    |> Enum.filter(fn {_ref, rec} -> rec.key == k end)
    |> Enum.reduce(state, fn {op_ref, _rec}, acc -> collect(acc, op_ref) end)
  end

  defp take_token(%{tokens: tokens, last_ms: last}, config, now) do
    refilled = if last, do: min(config.login_capacity, tokens + div(now - last, config.login_refill_ms)), else: tokens
    last = if last && refilled > tokens, do: now, else: last || now

    if refilled > 0, do: {true, %{tokens: refilled - 1, last_ms: last}}, else: {false, %{tokens: 0, last_ms: last}}
  end
end
