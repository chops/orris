defmodule AiOrchestrator.Run.Worker do
  @moduledoc """
  The run's effect owner: the one `:temporary` child of `Run.Work.Supervisor`, born by `Run.Server` after
  discovery. It owns the `Effects.Runtime` (gate handles, Ports, process-local memos) for the whole run; the
  runtime and any `Effects.Interrupted` never leave this process. It executes exactly what the Server hands it
  through the correlated protocol (admit / release_terminal / execute / settle) and answers every request with
  `(cap, gen, op_ref)` and its own pid as the CLAIMED sender (intra-BEAM correlation, not authentication).

  A trappable failure inside an effect settles the LATEST runtime in the same invocation and is answered as a
  CLOSED `effect_failed` (kind, result class, digest, stack depth, cleanup summary): this process never crashes
  from an effect, so its exit reason is the supervisor's `:shutdown`, carrying nothing. Its state and messages
  are redacted from every OTP report.

  Fenced deadline actuation (docs/contracts/observe-deadline.org U2a-1O, docs/contracts/delivery-deadline.org):
  an Observe, Dispatch or ReconcileSend is armed ONCE at dequeue with `Run.DeadlineFence` from one wall and one
  monotonic read of the configured clock; an already-due deadline is answered by the effect's own timeout
  observation without any adapter entry; otherwise the adapter closure runs in a task under this process's own
  `Task.Supervisor` while this process waits in bounded chunks and, when the fence is due, brutally kills the task
  and answers the expiry. Fence facts (test-only observer) carry the kind of the operation they describe; a
  retired operation's stale wake keeps that operation's kind, an unknown wake carries none. A birth bootstrap (a zero-arity closure) may supply test-only
  seams; it is executed under the closed failure boundary and never rendered.
  """
  use GenServer

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.AdapterFailure
  alias AiOrchestrator.Effects.AdapterRunner
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Run.DeadlineFence

  @default_cap_ms 60_000
  @boot_keys [:task_supervisor_start, :fence_observer, :fence_hold, :fence_cap_ms, :retention_observer]
  @dispositions [:staged, :foreign, :finalized, :overflow, :not_owner, :unsupported]
  @hold_stages [:before_go, :after_expiry]
  @death_join_ms 5_000

  @spec child_spec(pid() | {pid(), (-> map())}) :: Supervisor.child_spec()
  def child_spec(server) when is_pid(server),
    do: %{id: :worker, start: {__MODULE__, :start_link, [server]}, restart: :temporary, shutdown: 1_000}

  def child_spec({server, bootstrap}) when is_pid(server) and is_function(bootstrap, 0),
    do: %{id: :worker, start: {__MODULE__, :start_link, [server, bootstrap]}, restart: :temporary, shutdown: 1_000}

  @spec start_link(pid()) :: GenServer.on_start()
  def start_link(server) when is_pid(server), do: GenServer.start_link(__MODULE__, {server, &empty_bootstrap/0})

  @doc "Birth with a zero-arity bootstrap closure yielding only the closed seam keys; executed under init's boundary."
  @spec start_link(pid(), (-> map())) :: GenServer.on_start()
  def start_link(server, bootstrap) when is_pid(server) and is_function(bootstrap, 0),
    do: GenServer.start_link(__MODULE__, {server, bootstrap})

  defp empty_bootstrap, do: %{}

  @impl true
  def init({server, bootstrap}) do
    with {:ok, boot} <- boot(bootstrap),
         {:ok, task_sup} <- start_task_supervisor(boot) do
      state = %{
        server: server,
        cap: nil,
        gen: nil,
        runtime: nil,
        seams: [],
        boot: boot,
        task_sup: task_sup,
        fence: nil,
        retired: %{},
        pending: nil
      }

      observe_fact(Map.get(boot, :fence_observer), nil, nil, {:task_supervisor, task_sup}, nil)
      {:ok, state}
    else
      {:error, clause} -> {:stop, {:shutdown, clause}}
    end
  end

  @impl true
  def handle_info({:admit, cap, gen, seams}, %{runtime: nil} = state) when is_reference(cap) and is_list(seams) do
    case fence_config(seams, state.boot) do
      {:ok, fence} ->
        send(state.server, {:admitted, cap, gen, self()})
        {:noreply, %{state | cap: cap, gen: gen, runtime: Runtime.new(seams), seams: seams, fence: fence}}

      {:error, clause} ->
        {:stop, {:shutdown, clause}, state}
    end
  end

  def handle_info({:release_terminal, cap, gen, ref, suffix}, %{cap: cap, gen: gen} = state) when is_list(suffix) do
    state = %{state | runtime: Effects.release_terminal(state.runtime, suffix)}
    send(state.server, {:released, cap, gen, ref, self()})
    {:noreply, state}
  end

  # ---- the owner-resident AwaitGate (docs/contracts/gate-async-await-proposal.org: AW-M3, D-6, D-9) ----

  # D-9: while an await is pending EVERY execute (same ref, another ref, foreign cap/gen) is dropped without execution,
  # observation, abandonment or reply; the pending operation keeps its sole eventual result (fact only, D-10)
  def handle_info({:execute, _cap, _gen, _ref, _intent, _receipt}, %{pending: %{} = pending} = state) do
    gate_fact(state, pending_identity(state, pending), :pending_execute)
    {:noreply, state}
  end

  # the pending op's own Port record resumes it; the accessor is re-read from the LATEST runtime first (R1), and the
  # returned runtime is stored with pending cleared BEFORE the single answer; other owned Ports keep the staging route
  def handle_info({port, _payload} = message, %{pending: %{port: port} = pending} = state) when is_port(port) do
    {:noreply,
     complete_pending(state, :resume, fn -> Effects.resume(state.runtime, pending.key, message, inputs(state)) end)}
  end

  # the Server's deadline actuation, correlated on cap, gen and the pending op's ref: the owner-only settle primitive
  def handle_info({:gate_deadline, cap, gen, ref}, %{cap: cap, gen: gen, pending: %{ref: ref} = pending} = state) do
    {:noreply,
     complete_pending(state, :settle_await, fn -> Effects.settle_await(state.runtime, pending.key, inputs(state)) end)}
  end

  # every other wake (foreign cap / generation / ref, or a late wake after completion) is a fact only: no reply
  # (D-10: the fact carries a VALIDATED plain identity or nil, never the claim's bytes)
  def handle_info({:gate_deadline, cap, gen, ref}, state) do
    gate_fact(state, wake_identity(cap, gen, ref), {:stale, wake_class(state, cap, gen, ref)})
    {:noreply, state}
  end

  # the arming (clock reads, fence) runs INSIDE the owned boundary: a failing clock or observer settles the
  # latest runtime and answers a closed effect_failed exactly like a failing adapter
  def handle_info({:execute, cap, gen, ref, %Effect.Observe{} = intent, receipt}, %{cap: cap, gen: gen} = state),
    do: {:noreply, execute_fenced(state, ref, intent, receipt)}

  # the delivery effects (U2b) are ADMITTED only with a non-negative integer deadline: nil never disables the
  # fence; a missing or malformed deadline is a closed refusal (no adapter entry) under the same boundary
  def handle_info({:execute, cap, gen, ref, %Effect.Dispatch{} = intent, receipt}, %{cap: cap, gen: gen} = state),
    do: {:noreply, execute_fenced(state, ref, intent, receipt)}

  def handle_info({:execute, cap, gen, ref, %Effect.ReconcileSend{} = intent, receipt}, %{cap: cap, gen: gen} = state),
    do: {:noreply, execute_fenced(state, ref, intent, receipt)}

  # every other effect begins through Effects.begin/3 (AW-M6: `{:done, execute(...)}` unless an AwaitGate opts in);
  # capability selection, the begin and the validation of a pending return run INSIDE the boundary (latest runtime)
  def handle_info({:execute, cap, gen, ref, intent, receipt}, %{cap: cap, gen: gen} = state) do
    outcome = pending_boundary(state.runtime, fn -> begin_pending(state, intent, receipt) end)
    {:noreply, complete(state, ref, outcome)}
  end

  def handle_info({:settle, cap, gen, ref}, %{cap: cap, gen: gen} = state) do
    {cleanup, runtime} = Effects.settle(state.runtime)
    send(state.server, {:settled, cap, gen, ref, self(), cleanup})
    {:noreply, %{state | runtime: runtime, pending: nil}}
  end

  # a wake that reaches the loop-free owner belongs to a retired op (its timer outlived it) or to nobody: a retired
  # op's fact keeps that op's kind and is filtered by it; an unknown wake carries no kind (none is invented)
  def handle_info({:observe_fence_wake, identity}, %{fence: fence} = state) when is_map(fence) do
    case stale_class(state, identity) do
      :retired -> retired_fact(fence, state.retired, identity, nil)
      class -> observe_fact(fence.observer, identity, nil, {:stale, class}, nil)
    end

    {:noreply, state}
  end

  # an owned gate record reaching the loop (idle owner, or a loop return between operations) is STAGED for the
  # executor's next read instead of being dropped (docs/contracts/gate-record-retention-proposal.org); the routing
  # verdict only ever reaches the closed test-only observer, never the Server, and nothing else changes here
  def handle_info({port, _payload} = message, %{runtime: %Runtime{}} = state) when is_port(port) do
    _ = route_port_message(state, message)
    {:noreply, state}
  end

  # anything uncorrelated (wrong cap/gen, unknown shapes) is ignored here; the Server counts its own drops
  def handle_info(_other, state), do: {:noreply, state}

  # walk the owned entries in key order; `:foreign` continues, any other disposition is the verdict; an executor
  # without stage/2 (or no verdict) leaves today's drop in place
  # nothing owned means nothing to route: the executor selection is not even looked at (RG-M1); a selection that
  # is not a loadable module, or has no stage/2, is a closed "no capability", never a crash
  defp route_port_message(%{runtime: runtime, seams: seams, boot: boot}, message) do
    case Effects.owned_entries(runtime) do
      [] -> :unrouted
      entries -> stage_with_capability(stage_capability(Effects.gate_executor(seams)), entries, boot, message)
    end
  end

  defp stage_with_capability({:ok, mod}, entries, boot, message), do: stage_entries(entries, mod, boot, message)
  defp stage_with_capability(:none, _entries, _boot, _message), do: :unrouted

  defp stage_capability(mod) when is_atom(mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :stage, 2), do: {:ok, mod}, else: :none
  end

  defp stage_capability(_other), do: :none

  defp stage_entries(entries, mod, boot, message) do
    Enum.reduce_while(entries, :unrouted, fn {key, handle}, _acc ->
      case stage_guarded(mod, handle, message) do
        :foreign -> {:cont, :unrouted}
        disposition -> {:halt, retention_fact(boot, key, disposition)}
      end
    end)
  end

  # the callback boundary: any escape (raise/throw/exit) or out-of-set return is the closed `:stage_failed`,
  # which certifies nothing about retention and rolls nothing back (the executor's own writes stay its own)
  defp stage_guarded(mod, handle, message) do
    case mod.stage(handle, message) do
      disposition when disposition in @dispositions -> disposition
      _other -> :stage_failed
    end
  catch
    _kind, _reason -> :stage_failed
  end

  # the observer sees the verdict AFTER it is taken (payload-free identity = the Runtime key); its return is
  # ignored and its own escape is contained, so it can neither change the verdict nor take the owner down
  defp retention_fact(boot, key, disposition) do
    case Map.get(boot, :retention_observer) do
      nil -> disposition
      observer -> observe_retention(observer, key, disposition)
    end
  end

  defp observe_retention(observer, key, disposition) do
    _ = observer.({:retention, key, disposition})
    disposition
  catch
    _kind, _reason -> disposition
  end

  defp execute_fenced(state, ref, intent, receipt) do
    op = %{cap: state.cap, gen: state.gen, ref: ref}

    outcome =
      boundary(state.runtime, fn ->
        admit_deadline!(intent)
        seams = Keyword.put(state.seams, :adapter_runner, observe_runner(state, op, intent))
        Effects.execute(intent, state.runtime, opts: seams, receipt: receipt)
      end)

    state = answer(state, ref, outcome)
    %{state | retired: Map.put(state.retired, ref, intent.__struct__)}
  end

  # Observe's deadline is an enforced struct key; the delivery effects carry an optional one that this owner
  # refuses closed unless it is a non-negative integer (the refusal is described by its clause atom)
  defp admit_deadline!(%Effect.Observe{}), do: :ok
  defp admit_deadline!(%{deadline_unix: d}) when is_integer(d) and d >= 0, do: :ok
  defp admit_deadline!(_intent), do: :erlang.error(:dispatch_deadline_missing)

  # every OTP report of this process (crash report, sys status) renders only closed values
  @impl true
  def format_status(status) when is_map(status) do
    status
    |> Map.replace_lazy(:state, fn _ -> :redacted end)
    |> Map.replace_lazy(:message, fn _ -> :redacted end)
    |> Map.replace_lazy(:log, fn _ -> [] end)
    |> Map.replace_lazy(:queue, fn _ -> [] end)
  end

  # the returned runtime is stored BEFORE the single reply is sent (AW-M3 order), for success and failure alike
  defp answer(state, ref, {:ok, observation, runtime}) do
    state = %{state | runtime: runtime}
    send(state.server, {:effect_result, state.cap, state.gen, ref, self(), observation})
    state
  end

  defp answer(state, ref, {:failed, closed, runtime}) do
    state = %{state | runtime: runtime}
    send(state.server, {:effect_failed, state.cap, state.gen, ref, self(), closed})
    state
  end

  # the effect boundary: the observation, or the CLOSED failure with the latest runtime settled in-invocation
  defp boundary(runtime, fun) do
    guarded(runtime, fn ->
      {observation, runtime} = fun.()
      {:ok, observation, runtime}
    end)
  end

  # the same boundary over begin/3's grammar: a done answer, a validated pending runtime, or the closed failure; a
  # return outside the grammar is closed with the runtime given (no stack, no payload)
  defp pending_boundary(runtime, fun) do
    guarded(runtime, fn ->
      case fun.() do
        {:done, {observation, %Runtime{} = returned}} -> {:ok, observation, returned}
        {:pending, %Runtime{} = returned, %{key: _, port: _} = pending} -> {:pending, returned, pending}
        _other -> closed(:error, {:invalid_effect_return, :begin}, [], runtime)
      end
    end)
  end

  defp begin_pending(state, intent, receipt) do
    case Effects.begin(intent, state.runtime, opts: state.seams, receipt: receipt) do
      {:pending, %Runtime{} = runtime} -> {:pending, runtime, pending_of(runtime, intent)}
      done -> done
    end
  end

  # the pending identity is read back from the LATEST runtime through the opaque accessor and must name this effect
  defp pending_of(runtime, intent) do
    case Effects.pending(runtime) do
      {:ok, %{key: key, port: port}} when key == {intent.gate_run_id, intent.attempt} ->
        %{key: key, port: port}

      other ->
        raise Effects.Interrupted,
          kind: :error,
          reason: {:pending_invalid, disposition(other)},
          stacktrace: [],
          runtime: runtime
    end
  end

  # R1: before resuming or settling, the accessor is re-read from the latest runtime and compared with the cached
  # identity; stale metadata never routes a record or resurrects an operation, it is one closed failure
  defp complete_pending(%{pending: %{ref: ref} = pending} = state, entry, fun) do
    outcome =
      pending_boundary(state.runtime, fn ->
        case Effects.pending(state.runtime) do
          {:ok, %{key: key, port: port}} when key == pending.key and port == pending.port ->
            # the primitive-entry fact is emitted only on the validated path, immediately before the call
            gate_fact(state, pending_identity(state, pending), entry)
            fun.()

          :none ->
            :erlang.error({:pending_stale, :none})

          {:ok, _other} ->
            :erlang.error({:pending_stale, :mismatch})

          {:error, %{clause: clause}} ->
            :erlang.error({:pending_invalid, clause})
        end
      end)

    complete(state, ref, outcome)
  end

  defp disposition(:none), do: :none
  defp disposition({:ok, _other}), do: :mismatch
  defp disposition({:error, %{clause: clause}}), do: clause

  # the returned runtime is stored and the pending identity set or cleared FIRST; a pending begin sends nothing,
  # every other outcome is answered exactly once
  defp complete(state, ref, {:pending, runtime, %{key: key, port: port}}) do
    pending = %{ref: ref, key: key, port: port}
    gate_fact(state, pending_identity(state, pending), :pending)
    %{state | runtime: runtime, pending: pending}
  end

  defp complete(state, ref, outcome), do: answer(%{state | pending: nil}, ref, outcome)

  defp inputs(%{seams: seams}), do: [opts: seams, receipt: nil]

  defp pending_identity(%{cap: cap, gen: gen}, %{ref: ref}), do: %{cap: cap, gen: gen, ref: ref}

  defp wake_identity(cap, gen, ref) when is_reference(cap) and is_integer(gen) and gen >= 1 and is_reference(ref),
    do: %{cap: cap, gen: gen, ref: ref}

  defp wake_identity(_cap, _gen, _ref), do: nil

  defp wake_class(%{cap: own_cap}, cap, _gen, _ref) when own_cap != cap, do: :foreign_cap
  defp wake_class(%{gen: own_gen}, _cap, gen, _ref) when own_gen != gen, do: :foreign_generation
  defp wake_class(%{pending: nil}, _cap, _gen, _ref), do: :not_pending
  defp wake_class(_state, _cap, _gen, _ref), do: :foreign_ref

  # the closed test-only gate observer (D-6, D-10): its own seam, role :worker, identity = cap/gen/ref only, a closed
  # event grammar, sent only to a pid, return ignored; never the Observe fence observer
  defp gate_fact(%{seams: seams}, identity, event) do
    case Keyword.get(seams, :gate_deadline_observer) do
      observer when is_pid(observer) -> send(observer, {:gate_deadline, :worker, identity, event})
      _none -> :ok
    end

    :ok
  end

  defp guarded(runtime, fun) do
    fun.()
  catch
    # the carrier is validated at THIS trust boundary (OG-M1): any effect's adapter can raise the public exception;
    # only an exactly closed diagnostic is honoured, anything else is described as the ordinary raw failure it is
    :error, %Effects.Interrupted{reason: %AdapterFailure{diagnostic: diagnostic}} = interrupted ->
      if AdapterRunner.diagnostic?(diagnostic),
        do: closed_diagnostic(diagnostic, interrupted.runtime),
        else: closed(interrupted.kind, interrupted.reason, interrupted.stacktrace, interrupted.runtime)

    :error, %Effects.Interrupted{} = interrupted ->
      closed(interrupted.kind, interrupted.reason, interrupted.stacktrace, interrupted.runtime)

    kind, reason ->
      closed(kind, reason, __STACKTRACE__, runtime)
  end

  defp closed(kind, reason, stacktrace, runtime),
    do: closed_diagnostic(AdapterRunner.closed(kind, reason, length(stacktrace)), runtime)

  defp closed_diagnostic(diagnostic, runtime) do
    {cleanup, empty} = Effects.settle(runtime)
    {:failed, Map.put(diagnostic, :cleanup, summary(cleanup)), empty}
  end

  @doc "The closed cleanup summary of a settle report: attempts, proven settlements, unproven ones."
  @spec summary([map()]) :: %{attempts: non_neg_integer(), settled: non_neg_integer(), unproven: non_neg_integer()}
  def summary(cleanup) when is_list(cleanup) do
    settled = Enum.count(cleanup, &match?(%{"settle" => %{"settled" => true}}, &1))
    %{attempts: length(cleanup), settled: settled, unproven: length(cleanup) - settled}
  end

  # ---- birth: the bootstrap closure and the internal task supervisor, both under the closed boundary ----

  defp boot(bootstrap) do
    validate_boot(bootstrap.())
  catch
    _kind, _reason -> {:error, :worker_bootstrap_failed}
  end

  defp validate_boot(map) when is_map(map) and not is_struct(map) do
    if Enum.all?(map, fn {key, value} -> key in @boot_keys and boot_value?(key, value) end),
      do: {:ok, map},
      else: {:error, :worker_bootstrap_invalid}
  end

  defp validate_boot(_other), do: {:error, :worker_bootstrap_invalid}

  defp boot_value?(:task_supervisor_start, value), do: is_function(value, 0)
  defp boot_value?(:fence_observer, value), do: is_pid(value)
  defp boot_value?(:fence_hold, value), do: hold?(value)
  defp boot_value?(:fence_cap_ms, value), do: is_integer(value) and value >= 1
  defp boot_value?(:retention_observer, value), do: is_function(value, 1)

  defp hold?(value) when is_map(value) and not is_struct(value),
    do: Enum.all?(value, fn {stage, controller} -> stage in @hold_stages and is_pid(controller) end)

  defp hold?(_other), do: false

  defp start_task_supervisor(boot) do
    starter = Map.get(boot, :task_supervisor_start, &Task.Supervisor.start_link/0)

    case starter.() do
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      _other -> {:error, :worker_task_supervisor_failed}
    end
  catch
    _kind, _reason -> {:error, :worker_task_supervisor_failed}
  end

  # admit-time seams override the birth bootstrap; the clock is the configured seam
  defp fence_config(seams, boot) do
    observer = Keyword.get(seams, :observe_fence_observer, Map.get(boot, :fence_observer))
    hold = Keyword.get(seams, :observe_fence_hold, Map.get(boot, :fence_hold, %{}))
    cap = Keyword.get(seams, :observe_fence_cap_ms, Map.get(boot, :fence_cap_ms, @default_cap_ms))

    # which effect kinds report fence facts to the observer: Observe only by default, so the integrated Observe
    # evidence keeps its exact fact stream; delivery tests opt in explicitly (holds apply to every fenced kind)
    kinds = Keyword.get(seams, :observe_fence_kinds, [Effect.Observe])

    if (is_nil(observer) or is_pid(observer)) and hold?(hold) and is_integer(cap) and cap >= 1 and is_list(kinds),
      do:
        {:ok,
         %{
           observer: observer,
           hold: hold,
           cap_ms: cap,
           kinds: kinds,
           clock: Keyword.get(seams, :clock, SystemClock)
         }},
      else: {:error, :worker_seams_invalid}
  end

  # ---- the Observe runner: armed once here, then already-due or the fenced task ----

  defp observe_runner(%{fence: cfg} = state, op, %{deadline_unix: deadline} = intent) do
    # the kind rides on every fact of this op; the observer sees only the kinds it asked for, decided per fact
    kind = intent.__struct__
    observer = kind_observer(cfg, kind)
    observe_fact(observer, op, nil, :arming, kind)
    unix = cfg.clock.unix_now()
    mono = cfg.clock.monotonic_ms()

    case DeadlineFence.arm(op, deadline, unix, mono, cfg.cap_ms) do
      {:ok, fence} ->
        armed = {:armed, %{deadline_unix: deadline, due_ms: fence.due_ms, unix_now: unix}}
        observe_fact(observer, op, nil, armed, kind)
        runner_for(DeadlineFence.next(fence, mono), fence, state, op, kind)

      {:error, %{clause: clause}} ->
        fn _closure, _deadline -> {:failed, AdapterRunner.closed(:error, clause, 0)} end
    end
  end

  defp kind_observer(%{observer: observer, kinds: kinds}, kind) when is_pid(observer), do: if(kind in kinds, do: observer)

  defp kind_observer(_cfg, _kind), do: nil

  # already due at dequeue: no task, no adapter, no runner seam (D1); else the fenced task runner
  defp runner_for(:due, _fence, _state, _op, _kind), do: fn _closure, _deadline -> :expired end

  defp runner_for(_wait, fence, %{fence: cfg} = state, op, kind) do
    ctx = %{op: op, kind: kind, fence: fence, cfg: cfg, task_sup: state.task_sup, retired: state.retired}
    fn closure, _deadline -> fenced_run(closure, ctx) end
  end

  # allocate the task (it waits for GO), hold before GO if configured, then wait in fence chunks
  defp fenced_run(closure, ctx) do
    {:ok, outstanding} = DeadlineFence.outstanding(ctx.op)
    go = make_ref()

    task =
      Task.Supervisor.async_nolink(ctx.task_sup, fn ->
        receive do
          {:go, ^go} -> AdapterRunner.run(closure)
        end
      end)

    handle = %{ref: task.ref, pid: task.pid}
    fact(ctx, handle, {:task_allocated, task.pid})

    joined(ctx, task, handle, fn ->
      case hold(ctx, :before_go, handle, outstanding) do
        :proceed ->
          send(task.pid, {:go, go})
          fact(ctx, handle, {:task_started, task.pid})
          first_wait(ctx, task, handle, outstanding)

        :controller_down ->
          {_return, _death} = kill(ctx, task, handle)
          {:failed, AdapterRunner.closed(:exit, :hold_controller_down, 0)}
      end
    end)
  end

  # the owner failed (a clock, an observer) while its task lives: the task is killed and joined FIRST, then the
  # original failure continues to the closed boundary with its own kind, reason and stack
  defp joined(ctx, task, handle, fun) do
    fun.()
  catch
    kind, reason ->
      stack = __STACKTRACE__
      _ = kill(ctx, task, handle)
      :erlang.raise(kind, reason, stack)
  end

  # after a future-at-arm admission the eligibility is a TIMER, never a read (OG-M2): the first wait comes from
  # one read (a wall already due schedules a zero-delay wake); expiry is selected ONLY on a dequeued eligible wake,
  # so a reply already queued ahead of that wake wins in the same correlated receive
  defp first_wait(ctx, task, handle, outstanding) do
    now = ctx.cfg.clock.monotonic_ms()

    wait =
      case DeadlineFence.next(ctx.fence, now) do
        {:wait, wait} ->
          fact(ctx, handle, {:early, %{wait_ms: wait, due_ms: ctx.fence.due_ms}})
          wait

        :due ->
          0
      end

    timer = Process.send_after(self(), {:observe_fence_wake, ctx.op}, wait)
    await(ctx, task, handle, outstanding, timer)
  end

  # one bounded chunk after a DEQUEUED wake: read the clock, classify against the armed fence, then wait again
  defp chunk(ctx, task, handle, outstanding) do
    now = ctx.cfg.clock.monotonic_ms()

    case DeadlineFence.observe(outstanding, {:fence, ctx.fence, now}) do
      {:early, {:wait, wait}, outstanding} ->
        fact(ctx, handle, {:early, %{wait_ms: wait, due_ms: ctx.fence.due_ms}})
        timer = Process.send_after(self(), {:observe_fence_wake, ctx.op}, wait)
        await(ctx, task, handle, outstanding, timer)

      {:ok, outstanding, :request_expiration} ->
        fact(ctx, handle, :due)
        fact(ctx, handle, :expiry_selected)
        expire(ctx, task, handle, outstanding)
    end
  end

  defp await(ctx, %Task{ref: ref, pid: pid} = task, handle, outstanding, timer) do
    receive do
      {^ref, result} ->
        Process.cancel_timer(timer)
        {:ok, _completed, :retain_result} = DeadlineFence.observe(outstanding, {:result, ctx.op})
        fact(ctx, handle, {:death, join_death(ref, pid)})
        result

      {:DOWN, ^ref, :process, ^pid, reason} ->
        Process.cancel_timer(timer)
        fact(ctx, handle, {:death, death_class(reason)})
        {:failed, AdapterRunner.closed(:exit, reason, 0)}

      {:observe_fence_wake, identity} ->
        if identity == ctx.op do
          chunk(ctx, task, handle, outstanding)
        else
          stale_fact(ctx, handle, identity, stale_class(ctx, identity))
          await(ctx, task, handle, outstanding, timer)
        end
    end
  end

  # expiry won: hold after selection if configured (a lost controller proceeds ONLY to this shutdown), then kill
  defp expire(ctx, task, handle, outstanding) do
    _ = hold(ctx, :after_expiry, handle, outstanding)
    {return, _death} = kill(ctx, task, handle)

    if return == :ok_reply do
      {:ok, _late, :retain_late_result} = DeadlineFence.observe(outstanding, {:result, ctx.op})
      fact(ctx, handle, {:late_result, :stale})
    end

    :expired
  end

  defp kill(ctx, task, handle) do
    fact(ctx, handle, :kill_requested)

    {return, death} =
      case Task.shutdown(task, :brutal_kill) do
        {:ok, _reply} -> {:ok_reply, :normal}
        {:exit, reason} -> {:exit, death_class(reason)}
        nil -> {nil, :killed}
      end

    fact(ctx, handle, {:shutdown_return, return})
    fact(ctx, handle, {:death, death})
    {return, death}
  end

  # the acknowledged held point: the stage's controller is monitored; a duplicate wake while held is reported
  defp hold(%{cfg: %{hold: hold}} = ctx, stage, handle, outstanding) do
    case Map.get(hold, stage) do
      nil ->
        :proceed

      controller ->
        token = make_ref()
        monitor = Process.monitor(controller)
        send(controller, {:observe_fence_held, self(), token, %{op: ctx.op, task: handle, stage: stage}})
        held(ctx, handle, outstanding, token, monitor)
    end
  end

  defp held(ctx, handle, outstanding, token, monitor) do
    receive do
      {:observe_fence_proceed, ^token} ->
        Process.demonitor(monitor, [:flush])
        :proceed

      {:DOWN, ^monitor, :process, _controller, _reason} ->
        :controller_down

      {:observe_fence_wake, identity} ->
        stale_fact(ctx, handle, identity, held_wake_class(ctx, outstanding, identity))
        held(ctx, handle, outstanding, token, monitor)
    end
  end

  defp held_wake_class(ctx, %{state: state}, identity) when identity == ctx.op,
    do: if(state == :running, do: :early, else: :duplicate)

  defp held_wake_class(ctx, _outstanding, identity), do: stale_class(ctx, identity)

  defp stale_class(%{retired: retired}, %{ref: ref}) when is_reference(ref),
    do: if(Map.has_key?(retired, ref), do: :retired, else: :foreign)

  defp stale_class(_ctx, _identity), do: :foreign

  defp join_death(ref, pid) do
    receive do
      {:DOWN, ^ref, :process, ^pid, reason} -> death_class(reason)
    after
      @death_join_ms -> :unjoined
    end
  end

  defp death_class(:normal), do: :normal
  defp death_class(:killed), do: :killed
  defp death_class(:shutdown), do: :shutdown
  defp death_class({:shutdown, _}), do: :shutdown
  defp death_class(_other), do: :abnormal

  defp fact(%{cfg: cfg, op: op, kind: kind}, handle, fact),
    do: observe_fact(kind_observer(cfg, kind), op, handle, fact, kind)

  # a stale wake is reported against the identity it was resolved with: a retired op's own identity and kind
  # (filtered by THAT kind, whatever the live op is), otherwise the live op that ignored it
  defp stale_fact(%{cfg: cfg, retired: retired}, handle, identity, :retired),
    do: retired_fact(cfg, retired, identity, handle)

  defp stale_fact(ctx, handle, _identity, class), do: fact(ctx, handle, {:stale, class})

  defp retired_fact(cfg, retired, %{ref: ref} = identity, handle) do
    kind = Map.fetch!(retired, ref)
    observe_fact(kind_observer(cfg, kind), identity, handle, {:stale, :retired}, kind)
  end

  defp observe_fact(observer, op, handle, fact, kind) when is_pid(observer),
    do: send(observer, {:observe_fence, self(), %{op: op, task: handle, fact: fact, kind: kind}})

  defp observe_fact(_observer, _op, _handle, _fact, _kind), do: :ok
end
