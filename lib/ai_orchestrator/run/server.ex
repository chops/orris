defmodule AiOrchestrator.Run.Server do
  @moduledoc """
  The run's foreground driver: a `gen_statem` that commits and steps `AiOrchestrator.Lifecycle.Host`
  for one run through the staged record (`commit_step` / `resume_step` / `close_step` / `reject_step`).
  Effects, Ports and gates are NOT executed here: the run's one temporary `Run.Worker` under
  `Run.Work.Supervisor` owns the effect runtime and executes what this process hands it through the
  correlated protocol (admit / release_terminal / execute / settle), one operation outstanding at a time.

  States: `:opening` (discovery, birth, acknowledged handoff, open, admission), `:driving` (one
  committed stage per step; replies dequeued by correlation with observed-DOWN priority),
  `:finished` / `:failed` (cached result). `init/1` does no work. There is no statem-owned timer beyond
  the admission and settle budgets, no cancel and no automatic restart. A caller reads the result with
  `await/2`; a crash reaches callers only as the closed `run_server_down`.

  Discovery (D-2) asks the parent supervisor for its started children, selects the Writer sibling
  by its child id `{Journal.Writer, expanded_run_dir}`, reads `Writer.opened/1` (the verified,
  repaired journal view; the ONLY source of prior events), checks `Ownership.status/1` names that
  Writer as the live owner and confirms the Work supervisor started. Only then does it report
  `{:run_server_driving, self(), %{writer, work, ownership}}` to `config.trace` and open the loop.
  """

  @behaviour :gen_statem

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Commands.Idempotency
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.DeadlineFence
  alias AiOrchestrator.Run.Work

  # A post-admission owner loss answers `{:error, %{clause: "run_effect_owner_down", writer_generation: g}}`
  # (docs/contracts/owner-loss-generation.org): `g` is the RunLock generation of the exact Writer sibling as
  # captured in the registration at discovery - never `gen` below (the effect-protocol generation), never a
  # lookup after the loss. Pre-admission loss, generic Server death and a wait timeout keep their own clauses.
  @type result :: {:ok, map()} | {:error, map()}
  @type status :: :opening | :driving | :finished | :failed

  @doc "Started by `AiOrchestrator.Run.Supervisor`; the caller (the supervisor) is the discovery parent."
  @spec start_link(map()) :: :gen_statem.start_ret()
  def start_link(%{} = config), do: :gen_statem.start_link(__MODULE__, {config, self()}, [])

  @spec child_spec(map()) :: Supervisor.child_spec()
  def child_spec(%{} = config),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :worker, restart: :permanent}

  @doc """
  The foreground result channel. Blocks the CALLER only: the timeout is a wait timeout and never
  cancels or takes over work. Terminal results are cached; repeated awaits return the same value.
  Closed, payload-free errors: `%{clause: "await_timeout"}` (the wait expired, work continues) and
  `%{clause: "run_server_down"}` (the Server is not alive or exited; no exit-reason bytes).
  """
  @spec await(pid(), timeout()) :: result()
  def await(pid, timeout) when is_pid(pid) do
    :gen_statem.call(pid, :await, timeout)
  catch
    :exit, {:timeout, _call} -> {:error, %{clause: "await_timeout"}}
    :exit, _reason -> {:error, %{clause: "run_server_down"}}
  end

  @doc "The current state name; a call, so it queues behind the sequential driving steps."
  @spec status(pid()) :: status()
  def status(pid) when is_pid(pid), do: :gen_statem.call(pid, :status)

  @admit_budget 5_000
  @settle_budget 5_000
  # D-8: the default Server chunk cap for an armed AwaitGate; a resolved :gate_deadline_cap_ms may only lower it
  @gate_chunk_cap_ms 60_000
  @unknown_cleanup %{attempts: :unknown, settled: 0, unproven: :unknown}

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init({config, parent}) do
    data = %{
      config: config,
      parent: parent,
      siblings: nil,
      loop: nil,
      result: nil,
      worker: nil,
      cap: make_ref(),
      gen: 1,
      outstanding: nil,
      stage: nil,
      pending: nil,
      drops: 0,
      gate: nil
    }

    {:ok, :opening, data, [{:next_event, :internal, :discover}]}
  end

  # ---- :opening - discovery, birth, handoff, admission ----

  @impl :gen_statem
  def handle_event(:internal, :discover, :opening, data) do
    case closed(data, fn -> discover(data.config, data.parent) end) do
      {:ok, siblings} ->
        trace(data.config, {:run_server_driving, self(), siblings})
        {:keep_state, %{data | siblings: siblings}, [{:next_event, :internal, :birth}]}

      {:error, rejection} ->
        {:next_state, :failed, %{data | result: {:error, rejection}}}
    end
  end

  # every birth-phase step (barriers included) runs under the closed boundary: a term THROWN by a callback can
  # never become this callback's return value (a fabricated transition), it becomes a closed exit (WG-M3)
  def handle_event(:internal, :birth, :opening, %{siblings: %{work: work}} = data) do
    closed(data, fn ->
      birth_barrier(data, :before_birth, nil)

      case DynamicSupervisor.start_child(work, worker_child(data)) do
        {:ok, pid} ->
          Run.Supervisor.record_child(data.config, work, :worker, pid)
          worker = %{pid: pid, monitor: Process.monitor(pid), phase: :registered}
          data = %{data | worker: worker}
          birth_barrier(data, :registered, pid)
          {:keep_state, data, [{:next_event, :internal, :handoff}]}

        _other ->
          {:next_state, :failed, %{data | result: {:error, %{clause: "run_worker_start_failed"}}}}
      end
    end)
  end

  def handle_event(:internal, :handoff, :opening, %{config: config, worker: %{pid: pid}} = data) do
    case Map.get(config, :owner_handoff) do
      {owner, ref} when is_pid(owner) and is_reference(ref) ->
        send(owner, {:run_worker_registered, ref, pid, self()})
        {:keep_state, %{data | outstanding: %{op: :handoff, ref: ref}}, [{:state_timeout, @admit_budget, :admission}]}

      _none ->
        {:keep_state, data, [{:next_event, :internal, :open}]}
    end
  end

  def handle_event(:info, {:run_worker_acknowledged, ref}, :opening, %{outstanding: %{op: :handoff, ref: ref}} = data),
    do: {:keep_state, %{data | outstanding: nil}, [{:next_event, :internal, :open}]}

  # the loop opens (no effect runs) BEFORE admission: the seams the owner executes with are the loop's RESOLVED
  # options (Writer-bound sink, run directory, identity), which exist only once the loop is open
  def handle_event(:internal, :open, :opening, %{config: config, siblings: siblings} = data) do
    case closed(data, fn -> open(config, siblings) end) do
      {:ok, loop} -> {:keep_state, %{data | loop: loop}, [{:next_event, :internal, :admit}]}
      {:replay, result} -> {:next_state, :finished, %{data | result: {:ok, result}}}
      {:error, rejection} -> {:next_state, :failed, %{data | result: {:error, rejection}}}
    end
  end

  def handle_event(:internal, :admit, :opening, %{worker: %{pid: pid}, cap: cap, gen: gen, loop: loop} = data) do
    send(pid, {:admit, cap, gen, loop.opts})
    {:keep_state, %{data | outstanding: %{op: :admit, ref: nil}}, [{:state_timeout, @admit_budget, :admission}]}
  end

  def handle_event(:info, {:admitted, cap, gen, from}, :opening, %{outstanding: %{op: :admit}} = data) do
    case owner_liveness(data) || mismatch(data, :admit, cap, gen, nil, from) do
      :owner_down ->
        data = dropped(%{data | outstanding: nil, result: {:error, %{clause: "run_worker_start_failed"}}}, :owner_down)
        {:next_state, :failed, data}

      nil ->
        worker = %{data.worker | phase: :admitted}
        {:next_state, :driving, %{data | worker: worker, outstanding: nil}, [{:next_event, :internal, :step}]}

      reason ->
        {:keep_state, dropped(data, reason)}
    end
  end

  def handle_event(:state_timeout, :admission, :opening, data) do
    retire(data)
    {:next_state, :failed, %{data | outstanding: nil, result: {:error, %{clause: "run_worker_start_failed"}}}}
  end

  def handle_event(
        :info,
        {:DOWN, monitor, :process, pid, _reason},
        :opening,
        %{worker: %{pid: pid, monitor: monitor}} = data
      ), do: {:next_state, :failed, %{data | outstanding: nil, result: {:error, %{clause: "run_worker_start_failed"}}}}

  # ---- :driving - one committed stage at a time, executed by the worker ----

  def handle_event(:internal, :step, :driving, %{loop: loop} = data) do
    case closed(data, fn -> Host.commit_step(loop) end) do
      {:effect, stage} ->
        {data, actions} = request(%{data | stage: stage, loop: stage.loop}, :release, stage.suffix)
        {:keep_state, data, actions}

      {:done, stage, result} ->
        {data, actions} =
          request(%{data | stage: stage, loop: stage.loop, pending: {:done, result}}, :release, stage.suffix)

        {:keep_state, data, actions}

      {:rejected, rejection} ->
        {data, actions} = request(%{data | pending: {:rejected, rejection}}, :settle, nil)
        {:keep_state, data, actions}
    end
  end

  def handle_event(:info, {:released, cap, gen, ref, from}, :driving, data) do
    case owner_liveness(data) || mismatch(data, :release, cap, gen, ref, from) do
      :owner_down ->
        owner_lost(dropped(data, :owner_down))

      nil ->
        data = applied(data, :release)

        {data, actions} =
          case data.pending do
            {:done, _result} -> request(data, :settle, nil)
            _ -> request(data, :execute, {data.stage.intent, data.stage.receipt})
          end

        {:keep_state, data, actions}

      reason ->
        {:keep_state, dropped(data, reason)}
    end
  end

  # the correlated result leaves the outstanding gate FIRST (fence result transition, timer cancel, gate cleared,
  # fact), before the reducer step and the next :step are scheduled (AW-M2)
  def handle_event(:info, {:effect_result, cap, gen, ref, from, observation}, :driving, data) do
    case owner_liveness(data) || mismatch(data, :execute, cap, gen, ref, from) do
      :owner_down ->
        owner_lost(dropped(data, :owner_down))

      nil ->
        data = applied(data, :execute)
        {data, gate_actions} = closed(data, fn -> release_gate(data, :result) end)

        {:continue, loop} = closed(data, fn -> Host.resume_step(data.stage, observation) end)
        {:keep_state, %{data | loop: loop, stage: nil}, gate_actions ++ [{:next_event, :internal, :step}]}

      reason ->
        {:keep_state, dropped(data, reason)}
    end
  end

  # the worker already settled its latest runtime in-invocation: the closed diagnostic ends this process (parity)
  # the same fence result transition and cancellation as a result (inside the boundary); the terminal exit itself stays
  # OUTSIDE it so the worker's already-settled diagnostic passes through unchanged and nothing is settled twice
  def handle_event(:info, {:effect_failed, cap, gen, ref, from, closed}, :driving, data) do
    case owner_liveness(data) || mismatch(data, :execute, cap, gen, ref, from) do
      :owner_down ->
        owner_lost(dropped(data, :owner_down))

      nil ->
        data = applied(data, :execute)
        {_data, _gate_actions} = closed(data, fn -> release_gate(data, :result) end)
        exit({:run_step_failed, closed_or_rediagnosed(closed)})

      reason ->
        {:keep_state, dropped(data, reason)}
    end
  end

  def handle_event(:info, {:settled, cap, gen, ref, from, cleanup}, :driving, data) do
    case owner_liveness(data) || mismatch(data, :settle, cap, gen, ref, from) do
      :owner_down ->
        owner_lost(dropped(data, :owner_down))

      nil ->
        data = applied(data, :settle)

        case data.pending do
          {:done, result} ->
            {:ok, result} = Host.close_step(result, cleanup_list(cleanup))
            {:next_state, :finished, %{data | pending: nil, stage: nil, loop: nil, result: {:ok, result}}}

          {:rejected, rejection} ->
            {:error, rejection} = Host.reject_step(rejection, cleanup_list(cleanup))
            {:next_state, :failed, %{data | pending: nil, stage: nil, loop: nil, result: {:error, rejection}}}
        end

      reason ->
        {:keep_state, dropped(data, reason)}
    end
  end

  # the settle budget: no reply means nothing is known about the worker's handles - never invented counts
  def handle_event(:state_timeout, :settle, :driving, %{pending: {:done, result}} = data) do
    {:ok, result} = Host.close_step(result, unknown_cleanup_list())
    {:next_state, :finished, %{data | pending: nil, stage: nil, loop: nil, outstanding: nil, result: {:ok, result}}}
  end

  def handle_event(:state_timeout, :settle, :driving, %{pending: {:rejected, rejection}} = data) do
    {:error, rejection} = Host.reject_step(rejection, unknown_cleanup_list())
    {:next_state, :failed, %{data | pending: nil, stage: nil, loop: nil, outstanding: nil, result: {:error, rejection}}}
  end

  # the owner died after admission: the run closes; no replacement, no replayed effect; the starter tears down
  def handle_event(
        :info,
        {:DOWN, monitor, :process, pid, _reason},
        :driving,
        %{worker: %{pid: pid, monitor: monitor}} = data
      ), do: owner_lost(data)

  # ---- the outstanding AwaitGate's generic timeout (AW-M1, AW-M2): every DEQUEUED chunk is a claim ----

  # in :driving an observable owner DOWN wins first; then the chunk must be CORRELATED (the gate's identity, kind
  # AwaitGate and the outstanding execute op with this ref) before the fence classifies it at the monotonic clock
  def handle_event({:timeout, {:gate_deadline, ref}}, {:gate_chunk, identity}, :driving, data) do
    case owner_liveness(data) do
      :owner_down ->
        owner_lost(dropped(data, :owner_down))

      nil ->
        if gate_correlated?(data, ref, identity),
          do: gate_chunk(data, identity),
          else: {:keep_state, dropped(data, :gate_chunk_stale)}
    end
  end

  # a chunk in any other state is stale: counted, never actuated
  def handle_event({:timeout, {:gate_deadline, _ref}}, {:gate_chunk, _identity}, _state, data),
    do: {:keep_state, dropped(data, :gate_chunk_stale_state)}

  # any other protocol-shaped message is a reply without a matching outstanding operation
  def handle_event(:info, message, _state, data) when is_tuple(message) and tuple_size(message) >= 4 do
    case reply_op(message) do
      nil -> :keep_state_and_data
      op -> {:keep_state, dropped(data, if(data.outstanding, do: :op_mismatch, else: :no_outstanding), op)}
    end
  end

  def handle_event({:call, from}, :await, state, %{result: result}) when state in [:finished, :failed],
    do: {:keep_state_and_data, [{:reply, from, result}]}

  def handle_event({:call, _from}, :await, _state, _data), do: {:keep_state_and_data, [:postpone]}
  def handle_event({:call, from}, :status, state, _data), do: {:keep_state_and_data, [{:reply, from, state}]}
  def handle_event(:info, _message, _state, _data), do: :keep_state_and_data

  # ---- requests, correlation, drop accounting ----

  defp request(%{worker: %{pid: pid}, cap: cap, gen: gen} = data, op, payload) do
    ref = make_ref()
    {data, gate_actions} = gate_request(data, op, payload, ref)

    message =
      case op do
        :release -> {:release_terminal, cap, gen, ref, payload}
        :execute -> {:execute, cap, gen, ref, elem(payload, 0), elem(payload, 1)}
        :settle -> {:settle, cap, gen, ref}
      end

    kind = if op == :execute, do: elem(payload, 0).__struct__
    trace(data.config, {:run_effect_requested, self(), %{op: op, kind: kind, cap: cap, gen: gen, ref: ref, worker: pid}})
    send(pid, message)
    actions = if op == :settle, do: [{:state_timeout, @settle_budget, :settle}], else: []
    {%{data | outstanding: %{op: op, ref: ref}}, gate_actions ++ actions}
  end

  # ---- Server-owned deadline authority for the AwaitGate (docs/contracts/gate-async-await-proposal.org) ----

  # an AwaitGate execute is ARMED before its request is sent, under the closed boundary: the D-8 cap precondition,
  # exactly one wall and one monotonic sample of the RESOLVED loop clock, the pure fence, and the first chunk from
  # the same monotonic sample; an ordinary settle request is a gate exit (defensive cancel + clear)
  defp gate_request(data, :execute, {%Effect.AwaitGate{} = intent, _receipt}, ref) do
    identity = %{cap: data.cap, gen: data.gen, ref: ref}
    {gate, wait} = closed(data, fn -> arm_gate(data, identity, intent) end)
    gate_fact(data, identity, {:armed, wait})
    {%{data | gate: gate}, [{{:timeout, {:gate_deadline, ref}}, wait, {:gate_chunk, identity}}]}
  end

  defp gate_request(data, :settle, _payload, _ref), do: release_gate(data, :settle)
  defp gate_request(data, _op, _payload, _ref), do: {data, []}

  defp arm_gate(data, identity, %Effect.AwaitGate{deadline_unix: deadline}) do
    cap_ms = Keyword.get(loop_opts(data), :gate_deadline_cap_ms, @gate_chunk_cap_ms)

    # D-8 precondition, judged BEFORE the fence: an integer chunk cap in 1..60_000 (0 and 120_000 are both refused here)
    if not (is_integer(cap_ms) and cap_ms >= 1 and cap_ms <= @gate_chunk_cap_ms),
      do: :erlang.error(%{clause: "gate_deadline_cap_invalid"})

    clock = gate_clock(data)
    unix = clock.unix_now()
    mono = clock.monotonic_ms()

    with {:ok, fence} <- DeadlineFence.arm(identity, deadline, unix, mono, cap_ms),
         {:ok, outstanding} <- DeadlineFence.outstanding(identity),
         {:ok, wait} <- first_wait(DeadlineFence.next(fence, mono)) do
      {%{identity: identity, kind: Effect.AwaitGate, fence: fence, outstanding: outstanding}, wait}
    else
      {:error, %{clause: clause}} -> :erlang.error(%{clause: "gate_deadline_fence_refused", fence: clause})
    end
  end

  defp first_wait(:due), do: {:ok, 0}
  defp first_wait({:wait, ms}), do: {:ok, ms}
  defp first_wait({:error, _clause} = refusal), do: refusal

  defp gate_correlated?(
         %{gate: %{identity: identity, kind: Effect.AwaitGate}, outstanding: %{op: :execute, ref: ref}},
         ref,
         claimed
       ), do: identity == claimed and identity.ref == ref

  defp gate_correlated?(_data, _ref, _identity), do: false

  # one correlated chunk: the monotonic read and the pure transition run under the closed boundary; an early chunk
  # re-arms the SAME timer (monotonic only), the due chunk actuates the owner ONCE, a duplicate is a counted drop
  defp gate_chunk(%{gate: gate} = data, identity) do
    case closed(data, fn -> observe_chunk(data, gate) end) do
      {:early, {:wait, ms}, outstanding} ->
        gate_fact(data, identity, {:early, ms})

        {:keep_state, put_in(data, [:gate, :outstanding], outstanding),
         [{{:timeout, {:gate_deadline, identity.ref}}, ms, {:gate_chunk, identity}}]}

      {:ok, outstanding, :request_expiration} ->
        gate_fact(data, identity, :request_expiration)
        send(data.worker.pid, {:gate_deadline, identity.cap, identity.gen, identity.ref})
        {:keep_state, put_in(data, [:gate, :outstanding], outstanding)}

      {:stale, _reason, _outstanding} ->
        {:keep_state, dropped(data, :gate_duplicate_timeout)}
    end
  end

  defp observe_chunk(data, %{fence: fence, outstanding: outstanding}) do
    now = gate_clock(data).monotonic_ms()

    case DeadlineFence.observe(outstanding, {:fence, fence, now}) do
      {:error, %{clause: clause}} -> :erlang.error(%{clause: "gate_deadline_fence_refused", fence: clause})
      verdict -> verdict
    end
  end

  # every exit from an outstanding gate: the fence result transition for a correlated result (retain_result /
  # retain_late_result; a duplicate completion is counted), the cancel of the owned generic timeout, the cleared
  # gate and at most one :cancelled fact; without a gate there is nothing to cancel and no action is returned
  defp release_gate(%{gate: nil} = data, _exit), do: {data, []}

  defp release_gate(%{gate: %{identity: identity} = gate} = data, exit) do
    data = if exit == :result, do: retain_result(data, gate), else: data
    gate_fact(data, identity, :cancelled)
    {%{data | gate: nil}, [{{:timeout, {:gate_deadline, identity.ref}}, :cancel}]}
  end

  defp retain_result(data, %{identity: identity, outstanding: outstanding}) do
    case DeadlineFence.observe(outstanding, {:result, identity}) do
      {:ok, _completed, _decision} -> data
      {:stale, _reason, _outstanding} -> dropped(data, :gate_duplicate_completion)
      {:error, %{clause: clause}} -> :erlang.error(%{clause: "gate_deadline_fence_refused", fence: clause})
    end
  end

  # every owner-loss leg: the gate is released first (cancel action only when a gate exists), then the closed loss
  # result is cached in :failed; the Server stays alive
  defp owner_lost(data) do
    {data, actions} = release_gate(data, :owner_down)
    {:next_state, :failed, owner_down(data), actions}
  end

  defp loop_opts(%{loop: %{opts: opts}}) when is_list(opts), do: opts
  defp loop_opts(_data), do: []

  defp gate_clock(data), do: Keyword.get(loop_opts(data), :clock, SystemClock)

  # the closed test-only observer (D-10): role :server, identity = cap/gen/ref only, closed events, pid only
  defp gate_fact(data, identity, event) do
    case Keyword.get(loop_opts(data), :gate_deadline_observer) do
      observer when is_pid(observer) -> send(observer, {:gate_deadline, :server, identity, event})
      _none -> :ok
    end

    :ok
  end

  defp applied(%{outstanding: %{op: op, ref: ref}, cap: cap, gen: gen} = data, op) do
    trace(data.config, {:run_effect_applied, self(), {op, cap, gen, ref}})
    %{data | outstanding: nil}
  end

  # ratified priority: an owner whose DOWN is already observable (queued before this reply) is dead for every
  # reply behind it - nothing from it is applied, no observer/reducer/commit advances (WG-M4)
  defp owner_liveness(%{worker: %{pid: pid, monitor: monitor}}) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :owner_down
    after
      0 -> nil
    end
  end

  defp owner_liveness(_data), do: nil

  # the one loss result for both post-admission paths (direct correlated DOWN, queued DOWN ahead of a reply):
  # the positive writer_generation comes ONLY from the registration captured at discovery for the exact Writer
  # sibling; without that binding no generation is invented and the closed clause stands alone
  defp owner_down(data), do: %{data | outstanding: nil, stage: nil, loop: nil, result: owner_loss(data)}

  defp owner_loss(%{siblings: %{writer: writer, ownership: {:ok, %{writer: writer, generation: generation}}}})
       when is_pid(writer) and is_integer(generation) and generation >= 1,
       do: {:error, %{clause: "run_effect_owner_down", writer_generation: generation}}

  defp owner_loss(_data), do: {:error, %{clause: "run_effect_owner_down"}}

  defp mismatch(%{outstanding: nil}, _op, _cap, _gen, _ref, _from), do: :no_outstanding
  defp mismatch(%{outstanding: %{op: other}}, op, _cap, _gen, _ref, _from) when other != op, do: :op_mismatch
  defp mismatch(%{cap: cap}, _op, other, _gen, _ref, _from) when other != cap, do: :cap_mismatch
  defp mismatch(%{gen: gen}, _op, _cap, other, _ref, _from) when other != gen, do: :generation_stale
  defp mismatch(%{outstanding: %{ref: ref}}, _op, _cap, _gen, other, _from) when other != ref, do: :ref_mismatch
  defp mismatch(%{worker: %{pid: pid}}, _op, _cap, _gen, _ref, from) when from != pid, do: :sender_mismatch
  defp mismatch(_data, _op, _cap, _gen, _ref, _from), do: nil

  defp dropped(data, reason, _op \\ nil) do
    trace(data.config, {:run_effect_reply_dropped, self(), reason})
    %{data | drops: data.drops + 1}
  end

  defp reply_op(message) do
    case elem(message, 0) do
      :admitted -> :admit
      :released -> :release
      :effect_result -> :execute
      :effect_failed -> :execute
      :settled -> :settle
      _ -> nil
    end
  end

  defp worker_module(%{config: %{opts: opts}}), do: Keyword.get(opts, :worker_module, Run.Worker)

  # minimum bootstrap transport (U2a-1O D2): only when the run configures Worker-birth seams does the child
  # start carry a ZERO-ARITY closure next to the server pid; every other run (and every worker double) keeps
  # the pid-only child start unchanged
  @worker_bootstrap [
    observe_task_supervisor_start: :task_supervisor_start,
    observe_fence_observer: :fence_observer,
    observe_fence_hold: :fence_hold,
    observe_fence_cap_ms: :fence_cap_ms
  ]

  defp worker_child(%{config: %{opts: opts}} = data) do
    boot = for {opt, key} <- @worker_bootstrap, value = Keyword.get(opts, opt), value != nil, into: %{}, do: {key, value}
    if boot == %{}, do: {worker_module(data), self()}, else: {worker_module(data), {self(), fn -> boot end}}
  end

  # an un-admitted owner is not kept: nothing was ever handed to it
  defp retire(%{siblings: %{work: work}, worker: %{pid: pid, monitor: monitor}}) do
    Process.demonitor(monitor, [:flush])
    _ = DynamicSupervisor.terminate_child(work, pid)
    :ok
  end

  defp retire(_data), do: :ok

  defp birth_barrier(%{config: %{opts: opts}}, name, info) do
    case Keyword.get(opts, :birth_barrier) do
      fun when is_function(fun, 2) -> fun.(name, info)
      _ -> :ok
    end
  end

  # ---- cleanup accounting ----

  defp cleanup_list(cleanup) when is_list(cleanup), do: cleanup
  defp cleanup_list(_other), do: unknown_cleanup_list()
  defp unknown_cleanup_list, do: [%{"settle" => %{"clause" => "settle_unknown"}}]

  defp cleanup_summary(cleanup) when is_list(cleanup), do: Run.Worker.summary(cleanup)
  defp cleanup_summary(_other), do: @unknown_cleanup

  # a worker-built diagnostic passes only inside the closed domain; anything else is re-diagnosed here
  defp closed_or_rediagnosed(closed) do
    if closed_diagnostic?(closed), do: closed, else: diagnostic(:exit, closed, [])
  end

  # Status and crash reports (sys:get_status, the statem's own termination report, the supervisor's
  # child report) render only closed values: the state name; :redacted for the data (config with the
  # spec, plan, options and run directory; the loop); each queued/postponed/timeout event kept by type
  # with :redacted content; the debug log emptied; the exit reason normalized through the closed
  # diagnostic. Every replacement is valid for the callback's type, so OTP renders these and no default.
  @impl :gen_statem
  def format_status(status) when is_map(status) do
    status
    |> Map.replace_lazy(:data, fn _ -> :redacted end)
    |> Map.replace_lazy(:queue, &redact_events/1)
    |> Map.replace_lazy(:postponed, &redact_events/1)
    |> Map.replace_lazy(:timeouts, &redact_events/1)
    |> Map.replace_lazy(:log, fn _ -> [] end)
    |> Map.replace_lazy(:reason, &closed_reason/1)
  end

  defp redact_events(events) when is_list(events), do: Enum.map(events, &redact_event/1)
  defp redact_events(_other), do: []

  defp redact_event({{:call, from}, _content}), do: {{:call, from}, :redacted}
  defp redact_event({type, _content}) when is_atom(type), do: {type, :redacted}
  defp redact_event(_other), do: {:internal, :redacted}

  # a termination reason is a {class, reason, stacktrace} triple or a bare term; either way only the
  # class and the closed diagnostic survive - no stack (frames carry module, function and file names).
  # A reason already shaped like this process's own closed diagnostic passes only when every key and
  # value is inside the closed domain; a tag is not proof that this process built the map.
  defp closed_reason({class, reason, stacktrace}) when class in [:error, :exit, :throw] and is_list(stacktrace),
    do: {class, diagnostic(class, reason, stacktrace), []}

  defp closed_reason({:run_step_failed, diagnostic} = reason) do
    if closed_diagnostic?(diagnostic), do: reason, else: {:exit, diagnostic(:exit, reason, []), []}
  end

  defp closed_reason(reason), do: {:exit, diagnostic(:exit, reason, []), []}

  @closed_kinds [:error, :exit, :throw]
  @digest ~r/\Asha256:[0-9a-f]{64}\z/

  defp closed_diagnostic?(%{kind: kind, class: class, digest: digest, frames: frames, cleanup: cleanup} = map),
    do:
      map_size(map) == 5 and kind in @closed_kinds and class in Diagnostic.result_classes() and is_binary(digest) and
        Regex.match?(@digest, digest) and is_integer(frames) and frames >= 0 and closed_cleanup?(cleanup)

  defp closed_diagnostic?(_other), do: false

  defp closed_cleanup?(%{attempts: attempts, settled: settled, unproven: unproven} = map),
    do: map_size(map) == 3 and count?(attempts) and is_integer(settled) and settled >= 0 and count?(unproven)

  defp closed_cleanup?(_other), do: false

  defp count?(:unknown), do: true
  defp count?(n), do: is_integer(n) and n >= 0

  # The process boundary for a Host step. Two facts make this necessary: gen_statem takes a term THROWN
  # out of a callback as that callback's return value (a throw escaping the Host would be accepted as a
  # state transition and could fabricate a terminal), and a raw exit reason is logged by the supervisor
  # with whatever payload it carries. The Host has already settled its runtime and re-raised the original
  # kind/reason inside this invocation; here every trappable kind ends this process with the closed
  # diagnostic only: kind, class, sha256 digest of the term, and the depth of the stack (never its frames,
  # which carry module, function and file names of whatever raised).
  defp closed(data, fun) when is_function(fun, 0) do
    fun.()
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__
      cleanup = settle_before_exit(data)
      exit({:run_step_failed, Map.put(diagnostic(kind, reason, stacktrace), :cleanup, cleanup)})
  end

  # a Server-stage failure after admission: the worker's latest runtime is settled through the protocol
  # (bounded); the reply's cleanup is the ONLY count authority - no reply means unknown, never zero
  defp settle_before_exit(%{worker: %{pid: pid, phase: :admitted}, cap: cap, gen: gen} = data) do
    ref = make_ref()

    trace(
      data.config,
      {:run_effect_requested, self(), %{op: :settle, kind: nil, cap: cap, gen: gen, ref: ref, worker: pid}}
    )

    send(pid, {:settle, cap, gen, ref})
    data = %{data | outstanding: %{op: :settle, ref: ref}}
    await_settle(data, System.monotonic_time(:millisecond) + @settle_budget)
  end

  defp settle_before_exit(_data), do: %{attempts: 0, settled: 0, unproven: 0}

  defp await_settle(
         %{worker: %{pid: pid, monitor: monitor}, cap: cap, gen: gen, outstanding: %{ref: ref}} = data,
         deadline
       ) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:settled, ^cap, ^gen, ^ref, ^pid, cleanup} ->
        if owner_liveness(data) == :owner_down do
          @unknown_cleanup
        else
          trace(data.config, {:run_effect_applied, self(), {:settle, cap, gen, ref}})
          cleanup_summary(cleanup)
        end

      {:settled, other_cap, other_gen, other_ref, from, _cleanup} ->
        _ = dropped(data, mismatch(data, :settle, other_cap, other_gen, other_ref, from) || :no_outstanding)
        await_settle(data, deadline)

      {:DOWN, ^monitor, :process, ^pid, _reason} ->
        @unknown_cleanup
    after
      remaining -> @unknown_cleanup
    end
  end

  # the class is the contract's CLOSED result-class vocabulary for every kind (an exception is a "map";
  # an exit or throw atom is "atom"): no exception or module name, whatever marker a term carries, since
  # a structural marker is no trust boundary. The digest is the contract's, present for every term the
  # contract describes (known effect/observation structs included). Nothing here can raise on a term.
  defp diagnostic(kind, reason, stacktrace) do
    %{"digest" => digest} = Diagnostic.describe(reason)
    %{kind: kind, class: Diagnostic.result_class(reason), digest: digest, frames: length(stacktrace)}
  end

  # ---- discovery (D-2): the parent's started children, the exact Writer sibling, live ownership ----

  defp discover(%{run_dir: run_dir}, parent) do
    writer_id = {Writer, Path.expand(run_dir)}
    children = Supervisor.which_children(parent)

    with {:ok, writer} <- sibling(children, writer_id),
         {:ok, work} <- sibling(children, Work.Supervisor),
         {:ok, %{writer: ^writer, state: :live}} = ownership <- Ownership.status(run_dir) do
      {:ok, %{writer: writer, work: work, ownership: ownership}}
    else
      _ -> {:error, %{clause: "run_discovery_failed"}}
    end
  end

  defp sibling(children, id) do
    case List.keyfind(children, id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) -> {:ok, pid}
      _ -> :error
    end
  end

  # ---- the loop is opened against the Writer sibling: its verified view is the only prior ----

  # the executor bindings the CLI gives the Host, OWNED here: the Writer sibling as sink, the run
  # directory, the Writer's lock path and the repair this Writer actually performed. Whatever a caller
  # supplied under these keys is dropped first, so a clean verified prefix can never acquire a repair
  # record and no binding can point anywhere but this subtree.
  @owned_bindings [:event_sink, :run_dir, :run_lock_path, :tail_repair]

  defp open(%{mode: mode, opts: opts} = config, %{writer: writer}) do
    opened = Writer.opened(writer)

    case admit(config, opened) do
      {:replay, events} ->
        {:replay, replay_result(events)}

      {:continue, acceptance} ->
        open_loop({:ok, continuation_bindings(config)}, config, opened, writer, :continue, opts, acceptance)

      other ->
        open_loop(other, config, opened, writer, mode, opts, nil)
    end
  end

  # REPLAY: the durable result of an already complete command is the fold of the verified prefix; nothing is
  # appended and no effect runs (the shape is the Host's)
  defp replay_result(events) do
    {:ok, state} = Fold.fold_lines(Enum.map(events, &Jason.encode!/1))
    %{events: events, appended_events: [], summary: Fold.summary(state)}
  end

  # CONTINUE: the accepted command's own bindings minus its stamp (nothing is accepted again); the
  # acceptance row itself travels as Host INPUT, never as an option a caller could have supplied
  defp continuation_bindings(%{command: %Command{} = command}), do: [{:run_id, command.run_id} | reason_bindings(command)]

  defp open_loop(admitted, config, opened, writer, mode, opts, acceptance) do
    with {:ok, command_opts} <- admitted do
      opts =
        opts
        |> Keyword.drop(@owned_bindings)
        |> Keyword.put(:event_sink, &Writer.append(writer, &1))
        |> Keyword.put(:run_dir, config.run_dir)
        |> Keyword.put(:run_lock_path, opened.lock_path)
        |> put_tail_repair(Writer.tail_repair_data(opened.repair))
        |> Keyword.merge(command_opts)

      prior_lines = if mode == :run, do: [], else: opened.lines
      inputs = %{spec: Map.get(config, :spec), plan: Map.get(config, :plan), prior_lines: prior_lines}
      inputs = if acceptance, do: Map.put(inputs, :acceptance, acceptance), else: inputs
      Host.open(mode, inputs, opts)
    end
  end

  # ---- command admission under the Writer lock (docs/contracts/command-executor-migration.org) ----
  #
  # Unit A: verb/stamp/context were validated by the executor; here the LOCKED verified prefix decides.
  # Acceptance rows are the prior events whose data.requested_by.command_id equals the command's id -
  # structured stamps only; an unstamped acceptance is never a matchable identity. Zero rows under
  # :retry_only provenance preserve the Writer's journal_exists; zero rows otherwise bind the run id and
  # execute; more than one row fails closed. Exactly one matching row is retry admission (replay /
  # continue / conflict / superseded): unit B, with the CONTINUE arm opened as the Host's internal
  # :continue mode (unit C).
  defp admit(config, opened), do: admit(Map.get(config, :command), config, opened)

  defp admit(nil, _config, _opened), do: {:ok, []}

  defp admit(%Command{} = command, %{mode: mode} = config, opened) do
    command_id = command.requested_by["command_id"]
    rows = Enum.filter(decoded(opened.lines), &stamped_with?(&1, command_id))

    cond do
      # unit D (D-M1 precedence): the explicit restart admits ONLY a verified-EMPTY prefix (the Writer's post-repair
      # view under the lock); ANY accepted line is journal_exists, whatever a preflight saw and independently of
      # retry identity - decided before any stamp classification
      Map.get(config, :admission, :fresh) == :restart_empty and opened.lines != [] ->
        {:error, %{clause: "journal_exists"}}

      length(rows) > 1 ->
        {:error, %{clause: "acceptance_ambiguous"}}

      rows == [] and Map.get(config, :admission, :fresh) == :retry_only ->
        {:error, %{clause: "journal_exists"}}

      rows == [] ->
        with :ok <- bind_run_id(mode, command, opened) do
          {:ok, [{:run_id, command.run_id}, {:requested_by, command.requested_by} | reason_bindings(command)]}
        end

      true ->
        with :ok <- bind_run_id(:existing, command, opened) do
          decide(hd(rows), command, decoded(opened.lines))
        end
    end
  end

  # ---- the durable-prefix decision table for a matching acceptance row A ----
  @acceptance_types ~w(run_created run_resumed run_cancel_requested)
  @terminal_types ~w(run_completed run_failed run_cancelled run_budget_exhausted)

  defp decide(row, %Command{} = command, events) do
    case Idempotency.compare(row["data"]["requested_by"], command) do
      {:conflict, field} -> {:error, %{clause: "idempotency_conflict", field: field}}
      {:error, _rejection} -> {:error, %{clause: "command_stamp_invalid"}}
      :match -> classify(row, command, events)
    end
  end

  # A's OWN interval: strictly after A, strictly before the next lifecycle acceptance EVENT TYPE (stamped,
  # unstamped or legacy literal alike). Completion is decided inside that interval only.
  defp classify(row, %Command{requested_by: %{"verb" => verb}}, events) do
    after_a = events |> Enum.drop_while(&(&1["seq"] != row["seq"])) |> Enum.drop(1)
    {interval, rest} = Enum.split_while(after_a, &(&1["type"] not in @acceptance_types))
    later_acceptance? = rest != []
    terminal? = Enum.any?(events, &(&1["type"] in @terminal_types))

    cond do
      complete?(verb, row, interval, events) -> {:replay, events}
      later_acceptance? -> {:error, %{clause: "command_superseded"}}
      terminal? -> {:error, %{clause: "command_superseded"}}
      true -> {:continue, %{seq: row["seq"], verb: verb}}
    end
  end

  # start/resume: a terminal or a blocking attention inside the interval; cancel: run_cancelled inside it
  defp complete?("cancel", _row, interval, _events), do: Enum.any?(interval, &(&1["type"] == "run_cancelled"))

  defp complete?(_start_or_resume, row, interval, events) do
    Enum.any?(interval, &(&1["type"] in @terminal_types)) or blocked_at_interval_end?(row, interval, events)
  end

  defp blocked_at_interval_end?(_row, [], _events), do: false

  defp blocked_at_interval_end?(_row, interval, events) do
    last_seq = List.last(interval)["seq"]
    prefix = Enum.take_while(events, &(&1["seq"] <= last_seq))

    case Fold.fold_lines(Enum.map(prefix, &Jason.encode!/1)) do
      {:ok, %{status: "blocked"}} -> true
      _ -> false
    end
  end

  # start opens with the command's run id; resume/cancel (and any existing prefix) must fold to it
  defp bind_run_id(:run, _command, _opened), do: :ok

  defp bind_run_id(_mode, %Command{run_id: run_id}, %{lines: lines}) do
    case Fold.fold_lines(lines) do
      {:ok, %{run_id: ^run_id}} -> :ok
      {:ok, _state} -> {:error, %{clause: "command_run_mismatch"}}
      {:error, _rejection} -> {:error, %{clause: "command_run_mismatch"}}
    end
  end

  # the reasons the reducer journals on the acceptance event are the command's own arguments
  defp reason_bindings(%Command{requested_by: %{"verb" => "cancel"}, args: %{"reason" => reason}}),
    do: [cancel_reason: reason]

  defp reason_bindings(%Command{requested_by: %{"verb" => "resume"}, args: %{"recovery_reason" => r}}),
    do: [recovery_reason: r]

  defp reason_bindings(_command), do: []

  # only a STRUCTURED stamp names an identity; absent attribution and the read-side legacy literal ("operator")
  # are non-matching, never an error
  defp stamped_with?(%{"data" => %{"requested_by" => %{"command_id" => id}}}, command_id) when is_binary(id),
    do: id == command_id

  defp stamped_with?(_event, _command_id), do: false

  defp decoded(lines) do
    Enum.flat_map(lines, fn line ->
      case Jason.decode(line) do
        {:ok, %{} = event} -> [event]
        _ -> []
      end
    end)
  end

  defp put_tail_repair(opts, nil), do: opts
  defp put_tail_repair(opts, %{} = repair), do: Keyword.put(opts, :tail_repair, repair)

  defp trace(%{trace: pid}, message) when is_pid(pid), do: send(pid, message)
  defp trace(_config, _message), do: :ok
end
