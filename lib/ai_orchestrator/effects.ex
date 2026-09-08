defmodule AiOrchestrator.Effects do
  @moduledoc """
  The host-side effect executor (docs/contracts/effects-extraction.org).

  `execute/3` turns one `Contract.Effect` into its admissible `Contract.Observation` through the
  injected adapters, threading an explicit `Effects.Runtime` (the gate handles the invocation owns).
  The runtime is updated the moment a handle is transferred, and every later stage runs inside a
  guard that re-raises any `:error`, `:throw` or `:exit` as `Effects.Interrupted` carrying the
  LATEST runtime, so the Host can settle exactly the newest handles before propagating the
  original failure. Ports stay owned by the process running the loop; nothing here spawns.
  """

  use Boundary,
    deps: [
      AiOrchestrator.Clock,
      AiOrchestrator.Contract,
      AiOrchestrator.Dispatch,
      AiOrchestrator.Gate,
      AiOrchestrator.Journal
    ],
    exports: [Runtime, Interrupted, Unreachable, AdapterRunner, AdapterFailure]

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.ArtifactBaseline
  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.FileError
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Dispatch.PromptStore
  alias AiOrchestrator.Effects.AdapterFailure
  alias AiOrchestrator.Effects.AdapterRunner
  alias AiOrchestrator.Effects.Interrupted
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Effects.Unreachable
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Gate.Runner, as: GateRunner
  alias AiOrchestrator.Journal.Fs.SystemFs

  @file_errors FileError.errnos()
  # closed value domains shared by the gate diagnostics mapper (docs/contracts/gate-execution-wiring.org)
  @proofs ~w(gone alive unknown)
  @liveness ~w(gone alive unknown)
  @settle_reasons ~w(command parent_gone guardian_signaled release_failed deadline)
  @termination_kinds ~w(exited signaled timeout unknown)
  @settle_clauses ~w(guardian_gone settle_unproven abandon_unsettled)

  @type inputs :: [opts: keyword(), receipt: map() | nil]

  # ---- the executor API ----

  @doc """
  Execute one effect. `inputs[:opts]` is the Host's option keyword (the adapters' context);
  `inputs[:receipt]` is the persisted `gate_started` the Host selected for a `ReleaseGate`.
  Raises `Effects.Interrupted` (latest runtime inside) on any trappable failure of the adapter or
  executor code; raises `Effects.Unreachable` for `Effect.Notify`.
  """
  @spec execute(struct(), Runtime.t(), inputs()) :: {struct(), Runtime.t()}
  def execute(%Effect.Notify{} = effect, %Runtime{}, _inputs), do: raise(Unreachable, effect: effect.__struct__)

  def execute(effect, %Runtime{} = runtime, inputs) when is_struct(effect) and is_list(inputs) do
    run_effect(effect, runtime, Keyword.get(inputs, :opts, []), Keyword.get(inputs, :receipt))
  end

  @doc """
  Begin one effect on the owner-resident path (docs/contracts/gate-async-await-proposal.org, AW-M3/AW-M6): every effect
  except `AwaitGate` is `{:done, execute(...)}`; an `AwaitGate` reaches `begin_await/2` only when its Runtime retains a
  running handle AND the selected executor exports the COMPLETE protocol (`begin_await/2`, `resume_await/2`,
  `settle_await/1`, `expire/1`); otherwise it takes the synchronous `execute/3` path unchanged. `{:done, answer}` is
  mapped through the existing builders; `{:pending, waiting}` records the awaiting entry with the opaque descriptor.
  Every trappable failure, including an executor return outside the grammar, raises `Effects.Interrupted` with the
  runtime this stage saw.
  """
  @spec begin(struct(), Runtime.t(), inputs()) :: {:done, {struct(), Runtime.t()}} | {:pending, Runtime.t()}
  def begin(%Effect.AwaitGate{gate_run_id: id, attempt: attempt} = effect, %Runtime{} = runtime, inputs)
      when is_list(inputs) do
    opts = Keyword.get(inputs, :opts, [])
    key = {id, attempt}
    executor = gate_mod(opts)

    case Runtime.handle(runtime, key) do
      nil ->
        {:done, execute(effect, runtime, inputs)}

      running ->
        if async_capable?(executor),
          do: gate_begin(effect, key, running, executor, runtime, opts),
          else: {:done, execute(effect, runtime, inputs)}
    end
  end

  def begin(effect, %Runtime{} = runtime, inputs) when is_struct(effect) and is_list(inputs),
    do: {:done, execute(effect, runtime, inputs)}

  @doc """
  Resume the awaiting entry `key` with ONE real Port message the owner dequeued: `resume_await/2`, then the entry
  returns to its retained running handle BEFORE the answer is mapped, so a mapping failure carries the RETURNED
  runtime in `Effects.Interrupted`. Never `:pending`.
  """
  @spec resume(Runtime.t(), Runtime.key(), tuple(), inputs()) :: {:done, {struct(), Runtime.t()}}
  def resume(%Runtime{} = runtime, key, {port, _payload} = message, inputs) when is_port(port) and is_list(inputs),
    do: complete(runtime, key, inputs, :resume_await, & &1.resume_await(&2, message))

  @doc "Settle the awaiting entry `key` on the deadline wake path (`settle_await/1`); the same transition and mapping as `resume/4`."
  @spec settle_await(Runtime.t(), Runtime.key(), inputs()) :: {:done, {struct(), Runtime.t()}}
  def settle_await(%Runtime{} = runtime, key, inputs) when is_list(inputs),
    do: complete(runtime, key, inputs, :settle_await, fn executor, waiting -> executor.settle_await(waiting) end)

  @doc """
  The OPAQUE pending identity of a Runtime: `:none` when no entry awaits (and every entry is a well-formed retained
  one), `{:ok, %{key, port}}` for exactly one COMPLETE awaiting carrier (handle port, descriptor on the same port with
  its activation ref, the `AwaitGate` effect of that key), otherwise a closed refusal. A malformed or ambiguous
  runtime is never answered `:none`.
  """
  @spec pending(term()) :: {:ok, %{key: Runtime.key(), port: port()}} | :none | {:error, %{clause: String.t()}}
  def pending(%Runtime{gates: gates}) when is_map(gates) do
    classes = Enum.map(gates, fn {key, entry} -> {key, entry_class(key, entry)} end)
    awaiting = Enum.filter(classes, fn {_key, class} -> class != :retained end)

    cond do
      Enum.any?(classes, &match?({_key, :malformed}, &1)) ->
        {:error, %{clause: "runtime_malformed"}}

      awaiting == [] ->
        :none

      match?([{_key, {:awaiting, _port}}], awaiting) ->
        [{key, {:awaiting, port}}] = awaiting
        {:ok, %{key: key, port: port}}

      match?([_one], awaiting) ->
        {:error, %{clause: "pending_malformed"}}

      true ->
        {:error, %{clause: "pending_ambiguous"}}
    end
  end

  def pending(_other), do: {:error, %{clause: "runtime_malformed"}}

  @doc """
  The gate executor an option keyword selects (default `Gate.Execution`): the Worker's routing seam. The value is
  whatever the seams carry, so the caller judges it (a non-module selection is "no capability", never a crash).
  """
  @spec gate_executor(keyword()) :: term()
  def gate_executor(opts) when is_list(opts), do: gate_mod(opts)

  @doc "Pure: the `{key, handle}` entries this Runtime owns, in key order (retention routing reads only)."
  @spec owned_entries(Runtime.t()) :: [{Runtime.key(), term()}]
  def owned_entries(%Runtime{gates: gates}) do
    gates |> Enum.sort_by(fn {key, _entry} -> key end) |> Enum.map(fn {key, %{handle: handle}} -> {key, handle} end)
  end

  @doc "Pure: drop the handles whose gate_passed/gate_failed is in a suffix that committed as a whole."
  @spec release_terminal(Runtime.t(), [map()]) :: Runtime.t()
  def release_terminal(%Runtime{gates: gates} = runtime, committed_suffix) when is_list(committed_suffix) do
    finished =
      for %{"type" => type, "data" => %{"gate_run_id" => id}} <- committed_suffix,
          type in ["gate_passed", "gate_failed"],
          do: id

    %{runtime | gates: Map.reject(gates, fn {{id, _attempt}, _entry} -> id in finished end)}
  end

  @doc """
  Attempt cleanup for every retained handle through the executor and report the executor's actual
  answer per handle in the closed settle shape. A failing attempt (any kind) is reported
  `settle_unproven` and never stops the remaining attempts. The returned runtime is empty:
  tracking relinquished, not proof of native closure.
  """
  @spec settle(Runtime.t()) :: {[map()], Runtime.t()}
  def settle(%Runtime{gates: gates} = runtime) do
    {cleanup, runtime} =
      Enum.reduce(Map.keys(gates), {[], runtime}, fn key, {acc, rt} ->
        {entry, rt} = abandon_one(rt, key)
        {acc ++ [entry], rt}
      end)

    {cleanup, %{runtime | gates: %{}}}
  end

  @doc "How a contract effect struct reaches execution: served here, and constructed by the reducer."
  @spec classify(module()) :: %{executable: boolean(), emitted: boolean()}
  def classify(module) when is_atom(module),
    do: %{executable: module != Effect.Notify, emitted: module not in [Effect.Notify, Effect.RunGate]}

  # ---- dispatch by effect ----

  defp run_effect(%Effect.PrepareGate{} = effect, runtime, opts, _receipt), do: gate_prepare(effect, runtime, opts)

  defp run_effect(%Effect.ReleaseGate{} = effect, runtime, opts, receipt),
    do: gate_release(effect, runtime, opts, receipt)

  defp run_effect(%Effect.AwaitGate{} = effect, runtime, opts, _receipt), do: gate_await(effect, runtime, opts)

  defp run_effect(%Effect.ReconcileGate{} = effect, runtime, opts, _receipt),
    do: {guarded(runtime, fn -> gate_reconcile(effect, opts) end), runtime}

  # Observe, Dispatch and ReconcileSend take the adapter runner seam (U2a-1O, delivery-deadline): absent, the
  # adapter runs here as before; present, the owner's runner places the closure (a task, a fence) and answers the
  # disjoint grammar below
  defp run_effect(%Effect.Observe{} = intent, runtime, opts, _receipt) do
    case Keyword.get(opts, :adapter_runner) do
      nil -> {guarded(runtime, fn -> observe(intent, adapter(intent, opts), opts) end), runtime}
      runner -> {guarded(runtime, fn -> observe_through(intent, runner, opts) end), runtime}
    end
  end

  # the delivery effects (U2b) share the Observe seam and grammar: absent, the adapter runs here as before
  defp run_effect(%Effect.Dispatch{} = intent, runtime, opts, _receipt),
    do: {guarded(runtime, fn -> deliver_through(intent, Keyword.get(opts, :adapter_runner), opts) end), runtime}

  defp run_effect(%Effect.ReconcileSend{} = intent, runtime, opts, _receipt),
    do: {guarded(runtime, fn -> deliver_through(intent, Keyword.get(opts, :adapter_runner), opts) end), runtime}

  defp run_effect(intent, runtime, opts, _receipt),
    do: {guarded(runtime, fn -> observe(intent, adapter(intent, opts), opts) end), runtime}

  # the closure captures ONLY the adapter invocation inputs (module, command, observe options), never the
  # runtime or the host options; `:expired` is answered from the OWNER's retained intent (ruling B: this is the
  # first producer of the admissible TimedOut); a closed `{:failed, diagnostic}` is carried through the guarded
  # boundary unchanged (the owner re-validates it at its own trust boundary); every other return fails closed
  defp observe_through(%Effect.Observe{command: command, deadline_unix: deadline} = intent, runner, opts)
       when is_function(runner, 2) do
    module = dispatch_module(opts)
    observe_opts = observe_opts(deadline, opts)
    closure = fn -> module.observe(command, observe_opts) end

    case runner.(closure, %{deadline_unix: deadline}) do
      {:ok, raw} ->
        observe(intent, raw, opts)

      :expired ->
        %Observation.TimedOut{assignment_id: intent.assignment_id, deadline_unix: deadline, now: now(opts)}

      {:failed, diagnostic} = returned ->
        if AdapterRunner.diagnostic?(diagnostic),
          do: raise(AdapterFailure, diagnostic: diagnostic),
          else: observe(intent, {:invalid_runner_return, returned}, opts)

      other ->
        observe(intent, {:invalid_runner_return, other}, opts)
    end
  end

  defp observe_through(_intent, _runner, _opts), do: raise(ArgumentError, "adapter_runner must be a 2-arity function")

  # Dispatch / ReconcileSend through the same runner grammar: the closure captures ONLY the dispatch module, the
  # command and the dispatch options; `:expired` is answered from the OWNER's retained intent with the EXISTING
  # failure observation and a stable reason (the corroborated attention-only interface, no new vocabulary)
  @expired_dispatch %{"reason" => "dispatch_deadline_exceeded", "detector" => "dispatch_deadline"}
  @expired_reconcile %{"reason" => "dispatch_reconcile_timeout", "detector" => "dispatch_reconcile"}

  defp deliver_through(intent, nil, opts), do: observe(intent, adapter(intent, opts), opts)

  defp deliver_through(%{command: command, deadline_unix: deadline} = intent, runner, opts) when is_function(runner, 2) do
    module = dispatch_module(opts)
    dispatch_opts = dispatch_opts(opts)

    closure =
      case intent do
        %Effect.Dispatch{} -> fn -> module.deliver(command, dispatch_opts) end
        %Effect.ReconcileSend{} -> fn -> module.reconcile(command, dispatch_opts) end
      end

    case runner.(closure, %{deadline_unix: deadline}) do
      {:ok, raw} ->
        observe(intent, raw, opts)

      :expired ->
        expired(intent, opts)

      {:failed, diagnostic} = returned ->
        if AdapterRunner.diagnostic?(diagnostic),
          do: raise(AdapterFailure, diagnostic: diagnostic),
          else: observe(intent, {:invalid_runner_return, returned}, opts)

      other ->
        observe(intent, {:invalid_runner_return, other}, opts)
    end
  end

  defp deliver_through(_intent, _runner, _opts), do: raise(ArgumentError, "adapter_runner must be a 2-arity function")

  defp expired(%Effect.Dispatch{assignment_id: id}, opts),
    do: %Observation.DispatchFailed{assignment_id: id, reason: @expired_dispatch, now: now(opts)}

  defp expired(%Effect.ReconcileSend{assignment_id: id}, opts),
    do: %Observation.SendReconcileFailed{assignment_id: id, reason: @expired_reconcile, now: now(opts)}

  # every trappable failure inside an executor stage is carried out with the runtime that stage saw;
  # an Interrupted already carrying a NEWER runtime is never re-wrapped with an older one
  defp guarded(%Runtime{} = runtime, fun) when is_function(fun, 0) do
    fun.()
  catch
    :error, %Interrupted{} = interrupted ->
      reraise interrupted, __STACKTRACE__

    kind, reason ->
      raise Interrupted, kind: kind, reason: reason, stacktrace: __STACKTRACE__, runtime: runtime
  end

  # ---- gate effects: runtime objects (handles, Ack) live only in the runtime, keyed gate_run_id/attempt ----

  defp gate_prepare(%Effect.PrepareGate{gate_run_id: id, attempt: attempt} = effect, runtime, opts) do
    case helper_present(opts) do
      :ok ->
        # stage 1: the native prepare (no handle exists until it answers {:ok, prepared})
        request = prepare_request(effect, opts)
        prepared = guarded(runtime, fn -> gate_mod(opts).prepare(fs(opts), request, gate_opts(effect, opts)) end)
        prepared_observation(prepared, effect, runtime, opts)

      {:error, reason} ->
        {guarded(runtime, fn -> gate_prepare_failed(id, attempt, reason, opts) end), runtime}
    end
  end

  defp prepare_request(%Effect.PrepareGate{gate_run_id: id, attempt: attempt} = effect, opts) do
    %{
      run_id: Keyword.fetch!(opts, :run_id),
      gate_run_id: id,
      attempt: attempt,
      command_argv: effect.requested["command_argv"],
      repo_root: effect.repo_root,
      run_dir: effect.run_dir,
      deadline_unix: effect.deadline_unix,
      supervisor_instance: Keyword.fetch!(opts, :supervisor_instance)
    }
  end

  # stage 2: the handle is owned the moment it is transferred; everything after runs with it
  defp prepared_observation({:ok, prepared}, %Effect.PrepareGate{gate_run_id: id, attempt: attempt}, runtime, opts) do
    runtime = Runtime.put(runtime, {id, attempt}, :prepared, prepared)

    guarded(runtime, fn ->
      started = gate_mod(opts).started_data(prepared)
      {%Observation.GatePrepared{gate_run_id: id, attempt: attempt, started: started, now: now(opts)}, runtime}
    end)
  end

  defp prepared_observation({:error, reason}, %Effect.PrepareGate{gate_run_id: id, attempt: attempt}, runtime, opts),
    do: {guarded(runtime, fn -> gate_prepare_failed(id, attempt, reason, opts) end), runtime}

  defp gate_release(%Effect.ReleaseGate{gate_run_id: id, attempt: attempt}, runtime, opts, persisted) do
    key = {id, attempt}
    # the prepared handle stays owned while ack/release run: a failure here carries it out
    prepared = Runtime.handle(runtime, key)

    released =
      guarded(runtime, fn ->
        with {:ok, ack} <- gate_mod(opts).ack(prepared, persisted) do
          gate_mod(opts).release(prepared, ack, gate_opts_only(opts))
        end
      end)

    case released do
      {:ok, running} ->
        runtime = Runtime.put(runtime, key, :running, running)

        guarded(runtime, fn ->
          {%Observation.GateReleased{gate_run_id: id, attempt: attempt, now: now(opts)}, runtime}
        end)

      {:error, reason} ->
        # the abandonment's settlement is part of this failure's evidence: proven or unproven, it is
        # carried (it outranks any settle the rejection itself named). The handle is gone once
        # abandoned, so the observation is built under the POST-abandon runtime: a failure here must
        # not make the Host abandon the same handle again
        {%{"settle" => settle}, runtime} = abandon_one(runtime, key)

        guarded(runtime, fn ->
          mapped = Map.put(gate_reason(reason), "settle", settle)
          {%Observation.GateReleaseFailed{gate_run_id: id, attempt: attempt, reason: mapped, now: now(opts)}, runtime}
        end)
    end
  end

  defp gate_await(%Effect.AwaitGate{gate_run_id: id, attempt: attempt} = effect, runtime, opts) do
    # retained until the terminal's whole suffix commits (release_terminal/2) or the exit settles it
    running = Runtime.handle(runtime, {id, attempt})

    observation =
      guarded(runtime, fn ->
        case gate_mod(opts).await(running, gate_opts(effect, opts)) do
          {:exit, outcome} -> gate_exit_observation(id, attempt, outcome, opts)
          {:timeout, termination} -> gate_timeout_observation(id, attempt, termination, effect, opts)
          {:error, reason} -> %Observation.GateError{gate_run_id: id, reason: gate_reason(reason), now: now(opts)}
        end
      end)

    {observation, runtime}
  end

  # ---- the owner-resident await path (begin / resume / settle_await); the synchronous gate_await/3 is untouched ----

  @async_protocol [begin_await: 2, resume_await: 2, settle_await: 1, expire: 1]

  # opt-in is judged for the COMPLETE protocol, loadability- and type-safe; a partial double stays synchronous
  defp async_capable?(mod) when is_atom(mod) and not is_nil(mod) do
    Code.ensure_loaded?(mod) and Enum.all?(@async_protocol, fn {fun, arity} -> function_exported?(mod, fun, arity) end)
  end

  defp async_capable?(_other), do: false

  defp gate_begin(effect, key, running, executor, runtime, opts) do
    case guarded(runtime, fn -> executor.begin_await(running, gate_opts(effect, opts)) end) do
      {:done, answer} -> {:done, {guarded(runtime, fn -> gate_answer(effect, answer, opts) end), runtime}}
      {:pending, waiting} -> {:pending, Runtime.awaiting(runtime, key, waiting, effect)}
      _other -> interrupted(runtime, {:invalid_effect_return, :begin_await})
    end
  end

  # the primitive runs with the awaiting runtime (the latest at that stage); the entry then returns to its retained
  # running handle BEFORE the mapping, which runs with THAT runtime (a mapping failure carries it, never the stale one)
  defp complete(runtime, key, inputs, stage, call) do
    opts = Keyword.get(inputs, :opts, [])
    executor = gate_mod(opts)

    {waiting, effect} =
      case Map.get(runtime.gates, key) do
        %{phase: :awaiting, waiting: waiting, effect: %Effect.AwaitGate{} = effect} -> {waiting, effect}
        _other -> interrupted(runtime, {:not_awaiting, stage})
      end

    answer =
      case guarded(runtime, fn -> call.(executor, waiting) end) do
        {:done, answer} -> answer
        _other -> interrupted(runtime, {:invalid_effect_return, stage})
      end

    resumed = Runtime.resumed(runtime, key)
    {:done, {guarded(resumed, fn -> gate_answer(effect, answer, opts) end), resumed}}
  end

  # the answer grammar of await/2 mapped through the EXISTING builders; a shape outside it is a closed failure
  defp gate_answer(%Effect.AwaitGate{gate_run_id: id, attempt: attempt} = effect, answer, opts) do
    case answer do
      {:exit, outcome} when is_map(outcome) -> gate_exit_observation(id, attempt, outcome, opts)
      {:timeout, t} when is_map(t) -> gate_timeout_observation(id, attempt, t, effect, opts)
      {:error, reason} -> %Observation.GateError{gate_run_id: id, reason: gate_reason(reason), now: now(opts)}
      _other -> :erlang.error({:invalid_effect_return, :answer})
    end
  end

  defp interrupted(runtime, reason), do: guarded(runtime, fn -> :erlang.error(reason) end)

  # closed entry classes for pending/1: a complete awaiting carrier names its port; anything else is refused
  defp entry_class(
         key,
         %{
           phase: :awaiting,
           handle: %{port: port},
           waiting: %{port: port, ref: ref},
           effect: %Effect.AwaitGate{gate_run_id: id, attempt: attempt}
         } = entry
       )
       when is_port(port) and is_reference(ref) and map_size(entry) == 4 and key == {id, attempt}, do: {:awaiting, port}

  defp entry_class(_key, %{phase: :awaiting}), do: :awaiting_malformed

  defp entry_class(_key, %{phase: phase, handle: _handle} = entry)
       when phase in [:prepared, :running] and map_size(entry) == 2, do: :retained

  defp entry_class(_key, _entry), do: :malformed

  # one cleanup attempt through the executor; the executor's ACTUAL answer is what is reported. The
  # attempt AND its normalization sit under one catch: any escape or unclassifiable answer yields only
  # the closed settle_unproven, so a bad adapter return can neither stop later handles nor surface
  defp abandon_one(%Runtime{} = runtime, {id, attempt} = key) do
    settle =
      case Runtime.handle(runtime, key) do
        nil -> cleanup_settle(:no_handle)
        handle -> settle_attempt(runtime.executor, handle)
      end

    {%{"gate_run_id" => id, "attempt" => attempt, "settle" => settle}, Runtime.drop(runtime, key)}
  end

  defp settle_attempt(executor, handle) do
    cleanup_settle(executor.abandon(handle))
  catch
    _kind, _reason -> %{"clause" => "settle_unproven"}
  end

  defp gate_mod(opts), do: Keyword.get(opts, :gate_executor, Execution)

  defp gate_prepare_failed(id, attempt, reason, opts),
    do: %Observation.GatePrepareFailed{gate_run_id: id, attempt: attempt, reason: gate_reason(reason), now: now(opts)}

  # the helper the CLI/Config resolved must be a real executable file, else attention (never Gate.Runner)
  defp helper_present(opts) do
    case Keyword.get(opts, :gate_helper) do
      path when is_binary(path) ->
        stat = File.stat(path)

        if match?({:ok, %File.Stat{type: :regular}}, stat) and executable?(path),
          do: :ok,
          else: {:error, %{clause: "helper_missing"}}

      _ ->
        {:error, %{clause: "helper_missing"}}
    end
  end

  defp executable?(path) do
    case File.stat(path) do
      {:ok, %File.Stat{mode: mode}} -> Bitwise.band(mode, 0o111) != 0
      _ -> false
    end
  end

  defp gate_exit_observation(id, attempt, outcome, opts) do
    cond do
      match?({:error, _}, recorded_hashes(outcome)) and (gate_mod(opts).pass?(outcome) or settled_failure?(outcome)) ->
        {:error, reason} = recorded_hashes(outcome)
        %Observation.GateError{gate_run_id: id, reason: gate_reason(reason), now: now(opts)}

      gate_mod(opts).pass?(outcome) ->
        %Observation.GateFinished{gate_run_id: id, result: gate_passed_data(outcome), now: now(opts)}

      settled_failure?(outcome) ->
        %Observation.GateFailed{gate_run_id: id, result: gate_failed_data(outcome), now: now(opts)}

      true ->
        %Observation.GateUnsettled{gate_run_id: id, attempt: attempt, result: unsettled_result(outcome), now: now(opts)}
    end
  end

  defp gate_timeout_observation(id, attempt, termination, effect, opts) do
    gate_barrier(effect, opts, termination)

    if termination[:settled] and termination[:proof] == "gone" do
      with {:ok, evidence} <- gate_mod(opts).evidence(effect_run_dir(effect, opts), id, attempt),
           :ok <- recorded_hashes(evidence),
           {:ok, duration} <- measured_duration(termination) do
        %Observation.GateFailed{
          gate_run_id: id,
          result: timeout_failed_data(termination, evidence, duration),
          now: now(opts)
        }
      else
        {:error, reason} -> %Observation.GateError{gate_run_id: id, reason: gate_reason(reason), now: now(opts)}
      end
    else
      %Observation.GateUnsettled{
        gate_run_id: id,
        attempt: attempt,
        result: Map.new(termination, fn {k, v} -> {to_string(k), v} end),
        now: now(opts)
      }
    end
  end

  defp gate_barrier(%Effect.AwaitGate{}, opts, termination) do
    case opts |> Keyword.get(:gate_opts, []) |> Keyword.get(:barrier) do
      fun when is_function(fun, 2) -> fun.(:terminated, termination)
      _ -> :ok
    end
  end

  defp gate_reconcile(%Effect.ReconcileGate{gate_run_id: id, expected: expected} = effect, opts) do
    case gate_mod(opts).reconcile(fs(opts), effect_run_dir(effect, opts), expected, gate_opts(effect, opts)) do
      {:dead, facts} ->
        reconcile_dead(id, effect, facts, opts)

      {:unknown, facts} ->
        %Observation.GateReconciled{
          gate_run_id: id,
          attempt: expected["attempt"],
          verdict: :unknown,
          facts: gate_facts(facts),
          now: now(opts)
        }

      :no_claim ->
        %Observation.GateReconciled{
          gate_run_id: id,
          attempt: expected["attempt"],
          verdict: :no_claim,
          facts: %{},
          now: now(opts)
        }

      {:orphan_claim, info} ->
        %Observation.GateReconciled{
          gate_run_id: id,
          attempt: expected["attempt"],
          verdict: :orphan_claim,
          facts: gate_facts(info),
          now: now(opts)
        }

      {:error, reason} ->
        %Observation.GateReconcileFailed{
          gate_run_id: id,
          attempt: expected["attempt"],
          reason: gate_reason(reason),
          now: now(opts)
        }
    end
  end

  # a proven-dead prior attempt: the Host owns the evidence hashing (Execution.evidence),
  # journal-derived duration; unreadable evidence is attention, never a terminal with invented hashes
  defp reconcile_dead(id, effect, facts, opts) do
    with {:ok, evidence} <- gate_mod(opts).evidence(effect_run_dir(effect, opts), id, effect.attempt),
         :ok <- recorded_hashes(evidence) do
      %Observation.GateReconciled{
        gate_run_id: id,
        attempt: effect.attempt,
        verdict: :dead,
        facts: Map.put(gate_facts(facts), "evidence", Map.take(evidence, ["stdout_hash", "stderr_hash"])),
        now: now(opts)
      }
    else
      {:error, reason} ->
        %Observation.GateReconcileFailed{
          gate_run_id: id,
          attempt: effect.attempt,
          reason: gate_reason(reason),
          now: now(opts)
        }
    end
  end

  defp effect_run_dir(%{}, opts), do: Keyword.get(opts, :run_dir, ".")

  # ---- gate value building ----

  defp settled_failure?(outcome), do: outcome["settled"] == true and outcome["proof"] == "gone"

  defp gate_passed_data(outcome) do
    %{
      "exit_status" => outcome["exit_status"],
      "duration_ms" => outcome["duration_ms"],
      "stdout_hash" => outcome["stdout_hash"],
      "stderr_hash" => outcome["stderr_hash"]
    }
  end

  defp gate_failed_data(%{"exit_status" => status} = outcome) when is_integer(status) and status != 0 do
    base = %{
      "exit_status" => status,
      "duration_ms" => outcome["duration_ms"],
      "stdout_hash" => outcome["stdout_hash"],
      "stderr_hash" => outcome["stderr_hash"],
      "stderr_merged" => false
    }

    Map.put(base, "failure_summary", outcome["failure_summary"] || default_summary())
  end

  defp gate_failed_data(outcome) do
    termination = %{
      "kind" => signal_kind(outcome),
      "settled" => outcome["settled"],
      "leftovers" => outcome["leftovers"],
      "proof" => outcome["proof"]
    }

    termination = maybe_signal(termination, outcome["signal"])

    %{
      "exit_status" => nil,
      "termination" => termination,
      "duration_ms" => outcome["duration_ms"],
      "stdout_hash" => outcome["stdout_hash"],
      "stderr_hash" => outcome["stderr_hash"],
      "stderr_merged" => false,
      "failure_summary" => outcome["failure_summary"] || default_summary()
    }
  end

  defp signal_kind(%{"kind" => "signaled"}), do: "signal"
  defp signal_kind(_outcome), do: "timeout"
  defp maybe_signal(termination, signal) when is_integer(signal), do: Map.put(termination, "signal", signal)
  defp maybe_signal(termination, _), do: termination

  # the actual output hashes (Execution.evidence) and the termination's measured duration
  # evidence is two sha256 hashes in the exact grammar; anything else is unreadable evidence
  @sha256 ~r/\Asha256:[0-9a-f]{64}\z/
  defp recorded_hashes(evidence) when is_map(evidence) do
    Enum.find_value(["stdout_hash", "stderr_hash"], :ok, fn key ->
      value = Map.get(evidence, key)

      if is_binary(value) and Regex.match?(@sha256, value),
        do: nil,
        else: {:error, %{clause: "evidence_incomplete", field: key, missing: not is_binary(value)}}
    end)
  end

  defp recorded_hashes(_evidence), do: {:error, %{clause: "evidence_incomplete", missing: true}}

  # a terminal without a measured elapsed duration is never constructed: absent is unknown, not 0
  defp measured_duration(%{duration_ms: ms}) when is_integer(ms) and ms >= 0, do: {:ok, ms}

  defp measured_duration(_termination),
    do: {:error, %{clause: "evidence_incomplete", field: "duration_ms", missing: true}}

  defp timeout_failed_data(termination, evidence, duration) do
    %{
      "exit_status" => nil,
      "termination" => %{
        "kind" => "timeout",
        "settled" => termination[:settled],
        "leftovers" => termination[:leftovers],
        "proof" => termination[:proof]
      },
      "duration_ms" => duration,
      "stdout_hash" => evidence["stdout_hash"],
      "stderr_hash" => evidence["stderr_hash"],
      "stderr_merged" => false,
      "failure_summary" => default_summary()
    }
  end

  defp unsettled_result(outcome) when is_map(outcome) do
    %{}
    |> put_domain_optional("kind", raw(outcome, :kind), @termination_kinds)
    |> put_exit_status(raw(outcome, :exit_status))
    |> put_signal(raw(outcome, :signal))
    |> put_bool("settled", raw(outcome, :settled))
    |> put_leftovers(raw(outcome, :leftovers))
    |> put_domain_optional("proof", raw(outcome, :proof), @proofs)
  end

  defp unsettled_result(_outcome), do: %{}
  defp put_exit_status(map, v) when is_integer(v) and v >= 0 and v <= 255, do: Map.put(map, "exit_status", v)
  defp put_exit_status(map, _v), do: map
  defp put_signal(map, v) when is_integer(v) and v >= 1 and v <= 64, do: Map.put(map, "signal", v)
  defp put_signal(map, _v), do: map

  defp default_summary, do: %{"headline" => "gate did not pass", "failures" => [], "suggestion" => "inspect gate output"}

  # ---- closed diagnostic value domain (docs/contracts/gate-execution-wiring.org) ----

  @gate_clauses ~w(claim_unpublished claim_conflict prepare_failed release_failed already_released settle_unproven guardian_gone deadline_expired deadline_unsupported clock_unavailable helper_missing invalid_request output_unreadable evidence_incomplete claim_unreadable claim_mismatch claim_unexpected probe_invalid ack_mismatch await_failed)
  @gate_stages ~w(mkdir gates_dir open chmod write sync close link rm_temp dir_sync encode receipt list_dir lstat read)
  # errno classes (claim/evidence file work) plus the guardian's setup classes (protocol §SETUP_FAILED)
  @gate_errno ~w(eio enospc enoent eacces eexist erofs other) ++
                ~w(open_stdout open_stderr pipe fork chdir setsid ready_timeout identity usage protocol unknown)
  @gate_cleanup ~w(none removed removed_unsynced absent absent_unsynced cleanup_required foreign_final_untouched)
  @residue ~r"\Agates/(?!\.\.?\z)[A-Za-z0-9_.-]{1,200}\z"

  defp gate_reason(reason) when is_map(reason) do
    %{}
    |> put_domain("clause", raw(reason, :clause), @gate_clauses, "unknown")
    |> put_domain_optional("stage", raw(reason, :stage), @gate_stages)
    |> put_class(reason)
    |> put_domain_optional("cleanup", raw(reason, :cleanup), @gate_cleanup)
    |> put_temp_final(reason, "temp")
    |> put_temp_final(reason, "final")
    |> put_residue(raw(reason, :residue))
    |> put_settle(raw(reason, :settle))
    |> put_domain_optional("outputs", raw(reason, :outputs), ~w(removed left))
    |> put_field(raw(reason, :field))
    |> put_missing(raw(reason, :missing))
    |> put_int("max_ms", raw(reason, :max_ms))
  end

  defp gate_reason(_other), do: %{"clause" => "invalid_return"}

  # Execution reports atom-keyed maps; a test double may report string keys: one accessor for both
  # presence, never truthiness: a recorded `false` is a fact and survives
  defp raw(map, key) when is_atom(key) do
    cond do
      Map.has_key?(map, key) -> Map.fetch!(map, key)
      Map.has_key?(map, Atom.to_string(key)) -> Map.fetch!(map, Atom.to_string(key))
      true -> nil
    end
  end

  defp put_domain(map, key, value, domain, default) when is_binary(value),
    do: Map.put(map, key, if(Enum.member?(domain, value), do: value, else: default))

  defp put_domain(map, key, _value, _domain, default), do: Map.put(map, key, default)

  defp put_domain_optional(map, key, value, domain) when is_binary(value) do
    if Enum.member?(domain, value), do: Map.put(map, key, value), else: map
  end

  defp put_domain_optional(map, _key, _value, _domain), do: map

  defp put_class(map, reason),
    do: map |> put_domain("class", raw(reason, :class), @gate_errno, "other") |> drop_default_class(reason)

  defp drop_default_class(map, reason), do: if(raw(reason, :class) == nil, do: Map.delete(map, "class"), else: map)

  defp put_temp_final(map, reason, key),
    do:
      put_domain_optional(
        map,
        key,
        reason[String.to_atom(key)] || reason[key],
        ~w(removed removed_unsynced absent absent_unsynced)
      )

  defp put_residue(map, residue) when is_list(residue) do
    safe = residue |> Enum.filter(&(is_binary(&1) and Regex.match?(@residue, &1))) |> Enum.take(8)
    if safe == [], do: map, else: Map.put(map, "residue", safe)
  end

  defp put_residue(map, _residue), do: map

  defp put_settle(map, settle) when is_map(settle) do
    closed =
      %{}
      |> put_bool("settled", raw(settle, :settled))
      |> put_leftovers(raw(settle, :leftovers))
      |> put_domain_optional("proof", raw(settle, :proof), @proofs)
      |> put_domain_optional("reason", raw(settle, :reason), @settle_reasons)
      |> put_domain_optional("clause", raw(settle, :clause), @settle_clauses)
      |> put_worker(raw(settle, :worker))

    Map.put(map, "settle", closed)
  end

  defp put_settle(map, _settle), do: map

  # leftovers is a count or the word unknown; an integer count is normalized to its decimal string
  defp put_leftovers(map, v) when is_integer(v) and v >= 0, do: Map.put(map, "leftovers", Integer.to_string(v))

  defp put_leftovers(map, v) when is_binary(v) and v != "" and byte_size(v) <= 11 do
    if v == "unknown" or Regex.match?(~r/\A(0|[1-9][0-9]{0,9})\z/, v), do: Map.put(map, "leftovers", v), else: map
  end

  defp put_leftovers(map, _v), do: map
  defp put_worker(map, false), do: Map.put(map, "worker", false)
  defp put_worker(map, _), do: map
  defp put_bool(map, key, v) when is_boolean(v), do: Map.put(map, key, v)
  defp put_bool(map, _key, _v), do: map

  defp put_field(map, v) when is_binary(v),
    do: if(String.valid?(v) and byte_size(v) <= 64, do: Map.put(map, "field", v), else: map)

  defp put_field(map, _v), do: map
  defp put_missing(map, true), do: Map.put(map, "missing", true)
  defp put_missing(map, _v), do: map
  defp put_int(map, key, v) when is_integer(v), do: Map.put(map, key, v)
  defp put_int(map, _key, _v), do: map

  # reconcile facts are a closed value domain too: liveness words, a member count, the kernel
  # start token (or the dash for none), and the one reuse marker; anything else is dropped
  @kernel_start ~r/\A[0-9]{1,20}\.[0-9]{6}\z/
  defp gate_facts(facts) when is_map(facts) do
    %{}
    |> put_domain_optional("leader", raw(facts, :leader), @liveness)
    |> put_domain_optional("group", raw(facts, :group), @liveness)
    |> put_members(raw(facts, :members))
    |> put_start(raw(facts, :start))
    |> put_domain_optional("leader_pid", raw(facts, :leader_pid), ~w(reused))
  end

  defp gate_facts(_facts), do: %{}
  defp put_members(map, v) when is_integer(v) and v >= 0, do: Map.put(map, "members", v)
  defp put_members(map, "unknown"), do: Map.put(map, "members", "unknown")
  defp put_members(map, _v), do: map
  defp put_start(map, "-"), do: Map.put(map, "start", "-")

  defp put_start(map, v) when is_binary(v),
    do: if(Regex.match?(@kernel_start, v), do: Map.put(map, "start", v), else: map)

  defp put_start(map, _v), do: map

  defp cleanup_settle(:ok), do: %{"settled" => true, "proof" => "gone"}

  defp cleanup_settle({:error, %{} = reason}) do
    case raw(reason, :clause) do
      "abandon_unsettled" -> unproven_settle("abandon_unsettled", reason)
      "guardian_gone" -> %{"clause" => "guardian_gone"}
      "settle_unproven" -> unproven_settle("settle_unproven", reason)
      _other -> gate_reason(reason)["settle"] || unproven_settle("settle_unproven", reason)
    end
  end

  defp cleanup_settle(_other), do: %{"clause" => "settle_unproven"}

  defp unproven_settle(clause, fields) do
    %{"clause" => clause}
    |> put_bool("settled", raw(fields, :settled))
    |> put_leftovers(raw(fields, :leftovers))
    |> put_domain_optional("proof", raw(fields, :proof), @proofs)
    |> put_domain_optional("reason", raw(fields, :reason), @settle_reasons)
  end

  defp gate_opts(effect, opts) do
    opts
    |> Keyword.get(:gate_opts, [])
    |> Keyword.put_new(:helper, Keyword.get(opts, :gate_helper))
    |> Keyword.put_new(:clock, Keyword.get(opts, :clock, SystemClock))
    |> Keyword.put_new(:deadline_unix, Map.get(effect, :deadline_unix))
  end

  defp gate_opts_only(opts) do
    opts |> Keyword.get(:gate_opts, []) |> Keyword.put_new(:clock, Keyword.get(opts, :clock, SystemClock))
  end

  defp adapter(%Effect.Clock{}, opts), do: unix_now(opts)

  # ---- raw adapter results become the effect's admissible observation ----
  # A bijection with the shapes the reducer matches on; the host decides nothing. The only
  # additions are typed errors where an adapter returns a shape outside its behaviour.
  defp adapter(%Effect.Dispatch{command: command}, opts) do
    dispatch_module(opts).deliver(command, dispatch_opts(opts))
  end

  @reconcile_outcomes ~w(delivered queued absent ambiguous conflict)
  @receipt_outcomes ~w(delivered queued ambiguous)
  @snapshot_error_classes ~w(artifact_baseline_unstable artifact_baseline_failed)

  defp adapter(%Effect.SnapshotArtifact{command: command}, opts) do
    dispatch_module(opts).snapshot(command, dispatch_opts(opts))
  end

  defp adapter(%Effect.ReconcileSend{command: command}, opts) do
    dispatch_module(opts).reconcile(command, dispatch_opts(opts))
  end

  # The host owns the wait: it sleeps until the deadline by its own clock and answers with
  # the moment it woke. A scripted clock that is already past the deadline makes this free.
  defp adapter(%Effect.Timer{deadline_unix: deadline_unix}, opts) do
    Process.sleep(max(deadline_unix - unix_now(opts), 0) * 1000)
    :ok
  end

  defp adapter(%Effect.Observe{command: command, deadline_unix: deadline_unix}, opts) do
    dispatch_module(opts).observe(command, observe_opts(deadline_unix, opts))
  end

  defp adapter(%Effect.ReadReview{path: path}, opts) do
    reader = Keyword.get(opts, :review_reader, &File.read/1)
    reader.(path)
  end

  defp adapter(%Effect.RunGate{requested: gate_requested, repo_root: repo_root, run_dir: run_dir}, opts) do
    runner = Keyword.get(opts, :gate_runner, &GateRunner.run/2)

    gate_opts =
      opts |> Keyword.get(:gate_opts, []) |> Keyword.put_new(:repo_root, repo_root) |> Keyword.put_new(:run_dir, run_dir)

    cond do
      is_function(runner, 2) -> runner.(gate_requested, gate_opts)
      is_function(runner, 1) -> runner.(gate_requested)
      true -> {:error, %{"reason" => "invalid_gate_runner"}}
    end
  end

  # The store is not an injected adapter: it is the product's own code, reached through the
  # same `Journal.Fs` seam the writer uses, so a test injects faults at the seam rather than
  # substituting a store. What varies per call is only where the objects live.
  defp adapter(%Effect.RetainPrompt{assignment_id: id, bytes: %SensitiveBytes{} = bytes, scheme: scheme}, opts),
    do: PromptStore.put(fs(opts), prompt_root(opts), id, bytes, scheme: scheme)

  defp adapter(%Effect.FetchPrompt{object: %PromptObject{} = object}, opts),
    do: PromptStore.fetch_verified(fs(opts), prompt_root(opts), object)

  defp observe(%Effect.Clock{read_index: index}, unix, _opts) when is_integer(unix),
    do: %Observation.Clock{read_index: index, now: moment(unix)}

  defp observe(%Effect.Dispatch{assignment_id: id}, {:ok, result}, opts),
    do: %Observation.Dispatched{assignment_id: id, result: result, now: now(opts)}

  defp observe(%Effect.Dispatch{assignment_id: id}, {:error, reason}, opts) when is_map(reason),
    do: %Observation.DispatchFailed{assignment_id: id, reason: reason, now: now(opts)}

  defp observe(%Effect.Dispatch{assignment_id: id}, other, opts),
    do: %Observation.DispatchFailed{assignment_id: id, reason: invalid_return("dispatch", other), now: now(opts)}

  # The outcome set is closed here, at the boundary: an adapter's word outside the five
  # ratified outcomes, or an attempt that is not a count, never becomes an observation the
  # reducer has to have a clause for. It is an invalid return, described by class and digest.
  defp observe(%Effect.ReconcileSend{assignment_id: id}, {:ok, %{"outcome" => outcome} = answer}, opts)
       when outcome in @reconcile_outcomes do
    case reconcile_attempt(answer, outcome) do
      {:ok, attempt} ->
        %Observation.SendReconciled{assignment_id: id, outcome: outcome, delivery_attempt: attempt, now: now(opts)}

      :invalid ->
        %Observation.SendReconcileFailed{
          assignment_id: id,
          reason: invalid_return("dispatch_reconcile", answer),
          now: now(opts)
        }
    end
  end

  defp observe(%Effect.ReconcileSend{assignment_id: id}, {:error, reason}, opts) when is_map(reason),
    do: %Observation.SendReconcileFailed{assignment_id: id, reason: reason, now: now(opts)}

  defp observe(%Effect.ReconcileSend{assignment_id: id}, other, opts),
    do: %Observation.SendReconcileFailed{
      assignment_id: id,
      reason: invalid_return("dispatch_reconcile", other),
      now: now(opts)
    }

  # The baseline crosses the typed boundary only in the closed recorded grammar; anything
  # else is an invalid adapter return, described by class and digest, never a reducer event.
  defp observe(%Effect.SnapshotArtifact{assignment_id: id}, {:ok, baseline} = returned, opts) do
    if ArtifactBaseline.recorded?(baseline),
      do: %Observation.ArtifactSnapshot{assignment_id: id, baseline: baseline, now: now(opts)},
      else: %Observation.ArtifactSnapshotFailed{
        assignment_id: id,
        reason: invalid_return("dispatch_snapshot", returned),
        now: now(opts)
      }
  end

  # A snapshot error crosses the boundary as a closed class and nothing else: the adapter's
  # own detail (a posix reason, a path, any text) is dropped here, and a reason outside the
  # closed set is not a snapshot error at all but an invalid return, described by class and
  # digest. Nothing an adapter writes can reach an attention event.
  defp observe(%Effect.SnapshotArtifact{assignment_id: id}, {:error, %{"reason" => class}}, opts)
       when class in @snapshot_error_classes,
       do: %Observation.ArtifactSnapshotFailed{assignment_id: id, reason: %{"reason" => class}, now: now(opts)}

  defp observe(%Effect.SnapshotArtifact{assignment_id: id}, {:error, _other} = returned, opts),
    do: %Observation.ArtifactSnapshotFailed{
      assignment_id: id,
      reason: invalid_return("dispatch_snapshot", returned),
      now: now(opts)
    }

  defp observe(%Effect.SnapshotArtifact{assignment_id: id}, other, opts),
    do: %Observation.ArtifactSnapshotFailed{
      assignment_id: id,
      reason: invalid_return("dispatch_snapshot", other),
      now: now(opts)
    }

  defp observe(%Effect.Timer{purpose: purpose, deadline_unix: deadline_unix}, :ok, opts),
    do: %Observation.Deadline{purpose: purpose, deadline_unix: deadline_unix, now: now(opts)}

  defp observe(%Effect.Observe{assignment_id: id}, {:ok, artifact}, opts),
    do: %Observation.ArtifactObserved{assignment_id: id, artifact: artifact, now: now(opts)}

  defp observe(%Effect.Observe{assignment_id: id}, {:blocked, reason}, opts),
    do: %Observation.Blocked{assignment_id: id, reason: reason, now: now(opts)}

  defp observe(%Effect.Observe{assignment_id: id}, {:pending, details}, opts),
    do: %Observation.Pending{assignment_id: id, details: details, now: now(opts)}

  defp observe(%Effect.Observe{assignment_id: id}, {:error, reason}, opts) when is_map(reason),
    do: %Observation.ObserveFailed{assignment_id: id, reason: reason, now: now(opts)}

  defp observe(%Effect.Observe{assignment_id: id}, other, opts),
    do: %Observation.ObserveFailed{assignment_id: id, reason: invalid_return("observe", other), now: now(opts)}

  defp observe(%Effect.ReadReview{assignment_id: id}, {:ok, contents}, opts) when is_binary(contents),
    do: %Observation.ReviewRead{assignment_id: id, contents: contents, now: now(opts)}

  defp observe(%Effect.ReadReview{assignment_id: id, path: path}, {:error, reason}, opts) when reason in @file_errors do
    normalized = %{"reason" => "review_unreadable", "class" => Atom.to_string(reason)}
    %Observation.ReviewUnreadable{assignment_id: id, path: path, reason: normalized, now: now(opts)}
  end

  defp observe(%Effect.ReadReview{assignment_id: id, path: path}, other, opts),
    do: %Observation.ReviewUnreadable{
      assignment_id: id,
      path: path,
      reason: invalid_return("review_reader", other),
      now: now(opts)
    }

  defp observe(%Effect.RunGate{gate_run_id: id}, {:ok, result}, opts),
    do: %Observation.GateFinished{gate_run_id: id, result: result, now: now(opts)}

  defp observe(%Effect.RunGate{gate_run_id: id}, {:failed, result}, opts),
    do: %Observation.GateFailed{gate_run_id: id, result: result, now: now(opts)}

  defp observe(%Effect.RunGate{gate_run_id: id}, {:error, reason}, opts) when is_map(reason),
    do: %Observation.GateError{gate_run_id: id, reason: reason, now: now(opts)}

  # ---- identity, exactly as the sequential supervisor resolved it ----

  defp observe(%Effect.RunGate{gate_run_id: id}, other, opts),
    do: %Observation.GateError{gate_run_id: id, reason: invalid_return("gate_runner", other), now: now(opts)}

  # A store rejection is a pair; the observation's reason is a map. The translation happens
  # here, at the effect where the store ran, and nowhere later: handing the reducer the raw
  # pair would either teach it the rejection vocabulary -- a second copy of the table -- or
  # carry the store's own atoms onward to the journal writer. `describe_rejection/1` reflects
  # a class the table admits, drops one it does not, and never reflects a path.
  defp observe(%Effect.RetainPrompt{}, {:ok, %PromptObject{} = object}, opts),
    do: %Observation.PromptRetained{object: object, now: now(opts)}

  defp observe(%Effect.RetainPrompt{assignment_id: id}, {:error, {reason, _class} = rejection}, opts)
       when is_atom(reason),
       do: %Observation.PromptRetentionFailed{
         assignment_id: id,
         reason: Diagnostic.describe_rejection(rejection),
         now: now(opts)
       }

  defp observe(%Effect.RetainPrompt{assignment_id: id}, other, opts),
    do: %Observation.PromptRetentionFailed{
      assignment_id: id,
      reason: invalid_return("prompt_store", other),
      now: now(opts)
    }

  defp observe(%Effect.FetchPrompt{object: object}, {:ok, %SensitiveBytes{} = bytes}, opts),
    do: %Observation.PromptFetched{object: object, bytes: bytes, now: now(opts)}

  defp observe(
         %Effect.FetchPrompt{object: %PromptObject{assignment_id: id}},
         {:error, {reason, _class} = rejection},
         opts
       )
       when is_atom(reason),
       do: %Observation.PromptFetchFailed{
         assignment_id: id,
         reason: Diagnostic.describe_rejection(rejection),
         now: now(opts)
       }

  defp observe(%Effect.FetchPrompt{object: %PromptObject{assignment_id: id}}, other, opts),
    do: %Observation.PromptFetchFailed{assignment_id: id, reason: invalid_return("prompt_store", other), now: now(opts)}

  # Mirrors LocalPane's rule at the boundary so no adapter can hand the reducer a receipt
  # without its attempt: an answer that speaks for a stored receipt (a status, or a
  # delivered / queued / ambiguous outcome) must carry a positive count; only a genuine
  # no-record absent or conflict reads as 0.
  defp reconcile_attempt(%{"status" => _stored} = answer, _outcome), do: positive_attempt(answer)

  defp reconcile_attempt(answer, outcome) when outcome in @receipt_outcomes, do: positive_attempt(answer)

  defp reconcile_attempt(answer, _no_record) do
    case Map.get(answer, "delivery_attempt", 0) do
      attempt when is_integer(attempt) and attempt >= 0 -> {:ok, attempt}
      _not_a_count -> :invalid
    end
  end

  defp positive_attempt(%{"delivery_attempt" => attempt}) when is_integer(attempt) and attempt > 0, do: {:ok, attempt}
  defp positive_attempt(_answer), do: :invalid

  defp invalid_return(adapter, other), do: Map.put(Diagnostic.describe(other), "reason", adapter <> "_invalid_return")

  # Observation moments are evidence, never journal truth; deriving wall_ts from the unix read keeps
  # the clock seam's tick stream untouched (only the reducer's Clock effects consume it).
  defp now(opts), do: opts |> unix_now() |> moment()
  defp moment(unix), do: %Moment{unix: unix, wall_ts: unix |> DateTime.from_unix!() |> DateTime.to_iso8601()}

  defp observe_opts(deadline_unix, opts) do
    remaining_ms = max(deadline_unix - unix_now(opts), 0) * 1000
    base = dispatch_opts(opts)

    capped =
      case Keyword.get(base, :observe_timeout_ms) do
        existing when is_integer(existing) and existing >= 0 -> min(existing, remaining_ms)
        _other -> remaining_ms
      end

    Keyword.put(base, :observe_timeout_ms, capped)
  end

  defp fs(opts), do: Keyword.get(opts, :fs, SystemFs.new())
  # The objects live under the run directory unless a caller says otherwise. There is no
  # further default: an object root guessed from the working directory is a prompt written
  # somewhere nobody asked for, so a host with neither refuses to retain at all.
  defp prompt_root(opts) do
    case Keyword.get(opts, :prompt_root, Keyword.get(opts, :run_dir)) do
      root when is_binary(root) -> root
      _other -> raise ArgumentError, "prompt retention needs :prompt_root or :run_dir in the host options"
    end
  end

  defp dispatch_module(opts), do: Keyword.get(opts, :dispatch, AiOrchestrator.Dispatch.LocalPane)
  defp dispatch_opts(opts), do: Keyword.get(opts, :dispatch_opts, [])
  defp unix_now(opts), do: Keyword.get(opts, :clock, SystemClock).unix_now()
end
