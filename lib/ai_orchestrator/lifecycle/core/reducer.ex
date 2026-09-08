defmodule AiOrchestrator.Lifecycle.Core.Reducer do
  @moduledoc """
  Pure, replayable execution core: an explicit state machine, not a coroutine.

  `init/3`, `resume/4` and `cancel/2` run the same decision code the sequential
  supervisor ran until it reaches an effect site. There the machine suspends:
  `{:effect, intent, state, events}` carries the intent, the reified continuation,
  and the events emitted since the previous suspension. The host executes the
  intent, journals that suffix, and calls `step/2` with the observation, which
  consumes exactly that one observation and runs on to the next suspension or to
  `{:done, state, events}`. Nothing is re-run: the continuation is a stack of
  frames held in the state, so every step is a bounded, flat reduction and no
  observation transcript is kept.

  Events carry a placeholder timestamp; the host stamps and journals them. Wall
  clock reads are effects too, served after the preceding suffix is committed, so
  a resumed run reads the times a sequential supervisor would. Ids are
  sequence-derived; run and supervisor ids arrive in opts. Effects and
  observations are `AiOrchestrator.Contract` structs, and an observation that is
  not admissible for the pending effect, or that carries a different correlation
  id, is a named rejection rather than a silent divergence.
  """

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SendId
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Fold.Context
  alias AiOrchestrator.Lifecycle.Core.Diagnostic
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Spec.RunSpec

  @zero_hash "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  @default_assignment_timeout_s 900

  defstruct clock_reads: 0,
            completed_work_item_ids: [],
            emitted: [],
            events: [],
            next_assignment: 1,
            next_attention: 1,
            next_gate: 1,
            next_pane_release: 1,
            next_review: 1,
            next_workspace_release: 1,
            pending: nil,
            plan: nil,
            run: %{},
            spec: nil,
            stack: []

  @placeholder_ts "0000-00-00T00:00:00Z"

  @type t :: %__MODULE__{}
  @type intent :: Effect.t()
  @type outcome ::
          {:effect, intent(), t(), [map()]}
          | {:done, t(), [map()]}
          | {:error, map()}

  @doc "Starts a fresh run; `opts` must carry :run_id and :supervisor_instance (the host resolves ids)."
  @spec init(map(), map(), keyword()) :: outcome()
  def init(spec, plan, opts) do
    with {:ok, validated_spec} <- RunSpec.validate(spec),
         {:ok, validated_plan} <- Plan.validate(plan, validated_spec) do
      execute(%__MODULE__{
        plan: validated_plan,
        # ---- the abstract machine ----
        #
        # `perform/3` suspends: it records the pending effect, pushes the frame that will consume
        # that effect's observation, and hands the host the events emitted since the previous
        # suspension. `ret/2` returns a value to the frame on top of the stack; an empty stack ends
        # the run. Every step is one flat pass -- no nested driver, no re-execution of the prefix.
        run: run_metadata(validated_spec, validated_plan, opts),
        spec: validated_spec
      })
    end
  end

  @spec resume(map(), map(), [map()], keyword()) :: outcome()
  # Prior events arrive as decoded journal lines. Each is validated as written -- envelope
  # and payload at its own version -- before its upgraded view is built, so a malformed
  # line is a named rejection and never a crash in the upcaster; the reducer then works on
  # the views. The host keeps the exact maps for the result and the chain.
  def resume(spec, plan, prior_events, opts) do
    with {:ok, validated_spec} <- RunSpec.validate(spec),
         {:ok, validated_plan} <- Plan.validate(plan, validated_spec),
         {:ok, prior_views} <- upcast_all(prior_events),
         {:ok, prior_state} <- Fold.fold_views(prior_views) do
      resume_execution(validated_spec, validated_plan, prior_state, prior_views, opts)
    end
  end

  @spec cancel([map()], keyword()) :: outcome()
  def cancel(prior_events, opts) do
    with {:ok, prior_views} <- upcast_all(prior_events),
         {:ok, prior_state} <- Fold.fold_views(prior_views) do
      cancel_execution(prior_state, prior_views, opts)
    end
  end

  # ---- bounded internal continuation (docs/contracts/command-executor-migration.org, unit C) ----
  #
  # A command whose acceptance row is already durable and is the LAST lifecycle acceptance of a nonterminal
  # prefix is continued, never re-accepted: no second run_created / run_resumed / run_cancel_requested, no
  # stamp. `acceptance` names that row (its seq and the verb it stamps) and is descriptive provenance built
  # by the run server from the locked, verified prefix; it is validated against the prefix here and never
  # taken on trust. start emits only the genuinely missing preamble suffix, then the existing recovery;
  # resume is the existing recovery without a new run_resumed; cancel releases only the leases still held
  # (a request left pending is completed under its own id) and then journals run_cancelled.
  @continuation_acceptance %{"start" => "run_created", "resume" => "run_resumed", "cancel" => "run_cancel_requested"}
  @lifecycle_acceptances Map.values(@continuation_acceptance)
  @preamble_suffix ~w(run_spec_loaded plan_recorded run_started)

  @spec continue(map() | nil, map() | nil, [map()], %{seq: pos_integer(), verb: String.t()}, keyword()) :: outcome()
  def continue(spec, plan, prior_events, %{seq: seq, verb: verb}, opts) when is_list(prior_events) do
    with :ok <- continuation_acceptance(prior_events, seq, verb),
         {:ok, prior_views} <- upcast_all(prior_events),
         {:ok, prior_state} <- Fold.fold_views(prior_views),
         :ok <- continuable(prior_state) do
      continue_execution(verb, spec, plan, prior_state, prior_views, opts)
    end
  end

  def continue(_spec, _plan, _prior_events, _acceptance, _opts),
    do: {:error, %{clause: "continuation_acceptance_invalid"}}

  defp continuation_acceptance(events, seq, verb) do
    expected = Map.get(@continuation_acceptance, verb)
    row = Enum.find(events, &(&1["seq"] == seq))
    later = events |> Enum.drop_while(&(&1["seq"] != seq)) |> Enum.drop(1)

    cond do
      is_nil(expected) or is_nil(row) or row["type"] != expected -> {:error, %{clause: "continuation_acceptance_invalid"}}
      Enum.any?(later, &(&1["type"] in @lifecycle_acceptances)) -> {:error, %{clause: "continuation_acceptance_invalid"}}
      true -> :ok
    end
  end

  defp continuable(%{terminal?: true}), do: {:error, %{clause: "continuation_acceptance_invalid"}}
  defp continuable(_state), do: :ok

  defp continue_execution("cancel", _spec, _plan, fold_state, prior_views, opts) do
    state = cancel_state_from_fold(fold_state, prior_views, opts)

    state
    |> release_active_leases(fold_state)
    |> emit("run_cancelled", run_cancel_data(state.run, fold_state, opts))
    |> halt(:ok)
  end

  defp continue_execution(verb, spec, plan, fold_state, prior_views, opts) do
    with {:ok, validated_spec} <- RunSpec.validate(spec),
         {:ok, validated_plan} <- Plan.validate(plan, validated_spec) do
      # a continued start is still the original start: its run_started says resume false
      opts = if verb == "start", do: Keyword.put(opts, :resume, false), else: opts
      state = state_from_fold(validated_spec, validated_plan, fold_state, prior_views, opts)

      with :ok <- no_open_attention(fold_state),
           :ok <- lease_ownership_coherent(prior_views, fold_state) do
        state
        |> emit_missing_preamble(verb)
        |> repair_stale_leases(fold_state)
        |> push(:normalize_execution)
        |> continue_resumed(fold_state)
      end
    end
  end

  defp no_open_attention(fold_state) do
    if MapSet.size(fold_state.open_attention_ids) > 0,
      do:
        {:error, %{"reason" => "attention_required", "open_attention_ids" => sorted_set(fold_state.open_attention_ids)}},
      else: :ok
  end

  defp emit_missing_preamble(state, "start") do
    present = MapSet.new(state.events, & &1["type"])

    Enum.reduce(@preamble_suffix, state, fn type, state ->
      if MapSet.member?(present, type), do: state, else: emit(state, type, preamble_data(state, type))
    end)
  end

  defp emit_missing_preamble(state, _resume), do: state

  defp preamble_data(state, "run_spec_loaded"), do: run_spec_loaded_data(state.run)
  defp preamble_data(state, "plan_recorded"), do: plan_recorded_data(state.run, state.plan)
  defp preamble_data(state, "run_started"), do: run_started_data(state.run)

  @doc """
  Consumes exactly one observation for the pending effect and runs on to the next
  suspension or to the end. The observation must be admissible for that effect and
  carry the same correlation id; anything else is a typed, redacted rejection.
  """
  @spec step(t(), Observation.t()) :: outcome()
  def step(%__MODULE__{pending: nil}, observation) do
    {:error, %{"reason" => "no_pending_effect", "observed" => Diagnostic.describe(observation)}}
  end

  def step(%__MODULE__{pending: pending, stack: [frame | rest]} = state, observation) do
    if correlated?(pending, observation) do
      resume_effect(frame, %{state | pending: nil, stack: rest}, unwrap(observation))
    else
      {:error,
       %{
         "reason" => "observation_mismatch",
         "expected" => Diagnostic.describe(pending),
         "observed" => Diagnostic.describe(observation)
       }}
    end
  end

  defp perform(state, effect, frame) do
    {:effect, effect, %{state | emitted: [], pending: effect, stack: [frame | state.stack]}, Enum.reverse(state.emitted)}
  end

  defp push(state, frame), do: %{state | stack: [frame | state.stack]}

  defp ret(state, value) do
    case state.stack do
      [frame | rest] -> apply_frame(frame, %{state | stack: rest}, value)
      # ---- effect resumption: exactly one observation value per pending effect ----
      #
      # These clauses take raw adapter-facing values from `unwrap/1`. There is deliberately no
      # generic error clause here: a `{:error, _}` that a site handles locally (an unreadable review
      # artifact) must not be mistaken for one that aborts the run.
      [] -> halt(state, value)
    end
  end

  defp halt(state, value) when value in [:ok, :blocked] do
    {:done, %{state | emitted: []}, Enum.reverse(state.emitted)}
  end

  defp halt(_state, {:error, reason}), do: {:error, reason}

  # An observation answers the pending effect only when its type is admissible for that effect
  # and it carries the same correlation id. No contract struct carries two of these keys, so the
  # clause order below is unambiguous; an uncorrelated effect matches on admissibility alone.
  defp correlated?(effect, %{__struct__: module} = observation) do
    module in Effect.admissible_observations(effect) and correlation(effect) == correlation(observation)
  end

  defp correlated?(_effect, _observation), do: false

  defp correlation(%{read_index: read_index}), do: {:read_index, read_index}
  defp correlation(%{gate_run_id: gate_run_id}), do: {:gate_run_id, gate_run_id}
  defp correlation(%{assignment_id: assignment_id}), do: {:assignment_id, assignment_id}
  defp correlation(%{object: %PromptObject{assignment_id: assignment_id}}), do: {:assignment_id, assignment_id}
  defp correlation(_other), do: :uncorrelated

  defp resume_effect({:assignment_deadline, context}, state, now), do: assignment_requested(state, context, now)

  defp resume_effect({:prompt_retained, context, deadline_unix, rendered_prompt, prompt}, state, retained) do
    prompt_retained(state, context, deadline_unix, rendered_prompt, prompt, retained)
  end

  defp resume_effect({:resumed_prompt_retained, resumption}, state, retained) do
    resumed_prompt_retained(state, resumption, retained)
  end

  defp resume_effect({:resumed_prompt_fetched, resumption, object}, state, fetched) do
    resumed_prompt_fetched(state, resumption, object, fetched)
  end

  defp resume_effect({:resumed_v1_restored, resumption, object}, state, restored) do
    resumed_v1_restored(state, resumption, object, restored)
  end

  defp resume_effect({:resumed_snapshotted, resumption, object}, state, snapshotted) do
    resumed_snapshotted(state, resumption, object, snapshotted)
  end

  defp resume_effect({:artifact_snapshotted, context, deadline_unix, bytes, prompt, object}, state, snapshotted) do
    artifact_snapshotted(state, context, deadline_unix, bytes, prompt, object, snapshotted)
  end

  defp resume_effect({:assignment_dispatched, context, deadline_unix, command}, state, dispatched) do
    # ---- continuation frames: exactly one call return per frame ----
    #
    # These clauses take call returns, not observations. The two trailing clauses are the
    # pass-through the original `with`/`case` chains had: a blocked or failed step unwinds
    # unchanged until a frame names it.
    assignment_dispatched(state, context, deadline_unix, command, dispatched)
  end

  defp resume_effect({:assignment_observed, context}, state, observed) do
    assignment_observed(state, context, observed)
  end

  defp resume_effect({:dispatch_recorded, assignment_id, command}, state, dispatched) do
    dispatch_recorded(state, assignment_id, command, dispatched)
  end

  defp resume_effect({:queued_send_reconciled, queued}, state, outcome),
    do: queued_send_reconciled(state, queued, outcome)

  defp resume_effect({:queued_send_clock, queued}, state, now), do: queued_send_clock(state, queued, now)
  defp resume_effect({:queued_send_retry_clock, queued}, state, now), do: queued_send_retry_clock(state, queued, now)
  defp resume_effect({:queued_send_wake, queued}, state, _woke_at), do: converge_queued_send(state, queued)

  defp resume_effect({:queued_send_retried, queued}, state, dispatched),
    do: queued_send_retried(state, queued, dispatched)

  defp resume_effect({:queued_send_deadline, assignment_id, command, dispatch_data}, state, now) do
    converge_queued_send(state, recorded_queued_send(state, assignment_id, command, dispatch_data, now))
  end

  defp resume_effect({:observation_started_deadline, assignment_id}, state, now) do
    ret(observation_started(state, assignment_id, now), :ok)
  end

  defp resume_effect({:artifact_deadline, assignment_id, command}, state, now) do
    observe_artifact(state, assignment_id, command, now)
  end

  defp resume_effect({:artifact_observed, assignment_id, command}, state, observed) do
    artifact_observed(state, assignment_id, command, observed)
  end

  defp resume_effect({:review_read, artifact}, state, read), do: review_verdict(state, artifact, read)

  defp resume_effect({:gate_finished, gate_run_id}, state, finished) do
    gate_finished(state, gate_run_id, finished)
  end

  defp resume_effect({:resumed_gate_finished, gate_run_id, work_item, writer, attempt}, state, finished) do
    resumed_gate_finished(state, gate_run_id, work_item, writer, attempt, finished)
  end

  defp resume_effect({:gate_deadline, gate_requested, frame}, state, now),
    do: gate_deadline(state, gate_requested, frame, now)

  defp resume_effect({:gate_prepared, gate_requested, deadline_unix, attempt, frame}, state, prepared),
    do: gate_prepared(state, gate_requested, deadline_unix, attempt, frame, prepared)

  defp resume_effect({:gate_released, gate_requested, deadline_unix, attempt, frame}, state, released),
    do: gate_released(state, gate_requested, deadline_unix, attempt, frame, released)

  defp resume_effect({:gate_awaited, gate_run_id, frame}, state, outcome),
    do: gate_awaited(state, gate_run_id, frame, outcome)

  defp resume_effect({:gate_reconciled, gate_requested, attempt, frame}, state, verdict),
    do: gate_reconciled(state, gate_requested, attempt, frame, verdict)

  defp resume_effect({:gate_recovery, gate_requested, facts, attempt, frame}, state, now),
    do: gate_recovery(state, gate_requested, facts, attempt, frame, now)

  defp apply_frame({:work_items, rest}, state, :ok), do: work_items_loop(state, rest)
  defp apply_frame({:work_items, _rest}, state, :blocked), do: ret(state, :ok)
  defp apply_frame(:run_work_items, state, :ok), do: run_work_items(state)
  defp apply_frame(:normalize_execution, state, value) when value in [:ok, :blocked], do: ret(state, :ok)

  defp apply_frame({:work_item_writer, work_item, attempt}, state, {:ok, writer}) do
    review_and_gate(state, work_item, writer, attempt)
  end

  defp apply_frame({:after_review, work_item, writer, attempt}, state, :ok) do
    gate_and_complete(state, work_item, writer, attempt)
  end

  defp apply_frame({:after_gate, work_item, writer, attempt}, state, gated) do
    after_gate(state, work_item, writer, attempt, gated)
  end

  defp apply_frame({:after_review_assignment, review_id}, state, {:ok, review}) do
    adjudicate_review(state, review_id, review)
  end

  defp apply_frame({:after_verdict, review_id, review, artifact}, state, verdict) do
    review_adjudicated(state, review_id, review, artifact, verdict)
  end

  defp apply_frame({:assignment_dispatch, assignment_id, command}, state, {:ok, dispatch_data}) do
    # A recorded `queued` send with no observation started is a send the journal never saw
    # converge: the crash fell inside the daemon's drain window, and the only honest next
    # step is to ask the daemon again -- never to observe for work that may not have been
    # requested, and never to paste again.
    if queued_send?(dispatch_data) and
         is_nil(data_for_assignment(state.events, "assignment_observation_started", assignment_id)) do
      case recorded_deadline_unix(state, assignment_id) do
        nil ->
          clock(state, "recorded_deadline_fallback", {:queued_send_deadline, assignment_id, command, dispatch_data})

        deadline_unix ->
          converge_queued_send(state, recorded_queued_send(state, assignment_id, command, dispatch_data, deadline_unix))
      end
    else
      continue_observation_started(state, assignment_id, command, dispatch_data)
    end
  end

  defp apply_frame({:assignment_observation, assignment_id, command}, state, :ok) do
    continue_artifact_observed(state, assignment_id, command)
  end

  defp apply_frame({:assignment_artifact, assignment_id}, state, {:ok, artifact}) do
    continue_assignment_completed(state, assignment_id, artifact)
  end

  defp apply_frame({:after_continue_assignment, work_item, assignment}, state, {:ok, assignment_result}) do
    continue_after_assignment(state, work_item, assignment, assignment_result)
  end

  defp apply_frame({:after_review_adjudicated, work_item, writer}, state, :ok) do
    gate_and_complete(state, work_item, writer, 1)
  end

  defp apply_frame(_frame, state, :blocked), do: ret(state, :blocked)
  defp apply_frame(_frame, state, {:error, _reason} = error), do: ret(state, error)

  # Back to the adapter-facing shapes the pipeline matches on: a bijection with the host's
  # classification, so no lifecycle decision is made on either side of the seam.
  defp unwrap(%Observation.Clock{now: %{unix: unix}}), do: unix
  defp unwrap(%Observation.Dispatched{result: result}), do: {:ok, result}
  defp unwrap(%Observation.DispatchFailed{reason: reason}), do: {:error, reason}

  defp unwrap(%Observation.ArtifactSnapshot{baseline: baseline}), do: {:ok, baseline}
  defp unwrap(%Observation.ArtifactSnapshotFailed{reason: reason}), do: {:error, reason}

  defp unwrap(%Observation.SendReconciled{outcome: outcome, delivery_attempt: attempt}),
    do: {:ok, %{outcome: outcome, delivery_attempt: attempt}}

  defp unwrap(%Observation.SendReconcileFailed{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.Deadline{deadline_unix: deadline_unix}), do: deadline_unix
  defp unwrap(%Observation.ArtifactObserved{artifact: artifact}), do: {:ok, artifact}
  defp unwrap(%Observation.Blocked{reason: reason}), do: {:blocked, reason}
  defp unwrap(%Observation.Pending{details: details}), do: {:pending, details}
  defp unwrap(%Observation.ObserveFailed{reason: reason}), do: {:error, reason}

  defp unwrap(%Observation.TimedOut{deadline_unix: deadline_unix}),
    do: {:error, %{"reason" => "observation_timeout", "deadline_unix" => deadline_unix}}

  defp unwrap(%Observation.ReviewRead{contents: contents}), do: {:ok, contents}
  defp unwrap(%Observation.ReviewUnreadable{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.GateFinished{result: result}), do: {:ok, result}
  defp unwrap(%Observation.GateFailed{result: result}), do: {:failed, result}
  defp unwrap(%Observation.GateError{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.GatePrepared{started: started}), do: {:ok, started}
  defp unwrap(%Observation.GatePrepareFailed{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.GateReleased{}), do: :ok
  defp unwrap(%Observation.GateReleaseFailed{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.GateUnsettled{result: result}), do: {:unsettled, result}
  defp unwrap(%Observation.GateReconciled{verdict: :no_claim}), do: :no_claim
  defp unwrap(%Observation.GateReconciled{verdict: :orphan_claim, facts: facts}), do: {:orphan_claim, facts}
  defp unwrap(%Observation.GateReconciled{verdict: verdict, facts: facts}), do: {verdict, facts}
  defp unwrap(%Observation.GateReconcileFailed{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.PromptRetained{object: object}), do: {:ok, object}
  defp unwrap(%Observation.PromptRetentionFailed{reason: reason}), do: {:error, reason}
  defp unwrap(%Observation.PromptFetched{bytes: bytes}), do: {:ok, bytes}
  defp unwrap(%Observation.PromptFetchFailed{reason: reason}), do: {:error, reason}

  # A wall-clock read is an effect like any other: the host serves it only after the events
  # emitted so far are stamped, so the value equals what a sequential supervisor read.
  defp clock(state, purpose, frame) do
    index = state.clock_reads + 1
    perform(%{state | clock_reads: index}, %Effect.Clock{read_index: index, purpose: purpose}, frame)
  end

  defp execute(state) do
    state
    |> emit("run_created", run_created_data(state.run))
    |> emit("run_spec_loaded", run_spec_loaded_data(state.run))
    |> emit("plan_recorded", plan_recorded_data(state.run, state.plan))
    |> emit("run_started", run_started_data(state.run))
    |> run_work_items()
  end

  defp run_work_items(state) do
    work_items_loop(state, state.plan |> work_items_in_order() |> Enum.reject(&completed_work_item?(state, &1)))
  end

  # A blocked work item halts the loop without the completion event, exactly as the
  # `reduce_while` did; only an exhausted list completes the run.
  defp work_items_loop(state, []) do
    ret(emit(state, "run_completed", %{"completed_work_item_ids" => state.completed_work_item_ids}), :ok)
  end

  defp work_items_loop(state, [work_item | rest]) do
    state |> push({:work_items, rest}) |> run_work_item(work_item)
  end

  defp resume_execution(spec, plan, fold_state, prior_events, opts) do
    state = state_from_fold(spec, plan, fold_state, prior_events, opts)

    cond do
      fold_state.terminal? ->
        halt(state, :ok)

      MapSet.size(fold_state.open_attention_ids) > 0 ->
        {:error,
         %{
           "reason" => "attention_required",
           "open_attention_ids" => sorted_set(fold_state.open_attention_ids)
         }}

      true ->
        with :ok <- lease_ownership_coherent(prior_events, fold_state) do
          state
          |> emit("run_resumed", run_resumed_data(state.run, fold_state, opts))
          |> repair_stale_leases(fold_state)
          |> push(:normalize_execution)
          |> continue_resumed(fold_state)
        end
    end
  end

  defp state_from_fold(spec, plan, fold_state, events, opts) do
    opts =
      opts
      |> Keyword.put_new(:resume, true)
      |> Keyword.put_new(:run_id, fold_state.run_id)

    %__MODULE__{
      completed_work_item_ids: sorted_set(fold_state.completed_work_item_ids),
      events: events,
      next_assignment: next_counter(Map.keys(fold_state.assignments), "as"),
      next_attention: next_counter(event_data_values(events, "attention_id"), "att"),
      next_gate: next_counter(Map.keys(fold_state.gate_runs), "gr"),
      next_pane_release: next_counter(event_data_values(events, "release_request_id"), "plrr"),
      next_review: next_counter(Map.keys(fold_state.reviews), "rev"),
      next_workspace_release: next_counter(event_data_values(events, "release_request_id"), "wslr"),
      plan: plan,
      run: run_metadata(spec, plan, opts),
      spec: spec
    }
  end

  defp cancel_execution(fold_state, prior_events, opts) do
    state = cancel_state_from_fold(fold_state, prior_events, opts)

    if fold_state.terminal? do
      halt(state, :ok)
    else
      state
      |> emit(
        "run_cancel_requested",
        state.run
        |> run_cancel_data(fold_state, opts)
        |> put_tail_repair(opts)
        |> put_stamp(Keyword.get(opts, :requested_by))
      )
      |> release_active_leases(fold_state)
      |> emit("run_cancelled", run_cancel_data(state.run, fold_state, opts))
      |> halt(:ok)
    end
  end

  defp cancel_state_from_fold(fold_state, events, opts) do
    %__MODULE__{
      events: events,
      next_pane_release: next_counter(event_data_values(events, "release_request_id"), "plrr"),
      next_workspace_release: next_counter(event_data_values(events, "release_request_id"), "wslr"),
      run: %{
        run_id: fold_state.run_id,
        run_dir: Keyword.get(opts, :run_dir, "run"),
        run_lock_path: Keyword.get(opts, :run_lock_path),
        supervisor_instance: Keyword.get(opts, :supervisor_instance, "sup_cancel"),
        default_assignment_timeout_s: Keyword.get(opts, :default_assignment_timeout_s, @default_assignment_timeout_s)
      }
    }
  end

  defp continue_resumed(state, fold_state) do
    cond do
      review_id = unresolved_escalated_review(state.events) ->
        state
        |> ensure_escalated_disposition(review_id)
        |> ensure_review_attention(review_id, "review_verdict_unparseable")
        |> ret(:blocked)

      recovery = unfinished_valid_review(state.events) ->
        resume_valid_review(state, recovery)

      recovery = adjudicated_review_awaiting_gate(state.events) ->
        resume_adjudicated_review(state, recovery)

      open_assignment = first_open_assignment(fold_state) ->
        resume_open_assignment(state, open_assignment)

      pending_gate = first_unfinished_gate(fold_state) ->
        resume_gate(state, pending_gate)

      true ->
        run_work_items(state)
    end
  end

  # A durable clean/findings verdict whose disposition never landed recovers in
  # place: append the disposition, then gate the original writer artifact.
  defp unfinished_valid_review(events) do
    events
    |> Enum.filter(&(&1["type"] == "review_received" and &1["data"]["verdict"] in ["clean", "findings"]))
    |> Enum.find_value(fn event ->
      review_id = event["data"]["review_id"]

      if data_for_review(events, "review_disposition_recorded", review_id) do
        nil
      else
        {review_id, event["data"]["verdict"]}
      end
    end)
  end

  # A fully adjudicated review whose work item never reached its gate (crash
  # after disposition, before gate_requested) proceeds straight to the gate.
  defp adjudicated_review_awaiting_gate(events) do
    completed_items =
      events |> Enum.filter(&(&1["type"] == "work_item_completed")) |> MapSet.new(& &1["data"]["work_item_id"])

    gated_assignments =
      events |> Enum.filter(&(&1["type"] == "gate_requested")) |> MapSet.new(& &1["data"]["assignment_id"])

    events
    |> Enum.filter(
      &(&1["type"] == "review_disposition_recorded" and
          &1["data"]["disposition"] in ["accepted_clean", "changes_requested"])
    )
    |> Enum.find_value(&awaiting_gate_review_id(&1, events, completed_items, gated_assignments))
  end

  # Gate presence is keyed to the SUBJECT ASSIGNMENT, not the work item — an
  # earlier attempt gate must not mask the current attempt.
  defp awaiting_gate_review_id(event, events, completed_items, gated_assignments) do
    review_id = event["data"]["review_id"]

    case data_for_review(events, "review_requested", review_id) do
      %{"work_item_id" => work_item_id, "subject_assignment_id" => subject_assignment_id} ->
        if !(MapSet.member?(completed_items, work_item_id) or
               MapSet.member?(gated_assignments, subject_assignment_id)) do
          review_id
        end

      _missing ->
        nil
    end
  end

  defp resume_valid_review(state, {review_id, verdict}) do
    disposition = if verdict == "clean", do: "accepted_clean", else: "changes_requested"

    state
    |> record_disposition(review_id, disposition)
    |> gate_recovered_review(review_id)
  end

  defp resume_adjudicated_review(state, review_id), do: gate_recovered_review(state, review_id)

  defp gate_recovered_review(state, review_id) do
    with {:ok, review_request} <- review_request(state.events, review_id),
         {:ok, writer} <- writer_result_for_review(state.events, review_request),
         {:ok, work_item} <- fetch_work_item(state.plan, review_request["work_item_id"]) do
      attempt = assignment_attempt(state.events, writer.assignment_id) || 1

      state
      |> push(:run_work_items)
      |> gate_and_complete(work_item, writer, attempt)
    end
  end

  defp ensure_escalated_disposition(fsm, review_id) do
    if data_for_review(fsm.events, "review_disposition_recorded", review_id) do
      fsm
    else
      emit(fsm, "review_disposition_recorded", %{"review_id" => review_id, "disposition" => "escalated"})
    end
  end

  # A review whose recorded verdict/disposition escalated but whose attention
  # event never durably landed (crash window) must re-block on resume.
  defp unresolved_escalated_review(events) do
    attended =
      events
      |> Enum.filter(&(&1["type"] == "human_attention_required"))
      |> MapSet.new(& &1["data"]["blocking_entity"])

    events
    |> Enum.filter(&(&1["type"] in ["review_received", "review_disposition_recorded"]))
    |> Enum.find_value(fn event ->
      review_id = event["data"]["review_id"]

      escalated? =
        event["data"]["verdict"] not in [nil, "clean", "findings"] or event["data"]["disposition"] == "escalated"

      if escalated? and not MapSet.member?(attended, review_id), do: review_id
    end)
  end

  defp resume_open_assignment(state, {assignment_id, assignment}) do
    with {:ok, work_item} <- fetch_work_item(state.plan, assignment.work_item_id),
         {:ok, agent} <- fetch_agent(state.spec, assignment.role),
         recovery_item = recovery_work_item(state.events, assignment_id, work_item),
         {:ok, state, pane_ref} <- ensure_lease_prerequisites(state, assignment_id, assignment, recovery_item, agent) do
      assignment = Map.put(assignment, :pane_ref, pane_ref)

      state
      |> push(:run_work_items)
      |> push({:after_continue_assignment, work_item, assignment})
      |> continue_assignment(work_item, agent, assignment_id, assignment)
    end
  end

  # ---- unit L: lease prerequisites of an open assignment, DERIVED from the durable prefix ----
  #
  # Before any projection or dispatch of a resumed assignment, only the genuinely missing lawful prerequisites are
  # appended: a pending pane request is completed by pane_lease_acquired under its own journaled id and pane_ref (a
  # request+acquire pair only when none exists); the role-required workspace lease likewise under its journaled id
  # and mode. Ownership that the prefix cannot determine (no uniquely proven pair, more than one distinct pending
  # request) is REFUSED before any effect with the closed rejection below - no candidate is picked, no acquisition
  # invented. Identity is read from the DURABLE prefix only: nothing this step has emitted proves ownership.
  defp ensure_lease_prerequisites(state, assignment_id, assignment, work_item, agent) do
    with {:ok, state, pane_ref} <- ensure_pane_lease(state, assignment_id, assignment, agent),
         {:ok, state} <- ensure_workspace_lease(state, assignment_id, work_item) do
      {:ok, state, pane_ref}
    end
  end

  # LG-M1: the prerequisites are those of the assignment's ACTUAL recovery context, not the original task's kind: a
  # reviewer's request row names its review, and its work item is the review item (kind review, no workspace policy)
  defp recovery_work_item(events, assignment_id, work_item) do
    case data_for_assignment(events, "assignment_requested", assignment_id) do
      %{"review_id" => review_id} when is_binary(review_id) -> review_work_item(work_item, review_id)
      _other -> work_item
    end
  end

  defp ambiguous_ownership(assignment_id),
    do: {:error, %{"reason" => "lease_ownership_ambiguous", "assignment_id" => assignment_id}}

  # the durable prefix: everything this step has emitted so far is tentative and never proves ownership
  defp durable_events(state), do: Enum.drop(state.events, -length(state.emitted))

  defp ensure_pane_lease(state, assignment_id, assignment, agent) do
    durable = durable_events(state)
    requests = journaled_pane_requests(durable, assignment_id)
    held = held_pane_pair(durable, assignment_id)

    cond do
      match?({_id, _pane_ref}, held) ->
        {:ok, state, elem(held, 1)}

      # contradictory ownership evidence, or the fold says held but no coherent pair proves it: never guess
      held == :conflict or assignment[:pane_lease?] == true ->
        ambiguous_ownership(assignment_id)

      requests == [] ->
        pane_ref = assignment[:pane_ref] || pane_ref(agent)

        state =
          state
          |> emit("pane_lease_requested", pane_lease_requested_data(state.run, assignment_id, pane_ref))
          |> emit("pane_lease_acquired", pane_lease_acquired_data(state.run, assignment_id, pane_ref))

        {:ok, state, pane_ref}

      match?([_], requests) ->
        [{lease_request_id, pane_ref}] = requests
        acquired = pane_lease_acquired_data(state.run, assignment_id, pane_ref, lease_request_id)
        {:ok, emit(state, "pane_lease_acquired", acquired), pane_ref}

      true ->
        ambiguous_ownership(assignment_id)
    end
  end

  # the distinct journaled pane requests of one assignment, in journal order: {lease_request_id, pane_ref}
  defp journaled_pane_requests(events, assignment_id) do
    events
    |> Enum.filter(&(&1["type"] == "pane_lease_requested" and &1["data"]["assignment_id"] == assignment_id))
    |> Enum.map(&{&1["data"]["lease_request_id"], &1["data"]["pane_ref"]})
    |> Enum.uniq()
  end

  # The uniquely PROVEN held pair of an assignment, resolved CHRONOLOGICALLY over the durable prefix: an acquisition
  # naming one of the assignment's journaled requests with that request's pane becomes the held pair; a release of
  # that pane clears it; an acquisition naming one of its requests with a DIFFERENT pane is contradictory ownership
  # evidence and poisons the assignment (:conflict) - later evidence never resurrects an older pair (LG-M4). Repair
  # and continuation reuse exactly a coherent pair; they never combine one request's id with another's pane.
  defp held_pane_pair(events, assignment_id) do
    requests = Map.new(journaled_pane_requests(events, assignment_id))

    Enum.reduce(events, nil, fn
      _event, :conflict ->
        :conflict

      %{"type" => "pane_lease_acquired", "data" => %{"lease_request_id" => id, "pane_ref" => pane_ref}}, held ->
        case Map.fetch(requests, id) do
          {:ok, ^pane_ref} -> {id, pane_ref}
          {:ok, _other_pane} -> :conflict
          :error -> held
        end

      %{"type" => "pane_lease_released", "data" => %{"pane_ref" => pane_ref}}, {_id, pane_ref} ->
        nil

      _event, held ->
        held
    end)
  end

  # LG-M2/M3/M4: BEFORE any repair or emit, every open assignment's ownership must be coherent with the accepted
  # prefix: a pane lease the fold says is held needs one proven pair that AGREES with the folded pane, no
  # contradictory acquisition, and at most one held workspace lease; otherwise the closed ambiguity rejection,
  # nothing appended
  defp lease_ownership_coherent(prior_views, fold_state) do
    fold_state.open_assignment_ids
    |> Enum.sort()
    |> Enum.find(fn assignment_id ->
      assignment = fold_state.assignments[assignment_id] || %{}
      pair = held_pane_pair(prior_views, assignment_id)

      pane_incoherent? =
        pair == :conflict or
          ((assignment[:pane_lease?] == true and
              not match?({_id, _pane}, pair)) or (is_tuple(pair) and elem(pair, 1) != assignment[:pane_ref]))

      pane_incoherent? or length(held_workspace_ids(prior_views, assignment_id)) > 1
    end)
    |> case do
      nil -> :ok
      assignment_id -> ambiguous_ownership(assignment_id)
    end
  end

  defp ensure_workspace_lease(state, assignment_id, %{"kind" => kind} = work_item)
       when kind in ["implement", "integration"] do
    durable = durable_events(state)
    requests = journaled_workspace_requests(durable, assignment_id)

    cond do
      match?([_], held_workspace_ids(durable, assignment_id)) ->
        {:ok, state}

      held_workspace_ids(durable, assignment_id) != [] ->
        ambiguous_ownership(assignment_id)

      requests == [] ->
        {:ok, maybe_acquire_workspace(state, assignment_id, work_item)}

      match?([_], requests) ->
        [{workspace_lease_id, mode}] = requests
        {:ok, emit(state, "workspace_lease_acquired", %{"workspace_lease_id" => workspace_lease_id, "mode" => mode})}

      true ->
        ambiguous_ownership(assignment_id)
    end
  end

  defp ensure_workspace_lease(state, _assignment_id, _work_item), do: {:ok, state}

  defp journaled_workspace_requests(events, assignment_id) do
    events
    |> Enum.filter(&(&1["type"] == "workspace_lease_requested" and &1["data"]["assignment_id"] == assignment_id))
    |> Enum.map(&{&1["data"]["workspace_lease_id"], &1["data"]["mode"]})
    |> Enum.uniq()
  end

  # the assignment's HELD workspace lease ids, in acquisition order: every durable acquisition under one of its
  # journaled requests not undone by a release of that lease (more than one is ambiguous ownership, LG-M3)
  defp held_workspace_ids(events, assignment_id) do
    ids = MapSet.new(journaled_workspace_requests(events, assignment_id), &elem(&1, 0))

    events
    |> Enum.reduce([], fn
      %{"type" => "workspace_lease_acquired", "data" => %{"workspace_lease_id" => id}}, held ->
        if MapSet.member?(ids, id), do: [id | List.delete(held, id)], else: held

      %{"type" => "workspace_lease_released", "data" => %{"workspace_lease_id" => id}}, held ->
        List.delete(held, id)

      _event, held ->
        held
    end)
    |> Enum.reverse()
  end

  # the workspace lease a completion releases: the one HELD, else the single journaled one, else the generator's
  defp journaled_workspace_lease_id(events, assignment_id) do
    case {held_workspace_ids(events, assignment_id), journaled_workspace_requests(events, assignment_id)} do
      {[id], _requests} -> id
      {[], [{workspace_lease_id, _mode}]} -> workspace_lease_id
      {_held, _requests} -> "wsl_" <> assignment_id
    end
  end

  defp continue_assignment(state, work_item, agent, assignment_id, assignment) do
    pane_ref = assignment[:pane_ref] || pane_ref(agent)
    existing_prompt = data_for_assignment(state.events, "assignment_prompt_projected", assignment_id)
    expected_artifact = expected_artifact_from_prompt(existing_prompt) || expected_artifact(work_item, agent)
    artifact_id = artifact_id_for_assignment(state.events, assignment_id)

    resumption = %{
      assignment_id: assignment_id,
      pane_ref: pane_ref,
      expected_artifact: expected_artifact,
      artifact_id: artifact_id,
      work_item: work_item,
      agent: agent,
      prompt: existing_prompt,
      bytes: nil
    }

    # The branch is taken before anything is rendered. A render is licensed in exactly two
    # places -- no projection at all, and a validated legacy object that is missing -- and
    # each of those renders where it is licensed and nowhere earlier, so a retained send
    # reaches its fetch, and an already-sent one reaches its observation, holding no bytes.
    cond do
      # The journal never learned about any blob: replay renders again, and because the
      # object is named by its bytes a second put lands on the first one if it exists.
      is_nil(existing_prompt) ->
        case render(state, assignment_id, work_item, expected_artifact, agent) do
          {:ok, bytes, prompt} ->
            retain_prompt(
              state,
              assignment_id,
              bytes,
              {:resumed_prompt_retained, %{resumption | prompt: prompt, bytes: bytes}}
            )

          other ->
            ret(state, other)
        end

      # The send already happened. Nothing below needs the bytes -- observation reads the
      # artifact, not the prompt -- so nothing is rendered or fetched for a send that will
      # not be made, and a legacy journal resumes past its dispatch without a migration.
      data_for_assignment(state.events, "assignment_dispatch_sent", assignment_id) ->
        continue_dispatch(state, resumption)

      # An event names an object and the send is still owed. The bytes it names are the
      # bytes to send, and they are read back and verified rather than rendered again.
      true ->
        fetch_prompt(state, resumption)
    end
  end

  # MUST-7 on the replay path too: the projection is emitted only once the baseline is in
  # hand, exactly as on the fresh path.
  defp resumed_prompt_retained(state, resumption, {:ok, %PromptObject{} = object}) do
    snapshot_artifact(
      state,
      resumption.assignment_id,
      resumption.expected_artifact,
      {:resumed_snapshotted, resumption, object}
    )
  end

  defp resumed_prompt_retained(state, resumption, {:error, reason}) do
    state
    |> journal_prompt_failure(
      resumption.assignment_id,
      "prompt_retention_failed",
      retain_detail(reason, resumption.bytes)
    )
    |> ret(:blocked)
  end

  defp resumed_snapshotted(state, resumption, object, {:ok, baseline}) do
    prompt = resumption.prompt |> projected_prompt(object) |> Map.put("artifact_baseline", baseline)
    state = emit(state, "assignment_prompt_projected", prompt)
    continue_dispatch(state, %{resumption | prompt: prompt})
  end

  defp resumed_snapshotted(state, resumption, _object, {:error, reason}) do
    artifact_snapshot_failed(state, resumption.assignment_id, resumption.agent, resumption.pane_ref, reason)
  end

  # A projection is a decoded journal fragment, so the object is built through the checked
  # constructor: a projection whose fields do not describe one consistent object cannot be
  # fetched, and saying so is a fetch failure about that projection rather than a crash.
  defp fetch_prompt(state, resumption) do
    case prompt_object(resumption.prompt) do
      {:ok, object} ->
        perform(state, %Effect.FetchPrompt{object: object}, {:resumed_prompt_fetched, resumption, object})

      {:error, rejection} ->
        reason = Diagnostic.describe_rejection(rejection)

        state
        |> journal_prompt_failure(
          resumption.assignment_id,
          "prompt_fetch_failed",
          fetch_detail(reason, resumption.prompt)
        )
        |> ret(:blocked)
    end
  end

  # `version` is the naming scheme the journal used. The digested name is this slice's; the
  # bare `prompts/<id>.org` is what every projection before it wrote, and it is the whole
  # signal that a journal is legacy.
  defp prompt_object(%{"assignment_id" => id, "prompt_path" => path, "prompt_hash" => hash, "prompt_bytes" => size}) do
    version = if path == "prompts/#{id}.org", do: 1, else: 2
    PromptObject.new(%{assignment_id: id, path: path, hash: hash, byte_size: size, version: version})
  end

  defp prompt_object(_projection), do: {:error, {:prompt_object_incomplete, :path}}

  defp resumed_prompt_fetched(state, resumption, _object, {:ok, %SensitiveBytes{} = bytes}) do
    continue_dispatch(state, %{resumption | bytes: bytes})
  end

  # A legacy journal names its object by the bare `prompts/<id>.org`, and nothing before this
  # slice ever wrote that object. Absence is the one case that licenses a re-render, and only
  # under version 1: the render is checked against BOTH facts the journal holds about the
  # object before it is trusted, and a render that reproduces neither is not the journal's
  # prompt. A read fault, a drift or any other refusal licenses nothing, same as version 2.
  defp resumed_prompt_fetched(
         state,
         resumption,
         %PromptObject{version: 1} = object,
         {:error, %{"reason" => "prompt_object_missing"}}
       ) do
    case render(state, resumption.assignment_id, resumption.work_item, resumption.expected_artifact, resumption.agent) do
      {:ok, %SensitiveBytes{} = bytes, _prompt} ->
        if SensitiveBytes.hash(bytes) == object.hash and SensitiveBytes.byte_size(bytes) == object.byte_size do
          # Restoration is a repair of the object the journal already names, published under
          # the legacy scheme, create-only. It emits no projection: the event is already true
          # once the object exists, and a second projection would leave the only journaled
          # reference pointing at nothing on the next cold resume.
          effect = %Effect.RetainPrompt{assignment_id: resumption.assignment_id, bytes: bytes, scheme: 1}
          perform(state, effect, {:resumed_v1_restored, %{resumption | bytes: bytes}, object})
        else
          reason = %{"reason" => "prompt_v1_render_divergent"}

          state
          |> journal_prompt_failure(resumption.assignment_id, "prompt_fetch_failed", fetch_detail(reason, object))
          |> ret(:blocked)
        end

      other ->
        ret(state, other)
    end
  end

  # Missing evidence is missing: an absent, drifted or unreadable object stops the send with
  # the object named by its relative path and its digest, and nothing is pasted.
  defp resumed_prompt_fetched(state, resumption, %PromptObject{} = object, {:error, reason}) do
    state
    |> journal_prompt_failure(resumption.assignment_id, "prompt_fetch_failed", fetch_detail(reason, object))
    |> ret(:blocked)
  end

  # The restored object is the object the journal names, so the bytes just published are the
  # bytes to send; no fetch is owed for an object this process wrote and verified.
  defp resumed_v1_restored(state, resumption, %PromptObject{} = named, {:ok, %PromptObject{} = restored}) do
    if restored.path == named.path and restored.hash == named.hash do
      continue_dispatch(state, resumption)
    else
      reason = %{"reason" => "prompt_v1_render_divergent"}

      state
      |> journal_prompt_failure(resumption.assignment_id, "prompt_fetch_failed", fetch_detail(reason, named))
      |> ret(:blocked)
    end
  end

  defp resumed_v1_restored(state, resumption, %PromptObject{} = named, {:error, reason}) do
    state
    |> journal_prompt_failure(resumption.assignment_id, "prompt_retention_failed", fetch_detail(reason, named))
    |> ret(:blocked)
  end

  defp continue_dispatch(state, resumption) do
    command =
      dispatch_command(
        state,
        resumption.assignment_id,
        resumption.pane_ref,
        resumption.expected_artifact,
        resumption.artifact_id,
        resumption.prompt,
        resumption.bytes
      )

    # A send the journal already recorded keeps the id it was recorded under: that id is the
    # one the daemon was asked with, and for a legacy journal it is the only name the daemon
    # knows the send by. Minting again would ask about a send that never happened.
    command =
      case data_for_assignment(state.events, "assignment_dispatch_sent", resumption.assignment_id) do
        %{"send_message_id" => recorded} when is_binary(recorded) -> Map.put(command, "send_message_id", recorded)
        _none -> command
      end

    state
    |> push({:assignment_dispatch, resumption.assignment_id, command})
    |> ensure_dispatch_sent(resumption.assignment_id, command)
  end

  defp continue_observation_started(state, assignment_id, command, dispatch_data) do
    state
    |> push({:assignment_observation, assignment_id, observation_command(command, dispatch_data)})
    |> ensure_observation_started(assignment_id)
  end

  defp continue_artifact_observed(state, assignment_id, command) do
    state
    |> push({:assignment_artifact, assignment_id})
    |> ensure_artifact_observed(assignment_id, command)
  end

  defp continue_assignment_completed(state, assignment_id, artifact) do
    state =
      if assignment_terminal_event?(state.events, assignment_id) do
        state
      else
        emit(state, "assignment_completed", assignment_completed_data(assignment_id, artifact["artifact_id"]))
      end

    ret(state, {:ok, %{assignment_id: assignment_id, artifact_id: artifact["artifact_id"]}})
  end

  defp continue_after_assignment(state, work_item, %{role: "reviewer"}, review) do
    with {:ok, review_id} <- review_id_for_assignment(state.events, review.assignment_id),
         {:ok, review_request} <- review_request(state.events, review_id),
         {:ok, writer} <- writer_result_for_review(state.events, review_request) do
      state
      |> push({:after_review_adjudicated, work_item, writer})
      |> ensure_review_adjudicated(review_id, review)
    end
  end

  defp continue_after_assignment(state, work_item, _assignment, writer) do
    review_and_gate(state, work_item, writer, assignment_attempt(state.events, writer.assignment_id) || 1)
  end

  defp resume_gate(state, {gate_run_id, gate}) do
    with {:ok, gate_requested} <- gate_requested_data_for(state.events, gate_run_id),
         {:ok, work_item} <- fetch_work_item(state.plan, gate.work_item_id),
         {:ok, writer} <- writer_result_for_gate(gate) do
      resume_gate_status(state, gate.status, gate_run_id, gate_requested, work_item, writer)
    end
  end

  defp resume_gate_status(state, "passed", _gate_run_id, _gate_requested, work_item, writer) do
    state
    |> complete_work_item(work_item, writer, last_passed_gate_id(state.events, work_item["id"]))
    |> run_work_items()
  end

  # requested but never started (no worker was ever prepared): begin the gate fresh, resumed frame
  defp resume_gate_status(state, "requested", gate_run_id, gate_requested, work_item, writer) do
    attempt = assignment_attempt(state.events, writer.assignment_id) || 1
    frame = {:resumed_gate_finished, gate_run_id, work_item, writer, attempt}
    clock(state, "gate_deadline", {:gate_deadline, gate_requested, frame})
  end

  # a journaled start with no terminal: read-only reconcile of the exact journaled start
  defp resume_gate_status(state, "started", gate_run_id, gate_requested, work_item, writer) do
    attempt = assignment_attempt(state.events, writer.assignment_id) || 1
    frame = {:resumed_gate_finished, gate_run_id, work_item, writer, attempt}

    case journaled_start(state.events, gate_run_id) do
      {:v2, started, gate_attempt} ->
        expected = Map.merge(started, %{"run_id" => state.run.run_id, "journaled" => true})

        perform(
          state,
          %Effect.ReconcileGate{gate_run_id: gate_run_id, attempt: gate_attempt, expected: expected},
          {:gate_reconciled, gate_requested, gate_attempt, frame}
        )

      :v1 ->
        gate_attention(state, %{"reason" => "gate_start_unresolved"}, frame)
    end
  end

  # the reconcile verdict: dead before the deadline reruns at attempt+1 (bounded); dead otherwise
  # is the lawful recovery terminal; every other verdict is attention, never a rerun
  # a proven-dead prior attempt: one clock read is the reconcile time, then either rerun the SAME
  # gate_run_id at attempt+1 under the ORIGINAL deadline, or the recovery terminal whose duration
  # is journal-derived (gate_started ts to this reconcile clock read)
  defp gate_reconciled(state, gate_requested, attempt, frame, {:dead, facts}) do
    clock(state, "gate_recovery", {:gate_recovery, gate_requested, facts, attempt, frame})
  end

  defp gate_reconciled(state, _gate_requested, _attempt, frame, {:unknown, facts}),
    do: gate_attention(state, %{"reason" => "gate_recovery_unknown", "detail" => facts}, frame)

  defp gate_reconciled(state, _gate_requested, _attempt, frame, :no_claim),
    do: gate_attention(state, %{"reason" => "gate_start_unresolved"}, frame)

  defp gate_reconciled(state, _gate_requested, _attempt, frame, {:orphan_claim, info}),
    do: gate_attention(state, %{"reason" => "gate_claim_orphaned", "detail" => info}, frame)

  defp gate_reconciled(state, _gate_requested, _attempt, frame, {:error, reason}),
    do: gate_attention(state, %{"reason" => "gate_evidence_unreadable", "detail" => reason}, frame)

  defp gate_recovery(state, gate_requested, facts, attempt, frame, now) do
    gate_run_id = gate_requested["gate_run_id"]
    original = journaled_deadline(state.events, gate_run_id)
    start_unix = journaled_start_ts(state.events, gate_run_id)

    cond do
      # a reconcile read earlier than the journaled start is clock skew across owners: a fact to
      # report BEFORE any retry or terminal decision, never an elapsed duration to invent (an equal
      # instant is a genuine 0)
      now < start_unix ->
        gate_attention(
          state,
          %{
            "reason" => "gate_recovery_clock_skew",
            "detail" => %{"clause" => "clock_skew", "recorded_start_unix" => start_unix, "observed_now_unix" => now}
          },
          frame
        )

      attempt < 2 and now < original ->
        start_gate(state, gate_requested, original, frame)

      # the recovery terminal owns real evidence or does not exist: missing hashes are unknown, never zero
      not recorded_evidence?(facts) ->
        gate_attention(
          state,
          %{"reason" => "gate_evidence_unreadable", "detail" => %{"clause" => "evidence_incomplete"}},
          frame
        )

      true ->
        finish_gate(state, gate_run_id, {:failed, recovery_result(facts, (now - start_unix) * 1000)}, frame)
    end
  end

  @sha256 ~r/\Asha256:[0-9a-f]{64}\z/
  defp recorded_evidence?(%{"evidence" => %{"stdout_hash" => out, "stderr_hash" => err}})
       when is_binary(out) and is_binary(err), do: Regex.match?(@sha256, out) and Regex.match?(@sha256, err)

  defp recorded_evidence?(_facts), do: false

  defp resumed_gate_finished(state, gate_run_id, work_item, writer, _attempt, {:ok, gate_result}) do
    state
    |> emit("gate_passed", Map.put(gate_result, "gate_run_id", gate_run_id))
    |> complete_work_item(work_item, writer, gate_run_id)
    |> run_work_items()
  end

  # The resumed gate wraps its retry in `run_work_items`; the fresh gate path does not.
  defp resumed_gate_finished(state, gate_run_id, work_item, writer, attempt, {:failed, gate_result}) do
    state
    |> emit("gate_failed", Map.put(gate_result, "gate_run_id", gate_run_id))
    |> push(:run_work_items)
    |> retry_or_fail_work_item(work_item, writer, gate_run_id, attempt)
  end

  defp resumed_gate_finished(state, _gate_run_id, _work_item, _writer, _attempt, {:error, reason}) do
    ret(state, {:error, reason})
  end

  defp run_work_item(state, work_item), do: run_work_item_attempt(state, work_item, 1)

  defp run_work_item_attempt(state, work_item, attempt) do
    state
    |> push({:work_item_writer, work_item, attempt})
    |> run_assignment(work_item, writer_agent(state.spec, work_item), attempt)
  end

  defp review_and_gate(state, work_item, writer, attempt) do
    state
    |> push({:after_review, work_item, writer, attempt})
    |> maybe_review(work_item, writer, attempt)
  end

  defp gate_and_complete(state, work_item, writer, attempt) do
    state
    |> push({:after_gate, work_item, writer, attempt})
    |> run_gate(work_item, writer)
  end

  # `run_gate/3` never blocks, so these three clauses are total.
  defp after_gate(state, work_item, writer, _attempt, {:ok, gate_run_id}) do
    ret(complete_work_item(state, work_item, writer, gate_run_id), :ok)
  end

  defp after_gate(state, work_item, writer, attempt, {:failed, gate_run_id}) do
    retry_or_fail_work_item(state, work_item, writer, gate_run_id, attempt)
  end

  defp after_gate(state, _work_item, _writer, _attempt, {:error, _reason} = error), do: ret(state, error)
  # a gate that parked on attention (prepare/release/settlement/recovery could not resolve)
  defp after_gate(state, _work_item, _writer, _attempt, :blocked), do: ret(state, :blocked)

  defp run_assignment(state, work_item, agent, attempt) do
    assignment_id = next_id("as", state.next_assignment)

    context = %{
      agent: agent,
      artifact_id: "art_" <> assignment_id,
      assignment_id: assignment_id,
      attempt: attempt,
      expected_artifact: expected_artifact(work_item, agent),
      pane_ref: pane_ref(agent),
      work_item: work_item
    }

    clock(state, "assignment_deadline", {:assignment_deadline, context})
  end

  defp assignment_requested(state, context, now) do
    deadline_unix = now + (context.work_item["timeout_s"] || state.run.default_assignment_timeout_s)

    state =
      state
      |> emit(
        "assignment_requested",
        assignment_requested_data(
          context.assignment_id,
          context.work_item,
          context.agent,
          deadline_unix,
          context.attempt
        )
      )
      |> emit("pane_lease_requested", pane_lease_requested_data(state.run, context.assignment_id, context.pane_ref))
      |> emit("pane_lease_acquired", pane_lease_acquired_data(state.run, context.assignment_id, context.pane_ref))
      |> maybe_acquire_workspace(context.assignment_id, context.work_item)

    case render(state, context.assignment_id, context.work_item, context.expected_artifact, context.agent) do
      {:ok, bytes, prompt} ->
        # Blob strictly precedes event. The reducer cannot name the object until the put has
        # answered, so the projection is emitted by the frame that receives the object, and
        # the command is derived from the projection it will be journaled against.
        retain_prompt(state, context.assignment_id, bytes, {:prompt_retained, context, deadline_unix, bytes, prompt})

      other ->
        ret(state, other)
    end
  end

  # The one place the rendered bytes exist bare is inside this function, between the render
  # and the wrapper; nothing above it ever receives a binary. A continuation frame, a
  # resumption map and a suspended state therefore print the wrapper's facts and nothing
  # else, which is what a crash report of a suspended `Run.Server` will contain.
  defp render(state, assignment_id, work_item, expected_artifact, agent) do
    case prompt_bundle(state, assignment_id, work_item, expected_artifact, agent) do
      {:ok, rendered_prompt, prompt} -> {:ok, SensitiveBytes.new(rendered_prompt, :prompt), prompt}
      other -> other
    end
  end

  # An effect with no events: `Host.drive/3` serves it against a journal that has not moved,
  # which is the whole of the ordering guarantee. The bytes travel wrapped; the store is the
  # one place below here that reveals them.
  defp retain_prompt(state, assignment_id, %SensitiveBytes{} = bytes, frame) do
    perform(state, %Effect.RetainPrompt{assignment_id: assignment_id, bytes: bytes}, frame)
  end

  # MUST-7: the artifact baseline is taken at the adapter boundary and recorded on the
  # projection BEFORE anything is pasted, so a crash between the paste and the dispatch
  # event cannot destroy the one value observation depends on. The snapshot is an effect
  # with no events: nothing is journaled until the host has answered.
  defp prompt_retained(state, context, deadline_unix, %SensitiveBytes{} = bytes, prompt, {:ok, %PromptObject{} = object}) do
    snapshot_artifact(
      state,
      context.assignment_id,
      context.expected_artifact,
      {:artifact_snapshotted, context, deadline_unix, bytes, prompt, object}
    )
  end

  # No event may name an object the store refused, and nothing may be pasted: a failed put
  # is durable attention with the failure's evidence, and the run stops here.
  defp prompt_retained(state, context, _deadline_unix, %SensitiveBytes{} = bytes, _prompt, {:error, reason}) do
    state
    |> journal_prompt_failure(context.assignment_id, "prompt_retention_failed", retain_detail(reason, bytes))
    |> ret(:blocked)
  end

  defp snapshot_artifact(state, assignment_id, expected_artifact, frame) do
    perform(
      state,
      %Effect.SnapshotArtifact{
        assignment_id: assignment_id,
        command: %{
          "assignment_id" => assignment_id,
          "repo_root" => state.run.repo_root,
          "expected_artifact" => expected_artifact
        }
      },
      frame
    )
  end

  defp artifact_snapshotted(state, context, deadline_unix, %SensitiveBytes{} = bytes, prompt, object, {:ok, baseline}) do
    prompt = prompt |> projected_prompt(object) |> Map.put("artifact_baseline", baseline)
    state = emit(state, "assignment_prompt_projected", prompt)

    command =
      dispatch_command(
        state,
        context.assignment_id,
        context.pane_ref,
        context.expected_artifact,
        context.artifact_id,
        prompt,
        bytes
      )

    perform(
      state,
      dispatch_effect(context.assignment_id, command, deadline_unix),
      {:assignment_dispatched, context, deadline_unix, command}
    )
  end

  # A baseline the host could not take is attention, not a guess: observation without it
  # would compare the file against itself, and nothing has been pasted yet, so the run
  # stops here with the class the adapter named.
  defp artifact_snapshotted(state, context, _deadline_unix, _bytes, _prompt, _object, {:error, reason}) do
    artifact_snapshot_failed(state, context.assignment_id, context.agent, context.pane_ref, reason)
  end

  # Nothing about the pane was measured, so no `agent_wedge_detected` claims a pane state:
  # like a prompt-store failure, this is attention with the failure's class, and the run
  # stops before anything is pasted.
  defp artifact_snapshot_failed(state, assignment_id, _agent, _pane_ref, reason) do
    if invalid_return?(reason) do
      ret(state, {:error, reason})
    else
      detail = %{"error" => snapshot_error_class(reason), "stage" => "snapshot"}

      state
      |> journal_prompt_failure(assignment_id, "artifact_baseline_failed", detail)
      |> ret(:blocked)
    end
  end

  # The host already reduced the adapter's error to a closed class; this is the same closed
  # set, so a reason outside it can never be copied into an event even by a direct caller.
  @snapshot_error_classes ~w(artifact_baseline_unstable artifact_baseline_failed)
  defp snapshot_error_class(%{"reason" => class}) when class in @snapshot_error_classes, do: class
  defp snapshot_error_class(_reason), do: "artifact_snapshot_failed"

  defp assignment_dispatched(state, context, deadline_unix, command, {:ok, dispatch_data}) do
    state = emit(state, "assignment_dispatch_sent", dispatch_data)

    if queued_send?(dispatch_data) do
      converge_queued_send(
        state,
        queued_send(context.assignment_id, context.agent, command, dispatch_data, deadline_unix, {:fresh, context})
      )
    else
      begin_observation(state, context, deadline_unix, command, dispatch_data)
    end
  end

  # GAP-1 / R3 on the fresh path too: a send that the daemon refused to prove, or a preflight
  # that refused the daemon, is exactly the case a human must adjudicate. It becomes durable
  # attention with the concrete class, not an unjournaled {:error, _} that leaves the journal
  # saying the assignment was never dispatched and never saying why.
  defp assignment_dispatched(state, context, _deadline_unix, command, {:error, reason}) do
    cond do
      # An adapter that returned a shape outside its behaviour is a programming fault: there
      # is nothing an operator can repair, so the run stops with the error, as before.
      invalid_return?(reason) ->
        ret(state, {:error, reason})

      deadline_expiry?(reason) ->
        journal_deadline_attention(state, context.assignment_id, reason)

      true ->
        state
        |> journal_blocked_assignment(
          context.assignment_id,
          context.agent,
          command["pane_ref"],
          dispatch_failure_reason(reason)
        )
        |> ret(:blocked)
    end
  end

  defp invalid_return?(%{"reason" => reason}) when is_binary(reason), do: String.ends_with?(reason, "_invalid_return")
  defp invalid_return?(_reason), do: false

  defp begin_observation(state, context, deadline_unix, command, dispatch_data) do
    state
    |> emit("assignment_observation_started", assignment_observation_started_data(context.assignment_id, deadline_unix))
    |> perform(
      %Effect.Observe{
        assignment_id: context.assignment_id,
        command: observation_command(command, dispatch_data),
        deadline_unix: deadline_unix
      },
      {:assignment_observed, context}
    )
  end

  # ---- MUST-5: a queued send is a waypoint, not a delivery ----
  #
  # `queued` means the daemon holds the bytes and has not pasted them: the paste happens
  # later, in a drain the caller never hears about. Observation must not begin until the
  # daemon answers `delivered`, and the question is re-asked, paced, until the assignment
  # deadline. A proven `absent` (the daemon never pasted and no longer holds the bytes) is
  # retried once under the same id, which the daemon admits as a new delivery attempt; an
  # answer that cannot prove the paste did not land (`ambiguous`, `conflict`) is attention,
  # never a second paste. The bound is two facts, and both must allow it: the receipt's own
  # `delivery_attempt` (the daemon has admitted fewer than two physical attempts) and the
  # journal's recorded sends (fewer than two). The journal alone is not enough -- a crash
  # between the daemon admitting a retry and the journal committing it leaves the journal
  # one send behind, and only the receipt knows.
  @queued_poll_s 2
  @max_delivery_attempts 2

  defp queued_send?(%{"send_status" => "queued"}), do: true
  defp queued_send?(_dispatch_data), do: false

  defp queued_send(assignment_id, agent, command, dispatch_data, deadline_unix, then) do
    %{
      assignment_id: assignment_id,
      agent: agent,
      command: command,
      dispatch_data: dispatch_data,
      deadline_unix: deadline_unix,
      then: then
    }
  end

  defp recorded_queued_send(state, assignment_id, command, dispatch_data, deadline_unix) do
    agent = %{"name" => agent_name(state, assignment_id)}
    queued_send(assignment_id, agent, command, dispatch_data, deadline_unix, :resume)
  end

  defp recorded_sends(state, assignment_id) do
    Enum.count(state.events, &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == assignment_id))
  end

  defp retry_allowed?(state, queued, delivery_attempt) do
    delivery_attempt < @max_delivery_attempts and recorded_sends(state, queued.assignment_id) < @max_delivery_attempts
  end

  defp converge_queued_send(state, queued) do
    perform(
      state,
      %Effect.ReconcileSend{
        assignment_id: queued.assignment_id,
        command: queued.command,
        deadline_unix: queued.deadline_unix
      },
      {:queued_send_reconciled, queued}
    )
  end

  defp queued_send_reconciled(state, queued, {:ok, %{outcome: "delivered"}}), do: queued_send_delivered(state, queued)

  defp queued_send_reconciled(state, queued, {:ok, %{outcome: "queued"}}),
    do: clock(state, "queued_send_poll", {:queued_send_clock, queued})

  # A proven absence may be retried once, and only inside the deadline: the clock is read
  # before the retry so an expired assignment never gets a new paste.
  defp queued_send_reconciled(state, queued, {:ok, %{outcome: "absent", delivery_attempt: attempt}}) do
    if retry_allowed?(state, queued, attempt),
      do: clock(state, "queued_send_retry", {:queued_send_retry_clock, queued}),
      else: queued_send_blocked(state, queued, "dispatch_queued_absent", "queued")
  end

  defp queued_send_reconciled(state, queued, {:ok, %{outcome: outcome}}) when outcome in ["ambiguous", "conflict"],
    do: queued_send_blocked(state, queued, "dispatch_queued_" <> outcome, "queued")

  defp queued_send_reconciled(state, queued, {:error, reason}) do
    cond do
      invalid_return?(reason) -> ret(state, {:error, reason})
      deadline_expiry?(reason) -> journal_deadline_attention(state, queued.assignment_id, reason)
      true -> queued_send_blocked(state, queued, "dispatch_reconcile_failed", "unknown")
    end
  end

  defp queued_send_clock(state, %{deadline_unix: deadline_unix} = queued, now) when now >= deadline_unix,
    do: queued_send_blocked(state, queued, "dispatch_queued_deadline_exceeded", "queued")

  defp queued_send_clock(state, queued, now) do
    wake_unix = min(now + @queued_poll_s, queued.deadline_unix)
    perform(state, %Effect.Timer{purpose: "queued_send_poll", deadline_unix: wake_unix}, {:queued_send_wake, queued})
  end

  defp queued_send_retry_clock(state, %{deadline_unix: deadline_unix} = queued, now) when now >= deadline_unix,
    do: queued_send_blocked(state, queued, "dispatch_queued_deadline_exceeded", "queued")

  defp queued_send_retry_clock(state, queued, _now),
    do:
      perform(
        state,
        dispatch_effect(queued.assignment_id, queued.command, queued.deadline_unix),
        {:queued_send_retried, queued}
      )

  defp queued_send_retried(state, queued, {:ok, dispatch_data}) do
    state = emit(state, "assignment_dispatch_sent", dispatch_data)
    queued = %{queued | dispatch_data: dispatch_data}

    if queued_send?(dispatch_data), do: converge_queued_send(state, queued), else: queued_send_delivered(state, queued)
  end

  defp queued_send_retried(state, queued, {:error, reason}) do
    cond do
      invalid_return?(reason) ->
        ret(state, {:error, reason})

      deadline_expiry?(reason) ->
        journal_deadline_attention(state, queued.assignment_id, reason)

      true ->
        state
        |> journal_blocked_assignment(
          queued.assignment_id,
          queued.agent,
          queued.command["pane_ref"],
          dispatch_failure_reason(reason)
        )
        |> ret(:blocked)
    end
  end

  defp queued_send_delivered(state, %{then: {:fresh, context}} = queued),
    do: begin_observation(state, context, queued.deadline_unix, queued.command, queued.dispatch_data)

  defp queued_send_delivered(state, %{then: :resume} = queued),
    do: continue_observation_started(state, queued.assignment_id, queued.command, queued.dispatch_data)

  defp queued_send_blocked(state, queued, reason, pane_state) do
    failure = %{"reason" => reason, "detector" => "dispatch_reconcile", "pane_state" => pane_state}

    state
    |> journal_blocked_assignment(queued.assignment_id, queued.agent, queued.command["pane_ref"], failure)
    |> ret(:blocked)
  end

  defp assignment_observed(state, context, {:ok, artifact}) do
    state
    |> emit("artifact_observed", artifact)
    |> emit("assignment_completed", assignment_completed_data(context.assignment_id, context.artifact_id))
    |> Map.update!(:next_assignment, &(&1 + 1))
    |> ret({:ok, %{assignment_id: context.assignment_id, artifact_id: context.artifact_id}})
  end

  defp assignment_observed(state, context, {:blocked, reason}) do
    state
    |> journal_blocked_assignment(context.assignment_id, context.agent, context.pane_ref, reason)
    |> ret(:blocked)
  end

  defp assignment_observed(state, _context, {:pending, reason}), do: ret(state, {:error, reason})
  defp assignment_observed(state, _context, {:error, reason}), do: ret(state, {:error, reason})

  defp maybe_review(state, work_item, writer, attempt) do
    case reviewer_agent(state.spec) do
      nil ->
        ret(state, :ok)

      reviewer ->
        review_id = next_id("rev", state.next_review)

        state
        |> emit("review_requested", review_requested_data(review_id, work_item, writer, reviewer))
        |> push({:after_review_assignment, review_id})
        |> run_assignment(review_work_item(work_item, review_id), reviewer, attempt)
    end
  end

  defp adjudicate_review(state, review_id, review) do
    artifact = data_for_assignment(state.events, "artifact_observed", review.assignment_id) || %{}

    state
    |> push({:after_verdict, review_id, review, artifact})
    |> parse_review_verdict(review.assignment_id, artifact)
  end

  defp review_adjudicated(state, review_id, review, artifact, verdict) do
    state =
      state
      |> emit("review_received", review_received_data(review_id, review, artifact, verdict))
      |> Map.update!(:next_review, &(&1 + 1))

    case verdict do
      %{verdict: "clean"} ->
        ret(record_disposition(state, review_id, "accepted_clean"), :ok)

      %{verdict: "findings"} ->
        ret(record_disposition(state, review_id, "changes_requested"), :ok)

      %{verdict: "invalid"} = invalid ->
        state
        |> record_disposition(review_id, "escalated")
        |> ensure_review_attention(review_id, Map.get(invalid, :reason))
        |> ret(:blocked)
    end
  end

  defp record_disposition(state, review_id, disposition) do
    emit(state, "review_disposition_recorded", %{"review_id" => review_id, "disposition" => disposition})
  end

  defp ensure_review_attention(fsm, review_id, reason) do
    already =
      Enum.any?(
        fsm.events,
        &(&1["type"] == "human_attention_required" and &1["data"]["blocking_entity"] == review_id)
      )

    if already do
      fsm
    else
      attention_id = next_id("att", fsm.next_attention)

      fsm
      |> emit("human_attention_required", %{
        "attention_id" => attention_id,
        "reason" => reason || "review_verdict_unparseable",
        "blocking_entity" => review_id,
        "summary_path" => "attention/#{attention_id}.org",
        "summary_hash" => @zero_hash,
        "resume_command" => "run --resume RUN_DIR"
      })
      |> Map.update!(:next_attention, &(&1 + 1))
    end
  end

  defp parse_review_verdict(state, assignment_id, artifact) do
    case artifact["path"] do
      path when is_binary(path) ->
        perform(
          state,
          %Effect.ReadReview{assignment_id: assignment_id, path: Path.join(state.run.repo_root, path)},
          {:review_read, artifact}
        )

      _no_path ->
        ret(state, unreadable_verdict())
    end
  end

  # A review artifact we cannot read is a local verdict, never a machine abort -- which is why
  # effect resumption and frame application are separate dispatchers.
  defp review_verdict(state, artifact, {:ok, contents}) do
    parsed_hash = sha256(contents)

    if drifted?(artifact["sha256"], parsed_hash) do
      ret(state, %{verdict: "invalid", finding_count: nil, hash: parsed_hash, reason: "review_artifact_drift"})
    else
      ret(state, contents |> parse_canonical_verdict() |> Map.put(:hash, parsed_hash))
    end
  end

  defp review_verdict(state, _artifact, _unreadable), do: ret(state, unreadable_verdict())

  defp unreadable_verdict do
    %{verdict: "invalid", finding_count: nil, hash: nil, reason: "review_artifact_unreadable"}
  end

  # The zero hash is the unhashed-observation sentinel; only real observed
  # hashes participate in the drift comparison.
  defp drifted?(observed, parsed_hash) do
    is_binary(observed) and observed != @zero_hash and observed != parsed_hash
  end

  defp parse_canonical_verdict(contents) do
    verdicts = Regex.scan(~r/^\s*=?-\s+Verdict\s+::\s+(\S+?)=?\s*$/m, contents, capture: :all_but_first)
    counts = Regex.scan(~r/^\s*=?-\s+Findings\s+::\s+(\S+?)=?\s*$/m, contents, capture: :all_but_first)

    case {verdicts, counts} do
      {[["clean"]], []} -> %{verdict: "clean", finding_count: 0, reason: nil}
      {[["clean"]], [[count]]} -> clean_with_count(count)
      {[["findings"]], [[count]]} -> findings_with_count(count)
      {[["findings"]], []} -> %{verdict: "invalid", finding_count: nil, reason: "review_findings_count_missing"}
      _other -> %{verdict: "invalid", finding_count: nil, reason: "review_verdict_unparseable"}
    end
  end

  defp clean_with_count(count) do
    if positive_count(count) == 0 do
      %{verdict: "clean", finding_count: 0, reason: nil}
    else
      %{verdict: "invalid", finding_count: nil, reason: "review_verdict_conflicting"}
    end
  end

  defp findings_with_count(count) do
    case positive_count(count) do
      n when is_integer(n) and n > 0 -> %{verdict: "findings", finding_count: n, reason: nil}
      _other -> %{verdict: "invalid", finding_count: nil, reason: "review_findings_count_invalid"}
    end
  end

  defp positive_count(value) do
    case Integer.parse(value) do
      {n, ""} when n >= 0 -> n
      _other -> :error
    end
  end

  defp run_gate(state, work_item, writer) do
    gate_id = List.first(work_item["acceptance"])
    command_argv = get_in(state.spec, ["gates", gate_id])
    gate_run_id = next_id("gr", state.next_gate)
    gate_requested = gate_requested_data(gate_run_id, work_item, writer, gate_id, command_argv)

    state
    |> emit("gate_requested", gate_requested)
    |> Map.update!(:next_gate, &(&1 + 1))
    |> clock("gate_deadline", {:gate_deadline, gate_requested, {:gate_finished, gate_run_id}})
  end

  # The orchestrated gate route (docs/contracts/gate-execution-wiring.org): prepare + durable
  # claim, then gate_started v2 emitted from the prepared identity, release against the committed
  # event's Ack, await evidence, then a truthful terminal or attention. `frame` is the terminal
  # continuation, differing between the fresh ({:gate_finished, ...}) and resumed paths.
  defp start_gate(state, gate_requested, deadline_unix, frame) do
    gate_run_id = gate_requested["gate_run_id"]
    attempt = gate_attempt(state, gate_requested)

    perform(
      state,
      %Effect.PrepareGate{
        gate_run_id: gate_run_id,
        attempt: attempt,
        requested: gate_requested,
        deadline_unix: deadline_unix,
        repo_root: state.run.repo_root,
        run_dir: state.run.run_dir
      },
      {:gate_prepared, gate_requested, deadline_unix, attempt, frame}
    )
  end

  defp gate_deadline(state, gate_requested, frame, now) do
    deadline_unix = now + (gate_requested["timeout_s"] || state.run.default_assignment_timeout_s)
    start_gate(state, gate_requested, deadline_unix, frame)
  end

  # the attempt of this gate_run_id so far (its journaled gate_started events), plus one
  defp gate_attempt(state, gate_requested) do
    started =
      Enum.count(state.events, fn e ->
        e["type"] == "gate_started" and e["data"]["gate_run_id"] == gate_requested["gate_run_id"]
      end)

    started + 1
  end

  defp gate_prepared(state, _gate_requested, _deadline_unix, _attempt, frame, {:error, reason}) do
    name = if reason["clause"] == "helper_missing", do: "gate_helper_missing", else: "gate_prepare_failed"
    gate_attention(state, %{"reason" => name, "detail" => reason}, frame)
  end

  defp gate_prepared(state, gate_requested, deadline_unix, attempt, frame, {:ok, started}) do
    gate_run_id = gate_requested["gate_run_id"]
    state = emit(state, "gate_started", started)
    started_seq = List.last(state.events)["seq"]

    perform(
      state,
      %Effect.ReleaseGate{gate_run_id: gate_run_id, attempt: attempt, started_seq: started_seq},
      {:gate_released, gate_requested, deadline_unix, attempt, frame}
    )
  end

  defp gate_released(state, _gate_requested, _deadline_unix, _attempt, frame, {:error, reason}) do
    gate_attention(state, %{"reason" => "gate_release_failed", "detail" => reason}, frame)
  end

  defp gate_released(state, gate_requested, deadline_unix, attempt, frame, :ok) do
    perform(
      state,
      %Effect.AwaitGate{gate_run_id: gate_requested["gate_run_id"], attempt: attempt, deadline_unix: deadline_unix},
      {:gate_awaited, gate_requested["gate_run_id"], frame}
    )
  end

  # await evidence -> the terminal the frame decides, or attention that parks the run
  defp gate_awaited(state, gate_run_id, frame, {:ok, result}), do: finish_gate(state, gate_run_id, {:ok, result}, frame)

  defp gate_awaited(state, gate_run_id, frame, {:failed, result}),
    do: finish_gate(state, gate_run_id, {:failed, result}, frame)

  defp gate_awaited(state, _gate_run_id, frame, {:unsettled, result}) do
    gate_attention(state, %{"reason" => "gate_settlement_unknown", "detail" => settlement_detail(result)}, frame)
  end

  defp gate_awaited(state, _gate_run_id, frame, {:error, reason}) do
    gate_attention(state, %{"reason" => "gate_evidence_unreadable", "detail" => reason}, frame)
  end

  # the frame carries the same gate_run_id it was created with; the terminal path differs only
  # between the fresh and resumed frames
  defp finish_gate(state, gate_run_id, result, {:gate_finished, _}), do: gate_finished(state, gate_run_id, result)

  defp finish_gate(state, gate_run_id, result, {:resumed_gate_finished, _, work_item, writer, attempt}),
    do: resumed_gate_finished(state, gate_run_id, work_item, writer, attempt, result)

  # a gate that could not prepare/release/settle parks the run on attention and returns to the
  # frame's control so nothing is retried while the group may still run
  defp gate_attention(state, reason, frame) do
    state
    |> emit_gate_attention(reason)
    |> gate_frame_return(frame)
  end

  defp gate_frame_return(state, {:gate_finished, _}), do: ret(state, :blocked)
  defp gate_frame_return(state, {:resumed_gate_finished, _, _, _, _}), do: ret(state, :blocked)

  defp emit_gate_attention(state, %{"reason" => reason} = full) do
    attention_id = next_id("att", state.next_attention)

    data =
      maybe_put(
        %{
          "attention_id" => attention_id,
          "reason" => reason,
          "blocking_entity" => "gate",
          "summary_path" => "attention/#{attention_id}.org",
          "summary_hash" => @zero_hash,
          "resume_command" => "run --resume RUN_DIR"
        },
        "detail",
        gate_detail(full["detail"])
      )

    state
    |> emit("human_attention_required", data)
    |> Map.update!(:next_attention, &(&1 + 1))
  end

  defp gate_finished(state, gate_run_id, {:ok, gate_result}) do
    state
    |> emit("gate_passed", Map.put(gate_result, "gate_run_id", gate_run_id))
    |> ret({:ok, gate_run_id})
  end

  defp gate_finished(state, gate_run_id, {:failed, gate_result}) do
    state
    |> emit("gate_failed", Map.put(gate_result, "gate_run_id", gate_run_id))
    |> ret({:failed, gate_run_id})
  end

  defp gate_finished(state, _gate_run_id, {:error, reason}), do: ret(state, {:error, reason})

  # No effect site of its own, so it stays a plain state transformer and the caller decides
  # what happens next.
  defp complete_work_item(state, work_item, writer, gate_run_id) do
    data = %{
      "work_item_id" => work_item["id"],
      "completing_assignment_id" => writer.assignment_id,
      "required_gate_run_ids" => [gate_run_id],
      "accepted_artifact_ids" => [writer.artifact_id],
      "context_revision" => 0
    }

    state
    |> emit("work_item_completed", data)
    |> maybe_release_workspace(writer.assignment_id, work_item)
    |> Map.update!(:completed_work_item_ids, &Enum.uniq(&1 ++ [work_item["id"]]))
  end

  defp retry_or_fail_work_item(state, work_item, writer, _gate_run_id, attempt) do
    max_attempts = max_attempts(state, work_item)

    if attempt < max_attempts do
      state
      |> emit("work_item_retry_scheduled", retry_scheduled_data(work_item, writer, attempt + 1, max_attempts))
      |> maybe_release_workspace(writer.assignment_id, work_item)
      |> run_work_item_attempt(work_item, attempt + 1)
    else
      ret(state, {:error, %{"reason" => "gate_failed", "work_item_id" => work_item["id"]}})
    end
  end

  defp repair_stale_leases(fsm, state) do
    fsm
    |> release_workspace_leases(state, :reacquire_open)
    |> release_pane_leases(state, :reacquire_open)
  end

  defp release_active_leases(fsm, state) do
    fsm
    |> release_workspace_leases(state, :release_only)
    |> release_pane_leases(state, :release_only)
  end

  defp release_workspace_leases(fsm, state, mode) do
    state.active_workspace_leases
    |> Enum.sort_by(fn {workspace_lease_id, _lease} -> workspace_lease_id end)
    |> Enum.reduce(fsm, fn {workspace_lease_id, lease}, fsm ->
      assignment_id = lease.assignment_id
      fsm = finish_workspace_release(fsm, workspace_lease_id, lease_release_reason(mode))

      if mode == :reacquire_open and MapSet.member?(state.open_assignment_ids, assignment_id) do
        emit(fsm, "workspace_lease_acquired", %{"workspace_lease_id" => workspace_lease_id, "mode" => lease.mode})
      else
        fsm
      end
    end)
  end

  defp release_pane_leases(fsm, state, mode) do
    state.assignments
    |> Enum.sort_by(fn {assignment_id, _assignment} -> assignment_id end)
    |> Enum.reduce(fsm, fn {assignment_id, assignment}, fsm ->
      release_pane_lease(fsm, state, mode, assignment_id, assignment)
    end)
  end

  defp release_pane_lease(fsm, _state, _mode, _assignment_id, assignment)
       when not is_map_key(assignment, :pane_lease?) or not is_map_key(assignment, :pane_ref), do: fsm

  defp release_pane_lease(fsm, _state, _mode, _assignment_id, %{pane_lease?: false}), do: fsm

  defp release_pane_lease(fsm, _state, _mode, _assignment_id, %{pane_ref: nil}), do: fsm

  defp release_pane_lease(fsm, state, mode, assignment_id, assignment) do
    fsm
    |> finish_pane_release(assignment.pane_ref, lease_release_reason(mode))
    |> maybe_reacquire_open_pane_lease(state, mode, assignment_id, assignment.pane_ref)
  end

  # a pane release request left pending (journaled, never completed) is completed under its own id
  defp finish_pane_release(fsm, pane_ref, reason) do
    case pending_pane_release(fsm.events, pane_ref) do
      %{"release_request_id" => release_request_id} ->
        emit(fsm, "pane_lease_released", %{"release_request_id" => release_request_id, "pane_ref" => pane_ref})

      nil ->
        release_request_id = next_id("plrr", fsm.next_pane_release)

        fsm
        |> emit("pane_lease_release_requested", %{
          "release_request_id" => release_request_id,
          "pane_ref" => pane_ref,
          "reason" => reason
        })
        |> emit("pane_lease_released", %{"release_request_id" => release_request_id, "pane_ref" => pane_ref})
        |> Map.update!(:next_pane_release, &(&1 + 1))
    end
  end

  defp pending_pane_release(events, pane_ref) do
    released_ids =
      events
      |> Enum.filter(&(&1["type"] == "pane_lease_released"))
      |> MapSet.new(& &1["data"]["release_request_id"])

    events
    |> Enum.reverse()
    |> Enum.find_value(fn
      %{"type" => "pane_lease_release_requested", "data" => %{"pane_ref" => ^pane_ref, "release_request_id" => id} = data} ->
        if MapSet.member?(released_ids, id), do: nil, else: data

      _event ->
        nil
    end)
  end

  defp maybe_reacquire_open_pane_lease(fsm, state, mode, assignment_id, pane_ref) do
    if mode == :reacquire_open and MapSet.member?(state.open_assignment_ids, assignment_id) do
      # the pair was proven coherent by lease_ownership_coherent/2 before this repair began: reuse exactly it
      {lease_request_id, pane_ref} =
        case held_pane_pair(durable_events(fsm), assignment_id) do
          {id, pane} -> {id, pane}
          _unproven -> {nil, pane_ref}
        end

      emit(fsm, "pane_lease_acquired", pane_lease_acquired_data(fsm.run, assignment_id, pane_ref, lease_request_id))
    else
      fsm
    end
  end

  defp lease_release_reason(:reacquire_open), do: "resume_stale_repair"
  defp lease_release_reason(:release_only), do: "operator_cancel"

  defp ensure_dispatch_sent(state, assignment_id, command) do
    case data_for_assignment(state.events, "assignment_dispatch_sent", assignment_id) do
      nil ->
        perform(
          state,
          dispatch_effect(assignment_id, command, recorded_deadline_unix(state, assignment_id)),
          {:dispatch_recorded, assignment_id, command}
        )

      dispatch_data ->
        ret(state, {:ok, dispatch_data})
    end
  end

  defp dispatch_recorded(state, _assignment_id, _command, {:ok, dispatch_data}) do
    ret(emit(state, "assignment_dispatch_sent", dispatch_data), {:ok, dispatch_data})
  end

  # A failed dispatch used to unwind as a bare `{:error, _}`: the run aborted with no terminal
  # event and no attention event, so the journal recorded that the assignment was never
  # dispatched and never said why (GAP-1, ruling R3). A send that failed is exactly the case a
  # human must adjudicate -- the bytes may or may not have reached the pane -- so it becomes
  # durable attention instead of an unjournaled return.
  defp dispatch_recorded(state, assignment_id, command, {:error, reason}) do
    if deadline_expiry?(reason) do
      journal_deadline_attention(state, assignment_id, reason)
    else
      agent = %{"name" => agent_name(state, assignment_id)}

      state
      |> journal_blocked_assignment(assignment_id, agent, command["pane_ref"], dispatch_failure_reason(reason))
      |> ret(:blocked)
    end
  end

  # The pane state is not observed on this path: the send itself failed, so claiming the pane is
  # "blocked" would assert something nothing measured. The adapter names the class (`reason`)
  # and the check that fired (`detector`, M3: a fact about the run, never a region of the
  # implementation); a refusal with neither is a plain dispatch failure. Nothing here is the
  # adapter's own text: a reason that is not a map is described by class and digest.
  defp dispatch_failure_reason(reason) when is_map(reason) do
    reason
    |> Map.put_new("reason", "dispatch_failed")
    |> Map.put_new("detector", "dispatch_failed")
    |> Map.put_new("pane_state", "unknown")
  end

  defp dispatch_failure_reason(reason) do
    reason
    |> Diagnostic.describe()
    |> Map.merge(%{"reason" => "dispatch_failed", "detector" => "dispatch_failed", "pane_state" => "unknown"})
  end

  # The dispatch message id is the journaled idempotency key under its adapter-facing name; the deadline is the
  # already-durable assignment deadline propagated purely from the continuation (never read by the owner).
  defp dispatch_effect(assignment_id, command, deadline_unix) do
    %Effect.Dispatch{
      assignment_id: assignment_id,
      command: command,
      message_id: command["send_message_id"],
      deadline_unix: deadline_unix
    }
  end

  # U2b delivery deadline (corroborated interface): the owner answers a due deliver/reconcile with the EXISTING
  # failure observation carrying one of two stable reasons; the reducer records ONLY human attention for it
  # (no synthetic wedge facts), through the existing builder, and blocks. Every other reason keeps its route.
  @deadline_reasons ~w(dispatch_deadline_exceeded dispatch_reconcile_timeout)

  defp deadline_expiry?(%{"reason" => reason}) when reason in @deadline_reasons, do: true
  defp deadline_expiry?(_reason), do: false

  defp journal_deadline_attention(state, assignment_id, %{"reason" => reason}) do
    attention_id = next_id("att", state.next_attention)

    state
    |> emit("human_attention_required", human_attention_required_data(attention_id, assignment_id, %{"reason" => reason}))
    |> Map.update!(:next_attention, &(&1 + 1))
    |> ret(:blocked)
  end

  defp observation_command(command, %{"artifact_baseline" => %{} = baseline}) do
    Map.put(command, "artifact_baseline", baseline)
  end

  defp observation_command(command, _dispatch_data), do: command

  defp ensure_observation_started(state, assignment_id) do
    if data_for_assignment(state.events, "assignment_observation_started", assignment_id) do
      ret(state, :ok)
    else
      case recorded_deadline_unix(state, assignment_id) do
        nil -> clock(state, "recorded_deadline_fallback", {:observation_started_deadline, assignment_id})
        deadline_unix -> ret(observation_started(state, assignment_id, deadline_unix), :ok)
      end
    end
  end

  defp observation_started(state, assignment_id, deadline_unix) do
    emit(state, "assignment_observation_started", assignment_observation_started_data(assignment_id, deadline_unix))
  end

  defp ensure_artifact_observed(state, assignment_id, command) do
    case data_for_assignment(state.events, "artifact_observed", assignment_id) do
      nil ->
        case recorded_deadline_unix(state, assignment_id) do
          nil -> clock(state, "recorded_deadline_fallback", {:artifact_deadline, assignment_id, command})
          deadline_unix -> observe_artifact(state, assignment_id, command, deadline_unix)
        end

      artifact ->
        ret(state, {:ok, artifact})
    end
  end

  defp observe_artifact(state, assignment_id, command, deadline_unix) do
    perform(
      state,
      %Effect.Observe{assignment_id: assignment_id, command: command, deadline_unix: deadline_unix},
      {:artifact_observed, assignment_id, command}
    )
  end

  defp artifact_observed(state, _assignment_id, _command, {:ok, artifact}) do
    ret(emit(state, "artifact_observed", artifact), {:ok, artifact})
  end

  defp artifact_observed(state, assignment_id, command, {:blocked, reason}) do
    agent = %{"name" => agent_name(state, assignment_id)}

    state
    |> journal_blocked_assignment(assignment_id, agent, command["pane_ref"], reason)
    |> ret(:blocked)
  end

  defp artifact_observed(state, _assignment_id, _command, {:pending, reason}), do: ret(state, {:error, reason})
  defp artifact_observed(state, _assignment_id, _command, {:error, reason}), do: ret(state, {:error, reason})

  defp ensure_review_adjudicated(state, review_id, review) do
    received = data_for_review(state.events, "review_received", review_id)
    disposed = data_for_review(state.events, "review_disposition_recorded", review_id)

    cond do
      received && disposed && escalated_review?(received, disposed) ->
        state |> ensure_review_attention(review_id, "review_verdict_unparseable") |> ret(:blocked)

      received && disposed ->
        ret(state, :ok)

      received ->
        redisposition_from_recorded(state, review_id, received)

      true ->
        adjudicate_review(state, review_id, review)
    end
  end

  defp escalated_review?(%{"verdict" => verdict}, %{"disposition" => disposition}) do
    verdict not in ["clean", "findings"] or disposition == "escalated"
  end

  defp redisposition_from_recorded(state, review_id, %{"verdict" => "clean"}) do
    ret(record_disposition(state, review_id, "accepted_clean"), :ok)
  end

  defp redisposition_from_recorded(state, review_id, %{"verdict" => "findings"}) do
    ret(record_disposition(state, review_id, "changes_requested"), :ok)
  end

  defp redisposition_from_recorded(state, review_id, _received) do
    state
    |> record_disposition(review_id, "escalated")
    |> ensure_review_attention(review_id, "review_verdict_unparseable")
    |> ret(:blocked)
  end

  defp first_open_assignment(state) do
    state.open_assignment_ids
    |> sorted_set()
    |> Enum.find_value(fn assignment_id ->
      case state.assignments[assignment_id] do
        nil -> nil
        assignment -> {assignment_id, assignment}
      end
    end)
  end

  defp first_unfinished_gate(state) do
    state.gate_runs
    |> Enum.filter(fn {_gate_run_id, gate} ->
      gate.status in ["requested", "started", "passed"] and
        not MapSet.member?(state.completed_work_item_ids, gate.work_item_id)
    end)
    |> Enum.sort_by(fn {gate_run_id, _gate} -> gate_run_id end)
    |> List.first()
  end

  defp fetch_work_item(plan, work_item_id) do
    case Enum.find(plan["work_items"], &(&1["id"] == work_item_id)) do
      nil -> {:error, %{"reason" => "work_item_missing", "work_item_id" => work_item_id}}
      work_item -> {:ok, work_item}
    end
  end

  defp fetch_agent(spec, role) do
    case agent_for_role(spec, role) do
      nil -> {:error, %{"reason" => "agent_missing", "role" => role}}
      agent -> {:ok, agent}
    end
  end

  defp data_for_assignment(events, type, assignment_id) do
    data_for(events, type, "assignment_id", assignment_id)
  end

  defp data_for_review(events, type, review_id) do
    data_for(events, type, "review_id", review_id)
  end

  defp data_for(events, type, field, value) do
    events
    |> Enum.find(fn
      %{"type" => ^type, "data" => %{^field => ^value}} -> true
      _event -> false
    end)
    |> case do
      nil -> nil
      event -> event["data"]
    end
  end

  defp event_data_values(events, field) do
    events
    |> Enum.flat_map(fn
      %{"data" => data} -> List.wrap(data[field])
      _event -> []
    end)
    |> Enum.filter(&is_binary/1)
  end

  defp expected_artifact_from_prompt(%{"expected_artifact" => expected_artifact}), do: expected_artifact
  defp expected_artifact_from_prompt(_prompt), do: nil

  defp artifact_id_for_assignment(events, assignment_id) do
    case data_for_assignment(events, "artifact_observed", assignment_id) do
      %{"artifact_id" => artifact_id} -> artifact_id
      _no_artifact -> "art_" <> assignment_id
    end
  end

  defp assignment_terminal_event?(events, assignment_id) do
    Enum.any?(events, fn
      %{"type" => type, "data" => %{"assignment_id" => ^assignment_id}}
      when type in ["assignment_completed", "assignment_failed"] ->
        true

      _event ->
        false
    end)
  end

  defp assignment_attempt(events, assignment_id) do
    case data_for_assignment(events, "assignment_requested", assignment_id) do
      %{"attempt" => attempt} -> attempt
      _event -> nil
    end
  end

  defp agent_name(fsm, assignment_id) do
    case data_for_assignment(fsm.events, "assignment_requested", assignment_id) do
      %{"agent" => agent} -> agent
      _event -> "unknown_agent"
    end
  end

  defp review_id_for_assignment(events, assignment_id) do
    case data_for_assignment(events, "assignment_requested", assignment_id) do
      %{"review_id" => review_id} -> {:ok, review_id}
      _data -> {:error, %{"reason" => "review_link_missing", "assignment_id" => assignment_id}}
    end
  end

  defp review_request(events, review_id) do
    case data_for_review(events, "review_requested", review_id) do
      nil -> {:error, %{"reason" => "review_request_missing", "review_id" => review_id}}
      request -> {:ok, request}
    end
  end

  defp writer_result_for_review(events, %{"subject_assignment_id" => assignment_id}) do
    case data_for_assignment(events, "artifact_observed", assignment_id) do
      %{"artifact_id" => artifact_id} -> {:ok, %{assignment_id: assignment_id, artifact_id: artifact_id}}
      _event -> {:error, %{"reason" => "subject_artifact_missing", "assignment_id" => assignment_id}}
    end
  end

  defp writer_result_for_gate(%{assignment_id: assignment_id, artifact_ids: artifact_ids}) do
    case sorted_set(artifact_ids) do
      [artifact_id | _rest] -> {:ok, %{assignment_id: assignment_id, artifact_id: artifact_id}}
      [] -> {:error, %{"reason" => "gate_artifact_missing", "assignment_id" => assignment_id}}
    end
  end

  defp gate_requested_data_for(events, gate_run_id) do
    case data_for(events, "gate_requested", "gate_run_id", gate_run_id) do
      nil -> {:error, %{"reason" => "gate_request_missing", "gate_run_id" => gate_run_id}}
      data -> {:ok, data}
    end
  end

  defp last_passed_gate_id(events, work_item_id) do
    events
    |> Enum.filter(fn
      %{"type" => "gate_requested", "data" => %{"work_item_id" => ^work_item_id}} -> true
      _event -> false
    end)
    |> List.last()
    |> case do
      %{"data" => %{"gate_run_id" => gate_run_id}} -> gate_run_id
      _event -> nil
    end
  end

  defp journal_blocked_assignment(fsm, assignment_id, agent, pane_ref, reason) do
    attention_id = next_id("att", fsm.next_attention)

    fsm
    |> emit("agent_wedge_detected", agent_wedge_detected_data(assignment_id, agent, pane_ref, reason))
    |> emit("human_attention_required", human_attention_required_data(attention_id, assignment_id, reason))
    |> Map.update!(:next_attention, &(&1 + 1))
  end

  defp maybe_acquire_workspace(fsm, assignment_id, %{"kind" => kind, "allowed_paths" => allowed_paths})
       when kind in ["implement", "integration"] do
    fsm
    |> emit("workspace_lease_requested", workspace_lease_requested_data(assignment_id, allowed_paths))
    |> emit("workspace_lease_acquired", workspace_lease_acquired_data(assignment_id))
  end

  defp maybe_acquire_workspace(fsm, _assignment_id, _work_item), do: fsm

  defp maybe_release_workspace(fsm, assignment_id, %{"kind" => kind}) when kind in ["implement", "integration"] do
    release_workspace_lease(fsm, assignment_id)
  end

  defp maybe_release_workspace(fsm, _assignment_id, _work_item), do: fsm

  defp release_workspace_lease(fsm, assignment_id) do
    finish_workspace_release(fsm, journaled_workspace_lease_id(fsm.events, assignment_id), "work_item_completed")
  end

  defp finish_workspace_release(fsm, workspace_lease_id, reason) do
    case pending_workspace_release(fsm.events, workspace_lease_id) do
      %{"release_request_id" => release_request_id} ->
        emit(fsm, "workspace_lease_released", %{
          "release_request_id" => release_request_id,
          "workspace_lease_id" => workspace_lease_id
        })

      nil ->
        release_request_id = next_id("wslr", fsm.next_workspace_release)

        fsm
        |> emit("workspace_lease_release_requested", %{
          "release_request_id" => release_request_id,
          "workspace_lease_id" => workspace_lease_id,
          "reason" => reason
        })
        |> emit("workspace_lease_released", %{
          "release_request_id" => release_request_id,
          "workspace_lease_id" => workspace_lease_id
        })
        |> Map.update!(:next_workspace_release, &(&1 + 1))
    end
  end

  defp pending_workspace_release(events, workspace_lease_id) do
    released_ids =
      events
      |> Enum.filter(&(&1["type"] == "workspace_lease_released"))
      |> MapSet.new(& &1["data"]["release_request_id"])

    events
    |> Enum.reverse()
    |> Enum.find_value(fn
      %{
        "type" => "workspace_lease_release_requested",
        "data" =>
          %{
            "workspace_lease_id" => ^workspace_lease_id,
            "release_request_id" => release_request_id
          } = data
      } ->
        if MapSet.member?(released_ids, release_request_id), do: nil, else: data

      _event ->
        nil
    end)
  end

  defp emit(state, type, data) do
    seq = length(state.events) + 1
    event = base_event(state.run, seq, type, data)
    %{state | emitted: [event | state.emitted], events: state.events ++ [event]}
  end

  defp base_event(run, seq, type, data) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => Event.current_version(type) || 1,
      "seq" => seq,
      "event_id" => next_id("ev", seq),
      "type" => type,
      "ts" => @placeholder_ts,
      "run_id" => run.run_id,
      "actor" => "run_supervisor",
      "data" => data
    }
  end

  defp run_metadata(spec, plan, opts) do
    %{
      agent_roster_hash: Keyword.get_lazy(opts, :agent_roster_hash, fn -> roster_hash(spec) end),
      context_initial_hash: Keyword.get_lazy(opts, :context_initial_hash, fn -> Plan.context_initial_hash(plan) end),
      operator: Keyword.get(opts, :operator, "operator"),
      plan_hash: Keyword.get(opts, :plan_hash, @zero_hash),
      plan_path: Keyword.get(opts, :plan_path, "plan.json"),
      pane_claim_tokens: Keyword.get(opts, :pane_claim_tokens, %{}),
      project: Keyword.get_lazy(opts, :project, fn -> Path.basename(spec["repo_root"] || "run") end),
      repo_root: spec["repo_root"],
      resume: Keyword.get(opts, :resume, false),
      run_dir: Keyword.get(opts, :run_dir, "/tmp/example-run"),
      run_id: Keyword.fetch!(opts, :run_id),
      run_lock_path: Keyword.get(opts, :run_lock_path),
      requested_by: Keyword.get(opts, :requested_by),
      spec_hash: Keyword.get(opts, :spec_hash, @zero_hash),
      spec_path: Keyword.get(opts, :spec_path, "spec.json"),
      supervisor_instance: Keyword.fetch!(opts, :supervisor_instance),
      default_assignment_timeout_s: Keyword.get(opts, :default_assignment_timeout_s, @default_assignment_timeout_s)
    }
  end

  # the command's acceptance stamp (ADR-0001 item 4) lives on the one acceptance event only: run_created for
  # start, run_resumed for resume, run_cancel_requested for cancel; derived events never carry it
  defp run_created_data(run) do
    put_stamp(
      %{
        "project" => run.project,
        "repo_root" => run.repo_root,
        "run_dir" => run.run_dir,
        "operator" => run.operator,
        "spec_path" => run.spec_path,
        "spec_hash" => run.spec_hash
      },
      run.requested_by
    )
  end

  defp put_stamp(data, %{} = stamp), do: Map.put(data, "requested_by", stamp)
  defp put_stamp(data, _absent), do: data

  defp run_spec_loaded_data(run) do
    %{"spec_path" => run.spec_path, "spec_hash" => run.spec_hash, "agent_roster_hash" => run.agent_roster_hash}
  end

  defp plan_recorded_data(run, plan) do
    %{
      "plan_id" => plan["plan_id"],
      "plan_path" => run.plan_path,
      "plan_hash" => run.plan_hash,
      "work_item_ids" => Enum.map(plan["work_items"], & &1["id"]),
      "dag_edges" => dag_edges(plan),
      "context_initial_hash" => run.context_initial_hash,
      "context_revision" => 0
    }
  end

  defp dag_edges(plan) do
    plan["work_items"]
    |> Enum.flat_map(fn work_item ->
      Enum.map(work_item["deps"], fn dependency_id ->
        %{"item" => work_item["id"], "depends_on" => dependency_id}
      end)
    end)
    |> Enum.sort_by(&{&1["item"], &1["depends_on"]})
  end

  defp run_started_data(run) do
    put_present(
      %{"supervisor_instance" => run.supervisor_instance, "resume" => run.resume, "last_seen_seq" => 3},
      "run_lock_path",
      run.run_lock_path
    )
  end

  defp assignment_requested_data(assignment_id, work_item, agent, deadline_unix, attempt) do
    data = %{
      "assignment_id" => assignment_id,
      "work_item_id" => work_item["id"],
      "attempt" => attempt,
      "role" => agent["role"],
      "agent" => agent["name"],
      "deadline_unix" => deadline_unix,
      "idempotency_key" => "idem_" <> assignment_id,
      "context_revision" => 0,
      "context_hash" => @zero_hash
    }

    maybe_put(data, "review_id", work_item["review_id"])
  end

  defp pane_lease_requested_data(run, assignment_id, pane_ref) do
    %{
      "lease_request_id" => "plr_" <> assignment_id,
      "assignment_id" => assignment_id,
      "pane_ref" => pane_ref,
      "owner_instance" => run.supervisor_instance,
      "stale_after_ms" => 180_000
    }
  end

  defp pane_lease_acquired_data(run, assignment_id, pane_ref, lease_request_id \\ nil) do
    maybe_put(
      %{
        "lease_request_id" => lease_request_id || "plr_" <> assignment_id,
        "pane_ref" => pane_ref,
        "owner_instance" => run.supervisor_instance
      },
      "claim_token",
      run.pane_claim_tokens[pane_ref]
    )
  end

  defp workspace_lease_requested_data(assignment_id, allowed_paths) do
    %{
      "workspace_lease_id" => "wsl_" <> assignment_id,
      "assignment_id" => assignment_id,
      "mode" => "shared_repo",
      "allowed_paths" => allowed_paths
    }
  end

  defp workspace_lease_acquired_data(assignment_id),
    do: %{"workspace_lease_id" => "wsl_" <> assignment_id, "mode" => "shared_repo"}

  defp prompt_bundle(fsm, assignment_id, work_item, expected_artifact, agent) do
    with {:ok, context_revision, context_hash, context_document} <- run_context_projection(fsm.events) do
      prompt =
        render_prompt(
          fsm,
          assignment_id,
          work_item,
          expected_artifact,
          agent,
          context_document
        )

      {:ok, prompt, prompt_metadata(assignment_id, expected_artifact, prompt, context_revision, context_hash)}
    end
  end

  defp render_prompt(fsm, assignment_id, work_item, expected_artifact, agent, context_document) do
    [
      "#+title: Assignment #{assignment_id}",
      "",
      "* Run",
      "- goal :: #{fsm.spec["goal"]}",
      "- repo_root :: #{fsm.run.repo_root}",
      "",
      "* Work item",
      "- work_item_id :: #{work_item["id"]}",
      "- title :: #{Map.get(work_item, "title", work_item["id"])}",
      "- kind :: #{work_item["kind"]}",
      "- role :: #{agent["role"]}",
      "- agent :: #{agent["name"]}",
      "- assignment_id :: #{assignment_id}",
      "- expected_artifact :: #{expected_artifact}",
      "",
      "* Allowed paths",
      list_lines(Map.get(work_item, "allowed_paths", [])),
      "",
      "* Acceptance gates",
      gate_lines(fsm.spec, Map.get(work_item, "acceptance", [])),
      failure_context_lines(fsm.events, work_item),
      review_context_lines(fsm.events, work_item),
      review_protocol_lines(work_item),
      "",
      "* Instructions",
      "- Complete the work item in the repo root.",
      "- Write or update only the allowed paths for this assignment.",
      "- Produce the expected artifact at the path above.",
      "",
      "* Current run context",
      String.trim_trailing(context_document)
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  # `prompt_path` is content-addressed: one name for every render an assignment ever produces
  # is a name a retry overwrites while a journaled hash still refers to the old bytes. Naming
  # the object after its contents makes that unrepresentable. The name costs no I/O, which is
  # what lets a pure reducer compute it; making the bytes exist is what the effect adds.
  defp prompt_metadata(assignment_id, expected_artifact, prompt, context_revision, context_hash) do
    "sha256:" <> hex = hash = sha256(prompt)

    %{
      "assignment_id" => assignment_id,
      "prompt_path" => "prompts/#{assignment_id}-#{hex}.org",
      "prompt_hash" => hash,
      "prompt_bytes" => byte_size(prompt),
      "context_revision" => context_revision,
      "context_hash" => context_hash,
      "expected_artifact" => expected_artifact
    }
  end

  defp run_context_projection(events) do
    case fold_events(events) do
      {:ok, state} ->
        context = Context.render(state)
        {:ok, state.context_revision, sha256(context), context}

      {:error, reason} ->
        {:error, Map.put(reason, :reason, "prompt_context_unfoldable")}
    end
  end

  defp list_lines([]), do: "- none"
  defp list_lines(values), do: Enum.map(values, &"- #{&1}")

  defp gate_lines(_spec, []), do: "- none"

  defp gate_lines(spec, gate_ids) do
    Enum.map(gate_ids, fn gate_id ->
      "- #{gate_id} :: #{RunSpec.gate_json(spec, gate_id)}"
    end)
  end

  defp review_context_lines(events, %{"review_id" => review_id}) do
    case data_for_review(events, "review_requested", review_id) do
      %{"subject_assignment_id" => subject_assignment_id, "work_item_id" => work_item_id} ->
        [
          "",
          "* Review subject",
          "- review_id :: #{review_id}",
          "- subject_work_item_id :: #{work_item_id}",
          "- subject_assignment_id :: #{subject_assignment_id}",
          review_subject_artifact_line(events, subject_assignment_id)
        ]

      _review ->
        []
    end
  end

  defp review_context_lines(_events, _work_item), do: []

  defp review_protocol_lines(%{"kind" => "review"}) do
    [
      "",
      "* Review protocol",
      "- Your artifact MUST contain exactly one canonical verdict line, exactly as shown:",
      "#+begin_example",
      "- Verdict :: clean",
      "#+end_example",
      "- Or, when you have findings, both canonical lines with a positive count:",
      "#+begin_example",
      "- Verdict :: findings",
      "- Findings :: 2",
      "#+end_example",
      "- Any other form, duplicate verdict lines, or a missing count escalates to the operator."
    ]
  end

  defp review_protocol_lines(_work_item), do: []

  defp failure_context_lines(_events, %{"kind" => "review"}), do: []

  defp failure_context_lines(events, work_item) do
    work_item_id = work_item["id"]

    gate_ids =
      events
      |> Enum.filter(&(&1["type"] == "gate_requested" and &1["data"]["work_item_id"] == work_item_id))
      |> MapSet.new(& &1["data"]["gate_run_id"])

    events
    |> Enum.filter(&(&1["type"] == "gate_failed" and MapSet.member?(gate_ids, &1["data"]["gate_run_id"])))
    |> List.last()
    |> failure_summary_lines()
  end

  defp failure_summary_lines(%{"data" => %{"failure_summary" => %{} = summary}}) do
    [
      "",
      "* Previous attempt",
      "- The acceptance gate FAILED on the last attempt. Fix the cause below.",
      "- headline :: #{summary["headline"]}",
      Enum.map(Map.get(summary, "failures", []), &"- failure :: #{&1["line"]}"),
      "- suggestion :: #{summary["suggestion"]}"
    ]
  end

  defp failure_summary_lines(_event), do: []

  defp review_subject_artifact_line(events, assignment_id) do
    case data_for_assignment(events, "artifact_observed", assignment_id) do
      %{"path" => path, "artifact_id" => artifact_id} -> "- subject_artifact :: #{artifact_id} at #{path}"
      _artifact -> nil
    end
  end

  # The command is the longest-lived carrier of the prompt -- it is held in reducer state
  # across the whole dispatch step -- so it carries the wrapper. A state that gets printed
  # prints facts about the bytes; encoding the command is an error rather than a disclosure.
  # `bytes` is `nil` on the path where the send already happened: observation reads the
  # artifact, not the prompt, so the command for that path carries no prompt at all.
  defp dispatch_command(fsm, assignment_id, pane_ref, expected_artifact, artifact_id, prompt, bytes) do
    %{
      "assignment_id" => assignment_id,
      "artifact_id" => artifact_id,
      "expected_artifact" => expected_artifact,
      "pane_ref" => pane_ref,
      "repo_root" => fsm.run.repo_root,
      # MUST-2: the question the adapter asks the daemon is bound to the digest the journal
      # keeps, so a reconstructed dispatch is provably about the same bytes.
      "payload_hash" => Map.get(prompt, "prompt_hash"),
      # MUST-1: the receipt store is global, so the send id binds the run as well as the
      # assignment. Two runs that contained the same assignment id would otherwise ask one
      # question and each be answered about the other.
      "send_message_id" => SendId.mint(fsm.run.run_id, assignment_id),
      "stable_for_ms" => 5000,
      "prompt_metadata" => prompt
    }
    # MUST-7: the durable baseline travels with the command -- recorded on a version-2
    # projection, or the explicit unrecorded view of a version-1 one -- so the adapter
    # never takes a second snapshot for a projected assignment.
    |> maybe_put("artifact_baseline", Map.get(prompt, "artifact_baseline"))
    |> maybe_put(
      "prompt",
      bytes
    )
  end

  defp assignment_observation_started_data(assignment_id, deadline_unix),
    do: %{"assignment_id" => assignment_id, "deadline_unix" => deadline_unix}

  defp recorded_deadline_unix(state, assignment_id) do
    case data_for_assignment(state.events, "assignment_requested", assignment_id) do
      %{"deadline_unix" => deadline_unix} when is_integer(deadline_unix) -> deadline_unix
      _other -> nil
    end
  end

  defp assignment_completed_data(assignment_id, artifact_id) do
    %{"assignment_id" => assignment_id, "artifact_id" => artifact_id, "pane_state" => "idle"}
  end

  defp agent_wedge_detected_data(assignment_id, agent, pane_ref, reason) do
    maybe_put(
      %{
        "agent_ref" => agent["name"],
        "pane_ref" => pane_ref,
        "assignment_id" => assignment_id,
        "detector" => wedge_detector(reason),
        "stale_for_ms" => Map.get(reason, "stale_for_ms", 200_000),
        "pane_state" => Map.get(reason, "pane_state", "blocked"),
        "pending_count" => Map.get(reason, "pending_count", 0)
      },
      "reason",
      Map.get(reason, "reason")
    )
  end

  # The put answered with the object; its three facts are the projection's three facts.
  defp projected_prompt(prompt, %PromptObject{} = object) do
    prompt
    |> Map.put("prompt_path", object.path)
    |> Map.put("prompt_hash", object.hash)
    |> Map.put("prompt_bytes", object.byte_size)
  end

  # A store failure is not an agent wedge: nothing about the pane was measured, so no
  # `agent_wedge_detected` claims a pane state. It is attention with the evidence an operator
  # routes on, and `Fold` derives the blocked status from the attention event alone.
  defp journal_prompt_failure(state, assignment_id, reason, detail) do
    attention_id = next_id("att", state.next_attention)

    data =
      attention_id |> human_attention_required_data(assignment_id, %{"reason" => reason}) |> Map.put("detail", detail)

    state
    |> emit("human_attention_required", data)
    |> Map.update!(:next_attention, &(&1 + 1))
  end

  # No `prompt_path`: publication is what failed, so no object has a name yet, and inventing
  # one would point an operator at a file that does not exist. The keys are the contract.
  defp retain_detail(reason, %SensitiveBytes{} = bytes) do
    %{
      "error" => error_name(reason),
      "stage" => "retain",
      "prompt_hash" => SensitiveBytes.hash(bytes),
      "prompt_bytes" => SensitiveBytes.byte_size(bytes)
    }
  end

  # The object had a name, so the detail carries it: the relative path is what sends an
  # operator to look, and it is relative for the same reason the projection's is.
  defp fetch_detail(reason, %PromptObject{} = object) do
    %{
      "error" => error_name(reason),
      "stage" => "fetch",
      "prompt_path" => object.path,
      "prompt_hash" => object.hash,
      "prompt_bytes" => object.byte_size
    }
  end

  defp fetch_detail(reason, %{} = projection) do
    %{
      "error" => error_name(reason),
      "stage" => "fetch",
      "prompt_path" => Map.get(projection, "prompt_path"),
      "prompt_hash" => Map.get(projection, "prompt_hash"),
      "prompt_bytes" => Map.get(projection, "prompt_bytes")
    }
  end

  # The host normalized the store's pair into a map whose `reason` is a name from the closed
  # vocabulary, or an invalid-return description whose `reason` names the seam. A map with
  # neither is one the host described by class and digest because the reason was not in the
  # vocabulary; the name here says that, rather than reflecting whatever was in the map.
  defp error_name(%{"reason" => name}) when is_binary(name), do: name
  defp error_name(_reason), do: "prompt_rejection_unrecognized"

  defp human_attention_required_data(attention_id, assignment_id, reason) do
    maybe_put(
      %{
        "attention_id" => attention_id,
        "reason" => Map.get(reason, "reason", "agent_blocked"),
        "blocking_entity" => assignment_id,
        "summary_path" => "attention/#{attention_id}.org",
        "summary_hash" => @zero_hash,
        "resume_command" => "run --resume RUN_DIR"
      },
      "detail",
      attention_detail(reason)
    )
  end

  # D3: the operator's next action after a preflight refusal is to upgrade a daemon, so the
  # detail says which capability was absent. Every other refusal carries no detail here: the
  # class in `reason` is what an alert routes on, and the wedge holds the rest.
  defp attention_detail(%{"reason" => "dispatch_preflight_unsupported", "missing_capabilities" => missing})
       when is_list(missing), do: %{"missing_capabilities" => Enum.filter(missing, &is_binary/1)}

  defp attention_detail(_reason), do: nil

  defp retry_scheduled_data(work_item, writer, next_attempt, max_attempts) do
    %{
      "work_item_id" => work_item["id"],
      "prior_assignment_id" => writer.assignment_id,
      "next_attempt" => next_attempt,
      "reason" => "gate_failed",
      "remaining_attempts" => max_attempts - next_attempt
    }
  end

  defp review_requested_data(review_id, work_item, writer, reviewer) do
    %{
      "review_id" => review_id,
      "work_item_id" => work_item["id"],
      "subject_assignment_id" => writer.assignment_id,
      "reviewer_agent" => reviewer["name"],
      "context_revision" => 0,
      "context_hash" => @zero_hash
    }
  end

  defp review_work_item(work_item, review_id) do
    review_path = "review/#{work_item["id"]}.org"

    base = %{
      "id" => work_item["id"],
      "kind" => "review",
      "role" => "reviewer",
      "review_id" => review_id,
      # A reviewer owns one exact evidence file, not the broader review root.
      "allowed_paths" => [review_path],
      "expected_artifacts" => [review_path]
    }

    maybe_put(base, "timeout_s", work_item["timeout_s"])
  end

  defp review_received_data(review_id, review, artifact, verdict) do
    data = %{
      "review_id" => review_id,
      "reviewer_assignment_id" => review.assignment_id,
      "review_artifact_id" => review.artifact_id,
      "verdict" => verdict.verdict,
      "review_hash" => verdict.hash || Map.get(artifact, "sha256", @zero_hash)
    }

    maybe_put(data, "finding_count", verdict.finding_count)
  end

  defp gate_requested_data(gate_run_id, work_item, writer, gate_id, command_argv) do
    %{
      "gate_run_id" => gate_run_id,
      "work_item_id" => work_item["id"],
      "assignment_id" => writer.assignment_id,
      "gate_id" => gate_id,
      "command_argv" => command_argv,
      "artifact_ids" => [writer.artifact_id],
      "timeout_s" => 600
    }
  end

  # the exact v2 recovery terminal; hashes are the Host's real Execution.evidence of the attempt,
  # duration is the journal-derived span passed in
  defp recovery_result(%{"evidence" => evidence}, duration_ms) do
    %{
      "exit_status" => nil,
      "termination" => %{"kind" => "recovery", "settled" => true, "leftovers" => "0", "proof" => "gone"},
      "duration_ms" => duration_ms,
      "stdout_hash" => evidence["stdout_hash"],
      "stderr_hash" => evidence["stderr_hash"],
      "stderr_merged" => false,
      "failure_summary" => %{"headline" => "gate recovered dead", "failures" => [], "suggestion" => "inspect gate output"}
    }
  end

  defp journaled_start_ts(events, gate_run_id) do
    starts = Enum.filter(events, &(&1["type"] == "gate_started" and &1["data"]["gate_run_id"] == gate_run_id))

    case List.last(starts) do
      %{"ts" => ts} -> ts_unix(ts)
      _ -> 0
    end
  end

  defp ts_unix(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _} -> DateTime.to_unix(dt)
      _ -> 0
    end
  end

  # the journaled start for a gate_run_id: {:v2, data, attempt} or :v1 (unresolved legacy)
  defp journaled_start(events, gate_run_id) do
    starts = Enum.filter(events, &(&1["type"] == "gate_started" and &1["data"]["gate_run_id"] == gate_run_id))
    latest = List.last(starts)

    # the read view of a v1 start is upcast to v2 with an UNRECORDED execution arm: only a recorded
    # execution (a pid) is reconcilable; anything else is a legacy start
    case latest do
      %{"event_version" => 2, "data" => %{"execution" => %{"pid" => pid}} = data} when is_integer(pid) ->
        {:v2, data, data["attempt"]}

      _ ->
        :v1
    end
  end

  defp journaled_deadline(events, gate_run_id) do
    {:v2, started, _attempt} = journaled_start(events, gate_run_id)
    started["deadline_unix"]
  end

  # closed diagnostic detail for gate attention: the reason's detail map through the same domain
  # the Host mapper uses; nil detail stays absent
  defp gate_detail(nil), do: nil
  defp gate_detail(detail) when is_map(detail), do: detail
  defp gate_detail(_other), do: nil

  defp settlement_detail(result) when is_map(result) do
    Map.take(result, ["kind", "settled", "leftovers", "proof", "exit_status", "signal"])
  end

  @spec work_items_in_order(map()) :: [map()]
  defp work_items_in_order(plan) do
    items = plan["work_items"]
    by_id = Map.new(items, &{&1["id"], &1})

    do_order_work_items(items, by_id, %{}, [])
  end

  @spec do_order_work_items([map()], %{String.t() => map()}, %{String.t() => true}, [map()]) :: [map()]
  defp do_order_work_items(items, by_id, completed_ids, ordered) do
    {ready, waiting} = Enum.split_with(items, &deps_completed?(&1, completed_ids))

    case {ready, waiting} do
      {[], []} ->
        Enum.reverse(ordered)

      {[], _waiting} ->
        Enum.reverse(ordered)

      {ready, waiting} ->
        ready_ids = Map.new(ready, &{&1["id"], true})
        completed_ids = Map.merge(completed_ids, ready_ids)
        ordered = Enum.reduce(ready, ordered, &[Map.fetch!(by_id, &1["id"]) | &2])
        do_order_work_items(waiting, by_id, completed_ids, ordered)
    end
  end

  @spec deps_completed?(map(), %{String.t() => true}) :: boolean()
  defp deps_completed?(work_item, completed_ids) do
    work_item
    |> Map.get("deps", [])
    |> Enum.all?(&Map.has_key?(completed_ids, &1))
  end

  defp writer_agent(spec, work_item), do: agent_for_role(spec, work_item["role"])
  defp reviewer_agent(spec), do: agent_for_role(spec, "reviewer")

  defp agent_for_role(spec, role), do: Enum.find(spec["agents"], &(&1["role"] == role))

  defp pane_ref(%{"pane_hint" => %{"pane_ref" => pane_ref}}), do: pane_ref
  defp pane_ref(%{"role" => role}), do: "pane_" <> role

  defp expected_artifact(%{"kind" => "review", "expected_artifacts" => [artifact | _rest]}, _agent), do: artifact
  defp expected_artifact(%{"expected_artifacts" => [artifact | _rest]}, _agent), do: artifact

  defp max_attempts(fsm, work_item) do
    Map.get(work_item, "max_attempts") || get_in(fsm.spec, ["budgets", "max_attempts_default"]) || 1
  end

  # The reducer's events are upgraded views, validated as such; the host folds the exact
  # committed maps with the line validation.
  defp fold_events(events), do: Fold.fold_views(events)

  defp upcast_all(events) when is_list(events) do
    Enum.reduce_while(events, {:ok, []}, fn event, {:ok, views} ->
      case Event.validate_read(event) do
        {:ok, view} -> {:cont, {:ok, views ++ [view]}}
        {:error, rejection} -> {:halt, {:error, rejection}}
      end
    end)
  end

  defp upcast_all(_events), do: {:error, %{clause: "invalid_journal"}}

  defp run_resumed_data(run, state, opts) do
    %{
      "supervisor_instance" => run.supervisor_instance,
      "last_seen_seq" => state.last_seq,
      "recovery_reason" => Keyword.get(opts, :recovery_reason, "crash_recovery")
    }
    |> put_present("run_lock_path", run.run_lock_path)
    |> put_tail_repair(opts)
    |> put_stamp(Keyword.get(opts, :requested_by))
  end

  defp run_cancel_data(run, state, opts) do
    put_present(
      %{
        "supervisor_instance" => run.supervisor_instance,
        "last_seen_seq" => state.last_seq,
        "reason" => Keyword.get(opts, :cancel_reason, "operator_cancel")
      },
      "run_lock_path",
      run.run_lock_path
    )
  end

  # The repair the journal writer performed on open (Chain.reconcile plan) is journaled on the
  # first event after the reopen only: run_resumed, or run_cancel_requested for a cancel.
  # The lock actually held (its run-relative basename, run.lock.<gen>) is journaled when known; the
  # field is optional and never a family prefix or an absolute path.
  defp put_present(data, key, value) when is_binary(value), do: Map.put(data, key, value)
  defp put_present(data, _key, _value), do: data

  defp put_tail_repair(data, opts) do
    case Keyword.get(opts, :tail_repair) do
      %{} = repair -> Map.put(data, "tail_repair", repair)
      _ -> data
    end
  end

  defp completed_work_item?(fsm, work_item) do
    work_item["id"] in fsm.completed_work_item_ids
  end

  defp roster_hash(spec) do
    canonical =
      spec
      |> Map.get("agents", [])
      |> Enum.map(&"#{&1["name"]}:#{&1["role"]}:#{&1["agent_id"] || ""}")
      |> Enum.sort()
      |> Enum.join("\n")

    sha256(canonical)
  end

  defp next_id(prefix, value), do: "#{prefix}_#{String.pad_leading(Integer.to_string(value), 4, "0")}"

  defp next_counter(values, prefix) do
    values
    |> Enum.map(&id_number(&1, prefix))
    |> Enum.max(fn -> 0 end)
    |> Kernel.+(1)
  end

  defp id_number(value, prefix) when is_binary(value) do
    case Regex.run(~r/^#{prefix}_(\d+)$/, value) do
      [_match, number] -> String.to_integer(number)
      _other -> 0
    end
  end

  defp id_number(_value, _prefix), do: 0

  defp sorted_set(%MapSet{} = set), do: set |> MapSet.to_list() |> Enum.sort()
  defp sorted_set(values) when is_list(values), do: Enum.sort(values)

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  defp wedge_detector(%{"detector" => detector}) when is_binary(detector), do: detector
  defp wedge_detector(%{"reason" => "agent_auth_blocked"}), do: "auth_prompt_detected"
  defp wedge_detector(_reason), do: "pane_status_blocked"

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
