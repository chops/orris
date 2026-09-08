defmodule AiOrchestrator.Journal.Fold do
  @moduledoc false

  alias AiOrchestrator.Journal.Event

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{}

    defstruct active_workspace_leases: %{},
              assignments: %{},
              attention_seen?: false,
              completed_work_item_ids: MapSet.new(),
              context_revision: 0,
              contract_changes: %{},
              event_ids: MapSet.new(),
              gate_runs: %{},
              last_seq: 0,
              observed_artifact_ids: MapSet.new(),
              open_assignment_ids: MapSet.new(),
              open_attention_ids: MapSet.new(),
              pane_lease_requests: %{},
              phase: :new,
              reason: nil,
              recovery_reservations: 0,
              reviews: %{},
              run_id: nil,
              terminal?: false,
              status: "new",
              work_item_ids: MapSet.new(),
              workspace_lease_requests: %{}
  end

  @spec fold_lines([String.t()]) :: {:ok, term()} | {:error, rejection()}
  def fold_lines(lines) when is_list(lines) do
    Enum.reduce_while(lines, {:ok, %State{}}, fn line, {:ok, state} ->
      fold_line(state, line)
    end)
  end

  def fold_lines(_lines), do: {:error, %{clause: "invalid_journal"}}

  @doc "Folds already-decoded events with the same historical validation as `fold_lines/1`."
  @spec fold_events([map()]) :: {:ok, term()} | {:error, rejection()}
  def fold_events(events) when is_list(events) do
    Enum.reduce_while(events, {:ok, %State{}}, fn event, {:ok, state} ->
      fold_event(state, event)
    end)
  end

  def fold_events(_events), do: {:error, %{clause: "invalid_journal"}}

  @doc "Folds in-memory upgraded views (the reducer's events) with view validation, never a line's."
  @spec fold_views([map()]) :: {:ok, term()} | {:error, rejection()}
  def fold_views(events) when is_list(events) do
    Enum.reduce_while(events, {:ok, %State{}}, fn event, {:ok, state} ->
      with {:ok, event} <- Event.validate_view(event),
           {:ok, next_state} <- apply_event(state, event) do
        {:cont, {:ok, next_state}}
      else
        {:error, rejection} -> {:halt, {:error, rejection}}
      end
    end)
  end

  def fold_views(_events), do: {:error, %{clause: "invalid_journal"}}

  @spec summary(term()) :: map()
  def summary(%State{} = state) do
    base = %{
      "run_id" => state.run_id,
      "status" => state.status,
      "last_seq" => state.last_seq
    }

    cond do
      state.status == "failed" ->
        Map.put(base, "reason", state.reason)

      terminal_with_completed_ids?(state.status) ->
        Map.put(base, "completed_work_item_ids", sorted_set(state.completed_work_item_ids))

      state.attention_seen? ->
        Map.put(base, "open_attention_ids", sorted_set(state.open_attention_ids))

      MapSet.size(state.open_assignment_ids) > 0 ->
        Map.put(base, "open_assignment_ids", sorted_set(state.open_assignment_ids))

      true ->
        base
    end
  end

  def summary(_state), do: %{}

  defp apply_event(state, event) do
    with :ok <- validate_sequence(state, event),
         :ok <- validate_first_event(state, event),
         :ok <- validate_event_id(state, event),
         :ok <- validate_run_id(state, event),
         :ok <- validate_preamble(state, event),
         :ok <- validate_terminal(state, event),
         :ok <- validate_attention_block(state, event),
         :ok <- validate_domain(state, event) do
      {:ok, update_state(state, event)}
    end
  end

  defp validate_sequence(%State{last_seq: last_seq}, %{"seq" => seq}) when seq == last_seq + 1, do: :ok

  defp validate_sequence(%State{last_seq: last_seq}, %{"seq" => seq}) when seq <= last_seq,
    do: {:error, %{clause: "seq_duplicate", at_seq: seq}}

  defp validate_sequence(_state, %{"seq" => seq}), do: {:error, %{clause: "seq_gap", at_seq: seq}}

  defp validate_first_event(%State{last_seq: 0}, %{"type" => "run_created"}), do: :ok
  defp validate_first_event(%State{last_seq: 0}, _event), do: {:error, %{clause: "first_event_not_run_created"}}
  defp validate_first_event(_state, _event), do: :ok

  defp validate_event_id(state, %{"event_id" => event_id}) do
    if MapSet.member?(state.event_ids, event_id) do
      {:error, %{clause: "event_id_repeat", event_id: event_id}}
    else
      :ok
    end
  end

  defp validate_run_id(%State{run_id: nil}, _event), do: :ok
  defp validate_run_id(%State{run_id: run_id}, %{"run_id" => run_id}), do: :ok
  defp validate_run_id(_state, %{"seq" => seq}), do: {:error, %{clause: "run_id_mismatch", at_seq: seq}}

  defp validate_preamble(state, %{"seq" => seq, "type" => type}) do
    if preamble_allowed?(state.phase, type) do
      :ok
    else
      {:error, %{clause: "preamble_violation", at_seq: seq}}
    end
  end

  defp validate_terminal(%State{terminal?: true}, %{"type" => type, "seq" => seq}) do
    cond do
      run_terminal?(type) -> {:error, %{clause: "second_terminal_run", at_seq: seq}}
      cleanup_event?(type) -> :ok
      true -> {:error, %{clause: "events_after_terminal", at_seq: seq}}
    end
  end

  defp validate_terminal(_state, _event), do: :ok

  defp validate_attention_block(%State{open_attention_ids: open_attention_ids}, %{"type" => type, "seq" => seq}) do
    if MapSet.size(open_attention_ids) > 0 and blocks_on_attention?(type) do
      {:error, %{clause: "dispatch_during_attention_block", at_seq: seq}}
    else
      :ok
    end
  end

  # recovery reservation (docs/contracts/recovery-reservation.org): cross-field impossibility first (nothing could
  # be reserved), then history - the attempt must be the run-wide count + 1; the count never resets across lost
  # generations, authorities or accepted commands, and no ordering of lost_generation values is enforced
  defp validate_domain(state, %{"type" => "run_recovery_reserved", "seq" => seq, "data" => data}) do
    %{"attempt" => attempt, "limit" => limit} = data

    cond do
      limit == 0 or attempt > limit ->
        {:error, %{clause: "recovery_reservation_invalid", at_seq: seq, field: "attempt"}}

      attempt != state.recovery_reservations + 1 ->
        {:error, %{clause: "recovery_reservation_out_of_order", at_seq: seq, expected: state.recovery_reservations + 1}}

      true ->
        :ok
    end
  end

  defp validate_domain(state, %{"type" => "assignment_requested", "data" => data}),
    do: validate_assignment_requested(state, data)

  defp validate_domain(state, %{"type" => "assignment_prompt_projected", "data" => data}),
    do: validate_assignment_exists(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "assignment_dispatch_sent", "data" => data}),
    do: validate_assignment_dispatch(state, data)

  defp validate_domain(state, %{"type" => "assignment_observation_started", "data" => data}),
    do: validate_assignment_exists(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "artifact_observed", "data" => data}),
    do: validate_assignment_exists(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "assignment_completed", "data" => data}),
    do: validate_assignment_terminal(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "assignment_failed", "data" => data}),
    do: validate_assignment_terminal(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "pane_lease_requested", "data" => data}),
    do: validate_assignment_exists(state, data["assignment_id"])

  defp validate_domain(state, %{"type" => "pane_lease_acquired", "data" => data}),
    do: validate_pane_lease_acquired(state, data)

  defp validate_domain(state, %{"type" => "workspace_lease_requested", "data" => data}),
    do: validate_workspace_lease_requested(state, data)

  defp validate_domain(state, %{"type" => "workspace_lease_acquired", "data" => data}),
    do: validate_workspace_lease_acquired(state, data)

  defp validate_domain(state, %{"type" => "gate_requested", "data" => data}), do: validate_gate_requested(state, data)

  defp validate_domain(state, %{"type" => "gate_started", "data" => data}),
    do: validate_gate_exists(state, data["gate_run_id"])

  defp validate_domain(state, %{"type" => "gate_passed", "data" => data}),
    do: validate_gate_exists(state, data["gate_run_id"])

  defp validate_domain(state, %{"type" => "gate_failed", "data" => data}),
    do: validate_gate_exists(state, data["gate_run_id"])

  defp validate_domain(state, %{"type" => "review_requested", "data" => data}), do: validate_review_requested(state, data)

  defp validate_domain(state, %{"type" => "review_received", "data" => data}), do: validate_review_received(state, data)

  defp validate_domain(state, %{"type" => "review_disposition_recorded", "data" => data}),
    do: validate_review_disposition(state, data)

  defp validate_domain(state, %{"type" => "work_item_completed", "data" => data}),
    do: validate_work_item_completed(state, data)

  defp validate_domain(state, %{"type" => "run_completed", "data" => data}), do: validate_run_completed(state, data)

  defp validate_domain(state, %{"type" => "context_patch_accepted", "data" => data}),
    do: validate_context_patch_accepted(state, data)

  defp validate_domain(state, %{"type" => "contract_change_ratified", "data" => data}),
    do: validate_contract_change_ratified(state, data)

  defp validate_domain(state, event), do: validate_event_context_revision(state, event)

  defp fold_line(state, line) do
    with {:ok, event} <- Event.validate_line(line),
         {:ok, next_state} <- apply_event(state, event) do
      {:cont, {:ok, next_state}}
    else
      {:error, rejection} -> {:halt, {:error, rejection}}
    end
  end

  defp fold_event(state, event) do
    with {:ok, event} <- Event.validate_read(event),
         {:ok, next_state} <- apply_event(state, event) do
      {:cont, {:ok, next_state}}
    else
      {:error, rejection} -> {:halt, {:error, rejection}}
    end
  end

  defp update_state(state, event) do
    event
    |> apply_type_update(state)
    |> Map.update!(:event_ids, &MapSet.put(&1, event["event_id"]))
    |> Map.put(:last_seq, event["seq"])
  end

  defp apply_type_update(%{"type" => "run_created", "run_id" => run_id}, state) do
    %{state | phase: :run_created, run_id: run_id, status: "created"}
  end

  defp apply_type_update(%{"type" => "run_spec_loaded"}, state), do: %{state | phase: :run_spec_loaded}

  defp apply_type_update(%{"type" => "run_started"}, state), do: %{state | phase: :run_started, status: "in_flight"}

  defp apply_type_update(%{"type" => "run_resumed", "data" => data}, state) do
    resolved_attention_ids = Map.get(data, "resolves_attention_ids", [])

    %{
      state
      | open_attention_ids: drop_many(state.open_attention_ids, resolved_attention_ids),
        phase: :run_started,
        status: "in_flight"
    }
  end

  defp apply_type_update(
         %{
           "type" => "plan_recorded",
           "data" => %{"work_item_ids" => work_item_ids, "context_revision" => context_revision}
         },
         state
       ) do
    %{state | context_revision: context_revision, phase: :plan_recorded, work_item_ids: MapSet.new(work_item_ids)}
  end

  defp apply_type_update(%{"type" => "assignment_requested", "data" => data}, state) do
    assignment_id = data["assignment_id"]

    assignment = %{
      artifact_ids: MapSet.new(),
      context_revision: data["context_revision"],
      role: data["role"],
      terminal?: false,
      work_item_id: data["work_item_id"]
    }

    %{
      state
      | assignments: Map.put(state.assignments, assignment_id, assignment),
        open_assignment_ids: MapSet.put(state.open_assignment_ids, assignment_id),
        status: "in_flight"
    }
  end

  defp apply_type_update(%{"type" => "artifact_observed", "data" => data}, state) do
    state
    |> update_assignment(data["assignment_id"], fn assignment ->
      assignment
      |> Map.update!(:artifact_ids, &MapSet.put(&1, data["artifact_id"]))
      |> Map.put(:latest_artifact_seq, state.last_seq + 1)
    end)
    |> Map.update!(:observed_artifact_ids, &MapSet.put(&1, data["artifact_id"]))
  end

  defp apply_type_update(%{"type" => type, "data" => %{"assignment_id" => assignment_id}}, state)
       when type in ["assignment_completed", "assignment_failed"] do
    state
    |> update_assignment(assignment_id, &Map.put(&1, :terminal?, true))
    |> Map.update!(:open_assignment_ids, &MapSet.delete(&1, assignment_id))
  end

  defp apply_type_update(%{"type" => "pane_lease_requested", "data" => data}, state) do
    request = %{assignment_id: data["assignment_id"], pane_ref: data["pane_ref"]}
    %{state | pane_lease_requests: Map.put(state.pane_lease_requests, data["lease_request_id"], request)}
  end

  defp apply_type_update(%{"type" => "pane_lease_acquired", "data" => data}, state) do
    case state.pane_lease_requests[data["lease_request_id"]] do
      %{assignment_id: assignment_id, pane_ref: pane_ref} ->
        update_assignment(state, assignment_id, fn assignment ->
          assignment
          |> Map.put(:pane_lease?, true)
          |> Map.put(:pane_ref, pane_ref)
        end)

      nil ->
        state
    end
  end

  defp apply_type_update(%{"type" => "pane_lease_released", "data" => %{"pane_ref" => pane_ref}}, state) do
    assignments =
      Map.new(state.assignments, fn {assignment_id, assignment} ->
        if assignment[:pane_ref] == pane_ref do
          {assignment_id, Map.put(assignment, :pane_lease?, false)}
        else
          {assignment_id, assignment}
        end
      end)

    %{state | assignments: assignments}
  end

  defp apply_type_update(%{"type" => "workspace_lease_requested", "data" => data}, state) do
    request = %{
      allowed_paths: Map.get(data, "allowed_paths", []),
      assignment_id: data["assignment_id"],
      mode: data["mode"]
    }

    %{state | workspace_lease_requests: Map.put(state.workspace_lease_requests, data["workspace_lease_id"], request)}
  end

  defp apply_type_update(%{"type" => "workspace_lease_acquired", "data" => data}, state) do
    workspace_lease_id = data["workspace_lease_id"]

    case state.workspace_lease_requests[workspace_lease_id] do
      %{assignment_id: assignment_id} = request ->
        state
        |> update_assignment(assignment_id, &Map.put(&1, :workspace_lease?, true))
        |> Map.update!(:active_workspace_leases, &Map.put(&1, workspace_lease_id, request))

      nil ->
        state
    end
  end

  defp apply_type_update(%{"type" => "workspace_lease_released", "data" => data}, state) do
    workspace_lease_id = data["workspace_lease_id"]

    state
    |> Map.update!(:active_workspace_leases, &Map.delete(&1, workspace_lease_id))
    |> release_assignment_workspace_lease(workspace_lease_id)
  end

  defp apply_type_update(%{"type" => "gate_requested", "data" => data}, state) do
    gate = %{
      artifact_ids: MapSet.new(Map.get(data, "artifact_ids", [])),
      assignment_id: data["assignment_id"],
      status: "requested",
      work_item_id: data["work_item_id"]
    }

    %{state | gate_runs: Map.put(state.gate_runs, data["gate_run_id"], gate)}
  end

  defp apply_type_update(%{"type" => "gate_started", "data" => %{"gate_run_id" => gate_run_id}}, state) do
    update_gate(state, gate_run_id, &Map.put(&1, :status, "started"))
  end

  defp apply_type_update(%{"type" => "gate_passed", "data" => %{"gate_run_id" => gate_run_id}}, state) do
    update_gate(state, gate_run_id, &Map.put(&1, :status, "passed"))
  end

  defp apply_type_update(%{"type" => "gate_failed", "data" => %{"gate_run_id" => gate_run_id}}, state) do
    update_gate(state, gate_run_id, &Map.put(&1, :status, "failed"))
  end

  defp apply_type_update(%{"type" => "review_requested", "data" => data}, state) do
    review = %{subject_assignment_id: data["subject_assignment_id"], work_item_id: data["work_item_id"]}
    %{state | reviews: Map.put(state.reviews, data["review_id"], review)}
  end

  defp apply_type_update(%{"type" => "review_disposition_recorded", "data" => data}, state) do
    update_review(state, data["review_id"], &Map.put(&1, :disposition, data["disposition"]))
  end

  defp apply_type_update(%{"type" => "context_patch_accepted", "data" => data}, state) do
    %{state | context_revision: data["new_context_revision"]}
  end

  defp apply_type_update(%{"type" => "contract_change_proposed", "data" => data}, state) do
    change = %{affected_contract: data["affected_contract"], breaking?: data["breaking"], proposer: data["proposer"]}
    %{state | contract_changes: Map.put(state.contract_changes, data["contract_change_id"], change)}
  end

  defp apply_type_update(%{"type" => "contract_change_ratified", "data" => data}, state) do
    %{state | context_revision: data["new_context_revision"]}
  end

  defp apply_type_update(%{"type" => "human_attention_required", "data" => %{"attention_id" => attention_id}}, state) do
    %{
      state
      | attention_seen?: true,
        open_attention_ids: MapSet.put(state.open_attention_ids, attention_id),
        status: "blocked"
    }
  end

  defp apply_type_update(%{"type" => "work_item_completed", "data" => %{"work_item_id" => work_item_id}}, state) do
    %{state | completed_work_item_ids: MapSet.put(state.completed_work_item_ids, work_item_id)}
  end

  defp apply_type_update(%{"type" => "run_completed"}, state) do
    %{state | status: "completed", terminal?: true}
  end

  defp apply_type_update(%{"type" => "run_failed", "data" => data}, state) do
    %{state | status: "failed", reason: data["reason"], terminal?: true}
  end

  defp apply_type_update(%{"type" => "run_cancelled"}, state), do: %{state | status: "cancelled", terminal?: true}

  # bookkeeping only: phase, status, terminal and command classification are untouched
  defp apply_type_update(%{"type" => "run_recovery_reserved"}, state),
    do: %{state | recovery_reservations: state.recovery_reservations + 1}

  defp apply_type_update(%{"type" => "run_budget_exhausted"}, state) do
    %{state | status: "budget_exhausted", terminal?: true}
  end

  defp apply_type_update(_event, state), do: state

  defp validate_assignment_requested(state, data) do
    with :ok <- validate_work_item_exists(state, data["work_item_id"]) do
      validate_context_revision_available(state, data["context_revision"], data["assignment_id"])
    end
  end

  defp validate_assignment_dispatch(state, data) do
    assignment_id = data["assignment_id"]

    with {:ok, assignment} <- fetch_assignment(state, assignment_id),
         :ok <- validate_context_revision_available(state, assignment[:context_revision], assignment_id),
         :ok <- validate_pane_lease_for_dispatch(assignment, assignment_id) do
      validate_workspace_lease_for_dispatch(assignment, assignment_id)
    end
  end

  defp validate_assignment_terminal(state, assignment_id) do
    with {:ok, assignment} <- fetch_assignment(state, assignment_id) do
      if assignment.terminal? do
        {:error, %{clause: "second_terminal_assignment", entity_id: assignment_id}}
      else
        :ok
      end
    end
  end

  defp validate_pane_lease_acquired(state, data) do
    case state.pane_lease_requests[data["lease_request_id"]] do
      %{assignment_id: assignment_id} -> validate_assignment_exists(state, assignment_id)
      nil -> {:error, %{clause: "unknown_entity_ref", entity_id: data["lease_request_id"]}}
    end
  end

  defp validate_workspace_lease_requested(state, data) do
    validate_assignment_exists(state, data["assignment_id"])
  end

  defp validate_workspace_lease_acquired(state, data) do
    workspace_lease_id = data["workspace_lease_id"]

    case state.workspace_lease_requests[workspace_lease_id] do
      %{allowed_paths: allowed_paths, assignment_id: assignment_id} ->
        with :ok <- validate_assignment_exists(state, assignment_id) do
          validate_workspace_paths_available(state, workspace_lease_id, allowed_paths)
        end

      nil ->
        {:error, %{clause: "unknown_entity_ref", entity_id: workspace_lease_id}}
    end
  end

  defp validate_gate_requested(state, data) do
    with :ok <- validate_work_item_exists(state, data["work_item_id"]),
         :ok <- validate_gate_artifacts_known(state, Map.get(data, "artifact_ids", [])) do
      validate_assignment_exists(state, data["assignment_id"])
    end
  end

  defp validate_gate_exists(state, gate_run_id) do
    if Map.has_key?(state.gate_runs, gate_run_id) do
      :ok
    else
      {:error, %{clause: "unknown_entity_ref", entity_id: gate_run_id}}
    end
  end

  defp validate_review_requested(state, data) do
    with :ok <- validate_work_item_exists(state, data["work_item_id"]) do
      validate_assignment_exists(state, data["subject_assignment_id"])
    end
  end

  defp validate_review_received(state, data) do
    with :ok <- validate_review_exists(state, data["review_id"]) do
      validate_assignment_exists(state, data["reviewer_assignment_id"])
    end
  end

  defp validate_review_disposition(state, data) do
    with :ok <- validate_review_exists(state, data["review_id"]) do
      if data["disposition"] == "accepted_complete" do
        {:error, %{clause: "review_completes_item", entity_id: data["review_id"]}}
      else
        :ok
      end
    end
  end

  defp validate_work_item_completed(state, data) do
    work_item_id = data["work_item_id"]

    with :ok <- validate_work_item_exists(state, work_item_id),
         :ok <- validate_assignment_exists(state, data["completing_assignment_id"]) do
      validate_required_gate_passes(state, data)
    end
  end

  defp validate_context_patch_accepted(state, data) do
    prior_revision = data["prior_context_revision"]
    next_revision = data["new_context_revision"]

    if prior_revision == state.context_revision and next_revision == state.context_revision + 1 do
      :ok
    else
      {:error, %{clause: "context_revision_skip", entity_id: data["proposal_id"]}}
    end
  end

  defp validate_run_completed(state, data) do
    claimed_ids = MapSet.new(Map.get(data, "completed_work_item_ids", []))
    validate_run_completed_payload(state, claimed_ids)
  end

  defp validate_contract_change_ratified(state, data) do
    contract_change_id = data["contract_change_id"]
    ratifier = data["ratified_by"]
    change = state.contract_changes[contract_change_id]

    cond do
      ratifier == "policy_auto" and oracle_contract_change?(change) ->
        {:error, %{clause: "auto_accepted_oracle_change", entity_id: contract_change_id}}

      ratifier != "operator" ->
        {:error, %{clause: "agent_ratified_contract_change", entity_id: contract_change_id}}

      data["prior_context_revision"] != state.context_revision or
          data["new_context_revision"] != state.context_revision + 1 ->
        {:error, %{clause: "context_revision_skip", entity_id: contract_change_id}}

      true ->
        :ok
    end
  end

  defp validate_event_context_revision(state, %{
         "data" => %{"assignment_id" => assignment_id, "context_revision" => revision}
       })
       when is_integer(revision) do
    validate_context_revision_available(state, revision, assignment_id)
  end

  defp validate_event_context_revision(_state, _event), do: :ok

  defp validate_work_item_exists(%State{work_item_ids: work_item_ids}, work_item_id) do
    if MapSet.member?(work_item_ids, work_item_id) do
      :ok
    else
      {:error, %{clause: "unknown_entity_ref", entity_id: work_item_id}}
    end
  end

  defp validate_assignment_exists(state, assignment_id) do
    case fetch_assignment(state, assignment_id) do
      {:ok, _assignment} -> :ok
      {:error, rejection} -> {:error, rejection}
    end
  end

  defp validate_review_exists(state, review_id) do
    if Map.has_key?(state.reviews, review_id) do
      :ok
    else
      {:error, %{clause: "unknown_entity_ref", entity_id: review_id}}
    end
  end

  defp validate_context_revision_available(state, revision, assignment_id) when is_integer(revision) do
    if revision <= state.context_revision do
      :ok
    else
      {:error, %{clause: "dispatch_future_revision", entity_id: assignment_id}}
    end
  end

  defp validate_context_revision_available(_state, _revision, _assignment_id), do: :ok

  defp validate_pane_lease_for_dispatch(assignment, assignment_id) do
    if assignment[:pane_lease?] do
      :ok
    else
      {:error, %{clause: "dispatch_without_pane_lease", entity_id: assignment_id}}
    end
  end

  defp validate_workspace_lease_for_dispatch(%{role: role} = assignment, assignment_id)
       when role in ["writer", "integration"] do
    if assignment[:workspace_lease?] do
      :ok
    else
      {:error, %{clause: "writer_dispatch_without_workspace_lease", entity_id: assignment_id}}
    end
  end

  defp validate_workspace_lease_for_dispatch(_assignment, _assignment_id), do: :ok

  defp validate_workspace_paths_available(state, workspace_lease_id, allowed_paths) do
    if Enum.any?(state.active_workspace_leases, fn {_id, lease} ->
         paths_overlap?(allowed_paths, lease.allowed_paths)
       end) do
      {:error, %{clause: "workspace_lease_overlap", entity_id: workspace_lease_id}}
    else
      :ok
    end
  end

  defp validate_required_gate_passes(state, data) do
    gate_run_ids = Map.get(data, "required_gate_run_ids", [])
    work_item_id = data["work_item_id"]
    accepted_artifact_ids = MapSet.new(Map.get(data, "accepted_artifact_ids", []))

    if gate_run_ids != [] and
         Enum.all?(gate_run_ids, &passed_gate_for_work_item?(state, &1, work_item_id, accepted_artifact_ids)) do
      :ok
    else
      {:error, %{clause: "completion_without_fresh_gate", entity_id: work_item_id}}
    end
  end

  defp validate_gate_artifacts_known(state, artifact_ids) do
    case Enum.find(artifact_ids, &(not MapSet.member?(state.observed_artifact_ids, &1))) do
      nil -> :ok
      artifact_id -> {:error, %{clause: "unknown_entity_ref", entity_id: artifact_id}}
    end
  end

  defp validate_run_completed_payload(state, claimed_ids) do
    case first_mismatch(claimed_ids, state.completed_work_item_ids) do
      nil -> :ok
      work_item_id -> {:error, %{clause: "run_completed_mismatch", entity_id: work_item_id}}
    end
  end

  defp passed_gate_for_work_item?(state, gate_run_id, work_item_id, accepted_artifact_ids) do
    case state.gate_runs[gate_run_id] do
      %{artifact_ids: gate_artifact_ids, status: "passed", work_item_id: ^work_item_id} ->
        MapSet.subset?(accepted_artifact_ids, gate_artifact_ids)

      _other ->
        false
    end
  end

  defp fetch_assignment(state, assignment_id) do
    case state.assignments[assignment_id] do
      nil -> {:error, %{clause: "unknown_entity_ref", entity_id: assignment_id}}
      assignment -> {:ok, assignment}
    end
  end

  defp update_assignment(state, assignment_id, fun) do
    Map.update!(state, :assignments, fn assignments ->
      case assignments[assignment_id] do
        nil -> assignments
        assignment -> Map.put(assignments, assignment_id, fun.(assignment))
      end
    end)
  end

  defp update_gate(state, gate_run_id, fun) do
    Map.update!(state, :gate_runs, fn gate_runs ->
      case gate_runs[gate_run_id] do
        nil -> gate_runs
        gate -> Map.put(gate_runs, gate_run_id, fun.(gate))
      end
    end)
  end

  defp update_review(state, review_id, fun) do
    Map.update!(state, :reviews, fn reviews ->
      case reviews[review_id] do
        nil -> reviews
        review -> Map.put(reviews, review_id, fun.(review))
      end
    end)
  end

  defp release_assignment_workspace_lease(state, workspace_lease_id) do
    case state.workspace_lease_requests[workspace_lease_id] do
      %{assignment_id: assignment_id} -> update_assignment(state, assignment_id, &Map.put(&1, :workspace_lease?, false))
      nil -> state
    end
  end

  defp drop_many(set, values), do: Enum.reduce(values, set, &MapSet.delete(&2, &1))

  defp run_terminal?(type), do: type in ["run_completed", "run_cancelled", "run_budget_exhausted", "run_failed"]

  defp preamble_allowed?(:new, "run_created"), do: true
  defp preamble_allowed?(:run_created, type), do: type == "run_spec_loaded" or run_terminal?(type)
  defp preamble_allowed?(:run_spec_loaded, type), do: type == "plan_recorded" or run_terminal?(type)
  defp preamble_allowed?(:plan_recorded, type), do: type in ["run_started", "run_resumed"] or run_terminal?(type)
  defp preamble_allowed?(:run_started, _type), do: true
  defp preamble_allowed?(_phase, _type), do: false

  defp cleanup_event?(type) do
    type in [
      "pane_lease_release_requested",
      "pane_lease_released",
      "workspace_lease_release_requested",
      "workspace_lease_released",
      "notification_requested",
      "notification_sent",
      "notification_failed"
    ]
  end

  defp terminal_with_completed_ids?(status), do: status in ["completed", "cancelled", "budget_exhausted"]

  defp blocks_on_attention?(type) do
    type in [
      "assignment_requested",
      "assignment_prompt_projected",
      "assignment_dispatch_sent",
      "assignment_observation_started",
      "artifact_observed",
      "assignment_completed",
      "assignment_failed",
      "pane_lease_requested",
      "pane_lease_acquired",
      "workspace_lease_requested",
      "workspace_lease_acquired",
      "gate_requested",
      "gate_started",
      "gate_passed",
      "gate_failed",
      "review_requested",
      "review_received",
      "review_disposition_recorded",
      "work_item_completed",
      "work_item_failed"
    ]
  end

  defp oracle_contract_change?(%{affected_contract: affected_contract}) do
    affected_contract in ["gate", "gates", "oracle", "oracles", "acceptance"]
  end

  defp oracle_contract_change?(_change), do: false

  defp paths_overlap?(left_paths, right_paths) do
    Enum.any?(left_paths, fn left ->
      Enum.any?(right_paths, &path_overlap?(left, &1))
    end)
  end

  defp path_overlap?(left, right) do
    left = normalize_path(left)
    right = normalize_path(right)

    left == right or String.starts_with?(left, right <> "/") or String.starts_with?(right, left <> "/")
  end

  defp normalize_path(path), do: path |> String.trim() |> String.trim_trailing("/")

  defp first_mismatch(left_ids, right_ids) do
    left_ids
    |> MapSet.difference(right_ids)
    |> MapSet.union(MapSet.difference(right_ids, left_ids))
    |> sorted_set()
    |> List.first()
  end

  defp sorted_set(set), do: set |> MapSet.to_list() |> Enum.sort()
end
