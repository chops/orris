defmodule AiOrchestrator.Journal.Event do
  @moduledoc false

  alias AiOrchestrator.Journal.Schemas.EventData

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  @schema_name "ai-orchestrator/journal-event"
  @chain_hash_pattern ~r/^sha256:[0-9a-f]{64}$/
  @schema_cache_version :crypto.hash(:sha256, File.read!(__ENV__.file))
  @event_types MapSet.new([
                 "run_created",
                 "run_spec_loaded",
                 "plan_recorded",
                 "run_started",
                 "run_resumed",
                 "run_recovery_reserved",
                 "run_completed",
                 "run_cancelled",
                 "run_budget_exhausted",
                 "run_failed",
                 "run_pause_requested",
                 "run_paused",
                 "run_cancel_requested",
                 "assignment_requested",
                 "assignment_prompt_projected",
                 "assignment_dispatch_sent",
                 "assignment_observation_started",
                 "artifact_observed",
                 "assignment_completed",
                 "assignment_failed",
                 "scheduler_stalled",
                 "work_item_retry_scheduled",
                 "work_item_completed",
                 "work_item_failed",
                 "pane_lease_requested",
                 "pane_lease_acquired",
                 "pane_lease_failed",
                 "pane_lease_release_requested",
                 "pane_lease_released",
                 "workspace_lease_requested",
                 "workspace_lease_acquired",
                 "workspace_lease_failed",
                 "workspace_lease_release_requested",
                 "workspace_lease_released",
                 "gate_requested",
                 "gate_started",
                 "gate_passed",
                 "gate_failed",
                 "review_requested",
                 "review_received",
                 "review_disposition_recorded",
                 "context_patch_proposed",
                 "context_patch_accepted",
                 "context_patch_rejected",
                 "context_conflict_detected",
                 "contract_change_proposed",
                 "contract_change_ratified",
                 "contract_change_rejected",
                 "stop_policy_evaluated",
                 "agent_wedge_detected",
                 "human_attention_required",
                 "notification_requested",
                 "notification_sent",
                 "notification_failed"
               ])

  # Declared but not appendable by current production code (ledger NS-40). Each entry
  # names its target wave in `AiOrchestrator.Journal.Vocabulary`. Journal.Writer refuses
  # these at append time from Wave 2; historical fixtures containing them stay fold-legal.
  @reserved_types MapSet.new([
                    "assignment_failed",
                    "context_conflict_detected",
                    "context_patch_accepted",
                    "context_patch_proposed",
                    "context_patch_rejected",
                    "contract_change_proposed",
                    "contract_change_ratified",
                    "contract_change_rejected",
                    "notification_failed",
                    "notification_requested",
                    "notification_sent",
                    "pane_lease_failed",
                    "run_budget_exhausted",
                    "run_failed",
                    "run_pause_requested",
                    "run_paused",
                    "run_recovery_reserved",
                    "scheduler_stalled",
                    "stop_policy_evaluated",
                    "work_item_failed",
                    "workspace_lease_failed"
                  ])

  @spec declared_types() :: MapSet.t(String.t())
  def declared_types, do: @event_types

  @spec reserved_types() :: MapSet.t(String.t())
  def reserved_types, do: @reserved_types

  @spec reserved?(String.t()) :: boolean()
  def reserved?(type) when is_binary(type), do: MapSet.member?(@reserved_types, type)

  @spec appendable_types() :: MapSet.t(String.t())
  def appendable_types, do: MapSet.difference(@event_types, @reserved_types)

  @spec validate_line(String.t()) :: {:ok, map()} | {:error, rejection()}
  def validate_line(line) when is_binary(line) do
    case Jason.decode(line) do
      {:ok, event} -> validate_event(event, :read)
      _ -> {:error, %{clause: "invalid_event_shape"}}
    end
  end

  def validate_line(_line), do: {:error, %{clause: "invalid_event_shape"}}

  @doc "Validates an already-decoded event in historical (read) mode; the fold of decoded events uses it."
  @spec validate_read(map()) :: {:ok, map()} | {:error, rejection()}
  def validate_read(event) when is_map(event), do: validate_event(event, :read)
  def validate_read(_event), do: {:error, %{clause: "invalid_event_shape"}}

  @spec validate_append(map()) :: {:ok, map()} | {:error, rejection()}
  def validate_append(event) when is_map(event) do
    with {:ok, parsed} <- validate_envelope(event),
         :ok <- validate_appendable(parsed),
         {:ok, _data} <- EventData.parse(parsed, :append) do
      {:ok, parsed}
    end
  end

  def validate_append(_event), do: {:error, %{clause: "invalid_event_shape"}}

  @spec json_schema() :: map()
  def json_schema, do: EventData.json_schema()

  @doc "The version a producer appends for a type (MUST-7 per-type versioning); nil for an untyped type."
  @spec current_version(String.t()) :: pos_integer() | nil
  defdelegate current_version(type), to: EventData

  @doc "The read-side upgraded view of a decoded event; the line itself is never rewritten."
  @spec upcast(map()) :: {:ok, map()} | {:error, rejection()}
  defdelegate upcast(event), to: EventData

  @doc """
  Validates an in-memory upgraded view (the reducer's own events): the envelope, and the
  payload at the current version in view mode. A line on disk is never validated this way.
  """
  @spec validate_view(map()) :: {:ok, map()} | {:error, rejection()}
  def validate_view(event) when is_map(event) do
    with {:ok, parsed} <- validate_envelope(event),
         {:ok, _data} <- EventData.parse(parsed, :view) do
      {:ok, parsed}
    end
  end

  def validate_view(_event), do: {:error, %{clause: "invalid_event_shape"}}

  defp envelope_schema do
    key = {__MODULE__, @schema_cache_version, :envelope_schema}

    case :persistent_term.get(key, nil) do
      nil ->
        schema =
          Zoi.map(
            %{
              "schema" => Zoi.literal(@schema_name),
              "schema_version" => Zoi.enum([1, 2]),
              "event_version" => Zoi.integer(),
              "seq" => Zoi.integer(),
              "event_id" => Zoi.string(),
              "type" => Zoi.enum(MapSet.to_list(@event_types)),
              "ts" => Zoi.string(),
              "run_id" => Zoi.string(),
              "actor" => Zoi.literal("run_supervisor"),
              "traceparent" => Zoi.optional(Zoi.string()),
              "prev_line_sha256" => Zoi.optional(Zoi.regex(Zoi.string(), @chain_hash_pattern)),
              "data" => Zoi.map()
            },
            unrecognized_keys: :error
          )

        :persistent_term.put(key, schema)
        schema

      schema ->
        schema
    end
  end

  defp validate_required(event) when is_map(event) do
    required = [
      "schema",
      "schema_version",
      "event_version",
      "seq",
      "event_id",
      "type",
      "ts",
      "run_id",
      "actor",
      "data"
    ]

    case Enum.find(required, &(not Map.has_key?(event, &1))) do
      nil -> :ok
      field -> {:error, %{clause: "missing_required_field", field: field}}
    end
  end

  defp validate_required(_event), do: {:error, %{clause: "invalid_event_shape"}}

  defp validate_schema_version(%{"schema_version" => version}) when version in [1, 2], do: :ok

  defp validate_schema_version(%{"schema_version" => version}) when is_integer(version),
    do: {:error, %{clause: "unsupported_schema_version", schema_version: version}}

  defp validate_schema_version(%{"schema_version" => _version}), do: {:error, %{clause: "schema_version_not_numeric"}}

  defp validate_chain_field(%{"schema_version" => 1} = event) do
    if Map.has_key?(event, "prev_line_sha256") do
      {:error, %{clause: "chain_field_forbidden_on_v1"}}
    else
      :ok
    end
  end

  defp validate_chain_field(%{"schema_version" => 2, "prev_line_sha256" => hash}) when is_binary(hash) do
    if Regex.match?(@chain_hash_pattern, hash) do
      :ok
    else
      {:error, %{clause: "invalid_prev_line_sha256"}}
    end
  end

  defp validate_chain_field(%{"schema_version" => 2}), do: {:error, %{clause: "missing_prev_line_sha256"}}

  defp validate_type(%{"type" => type}) when is_binary(type) do
    if MapSet.member?(@event_types, type) do
      :ok
    else
      {:error, %{clause: "unknown_event_type", event_type: type}}
    end
  end

  defp validate_type(_event), do: {:error, %{clause: "invalid_event_shape"}}

  # The line is validated as written, at its own version; the read-side view returned to
  # readers is the upcast one (an older projection gains its explicit unrecorded baseline).
  # Append validates the current version and returns the exact map.
  defp validate_event(event, mode) do
    with {:ok, parsed} <- validate_envelope(event),
         {:ok, _data} <- EventData.parse(parsed, mode) do
      EventData.upcast(parsed)
    end
  end

  defp validate_envelope(event) do
    with :ok <- validate_required(event),
         :ok <- validate_schema_version(event),
         :ok <- validate_chain_field(event),
         :ok <- validate_type(event),
         {:ok, parsed} <- Zoi.parse(envelope_schema(), event) do
      {:ok, parsed}
    else
      {:error, %{clause: _clause} = rejection} -> {:error, rejection}
      {:error, _errors} -> {:error, %{clause: "invalid_event_shape"}}
    end
  end

  defp validate_appendable(%{"type" => type}) do
    if reserved?(type) do
      {:error, %{clause: "reserved_event_type", event_type: type}}
    else
      :ok
    end
  end
end
