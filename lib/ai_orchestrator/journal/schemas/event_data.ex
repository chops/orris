defmodule AiOrchestrator.Journal.Schemas.EventData do
  @moduledoc """
  Strict, versioned payload schemas for journal events.

  Parsing produces a read-side struct. Journal writers validate the original
  string-keyed event map and persist that map without encoding this struct.
  """

  alias AiOrchestrator.Journal.Schemas.RequestedBy

  defstruct [:type, :event_version, :data]

  @type t :: %__MODULE__{type: String.t(), event_version: pos_integer(), data: map()}
  # :read validates a line as written (older versions admitted); :append requires the
  # current version; :view validates the in-memory upgraded view the reducer folds -- the
  # current version, plus the read-side shapes a line may never carry.
  @type mode :: :read | :append | :view

  @hash_pattern ~r/^sha256:[0-9a-f]{64}$/
  @schema_cache_version :crypto.hash(
                          :sha256,
                          [File.read!(__ENV__.file), File.read!(Path.join(__DIR__, "requested_by.ex"))]
                        )

  @definitions %{
    "agent_wedge_detected" =>
      {~w(agent_ref pane_ref assignment_id detector stale_for_ms pane_state pending_count), ~w(reason)},
    "artifact_observed" =>
      {~w(assignment_id artifact_id path match_kind bytes sha256 stable_for_ms modified_after_dispatch), []},
    "assignment_completed" => {~w(assignment_id artifact_id pane_state), []},
    "assignment_dispatch_sent" =>
      {~w(assignment_id backend pane_ref send_status send_message_id replayed), ~w(artifact_baseline prompt_hash)},
    "assignment_failed" => {~w(assignment_id reason deadline_observed), []},
    "assignment_observation_started" => {~w(assignment_id deadline_unix), []},
    "assignment_prompt_projected" =>
      {~w(assignment_id prompt_path prompt_hash prompt_bytes context_revision context_hash expected_artifact), []},
    "assignment_requested" =>
      {~w(assignment_id work_item_id attempt role agent deadline_unix idempotency_key context_revision context_hash),
       ~w(review_id)},
    "context_patch_accepted" =>
      {~w(proposal_id accepted_by prior_context_revision new_context_revision new_context_hash), []},
    "context_patch_proposed" => {~w(proposal_id proposer target patch_kind patch_path patch_hash), []},
    "contract_change_proposed" => {~w(contract_change_id proposer affected_contract breaking patch_path patch_hash), []},
    "contract_change_ratified" =>
      {~w(contract_change_id ratified_by prior_context_revision new_context_revision new_context_hash), []},
    "gate_failed" => {~w(gate_run_id exit_status duration_ms stdout_hash failure_summary), ~w(stderr_hash stderr_merged)},
    "gate_passed" => {~w(gate_run_id exit_status duration_ms stdout_hash), ~w(stderr_hash stderr_merged)},
    "gate_requested" => {~w(gate_run_id work_item_id assignment_id gate_id command_argv artifact_ids timeout_s), []},
    "gate_started" => {~w(gate_run_id command_argv stdout_path stderr_path), []},
    "human_attention_required" =>
      {~w(attention_id reason blocking_entity summary_path summary_hash resume_command), ~w(detail)},
    "notification_failed" => {~w(notification_id channel reason), ~w(detail)},
    "notification_requested" => {~w(notification_id trigger_event_id channel payload_hash), []},
    "notification_sent" => {~w(notification_id channel), []},
    "pane_lease_acquired" => {~w(lease_request_id pane_ref owner_instance), ~w(claim_token)},
    "pane_lease_release_requested" => {~w(release_request_id pane_ref reason), []},
    "pane_lease_released" => {~w(release_request_id pane_ref), []},
    "pane_lease_requested" => {~w(lease_request_id assignment_id pane_ref owner_instance stale_after_ms), []},
    "plan_recorded" =>
      {~w(plan_id plan_path plan_hash work_item_ids dag_edges context_initial_hash context_revision), []},
    "review_disposition_recorded" => {~w(review_id disposition), []},
    "review_received" => {~w(review_id reviewer_assignment_id review_artifact_id verdict review_hash), ~w(finding_count)},
    "review_requested" =>
      {~w(review_id work_item_id subject_assignment_id reviewer_agent context_revision context_hash), []},
    "run_budget_exhausted" => {~w(budget_kind limit consumed blocking_entity), []},
    "run_cancel_requested" => {~w(reason), ~w(supervisor_instance last_seen_seq run_lock_path requested_by tail_repair)},
    "run_cancelled" => {~w(reason), ~w(released_lease_ids supervisor_instance last_seen_seq run_lock_path)},
    "run_completed" => {~w(completed_work_item_ids), []},
    "run_created" => {~w(project repo_root run_dir operator spec_path spec_hash), ~w(requested_by)},
    "run_failed" => {~w(reason diagnostic_path diagnostic_hash), []},
    "run_recovery_reserved" => {~w(attempt limit cause_class lost_generation authority), []},
    "run_resumed" =>
      {~w(supervisor_instance last_seen_seq recovery_reason),
       ~w(run_lock_path run_environment resolves_attention_ids tail_repair requested_by)},
    "run_spec_loaded" => {~w(spec_path spec_hash), ~w(agent_roster_hash)},
    "run_started" => {~w(supervisor_instance resume last_seen_seq), ~w(run_lock_path run_environment)},
    "stop_policy_evaluated" => {~w(policy_id scope reason decision), ~w(counters)},
    "work_item_completed" =>
      {~w(work_item_id completing_assignment_id required_gate_run_ids accepted_artifact_ids context_revision), []},
    "work_item_retry_scheduled" => {~w(work_item_id prior_assignment_id next_attempt reason remaining_attempts), []},
    "workspace_lease_acquired" => {~w(workspace_lease_id mode), []},
    "workspace_lease_release_requested" => {~w(release_request_id workspace_lease_id reason), []},
    "workspace_lease_released" => {~w(release_request_id workspace_lease_id), []},
    "workspace_lease_requested" => {~w(workspace_lease_id assignment_id mode allowed_paths), []}
  }

  @typed_types @definitions |> Map.keys() |> MapSet.new()

  # Per-type event versions (MUST-7, ruling m_1788571429536242416_dca73dd4). A type absent
  # here is at version 1. Read mode admits every version up to the current one; append
  # mode requires the current one; a version beyond the current one is refused by name.
  # The read-side view of an older version is upcast in memory; the line is never rewritten.
  @versions %{"assignment_prompt_projected" => 2, "gate_started" => 2, "gate_failed" => 2}
  @projection "assignment_prompt_projected"
  @unrecorded_view %{"status" => "unrecorded"}

  @spec typed_types() :: MapSet.t(String.t())
  def typed_types, do: @typed_types

  @spec current_version(String.t()) :: pos_integer() | nil
  def current_version(type) when is_binary(type) do
    if MapSet.member?(@typed_types, type), do: Map.get(@versions, type, 1)
  end

  @doc "Every version of a type that may appear on a journal line, oldest first."
  @spec known_versions(String.t()) :: [pos_integer()]
  def known_versions(type) when is_binary(type) do
    case current_version(type) do
      nil -> []
      current -> Enum.to_list(1..current)
    end
  end

  @spec schema(mode()) :: Zoi.schema()
  def schema(mode \\ :read) when mode in [:read, :append, :view] do
    cached_schema({:payload_union, mode}, fn -> Zoi.union(variants(mode, %{})) end)
  end

  # One variant per (type, version) the mode admits: append sees only current versions,
  # read sees every known one.
  defp variants(mode, extra_fields) do
    for type <- @definitions |> Map.keys() |> Enum.sort(),
        version <- admitted_versions(type, mode),
        do: event_variant(type, version, mode, extra_fields)
  end

  defp admitted_versions(type, :append), do: [current_version(type)]
  defp admitted_versions(type, :view), do: [current_version(type)]
  defp admitted_versions(type, :read), do: known_versions(type)

  @doc "Returns the public schema for complete chained journal envelopes."
  @spec envelope_schema() :: Zoi.schema()
  def envelope_schema do
    cached_schema(:public_envelope, fn ->
      envelope_fields = %{
        "schema" => Zoi.literal("ai-orchestrator/journal-event"),
        "schema_version" => Zoi.literal(2),
        "seq" => positive_integer(),
        "event_id" => nonempty_string(),
        "ts" => nonempty_string(),
        "run_id" => nonempty_string(),
        "actor" => Zoi.literal("run_supervisor"),
        "traceparent" => Zoi.optional(nonempty_string()),
        "prev_line_sha256" => hash()
      }

      Zoi.union(variants(:append, envelope_fields))
    end)
  end

  @spec parse(map(), mode()) :: {:ok, t()} | {:error, map()}
  def parse(event, mode \\ :read)

  def parse(%{"type" => type, "event_version" => version, "data" => _data} = event, mode)
      when is_binary(type) and is_integer(version) and mode in [:read, :append, :view] do
    with :ok <- ensure_schema_available(type),
         :ok <- ensure_known_version(type, version, mode),
         :ok <- ensure_append_authorship(event, mode),
         {:ok, parsed} <- Zoi.parse(event_schema(type, version, mode), Map.take(event, ["type", "event_version", "data"])),
         :ok <- ensure_append_authorship_bounds(parsed, mode) do
      {:ok, %__MODULE__{type: parsed["type"], event_version: parsed["event_version"], data: parsed["data"]}}
    else
      {:error, %{clause: _clause} = rejection} ->
        {:error, rejection}

      {:error, errors} ->
        rejection = %{
          clause: "invalid_event_data",
          event_type: type,
          event_version: version,
          errors: normalize_errors(errors)
        }

        {:error, maybe_name_incomplete_provenance(rejection, type, errors, mode)}
    end
  end

  def parse(_event, _mode), do: {:error, %{clause: "invalid_event_data"}}

  @spec upcast(map()) :: {:ok, map()} | {:error, map()}
  def upcast(%{"type" => type, "event_version" => version, "data" => data} = event)
      when is_binary(type) and is_integer(version) and is_map(data) do
    with :ok <- ensure_schema_available(type),
         :ok <- ensure_known_version(type, version, :read) do
      {:ok, upcast_view(type, version, event)}
    end
  end

  def upcast(_event), do: {:error, %{clause: "invalid_event_data"}}

  # The read-side view. A version-1 projection recorded no baseline: the view says so with
  # an explicit shape that no journal line may carry, so every reader matches one claim
  # instead of inventing an answer for a missing key.
  defp upcast_view(@projection, 1, event) do
    event
    |> Map.put("event_version", 2)
    |> Map.update!("data", &Map.put(&1, "artifact_baseline", %{"status" => "unrecorded"}))
  end

  # A version-1 gate start recorded no execution identity, attempt or deadline.
  defp upcast_view("gate_started", 1, event) do
    event
    |> Map.put("event_version", 2)
    |> Map.update!("data", &Map.put(&1, "execution", @unrecorded_view))
  end

  # A version-1 gate failure recorded its integer exit status and no termination.
  defp upcast_view("gate_failed", 1, event) do
    event
    |> Map.put("event_version", 2)
    |> Map.update!("data", &Map.put(&1, "termination", @unrecorded_view))
  end

  defp upcast_view(_type, _current, event), do: event

  @spec json_schema() :: map()
  def json_schema, do: Zoi.to_json_schema(envelope_schema())

  @doc false
  def artifact_baseline_schema(mode) when mode in [:read, :append, :view], do: field_schema("artifact_baseline", mode)

  @spec type_spec() :: Macro.t()
  def type_spec do
    view_schema =
      Zoi.struct(__MODULE__, %{
        type: @definitions |> Map.keys() |> Enum.sort() |> Zoi.enum(),
        event_version: Zoi.integer(),
        data: Zoi.map()
      })

    Zoi.type_spec(view_schema)
  end

  defp event_variant(type, version, mode, extra_fields) do
    Zoi.map(
      Map.merge(extra_fields, %{
        "type" => Zoi.literal(type),
        "event_version" => Zoi.literal(version),
        "data" => payload_schema(type, version, mode)
      }),
      unrecognized_keys: :error
    )
  end

  defp event_schema(type, version, mode),
    do: cached_schema({:event, type, version, mode}, fn -> event_variant(type, version, mode, %{}) end)

  defp cached_schema(name, build) do
    key = {__MODULE__, @schema_cache_version, name}

    case :persistent_term.get(key, nil) do
      nil ->
        schema = build.()
        :persistent_term.put(key, schema)
        schema

      schema ->
        schema
    end
  end

  # gate_passed (version 1) and gate_failed version 1: the stderr evidence union, exactly one of
  # stderr_hash / stderr_merged, as recorded since the first archived runs.
  defp payload_schema(type, 1, mode) when type in ["gate_passed", "gate_failed"] do
    {required, optional} = Map.fetch!(@definitions, type)
    result_optional = optional -- ["stderr_hash", "stderr_merged"]

    Zoi.union([
      strict_fields(required ++ ["stderr_hash"], result_optional, mode),
      strict_fields(required ++ ["stderr_merged"], result_optional, mode)
    ])
  end

  # gate_failed version 2 (D3): exit_status is the actual integer for a normal non-zero exit
  # and null otherwise, when a closed `termination` is required; the stderr evidence union is
  # preserved (at least one of stderr_hash / stderr_merged; both admitted, since an execution
  # with separate private files reports the hash and states that they were not merged).
  # The view of a version-1 line carries the integer plus {"status" => "unrecorded"}.
  defp payload_schema("gate_failed", 2, mode) do
    {required, _optional} = Map.fetch!(@definitions, "gate_failed")
    base = required -- ["exit_status"]
    stderr = [{["stderr_hash"], ["stderr_merged"]}, {["stderr_merged"], ["stderr_hash"]}]

    line_variants =
      for {evidence_required, evidence_optional} <- stderr,
          {ending_required, ending_fields} <- [
            {["exit_status"], %{"exit_status" => nonzero_exit_status()}},
            {["exit_status", "termination"], %{"exit_status" => Zoi.null()}}
          ] do
        strict_fields(base ++ evidence_required ++ ending_required, evidence_optional, mode, ending_fields)
      end

    view_variants =
      if mode == :view do
        for {evidence_required, evidence_optional} <- stderr do
          strict_fields(base ++ evidence_required ++ ["exit_status", "termination"], evidence_optional, :view, %{
            "termination" => unrecorded_view()
          })
        end
      else
        []
      end

    Zoi.union(line_variants ++ view_variants)
  end

  # gate_started version 2 (D1/D2): the execution identity and claim binding, the attempt and
  # the original absolute deadline. The view of a version-1 line carries {"status" =>
  # "unrecorded"} for the execution and no attempt or deadline: an unresolved start.
  defp payload_schema("gate_started", 2, mode) do
    {required, optional} = Map.fetch!(@definitions, "gate_started")

    recorded =
      strict_fields(required ++ ~w(attempt deadline_unix execution), optional, mode, %{"attempt" => gate_attempt()})

    if mode == :view do
      Zoi.union([
        recorded,
        strict_fields(required ++ ["execution"], optional, :view, %{"execution" => unrecorded_view()})
      ])
    else
      recorded
    end
  end

  defp payload_schema("run_cancelled", _version, mode) do
    Zoi.union([
      strict_fields(~w(reason released_lease_ids), [], mode),
      strict_fields(~w(reason supervisor_instance last_seen_seq), ~w(run_lock_path), mode)
    ])
  end

  # run_recovery_reserved version 1 (docs/contracts/recovery-reservation.org): the ratified domains, never the
  # generic validators - attempt >= 1, limit >= 0, the lost lock generation in the run.lock.N filename domain,
  # closed cause and authority enums; integers only (a float or boolean is not an integer)
  defp payload_schema("run_recovery_reserved", 1, mode) do
    {required, optional} = Map.fetch!(@definitions, "run_recovery_reserved")

    strict_fields(required, optional, mode, %{
      "attempt" => positive_integer(),
      "limit" => nonnegative_integer(),
      "lost_generation" => Zoi.integer() |> Zoi.min(1) |> Zoi.max(999_999_999_999),
      "cause_class" => Zoi.enum(~w(writer_exit writer_timeout owner_down)),
      "authority" => Zoi.enum(~w(executor host))
    })
  end

  defp payload_schema(type, version, mode) do
    {required, optional} = definition(type, version)
    strict_fields(required, optional, mode)
  end

  # The projection at version 2 records the artifact baseline it took before the paste;
  # at version 1 it recorded none. Every other type has one shape at version 1.
  defp definition(@projection, 2) do
    {required, optional} = Map.fetch!(@definitions, @projection)
    {required ++ ["artifact_baseline"], optional}
  end

  defp definition(type, _version), do: Map.fetch!(@definitions, type)

  defp strict_fields(required, optional, mode, overrides \\ %{}) do
    required_fields = Map.new(required, &{&1, Map.get(overrides, &1, field_schema(&1, mode))})
    optional_fields = Map.new(optional, &{&1, Zoi.optional(field_schema(&1, mode))})
    Zoi.map(Map.merge(required_fields, optional_fields), unrecognized_keys: :error)
  end

  defp unrecorded_view, do: Zoi.map(%{"status" => Zoi.literal("unrecorded")}, unrecognized_keys: :error)

  # the reviewed v2 domains (docs/contracts/gate-execution-claim.org), never the generic validators:
  # an attempt is exactly 1 or 2; a normal exit that failed is 1..255; a pid or pgid fits pid_t;
  # a kernel start identity is the guardian's grammar (sec.usec or ticks:N); leftovers is a
  # canonical non-negative count or the literal unknown
  # Numeric domains export as integer + minimum/maximum (a numeric enum exports as a string enum
  # no value can satisfy). String domains use an ECMA-portable strict end, `^...(?![\s\S])`: the
  # public export compiles under Draft 2020-12 engines, and a trailing newline, NUL or any other
  # suffix is refused exactly as `\A...\z` would, at runtime and in the export alike.
  defp gate_attempt, do: Zoi.integer() |> Zoi.min(1) |> Zoi.max(2)
  defp nonzero_exit_status, do: Zoi.integer() |> Zoi.min(1) |> Zoi.max(255)
  defp pid_domain, do: Zoi.integer() |> Zoi.min(1) |> Zoi.max(2_147_483_647)
  defp kernel_start, do: Zoi.regex(Zoi.string(), ~r/^(?:[0-9]{1,20}\.[0-9]{6}|ticks:[0-9]{1,20})(?![\s\S])/)
  defp leftovers, do: Zoi.regex(Zoi.string(), ~r/^(?:0|[1-9][0-9]{0,9}|unknown)(?![\s\S])/)
  # the claim binding is a byte-exact digest of this start's canonical document, not a historical hash
  defp claim_binding, do: Zoi.regex(Zoi.string(), ~r/^sha256:[0-9a-f]{64}(?![\s\S])/)

  # gate_started v2: the worker's kernel identity and the claim binding (contract
  # docs/contracts/gate-execution-claim.org); a line never carries the unrecorded view.
  defp recorded_execution do
    Zoi.map(
      %{"pid" => pid_domain(), "pgid" => pid_domain(), "start" => kernel_start(), "claim_hash" => claim_binding()},
      unrecognized_keys: :error
    )
  end

  # gate_failed v2: a closed termination; `signal` present exactly when kind is "signal".
  defp termination do
    common = %{
      "settled" => Zoi.boolean(),
      "leftovers" => leftovers(),
      "proof" => Zoi.enum(["gone", "alive", "unknown"])
    }

    Zoi.union([
      Zoi.map(Map.put(common, "kind", Zoi.enum(["timeout", "recovery"])), unrecognized_keys: :error),
      Zoi.map(Map.merge(common, %{"kind" => Zoi.literal("signal"), "signal" => positive_integer()}),
        unrecognized_keys: :error
      )
    ])
  end

  defp field_schema(field, _mode) when field in ~w(
              bytes prompt_bytes stable_for_ms stale_for_ms stale_after_ms pending_count attempt deadline_unix context_revision
              new_context_revision prior_context_revision exit_status duration_ms timeout_s finding_count limit consumed
              last_seen_seq next_attempt remaining_attempts
            ), do: nonnegative_integer()

  defp field_schema(field, _mode) when field in ~w(
              spec_hash agent_roster_hash plan_hash context_initial_hash context_hash new_context_hash patch_hash prompt_hash
              payload_hash
              sha256 stdout_hash stderr_hash summary_hash diagnostic_hash review_hash
            ), do: hash()

  defp field_schema(field, _mode) when field in ~w(
              modified_after_dispatch replayed deadline_observed breaking resume stderr_merged
            ), do: Zoi.boolean()

  defp field_schema(field, _mode) when field in ~w(
              work_item_ids artifact_ids completed_work_item_ids released_lease_ids resolves_attention_ids
              required_gate_run_ids accepted_artifact_ids allowed_paths command_argv
            ), do: Zoi.array(nonempty_string())

  defp field_schema("dag_edges", _mode) do
    edge = Zoi.map(%{"item" => nonempty_string(), "depends_on" => nonempty_string()}, unrecognized_keys: :error)
    Zoi.array(edge)
  end

  # The recorded shapes are what a line may carry and a producer may append. The upgraded
  # view of a version-1 projection carries {"status" => "unrecorded"} -- an explicit claim
  # that the journal does not know -- which only the :view mode admits: a line on disk may
  # never impersonate the in-memory upcast.
  defp field_schema("artifact_baseline", :view) do
    Zoi.union([recorded_baseline(), Zoi.map(%{"status" => Zoi.literal("unrecorded")}, unrecognized_keys: :error)])
  end

  defp field_schema("artifact_baseline", _line_mode), do: recorded_baseline()
  defp field_schema("execution", _mode), do: recorded_execution()
  defp field_schema("termination", _mode), do: termination()

  # Every other field's view grammar is its historical read grammar: a view is a validated
  # line plus the upcaster's additions, so what a line may carry in read mode (a structured
  # or literal `requested_by`, for example) a view must fold as well.
  defp field_schema(field, :view), do: field_schema(field, :read)

  defp field_schema("failure_summary", _mode) do
    line = Zoi.map(%{"line" => nonempty_string()}, unrecognized_keys: :error)
    assertion = Zoi.map(%{"test" => nonempty_string(), "assertion" => nonempty_string()}, unrecognized_keys: :error)

    Zoi.map(
      %{
        "headline" => nonempty_string(),
        "failures" => Zoi.array(Zoi.union([line, assertion])),
        "suggestion" => nonempty_string()
      },
      unrecognized_keys: :error
    )
  end

  defp field_schema("requested_by", :read), do: Zoi.union([Zoi.literal("operator"), RequestedBy.schema()])

  defp field_schema("requested_by", :append), do: RequestedBy.schema()

  defp field_schema("run_environment", _mode) do
    Zoi.map(
      %{
        "agent" => nonempty_string(),
        "provider" => nonempty_string(),
        "cli_name" => nonempty_string(),
        "cli_version" => nonempty_string(),
        "interface_mode" => nonempty_string(),
        "permission_mode" => nonempty_string(),
        "auth_profile_key" => nonempty_string(),
        "executable_sha256" => hash()
      },
      unrecognized_keys: :error
    )
  end

  defp field_schema("tail_repair", _mode) do
    Zoi.map(
      %{
        "action" => Zoi.enum(~w(truncate_tail advance_receipt advance_and_truncate)),
        "truncated_bytes" => nonnegative_integer(),
        "receipt_seq_before" => nonnegative_integer(),
        "receipt_seq_after" => nonnegative_integer()
      },
      unrecognized_keys: :error
    )
  end

  # `detail` is shared by two types with two shapes. `notification_failed` records a hook's
  # free-text reason. `human_attention_required` records a prompt-store failure as a closed
  # map an alert rule can be written against: `error` routes, `stage` says which half of the
  # store was running, and the hash and size identify the render without reproducing it.
  # `prompt_path` is present only when the object already had a name -- a retention that
  # failed has published nothing, so inventing one would point an operator at a file that
  # does not exist. Every value is a string or an integer a reader chose; a nested term is
  # the escape hatch the next failure would use.
  defp field_schema("detail", _mode) do
    prompt_failure =
      Zoi.map(
        %{
          "error" => nonempty_string(),
          "stage" => Zoi.enum(~w(retain fetch)),
          "prompt_hash" => hash(),
          "prompt_bytes" => nonnegative_integer(),
          "prompt_path" => Zoi.optional(nonempty_string())
        },
        unrecognized_keys: :error
      )

    # D3: a preflight refusal names the capabilities the daemon lacks, and nothing else.
    preflight_refusal = Zoi.map(%{"missing_capabilities" => Zoi.array(nonempty_string())}, unrecognized_keys: :error)

    # MUST-7: a baseline the host could not take names the class and the stage, nothing else.
    snapshot_failure =
      Zoi.map(%{"error" => nonempty_string(), "stage" => Zoi.literal("snapshot")}, unrecognized_keys: :error)

    Zoi.union([nonempty_string(), prompt_failure, preflight_refusal, snapshot_failure, gate_execution_detail()])
  end

  # EJ-7: `ok` is a send this process performed and the daemon acknowledged, `queued` one the
  # daemon is still holding, `reconciled` an event rebuilt from a receipt rather than a send.
  defp field_schema("send_status", _mode), do: Zoi.enum(~w(ok queued reconciled))

  defp field_schema("counters", _mode), do: Zoi.map(Zoi.string(), nonnegative_integer(), [])
  defp field_schema(_field, _mode), do: nonempty_string()
  # The orchestrated gate route's attention detail (docs/contracts/gate-execution-wiring.org):
  # a closed set of the diagnostic, settlement, and reconcile facts the Host mapper produces;
  # every field optional, every value in its own small domain. No paths off the run, no raw terms.
  # The Host's closed diagnostic value domain (docs/contracts/gate-execution-wiring.org), pinned
  # again at the journal: every arm is an enum, a bounded grammar or a bounded integer.
  @gate_clauses ~w(claim_unpublished claim_conflict prepare_failed release_failed already_released settle_unproven guardian_gone deadline_expired deadline_unsupported clock_unavailable helper_missing invalid_request output_unreadable evidence_incomplete claim_unreadable claim_mismatch claim_unexpected probe_invalid ack_mismatch await_failed unknown invalid_return clock_skew)
  @gate_stages ~w(mkdir gates_dir open chmod write sync close link rm_temp dir_sync encode receipt list_dir lstat read)
  @gate_classes ~w(eio enospc enoent eacces eexist erofs other open_stdout open_stderr pipe fork chdir setsid ready_timeout identity usage protocol unknown)
  @gate_cleanup ~w(none removed removed_unsynced absent absent_unsynced cleanup_required foreign_final_untouched)
  @gate_durability ~w(removed removed_unsynced absent absent_unsynced)
  @liveness ~w(gone alive unknown)
  @leftovers ~r/\A(0|[1-9][0-9]{0,9}|unknown)\z/
  @residue_entry ~r"\Agates/(?!\.\.?\z)[A-Za-z0-9_.-]{1,200}\z"

  defp gate_execution_detail do
    settle =
      Zoi.map(
        %{
          "settled" => Zoi.optional(Zoi.boolean()),
          "leftovers" => Zoi.string() |> Zoi.regex(@leftovers) |> Zoi.optional(),
          "proof" => Zoi.optional(Zoi.enum(@liveness)),
          "reason" => Zoi.optional(Zoi.enum(~w(command parent_gone guardian_signaled release_failed deadline))),
          "clause" => Zoi.optional(Zoi.enum(~w(guardian_gone settle_unproven abandon_unsettled))),
          "worker" => Zoi.optional(Zoi.boolean())
        },
        unrecognized_keys: :error
      )

    evidence =
      Zoi.map(
        %{
          "stdout_hash" => Zoi.optional(hash()),
          "stderr_hash" => Zoi.optional(hash()),
          "duration_ms" => Zoi.optional(nonnegative_integer())
        },
        unrecognized_keys: :error
      )

    Zoi.map(
      %{
        "clause" => Zoi.optional(Zoi.enum(@gate_clauses)),
        "stage" => Zoi.optional(Zoi.enum(@gate_stages)),
        "class" => Zoi.optional(Zoi.enum(@gate_classes)),
        "cleanup" => Zoi.optional(Zoi.enum(@gate_cleanup)),
        "temp" => Zoi.optional(Zoi.enum(@gate_durability)),
        "final" => Zoi.optional(Zoi.enum(@gate_durability)),
        "outputs" => Zoi.optional(Zoi.enum(~w(removed left))),
        "field" => Zoi.string() |> Zoi.min(1) |> Zoi.max(64) |> Zoi.optional(),
        "kind" => Zoi.optional(Zoi.enum(~w(exited signaled timeout unknown))),
        "proof" => Zoi.optional(Zoi.enum(@liveness)),
        "leader" => Zoi.optional(Zoi.enum(@liveness)),
        "group" => Zoi.optional(Zoi.enum(@liveness)),
        "leader_pid" => Zoi.optional(Zoi.enum(~w(reused))),
        "start" => Zoi.string() |> Zoi.regex(~r/\A([0-9]{1,20}\.[0-9]{6}|-)\z/) |> Zoi.optional(),
        "leftovers" => Zoi.string() |> Zoi.regex(@leftovers) |> Zoi.optional(),
        "settled" => Zoi.optional(Zoi.boolean()),
        "missing" => Zoi.optional(Zoi.boolean()),
        "residue" => Zoi.string() |> Zoi.regex(@residue_entry) |> Zoi.array() |> Zoi.max(8) |> Zoi.optional(),
        "max_ms" => Zoi.optional(nonnegative_integer()),
        "exit_status" => Zoi.integer() |> Zoi.min(0) |> Zoi.max(255) |> Zoi.optional(),
        "signal" => Zoi.integer() |> Zoi.min(1) |> Zoi.max(64) |> Zoi.optional(),
        "duration_ms" => Zoi.optional(nonnegative_integer()),
        "members" => Zoi.optional(Zoi.union([nonnegative_integer(), Zoi.enum(~w(unknown))])),
        "recorded_start_unix" => Zoi.optional(nonnegative_integer()),
        "observed_now_unix" => Zoi.optional(nonnegative_integer()),
        "settle" => Zoi.optional(settle),
        "evidence" => Zoi.optional(evidence)
      },
      unrecognized_keys: :error
    )
  end

  defp recorded_baseline do
    Zoi.union([
      Zoi.map(%{"exists" => Zoi.literal(false)}, unrecognized_keys: :error),
      Zoi.map(
        %{
          "exists" => Zoi.literal(true),
          "bytes" => nonnegative_integer(),
          "mtime_unix" => nonnegative_integer(),
          "sha256" => baseline_hash()
        },
        unrecognized_keys: :error
      )
    ])
  end

  # MUST-7 M5: the baseline's hash is anchored absolutely (\A...\z), matching
  # Contract.ArtifactBaseline exactly; the shared hash() pattern's $ admits a trailing
  # newline, and a line grammar looser than the adapter's would be a drift a fixture could
  # not catch. Other hash fields keep their historical pattern on purpose.
  defp baseline_hash, do: Zoi.regex(Zoi.string(), ~r/\Asha256:[0-9a-f]{64}\z/)

  defp ensure_schema_available(type) do
    if MapSet.member?(@typed_types, type) do
      :ok
    else
      {:error, %{clause: "event_schema_unavailable", event_type: type}}
    end
  end

  defp ensure_known_version(type, version, mode) do
    admitted = admitted_versions(type, mode)

    if version in admitted do
      :ok
    else
      {:error, %{clause: "unsupported_event_version", event_type: type, event_version: version}}
    end
  end

  defp ensure_append_authorship(%{"data" => %{"requested_by" => value}, "type" => type}, :append) when is_binary(value) do
    {:error, %{clause: "requested_by_object_required", event_type: type}}
  end

  defp ensure_append_authorship(_event, _mode), do: :ok

  defp ensure_append_authorship_bounds(%{"type" => type, "data" => %{"requested_by" => requested_by}}, :append) do
    case RequestedBy.validate_append_bounds(requested_by) do
      :ok -> :ok
      {:error, rejection} -> {:error, Map.put(rejection, :event_type, type)}
    end
  end

  defp ensure_append_authorship_bounds(_event, _mode), do: :ok

  defp normalize_errors(errors) do
    Enum.map(errors, fn %Zoi.Error{code: code, path: path} ->
      %{"code" => Atom.to_string(code), "path" => path}
    end)
  end

  defp maybe_name_incomplete_provenance(rejection, type, errors, :read)
       when type in ["run_created", "run_spec_loaded", "plan_recorded"] do
    required_hashes = %{
      "run_created" => "spec_hash",
      "run_spec_loaded" => "spec_hash",
      "plan_recorded" => "plan_hash"
    }

    expected_path = ["data", Map.fetch!(required_hashes, type)]

    if Enum.any?(errors, &match?(%Zoi.Error{code: :required, path: ^expected_path}, &1)) do
      Map.put(rejection, :reason, "journal_provenance_incomplete")
    else
      rejection
    end
  end

  defp maybe_name_incomplete_provenance(rejection, _type, _errors, _mode), do: rejection

  defp hash, do: Zoi.regex(Zoi.string(), @hash_pattern)
  defp nonempty_string, do: Zoi.min(Zoi.string(), 1)
  defp nonnegative_integer, do: Zoi.non_negative(Zoi.integer())
  defp positive_integer, do: Zoi.positive(Zoi.integer())
end
