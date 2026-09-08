defmodule AiOrchestrator.Protocol.Envelope do
  @moduledoc false

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  @agent_name_regex ~r/^[a-z][a-z0-9_-]{0,31}$/
  @kinds ["note", "ask", "answer", "status", "handoff", "consultation"]

  @spec validate(map()) :: {:ok, map()} | {:error, rejection()}
  def validate(envelope) when is_map(envelope) do
    with :ok <- validate_schema_version(envelope),
         :ok <- validate_peer_objects(envelope),
         :ok <- validate_kind(envelope),
         :ok <- validate_context_revision_pair(envelope),
         {:ok, parsed} <- Zoi.parse(schema(), envelope) do
      {:ok, parsed}
    else
      {:error, %{} = rejection} -> {:error, rejection}
      {:error, _errors} -> {:error, %{clause: "invalid_envelope_shape"}}
    end
  end

  def validate(_envelope), do: {:error, %{clause: "invalid_envelope_shape"}}

  defp schema do
    Zoi.map(
      %{
        "schema_version" => Zoi.literal("1.0"),
        "msg_id" => Zoi.string(),
        "ts" => Zoi.string(),
        "from" => peer_schema(),
        "to" => peer_schema(),
        "kind" => Zoi.enum(@kinds),
        "subject" => Zoi.string(),
        "body" => Zoi.string(),
        "in_reply_to" => Zoi.optional(Zoi.string()),
        "trace" => Zoi.optional(trace_schema()),
        "context" => Zoi.optional(context_schema())
      },
      unrecognized_keys: :error
    )
  end

  defp peer_schema do
    Zoi.map(
      %{
        "agent" => Zoi.string(),
        "pane_ref" => Zoi.optional(Zoi.string()),
        "pane_id" => Zoi.optional(Zoi.string())
      },
      unrecognized_keys: :error
    )
  end

  defp trace_schema do
    Zoi.map(
      %{"traceparent" => Zoi.string()},
      unrecognized_keys: :preserve
    )
  end

  defp context_schema do
    Zoi.map(
      %{
        "project" => Zoi.optional(Zoi.string()),
        "run_id" => Zoi.optional(Zoi.string()),
        "work_item_id" => Zoi.optional(Zoi.string()),
        "assignment_id" => Zoi.optional(Zoi.string()),
        "gate_run_id" => Zoi.optional(Zoi.string()),
        "review_id" => Zoi.optional(Zoi.string()),
        "proposal_id" => Zoi.optional(Zoi.string()),
        "contract_change_id" => Zoi.optional(Zoi.string()),
        "attention_id" => Zoi.optional(Zoi.string()),
        "journal_event_id" => Zoi.optional(Zoi.string()),
        "context_revision" => Zoi.optional(Zoi.integer()),
        "context_hash" => Zoi.optional(Zoi.string())
      },
      unrecognized_keys: :preserve
    )
  end

  defp validate_schema_version(%{"schema_version" => "1.0"}), do: :ok
  defp validate_schema_version(%{"schema_version" => _version}), do: {:error, %{clause: "unsupported_schema_version"}}
  defp validate_schema_version(_envelope), do: {:error, %{clause: "invalid_envelope_shape"}}

  defp validate_peer_objects(envelope) do
    with :ok <- validate_peer_object(envelope, "from") do
      validate_peer_object(envelope, "to")
    end
  end

  defp validate_peer_object(envelope, field) do
    case Map.fetch(envelope, field) do
      {:ok, %{"agent" => agent} = peer} when is_binary(agent) ->
        validate_peer_agent(peer, field)

      {:ok, _not_object} ->
        {:error, %{clause: "peer_not_object", field: field}}

      :error ->
        {:error, %{clause: "invalid_envelope_shape"}}
    end
  end

  defp validate_peer_agent(%{"agent" => agent}, _field) do
    if Regex.match?(@agent_name_regex, agent) do
      :ok
    else
      {:error, %{clause: "invalid_envelope_shape"}}
    end
  end

  defp validate_kind(%{"kind" => kind}) when kind in @kinds, do: :ok
  defp validate_kind(%{"kind" => kind}), do: {:error, %{clause: "unknown_envelope_kind", kind: kind}}
  defp validate_kind(_envelope), do: {:error, %{clause: "invalid_envelope_shape"}}

  defp validate_context_revision_pair(%{"context" => context}) when is_map(context) do
    has_revision? = Map.has_key?(context, "context_revision")
    has_hash? = Map.has_key?(context, "context_hash")

    if has_revision? == has_hash? do
      :ok
    else
      {:error, %{clause: "context_revision_without_hash"}}
    end
  end

  defp validate_context_revision_pair(%{"context" => _context}), do: {:error, %{clause: "invalid_envelope_shape"}}
  defp validate_context_revision_pair(_envelope), do: :ok
end
