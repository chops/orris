defmodule AiOrchestrator.Telemetry.Handler do
  @moduledoc """
  The application-boundary handler (north-star-architecture.org:528, NS-26.F.000/.F.002): it attaches
  to the delivered lifecycle events and creates one local OpenTelemetry span per instrumented
  operation. It is the FIRST span producer in this product; before it, the SDK and the OTLP exporter
  ran as applications that domain code never reached
  (docs/contracts/command-lifecycle-telemetry.org).

  ## Parentage follows propagated identity, never proximity (NS-26.F.002)

  A span is parented ONLY when the event carries the identifier of a scope this handler opened IN
  THIS PROCESS and has not yet closed:

  | span domain | parent scope it looks up | scope it opens |
  |-------------|-------------------------|----------------|
  | `host`      | none (root)             | `{:run, run_id}` |
  | `run`       | `{:run, run_id}`        | `{:run, run_id}`, if none is open |
  | `assignment`| `{:run, run_id}`        | `{:assignment, assignment_id}` |
  | `dispatch`  | `{:assignment, assignment_id}` | none |
  | `gate`, `journal`, `projection`, `attention`, `recovery` | `{:run, run_id}` | none |
  | `commands`  | none (root)             | none |

  `host` opens the run scope and `run` looks it up because `Host.Executor.execute/2` ENCLOSES
  `Run.Executor.execute/2` in the same process, so the outer span is the one that can be a parent.
  On the direct route, where no host span ran, `run` finds nothing, becomes a root, and opens the
  scope itself.

  `commands` is a root and opens nothing, and that is a property of the event rather than a choice:
  `AiOrchestrator.Commands.Telemetry` reports `command_id_digest` on the TERMINAL event only,
  because at `:start` no command has been built yet. There is therefore no journaled identifier in
  existence at the instant a command scope would have to open, and the only thing left to key it on
  would be arrival order. The command and the run it ran join on `command_id_digest`, which both
  carry as an attribute, not on parentage.

  The ambient OpenTelemetry context is NEVER read: every span starts from an explicitly built
  `:otel_ctx`, empty for a root and carrying exactly the looked-up parent otherwise. That is what
  makes "temporal proximity cannot manufacture parentage" a property of the code rather than of the
  workload -- a span that starts while an unrelated scope is open in the same process at the same
  instant is a ROOT span, because its identifier names no open scope.

  ## What that measurably produces, and why most spans are roots

  The scope map lives in the emitting process's dictionary, so it is discarded with that process and
  cannot leak across runs. The consequence is stated rather than hidden, and
  `test/telemetry/span_production_test.exs` MEASURES it rather than assuming it: one full command
  path produces many more root spans than parented ones, because the run's effects, its journal
  appends and its gates all execute in the owned subtree's own processes while the `run` span is
  open in the CALLER's process. A parent lookup across that boundary finds nothing and answers a
  root.

  That is a limit of propagation, not a defect of the rule. This product propagates no trace context
  across process boundaries yet -- the envelope's `traceparent` field exists and has no producer.
  Stamping it is a separate slice with its own fixture cost, and inventing parentage from a
  BEAM-global registry instead would join spans by "same node, same time", which is exactly what the
  rule forbids. Until then the joins NS-26.F.002 names are carried as attributes on every span
  (`run_id`, `assignment_id`, `gate_run_id`, `command_id_digest`), which is the rule's own acceptance
  wording -- follow the identifiers -- and the parentage that does exist is a strict subset of it.

  ## It cannot change domain behaviour

  `:telemetry` runs handlers synchronously in the emitting process. This one takes an ETS-free path
  (a process-dictionary read, a span start, an attribute write) and catches every escape of its own
  body, so a failing tracer provider neither raises into the caller nor gets this handler detached by
  `:telemetry` -- which would silently disable telemetry for the whole VM. What it can still change
  is TIMING, exactly as the command boundary already documents, and
  `test/telemetry/exporter_isolation_test.exs` re-asserts NS-26.F.001 with this handler installed
  rather than with a test double.
  """

  alias AiOrchestrator.Telemetry.Events

  @default_id "ai-orchestrator-application-boundary-spans"

  # the closed attribute allowlist: the metadata keys of this product's own lifecycle events and of
  # `AiOrchestrator.Commands.Telemetry`. Nothing else is read off an event, whatever it carries.
  @attributes [
    :domain,
    :operation,
    :verb,
    :actor_class,
    :run_id,
    :assignment_id,
    :gate_run_id,
    :send_id,
    :attention_id,
    :command_id_digest,
    :outcome,
    :stage,
    :clause,
    :result_class,
    :kind,
    :class,
    :stack_depth
  ]

  # `{parent scope to look up, scope this domain opens}`. The parent is looked up BEFORE the own
  # scope is registered, so a domain may name the same scope in both positions: it nests inside an
  # already-open scope of that name and otherwise becomes one. `host` opens the run scope because it
  # encloses `run`; `gate` opens none because no domain nests inside a gate.
  @scopes %{
    host: {nil, {:run, :run_id}},
    run: {{:run, :run_id}, {:run, :run_id}},
    assignment: {{:run, :run_id}, {:assignment, :assignment_id}},
    gate: {{:run, :run_id}, nil},
    dispatch: {{:assignment, :assignment_id}, nil},
    journal: {{:run, :run_id}, nil},
    projection: {{:run, :run_id}, nil},
    recovery: {{:run, :run_id}, nil},
    attention: {{:run, :run_id}, nil},
    commands: {nil, nil}
  }

  @doc """
  Attaches the handler to every event of `AiOrchestrator.Telemetry.Events.events/0`.

  Options: `:id` (the `:telemetry` handler id, so more than one instance can coexist) and `:tracer`
  (an explicit `t:opentelemetry.tracer/0`; by default the global provider's tracer for this
  application, resolved once at attach time).
  """
  @spec attach(keyword()) :: {:ok, String.t()} | {:error, :already_exists}
  def attach(opts \\ []) when is_list(opts) do
    id = Keyword.get(opts, :id, @default_id)
    config = %{id: id, tracer: Keyword.get_lazy(opts, :tracer, fn -> :opentelemetry.get_tracer(:ai_orchestrator) end)}

    case :telemetry.attach_many(id, Events.events(), &__MODULE__.handle/4, config) do
      :ok -> {:ok, id}
      {:error, :already_exists} -> {:error, :already_exists}
    end
  end

  @doc "Detaches the handler attached under `id`."
  @spec detach(String.t()) :: :ok | {:error, :not_found}
  def detach(id \\ @default_id), do: :telemetry.detach(id)

  @doc "The default `:telemetry` handler id."
  @spec default_id() :: String.t()
  def default_id, do: @default_id

  @doc false
  @spec handle([atom()], map(), map(), map()) :: :ok
  def handle(event, _measurements, metadata, %{id: id, tracer: tracer}) do
    case phase(event) do
      {:start, domain, operation} -> open(id, tracer, domain, operation, metadata)
      {:terminal, domain, _operation} -> close(id, domain, metadata)
      {:point, domain, operation} -> instant(id, tracer, domain, operation, metadata)
      :unknown -> :ok
    end
  catch
    # a span the SDK refuses must not raise into the caller and must not get this handler detached
    _kind, _reason -> :ok
  end

  # `[:ai_orchestrator, domain, operation, phase]`, the command boundary's own four-segment span, and
  # `[:ai_orchestrator, domain, operation]` for a point event
  defp phase([:ai_orchestrator, domain, operation, :start]), do: {:start, domain, operation}
  defp phase([:ai_orchestrator, domain, operation, :stop]), do: {:terminal, domain, operation}
  defp phase([:ai_orchestrator, domain, operation, :exception]), do: {:terminal, domain, operation}
  defp phase([:ai_orchestrator, domain, operation]), do: {:point, domain, operation}
  defp phase(_event), do: :unknown

  defp open(id, tracer, domain, operation, metadata) do
    ref = span_ref(metadata)
    {parent, own} = Map.get(@scopes, domain, {nil, nil})
    state = state(id)

    span = start_span(tracer, state, parent, domain, operation, metadata)
    own_key = scope_key(own, metadata)

    # an inner operation reusing an identifier never displaces the scope already open for it
    scopes =
      if own_key && not Map.has_key?(state.scopes, own_key),
        do: Map.put(state.scopes, own_key, span),
        else: state.scopes

    put_state(id, %{spans: Map.put(state.spans, ref, {span, own_key}), scopes: scopes})
  end

  defp close(id, _domain, metadata) do
    ref = span_ref(metadata)
    state = state(id)

    case Map.pop(state.spans, ref) do
      {nil, _spans} ->
        :ok

      {{span, own_key}, spans} ->
        :otel_span.set_attributes(span, attributes(metadata))
        :otel_span.end_span(span)

        scopes =
          if own_key && Map.get(state.scopes, own_key) == span,
            do: Map.delete(state.scopes, own_key),
            else: state.scopes

        put_state(id, %{spans: spans, scopes: scopes})
    end
  end

  # a point event has no duration: the span it produces opens and ends at the same instant, under the
  # same parentage rule as every other span
  defp instant(id, tracer, domain, operation, metadata) do
    {parent, _own} = Map.get(@scopes, domain, {nil, nil})
    span = start_span(tracer, state(id), parent, domain, operation, metadata)
    :otel_span.end_span(span)
    :ok
  end

  defp start_span(tracer, state, parent, domain, operation, metadata) do
    name = :"ai_orchestrator.#{domain}.#{operation}"
    opts = %{attributes: attributes(metadata), kind: :internal}
    :otel_tracer.start_span(parent_ctx(state, parent, metadata), tracer, name, opts)
  end

  # the ONLY way a span acquires a parent: an identifier on this event names a scope this handler
  # opened in this process. `:otel_ctx.new/0` is an EMPTY context, so the alternative is a root span
  # and never the ambient one.
  defp parent_ctx(state, parent, metadata) do
    case scope_key(parent, metadata) do
      nil -> :otel_ctx.new()
      key -> ctx_for(state.scopes, key)
    end
  end

  defp ctx_for(scopes, key) do
    case Map.get(scopes, key) do
      nil -> :otel_ctx.new()
      span -> :otel_tracer.set_current_span(:otel_ctx.new(), span)
    end
  end

  defp scope_key(nil, _metadata), do: nil

  defp scope_key({scope, key}, metadata) do
    case Map.get(metadata, key) do
      value when is_binary(value) -> {scope, value}
      _absent -> nil
    end
  end

  # the command boundary correlates on `invocation_ref`; this boundary's own events on `span_ref`
  defp span_ref(%{span_ref: ref}), do: ref
  defp span_ref(%{invocation_ref: ref}), do: ref
  defp span_ref(_metadata), do: nil

  defp attributes(metadata) do
    for key <- @attributes, value = Map.get(metadata, key), attribute?(value), into: %{} do
      {:"ai_orchestrator.#{key}", value}
    end
  end

  defp attribute?(value), do: is_binary(value) or is_atom(value) or is_integer(value)

  defp state(id), do: Process.get({__MODULE__, id}, %{spans: %{}, scopes: %{}})

  defp put_state(id, state) do
    _previous = Process.put({__MODULE__, id}, state)
    :ok
  end
end
