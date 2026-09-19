defmodule AiOrchestrator.Telemetry.Events do
  @moduledoc """
  The emit vocabulary for the lifecycle domains architecture:526-527 names
  (docs/contracts/lifecycle-telemetry.org).

  `span/4` wraps one operation in exactly one `[:ai_orchestrator, domain, operation, :start]` and
  then exactly one `:stop` (normal return) or `:exception` (trappable raise, throw or exit),
  correlated by a fresh `span_ref`. `emit/3` records a point event that has no duration.

  Domain behaviour is untouched, by construction and for the same reasons as
  `AiOrchestrator.Commands.Telemetry`: the wrapped function's term passes through unchanged, and an
  escape is re-raised with `:erlang.raise/3` carrying its original kind, reason and stacktrace. A
  `{domain, operation}` pair outside the closed table below emits NOTHING and still runs the
  function, so a mislabelled call site can never fail a run.

  Metadata is a closed allowlist. The domain and operation come from this module's own table; the
  identity keys are the journaled correlation identifiers NS-26.F.002 names (`run_id`,
  `assignment_id`, `gate_run_id`, `send_id`, `attention_id`) plus a bounded digest of a command id,
  and each value is admitted only if it matches `#{inspect(~r/\\A[A-Za-z0-9_.:@-]{1,128}\\z/)}` --
  a value outside that shape is DROPPED, never copied. The terminal event adds an outcome word from
  a closed four-value vocabulary and the closed `Contract.Diagnostic.result_class/1` of the returned
  term. Arguments, options, paths, raw payloads, rejection maps, observation structs, exception
  terms and stack frames are never emitted.

  `:telemetry` runs handlers synchronously in the calling process, with no timeout and no
  supervision, so the statement this boundary carries is the one the command boundary already
  carries: an observability failure changes no FACT, never "a handler has no effect". Every emit
  site below was chosen to run in the process that already owns the work, so that a handler's
  timing cost is charged to a caller that is already waiting rather than to a `GenServer` whose call
  deadline someone else is holding (docs/contracts/lifecycle-telemetry.org, "Where the events are
  emitted").
  """

  alias AiOrchestrator.Contract.Diagnostic

  @prefix :ai_orchestrator

  # The command boundary's own span, produced by `AiOrchestrator.Commands.Telemetry` and NOT by this
  # module. It is listed so the handler can consume it; `test/telemetry/span_production_test.exs`
  # fails if this literal ever stops naming the events that module really emits.
  @command_span [:ai_orchestrator, :commands, :invoke]

  # The closed domain/operation table. Every name here is a lifecycle domain of
  # north-star-architecture.org:526-527. `Effect.Clock` and `Effect.Timer` are deliberately absent:
  # a clock read is not one of the named domains, and instrumenting it would put a telemetry event
  # inside every tick of the reducer's clock seam.
  @operations [
    {:host, :mount},
    {:run, :execute},
    {:assignment, :dispatch},
    {:assignment, :observe},
    {:assignment, :reconcile_send},
    {:assignment, :snapshot},
    {:assignment, :retain_prompt},
    {:assignment, :fetch_prompt},
    {:assignment, :read_review},
    {:dispatch, :deliver},
    {:dispatch, :observe},
    {:dispatch, :reconcile},
    {:dispatch, :snapshot},
    {:gate, :prepare},
    {:gate, :release},
    {:gate, :await},
    {:gate, :reconcile},
    {:gate, :run},
    {:journal, :append},
    {:projection, :run_summary},
    {:projection, :run_context}
  ]

  # Point events: an instant the lifecycle records, with no duration this boundary can measure.
  #
  # `attention` is decided by the reducer and recorded by the journal, and the moment it becomes a
  # durable fact is the moment worth observing. `recovery` is a point event for a measured reason,
  # not a convenience: the ONE recovery the product performs on its own is the writer's bounded tail
  # repair, and that repair runs inside `Journal.Writer.init/1` before any run holds the writer, so
  # the first code that can observe it observes a completed fact. See
  # docs/contracts/lifecycle-telemetry.org for the recovery paths deliberately left uninstrumented.
  @points [{:attention, :raised}, {:recovery, :repaired}]

  @identity_keys [:run_id, :assignment_id, :gate_run_id, :send_id, :attention_id, :command_id_digest]
  @identity ~r/\A[A-Za-z0-9_.:@-]{1,128}\z/
  @outcomes [:ok, :error, :refused, :other]

  @type identity :: %{optional(atom()) => term()}

  @doc "Every `{domain, operation}` pair this module can emit a span for."
  @spec operations() :: [{atom(), atom()}]
  def operations, do: @operations

  @doc "Every `{domain, operation}` pair this module can emit a point event for."
  @spec points() :: [{atom(), atom()}]
  def points, do: @points

  @doc "The closed outcome vocabulary a terminal span event can report."
  @spec outcomes() :: [atom()]
  def outcomes, do: @outcomes

  @doc "The closed identity key allowlist; no other key ever reaches metadata."
  @spec identity_keys() :: [atom()]
  def identity_keys, do: @identity_keys

  @doc """
  Every event name a consumer can attach to: the command boundary's span, this module's spans, and
  its point events.
  """
  @spec events() :: [[atom()]]
  def events, do: command_events() ++ span_events() ++ point_events()

  @doc "The three event names `AiOrchestrator.Commands.Telemetry` emits."
  @spec command_events() :: [[atom()]]
  def command_events, do: for(phase <- [:start, :stop, :exception], do: @command_span ++ [phase])

  @doc "The three event names each `{domain, operation}` span uses."
  @spec span_events() :: [[atom()]]
  def span_events do
    for {domain, operation} <- @operations, phase <- [:start, :stop, :exception] do
      [@prefix, domain, operation, phase]
    end
  end

  @doc "The point event names."
  @spec point_events() :: [[atom()]]
  def point_events, do: for({domain, operation} <- @points, do: [@prefix, domain, operation])

  @doc """
  Runs `fun` inside one start / stop-or-exception span for `{domain, operation}`, answering its term
  unchanged. A pair outside `operations/0` emits nothing and still runs `fun`.
  """
  @spec span(atom(), atom(), identity(), (-> result)) :: result when result: term()
  def span(domain, operation, identity, fun) when is_function(fun, 0) do
    if {domain, operation} in @operations,
      do: instrumented(domain, operation, identity, fun),
      else: fun.()
  end

  @doc """
  Records one point event for `{domain, operation}`. A pair outside `points/0` records nothing.
  Answers `:ok` in both cases, so no call site can branch on whether it was observed.
  """
  @spec emit(atom(), atom(), identity()) :: :ok
  def emit(domain, operation, identity) do
    if {domain, operation} in @points do
      measurements = %{monotonic_time: System.monotonic_time(), system_time: System.system_time()}
      :telemetry.execute([@prefix, domain, operation], measurements, base(domain, operation, identity))
    end

    :ok
  end

  @doc """
  The bounded lookup aid for a command id: the first 16 hex characters of SHA-256 over it, never the
  id. `nil` for anything that is not a command id.
  """
  @spec command_id_digest(term()) :: String.t() | nil
  def command_id_digest(id) when is_binary(id),
    do: :sha256 |> :crypto.hash(id) |> Base.encode16(case: :lower) |> binary_part(0, 16)

  def command_id_digest(_id), do: nil

  defp instrumented(domain, operation, identity, fun) do
    start_mono = System.monotonic_time()
    base = base(domain, operation, identity)
    start = [@prefix, domain, operation, :start]
    :telemetry.execute(start, %{monotonic_time: start_mono, system_time: System.system_time()}, base)

    try do
      fun.()
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        terminal(domain, operation, :exception, start_mono, Map.merge(base, escape(kind, reason, stacktrace)))
        :erlang.raise(kind, reason, stacktrace)
    else
      result ->
        terminal(domain, operation, :stop, start_mono, Map.merge(base, classify(result)))
        result
    end
  end

  defp terminal(domain, operation, phase, start_mono, metadata) do
    now = System.monotonic_time()
    measurements = %{monotonic_time: now, duration: now - start_mono}
    :telemetry.execute([@prefix, domain, operation, phase], measurements, metadata)
  end

  defp base(domain, operation, identity),
    do: Map.merge(%{span_ref: make_ref(), domain: domain, operation: operation}, admitted(identity))

  # closed keys, closed shape: a key outside the allowlist and a value outside the identity shape are
  # both dropped, so nothing an adapter, a caller or a journal payload carries can widen this map
  defp admitted(identity) when is_map(identity) do
    for key <- @identity_keys, value = Map.get(identity, key), admissible?(value), into: %{}, do: {key, value}
  end

  defp admitted(_identity), do: %{}

  defp admissible?(value) when is_binary(value), do: String.valid?(value) and Regex.match?(@identity, value)
  defp admissible?(_value), do: false

  # the outcome word is this module's own, never the callee's: a struct answer is the admissible
  # answer of an effect and reads :ok, a closed refusal reads :refused, and anything else reads :other
  defp classify(:ok), do: outcome(:ok, :ok)
  defp classify({:ok, _value} = result), do: outcome(:ok, result)
  defp classify(:error), do: outcome(:error, :error)
  defp classify({:error, _reason} = result), do: outcome(:error, result)
  defp classify({:refused, _reason} = result), do: outcome(:refused, result)
  defp classify(%_struct{} = result), do: outcome(:ok, result)
  defp classify(result), do: outcome(:other, result)

  defp outcome(word, result), do: %{outcome: word, result_class: Diagnostic.result_class(result)}

  defp escape(kind, reason, stacktrace),
    do: %{kind: kind, class: Diagnostic.result_class(reason), stack_depth: length(stacktrace)}
end
