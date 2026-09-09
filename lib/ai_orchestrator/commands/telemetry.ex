defmodule AiOrchestrator.Commands.Telemetry do
  @moduledoc """
  Native lifecycle telemetry for `AiOrchestrator.Commands.invoke/4`
  (docs/contracts/command-lifecycle-telemetry.org, the first NS-26 slice).

  One invocation emits exactly one `[:ai_orchestrator, :commands, :invoke, :start]` and then exactly one
  `:stop` (normal return) or `:exception` (trappable raise, throw or exit), correlated by a fresh
  `invocation_ref`. Metadata is a closed allowlist: the verb and actor class from closed vocabularies, the
  outcome / stage / clause of the invocation from this module's own constants, the closed result class of an
  invalid executor reply, the kind / class / stack depth of an escape, and a bounded digest of the command id
  once a command was built. Arguments, options, raw identities, executor replies, rejection maps, exception
  terms and stack frames are never emitted.

  Domain behaviour is untouched: the caller's return term passes through unchanged, an escape is re-raised
  with `:erlang.raise/3` carrying its original kind, reason and stacktrace, and each stage function that is
  reached runs exactly once with no retry (a halted build stage never reaches the executor stage). Two
  sequential, lexical catch boundaries classify where an escape happened — the build stage (no command yet,
  digest `nil`) or the executor stage (digest from the command that was actually built) — without any
  process-global or mutable state, so every escape produces exactly one `:exception` event.

  `:telemetry` runs handlers synchronously in the calling process: a handler that raises is caught and
  detached by `:telemetry` and cannot change the command outcome; a handler that blocks blocks the
  invocation. Handlers are expected to be nonblocking; exporter isolation is a separate boundary. An abrupt
  external process death can prevent terminal emission: the absence of a `:stop` / `:exception` is not an outcome.
  """

  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Diagnostic

  @prefix [:ai_orchestrator, :commands, :invoke]
  @verbs ~w(start resume resolve_attention repair cancel pause update_context propose_plan propose_context_change ratify_plan)
  @actor_classes ~w(operator console agent system)
  @clauses ~w(invalid_command_options invalid_command_verb invalid_command_actor unknown_actor_class command_actor_fields command_actor_id command_actor_value command_not_authorized invalid_command_arguments unknown_command_verb command_argument_fields command_argument_value command_argument_hash repair_kind attention_ids run_id_required actor_run_id_mismatch invalid_command_id invalid_command_id_generator invalid_command_moment invalid_command_clock command_executor_required invalid_command_executor)

  @typedoc """
  The explicit origin carrier built by the caller at the real boundaries: outcome, stage, detail
  (the rejection map, the raw invalid reply, `:executor_rejected`, or `nil`) and the built command if any.
  """
  @type carrier ::
          {:accepted | :rejected | :invalid_executor_result, :build | :executor_port | :executor, term(),
           Command.t() | nil}

  @typedoc "What the build stage yields: a finished invocation, or the built command for the executor stage."
  @type built :: {:halt, term(), carrier()} | {:built, Command.t()}

  @doc """
  Runs one invocation inside a start / stop-or-exception span. `build_fun` runs first and yields either
  `{:halt, result, carrier}` (finished without a command) or `{:built, command}`; `executor_fun` then runs
  once with that command and yields `{result, carrier}`. The result term is returned unchanged.
  """
  @spec span(term(), term(), (-> built()), (Command.t() -> {term(), carrier()})) :: term()
  def span(verb, actor, build_fun, executor_fun) when is_function(build_fun, 0) and is_function(executor_fun, 1) do
    start_mono = System.monotonic_time()
    base = %{invocation_ref: make_ref(), verb: verb_label(verb), actor_class: actor_class(actor)}
    :telemetry.execute(@prefix ++ [:start], %{monotonic_time: start_mono, system_time: System.system_time()}, base)

    # build stage: a trappable escape here happened before any command existed
    built =
      try do
        build_fun.()
      catch
        kind, reason ->
          stacktrace = __STACKTRACE__
          emit(:exception, start_mono, Map.merge(base, exception_metadata(kind, reason, stacktrace, nil)))
          :erlang.raise(kind, reason, stacktrace)
      end

    case built do
      {:halt, result, carrier} ->
        emit(:stop, start_mono, Map.merge(base, stop_metadata(carrier)))
        result

      {:built, %Command{} = command} ->
        # executor stage: the escape is attributed to the command that was actually built
        try do
          executor_fun.(command)
        catch
          kind, reason ->
            stacktrace = __STACKTRACE__
            emit(:exception, start_mono, Map.merge(base, exception_metadata(kind, reason, stacktrace, command)))
            :erlang.raise(kind, reason, stacktrace)
        else
          {result, carrier} ->
            emit(:stop, start_mono, Map.merge(base, stop_metadata(carrier)))
            result
        end
    end
  end

  defp emit(event, start_mono, metadata) do
    now = System.monotonic_time()
    :telemetry.execute(@prefix ++ [event], %{monotonic_time: now, duration: now - start_mono}, metadata)
  end

  defp stop_metadata({outcome, stage, detail, command}) do
    %{
      outcome: outcome,
      stage: stage,
      clause: clause_label(outcome, stage, detail),
      result_class: if(outcome == :invalid_executor_result, do: Diagnostic.result_class(detail)),
      command_id_digest: command_id_digest(command)
    }
  end

  defp exception_metadata(kind, reason, stacktrace, command) do
    %{
      kind: kind,
      class: Diagnostic.result_class(reason),
      stack_depth: length(stacktrace),
      command_id_digest: command_id_digest(command)
    }
  end

  # closed labels: a value outside its vocabulary is reported as :invalid or "unlisted", never copied
  defp verb_label(verb) when is_binary(verb) and verb in @verbs, do: verb
  defp verb_label(_verb), do: :invalid

  defp actor_class(%{"class" => class}) when is_binary(class) and class in @actor_classes, do: class
  defp actor_class(_actor), do: :invalid

  defp clause_label(:rejected, :executor, _detail), do: "executor_rejected"
  defp clause_label(:rejected, _stage, %{clause: clause}) when is_binary(clause) and clause in @clauses, do: clause
  defp clause_label(:rejected, _stage, _detail), do: "unlisted"
  defp clause_label(_outcome, _stage, _detail), do: nil

  # a bounded lookup aid, never the id: the first 16 hex characters of sha256 over the command id
  defp command_id_digest(%Command{requested_by: %{"command_id" => id}}) when is_binary(id),
    do: :sha256 |> :crypto.hash(id) |> Base.encode16(case: :lower) |> binary_part(0, 16)

  defp command_id_digest(_command), do: nil
end
