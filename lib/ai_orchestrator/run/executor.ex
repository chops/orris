defmodule AiOrchestrator.Run.Executor do
  @moduledoc """
  The `AiOrchestrator.Commands.Executor` over the foreground foundation (NS-43 caller migration,
  docs/contracts/command-executor-migration.org). One authorized command runs as one owned run subtree:
  `Run.Supervisor` -> Journal.Writer -> `Run.Server` -> Work.Supervisor, started, awaited and torn
  down by a dedicated owner process (`AiOrchestrator.Run.Executor.Owner`) that the caller monitors.

  Order, each step before any Writer, file or effect activity of the next: verb scope (start, resume,
  cancel only), stamp revalidation (a caller can build a `Command` without `Commands`), executor
  context validation, the start arguments against the consumed inputs, then the owned subtree.
  Everything from there happens under the Writer lock inside the subtree, where `Run.Server` admits
  the command against the locked verified prefix.

  The executor context is never a command argument: run directory, the spec and plan the caller read
  and the hashes of the exact bytes it consumed, and the Host option keyword (adapters, seams, ids).
  Server-owned bindings supplied by a caller are dropped. There is no wait timeout in this unit: a
  supplied one is refused, never honoured.
  """

  @behaviour AiOrchestrator.Commands.Executor

  alias AiOrchestrator.Commands.Arguments
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Journal.Schemas.RequestedBy
  alias AiOrchestrator.Run.Executor.Owner

  @verbs %{"start" => :run, "resume" => :resume, "cancel" => :cancel}
  @owned_bindings [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  # spec_hash/plan_hash stay in the Host options: they are the input provenance the reducer journals
  @context_keys [:run_dir, :spec, :plan, :trace, :barrier, :restart_empty]

  @type context :: keyword()

  @doc """
  Everything `execute/2` decides BEFORE any Writer, file or effect activity, without starting: verb scope,
  stamp revalidation, context validation in the fixed order, and the start inputs. Answers the owner config
  and the caller's barrier, or exactly the refusal `execute/2` would answer. The host's mounted route shares
  this validation instead of duplicating it.
  """
  @spec prepare(Command.t(), context()) ::
          {:ok, %{config: map(), barrier: (atom(), map() -> :ok) | nil}} | {:error, map()}
  def prepare(%Command{} = command, context) when is_list(context) do
    with {:ok, mode} <- verb(command),
         :ok <- revalidate(command),
         {:ok, ctx} <- validate_context(mode, command, context) do
      {:ok, %{config: config(mode, command, ctx), barrier: ctx[:barrier]}}
    end
  end

  def prepare(_command, _context), do: {:error, %{clause: "command_stamp_invalid"}}

  @impl AiOrchestrator.Commands.Executor
  @spec execute(Command.t(), context()) :: {:ok, map()} | {:error, map()}
  def execute(command, context) do
    case prepare(command, context) do
      {:ok, %{config: config, barrier: barrier}} -> Owner.run(config, barrier)
      {:error, _} = refusal -> refusal
    end
  end

  # verb precedence applies to a structured stamp; a stamp that is not even a map is a malformed stamp
  defp verb(%Command{requested_by: %{"verb" => verb}}) when is_map_key(@verbs, verb), do: {:ok, Map.fetch!(@verbs, verb)}
  defp verb(%Command{requested_by: %{}}), do: {:error, %{clause: "command_verb_unsupported"}}
  defp verb(_command), do: {:error, %{clause: "command_stamp_invalid"}}

  # the COMPLETE structured stamp is revalidated (a caller can build a Command without Commands): the closed
  # RequestedBy object schema and its append bounds (no unknown field survives), then the arguments it claims
  # (closed grammar, ARGS-CANON-1 equality) and policy for the stamped class/id
  defp revalidate(%Command{requested_by: stamp, args: args}) when is_map(stamp) and is_map(args) do
    verb = stamp["verb"]

    with {:ok, _} <- RequestedBy.parse(stamp),
         :ok <- RequestedBy.validate_append_bounds(stamp),
         {:ok, _} <- CommandId.validate(stamp["command_id"]),
         {:ok, _} <- Arguments.validate(verb, args),
         true <- Arguments.hash(verb, args) == stamp["args_hash"],
         {:ok, _} <- Policy.authorize(Map.take(stamp, ["class", "id"]), verb) do
      :ok
    else
      _ -> {:error, %{clause: "command_stamp_invalid"}}
    end
  end

  defp revalidate(_command), do: {:error, %{clause: "command_stamp_invalid"}}

  # the context is checked field by field, in a fixed order, before anything else touches the run
  defp validate_context(mode, command, context) do
    # reserved keys a caller may never supply: the wait timeout, and the continuation selectors the run
    # server derives from the locked prefix
    reserved =
      for key <- [:await_timeout, :acceptance, :continuation, :mode],
          do: {Atom.to_string(key), fn -> not Keyword.has_key?(context, key) end}

    checks =
      reserved ++
        [
          {"restart_empty", fn -> restart_selector_ok?(mode, context) end},
          {"barrier", fn -> is_nil(context[:barrier]) or is_function(context[:barrier], 2) end},
          {"run_dir", fn -> is_binary(context[:run_dir]) end},
          {"spec", fn -> mode == :cancel or is_map(context[:spec]) end},
          {"plan", fn -> mode == :cancel or is_map(context[:plan]) end},
          {"spec_hash", fn -> mode != :run or is_binary(context[:spec_hash]) end},
          {"plan_hash", fn -> mode != :run or is_binary(context[:plan_hash]) end}
        ]

    case Enum.find(checks, fn {_field, ok?} -> not ok?.() end) do
      {field, _} -> invalid(field)
      nil -> validate_inputs(mode, command, context)
    end
  end

  # unit D: the explicit restart-empty selector is exactly `true` for start only; absence means the ordinary path
  defp restart_selector_ok?(mode, context) do
    case Keyword.fetch(context, :restart_empty) do
      :error -> true
      {:ok, true} -> mode == :run
      {:ok, _other} -> false
    end
  end

  defp validate_inputs(:run, command, context) do
    if inputs_match?(command, context), do: {:ok, context}, else: {:error, %{clause: "command_inputs_mismatch"}}
  end

  defp validate_inputs(_mode, _command, context), do: {:ok, context}

  defp invalid(field), do: {:error, %{clause: "command_context_invalid", field: field}}

  # the start arguments must name exactly the inputs the caller consumed (sha256 of the exact bytes)
  defp inputs_match?(%Command{args: args}, context),
    do: args["spec_hash"] == context[:spec_hash] and args["plan_hash"] == context[:plan_hash]

  defp config(mode, command, ctx) do
    config = %{
      run_dir: ctx[:run_dir],
      mode: mode,
      spec: ctx[:spec],
      plan: ctx[:plan],
      opts: ctx |> Keyword.drop(@context_keys) |> Keyword.drop(@owned_bindings),
      trace: ctx[:trace],
      command: command
    }

    # the explicit restart opens the EXISTING journal (never creates) under its own admission; the locked
    # verified prefix decides (Run.Server), the CLI preflight only requested it
    if ctx[:restart_empty] == true, do: Map.merge(config, %{open: :existing, admission: :restart_empty}), else: config
  end
end
