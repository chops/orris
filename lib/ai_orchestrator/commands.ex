defmodule AiOrchestrator.Commands do
  @moduledoc """
  Framework-neutral, actor-aware command entry point.

  Policy is checked and the NS-41 acceptance stamp is constructed before the
  command reaches an executor. Lifecycle admissibility and durable
  idempotency remain responsibilities of the run host.
  """

  use Boundary,
    deps: [AiOrchestrator.Clock, AiOrchestrator.Contract],
    exports: [Arguments, CommandId, Executor, Idempotency, Policy]

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Commands.Arguments
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Moment

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type result :: {:ok, map()} | {:error, rejection()}

  @doc "Builds an authorized command without executing it."
  @spec build(map(), String.t(), map(), keyword()) :: {:ok, Command.t()} | {:error, rejection()}
  def build(actor, verb, args, opts) when is_binary(verb) and is_list(opts) do
    with {:ok, actor} <- Policy.authorize(actor, verb),
         {:ok, args} <- Arguments.validate(verb, args),
         {:ok, run_id} <- fetch_run_id(opts),
         :ok <- validate_agent_run(actor, run_id),
         {:ok, command_id} <- command_id(opts),
         {:ok, now} <- moment(opts) do
      requested_by =
        actor
        |> Map.put("command_id", command_id)
        |> Map.put("verb", verb)
        |> Map.put("args_hash", Arguments.hash(verb, args))

      {:ok, %Command{requested_by: requested_by, run_id: run_id, args: args, now: now}}
    end
  end

  def build(_actor, verb, _args, opts) when is_list(opts) and not is_binary(verb) do
    {:error, %{clause: "invalid_command_verb"}}
  end

  def build(_actor, _verb, _args, _opts), do: {:error, %{clause: "invalid_command_options"}}

  @doc "Authorizes, builds, and sends a command through the configured executor."
  @spec invoke(map(), String.t(), map(), keyword()) :: result()
  def invoke(actor, verb, args, opts) when is_list(opts) do
    with {:ok, command} <- build(actor, verb, args, opts),
         {:ok, executor} <- fetch_executor(opts) do
      command
      |> executor.execute(Keyword.get(opts, :executor_opts, []))
      |> normalize_executor_result()
    end
  end

  def invoke(_actor, _verb, _args, _opts), do: {:error, %{clause: "invalid_command_options"}}

  defp fetch_run_id(opts) do
    case Keyword.fetch(opts, :run_id) do
      {:ok, run_id} when is_binary(run_id) and run_id != "" -> {:ok, run_id}
      _other -> {:error, %{clause: "run_id_required"}}
    end
  end

  defp fetch_executor(opts) do
    case Keyword.fetch(opts, :executor) do
      {:ok, executor} when is_atom(executor) ->
        if Code.ensure_loaded?(executor) and function_exported?(executor, :execute, 2) do
          {:ok, executor}
        else
          {:error, %{clause: "invalid_command_executor"}}
        end

      _other ->
        {:error, %{clause: "command_executor_required"}}
    end
  end

  defp validate_agent_run(%{"class" => "agent", "run_id" => run_id}, run_id), do: :ok

  defp validate_agent_run(%{"class" => "agent"}, _run_id) do
    {:error, %{clause: "actor_run_id_mismatch"}}
  end

  defp validate_agent_run(_actor, _run_id), do: :ok

  defp command_id(opts) do
    case Keyword.fetch(opts, :command_id) do
      {:ok, value} -> CommandId.validate(value)
      :error -> opts |> Keyword.get(:command_id_generator, CommandId) |> generate_command_id()
    end
  end

  defp generate_command_id(generator) when is_atom(generator) do
    if Code.ensure_loaded?(generator) and function_exported?(generator, :generate, 0) do
      CommandId.validate(generator.generate())
    else
      {:error, %{clause: "invalid_command_id_generator"}}
    end
  end

  defp generate_command_id(_generator), do: {:error, %{clause: "invalid_command_id_generator"}}

  defp moment(opts) do
    case Keyword.fetch(opts, :now) do
      {:ok, %Moment{} = now} -> {:ok, now}
      {:ok, _invalid} -> {:error, %{clause: "invalid_command_moment"}}
      :error -> opts |> Keyword.get(:clock, SystemClock) |> read_moment()
    end
  end

  defp read_moment(clock) when is_atom(clock) do
    with true <- clock_module?(clock),
         unix when is_integer(unix) <- clock.unix_now(),
         {:ok, datetime} <- DateTime.from_unix(unix) do
      {:ok, %Moment{wall_ts: DateTime.to_iso8601(datetime), unix: unix}}
    else
      _invalid -> {:error, %{clause: "invalid_command_clock"}}
    end
  end

  defp read_moment(_clock), do: {:error, %{clause: "invalid_command_clock"}}

  defp clock_module?(clock) do
    Code.ensure_loaded?(clock) and function_exported?(clock, :wall_ts, 0) and
      function_exported?(clock, :unix_now, 0)
  end

  defp normalize_executor_result({:ok, %{} = result}), do: {:ok, result}
  defp normalize_executor_result({:error, %{} = rejection}), do: {:error, rejection}

  defp normalize_executor_result(other) do
    {:error,
     %{
       clause: "invalid_executor_result",
       result_class: term_class(other),
       digest: "sha256:" <> (:sha256 |> :crypto.hash(:erlang.term_to_binary(other)) |> Base.encode16(case: :lower))
     }}
  end

  defp term_class(term) when is_atom(term), do: "atom"
  defp term_class(term) when is_binary(term), do: "binary"
  defp term_class(term) when is_list(term), do: "list"
  defp term_class(term) when is_map(term), do: "map"
  defp term_class(term) when is_tuple(term), do: "tuple"
  defp term_class(_term), do: "other"
end
