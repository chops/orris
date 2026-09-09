defmodule AiOrchestrator.Host.Executor do
  @moduledoc """
  `AiOrchestrator.Commands.Executor` over `AiOrchestrator.Run.Executor` that registers the owned run
  subtree with `AiOrchestrator.Host.Monitor` (R6: fail-soft).

  The composed barrier registers the owned identities at `:subtree_started` (a guarded cast: every
  raise, throw or exit of the registration collapses to nothing) and then calls the user's barrier
  exactly once with the identical label and map, returning its value unchanged and propagating any
  escape unchanged, so the owner's closed `run_executor_down` semantics are untouched. Every other
  label passes straight through. After `Run.Executor.execute/2` returns, the caller-side unregister
  cast is guarded the same way. The command result, the journal bytes and the teardown are exactly
  what `Run.Executor` produces for the same inputs.

  In-VM seam: the context key `:host_monitor` (pid or registered name, default the Application
  instance) selects the monitor and is stripped before delegation; it is never a command argument.
  """

  @behaviour AiOrchestrator.Commands.Executor

  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Executor, as: RunExecutor

  # the generation is read from the arbiter inside the owner, bounded so a silent arbiter costs the
  # command at most this much and never its result
  @generation_budget 1_000

  @impl AiOrchestrator.Commands.Executor
  @spec execute(Command.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def execute(%Command{} = command, context) when is_list(context) do
    {monitor, context} = Keyword.pop(context, :host_monitor, Monitor)

    case Keyword.get(context, :barrier) do
      user_barrier when is_nil(user_barrier) or is_function(user_barrier, 2) ->
        caller = self()
        token = make_ref()
        ownership = Keyword.get(context, :ownership, [])
        composed = compose(user_barrier, monitor, command, context[:run_dir], ownership, caller, token)
        result = RunExecutor.execute(command, Keyword.put(context, :barrier, composed))
        unregister_registered(monitor, token)
        result

      # any other barrier value is the caller's error: it reaches Run.Executor's context validation
      # unchanged (only the Host seam was removed), so the refusal, its field and its precedence over
      # every effect are exactly the direct route's
      _invalid ->
        RunExecutor.execute(command, context)
    end
  end

  def execute(command, context), do: RunExecutor.execute(command, context)

  defp compose(user_barrier, monitor, command, run_dir, ownership, caller, token) do
    fn
      :subtree_started = label, owned ->
        guarded(fn -> register(monitor, command, run_dir, ownership, owned, caller, token) end)
        call_user(user_barrier, label, owned)

      label, owned ->
        call_user(user_barrier, label, owned)
    end
  end

  # no user barrier: the owner would have skipped the call, so the composed one answers :ok itself
  defp call_user(nil, _label, _owned), do: :ok
  defp call_user(barrier, label, owned) when is_function(barrier, 2), do: barrier.(label, owned)

  # the generation comes from the SAME arbiter the run registered with (context[:ownership], default global)
  defp register(monitor, %Command{run_id: run_id}, run_dir, ownership, owned, caller, token) when is_binary(run_dir) do
    lookup = Keyword.merge([acquire_timeout: @generation_budget], ownership)

    with {:ok, %{generation: generation}} <- Ownership.status(run_dir, lookup),
         {:ok, record} <- record(run_dir, run_id, owned, generation) do
      :ok = Monitor.register(monitor, record)
      send(caller, {__MODULE__, token, record})
    end

    :ok
  end

  defp register(_monitor, _command, _run_dir, _ownership, _owned, _caller, _token), do: :ok

  defp record(run_dir, run_id, owned, generation) do
    case registration(run_dir, run_id, owned, generation) do
      nil -> :error
      record -> {:ok, record}
    end
  end

  @doc "The complete identity record for an owned map, or nil when an identity is missing (shared with the mounted owner)."
  @spec registration(Path.t(), String.t(), map(), pos_integer()) :: map() | nil
  def registration(run_dir, run_id, owned, generation) do
    identities = Map.take(owned, [:owner, :supervisor, :server, :writer, :worker])

    if map_size(identities) == 5 and Enum.all?(Map.values(identities), &is_pid/1) do
      Map.merge(identities, %{run_dir: run_dir, run_id: run_id, generation: generation})
    end
  end

  # the record the barrier registered reaches the caller as one private message consumed here; when
  # the barrier never ran (refused command, escaped barrier) there is nothing to consume
  defp unregister_registered(monitor, token) do
    receive do
      {__MODULE__, ^token, record} -> guarded(fn -> Monitor.unregister(monitor, record) end)
    after
      0 -> :ok
    end
  end

  defp guarded(fun) do
    fun.()
    :ok
  catch
    _kind, _reason -> :ok
  end
end
