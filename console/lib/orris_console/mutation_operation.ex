defmodule OrrisConsole.MutationOperation do
  @moduledoc """
  One accepted cancel (docs/contracts/console-mutations.org §start ownership, §outcome classes). Started by the
  starter helper under MutationWorkers: `start_link/1` runs inside the DynamicSupervisor and reports the attempt to
  the authority after GenServer.start_link returns; init registers the operation, honours the init gate seam, then
  monitors the accepting authority and arms its own grant timeout. Work starts only on {:grant, op_ref}: the
  invoker runs in an OWNED linked+monitored process; its result (with provenance) is classified, a bounded owned
  read observes the journal, and {:finished, op_ref, outcome} reaches the authority. The operation traps exits: the
  invoker's EXIT is consumed (its DOWN decides); any other exit signal while responsive is an abort (the invoker is
  killed, the operation stops with the received reason). Formatted status is redacted.
  """
  use GenServer, restart: :temporary

  alias AiOrchestrator.{Prepare, Query}
  alias OrrisConsole.{MutationRegistry, SessionStore}

  @pre_write_busy ~w(second_live_writer run_locked ownership_unavailable lock_unavailable lock_unreadable)
  @pre_write_refused ~w(pane_claim_refused invalid_command_actor unknown_actor_class command_actor_fields command_actor_id command_verb_unsupported)
  @terminal ~w(completed failed budget_exhausted)

  def child_spec(args) do
    %{
      id: args.op_ref,
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: args.config.mutation_shutdown_ms
    }
  end

  @doc "Runs inside the DynamicSupervisor: the attempt is reported to the authority by the supervisor itself."
  def start_link(args) do
    result = GenServer.start_link(__MODULE__, args)
    to_authority({:child_start_attempt, args.op_ref, result})
    result
  end

  defp to_authority(message) do
    case Process.whereis(SessionStore) do
      pid when is_pid(pid) -> send(pid, message)
      nil -> :ok
    end
  end

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)
    {:ok, _} = Registry.register(MutationRegistry, {:operation, args.op_ref}, :operation)
    gate(args, :operation_gate, :operation_init)
    amon = Process.monitor(args.authority)
    timer = Process.send_after(self(), :grant_timeout, args.config.mutation_start_ms)

    {:ok,
     %{
       args: args,
       amon: amon,
       granted: false,
       invoker: nil,
       imon: nil,
       finished: false,
       finish_wait: false,
       read_task: nil,
       grant_timer: timer
     }}
  end

  # ---- seams ----
  defp gate(args, key, kind) do
    case Map.get(args.config, key) do
      pid when is_pid(pid) ->
        send(pid, {:mutation, kind, args.op_ref, self()})
        witness(args, kind, self(), pid)

        receive do
          :proceed -> :ok
        end

      _ ->
        :ok
    end
  end

  defp witness(args, kind, payload, except \\ nil) do
    case args.config.mutation_witness do
      pid when is_pid(pid) and pid != except -> send(pid, {:mutation, kind, args.op_ref, payload})
      _ -> :ok
    end
  end

  # ---- grant → invoke ----
  @impl true
  def handle_info({:grant, ref}, %{granted: false, args: %{op_ref: ref} = args} = s) do
    if s.grant_timer, do: Process.cancel_timer(s.grant_timer)
    me = self()
    {invoker, imon} = Process.spawn(fn -> send(me, {:invoked, ref, invoke(args)}) end, [:link, :monitor])
    witness(args, :invoker, invoker)
    {:noreply, %{s | granted: true, invoker: invoker, imon: imon, grant_timer: nil}}
  end

  def handle_info({:invoked, ref, result}, %{args: %{op_ref: ref}, finished: false} = s), do: finish(s, result)

  # the invoker died without a result: unknown effect (never ignored)
  def handle_info({:DOWN, imon, :process, _pid, _reason}, %{imon: imon, finished: false} = s),
    do: finish(%{s | invoker: nil, imon: nil}, :unknown)

  def handle_info({:DOWN, imon, :process, _pid, _reason}, %{imon: imon} = s), do: {:noreply, %{s | invoker: nil}}

  # the accepting authority died before the grant: no work
  def handle_info({:DOWN, amon, :process, _pid, _reason}, %{amon: amon, granted: false} = s), do: {:stop, :normal, s}
  def handle_info({:DOWN, amon, :process, _pid, _reason}, %{amon: amon} = s), do: {:noreply, s}
  def handle_info(:grant_timeout, %{granted: false} = s), do: {:stop, :normal, s}
  def handle_info(:grant_timeout, s), do: {:noreply, s}
  # the responsive finish gate: released by :proceed
  def handle_info(:proceed, %{finish_wait: true} = s), do: {:stop, :normal, s}
  # the linked invoker's exit signal is consumed; its DOWN decides
  def handle_info({:EXIT, invoker, _reason}, %{invoker: invoker} = s), do: {:noreply, s}
  # the owned read task (linked) ends normally after its result: not an abort
  def handle_info({:EXIT, task, _reason}, %{read_task: task} = s), do: {:noreply, %{s | read_task: nil}}
  # any other exit signal while responsive is an abort with the received reason
  def handle_info({:EXIT, _from, reason}, s), do: {:stop, reason, s}
  def handle_info(_other, s), do: {:noreply, s}

  @impl true
  def terminate(_reason, %{invoker: invoker}) when is_pid(invoker) do
    # abort: caller death for the core Owner
    Process.exit(invoker, :kill)
    :ok
  end

  def terminate(_reason, _s), do: :ok

  @impl true
  def format_status(status),
    do: status |> Map.put(:state, :redacted) |> Map.put(:message, :redacted) |> Map.put(:queue, :redacted)

  # ---- the invoke (in the owned invoker process) ----
  defp invoke(args) do
    opts = server_opts(args)

    case args.config.mutation_invoke do
      fun when is_function(fun, 3) ->
        {:invoke, fun.(args.actor, args.run_ref, opts)}

      _ ->
        case Prepare.cancel(args.run_ref, opts) do
          {:ok, prepared} -> {:invoke, Prepare.invoke(args.actor, prepared, opts)}
          {:error, _} = rejection -> {:cancel, rejection}
        end
    end
  end

  defp server_opts(args),
    do:
      [root: args.dir, operator: args.config.operator.id] ++ Keyword.take(args.config.mutation_opts, [:fs, :clock, :id])

  # ---- outcome ----
  defp finish(s, result) do
    args = s.args
    {phase, invoke, class} = classify(result)
    {observed, task} = if phase == :pre_admission_refused, do: {nil, nil}, else: bounded_read(args)
    outcome = %{phase: phase, invoke: invoke, observed: observed, message: message(phase, invoke, class, observed)}
    to_authority({:finished, args.op_ref, outcome})
    s = %{s | finished: true, read_task: task}

    case args.config.operation_finish_gate do
      pid when is_pid(pid) ->
        send(pid, {:mutation, :operation_finish, args.op_ref, self()})
        witness(args, :operation_finish, self(), pid)
        {:noreply, %{s | finish_wait: true}}

      _ ->
        {:stop, :normal, s}
    end
  end

  # provenance first: a Prepare.cancel rejection precedes any Writer; an invoke rejection is pre-write only by allowlist
  defp classify({:cancel, {:error, %{clause: clause}}}), do: {:pre_admission_refused, {:error, clause}, "not available"}
  defp classify({:invoke, {:ok, %{close: :ok}}}), do: {:invoked, :ok, nil}
  defp classify({:invoke, {:ok, %{close: {:error, %{clause: clause}}}}}), do: {:invoked, {:close_failed, clause}, nil}
  defp classify({:invoke, {:ok, %{close: _other}}}), do: {:invoked, {:close_failed, "close_failed"}, nil}

  defp classify({:invoke, {:error, %{clause: clause}}}) do
    cond do
      clause in @pre_write_busy -> {:pre_admission_refused, {:error, clause}, "busy"}
      clause in @pre_write_refused -> {:pre_admission_refused, {:error, clause}, "refused"}
      true -> {:invoked, {:error, clause}, nil}
    end
  end

  defp classify(:unknown), do: {:unknown, :unknown, nil}
  defp classify(_other), do: {:invoked, {:error, "invalid_result"}, nil}

  defp bounded_read(args) do
    ref = make_ref()

    task =
      Task.async(fn ->
        case args.config.read_gate do
          gate when is_pid(gate) ->
            send(gate, {:read_gate, self(), ref})

            receive do
              :go -> :ok
            end

          _ ->
            :ok
        end

        Query.run_summary(args.run_ref, root: args.dir)
      end)

    observed =
      case Task.yield(task, args.config.mutation_read_ms) || Task.shutdown(task, :brutal_kill) do
        {:ok, {:ok, %{status: status, last_seq: seq}}} -> %{status: status, last_seq: seq}
        {:ok, {:error, %{clause: clause}}} -> %{unavailable: clause}
        {:ok, _other} -> %{unavailable: "read_failed"}
        {:exit, _} -> %{unavailable: "read_failed"}
        nil -> %{unavailable: "read_timeout"}
      end

    {observed, task.pid}
  end

  defp message(:pre_admission_refused, _invoke, class, _observed), do: "Cancel refused before any write (#{class})"
  defp message(:invoked, :ok, _class, observed), do: table(observed)

  defp message(:invoked, {:close_failed, _}, _class, observed),
    do: table(observed) <> "; the journal close reported an error (attention)"

  defp message(:invoked, {:error, clause}, _class, observed),
    do: "Cancel outcome uncertain (#{clause}): " <> table(observed)

  defp message(:unknown, _invoke, _class, observed), do: "Cancel outcome uncertain (unknown): " <> table(observed)

  defp table(%{status: "cancelled", last_seq: seq}), do: "Run is cancelled (verified journal at seq #{seq})"

  defp table(%{status: status, last_seq: seq}) when status in @terminal,
    do: "Run already finished: #{status} (verified journal at seq #{seq})"

  defp table(%{status: status, last_seq: seq}),
    do: "Run is #{status} (verified journal at seq #{seq}); the cancel did not take effect"

  defp table(%{unavailable: clause}), do: "Current journal state unavailable (#{clause})"
  defp table(_), do: "Current journal state unavailable (read_failed)"
end
