defmodule AiOrchestrator.Run.Executor.Owner do
  @moduledoc """
  The owned foreground harness of one command: a dedicated process that traps exits so it can be the
  run supervisor's linked parent and see a failed start as a return value, starts the subtree, reports
  `{:run_executor_started, owner, supervisor}` to the trace, calls the optional test barrier, awaits
  the Server through an asynchronous request so the caller-death monitor stays responsive while work
  is held, tears the subtree down and replies to the caller by a fresh reference.

  Every trappable failure of the owner's own work runs under one closed boundary: the subtree is torn
  down first with the latest known identities, then the caller receives the closed
  `%{clause: "run_executor_down", kind, class, digest}` (the contract's result class and digest, no
  reason or stack bytes), and the owner exits `:normal`. Teardown is a protocol over EVERY owned
  identity (supervisor, Writer, Server, Work): an orderly bounded `Supervisor.stop`, then a direct
  kill of each still-live owned process, each joined under a bound; a survivor is reported as
  `%{clause: "run_executor_teardown_incomplete"}`, never as success. The owner replies only after that
  protocol completed, and the caller waits for the owner's own completion (its DOWN) before returning.
  The caller only monitors this process: no link, no change to its `trap_exit`, no message of its own
  consumed. There is no wait timeout; `Run.Server.await` semantics are untouched.

  `start`'s second attempt (docs/contracts/command-executor-migration.org): if and only if the Writer
  refuses the `:create` attempt with exactly `journal_exists`, one attempt with `open: :existing` and
  `admission: :retry_only` follows; the second lock's verified prefix is the sole retry authority.
  """

  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run

  @stop_timeout 15_000
  @join_timeout 5_000
  @owner_completion_timeout 60_000

  @spec run(map(), (atom(), map() -> :ok) | nil) :: {:ok, map()} | {:error, map()}
  def run(config, barrier) do
    caller = self()
    ref = make_ref()
    owner = spawn(fn -> own(caller, ref, config, barrier) end)
    monitor = Process.monitor(owner)

    receive do
      {^ref, result} ->
        # the reply follows the teardown protocol; the caller still observes the owner's completion
        receive do
          {:DOWN, ^monitor, :process, ^owner, _reason} -> result
        after
          @owner_completion_timeout -> {:error, %{clause: "run_executor_teardown_incomplete"}}
        end

      {:DOWN, ^monitor, :process, ^owner, _reason} ->
        {:error, %{clause: "run_executor_down"}}
    end
  end

  @handoff_budget 5_000

  defp own(caller, ref, config, barrier) do
    Process.flag(:trap_exit, true)
    caller_monitor = Process.monitor(caller)
    # the acknowledged identity handoff: the Server admits no effect before this process, the reaper, knows the worker
    config = Map.put(config, :owner_handoff, {self(), make_ref()})

    result =
      case start(config) do
        {:ok, sup} -> owned(sup, config, barrier, caller_monitor)
        {:error, rejection} -> {:error, rejection}
      end

    # returning ends the owner normally (its DOWN is the caller's completion signal); no reply to a dead caller
    case result do
      :caller_gone -> :ok
      result -> send(caller, {ref, result})
    end

    :ok
  end

  defp start(%{mode: :run} = config) do
    case Run.Supervisor.start_link(config) do
      {:error, %{clause: "journal_exists"}} when is_map(config.command) and not is_map_key(config, :admission) ->
        Run.Supervisor.start_link(Map.merge(config, %{open: :existing, admission: :retry_only}))

      other ->
        other
    end
  end

  defp start(config), do: Run.Supervisor.start_link(config)

  # the closed boundary: the subtree is owned from here on. The COMPLETE owned map (root + every child) is
  # established BEFORE any fallible work and is what teardown receives on every path - the catch scope must
  # never see a root-only map (EA-M5). Facts acquisition itself falls back to root-only teardown.
  defp owned(sup, config, barrier, caller_monitor) do
    trace(config, {:run_executor_started, self(), sup})

    case facts(sup) do
      {:ok, facts} ->
        {owned, result} = closed_work(Map.put(facts, :supervisor, sup), config, barrier, caller_monitor)
        finish(owned, result)

      :error ->
        finish(%{supervisor: sup}, {:error, %{clause: "run_server_down"}})
    end
  end

  # every fallible owner step (the handoff barrier, the subtree barrier, the await) runs under ONE closed boundary
  # with the complete owned map already established; the map is extended by the registered worker BEFORE any
  # fallible call, and whatever escapes becomes the closed owner failure while the LATEST owned map (worker
  # included) is what teardown receives - never a root-only or pre-handoff map after acquisition (EA-M5, WG-M1)
  defp closed_work(owned, config, barrier, caller_monitor) do
    {owned, registered} = handoff(owned, config)

    result =
      try do
        acknowledge(owned, registered, barrier)
        :ok = call_barrier(barrier, :subtree_started, Map.put(owned, :owner, self()))
        await(owned.server, caller_monitor)
      catch
        kind, reason -> {:error, owner_failure(kind, reason)}
      end

    {owned, result}
  end

  # the worker's identity reaches the reaper BEFORE any effect is admitted: registered by the Server; this step
  # only RECEIVES (no fallible call). An absent worker or an exhausted budget leaves the facts unchanged (closure
  # then rests on the acknowledged orderly stop of the root).
  defp handoff(owned, %{owner_handoff: {_owner, ref}}) do
    receive do
      {:run_worker_registered, ^ref, worker, server} -> {Map.put(owned, :worker, worker), {ref, server}}
      {:run_worker_absent, ^ref, _clause} -> {owned, nil}
    after
      @handoff_budget -> {owned, nil}
    end
  end

  # the acknowledgement follows the :handoff_received barrier (fallible, under the closed boundary): a refused
  # barrier means no acknowledgement, so the Server never admits an effect
  defp acknowledge(_owned, nil, _barrier), do: :ok

  defp acknowledge(owned, {ref, server}, barrier) do
    :ok = call_barrier(barrier, :handoff_received, Map.put(owned, :owner, self()))
    send(server, {:run_worker_acknowledged, ref})
    :ok
  end

  # a finished command closes its Writer explicitly BEFORE the orderly stop, so the close legs (descriptor,
  # lock release) are observable: a failed close after a successful run is surfaced on the result as
  # close: {:error, rejection} (the command's own error still wins), never hidden by a silent shutdown
  defp finish(owned, result) do
    result = close_writer(owned, result)

    case teardown(owned) do
      :ok -> result
      {:error, incomplete} -> if result == :caller_gone, do: :caller_gone, else: {:error, incomplete}
    end
  end

  # only an OBSERVED :ok from Writer.close/1 proves the close (descriptor closed, lock released). A Writer that is
  # already gone, one that does not answer within the bound, or one that dies during the call has NOT proven
  # anything: that is a closed close-unproven failure, never :ok, even though teardown will kill the process.
  defp close_writer(%{writer: writer}, {:ok, %{} = result}) when is_pid(writer) do
    case observed_close(writer) do
      :ok -> {:ok, result}
      {:error, rejection} -> {:ok, Map.put(result, :close, {:error, rejection})}
    end
  end

  defp close_writer(_owned, result), do: result

  @close_timeout 15_000

  defp observed_close(writer) do
    if Process.alive?(writer) do
      try do
        GenServer.call(writer, :close, @close_timeout)
      catch
        :exit, {:timeout, _} -> {:error, %{clause: "close_unproven", cause: "timeout"}}
        :exit, {:noproc, _} -> {:error, %{clause: "close_unproven", cause: "writer_gone"}}
        :exit, _other -> {:error, %{clause: "close_unproven", cause: "writer_down"}}
      end
    else
      {:error, %{clause: "close_unproven", cause: "writer_gone"}}
    end
  end

  defp owner_failure(kind, reason) do
    %{"digest" => digest} = Diagnostic.describe(reason)
    %{clause: "run_executor_down", kind: kind, class: Diagnostic.result_class(reason), digest: digest}
  end

  defp facts(sup) do
    children = Supervisor.which_children(sup)

    with {:ok, server} <- child(children, Run.Server),
         {:ok, work} <- child(children, Run.Work.Supervisor),
         {:ok, writer} <- writer(children) do
      {:ok, %{server: server, work: work, writer: writer}}
    end
  catch
    :exit, _ -> :error
  end

  defp child(children, id) do
    case List.keyfind(children, id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) -> {:ok, pid}
      _ -> :error
    end
  end

  defp writer(children) do
    case Enum.find(children, &match?({{Writer, _}, pid, _, _} when is_pid(pid), &1)) do
      {_id, pid, _type, _modules} -> {:ok, pid}
      nil -> :error
    end
  end

  # the await is an asynchronous request: a caller DOWN or a Server DOWN is handled while it is outstanding
  defp await(server, caller_monitor) do
    request = :gen_statem.send_request(server, :await)
    server_monitor = Process.monitor(server)
    await_loop(request, server, server_monitor, caller_monitor)
  end

  defp await_loop(request, server, server_monitor, caller_monitor) do
    receive do
      {:DOWN, ^caller_monitor, :process, _caller, _reason} ->
        :caller_gone

      {:DOWN, ^server_monitor, :process, ^server, _reason} ->
        {:error, %{clause: "run_server_down"}}

      message ->
        case :gen_statem.check_response(message, request) do
          {:reply, result} -> result
          {:error, {_reason, _server}} -> {:error, %{clause: "run_server_down"}}
          :no_reply -> await_loop(request, server, server_monitor, caller_monitor)
        end
    end
  end

  # ---- teardown protocol over every owned identity ----
  # orderly first: a bounded Supervisor.stop (children shut down in reverse order); then every owned process
  # still alive - the root or any descendant - is killed directly and joined under a bound; a survivor is
  # reported, never equated with success. Root death is never taken as subtree closure.
  defp teardown(owned) do
    pids = owned |> Map.values() |> Enum.filter(&is_pid/1) |> Enum.uniq()
    monitors = for pid <- pids, do: {pid, Process.monitor(pid)}
    orderly_stop(owned.supervisor)

    for {pid, _monitor} <- monitors, Process.alive?(pid), do: Process.exit(pid, :kill)

    survivors =
      for {pid, monitor} <- monitors,
          not joined?(pid, monitor, @join_timeout),
          do: pid

    if survivors == [],
      do: :ok,
      else: {:error, %{clause: "run_executor_teardown_incomplete", survivors: length(survivors)}}
  end

  defp orderly_stop(sup) do
    if Process.alive?(sup) do
      try do
        Supervisor.stop(sup, :shutdown, @stop_timeout)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  defp joined?(pid, monitor, timeout) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _} -> true
    after
      timeout -> false
    end
  end

  # THE BARRIER CONTRACT IS TOTAL: a barrier (test-only, `nil` = no barrier) is called with exactly two names,
  # `:handoff_received` (after the worker's identity reached this reaper, before the acknowledgement) and
  # `:subtree_started` (before the await), and must answer `:ok` to both. Every failure of the barrier - a
  # missing clause included - is an owner failure under the closed boundary; nothing here infers "absence" from
  # compiler names, stack frames or exception shapes (WG-M2 ruling).
  # only an exact :ok releases the owner; any other result is an owner-local failure under the closed boundary
  defp call_barrier(nil, _name, _facts), do: :ok

  defp call_barrier(barrier, name, facts) when is_function(barrier, 2) do
    case barrier.(name, facts) do
      :ok -> :ok
      other -> exit({:barrier_result_invalid, other})
    end
  end

  defp trace(%{trace: pid}, message) when is_pid(pid), do: send(pid, message)
  defp trace(_config, _message), do: :ok
end
