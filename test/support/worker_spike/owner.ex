defmodule AiOrchestrator.Test.WorkerSpike.Owner do
  @moduledoc """
  TEST-ONLY mechanism spike (ruling m_1788677511000): ONE long-lived `:temporary` effect-runtime owner born under
  the run's existing `Run.Work.Supervisor`, owning the `Effects.Runtime` (gate handles, Ports, process-local memos)
  for a whole run execution. Nothing here is product code; it exists to measure the ownership mechanism.

  Protocol (every message carries the run-generation CAPABILITY `cap`, a private reference minted by the driving
  Server and handed to the owner at birth; this is intra-BEAM correlation against accidental or forged ordinary
  messages, NOT authentication - a PID or a ref in a message is a claim, and no security is claimed against code
  introspecting the same BEAM):

    admission   {:admit, cap, generation}            -> {:admitted, cap, generation}   (before ANY effect)
    release     {:release_terminal, cap, gen, ref, appended} -> {:released, cap, gen, ref}
    execute     {:execute, cap, gen, ref, effect, inputs}   -> {:effect_result, cap, gen, ref, observation}
                                                          | {:effect_failed, cap, gen, ref, closed}
    settle      {:settle, cap, gen}                   -> {:settled, cap, gen, cleanup}

  Dequeue validation: a message with the wrong cap is dropped; an op of an older generation is dropped closed; an
  execute whose (gen, ref) already completed is refused `effect_duplicate` and NEVER re-executed (same-generation
  dedupe); anything before admission is refused `not_admitted`. Exactly one op is current at a time (the Server
  enforces one in flight; the owner's callback is synchronous, so a second execute simply queues behind it and is
  validated when dequeued). A trappable failure inside an effect settles the LATEST runtime in the same invocation
  and answers `effect_failed` with the closed diagnostic (kind/class/digest); the owner stays alive. A hard death
  (kill, VM) runs nothing here - the design never relies on `terminate/2`.

  Secrecy: the child spec carries only {server pid, cap, seams holder pid}; the seams (adapter closures, which may
  close over anything) are fetched inside `init/1` and the Runtime is built here; `format_status/1` renders a
  closed shape; the owner exits only with closed reasons except an external kill.
  """

  use GenServer, restart: :temporary, shutdown: 1_000

  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Interrupted
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Test.WorkerSpike.Seams

  @type cap :: reference()

  def child_spec(%{server: server, cap: cap, seams: seams} = arg)
      when is_pid(server) and is_reference(cap) and is_pid(seams) do
    %{
      id: {__MODULE__, cap},
      start: {__MODULE__, :start_link, [arg]},
      restart: :temporary,
      shutdown: 1_000,
      type: :worker
    }
  end

  def start_link(%{server: _, cap: _, seams: _} = arg), do: GenServer.start_link(__MODULE__, arg)

  @impl true
  # every init stage (holder lookup, seams shape, runtime construction) is under one closed boundary: a
  # failure stops with a closed clause, never with a reason carrying the seams or an exception
  def init(%{server: server, cap: cap, seams: seams}) do
    case Seams.fetch(seams, cap) do
      {:ok, opts} when is_list(opts) ->
        {:ok,
         %{
           server: server,
           cap: cap,
           opts: opts,
           runtime: Runtime.new(opts),
           phase: :born,
           generation: nil,
           done: MapSet.new(),
           dropped: 0
         }}

      {:ok, _not_a_keyword} ->
        {:stop, {:shutdown, %{clause: "owner_seams_invalid"}}}

      :error ->
        {:stop, {:shutdown, %{clause: "owner_seams_missing"}}}
    end
  catch
    _kind, _reason -> {:stop, {:shutdown, %{clause: "owner_init_failed"}}}
  end

  # ---- protocol (all asynchronous; the Server never blocks on the owner) ----

  @impl true
  def handle_info({:admit, cap, generation}, %{cap: cap, phase: :born} = state) when is_integer(generation) do
    send(state.server, {:admitted, cap, generation})
    {:noreply, %{state | phase: :admitted, generation: generation}}
  end

  def handle_info({:release_terminal, cap, gen, ref, appended}, %{cap: cap} = state) do
    cond do
      not (is_list(appended) and Enum.all?(appended, &is_map/1)) ->
        # shape refused without reflecting the value
        send(state.server, {:refused, cap, gen, ref, "release_shape_invalid"})
        {:noreply, state}

      admitted(state, gen) != :ok ->
        {:refused, clause} = admitted(state, gen)
        send(state.server, {:refused, cap, gen, ref, clause})
        {:noreply, state}

      true ->
        state = guarded_stage(state, gen, ref, fn -> {:released, Effects.release_terminal(state.runtime, appended)} end)
        {:noreply, state}
    end
  end

  def handle_info({:execute, cap, gen, ref, effect, inputs}, %{cap: cap} = state) do
    cond do
      not execute_shape?(effect, inputs) ->
        # the protocol shape is validated first, without reflecting any value into a reply, log or crash
        send(state.server, {:refused, cap, gen, ref, "execute_shape_invalid"})
        {:noreply, state}

      admitted(state, gen) != :ok ->
        {:refused, clause} = admitted(state, gen)
        send(state.server, {:refused, cap, gen, ref, clause})
        {:noreply, state}

      MapSet.member?(state.done, ref) ->
        # same-generation duplicate: refused, never re-executed
        send(state.server, {:refused, cap, gen, ref, "effect_duplicate"})
        {:noreply, state}

      true ->
        state = execute(state, gen, ref, effect, inputs)
        {:noreply, state}
    end
  end

  def handle_info({:settle, cap, gen}, %{cap: cap} = state) do
    case admitted(state, gen) do
      :ok ->
        {cleanup, runtime} = Effects.settle(state.runtime)
        send(state.server, {:settled, cap, gen, cleanup_summary(cleanup)})
        {:noreply, %{state | runtime: runtime}}

      {:refused, clause} ->
        send(state.server, {:refused, cap, gen, nil, clause})
        {:noreply, state}
    end
  end

  # a test-only counter of dropped messages (closed number, no content)
  def handle_info({:dropped?, cap, from}, %{cap: cap} = state) do
    send(from, {:dropped, cap, state.dropped})
    {:noreply, state}
  end

  # wrong cap, unknown shape, stale/forged: dropped and counted; never acted on
  def handle_info(_other, state), do: {:noreply, %{state | dropped: state.dropped + 1}}

  # closed status and crash reports: state, the last message, the log and the reason are all redacted (the seams,
  # the runtime and any message content never reach a printer)
  @impl true
  def format_status(status) do
    status
    |> Map.put(:state, :redacted)
    |> Map.replace(:message, :redacted)
    |> Map.replace(:log, [])
    |> Map.replace(:queue, [])
    |> Map.replace(:reason, :redacted)
  end

  defp execute_shape?(effect, inputs) do
    is_struct(effect) and is_list(inputs) and Keyword.keyword?(inputs) and
      Enum.all?(inputs, fn
        {:receipt, receipt} -> is_nil(receipt) or is_map(receipt)
        _other -> false
      end)
  end

  # a non-effect stage (release_terminal, settle) under the same closed boundary: any escape answers closed
  defp guarded_stage(state, gen, ref, fun) do
    {:released, runtime} = fun.()
    send(state.server, {:released, state.cap, gen, ref})
    %{state | runtime: runtime}
  catch
    kind, reason ->
      send(state.server, {:refused, state.cap, gen, ref, closed_stage(kind, reason)})
      state
  end

  defp closed_stage(kind, reason), do: %{clause: "stage_failed", kind: kind, class: Diagnostic.result_class(reason)}

  # ---- execution under the same-invocation settle discipline (the Host's guard, moved with the stage) ----

  # the Server sends only the effect and the receipt; the seams are the owner's own (a queued execute message never
  # carries adapter closures or their environment). The shape was validated before this point.
  defp execute(state, gen, ref, effect, inputs) do
    inputs = Keyword.put(inputs, :opts, state.opts)

    try do
      {observation, runtime} = Effects.execute(effect, state.runtime, inputs)
      send(state.server, {:effect_result, state.cap, gen, ref, observation})
      %{state | runtime: runtime, done: MapSet.put(state.done, ref)}
    catch
      :error, %Interrupted{} = interrupted ->
        # settle the LATEST runtime in this very invocation, then answer closed; stay alive
        {cleanup, runtime} = Effects.settle(interrupted.runtime)
        send(state.server, {:effect_failed, state.cap, gen, ref, closed(interrupted.kind, interrupted.reason, cleanup)})
        %{state | runtime: runtime, done: MapSet.put(state.done, ref)}

      kind, reason ->
        {cleanup, runtime} = Effects.settle(state.runtime)
        send(state.server, {:effect_failed, state.cap, gen, ref, closed(kind, reason, cleanup)})
        %{state | runtime: runtime, done: MapSet.put(state.done, ref)}
    end
  end

  # closed: kind, the contract's result class and digest, and the same-invocation cleanup SUMMARY: attempts, and how
  # many the executor actually reported settled vs unproven. An emptied runtime is tracking relinquished, never
  # proof of native closure (Effects.settle), so an unproven cleanup is never counted as settled.
  defp closed(kind, reason, cleanup) do
    %{
      clause: "effect_failed",
      kind: kind,
      class: Diagnostic.result_class(reason),
      digest: Diagnostic.describe(reason)["digest"],
      cleanup: cleanup_summary(cleanup)
    }
  end

  @unproven ~w(settle_unproven abandon_unsettled guardian_gone)

  defp cleanup_summary(cleanup) do
    unproven = Enum.count(cleanup, fn row -> get_in(row, ["settle", "clause"]) in @unproven end)
    %{attempts: length(cleanup), settled: length(cleanup) - unproven, unproven: unproven}
  end

  defp admitted(%{phase: :admitted, generation: generation}, gen) when gen == generation, do: :ok
  defp admitted(%{phase: :admitted}, _gen), do: {:refused, "effect_generation_stale"}
  defp admitted(_state, _gen), do: {:refused, "not_admitted"}
end
