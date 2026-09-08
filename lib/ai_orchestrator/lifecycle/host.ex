defmodule AiOrchestrator.Lifecycle.Host do
  @moduledoc """
  In-process driver for the pure reducer (Gate C).

  Same signatures as the sequential supervisor had: `run/3`, `resume/4`, `cancel/2`. The host
  resolves ids through the id seam, then drives the machine: each suspension hands back the event
  suffix emitted since the previous one, which the host stamps and journals line by line (intent
  before effect; a refusal halts the suffix, terminal handles are retired only after the WHOLE
  suffix committed), selects the persisted receipt a `ReleaseGate` needs, executes the pending
  intent through `AiOrchestrator.Effects` with an explicit runtime, notifies the observer, and
  feeds the single observation back to `Reducer.step/2`. Every trappable failure (`:error`,
  `:throw`, `:exit`) from execution, observer or sink settles the latest runtime in this same
  invocation and then propagates unchanged.

  The loop is exposed as the smallest stepping API: `open/3` resolves identity and produces a
  `Host.Loop`; `advance/1` performs exactly one step (guard(commit) -> release_terminal -> receipt
  selection -> guard(execute) -> guard(observer) -> guard(Reducer.step)). `run/3`, `resume/4` and
  `cancel/2` are open + advance-until-halt, so a caller stepping the loop itself (the foreground
  `AiOrchestrator.Run.Server`) shares every commit, receipt and cleanup rule with them.
  """

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Id.SystemId
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Lifecycle.Host.Loop
  alias AiOrchestrator.Lifecycle.Host.Stage

  @type mode :: :run | :resume | :cancel | :continue
  @type acceptance :: %{seq: pos_integer(), verb: String.t()}
  @type inputs :: %{
          optional(:spec) => map() | nil,
          optional(:plan) => map() | nil,
          optional(:prior_lines) => [String.t()],
          optional(:acceptance) => acceptance()
        }

  @spec run(map(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def run(spec, plan, opts \\ []) when is_map(spec) and is_map(plan),
    do: :run |> open(%{spec: spec, plan: plan, prior_lines: []}, opts) |> drive()

  @spec resume(map(), map(), [String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def resume(spec, plan, prior_lines, opts \\ []) when is_map(spec) and is_map(plan) and is_list(prior_lines),
    do: :resume |> open(%{spec: spec, plan: plan, prior_lines: prior_lines}, opts) |> drive()

  @spec cancel([String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def cancel(prior_lines, opts \\ []) when is_list(prior_lines),
    do: :cancel |> open(%{prior_lines: prior_lines}, opts) |> drive()

  @doc """
  Resolves identity and opens a loop without executing anything: `:run` takes fresh identity from
  the id seam; `:resume`/`:cancel` decode the prior lines and take the run id from the fold.
  """
  @spec open(mode(), inputs(), keyword()) :: {:ok, Loop.t()} | {:error, map()}
  def open(:run, %{spec: spec, plan: plan}, opts) when is_map(spec) and is_map(plan) and is_list(opts) do
    opts = resolve_fresh_identity(opts)
    {:ok, %Loop{step: Reducer.init(spec, plan, opts), committed: [], runtime: Runtime.new(opts), opts: opts}}
  end

  def open(:resume, %{spec: spec, plan: plan, prior_lines: prior_lines}, opts)
      when is_map(spec) and is_map(plan) and is_list(prior_lines) and is_list(opts) do
    with {:ok, prior_events} <- decode(prior_lines),
         {:ok, opts} <- resolve_resume_identity(prior_lines, opts) do
      # The reducer validates each line as written and works on the upgraded views; the
      # decoded maps stay exact here, and they are what the result and the chain carry.
      opts = Keyword.put(opts, :prior_count, length(prior_events))
      step = Reducer.resume(spec, plan, prior_events, opts)
      {:ok, %Loop{step: step, committed: prior_events, runtime: Runtime.new(opts), opts: opts}}
    end
  end

  def open(:cancel, %{prior_lines: prior_lines}, opts) when is_list(prior_lines) and is_list(opts) do
    with {:ok, prior_events} <- decode(prior_lines) do
      opts = Keyword.put(opts, :prior_count, length(prior_events))
      step = Reducer.cancel(prior_events, opts)
      {:ok, %Loop{step: step, committed: prior_events, runtime: Runtime.new(opts), opts: opts}}
    end
  end

  # INTERNAL continuation of an already-accepted command (unit C): selected only by the run server from
  # the locked, verified prefix; `acceptance` names the durable acceptance row (seq, verb) and the reducer
  # validates it against the prefix. No public verb opens this mode.
  def open(:continue, %{prior_lines: prior_lines, acceptance: %{seq: seq, verb: verb} = acceptance} = inputs, opts)
      when is_list(prior_lines) and is_integer(seq) and is_binary(verb) and is_list(opts) do
    with {:ok, prior_events} <- decode(prior_lines),
         {:ok, opts} <- resolve_resume_identity(prior_lines, opts) do
      opts = Keyword.put(opts, :prior_count, length(prior_events))
      step = Reducer.continue(Map.get(inputs, :spec), Map.get(inputs, :plan), prior_events, acceptance, opts)
      {:ok, %Loop{step: step, committed: prior_events, runtime: Runtime.new(opts), opts: opts}}
    end
  end

  @doc """
  One loop step. `{:continue, loop}` after an executed effect; `{:halt, result}` once the reducer
  is done or rejected (every retained handle settled first). Trappable failures propagate after
  settlement, exactly as in `run/3`.
  """
  @spec advance(Loop.t()) :: {:continue, Loop.t()} | {:halt, {:ok, map()} | {:error, map()}}
  def advance(%Loop{step: {:error, rejection}, runtime: runtime}) when is_map_key(rejection, "reason") do
    case Effects.settle(runtime) do
      {[], _} -> {:halt, {:error, rejection}}
      {cleanup, _} -> {:halt, {:error, Map.put(rejection, "gate_cleanup", cleanup)}}
    end
  end

  def advance(%Loop{step: {:error, rejection}}), do: {:halt, {:error, rejection}}

  def advance(%Loop{step: {:done, _state, appended}, committed: committed, runtime: runtime, opts: opts} = loop) do
    with {:ok, committed} <- guard(runtime, fn -> commit(appended, committed, opts) end),
         runtime = Effects.release_terminal(runtime, appended),
         {:ok, folded} <- fold(committed) do
      result = %{events: committed, appended_events: appended(committed, opts), summary: Fold.summary(folded)}
      {cleanup, _empty} = Effects.settle(runtime)
      {:halt, {:ok, with_gate_cleanup(result, cleanup)}}
    else
      # a refused final commit is an error exit of the Host: every retained handle is settled first
      {:error, rejection} -> advance(%{loop | step: {:error, rejection}})
    end
  end

  def advance(%Loop{step: {:effect, intent, state, appended}, committed: committed, runtime: runtime, opts: opts} = loop) do
    case guard(runtime, fn -> commit(appended, committed, opts) end) do
      {:ok, committed} ->
        runtime = Effects.release_terminal(runtime, appended)
        inputs = [opts: opts, receipt: receipt_for(intent, committed)]
        {observation, runtime} = guard(runtime, fn -> Effects.execute(intent, runtime, inputs) end)
        guard(runtime, fn -> notify_observer(intent, observation, opts) end)
        step = guard(runtime, fn -> Reducer.step(state, observation) end)
        {:continue, %{loop | step: step, committed: committed, runtime: runtime}}

      # a refused commit is an error exit of the Host: every retained handle is settled first
      {:error, rejection} ->
        advance(%{loop | step: {:error, rejection}})
    end
  end

  # ---- the staged interface: the same stages as advance/1, split at the runtime boundary ----
  #
  # The Server commits through `commit_step/1` and steps through `resume_step/2`; the effect runtime never enters
  # these functions (the loop they hand back carries `runtime: nil`). Trappable failures of the sink or the
  # observer escape UNCHANGED and settle nothing here: settlement is the runtime owner's, requested by the driver.

  @doc """
  Commits the suffix the machine just emitted. `{:effect, stage}` when an effect must now be executed by the
  runtime owner; `{:done, stage, result}` when the terminal suffix committed (the result carries no cleanup yet);
  `{:rejected, rejection}` when the commit was refused or the step is a rejection: no stage exists, so nothing can
  be executed.
  """
  @spec commit_step(Loop.t()) :: {:effect, Stage.t()} | {:done, Stage.t(), map()} | {:rejected, map()}
  def commit_step(%Loop{step: {:error, rejection}}), do: {:rejected, rejection}

  def commit_step(%Loop{step: {:done, _state, appended}, committed: committed, opts: opts} = loop) do
    with {:ok, committed} <- commit(appended, committed, opts),
         {:ok, folded} <- fold(committed) do
      result = %{events: committed, appended_events: appended(committed, opts), summary: Fold.summary(folded)}
      {:done, stage(:done, loop, committed, nil, nil), result}
    else
      {:error, rejection} -> {:rejected, rejection}
    end
  end

  def commit_step(%Loop{step: {:effect, intent, _state, appended}, committed: committed, opts: opts} = loop) do
    case commit(appended, committed, opts) do
      {:ok, committed} -> {:effect, stage(:effect, loop, committed, intent, receipt_for(intent, committed))}
      {:error, rejection} -> {:rejected, rejection}
    end
  end

  @doc "Notifies the observer and steps the reducer with the observation the runtime owner produced."
  @spec resume_step(Stage.t(), struct()) :: {:continue, Loop.t()}
  def resume_step(%Stage{kind: :effect, loop: %Loop{step: {:effect, intent, state, _}} = loop}, observation) do
    notify_observer(intent, observation, loop.opts)
    {:continue, %{loop | step: Reducer.step(state, observation)}}
  end

  @doc "The done leg's cleanup merge: `gate_cleanup` only when the settlement reported something."
  @spec close_step(map(), [map()]) :: {:ok, map()}
  def close_step(%{} = result, cleanup) when is_list(cleanup), do: {:ok, with_gate_cleanup(result, cleanup)}

  @doc "The error leg's cleanup merge: `\"gate_cleanup\"` only when the settlement reported something."
  @spec reject_step(map(), [map()]) :: {:error, map()}
  def reject_step(%{} = rejection, []), do: {:error, rejection}

  def reject_step(%{} = rejection, cleanup) when is_list(cleanup),
    do: {:error, Map.put(rejection, "gate_cleanup", cleanup)}

  defp stage(kind, %Loop{committed: prior} = loop, committed, intent, receipt) do
    %Stage{
      kind: kind,
      loop: %{loop | committed: committed, runtime: nil},
      intent: intent,
      suffix: Enum.drop(committed, length(prior)),
      receipt: receipt,
      ref: make_ref()
    }
  end

  # ---- the loop ----

  defp drive({:error, rejection}), do: {:error, rejection}

  defp drive({:ok, %Loop{} = loop}) do
    case advance(loop) do
      {:continue, next} -> drive({:ok, next})
      {:halt, result} -> result
    end
  end

  # ---- the loop ----

  # Every trappable exit closes in this invocation: the LATEST runtime (the one an Interrupted
  # carries, or the one this stage holds) is settled, then the ORIGINAL kind, reason and stacktrace
  # propagate unchanged. Settlement never raises, so cleanup never replaces the primary failure.
  defp guard(%Runtime{} = runtime, fun) when is_function(fun, 0) do
    fun.()
  catch
    :error, %Effects.Interrupted{} = interrupted ->
      _ = Effects.settle(interrupted.runtime)
      :erlang.raise(interrupted.kind, interrupted.reason, interrupted.stacktrace)

    kind, reason ->
      _ = Effects.settle(runtime)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # receipt selection is the Host's: the persisted event whose seq is the journaled started_seq
  defp receipt_for(%Effect.ReleaseGate{started_seq: seq}, committed), do: Enum.find(committed, &(&1["seq"] == seq))
  defp receipt_for(_intent, _committed), do: nil

  defp with_gate_cleanup(result, []), do: result
  defp with_gate_cleanup(result, cleanup), do: Map.put(result, :gate_cleanup, cleanup)

  # Stamps and journals the suffix the machine just emitted, after proving it continues the
  # journal without a gap or an overlap.
  defp commit(suffix, committed, opts) do
    expected = length(committed) + 1

    case suffix do
      [] ->
        {:ok, committed}

      [%{"seq" => ^expected} | _] ->
        stamp_and_sink(suffix, committed, opts)

      [%{"seq" => seq} | _] ->
        {:error, %{"reason" => "event_sequence_gap", "committed" => length(committed), "seq" => seq}}
    end
  end

  defp stamp_and_sink(suffix, committed, opts) do
    clock = Keyword.get(opts, :clock, SystemClock)
    sink = Keyword.get(opts, :event_sink)

    Enum.reduce_while(suffix, {:ok, committed}, fn event, {:ok, acc} ->
      stamped = Map.put(event, "ts", clock.wall_ts())

      case persist(sink, stamped) do
        {:ok, persisted} -> {:cont, {:ok, acc ++ [persisted]}}
        {:error, rejection} -> {:halt, {:error, sink_failure(rejection)}}
      end
    end)
  end

  defp persist(nil, event), do: {:ok, event}

  defp persist(sink, event) when is_function(sink, 1) do
    case sink.(event) do
      :ok -> {:ok, event}
      {:ok, %{} = persisted} -> {:ok, persisted}
      {:error, rejection} -> {:error, rejection}
      other -> {:error, Map.put(Diagnostic.describe(other), "clause", "invalid_sink_result")}
    end
  end

  defp notify_observer(effect, observation, opts) do
    case Keyword.get(opts, :effect_observer) do
      nil -> :ok
      observer when is_function(observer, 2) -> observer.(effect, observation)
    end
  end

  defp resolve_fresh_identity(opts) do
    id = Keyword.get(opts, :id, SystemId)

    opts
    |> Keyword.put_new_lazy(:run_id, &id.run_id/0)
    |> Keyword.put_new_lazy(:supervisor_instance, &id.supervisor_instance/0)
  end

  defp resolve_resume_identity(prior_lines, opts) do
    id = Keyword.get(opts, :id, SystemId)

    with {:ok, state} <- Fold.fold_lines(prior_lines) do
      {:ok,
       opts
       |> Keyword.put_new(:run_id, state.run_id)
       |> Keyword.put_new_lazy(:supervisor_instance, &id.supervisor_instance/0)}
    end
  end

  defp appended(committed, opts) do
    case Keyword.get(opts, :prior_count) do
      n when is_integer(n) -> Enum.drop(committed, n)
      _ -> committed
    end
  end

  defp fold(events), do: Fold.fold_events(events)

  defp decode(lines) do
    Enum.reduce_while(lines, {:ok, []}, fn line, {:ok, events} ->
      case Jason.decode(line) do
        {:ok, event} -> {:cont, {:ok, events ++ [event]}}
        {:error, _reason} -> {:halt, {:error, %{clause: "invalid_event_shape"}}}
      end
    end)
  end

  defp sink_failure(%{} = rejection) do
    rejection
    |> Map.new(fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
    |> Map.put("reason", "journal_append_failed")
  end

  defp sink_failure(other), do: Map.put(Diagnostic.describe(other), "reason", "journal_append_failed")
end
