defmodule OrrisConsole.Reader do
  @moduledoc """
  The read cycle shared by both views (C1-09/11/12): one admitted read per view at a time through QueryJob; the
  pure `deliver/2` accepts a result only when the session is still valid AND the result is the view's current
  admitted read; an accepted failure never erases the last successful value (it is marked stale with its
  last-success timestamp); a refusal or the loss of the current controller marks stale (or unavailable before any
  success) with "refresh delayed"; exactly one retry timer, retry_ms after completion, refusal or controller loss.
  The current controller is monitored by its exact reference; a stale DOWN is ignored. Trusted test seams:
  `read_gate` (the worker announces and waits) and `read_witness` (the applied correlation), server-side only.
  """
  alias OrrisConsole.{AuthHook, Config, QueryJob}

  def initial(session_id) do
    %{
      valid?: fn -> AuthHook.valid?(session_id) end,
      current: nil,
      monitor: nil,
      data: nil,
      last_result: nil,
      dropped: 0,
      stale?: false,
      delayed?: false,
      last_success_ms: nil,
      timer: nil
    }
  end

  @doc """
  Pure delivery: accepted only for a valid session and the exact current {job, correlation}; otherwise dropped.
  An accepted success replaces the retained value; an accepted failure keeps the last successful value.
  """
  @spec deliver(map(), {:query_result, pid() | term(), term(), term()}) :: map()
  def deliver(state, {:query_result, job, correlation, result}) do
    cond do
      not (state.valid?.() and state.current == {job, correlation}) ->
        Map.update(state, :dropped, 1, &(&1 + 1))

      match?({:ok, _}, result) ->
        Map.merge(state, %{data: result, current: nil, last_result: result})

      true ->
        Map.merge(state, %{current: nil, last_result: result})
    end
  end

  @doc "Admits a read for the view (self()) unless one is current; the controller is monitored; answers the state."
  def admit(state, read) do
    config = Config.current()

    if current_alive?(state) do
      state
    else
      # a current read whose controller already died (its DOWN not yet observed) is a LOST read: it is marked
      # stale/delayed before the replacement is admitted, and its late DOWN is then ignored
      state = if Map.get(state, :current) != nil, do: state |> tap(&demonitor/1) |> refused(), else: state
      correlation = System.unique_integer([:positive, :monotonic])
      fun = gated(read, correlation, config)

      case QueryJob.start(self(), fun, correlation: correlation, deadline_ms: config.read_deadline_ms) do
        {:ok, job} -> %{state | current: {job, correlation}, monitor: Process.monitor(job)}
        {:error, _refused} -> state |> refused() |> schedule(config)
      end
    end
  end

  @doc "Applies a delivered result; answers {state, :applied | :dropped}."
  def receive_result(state, {:query_result, _job, correlation, _result} = message) do
    before = Map.get(state, :dropped, 0)
    delivered = deliver(state, message)
    config = Config.current()

    if Map.get(delivered, :dropped, 0) == before do
      demonitor(state)

      state =
        case delivered.last_result do
          {:ok, _} ->
            Map.merge(delivered, %{monitor: nil, stale?: false, delayed?: false, last_success_ms: clock(config)})

          _failure ->
            delivered |> Map.put(:monitor, nil) |> refused()
        end

      if config.read_witness, do: send(config.read_witness, {:read_applied, self(), correlation})
      {schedule(state, config), :applied}
    else
      {schedule(delivered, config), :dropped}
    end
  end

  @doc """
  A DOWN for the CURRENT controller (exact monitor reference): the read is lost, the last value is marked stale (or
  the view stays unavailable), one retry is scheduled; any other DOWN is ignored. Answers {state, :current_lost | :ignored}.
  """
  def controller_down(%{monitor: ref} = state, ref) when is_reference(ref) do
    {state |> Map.put(:monitor, nil) |> refused() |> schedule(Config.current()), :current_lost}
  end

  def controller_down(state, _ref), do: {state, :ignored}

  def current_alive?(%{current: {job, _}}), do: Process.alive?(job)
  def current_alive?(_), do: false

  defp demonitor(%{monitor: ref}) when is_reference(ref), do: Process.demonitor(ref, [:flush])
  defp demonitor(_), do: :ok

  defp refused(state), do: Map.merge(state, %{stale?: Map.get(state, :data) != nil, delayed?: true, current: nil})

  # exactly one pending retry timer
  defp schedule(state, config) do
    if Map.get(state, :timer), do: Process.cancel_timer(state.timer)
    Map.put(state, :timer, Process.send_after(self(), :refresh, config.retry_ms))
  end

  defp gated(read, _correlation, %Config{read_gate: nil}), do: fn -> tag(read.()) end

  defp gated(read, correlation, %Config{read_gate: gate}) do
    fn ->
      send(gate, {:read_gate, self(), correlation})

      receive do
        :go -> tag(read.())
      end
    end
  end

  defp tag({:ok, value}), do: {:ok, value}
  defp tag({:error, reason}), do: {:error, reason}
  defp tag(_other), do: {:error, :failed}

  def clock(%Config{clock: nil}), do: System.monotonic_time(:millisecond)
  def clock(%Config{clock: clock}), do: clock.()

  @doc "The last successful value, or nil."
  def value(%{data: {:ok, value}}), do: value
  def value(_), do: nil
end
