defmodule OrrisConsole.QueryJob do
  @moduledoc """
  The responsive controller of one read (C1-11): admission refuses while the view's worker or controller key exists
  (:view_busy) or the controller cap is reached (:capacity); the worker is started under QueryWorkers synchronously
  (worker cap -> :capacity), linked to this controller before it registers and acknowledges, and told to read.
  The controller monitors the view and the worker, kills the worker at the deadline or on view loss, delivers
  {:query_result, self(), correlation, result} to the view ONLY after the worker's actual DOWN, and keeps the
  controller key until then. Ordinary stop kills the worker; the link covers a controller :kill.
  """
  use GenServer, restart: :temporary

  alias OrrisConsole.{Config, QueryControllers, QueryRegistry, QueryWorkers}

  @spec start(pid(), (-> term()), keyword()) :: {:ok, pid()} | {:error, :capacity | :view_busy | :unavailable}
  def start(view, fun, opts \\ []) when is_pid(view) and is_function(fun, 0) do
    cond do
      occupied?({:worker, view}) or occupied?({:controller, view}) ->
        {:error, :view_busy}

      true ->
        case DynamicSupervisor.start_child(QueryControllers, {__MODULE__, {view, fun, opts}}) do
          {:ok, pid} -> {:ok, pid}
          {:error, :max_children} -> {:error, :capacity}
          {:error, {:shutdown, reason}} when reason in [:capacity, :view_busy] -> {:error, reason}
          {:error, _} -> {:error, :unavailable}
        end
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  def child_spec({view, fun, opts}),
    do: %{id: make_ref(), start: {__MODULE__, :start_link, [{view, fun, opts}]}, restart: :temporary}

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  # a dead pid may linger in the Registry for a moment after its exit: only a live holder occupies the key
  defp occupied?(key), do: Enum.any?(Registry.lookup(QueryRegistry, key), fn {pid, _} -> Process.alive?(pid) end)

  @impl true
  def init({view, fun, opts}) do
    Process.flag(:trap_exit, true)

    with {:ok, _} <- Registry.register(QueryRegistry, {:controller, view}, :controller),
         {:ok, worker} <- start_worker(view, fun) do
      vref = Process.monitor(view)
      wref = Process.monitor(worker)
      deadline = Keyword.get(opts, :deadline_ms, Config.current().read_deadline_ms)
      timer = Process.send_after(self(), :deadline, deadline)
      send(worker, {:run, self()})

      {:ok,
       %{
         view: view,
         vref: vref,
         worker: worker,
         wref: wref,
         correlation: Keyword.get(opts, :correlation),
         result: nil,
         timer: timer
       }}
    else
      {:error, {:already_registered, _}} -> {:stop, {:shutdown, :view_busy}}
      {:error, :max_children} -> {:stop, {:shutdown, :capacity}}
      {:error, :view_busy} -> {:stop, {:shutdown, :view_busy}}
      {:error, _} -> {:stop, {:shutdown, :unavailable}}
    end
  end

  defp start_worker(view, fun) do
    DynamicSupervisor.start_child(QueryWorkers, {OrrisConsole.QueryWorker, {self(), view, fun}})
  end

  @impl true
  def handle_info({:worker_value, worker, value}, %{worker: worker} = s), do: {:noreply, %{s | result: value}}

  def handle_info(:deadline, s) do
    if s.worker, do: Process.exit(s.worker, :kill)
    {:noreply, %{s | result: s.result || {:error, :deadline}}}
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{vref: ref} = s) do
    if s.worker, do: Process.exit(s.worker, :kill)
    {:noreply, %{s | view: nil}}
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{wref: ref} = s) do
    if s.view, do: send(s.view, {:query_result, self(), s.correlation, s.result || {:error, :down}})
    {:stop, :normal, %{s | worker: nil}}
  end

  def handle_info(_other, s), do: {:noreply, s}

  @impl true
  def terminate(_reason, s) do
    if s.timer, do: Process.cancel_timer(s.timer)
    if s.worker, do: Process.exit(s.worker, :kill)
    :ok
  end

  @impl true
  def format_status(status), do: Map.put(status, :state, :redacted)
end
