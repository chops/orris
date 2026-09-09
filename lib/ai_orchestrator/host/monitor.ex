defmodule AiOrchestrator.Host.Monitor do
  @moduledoc """
  The observational registry of live run subtrees in this BEAM (R5: one monitor process).

  State: `by_dir` (canonical run directory -> identity record, unique), `by_id` (run id -> the set of
  directories carrying it, non-unique) and a process monitor on every registered owner. Registration
  and unregistration are casts: they never block or fail the caller. Cleanup is the owner's DOWN
  (authoritative) and the idempotent caller-side unregister.

  Stale-cleanup protection: an entry carries its owner pid and generation, and only a DOWN whose
  monitor reference belongs to the current entry, or an unregister naming the current owner AND
  generation, removes it. A registration whose owner is already dead is dropped by its immediate DOWN
  and never persists. No client can remove another owner's entry.
  """

  use GenServer

  @record_keys [:run_dir, :run_id, :owner, :supervisor, :server, :writer, :worker, :generation]

  @type entry :: %{
          run_dir: Path.t(),
          run_id: String.t(),
          owner: pid(),
          supervisor: pid(),
          server: pid(),
          writer: pid(),
          worker: pid(),
          generation: pos_integer()
        }

  @doc """
  Starts a monitor, registered as `__MODULE__` unless `:name` says otherwise; `name: nil` starts an
  unregistered instance that only callers addressing it by pid reach (test-owned instances).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, registration(name))
  end

  defp registration(nil), do: []
  defp registration(name), do: [name: name]

  @doc "Records a complete identity record (asynchronous, fail-soft; an incomplete record is ignored)."
  @spec register(GenServer.server(), entry()) :: :ok
  def register(monitor, record) when is_map(record), do: GenServer.cast(monitor, {:register, record})

  @doc "Removes the entry for `record.run_dir` only if it names the current owner and generation."
  @spec unregister(GenServer.server(), entry()) :: :ok
  def unregister(monitor, record) when is_map(record), do: GenServer.cast(monitor, {:unregister, record})

  @impl true
  def init(_opts), do: {:ok, %{by_dir: %{}, by_id: %{}, refs: %{}}}

  @impl true
  def handle_call({:lookup, run_dir}, _from, state), do: {:reply, Map.get(state.by_dir, run_dir), state}

  def handle_call({:lookup_run_id, run_id}, _from, state) do
    entries = state.by_id |> Map.get(run_id, MapSet.new()) |> Enum.map(&Map.fetch!(state.by_dir, &1))
    {:reply, entries, state}
  end

  @impl true
  # the incoming registration is judged in full (complete record AND live owner) BEFORE the current
  # entry is touched: a rejected registration, including a late one from an owner that has since
  # died, mutates neither the current entry nor its monitor reference
  def handle_cast({:register, record}, state) do
    case complete(record) do
      {:ok, %{owner: owner} = record} ->
        ref = Process.monitor(owner)

        if Process.alive?(owner) do
          {:noreply, state |> drop(record.run_dir) |> index(record, ref)}
        else
          Process.demonitor(ref, [:flush])
          {:noreply, state}
        end

      :error ->
        {:noreply, state}
    end
  end

  def handle_cast({:unregister, %{run_dir: run_dir} = record}, state) when is_binary(run_dir) do
    run_dir = Path.expand(run_dir)
    claimed = {Map.get(record, :owner), Map.get(record, :generation)}

    case Map.get(state.by_dir, run_dir) do
      %{owner: owner, generation: generation} when {owner, generation} == claimed -> {:noreply, drop(state, run_dir)}
      _stale_or_absent -> {:noreply, state}
    end
  end

  def handle_cast(_other, state), do: {:noreply, state}

  # only the DOWN carrying the CURRENT entry's monitor reference removes it: an old owner's late DOWN
  # finds its reference gone (demonitored when the replacement registered) and touches nothing
  @impl true
  def handle_info({:DOWN, ref, :process, _owner, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} -> {:noreply, state}
      {run_dir, refs} -> {:noreply, drop(%{state | refs: refs}, run_dir)}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp complete(record) do
    with true <- Enum.all?(@record_keys, &Map.has_key?(record, &1)),
         true <- is_binary(record.run_dir) and is_binary(record.run_id),
         true <- Enum.all?([:owner, :supervisor, :server, :writer, :worker], &is_pid(record[&1])),
         true <- is_integer(record.generation) and record.generation > 0 do
      {:ok, record |> Map.take(@record_keys) |> Map.put(:run_dir, Path.expand(record.run_dir))}
    else
      _ -> :error
    end
  end

  defp index(state, %{run_dir: run_dir, run_id: run_id} = record, ref) do
    %{
      state
      | by_dir: Map.put(state.by_dir, run_dir, record),
        by_id: Map.update(state.by_id, run_id, MapSet.new([run_dir]), &MapSet.put(&1, run_dir)),
        refs: Map.put(state.refs, ref, run_dir)
    }
  end

  defp drop(state, run_dir) do
    case Map.pop(state.by_dir, run_dir) do
      {nil, _by_dir} ->
        state

      {%{run_id: run_id}, by_dir} ->
        {refs_for_dir, refs} = Enum.split_with(state.refs, fn {_ref, dir} -> dir == run_dir end)
        Enum.each(refs_for_dir, fn {ref, _dir} -> Process.demonitor(ref, [:flush]) end)

        dirs = state.by_id |> Map.get(run_id, MapSet.new()) |> MapSet.delete(run_dir)
        by_id = if MapSet.size(dirs) == 0, do: Map.delete(state.by_id, run_id), else: Map.put(state.by_id, run_id, dirs)

        %{state | by_dir: by_dir, by_id: by_id, refs: Map.new(refs)}
    end
  end
end
