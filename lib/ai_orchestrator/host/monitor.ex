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

  alias AiOrchestrator.Host.RunOwner

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

  @default_census_timeout 1_000
  @census_batch 64

  @impl true
  def init(opts) do
    # exits are trapped so the census tasks are LINKED (this process's death ends them) without their failure
    # ending this process
    Process.flag(:trap_exit, true)

    state = %{
      by_dir: %{},
      by_id: %{},
      refs: %{},
      census: :pending,
      pending: %{},
      confirming: %{},
      skipped: 0,
      task: nil,
      epoch: nil,
      deadline_ms: nil
    }

    {:ok, start_census(state, opts)}
  end

  # The census (docs/contracts/host-mounted-runs.org): a LINKED task performs the discovery and sends the
  # requests in batches, each batch's pending references first announced to this process by an ACKNOWLEDGED
  # call (a happens-before, not a cross-recipient ordering assumption); the census carries an epoch, is
  # bounded by census_timeout measured from init, and at the deadline the task and every confirmation in
  # flight are killed, the unanswered requests counted as skipped, and later replies for that epoch dropped.
  defp start_census(state, opts) do
    case Keyword.get(opts, :host_supervisor) do
      nil ->
        %{state | census: :complete}

      supervisor ->
        monitor = self()
        epoch = make_ref()
        timeout = Keyword.get(opts, :census_timeout, @default_census_timeout)
        task = spawn_link(fn -> census_requests(supervisor, monitor, epoch) end)
        Process.send_after(monitor, {:census_deadline, epoch}, timeout)
        %{state | task: task, epoch: epoch, deadline_ms: System.monotonic_time(:millisecond) + timeout}
    end
  end

  defp census_requests(supervisor, monitor, epoch) do
    targets =
      try do
        for {_, pid, _, _} <- DynamicSupervisor.which_children(supervisor), is_pid(pid), do: {pid, make_ref()}
      catch
        :exit, _ -> []
      end

    for batch <- Enum.chunk_every(targets, @census_batch) do
      :ok = GenServer.call(monitor, {:census_pending, epoch, Enum.map(batch, &elem(&1, 1))})
      for {pid, ref} <- batch, do: send(pid, {:census, ref, monitor})
    end
  end

  @impl true
  def handle_call({:lookup, run_dir}, _from, state), do: {:reply, Map.get(state.by_dir, run_dir), state}

  def handle_call(:census, _from, state), do: {:reply, %{census: state.census, skipped: state.skipped}, state}

  def handle_call({:census_pending, epoch, refs}, _from, %{epoch: epoch, census: :pending} = state),
    do: {:reply, :ok, %{state | pending: Enum.reduce(refs, state.pending, &Map.put(&2, &1, true))}}

  def handle_call({:census_pending, _epoch, _refs}, _from, state), do: {:reply, :ok, state}

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

  # ---- census messages ----
  @impl true
  def handle_info({:census_deadline, epoch}, %{epoch: epoch} = state) do
    if is_pid(state.task) and Process.alive?(state.task), do: Process.exit(state.task, :kill)
    for {_ref, %{task: task}} <- state.confirming, Process.alive?(task), do: Process.exit(task, :kill)
    skipped = map_size(state.pending) + map_size(state.confirming)
    {:noreply, %{state | census: :complete, skipped: skipped, pending: %{}, confirming: %{}, task: nil}}
  end

  def handle_info({:census_deadline, _old_epoch}, state), do: {:noreply, state}

  # eligibility: the request must still be pending in the current epoch; the owner's confirmation then runs
  # in a bounded linked task under the common deadline so this process stays responsive throughout
  def handle_info({:census_reply, ref, record, _phase}, %{census: :pending} = state) do
    if Map.has_key?(state.pending, ref) and is_map(record) and is_pid(Map.get(record, :owner)) do
      monitor = self()
      epoch = state.epoch
      budget = max(state.deadline_ms - System.monotonic_time(:millisecond), 1)

      task =
        spawn_link(fn -> send(monitor, {:census_confirmed, epoch, ref, record, confirmed_active?(record, budget)}) end)

      {:noreply,
       %{state | pending: Map.delete(state.pending, ref), confirming: Map.put(state.confirming, ref, %{task: task})}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:census_reply, _ref, _record, _phase}, state), do: {:noreply, state}

  def handle_info({:census_confirmed, epoch, ref, record, confirmed?}, %{epoch: epoch} = state) do
    case Map.pop(state.confirming, ref) do
      {nil, _} -> {:noreply, state}
      {_task, confirming} -> {:noreply, apply_census(%{state | confirming: confirming}, record, confirmed?)}
    end
  end

  def handle_info({:census_confirmed, _epoch, _ref, _record, _ok}, state), do: {:noreply, state}

  def handle_info({:EXIT, _task, _reason}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _owner, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} -> {:noreply, state}
      {run_dir, refs} -> {:noreply, drop(%{state | refs: refs}, run_dir)}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # a normal termination (supervisor shutdown, retirement) ends the census work it owns; a link alone would not
  @impl true
  def terminate(_reason, state) do
    if is_pid(state.task) and Process.alive?(state.task), do: Process.exit(state.task, :kill)
    for {_ref, %{task: task}} <- state.confirming, Process.alive?(task), do: Process.exit(task, :kill)
    :ok
  end

  defp confirmed_active?(%{owner: owner}, budget) when is_pid(owner) do
    RunOwner.inspect(owner, budget).phase not in [:terminal, :tearing_down]
  catch
    :exit, _ -> false
  end

  defp confirmed_active?(_record, _budget), do: false

  # census precedence: a confirmed reply never displaces a live different owner or a higher generation
  defp apply_census(state, _record, false), do: state

  defp apply_census(state, record, true) do
    case complete(record) do
      {:ok, record} ->
        if census_admissible?(state, record),
          do: state |> drop(record.run_dir) |> index(record, Process.monitor(record.owner)),
          else: state

      :error ->
        state
    end
  end

  # a live different owner or a higher generation is never displaced; a dead owner is never indexed
  defp census_admissible?(state, %{run_dir: run_dir, owner: owner, generation: generation}) do
    case Map.get(state.by_dir, run_dir) do
      %{owner: other} when other != owner and is_pid(other) -> not Process.alive?(other) and Process.alive?(owner)
      %{generation: existing} when existing > generation -> false
      _ -> Process.alive?(owner)
    end
  end

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
