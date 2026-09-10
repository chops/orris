defmodule AiOrchestrator.Test.StartupCleanup do
  @moduledoc """
  TEST-ONLY shared cleanup owner for the core-startup-bound RED files (docs/contracts/core-startup-bound.org, R2 of the
  bc3fb42 review). One tracker per row (an unlinked Agent) holds every owned pid, every filesystem gate and every
  test-owned directory. `row/2` CATCHES the body's outcome (rescue, and catch of exits and throws), then runs
  `cleanup!/1`: gates released, every owned identity killed and joined with recorded survivors, a SCOPED census of
  run-tree processes born since the row's baseline (helper/starter/server/work/worker included), directories removed
  only when nothing survived; the original failure is re-raised annotated with the cleanup result and a clean body with
  survivors fails. There is no literal try/after: a body whose process is killed or times out cannot be caught here, so
  `setup_row/1` registers an idempotent on_exit fallback that runs the same `cleanup!/1` (CO-2 proves it).
  """
  import ExUnit.Assertions, only: [flunk: 1]

  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run

  @type ctx :: %{tracker: pid(), baseline: MapSet.t(), dir: Path.t()}

  @doc "Call from `setup` (the test process): starts the tracker, snapshots the census baseline, registers the fallback."
  @spec setup_row(Path.t()) :: ctx()
  def setup_row(dir) do
    {:ok, tracker} = Agent.start(fn -> %{pids: [], gates: [], dirs: [dir], done: nil} end)
    File.mkdir_p!(dir)
    ctx = %{tracker: tracker, baseline: MapSet.new(census_pids()), dir: dir}

    ExUnit.Callbacks.on_exit(fn ->
      # idempotent fallback: a row whose wrapper never reached its after-path (external kill, timeout) is cleaned here
      case cleanup!(ctx) do
        :already -> :ok
        %{survivors: [], leaked: [], dirs_left: []} -> :ok
        bad -> raise "startup cleanup fallback found #{inspect(bad)}"
      end

      if Process.alive?(tracker), do: Agent.stop(tracker)
    end)

    ctx
  end

  @spec own(ctx(), pid() | [pid() | nil] | nil) :: :ok
  def own(%{tracker: t}, pids),
    do: Agent.update(t, fn s -> %{s | pids: Enum.filter(List.wrap(pids), &is_pid/1) ++ s.pids} end)

  @spec gate(ctx(), pid() | nil) :: :ok
  def gate(%{tracker: t}, pid) when is_pid(pid), do: Agent.update(t, fn s -> %{s | gates: [pid | s.gates]} end)
  def gate(_ctx, _), do: :ok

  @spec dir(ctx(), Path.t()) :: :ok
  def dir(%{tracker: t}, path), do: Agent.update(t, fn s -> %{s | dirs: [path | s.dirs]} end)

  @doc "Catches `body`'s outcome, runs `cleanup!/1`, then re-raises annotated or fails on cleanup findings."
  @spec row(ctx(), (-> any())) :: :ok
  def row(ctx, body) do
    outcome =
      try do
        body.()
        :ok
      rescue
        e -> {:error, e, __STACKTRACE__}
      catch
        kind, reason -> {kind, reason, __STACKTRACE__}
      end

    cleanup = cleanup!(ctx)

    case {outcome, cleanup} do
      {:ok, %{survivors: [], leaked: [], dirs_left: []}} ->
        :ok

      {:ok, :already} ->
        :ok

      {:ok, bad} ->
        flunk("row cleanup found #{inspect(bad)}")

      {{:error, %ExUnit.AssertionError{message: m} = e, st}, bad} ->
        reraise %{e | message: "#{m}\n(cleanup: #{render(bad)})"}, st

      {{:error, e, st}, _bad} ->
        reraise e, st

      {{kind, reason, st}, _bad} ->
        :erlang.raise(kind, reason, st)
    end
  end

  # The annotation is rendered field by field, never by inspecting the map: map key order is not stable across VM
  # instances, so an inspected cleanup result makes any assertion about the annotation intermittent.
  defp render(%{survivors: survivors, leaked: leaked, dirs_left: dirs_left}),
    do: "%{survivors: #{inspect(survivors)}, leaked: #{inspect(leaked)}, dirs_left: #{inspect(dirs_left)}}"

  defp render(other), do: inspect(other)

  @doc "Idempotent: the first call cleans and records; later calls answer :already."
  @spec cleanup!(ctx()) :: :already | %{survivors: [pid()], leaked: [{term(), pid()}], dirs_left: [Path.t()]}
  def cleanup!(%{tracker: t, baseline: baseline}) do
    if Process.alive?(t), do: tracked(t, baseline), else: :already
  end

  defp tracked(t, baseline) do
    case Agent.get(t, & &1) do
      %{done: done} when is_map(done) -> :already
      %{pids: pids, gates: gates, dirs: dirs} -> reap(t, baseline, pids, gates, dirs)
    end
  end

  defp reap(t, baseline, pids, gates, dirs) do
    for g <- gates, Process.alive?(g), do: send(g, :unblock)
    survivors = kill_join(Enum.uniq(pids), 2_000)
    # everything the row's identities gave birth to and did not track is reaped and reported as leaked
    born = Enum.reject(census_pids(), &MapSet.member?(baseline, &1))
    late = kill_join(born, 2_000)
    leaked = for p <- born, do: {initial_call(p), p}
    remaining = Enum.reject(census_pids(), &MapSet.member?(baseline, &1))
    alive = survivors ++ late ++ remaining
    result = %{survivors: alive, leaked: leaked, dirs_left: dirs_left(dirs, alive)}
    Agent.update(t, &%{&1 | done: result})
    result
  end

  # directories go only when no owned process survived (a survivor may still hold them)
  defp dirs_left(dirs, []) do
    for d <- Enum.uniq(dirs), do: File.rm_rf!(d)
    Enum.filter(Enum.uniq(dirs), &File.exists?/1)
  end

  defp dirs_left(dirs, _alive), do: Enum.filter(Enum.uniq(dirs), &File.exists?/1)

  @doc "Kills each live pid and joins its DOWN; answers the pids whose DOWN was not observed within `join_ms`."
  @spec kill_join([pid()], timeout()) :: [pid()]
  def kill_join(pids, join_ms) do
    mons = for p <- pids, is_pid(p), Process.alive?(p), do: {p, Process.monitor(p)}
    for {p, _} <- mons, do: Process.exit(p, :kill)

    for {p, m} <- mons,
        not (receive do
               {:DOWN, ^m, :process, ^p, _} -> true
             after
               join_ms -> false
             end),
        do: p
  end

  @doc "Every run-tree process in the VM: supervisor, Writer, RunOwner, Server, Work supervisor, Worker and any startup-seam process."
  @spec census_pids() :: [pid()]
  def census_pids, do: for(p <- Process.list(), run_tree?(initial_call(p)), do: p)

  @spec initial_call(pid()) :: term()
  def initial_call(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, d} -> Keyword.get(d, :"$initial_call")
      nil -> :dead
    end
  end

  defp run_tree?({:supervisor, Run.Supervisor, _}), do: true
  defp run_tree?({:supervisor, Run.Work.Supervisor, _}), do: true
  defp run_tree?({Writer, :init, _}), do: true
  defp run_tree?({RunOwner, :init, _}), do: true
  defp run_tree?({Run.Server, :init, _}), do: true
  defp run_tree?({Run.Worker, _, _}), do: true

  defp run_tree?({mod, _, _}) when is_atom(mod),
    do: String.starts_with?(Atom.to_string(mod), "Elixir.AiOrchestrator.Run.Executor.Startup")

  defp run_tree?(_), do: false
end
