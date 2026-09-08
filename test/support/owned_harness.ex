defmodule AiOrchestrator.Test.OwnedHarness do
  @moduledoc """
  TEST-ONLY tracked-ownership harness (ported from the executor suite's E-M10 / E-M15 / E-M17 rules, EO-M4).

  Every trace message a subject emits passes through a test-owned COLLECTOR that registers each pid the message
  names under the test's key BEFORE forwarding the message to the test, so a partial start, a failed assertion or
  a missing final notification still leaves every learned identity registered for the reaper. Callers spawned by a
  test are tracked at spawn. Teardown is a protocol, not a snapshot: reap the tracked pids, flush the collector so
  every identity published before they died is registered, reap again, until the set closes (bounded rounds); the
  collector is stopped last and temporary directories are removed after the processes that may hold them.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  defmodule Seam do
    @moduledoc false
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
    def put(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))
    def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))
    def push(key, value), do: Agent.update(__MODULE__, &Map.update(&1, key, [value], fn l -> [value | l] end))
  end

  @doc "Call from the test process (setup or test body): starts the collector and registers the teardown."
  @spec setup_owned() :: %{collector: pid(), key: {:tracked, pid()}}
  def setup_owned do
    case Seam.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    test = self()
    key = {:tracked, test}
    Seam.put(key, [])
    Seam.put({:dirs, test}, [])
    Seam.put({:oracles, test}, [])
    collector = start_collector!(test, key)
    Seam.put({:collector, test}, collector)
    on_exit(fn -> teardown_tracked!(key, collector) end)
    %{collector: collector, key: key}
  end

  @doc "The test's collector: pass it as the subject's `trace` so every trace is registered before it is seen."
  def collector, do: Seam.get({:collector, self()})

  def track!(pids) when is_list(pids), do: Enum.each(pids, &Seam.push({:tracked, self()}, &1))
  def track!(pid), do: track!([pid])

  def track_dir!(dir), do: Seam.push({:dirs, self()}, dir)

  @doc "Flush the collector: every trace queued before this call has been registered and forwarded."
  def flush!, do: flush_collector!(collector(), 5_000)

  @doc """
  Spawn a caller that runs `fun` and sends `{:result, fun.()}` to the test. The caller is REGISTERED before it is
  permitted to start: it waits for the test's permit, which is sent only after `track!/1` recorded its pid, so no
  user work (and no pid it could learn) precedes ownership.
  """
  def spawn_caller!(fun) when is_function(fun, 0) do
    parent = self()
    permit = make_ref()

    {pid, mon} =
      spawn_monitor(fn ->
        receive do
          {:start, ^permit} -> send(parent, {:result, fun.()})
        after
          5_000 -> exit(:caller_never_permitted)
        end
      end)

    track!(pid)
    send(pid, {:start, permit})
    {pid, mon}
  end

  @doc """
  Register an OS-absence oracle (a zero-arity function returning true when the owned OS group is gone). Oracles run
  inside the harness teardown AFTER every BEAM owner is reaped and BEFORE directories are removed - not as a
  separate on_exit, whose LIFO order would run it before the reaper.
  """
  def os_oracle!(fun) when is_function(fun, 0), do: Seam.push({:oracles, self()}, fun)

  @doc "Run the teardown protocol now (idempotent; the registered on_exit repeats it harmlessly)."
  def close!, do: teardown_tracked!({:tracked, self()}, collector())

  @doc "Every pid the harness owns right now (for controls)."
  def owned, do: Seam.get({:tracked, self()}) || []

  # ---- teardown protocol ----

  defp teardown_tracked!({:tracked, test} = key, collector) do
    teardown_round!(key, collector, MapSet.new([collector]), 0)
  after
    _ = reap_all!([collector | Enum.filter(Seam.get(key) || [], &Process.alive?/1)])
    # OS oracles only after every BEAM owner is gone: a live owner could still be settling its group
    oracles = Seam.get({:oracles, test}) || []
    dirs = Seam.get({:dirs, test}) || []
    # consumed once: the registered on_exit repeats the protocol without re-running settled oracles
    Seam.put({:oracles, test}, [])
    Seam.put({:dirs, test}, [])
    for oracle <- oracles, do: oracle_settled!(oracle, 15_000)
    for dir <- dirs, do: File.rm_rf(dir)
  end

  defp oracle_settled!(oracle, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    settled = Stream.repeatedly(fn -> oracle.() or (System.monotonic_time(:millisecond) > deadline and :expired) end)
    if Enum.find(settled, &(&1 != false)) == :expired, do: raise("an owned OS group is still present after the reaper")
    Process.sleep(0)
  end

  defp teardown_round!(_key, _collector, _done, 6), do: raise("owned set did not close after six teardown rounds")

  defp teardown_round!(key, collector, done, round) do
    known = Enum.reject(Seam.get(key) || [], &MapSet.member?(done, &1))
    _ = reap_all!(known)
    done = Enum.reduce(known, done, &MapSet.put(&2, &1))
    :ok = flush_collector!(collector, 5_000)
    remaining = (Seam.get(key) || []) |> Enum.reject(&MapSet.member?(done, &1)) |> Enum.filter(&Process.alive?/1)
    if remaining == [], do: MapSet.to_list(done), else: teardown_round!(key, collector, done, round + 1)
  end

  defp flush_collector!(collector, timeout) do
    if is_pid(collector) and Process.alive?(collector) do
      ref = make_ref()
      send(collector, {:flush, ref, self()})

      receive do
        {:flushed, ^ref} -> :ok
      after
        timeout -> raise("the collector did not flush within #{timeout} ms")
      end
    else
      :ok
    end
  end

  defp start_collector!(test, key) do
    collector = spawn(fn -> collector_loop(test, key) end)
    Seam.push(key, collector)
    collector
  end

  defp collector_loop(test, key) do
    receive do
      {:flush, ref, from} ->
        send(from, {:flushed, ref})
        collector_loop(test, key)

      message ->
        for pid <- pids_in(message), do: Seam.push(key, pid)
        send(test, message)
        collector_loop(test, key)
    end
  end

  # every trace shape any owned subject emits; a pid named anywhere in it is owned by the test
  defp pids_in({:run_child_started, sup, _id, pid}), do: [sup, pid]
  defp pids_in({:run_executor_started, owner, sup}), do: [owner, sup]
  defp pids_in({:run_server_driving, server, _facts}), do: [server]
  defp pids_in({:subtree_started, facts, _ref}) when is_map(facts), do: pids_of(facts)
  defp pids_in({:facts, facts}) when is_map(facts), do: pids_of(facts)
  defp pids_in({:gate_entered, pid}), do: [pid]
  defp pids_in({:deliver_entered, pid}), do: [pid]
  defp pids_in({:identity, _identity, pid}), do: [pid]
  defp pids_in({:run_worker_registered, _ref, worker, server}), do: [worker, server]
  defp pids_in({:run_effect_requested, server, %{worker: worker}}), do: [server, worker]
  defp pids_in({:run_effect_applied, server, _key}), do: [server]
  defp pids_in({:run_effect_reply_dropped, server, _reason}), do: [server]
  defp pids_in(_other), do: []

  defp pids_of(facts), do: facts |> Map.values() |> Enum.filter(&is_pid/1)

  @doc "Kill + reap every still-alive pid, bounded; survivors are a failure, never success."
  def reap_all!(pids) do
    alive = Enum.filter(pids, &(is_pid(&1) and Process.alive?(&1)))
    refs = for pid <- alive, do: {pid, Process.monitor(pid)}
    for pid <- alive, do: Process.exit(pid, :kill)

    survivors =
      for {pid, ref} <- refs,
          (receive do
             {:DOWN, ^ref, :process, ^pid, _} -> false
           after
             5_000 -> true
           end),
          do: pid

    if survivors != [], do: flunk("tracked processes survived the reaper: #{length(survivors)}")
    alive
  end
end
