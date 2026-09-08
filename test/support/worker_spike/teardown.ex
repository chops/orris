defmodule AiOrchestrator.Test.WorkerSpike.Teardown do
  @moduledoc """
  TEST-ONLY bounded reap protocol for dynamically born workers (constraints 1, 2, 6 of m_1788677511000; WM-1/WM-2
  of m_1788678466000).

  Owned identities come ONLY from (a) the frozen birth set of the driver's `Registry` (or an explicit list) and
  (b) the child pids the work supervisor itself reports through a BOUNDED `which_children`. A link scan is never a
  source of kill authority. Every operation - discovery, orderly stop, each join - is clamped to the remaining part
  of ONE absolute monotonic deadline (`total_budget`), the budget is re-checked after every wait and before any
  success, and no new admission or wait starts once it is exhausted.

  Closure truth (WM-2): success means every admitted pid was observed DOWN AND the subtree is provably closed -
  either the supervisor enumerated its children (discovery :ok) or, when it could not (suspended: :unknown), the
  registry's frozen birth set is COMPLETE, so nothing unenumerated can exist. A dead supervisor is never proof its
  trapping children died. An explicit pid list carries no completeness, so unknown discovery with a list is
  `teardown_incomplete` (unknown/unregistered refusal), never success.
  """

  alias AiOrchestrator.Test.WorkerSpike.Registry

  @defaults [max_rounds: 6, op_timeout: 2_000, join_timeout: 2_000, total_budget: 20_000]

  @spec reap(pid(), [pid()] | pid(), keyword()) :: {:ok, map()} | {:error, map()}
  def reap(work_sup, owned, opts \\ []) when is_pid(work_sup) do
    opts = Keyword.merge(@defaults, opts)
    started = System.monotonic_time(:millisecond)
    # the ONE absolute deadline exists before any operation, the registry freeze included
    deadline = started + opts[:total_budget]

    acc = %{
      joined: MapSet.new(),
      discovery: :ok,
      stop: :absent,
      ops: %{freeze_ms: 0, discover_ms: 0, stop_ms: 0, join_ms: 0}
    }

    case frozen_births(owned, deadline, opts) do
      {:ok, registered, complete?, f_ms} ->
        acc = %{acc | ops: bump(acc.ops, :freeze_ms, f_ms)}
        rounds(work_sup, registered, complete?, acc, 1, started, deadline, opts)

      {:error, cause, f_ms} ->
        acc = %{acc | ops: bump(acc.ops, :freeze_ms, f_ms)}
        incomplete(MapSet.new(), acc, 0, started, cause)
    end
  end

  # the owned identities: an explicit list (no completeness) or the registry's frozen birth set, clamped to the deadline
  defp frozen_births(pids, _deadline, _opts) when is_list(pids), do: {:ok, MapSet.new(pids), false, 0}

  defp frozen_births(registry, deadline, opts) when is_pid(registry) do
    {result, f_ms} = timed(fn -> Registry.freeze(registry, clamp(opts[:op_timeout], deadline)) end)

    case result do
      %{pids: pids, complete: complete?} -> {:ok, MapSet.new(pids), complete?, f_ms}
      :unavailable -> {:error, "registry_unavailable", f_ms}
    end
  end

  defp rounds(work_sup, admitted, complete?, acc, round, started, deadline, opts) do
    cond do
      round > opts[:max_rounds] -> incomplete(admitted, acc, round - 1, started, "rounds_exhausted")
      remaining(deadline) <= 0 -> incomplete(admitted, acc, round - 1, started, "budget_exhausted")
      true -> round_once(work_sup, admitted, complete?, acc, round, started, deadline, opts)
    end
  end

  defp round_once(work_sup, admitted, complete?, acc, round, started, deadline, opts) do
    {{discovered, discovery}, d_ms} = timed(fn -> discover(work_sup, clamp(opts[:op_timeout], deadline)) end)
    admitted = MapSet.union(admitted, discovered)
    acc = %{acc | discovery: discovery, ops: bump(acc.ops, :discover_ms, d_ms)}
    pending = admitted |> MapSet.difference(acc.joined) |> MapSet.to_list()
    monitors = for pid <- pending, do: {pid, Process.monitor(pid)}
    {stop, s_ms} = timed(fn -> orderly_stop(work_sup, clamp(opts[:op_timeout], deadline), deadline) end)
    acc = %{acc | stop: stop, ops: bump(acc.ops, :stop_ms, s_ms)}
    for {pid, _} <- monitors, Process.alive?(pid), do: Process.exit(pid, :kill)
    {joined_now, j_ms} = join_all(monitors, deadline, opts[:join_timeout])
    acc = %{acc | joined: MapSet.union(acc.joined, MapSet.new(joined_now)), ops: bump(acc.ops, :join_ms, j_ms)}
    outcome(work_sup, admitted, complete?, acc, round, started, deadline, opts)
  end

  defp outcome(work_sup, admitted, complete?, acc, round, started, deadline, opts) do
    alive = admitted |> MapSet.difference(acc.joined) |> MapSet.to_list()

    case decision(alive == [], closure_proven?(work_sup, complete?, acc), remaining(deadline)) do
      :budget_exhausted -> incomplete(admitted, acc, round, started, "budget_exhausted")
      :success -> success(acc, round, started)
      :closure_unproven -> incomplete(admitted, acc, round, started, "closure_unproven")
      :continue -> rounds(work_sup, admitted, complete?, acc, round + 1, started, deadline, opts)
    end
  end

  @doc """
  The pure end-of-round decision, exposed so the boundary is testable without timing: the budget is consulted
  BEFORE any success - a round whose joins all completed at or after the deadline is `:budget_exhausted`.
  """
  @spec decision(boolean(), boolean(), integer()) :: :budget_exhausted | :success | :closure_unproven | :continue
  def decision(_all_joined?, _closed?, remaining_ms) when remaining_ms <= 0, do: :budget_exhausted
  def decision(true, true, _remaining_ms), do: :success
  def decision(true, false, _remaining_ms), do: :closure_unproven
  def decision(false, _closed?, _remaining_ms), do: :continue

  # The subtree is provably closed only when (a) a LIVE supervisor enumerated its children AND the orderly stop was
  # ACKNOWLEDGED (Supervisor.stop returned :ok, so the supervisor itself terminated every child it knew, and its
  # exit was observed) - a pre-stop snapshot followed by a forced kill is stale: a birth queued after the listing
  # can survive the kill (WM-5); or (b) the frozen birth set is COMPLETE and the supervisor is gone, whatever the
  # discovery said (:unknown - suspended; :absent - already dead, which never enumerates).
  defp closure_proven?(_work_sup, _complete?, %{discovery: :ok, stop: :orderly}), do: true
  defp closure_proven?(work_sup, complete?, _acc), do: complete? and not Process.alive?(work_sup)

  defp success(acc, round, started) do
    {:ok,
     %{
       rounds: round,
       joined: MapSet.size(acc.joined),
       closure: if(acc.discovery == :ok and acc.stop == :orderly, do: "enumerated", else: "complete_registry"),
       elapsed_ms: System.monotonic_time(:millisecond) - started,
       ops: acc.ops
     }}
  end

  # bounded discovery through the supervisor's own child registry; a suspended supervisor answers :unknown
  defp discover(work_sup, timeout) do
    if Process.alive?(work_sup) and timeout > 0 do
      try do
        children = GenServer.call(work_sup, :which_children, timeout)
        {MapSet.new(for {_, pid, _, _} <- children, is_pid(pid), do: pid), :ok}
      catch
        :exit, _ -> {MapSet.new(), :unknown}
      end
    else
      # a dead supervisor is ABSENT, never an enumeration; a live one we had no budget to ask is unknown
      {MapSet.new(), if(Process.alive?(work_sup), do: :unknown, else: :absent)}
    end
  end

  # :orderly only when Supervisor.stop was acknowledged AND the supervisor's exit was observed; a timeout kills the
  # supervisor (:forced) and its exit is still observed under the remaining budget; :absent when already gone
  defp orderly_stop(work_sup, timeout, deadline) do
    cond do
      not Process.alive?(work_sup) ->
        :absent

      timeout <= 0 ->
        forced(work_sup, deadline)

      true ->
        mon = Process.monitor(work_sup)

        try do
          :ok = Supervisor.stop(work_sup, :shutdown, timeout)
          if observed_down?(work_sup, mon, clamp(timeout, deadline)), do: :orderly, else: :forced
        catch
          :exit, _ ->
            Process.demonitor(mon, [:flush])
            forced(work_sup, deadline)
        end
    end
  end

  defp forced(work_sup, deadline) do
    mon = Process.monitor(work_sup)
    Process.exit(work_sup, :kill)
    _ = observed_down?(work_sup, mon, clamp(1_000, deadline))
    :forced
  end

  defp observed_down?(pid, mon, timeout) do
    receive do
      {:DOWN, ^mon, :process, ^pid, _} -> true
    after
      timeout -> false
    end
  end

  # each join takes at most min(join_timeout, remaining); once the deadline passes no further wait starts
  defp join_all(monitors, deadline, join_timeout) do
    t0 = System.monotonic_time(:millisecond)

    joined =
      Enum.reduce(monitors, [], fn {pid, mon}, acc ->
        timeout = clamp(join_timeout, deadline)

        receive do
          {:DOWN, ^mon, :process, ^pid, _} -> [pid | acc]
        after
          timeout -> acc
        end
      end)

    {joined, System.monotonic_time(:millisecond) - t0}
  end

  defp remaining(deadline), do: deadline - System.monotonic_time(:millisecond)
  defp clamp(timeout, deadline), do: max(min(timeout, remaining(deadline)), 0)
  defp bump(ops, key, ms), do: Map.update!(ops, key, &(&1 + ms))

  defp timed(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    {result, System.monotonic_time(:millisecond) - t0}
  end

  defp incomplete(admitted, acc, rounds, started, cause) do
    survivors = admitted |> MapSet.difference(acc.joined) |> MapSet.to_list()

    {:error,
     %{
       clause: "teardown_incomplete",
       survivors: length(survivors),
       rounds: rounds,
       cause: cause,
       discovery: acc.discovery,
       stop: acc.stop,
       elapsed_ms: System.monotonic_time(:millisecond) - started,
       ops: acc.ops
     }}
  end
end
