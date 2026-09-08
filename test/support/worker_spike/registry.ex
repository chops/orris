defmodule AiOrchestrator.Test.WorkerSpike.Registry do
  @moduledoc """
  TEST-ONLY registration-before-use for dynamically born workers (WM-2). Every birth goes through `birth/3`:
  an INTENT is recorded before the supervisor is asked, the pid is recorded when the start answers, and a start
  that fails or raises clears the intent. `freeze/1` closes admission for teardown: later births are refused.
  A frozen registry with no pending intent is a COMPLETE birth set: only that completeness can prove closure of
  a subtree whose supervisor can no longer enumerate its children.
  """

  def start, do: Agent.start(fn -> %{pids: MapSet.new(), intents: 0, frozen: false} end)

  @spec birth(pid(), pid(), term()) :: {:ok, pid()} | {:error, term()}
  def birth(registry, work_sup, child_spec) do
    case Agent.get_and_update(registry, &admit/1) do
      :refused ->
        {:error, :admission_frozen}

      :admitted ->
        try do
          case DynamicSupervisor.start_child(work_sup, child_spec) do
            {:ok, pid} ->
              Agent.update(registry, fn s -> %{s | pids: MapSet.put(s.pids, pid), intents: s.intents - 1} end)
              {:ok, pid}

            other ->
              Agent.update(registry, fn s -> %{s | intents: s.intents - 1} end)
              other
          end
        catch
          :exit, reason ->
            Agent.update(registry, fn s -> %{s | intents: s.intents - 1} end)
            {:error, {:exit, reason}}
        end
    end
  end

  defp admit(%{frozen: true} = s), do: {:refused, s}
  defp admit(s), do: {:admitted, %{s | intents: s.intents + 1}}

  @doc """
  Close admission under a bounded call; returns the birth set and whether it is complete (no intent still in
  flight). A registry that cannot answer within `timeout` (suspended, dead) is reported, never waited on
  with a default timeout.
  """
  def freeze(registry, timeout \\ 5_000) do
    Agent.get_and_update(
      registry,
      fn s ->
        frozen = %{s | frozen: true}
        {%{pids: MapSet.to_list(s.pids), complete: s.intents == 0}, frozen}
      end,
      timeout
    )
  catch
    :exit, _ -> :unavailable
  end

  def pids(registry), do: Agent.get(registry, &MapSet.to_list(&1.pids))
end
