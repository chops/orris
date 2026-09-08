defmodule AiOrchestrator.Test.OwnerDoubles do
  @moduledoc """
  TEST-ONLY doubles shared by the effect-owner RED suites. Every double reports the pid that runs it to the
  COLLECTOR named in its opts/control, so the harness owns that pid before the test can act on it.
  """

  @pass %{
    "exit_status" => 0,
    "duration_ms" => 1,
    "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
    "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
  }

  @doc "A legacy-runner gate that reports entry and waits for :release_gate (bounded)."
  def held_gate(collector) do
    fn _gate ->
      send(collector, {:gate_entered, self()})

      receive do
        :release_gate -> {:ok, @pass}
      after
        30_000 -> exit(:held_gate_never_released)
      end
    end
  end

  defmodule HoldingDispatch do
    @moduledoc false
    # deliver reports entry, waits for an instruction, then proceeds / raises / throws / exits with the sentinel
    alias AiOrchestrator.Dispatch.LocalPane

    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def deliver(command, opts) do
      send(Keyword.fetch!(opts, :collector), {:deliver_entered, self()})
      secret = Keyword.fetch!(opts, :sentinel)

      receive do
        :proceed -> LocalPane.deliver(command, Keyword.drop(opts, [:collector, :sentinel]))
        :raise -> raise("adapter failure carrying " <> secret)
        :throw -> throw({:adapter_throw, secret})
        :exit -> exit({:adapter_exit, secret})
      after
        30_000 -> exit(:deliver_never_instructed)
      end
    end
  end

  defmodule AbandonGate do
    @moduledoc false
    # the C4 double with a controllable abandon (:ok | :error | :hang); every abandon is reported to the collector
    alias AiOrchestrator.Test.GateDouble

    def control(collector, mode), do: :persistent_term.put({__MODULE__, :control}, {collector, mode})

    def prepare(fs, request, opts), do: GateDouble.prepare(fs, request, opts)
    def started_data(prepared), do: GateDouble.started_data(prepared)
    def identity(prepared), do: GateDouble.identity(prepared)
    def ack(prepared, persisted), do: GateDouble.ack(prepared, persisted)
    def release(prepared, ack, opts \\ []), do: GateDouble.release(prepared, ack, opts)
    def expire(handle), do: GateDouble.expire(handle)
    def await(running, opts \\ []), do: GateDouble.await(running, opts)
    def evidence(run_dir, id, attempt), do: GateDouble.evidence(run_dir, id, attempt)
    def reconcile(fs, run_dir, expected, opts \\ []), do: GateDouble.reconcile(fs, run_dir, expected, opts)
    def pass?(data), do: GateDouble.pass?(data)

    def abandon(_handle) do
      {collector, mode} = :persistent_term.get({__MODULE__, :control})

      case mode do
        :ok ->
          send(collector, {:abandoned, self(), mode})
          :ok

        :error ->
          send(collector, {:abandoned, self(), mode})
          {:error, %{clause: "abandon_failed"}}

        :hang ->
          send(collector, {:abandoned, self(), mode})
          Process.sleep(:infinity)

        # the hang whose ENTRY is acknowledged DIRECTLY to a controller (no collector hop): the controller learns the
        # exact process that is inside abandon the moment it enters, and can act before any settle budget elapses;
        # the collector still receives the ordinary report; the process then hangs until it is killed
        {:hang_ack, controller} when is_pid(controller) ->
          send(collector, {:abandoned, self(), :hang})
          send(controller, {:abandon_entered, self()})

          receive do
            :never_sent -> :ok
          end
      end
    end
  end
end
