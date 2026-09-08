defmodule AiOrchestrator.Test.WorkerSpike.ControlledExecutor do
  @moduledoc """
  TEST-ONLY gate executor double for the mechanism spike: `prepare/3` reports entry to the control pid named in
  the opts and then waits for an instruction (:proceed | :raise | :hang), so a test can hold the owner inside a
  blocking effect, make it fail trappably, or leave it blocked forever. The failure message closes over the
  SENTINEL from the opts: any disclosure surface printing it is a leak. Shapes mirror the C4 double.
  """

  alias AiOrchestrator.Gate.Execution

  def prepare(_fs, request, opts) do
    control = Keyword.fetch!(opts, :spike_control)
    secret = Keyword.fetch!(opts, :spike_secret)
    send(control, {:prepare_entered, self(), request.gate_run_id})

    receive do
      instruction when instruction in [:proceed, :proceed_then_fail_started] ->
        started = %{
          "gate_run_id" => request.gate_run_id,
          "command_argv" => request.command_argv,
          "stdout_path" => "gates/#{request.gate_run_id}.#{request.attempt}.out",
          "stderr_path" => "gates/#{request.gate_run_id}.#{request.attempt}.err",
          "attempt" => request.attempt,
          "deadline_unix" => request.deadline_unix,
          "execution" => %{
            "pid" => 4242,
            "pgid" => 4242,
            "start" => "1756728000.123456",
            "claim_hash" => "sha256:" <> String.duplicate("ab", 32)
          }
        }

        {:ok,
         %{
           started_data: started,
           identity: %{guardian: 4241, worker: 4242, pgid: 4242, start: "1756728000.123456"},
           request: request,
           control: control,
           abandon_mode: Keyword.get(opts, :spike_abandon, :ok),
           fail_started: instruction == :proceed_then_fail_started,
           secret: secret
         }}

      :raise ->
        raise "controlled failure carrying " <> secret

      :hang ->
        Process.sleep(:infinity)
    end
  end

  # :proceed_then_fail_started transfers the handle into the runtime and THEN fails at started_data: the
  # transfer-boundary witness (the NEW handle must be settled by the same-invocation cleanup)
  def started_data(%{fail_started: true, secret: secret}), do: raise("started_data failed after transfer " <> secret)
  def started_data(%{started_data: data}), do: data
  def identity(%{identity: identity}), do: identity
  def ack(prepared, persisted), do: Execution.ack(prepared, persisted)
  def release(prepared, _ack, _opts \\ []), do: {:ok, Map.put(prepared, :released, true)}

  # the executor's ACTUAL cleanup answer, observable by the control process: :ok (proven), {:error, _} or a raise
  # (unproven), or an explicit unsettled shape
  def abandon(%{control: control, request: request, abandon_mode: mode, secret: secret}) do
    send(control, {:abandoned, request.gate_run_id, request.attempt, mode})

    case mode do
      :ok -> :ok
      :error -> {:error, %{clause: "abandon_failed"}}
      :raise -> raise "abandon raised " <> secret
      :unsettled -> {:error, %{clause: "abandon_unsettled"}}
    end
  end

  def expire(_handle), do: {:timeout, %{kind: "timeout", settled: true, leftovers: "0", proof: "gone", duration_ms: 1}}

  def await(_running, _opts \\ []),
    do:
      {:exit,
       %{
         "kind" => "exited",
         "exit_status" => 0,
         "settled" => true,
         "leftovers" => "0",
         "proof" => "gone",
         "escaped" => "unknown",
         "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
         "stderr_hash" => "sha256:" <> String.duplicate("ab", 32),
         "stderr_merged" => false,
         "duration_ms" => 12
       }}

  def evidence(_run_dir, _id, _attempt),
    do:
      {:ok,
       %{
         "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
         "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
       }}

  def reconcile(_fs, _run_dir, _expected, _opts \\ []),
    do: {:dead, %{"leader" => "gone", "group" => "gone", "members" => 0}}

  def pass?(%{"exit_status" => 0, "settled" => true, "proof" => "gone"}), do: true
  def pass?(_), do: false
end
