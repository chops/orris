defmodule C4Rev3ReviewTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias C4Rev2ReviewTest.Executor

  defp real_unsettled do
    script =
      "read command; printf '%s\\n' 'DEAD reason=command kind=unknown settled=0 leftovers=unknown proof=unknown escaped=unknown'"

    port = Port.open({:spawn_executable, "/bin/sh"}, [:binary, :exit_status, {:line, 1024}, {:args, ["-c", script]}])

    try do
      Execution.abandon(%{port: port, opts: []})
    after
      if Port.info(port) != nil, do: Port.close(port)
    end
  end

  test "control: real Execution returns its documented unsettled producer shape" do
    assert {:error, %{clause: "abandon_unsettled", settled: false, proof: "unknown", leftovers: "unknown"}} =
             real_unsettled()
  end

  test "real unsettled abandonment remains journalable on release rejection" do
    H.reset_seams()
    Process.put(:release_result, {:error, %{clause: "ack_mismatch"}})
    Process.put(:abandon_result, real_unsettled())
    {_, :run, "gated_run_seed", [], make_opts} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    opts = Keyword.put(make_opts.(), :gate_executor, Executor)

    assert {:ok, %{events: events, summary: %{"status" => "blocked"}}} =
             Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)

    attention = Enum.find(events, &(&1["type"] == "human_attention_required"))
    assert get_in(attention, ["data", "detail", "settle", "settled"]) == false
    assert get_in(attention, ["data", "detail", "settle", "proof"]) == "unknown"
    assert get_in(attention, ["data", "detail", "settle", "leftovers"]) == "unknown"
  end
end
