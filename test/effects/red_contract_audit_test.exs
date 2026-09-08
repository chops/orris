defmodule AiOrchestrator.Effects.RedContractAuditTest do
  # Imported from Codex's audit probes (/tmp/effects-red-contract-audit_test.exs): cases 1 and 3
  # assertion-preserving; case 2 documented below as baseline evidence (see comment).
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  defmodule QueuedAdapter do
    @moduledoc false
    alias AiOrchestrator.Dispatch.LocalPane

    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane

    def deliver(command, opts) do
      {:ok, result} = LocalPane.deliver(command, opts)
      {:ok, Map.put(result, "send_status", "queued")}
    end

    def reconcile(_command, _opts) do
      outcome = if Process.get(:audit_polled), do: "delivered", else: "queued"
      Process.put(:audit_polled, true)
      {:ok, %{"outcome" => outcome, "delivery_attempt" => 1}}
    end
  end

  defmodule Executor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate release(handle, ack, opts), to: GateDouble
    defdelegate await(handle, opts), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble

    def abandon(handle) do
      Process.put(:audit_abandoned, [handle | Process.get(:audit_abandoned, [])])
      :ok
    end
  end

  defp run(extra) do
    {_, :run, "gated_run_seed", [], make_opts} =
      Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

    opts = make_opts.() |> Keyword.put(:gate_executor, Executor) |> Keyword.merge(extra)
    Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
  end

  setup do
    H.reset_seams()
    :ok
  end

  test "current reducer constructs Timer but never constructs RunGate or Notify" do
    source = File.read!(Path.expand("../../lib/ai_orchestrator/lifecycle/core/reducer.ex", __DIR__))

    {_, names} =
      Macro.prewalk(Code.string_to_quoted!(source), [], fn
        {:%, _, [{:__aliases__, _, [:Effect, name]}, _]} = node, names -> {node, [name | names]}
        node, names -> {node, names}
      end)

    assert :Timer in names
    refute :RunGate in names
    refute :Notify in names
  end

  # Codex's second audit case ("caught throw can leave a same-process handle that the next
  # invocation reports as a straggler") is BASELINE evidence at 61a16ac: a caught :throw from the
  # observer left the running handle in the process dictionary, unabandoned, and the next Host.run
  # reported it as gate_cleanup. It is NOT imported as a passing test because the extraction pins the
  # opposite, disclosed behavioural change (R-1 revised): every trappable exit settles the latest
  # runtime in the same invocation. That pin lives in effects_extraction_red_test.exs ("the Host
  # closes every trappable exit in the same invocation").

  test "queued delivery actually yields Timer through Host before convergence" do
    observer = fn effect, _ ->
      Process.put(:audit_effects, [effect.__struct__ | Process.get(:audit_effects, [])])
    end

    assert {:ok, %{summary: %{"status" => "completed"}}} =
             run(dispatch: QueuedAdapter, effect_observer: observer)

    assert Effect.Timer in Process.get(:audit_effects)
    refute Effect.RunGate in Process.get(:audit_effects)
  end
end
