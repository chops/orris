defmodule AiOrchestrator.Effects.DispatchCapabilityAdmissionTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Run.Worker

  defmodule ExportedOnly do
    @moduledoc false
    def deliver(_, opts), do: send(opts[:test], :unexpected_deliver)
    def reconcile(_, opts), do: send(opts[:test], :unexpected_reconcile)
  end

  defmodule Declared do
    @moduledoc false
    def capabilities(opts) do
      send(opts[:test], {:capability, self()})

      case opts[:capability] do
        :raise -> raise "PRIVATE_CAPABILITY_CANARY"
        :throw -> throw("PRIVATE_CAPABILITY_CANARY")
        :exit -> exit("PRIVATE_CAPABILITY_CANARY")
        :block -> receive do: (:never -> {:ok, ["delivery_reconcile"]})
        result -> result
      end
    end

    def deliver(_, opts) do
      send(opts[:test], :delivered)
      {:ok, %{"send_status" => "ok"}}
    end

    def reconcile(_, opts) do
      send(opts[:test], :reconciled)
      {:ok, %{"outcome" => "delivered", "delivery_attempt" => 1}}
    end
  end

  defp intent(:deliver), do: %Effect.Dispatch{assignment_id: "as_1", message_id: "m", command: %{}, deadline_unix: 0}

  defp intent(:reconcile), do: %Effect.ReconcileSend{assignment_id: "as_1", command: %{}, deadline_unix: 0}

  defp execute(operation, module, capability, runner?) do
    owner = self()
    opts = [dispatch: module, dispatch_opts: [test: owner, capability: capability]]

    opts =
      if runner?,
        do:
          Keyword.put(opts, :adapter_runner, fn fun, _deadline ->
            send(owner, :inside_runner)
            {:ok, fun.()}
          end),
        else: opts

    {observation, _} = Effects.execute(intent(operation), Runtime.new([]), opts: opts)
    observation
  end

  for operation <- [:deliver, :reconcile], runner? <- [false, true] do
    test "#{operation} admission is enforced inside runner=#{runner?}, not inferred from exported functions" do
      observation = execute(unquote(operation), ExportedOnly, nil, unquote(runner?))
      assert observation.reason["reason"] == "dispatch_adapter_cannot_reconcile"
      refute_received :unexpected_deliver
      refute_received :unexpected_reconcile
      if unquote(runner?), do: assert_received(:inside_runner)
    end

    test "#{operation} admits declared capability with additive tokens inside runner=#{runner?}" do
      observation = execute(unquote(operation), Declared, {:ok, ["delivery_reconcile", "future_v3"]}, unquote(runner?))
      assert_received {:capability, _}
      if unquote(runner?), do: assert_received(:inside_runner)

      case unquote(operation) do
        :deliver ->
          assert %Observation.Dispatched{} = observation
          assert_received :delivered

        :reconcile ->
          assert %Observation.SendReconciled{outcome: "delivered"} = observation
          assert_received :reconciled
      end
    end

    test "#{operation} rejects unavailable or malformed capability before entry, runner=#{runner?}" do
      for reply <- [
            {:ok, []},
            {:ok, ["delivery_reconcile", 1]},
            {:ok, ["delivery_reconcile", "bad token"]},
            {:ok, ["delivery_reconcile", "bad\n"]},
            {:ok, "delivery_reconcile"},
            {:error, %{"private" => "PRIVATE_CAPABILITY_CANARY"}},
            :raise,
            :throw,
            :exit
          ] do
        observation = execute(unquote(operation), Declared, reply, unquote(runner?))

        assert observation.reason["reason"] in [
                 "dispatch_preflight_unsupported",
                 "dispatch_capabilities_invalid",
                 "dispatch_capabilities_failed"
               ]

        refute inspect(observation) =~ "PRIVATE_CAPABILITY_CANARY"
        refute_received :delivered
        refute_received :reconciled
      end
    end
  end

  test "raw wire capabilities reject malformed tokens and preserve unknown valid tokens" do
    alias AiOrchestrator.Dispatch.PaneClient

    for tokens <- [["delivery_reconcile", 1], ["delivery_reconcile", "bad token"], ["delivery_reconcile", ""]] do
      raw = Jason.encode!(%{"ok" => true, "protocol_version" => 2, "capabilities" => tokens})
      opts = [ap_path: "/unused", runner: fn _, _, _ -> {raw, 0} end]
      assert {:error, %{"reason" => "dispatch_capabilities_invalid"}} = PaneClient.capabilities(opts)

      assert {:error, _} =
               LocalPane.deliver(
                 %{
                   "pane_ref" => "pane_fixture",
                   "send_message_id" => "id"
                 },
                 Keyword.put(opts, :input_runner, fn _, _, _, _ -> flunk("malformed capability reached send") end)
               )
    end

    raw = ~s({"ok":true,"protocol_version":2,"capabilities":["delivery_reconcile",") <> <<255>> <> "\"]}"
    assert {:error, _} = PaneClient.capabilities(ap_path: "/unused", runner: fn _, _, _ -> {raw, 0} end)

    tokens = ["delivery_reconcile", "Vendor:future-v3/2", "未来"]
    raw = Jason.encode!(%{"ok" => true, "protocol_version" => 2, "capabilities" => tokens})
    assert {:ok, ^tokens} = PaneClient.capabilities(ap_path: "/unused", runner: fn _, _, _ -> {raw, 0} end)
    assert {:ok, ["delivery_reconcile"]} = LocalPane.capabilities(ap_path: "/unused", runner: fn _, _, _ -> {raw, 0} end)
    refute Dispatch.valid_capabilities?(["delivery_reconcile", <<255>>])
  end

  test "preflight invokes the declaration and refuses non-module adapters without disclosing them" do
    assert {:error, %{"reason" => "dispatch_capabilities_failed"}} = Dispatch.preflight("PRIVATE_CAPABILITY_CANARY")
  end

  for operation <- [:deliver, :reconcile] do
    test "a blocking #{operation} capability query belongs to the existing deadline task and is joined" do
      worker = start_supervised!({Worker, self()})
      cap = make_ref()
      opts = [clock: SystemClock, dispatch: Declared, dispatch_opts: [test: self(), capability: :block]]
      send(worker, {:admit, cap, 1, opts})
      assert_receive {:admitted, ^cap, 1, ^worker}, 1_000
      ref = make_ref()
      effect = Map.put(intent(unquote(operation)), :deadline_unix, System.system_time(:second) + 1)
      send(worker, {:execute, cap, 1, ref, effect, nil})
      assert_receive {:capability, task}, 1_000
      refute task == worker
      monitor = Process.monitor(task)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, observation}, 3_000

      assert observation.reason["reason"] ==
               if(unquote(operation) == :deliver, do: "dispatch_deadline_exceeded", else: "dispatch_reconcile_timeout")

      assert_receive {:DOWN, ^monitor, :process, ^task, _}, 1_000
      refute Process.alive?(task)
      refute_received :delivered
      refute_received :reconciled
      settle = make_ref()
      send(worker, {:settle, cap, 1, settle})
      assert_receive {:settled, ^cap, 1, ^settle, ^worker, []}, 1_000
    end
  end
end
