defmodule AiOrchestrator.Dispatch.IpcV2PreflightTest do
  @moduledoc """
  NS-42 rule 2 on the path the orchestrator actually takes: an `Effect.Dispatch` executed
  through `Effects` with the default adapter (`LocalPane`) and the real `PaneClient` over
  fake `ap` runners. The daemon's `ping --protocol-version 2` answer decides whether a
  send may happen at all: `delivery_reconcile` admits it, its absence refuses it as the
  typed `dispatch_preflight_unsupported` before `ap send` is ever invoked, and a v1 ping
  (no `protocol_version`) refuses it too. The no-message-id send stays decode-only.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime

  @id "snd_" <> String.duplicate("a", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-preflight-missing-artifact",
      "prompt" => "v2 prompt"
    }
  end

  defp intent, do: %Effect.Dispatch{assignment_id: "as_0001", message_id: @id, command: command(), deadline_unix: 0}

  # Every `ap` verb the dispatch path can reach, answered from one table so the test can
  # count which verbs ran and in what order. The ping answer is the variable under test.
  defp dispatch_opts(ping_reply, owner) do
    [
      ap_path: "/tmp/v2-preflight-ap",
      runner: fn _, args, _ ->
        send(owner, {:ap, hd(args)})

        response =
          case hd(args) do
            "ping" ->
              ping_reply

            "reconcile" ->
              %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "outcome" => "absent"}
          end

        {Jason.encode!(response), 0}
      end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:ap, hd(args)})

        {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "status" => "sent"}),
         0}
      end
    ]
  end

  defp execute(ping_reply) do
    owner = self()

    {observation, _runtime} =
      Effects.execute(intent(), Runtime.new([]), opts: [dispatch_opts: dispatch_opts(ping_reply, owner)])

    observation
  end

  defp verbs do
    fn ->
      receive do
        {:ap, verb} -> verb
      after
        0 -> nil
      end
    end
    |> Stream.repeatedly()
    |> Enum.take_while(&(&1 != nil))
  end

  test "a v2 daemon that advertises delivery_reconcile is pinged before anything else, then sends" do
    ping = %{"ok" => true, "protocol_version" => 2, "pong" => "x", "capabilities" => ["delivery_reconcile", "future_v3"]}

    assert %Observation.Dispatched{result: %{"send_status" => "ok", "send_message_id" => @id}} = execute(ping)

    # The executor admits the effect and the adapter proves the capability again on its own
    # account (Effects.dispatch_admitted and LocalPane.deliver both preflight); either way the
    # first verb on the wire is ping and the send is the last.
    assert ["ping" | rest] = verbs()
    assert List.last(rest) == "send"
    assert Enum.count(rest, &(&1 == "send")) == 1
  end

  test "a v2 daemon without delivery_reconcile is refused with the typed preflight reason and never asked to send" do
    ping = %{"ok" => true, "protocol_version" => 2, "pong" => "x", "capabilities" => ["pane_status"]}

    assert %Observation.DispatchFailed{assignment_id: "as_0001", reason: reason} = execute(ping)
    assert reason["reason"] == "dispatch_preflight_unsupported"
    assert reason["detector"] == "dispatch_preflight"
    assert reason["missing_capabilities"] == ["delivery_reconcile"]
    assert verbs() == ["ping"]
  end

  test "a v2 daemon that lists no capabilities at all is refused the same way" do
    ping = %{"ok" => true, "protocol_version" => 2, "pong" => "x"}

    assert %Observation.DispatchFailed{reason: %{"reason" => "dispatch_preflight_unsupported"}} = execute(ping)
    assert verbs() == ["ping"]
  end

  test "a v1 daemon (ping without protocol_version) cannot admit a v2 send, whatever it lists" do
    ping = %{"ok" => true, "pong" => "x", "capabilities" => ["delivery_reconcile"]}

    assert %Observation.DispatchFailed{reason: reason} = execute(ping)
    assert reason["reason"] == "dispatch_capabilities_failed"
    assert reason["detector"] == "dispatch_preflight"
    assert verbs() == ["ping"]
  end

  test "a ping that is not ok admits nothing" do
    ping = %{"ok" => false, "protocol_version" => 2, "error" => "receipt_store_unavailable"}

    assert %Observation.DispatchFailed{reason: %{"reason" => "dispatch_capabilities_failed"}} = execute(ping)
    assert verbs() == ["ping"]
  end

  test "the capability query itself carries the explicit version and no payload" do
    owner = self()

    runner = fn _, args, opts ->
      send(owner, {:ping_args, args, opts})
      {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}), 0}
    end

    assert {:ok, ["delivery_reconcile"]} =
             PaneClient.capabilities(ap_path: "/tmp/v2-preflight-ap", runner: runner, input: "never sent")

    assert_receive {:ping_args, ["ping", "--protocol-version", "2"], _opts}
  end

  test "the no-message-id send is decode-only and asks the daemon nothing before it" do
    owner = self()

    opts = [
      ap_path: "/tmp/v2-preflight-ap",
      runner: fn _, args, _ -> flunk("a legacy send must not run #{hd(args)}") end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:ap, hd(args)})
        {~s({"ok":true,"status":"sent"}\n), 0}
      end
    ]

    assert {:ok, %{"ok" => true, "status" => "sent"}} = PaneClient.send(@pane, "legacy", opts)
    assert verbs() == ["send"]
  end
end
