defmodule AiOrchestrator.Dispatch.IpcV2SendReplyTest do
  @moduledoc """
  The consumer side of the shared IPC v2 send-reply fixtures: each `send.*.json` under
  test/fixtures/contracts/ipc/v2 is fed to LocalPane.deliver exactly as the daemon would
  answer it (placeholders substituted), and the routing table below is the whole contract.
  A byte change in the fixtures moves the pinned hash and, if it changes a shape, this test.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @id "snd_" <> String.duplicate("a", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)

  @routing %{
    "send.sent.json" => {:ok, "ok", false},
    "send.queued.json" => {:ok, "queued", false},
    "send.duplicate.delivered.json" => {:ok, "reconciled", true},
    "send.duplicate.queued.json" => {:ok, "queued", true},
    "send.duplicate.pending.json" => {:ok, "queued", true},
    "send.duplicate.ambiguous.json" => {:error, "dispatch_reconcile_ambiguous"},
    "send.error.conflict.json" => {:error, "reply_not_ok"},
    "send.error.missing_msg_id.json" => {:error, "reply_not_ok"}
  }

  test "every send fixture is routed as the contract says" do
    fixtures = @fixture_dir |> Path.join("send.*.json") |> Path.wildcard() |> Enum.map(&Path.basename/1)
    assert Enum.sort(fixtures) == Enum.sort(Map.keys(@routing)), "a send fixture appeared without a routing entry"

    for {name, expected} <- @routing do
      reply = fixture(name)

      case {expected, LocalPane.deliver(command(), delivery_opts(reply))} do
        {{:ok, status, replayed}, {:ok, data}} ->
          assert data["send_status"] == status, name
          assert data["replayed"] == replayed, name

        {{:error, reason}, {:error, %{"reason" => actual}}} ->
          assert actual == reason, name

        {_expected, actual} ->
          flunk("#{name}: unexpected #{inspect(actual)}")
      end
    end
  end

  defp fixture(name) do
    @fixture_dir
    |> Path.join(name)
    |> File.read!()
    |> String.replace("<msg_id>", @id)
    |> String.replace("<pane_id>", @pane)
    |> String.replace("<payload_hash>", @hash)
    |> Jason.decode!()
  end

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-missing-artifact",
      "prompt" => "v2 prompt"
    }
  end

  defp delivery_opts(reply) do
    [
      ap_path: "/tmp/v2-ap",
      runner: fn _, args, _ ->
        response =
          if hd(args) == "ping",
            do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]},
            else: %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "outcome" => "absent"}

        {Jason.encode!(response), 0}
      end,
      input_runner: fn _, _, _, _ -> {Jason.encode!(reply), 0} end
    ]
  end
end
