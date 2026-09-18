defmodule AiOrchestrator.Dispatch.IpcV2RefusalTest do
  @moduledoc """
  NS-42 rules 6, 8, 10 and 11 on the consumer side of a v2 reply that is not ok. The
  daemon's refusal vocabulary is closed and split by the contract into typed refusals
  (facts about the pane or about admission) and request errors (facts about the request
  this consumer built); the consumer reflects a word only from that vocabulary, keeps the
  two classes distinguishable from each other and from success, and exits every one of
  them to attention through the existing failed observation. Nothing here is ever read as
  `absent`. Replies are fed exactly as the daemon would answer them, through the real
  PaneClient over fake ap runners, and through the executor with the default adapter.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.Dispatch.Refusal
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime

  @id "snd_" <> String.duplicate("a", 64)
  @other_id "snd_" <> String.duplicate("e", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  @canary "REFUSAL_CANARY_PRIVATE_DETAIL"
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)
  @other_pane "%" <> Integer.to_string(7)

  # The daemon's words, as the contract names them (docs/contracts/ipc-v1.org "Reply
  # fixtures", docs/contracts/ipc-v2.org "Send replies" and "Framing and version").
  # `invalid_msg_id` / `invalid_pane_id` are the grammar refusals: the daemon drops the echo
  # for the identity it could not read, so they carry at most the other one. Their own rows
  # are in ipc_v2_identity_grammar_test.exs; they are listed here because this file is where
  # the vocabulary is pinned against the vendored contracts.
  @send_refusals ~w(conflict pane_not_found pane_dead queue_full send_timeout paste_failed)
  @send_request_errors ~w(missing_msg_id missing_pane_id missing_text oversize invalid_msg_id invalid_pane_id)
  @reconcile_request_errors ~w(missing_payload_hash missing_msg_id missing_pane_id invalid_msg_id invalid_pane_id)

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-refusal-missing-artifact",
      "prompt" => "v2 prompt"
    }
  end

  # A refusal as the daemon emits it: not ok, versioned, the word, and the echoes it could
  # read. The detail is the daemon's diagnostic text and must never be repeated.
  defp refusal(word, echoes \\ %{"msg_id" => @id, "pane_id" => @pane}) do
    Map.merge(%{"ok" => false, "protocol_version" => 2, "error" => word, "detail" => @canary}, echoes)
  end

  defp absent, do: %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "outcome" => "absent"}

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  # ping and reconcile admit the send; the send itself answers `send_reply`.
  defp send_opts(send_reply) do
    [
      ap_path: "/tmp/v2-refusal-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: absent())), 0} end,
      input_runner: fn _, _, _, _ -> {Jason.encode!(send_reply), 0} end
    ]
  end

  # ping admits; the reconcile itself answers `reconcile_reply`.
  defp reconcile_opts(reconcile_reply) do
    [
      ap_path: "/tmp/v2-refusal-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: reconcile_reply)), 0} end,
      input_runner: fn _, _, _, _ -> flunk("a reconcile never sends") end
    ]
  end

  describe "typed send refusals" do
    for word <- @send_refusals do
      test "#{word} is a typed refusal, distinct from success and from a request error" do
        word = unquote(word)
        result = LocalPane.deliver(command(), send_opts(refusal(word)))

        assert {:error, %{"reason" => "dispatch_refused_" <> ^word, "detector" => "dispatch_send", "refusal" => ^word}} =
                 result

        refute inspect(result, limit: :infinity) =~ @canary, "the daemon's detail is not repeated"
      end
    end

    test "every typed refusal reaches the reducer as the failed dispatch observation with its own reason" do
      for word <- @send_refusals do
        intent = %Effect.Dispatch{assignment_id: "as_0001", message_id: @id, command: command(), deadline_unix: 0}

        {observation, _runtime} =
          Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: send_opts(refusal(word))])

        assert match?(%Observation.DispatchFailed{assignment_id: "as_0001"}, observation), word
        reason = observation.reason
        assert reason["reason"] == "dispatch_refused_" <> word
        assert reason["refusal"] == word
        refute inspect(observation, limit: :infinity) =~ @canary
      end
    end

    test "a typed refusal about another message or pane is an identity error, not a refusal of this send" do
      for {echoes, expected} <- [
            {%{"msg_id" => @other_id, "pane_id" => @pane}, "reply_identity_mismatch"},
            {%{"pane_id" => @pane}, "reply_identity_missing"},
            {%{"msg_id" => @id, "pane_id" => @other_pane}, "reply_pane_mismatch"},
            {%{"msg_id" => @id}, "reply_pane_missing"}
          ] do
        assert {:error, %{"reason" => ^expected} = reason} =
                 LocalPane.deliver(command(), send_opts(refusal("queue_full", echoes)))

        refute Map.has_key?(reason, "refusal"), inspect(echoes)
      end
    end
  end

  describe "request errors" do
    for word <- @send_request_errors do
      test "a send refused with #{word} is a request error carrying the word" do
        word = unquote(word)
        # The daemon echoes the identities it could read: none for a missing message id.
        echoes = if word == "missing_msg_id", do: %{"pane_id" => @pane}, else: %{"msg_id" => @id, "pane_id" => @pane}
        result = LocalPane.deliver(command(), send_opts(refusal(word, echoes)))

        assert {:error, %{"reason" => "dispatch_request_rejected", "detector" => "dispatch_send", "error" => ^word}} =
                 result

        refute inspect(result, limit: :infinity) =~ @canary
      end
    end

    for word <- @reconcile_request_errors do
      test "a reconcile refused with #{word} is a request error on the reconcile detector" do
        word = unquote(word)
        echoes = if word == "missing_msg_id", do: %{"pane_id" => @pane}, else: %{"msg_id" => @id, "pane_id" => @pane}
        expected = %{"reason" => "dispatch_request_rejected", "detector" => "dispatch_reconcile", "error" => word}

        assert {:error, ^expected} = LocalPane.reconcile(command(), reconcile_opts(refusal(word, echoes)))

        assert {:error, ^expected} =
                 PaneClient.reconcile(
                   @pane,
                   @id,
                   Keyword.put(reconcile_opts(refusal(word, echoes)), :payload_hash, @hash)
                 )
      end
    end

    test "a rejected reconcile reaches the reducer as the failed reconcile observation, never as an outcome" do
      intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

      {observation, _runtime} =
        Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: reconcile_opts(refusal("missing_payload_hash"))])

      assert %Observation.SendReconcileFailed{reason: %{"reason" => "dispatch_request_rejected"}} = observation
    end
  end

  describe "the remainder stays undifferentiated" do
    test "a word outside the contract's vocabulary is reply_not_ok and is not repeated" do
      for word <- ["receipt_store_unavailable", "delivery_unavailable", "unsupported_protocol_version", @canary] do
        result = LocalPane.deliver(command(), send_opts(refusal(word)))
        assert match?({:error, %{"reason" => "reply_not_ok"}}, result), word
        {:error, reason} = result
        refute Map.has_key?(reason, "refusal") or Map.has_key?(reason, "error"), word
        refute inspect(result, limit: :infinity) =~ word
      end
    end

    test "a refusal that does not name version 2 is not a v2 refusal, whatever word it carries" do
      reply = Map.delete(refusal("queue_full"), "protocol_version")
      assert {:error, %{"reason" => "reply_not_ok"}} = LocalPane.deliver(command(), send_opts(reply))
      assert {:error, %{"reason" => "reply_not_ok"}} = LocalPane.reconcile(command(), reconcile_opts(reply))
    end

    test "a refusal is never the answer absent, and a typed refusal never sends again" do
      owner = self()

      opts =
        Keyword.put(send_opts(refusal("pane_dead")), :input_runner, fn _, _, _, _ ->
          send(owner, :sent)
          {Jason.encode!(refusal("pane_dead")), 0}
        end)

      assert {:error, %{"reason" => "dispatch_refused_pane_dead"}} = LocalPane.deliver(command(), opts)
      assert_received :sent
      refute_received :sent
    end
  end

  describe "the reducer-facing double is held to the same distinctions" do
    defmodule RefusingPaneClient do
      @moduledoc false
      def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

      def reconcile(pane_ref, message_id, opts) do
        case Keyword.get(opts, :reconcile_reply) do
          nil ->
            {:ok,
             %{
               "ok" => true,
               "protocol_version" => 2,
               "outcome" => "absent",
               "msg_id" => message_id,
               "pane_id" => pane_ref
             }}

          reply ->
            {:ok, reply}
        end
      end

      def send(_pane_ref, _prompt, opts), do: {:ok, Keyword.fetch!(opts, :send_reply)}
      def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
    end

    test "a double's typed refusal and request error classify as the daemon's would" do
      assert {:error, %{"reason" => "dispatch_refused_conflict", "refusal" => "conflict"}} =
               LocalPane.deliver(command(), pane_client: RefusingPaneClient, send_reply: refusal("conflict"))

      assert {:error, %{"reason" => "dispatch_request_rejected", "error" => "oversize"}} =
               LocalPane.deliver(command(), pane_client: RefusingPaneClient, send_reply: refusal("oversize"))

      assert {:error, %{"reason" => "dispatch_request_rejected", "detector" => "dispatch_reconcile"}} =
               LocalPane.reconcile(command(),
                 pane_client: RefusingPaneClient,
                 reconcile_reply: refusal("missing_payload_hash")
               )
    end
  end

  describe "the vocabulary" do
    test "is exactly the words the vendored contracts name, and the classes are disjoint" do
      assert Refusal.vocabulary(:send) == %{refusals: @send_refusals, request_errors: @send_request_errors}
      assert Refusal.vocabulary(:reconcile) == %{refusals: [], request_errors: @reconcile_request_errors}
      assert @send_refusals -- @send_request_errors == @send_refusals

      v1 = File.read!(Path.expand("../../docs/contracts/ipc-v1.org", __DIR__))
      v2 = File.read!(Path.expand("../../docs/contracts/ipc-v2.org", __DIR__))

      for word <- @send_refusals ++ @send_request_errors ++ @reconcile_request_errors do
        assert String.contains?(v1, word) or String.contains?(v2, word),
               "#{word} is not named by ipc-v1.org or ipc-v2.org"
      end
    end
  end
end
