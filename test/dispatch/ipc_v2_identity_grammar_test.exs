defmodule AiOrchestrator.Dispatch.IpcV2IdentityGrammarTest do
  @moduledoc """
  NS-42 rule 3, the producer half the consumer could not name.

  Rule 3 says both echoes match or the send becomes durable attention, and the consumer
  implements that in three places. But there is a case where the producer cannot echo at
  all: when an identity fails the producer's own grammar there is nothing safe to echo
  back, so the daemon drops that echo and answers `invalid_msg_id` or `invalid_pane_id`.
  Neither word was in the consumer's closed vocabulary, so both degraded into
  `reply_not_ok` -- the undifferentiated remainder, which by design does not repeat the
  word. The operator was told the reply was not ok and nothing else, for a failure whose
  fix is in the request this consumer built.

  MEASURED BEFORE THE CHANGE (R06 audit section 4, rule 3): with the words absent from
  `@send_request_errors` / `@reconcile_request_errors`, every row below answered
  `%{"reason" => "reply_not_ok"}` with no `error` key -- correct in the only way that
  matters (never `absent`, never a paste) and unusable for anything else.

  Both words are producer-emitted today, so naming them is a classification the consumer
  was missing rather than a new word on the wire: the request-error class is exactly "a
  fact about the request this consumer built", and an identity that fails the grammar is
  that fact. The class requires no echo, which is what makes it the right class -- the
  daemon dropped the echo precisely because it could not read the identity.

  The two pane grammars: the producer admits a hyphen when it decides what to ECHO and
  rejects it when it decides what to STORE, so a hyphenated pane is echoed and then
  refused. That reconciliation is a one-predicate change in the daemon and is owed in the
  producer repository; what is owed here is that the consumer names the refusal when it
  arrives, and that the governing grammar is written down in the vendored contract. Both
  are in this commit.
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
  @hash "sha256:" <> String.duplicate("b", 64)
  @detail "GRAMMAR_CANARY_PRIVATE_DETAIL"
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)
  # The divergence case: the producer's echo predicate admits a hyphen and its receipt
  # predicate does not, so this is the spelling that is echoed and then refused.
  @hyphenated "%" <> Integer.to_string(9) <> "-b"

  @words ~w(invalid_msg_id invalid_pane_id)

  defp command(pane_ref \\ @pane) do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => pane_ref,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-identity-grammar-missing-artifact",
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  # The daemon's refusal as it actually leaves the daemon: the echo for the identity that
  # failed the grammar is DROPPED, because there is nothing it could safely echo.
  defp refusal(word, pane_ref \\ @pane) do
    echoes =
      case word do
        "invalid_msg_id" -> %{"pane_id" => pane_ref}
        "invalid_pane_id" -> %{"msg_id" => @id}
      end

    Map.merge(%{"ok" => false, "protocol_version" => 2, "error" => word, "detail" => @detail}, echoes)
  end

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  # `assert PATTERN = EXPR, MSG` evaluates the match before the assertion runs, so the message
  # never prints and the repository guards the shape. These are the repairs, named once.
  defp rejected?(result, word), do: match?({:error, %{"reason" => "dispatch_request_rejected", "error" => ^word}}, result)

  defp not_ok?(result), do: match?({:error, %{"reason" => "reply_not_ok"}}, result)

  defp absent(pane_ref),
    do: %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => pane_ref, "outcome" => "absent"}

  # ping and reconcile admit the send; the send itself answers `reply`.
  defp send_opts(reply, pane_ref \\ @pane) do
    owner = self()

    [
      ap_path: "/tmp/v2-identity-grammar-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: absent(pane_ref))), 0} end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:pasted, args})
        {Jason.encode!(reply), 0}
      end
    ]
  end

  defp reconcile_opts(reply) do
    [
      ap_path: "/tmp/v2-identity-grammar-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: reply)), 0} end,
      input_runner: fn _, _, _, _ -> flunk("a reconcile never sends") end
    ]
  end

  describe "the vocabulary names both producer words" do
    test "each word is a request error on both operations, and neither is a typed refusal" do
      %{refusals: send_refusals, request_errors: send_errors} = Refusal.vocabulary(:send)
      %{refusals: reconcile_refusals, request_errors: reconcile_errors} = Refusal.vocabulary(:reconcile)

      for word <- @words do
        assert word in send_errors, "#{word} is a fact about the request this consumer built"
        assert word in reconcile_errors, word
        refute word in send_refusals, "#{word} is not a fact about the pane or about admission"
        refute word in reconcile_refusals, word
      end
    end

    test "both words are named by the vendored contract, so the vocabulary is not invented here" do
      v1 = File.read!(Path.expand("../../docs/contracts/ipc-v1.org", __DIR__))
      v2 = File.read!(Path.expand("../../docs/contracts/ipc-v2.org", __DIR__))

      for word <- @words do
        assert String.contains?(v1, word) or String.contains?(v2, word),
               "#{word} must be named by the contract the consumer validates against, not only by this test"
      end
    end
  end

  describe "a grammar refusal is named rather than collapsed into the remainder" do
    test "a send refused for either identity is a request error carrying the word" do
      for word <- @words do
        result = LocalPane.deliver(command(), send_opts(refusal(word)))

        assert match?(
                 {:error, %{"reason" => "dispatch_request_rejected", "detector" => "dispatch_send", "error" => ^word}},
                 result
               ),
               "#{word}: #{inspect(result)}"

        refute match?({:error, %{"reason" => "reply_not_ok"}}, result),
               "#{word} is the behaviour this commit changed: it must no longer be the undifferentiated remainder"

        refute inspect(result, limit: :infinity) =~ @detail, "the daemon's detail is still never repeated"
      end
    end

    test "a reconcile refused for either identity is a request error on the reconcile detector" do
      for word <- @words do
        expected = %{"reason" => "dispatch_request_rejected", "detector" => "dispatch_reconcile", "error" => word}

        assert LocalPane.reconcile(command(), reconcile_opts(refusal(word))) == {:error, expected}, word

        assert PaneClient.reconcile(@pane, @id, Keyword.put(reconcile_opts(refusal(word)), :payload_hash, @hash)) ==
                 {:error, expected},
               word
      end
    end

    test "the class requires no echo, because the daemon dropped the echo it could not read" do
      for word <- @words do
        bare = %{"ok" => false, "protocol_version" => 2, "error" => word}

        assert rejected?(LocalPane.deliver(command(), send_opts(bare)), word), word
        assert rejected?(LocalPane.reconcile(command(), reconcile_opts(bare)), word), word
      end
    end

    test "the word is still refused when it does not name version 2, because a v1 daemon has no receipt" do
      for word <- @words do
        reply = Map.delete(refusal(word), "protocol_version")
        assert not_ok?(LocalPane.deliver(command(), send_opts(reply))), word
        assert not_ok?(LocalPane.reconcile(command(), reconcile_opts(reply))), word
      end
    end
  end

  describe "the divergent pane grammar is the case that produced the collapse" do
    test "a hyphenated pane is echoed by the producer and then refused, and the consumer names the refusal" do
      # The daemon's echo predicate admits the hyphen, so the reconcile that precedes the
      # send binds and answers absent; the receipt predicate rejects it, so the send that
      # follows is refused `invalid_pane_id` with no pane echo. Before this commit that
      # pair was indistinguishable from any other not-ok reply.
      result = LocalPane.deliver(command(@hyphenated), send_opts(refusal("invalid_pane_id", @hyphenated), @hyphenated))

      assert rejected?(result, "invalid_pane_id"), inspect(result)
      assert_received {:pasted, _}, "the paste is the attempt the daemon refused, not a retry of it"
      refute_received {:pasted, _}
    end

    test "a refusal is never an answer about delivery, and never a second paste" do
      for word <- @words do
        result = LocalPane.deliver(command(), send_opts(refusal(word)))
        assert match?({:error, _error}, result), "#{word}: #{inspect(result)}"
        {:error, error} = result
        refute Map.has_key?(error, "outcome"), word
        refute Map.has_key?(error, "send_status"), word
        assert_received {:pasted, _}
        refute_received {:pasted, _}, word
      end
    end
  end

  describe "the refusal reaches durable attention with its own class" do
    test "a refused send is the failed dispatch observation, naming the word" do
      for word <- @words do
        intent = %Effect.Dispatch{assignment_id: "as_0001", message_id: @id, command: command(), deadline_unix: 0}

        {observation, _runtime} =
          Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: send_opts(refusal(word))])

        assert %Observation.DispatchFailed{assignment_id: "as_0001", reason: reason} = observation
        assert reason["reason"] == "dispatch_request_rejected", word
        assert reason["error"] == word
        refute inspect(observation, limit: :infinity) =~ @detail
      end
    end

    test "a refused reconcile is the failed reconcile observation, never an outcome" do
      for word <- @words do
        intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

        {observation, _runtime} =
          Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: reconcile_opts(refusal(word))])

        assert %Observation.SendReconcileFailed{reason: reason} = observation
        assert reason["reason"] == "dispatch_request_rejected", word
        assert reason["error"] == word
      end
    end
  end
end
