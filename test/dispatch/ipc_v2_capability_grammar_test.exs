defmodule AiOrchestrator.Dispatch.IpcV2CapabilityGrammarTest do
  @moduledoc """
  NS-42 rule 2, the three clauses the register records as still UNPROVEN after the
  executed-path pin (8977b5d): "unknown-extra-token admissibility, malformed-token
  rejection and daemon replacement after preflight remain UNPROVEN".

  The existing evidence proves the *decision* the capability set drives -- a daemon
  advertising `delivery_reconcile` admits one send, a daemon without it refuses before
  `ap send` runs. It says nothing about the grammar the set is read under, which is the
  other half of rule 2: a capability list is a forward-compatible vocabulary, so the
  consumer must accept words it has never heard of and must refuse words that are not
  words at all. Accepting everything and refusing everything are both ways of making the
  declaration stop meaning anything.

  Three properties, in order:

    * An unknown but well-formed token is admissible. Punctuation, case, digits and
      non-ASCII spellings are all future vocabulary; none of them may turn a daemon that
      *does* advertise `delivery_reconcile` into one that cannot be dispatched to.
    * A malformed token is refused, before any pane effect. "Malformed" is the closed
      judgement `Dispatch.valid_capabilities?/1` makes -- empty, whitespace-bearing,
      control-bearing, invalid UTF-8, or not a string at all -- and the refusal arrives
      at two different boundaries under two different names, which is asserted rather
      than glossed: `PaneClient` answers `dispatch_capabilities_invalid` for the wire it
      read, and `Dispatch.preflight` answers `dispatch_capabilities_failed` for an
      adapter whose query failed. Both refuse before a paste.
    * A daemon replaced after the preflight is still validated per effect. The ping is
      not a session: it authorizes an attempt, not a reply, so a reply from a daemon
      that is not the one that answered the ping is refused on its own terms and never
      becomes optimistic success or `absent`.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime

  @id "snd_" <> String.duplicate("a", 64)
  @other_id "snd_" <> String.duplicate("e", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)
  @other_pane "%" <> Integer.to_string(7)

  # Well-formed under the wire's own rule ("reject malformed tokens, not future
  # punctuation, case or Unicode spellings the adapter does not yet understand"), and
  # unknown to this release. Each is a spelling a later daemon could plausibly choose.
  @unknown_tokens [
    "future_v3",
    "delivery-reconcile-v3",
    "delivery.reconcile.v3",
    "DELIVERY_RECONCILE",
    "subscribe:v1",
    "pane_identity/1",
    "sessions?",
    "capability-" <> String.duplicate("z", 200),
    "télémétrie",
    "能力"
  ]

  # Built, not written: `"\\u0000..."` is normalized by the formatter into a literal NUL in
  # this file, which makes the source a binary to every tool that reads it.
  @control_token <<0>> <> "delivery_reconcile"

  # Malformed under the same rule: not a token at all, whatever a daemon meant by it.
  # Every one of these is a value the JSON the daemon speaks can actually carry, which is
  # what makes them a wire claim rather than an in-process one.
  @malformed_tokens [
    "",
    " ",
    "delivery reconcile",
    "delivery_reconcile\n",
    "delivery_reconcile\t",
    @control_token,
    1,
    2.0,
    nil,
    true,
    ["delivery_reconcile"],
    %{"name" => "delivery_reconcile"}
  ]

  # Malformed and NOT carriable by the wire: a JSON string is UTF-8 by definition, so
  # invalid bytes cannot arrive from a daemon at all -- measured here, because the
  # predicate's `String.valid?/1` clause would otherwise look like dead code. An adapter
  # is an in-process module and can return one, so the clause is reachable and is pinned
  # against the predicate directly rather than through a reply nobody can send.
  @unencodable_token <<0xFF, 0xFE>>

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-capability-missing-artifact",
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  defp answer(fields),
    do: Map.merge(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane}, fields)

  defp ping(tokens), do: %{"ok" => true, "protocol_version" => 2, "pong" => "x", "capabilities" => tokens}

  # ping answers the capability list under test; reconcile answers a no-record absent, so
  # the only thing that can stop a paste is the capability judgement itself. Every argv
  # call is reported, so a refusal can be shown to have happened before `ap send` ran.
  defp opts(tokens) do
    owner = self()

    [
      ap_path: "/tmp/v2-capability-ap",
      runner: fn _, args, _ ->
        send(owner, {:ap, hd(args)})
        {Jason.encode!(if(hd(args) == "ping", do: ping(tokens), else: answer(%{"outcome" => "absent"}))), 0}
      end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:ap, hd(args)})
        {Jason.encode!(answer(%{"status" => "sent"})), 0}
      end
    ]
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

  # `assert PATTERN = EXPR, MSG` evaluates the match before the assertion runs, so a
  # failing pattern raises MatchError and the message naming WHICH token failed is never
  # printed -- in a file whose every claim is quantified over a list of tokens, that is the
  # one thing a reader needs. The repository guards the shape; these two read the answer as
  # data instead, so every row can say `== LITERAL, MSG`.
  defp reason_of({:error, %{} = reason}), do: reason
  defp reason_of(_other), do: %{}

  defp ok?(result), do: match?({:ok, _answer}, result)

  describe "an unknown well-formed token is admissible" do
    test "each unknown spelling is admitted beside delivery_reconcile, and the send still happens" do
      for token <- @unknown_tokens do
        result = LocalPane.deliver(command(), opts(["delivery_reconcile", token]))

        assert ok?(result), "#{inspect(token)}: #{inspect(result)}"
        assert "send" in verbs(), inspect(token)
      end
    end

    test "the whole unknown set at once, with the required token last, is still admissible" do
      tokens = @unknown_tokens ++ ["delivery_reconcile"]

      assert {:ok, %{"send_status" => "ok"}} = LocalPane.deliver(command(), opts(tokens))
      assert "send" in verbs()
    end

    test "the adapter narrows its declaration to what it actually requires, whatever else was offered" do
      assert {:ok, ["delivery_reconcile"]} =
               LocalPane.capabilities(opts(@unknown_tokens ++ ["delivery_reconcile"]))

      assert Dispatch.valid_capabilities?(@unknown_tokens),
             "the unknown set has to be well formed for the admissibility claim above to be about admissibility"
    end

    test "an unknown token cannot stand in for the required one" do
      assert {:error, %{"reason" => "dispatch_preflight_unsupported"}} =
               LocalPane.deliver(command(), opts(@unknown_tokens))

      assert verbs() == ["ping"], "a daemon without the capability is refused before it is asked to send"
    end
  end

  describe "a malformed token is refused before any pane effect" do
    test "each malformed token refuses the dispatch, and ap send is never reached" do
      for token <- @malformed_tokens do
        result = LocalPane.deliver(command(), opts(["delivery_reconcile", token]))

        assert reason_of(result)["reason"] == "dispatch_capabilities_failed", inspect(token)
        assert reason_of(result)["detector"] == "dispatch_preflight", inspect(token)
        assert verbs() == ["ping"], "#{inspect(token)} reached the pane"
      end
    end

    test "a malformed token is refused even when it is the only thing offered" do
      for token <- @malformed_tokens do
        result = LocalPane.deliver(command(), opts([token]))

        assert reason_of(result)["reason"] == "dispatch_capabilities_failed", inspect(token)
        assert verbs() == ["ping"], inspect(token)
      end
    end

    test "the wire boundary names the malformed list for what it is, rather than as a failed query" do
      for token <- @malformed_tokens do
        result = PaneClient.capabilities(opts(["delivery_reconcile", token]))

        assert reason_of(result)["reason"] == "dispatch_capabilities_invalid", inspect(token)
        refute Dispatch.valid_capabilities?([token]), inspect(token)
      end
    end

    test "an adapter that declares a malformed token itself is refused as invalid, not as failed" do
      defmodule MalformedAdapter do
        @moduledoc false
        def capabilities(_opts), do: {:ok, ["delivery_reconcile", "malformed token"]}
      end

      assert {:error, %{"reason" => "dispatch_capabilities_invalid", "detector" => "dispatch_preflight"}} =
               Dispatch.preflight(MalformedAdapter, [])
    end

    test "invalid UTF-8 is malformed, and is a claim about adapters rather than about the wire" do
      refute Dispatch.valid_capabilities?([@unencodable_token])

      assert_raise Jason.EncodeError, fn -> Jason.encode!(%{"capabilities" => [@unencodable_token]}) end

      defmodule UnencodableAdapter do
        @moduledoc false
        def capabilities(_opts), do: {:ok, [<<0xFF, 0xFE>>]}
      end

      assert reason_of(Dispatch.preflight(UnencodableAdapter, []))["reason"] == "dispatch_capabilities_invalid",
             """
             A JSON string is UTF-8 by construction, so no conforming daemon can put these
             bytes on the wire and the encoder refuses to pretend otherwise. The predicate
             still has to judge them, because an adapter is an ordinary module returning an
             ordinary binary and nothing on that path went through a JSON decoder.
             """
    end

    test "a capabilities field that is not a list is not a capability set, and admits nothing" do
      for shape <- ["delivery_reconcile", %{"delivery_reconcile" => true}, 2, true] do
        result = LocalPane.deliver(command(), opts(shape))

        assert reason_of(result)["reason"] == "dispatch_preflight_unsupported", inspect(shape)
        assert verbs() == ["ping"], inspect(shape)
      end
    end

    test "a malformed declaration reaches the reducer as the failed dispatch observation" do
      intent = %Effect.Dispatch{assignment_id: "as_0001", message_id: @id, command: command(), deadline_unix: 0}

      {observation, _runtime} =
        Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: opts(["delivery_reconcile", "bad token"])])

      assert %Observation.DispatchFailed{assignment_id: "as_0001", reason: reason} = observation
      assert reason["reason"] == "dispatch_capabilities_failed"
      assert reason["detector"] == "dispatch_preflight"
    end
  end

  describe "the ping authorizes an attempt, not a reply" do
    # A daemon replaced between the preflight and the effect: the ping was answered by one
    # that advertises the capability, and the effect is answered by one that does not bind.
    # Each reply shape is refused on its own terms; none of them is `absent` and none is
    # an optimistic success.
    @replacements [
      {"an unversioned reply", %{"ok" => true, "outcome" => "absent"}, "protocol_version_unsupported"},
      {"another pane", %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @other_pane},
       "reply_pane_mismatch"},
      {"another message", %{"ok" => true, "protocol_version" => 2, "msg_id" => @other_id, "pane_id" => @pane},
       "reply_identity_mismatch"},
      {"no identities at all", %{"ok" => true, "protocol_version" => 2, "outcome" => "absent"}, "reply_identity_missing"}
    ]

    @pane_refusal %{
      "ok" => false,
      "protocol_version" => 2,
      "error" => "pane_not_found",
      "msg_id" => @id,
      "pane_id" => @pane
    }

    defp replaced_opts(reply, verb) do
      owner = self()
      good = ping(["delivery_reconcile"])

      [
        ap_path: "/tmp/v2-capability-ap",
        runner: fn _, args, _ ->
          send(owner, {:ap, hd(args)})

          response =
            cond do
              hd(args) == "ping" -> good
              verb == "reconcile" -> reply
              true -> answer(%{"outcome" => "absent"})
            end

          {Jason.encode!(response), 0}
        end,
        input_runner: fn _, args, _, _ ->
          send(owner, {:ap, hd(args)})
          {Jason.encode!(if(verb == "send", do: reply, else: answer(%{"status" => "sent"}))), 0}
        end
      ]
    end

    for {label, reply, expected} <- @replacements do
      test "a reconcile answered by #{label} is refused per effect, never absent" do
        result = LocalPane.deliver(command(), replaced_opts(unquote(Macro.escape(reply)), "reconcile"))

        assert {:error, %{"reason" => unquote(expected)}} = result
        refute "send" in verbs(), "#{unquote(label)} must not license a paste"
      end
    end

    for {label, reply, expected} <- @replacements do
      test "a send answered by #{label} is refused per effect, and the answer is not a success" do
        result = LocalPane.deliver(command(), replaced_opts(unquote(Macro.escape(reply)), "send"))

        assert {:error, %{"reason" => unquote(expected)}} = result
        refute match?({:ok, _}, result), unquote(label)
      end
    end

    test "the replacement cases are refusals about the reply, not about the capability" do
      for {label, reply, _expected} <- @replacements do
        {:error, reason} = LocalPane.deliver(command(), replaced_opts(reply, "reconcile"))

        refute reason["detector"] == "dispatch_preflight", """
        #{label} was refused by the preflight rather than by the reply, so this file would be
        asserting that the ping failed rather than that a validated ping does not vouch for
        what comes after it.
        """

        verbs()
      end
    end

    test "a replacement that refuses the send names the pane word; the same word on a reconcile does not" do
      assert {:error, %{"reason" => "dispatch_refused_pane_not_found", "refusal" => "pane_not_found"}} =
               LocalPane.deliver(command(), replaced_opts(@pane_refusal, "send"))

      verbs()

      assert {:error, %{"reason" => "reply_not_ok"} = reason} =
               LocalPane.deliver(command(), replaced_opts(@pane_refusal, "reconcile"))

      refute Map.has_key?(reason, "refusal"), """
      This asymmetry is the vendored contract's, not a defect found here. ipc-v2.org names
      typed pane refusals only for send replies; a reconcile answer is one of the five
      outcomes or one of the named request errors, and `reconcile.error.*` has exactly one
      fixture, `missing_payload_hash`. So a pane word arriving on a reconcile is a word the
      contract does not give that operation, and the consumer declines to name it rather
      than promoting it into a vocabulary the document does not have. Both legs refuse and
      neither is absent, which is what rule 2 requires of a replaced daemon.
      """

      refute "send" in verbs()
    end

    test "the same harness with a conforming daemon does send, so the refusals are about the replacement" do
      assert {:ok, %{"send_status" => "ok"}} =
               LocalPane.deliver(command(), replaced_opts(answer(%{"outcome" => "absent"}), "reconcile"))

      assert "send" in verbs()
    end
  end
end
