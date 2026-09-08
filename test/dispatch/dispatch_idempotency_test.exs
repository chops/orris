defmodule AiOrchestrator.Dispatch.IdempotencyTest do
  @moduledoc """
  RED for the dispatch idempotency surface, revised against Codex review
  `m_1788508818157722000_82f1b70d`.

  GAP-1: a crash between the pane send and the journaled `assignment_dispatch_sent` event
  leaves replay unable to tell whether the bytes reached the pane. The orchestrator's own
  journal cannot answer that question -- only the daemon observed the paste -- so the
  daemon must keep a durable receipt and expose a query the orchestrator can bind to the
  exact payload it believes it sent.

  This file pins the adapter boundary that query travels over. Four properties are load
  bearing and each one is a way the naive version silently loses or duplicates work:

    * the query binds the payload (MUST-2), so a same-id/different-payload state is a
      `conflict` rather than a false `delivered`;
    * the reply's identity is checked (MUST-8), so a stale or crossed answer cannot be
      read as an answer about this message;
    * the capability is proven before any side effect (R1/R4), so a daemon that cannot
      reconcile is refused rather than silently degraded into "assume nothing was sent";
    * the absence of an answer is never the answer `absent` (R4), so an unknown command
      or a transport failure blocks for attention instead of duplicating a prompt.

  The journal-facing status is separately constrained: EJ-7 ratified
  `assignment_dispatch_sent.send_status` as `ok | queued | reconciled`, so the daemon's
  own vocabulary is normalized at this boundary and never leaks into journal data
  (MUST-6).
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient

  @msg_id "snd_" <> String.duplicate("ab", 32)
  @payload_hash "sha256:" <> String.duplicate("cd", 32)

  describe "PaneClient transmits the journaled idempotency key" do
    test "send passes --msg-id with the journaled message id" do
      parent = self()

      input_runner = fn ap_path, args, input, opts ->
        Process.send(parent, {:ap_call, ap_path, args, input, opts}, [])
        # M2: a v2 send reply echoes the pane as well as the message.
        {reply(%{"status" => "sent", "pane_id" => "pane_writer"}), 0}
      end

      assert {:ok, %{"status" => "sent"}} =
               PaneClient.send("pane_writer", "Do the work",
                 ap_path: "/tmp/ap",
                 message_id: @msg_id,
                 input_runner: input_runner
               )

      assert_receive {:ap_call, "/tmp/ap", args, "Do the work", _opts}

      # M1 (S1 review): a v2 operation states its version on the wire, not only in options.
      assert args == ["send", "pane_writer", "--stdin", "--msg-id", @msg_id, "--protocol-version", "2"],
             "PaneClient.send/3 must transmit the journaled message id and the protocol version, got: #{inspect(args)}"
    end

    test "send without a message id keeps the pre-existing argument vector" do
      parent = self()

      input_runner = fn ap_path, args, input, opts ->
        Process.send(parent, {:ap_call, ap_path, args, input, opts}, [])
        {~s({"ok":true,"status":"sent"}\n), 0}
      end

      assert {:ok, _} = PaneClient.send("pane_writer", "hi", ap_path: "/tmp/ap", input_runner: input_runner)
      assert_receive {:ap_call, "/tmp/ap", ["send", "pane_writer", "--stdin"], "hi", _opts}
    end

    test "reconcile binds the message id to the payload hash" do
      parent = self()

      runner = fn ap_path, args, opts ->
        Process.send(parent, {:ap_call, ap_path, args, opts}, [])
        {reply(%{"outcome" => "delivered", "pane_id" => "pane_writer"}), 0}
      end

      assert {:ok, %{"outcome" => "delivered"}} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      assert_receive {:ap_call, "/tmp/ap", args, _opts}

      assert args == [
               "reconcile",
               "pane_writer",
               "--msg-id",
               @msg_id,
               "--payload-hash",
               @payload_hash,
               "--protocol-version",
               "2"
             ],
             """
             MUST-2: a reconcile carrying only a pane and a message id cannot detect a
             same-id/different-payload conflict, so it would answer `delivered` about a
             prompt the daemon never saw. Got: #{inspect(args)}
             """
    end

    test "reconcile refuses to run without a payload hash" do
      runner = fn _ap_path, _args, _opts -> flunk("the daemon must not be asked an unbound question") end

      assert {:error, reason} = PaneClient.reconcile("pane_writer", @msg_id, ap_path: "/tmp/ap", runner: runner)
      assert reason["reason"] == "reconcile_payload_hash_missing"
    end

    test "reconcile rejects a payload hash outside the pinned grammar" do
      runner = fn _ap_path, _args, _opts -> flunk("a malformed hash must never reach the daemon") end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: "cafebabe",
                 runner: runner
               )

      assert reason["reason"] == "reconcile_payload_hash_invalid"
    end

    test "reconcile never puts prompt bytes on the wire" do
      parent = self()

      runner = fn ap_path, args, opts ->
        Process.send(parent, {:ap_call, ap_path, args, opts}, [])
        {reply(%{"outcome" => "delivered", "pane_id" => "pane_writer"}), 0}
      end

      assert {:ok, _} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 prompt: "Implement item A",
                 runner: runner
               )

      assert_receive {:ap_call, "/tmp/ap", args, opts}

      refute Enum.any?(args, &String.contains?(&1, "Implement item A")),
             "NS-42 rule 5: the hash is the binding; prompt bytes must never travel on reconcile"

      refute Keyword.has_key?(opts, :input),
             "reconcile is a query and must not carry a payload body"
    end

    test "capabilities asks the daemon what it can do" do
      parent = self()

      runner = fn ap_path, args, opts ->
        Process.send(parent, {:ap_call, ap_path, args, opts}, [])
        {reply(%{"capabilities" => ["delivery_reconcile"]}), 0}
      end

      assert {:ok, ["delivery_reconcile"]} = PaneClient.capabilities(ap_path: "/tmp/ap", runner: runner)
      assert_receive {:ap_call, "/tmp/ap", ["ping", "--protocol-version", "2"], _opts}
    end
  end

  describe "protocol version 2 replies" do
    test "a reply without protocol_version 2 is refused" do
      runner = fn _ap_path, _args, _opts ->
        {~s({"ok":true,"msg_id":"#{@msg_id}","outcome":"delivered"}\n), 0}
      end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      assert reason["reason"] == "protocol_version_unsupported",
             """
             NS-42 rule 1: a reply with no explicit protocol_version is a v1 reply, and a
             v1 daemon has no durable receipt. Reading it as a v2 answer would treat
             "this daemon does not remember" as "this message was never sent".
             """
    end

    test "a reply whose msg_id does not echo the request is a conflict, not an answer" do
      runner = fn _ap_path, _args, _opts ->
        {reply(%{"outcome" => "absent", "msg_id" => "snd_" <> String.duplicate("ef", 32)}), 0}
      end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      assert reason["reason"] == "reply_identity_mismatch",
             """
             MUST-8: an answer about a different message id says nothing about this one.
             Accepting it as `absent` would resend a prompt that is already in flight.
             """
    end

    test "a reply with no msg_id at all is a conflict, not an answer" do
      # Built without reply/1 on purpose: that helper put_new's the message id, so a reply
      # "with no msg_id at all" made through it always had one, and this test could never
      # have failed for the reason it names.
      runner = fn _ap_path, _args, _opts ->
        {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "outcome" => "absent"}) <> "\n", 0}
      end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      assert reason["reason"] == "reply_identity_missing"
    end

    test "a send reply must echo the message id too" do
      input_runner = fn _ap_path, _args, _input, _opts ->
        {reply(%{"status" => "sent", "msg_id" => "snd_" <> String.duplicate("ef", 32)}), 0}
      end

      assert {:error, reason} =
               PaneClient.send("pane_writer", "Do the work",
                 ap_path: "/tmp/ap",
                 message_id: @msg_id,
                 input_runner: input_runner
               )

      assert reason["reason"] == "reply_identity_mismatch"
    end
  end

  describe "absence of an answer is never the answer `absent`" do
    test "an unknown command is refused rather than read as absent" do
      runner = fn _ap_path, _args, _opts ->
        {~s({"ok":false,"error":"unknown command: reconcile"}\n), 1}
      end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      assert reason["reason"] == "reconcile_unsupported",
             """
             R4: a daemon that does not implement reconcile has not told us the message is
             absent -- it has told us nothing. Mapping it to `absent` duplicates prompts on
             every resume against an old daemon.
             """

      refute reason["outcome"] == "absent"
    end

    test "a transport failure is refused rather than read as absent" do
      runner = fn _ap_path, _args, _opts -> {"", 127} end

      assert {:error, reason} =
               PaneClient.reconcile("pane_writer", @msg_id,
                 ap_path: "/tmp/ap",
                 payload_hash: @payload_hash,
                 runner: runner
               )

      refute reason["outcome"] == "absent"
    end
  end

  describe "Dispatch behaviour" do
    test "declares a reconcile callback" do
      callbacks = Dispatch.behaviour_info(:callbacks)

      assert {:reconcile, 2} in callbacks,
             """
             R1: delivery idempotency is part of dispatch semantics, not an optional
             adapter feature. Declared callbacks: #{inspect(callbacks)}
             """
    end

    test "preflight admits an adapter that implements reconcile" do
      assert :ok = Dispatch.preflight(LocalPane)
    end

    test "preflight refuses an adapter that cannot reconcile" do
      defmodule CannotReconcile do
        @moduledoc false
        def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

        def deliver(_command, _opts), do: {:ok, %{}}
        def observe(_command, _opts), do: {:ok, %{}}
      end

      assert {:error, reason} = Dispatch.preflight(CannotReconcile)

      assert reason["reason"] == "dispatch_adapter_cannot_reconcile",
             """
             R1: an adapter that cannot reconcile is not admissible for durable execution.
             It must fail preflight -- a success stub would make every resume claim the
             prompt was never sent.
             """
    end
  end

  describe "LocalPane proves the capability before it causes a side effect" do
    defmodule RecordingPaneClient do
      @moduledoc false

      def capabilities(opts) do
        Process.send(Keyword.fetch!(opts, :test_pid), :capabilities_called, [])
        {:ok, Keyword.get(opts, :capabilities, ["delivery_reconcile"])}
      end

      def send(pane_ref, prompt, opts) do
        Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, prompt, opts[:message_id]}, [])
        # M2: a v2 send reply echoes both the message and the pane; a stub standing in for
        # the daemon is held to the daemon's contract.
        echoes = %{"msg_id" => opts[:message_id], "pane_id" => pane_ref}

        {:ok,
         Map.merge(echoes, Keyword.get(opts, :send_reply, %{"ok" => true, "protocol_version" => 2, "status" => "sent"}))}
      end

      def reconcile(pane_ref, message_id, opts) do
        Process.send(
          Keyword.fetch!(opts, :test_pid),
          {:reconcile_called, pane_ref, message_id, opts[:payload_hash]},
          []
        )

        default = %{
          "ok" => true,
          "protocol_version" => 2,
          "outcome" => "absent",
          "msg_id" => message_id,
          "pane_id" => pane_ref
        }

        # A receipt-bearing answer names its attempt, as the wire fixtures do; the tests
        # that probe the attempt itself set it explicitly.
        reply = Keyword.get(opts, :reconcile_reply, default)

        reply =
          if reply["outcome"] in ~w(delivered queued ambiguous),
            do: Map.put_new(reply, "delivery_attempt", 1),
            else: reply

        {:ok, reply}
      end

      def status(pane_ref, opts) do
        {:ok, Keyword.get(opts, :pane_status, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0})}
      end
    end

    test "deliver refuses a daemon that does not advertise delivery_reconcile" do
      assert {:error, reason} =
               LocalPane.deliver(command(),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 capabilities: ["pane_status"]
               )

      # D3 (reconcile RED, later revision): the refusal is the named preflight step, and it
      # carries the capability both as the adapter-level `capability` and as the operator-
      # facing `missing_capabilities` the attention record repeats.
      assert reason["reason"] == "dispatch_preflight_unsupported"
      assert reason["capability"] == "delivery_reconcile"
      assert "delivery_reconcile" in reason["missing_capabilities"]

      refute_received {:send_called, _, _, _},
                      "R1: the capability must be proven before the pane side effect, not after"
    end

    test "deliver forwards the journaled send_message_id to the pane client" do
      assert {:ok, _} = LocalPane.deliver(command(), pane_client: RecordingPaneClient, test_pid: self())

      assert_receive {:send_called, "pane_writer", "Implement item A", message_id}

      assert message_id == @msg_id,
             "LocalPane.deliver/2 must pass command[\"send_message_id\"] to the pane client, got: #{inspect(message_id)}"
    end

    test "deliver normalizes a fresh send to the ratified EJ-7 value ok" do
      assert {:ok, result} = LocalPane.deliver(command(), pane_client: RecordingPaneClient, test_pid: self())

      assert result["send_status"] == "ok",
             """
             MUST-6: EJ-7 ratified send_status as ok | queued | reconciled. The daemon's own
             "sent" must be normalized here; leaking it would widen ratified journal data
             without an event_version bump. Got: #{inspect(result["send_status"])}
             """
    end

    test "deliver normalizes a fresh queued send to queued" do
      assert {:ok, result} =
               LocalPane.deliver(command(),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 send_reply: %{
                   "ok" => true,
                   "protocol_version" => 2,
                   "status" => "queued",
                   "queue_reason" => "busy"
                 }
               )

      assert result["send_status"] == "queued"
    end

    test "deliver refuses a daemon status outside the mapped vocabulary" do
      assert {:error, reason} =
               LocalPane.deliver(command(),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 send_reply: %{"ok" => true, "protocol_version" => 2, "status" => "probably_sent"}
               )

      assert reason["reason"] == "send_status_unmapped",
             "an unrecognized daemon status must block, not be guessed into the EJ-7 enum"
    end

    for outcome <- ~w(delivered queued absent ambiguous conflict) do
      test "reconcile surfaces the #{outcome} outcome" do
        outcome = unquote(outcome)

        assert {:ok, result} =
                 LocalPane.reconcile(command(),
                   pane_client: RecordingPaneClient,
                   test_pid: self(),
                   reconcile_reply: %{
                     "ok" => true,
                     "protocol_version" => 2,
                     "outcome" => outcome,
                     "msg_id" => @msg_id,
                     "pane_id" => "pane_writer"
                   }
                 )

        assert result["outcome"] == outcome
        assert result["assignment_id"] == "as_0001"
        assert result["send_message_id"] == @msg_id
        assert_receive {:reconcile_called, "pane_writer", @msg_id, @payload_hash}
      end
    end

    test "reconcile binds the payload hash the reducer journaled" do
      assert {:ok, _} =
               LocalPane.reconcile(command(),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 reconcile_reply: %{
                   "ok" => true,
                   "protocol_version" => 2,
                   "outcome" => "delivered",
                   "msg_id" => @msg_id,
                   "pane_id" => "pane_writer"
                 }
               )

      assert_receive {:reconcile_called, "pane_writer", @msg_id, payload_hash}

      assert payload_hash == @payload_hash,
             """
             MUST-2: the bound hash is the one already journaled on
             assignment_prompt_projected.prompt_hash, so a reconstructed dispatch is
             provably about the same bytes. Got: #{inspect(payload_hash)}
             """
    end

    test "reconcile refuses a command with no journaled payload hash" do
      assert {:error, reason} =
               LocalPane.reconcile(Map.delete(command(), "payload_hash"),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 reconcile_reply: %{"ok" => true, "protocol_version" => 2, "outcome" => "delivered"}
               )

      assert reason["reason"] == "reconcile_payload_hash_missing"
      refute_received {:reconcile_called, _, _, _}
    end

    test "reconcile rejects an outcome outside the five-valued result" do
      assert {:error, reason} =
               LocalPane.reconcile(command(),
                 pane_client: RecordingPaneClient,
                 test_pid: self(),
                 reconcile_reply: %{
                   "ok" => true,
                   "protocol_version" => 2,
                   "outcome" => "probably_fine",
                   "msg_id" => @msg_id,
                   "pane_id" => "pane_writer"
                 }
               )

      assert reason["reason"] == "reconcile_outcome_invalid"
    end
  end

  defp reply(fields) do
    fields
    |> Map.merge(%{"ok" => true, "protocol_version" => 2})
    |> Map.put_new("msg_id", @msg_id)
    |> Jason.encode!()
    |> Kernel.<>("\n")
  end

  defp command do
    %{
      "assignment_id" => "as_0001",
      "artifact_id" => "art_as_0001",
      "expected_artifact" => "lib/item_a.ex",
      "pane_ref" => "pane_writer",
      "prompt" => "Implement item A",
      "payload_hash" => @payload_hash,
      "repo_root" => File.cwd!(),
      "send_message_id" => @msg_id,
      "stable_for_ms" => 5000
    }
  end
end
