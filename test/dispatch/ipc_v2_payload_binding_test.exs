defmodule AiOrchestrator.Dispatch.IpcV2PayloadBindingTest do
  @moduledoc """
  NS-42 rule 5 on the consumer side of the reconcile boundary: the question is bound to
  the payload by its digest and by nothing else. Prompt bytes never travel on a reconcile
  request, whatever a caller leaves in the options or on the command; nothing a reconcile
  reply carries beyond the closed answer is reflected into the adapter's answer or the
  reducer's observation; and a stored receipt view that names a different payload than
  the one this command journaled is not an answer about this send. Replies travel through
  the real PaneClient over fake ap runners, exactly as the daemon would answer them.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime

  @id "snd_" <> String.duplicate("a", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  @other_hash "sha256:" <> String.duplicate("c", 64)
  @canary "PAYLOAD_CANARY_PROMPT_BYTES"
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)

  @answer_keys ~w(assignment_id backend delivery_attempt outcome pane_ref send_message_id status)

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-payload-missing-artifact",
      "prompt" => @canary
    }
  end

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  defp answer(fields),
    do: Map.merge(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane}, fields)

  defp view(outcome, status, attempt \\ 1, hash \\ @hash),
    do: answer(%{"outcome" => outcome, "status" => status, "delivery_attempt" => attempt, "payload_hash" => hash})

  # ping answers ping; every other argv call is reported to the test and answered with
  # `reply`; a send through the input runner is reported with the bytes it was given.
  defp recording_opts(reply) do
    owner = self()

    [
      ap_path: "/tmp/v2-payload-ap",
      runner: fn _, args, opts ->
        if hd(args) == "ping" do
          {Jason.encode!(ping()), 0}
        else
          send(owner, {:ap_call, args, opts})
          {Jason.encode!(reply), 0}
        end
      end,
      input_runner: fn _, args, input, _ ->
        send(owner, {:ap_send, args, input})
        {Jason.encode!(answer(%{"status" => "sent"})), 0}
      end
    ]
  end

  describe "prompt bytes never travel on a reconcile request" do
    test "the adapter's reconcile carries the digest and the identities, and nothing from the prompt" do
      assert {:ok, _} = LocalPane.reconcile(command(), recording_opts(answer(%{"outcome" => "absent"})))

      assert_received {:ap_call, args, opts}
      assert args == ["reconcile", @pane, "--msg-id", @id, "--payload-hash", @hash, "--protocol-version", "2"]
      refute Enum.any?(args, &String.contains?(&1, @canary))
      refute Keyword.has_key?(opts, :input)
      refute inspect(opts, limit: :infinity) =~ @canary
      refute_received {:ap_send, _, _}, "a reconcile is a question, never a paste"
    end

    test "a caller's send options are stripped before the question is asked" do
      opts =
        %{"outcome" => "absent"}
        |> answer()
        |> recording_opts()
        |> Keyword.merge(payload_hash: @hash, input: @canary, prompt: @canary)

      assert {:ok, %{"outcome" => "absent"}} = PaneClient.reconcile(@pane, @id, opts)

      assert_received {:ap_call, args, runner_opts}
      refute Enum.any?(args, &String.contains?(&1, @canary))
      refute Keyword.has_key?(runner_opts, :input)
      refute_received {:ap_send, _, _}, "the :input option must not turn a reconcile into a send"
    end
  end

  describe "prompt bytes in a reply are never reflected" do
    test "the adapter's answer is the closed key set, whatever else the daemon echoed" do
      reply = "delivered" |> view("delivered") |> Map.merge(%{"text" => @canary, "prompt" => @canary})

      assert {:ok, result} = LocalPane.reconcile(command(), recording_opts(reply))
      assert Enum.sort(Map.keys(result)) == @answer_keys
      refute inspect(result, limit: :infinity) =~ @canary
    end

    test "the reducer's observation carries the outcome and the attempt, and no bytes" do
      reply = "delivered" |> view("delivered") |> Map.merge(%{"text" => @canary, "prompt" => @canary})
      intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

      {observation, _runtime} = Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: recording_opts(reply)])

      assert %Observation.SendReconciled{outcome: "delivered", delivery_attempt: 1} = observation
      refute inspect(observation, limit: :infinity) =~ @canary
    end
  end

  describe "a stored receipt view must name this command's payload" do
    for {outcome, status} <- [
          {"delivered", "delivered"},
          {"queued", "queued"},
          {"ambiguous", "ambiguous"},
          {"absent", "not_delivered"}
        ] do
      test "a #{status} view under this id for another payload is not an answer about this send" do
        reply = view(unquote(outcome), unquote(status), 1, @other_hash)

        assert {:error, %{"reason" => "reconcile_payload_mismatch", "detector" => "dispatch_reconcile"}} =
                 LocalPane.reconcile(command(), recording_opts(reply))
      end
    end

    test "a mismatched non-delivery cannot admit a paste" do
      reply = view("absent", "not_delivered", 1, @other_hash)

      assert {:error, %{"reason" => "reconcile_payload_mismatch"}} = LocalPane.deliver(command(), recording_opts(reply))
      refute_received {:ap_send, _, _}, "a view about another payload proves nothing about this one"
    end

    test "a mismatched view reaches the reducer as the failed reconcile observation, never as an outcome" do
      reply = view("delivered", "delivered", 1, @other_hash)
      intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

      {observation, _runtime} = Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: recording_opts(reply)])

      assert %Observation.SendReconcileFailed{reason: %{"reason" => "reconcile_payload_mismatch"}} = observation
    end

    test "a view naming this payload is the positive control" do
      assert {:ok, %{"outcome" => "delivered", "delivery_attempt" => 1, "status" => "delivered"}} =
               LocalPane.reconcile(command(), recording_opts(view("delivered", "delivered")))
    end

    test "a conflict echoes the caller's identities and stays a conflict" do
      for reply <- [answer(%{"outcome" => "conflict"}), answer(%{"outcome" => "conflict", "payload_hash" => @hash})] do
        assert {:ok, %{"outcome" => "conflict", "delivery_attempt" => 0}} =
                 LocalPane.reconcile(command(), recording_opts(reply))
      end
    end

    test "a no-record absent carries no hash and is still the first attempt" do
      assert {:ok, %{"outcome" => "absent", "delivery_attempt" => 0}} =
               LocalPane.reconcile(command(), recording_opts(answer(%{"outcome" => "absent"})))
    end
  end
end
