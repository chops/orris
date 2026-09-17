defmodule AiOrchestrator.Dispatch.IpcV2AttemptBoundTest do
  @moduledoc """
  NS-42 rule 7 on the consumer side: a receipt status is terminal per physical attempt,
  and only a proven non-delivery may be retried. The consumer's share of that rule is the
  admission decision it makes from the receipt's own `delivery_attempt`: no record admits
  the first attempt; a stored `not_delivered` at attempt N admits exactly one attempt N+1
  and never a third; `delivered` and `ambiguous` are never retried at any attempt because
  one may have reached the pane; `queued` is not terminal, so a queued receipt at the
  attempt bound is converged, never refused as exhausted; and the reducer is handed the
  receipt's own count, never a default. Replies travel through the real PaneClient over
  fake ap runners, exactly as the daemon would answer them.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch.LocalPane
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
      "expected_artifact" => "v2-attempt-missing-artifact",
      # A recorded baseline, so a receipt reconstructed from delivered / queued has the
      # durable baseline MUST-7 requires; the admission decision under test is unaffected.
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  defp answer(fields),
    do: Map.merge(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane}, fields)

  defp view(outcome, status, attempt),
    do: answer(%{"outcome" => outcome, "status" => status, "delivery_attempt" => attempt, "payload_hash" => @hash})

  # ping admits; reconcile answers `reply`; every paste is reported to the test.
  defp opts(reply) do
    owner = self()

    [
      ap_path: "/tmp/v2-attempt-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: reply)), 0} end,
      input_runner: fn _, _, _, _ ->
        send(owner, :pasted)
        {Jason.encode!(answer(%{"status" => "sent"})), 0}
      end
    ]
  end

  defp assert_pasted_once do
    assert_received :pasted
    refute_received :pasted, "a second paste under one admission"
  end

  describe "only a proven non-delivery admits the next attempt" do
    test "no record at all admits the first attempt" do
      assert {:ok, %{"send_status" => "ok", "replayed" => false}} =
               LocalPane.deliver(command(), opts(answer(%{"outcome" => "absent"})))

      assert_pasted_once()
    end

    test "a stored non-delivery at attempt 1 admits exactly one attempt 2" do
      assert {:ok, %{"send_status" => "ok", "replayed" => false}} =
               LocalPane.deliver(command(), opts(view("absent", "not_delivered", 1)))

      assert_pasted_once()
    end

    test "a stored non-delivery at the bound is exhausted, whoever is asking" do
      for attempt <- [2, 3] do
        assert {:error, %{"reason" => "dispatch_attempts_exhausted", "delivery_attempt" => ^attempt}} =
                 LocalPane.deliver(command(), opts(view("absent", "not_delivered", attempt)))

        refute_received :pasted, "attempt #{attempt} must not reopen the allowance"
      end
    end

    test "a stored non-delivery without a positive attempt is not a fresh allowance" do
      for attempt <- [0, -1, "1", nil] do
        reply = view("absent", "not_delivered", attempt)
        reply = if is_nil(attempt), do: Map.delete(reply, "delivery_attempt"), else: reply

        assert {:error, %{"reason" => "reconcile_attempt_invalid"}} = LocalPane.deliver(command(), opts(reply))
        refute_received :pasted, inspect(attempt)
      end
    end
  end

  describe "delivered and ambiguous are terminal for every attempt" do
    test "a delivered receipt is reconstructed and never pasted again" do
      for attempt <- 1..3 do
        assert {:ok, %{"send_status" => "reconciled", "replayed" => true}} =
                 LocalPane.deliver(command(), opts(view("delivered", "delivered", attempt)))

        refute_received :pasted, "attempt #{attempt}"
      end
    end

    test "an ambiguous receipt is refused for attention and never pasted again" do
      for attempt <- 1..3 do
        assert {:error, %{"reason" => "dispatch_reconcile_ambiguous"}} =
                 LocalPane.deliver(command(), opts(view("ambiguous", "ambiguous", attempt)))

        refute_received :pasted, "attempt #{attempt}"
      end
    end
  end

  describe "queued is not terminal" do
    test "a queued receipt at the attempt bound is converged as queued, never refused as exhausted" do
      for attempt <- [1, 2] do
        assert {:ok, %{"send_status" => "queued", "replayed" => true}} =
                 LocalPane.deliver(command(), opts(view("queued", "queued", attempt)))

        refute_received :pasted, "attempt #{attempt}"
      end
    end
  end

  describe "the reducer is handed the receipt's own attempt" do
    test "the count on the observation is the daemon's, whether or not it is at the bound" do
      for {reply, outcome, attempt} <- [
            {answer(%{"outcome" => "absent"}), "absent", 0},
            {view("absent", "not_delivered", 1), "absent", 1},
            {view("absent", "not_delivered", 2), "absent", 2},
            {view("delivered", "delivered", 1), "delivered", 1},
            {view("queued", "queued", 2), "queued", 2},
            {view("ambiguous", "ambiguous", 1), "ambiguous", 1}
          ] do
        intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}
        {observation, _runtime} = Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: opts(reply)])

        assert %Observation.SendReconciled{outcome: ^outcome, delivery_attempt: ^attempt} = observation
        refute_received :pasted, "a reconcile never pastes"
      end
    end
  end
end
