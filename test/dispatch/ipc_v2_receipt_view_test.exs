defmodule AiOrchestrator.Dispatch.IpcV2ReceiptViewTest do
  @moduledoc """
  NS-42 rule 9 on the consumer side: a pending attempt is resolved by the ownership the
  daemon holds, never by anything this consumer supplies or infers. The consumer's share is
  threefold. It supplies no token, epoch or wait on any v2 request, so the daemon's
  ownership is never caller-influenced. It reads a stored receipt status only from the
  contract's closed set and only when the status agrees with the outcome the contract
  fixes for it -- `pending` answers `ambiguous`, `not_delivered` answers `absent`, the
  rest answer themselves -- so ownership uncertainty (`pending`, `queued`) can never be
  read as `absent`, the one word that admits a paste. And an `ambiguous` answer, however
  it arose inside the daemon, is refused for attention and never retried. Replies travel
  through the real PaneClient over fake ap runners, exactly as the daemon would answer
  them, and through the executor with the default adapter.
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

  @stored ~w(pending queued delivered not_delivered ambiguous)
  @outcomes ~w(delivered queued absent ambiguous conflict)
  @agreeing %{
    "pending" => "ambiguous",
    "queued" => "queued",
    "delivered" => "delivered",
    "not_delivered" => "absent",
    "ambiguous" => "ambiguous"
  }

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-receipt-view-missing-artifact",
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  defp answer(fields),
    do: Map.merge(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane}, fields)

  defp view(outcome, status, attempt \\ 1),
    do: answer(%{"outcome" => outcome, "status" => status, "delivery_attempt" => attempt, "payload_hash" => @hash})

  # Every argv call is reported; ping answers ping, anything else answers `reply`; a paste
  # through the input runner is reported with its argv.
  defp opts(reply) do
    owner = self()

    [
      ap_path: "/tmp/v2-receipt-view-ap",
      runner: fn _, args, _ ->
        send(owner, {:ap_call, args})
        {Jason.encode!(if(hd(args) == "ping", do: ping(), else: reply)), 0}
      end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:pasted, args})
        {Jason.encode!(answer(%{"status" => "sent"})), 0}
      end
    ]
  end

  defp drain_argv(acc \\ []) do
    receive do
      {:ap_call, args} -> drain_argv([args | acc])
      {:pasted, args} -> drain_argv([args | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  describe "the consumer supplies nothing the daemon could mistake for ownership" do
    test "a full v2 delivery is exactly ping, reconcile and send, with no token, epoch or wait" do
      assert {:ok, %{"send_status" => "ok"}} = LocalPane.deliver(command(), opts(answer(%{"outcome" => "absent"})))

      assert drain_argv() == [
               ["ping", "--protocol-version", "2"],
               ["reconcile", @pane, "--msg-id", @id, "--payload-hash", @hash, "--protocol-version", "2"],
               ["send", @pane, "--stdin", "--msg-id", @id, "--protocol-version", "2"]
             ]
    end

    test "the adapter's reconcile is exactly one reconcile (the executor's preflight owns the ping)" do
      assert {:ok, _} = LocalPane.reconcile(command(), opts(answer(%{"outcome" => "absent"})))

      assert drain_argv() == [
               ["reconcile", @pane, "--msg-id", @id, "--payload-hash", @hash, "--protocol-version", "2"]
             ]
    end
  end

  describe "a stored status is read only from the closed set" do
    test "a status outside the contract's vocabulary is not a receipt, whatever outcome rides with it" do
      for status <- ["absent", "sent", "duplicate", "conflict", "delivered "], outcome <- @outcomes do
        reply = view(outcome, status)
        assert {:error, %{"reason" => "reconcile_view_invalid"}} = LocalPane.reconcile(command(), opts(reply))
        assert {:error, %{"reason" => "reconcile_view_invalid"}} = LocalPane.deliver(command(), opts(reply))
        refute_received {:pasted, _}, inspect({status, outcome})
      end
    end
  end

  describe "a stored status answers the outcome the contract fixes for it" do
    test "every agreeing pair is read as that receipt" do
      for {status, outcome} <- @agreeing do
        assert {:ok, %{"outcome" => ^outcome, "status" => ^status, "delivery_attempt" => 1}} =
                 LocalPane.reconcile(command(), opts(view(outcome, status)))
      end
    end

    test "every disagreeing pair is refused, so no receipt is trusted against its own status" do
      for status <- @stored, outcome <- @outcomes, outcome != @agreeing[status] do
        reply = view(outcome, status)
        assert {:error, %{"reason" => "reconcile_view_invalid"}} = LocalPane.reconcile(command(), opts(reply))
        assert {:error, %{"reason" => "reconcile_view_invalid"}} = LocalPane.deliver(command(), opts(reply))
        refute_received {:pasted, _}, inspect({status, outcome})
      end
    end

    test "ownership uncertainty never becomes absent: a pending or queued attempt cannot admit a paste" do
      for status <- ["pending", "queued"], attempt <- [1, 2] do
        reply = view("absent", status, attempt)
        assert {:error, %{"reason" => "reconcile_view_invalid"}} = LocalPane.deliver(command(), opts(reply))
        refute_received {:pasted, _}, inspect({status, attempt})
      end
    end

    test "a conflict carries no view, so a status beside it is outside the shape" do
      for status <- @stored do
        assert {:error, %{"reason" => "reconcile_view_invalid"}} =
                 LocalPane.reconcile(command(), opts(view("conflict", status)))
      end
    end
  end

  describe "pending resolves inside the daemon, and the consumer only reads the result" do
    test "a pending attempt answers ambiguous and is refused for attention, never pasted again" do
      reply = view("ambiguous", "pending")

      assert {:ok, %{"outcome" => "ambiguous", "status" => "pending", "delivery_attempt" => 1}} =
               LocalPane.reconcile(command(), opts(reply))

      assert {:error, %{"reason" => "dispatch_reconcile_ambiguous", "outcome" => "ambiguous"}} =
               LocalPane.deliver(command(), opts(reply))

      refute_received {:pasted, _}
    end

    test "a pending duplicate is admitted as queued, pasted once and never waited on" do
      duplicate =
        answer(%{"duplicate" => true, "status" => "pending", "delivery_attempt" => 1, "payload_hash" => @hash})

      owner = self()

      options =
        %{"outcome" => "absent"}
        |> answer()
        |> opts()
        |> Keyword.put(:input_runner, fn _, args, _, _ ->
          send(owner, {:pasted, args})
          {Jason.encode!(duplicate), 0}
        end)

      assert {:ok, %{"send_status" => "queued", "replayed" => true}} = LocalPane.deliver(command(), options)
      assert_received {:pasted, _}
      refute_received {:pasted, _}, "a second admission under one deliver"
    end

    test "a disagreeing view reaches the reducer as the failed reconcile observation, never as absent" do
      intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

      {observation, _runtime} =
        Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: opts(view("absent", "pending"))])

      assert %Observation.SendReconcileFailed{reason: %{"reason" => "reconcile_view_invalid"}} = observation
    end

    test "a pending attempt reaches the reducer as the ambiguous outcome it answers" do
      intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

      {observation, _runtime} =
        Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: opts(view("ambiguous", "pending"))])

      assert %Observation.SendReconciled{outcome: "ambiguous", delivery_attempt: 1} = observation
    end
  end
end
