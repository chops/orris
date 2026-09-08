defmodule AiOrchestrator.Dispatch.ContractReviewTest do
  @moduledoc """
  Codex's executable counterexamples from the S1 review of 71aa71b
  (`m_1788569937780000000_s1_nogo`), kept as regressions: each one reproduced a way the
  adapter contract accepted something it must refuse, or repeated something it must not.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient

  @id "snd_" <> String.duplicate("a", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  @canary "REVIEW_CANARY_PRIVATE_DETAIL"
  # A pane id is built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => @hash,
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "review-missing-artifact",
      "prompt" => "review prompt"
    }
  end

  defp reply, do: %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "outcome" => "absent"}

  defp opts(value, exit_code \\ 0) do
    [ap_path: "/tmp/review-ap", runner: fn _, _, _ -> {Jason.encode!(value), exit_code} end]
  end

  # M1
  test "protocol version reaches real reconcile argv" do
    owner = self()

    runner = fn _, argv, _ ->
      send(owner, {:argv, argv})
      {Jason.encode!(reply()), 0}
    end

    assert {:ok, _} =
             PaneClient.reconcile(@pane, @id,
               ap_path: "/tmp/review-ap",
               payload_hash: @hash,
               protocol_version: 2,
               runner: runner
             )

    assert_received {:argv, argv}
    assert ["--protocol-version", "2"] in Enum.chunk_every(argv, 2, 1, :discard)
  end

  test "protocol version reaches real ping and v2 send argv too" do
    owner = self()

    runner = fn _, argv, _ ->
      send(owner, {:argv, argv})
      {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "capabilities" => []}), 0}
    end

    assert {:ok, _} = PaneClient.capabilities(ap_path: "/tmp/review-ap", runner: runner)
    assert_received {:argv, ["ping", "--protocol-version", "2"]}

    input_runner = fn _, argv, _, _ ->
      send(owner, {:send_argv, argv})

      {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "status" => "sent"}),
       0}
    end

    assert {:ok, _} = PaneClient.send(@pane, "p", ap_path: "/tmp/review-ap", message_id: @id, input_runner: input_runner)
    assert_received {:send_argv, argv}
    assert ["--protocol-version", "2"] in Enum.chunk_every(argv, 2, 1, :discard)

    # send-argv parity: a pre-receipt send with no message id keeps the vector it always had
    input_runner = fn _, argv, _, _ ->
      send(owner, {:legacy_argv, argv})
      {~s({"ok":true,"status":"sent"}\n), 0}
    end

    assert {:ok, _} = PaneClient.send(@pane, "p", ap_path: "/tmp/review-ap", input_runner: input_runner)
    assert_received {:legacy_argv, ["send", @pane, "--stdin"]}
  end

  # M2
  test "negative daemon replies cannot establish absence" do
    assert {:error, _} = LocalPane.reconcile(command(), opts(Map.put(reply(), "ok", false)))
  end

  test "reconcile requires a pane echo" do
    assert {:error, _} = LocalPane.reconcile(command(), opts(Map.delete(reply(), "pane_id")))
  end

  test "reconcile requires a message echo and an ok" do
    assert {:error, %{"reason" => "reply_identity_missing"}} =
             LocalPane.reconcile(command(), opts(Map.delete(reply(), "msg_id")))

    assert {:error, %{"reason" => "reply_not_ok"}} = LocalPane.reconcile(command(), opts(Map.delete(reply(), "ok")))

    assert {:error, %{"reason" => "reply_not_ok"}} =
             PaneClient.reconcile(@pane, @id, Keyword.put(opts(Map.delete(reply(), "ok")), :payload_hash, @hash))
  end

  test "version two send refuses an unversioned reply" do
    options = [
      ap_path: "/tmp/review-ap",
      runner: fn _, _, _ -> {Jason.encode!(%{"ok" => true, "capabilities" => ["delivery_reconcile"]}), 0} end,
      input_runner: fn _, _, _, _ ->
        {Jason.encode!(%{"ok" => true, "msg_id" => @id, "pane_id" => @pane, "status" => "sent"}), 0}
      end
    ]

    assert {:error, _} = LocalPane.deliver(command(), options)
  end

  test "a version two send reply must echo both the message and the pane" do
    base = %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => @id, "pane_id" => @pane}

    for missing <- ["msg_id", "pane_id"] do
      input_runner = fn _, _, _, _ -> {Jason.encode!(Map.delete(base, missing)), 0} end
      result = PaneClient.send(@pane, "p", ap_path: "/tmp/review-ap", message_id: @id, input_runner: input_runner)
      assert match?({:error, _}, result), "a send reply without #{missing} was accepted"
    end
  end

  # M3
  test "decoded transport errors do not disclose arbitrary details" do
    result = LocalPane.reconcile(command(), opts(%{"ok" => false, "error" => "internal", "detail" => @canary}, 1))
    assert {:error, _} = result
    refute inspect(result, limit: :infinity) =~ @canary
  end

  test "invalid outcomes are not reflected into diagnostics" do
    result = LocalPane.reconcile(command(), opts(Map.put(reply(), "outcome", @canary)))
    assert {:error, _} = result
    refute inspect(result, limit: :infinity) =~ @canary
  end

  # M4
  test "payload hash grammar excludes a trailing newline before invoking ap" do
    options = reply() |> opts() |> Keyword.put(:payload_hash, @hash <> "\n")
    assert {:error, %{"reason" => "reconcile_payload_hash_invalid"}} = PaneClient.reconcile(@pane, @id, options)
  end

  # M5 / M6 (second review of b0bda31)
  test "version two send requires a status even when identity is valid" do
    options = [
      ap_path: "/tmp/review-ap",
      runner: fn _, _, _ ->
        {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}), 0}
      end,
      input_runner: fn _, _, _, _ -> {Jason.encode!(Map.delete(reply(), "outcome")), 0} end
    ]

    assert {:error, _} = LocalPane.deliver(command(), options)
  end

  for capability_reply <- [
        %{"ok" => false, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]},
        %{"ok" => true, "capabilities" => ["delivery_reconcile"]}
      ] do
    test "capability proof refuses #{inspect(capability_reply)} before send" do
      owner = self()
      capability_reply = unquote(Macro.escape(capability_reply))

      options = [
        ap_path: "/tmp/review-ap",
        runner: fn _, _, _ -> {Jason.encode!(capability_reply), 0} end,
        input_runner: fn _, _, _, _ ->
          send(owner, :send_attempted)
          {Jason.encode!(Map.put(reply(), "status", "sent")), 0}
        end
      ]

      result = LocalPane.deliver(command(), options)
      refute_received :send_attempted
      assert {:error, _} = result
    end
  end

  # The pre-receipt compatibility case lives on the low-level path only: a send with no
  # message id is decode-only, and a reply with no status is still an answer there.
  test "the no-message-id send is the only place a status-less reply is an answer" do
    input_runner = fn _, _, _, _ -> {~s({"ok":true}\n), 0} end
    assert {:ok, %{"ok" => true}} = PaneClient.send(@pane, "p", ap_path: "/tmp/review-ap", input_runner: input_runner)
  end

  # Duplicate wire shape (m_1788571895459_duplicate_wire; ruling m_1788572363058_queued_ruling)
  defp delivery_opts(value) do
    owner = self()

    [
      ap_path: "/tmp/review-ap",
      runner: fn _, args, _ ->
        response =
          if hd(args) == "ping",
            do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]},
            else: reply()

        {Jason.encode!(response), 0}
      end,
      input_runner: fn _, _, _, _ ->
        send(owner, :send_attempted)
        {Jason.encode!(value), 0}
      end
    ]
  end

  defp duplicate_view(status),
    do:
      reply()
      |> Map.delete("outcome")
      |> Map.merge(%{"duplicate" => true, "status" => status, "delivery_attempt" => 1, "payload_hash" => @hash})

  for {status, expected} <- [{"delivered", "reconciled"}, {"queued", "queued"}, {"pending", "queued"}] do
    test "ratified duplicate boolean with #{status} stored status reconstructs as #{expected}" do
      result = LocalPane.deliver(command(), delivery_opts(duplicate_view(unquote(status))))
      assert_received :send_attempted
      assert {:ok, %{"send_status" => unquote(expected), "replayed" => true}} = result
    end
  end

  test "an ambiguous duplicate is refused, never resent" do
    assert {:error, %{"reason" => "dispatch_reconcile_ambiguous"}} =
             LocalPane.deliver(command(), delivery_opts(duplicate_view("ambiguous")))
  end

  test "a not_delivered duplicate is not a receipt the adapter acts on" do
    assert {:error, %{"reason" => "duplicate_view_invalid"}} =
             LocalPane.deliver(command(), delivery_opts(duplicate_view("not_delivered")))
  end

  test "a duplicate whose outcome disagrees with its status is invalid" do
    view = "delivered" |> duplicate_view() |> Map.put("outcome", "absent")
    assert {:error, %{"reason" => "duplicate_view_invalid"}} = LocalPane.deliver(command(), delivery_opts(view))
  end

  test "a duplicate under this id for a different payload is not a duplicate of this send" do
    view = "delivered" |> duplicate_view() |> Map.put("payload_hash", "sha256:" <> String.duplicate("c", 64))
    assert {:error, %{"reason" => "duplicate_identity_mismatch"}} = LocalPane.deliver(command(), delivery_opts(view))
  end

  test "the stand-in status string duplicate is not a v2 shape" do
    view = reply() |> Map.delete("outcome") |> Map.put("status", "duplicate")
    assert {:error, %{"reason" => "send_status_unmapped"}} = LocalPane.deliver(command(), delivery_opts(view))
  end

  # Budget on every path + reply completeness (m_1788573025412742750_b5046cfe)
  test "initial delivery cannot reopen an exhausted receipt attempt budget" do
    owner = self()

    options = [
      ap_path: "/tmp/review-ap",
      runner: fn _, args, _ ->
        value =
          if hd(args) == "ping",
            do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]},
            else: Map.merge(reply(), %{"delivery_attempt" => 2, "status" => "not_delivered"})

        {Jason.encode!(value), 0}
      end,
      input_runner: fn _, _, _, _ ->
        send(owner, :third_admission)
        {Jason.encode!(Map.put(reply(), "status", "sent")), 0}
      end
    ]

    result = LocalPane.deliver(command(), options)
    refute_received :third_admission
    assert {:error, %{"reason" => "dispatch_attempts_exhausted"}} = result
  end

  for fields <- [
        %{"outcome" => "absent", "status" => "not_delivered"},
        %{"outcome" => "absent", "status" => "not_delivered", "delivery_attempt" => 0},
        %{"outcome" => "queued"}
      ] do
    test "stored receipt cannot lose its attempt #{inspect(fields)}" do
      value = Map.merge(reply(), unquote(Macro.escape(fields)))
      assert {:error, %{"reason" => "reconcile_attempt_invalid"}} = LocalPane.reconcile(command(), opts(value))
    end
  end

  test "a genuine no-record absent reads as attempt zero" do
    assert {:ok, %{"outcome" => "absent", "delivery_attempt" => 0}} = LocalPane.reconcile(command(), opts(reply()))
  end
end
