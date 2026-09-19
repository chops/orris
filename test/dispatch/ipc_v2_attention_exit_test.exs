defmodule AiOrchestrator.Dispatch.IpcV2AttentionExitTest do
  @moduledoc """
  NS-42 rules 3 and 8, the exits the register still records as owed.

  Rule 3's own failure control is stated in two halves and only the first is evidenced:
  "Independently omit or mismatch each pane_id/msg_id echo on send and reconcile replies.
  Reporting such protocol conflict as success or absent instead of durable attention
  fails." The four refusals are pinned (979a2b7, and again in `ipc_v2_refusal_test.exs`);
  the register's own `required_evidence` says "echo refusal proven; durable attention exit
  pending". `ipc_v2_outcome_shape_test.exs` then proved the durable exit for seven shape
  classes -- but every one of its mutations is on a RECONCILE reply, and none of them is an
  echo. So the echo classes, and the send leg entirely, are the residue.

  Rule 8's residue is the same shape. The three generic-failure words are pinned as typed
  refusals that never become `absent`; what is not pinned is that they leave a durable
  record an operator can act on rather than a run that quietly stops, and that none of them
  licenses a second send.

  Both are asserted on the path a real run takes, through `RunFSM.resume` on appended
  events, rather than on the adapter's return value.

  ## What the send leg can and cannot claim

  A reconcile answered badly is refused before anything is pasted, so "never pastes" is a
  fair claim there. A send answered badly has already put the bytes on the wire -- that is
  what a send is -- so the claim on that leg is the honest one: the reply is never read as
  a success, the run blocks with the wedge and the attention record, and no SECOND send is
  issued. A test asserting no paste at all on the send leg would be asserting that the
  harness never got as far as the thing under test.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  @id "snd_" <> String.duplicate("a", 64)
  @other_id "snd_" <> String.duplicate("e", 64)
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)
  @other_pane "%" <> Integer.to_string(7)

  # The four echo failures rule 3 names, as independent mutations of one conforming reply.
  # `@other_pane` and `@other_id` are well-formed identities of something else, which is
  # the case the rule is about: a reply that binds to a different conversation.
  @echo_faults [
    {"an omitted message echo", {:delete, "msg_id"}, "reply_identity_missing"},
    {"a mismatched message echo", {:put, "msg_id", @other_id}, "reply_identity_mismatch"},
    {"an omitted pane echo", {:delete, "pane_id"}, "reply_pane_missing"},
    {"a mismatched pane echo", {:put, "pane_id", @other_pane}, "reply_pane_mismatch"}
  ]

  # Rule 8's three words. Each is a fact the daemon states about an attempt whose outcome
  # it cannot prove, so each must leave a durable record and none may be read as absent.
  @ambiguity_words ~w(paste_failed send_timeout queue_full)

  @doc false
  # Data rather than a closure, so one description can name the mutation in a test name, be
  # escaped into the generated test, and travel through the adapter's opts to the double.
  def mutate(reply, :none), do: reply
  def mutate(reply, {:delete, field}), do: Map.delete(reply, field)
  def mutate(reply, {:put, field, value}), do: Map.put(reply, field, value)

  # A refusal is built FROM the conforming reply rather than beside it, so it carries the
  # identities the adapter actually asked with. A refusal written out with literal ids is a
  # refusal that fails its echo check first, and the test then measures rule 3 while
  # claiming to measure rule 8 -- which is exactly what the first run of this file did.
  def mutate(reply, {:refuse, word}) do
    reply
    |> Map.take(["msg_id", "pane_id"])
    |> Map.merge(%{"ok" => false, "protocol_version" => 2, "error" => word})
  end

  defmodule EchoPaneClient do
    @moduledoc false

    alias AiOrchestrator.Dispatch.IpcV2AttentionExitTest, as: Exit

    # Each reply is built from the identities the adapter actually asked with and then
    # handed to the scenario's own mutation, so a fault is expressed against a reply that
    # would otherwise have bound. The two legs carry separate mutations, so one test moves
    # one leg and leaves the other conforming.
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def reconcile(pane_ref, message_id, opts) do
      base = %{
        "ok" => true,
        "protocol_version" => 2,
        "msg_id" => message_id,
        "pane_id" => pane_ref,
        "outcome" => "absent"
      }

      {:ok, Exit.mutate(base, Keyword.get(opts, :reconcile_mutation, :none))}
    end

    def send(pane_ref, _prompt, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:sent, pane_ref}, [])

      base = %{
        "ok" => true,
        "protocol_version" => 2,
        "status" => "sent",
        "msg_id" => opts[:message_id],
        "pane_id" => pane_ref
      }

      {:ok, Exit.mutate(base, Keyword.get(opts, :send_mutation, :none))}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  describe "rule 3: an echo failure on a reconcile exits through durable attention" do
    for {label, mutation, expected} <- @echo_faults do
      test "#{label} on a reconcile blocks the assignment and pastes nothing" do
        assert {:ok, result} = resume(reconcile_mutation: unquote(Macro.escape(mutation)))

        refute_received {:sent, _}, "a reconcile is a question; a badly echoed answer to it may not license a paste"
        assert_blocked(result, unquote(expected))
      end
    end
  end

  describe "rule 3: an echo failure on a send exits through durable attention" do
    for {label, mutation, expected} <- @echo_faults do
      test "#{label} on a send blocks the assignment and issues no second send" do
        assert {:ok, result} = resume(send_mutation: unquote(Macro.escape(mutation)))

        assert_received {:sent, _}, "the send under test has to have been issued for its reply to be the subject"
        refute_received {:sent, _}, "a reply this consumer could not bind is not a reason to send the prompt again"
        assert_blocked(result, unquote(expected))
      end
    end
  end

  describe "rule 8: the three generic-failure words exit through durable attention" do
    for word <- @ambiguity_words do
      test "a send refused with #{word} blocks the assignment, and is never absent" do
        assert {:ok, result} = resume(send_mutation: {:refuse, unquote(word)})

        assert_received {:sent, _}
        refute_received {:sent, _}, "#{unquote(word)} does not prove the bytes did not land, so it is not a retry"
        assert_blocked(result, "dispatch_refused_" <> unquote(word))
      end
    end

    test "none of the three is reported as a delivery, an absence or a non-delivery" do
      for word <- @ambiguity_words do
        assert {:ok, result} = resume(send_mutation: {:refuse, word})
        drain()

        recorded = Enum.map_join(result.appended_events, "\n", &Jason.encode!/1)

        refute recorded =~ "\"absent\"", word
        refute recorded =~ "not_delivered", word
        refute recorded =~ "\"send_status\"", "#{word}: a refused send has no send status to journal"
      end
    end
  end

  describe "the harness proves a conforming daemon through the same path" do
    test "with neither leg mutated the assignment is dispatched and nothing is blocked" do
      assert {:ok, result} = resume([])

      types = Enum.map(result.appended_events, & &1["type"])

      assert_received {:sent, _}
      assert "assignment_dispatch_sent" in types
      refute "agent_wedge_detected" in types
      assert result.summary["status"] != "blocked"
    end

    test "each echo fault is a distinct class, so the table is four rows and not one" do
      classes = for {_label, _mutation, expected} <- @echo_faults, do: expected

      assert Enum.uniq(classes) == classes
      assert length(classes) == 4
    end

    test "the adapter refuses each echo fault on both legs before the run is consulted" do
      # The same four faults at the adapter boundary, so a change that moved the durable
      # exit without moving the refusal (or the reverse) cannot pass both files quietly.
      for {_label, mutation, expected} <- @echo_faults do
        assert {:error, %{"reason" => ^expected}} =
                 LocalPane.reconcile(command(), pane_client: EchoPaneClient, reconcile_mutation: mutation)

        assert {:error, %{"reason" => ^expected}} =
                 LocalPane.deliver(command(),
                   pane_client: EchoPaneClient,
                   send_mutation: mutation,
                   test_pid: self()
                 )

        drain()
      end
    end
  end

  defp command do
    %{
      "assignment_id" => "as_0001",
      "pane_ref" => @pane,
      "send_message_id" => @id,
      "payload_hash" => "sha256:" <> String.duplicate("b", 64),
      "repo_root" => System.tmp_dir!(),
      "expected_artifact" => "v2-attention-missing-artifact",
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  # Blocked, with the wedge and the attention record both naming the same class, and with
  # no dispatch claimed. The pair matters: a wedge with no attention record is a run that
  # stopped without telling anybody, and an attention record with no wedge is a note about
  # an agent nobody stopped trusting.
  defp assert_blocked(result, expected_reason) do
    types = Enum.map(result.appended_events, & &1["type"])

    refute "assignment_dispatch_sent" in types, expected_reason
    assert "agent_wedge_detected" in types, expected_reason
    assert "human_attention_required" in types, expected_reason
    assert result.summary["status"] == "blocked", expected_reason
    assert result.summary["open_attention_ids"] != []

    assert event_data!(result, "human_attention_required")["reason"] == expected_reason
    assert event_data!(result, "agent_wedge_detected")["reason"] == expected_reason
  end

  defp resume(dispatch_extra) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    lines =
      [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", "events_post_prompt_v2.jsonl"]
      |> Path.join()
      |> File.read!()
      |> String.split("\n", trim: true)

    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    artifact_by_assignment =
      fixture_events
      |> Enum.filter(&(&1["type"] == "artifact_observed"))
      |> Map.new(fn event -> {Map.fetch!(event["data"], "assignment_id"), event["data"]} end)

    gate_pass =
      fixture_events
      |> Enum.find(&(&1["type"] == "gate_passed"))
      |> Map.fetch!("data")
      |> Map.delete("gate_run_id")

    RunFSM.resume(spec, plan, lines,
      dispatch: LocalPane,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts:
        [
          artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
          pane_client: EchoPaneClient,
          test_pid: self()
        ] ++ dispatch_extra,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    )
  end

  defp event_data!(%{appended_events: events}, type) do
    assert event = Enum.find(events, &(&1["type"] == type)),
           "no #{type} event was appended; the run produced #{inspect(Enum.map(events, & &1["type"]))}"

    Map.fetch!(event, "data")
  end

  defp drain do
    receive do
      _message -> drain()
    after
      0 -> :ok
    end
  end
end
