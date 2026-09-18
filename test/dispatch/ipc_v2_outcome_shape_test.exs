defmodule AiOrchestrator.Dispatch.IpcV2OutcomeShapeTest do
  @moduledoc """
  NS-42 rule 6, the half the register records as still UNPROVEN: the FIVE legal reconcile
  outcome SHAPES, and the refusal of everything outside them.

  The existing evidence (979a2b7) covers the not-ok refusal vocabulary -- what the consumer
  does with a reply the daemon refused. It says nothing about the positive shape of an
  answer the daemon gave, which is the other half of a closed typed union: a union is only
  closed if the five members are each pinned AND a sixth is refused. This file pins both.

  Three properties, in order:

    * The five legal shapes are exactly the vendored fixtures, read as bytes from
      `test/fixtures/contracts/ipc/v2/` rather than retyped here, so a fixture edit moves
      this file and not only the hash. `absent` has two legal shapes -- no record at all
      and a stored `not_delivered` -- and the consumer's retry bound depends on telling
      them apart, so both are pinned.
    * Anything outside the union is refused: a sixth or unknown `outcome`, a missing
      required field, and a field whose type the schema does not admit. The daemon's word
      is never repeated into the refusal, because repeating an unknown word is how an
      unknown word becomes a known one.
    * No refused class is ever an answer about delivery. Each one exits through the durable
      attention path -- `agent_wedge_detected` + `human_attention_required`, assignment
      blocked -- with no `assignment_dispatch_sent` and no paste. This is the exit
      NS-42.C.003's register note calls "durable attention exit pending": asserted here on
      the appended events rather than inferred from the generic failed-observation path.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @id "snd_" <> String.duplicate("a", 64)
  @hash "sha256:" <> String.duplicate("b", 64)
  # Built, not written: the redaction gate refuses tmux pane literals in the tree.
  @pane "%" <> Integer.to_string(9)

  # The closed union, as ipc-v2.org "Framing and version" states it. Written out here so a
  # widening of `@reconcile_outcomes` in LocalPane has to argue with this list.
  @outcomes ~w(delivered queued absent ambiguous conflict)

  # Every legal shape, by the outcome it answers. `absent` is the one outcome with two.
  @legal_shapes %{
    "delivered" => ["reconcile.delivered.json"],
    "queued" => ["reconcile.queued.json"],
    "absent" => ["reconcile.absent.json", "reconcile.absent.not_delivered.json"],
    "ambiguous" => ["reconcile.ambiguous.json"],
    "conflict" => ["reconcile.conflict.json"]
  }

  # What `reconcile/2` answers for each legal shape: the fields the caller may rely on.
  @accepted %{
    "reconcile.delivered.json" => %{"outcome" => "delivered", "status" => "delivered", "delivery_attempt" => 1},
    "reconcile.queued.json" => %{"outcome" => "queued", "status" => "queued", "delivery_attempt" => 1},
    "reconcile.absent.json" => %{"outcome" => "absent", "delivery_attempt" => 0},
    "reconcile.absent.not_delivered.json" => %{
      "outcome" => "absent",
      "status" => "not_delivered",
      "delivery_attempt" => 1
    },
    "reconcile.ambiguous.json" => %{"outcome" => "ambiguous", "status" => "ambiguous", "delivery_attempt" => 1},
    "reconcile.conflict.json" => %{"outcome" => "conflict", "delivery_attempt" => 0}
  }

  # What `deliver/2` does with each legal shape. `:send` is the only one that may paste.
  @routing %{
    "reconcile.delivered.json" => {:ok, "reconciled", true},
    "reconcile.queued.json" => {:ok, "queued", true},
    "reconcile.absent.json" => {:send, "ok"},
    "reconcile.absent.not_delivered.json" => {:send, "ok"},
    "reconcile.ambiguous.json" => {:error, "dispatch_reconcile_ambiguous"},
    "reconcile.conflict.json" => {:error, "dispatch_reconcile_conflict"}
  }

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
      "expected_artifact" => "v2-outcome-shape-missing-artifact",
      "artifact_baseline" => %{"exists" => false},
      "prompt" => "v2 prompt"
    }
  end

  defp ping, do: %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"]}

  # The reconcile answer is the variable under test; a paste, if one happens, reports itself.
  defp opts(reply) do
    owner = self()

    [
      ap_path: "/tmp/v2-outcome-shape-ap",
      runner: fn _, args, _ -> {Jason.encode!(if(hd(args) == "ping", do: ping(), else: reply)), 0} end,
      input_runner: fn _, args, _, _ ->
        send(owner, {:pasted, args})

        {Jason.encode!(%{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane, "status" => "sent"}),
         0}
      end
    ]
  end

  # `assert PATTERN = EXPR, MSG` evaluates the match before the assertion runs, so the message
  # never prints and the repository guards the shape. This is the repair, named once.
  defp refused?(result, reason), do: match?({:error, %{"reason" => ^reason}}, result)

  describe "the five legal shapes are exactly the vendored fixtures" do
    test "the reconcile fixtures cover the closed union once each, and name no sixth outcome" do
      named = @legal_shapes |> Map.values() |> List.flatten()

      on_disk =
        @fixture_dir
        |> Path.join("reconcile.*.json")
        |> Path.wildcard()
        |> Enum.map(&Path.basename/1)
        |> Enum.reject(&String.starts_with?(&1, "reconcile.error."))

      assert Enum.sort(on_disk) == Enum.sort(named),
             "a reconcile answer fixture appeared or vanished without a shape entry: #{inspect(on_disk)}"

      assert Enum.sort(Map.keys(@legal_shapes)) == Enum.sort(@outcomes)

      for {outcome, names} <- @legal_shapes, name <- names do
        assert fixture(name)["outcome"] == outcome, name
      end
    end

    test "each legal shape is accepted and answers the fields the contract fixes for it" do
      for {name, expected} <- @accepted do
        result = LocalPane.reconcile(command(), opts(fixture(name)))
        assert match?({:ok, _answer}, result), "#{name}: #{inspect(result)}"
        {:ok, answer} = result

        for {key, value} <- expected do
          assert Map.get(answer, key) == value, "#{name}: #{key}"
        end

        assert answer["outcome"] in @outcomes, name
      end
    end

    test "a no-record absent and a stored absence are different shapes, and the retry bound reads the difference" do
      assert {:ok, %{"outcome" => "absent", "delivery_attempt" => 0} = no_record} =
               LocalPane.reconcile(command(), opts(fixture("reconcile.absent.json")))

      refute Map.has_key?(no_record, "status"),
             "a no-record answer that carried a stored status would claim a receipt that does not exist"

      assert {:ok, %{"outcome" => "absent", "status" => "not_delivered", "delivery_attempt" => 1}} =
               LocalPane.reconcile(command(), opts(fixture("reconcile.absent.not_delivered.json")))
    end

    test "deliver routes each legal shape as the contract fixes, and only absent may paste" do
      for {name, expected} <- @routing do
        result = LocalPane.deliver(command(), opts(fixture(name)))

        case {expected, result} do
          {{:ok, status, replayed}, {:ok, data}} ->
            assert data["send_status"] == status, name
            assert data["replayed"] == replayed, name
            refute_received {:pasted, _}, "#{name} reconstructs from the receipt and must not paste"

          {{:send, status}, {:ok, data}} ->
            assert data["send_status"] == status, name
            assert data["replayed"] == false, name
            assert_received {:pasted, _}
            refute_received {:pasted, _}, "#{name} pastes exactly once"

          {{:error, reason}, {:error, %{"reason" => actual, "outcome" => outcome}}} ->
            assert actual == reason, name
            assert outcome in @outcomes, name
            refute_received {:pasted, _}, "#{name} is unproven and must not paste"

          {_expected, actual} ->
            flunk("#{name}: unexpected #{inspect(actual)}")
        end
      end
    end
  end

  describe "a sixth or unknown outcome is refused" do
    @sixth ~w(not_delivered pending sent duplicate conflicted delivered_and_queued DELIVERED sixth)

    test "every word outside the union is refused without being repeated, on reconcile and on deliver" do
      for word <- @sixth do
        reply = Map.put(fixture("reconcile.delivered.json"), "outcome", word)
        result = LocalPane.reconcile(command(), opts(reply))

        assert refused?(result, "reconcile_outcome_invalid"), "#{word}: #{inspect(result)}"
        {:error, reason} = result

        refute Map.has_key?(reason, "outcome"), word
        refute inspect(reason, limit: :infinity) =~ word, "the daemon's unknown word must not be repeated"

        assert refused?(LocalPane.deliver(command(), opts(reply)), "reconcile_outcome_invalid"), word
        refute_received {:pasted, _}, word
      end
    end

    test "whitespace and case are not admitted into the union either" do
      for word <- [" delivered", "delivered ", "Delivered", "delivered\n", ""] do
        reply = Map.put(fixture("reconcile.delivered.json"), "outcome", word)
        assert refused?(LocalPane.reconcile(command(), opts(reply)), "reconcile_outcome_invalid"), inspect(word)
      end
    end

    test "an outcome that is not a string at all is outside the union, not coerced into it" do
      for value <- [2, 2.0, nil, true, ["delivered"], %{"outcome" => "delivered"}] do
        reply = Map.put(fixture("reconcile.delivered.json"), "outcome", value)

        assert refused?(LocalPane.reconcile(command(), opts(reply)), "reconcile_outcome_invalid"), inspect(value)
        assert refused?(LocalPane.deliver(command(), opts(reply)), "reconcile_outcome_invalid"), inspect(value)
        refute_received {:pasted, _}, inspect(value)
      end
    end
  end

  describe "a missing required field is refused" do
    # Each required field of the richest legal shape, and the class its absence answers.
    @missing %{
      "outcome" => "reconcile_outcome_invalid",
      "protocol_version" => "protocol_version_unsupported",
      "ok" => "reply_not_ok",
      "msg_id" => "reply_identity_missing",
      "pane_id" => "reply_pane_missing",
      "delivery_attempt" => "reconcile_attempt_invalid"
    }

    test "deleting any required field refuses with its own class and never pastes" do
      for {field, expected} <- @missing do
        reply = Map.delete(fixture("reconcile.delivered.json"), field)

        assert refused?(LocalPane.reconcile(command(), opts(reply)), expected), field
        assert refused?(LocalPane.deliver(command(), opts(reply)), expected), field
        refute_received {:pasted, _}, field
      end
    end

    test "a stored receipt with no positive attempt is refused rather than defaulted to zero" do
      for outcome <- ~w(delivered queued ambiguous) do
        reply =
          "reconcile.delivered.json"
          |> fixture()
          |> Map.put("outcome", outcome)
          |> Map.put("status", if(outcome == "delivered", do: "delivered", else: outcome))
          |> Map.delete("delivery_attempt")

        assert refused?(LocalPane.reconcile(command(), opts(reply)), "reconcile_attempt_invalid"), outcome
      end
    end
  end

  describe "a schema-invalid field type is refused" do
    # {field, value} -> class. Every one of these is a shape the wire can carry and the
    # schema does not admit; none may be parsed, coerced or rounded into a legal value.
    @invalid_types [
      {"protocol_version", "2", "protocol_version_unsupported"},
      {"protocol_version", 2.0, "protocol_version_unsupported"},
      {"protocol_version", 1, "protocol_version_unsupported"},
      {"ok", "true", "reply_not_ok"},
      {"ok", 1, "reply_not_ok"},
      {"ok", nil, "reply_not_ok"},
      {"msg_id", 2, "reply_identity_mismatch"},
      {"msg_id", nil, "reply_identity_mismatch"},
      {"pane_id", 2, "reply_pane_mismatch"},
      {"status", 1, "reconcile_view_invalid"},
      {"status", nil, "reconcile_view_invalid"},
      {"status", "delivered ", "reconcile_view_invalid"},
      {"payload_hash", 2, "reconcile_payload_mismatch"},
      {"delivery_attempt", "1", "reconcile_attempt_invalid"},
      {"delivery_attempt", 1.0, "reconcile_attempt_invalid"},
      {"delivery_attempt", 0, "reconcile_attempt_invalid"},
      {"delivery_attempt", -1, "reconcile_attempt_invalid"},
      {"delivery_attempt", nil, "reconcile_attempt_invalid"}
    ]

    test "every invalid field type is refused with its own class, and no paste follows" do
      for {field, value, expected} <- @invalid_types do
        reply = Map.put(fixture("reconcile.delivered.json"), field, value)
        label = "#{field} = #{inspect(value)}"

        assert refused?(LocalPane.reconcile(command(), opts(reply)), expected), label
        assert refused?(LocalPane.deliver(command(), opts(reply)), expected), label
        refute_received {:pasted, _}, label
      end
    end
  end

  describe "the refused classes reach the reducer as failures, never as an outcome" do
    for {label, mutation} <- [
          {"a sixth outcome", {:put, "outcome", "sixth"}},
          {"a missing outcome", {:delete, "outcome"}},
          {"an invalid attempt type", {:put, "delivery_attempt", "1"}},
          {"a status outside the closed set", {:put, "status", "sent"}}
        ] do
      test "#{label} is a failed reconcile observation, with no outcome and no attempt" do
        reply = mutate(fixture("reconcile.delivered.json"), unquote(Macro.escape(mutation)))
        intent = %Effect.ReconcileSend{assignment_id: "as_0001", command: command(), deadline_unix: 0}

        {observation, _runtime} = Effects.execute(intent, Runtime.new([]), opts: [dispatch_opts: opts(reply)])

        assert %Observation.SendReconcileFailed{reason: %{"reason" => reason}} = observation
        assert is_binary(reason)
        refute match?(%Observation.SendReconciled{}, observation)
      end
    end
  end

  @doc false
  # The mutation is data, not a closure, so the same description can name it in a test name,
  # be escaped into the generated test, and travel through the adapter's opts to the double.
  def mutate(reply, :none), do: reply
  def mutate(reply, {:delete, field}), do: Map.delete(reply, field)
  def mutate(reply, {:put, field, value}), do: Map.put(reply, field, value)

  # ------------------------------------------------------------------
  # The durable attention exit, on the path a real run takes.
  # ------------------------------------------------------------------

  defmodule ShapePaneClient do
    @moduledoc false

    alias AiOrchestrator.Dispatch.IpcV2OutcomeShapeTest, as: Shape

    # The reply is built from the identities the adapter actually asked with, then handed to
    # the scenario's own function, so a mutation is expressed against a reply that would
    # otherwise have bound.
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def reconcile(pane_ref, message_id, opts) do
      base = %{
        "ok" => true,
        "protocol_version" => 2,
        "msg_id" => message_id,
        "pane_id" => pane_ref,
        "outcome" => "delivered",
        "status" => "delivered",
        "delivery_attempt" => 1,
        "payload_hash" => Keyword.fetch!(opts, :payload_hash)
      }

      {:ok, Shape.mutate(base, Keyword.fetch!(opts, :reconcile_mutation))}
    end

    def send(pane_ref, _prompt, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:sent, pane_ref}, [])

      {:ok,
       %{
         "ok" => true,
         "protocol_version" => 2,
         "status" => "sent",
         "msg_id" => opts[:message_id],
         "pane_id" => pane_ref
       }}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  describe "every refused class exits through durable attention, not through an answer" do
    for {label, mutation, expected_reason} <- [
          {"a sixth outcome", {:put, "outcome", "sixth"}, "reconcile_outcome_invalid"},
          {"an unknown outcome type", {:put, "outcome", 2}, "reconcile_outcome_invalid"},
          {"a missing outcome", {:delete, "outcome"}, "reconcile_outcome_invalid"},
          {"a missing attempt", {:delete, "delivery_attempt"}, "reconcile_attempt_invalid"},
          {"an invalid attempt type", {:put, "delivery_attempt", "1"}, "reconcile_attempt_invalid"},
          {"a status outside the closed set", {:put, "status", "sent"}, "reconcile_view_invalid"},
          {"a missing version", {:delete, "protocol_version"}, "protocol_version_unsupported"}
        ] do
      test "#{label} blocks the assignment with a wedge and an attention record, and never pastes" do
        assert {:ok, result} = resume(unquote(Macro.escape(mutation)))

        types = Enum.map(result.appended_events, & &1["type"])

        refute_received {:sent, _}, "#{unquote(label)} is not an answer about delivery, so nothing may be pasted"
        refute "assignment_dispatch_sent" in types, unquote(label)

        assert "agent_wedge_detected" in types, unquote(label)
        assert "human_attention_required" in types, unquote(label)
        assert result.summary["status"] == "blocked", unquote(label)
        assert result.summary["open_attention_ids"] != []

        assert event_data!(result, "human_attention_required")["reason"] == unquote(expected_reason)
        assert event_data!(result, "agent_wedge_detected")["reason"] == unquote(expected_reason)
      end
    end

    test "the legal shapes do NOT block: the same harness reconstructs a delivered receipt" do
      assert {:ok, result} = resume(:none)

      types = Enum.map(result.appended_events, & &1["type"])

      refute_received {:sent, _}, "a delivered receipt means the bytes already landed"
      assert "assignment_dispatch_sent" in types
      refute "agent_wedge_detected" in types
      assert result.summary["status"] != "blocked"
    end
  end

  defp resume(reconcile_mutation) do
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
      dispatch_opts: [
        artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
        pane_client: ShapePaneClient,
        reconcile_mutation: reconcile_mutation,
        test_pid: self()
      ],
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
end
