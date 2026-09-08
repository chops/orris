defmodule AiOrchestrator.Lifecycle.RunFSMDispatchReconcileTest do
  @moduledoc """
  RED for GAP-1, revised against Codex review `m_1788508818157722000_82f1b70d`.

  `events_post_prompt.jsonl` is the journal of a run killed in the window that actually
  loses information. `Host.drive/3` commits the emitted suffix before it executes the
  effect, so `assignment_prompt_projected` -- and with it the prompt hash -- is durable
  before `LocalPane.deliver/2` pastes anything. The crash therefore lands between a paste
  that may have succeeded and an `assignment_dispatch_sent` that was never written. Replay
  cannot distinguish "the agent already has this prompt" from "the agent never saw it",
  and the two repairs are opposites: one duplicates the work, the other drops it.

  (`events_pre_dispatch.jsonl` stops before the projection, so no send could have
  occurred there; it is kept as the weaker resume case, not as evidence about this one.)

  The daemon is the only party that observed the paste, so resume asks it about the
  journaled send id and acts on the five-valued answer:

    absent    -> dispatch (the only case that may send)
    delivered -> reconstruct the dispatch event from the receipt, never send
    queued    -> reconstruct, then keep converging (see the queued test file)
    ambiguous -> durable human attention, never send
    conflict  -> durable human attention, never send

  Under R2 the query runs before *every* attempted dispatch, fresh runs included, so the
  reducer has one path rather than a resume special case -- and so a retry or a re-entry
  after a partial failure is covered by the same rule.

  Second revision, against `m_1788512437923664000_ba320aac` and
  `m_1788513216000000000_a93fb78e`:

    * D2: `protocol_version` is explicit on every request the orchestrator makes. A
      client that relies on a daemon's default cannot tell a v2 answer from a v1 one.
    * D3: capability discovery is `Dispatch.preflight/2`, a named step with a named
      refusal, rather than a `capabilities/1` call the reducer happens to make first.
    * D5: a duplicate is a receipt view, and the reducer must reach the same conclusion
      from it that it would reach from reconciling. Two paths to one fact is one path too
      many.
    * M3: a wedge names its `detector`. `phase` said where in the code the failure was
      noticed, which is not a fact about the run.
    * M4: the queued-crash fixture is a journal killed after
      `assignment_dispatch_sent(send_status=queued)`, which is the state a queued send
      actually leaves behind.
    * M5: a reply is bound to `msg_id` *and* `pane_id`, because a receipt is keyed by
      both.
    * M7: an unknown but well-formed capability token is not an error. A client that
      refuses what it does not recognise makes the daemon unable to grow.
    * MUST-7: `assignment_prompt_projected` carries `artifact_baseline` at v2, and a v1
      event is upcast on read to the explicit `{"status" => "unrecorded"}` rather than to
      a silent absence.

  Prompt retention -- the ruling's correctness prerequisite for all of this -- is in
  `run_fsm_prompt_retention_test.exs`; this file assumes the bytes are already durable
  and is about the daemon conversation on top of them.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SendId
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  # `assert_receive/3` takes the timeout in the second slot, so a failure message has to
  # be passed third. Reading the configured default keeps these in step with the suite
  # rather than freezing a number here.
  @receive_timeout ExUnit.configuration()[:assert_receive_timeout]

  @run_id "run_scenario_0001"
  # The hash events_post_prompt.jsonl journals for as_0001. It is the renderer's real digest
  # rather than a placeholder: the ratified v1 rule refuses a legacy journal whose two facts
  # do not reproduce, so a placeholder journal could never reach a reconcile at all. The
  # "bound to the journal, not to a re-render" claim below is carried by the retention
  # evidence (a retained send is fetched, never rendered) rather than by a hash that differs.
  @journaled_prompt_hash "sha256:251abbecdffd811b9f808b7e55923fe4313fd812857cf1a6f5af75e47269fb02"

  defmodule ReconcilingPaneClient do
    @moduledoc false

    def capabilities(opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:capabilities_called, opts[:protocol_version]}, [])
      {:ok, Keyword.get(opts, :capabilities, ["delivery_reconcile"])}
    end

    def send(pane_ref, prompt, opts) do
      Process.send(
        Keyword.fetch!(opts, :test_pid),
        {:send_called, pane_ref, prompt, opts[:message_id], opts[:protocol_version]},
        []
      )

      case Keyword.get(opts, :send_result, :ok) do
        :ok ->
          {:ok,
           %{
             "ok" => true,
             "protocol_version" => 2,
             "status" => "sent",
             "msg_id" => opts[:message_id],
             "pane_id" => Keyword.get(opts, :reply_pane_id, pane_ref)
           }}

        # NS-42 rule 11: duplicate: true beside the receipt view. The payload hash is the
        # one the pre-send reconcile was asked with (reconcile precedes every send).
        {:duplicate, view} ->
          {:ok,
           Map.merge(
             %{
               "ok" => true,
               "protocol_version" => 2,
               "duplicate" => true,
               "msg_id" => opts[:message_id],
               "pane_id" => pane_ref,
               "payload_hash" => Process.get({__MODULE__, :payload_hash})
             },
             view
           )}

        {:error, reason} ->
          {:error, reason}
      end
    end

    def reconcile(pane_ref, message_id, opts) do
      Process.put({__MODULE__, :payload_hash}, opts[:payload_hash])

      Process.send(
        Keyword.fetch!(opts, :test_pid),
        {:reconcile_called, pane_ref, message_id, opts[:payload_hash], opts[:protocol_version]},
        []
      )

      outcome = Keyword.get(opts, :reconcile_outcome, "absent")

      {:ok,
       %{
         "ok" => true,
         "protocol_version" => Keyword.get(opts, :reply_protocol_version, 2),
         "outcome" => outcome,
         "msg_id" => Keyword.get(opts, :reply_msg_id, message_id),
         "pane_id" => Keyword.get(opts, :reply_pane_id, pane_ref),
         "delivery_attempt" => Keyword.get(opts, :reply_delivery_attempt, 1)
       }}
    end

    def status(pane_ref, opts) do
      {:ok, Keyword.get(opts, :pane_status, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0})}
    end
  end

  describe "R2: the daemon is asked before every attempted dispatch" do
    test "a fresh run reconciles before it pastes" do
      assert {:ok, _result} = fresh_run(reconcile_outcome: "absent")

      # All messages come from the adapter running in this process, so mailbox order is
      # call order. D3 (the later revision) puts capability preflight first; R2's claim is
      # that the reconcile precedes the SEND, so that is what is asserted.
      calls = drain_calls()
      reconcile_at = Enum.find_index(calls, &match?({:reconcile_called, "pane_writer", _, _, _}, &1))
      send_at = Enum.find_index(calls, &match?({:send_called, "pane_writer", _, _, _}, &1))

      assert is_integer(reconcile_at) and is_integer(send_at) and reconcile_at < send_at,
             """
             R2: reconcile runs before every attempted dispatch, not only on resume. A
             resume-only query leaves retries and re-entry after a partial failure
             unprotected, and it gives the reducer two paths where one will do.
             Adapter calls, in order: #{inspect(Enum.map(calls, &label/1))}
             """

      for call <- calls, do: send(self(), call)

      assert_receive {:send_called, "pane_writer", _prompt, _msg_id, _version},
                     @receive_timeout,
                     "a fresh run must still dispatch once the daemon answers absent"
    end

    test "a resume reconciles before it pastes" do
      assert {:ok, _result} = resume_post_prompt(reconcile_outcome: "absent")

      assert_receive {:reconcile_called, "pane_writer", _msg_id, _hash, _version}
      assert "pane_writer" in sent_panes()
    end
  end

  describe "MUST-1: the send id is globally scoped" do
    test "the daemon is asked about a run-scoped send id" do
      assert {:ok, _} = resume_post_prompt(reconcile_outcome: "absent")

      assert_receive {:reconcile_called, "pane_writer", msg_id, _hash, _version}

      assert msg_id == SendId.mint(@run_id, "as_0001"),
             """
             MUST-1: the receipt store is global but "send_as_0001" repeats in every run.
             Two runs asking the same question get one another's answer: one silently
             skips a prompt it never sent, or blocks on a conflict it never caused.
             Got: #{inspect(msg_id)}
             """
    end
  end

  describe "MUST-2: the query is bound to the payload" do
    test "reconcile carries the hash the journal already recorded" do
      assert {:ok, _} = resume_post_prompt(reconcile_outcome: "absent")

      assert_receive {:reconcile_called, "pane_writer", _msg_id, payload_hash, _version}

      assert payload_hash == @journaled_prompt_hash,
             """
             MUST-2: the bound hash must come from the durable
             assignment_prompt_projected.prompt_hash, not from re-rendering the prompt at
             resume. The fixture's hash is deliberately unrelated to the rendered bytes,
             so a recomputation shows up here instead of silently answering `conflict`
             forever in production.
             """
    end

    test "a fresh dispatch command binds the hash it is about to journal" do
      assert {:ok, result} = fresh_run(reconcile_outcome: "absent")

      assert_receive {:reconcile_called, "pane_writer", _msg_id, payload_hash, _version}

      projected =
        result.events
        |> Enum.find(&(&1["type"] == "assignment_prompt_projected"))
        |> Map.fetch!("data")

      assert payload_hash == projected["prompt_hash"],
             """
             The hash the daemon checks and the hash the journal keeps must be one value
             over one set of bytes. Nothing tests that invariant today, so a change to
             prompt rendering or to the projection would decouple them and turn every
             reconcile into a permanent conflict.
             """
    end
  end

  describe "MUST-6: the journal keeps the ratified EJ-7 vocabulary" do
    # Queued ruling (m_1788572363058_queued_ruling): a delivered receipt reconstructs as
    # `reconciled` (rebuilt from the receipt, and the daemon's proof the bytes landed); a
    # queued receipt is exactly as unpasted as a fresh queued send, so it reconstructs as
    # `queued` and the lifecycle converges it before observation. Both carry replayed: true.
    for {outcome, expected_status} <- [{"delivered", "reconciled"}, {"queued", "queued"}] do
      test "a #{outcome} receipt reconstructs the dispatch event without resending" do
        outcome = unquote(outcome)
        expected_status = unquote(expected_status)
        assert {:ok, result} = resume_post_prompt(reconcile_outcome: outcome)

        assert_receive {:reconcile_called, "pane_writer", _msg_id, _hash, _version}

        refute "pane_writer" in sent_panes(),
               "a #{outcome} receipt means the bytes already reached the pane; resending duplicates the prompt"

        data = dispatch_data(result)
        assert data, "resume must still journal assignment_dispatch_sent from the receipt"

        assert data["send_status"] == expected_status,
               """
               MUST-6: EJ-7 ratified send_status as ok | queued | reconciled, where
               `reconciled` names "this event was rebuilt from a receipt rather than from
               a send this process performed" and `queued` names an accepted, unpasted
               send whatever process accepted it. Echoing any other daemon word here
               would widen ratified journal data without an event_version bump.
               Got: #{inspect(data["send_status"])}
               """

        assert data["replayed"] == true,
               "the existing `replayed` field already carries this; a new `reconciled` boolean would be unversioned"

        refute Map.has_key?(data, "reconciled"),
               "MUST-6 forbids an unversioned `reconciled` field alongside the ratified enum"
      end
    end

    test "a fresh send journals the normalized value ok" do
      assert {:ok, result} = fresh_run(reconcile_outcome: "absent")

      assert dispatch_data(result)["send_status"] == "ok",
             "the daemon's \"sent\" normalizes to the ratified `ok`; parity fixtures stay unchanged"
    end
  end

  describe "R3: an unproven send blocks rather than guessing" do
    for outcome <- ~w(ambiguous conflict) do
      test "#{outcome} becomes durable human attention and never resends" do
        outcome = unquote(outcome)
        assert {:ok, result} = resume_post_prompt(reconcile_outcome: outcome)

        types = Enum.map(result.appended_events, & &1["type"])

        refute "pane_writer" in sent_panes(),
               "R3 forbids resending on #{outcome}: the daemon cannot prove the bytes did not land"

        refute "assignment_dispatch_sent" in types,
               "an unproven send must not be journaled as sent"

        assert "agent_wedge_detected" in types
        assert "human_attention_required" in types

        refute "assignment_failed" in types,
               """
               R3: `#{outcome}` is a nonterminal blocked state. `assignment_failed` stays
               reserved for the Wave 4 terminal/stop-policy producer; appending it here
               would close an assignment a human can still repair.
               """

        assert result.summary["status"] == "blocked"
        assert result.summary["open_attention_ids"] != []
      end
    end

    test "the concrete dispatch error class survives into the attention record" do
      assert {:ok, result} = resume_post_prompt(reconcile_outcome: "conflict")

      wedge = event_data!(result, "agent_wedge_detected")

      assert wedge["reason"] == "dispatch_reconcile_conflict",
             """
             R3: the wedge/attention pair is the right vocabulary, but it must not erase
             which dispatch error occurred. A human reading "wedged" with no class cannot
             tell a payload conflict from a transport timeout. Got: #{inspect(wedge["reason"])}
             """

      assert wedge["detector"] == "dispatch_reconcile",
             """
             M3: `detector` names the check that fired, which is a fact about the run and
             stays true however the code is arranged. `phase` named a region of the
             implementation, so a refactor could change a journalled record's meaning
             without changing anything that happened.
             """

      refute Map.has_key?(wedge, "phase"),
             "M3 removes the field rather than leaving both, which would let readers diverge"
    end

    test "the attention record repeats the class in its own vocabulary" do
      assert {:ok, result} = resume_post_prompt(reconcile_outcome: "conflict")

      attention = event_data!(result, "human_attention_required")

      assert attention["reason"] == "dispatch_reconcile_conflict",
             """
             M3: the wedge and the attention are read by different people at different
             times. An attention record that says only "see the wedge" makes the operator
             reconstruct the join by hand at the worst possible moment.
             """
    end

    test "an ordinary dispatch failure reaches a durable terminal instead of an unjournaled error" do
      assert {:ok, result} =
               resume_post_prompt(
                 reconcile_outcome: "absent",
                 send_result: {:error, %{"reason" => "pane_dead", "pane_ref" => "pane_writer"}}
               )

      types = Enum.map(result.appended_events, & &1["type"])

      assert "human_attention_required" in types,
             "a failed dispatch must leave durable attention, not return {:error, _} with an empty journal"

      refute "assignment_dispatch_sent" in types,
             "a send that failed was not proven to arrive and must not be journaled as sent"

      assert result.summary["status"] == "blocked"
    end
  end

  describe "R4/D3: a daemon that cannot answer is refused at a named step" do
    test "a daemon without delivery_reconcile blocks before any paste" do
      assert {:ok, result} = resume_post_prompt(capabilities: ["pane_status"])

      types = Enum.map(result.appended_events, & &1["type"])

      refute "pane_writer" in sent_panes(),
             """
             R4: an old daemon has no receipt, so it cannot say the prompt is absent. Sending
             anyway is the duplicate-prompt bug this whole slice exists to prevent.
             """

      assert "human_attention_required" in types
      assert result.summary["status"] == "blocked"
    end

    test "the refusal names the capability that was missing" do
      assert {:ok, result} = resume_post_prompt(capabilities: ["pane_status"])

      attention = event_data!(result, "human_attention_required")

      assert attention["reason"] == "dispatch_preflight_unsupported"

      assert "delivery_reconcile" in attention["detail"]["missing_capabilities"],
             """
             D3: the operator's next action is to upgrade a daemon. A refusal that does
             not say which capability is absent makes them read this code to find out.
             """
    end

    test "preflight runs before the daemon is asked anything else" do
      assert {:ok, _result} = resume_post_prompt(reconcile_outcome: "absent")

      assert_receive first_call

      assert match?({:capabilities_called, _}, first_call),
             """
             D3: preflight is a step, not an inference. Discovering mid-conversation that
             the daemon cannot answer leaves the reducer holding a half-finished dispatch
             it has no rule for. First adapter call was: #{inspect(label(first_call))}
             """
    end

    test "an unknown capability token is not a reason to refuse" do
      assert {:ok, result} =
               resume_post_prompt(
                 reconcile_outcome: "absent",
                 capabilities: ["delivery_reconcile", "delivery_stream_v9", "something_from_the_future"]
               )

      assert "pane_writer" in sent_panes(),
             """
             M7: capabilities are an open set. A client that treats an unrecognised token
             as a fault makes every future daemon feature a breaking change, so the only
             safe question is whether what this client needs is present.
             """

      assert result.summary["status"] != "blocked"
    end
  end

  describe "D2: the protocol version is stated, never assumed" do
    test "a fresh run names its version on every call it makes" do
      assert {:ok, _result} = fresh_run(reconcile_outcome: "absent")

      assert_receive {:capabilities_called, 2}

      assert_receive {:send_called, "pane_writer", _prompt, _msg_id, 2},
                     @receive_timeout,
                     """
                     D2: an ambient default is a version the two sides agree on by
                     coincidence. Stating it makes a mismatch a refusal instead of a
                     misreading.
                     """
    end

    test "a resume names its version on the reconcile as well" do
      assert {:ok, _result} = resume_post_prompt(reconcile_outcome: "absent")

      assert_receive {:capabilities_called, 2}
      assert_receive {:reconcile_called, "pane_writer", _msg_id, _hash, 2}
      assert_receive {:send_called, "pane_writer", _prompt, _msg_id, 2}
    end

    test "a reply that does not name version 2 is not read as a version 2 answer" do
      assert {:ok, result} =
               resume_post_prompt(reconcile_outcome: "absent", reply_protocol_version: 1)

      refute "pane_writer" in sent_panes(),
             """
             D2: `outcome` means something specific at v2. Reading a v1-shaped reply as
             though it carried that meaning is exactly the confusion an explicit version
             exists to prevent.
             """

      assert result.summary["status"] == "blocked"
    end
  end

  describe "M5: a reply is bound to the pane as well as the message" do
    test "a reply about another pane is refused" do
      assert {:ok, result} =
               resume_post_prompt(reconcile_outcome: "delivered", reply_pane_id: "pane_reviewer")

      refute "pane_writer" in sent_panes()

      assert result.summary["status"] == "blocked",
             """
             M5: a receipt is keyed by pane and message together. A client that checked
             only the message id would accept "delivered" about a different pane and
             journal a send that never happened here.
             """
    end

    test "a reply about another message is refused" do
      assert {:ok, result} =
               resume_post_prompt(reconcile_outcome: "delivered", reply_msg_id: "snd_" <> String.duplicate("0", 64))

      refute "pane_writer" in sent_panes()
      assert result.summary["status"] == "blocked"
    end
  end

  describe "D5: a duplicate is answered the way a reconciliation is" do
    test "a duplicate reporting delivery journals a send without pasting again" do
      assert {:ok, result} =
               fresh_run(
                 reconcile_outcome: "absent",
                 send_result: {:duplicate, %{"status" => "delivered", "delivery_attempt" => 1}}
               )

      assert dispatch_data(result)["send_status"] == "reconciled",
             """
             D5: the daemon already had this send. That is the same fact reconciliation
             reports, so it must produce the same journal entry; a second vocabulary for
             one fact is a second set of bugs.
             """

      assert Enum.count(sent_panes(), &(&1 == "pane_writer")) == 1,
             "the one paste is the attempt that was refused as a duplicate, not a retry of it"
    end

    test "a duplicate that cannot prove delivery blocks like an ambiguous reconcile" do
      assert {:ok, result} =
               fresh_run(
                 reconcile_outcome: "absent",
                 send_result: {:duplicate, %{"status" => "ambiguous", "delivery_attempt" => 1}}
               )

      types = Enum.map(result.appended_events, & &1["type"])

      refute "assignment_dispatch_sent" in types
      assert "human_attention_required" in types
      assert result.summary["status"] == "blocked"
    end
  end

  describe "M4: a send left queued by a crash is reconciled, never repeated" do
    test "the fixture is the state a queued send actually leaves behind" do
      [last | _] = "events_post_dispatch_queued.jsonl" |> kill9_lines() |> Enum.reverse()
      event = Jason.decode!(last)

      assert event["type"] == "assignment_dispatch_sent"
      assert event["data"]["send_status"] == "queued"

      assert event["data"]["send_message_id"] == SendId.mint(@run_id, "as_0001"),
             "the fixture must speak the pinned send-id grammar, or M4 fails for the wrong reason"
    end

    test "resume asks about the queued send and does not paste it again" do
      # The daemon drained the queue after the crash: it answers `delivered`, so the resume
      # continues to observation. (A daemon still answering `queued` is MUST-5's convergence
      # case and is bounded by the assignment deadline in run_fsm_dispatch_queued_test.)
      assert {:ok, result} = resume_post_dispatch_queued(reconcile_outcome: "delivered")

      assert_receive {:reconcile_called, "pane_writer", _msg_id, _hash, _version}

      refute "pane_writer" in sent_panes(),
             """
             M4: the crash proves nothing about the paste. The daemon accepted the bytes
             before the process died, so a resend is a duplicate prompt and a refusal to
             ask is a lost one.
             """

      assert result.summary["status"] != "blocked"
    end

    test "a queued send the daemon can no longer account for blocks" do
      assert {:ok, result} = resume_post_dispatch_queued(reconcile_outcome: "ambiguous")

      refute "pane_writer" in sent_panes()
      assert result.summary["status"] == "blocked"
    end
  end

  describe "MUST-7: the projection carries an artifact baseline at v2" do
    test "a fresh projection is version 2 and records the baseline" do
      assert {:ok, result} = fresh_run(reconcile_outcome: "absent")

      event = Enum.find(result.events, &(&1["type"] == "assignment_prompt_projected"))

      assert event["event_version"] == 2,
             "MUST-7: a per-type bump is what lets a reader know which required fields to expect"

      assert event["data"]["artifact_baseline"],
             """
             MUST-7: "was there already an artifact at this path when the assignment
             started" is the fact that separates work the agent did from work that was
             already there. Recomputing it later reads a tree that has since changed.
             """
    end

    test "a version 1 projection is upcast to an explicit unrecorded baseline" do
      assert {:ok, result} = resume_post_prompt_v1(reconcile_outcome: "absent")

      # The journal map is exact -- version 1, no baseline -- and the read-side view is
      # where the upcaster speaks (ruling: the upgraded view stays separate from the
      # validated journal maps and the chain).
      event = Enum.find(result.events, &(&1["type"] == "assignment_prompt_projected"))
      assert event["event_version"] == 1
      refute Map.has_key?(event["data"], "artifact_baseline")

      assert {:ok, view} = EventData.upcast(event)
      baseline = get_in(view, ["data", "artifact_baseline"])

      # M3: a version-1 projection recorded no baseline, so the resume blocks for attention
      # before any send -- on an absent receipt too, since a snapshot taken now could only
      # land after the paste.
      assert result.summary["status"] == "blocked"
      refute "pane_writer" in sent_panes(), "no paste for an assignment whose baseline is unrecorded"

      assert Enum.any?(
               result.appended_events,
               &(&1["type"] == "human_attention_required" and &1["data"]["reason"] == "artifact_baseline_unrecorded")
             )

      assert baseline == %{"status" => "unrecorded"},
             """
             MUST-7: an old journal genuinely does not know. Saying so explicitly lets
             every later reader match on one shape, where a missing key would make each
             of them invent its own answer -- and some would invent "absent", which is a
             claim the journal never made.
             """
    end
  end

  # An ordering assertion fails by naming the call that ran first, and the call that ran
  # first may be the send -- which carries the prompt. The label says which call it was.
  defp label({:send_called, pane, _prompt, msg_id, version}), do: {:send_called, pane, msg_id, version}

  defp label({:send_called, pane, _prompt, msg_id}), do: {:send_called, pane, msg_id}
  defp label(other), do: other

  defp fresh_run(extra_dispatch_opts) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.run(spec, plan, [run_id: @run_id] ++ fsm_opts(extra_dispatch_opts))
  end

  # The post-prompt crash window at the current projection version (a recorded baseline);
  # the version-1 variant is the legacy journal the upcast test reads.
  defp resume_post_prompt(extra_dispatch_opts), do: resume_post_prompt("events_post_prompt_v2.jsonl", extra_dispatch_opts)
  defp resume_post_prompt_v1(extra_dispatch_opts), do: resume_post_prompt("events_post_prompt.jsonl", extra_dispatch_opts)

  defp resume_post_prompt(file, extra_dispatch_opts) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.resume(spec, plan, kill9_lines(file), fsm_opts(extra_dispatch_opts))
  end

  defp resume_post_dispatch_queued(extra_dispatch_opts) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.resume(spec, plan, kill9_lines("events_post_dispatch_queued.jsonl"), fsm_opts(extra_dispatch_opts))
  end

  defp kill9_lines(file) do
    [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", file]
    |> Path.join()
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  defp fsm_opts(extra_dispatch_opts) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    [
      dispatch: LocalPane,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts:
        [
          artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
          pane_client: ReconcilingPaneClient,
          test_pid: self()
        ] ++ extra_dispatch_opts,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  # An absent event is a distinct failure from a present event with the wrong
  # contents, and only the first is what a missing implementation looks like.
  # Reaching into `Enum.find/2` directly turns the first into a `BadMapError` on
  # `nil`, which reports the shape of the test rather than the shape of the gap.
  defp event_data!(%{appended_events: events}, type) do
    assert event = Enum.find(events, &(&1["type"] == type)),
           "no #{type} event was appended; the run produced #{inspect(Enum.map(events, & &1["type"]))}"

    Map.fetch!(event, "data")
  end

  defp fixture_data(events, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("data")

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(fn event -> {Map.fetch!(event["data"], "assignment_id"), event["data"]} end)
  end

  defp dispatch_data(%{appended_events: events}), do: dispatch_data_from(events)
  defp dispatch_data(%{events: events}), do: dispatch_data_from(events)

  defp dispatch_data_from(events) do
    case Enum.find(events, &(&1["type"] == "assignment_dispatch_sent")) do
      nil -> nil
      event -> event["data"]
    end
  end

  defp sent_panes do
    receive do
      {:send_called, pane_ref, _prompt, _msg_id, _version} -> [pane_ref | sent_panes()]
    after
      0 -> []
    end
  end

  # Every adapter call recorded so far, in the order it was made.
  defp drain_calls do
    receive do
      call when is_tuple(call) -> [call | drain_calls()]
    after
      0 -> []
    end
  end
end
