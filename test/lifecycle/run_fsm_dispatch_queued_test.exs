defmodule AiOrchestrator.Lifecycle.RunFSMDispatchQueuedTest do
  @moduledoc """
  RED for MUST-5 (Codex review `m_1788508818157722000_82f1b70d`): `queued` is not terminal.

  A `queued` reply means the daemon accepted the prompt but has not pasted it yet, because
  the pane was busy. The paste happens later during drain, and it can fail there -- the
  caller already received `{:queued, _}` and is never told. So a lifecycle that treats
  `queued` as "the agent has the prompt" and moves straight to artifact observation is
  waiting for work that may never have been requested.

  The failure that follows is the dangerous kind, because it is plausible: the artifact
  never appears, observation times out, and the run reports an *artifact* problem. The
  operator reads "the agent did not produce lib/item_a.ex" and goes looking at the agent.
  Nothing in the journal says the prompt was dropped in a queue.

  Second revision, against the queued ruling (`m_..._store_started`) and D3/R2:

    * The daemon is asked about a queued send until it answers, bounded by the assignment
      deadline, and observation begins only on `delivered`.
    * A loss the daemon cannot place before the paste is `ambiguous` and blocks: the bytes
      may have landed, so a second paste is a duplicate prompt.
    * A loss the daemon proves happened before any paste is `absent`, and is retried once
      under the same id (the daemon admits it as a new delivery attempt). A second `absent`
      is attention, not a third paste. The five outcomes are not widened.
    * Reconcile precedes every SEND (D3/R2), so each script opens with the pre-send answer.

  The clock is scripted: every read steps it forward, so the deadline bound is proven
  without waiting for it, and the host's paced wait between polls costs nothing.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  @run_id "run_scenario_0001"

  defmodule SteppingClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    @base 1_800_000_000
    @step 30

    @impl true
    def unix_now do
      reads = Process.get({__MODULE__, :reads}, 0)
      Process.put({__MODULE__, :reads}, reads + 1)
      @base + reads * @step
    end

    @impl true
    def wall_ts, do: unix_now() |> DateTime.from_unix!() |> DateTime.to_iso8601()

    @impl true
    def monotonic_ms, do: unix_now() * 1000
  end

  defmodule ScriptedPaneClient do
    @moduledoc false

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, opts[:message_id]}, [])

      {:ok,
       %{
         "ok" => true,
         "protocol_version" => 2,
         "status" => Keyword.get(opts, :send_status, "queued"),
         "msg_id" => opts[:message_id],
         "pane_id" => pane_ref
       }}
    end

    def reconcile(pane_ref, message_id, opts) do
      outcome = Agent.get_and_update(Keyword.fetch!(opts, :script), &next_outcome/1)
      Process.send(Keyword.fetch!(opts, :test_pid), {:reconcile_called, pane_ref, outcome}, [])

      # A receipt-bearing answer names its attempt, as the wire fixtures do; a no-record
      # absent carries none.
      answer = %{
        "ok" => true,
        "protocol_version" => 2,
        "outcome" => outcome,
        "msg_id" => message_id,
        "pane_id" => pane_ref
      }

      {:ok, if(outcome in ~w(delivered queued ambiguous), do: Map.put(answer, "delivery_attempt", 1), else: answer)}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}

    # The last scripted outcome repeats, so a lifecycle that never stops re-querying hangs
    # here instead of accidentally passing on a scripted terminal.
    defp next_outcome([last]), do: {last, [last]}
    defp next_outcome([head | rest]), do: {head, rest}
  end

  test "a queued send is journaled as queued, not as ok" do
    assert {:ok, result} = run_with(["absent", "queued", "delivered"])

    assert dispatch_data(result)["send_status"] == "queued",
           "EJ-7 distinguishes an accepted-but-unpasted send from a completed one"
  end

  test "a queued send converges to delivered before observation treats it as usable" do
    assert {:ok, result} = run_with(["absent", "queued", "delivered"])

    assert Enum.take(reconcile_outcomes(), 3) == ["absent", "queued", "delivered"],
           """
           MUST-5: the lifecycle must keep converging a queued receipt. Stopping at
           `queued` means observation begins for a prompt that may still be dropped
           during drain.
           """

    assert result.summary["status"] == "completed"

    assert seq_of(result, "assignment_dispatch_sent") < seq_of(result, "assignment_observation_started"),
           "observation is journaled only after the daemon answered delivered"
  end

  test "a queued send lost in the paste window is ambiguous and becomes dispatch attention" do
    assert {:ok, result} = run_with(["absent", "queued", "ambiguous"])

    types = Enum.map(result.events, & &1["type"])

    assert "human_attention_required" in types
    assert result.summary["status"] == "blocked"

    refute "artifact_observation_timed_out" in types,
           """
           MUST-5: a prompt dropped in the daemon's queue is a dispatch failure. Reporting
           it as a missing artifact sends the operator to inspect an agent that was never
           asked to do anything.
           """

    wedge = event_data(result, "agent_wedge_detected")

    assert wedge["detector"] == "dispatch_reconcile",
           "the attention record must name the check that fired, got: #{inspect(wedge["detector"])}"

    assert wedge["reason"] == "dispatch_queued_ambiguous"
  end

  test "a queued send that becomes ambiguous during drain blocks rather than resending" do
    assert {:ok, result} = run_with(["absent", "queued", "ambiguous"])

    assert result.summary["status"] == "blocked"

    assert Enum.count(sent_panes()) == 1,
           """
           An ambiguous drain failure cannot prove the bytes did not land, so the prompt
           must not be sent a second time.
           """
  end

  test "a queued send the daemon proves it never pasted is retried once under the same id" do
    assert {:ok, result} = run_with(["absent", "queued", "absent", "absent", "queued", "delivered"])

    assert result.summary["status"] == "completed"

    sends = sent_messages()

    assert length(sends) == 2, "the proven pre-paste loss is retried exactly once"

    assert match?([{"pane_writer", id}, {"pane_writer", id}] when is_binary(id), sends),
           "the retry keeps the recorded send id, got: #{inspect(sends)}"

    assert Enum.count(
             result.events,
             &(&1["type"] == "assignment_dispatch_sent" and &1["data"]["assignment_id"] == "as_0001")
           ) == 2,
           "both physical attempts are journaled"
  end

  test "a second proven loss is attention, not a third paste" do
    assert {:ok, result} = run_with(["absent", "queued", "absent", "absent", "queued", "absent"])

    assert result.summary["status"] == "blocked"
    assert length(sent_messages()) == 2
    assert event_data(result, "agent_wedge_detected")["reason"] == "dispatch_queued_absent"
  end

  test "a send that never leaves the queue stops at the assignment deadline" do
    assert {:ok, result} = run_with(["absent", "queued"])

    assert result.summary["status"] == "blocked",
           """
           MUST-5 bounds convergence by the assignment deadline. Without a bound this is
           an unbounded poll against a daemon that will never change its answer.
           """

    types = Enum.map(result.events, & &1["type"])
    assert "human_attention_required" in types
    assert event_data(result, "agent_wedge_detected")["reason"] == "dispatch_queued_deadline_exceeded"
    refute "assignment_failed" in types, "R3 keeps assignment_failed reserved for the Wave 4 producer"
    refute "assignment_observation_started" in types, "observation never began for a prompt that was never pasted"
  end

  defp run_with(outcomes) do
    {:ok, script} = Agent.start_link(fn -> outcomes end)
    on_exit(fn -> if Process.alive?(script), do: Agent.stop(script) end)

    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.run(spec, plan, [run_id: @run_id] ++ fsm_opts(script))
  end

  defp fsm_opts(script) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    [
      dispatch: LocalPane,
      clock: SteppingClock,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts: [
        artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
        pane_client: ScriptedPaneClient,
        script: script,
        test_pid: self()
      ],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  defp fixture_data(events, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("data")

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(fn event -> {Map.fetch!(event["data"], "assignment_id"), event["data"]} end)
  end

  defp dispatch_data(result), do: event_data(result, "assignment_dispatch_sent")

  defp event_data(%{events: events}, type) do
    case Enum.find(events, &(&1["type"] == type)) do
      nil -> nil
      event -> event["data"]
    end
  end

  defp seq_of(%{events: events}, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("seq")

  defp reconcile_outcomes do
    receive do
      {:reconcile_called, _pane_ref, outcome} -> [outcome | reconcile_outcomes()]
    after
      0 -> []
    end
  end

  defp sent_messages do
    receive do
      {:send_called, pane_ref, message_id} -> [{pane_ref, message_id} | sent_messages()]
    after
      0 -> []
    end
  end

  defp sent_panes, do: Enum.map(sent_messages(), &elem(&1, 0))
end
