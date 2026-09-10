defmodule AiOrchestrator.Lifecycle.S4RecoveryReviewTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.FixedId
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule ReceiptClient do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane, _prompt, opts) do
      Agent.update(opts[:receipt], &Map.update!(&1, :sends, fn n -> n + 1 end))

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "pane_id" => pane, "msg_id" => opts[:message_id], "status" => "queued"}}
    end

    def reconcile(pane, id, opts) do
      {outcome, count} =
        Agent.get_and_update(opts[:receipt], fn s ->
          [outcome | rest] = s.outcomes
          {{outcome, s.sends}, %{s | outcomes: if(rest == [], do: [outcome], else: rest)}}
        end)

      response = %{"ok" => true, "protocol_version" => 2, "pane_id" => pane, "msg_id" => id, "outcome" => outcome}
      response = if count > 0, do: Map.put(response, "delivery_attempt", count), else: response
      {:ok, response}
    end

    def status(pane, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane, "pending_count" => 0}}
  end

  defmodule InvalidOutcomeDispatch do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def deliver(command, opts) do
      {:ok, data} = ScenarioHarness.OkDispatch.deliver(command, opts)
      {:ok, Map.put(data, "send_status", "queued")}
    end

    def reconcile(_command, _opts), do: {:ok, %{"outcome" => "UNSUPPORTED_REVIEW_CANARY"}}
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    def observe(command, opts), do: ScenarioHarness.OkDispatch.observe(command, opts)
  end

  setup do
    FixedClock.reset()
    FixedId.reset()
    events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    artifacts =
      events
      |> Enum.filter(&(&1["type"] == "artifact_observed"))
      |> Map.new(&{&1["data"]["assignment_id"], &1["data"]})

    gate = Map.delete(Enum.find(events, &(&1["type"] == "gate_passed"))["data"], "gate_run_id")
    receipt = start_supervised!({Agent, fn -> %{outcomes: ["absent"], sends: 0} end})

    opts = [
      run_id: "run_scenario_0001",
      clock: FixedClock,
      id: FixedId,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch: LocalPane,
      dispatch_opts: [
        pane_client: ReceiptClient,
        receipt: receipt,
        artifact_reader: fn c -> {:ok, Map.fetch!(artifacts, c["assignment_id"])} end
      ],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _, _ -> {:ok, gate} end],
      review_reader: fn _ -> {:ok, "- Verdict :: clean\n"} end,
      event_sink: GateDouble.receipt_sink()
    ]

    {:ok,
     receipt: receipt,
     opts: opts,
     spec: F.json("scenarios", "kill9_resume", "spec.json"),
     plan: F.json("scenarios", "kill9_resume", "plan.json")}
  end

  test "a receipt already queued at initial reconciliation must converge before observing", c do
    Agent.update(c.receipt, &%{&1 | outcomes: ["queued", "ambiguous"]})
    assert {:ok, result} = RunFSM.run(c.spec, c.plan, c.opts)
    assert result.summary["status"] == "blocked"
    refute Enum.any?(result.events, &(&1["type"] == "assignment_observation_started"))
    assert Agent.get(c.receipt, & &1.sends) == 0
  end

  test "unknown reconcile outcome is a named rejection rather than a reducer crash", c do
    opts = Keyword.put(c.opts, :dispatch, InvalidOutcomeDispatch)
    assert {:error, %{"reason" => "dispatch_reconcile_invalid_return"}} = RunFSM.run(c.spec, c.plan, opts)
  end

  test "retry allowance survives a crash after second admission before its journal event", c do
    {:ok, committed} = Agent.start_link(fn -> [] end)

    sink = fn event ->
      count = Agent.get(c.receipt, & &1.sends)

      if event["type"] == "assignment_dispatch_sent" and count == 2 do
        {:error, %{"reason" => "review_injected_crash"}}
      else
        Agent.update(committed, &(&1 ++ [event]))
        {:ok, event}
      end
    end

    assert {:error, _} = RunFSM.run(c.spec, c.plan, Keyword.put(c.opts, :event_sink, GateDouble.receipt(sink)))
    assert Agent.get(c.receipt, & &1.sends) == 2
    prefix = Agent.get(committed, & &1)
    assert Enum.count(prefix, &(&1["type"] == "assignment_dispatch_sent")) == 1
    assert {:ok, result} = RunFSM.resume(c.spec, c.plan, Enum.map(prefix, &Jason.encode!/1), c.opts)
    assert result.summary["status"] == "blocked"
    assert Agent.get(c.receipt, & &1.sends) == 2
  end
end
