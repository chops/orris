defmodule AiOrchestrator.Lifecycle.RunFSMClockTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule CapturingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "backend" => "local_pane",
         "pane_ref" => command["pane_ref"],
         "send_status" => "ok",
         "send_message_id" => command["send_message_id"],
         "replayed" => false
       }}
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, opts) do
      if pid = Keyword.get(opts, :test_pid) do
        send(pid, {:observe_opts, command["assignment_id"], Keyword.get(opts, :observe_timeout_ms)})
      end

      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => LocalPane.zero_hash(),
         "stable_for_ms" => 5000,
         "modified_after_dispatch" => true
       }}
    end
  end

  setup do
    FixedClock.reset()
    :ok
  end

  defp stub_gate_runner(_gate) do
    {:ok,
     %{
       "exit_status" => 0,
       "duration_ms" => 100,
       "stdout_hash" => LocalPane.zero_hash(),
       "stderr_hash" => LocalPane.zero_hash()
     }}
  end

  defp run_seed(extra_opts \\ []) do
    spec = F.json("scenarios", "gated_run_seed", "spec.json")
    plan = F.json("scenarios", "gated_run_seed", "plan.json")

    RunFSM.run(
      spec,
      plan,
      extra_opts ++
        [
          clock: FixedClock,
          dispatch: CapturingDispatch,
          prompt_root: ScenarioHarness.prompt_root(),
          dispatch_opts: [test_pid: self()],
          gate_executor: GateDouble,
          gate_helper: GateDouble.helper(),
          gate_opts: [runner: &stub_gate_runner/1],
          event_sink: GateDouble.receipt_sink()
        ]
    )
  end

  # Deadline truth (post-smoke follow-up): the journaled deadline_unix must be
  # computed from the clock at dispatch plus the effective timeout — never the
  # fixture-era constant 1_767_225_600.
  test "deadline_unix is clock-derived, not the fixture constant" do
    assert {:ok, %{events: events}} = run_seed()

    requested = Enum.filter(events, &(&1["type"] == "assignment_requested"))
    assert requested != []

    for event <- requested do
      deadline = event["data"]["deadline_unix"]
      refute deadline == 1_767_225_600
      assert deadline > FixedClock.base_unix()
      assert deadline <= FixedClock.base_unix() + 24 * 3600
    end
  end

  test "observation_started reuses the assignment deadline verbatim (durable deadline rule)" do
    assert {:ok, %{events: events}} = run_seed()

    deadlines =
      events
      |> Enum.filter(&(&1["type"] == "assignment_requested"))
      |> Map.new(&{&1["data"]["assignment_id"], &1["data"]["deadline_unix"]})

    observations = Enum.filter(events, &(&1["type"] == "assignment_observation_started"))
    assert observations != []

    for event <- observations do
      assert event["data"]["deadline_unix"] == Map.fetch!(deadlines, event["data"]["assignment_id"])
    end
  end

  test "work item timeout_s overrides the default deadline budget" do
    spec = F.json("scenarios", "gated_run_seed", "spec.json")

    plan =
      "scenarios"
      |> F.json("gated_run_seed", "plan.json")
      |> update_in(["work_items", Access.at(0)], &Map.put(&1, "timeout_s", 120))

    assert {:ok, %{events: events}} =
             RunFSM.run(spec, plan,
               clock: FixedClock,
               dispatch: CapturingDispatch,
               prompt_root: ScenarioHarness.prompt_root(),
               dispatch_opts: [test_pid: self()],
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: &stub_gate_runner/1],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
             )

    writer_request =
      Enum.find(events, &(&1["type"] == "assignment_requested" and &1["data"]["role"] == "writer"))

    dispatched_at_bound = FixedClock.base_unix() + 100
    assert writer_request["data"]["deadline_unix"] <= dispatched_at_bound + 120
    assert writer_request["data"]["deadline_unix"] >= FixedClock.base_unix() + 120
  end

  test "observe receives its timeout derived from the journaled deadline" do
    assert {:ok, _result} = run_seed()

    assert_receive {:observe_opts, "as_0001", timeout_ms}
    assert is_integer(timeout_ms)
    assert timeout_ms > 0
    assert timeout_ms <= 900 * 1000
  end

  test "event ts values come from the clock, not a seq-derived epoch" do
    assert {:ok, %{events: events}} = run_seed()

    for event <- events do
      assert {:ok, parsed, 0} = DateTime.from_iso8601(event["ts"])
      assert DateTime.to_unix(parsed) >= FixedClock.base_unix()
    end

    refute Enum.any?(events, &String.starts_with?(&1["ts"], "2026-01-01T"))
  end

  # F-2 (NO-GO review): crash-never-grants-more-time, proven ON RESUME.
  test "resume derives observe timeout from the recorded deadline, not a fresh default" do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    prior_lines =
      "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
      |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
      |> File.read!()
      |> String.split("\n", trim: true)

    recorded_deadline = FixedClock.base_unix() + 900
    prior_lines = reanchor_deadlines(prior_lines, recorded_deadline)

    # Advance the clock so 100 seconds of the recorded budget remain.
    FixedClock.advance(800)

    assert {:ok, _result} =
             RunFSM.resume(spec, plan, prior_lines,
               clock: FixedClock,
               dispatch: CapturingDispatch,
               prompt_root: ScenarioHarness.prompt_root(),
               dispatch_opts: [test_pid: self()],
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: &stub_gate_runner/1],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
             )

    assert_receive {:observe_opts, "as_0001", timeout_ms}
    assert timeout_ms <= 100 * 1000
    assert timeout_ms > 0
  end

  test "resume with an expired recorded deadline grants zero observation budget" do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    prior_lines =
      "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
      |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
      |> File.read!()
      |> String.split("\n", trim: true)

    recorded_deadline = FixedClock.base_unix() + 900
    prior_lines = reanchor_deadlines(prior_lines, recorded_deadline)

    FixedClock.advance(900 + 3600)

    RunFSM.resume(spec, plan, prior_lines,
      clock: FixedClock,
      dispatch: CapturingDispatch,
      prompt_root: ScenarioHarness.prompt_root(),
      dispatch_opts: [test_pid: self()],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: &stub_gate_runner/1],
      event_sink: GateDouble.receipt_sink()
    )

    assert_receive {:observe_opts, "as_0001", timeout_ms}
    assert timeout_ms == 0
  end

  # F-3: deadline is an upper bound — a stricter caller cap survives, a looser one is reduced.
  test "caller observe_timeout_ms below the deadline remainder is preserved" do
    assert {:ok, _result} = run_seed(dispatch_opts: [test_pid: self(), observe_timeout_ms: 5_000])

    assert_receive {:observe_opts, "as_0001", timeout_ms}
    assert timeout_ms == 5_000
  end

  test "caller observe_timeout_ms above the deadline remainder is capped down" do
    assert {:ok, _result} =
             run_seed(dispatch_opts: [test_pid: self(), observe_timeout_ms: 100_000_000])

    assert_receive {:observe_opts, "as_0001", timeout_ms}
    assert timeout_ms <= 900 * 1000
  end

  # F-4: the synthesized review assignment inherits the subject work item timeout.
  test "review assignment deadline inherits the subject work item timeout_s" do
    spec = F.json("scenarios", "gated_run_seed", "spec.json")

    plan =
      "scenarios"
      |> F.json("gated_run_seed", "plan.json")
      |> update_in(["work_items", Access.at(0)], &Map.put(&1, "timeout_s", 120))

    assert {:ok, %{events: events}} =
             RunFSM.run(spec, plan,
               clock: FixedClock,
               dispatch: CapturingDispatch,
               prompt_root: ScenarioHarness.prompt_root(),
               dispatch_opts: [test_pid: self()],
               gate_executor: GateDouble,
               gate_helper: GateDouble.helper(),
               gate_opts: [runner: &stub_gate_runner/1],
               event_sink: GateDouble.receipt_sink(),
               review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
             )

    reviewer_request =
      Enum.find(events, &(&1["type"] == "assignment_requested" and &1["data"]["role"] == "reviewer"))

    assert reviewer_request["data"]["deadline_unix"] <= FixedClock.base_unix() + 200 + 120
    assert reviewer_request["data"]["deadline_unix"] >= FixedClock.base_unix() + 120
  end

  defp reanchor_deadlines(lines, deadline_unix) do
    Enum.map(lines, fn line ->
      event = Jason.decode!(line)

      case event do
        %{"data" => %{"deadline_unix" => _old} = data} ->
          Jason.encode!(Map.put(event, "data", Map.put(data, "deadline_unix", deadline_unix)))

        _other ->
          line
      end
    end)
  end
end
