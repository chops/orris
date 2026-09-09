defmodule C1.RunExplanationTest do
  use ExUnit.Case, async: true
  alias OrrisConsole.RunExplanation

  defp run(ref, status, seq, id \\ "independent_run", error \\ nil),
    do: %{run_ref: ref, status: status, last_seq: seq, run_id: id, error: error}

  test "attention precedes active and terminal runs without comparing unrelated event sequences" do
    runs = [
      run("z-active", "in_flight", 1000),
      run("a-active", "in_flight", 1),
      run("done", "completed", 5000),
      run("blocked", "blocked", 3),
      run("failed", "failed", 4),
      run("unreadable", "invalid", nil, "x", :unavailable)
    ]

    assert Enum.map(RunExplanation.ordered(runs), & &1.run_ref) == [
             "blocked",
             "failed",
             "unreadable",
             "a-active",
             "z-active",
             "done"
           ]
  end

  test "the three supplied checkpoints sort by journal position; same names on other runs do not identify the demo" do
    entries = [
      run("awaiting-artifact", "in_flight", 12, "run_scenario_0001"),
      run("before-dispatch", "in_flight", 9, "run_scenario_0001"),
      run("before-gate", "in_flight", 27, "run_scenario_0001")
    ]

    assert RunExplanation.demo_runs?(entries)
    assert Enum.map(RunExplanation.ordered(entries), & &1.last_seq) == [9, 12, 27]
    refute RunExplanation.demo_runs?(Enum.map(entries, &%{&1 | run_id: "other"}))
    refute RunExplanation.demo_runs?([%{hd(entries) | last_seq: 13} | tl(entries)])
  end

  test "an incomplete journal takes priority over a recorded terminal outcome" do
    guidance = RunExplanation.guidance(%{status: "completed", pending_repair: %{action: :truncate_tail}})
    assert guidance.title == "The journal needs attention"
    assert guidance.meaning =~ "verified part"
  end

  test "in-flight guidance does not infer agent liveness and unknown status is not described as success" do
    assert RunExplanation.guidance(%{status: "in_flight", pending_repair: nil}).meaning =~
             "does not tell you whether an agent is currently running"

    assert RunExplanation.guidance(%{status: "future_state", pending_repair: nil}).title == "Inspect the recorded state"
  end
end
