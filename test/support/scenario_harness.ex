defmodule AiOrchestrator.Test.ScenarioHarness do
  @moduledoc """
  Scenario inputs shared by the reducer parity and determinism tests.

  Each case names the fixture, the entry point, the prior lines, and a function
  that builds fresh options (fresh Agents for stateful doubles). The doubles
  mirror the ones the lifecycle tests already use so both engines receive the
  same observations; no existing lifecycle test is edited.
  """
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.FixedId
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScriptedDispatchReceipt

  defmodule FakePaneClient do
    @moduledoc false
    # Every stand-in answers a reconcile: the adapter asks before every send.
    def reconcile(pane_ref, message_id, _opts),
      do:
        {:ok,
         %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts),
      do:
        {:ok,
         %{
           "ok" => true,
           "protocol_version" => 2,
           "status" => "sent",
           "msg_id" => opts[:message_id],
           "pane_id" => pane_ref
         }}

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  defmodule OkDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def deliver(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "backend" => "local_pane",
         "pane_ref" => command["pane_ref"],
         "send_status" => "ok",
         "send_message_id" => command["send_message_id"],
         "prompt_hash" => command["payload_hash"],
         "replayed" => false
       }}
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => LocalPane.zero_hash(),
         "stable_for_ms" => 5_000,
         "modified_after_dispatch" => true
       }}
    end
  end

  defmodule BlockedDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def deliver(command, opts), do: OkDispatch.deliver(command, opts)

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, _opts) do
      {:blocked,
       %{
         "reason" => "agent_auth_blocked",
         "pane_ref" => command["pane_ref"],
         "pane_state" => "blocked",
         "pending_count" => 0
       }}
    end
  end

  @doc "Resets the fixed seams so two engine runs see identical timestamps and ids."
  def reset_seams do
    FixedClock.reset()
    FixedId.reset()
  end

  @doc "Every scenario case: {name, kind, spec, plan, prior_lines, opts_fun}."
  def cases do
    [
      {"gated_run_seed run", :run, "gated_run_seed", [], &local_pane_opts/0},
      {"gated_run_seed resume from terminal", :resume, "gated_run_seed", F.lines("scenarios", "gated_run_seed"),
       &local_pane_opts/0},
      {"gated_run_seed cancel from terminal", :cancel, "gated_run_seed", F.lines("scenarios", "gated_run_seed"),
       &plain_opts/0},
      {"auth_blocked_pane run", :run, "auth_blocked_pane", [],
       fn ->
         [
           dispatch: BlockedDispatch,
           clock: FixedClock,
           id: FixedId,
           prompt_root: prompt_root(),
           event_sink: GateDouble.receipt_sink()
         ]
       end},
      {"gate_failure_summary_feedback run", :run, "gate_failure_summary_feedback", [], &flaky_gate_opts/0},
      {"concurrency_cap run", :run, "concurrency_cap", [], &ok_dispatch_opts/0},
      {"fingerprint_drift run", :run, "fingerprint_drift", [], &ok_dispatch_opts/0},
      {"kill9 resume pre_dispatch", :resume, "kill9_resume", kill9_lines("events_pre_dispatch.jsonl"),
       &local_pane_opts/0},
      {"kill9 resume awaiting_artifact", :resume, "kill9_resume", kill9_lines("events_awaiting_artifact.jsonl"),
       &local_pane_opts/0},
      {"kill9 resume pre_gate", :resume, "kill9_resume", kill9_lines("events_pre_gate.jsonl"), &local_pane_opts/0},
      {"kill9 cancel pre_dispatch", :cancel, "kill9_resume", kill9_lines("events_pre_dispatch.jsonl"), &plain_opts/0}
    ]
  end

  def spec(name), do: F.json("scenarios", name, "spec.json")
  def plan(name), do: F.json("scenarios", name, "plan.json")

  defp kill9_lines(file) do
    [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", file]
    |> Path.join()
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  # Every fresh assignment retains its prompt before it projects, so every case needs a run
  # directory to retain into. A fresh directory per call keeps cases independent; the store
  # is content-addressed and create-only, so nothing in one case can be read by another
  # even if two share a render.
  @doc "A fresh, existing run directory for prompt objects, removed when the test exits."
  def prompt_root do
    root = Path.join(System.tmp_dir!(), "scenario-prompts-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(root) end)
    root
  end

  defp plain_opts, do: [clock: FixedClock, id: FixedId, prompt_root: prompt_root(), event_sink: GateDouble.receipt_sink()]

  defp ok_dispatch_opts do
    [
      dispatch: OkDispatch,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate -> pass_gate() end],
      review_reader: &clean_review/1,
      clock: FixedClock,
      id: FixedId,
      prompt_root: prompt_root(),
      event_sink: GateDouble.receipt_sink()
    ]
  end

  defp local_pane_opts do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    artifact_by_assignment =
      fixture_events
      |> Enum.filter(&(&1["type"] == "artifact_observed"))
      |> Map.new(fn event -> {event["data"]["assignment_id"], event["data"]} end)

    gate_pass =
      fixture_events |> Enum.find(&(&1["type"] == "gate_passed")) |> Map.fetch!("data") |> Map.delete("gate_run_id")

    artifact_reader = fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end

    [
      dispatch: LocalPane,
      dispatch_opts: [artifact_reader: artifact_reader, pane_client: FakePaneClient],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      review_reader: &clean_review/1,
      clock: FixedClock,
      id: FixedId,
      prompt_root: prompt_root(),
      event_sink: GateDouble.receipt_sink()
    ]
  end

  defp flaky_gate_opts do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    flaky_gate = fn _gate ->
      case Agent.get_and_update(counter, &{&1, &1 + 1}) do
        0 ->
          {:failed,
           %{
             "exit_status" => 1,
             "duration_ms" => 100,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash(),
             "failure_summary" => %{
               "headline" => "1 test failed",
               "failures" => [%{"line" => "test/item_a_test.exs:12"}],
               "suggestion" => "fix the failing check"
             }
           }}

        _later ->
          pass_gate()
      end
    end

    [
      dispatch: OkDispatch,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: flaky_gate],
      review_reader: &clean_review/1,
      clock: FixedClock,
      id: FixedId,
      prompt_root: prompt_root(),
      event_sink: GateDouble.receipt_sink()
    ]
  end

  defp pass_gate do
    {:ok,
     %{
       "exit_status" => 0,
       "duration_ms" => 100,
       "stdout_hash" => LocalPane.zero_hash(),
       "stderr_hash" => LocalPane.zero_hash()
     }}
  end

  defp clean_review(_path), do: {:ok, "- Verdict :: clean\n"}
end
