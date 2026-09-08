defmodule AiOrchestrator.Lifecycle.RunFSMDispatchBaselineTest do
  @moduledoc """
  RED for MUST-7 (Codex review `m_1788508818157722000_82f1b70d`): the artifact baseline
  must be durable before the side effect that depends on it.

  `LocalPane.deliver/2` snapshots the expected artifact *before* it pastes, because the
  observation that follows is "has this file changed since we asked?". But the snapshot is
  returned in the deliver result, and that result is only journaled in
  `assignment_dispatch_sent` *after* the paste. The window between those two moments is
  exactly the GAP-1 crash window, so the one value observation depends on is the one value
  a crash there destroys.

  What makes this worse than a lost field is that recomputing it looks like a repair. On
  resume the agent has, by then, already edited the file. A baseline taken now equals the
  post-edit state, so `modified_after_dispatch?/2` compares the file against itself and
  answers "not modified" forever: the run waits out its deadline and reports a missing
  artifact that is sitting on disk, finished. The observation does not fail -- it lies.

  So the baseline belongs in a pre-send event. `assignment_prompt_projected` is the
  nearest existing bracket: `Host.drive/3` commits it before it executes the dispatch
  effect, which is the same ordering guarantee that makes `prompt_hash` usable for MUST-2.
  A reconstructed dispatch then derives its baseline from that durable value, and a
  historical assignment that has no baseline blocks for attention rather than guessing.

  These tests deliberately do not inject `artifact_reader`. The injected reader answers
  from a fixture map and never touches the file the baseline describes, which is precisely
  what hides this defect today.

  Ruling (`m_1788571429536242416_dca73dd4`): per-event-type versioning. The projection is
  version 2 with a required baseline; a historical version-1 projection has none and its
  read-side view says so explicitly. The pre-crash journals below are therefore written at
  version 2 when they carry a baseline and at version 1 when they do not, and the prompt
  they name is a real object under a per-test prompt root, so the resume's fetch verifies
  the bytes the journal claims instead of re-rendering against a different repo root.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble

  @run_id "run_scenario_0001"
  @artifact "lib/item_a.ex"
  @pre_edit "defmodule ItemA do\nend\n"
  @post_edit "defmodule ItemA do\n  def call, do: :ok\nend\n"

  # The kill9 prefix records its assignment deadline at 2026-01-01; under the wall clock that
  # window is already closed and the observation's timeout is capped to nothing. The clock is
  # pinned before the deadline so the observation loop runs on its virtual budget alone.
  defmodule PinnedClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    @unix 1_767_225_000

    @impl true
    def unix_now, do: @unix

    @impl true
    def wall_ts, do: @unix |> DateTime.from_unix!() |> DateTime.to_iso8601()

    @impl true
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  defmodule ReceiptPaneClient do
    @moduledoc false

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref}, [])
      opts |> Keyword.get(:on_send, fn _pane_ref -> :ok end) |> apply([pane_ref])

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    # The scripted outcome is about the writer's crash-window send; the reviewer's send is
    # fresh on every path and the daemon has no record of it.
    def reconcile(pane_ref, message_id, opts) do
      outcome = if pane_ref == "pane_writer", do: Keyword.get(opts, :reconcile_outcome, "absent"), else: "absent"

      answer = %{
        "ok" => true,
        "protocol_version" => 2,
        "outcome" => outcome,
        "msg_id" => message_id,
        "pane_id" => pane_ref
      }

      # A receipt-bearing answer names its attempt, as the wire fixtures do.
      {:ok, if(outcome in ~w(delivered queued ambiguous), do: Map.put(answer, "delivery_attempt", 1), else: answer)}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  @prompt_bytes "* Assignment as_0001 (baseline fixture)\n"

  setup do
    repo_root = Path.join(System.tmp_dir!(), "ai_orch_baseline_#{System.unique_integer([:positive])}")
    prompt_root = Path.join(System.tmp_dir!(), "ai_orch_baseline_prompts_#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(repo_root, "lib"))
    File.mkdir_p!(Path.join(prompt_root, "prompts"))
    # The legacy (version-1 path) object the pre-crash journal names, as the migration wrote it.
    File.write!(Path.join(prompt_root, "prompts/as_0001.org"), @prompt_bytes)
    File.chmod!(Path.join(prompt_root, "prompts/as_0001.org"), 0o600)
    Process.put({__MODULE__, :prompt_root}, prompt_root)
    on_exit(fn -> File.rm_rf(repo_root) end)
    on_exit(fn -> File.rm_rf(prompt_root) end)
    {:ok, repo_root: repo_root}
  end

  describe "the baseline is durable before the paste" do
    test "a fresh run journals the baseline in the pre-send event", %{repo_root: repo_root} do
      write_artifact(repo_root, @pre_edit)

      assert {:ok, result} = fresh_run(repo_root)

      projected = event_data(result, "assignment_prompt_projected")

      assert projected["artifact_baseline"],
             """
             MUST-7: the baseline must be recorded before the side effect it brackets. It
             lives only in the post-paste event today, so the crash window destroys it.
             """

      assert projected["artifact_baseline"]["sha256"] == sha256(@pre_edit)
      assert projected["artifact_baseline"]["exists"] == true
    end

    test "the pre-send event is committed before the paste happens", %{repo_root: repo_root} do
      write_artifact(repo_root, @pre_edit)

      assert {:ok, result} = fresh_run(repo_root)

      projected_seq = event_seq(result, "assignment_prompt_projected")
      dispatch_seq = event_seq(result, "assignment_dispatch_sent")

      assert projected_seq < dispatch_seq,
             "the durable bracket must precede the event that records the send"
    end

    test "the dispatch event derives its baseline from the durable value", %{repo_root: repo_root} do
      write_artifact(repo_root, @pre_edit)

      assert {:ok, result} = fresh_run(repo_root)

      assert event_data(result, "assignment_dispatch_sent")["artifact_baseline"] ==
               event_data(result, "assignment_prompt_projected")["artifact_baseline"],
             "one baseline, recorded once; a second snapshot is a second answer to the same question"
    end
  end

  describe "resume from the crash window" do
    test "a reconstructed dispatch uses the journaled baseline, not a fresh snapshot", %{repo_root: repo_root} do
      # The pre-crash run snapshotted the file, pasted the prompt, and died before it could
      # journal the dispatch. The agent then finished the edit, so the file on disk now
      # differs from the baseline the journal holds.
      baseline = snapshot(repo_root, @pre_edit)
      write_artifact(repo_root, @post_edit)

      assert {:ok, result} = resume_with_baseline(repo_root, baseline, "delivered")

      refute_received {:send_called, "pane_writer"},
                      "a delivered receipt means the prompt already landed; resending duplicates it"

      dispatch = event_data(result, "assignment_dispatch_sent")

      assert dispatch["artifact_baseline"] == baseline,
             """
             MUST-7: the reconstructed event must carry the durable pre-send baseline. A
             fresh snapshot here equals the post-edit file, and the observation that
             follows would conclude the agent did nothing.
             """

      assert dispatch["send_status"] == "reconciled"
    end

    test "observation against the journaled baseline sees the completed work", %{repo_root: repo_root} do
      baseline = snapshot(repo_root, @pre_edit)
      write_artifact(repo_root, @post_edit)

      assert {:ok, result} = resume_with_baseline(repo_root, baseline, "delivered")

      types = Enum.map(result.appended_events, & &1["type"])

      assert "artifact_observed" in types,
             """
             This is the whole point of the durable baseline: with it, the finished file is
             recognized as modified. Without it, the run waits out its deadline while the
             artifact sits on disk complete.
             """
    end

    test "a historical assignment with no baseline blocks instead of guessing", %{repo_root: repo_root} do
      write_artifact(repo_root, @post_edit)

      assert {:ok, result} = resume_with_baseline(repo_root, nil, "delivered")

      types = Enum.map(result.appended_events, & &1["type"])

      assert "human_attention_required" in types,
             """
             MUST-7: a journal written before this change has no pre-send baseline. Taking
             one now would silently produce the lying observation described above, so the
             only honest answer is to stop and say so.
             """

      assert result.summary["status"] == "blocked"

      refute "artifact_observed" in types
      refute_received {:send_called, "pane_writer"}
    end
  end

  defp fresh_run(repo_root) do
    opts = fsm_opts(reconcile_outcome: "absent", on_send: agent_edits(repo_root))
    RunFSM.run(spec(repo_root), plan(), [run_id: @run_id] ++ opts)
  end

  # The fake agent does its work when the prompt lands, so a fresh run reaches the
  # observation the baseline exists to serve instead of timing out on an untouched file.
  defp agent_edits(repo_root) do
    fn
      "pane_reviewer" -> write_file(repo_root, "review/item_a.org", "- Verdict :: clean\n")
      _writer_pane -> write_file(repo_root, @artifact, @post_edit)
    end
  end

  # The resumed run continues past the writer to the reviewer, whose prompt is sent fresh,
  # so the fake agent is wired here as well: it writes the review when that prompt lands.
  defp resume_with_baseline(repo_root, baseline, outcome) do
    RunFSM.resume(
      spec(repo_root),
      plan(),
      journal_lines(baseline),
      fsm_opts(reconcile_outcome: outcome, on_send: agent_edits(repo_root))
    )
  end

  # The pre-crash journal: the shipped kill9 prefix through the pane and workspace leases,
  # plus the projection the pre-crash run committed before it pasted.
  defp journal_lines(baseline) do
    prefix =
      [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", "events_pre_dispatch.jsonl"]
      |> Path.join()
      |> File.read!()
      |> String.split("\n", trim: true)

    data =
      then(
        %{
          "assignment_id" => "as_0001",
          "prompt_path" => "prompts/as_0001.org",
          "prompt_hash" => sha256(@prompt_bytes),
          "prompt_bytes" => byte_size(@prompt_bytes),
          "context_revision" => 0,
          "context_hash" => "sha256:" <> String.duplicate("00", 32),
          "expected_artifact" => @artifact
        },
        fn data -> if baseline, do: Map.put(data, "artifact_baseline", baseline), else: data end
      )

    projected =
      Jason.encode!(%{
        "schema" => "ai-orchestrator/journal-event",
        "schema_version" => 1,
        # Version 2 is the projection that recorded its baseline; a journal written before
        # this change is version 1 and has none.
        "event_version" => if(baseline, do: 2, else: 1),
        "seq" => 10,
        "event_id" => "ev_0010",
        "type" => "assignment_prompt_projected",
        "ts" => "2026-01-01T00:00:10Z",
        "run_id" => @run_id,
        "actor" => "run_supervisor",
        "data" => data
      })

    prefix ++ [projected]
  end

  defp spec(repo_root) do
    "scenarios" |> F.json("kill9_resume", "spec.json") |> Map.put("repo_root", repo_root)
  end

  defp plan, do: F.json("scenarios", "kill9_resume", "plan.json")

  # No artifact_reader: the real LocalPane reader is the subject of these tests.
  defp fsm_opts(extra_dispatch_opts) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    [
      clock: PinnedClock,
      dispatch: LocalPane,
      prompt_root: Process.get({__MODULE__, :prompt_root}),
      dispatch_opts: [pane_client: ReceiptPaneClient, test_pid: self()] ++ observation_budget() ++ extra_dispatch_opts,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  # The observation loop is not under test here; only the baseline is. A virtual
  # clock plus a no-op sleeper lets the real `stable_for_ms: 5000` requirement be
  # satisfied and the real timeout be reached without burning wall-clock time.
  defp observation_budget do
    [
      observe_timeout_ms: 60_000,
      poll_interval_ms: 1,
      sleeper: fn _milliseconds -> :ok end,
      monotonic_ms: virtual_clock(1_000)
    ]
  end

  defp virtual_clock(step_ms) do
    counter = :counters.new(1, [:atomics])

    fn ->
      :counters.add(counter, 1, step_ms)
      :counters.get(counter, 1)
    end
  end

  defp fixture_data(events, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("data")

  defp write_artifact(repo_root, contents), do: write_file(repo_root, @artifact, contents)

  defp write_file(repo_root, path, contents) do
    full = Path.join(repo_root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, contents)
  end

  defp snapshot(repo_root, contents) do
    write_artifact(repo_root, contents)
    stat = File.stat!(Path.join(repo_root, @artifact), time: :posix)

    %{
      "exists" => true,
      "bytes" => byte_size(contents),
      "mtime_unix" => stat.mtime,
      "sha256" => sha256(contents)
    }
  end

  defp sha256(contents), do: "sha256:" <> (:sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower))

  defp events(%{appended_events: events}), do: events
  defp events(%{events: events}), do: events

  defp event_data(result, type) do
    case Enum.find(events(result), &(&1["type"] == type)) do
      nil -> nil
      event -> event["data"]
    end
  end

  defp event_seq(result, type) do
    case Enum.find(events(result), &(&1["type"] == type)) do
      nil -> nil
      event -> event["seq"]
    end
  end
end
