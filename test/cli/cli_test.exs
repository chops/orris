defmodule AiOrchestrator.CLITest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  test "validate checks the run spec and plan in a run directory" do
    run_dir =
      "validate"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    assert CLI.run(["validate", run_dir]) == %{status: 0, stdout: "valid\n", stderr: ""}
  end

  test "run writes the journal and org projections" do
    run_dir =
      "run"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["run", run_dir], fsm_opts())
    assert stdout =~ "* Status: completed"
    assert File.exists?(Path.join(run_dir, "events.jsonl"))
    assert File.read!(Path.join(run_dir, "run-summary.org")) =~ "* Status: completed"
    assert File.read!(Path.join(run_dir, "run-context.org")) =~ "- item_a :: DONE"
  end

  test "run journals each event before asynchronous observation completes" do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> Map.update!("agents", &[List.first(&1)])

    run_dir =
      "durable-run"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    parent = self()

    opts =
      fsm_opts()
      |> Keyword.put(:dispatch, AiOrchestrator.CLITest.BlockingDispatch)
      |> Keyword.put(:dispatch_opts, test_pid: parent)

    task = Task.async(fn -> CLI.run(["run", run_dir], opts) end)

    assert_receive {:observation_waiting, observer, "as_0001"}, 5_000

    prefix = File.read!(Path.join(run_dir, "events.jsonl"))
    assert prefix =~ ~s("type":"run_created")
    assert prefix =~ ~s("type":"assignment_dispatch_sent")
    assert prefix =~ ~s("type":"assignment_observation_started")
    refute prefix =~ ~s("type":"run_completed")

    send(observer, {:release_observation, "as_0001"})

    assert %{status: 0, stdout: stdout, stderr: ""} = Task.await(task, 5_000)
    assert stdout =~ "* Status: completed"

    events =
      run_dir
      |> Path.join("events.jsonl")
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)

    assert Enum.map(events, & &1["seq"]) == Enum.to_list(1..length(events))
    assert List.last(events)["type"] == "run_completed"
  end

  test "overlapping run processes cannot claim the same pane", %{test: _test} do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> Map.update!("agents", &[List.first(&1)])

    first_dir =
      "pane-owner-a"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    second_dir =
      "pane-owner-b"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    registry_root = tmp_dir("shared-pane-registry")
    parent = self()

    first_opts =
      registry_root
      |> fsm_opts()
      |> Keyword.put(:dispatch, AiOrchestrator.CLITest.BlockingDispatch)
      |> Keyword.put(:dispatch_opts, test_pid: parent)

    first = Task.async(fn -> CLI.run(["run", first_dir], first_opts) end)
    assert_receive {:observation_waiting, observer, "as_0001"}, 5_000

    journal_before = File.read!(Path.join(first_dir, "events.jsonl"))
    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", second_dir], fsm_opts(registry_root))
    assert %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_writer"} = Jason.decode!(stderr)
    refute File.exists?(Path.join(second_dir, "events.jsonl"))
    assert File.read!(Path.join(first_dir, "events.jsonl")) == journal_before

    send(observer, {:release_observation, "as_0001"})
    assert %{status: 0} = Task.await(first, 5_000)
  end

  test "resume claim rejection leaves the existing journal byte-identical" do
    run_dir =
      "resume-pane-owner"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "kill9_resume", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "kill9_resume", "plan.json"))

    prior_lines = "events_pre_gate.jsonl" |> kill9_lines() |> reanchor_provenance(run_dir)
    write_journal(run_dir, prior_lines)
    before = File.read!(Path.join(run_dir, "events.jsonl"))
    registry_root = tmp_dir("resume-pane-registry")
    pane_refs = FileRegistry.pane_refs(F.json("scenarios", "kill9_resume", "spec.json"))

    assert {:ok, claim} =
             FileRegistry.claim(
               pane_refs,
               %{
                 "run_id" => "run_other",
                 "run_dir" => "/tmp/other",
                 "supervisor_instance" => "sup_other"
               },
               root: registry_root
             )

    assert %{status: 70, stdout: "", stderr: stderr} =
             CLI.run(["run", "--resume", run_dir], fsm_opts(registry_root))

    assert %{"reason" => "pane_claim_rejected"} = Jason.decode!(stderr)
    assert File.read!(Path.join(run_dir, "events.jsonl")) == before
    assert :ok = FileRegistry.release(claim)
  end

  test "successful run releases its process-wide roster claims" do
    run_dir =
      "claim-release"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    registry_root = tmp_dir("claim-release-registry")
    assert %{status: 0} = CLI.run(["run", run_dir], fsm_opts(registry_root))
    assert Path.wildcard(Path.join(registry_root, "pane-*.json")) == []

    acquired =
      run_dir
      |> Path.join("events.jsonl")
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)
      |> Enum.filter(&(&1["type"] == "pane_lease_acquired"))

    assert acquired != []
    assert Enum.all?(acquired, &(&1["data"]["claim_token"] =~ ~r/^[0-9a-f]{24}$/))
  end

  test "failed run releases its process-wide roster claims" do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> put_in(["budgets", "max_attempts_default"], 1)

    run_dir =
      "failed-claim-release"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    registry_root = tmp_dir("failed-claim-release-registry")

    failed_gate = fn _gate, _opts ->
      {:failed,
       %{
         "exit_status" => 1,
         "duration_ms" => 1,
         "stdout_hash" => LocalPane.zero_hash(),
         "stderr_hash" => LocalPane.zero_hash(),
         "stderr_merged" => true,
         "failure_summary" => %{
           "headline" => "test failure",
           "failures" => [%{"line" => "failed"}],
           "suggestion" => "fix it"
         }
       }}
    end

    assert %{status: 70, stderr: stderr} =
             CLI.run(["run", run_dir], Keyword.put(fsm_opts(registry_root), :gate_opts, runner: failed_gate))

    assert stderr =~ "gate_failed"
    assert Path.wildcard(Path.join(registry_root, "pane-*.json")) == []
  end

  test "unexpected runtime exception still releases roster claims" do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> Map.update!("agents", &[List.first(&1)])

    run_dir =
      "raising-claim-release"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    registry_root = tmp_dir("raising-claim-release-registry")
    opts = Keyword.put(fsm_opts(registry_root), :dispatch, AiOrchestrator.CLITest.RaisingDispatch)

    # NS-43 migration: the adapter's exception is closed inside the owned run subtree (run_server_down) instead of
    # propagating through the CLI process; the roster claims are released either way
    assert %{status: 70, stderr: stderr} = CLI.run(["run", run_dir], opts)
    assert %{"reason" => "run_server_down"} = Jason.decode!(stderr)
    refute stderr =~ "dispatch exploded", "the adapter's message is not reflected"
    assert Path.wildcard(Path.join(registry_root, "pane-*.json")) == []
  end

  test "blocked run releases its process-wide roster claims" do
    spec =
      "scenarios"
      |> F.json("gated_run_seed", "spec.json")
      |> Map.update!("agents", &[List.first(&1)])

    run_dir =
      "blocked-claim-release"
      |> tmp_dir()
      |> write_json("spec.json", spec)
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    registry_root = tmp_dir("blocked-claim-release-registry")
    opts = Keyword.put(fsm_opts(registry_root), :dispatch, AiOrchestrator.CLITest.BlockedDispatch)

    assert %{status: 0, stdout: stdout} = CLI.run(["run", run_dir], opts)
    assert stdout =~ "* Status: BLOCKED"
    assert Path.wildcard(Path.join(registry_root, "pane-*.json")) == []
  end

  test "release failure is loud even when the journal reached terminal state" do
    run_dir =
      "claim-release-failure"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

    opts =
      "unused-release-failure-registry"
      |> tmp_dir()
      |> fsm_opts()
      |> Keyword.put(:pane_registry, AiOrchestrator.CLITest.ReleaseFailRegistry)

    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", run_dir], opts)
    assert %{"reason" => "pane_claim_release_failed"} = Jason.decode!(stderr)
    assert File.read!(Path.join(run_dir, "events.jsonl")) =~ ~s("type":"run_completed")
    assert %{status: 0, stdout: status} = CLI.run(["status", run_dir])
    assert status =~ "* Status: completed"
  end

  test "run --resume appends the recovered suffix and rewrites projections" do
    run_dir =
      "resume"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "kill9_resume", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "kill9_resume", "plan.json"))

    prior_lines = "events_pre_gate.jsonl" |> kill9_lines() |> reanchor_provenance(run_dir)
    write_journal(run_dir, prior_lines)

    # the prior holds a legacy (version 1) gate_started with no result: the resumed owner cannot know
    # whether that gate still runs, so the recovered suffix ends in attention, never a silent rerun
    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["run", "--resume", run_dir], fsm_opts())
    assert stdout =~ "* Status: BLOCKED"

    journal = File.read!(Path.join(run_dir, "events.jsonl"))
    assert length(String.split(journal, "\n", trim: true)) > length(prior_lines)
    assert journal =~ ~s("type":"run_resumed")
    assert journal =~ ~s("reason":"gate_start_unresolved")
    assert File.read!(Path.join(run_dir, "run-summary.org")) =~ "* Status: BLOCKED"
  end

  test "run --resume restarts a journal killed before its first event" do
    run_dir =
      "empty-resume"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))
      |> write_file("events.jsonl", "")

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["run", "--resume", run_dir], fsm_opts())
    assert stdout =~ "* Status: completed"

    [first | _rest] =
      run_dir
      |> Path.join("events.jsonl")
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!/1)

    assert %{"seq" => 1, "type" => "run_created"} = first
  end

  # CD-M2: the real preflight-empty -> locked-nonempty race. The CLI preflight reads through the plain filesystem
  # and sees an EMPTY journal (so it requests the explicit restart); only the Writer runs through the injected
  # FaultFs, whose hook on the lock tempfile open (the Writer's first write-side operation, strictly before
  # acquisition) plants a Writer-valid nonempty prefix. The locked verification must then refuse: no append, no
  # effect, planted bytes preserved.
  test "run --resume: a journal that fills between preflight and the lock is refused as journal_exists, nothing appended" do
    planted = kill9_lines("events_pre_dispatch.jsonl")
    planted_bytes = Enum.join(planted, "\n") <> "\n"

    run_dir =
      "empty-resume-race"
      |> tmp_dir()
      |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
      |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))
      |> write_file("events.jsonl", "")

    journal = Path.join(run_dir, "events.jsonl")
    parent = self()
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :open,
      fn
        ["run.lock." <> _, _modes] -> true
        _ -> false
      end,
      {:hook,
       fn ->
         # first lock attempt only: the journal is still empty when the hook fires (preflight already saw it empty)
         if File.read!(journal) == "" do
           File.write!(journal, planted_bytes)
           send(parent, :planted_before_lock)
         end

         true
       end}
    )

    assert %{status: 70, stdout: "", stderr: stderr} =
             CLI.run(["run", "--resume", run_dir], Keyword.put(fsm_opts(), :fs, fs))

    assert_received :planted_before_lock
    assert stderr =~ "journal_exists"
    assert File.read!(journal) == planted_bytes, "the planted accepted lines are exact; no command event appended"
    refute File.exists?(Path.join(run_dir, "run-summary.org"))
  end

  test "cancel appends cancellation events and rewrites projections" do
    run_dir = "cancel" |> tmp_dir() |> write_journal(kill9_lines("events_pre_dispatch.jsonl"))

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["cancel", run_dir])
    assert stdout =~ "* Status: cancelled"
    assert File.read!(Path.join(run_dir, "events.jsonl")) =~ ~s("type":"run_cancelled")
    assert File.read!(Path.join(run_dir, "run-summary.org")) =~ "* Status: cancelled"
  end

  test "status --json returns the folded journal summary" do
    run_dir = write_journal(tmp_dir("completed"), F.lines("scenarios", "gated_run_seed"))

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["status", "--json", run_dir])
    assert Jason.decode!(stdout) == F.json("scenarios", "gated_run_seed", "expected.json")
  end

  test "status renders the org projection by default" do
    run_dir = write_journal(tmp_dir("blocked"), F.lines("scenarios", "auth_blocked_pane"))

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["status", run_dir])
    assert stdout =~ "#+title: Run summary"
    assert stdout =~ "* Status: BLOCKED"
    assert stdout =~ "** TODO resolve att_0001"
  end

  test "status reports a bad journal without a successful summary" do
    run_dir = "bad" |> tmp_dir() |> write_file("events.jsonl", "not-json\n")

    assert %{status: 66, stdout: "", stderr: stderr} = CLI.run(["status", "--json", run_dir])
    assert Jason.decode!(stderr) == %{"reason" => "journal_undecodable_line", "at_seq" => 1, "file" => "events.jsonl"}
  end

  test "list --json reports valid and invalid run directories without aborting the list" do
    project_dir = tmp_dir("project")
    runs_root = Path.join([project_dir, ".ai-orchestrator", "runs"])

    runs_root |> Path.join("complete") |> write_journal(F.lines("scenarios", "gated_run_seed"))
    runs_root |> Path.join("blocked") |> write_journal(F.lines("scenarios", "auth_blocked_pane"))
    runs_root |> Path.join("invalid") |> write_file("events.jsonl", "")

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["list", "--json"], cwd: project_dir)

    entries = Jason.decode!(stdout)

    assert Enum.map(entries, & &1["run_ref"]) == ["blocked", "complete", "invalid"]
    assert %{"status" => "blocked", "run_id" => "run_scenario_0002"} = Enum.find(entries, &(&1["run_ref"] == "blocked"))

    assert %{"status" => "completed", "run_id" => "run_scenario_0001"} =
             Enum.find(entries, &(&1["run_ref"] == "complete"))

    assert %{"status" => "invalid", "error" => %{"reason" => "journal_empty"}} =
             Enum.find(entries, &(&1["run_ref"] == "invalid"))
  end

  test "list renders an org table by default" do
    project_dir = tmp_dir("project-list")
    runs_root = Path.join([project_dir, ".ai-orchestrator", "runs"])
    runs_root |> Path.join("complete") |> write_journal(F.lines("scenarios", "gated_run_seed"))

    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["list"], cwd: project_dir)
    assert stdout =~ "#+title: Runs"
    assert stdout =~ "| complete | run_scenario_0001 | completed | 32 |"
  end

  test "list is empty when the project has no runs root yet" do
    assert CLI.run(["list"], cwd: tmp_dir("empty")) == %{
             status: 0,
             stdout: "#+title: Runs\n\n* Runs\n- none\n",
             stderr: ""
           }
  end

  describe "journal durability" do
    test "run leaves a chained journal, a matching receipt, and no lock" do
      run_dir =
        "durable-chain"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

      assert %{status: 0} = CLI.run(["run", run_dir], fsm_opts())
      bytes = File.read!(Path.join(run_dir, "events.jsonl"))
      assert {:ok, %{count: count, envelope_version: 2, version_2_from: 1, last_line_sha256: last}} = Chain.verify(bytes)
      assert count > 4
      assert {:ok, %{seq: ^count, line_sha256: ^last}} = dir_receipt(run_dir)
      assert :none = RunLock.owner(SystemFs.new(), run_dir)
      refute File.exists?(Path.join(run_dir, "events.head.tmp"))
    end

    test "a held run lock refuses run, resume, and cancel by name and changes nothing" do
      run_dir =
        "durable-locked"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "kill9_resume", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "kill9_resume", "plan.json"))

      fs = SystemFs.new()
      {:ok, held} = RunLock.acquire(fs, run_dir, supervisor_instance: "sup_holder")

      assert %{status: 70, stderr: stderr} = CLI.run(["run", run_dir], fsm_opts())

      assert %{"reason" => "journal_run_locked", "owner" => %{"supervisor_instance" => "sup_holder"}} =
               Jason.decode!(stderr)

      refute File.exists?(Path.join(run_dir, "events.jsonl"))

      prior_lines = "events_pre_gate.jsonl" |> kill9_lines() |> reanchor_provenance(run_dir)
      write_journal(run_dir, prior_lines)
      before = File.read!(Path.join(run_dir, "events.jsonl"))

      assert %{status: 70, stderr: stderr} = CLI.run(["run", "--resume", run_dir], fsm_opts())
      assert %{"reason" => "journal_run_locked"} = Jason.decode!(stderr)
      assert %{status: 70, stderr: stderr} = CLI.run(["cancel", run_dir], fsm_opts())
      assert %{"reason" => "journal_run_locked"} = Jason.decode!(stderr)
      assert File.read!(Path.join(run_dir, "events.jsonl")) == before

      :ok = RunLock.release(fs, held)
      assert %{status: 0} = CLI.run(["cancel", run_dir], fsm_opts())
    end

    test "resume repairs a torn tail, journals the repair, and upgrades the legacy journal" do
      run_dir =
        "durable-torn-resume"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "kill9_resume", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "kill9_resume", "plan.json"))

      prior_lines = "events_pre_gate.jsonl" |> kill9_lines() |> reanchor_provenance(run_dir)
      write_journal(run_dir, prior_lines)
      journal = Path.join(run_dir, "events.jsonl")
      File.write!(journal, File.read!(journal) <> ~s({"schema":"ai-orch))
      n = length(prior_lines)

      # legacy (version 1) gate_started in the prior: the resumed run ends in gate_start_unresolved attention
      assert %{status: 0, stdout: stdout} = CLI.run(["run", "--resume", run_dir], fsm_opts())
      assert stdout =~ "* Status: BLOCKED"

      events = journal |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
      resumed = Enum.find(events, &(&1["type"] == "run_resumed"))

      assert %{"action" => "truncate_tail", "truncated_bytes" => 18, "receipt_seq_before" => 0, "receipt_seq_after" => 0} =
               resumed["data"]["tail_repair"]

      assert {:ok, %{count: count, envelope_version: 2, version_2_from: from}} = Chain.verify(File.read!(journal))
      assert from == n + 1
      assert {:ok, %{seq: ^count}} = dir_receipt(run_dir)
    end

    test "cancel journals the repair on its first event" do
      run_dir = "durable-torn-cancel" |> tmp_dir() |> write_journal(kill9_lines("events_pre_dispatch.jsonl"))
      journal = Path.join(run_dir, "events.jsonl")
      File.write!(journal, File.read!(journal) <> "{\"x")

      assert %{status: 0} = CLI.run(["cancel", run_dir], fsm_opts())
      events = journal |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
      requested = Enum.find(events, &(&1["type"] == "run_cancel_requested"))
      assert %{"action" => "truncate_tail", "truncated_bytes" => 3} = requested["data"]["tail_repair"]
      assert {:ok, %{envelope_version: 2}} = Chain.verify(File.read!(journal))
    end

    test "a close failure after a successful run is surfaced, never hidden" do
      run_dir =
        "durable-close-failure"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

      fs = FaultFs.new()

      FaultFs.inject(
        fs,
        :link,
        fn
          [_tmp, "run.lock.2"] -> true
          _ -> false
        end,
        {:error, :eacces}
      )

      assert %{status: 70, stdout: stdout, stderr: stderr} = CLI.run(["run", run_dir], Keyword.put(fsm_opts(), :fs, fs))
      assert stdout =~ "* Status: completed"
      assert %{"reason" => "journal_close_failed", "failures" => [%{"leg" => "lock"}]} = Jason.decode!(stderr)
      assert File.read!(Path.join(run_dir, "events.jsonl")) =~ ~s("type":"run_completed")
      assert {:ok, %{"state" => "held"}} = RunLock.owner(SystemFs.new(), run_dir)
      assert %{status: 0} = CLI.run(["status", "--json", run_dir])
    end

    test "a dead lower generation that cannot be compacted is surfaced as journal_cleanup_required" do
      run_dir =
        "durable-cleanup-required"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

      dead = %{
        "schema" => "ai-orchestrator/run-lock",
        "schema_version" => 1,
        "state" => "held",
        "pid" => "4000000",
        "pid_start" => "gone",
        "supervisor_instance" => "sup_dead",
        "token" => "token_dead",
        "acquired_at" => "2026-09-01T12:00:00Z"
      }

      File.write!(Path.join(run_dir, "run.lock.1"), Jason.encode!(dead) <> "\n")
      fs = FaultFs.new()

      FaultFs.inject(
        fs,
        :rm,
        fn
          ["run.lock.1"] -> true
          _ -> false
        end,
        {:error, :eacces}
      )

      assert %{status: 70, stderr: stderr} = CLI.run(["run", run_dir], Keyword.put(fsm_opts(), :fs, fs))

      assert %{"reason" => "journal_cleanup_required", "path" => path, "token" => "token_dead", "owner" => owner} =
               Jason.decode!(stderr)

      assert Path.basename(path) == "run.lock.1"
      assert owner == dead
      refute File.exists?(Path.join(run_dir, "events.jsonl"))
    end

    test "a failed rollback release after a failed journal creation is visible by name" do
      run_dir =
        "durable-open-cleanup"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

      fs = FaultFs.new()

      FaultFs.inject(
        fs,
        :open,
        fn
          ["events.jsonl", _modes] -> true
          _ -> false
        end,
        {:error, :eacces}
      )

      FaultFs.inject(
        fs,
        :link,
        fn
          [_tmp, "run.lock.2"] -> true
          _ -> false
        end,
        {:error, :eacces}
      )

      assert %{status: 70, stderr: stderr} = CLI.run(["run", run_dir], Keyword.put(fsm_opts(), :fs, fs))

      assert %{
               "reason" => "journal_writer_open_cleanup_failed",
               "cause" => %{"clause" => "journal_create_failed"},
               "release" => %{"clause" => "release_failed"},
               "lock_path" => "run.lock.1"
             } = Jason.decode!(stderr)

      assert {:ok, %{"state" => "held"}} = RunLock.owner(SystemFs.new(), run_dir)
    end

    test "status fails closed on a corrupted chain" do
      run_dir =
        "durable-corrupt"
        |> tmp_dir()
        |> write_json("spec.json", F.json("scenarios", "gated_run_seed", "spec.json"))
        |> write_json("plan.json", F.json("scenarios", "gated_run_seed", "plan.json"))

      assert %{status: 0} = CLI.run(["run", run_dir], fsm_opts())
      journal = Path.join(run_dir, "events.jsonl")
      [first | rest] = journal |> File.read!() |> String.split("\n", trim: true)
      flipped = first |> Jason.decode!() |> put_in(["data", "project"], "flipped") |> Jason.encode!()
      File.write!(journal, Enum.join([flipped | rest], "\n") <> "\n")

      assert %{status: 66, stderr: stderr} = CLI.run(["status", "--json", run_dir])
      assert %{"reason" => "journal_chain_mismatch", "at_seq" => 2} = Jason.decode!(stderr)
      assert %{status: 70, stderr: stderr} = CLI.run(["cancel", run_dir], fsm_opts())
      assert %{"reason" => "journal_chain_mismatch"} = Jason.decode!(stderr)
    end
  end

  defp dir_receipt(run_dir), do: run_dir |> Path.join("events.head") |> File.read!() |> Chain.decode_receipt()

  defp tmp_dir(name) do
    path = Path.join(System.tmp_dir!(), "ai_orchestrator_cli_#{name}_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp write_json(dir, file, data), do: write_file(dir, file, Jason.encode!(data))

  defp write_journal(dir, lines), do: write_file(dir, "events.jsonl", Enum.join(lines, "\n") <> "\n")

  defp write_file(dir, file, contents) do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, file), contents)
    dir
  end

  defp kill9_lines(file) do
    [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", file]
    |> Path.join()
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  defp fsm_opts(registry_root \\ nil) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")

    artifact_reader = fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end

    gate_runner = fn _gate, _gate_opts -> {:ok, gate_pass} end

    registry_root =
      registry_root ||
        Path.join(System.tmp_dir!(), "ai_orchestrator_cli_registry_#{System.unique_integer([:positive])}")

    [
      pane_registry_root: registry_root,
      dispatch: LocalPane,
      dispatch_opts: [
        artifact_reader: artifact_reader,
        pane_client: AiOrchestrator.CLITest.FakePaneClient,
        test_pid: self()
      ],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: gate_runner],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

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

    def status(pane_ref, _opts) do
      {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
    end
  end

  defmodule BlockingDispatch do
    @moduledoc false

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

    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    def observe(command, opts) do
      parent = Keyword.fetch!(opts, :test_pid)
      assignment_id = command["assignment_id"]
      send(parent, {:observation_waiting, self(), assignment_id})

      receive do
        {:release_observation, ^assignment_id} ->
          {:ok,
           %{
             "assignment_id" => assignment_id,
             "artifact_id" => command["artifact_id"],
             "path" => command["expected_artifact"],
             "match_kind" => "exact",
             "bytes" => 5,
             "sha256" => LocalPane.zero_hash(),
             "stable_for_ms" => 5_000,
             "modified_after_dispatch" => true
           }}
      end
    end
  end

  defmodule RaisingDispatch do
    @moduledoc false

    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    def deliver(_command, _opts), do: raise("dispatch exploded")
    def observe(_command, _opts), do: raise("unreachable")
  end

  defmodule BlockedDispatch do
    @moduledoc false

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

    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    def observe(command, _opts) do
      {:blocked,
       %{
         "reason" => "agent_auth_blocked",
         "pane_ref" => command["pane_ref"],
         "pane_state" => "blocked",
         "pending_count" => 1
       }}
    end
  end

  defmodule ReleaseFailRegistry do
    @moduledoc false

    def pane_refs(spec), do: FileRegistry.pane_refs(spec)

    def claim(pane_refs, _owner, opts) do
      {:ok, %{root: Keyword.fetch!(opts, :root), token: "release_failure_token", pane_refs: pane_refs}}
    end

    def release(_claim), do: {:error, %{"reason" => "pane_claim_release_failed"}}
  end

  defp fixture_data(events, type) do
    events
    |> Enum.find(&(&1["type"] == type))
    |> Map.fetch!("data")
  end

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(fn event ->
      data = Map.fetch!(event, "data")
      {Map.fetch!(data, "assignment_id"), data}
    end)
  end

  defp reanchor_provenance(lines, run_dir) do
    hash = fn file ->
      digest = :crypto.hash(:sha256, File.read!(Path.join(run_dir, file)))
      "sha256:" <> Base.encode16(digest, case: :lower)
    end

    spec_hash = hash.("spec.json")
    plan_hash = hash.("plan.json")

    Enum.map(lines, fn line ->
      event = Jason.decode!(line)

      updated =
        case event do
          %{"type" => type, "data" => data} when type in ["run_created", "run_spec_loaded"] ->
            Map.put(event, "data", Map.put(data, "spec_hash", spec_hash))

          %{"type" => "plan_recorded", "data" => data} ->
            Map.put(event, "data", Map.put(data, "plan_hash", plan_hash))

          other ->
            other
        end

      Jason.encode!(updated)
    end)
  end
end
