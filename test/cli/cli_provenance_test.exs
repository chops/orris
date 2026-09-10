defmodule AiOrchestrator.CLIProvenanceTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Test.GateDouble

  defmodule StubDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, opts) do
      if pid = Keyword.get(opts, :test_pid), do: send(pid, {:delivered, command["assignment_id"]})

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
    def observe(command, _opts) do
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
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_cli_provenance_test",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  defp sha256(bytes), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp seed_run_dir(tmp_dir, name) do
    run_dir = Path.join(tmp_dir, name)
    File.mkdir_p!(run_dir)
    spec = F.json("scenarios", "gated_run_seed", "spec.json")
    plan = F.json("scenarios", "gated_run_seed", "plan.json")
    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(spec, pretty: true))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(plan, pretty: true))
    run_dir
  end

  defp stub_gate(_gate) do
    {:ok,
     %{
       "exit_status" => 0,
       "duration_ms" => 100,
       "stdout_hash" => LocalPane.zero_hash(),
       "stderr_hash" => LocalPane.zero_hash()
     }}
  end

  defp cli_opts do
    [
      pane_registry_root:
        Path.join(System.tmp_dir!(), "ai_orchestrator_provenance_registry_#{System.unique_integer([:positive])}"),
      dispatch: StubDispatch,
      dispatch_opts: [test_pid: self()],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn gate, _opts -> stub_gate(gate) end],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end,
      env: %{}
    ]
  end

  defp journal_events(run_dir) do
    run_dir
    |> Path.join("events.jsonl")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
  end

  # MUST-4: the journaled hashes equal the exact on-disk bytes the CLI executed.
  test "CLI journals content hashes of the exact input bytes", %{tmp_dir: tmp_dir} do
    run_dir = seed_run_dir(tmp_dir, "byte-truth")
    spec_hash = run_dir |> Path.join("spec.json") |> File.read!() |> sha256()
    plan_hash = run_dir |> Path.join("plan.json") |> File.read!() |> sha256()

    assert %{status: 0} = CLI.run(["run", run_dir], cli_opts())

    [created, spec_loaded, plan_recorded | _rest] = journal_events(run_dir)
    assert created["data"]["spec_hash"] == spec_hash
    assert spec_loaded["data"]["spec_hash"] == spec_hash
    assert plan_recorded["data"]["plan_hash"] == plan_hash
  end

  test "formatting-only byte change yields a different hash (content addressing)", %{tmp_dir: tmp_dir} do
    run_dir_a = seed_run_dir(tmp_dir, "fmt-a")
    run_dir_b = seed_run_dir(tmp_dir, "fmt-b")
    spec_path = Path.join(run_dir_b, "spec.json")
    File.write!(spec_path, File.read!(spec_path) <> "\n")

    assert %{status: 0} = CLI.run(["run", run_dir_a], cli_opts())
    assert %{status: 0} = CLI.run(["run", run_dir_b], cli_opts())

    [created_a | _] = journal_events(run_dir_a)
    [created_b | _] = journal_events(run_dir_b)
    refute created_a["data"]["spec_hash"] == created_b["data"]["spec_hash"]
  end

  # MUST-2: resume refuses drifted inputs with zero append and zero dispatch.
  for file <- ["spec.json", "plan.json"] do
    test "resume refuses a mutated #{file} with no append and no dispatch", %{tmp_dir: tmp_dir} do
      file = unquote(file)
      run_dir = seed_run_dir(tmp_dir, "drift-#{file}")

      prior_lines =
        "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
        |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
        |> File.read!()
        |> String.split("\n", trim: true)
        |> reanchor_hashes(run_dir)

      journal_path = Path.join(run_dir, "events.jsonl")
      File.write!(journal_path, Enum.join(prior_lines, "\n") <> "\n")

      target = Path.join(run_dir, file)
      File.write!(target, File.read!(target) <> "\n")
      journal_before = File.read!(journal_path)

      result = CLI.run(["run", "--resume", run_dir], cli_opts())

      assert result.status != 0
      assert result.stderr =~ "input_provenance_mismatch"
      assert result.stderr =~ file
      assert File.read!(journal_path) == journal_before
      refute_receive {:delivered, _assignment}, 50
    end
  end

  # MUST-5: malformed journal lines produce a controlled rejection, never a raise.
  test "resume with malformed journal JSON rejects without raising, appending, or dispatching",
       %{tmp_dir: tmp_dir} do
    run_dir = seed_run_dir(tmp_dir, "malformed-journal")
    journal_path = Path.join(run_dir, "events.jsonl")
    File.write!(journal_path, "{not-json}\n")
    journal_before = File.read!(journal_path)

    result = CLI.run(["run", "--resume", run_dir], cli_opts())

    assert result.status != 0
    assert result.stderr =~ "journal_undecodable_line"
    assert File.read!(journal_path) == journal_before
    refute_receive {:delivered, _assignment}, 50
  end

  # MUST-6: conflicting recorded spec hashes refuse before any side effect.
  test "resume refuses conflicting preamble spec hashes", %{tmp_dir: tmp_dir} do
    run_dir = seed_run_dir(tmp_dir, "conflicting-preamble")

    prior_lines =
      "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
      |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
      |> File.read!()
      |> String.split("\n", trim: true)
      |> reanchor_hashes(run_dir)
      |> corrupt_hash("run_spec_loaded", "spec_hash", "sha256:" <> String.duplicate("ab", 32))

    journal_path = Path.join(run_dir, "events.jsonl")
    File.write!(journal_path, Enum.join(prior_lines, "\n") <> "\n")
    journal_before = File.read!(journal_path)

    result = CLI.run(["run", "--resume", run_dir], cli_opts())

    assert result.status != 0
    assert result.stderr =~ "journal_provenance_conflict"
    assert File.read!(journal_path) == journal_before
    refute_receive {:delivered, _assignment}, 50
  end

  # MUST-6: a preamble event missing its hash slot is incomplete, not silently accepted.
  test "resume refuses a plan_recorded event missing its plan hash", %{tmp_dir: tmp_dir} do
    run_dir = seed_run_dir(tmp_dir, "missing-plan-hash")

    prior_lines =
      "scenarios/kill9_resume/events_awaiting_artifact.jsonl"
      |> then(&Path.join([__DIR__, "..", "fixtures", "contracts", &1]))
      |> File.read!()
      |> String.split("\n", trim: true)
      |> reanchor_hashes(run_dir)
      |> drop_field("plan_recorded", "plan_hash")

    journal_path = Path.join(run_dir, "events.jsonl")
    File.write!(journal_path, Enum.join(prior_lines, "\n") <> "\n")
    journal_before = File.read!(journal_path)

    result = CLI.run(["run", "--resume", run_dir], cli_opts())

    assert result.status != 0
    assert result.stderr =~ "journal_provenance_incomplete"
    assert File.read!(journal_path) == journal_before
    refute_receive {:delivered, _assignment}, 50
  end

  defp corrupt_hash(lines, event_type, field, value) do
    map_event(lines, event_type, &Map.put(&1, field, value))
  end

  defp drop_field(lines, event_type, field) do
    map_event(lines, event_type, &Map.delete(&1, field))
  end

  defp map_event(lines, event_type, fun) do
    Enum.map(lines, fn line ->
      event = Jason.decode!(line)

      case event do
        %{"type" => ^event_type, "data" => data} -> Jason.encode!(Map.put(event, "data", fun.(data)))
        _other -> line
      end
    end)
  end

  defp reanchor_hashes(lines, run_dir) do
    spec_hash = run_dir |> Path.join("spec.json") |> File.read!() |> sha256()
    plan_hash = run_dir |> Path.join("plan.json") |> File.read!() |> sha256()

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
