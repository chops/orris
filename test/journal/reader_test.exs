defmodule AiOrchestrator.Journal.ReaderTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FixedClock

  setup do
    dir = Path.join(System.tmp_dir!(), "reader_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    FixedClock.reset()
    %{dir: dir}
  end

  @created_data %{
    "project" => "example",
    "repo_root" => "/tmp/example-repo",
    "run_dir" => "/tmp/example-run",
    "operator" => "operator",
    "spec_path" => "spec.json",
    "spec_hash" => "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  }
  @loaded_data %{
    "spec_path" => "spec.json",
    "spec_hash" => "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  }

  defp event(seq, type, data) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 1,
      "seq" => seq,
      "event_id" => "ev_#{seq}",
      "type" => type,
      "ts" => "2026-09-03T00:00:0#{seq}Z",
      "run_id" => "run_fixture_0001",
      "actor" => "run_supervisor",
      "data" => data
    }
  end

  defp written(dir) do
    lock = [supervisor_instance: "sup_0001", pid: "1", pid_start: "s", owner_status: fn _ -> :live end]
    {:ok, w, _} = Writer.open(dir, clock: FixedClock, lock: lock, create: true)
    {:ok, _} = Writer.append(w, event(1, "run_created", @created_data))
    {:ok, _} = Writer.append(w, event(2, "run_spec_loaded", @loaded_data))
    :ok = Writer.close(w)
    File.read!(Path.join(dir, "events.jsonl"))
  end

  test "a missing journal and an empty journal are distinguished", %{dir: dir} do
    assert {:error, %{clause: "journal_missing"}} = Reader.load(dir)
    File.write!(Path.join(dir, "events.jsonl"), "")
    assert {:ok, %{lines: [], last_seq: 0, envelope_version: 0, pending_repair: nil}} = Reader.load(dir)
  end

  test "a chained journal loads with its receipt and never writes", %{dir: dir} do
    bytes = written(dir)

    assert {:ok, %{lines: lines, last_seq: 2, envelope_version: 2, receipt: %{seq: 2}, pending_repair: nil}} =
             Reader.load(dir)

    assert Enum.join(lines, "\n") <> "\n" == bytes
    assert File.read!(Path.join(dir, "events.jsonl")) == bytes
  end

  test "a torn tail is reported as a pending repair and left on disk", %{dir: dir} do
    bytes = written(dir)
    File.write!(Path.join(dir, "events.jsonl"), bytes <> "{\"partial")
    assert {:ok, %{last_seq: 2, pending_repair: %{action: :truncate_tail, truncate_bytes: 9}}} = Reader.load(dir)
    assert File.read!(Path.join(dir, "events.jsonl")) == bytes <> "{\"partial"
  end

  test "chain and receipt violations fail closed by name", %{dir: dir} do
    bytes = written(dir)
    corrupted = String.replace(bytes, ~s("project":"example"), ~s("project":"exampls"), global: false)
    File.write!(Path.join(dir, "events.jsonl"), corrupted)
    assert {:error, %{clause: "chain_mismatch", at_seq: 2}} = Reader.load(dir)

    File.write!(Path.join(dir, "events.jsonl"), bytes)
    {:ok, receipt} = dir |> Path.join("events.head") |> File.read!() |> Chain.decode_receipt()
    File.write!(Path.join(dir, "events.head"), Chain.encode_receipt(%{receipt | seq: 3}))
    assert {:error, %{clause: "receipt_beyond_tail", receipt_seq: 3}} = Reader.load(dir)
  end

  test "every complete line is validated before a repair is even planned", %{dir: dir} do
    legacy_bad = """
    {"schema":"ai-orchestrator/journal-event","schema_version":1,"event_version":1,"seq":1,"event_id":"ev_1","type":"run_created","ts":"2026-09-03T00:00:01Z","run_id":"run_fixture_0001","actor":"run_supervisor","data":{"project":"example"}}
    {"partial\
    """

    File.write!(Path.join(dir, "events.jsonl"), String.trim_trailing(legacy_bad, "\n"))

    assert {:error,
            %{clause: "invalid_event_data", at_seq: 1, event_type: "run_created", reason: "journal_provenance_incomplete"}} =
             Reader.load(dir)
  end

  test "the seam is injectable", %{dir: dir} do
    File.write!(Path.join(dir, "events.jsonl"), "")
    assert {:ok, %{lines: []}} = Reader.load(dir, fs: SystemFs.new())
  end
end
