defmodule WriterSnapshotReviewControlsTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  defp event do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "event_version" => 1,
      "event_id" => "ev_0001",
      "run_id" => "run_snapshot_0001",
      "seq" => 1,
      "ts" => "2026-09-06T14:00:01Z",
      "actor" => "run_supervisor",
      "type" => "run_created",
      "data" => %{
        "project" => "example",
        "repo_root" => "/tmp/example",
        "run_dir" => "/tmp/example",
        "operator" => "operator",
        "spec_path" => "spec.json",
        "spec_hash" => "sha256:" <> String.duplicate("0", 64)
      }
    }
  end

  test "next sync after a completed append is journal sync, not receipt sync" do
    dir = Path.join(System.tmp_dir!(), "snapshot_control_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    fs = FaultFs.new()

    {:ok, writer, _} =
      Writer.open(dir,
        fs: fs,
        clock: FixedClock,
        lock: [supervisor_instance: "sup_snap", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
      )

    try do
      assert {:ok, _} = Writer.append(writer, event())
      before = FaultFs.trace(fs)
      nth = Enum.count(before, &(elem(&1, 0) == :sync)) + 1
      FaultFs.inject(fs, :sync, nth, {:error, :review_fault})

      next = %{
        event()
        | "seq" => 2,
          "event_id" => "ev_0002",
          "type" => "run_spec_loaded",
          "data" => %{"spec_path" => "spec.json", "spec_hash" => "sha256:" <> String.duplicate("0", 64)}
      }

      assert {:error, %{clause: "append_failed", stage: "sync"}} = Writer.append(writer, next)
      delta = Enum.drop(FaultFs.trace(fs), length(before))
      assert Enum.map(delta, &elem(&1, 0)) == [:write, :sync]
      refute Enum.any?(delta, &match?({:open, "events.head.tmp", _}, &1))
    after
      monitor = Process.monitor(writer)
      Writer.close(writer)
      assert_receive {:DOWN, ^monitor, :process, ^writer, _}, 5_000
      File.rm_rf!(dir)
    end
  end
end
