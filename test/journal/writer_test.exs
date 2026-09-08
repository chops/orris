defmodule AiOrchestrator.Journal.WriterTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @legacy_fixture Path.expand("../fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "writer_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    on_exit(fn -> File.rm_rf!(dir) end)
    FixedClock.reset()
    %{dir: dir}
  end

  defp lock_opts(overrides \\ []) do
    Keyword.merge(
      [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
      overrides
    )
  end

  defp open(dir, fs, overrides \\ []) do
    Writer.open(dir, Keyword.merge([fs: fs, clock: FixedClock, lock: lock_opts()], overrides))
  end

  # After a simulated crash the previous holder is dead: its lock is reclaimable.
  defp reopen_after_crash(dir), do: open(dir, SystemFs.new(), lock: lock_opts(owner_status: fn _ -> :dead end))

  # A registration only becomes `:down` when the arbiter has processed the
  # monitor message for the writer that just died, which is not ordered
  # against this process observing the open's return value.
  defp wait_until(_fun, 0), do: flunk("condition never held")

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, tries - 1)
    end
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

  defp event(seq, type \\ "run_spec_loaded", data \\ @loaded_data) do
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

  defp disk(dir), do: dir |> Path.join("events.jsonl") |> File.read!()
  defp receipt(dir), do: dir |> Path.join("events.head") |> File.read!() |> Chain.decode_receipt()
  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))

  test "appends stamp the chain, persist exact bytes, and commit a receipt per event", %{dir: dir} do
    fs = FaultFs.new()
    assert {:ok, w, %{last_seq: 0, lines: [], envelope_version: 0, repair: nil}} = open(dir, fs)

    assert {:ok, first} = Writer.append(w, event(1, "run_created", @created_data))
    assert %{"schema_version" => 2, "prev_line_sha256" => anchor, "seq" => 1} = first
    assert anchor == Chain.anchor()

    before = length(FaultFs.trace(fs))
    assert {:ok, second} = Writer.append(w, event(2))
    line1 = Jason.encode!(first) <> "\n"
    assert second["prev_line_sha256"] == Chain.line_sha256(line1)
    assert disk(dir) == line1 <> Jason.encode!(second) <> "\n"

    assert [
             {:write, _},
             {:sync},
             {:open, "events.head.tmp", [:write]},
             {:write, _},
             {:sync},
             {:close},
             {:rename, "events.head.tmp", "events.head"},
             {:dir_sync, _}
           ] = fs |> FaultFs.trace() |> Enum.drop(before)

    assert {:ok, %{seq: 2, line_sha256: last}} = receipt(dir)
    assert last == Chain.line_sha256(Jason.encode!(second) <> "\n")
    assert {:ok, %{count: 2, envelope_version: 2, version_2_from: 1}} = Chain.verify(disk(dir))
    assert Writer.last_seq(w) == 2
    assert :ok = Writer.close(w)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "an append is refused before any byte is written when it is out of sequence, reserved, or malformed",
       %{dir: dir} do
    fs = FaultFs.new()
    {:ok, w, _} = open(dir, fs)
    before = length(FaultFs.trace(fs))

    assert {:error, %{clause: "seq_mismatch", expected: 1, got: 2}} = Writer.append(w, event(2))
    reserved = Event.reserved_types() |> Enum.sort() |> hd()
    assert {:error, %{clause: "reserved_event_type"}} = Writer.append(w, event(1, reserved, %{}))
    assert {:error, %{clause: "missing_required_field", field: "data"}} = Writer.append(w, Map.delete(event(1), "data"))
    assert length(FaultFs.trace(fs)) == before
    assert disk(dir) == ""
    assert {:ok, _} = Writer.append(w, event(1))
    :ok = Writer.close(w)
  end

  test "a second writer is refused while the lock is held and a missing journal fails closed", %{dir: dir} do
    fs = SystemFs.new()
    {:ok, w, _} = open(dir, fs)
    # Inside one BEAM the arbiter answers first, and it can say something the
    # on-disk lock cannot: the holder is a process it is still monitoring.
    assert {:error, %{clause: "second_live_writer", generation: 1}} = open(dir, fs, lock: lock_opts(token: "other"))
    :ok = Writer.close(w)

    File.rm!(Path.join(dir, "events.jsonl"))
    assert {:error, %{clause: "journal_missing"}} = open(dir, fs)
    assert :none = RunLock.owner(fs, dir)

    # A rollback that released the disk lock leaves the arbiter nothing to
    # reclaim, so the record goes with it. A registration surviving here would
    # be a lock nobody holds, waiting to be "reclaimed" on the next acquire.
    assert :none = Ownership.status(dir)
  end

  test "reopen replays identical lines with no repair", %{dir: dir} do
    {:ok, w, _} = open(dir, SystemFs.new())
    {:ok, e1} = Writer.append(w, event(1))
    {:ok, e2} = Writer.append(w, event(2))
    :ok = Writer.close(w)

    assert {:ok, w2, %{last_seq: 2, lines: lines, envelope_version: 2, repair: nil}} = open(dir, SystemFs.new())
    assert lines == [Jason.encode!(e1), Jason.encode!(e2)]
    assert {:ok, e3} = Writer.append(w2, event(3))
    assert e3["prev_line_sha256"] == Chain.line_sha256(Jason.encode!(e2) <> "\n")
    :ok = Writer.close(w2)
  end

  describe "crash matrix at every append boundary" do
    setup %{dir: dir} do
      fs = FaultFs.new()
      {:ok, w, _} = open(dir, fs)
      {:ok, e1} = Writer.append(w, event(1))
      %{fs: fs, w: w, line1: Jason.encode!(e1)}
    end

    test "a write error fails the writer and loses nothing", %{dir: dir, fs: fs, w: w, line1: line1} do
      FaultFs.inject(fs, :write, count(fs, :write) + 1, {:error, :enospc})
      assert {:error, %{clause: "append_failed", stage: "write"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "writer_failed"}} = Writer.append(w, event(2))
      :ok = Writer.close(w)
      assert {:ok, w2, %{lines: [^line1], repair: nil, last_seq: 1}} = open(dir, SystemFs.new())
      :ok = Writer.close(w2)
    end

    test "a torn line is truncated exactly once and recorded", %{dir: dir, fs: fs, w: w, line1: line1} do
      FaultFs.inject(fs, :write, count(fs, :write) + 1, {:torn, 7})
      assert {:error, %{clause: "append_failed", stage: "write"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "close_failed"}} = Writer.close(w)
      assert disk(dir) == line1 <> "\n" <> String.slice(Jason.encode!(event(2)), 0, 7)

      assert {:ok, w2, %{lines: [^line1], last_seq: 1, repair: repair}} = reopen_after_crash(dir)
      assert %{action: :truncate_tail, truncate_bytes: 7, receipt_seq_before: 1, receipt_seq_after: 1} = repair
      assert disk(dir) == line1 <> "\n"
      assert {:ok, _} = Writer.append(w2, event(2))
      :ok = Writer.close(w2)
    end

    test "dying between the line fsync and the receipt keeps the line via receipt advance",
         %{dir: dir, fs: fs, w: w, line1: line1} do
      FaultFs.inject(fs, :sync, count(fs, :sync) + 1, :halt)
      assert {:error, %{clause: "append_failed", stage: "sync"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "close_failed"}} = Writer.close(w)

      assert {:ok, w2, %{lines: [^line1, line2], last_seq: 2, repair: repair}} = reopen_after_crash(dir)
      assert %{action: :advance_receipt, receipt_seq_before: 1, receipt_seq_after: 2} = repair
      assert {:ok, %{seq: 2, line_sha256: hash}} = receipt(dir)
      assert hash == Chain.line_sha256(line2 <> "\n")
      :ok = Writer.close(w2)
    end

    test "dying after the receipt is written but before it is renamed leaves a tmp that open clears",
         %{dir: dir, fs: fs, w: w, line1: line1} do
      FaultFs.inject(fs, :rename, count(fs, :rename) + 1, :halt)
      assert {:error, %{clause: "append_failed", stage: "receipt"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "close_failed"}} = Writer.close(w)
      assert File.exists?(Path.join(dir, "events.head.tmp"))

      assert {:ok, w2, %{lines: [^line1, _line2], last_seq: 2, repair: %{action: :advance_receipt}}} =
               reopen_after_crash(dir)

      refute File.exists?(Path.join(dir, "events.head.tmp"))
      :ok = Writer.close(w2)
    end

    test "dying after the receipt rename but before the directory fsync needs no repair",
         %{dir: dir, fs: fs, w: w, line1: line1} do
      FaultFs.inject(fs, :dir_sync, count(fs, :dir_sync) + 1, :halt)
      assert {:error, %{clause: "append_failed", stage: "receipt"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "close_failed"}} = Writer.close(w)
      assert {:ok, w2, %{lines: [^line1, _], last_seq: 2, repair: nil}} = reopen_after_crash(dir)
      :ok = Writer.close(w2)
    end
  end

  test "close reports a failed lock release by leg and never claims success", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, w, _} = open(dir, fs)
    {:ok, _} = Writer.append(w, event(1, "run_created", @created_data))

    FaultFs.inject(
      fs,
      :link,
      fn
        [_tmp, "run.lock.2"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "close_failed", failures: [%{leg: "lock", detail: detail}]}} = Writer.close(w)
    assert detail =~ "eacces"
    refute Process.alive?(w)
    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, %{count: 1}} = Chain.verify(disk(dir))
  end

  test "open never repairs a journal whose lines fail typed validation", %{dir: dir} do
    {:ok, w, _} = open(dir, SystemFs.new())
    {:ok, e1} = Writer.append(w, event(1, "run_created", @created_data))
    :ok = Writer.close(w)
    line1 = Jason.encode!(e1) <> "\n"

    bad =
      2
      |> event("run_spec_loaded", %{"spec_path" => "spec.json"})
      |> Map.merge(%{"schema_version" => 2, "prev_line_sha256" => Chain.line_sha256(line1)})
      |> Jason.encode!()

    torn = line1 <> bad <> "\n" <> "{\"partial"
    File.write!(Path.join(dir, "events.jsonl"), torn)

    assert {:error, %{clause: "invalid_event_data", at_seq: 2, event_type: "run_spec_loaded"}} =
             open(dir, SystemFs.new())

    assert File.read!(Path.join(dir, "events.jsonl")) == torn
    assert {:ok, %{seq: 1}} = receipt(dir)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "open fails closed with cleanup_required when a dead lower generation cannot be compacted", %{dir: dir} do
    fs = FaultFs.new()

    dead = %{
      "schema" => "ai-orchestrator/run-lock",
      "schema_version" => 1,
      "state" => "held",
      "pid" => "777",
      "pid_start" => "gone",
      "supervisor_instance" => "sup_dead",
      "token" => "token_dead",
      "acquired_at" => "2026-09-01T12:00:00Z"
    }

    File.write!(Path.join(dir, "run.lock.1"), Jason.encode!(dead) <> "\n")

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", path: path, owner: ^dead, token: "token_dead"}} =
             open(dir, fs, lock: lock_opts(token: "sup_0001_token", owner_status: fn _ -> :dead end))

    assert Path.basename(path) == "run.lock.1"
    assert {:ok, %{"token" => "token_dead"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "an open that fails after the lock is acquired reports a failed rollback release by name", %{dir: dir} do
    fs = FaultFs.new()
    File.rm!(Path.join(dir, "events.jsonl"))

    FaultFs.inject(
      fs,
      :link,
      fn
        [_tmp, "run.lock.2"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error,
            %{
              clause: "writer_open_cleanup_failed",
              cause: %{clause: "journal_missing"},
              release: %{clause: "release_failed"},
              lock_path: "run.lock.1"
            }} = open(dir, fs)

    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)

    # The mirror image of the clean rollback: the lock is still on disk, and
    # this registration is the only authority that can release it safely, so
    # the arbiter keeps it and reclaims it when this directory is next opened.
    wait_until(fn -> match?({:ok, %{state: :down}}, Ownership.status(dir)) end, 500)
    assert {:ok, %{generation: 1, state: :down}} = Ownership.status(dir)
  end

  test "an open that fails on the journal descriptor retires the registration with the lock", %{dir: dir} do
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :open,
      fn
        ["events.jsonl", [:append]] -> true
        _other -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "journal_open_failed"}} = open(dir, fs)
    assert :none = RunLock.owner(SystemFs.new(), dir)
    assert :none = Ownership.status(dir)
  end

  test "close names a release whose old file was removed but not durably synced", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, w, _} = open(dir, fs)
    FaultFs.inject(fs, :dir_sync, count(fs, :dir_sync) + 2, {:error, :eio})

    assert {:error, %{clause: "close_failed", failures: [%{leg: "lock", detail: detail}]}} = Writer.close(w)
    assert detail =~ "release_removed_unsynced"
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "a stale receipt temp that cannot be cleared durably is a named open rejection", %{dir: dir} do
    File.write!(Path.join(dir, "events.head.tmp"), "stale")
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["events.head.tmp"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "head_temp_cleanup_required", path: "events.head.tmp"}} = open(dir, fs)
    assert :none = RunLock.owner(SystemFs.new(), dir)
    assert File.exists?(Path.join(dir, "events.head.tmp"))

    # The second open publishes generation 3 (sync 1), compacts the tombstone (sync 2), then clears
    # the receipt temp (sync 3).
    fs2 = FaultFs.new()
    FaultFs.inject(fs2, :dir_sync, 3, {:error, :eio})
    assert {:error, %{clause: "head_temp_removed_unsynced", path: "events.head.tmp"}} = open(dir, fs2)
    refute File.exists?(Path.join(dir, "events.head.tmp"))
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "a corrupted middle line fails closed at its sequence and releases the lock", %{dir: dir} do
    {:ok, w, _} = open(dir, SystemFs.new())
    {:ok, _} = Writer.append(w, event(1, "run_created", @created_data))
    {:ok, _} = Writer.append(w, event(2))
    {:ok, _} = Writer.append(w, event(3))
    :ok = Writer.close(w)

    corrupted = String.replace(disk(dir), ~s("project":"example"), ~s("project":"exampls"), global: false)
    File.write!(Path.join(dir, "events.jsonl"), corrupted)
    assert {:error, %{clause: "chain_mismatch", at_seq: 2}} = open(dir, SystemFs.new())
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "a legacy journal opens read-consistent and upgrades on the first append", %{dir: dir} do
    File.cp!(@legacy_fixture, Path.join(dir, "events.jsonl"))
    {:ok, legacy} = Chain.verify(disk(dir))
    n = legacy.count

    assert {:ok, w, %{last_seq: ^n, envelope_version: 1, repair: nil}} = open(dir, SystemFs.new())
    assert {:ok, upgraded} = Writer.append(w, event(n + 1, "run_cancel_requested", %{"reason" => "operator"}))
    assert upgraded["prev_line_sha256"] == legacy.last_line_sha256
    assert {:ok, %{envelope_version: 2, version_2_from: from, version_2_count: 1}} = Chain.verify(disk(dir))
    assert from == n + 1
    assert {:ok, %{seq: ^from}} = receipt(dir)
    :ok = Writer.close(w)
    assert {:ok, w2, %{last_seq: ^from, repair: nil}} = open(dir, SystemFs.new())
    :ok = Writer.close(w2)
  end
end
