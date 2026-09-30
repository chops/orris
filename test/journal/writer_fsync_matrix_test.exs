defmodule AiOrchestrator.Journal.WriterFsyncMatrixTest do
  @moduledoc """
  NS-08.B.003 control: every accepted event and required directory operation fsyncs, and
  durability is never acknowledged before it.

  The row's acceptance is "inject a file/directory fsync failure at EACH boundary". The
  delivered crash matrix covers four of the append boundaries; the receipt temp open, the
  receipt write, the receipt fsync, the receipt close and the whole creation path had no
  row. This is the boundary matrix: one fault at a time, the named stage, no `{:ok, _}`
  reply, an unchanged acknowledged head, and a writer that refuses the next append.

  The repair path (the truncate publish and the receipt advance `Writer.open/2` performs
  before it takes appends) has the same matrix: each of its twelve seams faulted once, the
  refusal named `repair_failed` at the publish's stage, no `{:ok, _, _}`, the published file
  unchanged when the fault precedes its rename (no such claim after a rename or dir_sync), and
  a journal the next clean open recovers. A completeness row checks the faulted seams against
  the seams a clean repair actually performs.

  DISCLOSED DEVIATION (recorded, not repaired here): `Fs.SystemFs.dir_sync/2` documents
  that `:file.sync/1` is `fsync(2)` and does NOT force the macOS drive cache, which Erlang
  cannot request without a NIF (system_fs.ex:5-8). These rows prove the ORDERING and the
  refusal, not that the platform flushed its own cache.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

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

  # {label, seam operation, its offset from the count taken before the append, expected stage}.
  # `persist/3` performs write, sync, then the receipt publish (open, write, sync, close,
  # rename, dir_sync), so the receipt write is the SECOND write of the append and so on.
  @append_boundaries [
    {"the journal line write", :write, 1, "write"},
    {"the journal file fsync", :sync, 1, "sync"},
    {"the receipt temp open", :open, 1, "receipt"},
    {"the receipt write", :write, 2, "receipt"},
    {"the receipt fsync", :sync, 2, "receipt"},
    {"the receipt close", :close, 1, "receipt"},
    {"the receipt rename", :rename, 1, "receipt"},
    {"the directory fsync that publishes the receipt", :dir_sync, 1, "receipt"}
  ]

  setup do
    FixedClock.reset()
    :ok
  end

  for {label, op, offset, stage} <- @append_boundaries do
    test "an append that cannot complete #{label} is refused at stage #{stage} and never acknowledged" do
      dir = run_dir_with_journal()
      fs = FaultFs.new()
      {:ok, writer, _opened} = open(dir, fs)
      {:ok, _first} = Writer.append(writer, event(1, "run_created", @created_data))
      assert Writer.last_seq(writer) == 1

      FaultFs.inject(fs, unquote(op), count(fs, unquote(op)) + unquote(offset), {:error, :eio})
      result = Writer.append(writer, event(2))

      assert match?({:error, %{clause: "append_failed", stage: unquote(stage)}}, result),
             "#{unquote(label)}: expected append_failed at stage #{unquote(stage)}, got #{inspect(result)}"

      refute match?({:ok, _event}, result),
             "#{unquote(label)}: durability was acknowledged although the boundary failed"

      assert Writer.last_seq(writer) == 1,
             "#{unquote(label)}: the acknowledged head advanced past a boundary that failed"

      assert match?({:error, %{clause: "writer_failed"}}, Writer.append(writer, event(3))),
             "#{unquote(label)}: the writer accepted a further append after failing closed"

      _closed = Writer.close(writer)
    end
  end

  test "the append matrix covers every seam operation the publish protocol performs" do
    covered = MapSet.new(@append_boundaries, fn {_label, op, _offset, _stage} -> op end)

    assert covered == MapSet.new([:write, :sync, :open, :close, :rename, :dir_sync]),
           "a seam operation of the append protocol has no fault row: #{inspect(covered)}"
  end

  # ---- the repair path (JS2 W1) ----
  #
  # `Writer.open/2` performs the ONE bounded repair a verified journal needs before it takes
  # appends (writer.ex:367-394). An `:advance_and_truncate` plan runs both durable publishes: the
  # truncate publish (`events.jsonl.repair` renamed over `events.jsonl`, stage "truncate") and
  # then the receipt advance (`events.head.tmp` renamed over `events.head`, stage "receipt").
  # Each publish is open, write, sync, close, rename, dir_sync (writer.ex:550-569). Every seam of
  # both publishes is faulted once, located in a probe trace of a clean repair of the same shape
  # rather than assumed.
  @repair_publishes [{"truncate", "events.jsonl.repair"}, {"receipt", "events.head.tmp"}]
  @repair_seams [:open, :write, :sync, :close, :rename, :dir_sync]
  @repair_boundaries for {stage, tmp} <- @repair_publishes, op <- @repair_seams, do: {stage, tmp, op}

  for {stage, tmp, op} <- @repair_boundaries do
    test "a repair whose #{stage} publish cannot complete its #{op} is refused as repair_failed at stage #{stage}" do
      stage = unquote(stage)
      op = unquote(op)
      nth = repair_seam_nth(unquote(tmp), op)
      dir = repair_fixture()
      journal_before = File.read!(Path.join(dir, "events.jsonl"))
      head_before = File.read!(Path.join(dir, "events.head"))

      fs = FaultFs.new()
      FaultFs.inject(fs, op, nth, {:error, :eio})
      result = open(dir, fs)

      assert match?({:error, %{clause: "repair_failed", stage: ^stage}}, result),
             "#{stage}/#{op}: expected repair_failed at stage #{stage}, got #{inspect(result)}"

      refute match?({:ok, _writer, _opened}, result),
             "#{stage}/#{op}: a repair whose #{op} failed was acknowledged: #{inspect(result)}"

      assert match?({:error, %{detail: ":eio"}}, result),
             "#{stage}/#{op}: the rejection does not carry the injected fault: #{inspect(result)}"

      # before the rename nothing was published; after it the disk may already hold the new bytes
      if op in [:open, :write, :sync, :close] do
        {target, before} =
          if stage == "truncate", do: {"events.jsonl", journal_before}, else: {"events.head", head_before}

        after_bytes = File.read!(Path.join(dir, target))

        assert after_bytes == before,
               "#{stage}/#{op}: #{target} changed before its rename: #{inspect(after_bytes)}"
      end

      # the refusal released the lock and left a journal the next clean open recovers to seq 2
      reopened = open(dir, FaultFs.new())

      assert match?({:ok, _writer, %{last_seq: 2, receipt_seq: 2}}, reopened),
             "#{stage}/#{op}: the journal was not recoverable after the refusal: #{inspect(reopened)}"

      {:ok, writer, _opened} = reopened
      _closed = Writer.close(writer)
    end
  end

  test "the repair fixture needs both publishes, and a clean repair acknowledges the advanced receipt" do
    dir = repair_fixture()
    result = open(dir, FaultFs.new())

    assert match?({:ok, _writer, %{repair: %{action: :advance_and_truncate}}}, result),
           "the fixture does not exercise both repair publishes: #{inspect(result)}"

    {:ok, writer, opened} = result
    assert opened.last_seq == 2 and opened.receipt_seq == 2, "clean repair opened at: #{inspect(opened)}"
    _closed = Writer.close(writer)
  end

  test "the repair matrix covers every seam operation each repair publish performs" do
    trace = repair_probe_trace()

    for {stage, tmp} <- @repair_publishes do
      performed = trace |> publish_window(tmp) |> MapSet.new(&elem(&1, 0))
      covered = for {^stage, _tmp, op} <- @repair_boundaries, into: MapSet.new(), do: op

      assert covered == performed,
             "#{stage}: the publish performs #{inspect(performed)} but the matrix faults #{inspect(covered)}"
    end
  end

  test "creation refuses when the run directory cannot be made" do
    dir = empty_run_dir()
    fs = FaultFs.new()
    FaultFs.inject(fs, :mkdir_p, fn _args -> true end, {:error, :eacces})

    assert match?({:error, %{clause: "journal_create_failed"}}, open(dir, fs, create: true))
    refute File.exists?(Path.join(dir, "events.jsonl"))
  end

  test "creation refuses when the exclusive create of the journal fails" do
    dir = empty_run_dir()
    fs = FaultFs.new()
    FaultFs.inject(fs, :open, &events_journal_exclusive?/1, {:error, :eacces})

    assert match?({:error, %{clause: "journal_create_failed"}}, open(dir, fs, create: true))
    refute File.exists?(Path.join(dir, "events.jsonl"))
  end

  test "creation refuses an existing journal by name rather than reusing it" do
    dir = run_dir_with_journal()
    assert match?({:error, %{clause: "journal_exists"}}, open(dir, FaultFs.new(), create: true))
  end

  test "creation refuses when the directory fsync that publishes the new journal fails" do
    # Measured, not assumed: the create path's `dir_sync` is the last one a clean create
    # performs, because `acquire_lock/4` runs before `maybe_create/3` and the reader that
    # follows performs none. The index is taken from a real create in this same shape.
    probe_dir = empty_run_dir()
    probe_fs = FaultFs.new()
    {:ok, probe, _opened} = open(probe_dir, probe_fs, create: true)
    create_dir_sync = count(probe_fs, :dir_sync)
    _closed = Writer.close(probe)

    assert create_dir_sync > 0, "a clean create performed no directory fsync at all"

    dir = empty_run_dir()
    fs = FaultFs.new()
    FaultFs.inject(fs, :dir_sync, create_dir_sync, {:error, :eio})

    assert match?({:error, %{clause: "journal_create_failed"}}, open(dir, fs, create: true)),
           "a journal whose directory entry was never fsynced was reported as created"
  end

  # A journal the Writer itself wrote (two chained events, receipt at seq 2), then set back to a
  # crash shape: the receipt one behind the tail (an advance is due) and an incomplete final line
  # (a truncate is due).
  defp repair_fixture do
    dir = run_dir_with_journal()
    {:ok, writer, _opened} = open(dir, FaultFs.new())
    {:ok, _first} = Writer.append(writer, event(1, "run_created", @created_data))
    first_head = File.read!(Path.join(dir, "events.head"))
    {:ok, _second} = Writer.append(writer, event(2))
    _closed = Writer.close(writer)

    File.write!(Path.join(dir, "events.head"), first_head)
    File.write!(Path.join(dir, "events.jsonl"), ~s({"schema":"ai-orch), [:append])
    dir
  end

  defp repair_probe_trace do
    fs = FaultFs.new()
    {:ok, writer, _opened} = open(repair_fixture(), fs)
    _closed = Writer.close(writer)
    FaultFs.trace(fs)
  end

  # the publish that starts at the open of `tmp` and ends at the directory fsync after it
  defp publish_window(trace, tmp) do
    start = Enum.find_index(trace, &match?({:open, ^tmp, _modes}, &1))
    true = is_integer(start)
    rest = Enum.drop(trace, start)
    stop = Enum.find_index(rest, &(elem(&1, 0) == :dir_sync))
    Enum.take(rest, stop + 1)
  end

  # the FaultFs call index of the first `op` inside the publish of `tmp`, measured on a clean repair
  defp repair_seam_nth(tmp, op) do
    trace = repair_probe_trace()
    start = Enum.find_index(trace, &match?({:open, ^tmp, _modes}, &1))
    offset = trace |> publish_window(tmp) |> Enum.find_index(&(elem(&1, 0) == op))
    trace |> Enum.take(start + offset + 1) |> Enum.count(&(elem(&1, 0) == op))
  end

  defp events_journal_exclusive?(["events.jsonl", modes]), do: :exclusive in modes
  defp events_journal_exclusive?(_args), do: false

  defp lock_opts do
    [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _owner -> :live end]
  end

  defp open(dir, fs, overrides \\ []) do
    Writer.open(dir, Keyword.merge([fs: fs, clock: FixedClock, lock: lock_opts()], overrides))
  end

  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))

  defp empty_run_dir do
    dir = Path.join(System.tmp_dir!(), "writer_fsync_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp run_dir_with_journal do
    dir = empty_run_dir()
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    dir
  end

  defp event(seq, type \\ "run_spec_loaded", data \\ @loaded_data) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 1,
      "seq" => seq,
      "event_id" => "ev_#{seq}",
      "type" => type,
      "ts" => "2026-09-18T00:00:0#{seq}Z",
      "run_id" => "run_fixture_0001",
      "actor" => "run_supervisor",
      "data" => data
    }
  end
end
