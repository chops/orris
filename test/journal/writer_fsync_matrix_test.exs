defmodule AiOrchestrator.Journal.WriterFsyncMatrixTest do
  @moduledoc """
  NS-08.B.003 control: every accepted event and required directory operation fsyncs, and
  durability is never acknowledged before it.

  The row's acceptance is "inject a file/directory fsync failure at EACH boundary". The
  delivered crash matrix covers four of the append boundaries; the receipt temp open, the
  receipt write, the receipt fsync, the receipt close and the whole creation path had no
  row. This is the boundary matrix: one fault at a time, the named stage, no `{:ok, _}`
  reply, an unchanged acknowledged head, and a writer that refuses the next append.

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
