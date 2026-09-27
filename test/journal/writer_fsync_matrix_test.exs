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

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.RunLock
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

  # NS-08.B.003 B3-4: {label, seed, temp the repair publishes, its final name, faulted op, repair_failed stage}.
  # The seed decides which repair publish runs (Chain.plan/3): a torn tail -> :truncate_tail (writer.ex:378),
  # a receipt one line behind -> :advance_receipt (writer.ex:388). Both go through publish/5.
  #
  # UNTESTED LIMIT: there is no :advance_and_truncate row. The combined ordering (the truncate publish runs
  # first and a truncate failure must prevent the receipt publish, writer.ex:370-371) is not exercised here.
  @repair_boundaries [
    {"the truncate publish file fsync", :torn_tail, "events.jsonl.repair", "events.jsonl", :sync, "truncate"},
    {"the truncate publish directory fsync", :torn_tail, "events.jsonl.repair", "events.jsonl", :dir_sync,
     "truncate"},
    {"the receipt advance file fsync", :stale_receipt, "events.head.tmp", "events.head", :sync, "receipt"},
    {"the receipt advance directory fsync", :stale_receipt, "events.head.tmp", "events.head", :dir_sync,
     "receipt"}
  ]

  # 9 bytes with no newline: an incomplete tail (chain.ex:109-113)
  @torn_tail ~s({"partial)

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

  describe "reopen repair publishes (NS-08.B.003 B3-4)" do
    for {label, seed, tmp, final, op, stage} <- @repair_boundaries do
      test "an open whose repair cannot complete #{label} is refused at repair_failed/#{stage} and acknowledges nothing" do
        {dir, seeded} = seed(unquote(seed))
        fs = FaultFs.new()
        fail_next_after_open(fs, unquote(tmp), unquote(op))

        result = open(dir, fs)

        assert result == {:error, %{clause: "repair_failed", stage: unquote(stage), detail: ":eio"}},
               "#{unquote(label)}: expected repair_failed at stage #{unquote(stage)}, got #{inspect(result)}"

        refute match?({:ok, _writer, _opened}, result),
               "#{unquote(label)}: the open acknowledged a repair whose publish failed"

        assert :none = RunLock.owner(SystemFs.new(), dir)

        # the fault landed on THIS publish at THIS op, not elsewhere in the open
        assert_publish_stopped_at(fs, unquote(tmp), unquote(final), unquote(op))

        assert_disk_after_refusal(unquote(seed), unquote(op), dir, seeded)
      end
    end

    for {seed, tmp, final, action} <- [
          {:torn_tail, "events.jsonl.repair", "events.jsonl", :truncate_tail},
          {:stale_receipt, "events.head.tmp", "events.head", :advance_receipt}
        ] do
      test "control: the #{seed} seed repairs through #{tmp} -> #{final} when nothing fails" do
        {dir, seeded} = seed(unquote(seed))
        fs = FaultFs.new()

        assert {:ok, writer, %{repair: %{action: unquote(action)} = repair, last_seq: 2, receipt_seq: 2}} =
                 open(dir, fs)

        assert [
                 {:open, unquote(tmp), [:write]},
                 {:write, _bytes},
                 {:sync},
                 {:close},
                 {:rename, unquote(tmp), unquote(final)},
                 {:dir_sync, _dir}
               ] = publish_slice(fs, unquote(tmp))

        assert_repaired(unquote(seed), dir, seeded, repair)
        assert {:ok, _third} = Writer.append(writer, event(3))
        :ok = Writer.close(writer)
      end
    end

    test "the repair matrix covers the file and directory fsync of both repair publishes" do
      covered = MapSet.new(@repair_boundaries, fn {_label, _seed, _tmp, _final, op, stage} -> {stage, op} end)

      assert covered ==
               MapSet.new([
                 {"truncate", :sync},
                 {"truncate", :dir_sync},
                 {"receipt", :sync},
                 {"receipt", :dir_sync}
               ]),
             "a repair publish fsync has no fault row: #{inspect(covered)}"
    end
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

  # Two chained events closed cleanly, then a torn tail appended.
  # Chain.plan/3: receipt seq 2 == count 2, tail 9 bytes -> :truncate_tail (2 -> 2).
  defp seed(:torn_tail) do
    dir = run_dir_with_journal()
    {:ok, writer, _opened} = open(dir, FaultFs.new())
    {:ok, _first} = Writer.append(writer, event(1, "run_created", @created_data))
    {:ok, _second} = Writer.append(writer, event(2))
    :ok = Writer.close(writer)
    clean = read(dir, "events.jsonl")
    File.write!(Path.join(dir, "events.jsonl"), @torn_tail, [:append])
    {dir, %{journal: clean <> @torn_tail, repaired: clean, head: read(dir, "events.head")}}
  end

  # Two chained events closed cleanly, then the seq-1 receipt the writer itself published put back.
  # Chain.plan/3: receipt seq 1 == count - 1, no tail -> :advance_receipt (1 -> 2).
  defp seed(:stale_receipt) do
    dir = run_dir_with_journal()
    {:ok, writer, _opened} = open(dir, FaultFs.new())
    {:ok, _first} = Writer.append(writer, event(1, "run_created", @created_data))
    stale = read(dir, "events.head")
    {:ok, second} = Writer.append(writer, event(2))
    :ok = Writer.close(writer)
    File.write!(Path.join(dir, "events.head"), stale)

    {dir,
     %{
       journal: read(dir, "events.jsonl"),
       head: stale,
       advanced_hash: Chain.line_sha256(Jason.encode!(second) <> "\n")
     }}
  end

  # Arms `op` to fail on its NEXT call at the moment `tmp` is opened for write. The hook runs in the writer
  # after the open is counted and before it is performed (fault_fs.ex:140,152-155); publish/5 performs no
  # other sync or dir_sync between that open and its own (writer.ex:553-556, 561-563), so no offset is
  # hard-coded against the lock's own seam traffic.
  defp fail_next_after_open(fs, tmp, op) do
    FaultFs.inject(
      fs,
      :open,
      fn args -> args == [tmp, [:write]] end,
      {:hook, fn -> FaultFs.inject(fs, op, count(fs, op) + 1, {:error, :eio}) end}
    )
  end

  defp publish_slice(fs, tmp) do
    fs |> FaultFs.trace() |> Enum.drop_while(&(&1 != {:open, tmp, [:write]})) |> Enum.take(6)
  end

  # file fsync failed: write_all closes the temp (writer.ex:566) and the rename never runs
  defp assert_publish_stopped_at(fs, tmp, final, :sync) do
    assert [{:open, ^tmp, [:write]}, {:write, _bytes}, {:sync}, {:close} | _rest] = publish_slice(fs, tmp)
    refute {:rename, tmp, final} in FaultFs.trace(fs), "#{tmp} was renamed although its file fsync failed"
  end

  # directory fsync failed: it is the publish's LAST op, after the rename (writer.ex:555-556)
  defp assert_publish_stopped_at(fs, tmp, final, :dir_sync) do
    assert [
             {:open, ^tmp, [:write]},
             {:write, _bytes},
             {:sync},
             {:close},
             {:rename, ^tmp, ^final},
             {:dir_sync, _dir}
           ] = publish_slice(fs, tmp)
  end

  # truncate, file fsync: nothing published; journal still torn, receipt untouched
  defp assert_disk_after_refusal(:torn_tail, :sync, dir, seeded) do
    assert read(dir, "events.jsonl") == seeded.journal
    assert read(dir, "events.head") == seeded.head
  end

  # truncate, dir fsync: a visible rename is not a durability witness, so the journal bytes are not asserted;
  # the untouched receipt proves the second publication did not run
  defp assert_disk_after_refusal(:torn_tail, :dir_sync, dir, seeded) do
    assert read(dir, "events.head") == seeded.head
  end

  # receipt, file fsync: the acknowledged head receipt keeps its stale seq-1 bytes
  defp assert_disk_after_refusal(:stale_receipt, :sync, dir, seeded) do
    assert read(dir, "events.jsonl") == seeded.journal
    assert read(dir, "events.head") == seeded.head
  end

  # receipt, dir fsync: the rename precedes the directory fsync (writer.ex:555-556), so the advanced receipt
  # is expected to be visible here, but it is neither durable nor acknowledged. No disk bytes are asserted
  # and no rollback is claimed: the exact repair_failed/receipt open refusal is this row's control.
  defp assert_disk_after_refusal(:stale_receipt, :dir_sync, _dir, _seeded), do: :ok

  defp assert_repaired(:torn_tail, dir, seeded, repair) do
    assert %{truncate_bytes: 9, receipt_seq_before: 2, receipt_seq_after: 2} = repair
    assert read(dir, "events.jsonl") == seeded.repaired
    assert read(dir, "events.head") == seeded.head
  end

  defp assert_repaired(:stale_receipt, dir, seeded, repair) do
    assert %{truncate_bytes: 0, receipt_seq_before: 1, receipt_seq_after: 2} = repair
    assert read(dir, "events.jsonl") == seeded.journal
    assert {:ok, %{seq: 2, line_sha256: hash}} = Chain.decode_receipt(read(dir, "events.head"))
    assert hash == seeded.advanced_hash
  end

  defp read(dir, name), do: File.read!(Path.join(dir, name))
end
