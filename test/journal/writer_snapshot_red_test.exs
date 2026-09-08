defmodule AiOrchestrator.Journal.WriterSnapshotRedTest do
  @moduledoc """
  U1a-S RED/interface, revision 2 (review m_1788700867000): `Writer.verified/1` (live, re-read-and-hash-verified
  snapshot carrying the accepted lines) and the Writer-owned append fence. Contract: docs/contracts/writer-snapshot.org.
  RED rows are late-bound on the missing functions; every control runs on the UNCHANGED Writer/Reader without the
  guard. Every Writer, caller and task is tracked and reaped (bounded) on exit; directories are removed last.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @canary "SNAPSHOT-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @legacy_fixture Path.expand("../fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl", __DIR__)

  # late-bound: the snapshot functions do not exist yet; a dynamic module reference keeps --warnings-as-errors honest
  defp writer, do: Module.concat(["AiOrchestrator", "Journal", "Writer"])

  defp require_snapshot! do
    Code.ensure_loaded(Writer)
    assert function_exported?(Writer, :verified, 1), "Writer.verified/1 does not exist"
    assert function_exported?(Writer, :append, 3), "Writer.append/3 (fence:) does not exist"
  end

  setup do
    # Writer.open links the opener: a crash witness ends the Writer, and this process must observe it, not die
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "writer_snapshot_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    FixedClock.reset()
    # unlinked: it must outlive the test process so the exit callback can still read it
    {:ok, tracked} = Agent.start(fn -> [] end)

    on_exit(fn ->
      # bounded teardown over EVERY tracked subject (writers, callers, tasks), survivors reported, directory last
      survivors =
        for pid <- Agent.get(tracked, & &1), Process.alive?(pid), reduce: [] do
          acc ->
            ref = Process.monitor(pid)
            Process.exit(pid, :kill)

            receive do
              {:DOWN, ^ref, :process, ^pid, _} -> acc
            after
              5_000 -> [pid | acc]
            end
        end

      File.rm_rf!(dir)
      Agent.stop(tracked)
      if survivors != [], do: raise("tracked subjects survived the reaper: #{length(survivors)}")
    end)

    %{dir: dir, tracked: tracked}
  end

  defp track!(tracked, pid), do: Agent.update(tracked, &[pid | &1])

  defp lock_opts(overrides \\ []) do
    Keyword.merge(
      [supervisor_instance: "sup_snap", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
      overrides
    )
  end

  defp open!(dir, tracked, fs, overrides \\ []) do
    {:ok, w, opened} = Writer.open(dir, Keyword.merge([fs: fs, clock: FixedClock, lock: lock_opts()], overrides))
    track!(tracked, w)
    {w, opened}
  end

  defp reopen_after_crash!(dir, tracked),
    do: open!(dir, tracked, SystemFs.new(), lock: lock_opts(owner_status: fn _ -> :dead end))

  # a tracked caller: registered BEFORE it starts its work; the test observes its DOWN, never assumes it
  defp caller!(tracked, fun) do
    parent = self()
    permit = make_ref()

    {pid, mon} =
      spawn_monitor(fn ->
        receive do
          {:start, ^permit} -> send(parent, {:caller_result, fun.()})
        after
          5_000 -> exit(:caller_never_permitted)
        end
      end)

    track!(tracked, pid)
    send(pid, {:start, permit})
    {pid, mon}
  end

  # the event corpus: the FIRST lines of the existing valid fixture, decoded (envelope fields dropped; the Writer
  # stamps them again), so every event is grammar-valid by construction; C-0 proves their acceptance on the
  # unchanged Writer
  @corpus @legacy_fixture
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.take(6)
          |> Enum.map(&Jason.decode!/1)
          |> Enum.map(&Map.drop(&1, ["schema_version", "prev_line_sha256"]))

  defp event(seq) when seq in 1..6, do: Enum.at(@corpus, seq - 1)
  defp with_project(event, project), do: put_in(event, ["data", "project"], project)

  defp disk(dir), do: File.read!(Path.join(dir, "events.jsonl"))
  defp head(dir), do: File.read(Path.join(dir, "events.head"))
  defp append_torn!(dir), do: File.write!(Path.join(dir, "events.jsonl"), "{\"seq\":4,\"torn", [:append])
  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))
  defp ops(fs, since), do: fs |> FaultFs.trace() |> Enum.drop(since) |> Enum.map(&elem(&1, 0))
  defp trace_len(fs), do: length(FaultFs.trace(fs))
  # the mutating operations of the seam (reads are not mutations)
  @mutating [:write, :sync, :rename, :dir_sync, :rm, :mkdir, :mkdir_p, :chmod, :link, :rmdir]
  defp mutations(fs, since) do
    fs |> ops(since) |> Enum.filter(&(&1 in @mutating))
  end

  # the Nth sync of the NEXT append: 1 = the journal fsync, 2 = the receipt temp fsync (matcher-based, not a
  # global ordinal)
  defp inject_nth_sync_of_next_append!(fs, nth, fault, tracked) do
    {:ok, counter} = Agent.start(fn -> 0 end)
    track!(tracked, counter)
    FaultFs.inject(fs, :sync, fn _args -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) == nth end, fault)
    counter
  end

  # the Writer's mailbox holds the snapshot call while it is blocked: the request provably reached it
  defp enqueued?(w) do
    Enum.any?(1..50, fn _ ->
      {:messages, messages} = Process.info(w, :messages)
      Enum.any?(messages, &match?({:"$gen_call", _, :verified}, &1)) or (Process.sleep(10) && false)
    end)
  end

  # =================================================================================================
  describe "controls (unchanged Writer/Reader, no guard)" do
    test "C-5 the torn-tail fixture preserves exactly three complete lines and needs only truncation", %{
      dir: dir,
      tracked: tracked
    } do
      {w, _} = open!(dir, tracked, SystemFs.new())
      for seq <- 1..3, do: assert({:ok, _} = Writer.append(w, event(seq)))
      assert {:ok, before} = Reader.load(dir)
      append_torn!(dir)
      assert {:ok, after_tail} = Reader.load(dir)
      assert after_tail.lines == before.lines
      assert after_tail.last_seq == 3
      assert %{action: :truncate_tail} = after_tail.pending_repair
      :ok = Writer.close(w)
    end

    test "C-0 the corpus: every row event is accepted by the unchanged Writer in order (grammar control)", %{
      dir: dir,
      tracked: tracked
    } do
      {w, _} = open!(dir, tracked, SystemFs.new())
      for seq <- 1..6, do: assert({:ok, %{"seq" => ^seq}} = Writer.append(w, event(seq)))
      {:ok, e3} = Chain.verify(disk(dir))
      assert e3.count == 6
      :ok = Writer.close(w)
    end

    test "C-0b read-fault isolation on the unchanged Reader: a journal fault stops at the journal; a receipt fault is reachable only when the journal reads",
         %{dir: dir, tracked: tracked} do
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, _} = Writer.append(w, event(1))
      :ok = Writer.close(w)
      fs_j = FaultFs.new()
      FaultFs.inject(fs_j, :read, fn [name] -> name == "events.jsonl" end, {:error, {:seam, @canary}})
      assert {:error, %{clause: "journal_unreadable"}} = Reader.load(dir, fs: fs_j)
      refute Enum.any?(FaultFs.trace(fs_j), &match?({:read, "events.head"}, &1)), "the receipt is never read"
      fs_r = FaultFs.new()
      FaultFs.inject(fs_r, :read, fn [name] -> name == "events.head" end, {:error, {:seam, @canary}})
      assert {:error, %{clause: clause}} = Reader.load(dir, fs: fs_r)
      assert is_binary(clause)
      assert Enum.any?(FaultFs.trace(fs_r), &match?({:read, "events.head"}, &1)), "the receipt read was reached"
    end

    test "C-1 opened/1 is the cached startup view and stays so after appends", %{dir: dir, tracked: tracked} do
      {w, opened} = open!(dir, tracked, SystemFs.new())
      assert opened.last_seq == 0
      {:ok, _} = Writer.append(w, event(1))
      assert Writer.opened(w).last_seq == 0, "the startup view never moves; that is the semantics to preserve"
      assert Writer.last_seq(w) == 1
      :ok = Writer.close(w)
    end

    test "C-2 fault-ordinal proof: the next sync after a completed append is the JOURNAL sync; the receipt sync follows the head temp open",
         %{dir: dir, tracked: tracked} do
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, _} = Writer.append(w, event(1))
      since = trace_len(fs)
      FaultFs.inject(fs, :sync, count(fs, :sync) + 1, {:error, :review_fault})
      assert {:error, %{clause: "append_failed", stage: "sync"}} = Writer.append(w, event(2))
      assert ops(fs, since) == [:write, :sync], "journal write then journal sync; the receipt temp was never opened"
      refute Enum.any?(Enum.drop(FaultFs.trace(fs), since), &match?({:open, "events.head.tmp", _}, &1))
      # an error-returning fault does not halt the seam: the failed writer still closes its descriptor and lock
      assert :ok = Writer.close(w)
    end

    for {label, stage, target, expected_repair} <- [
          {"journal sync", "sync", {:sync, 1}, :advance_receipt},
          {"receipt sync", "receipt", {:sync, 2}, :advance_receipt},
          {"receipt rename", "receipt", {:rename, 1}, :advance_receipt},
          {"post-rename dir_sync", "receipt", {:dir_sync, 1}, nil}
        ] do
      test "C-3 #{label} fault on the unchanged Writer: append_failed stage #{stage}; close_failed; reopen verdict #{inspect(expected_repair)}",
           %{dir: dir, tracked: tracked} do
        fs = FaultFs.new()
        {w, _} = open!(dir, tracked, fs)
        {:ok, e1} = Writer.append(w, event(1))
        since = trace_len(fs)

        case unquote(Macro.escape(target)) do
          {:sync, nth} -> inject_nth_sync_of_next_append!(fs, nth, :halt, tracked)
          {op, nth} -> FaultFs.inject(fs, op, count(fs, op) + nth, :halt)
        end

        assert {:error, %{clause: "append_failed", stage: unquote(stage)}} = Writer.append(w, event(2))
        delta = ops(fs, since)

        if unquote(stage) == "receipt",
          do:
            assert(
              Enum.any?(Enum.drop(FaultFs.trace(fs), since), &match?({:open, "events.head.tmp", _}, &1)),
              "the receipt path was reached"
            ),
          else: refute(:open in delta)

        assert {:error, %{clause: "close_failed"}} = Writer.close(w)
        {w2, opened2} = reopen_after_crash!(dir, tracked)
        assert length(opened2.lines) == 2 and hd(opened2.lines) == Jason.encode!(e1)

        case unquote(expected_repair) do
          nil -> assert opened2.repair == nil, "durable through the rename: nothing to repair"
          action -> assert %{action: ^action} = opened2.repair
        end

        :ok = Writer.close(w2)
      end
    end

    test "C-4 after-success dir_sync witness on the unchanged Writer: the publication's final fsync completed before the reply; a lost-reply crash reopens with repair nil",
         %{dir: dir, tracked: tracked} do
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      parent = self()
      # {:after, fun} runs INSIDE the Writer only if the dir_sync returned :ok: the receipt publication is durable
      FaultFs.inject(
        fs,
        :dir_sync,
        count(fs, :dir_sync) + 1,
        {:after,
         fn trace ->
           send(parent, {:durable, self(), hd(trace)})

           receive do
             :release -> :ok
             :crash -> exit(:crash_after_durable_publication)
           after
             10_000 -> exit(:witness_never_released)
           end
         end}
      )

      {caller, cmon} = caller!(tracked, fn -> Writer.append(w, event(1)) end)
      assert_receive {:durable, ^w, {:dir_sync, _}}, 5_000
      refute_received {:caller_result, _}, "no reply yet"
      assert {:ok, receipt} = head(dir)
      assert receipt =~ ~s("seq":1), "the head receipt is published and fsynced before the reply"
      send(w, :crash)
      assert_receive {:EXIT, ^w, :crash_after_durable_publication}, 5_000, "the writer died after the durable publication"
      assert_receive {:DOWN, ^cmon, :process, ^caller, _}, 5_000, "the caller observes the lost reply"
      {w2, opened2} = reopen_after_crash!(dir, tracked)
      assert opened2.repair == nil and opened2.last_seq == 1, "everything was durable: nothing to repair"
      :ok = Writer.close(w2)
    end
  end

  # =================================================================================================
  describe "snapshot" do
    test "S-1 empty journal: lines [], seq 0, the anchor hash, receipt nil (legitimately absent), no repair, a token",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      assert {:ok, snapshot} = writer().verified(w)
      assert %{lines: [], seq: 0, receipt: nil, repair: nil, envelope_version: 0} = snapshot
      assert snapshot.last_line_sha256 == Chain.anchor()
      assert is_integer(snapshot.generation) and is_reference(snapshot.token.ref)

      assert snapshot.token == %{
               seq: 0,
               last_line_sha256: Chain.anchor(),
               generation: snapshot.generation,
               ref: snapshot.token.ref
             }

      :ok = Writer.close(w)
    end

    test "S-2 a normal append: both disk reads are traced; lines are the persisted line; hash and receipt agree", %{
      dir: dir,
      tracked: tracked
    } do
      require_snapshot!()
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, e1} = Writer.append(w, event(1))
      since = trace_len(fs)
      assert {:ok, snapshot} = writer().verified(w)
      reads = for {:read, name} <- Enum.drop(FaultFs.trace(fs), since), do: name
      assert Enum.sort(reads) == ["events.head", "events.jsonl"], "the snapshot re-reads both sources"
      assert mutations(fs, since) == [], "a snapshot mutates nothing"
      line = Jason.encode!(e1)
      assert snapshot.lines == [line] and snapshot.seq == 1
      assert snapshot.last_line_sha256 == Chain.line_sha256(line <> "\n")
      assert snapshot.receipt == %{seq: 1, line_sha256: snapshot.last_line_sha256}
      assert Writer.opened(w).last_seq == 0, "opened/1 unchanged"
      :ok = Writer.close(w)
    end

    test "S-3 in-flight serialization (BEFORE-hook hold): the snapshot request is enqueued at the held Writer and answered after",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      parent = self()

      FaultFs.inject(
        fs,
        :dir_sync,
        count(fs, :dir_sync) + 1,
        {:hook,
         fn ->
           send(parent, {:held, self()})

           receive do
             :go -> :ok
           after
             10_000 -> exit(:witness_never_released)
           end

           :ok
         end}
      )

      {caller, cmon} = caller!(tracked, fn -> Writer.append(w, event(1)) end)
      assert_receive {:held, ^w}, 5_000
      refute_received {:caller_result, _}
      snap = Task.async(fn -> writer().verified(w) end)
      track!(tracked, snap.pid)
      assert enqueued?(w), "the snapshot call reached the held Writer"

      assert Task.yield(snap, 200) == nil
      send(w, :go)
      assert_receive {:caller_result, {:ok, _}}, 5_000
      assert_receive {:DOWN, ^cmon, :process, ^caller, :normal}, 5_000
      assert {:ok, %{seq: 1, receipt: %{seq: 1}}} = Task.await(snap, 5_000)
      :ok = Writer.close(w)
    end

    for {label, release} <- [{"normal release", :release}, {"lost-reply crash then reopen", :crash}] do
      test "S-3b AFTER-success witness (#{label}): the receipt publication is durable before the reply; the snapshot is linearized behind it",
           %{dir: dir, tracked: tracked} do
        require_snapshot!()
        fs = FaultFs.new()
        {w, _} = open!(dir, tracked, fs)
        parent = self()

        FaultFs.inject(
          fs,
          :dir_sync,
          count(fs, :dir_sync) + 1,
          {:after,
           fn _trace ->
             send(parent, {:durable, self()})

             receive do
               :release -> :ok
               :crash -> exit(:crash_after_durable_publication)
             after
               10_000 -> exit(:witness_never_released)
             end
           end}
        )

        {caller, cmon} = caller!(tracked, fn -> Writer.append(w, event(1)) end)
        assert_receive {:durable, ^w}, 5_000
        refute_received {:caller_result, _}
        assert {:ok, _} = head(dir)
        snap = Task.async(fn -> writer().verified(w) end)
        track!(tracked, snap.pid)
        assert enqueued?(w), "the snapshot call reached the held Writer (AFTER leg)"
        assert Task.yield(snap, 200) == nil
        smon = Process.monitor(snap.pid)
        send(w, unquote(release))

        case unquote(release) do
          :release ->
            assert_receive {:caller_result, {:ok, _}}, 5_000
            assert_receive {:DOWN, ^cmon, :process, ^caller, :normal}, 5_000
            assert {:ok, %{seq: 1, receipt: %{seq: 1}}} = Task.await(snap, 5_000)
            assert_receive {:DOWN, ^smon, :process, _, :normal}, 5_000
            :ok = Writer.close(w)

          :crash ->
            assert_receive {:EXIT, ^w, :crash_after_durable_publication}, 5_000
            assert_receive {:DOWN, ^cmon, :process, ^caller, _}, 5_000
            # the queued snapshot call dies with the Writer: an OBSERVED task exit, never a synthetic fallback
            assert_receive {:DOWN, ^smon, :process, _, _}, 5_000
            {w2, _} = reopen_after_crash!(dir, tracked)
            assert {:ok, %{seq: 1, repair: nil, receipt: %{seq: 1}}} = writer().verified(w2)
            :ok = Writer.close(w2)
        end
      end
    end

    test "S-4 write-stage failure: a WARM FAILED writer never snapshots; the reopen (repair authority) reports truncate_tail",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, e1} = Writer.append(w, event(1))
      FaultFs.inject(fs, :write, count(fs, :write) + 1, {:torn, 7})
      assert {:error, %{clause: "append_failed", stage: "write"}} = Writer.append(w, event(2))
      assert {:error, %{clause: "writer_failed", cause: %{clause: "append_failed"}}} = writer().verified(w)
      assert {:error, %{clause: "close_failed"}} = Writer.close(w)
      {w2, opened2} = reopen_after_crash!(dir, tracked)
      assert %{action: :truncate_tail} = opened2.repair
      assert {:ok, snapshot} = writer().verified(w2)
      assert snapshot.lines == [Jason.encode!(e1)] and snapshot.seq == 1
      assert snapshot.repair == nil, "the reopen executed the repair; the live snapshot has nothing pending"
      :ok = Writer.close(w2)
    end

    for {label, stage, target, expected_repair} <- [
          {"journal sync", "sync", {:sync, 1}, :advance_receipt},
          {"receipt sync", "receipt", {:sync, 2}, :advance_receipt},
          {"receipt rename", "receipt", {:rename, 1}, :advance_receipt},
          {"post-rename dir_sync", "receipt", {:dir_sync, 1}, nil}
        ] do
      test "S-5 #{label} failure: the failed writer refuses (writer_failed); the reopen snapshot reports the verdict #{inspect(expected_repair)}",
           %{dir: dir, tracked: tracked} do
        require_snapshot!()
        fs = FaultFs.new()
        {w, _} = open!(dir, tracked, fs)
        {:ok, _} = Writer.append(w, event(1))

        case unquote(Macro.escape(target)) do
          {:sync, nth} -> inject_nth_sync_of_next_append!(fs, nth, :halt, tracked)
          {op, nth} -> FaultFs.inject(fs, op, count(fs, op) + nth, :halt)
        end

        assert {:error, %{clause: "append_failed", stage: unquote(stage)}} = Writer.append(w, event(2))
        assert {:error, %{clause: "writer_failed"}} = writer().verified(w)
        assert {:error, %{clause: "close_failed"}} = Writer.close(w)
        {w2, opened2} = reopen_after_crash!(dir, tracked)
        assert {:ok, snapshot} = writer().verified(w2)
        assert snapshot.seq == 2 and snapshot.receipt == %{seq: 2, line_sha256: snapshot.last_line_sha256}
        assert snapshot.repair == nil, "the reopen executed #{inspect(unquote(expected_repair))}; the live view is clean"
        assert (opened2.repair && opened2.repair.action) == unquote(expected_repair)
        :ok = Writer.close(w2)
      end
    end
  end

  # =================================================================================================
  describe "verification failures on an OPEN writer (read-only; closed errors; tokens invalidated)" do
    setup %{dir: dir, tracked: tracked} do
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, e1} = Writer.append(w, event(1))
      {:ok, e2} = Writer.append(w, event(2))
      {:ok, e3} = Writer.append(w, event(3))
      %{fs: fs, w: w, lines: Enum.map([e1, e2, e3], &Jason.encode!/1)}
    end

    defp rewrite!(dir, new_lines), do: File.write!(Path.join(dir, "events.jsonl"), Enum.join(new_lines, "\n") <> "\n")

    defp write_receipt!(dir, seq, hash),
      do:
        File.write!(
          Path.join(dir, "events.head"),
          Chain.encode_receipt(%{seq: seq, line_sha256: hash, updated_at: "2026-09-06T14:00:09Z"})
        )

    test "S-6a a corrupt middle line: snapshot_corrupt names the Chain clause only; no bytes; tokens invalidated", %{
      dir: dir,
      w: w,
      lines: [l1, l2, l3]
    } do
      require_snapshot!()
      assert {:ok, %{token: token}} = writer().verified(w)
      rewrite!(dir, [l1, String.replace(l2, "run_spec_loaded", @canary), l3])
      assert {:error, %{clause: "snapshot_corrupt", reason: reason} = rejection} = writer().verified(w)
      assert is_binary(reason)
      refute inspect(rejection, limit: :infinity) =~ @canary

      assert match?({:error, %{clause: "snapshot_foreign"}}, writer().append(w, event(4), fence: token)),
             "a token minted before a failed verification is dead"

      :ok = Writer.close(w)
    end

    test "S-6b an independently VALID but divergent disk head: snapshot_divergent with both seqs", %{
      dir: dir,
      w: w,
      lines: [l1, l2, _l3]
    } do
      require_snapshot!()
      # a different valid third line, correctly chained, with a matching receipt: disk verifies, memory disagrees
      other = put_in(event(3), ["data", "plan_hash"], "sha256:" <> String.duplicate("3", 64))

      stamped = Map.merge(other, %{"schema_version" => 2, "prev_line_sha256" => Chain.line_sha256(l2 <> "\n")})
      l3b = Jason.encode!(stamped)
      rewrite!(dir, [l1, l2, l3b])
      write_receipt!(dir, 3, Chain.line_sha256(l3b <> "\n"))
      assert match?({:ok, _}, Chain.verify(disk(dir))), "the rewritten disk state is valid on its own"
      assert {:error, %{clause: "snapshot_divergent", disk_seq: 3, memory_seq: 3}} = writer().verified(w)
      :ok = Writer.close(w)
    end

    test "S-6c receipt corrupt / missing on a v2 tail / mismatched: snapshot_corrupt with the Chain reason", %{
      dir: dir,
      w: w,
      lines: [_l1, l2, _l3]
    } do
      require_snapshot!()
      File.write!(Path.join(dir, "events.head"), @canary <> "\n")
      assert {:error, %{clause: "snapshot_corrupt"} = r1} = writer().verified(w)
      refute inspect(r1, limit: :infinity) =~ @canary
      File.rm!(Path.join(dir, "events.head"))

      assert match?({:error, %{clause: "snapshot_corrupt", reason: "receipt_missing"}}, writer().verified(w)),
             "absent is not legitimate on a v2 tail"

      write_receipt!(dir, 3, Chain.line_sha256(l2 <> "\n"))
      assert {:error, %{clause: "snapshot_corrupt", reason: "receipt_hash_mismatch"}} = writer().verified(w)
      :ok = Writer.close(w)
    end

    test "S-6d-j an unreadable journal: snapshot_unreadable names the journal, never the seam reason; the receipt is not read",
         %{fs: fs, w: w} do
      require_snapshot!()
      since = trace_len(fs)
      FaultFs.inject(fs, :read, fn [name] -> name == "events.jsonl" end, {:error, {:seam, @canary}})
      assert {:error, r1} = writer().verified(w)
      assert r1 == %{clause: "snapshot_unreadable", source: "journal", stage: "read"}
      refute Enum.any?(Enum.drop(FaultFs.trace(fs), since), &match?({:read, "events.head"}, &1))
      :ok = Writer.close(w)
    end

    test "S-6d-r an unreadable receipt (journal readable): snapshot_unreadable names the receipt, never the seam reason",
         %{fs: fs, w: w} do
      require_snapshot!()
      FaultFs.inject(fs, :read, fn [name] -> name == "events.head" end, {:error, {:seam, @canary}})
      assert {:error, r2} = writer().verified(w)
      assert r2 == %{clause: "snapshot_unreadable", source: "receipt", stage: "read"}
      :ok = Writer.close(w)
    end

    test "S-6e a pending advance_receipt on a warm writer: read-only success with the actual receipt and the plan, token nil, prior token dead, no writes",
         %{dir: dir, fs: fs, w: w, lines: [_l1, l2, _l3]} do
      require_snapshot!()
      assert {:ok, %{token: earlier}} = writer().verified(w)
      # the receipt one line behind: a state the write protocol can produce; the Reader plans advance_receipt
      write_receipt!(dir, 2, Chain.line_sha256(l2 <> "\n"))
      since = trace_len(fs)
      assert {:ok, snapshot} = writer().verified(w)

      assert %{
               seq: 3,
               receipt: %{seq: 2},
               repair: %{action: :advance_receipt, receipt_seq_before: 2, receipt_seq_after: 3},
               token: nil
             } = snapshot

      assert mutations(fs, since) == [], "no repair executed by the snapshot"
      assert {:ok, %{seq: 2}} = dir |> head() |> elem(1) |> Chain.decode_receipt()

      assert match?({:error, %{clause: "snapshot_foreign"}}, writer().append(w, event(4), fence: earlier)),
             "no fenced capability while a repair is pending"

      assert dir |> head() |> elem(1) |> Chain.decode_receipt() ==
               {:ok, %{seq: 2, line_sha256: Chain.line_sha256(l2 <> "\n"), updated_at: "2026-09-06T14:00:09Z"}}

      :ok = Writer.close(w)
    end

    test "S-6f a pending truncate (torn tail on disk) on a warm writer: read-only success with the plan, token nil, no writes",
         %{dir: dir, fs: fs, w: w} do
      require_snapshot!()
      append_torn!(dir)
      since = trace_len(fs)
      assert {:ok, %{seq: 3, repair: %{action: :truncate_tail}, token: nil}} = writer().verified(w)
      assert mutations(fs, since) == [], "the torn tail is reported, not truncated, by the snapshot"
      :ok = Writer.close(w)
    end
  end

  describe "receipt absence from an empty journal" do
    test "C-6 the receipt-absence fixture reaches both Reader conditions without a second Writer", %{
      dir: dir,
      tracked: tracked
    } do
      {w, opened} = open!(dir, tracked, SystemFs.new())
      assert opened.last_seq == 0
      {:ok, _} = Writer.append(w, event(1))
      File.rm!(Path.join(dir, "events.head"))
      assert {:ok, %{last_seq: 1, receipt: nil, pending_repair: %{action: :advance_receipt}}} = Reader.load(dir)
      {:ok, _} = Writer.append(w, event(2))
      File.rm!(Path.join(dir, "events.head"))
      assert {:error, %{clause: "receipt_missing"}} = Reader.load(dir)
      :ok = Writer.close(w)
    end

    test "S-6g absence rules: a FIRST v2 line without a receipt is repairable (absent receipt, pending plan, no token); two chained lines without one are refused",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      {:ok, _} = Writer.append(w, event(1))
      File.rm!(Path.join(dir, "events.head"))
      assert {:ok, %{seq: 1, receipt: nil, repair: %{action: :advance_receipt}, token: nil}} = writer().verified(w)
      {:ok, _} = Writer.append(w, event(2))
      File.rm!(Path.join(dir, "events.head"))
      assert {:error, %{clause: "snapshot_corrupt", reason: "receipt_missing"}} = writer().verified(w)
      :ok = Writer.close(w)
    end
  end

  describe "warm failed writer: the closed cause (S-M10)" do
    test "S-4b a canary-bearing non-halting append fault, then verified: the cause is EXACTLY clause + stage, no seam bytes",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, _} = Writer.append(w, event(1))
      FaultFs.inject(fs, :write, count(fs, :write) + 1, {:error, {:seam, @canary}})
      assert {:error, %{clause: "append_failed", stage: "write"}} = Writer.append(w, event(2))
      assert {:error, refusal} = writer().verified(w)

      assert refusal == %{clause: "writer_failed", cause: %{clause: "append_failed", stage: "write"}},
             "strict equality: no detail field, no seam bytes"

      refute inspect(refusal, limit: :infinity) =~ @canary
      :ok = Writer.close(w)
    end
  end

  # =================================================================================================
  describe "the fence" do
    test "S-7a stale: an intervening append supersedes the token; nothing written (journal, receipt, mutating trace); then unfenced works",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      fs = FaultFs.new()
      {w, _} = open!(dir, tracked, fs)
      {:ok, _} = Writer.append(w, event(1))
      assert {:ok, %{token: token}} = writer().verified(w)
      {:ok, _} = Writer.append(w, event(2))
      bytes = disk(dir)
      receipt = head(dir)
      since = trace_len(fs)

      assert {:error, %{clause: "snapshot_stale", expected_seq: 1, current_seq: 2}} =
               writer().append(w, event(3), fence: token)

      assert disk(dir) == bytes and head(dir) == receipt and mutations(fs, since) == [] and Writer.last_seq(w) == 2
      assert match?({:ok, %{"seq" => 3}}, Writer.append(w, event(3))), "an unfenced valid append still works"
      :ok = Writer.close(w)
    end

    test "S-7b token lifecycle table: same token at an unchanged head; retained token stale after an append; old token foreign after replacement; cleared after a failed verification",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      {:ok, _} = Writer.append(w, event(1))
      assert {:ok, %{token: t1}} = writer().verified(w)
      assert match?({:ok, %{token: ^t1}}, writer().verified(w)), "unchanged healthy head: the same token"
      assert {:ok, %{"seq" => 2}} = writer().append(w, event(2), fence: t1)
      # the head advanced; t1 is still the RETAINED entry: it authenticates and fails stale
      assert {:error, %{clause: "snapshot_stale", expected_seq: 1, current_seq: 2}} =
               writer().append(w, event(3), fence: t1)

      assert {:ok, %{token: t2}} = writer().verified(w)
      refute t2 == t1
      # replaced: t1 is now unrecognized (foreign), never stale
      assert {:error, %{clause: "snapshot_foreign"}} = writer().append(w, event(3), fence: t1)
      assert {:ok, %{"seq" => 3}} = writer().append(w, event(3), fence: t2)
      # a failed verification clears the entry: the last minted token is foreign afterwards
      assert {:ok, %{token: t3}} = writer().verified(w)
      File.write!(Path.join(dir, "events.head"), @canary <> "\n")
      assert {:error, %{clause: "snapshot_corrupt"}} = writer().verified(w)
      assert {:error, %{clause: "snapshot_foreign"}} = writer().append(w, event(4), fence: t3)
      :ok = Writer.close(w)
    end

    for {label, mutate} <- [
          {"seq", quote(do: fn t -> %{t | seq: t.seq + 1} end)},
          {"last_line_sha256", quote(do: fn t -> %{t | last_line_sha256: "sha256:" <> String.duplicate("f", 64)} end)},
          {"generation", quote(do: fn t -> %{t | generation: t.generation + 1} end)},
          {"ref", quote(do: fn t -> %{t | ref: make_ref()} end)}
        ] do
      test "S-7c a retained token with a mutated #{label} is foreign (whole-token authentication); nothing written",
           %{dir: dir, tracked: tracked} do
        require_snapshot!()
        {w, _} = open!(dir, tracked, SystemFs.new())
        {:ok, _} = Writer.append(w, event(1))
        assert {:ok, %{token: token}} = writer().verified(w)
        bytes = disk(dir)
        assert {:error, %{clause: "snapshot_foreign"}} = writer().append(w, event(2), fence: unquote(mutate).(token))
        assert disk(dir) == bytes and Writer.last_seq(w) == 1
        :ok = Writer.close(w)
      end
    end

    for {label, fence} <- [
          {"nil", nil},
          {"a bare reference", quote(do: make_ref())},
          {"a map missing the ref", quote(do: %{seq: 1, last_line_sha256: "x", generation: 1})},
          {"a map with an extra key",
           quote(do: %{seq: 1, last_line_sha256: "x", generation: 1, ref: make_ref(), extra: 1})},
          {"a wrongly typed seq", quote(do: %{seq: "1", last_line_sha256: "x", generation: 1, ref: make_ref()})}
        ] do
      test "S-7d fence: #{label} is fence_invalid (never a silent unfenced append) with a VALID event", %{
        dir: dir,
        tracked: tracked
      } do
        require_snapshot!()
        {w, _} = open!(dir, tracked, SystemFs.new())
        {:ok, _} = Writer.append(w, event(1))
        bytes = disk(dir)
        assert {:error, %{clause: "fence_invalid", field: field}} = writer().append(w, event(2), fence: unquote(fence))
        assert is_binary(field)
        assert disk(dir) == bytes and Writer.last_seq(w) == 1
        :ok = Writer.close(w)
      end
    end

    test "S-7e malformed append options (unknown key) are refused closed; precedence: options, then token, then head, then the event",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      {:ok, _} = Writer.append(w, event(1))
      assert {:ok, %{token: token}} = writer().verified(w)
      assert {:error, %{clause: "fence_invalid"}} = writer().append(w, event(2), fence: token, bogus: true)
      # a stale token with an INVALID event: the fence is judged first
      {:ok, _} = Writer.append(w, event(2))
      assert {:error, %{clause: "snapshot_stale"}} = writer().append(w, %{event(3) | "seq" => 99}, fence: token)
      # a fresh token with an invalid event: the event's own clause
      assert {:ok, %{token: fresh}} = writer().verified(w)
      assert {:error, %{clause: "seq_mismatch"}} = writer().append(w, %{event(3) | "seq" => 99}, fence: fresh)
      assert Writer.last_seq(w) == 2
      :ok = Writer.close(w)
    end

    test "S-7f a second live Writer's token at the SAME head is foreign here", %{dir: dir, tracked: tracked} do
      require_snapshot!()
      other = Path.join(dir, "other")
      File.mkdir_p!(other)
      File.write!(Path.join(other, "events.jsonl"), "", [:exclusive])
      {w, _} = open!(dir, tracked, SystemFs.new())
      {w_other, _} = open!(other, tracked, SystemFs.new(), lock: lock_opts(pid: "41002"))
      {:ok, _} = Writer.append(w, event(1))
      {:ok, _} = Writer.append(w_other, event(1))
      assert {:ok, %{token: mine}} = writer().verified(w)
      assert {:ok, %{token: theirs}} = writer().verified(w_other)
      assert {mine.seq, mine.last_line_sha256} == {theirs.seq, theirs.last_line_sha256}, "same head by construction"
      assert {:error, %{clause: "snapshot_foreign"}} = writer().append(w, event(2), fence: theirs)
      assert {:ok, _} = writer().append(w, event(2), fence: mine)
      :ok = Writer.close(w)
      :ok = Writer.close(w_other)
    end

    test "S-8 cross-generation: a generation-1 token is foreign to the generation-2 writer", %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      {:ok, _} = Writer.append(w, event(1))
      assert {:ok, %{token: token, generation: gen1}} = writer().verified(w)
      :ok = Writer.close(w)
      {w2, _} = open!(dir, tracked, SystemFs.new())
      assert {:ok, %{generation: gen2}} = writer().verified(w2)
      assert gen2 != gen1
      assert {:error, %{clause: "snapshot_foreign"}} = writer().append(w2, event(2), fence: token)
      assert Writer.last_seq(w2) == 1
      :ok = Writer.close(w2)
    end
  end

  # =================================================================================================
  describe "contention, disclosure surfaces, legacy" do
    test "S-9 the second writer is refused and the first's snapshot unaffected; the canary appears ONLY in the returned lines, never in errors, token, status or logs",
         %{dir: dir, tracked: tracked} do
      require_snapshot!()
      {w, _} = open!(dir, tracked, SystemFs.new())
      {:ok, _} = Writer.append(w, with_project(event(1), @canary))

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, second} = Writer.open(dir, fs: SystemFs.new(), clock: FixedClock, lock: lock_opts(pid: "41002"))
          refute inspect(second, limit: :infinity) =~ @canary
          assert {:ok, %{seq: 1, lines: [line], token: token} = snapshot} = writer().verified(w)
          assert line =~ @canary, "the returned evidence IS the accepted line"
          refute inspect(token, limit: :infinity) =~ @canary
          refute inspect(Map.delete(snapshot, :lines), limit: :infinity) =~ @canary
          refute inspect(:sys.get_status(w), limit: :infinity) =~ @canary
        end)

      refute log =~ @canary
      :ok = Writer.close(w)
    end

    test "S-10 a legacy envelope-1 journal snapshots (receipt nil is legitimate; lines = the fixture); not rejected", %{
      dir: dir,
      tracked: tracked
    } do
      require_snapshot!()
      File.cp!(@legacy_fixture, Path.join(dir, "events.jsonl"))
      {:ok, legacy} = Chain.verify(disk(dir))
      {w, %{envelope_version: 1}} = open!(dir, tracked, SystemFs.new())
      assert {:ok, snapshot} = writer().verified(w)
      assert snapshot.envelope_version == 1 and snapshot.receipt == nil and snapshot.seq == legacy.count
      assert snapshot.lines == legacy.lines and snapshot.last_line_sha256 == legacy.last_line_sha256
      :ok = Writer.close(w)
    end
  end
end
