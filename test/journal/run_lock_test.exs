defmodule AiOrchestrator.Journal.RunLockTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  setup do
    dir = Path.join(System.tmp_dir!(), "run_lock_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    FixedClock.reset()
    %{dir: dir}
  end

  defp opts(overrides) do
    Keyword.merge(
      [
        pid: "41001",
        pid_start: "start_41001",
        supervisor_instance: "sup_0001",
        token: "token_a",
        clock: FixedClock,
        owner_status: fn _metadata -> :live end
      ],
      overrides
    )
  end

  defp gen_path(dir, gen), do: Path.join(dir, "run.lock.#{gen}")
  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))

  defp generations(dir) do
    dir
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      case Regex.run(~r/^run\.lock\.([1-9][0-9]{0,11})$/, name) do
        [_, digits] -> [String.to_integer(digits)]
        nil -> []
      end
    end)
    |> Enum.sort()
  end

  defp lock_file(dir, gen), do: dir |> gen_path(gen) |> File.read!() |> Jason.decode!()
  defp lock_file(dir), do: lock_file(dir, List.last(generations(dir)))

  defp metadata(overrides) do
    Map.merge(
      %{
        "schema" => "ai-orchestrator/run-lock",
        "schema_version" => 1,
        "state" => "held",
        "pid" => "999",
        "pid_start" => "start_999",
        "supervisor_instance" => "sup_other",
        "token" => "token_other",
        "acquired_at" => "2026-09-01T12:00:00Z"
      },
      overrides
    )
  end

  defp write_generation(dir, gen, meta), do: File.write!(gen_path(dir, gen), Jason.encode!(meta) <> "\n")

  # Claimant B pauses once, inside its first liveness verdict on `pause_pid`; the test acts, then
  # B resumes. Later evaluations of the same holder (compaction) do not pause again.
  defp delayed_claimant(fs, dir, pause_pid, verdicts, overrides) do
    test = self()

    status = fn
      %{"pid" => ^pause_pid} = meta ->
        if !Process.get(:paused_once) do
          Process.put(:paused_once, true)
          send(test, {:paused, self()})

          receive do
            :continue -> :ok
          end
        end

        verdicts.(meta)

      meta ->
        verdicts.(meta)
    end

    Task.async(fn ->
      RunLock.acquire(fs, dir, opts(Keyword.merge([token: "token_b", owner_status: status], overrides)))
    end)
  end

  test "a fresh acquire publishes generation 1 durably and holds only after the rescan", %{dir: dir} do
    fs = FaultFs.new()

    assert {:ok, %{path: path, generation: 1, token: "token_a", owner: owner}} =
             RunLock.acquire(fs, dir, opts([]))

    assert path == gen_path(dir, 1)

    assert %{
             "schema" => "ai-orchestrator/run-lock",
             "schema_version" => 1,
             "state" => "held",
             "pid" => "41001",
             "pid_start" => "start_41001",
             "supervisor_instance" => "sup_0001",
             "token" => "token_a",
             "acquired_at" => "2026-09-01T12:00:00Z"
           } = lock_file(dir, 1)

    assert owner == lock_file(dir, 1)

    # Metadata is complete before the name exists; the rescan precedes the held result.
    assert [
             {:list_dir, _},
             {:open, "run.lock.1.token_a.tmp", [:exclusive]},
             {:write, _},
             {:sync},
             {:close},
             {:link, "run.lock.1.token_a.tmp", "run.lock.1"},
             {:rm, "run.lock.1.token_a.tmp"},
             {:dir_sync, _},
             {:list_dir, _},
             {:read, "run.lock.1"}
           ] = FaultFs.trace(fs)

    refute File.exists?(Path.join(dir, "run.lock.1.token_a.tmp"))
  end

  test "a live owner refuses a second holder and names the owner", %{dir: dir} do
    fs = SystemFs.new()
    {:ok, _held} = RunLock.acquire(fs, dir, opts([]))

    assert {:error, %{clause: "run_locked", owner: %{"pid" => "41001", "token" => "token_a"}}} =
             RunLock.acquire(fs, dir, opts(token: "token_b", owner_status: fn _metadata -> :live end))

    assert generations(dir) == [1]
  end

  test "the process's own real identity refuses itself, and release publishes a tombstone", %{dir: dir} do
    fs = SystemFs.new()
    real = [supervisor_instance: "sup_0001", clock: FixedClock]
    assert {:ok, held} = RunLock.acquire(fs, dir, real)
    assert lock_file(dir, 1)["pid"] == System.pid()
    assert {:error, %{clause: "run_locked"}} = RunLock.acquire(fs, dir, real)
    assert :ok = RunLock.release(fs, held)
    assert generations(dir) == [2]
    assert %{"state" => "released", "token" => token} = lock_file(dir, 2)
    assert token == held.token
    assert :none = RunLock.owner(fs, dir)
    assert {:ok, %{generation: 3}} = RunLock.acquire(fs, dir, real)
    assert generations(dir) == [3]
  end

  test "a dead owner, including a reused pid with a new start time, is reclaimed at the next generation",
       %{dir: dir} do
    fs = FaultFs.new()
    {:ok, _held} = RunLock.acquire(fs, dir, opts([]))
    dead_if_original = fn meta -> if meta["pid_start"] == "start_41001", do: :dead, else: :live end

    assert {:ok, %{generation: 2, token: "token_b"}} =
             RunLock.acquire(
               fs,
               dir,
               opts(pid: "41001", pid_start: "start_reused", token: "token_b", owner_status: dead_if_original)
             )

    assert generations(dir) == [2]
    assert %{"pid_start" => "start_reused", "token" => "token_b"} = lock_file(dir, 2)
    trace = FaultFs.trace(fs)
    assert {:rm, "run.lock.1"} in trace
    refute {:rm, "run.lock.2"} in trace
  end

  test "an owner whose liveness cannot be determined is never reclaimed", %{dir: dir} do
    fs = SystemFs.new()
    {:ok, _held} = RunLock.acquire(fs, dir, opts([]))

    assert {:error, %{clause: "lock_unavailable"}} =
             RunLock.acquire(fs, dir, opts(token: "token_b", owner_status: fn _ -> {:error, %{"reason" => "ps"}} end))

    assert generations(dir) == [1]
    assert lock_file(dir, 1)["token"] == "token_a"
  end

  test "a highest lock with partial or empty bytes fails closed and is never reclaimed", %{dir: dir} do
    fs = FaultFs.new()

    for bytes <- ["", "{\"pid\":\"41"] do
      File.write!(gen_path(dir, 1), bytes)

      assert {:error, %{clause: "lock_unavailable", detail: detail}} =
               RunLock.acquire(fs, dir, opts(owner_status: fn _ -> :dead end))

      assert detail =~ "lock_unreadable"
      assert File.read!(gen_path(dir, 1)) == bytes
    end

    refute Enum.any?(FaultFs.trace(fs), &match?({:link, _, _}, &1))
    assert generations(dir) == [1]
  end

  test "lock metadata must be the complete closed shape; anything else fails closed untouched", %{dir: dir} do
    fs = FaultFs.new()
    good = metadata(%{})

    variants =
      Enum.map(Map.keys(good), fn key -> {"missing #{key}", Map.delete(good, key)} end) ++
        [
          {"wrong schema", Map.put(good, "schema", "other")},
          {"wrong schema_version", Map.put(good, "schema_version", 2)},
          {"unknown state", Map.put(good, "state", "other")},
          {"empty token", Map.put(good, "token", "")},
          {"non-string pid", Map.put(good, "pid", 41_001)},
          {"non-string acquired_at", Map.put(good, "acquired_at", 1)},
          {"extra key", Map.put(good, "extra", true)}
        ]

    for {label, meta} <- variants do
      bytes = Jason.encode!(meta) <> "\n"
      File.write!(gen_path(dir, 1), bytes)

      result = RunLock.acquire(fs, dir, opts(token: "token_b", owner_status: fn _ -> :dead end))

      assert match?({:error, %{clause: "lock_unavailable", detail: _}}, result), label

      {:error, %{detail: detail}} = result

      assert detail =~ "lock_unreadable", label
      assert File.read!(gen_path(dir, 1)) == bytes, label
      assert match?({:error, %{clause: "lock_unreadable"}}, RunLock.owner(fs, dir)), label
    end

    refute Enum.any?(FaultFs.trace(fs), &match?({:rm, _}, &1))
  end

  test "a failure after the lock is visible rolls it back and reports the rollback", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :dir_sync, 1, {:error, :eio})

    assert {:error, %{clause: "lock_unavailable", rollback: "removed", detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert detail =~ "eio"
    assert :none = RunLock.owner(fs, dir)
    refute File.exists?(Path.join(dir, "run.lock.1.token_a.tmp"))
    assert {:ok, %{generation: 1}} = RunLock.acquire(fs, dir, opts([]))
  end

  test "a failed rollback is reported as cleanup_required with evidence", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :dir_sync, 1, {:error, :eio})

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", token: "token_a", owner: %{"token" => "token_a"}, detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert detail =~ "eio" and detail =~ "eacces"
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a rollback whose own directory fsync fails says so", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :dir_sync, 1, {:error, :eio})
    FaultFs.inject(fs, :dir_sync, 2, {:error, :eio})

    assert {:error, %{clause: "lock_unavailable", rollback: "removed_unsynced", detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert detail =~ "eio"
    assert :none = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, _held} = RunLock.acquire(fs, dir, opts([]))
  end

  test "a listing failure fails closed before anything is published", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :list_dir, 1, {:error, :eio})
    assert {:error, %{clause: "lock_unavailable", detail: detail}} = RunLock.acquire(fs, dir, opts([]))
    assert detail =~ "list_dir"
    assert generations(dir) == []
  end

  test "a highest generation that keeps vanishing exhausts the bounded retries", %{dir: dir} do
    fs = FaultFs.new()
    write_generation(dir, 1, metadata(%{}))

    FaultFs.inject(
      fs,
      :read,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :enoent}
    )

    assert {:error, %{clause: "lock_unavailable", detail: detail}} = RunLock.acquire(fs, dir, opts([]))
    assert detail =~ "exhausted"
    assert generations(dir) == [1]
  end

  test "release removes only a lock this holder still owns", %{dir: dir} do
    fs = SystemFs.new()
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))
    {:ok, other} = RunLock.acquire(fs, dir, opts(token: "token_b", owner_status: fn _ -> :dead end))
    assert {:error, %{clause: "not_owner"}} = RunLock.release(fs, held)
    assert lock_file(dir)["token"] == "token_b"
    assert :ok = RunLock.release(fs, other)
    assert generations(dir) == [3]
    assert :none = RunLock.owner(fs, dir)
    assert {:error, %{clause: "not_owner"}} = RunLock.release(fs, other)
  end

  test "owner/2 reports the holder, absence, a tombstone, or an unreadable file", %{dir: dir} do
    fs = SystemFs.new()
    assert :none = RunLock.owner(fs, dir)
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(fs, dir)
    :ok = RunLock.release(fs, held)
    assert :none = RunLock.owner(fs, dir)
    File.write!(gen_path(dir, 5), "garbage")
    assert {:error, %{clause: "lock_unreadable"}} = RunLock.owner(fs, dir)
  end

  test "a write fault while publishing leaves no lock behind and is reported", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :write, 1, {:error, :enospc})
    assert {:error, %{clause: "lock_unavailable", detail: detail}} = RunLock.acquire(fs, dir, opts([]))
    assert detail =~ "enospc"
    assert generations(dir) == []
    refute File.exists?(Path.join(dir, "run.lock.1.token_a.tmp"))
    assert {:ok, _held} = RunLock.acquire(fs, dir, opts([]))
  end

  test "a missing supervisor_instance is refused before touching the disk", %{dir: dir} do
    fs = FaultFs.new()

    assert {:error, %{clause: "lock_unavailable"}} =
             RunLock.acquire(fs, dir, Keyword.delete(opts([]), :supervisor_instance))

    assert FaultFs.trace(fs) == []
  end

  test "malformed lock-family names fail closed; candidate temps and unrelated files are ignored", %{dir: dir} do
    fs = FaultFs.new()

    for name <- ["run.lock.9x", "run.lock.0", "run.lock.01", "run.lock." <> String.duplicate("9", 30), "run.lock.reclaim"] do
      File.write!(Path.join(dir, name), "anything")

      assert match?({:error, %{clause: "malformed_lock_family", entries: [^name]}}, RunLock.acquire(fs, dir, opts([]))),
             name

      assert generations(dir) == [], name
      assert File.read!(Path.join(dir, name)) == "anything", name
      File.rm!(Path.join(dir, name))
    end

    refute Enum.any?(FaultFs.trace(fs), &match?({:link, _, _}, &1))

    File.write!(Path.join(dir, "run.lock.7.token_z.tmp"), "stale candidate")
    File.write!(Path.join(dir, "events.jsonl"), "")
    assert {:ok, %{generation: 1}} = RunLock.acquire(fs, dir, opts([]))
    assert File.exists?(Path.join(dir, "run.lock.7.token_z.tmp"))
  end

  test "a withdrawal that cannot remove the file reports cleanup_required instead of the original outcome",
       %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :list_dir, 2, {:error, :eio})

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", token: "token_a", detail: detail} = rejection} =
             RunLock.acquire(fs, dir, opts([]))

    assert Path.basename(rejection.path) == "run.lock.1"
    assert detail =~ "eacces"
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a withdrawal whose removal succeeds but whose directory fsync fails says so", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :list_dir, 2, {:error, :eio})
    FaultFs.inject(fs, :dir_sync, 2, {:error, :eio})

    assert {:error, %{clause: "lock_unavailable", rollback: "removed_unsynced"}} = RunLock.acquire(fs, dir, opts([]))
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "losing to a higher generation with a failed withdrawal reports cleanup_required", %{dir: dir} do
    fs = FaultFs.new()
    write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

    verdicts = fn
      %{"pid" => "777"} -> :dead
      _ -> :live
    end

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.2"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    claimant = delayed_claimant(fs, dir, "777", verdicts, [])
    assert_receive {:paused, claimant_pid}, 5_000
    write_generation(dir, 4, metadata(%{"pid" => "888", "token" => "token_c"}))
    send(claimant_pid, :continue)

    assert {:error, %{clause: "cleanup_required", token: "token_b"} = rejection} = Task.await(claimant, 5_000)
    assert Path.basename(rejection.path) == "run.lock.2"
    assert generations(dir) == [1, 2, 4]
  end

  test "own bytes that are unreadable or foreign after the link fail closed without abandoning the file",
       %{dir: dir} do
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :read,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eio}
    )

    assert {:error, %{clause: "cleanup_required", token: "token_a"} = rejection} = RunLock.acquire(fs, dir, opts([]))
    assert Path.basename(rejection.path) == "run.lock.1"
    assert generations(dir) == [1]

    File.rm!(gen_path(dir, 1))
    foreign = Jason.encode!(metadata(%{"token" => "token_foreign"})) <> "\n"
    fs2 = FaultFs.new()

    FaultFs.inject(
      fs2,
      :read,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:return, {:ok, foreign}}
    )

    assert {:error, %{clause: "ownership_lost", token: "token_a"}} = RunLock.acquire(fs2, dir, opts([]))
    assert lock_file(dir, 1)["token"] == "token_a"
    refute {:rm, "run.lock.1"} in FaultFs.trace(fs2)
  end

  test "an uncompactable dead lower generation blocks acquisition with evidence describing that file",
       %{dir: dir} do
    fs = FaultFs.new()
    dead = metadata(%{"pid" => "777", "token" => "token_dead", "supervisor_instance" => "sup_dead"})
    write_generation(dir, 1, dead)

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", path: path, owner: owner, token: "token_dead", detail: detail}} =
             RunLock.acquire(fs, dir, opts(owner_status: fn _ -> :dead end))

    assert path == gen_path(dir, 1)
    assert owner == dead
    assert owner == lock_file(dir, 1)
    assert detail =~ "eacces"
    assert generations(dir) == [1]
  end

  test "an uncompactable released tombstone blocks acquisition with evidence describing that file",
       %{dir: dir} do
    fs = FaultFs.new()
    tombstone = metadata(%{"pid" => "778", "token" => "token_released", "state" => "released"})
    write_generation(dir, 3, tombstone)

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.3"] -> true
        _ -> false
      end,
      {:error, :eperm}
    )

    assert {:error, %{clause: "cleanup_required", path: path, owner: owner, token: "token_released"}} =
             RunLock.acquire(fs, dir, opts([]))

    assert path == gen_path(dir, 3)
    assert owner == tombstone
    assert owner == lock_file(dir, 3)
    assert generations(dir) == [3]
  end

  test "a lower file removed but not durably synced is reported as removed_unsynced, never as remaining",
       %{dir: dir} do
    fs = FaultFs.new()
    dead = metadata(%{"pid" => "777", "token" => "token_dead"})
    write_generation(dir, 1, dead)
    FaultFs.inject(fs, :dir_sync, 2, {:error, :eio})

    assert {:error,
            %{clause: "lock_unavailable", rollback: "removed_unsynced", path: path, owner: ^dead, token: "token_dead"}} =
             RunLock.acquire(fs, dir, opts(owner_status: fn _ -> :dead end))

    assert path == gen_path(dir, 1)
    refute File.exists?(path)
    assert generations(dir) == []
  end

  test "release names the leg that failed: old file remaining versus removed but unsynced", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "release_incomplete", path: path, owner: owner, token: "token_a"}} =
             RunLock.release(fs, held)

    assert path == gen_path(dir, 1)
    assert owner == held.owner
    assert generations(dir) == [1, 2]
    assert :none = RunLock.owner(SystemFs.new(), dir)

    fs2 = FaultFs.new()
    {:ok, held2} = RunLock.acquire(fs2, dir, opts(token: "token_b"))
    FaultFs.inject(fs2, :dir_sync, count(fs2, :dir_sync) + 2, {:error, :eio})

    assert {:error, %{clause: "release_removed_unsynced", path: path2, token: "token_b"}} = RunLock.release(fs2, held2)
    assert path2 == held2.path
    refute File.exists?(path2)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "a candidate temp that cannot be removed is reported, never hidden, on every path", %{dir: dir} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :write, 1, {:error, :enospc})

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1.token_a.tmp"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", path: path, token: "token_a", detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert Path.basename(path) == "run.lock.1.token_a.tmp"
    assert detail =~ "enospc" and detail =~ "eacces"
    assert File.exists?(path)
    File.rm!(path)

    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1.token_a.tmp"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "cleanup_required", path: path, token: "token_a", detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert Path.basename(path) == "run.lock.1.token_a.tmp"
    assert detail =~ "retracted"
    assert :none = RunLock.owner(SystemFs.new(), dir)
    assert generations(dir) == []
    File.rm!(path)

    fs = FaultFs.new()
    FaultFs.inject(fs, :rm, fn [name] -> name in ["run.lock.1.token_a.tmp", "run.lock.1"] end, {:error, :eacces})

    assert {:error, %{clause: "cleanup_required", path: path, token: "token_a", detail: detail}} =
             RunLock.acquire(fs, dir, opts([]))

    assert Path.basename(path) == "run.lock.1"
    assert detail =~ "run.lock.1.token_a.tmp" and detail =~ "retraction failed"
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a link that loses to a higher generation still reports a temp it could not remove", %{dir: dir} do
    fs = FaultFs.new()
    write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

    verdicts = fn
      %{"pid" => "777"} -> :dead
      _ -> :live
    end

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.2.token_b.tmp"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    claimant = delayed_claimant(fs, dir, "777", verdicts, [])
    assert_receive {:paused, claimant_pid}, 5_000
    write_generation(dir, 2, metadata(%{"pid" => "888", "token" => "token_c"}))
    send(claimant_pid, :continue)

    assert {:error, %{clause: "cleanup_required", path: path, token: "token_b"}} = Task.await(claimant, 5_000)
    assert Path.basename(path) == "run.lock.2.token_b.tmp"
    assert lock_file(dir, 2)["token"] == "token_c"
  end

  test "a candidate this attempt did not create is never unlinked and names a conflict", %{dir: dir} do
    fs = FaultFs.new()
    stale = Path.join(dir, "run.lock.1.token_a.tmp")
    File.write!(stale, "sentinel")

    assert {:error, %{clause: "candidate_conflict", path: ^stale, token: "token_a", claimant: claimant} = rejection} =
             RunLock.acquire(fs, dir, opts([]))

    # The metadata carried is this attempt's own identity, never bytes decoded from the candidate.
    refute Map.has_key?(rejection, :owner)
    assert claimant["token"] == "token_a" and claimant["state"] == "held"
    assert File.read!(stale) == "sentinel"
    assert generations(dir) == []
    refute Enum.any?(FaultFs.trace(fs), &match?({:rm, _}, &1))
    refute Enum.any?(FaultFs.trace(fs), &match?({:link, _, _}, &1))
  end

  test "a temp already absent at removal is cleanup achieved, after a won and after a lost link", %{dir: dir} do
    fs = FaultFs.new()
    tmp = Path.join(dir, "run.lock.1.token_a.tmp")

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1.token_a.tmp"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(tmp) == :ok end}
    )

    assert {:ok, %{generation: 1}} = RunLock.acquire(fs, dir, opts([]))
    refute File.exists?(tmp)
    assert {:rm, "run.lock.1.token_a.tmp"} in FaultFs.trace(fs)

    fs2 = FaultFs.new()
    File.rm!(gen_path(dir, 1))
    write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

    verdicts = fn
      %{"pid" => "777"} -> :dead
      _ -> :live
    end

    tmp2 = Path.join(dir, "run.lock.2.token_b.tmp")

    FaultFs.inject(
      fs2,
      :rm,
      fn
        ["run.lock.2.token_b.tmp"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(tmp2) == :ok end}
    )

    claimant = delayed_claimant(fs2, dir, "777", verdicts, [])
    assert_receive {:paused, claimant_pid}, 5_000
    write_generation(dir, 2, metadata(%{"pid" => "888", "token" => "token_c"}))
    send(claimant_pid, :continue)

    assert {:error, %{clause: "run_locked", owner: %{"token" => "token_c"}}} = Task.await(claimant, 5_000)
    refute File.exists?(tmp2)
  end

  test "release preserves the path, owner, and cleanup outcome of a temp it could not remove", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.2.token_a.tmp"] -> true
        _ -> false
      end,
      {:error, :eacces}
    )

    assert {:error, %{clause: "release_failed", cleanup: "cleanup_required", path: path, token: "token_a"} = rejection} =
             RunLock.release(fs, held)

    assert Path.basename(path) == "run.lock.2.token_a.tmp"
    assert rejection.detail =~ "retracted"
    # The file described is the tombstone candidate, so its metadata is the released record.
    assert rejection.owner == Map.put(held.owner, "state", "released")
    assert generations(dir) == [1]
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(SystemFs.new(), dir)
    File.rm!(path)

    fs2 = FaultFs.new()
    FaultFs.inject(fs2, :rm, fn [name] -> name in ["run.lock.2.token_a.tmp", "run.lock.2"] end, {:error, :eacces})

    assert {:error,
            %{clause: "release_incomplete", cleanup: "cleanup_required", path: path2, token: "token_a"} = rejection2} =
             RunLock.release(fs2, held)

    assert Path.basename(path2) == "run.lock.2"
    assert rejection2.detail =~ "run.lock.2.token_a.tmp" and rejection2.detail =~ "retraction failed"
    assert generations(dir) == [1, 2]
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "release names a candidate conflict during tombstone publication", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))
    File.write!(Path.join(dir, "run.lock.2.token_a.tmp"), "sentinel")

    assert {:error, %{clause: "release_failed", cleanup: "candidate_conflict", path: path, token: "token_a"} = rejection} =
             RunLock.release(fs, held)

    # The class is structural: the wording carries no discriminator phrase, and the claimant, not an
    # observed owner, is reported for a candidate nothing was decoded from.
    refute rejection.detail =~ "already exists"
    refute Map.has_key?(rejection, :owner)
    assert rejection.claimant["token"] == "token_a"
    assert Path.basename(path) == "run.lock.2.token_a.tmp"
    assert File.read!(path) == "sentinel"
    assert {:ok, %{"token" => "token_a"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a lower generation that vanished before compaction is cleanup achieved, or unsynced if its sync fails",
       %{dir: dir} do
    fs = FaultFs.new()
    write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))
    lower = gen_path(dir, 1)

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(lower) == :ok end}
    )

    assert {:ok, %{generation: 2}} = RunLock.acquire(fs, dir, opts(owner_status: fn _ -> :dead end))
    assert generations(dir) == [2]

    fs2 = FaultFs.new()
    File.rm!(gen_path(dir, 2))
    write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

    FaultFs.inject(
      fs2,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(lower) == :ok end}
    )

    FaultFs.inject(fs2, :dir_sync, 2, {:error, :eio})

    assert {:error, %{clause: "lock_unavailable", rollback: "removed_unsynced", path: ^lower}} =
             RunLock.acquire(fs2, dir, opts(owner_status: fn _ -> :dead end))

    refute File.exists?(lower)
    assert generations(dir) == []
  end

  test "an old held generation that vanished after the tombstone is a clean release, or unsynced", %{dir: dir} do
    fs = FaultFs.new()
    {:ok, held} = RunLock.acquire(fs, dir, opts([]))
    own = held.path

    FaultFs.inject(
      fs,
      :rm,
      fn
        ["run.lock.1"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(own) == :ok end}
    )

    assert :ok = RunLock.release(fs, held)
    assert generations(dir) == [2]
    assert :none = RunLock.owner(SystemFs.new(), dir)

    fs2 = FaultFs.new()
    {:ok, held2} = RunLock.acquire(fs2, dir, opts(token: "token_b"))
    own2 = held2.path

    FaultFs.inject(
      fs2,
      :rm,
      fn
        ["run.lock.3"] -> true
        _ -> false
      end,
      {:hook, fn -> File.rm(own2) == :ok end}
    )

    FaultFs.inject(fs2, :dir_sync, count(fs2, :dir_sync) + 2, {:error, :eio})
    assert {:error, %{clause: "release_removed_unsynced", path: ^own2, token: "token_b"}} = RunLock.release(fs2, held2)
    refute File.exists?(own2)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  describe "delayed claimants (peer review scenarios)" do
    test "a claimant delayed after judging the holder dead loses to the claimant that took over", %{dir: dir} do
      fs = SystemFs.new()
      {:ok, _held} = RunLock.acquire(fs, dir, opts([]))

      verdicts = fn
        %{"token" => "token_a"} -> :dead
        _ -> :live
      end

      claimant = delayed_claimant(fs, dir, "41001", verdicts, [])
      assert_receive {:paused, claimant_pid}, 5_000

      assert {:ok, %{generation: 2}} =
               RunLock.acquire(fs, dir, opts(token: "token_c", pid: "888", pid_start: "s888", owner_status: verdicts))

      send(claimant_pid, :continue)
      assert {:error, %{clause: "run_locked", owner: %{"token" => "token_c"}}} = Task.await(claimant, 5_000)
      assert generations(dir) == [2]
      assert lock_file(dir, 2)["token"] == "token_c"
    end

    test "a stale claimant that links a freed lower generation withdraws when a higher holder is live",
         %{dir: dir} do
      fs = SystemFs.new()
      write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

      verdicts = fn
        %{"pid" => "777"} -> :dead
        _ -> :live
      end

      claimant = delayed_claimant(fs, dir, "777", verdicts, [])
      assert_receive {:paused, claimant_pid}, 5_000

      write_generation(dir, 4, metadata(%{"pid" => "888", "token" => "token_c"}))
      send(claimant_pid, :continue)

      assert {:error, %{clause: "run_locked", owner: %{"token" => "token_c"}}} = Task.await(claimant, 5_000)
      assert generations(dir) == [1, 4]
      assert lock_file(dir, 4)["token"] == "token_c"
    end

    test "a stale claimant becomes the owner only after the higher holder has released", %{dir: dir} do
      fs = SystemFs.new()
      write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

      verdicts = fn
        %{"pid" => "777"} -> :dead
        _ -> :live
      end

      claimant = delayed_claimant(fs, dir, "777", verdicts, [])
      assert_receive {:paused, claimant_pid}, 5_000

      write_generation(dir, 4, metadata(%{"pid" => "888", "token" => "token_c", "state" => "released"}))
      send(claimant_pid, :continue)

      assert {:ok, %{generation: 5, token: "token_b"}} = Task.await(claimant, 5_000)
      refute File.exists?(gen_path(dir, 2))
      assert lock_file(dir, 5)["token"] == "token_b"
    end

    test "a stale claimant withdraws below a crashed higher holder and retries above it", %{dir: dir} do
      fs = SystemFs.new()
      write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

      verdicts = fn
        %{"pid" => "777"} -> :dead
        %{"pid" => "999"} -> :dead
        _ -> :live
      end

      claimant = delayed_claimant(fs, dir, "777", verdicts, [])
      assert_receive {:paused, claimant_pid}, 5_000

      write_generation(dir, 4, metadata(%{"pid" => "999", "token" => "token_crashed"}))
      send(claimant_pid, :continue)

      assert {:ok, %{generation: 5, token: "token_b"}} = Task.await(claimant, 5_000)
      assert generations(dir) == [5]
    end

    test "a link loser rescans from the highest generation, never trusting its old target", %{dir: dir} do
      fs = SystemFs.new()
      write_generation(dir, 1, metadata(%{"pid" => "777", "token" => "token_dead"}))

      verdicts = fn
        %{"pid" => "777"} -> :dead
        _ -> :live
      end

      claimant = delayed_claimant(fs, dir, "777", verdicts, [])
      assert_receive {:paused, claimant_pid}, 5_000

      write_generation(dir, 2, metadata(%{"pid" => "888", "token" => "token_c", "state" => "released"}))
      write_generation(dir, 3, metadata(%{"pid" => "889", "token" => "token_d"}))
      send(claimant_pid, :continue)

      assert {:error, %{clause: "run_locked", owner: %{"token" => "token_d"}}} = Task.await(claimant, 5_000)
      assert generations(dir) == [1, 2, 3]
    end
  end
end
