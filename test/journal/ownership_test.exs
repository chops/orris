defmodule AiOrchestrator.Journal.OwnershipTest do
  @moduledoc """
  The arbiter closes one ambiguity `AiOrchestrator.Journal.RunLock` cannot:
  two writers inside one BEAM share an OS pid, so a brutally killed writer
  strands a lock naming a process that is still alive.

  Every lock here is written with `owner_status: fn _ -> :live end`, which is
  what that shared pid really looks like to `RunLock`: alive. A test that
  reported the holder dead would be testing the cross-process path instead,
  and would pass without an arbiter at all.
  """

  # The host-root case registers its arbiter under a fixed name, and several
  # cases exercise the registered arbiter the application starts.
  use ExUnit.Case, async: false

  alias AiOrchestrator.Application, as: Host
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs

  @arbiter __MODULE__.HostArbiter

  setup do
    dir = Path.join(System.tmp_dir!(), "ownership_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp lock_opts(overrides \\ []) do
    Keyword.merge(
      [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
      overrides
    )
  end

  defp open(dir, overrides \\ []) do
    Writer.open(dir, Keyword.merge([fs: SystemFs.new(), lock: lock_opts()], overrides))
  end

  # A hard kill is the only way to leave a lock behind without releasing it,
  # which is the state the arbiter exists to resolve. The unlink is only so
  # the kill does not take this test process with it; nothing here depends on
  # a writer surviving its caller.
  defp kill(pid) do
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
    :ok
  end

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

  defp lock_path(run, generation), do: Path.join(run, "run.lock." <> Integer.to_string(generation))

  # A run directory per matrix row, so one row's disk state cannot reach the next.
  defp fresh_run(dir, name) do
    run = Path.join(dir, name)
    File.mkdir_p!(run)
    File.write!(Path.join(run, "events.jsonl"), "", [:exclusive])
    run
  end

  # Rewrites one field of a lock in place, leaving its generation (the file
  # name) and its token untouched.
  defp mutate_lock!(run, generation, field, value) do
    path = lock_path(run, generation)
    metadata = path |> File.read!() |> Jason.decode!()
    File.write!(path, Jason.encode!(Map.put(metadata, field, value)) <> "\n")
    File.read!(path)
  end

  defp deep_binaries(term) when is_binary(term), do: [term]
  defp deep_binaries(term) when is_map(term), do: Enum.flat_map(term, &deep_binaries/1)
  defp deep_binaries(term) when is_list(term), do: Enum.flat_map(term, &deep_binaries/1)
  defp deep_binaries(term) when is_tuple(term), do: term |> Tuple.to_list() |> deep_binaries()
  defp deep_binaries(_term), do: []

  defp deep_keys(term) when is_map(term), do: Enum.flat_map(term, fn {k, v} -> [k | deep_keys(v)] end)
  defp deep_keys(term) when is_list(term), do: Enum.flat_map(term, &deep_keys/1)
  defp deep_keys(term) when is_tuple(term), do: term |> Tuple.to_list() |> deep_keys()
  defp deep_keys(_term), do: []

  defp wait_until(_fun, 0), do: flunk("condition never held")

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, tries - 1)
    end
  end

  test "a killed writer's stranded lock is reclaimed by the next open in the same BEAM", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    :ok = kill(writer)
    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)

    assert {:ok, replacement, %{last_seq: 0}} = open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))
    assert {:ok, %{"supervisor_instance" => "sup_0002"}} = RunLock.owner(SystemFs.new(), dir)
    :ok = Writer.close(replacement)
  end

  test "the reclaim happens inside the acquire transaction, before any claim", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    :ok = kill(writer)

    # The same disk state, claimed without the arbiter: this is what a writer
    # that acquired first would see, and why it may not acquire first.
    assert {:error, %{clause: "run_locked"}} =
             RunLock.acquire(SystemFs.new(), dir, lock_opts(supervisor_instance: "sup_bare"))

    assert {:ok, replacement, _} = open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))
    :ok = Writer.close(replacement)
  end

  test "a second live writer is refused before anything on disk is touched", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    assert generations(dir) == [1]

    assert {:error, %{clause: "second_live_writer", generation: 1}} =
             open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))

    assert generations(dir) == [1]
    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)
    :ok = Writer.close(writer)
  end

  test "a registration the disk has already superseded is dropped, never released", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    :ok = kill(writer)

    # A claimant outside this arbiter takes the lock over legitimately, so the
    # registration still names a generation and token that are no longer held.
    {:ok, _foreign} =
      RunLock.acquire(SystemFs.new(), dir, lock_opts(supervisor_instance: "sup_foreign", owner_status: fn _ -> :dead end))

    assert {:error, %{clause: "run_locked"}} = open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))
    assert {:ok, %{"supervisor_instance" => "sup_foreign"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a superseded registration does not block an acquisition RunLock would serve", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    :ok = kill(writer)

    {:ok, _foreign} =
      RunLock.acquire(SystemFs.new(), dir, lock_opts(supervisor_instance: "sup_foreign", owner_status: fn _ -> :dead end))

    assert {:ok, replacement, _} =
             open(dir, lock: lock_opts(supervisor_instance: "sup_0002", owner_status: fn _ -> :dead end))

    assert {:ok, %{"supervisor_instance" => "sup_0002"}} = RunLock.owner(SystemFs.new(), dir)
    :ok = Writer.close(replacement)
  end

  test "a reclaim that cannot release fails closed and names only bounded facts", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    {:ok, %{"token" => token}} = RunLock.owner(SystemFs.new(), dir)
    :ok = kill(writer)

    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :link,
      fn
        [_tmp, "run.lock.2"] -> true
        _other -> false
      end,
      {:error, :eacces}
    )

    assert {:error, rejection} = open(dir, fs: fs, lock: lock_opts(supervisor_instance: "sup_0002"))

    assert %{
             clause: "reclaim_failed",
             generation: 1,
             lock_basename: "run.lock.1",
             cause_clause: "release_failed",
             lock_state: :still_held
           } = rejection

    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)

    # The `RunLock` release rejection this was built from carries the raw
    # token, the lock's absolute path and the whole decoded owner map. This is
    # the arbiter's own error, rebuilt, so none of that may appear at any
    # depth — and no key beyond the five the contract names.
    binaries = deep_binaries(rejection)
    refute token in binaries
    refute Enum.any?(binaries, &String.contains?(&1, dir))
    refute Enum.any?(binaries, &(&1 in ["sup_0001", "41001", "start_41001"]))
    assert deep_keys(rejection) -- [:clause, :generation, :lock_basename, :cause_clause, :lock_state] == []
  end

  # Generation and token are held constant on every row, so a rule that
  # compared only those two would release each of these locks. Only these four
  # fields can differ and still leave a readable, held lock for the arbiter to
  # be tempted by; the unreadable shapes are the row below.
  @mutations [
    {"pid", "999999"},
    {"pid_start", "start_999999"},
    {"supervisor_instance", "sup_impostor"},
    {"acquired_at", "2000-01-01T00:00:00.000000Z"}
  ]

  test "the reclaim releases the exact record, not merely the generation and token", %{dir: dir} do
    # Control: with the record untouched, the reclaim really does fire, so the
    # rows below are refusals and not an inert matrix.
    control = fresh_run(dir, "control")
    {:ok, writer, _} = open(control)
    :ok = kill(writer)
    assert {:ok, replacement, _} = open(control, lock: lock_opts(supervisor_instance: "sup_0002"))
    :ok = Writer.close(replacement)

    for {field, value} <- @mutations do
      run = fresh_run(dir, field)
      {:ok, held_writer, _} = open(run)
      original = File.read!(lock_path(run, 1))
      :ok = kill(held_writer)

      mutated = mutate_lock!(run, 1, field, value)
      refute mutated == original, field

      # The registration is now stale evidence: `RunLock` adjudicates, and it
      # sees a live holder.
      assert match?({:error, %{clause: "run_locked"}}, open(run, lock: lock_opts(supervisor_instance: "sup_0002"))),
             field

      assert File.read!(lock_path(run, 1)) == mutated, field
      assert generations(run) == [1], field
    end
  end

  test "a lock whose shape the arbiter cannot read is never released", %{dir: dir} do
    for {field, value} <- [{"schema", "other/run-lock"}, {"schema_version", 2}, {"state", "wedged"}] do
      run = fresh_run(dir, "unreadable_" <> field)
      {:ok, writer, _} = open(run)
      :ok = kill(writer)
      mutated = mutate_lock!(run, 1, field, value)

      assert match?({:error, %{clause: "lock_unreadable"}}, open(run, lock: lock_opts(supervisor_instance: "sup_0002"))),
             field

      assert File.read!(lock_path(run, 1)) == mutated, field
      assert generations(run) == [1], field
    end
  end

  test "a graceful close retires the registration and the lock together", %{dir: dir} do
    {:ok, writer, _} = open(dir)
    assert {:ok, %{writer: ^writer, generation: 1, state: :live}} = Ownership.status(dir)

    :ok = Writer.close(writer)
    assert :none = Ownership.status(dir)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "an arbiter holding no registration reclaims nothing and fails closed", %{dir: dir} do
    {:ok, first} = Ownership.start_link(name: nil)
    {:ok, writer, _} = open(dir, ownership: [server: first])
    :ok = kill(writer)

    # What a restarted arbiter has: no registrations, and a lock on disk whose
    # OS pid is this very process. Inferring ownership from that pid is the
    # mistake; it fails closed instead.
    {:ok, restarted} = Ownership.start_link(name: nil)

    assert {:error, %{clause: "run_locked"}} =
             open(dir, ownership: [server: restarted], lock: lock_opts(supervisor_instance: "sup_0002"))

    assert :none = Ownership.status(dir, server: restarted)
    assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)
  end

  test "a caller is registered before it is told it holds the lock", %{dir: dir} do
    test = self()

    claimant =
      spawn(fn ->
        send(test, {:acquired, Ownership.acquire(dir, self(), fs: SystemFs.new(), lock: lock_opts())})
        Process.sleep(:infinity)
      end)

    assert_receive {:acquired, {:ok, %{generation: 1}}}, 2_000
    :ok = kill(claimant)

    # Had the registration followed the reply, this claimant would have died
    # inside that window and stranded a lock nothing could prove was ours.
    assert {:ok, replacement, _} = open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))
    :ok = Writer.close(replacement)
  end

  test "an absent arbiter is a named rejection, not a crash, and claims nothing", %{dir: dir} do
    {:ok, arbiter} = Ownership.start_link(name: nil)
    :ok = GenServer.stop(arbiter)

    assert {:error, %{clause: "ownership_unavailable"}} = open(dir, ownership: [server: arbiter])
    assert generations(dir) == []

    # `:none` would say "no registration exists", which nothing here knows.
    assert {:error, %{clause: "ownership_unavailable"}} = Ownership.status(dir, server: arbiter)
  end

  test "an arbiter killed with a call in flight is unavailable, never an authoritative :none", %{dir: dir} do
    {:ok, arbiter} = Ownership.start_link(name: nil)

    # Suspended, the call is enqueued but never answered, so the kill lands
    # squarely in flight rather than before the call is even sent.
    :sys.suspend(arbiter)
    asking = Task.async(fn -> Ownership.status(dir, server: arbiter) end)
    wait_until(fn -> Process.info(arbiter, :message_queue_len) == {:message_queue_len, 1} end, 500)

    Process.unlink(arbiter)
    Process.exit(arbiter, :kill)

    # `:killed` is not in any exit whitelist worth writing: the caller learns
    # only that no answer came, which is the opposite of `:none`.
    assert {:error, %{clause: "ownership_unavailable"}} = Task.await(asking, 5_000)
  end

  test "the host root mounts the arbiter first under rest_for_one" do
    assert [Ownership | _] = Host.children()
    assert Host.options()[:strategy] == :rest_for_one
    assert is_pid(Process.whereis(AiOrchestrator.Supervisor))
  end

  test "an arbiter crash takes the writer with it and the tree comes back holding a fresh lock", %{dir: dir} do
    children = [
      {Ownership, name: @arbiter},
      {Writer, {dir, [lock: lock_opts(), ownership: [server: @arbiter]]}}
    ]

    root =
      start_supervised!(%{
        id: :host_root,
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    assert {:ok, %{writer: writer, state: :live}} = Ownership.status(dir, server: @arbiter)
    assert {:ok, %{"token" => before_token}} = RunLock.owner(SystemFs.new(), dir)

    ref = Process.monitor(writer)
    :ok = kill(Process.whereis(@arbiter))

    # rest_for_one is what makes this a shutdown rather than an abandonment:
    # the writer is terminated, so its own `terminate/2` releases its own lock.
    assert_receive {:DOWN, ^ref, :process, ^writer, _reason}, 5_000

    wait_until(fn -> match?({:ok, %{state: :live}}, Ownership.status(dir, server: @arbiter)) end, 500)
    assert {:ok, %{writer: restarted}} = Ownership.status(dir, server: @arbiter)
    refute restarted == writer

    assert {:ok, %{"token" => after_token}} = RunLock.owner(SystemFs.new(), dir)
    refute after_token == before_token
    assert Process.alive?(root)
  end

  test "a supervised writer reports what open reports", %{dir: dir} do
    assert {:ok, writer} = Writer.start_link(dir, lock: lock_opts())
    assert %{last_seq: 0, lines: [], repair: nil} = Writer.opened(writer)
    :ok = Writer.close(writer)
  end
end
