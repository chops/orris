defmodule AiOrchestrator.PaneRegistry.RootLockRedTest do
  @moduledoc """
  B3a RED (design r7, D/B3A-RED-DESIGN-r7.org): the one claims-root lock, a kernel flock on <root>/.root-lock held by
  a native helper over an Erlang Port, shared by FileRegistry reclaim and the claim-refusal diagnosis.

  API these rows name (absent at this head, reached through apply/3):
  - `FileRegistry.with_root_lock(root, owner_token, opts, fun)` where `fun.(lock)` runs while the lock is held and
    `lock` carries `"helper_os_pid"`; answers `{:ok, fun_result}` or `{:error, reason}` with reason one of
    `"lock_busy"`, `"lock_unavailable"`, `"lock_unidentified"`. opts: `:wait_ms`, `:root_lock_helper`, and
    `:on_contend` (a zero-arity function called once when the caller first finds the lock held, so a test can prove
    a waiter is really contending before it changes the holder).

  Rows L1-L8 and (ii) of the design. Expected at this head: every row fails on the named
  "root lock helper is not available" assertion (nine rows).
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry

  @diagnosis Module.concat([AiOrchestrator, PaneRegistry, Diagnosis])

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_root_lock_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp api! do
    Code.ensure_loaded!(FileRegistry)

    assert function_exported?(FileRegistry, :with_root_lock, 4),
           "root lock helper is not available (FileRegistry.with_root_lock/4)"
  end

  defp with_lock(root, token, opts, fun), do: apply(FileRegistry, :with_root_lock, [root, token, opts, fun])

  # a holder that takes the lock and waits for :release; it reports :locked with the lock map. It is killed at test
  # exit whatever happened, so a failed assertion never leaves it holding the lock.
  defp start_holder(root) do
    parent = self()

    holder =
      spawn(fn ->
        with_lock(root, "holder", [], fn lock ->
          send(parent, {:locked, self(), lock})

          receive do
            :release -> :released
          end
        end)
      end)

    on_exit(fn -> Process.exit(holder, :kill) end)
    holder
  end

  defp await_locked do
    assert_receive {:locked, holder, lock}, 5_000
    {holder, lock}
  end

  defp attrs do
    %{
      "trigger" => "dead",
      "pane_ref" => "pane_writer",
      "daemon_pane_id" => nil,
      "holder" => nil,
      "observed_daemon_state" => %{"source" => "pane_status_v1"},
      "next_action" => %{"code" => "reattach_pane", "text" => "reattach the pane, then retry"}
    }
  end

  test "L1 RED a live holder past any age keeps the lock against a diagnosis", %{root: root} do
    api!()
    {:ok, _} = apply(@diagnosis, :open, [root, attrs(), []])
    [name] = root |> Path.join("diagnoses") |> File.ls!()
    before = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!()

    start_holder(root)
    {holder, _lock} = await_locked()
    File.touch!(Path.join(root, ".root-lock"), System.os_time(:second) - 3_600)

    result = apply(@diagnosis, :open, [root, attrs(), [wait_ms: 50]])
    assert result == {:error, %{"persistence" => %{"ok" => false, "error" => "lock_busy"}}}
    assert root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() == before

    send(holder, :release)
    assert match?({:ok, _}, apply(@diagnosis, :open, [root, attrs(), []]))
  end

  test "L2 RED a live holder keeps the lock against reclaim", %{root: root} do
    api!()
    dead = fn _metadata -> :dead end
    owner = %{"run_id" => "run_a", "run_dir" => "/tmp/run_a", "supervisor_instance" => "sup_a"}
    {:ok, _orphan} = FileRegistry.claim(["pane_shared"], owner, root: root)
    path = FileRegistry.claim_path(root, "pane_shared")
    before = File.read!(path)

    start_holder(root)
    {holder, _lock} = await_locked()

    result =
      FileRegistry.claim(["pane_shared"], %{owner | "run_id" => "run_b"},
        root: root,
        owner_status: dead,
        wait_ms: 50
      )

    assert result == {:error, %{"reason" => "pane_registry_unavailable", "detail" => "lock_busy"}}
    assert File.read!(path) == before
    send(holder, :release)
  end

  test "L3 RED a killed Erlang holder releases the lock", %{root: root} do
    api!()
    start_holder(root)
    {holder, _lock} = await_locked()
    Process.exit(holder, :kill)

    assert with_lock(root, "next", [wait_ms: 5_000], fn _lock -> :acquired end) == {:ok, :acquired}
  end

  test "L4 RED a helper killed with -9 releases the lock through the kernel", %{root: root} do
    api!()
    start_holder(root)
    {_holder, %{"helper_os_pid" => os_pid}} = await_locked()
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)

    assert with_lock(root, "next", [wait_ms: 5_000], fn _lock -> :acquired end) == {:ok, :acquired}
  end

  test "L5 RED a legacy .reclaim-lock directory fails closed and is never removed", %{root: root} do
    api!()
    legacy = Path.join(root, ".reclaim-lock")
    File.mkdir_p!(legacy)
    File.write!(Path.join(legacy, "token"), "legacy")
    File.touch!(legacy, System.os_time(:second) - 3_600)

    assert with_lock(root, "next", [wait_ms: 50], fn _lock -> :ran end) == {:error, "lock_unidentified"}
    assert File.read!(Path.join(legacy, "token")) == "legacy"
  end

  # Concurrent repeats: both repeats are started while a holder has the lock and each reports that it is contending
  # (Diagnosis.open forwards :on_contend to the lock); only then is the holder released, so both read-modify-write
  # sequences race for the lock and neither update may be lost.
  test "L6 RED two concurrent repeats through the lock are not lost", %{root: root} do
    api!()
    assert match?({:ok, _}, apply(@diagnosis, :open, [root, attrs(), []]))
    holder = start_holder(root)
    await_locked()
    parent = self()

    repeats =
      for name <- [:a, :b] do
        Task.async(fn ->
          opts = [wait_ms: 5_000, on_contend: fn -> send(parent, {:contending, name}) end]
          apply(@diagnosis, :open, [root, attrs(), opts])
        end)
      end

    assert_receive {:contending, :a}, 5_000
    assert_receive {:contending, :b}, 5_000
    send(holder, :release)

    for repeat <- repeats, do: assert(match?({:ok, _}, Task.await(repeat, 10_000)))

    [name] = root |> Path.join("diagnoses") |> File.ls!()
    decoded = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert decoded["seen_count"] == 3
  end

  test "L7 RED an unavailable helper fails closed and the function never runs", %{root: root} do
    api!()
    helper = System.find_executable("false")

    result = with_lock(root, "next", [root_lock_helper: helper], fn _lock -> send(self(), :ran) end)

    assert result == {:error, "lock_unavailable"}
    refute_received :ran
  end

  test "L8 RED two waiters after a holder's death never overlap and remove nothing", %{root: root} do
    api!()
    start_holder(root)
    {holder, _lock} = await_locked()
    clock = :counters.new(1, [:atomics])
    parent = self()

    waiters =
      for name <- [:a, :b] do
        Task.async(fn ->
          opts = [wait_ms: 5_000, on_contend: fn -> send(parent, {:contending, name}) end]

          with_lock(root, Atom.to_string(name), opts, fn _lock ->
            :counters.add(clock, 1, 1)
            entered = :counters.get(clock, 1)
            Process.sleep(50)
            :counters.add(clock, 1, 1)
            send(parent, {:interval, name, entered, :counters.get(clock, 1)})
          end)
        end)
      end

    # barrier: both waiters have found the lock held before the holder dies
    assert_receive {:contending, :a}, 5_000
    assert_receive {:contending, :b}, 5_000
    Process.exit(holder, :kill)
    Enum.each(waiters, &Task.await(&1, 10_000))

    assert_receive {:interval, :a, a_in, a_out}
    assert_receive {:interval, :b, b_in, b_out}
    assert a_out < b_in or b_out < a_in, "the two critical sections overlapped"
    assert File.exists?(Path.join(root, ".root-lock")), "nothing removes the lock file"
  end

  test "(ii) RED no lock path other than .root-lock is created under the root", %{root: root} do
    api!()
    assert with_lock(root, "only", [], fn _lock -> :ok end) == {:ok, :ok}

    dotted = root |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "."))
    assert dotted == [".root-lock"]
  end
end
