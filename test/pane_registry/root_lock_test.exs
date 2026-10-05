defmodule AiOrchestrator.PaneRegistry.RootLockTest do
  @moduledoc """
  B3a G2 (scope r3, D/B3A-GREEN-SCOPE-r3.org): the claims-root lock's waiter lifecycle. L9 a waiter that times out
  while contended has its helper exit (reported by the runtime) without acquiring, and the lock is then free; L10 a
  waiter whose caller dies while contended leaves no holder behind; L11 `:on_contend` follows an actual contended
  attempt, once, and never fires otherwise; L12 a helper killed while the function runs is a lost lock, never
  exclusive success; L13 a raising `:on_contend` closes the waiter's port, so no holder is left behind; L14 a reclaim
  whose lock is lost answers lock_lost and removes nothing (recovery is explicit). Helper exits are observed through the runtime's exit status (the
  test-only `:helper_observer` copies), never by signalling a pid.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_root_lock_g2_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp start_holder(root) do
    parent = self()

    holder =
      spawn(fn ->
        FileRegistry.with_root_lock(root, "holder", [], fn _lock ->
          send(parent, {:locked, self()})

          receive do
            :release -> :released
          end
        end)
      end)

    on_exit(fn -> Process.exit(holder, :kill) end)
    assert_receive {:locked, ^holder}, 5_000
    holder
  end

  test "L9 a waiter that times out while contended exits without acquiring and the lock is then free", %{root: root} do
    holder = start_holder(root)
    owner = self()

    result =
      FileRegistry.with_root_lock(root, "waiter", [wait_ms: 50, helper_observer: owner], fn _lock ->
        send(owner, :waiter_ran)
      end)

    assert result == {:error, "lock_busy"}
    assert_received {:root_lock_helper, ^owner, {:spawned, os_pid}}
    assert is_integer(os_pid)
    assert_received {:root_lock_helper, ^owner, {:line, "contended " <> _pid}}
    assert_received {:root_lock_helper, ^owner, {:line, "busy"}}
    assert_received {:root_lock_helper, ^owner, {:exit_status, 0}}
    refute_received {:root_lock_helper, ^owner, {:line, "acquired " <> _pid}}
    refute_received :waiter_ran

    send(holder, :release)
    assert FileRegistry.with_root_lock(root, "next", [wait_ms: 5_000], fn _lock -> :acquired end) == {:ok, :acquired}
  end

  test "L10 a waiter whose caller dies while contended leaves no holder behind", %{root: root} do
    holder = start_holder(root)
    parent = self()

    waiter =
      spawn(fn ->
        FileRegistry.with_root_lock(root, "waiter", [wait_ms: 60_000, helper_observer: parent], fn _lock ->
          send(parent, :waiter_ran)
        end)
      end)

    assert_receive {:root_lock_helper, ^waiter, {:line, "contended " <> _pid}}, 5_000
    Process.exit(waiter, :kill)
    send(holder, :release)

    assert FileRegistry.with_root_lock(root, "third", [wait_ms: 5_000], fn _lock -> :acquired end) == {:ok, :acquired}
    assert FileRegistry.with_root_lock(root, "fourth", [wait_ms: 1_000], fn _lock -> :acquired end) == {:ok, :acquired}
    refute_received :waiter_ran
  end

  test "L12 a helper killed while the function runs answers a lost lock, never exclusive success", %{root: root} do
    parent = self()

    result =
      FileRegistry.with_root_lock(root, "victim", [], fn %{"helper_os_pid" => os_pid} ->
        {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)

        other =
          Task.async(fn -> FileRegistry.with_root_lock(root, "other", [wait_ms: 5_000], fn _lock -> :entered end) end)

        send(parent, {:overlap, Task.await(other, 10_000)})
        :done
      end)

    assert result == {:error, %{"lock" => "lost", "result" => :done}}
    assert_received {:overlap, {:ok, :entered}}
  end

  test "L14 a reclaim whose lock is lost answers lock_lost and removes nothing", %{root: root} do
    owner = %{"run_id" => "run_a", "run_dir" => "/tmp/run_a", "supervisor_instance" => "sup_a"}
    base = [root: root, pid: "41001", pid_start: "start_41001"]
    {:ok, _stale} = FileRegistry.claim(["pane_shared"], owner, base ++ [token_fun: fn -> "token_a" end])
    test_pid = self()

    # Called once before the lock (no helper yet) and once under it, where it kills the helper that holds the lock.
    status = fn _metadata ->
      receive do
        {:root_lock_helper, ^test_pid, {:spawned, os_pid}} ->
          {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
          :dead
      after
        0 -> :dead
      end
    end

    reclaim = base ++ [token_fun: fn -> "token_b" end, owner_status: status, helper_observer: test_pid]
    path = FileRegistry.claim_path(root, "pane_shared")

    assert FileRegistry.claim(["pane_shared"], %{owner | "run_id" => "run_b"}, reclaim) ==
             {:error,
              %{
                "reason" => "pane_registry_unavailable",
                "detail" => "lock_lost",
                "reclaim" => "indeterminate",
                "path" => path
              }}

    assert path |> File.read!() |> Jason.decode!() |> Map.fetch!("token") == "token_b"
  end

  test "L13 an on_contend that raises closes the waiter's port and leaves no holder behind", %{root: root} do
    holder = start_holder(root)
    raising = [wait_ms: 60_000, on_contend: fn -> raise "contend failed" end]

    assert_raise RuntimeError, "contend failed", fn ->
      FileRegistry.with_root_lock(root, "raiser", raising, fn _lock -> :ran end)
    end

    send(holder, :release)
    assert FileRegistry.with_root_lock(root, "third", [wait_ms: 5_000], fn _lock -> :acquired end) == {:ok, :acquired}
    assert FileRegistry.with_root_lock(root, "fourth", [wait_ms: 1_000], fn _lock -> :acquired end) == {:ok, :acquired}
  end

  test "L11 on_contend fires once after an actual contended attempt and never otherwise", %{root: root} do
    parent = self()
    on_contend = fn -> send(parent, :contended) end

    assert FileRegistry.with_root_lock(root, "free", [on_contend: on_contend], fn _lock -> :ok end) == {:ok, :ok}
    refute_received :contended

    unavailable = [on_contend: on_contend, root_lock_helper: System.find_executable("false")]

    assert FileRegistry.with_root_lock(root, "unavailable", unavailable, fn _lock -> :ok end) ==
             {:error, "lock_unavailable"}

    refute_received :contended

    holder = start_holder(root)
    contended = [wait_ms: 100, on_contend: on_contend]
    assert FileRegistry.with_root_lock(root, "waiter", contended, fn _lock -> :ok end) == {:error, "lock_busy"}
    assert_received :contended
    refute_received :contended
    send(holder, :release)

    File.mkdir_p!(Path.join(root, ".reclaim-lock"))
    unidentified = [wait_ms: 100, on_contend: on_contend]
    assert FileRegistry.with_root_lock(root, "legacy", unidentified, fn _lock -> :ok end) == {:error, "lock_unidentified"}
    refute_received :contended
  end
end
