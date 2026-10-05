defmodule AiOrchestrator.PaneRegistry.ClaimLeakTest do
  @moduledoc """
  B2 GREEN (NS-15.G.004, D/B2-GREEN-SCOPE-r2.org): one-variable controls for the claimant-scoped owner reading and the
  read-only `FileRegistry.held?/3`, next to the RED rows in claim_leak_red_test.exs.

  - C1 a live claimant in the same BEAM still holds its pane (R2.1 without the kill).
  - C2 held?/3 reads a live owner in another BEAM as held.
  - C3 held?/3 reads no claim file as not held, and never writes (directory and bytes unchanged after reads).
  - C4 held?/3 reads a malformed claim file as held and leaves it unchanged.
  - C5 an older claim without "erlang_pid" keeps the OS-only reading (a dead claimant's pane stays held).
  - C6 a malformed "erlang_pid" is unknown, never dead: a claim is refused, nothing is reclaimed, held?/3 is true.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry

  @holder Path.expand("../support/pane_claim_process.exs", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_claim_leak_g_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp owner(run_id), do: %{"run_id" => run_id, "run_dir" => "/tmp/#{run_id}", "supervisor_instance" => "sup_#{run_id}"}

  # a claimant process in this BEAM that claims and then waits; the caller decides whether it dies
  defp claimant(root) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:claimed, FileRegistry.claim(["pane_shared"], owner("run_a"), root: root)})
        Process.sleep(:infinity)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    assert_receive {:claimed, {:ok, _claim}}, 5_000
    pid
  end

  defp kill!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5_000
  end

  defp snapshot(root), do: root |> File.ls!() |> Enum.sort() |> Map.new(&{&1, File.read!(Path.join(root, &1))})

  defp rewrite_claim!(root, fun) do
    path = FileRegistry.claim_path(root, "pane_shared")
    updated = path |> File.read!() |> Jason.decode!() |> fun.()
    File.write!(path, Jason.encode!(updated))
    path
  end

  test "C1 a live claimant in the same BEAM still holds its pane", %{root: root} do
    claimant(root)
    path = FileRegistry.claim_path(root, "pane_shared")
    before = File.read!(path)

    result = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)

    assert match?({:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_shared"}}, result), inspect(result)
    assert File.read!(path) == before
    assert FileRegistry.held?(root, "pane_shared", [])
  end

  test "C2 held?/3 reads a live owner in another BEAM as held", %{root: root} do
    holder = start_helper(root)
    assert %{"status" => "acquired"} = read_json_line(holder)

    assert FileRegistry.held?(root, "pane_shared", [])

    {:os_pid, os_pid} = Port.info(holder, :os_pid)
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
    assert_receive {^holder, {:exit_status, _status}}, 5_000
  end

  test "C3 held?/3 reads no claim file as not held and never writes", %{root: root} do
    refute FileRegistry.held?(root, "pane_shared", [])
    refute File.exists?(root)

    pid = claimant(root)
    kill!(pid)
    before = snapshot(root)

    refute FileRegistry.held?(root, "pane_shared", [])
    refute FileRegistry.held?(root, "pane_shared", [])
    assert snapshot(root) == before
  end

  test "C4 held?/3 reads a malformed claim file as held and leaves it unchanged", %{root: root} do
    path = FileRegistry.claim_path(root, "pane_shared")
    File.mkdir_p!(root)
    File.write!(path, "not-json")

    assert FileRegistry.held?(root, "pane_shared", [])
    assert File.read!(path) == "not-json"
  end

  test "C5 an older claim without erlang_pid keeps the OS-only reading", %{root: root} do
    pid = claimant(root)
    kill!(pid)
    path = rewrite_claim!(root, &Map.delete(&1, "erlang_pid"))
    before = File.read!(path)

    result = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)

    assert match?({:error, %{"reason" => "pane_claim_rejected"}}, result), inspect(result)
    assert File.read!(path) == before
    assert FileRegistry.held?(root, "pane_shared", [])
  end

  test "C6 a malformed erlang_pid is unknown, never dead: the claim is refused and nothing is reclaimed", %{root: root} do
    pid = claimant(root)
    kill!(pid)

    for malformed <- ["not-a-pid", 42] do
      path = rewrite_claim!(root, &Map.put(&1, "erlang_pid", malformed))
      before = File.read!(path)

      result = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)

      assert result == {:error, %{"reason" => "pane_registry_unavailable", "detail" => "owner_status_unknown"}},
             inspect({malformed, result})

      assert File.read!(path) == before
      assert FileRegistry.held?(root, "pane_shared", [])
    end
  end

  defp start_helper(root) do
    Port.open(
      {:spawn_executable, System.find_executable("elixir")},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: code_path_args() ++ [@holder, root, "pane_shared", "run_a", "30000"]
      ]
    )
  end

  defp read_json_line(port, buffer \\ "") do
    receive do
      {^port, {:data, data}} ->
        combined = buffer <> data

        case String.split(combined, "\n", parts: 2) do
          [line, rest] -> decoded_or_next(port, line, rest)
          [_partial] -> read_json_line(port, combined)
        end

      {^port, {:exit_status, status}} ->
        flunk("helper exited before publishing a result: #{status}; output=#{inspect(buffer)}")
    after
      5_000 -> flunk("timed out waiting for helper; output=#{inspect(buffer)}")
    end
  end

  defp decoded_or_next(port, line, rest) do
    case Jason.decode(line) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> read_json_line(port, rest)
    end
  end

  defp code_path_args do
    paths = [Mix.Project.compile_path() | Path.wildcard(Path.join([Mix.Project.build_path(), "lib", "*", "ebin"]))]
    Enum.flat_map(paths, &["-pa", &1])
  end
end
