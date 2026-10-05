defmodule AiOrchestrator.PaneRegistry.ClaimLeakRedTest do
  @moduledoc """
  NS-15.G.004 (pane claims do not leak across kill, BEAM restart and daemon restart), B2 scope r4
  (D/B2-B3-SCOPE-r4.org): RED rows R2.1 and R2.4 with BASELINE rows R2.2 and R2.3.

  - R2.1 RED: a claiming process killed with `:kill` inside a BEAM that stays alive must not leave its claim held;
    today the claim file names the live BEAM pid, so a second claimant in the same BEAM is rejected.
  - R2.4 RED: after the owning BEAM exits without releasing, a NEW BEAM must read the pane as not held BEFORE any
    claim attempt (a pre-contention witness). Today no registry read exists; the row names the read it needs
    (`FileRegistry.held?/3`) and fails on that name rather than on an undefined-function crash. Its control is a
    BASELINE through the existing contention path (a live owner in another BEAM still rejects a claim).
  - Expected at this head: exactly three failures (R2.1, and the two R2.4 rows); every other test passes.
  - R2.2 BASELINE: the next claimant reclaims a dead owner's claim on contention (kill -9 is covered by
    process_integration_test.exs:6; this adds a clean exit without release).
  - R2.3 BASELINE: the registry makes no call into dispatch or the daemon client. This is a structural witness of the
    compiled module's imports; it is NOT a daemon-restart witness.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry

  @holder Path.expand("../support/pane_claim_process.exs", __DIR__)
  @no_release Path.expand("../support/pane_claim_no_release_process.exs", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_claim_leak_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "R2.1 RED: a claimant killed with :kill in a live BEAM leaves the pane free", %{root: root} do
    parent = self()

    claimant =
      spawn(fn ->
        send(parent, {:claimed, FileRegistry.claim(["pane_shared"], owner("run_a"), root: root)})
        Process.sleep(:infinity)
      end)

    assert_receive {:claimed, {:ok, _claim}}, 5_000
    ref = Process.monitor(claimant)
    Process.exit(claimant, :kill)
    assert_receive {:DOWN, ^ref, :process, ^claimant, :killed}, 5_000

    result = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)

    assert match?({:ok, _claim}, result),
           "the killed claimant's pane is still held by this live BEAM: #{inspect(result)}"

    {:ok, claim} = result
    assert :ok = FileRegistry.release(claim)
  end

  test "R2.1 control: a claimant that releases normally leaves the pane free", %{root: root} do
    task =
      Task.async(fn ->
        {:ok, claim} = FileRegistry.claim(["pane_shared"], owner("run_a"), root: root)
        FileRegistry.release(claim)
      end)

    assert :ok = Task.await(task, 5_000)
    assert {:ok, claim} = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)
    assert :ok = FileRegistry.release(claim)
  end

  test "R2.4 RED: a new BEAM reads a clean-exit owner's pane as not held before any claim", %{root: root} do
    assert %{"status" => "acquired"} = run_helper(@no_release, root, "pane_shared", "run_a")
    assert File.exists?(FileRegistry.claim_path(root, "pane_shared"))

    assert_held_read!()
    assert apply(FileRegistry, :held?, [root, "pane_shared", []]) == false
  end

  test "R2.4 RED: a new BEAM reads a kill -9 owner's pane as not held before any claim", %{root: root} do
    holder = start_helper(@holder, root, "pane_shared", "run_a", 30_000)
    assert %{"status" => "acquired"} = read_json_line(holder)
    kill_9!(holder)

    assert_held_read!()
    assert apply(FileRegistry, :held?, [root, "pane_shared", []]) == false
  end

  # A BASELINE through the existing contention path, not through held?/3: it witnesses only that a live owner in
  # another BEAM is still respected, which is narrower than the pre-contention read the R2.4 rows require.
  test "R2.4 BASELINE control: a live owner in another BEAM still rejects a claim", %{root: root} do
    holder = start_helper(@holder, root, "pane_shared", "run_a", 30_000)
    assert %{"status" => "acquired"} = read_json_line(holder)

    assert {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_shared"}} =
             FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)

    kill_9!(holder)
  end

  test "R2.2 BASELINE: the next claimant reclaims a pane left by a BEAM that exited", %{root: root} do
    assert %{"status" => "acquired"} = run_helper(@no_release, root, "pane_shared", "run_a")
    assert {:ok, claim} = FileRegistry.claim(["pane_shared"], owner("run_b"), root: root)
    assert :ok = FileRegistry.release(claim)
  end

  test "R2.3 BASELINE: the compiled registry imports nothing from dispatch" do
    {:ok, {_module, [imports: imports]}} =
      FileRegistry
      |> :code.which()
      |> :beam_lib.chunks([:imports])

    called =
      imports
      |> Enum.map(fn {module, _function, _arity} -> Atom.to_string(module) end)
      |> Enum.uniq()

    refute Enum.any?(called, &String.starts_with?(&1, "Elixir.AiOrchestrator.Dispatch")), inspect(called)
  end

  defp assert_held_read! do
    Code.ensure_loaded!(FileRegistry)

    assert function_exported?(FileRegistry, :held?, 3),
           "FileRegistry.held?/3 (a read of whether a pane is held, without claiming) does not exist"
  end

  defp owner(run_id) do
    %{
      "run_id" => run_id,
      "run_dir" => "/tmp/#{run_id}",
      "supervisor_instance" => "sup_#{run_id}"
    }
  end

  defp start_helper(helper, root, pane_ref, run_id, hold_ms) do
    Port.open(
      {:spawn_executable, System.find_executable("elixir")},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: code_path_args() ++ [helper, root, pane_ref, run_id, Integer.to_string(hold_ms)]
      ]
    )
  end

  defp run_helper(helper, root, pane_ref, run_id) do
    {output, 0} =
      System.cmd(
        System.find_executable("elixir"),
        code_path_args() ++ [helper, root, pane_ref, run_id, "0"],
        stderr_to_stdout: true
      )

    output
    |> String.split("\n", trim: true)
    |> List.last()
    |> Jason.decode!()
  end

  defp kill_9!(port) do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
    assert_receive {^port, {:exit_status, _status}}, 5_000
  end

  defp read_json_line(port, buffer \\ "") do
    receive do
      {^port, {:data, data}} ->
        combined = buffer <> data

        case String.split(combined, "\n", parts: 2) do
          [line, rest] ->
            case Jason.decode(line) do
              {:ok, decoded} -> decoded
              {:error, _reason} -> read_json_line(port, rest)
            end

          [_partial] ->
            read_json_line(port, combined)
        end

      {^port, {:exit_status, status}} ->
        flunk("helper exited before publishing a result: #{status}; output=#{inspect(buffer)}")
    after
      5_000 -> flunk("timed out waiting for helper; output=#{inspect(buffer)}")
    end
  end

  defp code_path_args do
    paths = [Mix.Project.compile_path() | Path.wildcard(Path.join([Mix.Project.build_path(), "lib", "*", "ebin"]))]
    Enum.flat_map(paths, &["-pa", &1])
  end
end
