defmodule AiOrchestrator.PaneRegistry.ProcessIntegrationTest do
  use ExUnit.Case, async: false

  @helper Path.expand("../support/pane_claim_process.exs", __DIR__)

  test "separate OS processes reject a live owner and reclaim after kill -9" do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_process_registry_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)

    first = start_claimant(root, "pane_shared", "run_a", 30_000)
    assert %{"status" => "acquired", "pid" => first_pid} = read_json_line(first)

    assert %{
             "status" => "rejected",
             "error" => %{
               "reason" => "pane_claim_rejected",
               "pane_ref" => "pane_shared",
               "owner" => %{"run_id" => "run_a", "pid" => ^first_pid}
             }
           } = run_claimant(root, "pane_shared", "run_b", 0)

    {:os_pid, os_pid} = Port.info(first, :os_pid)
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
    assert_receive {^first, {:exit_status, _status}}, 5_000

    assert %{"status" => "acquired"} = run_claimant(root, "pane_shared", "run_c", 0)
  end

  defp start_claimant(root, pane_ref, run_id, hold_ms) do
    Port.open(
      {:spawn_executable, System.find_executable("elixir")},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: code_path_args() ++ [@helper, root, pane_ref, run_id, Integer.to_string(hold_ms)]
      ]
    )
  end

  defp run_claimant(root, pane_ref, run_id, hold_ms) do
    {output, 0} =
      System.cmd(
        System.find_executable("elixir"),
        code_path_args() ++ [@helper, root, pane_ref, run_id, Integer.to_string(hold_ms)],
        stderr_to_stdout: true
      )

    output
    |> String.split("\n", trim: true)
    |> List.last()
    |> Jason.decode!()
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
        flunk("claimant exited before publishing a result: #{status}; output=#{inspect(buffer)}")
    after
      5_000 -> flunk("timed out waiting for claimant; output=#{inspect(buffer)}")
    end
  end

  defp code_path_args do
    paths = [Mix.Project.compile_path() | Path.wildcard(Path.join([Mix.Project.build_path(), "lib", "*", "ebin"]))]
    Enum.flat_map(paths, &["-pa", &1])
  end
end
