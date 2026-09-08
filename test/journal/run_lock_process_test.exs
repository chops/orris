defmodule AiOrchestrator.Journal.RunLockProcessTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer

  @helper Path.expand("../support/run_lock_process.exs", __DIR__)
  @moduletag timeout: 60_000

  setup do
    dir = Path.join(System.tmp_dir!(), "run_lock_os_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  test "a writer in another OS process refuses this one by name, and a killed holder is reclaimed", %{dir: dir} do
    holder = start_holder(dir, 30_000)
    assert %{"status" => "acquired", "pid" => holder_pid} = read_json_line(holder)

    assert {:error, %{clause: "run_locked", owner: %{"pid" => ^holder_pid}}} =
             Writer.open(dir, lock: [supervisor_instance: "sup_test"])

    assert %{"status" => "rejected", "error" => %{"clause" => "run_locked", "owner" => %{"pid" => ^holder_pid}}} =
             run_holder(dir, 0)

    {:os_pid, os_pid} = Port.info(holder, :os_pid)
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
    assert_receive {^holder, {:exit_status, _status}}, 10_000

    assert {:ok, %{"pid" => ^holder_pid}} = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, writer, %{last_seq: 0}} = Writer.open(dir, lock: [supervisor_instance: "sup_test"])
    assert {:ok, %{"supervisor_instance" => "sup_test"}} = RunLock.owner(SystemFs.new(), dir)
    :ok = Writer.close(writer)
    assert :none = RunLock.owner(SystemFs.new(), dir)
  end

  test "a holder that closes cleanly hands the lock over", %{dir: dir} do
    assert %{"status" => "released"} = run_holder(dir, 0)
    assert :none = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, writer, _} = Writer.open(dir, lock: [supervisor_instance: "sup_test"])
    :ok = Writer.close(writer)
  end

  defp start_holder(dir, hold_ms) do
    Port.open(
      {:spawn_executable, System.find_executable("elixir")},
      [:binary, :exit_status, :stderr_to_stdout, args: code_path_args() ++ [@helper, dir, Integer.to_string(hold_ms)]]
    )
  end

  defp run_holder(dir, hold_ms) do
    {output, 0} =
      System.cmd(
        System.find_executable("elixir"),
        code_path_args() ++ [@helper, dir, Integer.to_string(hold_ms)],
        stderr_to_stdout: true
      )

    output |> String.split("\n", trim: true) |> List.last() |> Jason.decode!()
  end

  defp read_json_line(port, buffer \\ "") do
    receive do
      {^port, {:data, chunk}} ->
        case String.split(buffer <> chunk, "\n", parts: 2) do
          [line, _rest] -> Jason.decode!(line)
          [partial] -> read_json_line(port, partial)
        end

      {^port, {:exit_status, status}} ->
        flunk("holder exited early with status #{status}: #{inspect(buffer)}")
    after
      20_000 -> flunk("holder produced no output: #{inspect(buffer)}")
    end
  end

  defp code_path_args do
    paths = [Mix.Project.compile_path() | Path.wildcard(Path.join([Mix.Project.build_path(), "lib", "*", "ebin"]))]
    Enum.flat_map(paths, &["-pa", &1])
  end
end
