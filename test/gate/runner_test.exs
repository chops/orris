defmodule AiOrchestrator.Gate.RunnerTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Gate.Runner

  test "started_data returns deterministic gate output paths" do
    gate = %{"gate_run_id" => "gr_0001", "command_argv" => ["mix", "test"]}

    assert Runner.started_data(gate) == %{
             "gate_run_id" => "gr_0001",
             "command_argv" => ["mix", "test"],
             "stdout_path" => "gates/gr_0001.out",
             "stderr_path" => "gates/gr_0001.err"
           }
  end

  test "successful gate writes output artifacts and returns hashes" do
    run_dir = tmp_dir()
    parent = self()

    runner = fn executable, args, opts ->
      Process.send(parent, {:gate_call, executable, args, opts}, [])
      {"all green\n", 0}
    end

    assert {:ok,
            %{
              "exit_status" => 0,
              "duration_ms" => 42,
              "stdout_hash" => stdout_hash,
              "stderr_merged" => true
            }} = Runner.run(base_gate(), repo_root: "/repo", run_dir: run_dir, runner: runner, duration_ms: 42)

    assert stdout_hash == sha256("all green\n")
    assert File.read!(Path.join(run_dir, "gates/gr_0001.out")) == "all green\n"
    refute File.exists?(Path.join(run_dir, "gates/gr_0001.err"))
    assert_receive {:gate_call, "mix", ["test"], [cd: "/repo", stderr_to_stdout: true]}
  end

  test "failed gate returns a bounded failure summary" do
    runner = fn _executable, _args, _opts ->
      {"""
       mix test failed
       Assertion with == failed
       another error line
       """, 1}
    end

    assert {:failed,
            %{
              "exit_status" => 1,
              "failure_summary" => %{
                "headline" => "mix test failed",
                "failures" => failures,
                "suggestion" => "inspect gate output and retry after fixing the failing check"
              }
            }} = Runner.run(base_gate(), run_dir: tmp_dir(), runner: runner, duration_ms: 42)

    assert length(failures) == 3
  end

  test "failure summary is capped by encoded byte size" do
    multibyte = <<240, 159, 146, 165>>

    runner = fn _executable, _args, _opts ->
      {String.duplicate("Assertion " <> String.duplicate(multibyte, 200) <> "\n", 10), 1}
    end

    assert {:failed, %{"failure_summary" => summary}} =
             Runner.run(base_gate(), run_dir: tmp_dir(), runner: runner, duration_ms: 42)

    assert summary |> Jason.encode!() |> byte_size() <= 4096
    assert String.valid?(summary["headline"])
  end

  test "duration can come from an injected monotonic clock" do
    {:ok, clock} = Agent.start_link(fn -> [1_000, 1_042] end)

    monotonic_ms = fn ->
      Agent.get_and_update(clock, fn [value | rest] -> {value, rest} end)
    end

    assert {:ok, %{"duration_ms" => 42}} =
             Runner.run(base_gate(),
               run_dir: tmp_dir(),
               runner: fn _cmd, _args, _opts -> {"ok", 0} end,
               monotonic_ms: monotonic_ms
             )
  end

  test "invalid gate command is rejected without calling a runner" do
    assert {:error, %{"reason" => "invalid_gate_command"}} = Runner.run(%{"command_argv" => []})
    assert {:error, %{"reason" => "invalid_gate_command"}} = Runner.run(%{"command_argv" => ["mix", :test]})
  end

  test "invalid runner returns are described without payload bytes" do
    secret = "SECRET_PROVIDER_OUTPUT_9f3c"
    runner = fn _executable, _args, _opts -> {:unexpected, secret} end

    assert {:error,
            %{
              "reason" => "gate_runner_invalid_return",
              "result_class" => "tuple",
              "digest" => "sha256:" <> digest
            } = error} = Runner.run(base_gate(), runner: runner)

    assert byte_size(digest) == 64
    refute inspect(error) =~ secret
  end

  test "runner exceptions are described without exception or path bytes" do
    secret_path = "/private/SECRET_GATE_PATH_9f3c"
    runner = fn _executable, _args, _opts -> raise File.Error, reason: :enoent, action: "open", path: secret_path end

    assert {:error,
            %{
              "reason" => "gate_runner_crashed",
              "result_class" => "map",
              "digest" => "sha256:" <> digest
            } = error} = Runner.run(base_gate(), runner: runner)

    assert byte_size(digest) == 64
    refute inspect(error) =~ secret_path
    refute Map.has_key?(error, "detail")
  end

  defp base_gate do
    %{"gate_run_id" => "gr_0001", "command_argv" => ["mix", "test"]}
  end

  defp tmp_dir do
    path = Path.join(System.tmp_dir!(), "ai_orchestrator_gate_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end
end
