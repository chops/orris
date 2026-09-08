defmodule AiOrchestrator.Config.RuntimePrecedenceTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Config.Runtime

  # F-1: full precedence chain per leg, fail-closed on malformed declared values.

  defp no_file, do: [file_reader: fn _path -> {:error, :enoent} end]

  test "defaults apply when nothing is declared" do
    assert {:ok, config} = Runtime.resolve(no_file() ++ [env: %{}])
    assert config[:poll_interval_ms] == 250
    assert config[:default_assignment_timeout_s] == 900
    assert config[:ap_path] == "ap"
    assert config[:pane_registry_root] == Path.expand("~/.local/state/ai-orchestrator/pane-claims")
  end

  test "file overrides defaults" do
    reader = fn _path -> {:ok, ~s({"poll_interval_ms": 500})} end
    assert {:ok, config} = Runtime.resolve(env: %{}, file_reader: reader)
    assert config[:poll_interval_ms] == 500
  end

  test "env overrides file" do
    reader = fn _path -> {:ok, ~s({"poll_interval_ms": 500})} end

    assert {:ok, config} =
             Runtime.resolve(env: %{"AI_ORCHESTRATOR_POLL_INTERVAL_MS" => "750"}, file_reader: reader)

    assert config[:poll_interval_ms] == 750
  end

  test "explicit flag overrides env and file" do
    reader = fn _path -> {:ok, ~s({"poll_interval_ms": 500})} end

    assert {:ok, config} =
             Runtime.resolve(
               env: %{"AI_ORCHESTRATOR_POLL_INTERVAL_MS" => "750"},
               file_reader: reader,
               poll_interval_ms: 990
             )

    assert config[:poll_interval_ms] == 990
  end

  test "timeout precedence: env over file, flag over env" do
    reader = fn _path -> {:ok, ~s({"default_assignment_timeout_s": 100})} end
    env = %{"AI_ORCHESTRATOR_ASSIGNMENT_TIMEOUT_S" => "200"}

    assert {:ok, config} = Runtime.resolve(env: env, file_reader: reader)
    assert config[:default_assignment_timeout_s] == 200

    assert {:ok, config} =
             Runtime.resolve(env: env, file_reader: reader, default_assignment_timeout_s: 300)

    assert config[:default_assignment_timeout_s] == 300
  end

  test "pane registry root follows flag over env over file over XDG state defaults" do
    reader = fn _path -> {:ok, ~s({"pane_registry_root":"/file/claims"})} end

    assert {:ok, %{pane_registry_root: "/env/claims"}} =
             Runtime.resolve(
               env: %{"AI_ORCHESTRATOR_PANE_REGISTRY_ROOT" => "/env/claims"},
               file_reader: reader
             )

    assert {:ok, %{pane_registry_root: "/flag/claims"}} =
             Runtime.resolve(
               pane_registry_root: "/flag/claims",
               env: %{"AI_ORCHESTRATOR_PANE_REGISTRY_ROOT" => "/env/claims"},
               file_reader: reader
             )

    assert {:ok, %{pane_registry_root: "/state/ai-orchestrator/pane-claims"}} =
             Runtime.resolve(env: %{"XDG_STATE_HOME" => "/state"}, file_reader: fn _path -> {:error, :enoent} end)
  end

  test "malformed declared env value fails closed, never silently dropped" do
    assert {:error, %{"reason" => "invalid_config", "field" => "poll_interval_ms"}} =
             Runtime.resolve(no_file() ++ [env: %{"AI_ORCHESTRATOR_POLL_INTERVAL_MS" => "not-an-int"}])
  end

  test "non-positive values are rejected wherever declared" do
    assert {:error, %{"reason" => "invalid_config"}} =
             Runtime.resolve(no_file() ++ [env: %{}, poll_interval_ms: 0])

    reader = fn _path -> {:ok, ~s({"default_assignment_timeout_s": -5})} end
    assert {:error, %{"reason" => "invalid_config"}} = Runtime.resolve(env: %{}, file_reader: reader)
  end

  test "invalid config file fails closed" do
    reader = fn _path -> {:ok, "{ not json"} end
    assert {:error, %{"reason" => _reason}} = Runtime.resolve(env: %{}, file_reader: reader)
  end
end
