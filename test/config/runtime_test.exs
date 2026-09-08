defmodule AiOrchestrator.Config.RuntimeTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Config.Runtime

  test "defaults to public ap executable name" do
    assert {:ok, %{ap_path: "ap"}} =
             Runtime.resolve(env: %{}, file_reader: fn _path -> {:error, :enoent} end)
  end

  test "local file overrides defaults" do
    assert {:ok, %{ap_path: "/local/ap"}} =
             Runtime.resolve(
               env: %{},
               config_file: "local.json",
               file_reader: fn "local.json" -> {:ok, ~s({"ap_path":"/local/ap"})} end
             )
  end

  test "environment overrides local file" do
    assert {:ok, %{ap_path: "/env/ap"}} =
             Runtime.resolve(
               env: %{"AI_ORCHESTRATOR_AP_PATH" => "/env/ap"},
               config_file: "local.json",
               file_reader: fn "local.json" -> {:ok, ~s({"ap_path":"/local/ap"})} end
             )
  end

  test "explicit flag overrides environment" do
    assert {:ok, %{ap_path: "/flag/ap"}} =
             Runtime.resolve(
               ap_path: "/flag/ap",
               env: %{"AI_ORCHESTRATOR_AP_PATH" => "/env/ap"},
               config_file: "local.json",
               file_reader: fn "local.json" -> {:ok, ~s({"ap_path":"/local/ap"})} end
             )
  end

  test "rejects invalid local config shape" do
    assert {:error, %{"reason" => "invalid_config_file", "path" => "local.json"}} =
             Runtime.resolve(
               env: %{},
               config_file: "local.json",
               file_reader: fn "local.json" -> {:ok, ~s({"unexpected":true})} end
             )
  end

  test "rejects blank ap path" do
    assert {:error, %{"reason" => "invalid_config", "field" => "ap_path"}} =
             Runtime.resolve(ap_path: " ", env: %{}, file_reader: fn _path -> {:error, :enoent} end)
  end
end
