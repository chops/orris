defmodule AiOrchestrator.CLIConfigTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI

  setup do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_cli_config_test",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  # F-1: config rejection must fail closed BEFORE journal creation.
  test "run with malformed env config writes no journal", %{tmp_dir: tmp_dir} do
    run_dir = Path.join(tmp_dir, "run")
    File.mkdir_p!(run_dir)

    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(minimal_spec()))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(minimal_plan()))

    result = CLI.run(["run", run_dir], env: %{"AI_ORCHESTRATOR_POLL_INTERVAL_MS" => "banana"})

    assert result.status != 0
    assert result.stderr =~ "invalid_config"
    refute File.exists?(Path.join(run_dir, "events.jsonl"))
  end

  defp minimal_spec do
    %{
      "schema" => "ai-orchestrator/run-spec",
      "schema_version" => 1,
      "goal" => "config fail-closed test",
      "repo_root" => "/tmp/example-repo",
      "allowed_roots" => ["lib"],
      "agents" => [
        %{"name" => "writer_agent", "role" => "writer"},
        %{"name" => "reviewer_agent", "role" => "reviewer"}
      ],
      "gates" => %{"tests" => ["true"]},
      "budgets" => %{"max_wall_clock_s" => 60, "max_attempts_default" => 1},
      "stop_policy" => %{"max_no_progress_attempts" => 1, "wedge_stale_ms" => 180_000}
    }
  end

  defp minimal_plan do
    %{
      "schema" => "ai-orchestrator/run-plan",
      "schema_version" => 1,
      "plan_id" => "plan_config_test",
      "work_items" => [
        %{
          "id" => "item_a",
          "title" => "Config test item",
          "role" => "writer",
          "deps" => [],
          "allowed_paths" => ["lib"],
          "acceptance" => ["tests"],
          "expected_artifacts" => ["lib/item_a.ex"],
          "kind" => "implement"
        }
      ]
    }
  end
end
