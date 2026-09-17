defmodule AiOrchestrator.Prepare.TrustedPathBoundaryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Prepare.Trusted

  # trusted admission (the CLI's validate/start/resume) is where the physical layer of the containment
  # rule runs: the plan check is pure, so a symlink under the repository root is only visible here
  setup do
    base =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_trusted_path_boundary_test",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    repo = Path.join(base, "repo")
    outside = Path.join(base, "outside")
    run_dir = Path.join(base, "run")
    File.mkdir_p!(Path.join(repo, "lib/plain"))
    File.mkdir_p!(outside)
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf!(base) end)
    {:ok, repo: repo, outside: outside, run_dir: run_dir}
  end

  defp seed(run_dir, repo, writer_paths) do
    spec = "plans" |> F.json("valid_linear", "spec.json") |> Map.put("repo_root", repo)
    plan = F.json("plans", "valid_linear", "plan.json")
    [writer | rest] = plan["work_items"]
    plan = Map.put(plan, "work_items", [Map.put(writer, "allowed_paths", writer_paths) | rest])
    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(spec))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(plan))
  end

  test "an in-root allowed path under a real directory is admitted", %{repo: repo, run_dir: run_dir} do
    seed(run_dir, repo, ["lib/plain", "lib/absent/later"])
    assert {:ok, %{spec: %{"repo_root" => ^repo}}} = Trusted.validate(run_dir)
  end

  test "a symlink under an allowed root that leaves the repository refuses admission",
       %{repo: repo, outside: outside, run_dir: run_dir} do
    File.ln_s!(outside, Path.join(repo, "lib/escape"))
    seed(run_dir, repo, ["lib/plain", "lib/escape/x.ex"])

    assert Trusted.validate(run_dir) ==
             {:error, %{clause: "work_item_path_symlink_escape", field: "lib/escape/x.ex"}}
  end

  test "the pure layer still answers first through the same admission", %{repo: repo, run_dir: run_dir} do
    seed(run_dir, repo, ["lib/../etc"])
    assert Trusted.validate(run_dir) == {:error, %{clause: "work_item_path_traversal", field: "lib/../etc"}}
  end

  test "a repository root that does not exist yet is judged lexically only", %{run_dir: run_dir, repo: repo} do
    seed(run_dir, Path.join(repo, "not-yet"), ["lib/x"])
    assert {:ok, _inputs} = Trusted.validate(run_dir)
  end
end
