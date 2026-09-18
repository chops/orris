defmodule AiOrchestrator.Dispatch.LocalPaneArtifactBoundaryTest do
  @moduledoc """
  R09 slice S2 (NS-20.D.001, NS-20.I.001): the artifact path is judged PHYSICALLY at the snapshot
  boundary, the last moment before the bytes become the run's evidence. `File.stat` and `File.read`
  follow symlinks, so without this judgement an `expected_artifact` that is a symlink out of the
  worktree is baselined, observed and hashed as the assignment's artifact. A rejection answers the
  EXISTING closed class `artifact_baseline_failed`; no new class, event or detail arm.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @outside_bytes "OUT OF SCOPE BYTES\n"

  setup do
    base =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_local_pane_artifact_boundary",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    repo = Path.join(base, "repo")
    outside = Path.join(base, "outside")
    File.mkdir_p!(Path.join(repo, "lib"))
    File.mkdir_p!(Path.join(repo, "docs"))
    File.mkdir_p!(outside)
    File.write!(Path.join(outside, "secret.txt"), @outside_bytes)
    on_exit(fn -> File.rm_rf!(base) end)
    {:ok, base: base, repo: repo, outside: outside}
  end

  defp command(repo, artifact, roots) do
    %{
      "assignment_id" => "as_0001",
      "repo_root" => repo,
      "expected_artifact" => artifact,
      "allowed_roots" => roots
    }
  end

  defp refusal(clause), do: {:error, %{"reason" => "artifact_baseline_failed", "detail" => clause}}

  test "an artifact that is a symlink out of the worktree is refused, not read and hashed",
       %{repo: repo, outside: outside} do
    File.ln_s!(Path.join(outside, "secret.txt"), Path.join(repo, "lib/out.org"))

    answer = LocalPane.snapshot(command(repo, "lib/out.org", ["lib"]))
    assert answer == refusal("path_symlink_escape")
    refute inspect(answer, limit: :infinity) =~ "sha256"
  end

  test "an artifact reached through a symlinked DIRECTORY that leaves the worktree is refused",
       %{repo: repo, outside: outside} do
    File.ln_s!(outside, Path.join(repo, "lib/escape"))
    File.write!(Path.join(outside, "out.org"), @outside_bytes)

    assert LocalPane.snapshot(command(repo, "lib/escape/out.org", ["lib"])) == refusal("path_symlink_escape")
  end

  test "an in-root real artifact is still baselined with its fingerprint", %{repo: repo} do
    File.write!(Path.join(repo, "lib/out.org"), "in scope\n")

    assert {:ok, %{"exists" => true, "bytes" => 9, "sha256" => "sha256:" <> _digest}} =
             LocalPane.snapshot(command(repo, "lib/out.org", ["lib"]))
  end

  test "an in-root artifact that does not exist yet is still the absent baseline", %{repo: repo} do
    assert LocalPane.snapshot(command(repo, "lib/not-yet.org", ["lib"])) == {:ok, %{"exists" => false}}
  end

  test "a symlink INSIDE the worktree and under an allowed root is admitted", %{repo: repo} do
    File.write!(Path.join(repo, "lib/real.org"), "in scope\n")
    File.ln_s!(Path.join(repo, "lib/real.org"), Path.join(repo, "lib/out.org"))

    assert {:ok, %{"exists" => true, "bytes" => 9}} = LocalPane.snapshot(command(repo, "lib/out.org", ["lib"]))
  end

  test "an artifact inside the worktree but outside every allowed root is refused", %{repo: repo} do
    File.write!(Path.join(repo, "docs/out.org"), "in repo, out of roots\n")

    assert LocalPane.snapshot(command(repo, "docs/out.org", ["lib"])) == refusal("path_outside_roots")
  end

  test "form is judged here too: an absolute artifact and a .. artifact are refused", %{repo: repo} do
    assert LocalPane.snapshot(command(repo, "/etc/hosts", ["lib"])) == refusal("path_absolute")
    assert LocalPane.snapshot(command(repo, "lib/../../outside/secret.txt", ["lib"])) == refusal("path_traversal")
  end

  test "a command with no allowed roots is judged against the repository itself", %{repo: repo, outside: outside} do
    File.write!(Path.join(repo, "docs/out.org"), "in repo, no roots declared\n")
    bare = %{"assignment_id" => "as_0001", "repo_root" => repo, "expected_artifact" => "docs/out.org"}

    assert {:ok, %{"exists" => true}} = LocalPane.snapshot(bare)

    File.ln_s!(Path.join(outside, "secret.txt"), Path.join(repo, "docs/escape.org"))

    assert LocalPane.snapshot(%{bare | "expected_artifact" => "docs/escape.org"}) == refusal("path_symlink_escape")
  end

  # The physical layer is point in time (PathBoundary's own documented limit): a repository root
  # that does not exist leaves the lexical layer, which is still the form rule.
  test "an absent repository root leaves the lexical layer only", %{base: base} do
    absent = Path.join(base, "no-such-repo")

    assert LocalPane.snapshot(command(absent, "lib/out.org", ["lib"])) == {:ok, %{"exists" => false}}
    assert LocalPane.snapshot(command(absent, "/etc/hosts", ["lib"])) == refusal("path_absolute")
  end

  describe "the roots the reducer carries to the snapshot boundary" do
    setup do
      H.reset_seams()
      {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
      {:ok, scenario: scenario, opts: opts_fun.()}
    end

    defp snapshot_command(spec, plan, opts) do
      now = %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

      assert {:effect, %Effect.Clock{read_index: 1}, state, _events} = Reducer.init(spec, plan, opts)

      assert {:effect, %Effect.RetainPrompt{assignment_id: id, bytes: %SensitiveBytes{} = bytes}, at_retain, _events} =
               Reducer.step(state, %Observation.Clock{read_index: 1, now: now})

      assert {:effect, %Effect.SnapshotArtifact{command: command}, _state, _events} =
               Reducer.step(at_retain, %Observation.PromptRetained{object: retained(id, bytes), now: now})

      command
    end

    defp retained(assignment_id, %SensitiveBytes{} = bytes) do
      "sha256:" <> hex = hash = SensitiveBytes.hash(bytes)

      {:ok, object} =
        PromptObject.new(%{
          assignment_id: assignment_id,
          path: "prompts/#{assignment_id}-#{hex}.org",
          hash: hash,
          byte_size: SensitiveBytes.byte_size(bytes),
          version: 2
        })

      object
    end

    test "a writer assignment carries the spec's allowed roots", %{scenario: scenario, opts: opts} do
      spec = H.spec(scenario)
      opts = Keyword.merge(opts, run_id: "run_s2_0001", supervisor_instance: "sup_s2_0001")
      command = snapshot_command(spec, H.plan(scenario), opts)

      assert command["allowed_roots"] == spec["allowed_roots"]
      assert command["repo_root"] == spec["repo_root"]
    end

    # a review item names its own document, which need not lie under a writer's root, so the
    # snapshot boundary judges it for containment under the repository and nothing more
    test "a review assignment carries the repository itself", %{scenario: scenario, opts: opts} do
      spec = H.spec(scenario)
      plan = review_only_plan(H.plan(scenario))
      opts = Keyword.merge(opts, run_id: "run_s2_0002", supervisor_instance: "sup_s2_0002")

      assert snapshot_command(spec, plan, opts)["allowed_roots"] == ["."]
    end

    defp review_only_plan(plan) do
      item =
        plan["work_items"]
        |> hd()
        |> Map.merge(%{
          "id" => "item_r",
          "deps" => [],
          "kind" => "review",
          "role" => "reviewer",
          "allowed_paths" => ["review"],
          "expected_artifacts" => ["review/item_r.org"]
        })

      Map.put(plan, "work_items", [item])
    end
  end
end
