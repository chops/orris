defmodule AiOrchestrator.ProjectionTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary

  defp fold!(class, name) do
    {:ok, state} = Fold.fold_lines(F.lines(class, name))
    state
  end

  describe "RunSummary.render/1" do
    test "completed run renders title, status, and a completed work-item row" do
      org = RunSummary.render(fold!("scenarios", "gated_run_seed"))

      assert org =~ "#+title: Run summary — run_scenario_0001"
      assert org =~ "* Status: completed"
      assert org =~ "| item_a | completed |"
      assert org =~ "- last_seq :: 32"
      refute org =~ "* Attention"
    end

    test "blocked run renders a TODO heading per open attention with the resume command" do
      org = RunSummary.render(fold!("scenarios", "auth_blocked_pane"))

      assert org =~ "* Status: BLOCKED — human attention required"
      assert org =~ "** TODO resolve att_0001"
      assert org =~ "=run --resume <run-dir>="
    end

    test "failed run renders the failure reason" do
      org = RunSummary.render(fold!("journals", "valid_minimal"))

      assert org =~ "* Status: failed"
      assert org =~ "- reason :: spec_invalid"
    end

    test "rendering is deterministic and rebuildable from the journal alone" do
      state = fold!("scenarios", "concurrency_cap")
      assert RunSummary.render(state) == RunSummary.render(state)
    end
  end

  describe "RunContext.render/1" do
    test "worldview carries revision, per-item status, and open sets" do
      org = RunContext.render(fold!("journals", "fold_resume_mid_run"))

      assert org =~ "#+title: Run context — run_fixture_0002"
      assert org =~ "- revision :: 0"
      assert org =~ "- item_a :: OPEN"
      assert org =~ "* Open assignments\n- as_0001"
    end

    test "completed items render DONE and empty open sets render none" do
      org = RunContext.render(fold!("scenarios", "concurrency_cap"))

      assert org =~ "- item_a :: DONE"
      assert org =~ "- item_b :: DONE"
      assert org =~ "* Open assignments\n- none"
      assert org =~ "* Open attention\n- none"
    end
  end
end
