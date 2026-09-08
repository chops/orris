defmodule AiOrchestrator.Contracts.FoldRejectionTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold

  test "valid_minimal: folds to the expected terminal state" do
    expected = F.json("journals", "valid_minimal", "expected.json")
    assert {:ok, state} = Fold.fold_lines(F.lines("journals", "valid_minimal"))
    assert Fold.summary(state) == expected
  end

  for name <- [
        "reject_seq_gap",
        "reject_unknown_event_type",
        "reject_first_event_not_run_created",
        "reject_second_terminal_run",
        "reject_seq_duplicate",
        "reject_event_id_repeat",
        "reject_unknown_entity_ref",
        "reject_second_terminal_assignment",
        "reject_dispatch_without_pane_lease",
        "reject_writer_dispatch_without_workspace_lease",
        "reject_workspace_lease_overlap",
        "reject_completion_without_fresh_gate",
        "reject_review_completes_item",
        "reject_agent_ratified_contract_change",
        "reject_auto_accepted_oracle_change",
        "reject_context_revision_skip",
        "reject_dispatch_future_revision",
        "reject_events_after_terminal",
        "reject_dispatch_during_attention_block",
        "reject_run_completed_mismatch",
        "reject_preamble_violation",
        "reject_run_id_mismatch",
        "reject_gate_unknown_artifact"
      ] do
    test "#{name}: fold rejects with the named EJ-13 clause" do
      name = unquote(name)
      expected = F.json("journals", name, "expected_rejection.json")
      assert {:error, rejection} = Fold.fold_lines(F.lines("journals", name))
      F.assert_rejection_matches(rejection, expected)
    end
  end

  for name <- [
        "fold_happy_writer_reviewer",
        "fold_resume_mid_run",
        "fold_cancel_converged",
        "fold_budget_exhausted",
        "fold_attention_block",
        "fold_attention_resolved"
      ] do
    test "#{name}: folds to the expected state" do
      name = unquote(name)
      expected = F.json("journals", name, "expected.json")
      assert {:ok, state} = Fold.fold_lines(F.lines("journals", name))
      assert Fold.summary(state) == expected
    end
  end
end
