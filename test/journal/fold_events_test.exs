defmodule AiOrchestrator.Journal.FoldEventsTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Projection.RunContext

  @scenarios ~w(gated_run_seed auth_blocked_pane concurrency_cap fingerprint_drift gate_failure_summary_feedback)

  test "folding decoded events equals folding the persisted lines" do
    for name <- @scenarios do
      lines = F.lines("scenarios", name)
      assert Fold.fold_events(Enum.map(lines, &Jason.decode!/1)) == Fold.fold_lines(lines), name
    end
  end

  test "an invalid decoded event is rejected with the historical clauses" do
    assert {:error, %{clause: "invalid_event_shape"}} = Fold.fold_events([:not_an_event])
    assert {:error, %{clause: "invalid_journal"}} = Fold.fold_events(:not_a_list)
  end

  test "the context view derives facts and renders the bytes the read model shows" do
    for name <- @scenarios do
      {:ok, state} = Fold.fold_lines(F.lines("scenarios", name))
      derived = Fold.Context.derive(state)

      assert derived |> Map.keys() |> Enum.sort() ==
               ~w(context_revision open_assignment_ids open_attention_ids run_id work_items)a

      assert derived.run_id == state.run_id
      rendered = Fold.Context.render(state)
      assert rendered == RunContext.render(state)
      assert String.starts_with?(rendered, "#+title: Run context — #{state.run_id}\n")
    end
  end
end
