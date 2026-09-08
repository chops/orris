defmodule AiOrchestrator.Projection.RunSummary do
  @moduledoc false

  alias AiOrchestrator.Journal.Fold

  @doc """
  Renders the operator-facing run summary as an org-mode document.

  Pure projection of folded state: deletable, rebuildable, never journaled.
  Carries no wall-clock reads — every fact comes from the fold.
  """
  @spec render(Fold.State.t()) :: String.t()
  def render(%Fold.State{} = state) do
    [
      "#+title: Run summary — #{state.run_id}",
      "",
      "* Status: #{status_label(state)}",
      status_detail(state),
      "",
      "* Work items",
      work_item_table(state),
      attention_section(state),
      "",
      "* Journal position",
      "- last_seq :: #{state.last_seq}",
      "- context_revision :: #{state.context_revision}"
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp status_label(%{status: "blocked"}), do: "BLOCKED — human attention required"
  defp status_label(%{status: status}), do: status

  defp status_detail(%{status: "failed", reason: reason}), do: "- reason :: #{reason}"
  defp status_detail(_state), do: nil

  defp work_item_table(state) do
    completed = state.completed_work_item_ids

    rows =
      for id <- Enum.sort(state.work_item_ids) do
        marker = if MapSet.member?(completed, id), do: "completed", else: "pending"
        "| #{id} | #{marker} |"
      end

    case rows do
      [] -> "(no work items recorded)"
      rows -> ["| Work item | State |", "|-----------+-------|" | rows]
    end
  end

  defp attention_section(%{open_attention_ids: open} = _state) do
    case Enum.sort(open) do
      [] ->
        nil

      ids ->
        [
          "",
          "* Attention"
          | Enum.map(ids, fn id ->
              "** TODO resolve #{id}\nResume with =run --resume <run-dir>= after resolving."
            end)
        ]
    end
  end
end
