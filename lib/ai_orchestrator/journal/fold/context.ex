defmodule AiOrchestrator.Journal.Fold.Context do
  @moduledoc """
  The shared run-context view of folded state.

  `derive/1` is the pure transformation from `Fold.State` to the context facts
  the execution core and the read model both consume; `render/1` is the fixed
  org-mode presentation of those facts whose bytes are hashed into the
  assignment prompt. Derivation and presentation stay separate functions so
  formatting can change only as a deliberate, hashed wire change.
  """

  alias AiOrchestrator.Journal.Fold

  @type t :: %{
          run_id: String.t() | nil,
          context_revision: non_neg_integer(),
          work_items: [{String.t(), :done | :open}],
          open_assignment_ids: [String.t()],
          open_attention_ids: [String.t()]
        }

  @spec derive(Fold.State.t()) :: t()
  def derive(%Fold.State{} = state) do
    %{
      run_id: state.run_id,
      context_revision: state.context_revision,
      work_items:
        state.work_item_ids
        |> Enum.sort()
        |> Enum.map(fn id -> {id, if(MapSet.member?(state.completed_work_item_ids, id), do: :done, else: :open)} end),
      open_assignment_ids: Enum.sort(state.open_assignment_ids),
      open_attention_ids: Enum.sort(state.open_attention_ids)
    }
  end

  @spec render(Fold.State.t()) :: String.t()
  def render(%Fold.State{} = state) do
    context = derive(state)

    [
      "#+title: Run context — #{context.run_id}",
      "",
      "* Context revision",
      "- revision :: #{context.context_revision}",
      "",
      "* Work item status",
      status_lines(context.work_items),
      "",
      "* Open assignments",
      list_or_none(context.open_assignment_ids),
      "",
      "* Open attention",
      list_or_none(context.open_attention_ids)
    ]
    |> List.flatten()
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp status_lines([]), do: "- (no plan recorded)"
  defp status_lines(items), do: Enum.map(items, fn {id, status} -> "- #{id} :: #{marker(status)}" end)

  defp marker(:done), do: "DONE"
  defp marker(:open), do: "OPEN"

  defp list_or_none([]), do: "- none"
  defp list_or_none(ids), do: Enum.map(ids, &"- #{&1}")
end
