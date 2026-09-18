defmodule AiOrchestrator.CLI.Read do
  @moduledoc """
  The CLI's ONE verified journal read and its two renderings.

  Every directory-based operator read (`status`, the legacy `list` rows and the `--watch` loop over them)
  folds the verified journal prefix through `AiOrchestrator.Prepare.Trusted.read_journal/1` and renders the
  same value. The loaded map's
  `pending_repair` is carried into both renderings, exactly as `list --root` and
  `AiOrchestrator.Query.run_summary/2` already carry it: it is the plan the WRITER would execute, reported as
  data. Nothing here writes, repairs or advances a receipt.
  """

  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Prepare.Trusted
  alias AiOrchestrator.Projection.RunSummary

  @terminal_statuses ~w(completed cancelled failed budget_exhausted)

  @type loaded :: %{state: Fold.State.t(), pending_repair: map() | nil}

  @doc "The verified prefix of a run directory, folded, with the repair plan the read observed."
  @spec load(Path.t()) :: {:ok, loaded()} | {:error, map()}
  def load(run_dir) do
    with {:ok, journal} <- Trusted.read_journal(run_dir),
         {:ok, state} <- Fold.fold_lines(journal.lines) do
      {:ok, %{state: state, pending_repair: journal.pending_repair}}
    end
  end

  @doc "The JSON summary: `Fold.summary/1` plus `pending_repair` ONLY when the read observed one."
  @spec summary(loaded()) :: map()
  def summary(%{state: state, pending_repair: nil}), do: Fold.summary(state)

  def summary(%{state: state, pending_repair: plan}), do: Map.put(Fold.summary(state), "pending_repair", repair_map(plan))

  @doc """
  The legacy `list` row: the fold summary plus the SAME boolean `list --root` has carried since the
  explicit-root unit (`AiOrchestrator.CLI.Discovery`), so both listings answer the repair question alike.
  """
  @spec row(loaded()) :: map()
  def row(%{state: state} = loaded), do: Map.put(Fold.summary(state), "pending_repair", repair?(loaded))

  @doc "The operator rendering: JSON when `json?`, otherwise the Org summary projection."
  @spec render(loaded(), boolean()) :: String.t()
  def render(loaded, true), do: Jason.encode!(summary(loaded)) <> "\n"

  def render(%{state: state, pending_repair: plan}, false), do: RunSummary.render(state) <> repair_line(plan)

  @doc "Whether the recorded status is terminal (nothing later can be appended to this run)."
  @spec terminal?(loaded()) :: boolean()
  def terminal?(%{state: state}), do: Fold.summary(state)["status"] in @terminal_statuses

  @doc "Whether a read observed a pending repair; `nil` keeps the `list --root` shape for an unreadable row."
  @spec repair?(loaded()) :: boolean()
  def repair?(%{pending_repair: plan}), do: not is_nil(plan)

  # the Org projection stays byte-identical (it is also the written run-summary.org); the signal is one line
  defp repair_line(nil), do: ""

  defp repair_line(plan) do
    "- pending_repair :: #{plan.action} (truncate_bytes #{plan.truncate_bytes}, " <>
      "receipt_seq #{plan.receipt_seq_before} -> #{plan.receipt_seq_after})\n"
  end

  defp repair_map(plan) do
    %{
      "action" => Atom.to_string(plan.action),
      "truncate_bytes" => plan.truncate_bytes,
      "receipt_seq_before" => plan.receipt_seq_before,
      "receipt_seq_after" => plan.receipt_seq_after
    }
  end
end
