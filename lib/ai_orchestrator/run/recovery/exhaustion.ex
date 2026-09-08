defmodule AiOrchestrator.Run.Recovery.Exhaustion do
  @moduledoc """
  The pure restart-budget judge (docs/contracts/exhaustion-judge.org, U1b-0a): a calculation over SUPPLIED facts.

  `judge/2` takes the `Spec.RunSpec.budgets/1` result shape and a caller-supplied count of reservations already
  in the verified prefix. It performs no IO, holds no state, reads no clock, authenticates no run, Writer or
  journal, and grants no capability: a `{:reserve, _}` verdict is never permission to append or to replace
  effects. The typed tag is not validation evidence - the function is directly callable, so a typed section is
  re-judged by `Spec.Budgets.validate/1`.

  Precedence is fixed: classify and validate the budget view first (a proper legacy map refuses untyped and any
  other malformed view refuses invalid, both BEFORE the count is inspected), then validate the count, then
  decide. Omission of `restart_attempts` (R-e) is neither 0 nor infinity: it refuses `restart_budget_unset`,
  and `max_attempts_default` is never a fallback. Refusals carry the clause only; nothing from the input is
  copied into a refusal. Integers are unbounded and compared exactly.
  """

  alias AiOrchestrator.Spec.Budgets

  @type verdict ::
          {:reserve, pos_integer()}
          | {:exhausted, %{limit: non_neg_integer(), consumed: non_neg_integer()}}
          | {:error, %{clause: String.t()}}

  @spec judge(term(), term()) :: verdict()
  def judge(budget_view, consumed) do
    with {:ok, typed} <- classify(budget_view),
         :ok <- count(consumed) do
      decide(typed, consumed)
    end
  end

  # stage 1: the budget view. A typed section is re-validated; its rejection detail is dropped (never copied).
  defp classify({:typed, section}) do
    case Budgets.validate(section) do
      {:ok, typed} -> {:ok, typed}
      {:error, _rejection} -> refuse("restart_budget_invalid")
    end
  end

  defp classify({:legacy, legacy}) when is_map(legacy), do: refuse("restart_budget_untyped")
  defp classify(_other), do: refuse("restart_budget_invalid")

  # stage 2: the count (booleans are atoms, never integers)
  defp count(consumed) when is_integer(consumed) and consumed >= 0, do: :ok
  defp count(_other), do: refuse("recovery_count_invalid")

  # stage 3: the decision over a VALIDATED typed section
  defp decide(typed, consumed) do
    case Map.fetch(typed, "restart_attempts") do
      :error -> refuse("restart_budget_unset")
      {:ok, limit} when limit > consumed -> {:reserve, consumed + 1}
      {:ok, limit} -> {:exhausted, %{limit: limit, consumed: consumed}}
    end
  end

  defp refuse(clause), do: {:error, %{clause: clause}}
end
