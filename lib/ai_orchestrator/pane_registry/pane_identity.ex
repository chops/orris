defmodule AiOrchestrator.PaneRegistry.PaneIdentity do
  @moduledoc """
  Comparison of a claim's recorded daemon pane identity with a version 3 reply's (NS-15.G.005 B3b,
  docs/contracts/ipc-v3.org "Pane identity"): byte-for-byte equality of `pane_id`, `registration_id` and
  `generation`. A generation is a decimal string compared as text, never converted to a number.

  Both sides must be complete (all three keys present as strings); otherwise the answer is
  `{:error, :incomplete_identity}` and never `:match`, so two identities missing the same key do not compare equal.
  """

  @fields ~w(pane_id registration_id generation)

  @spec compare(term(), term()) :: :match | {:mismatch, [String.t()]} | {:error, :incomplete_identity}
  def compare(claim_identity, reply_identity) do
    if complete?(claim_identity) and complete?(reply_identity) do
      case Enum.reject(@fields, &(Map.fetch!(claim_identity, &1) == Map.fetch!(reply_identity, &1))) do
        [] -> :match
        fields -> {:mismatch, fields}
      end
    else
      {:error, :incomplete_identity}
    end
  end

  defp complete?(identity) when is_map(identity), do: Enum.all?(@fields, &is_binary(Map.get(identity, &1)))
  defp complete?(_identity), do: false
end
