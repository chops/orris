defmodule AiOrchestrator.PaneRegistry.PaneIdentity do
  @moduledoc """
  Comparison of a claim's recorded daemon pane identity with a version 3 reply's (NS-15.G.005 B3b,
  docs/contracts/ipc-v3.org "Pane identity"): byte-for-byte equality of `pane_id`, `registration_id` and
  `generation`. A generation is a decimal string compared as text, never converted to a number.

  Both sides must be complete (all three keys present as strings); otherwise the answer is
  `{:error, :incomplete_identity}` and never `:match`, so two identities missing the same key do not compare equal.
  """

  @fields ~w(pane_id registration_id generation)
  @registration_id ~r/\Areg_[0-9a-f]{32}\z/
  @generation ~r/\A[0-9]+\z/

  @doc """
  Whether `identity` is a well-formed pane identity: a non-empty `pane_id`, a `registration_id` of `reg_` + 32
  lowercase hex, and a `generation` string of decimal digits (ipc-v3.org L74-89). Extra keys are ignored.
  """
  @spec valid?(term()) :: boolean()
  def valid?(%{"pane_id" => pane_id, "registration_id" => registration_id, "generation" => generation})
      when is_binary(pane_id) and pane_id != "" and is_binary(registration_id) and is_binary(generation) do
    Regex.match?(@registration_id, registration_id) and Regex.match?(@generation, generation)
  end

  def valid?(_identity), do: false

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
