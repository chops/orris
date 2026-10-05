defmodule AiOrchestrator.Dispatch.V3Status do
  @moduledoc """
  Decoder for a version 3 `status {pane_id}` reply (NS-15.G.005 B3b, docs/contracts/ipc-v3.org "Pane identity" and
  "Status"). Consumer side only: no producer emits version 3 yet.

  - An ok status answers `{:ok, %{"state", "quarantined", "queue_depth", "pane_pid", "pane_identity"}}`, where
    `pane_identity` is exactly `%{"pane_id", "registration_id", "generation"}` and `generation` stays a string.
  - A typed refusal (`ok: false`) answers `{:refused, error}`; a refusal names no registration and needs no identity.
  - Anything else is a reply-identity error, `{:error, :reply_identity, detail}`, and is never used for a claim
    comparison: a version other than 3, a `pane_id` echo that is not the asked pane, an ok status without
    `pane_identity`, an identity naming another pane, a `registration_id` outside `reg_` + 32 lowercase hex, a
    `generation` that is not a string of decimal digits (a JSON number included), or an ok status missing any of
    `state` (non-empty string), `quarantined` (boolean), `queue_depth` (integer >= 0) and `pane_pid` (integer > 0).
  """

  @identity_keys ~w(pane_id registration_id generation)
  @status_keys ~w(state quarantined queue_depth pane_pid)
  @registration_id ~r/\Areg_[0-9a-f]{32}\z/
  @generation ~r/\A[0-9]+\z/

  @spec decode(binary(), String.t()) ::
          {:ok, map()} | {:refused, String.t()} | {:error, :reply_identity, String.t()}
  def decode(bytes, pane_ref) when is_binary(bytes) and is_binary(pane_ref) do
    case Jason.decode(bytes) do
      {:ok, %{} = reply} -> classify(reply, pane_ref)
      _not_an_object -> reply_identity("not_a_json_object")
    end
  end

  defp classify(%{"protocol_version" => 3, "pane_id" => pane_ref} = reply, pane_ref), do: answer(reply, pane_ref)
  defp classify(%{"protocol_version" => 3}, _pane_ref), do: reply_identity("pane_id_echo")
  defp classify(_reply, _pane_ref), do: reply_identity("protocol_version")

  defp answer(%{"ok" => false, "error" => error}, _pane_ref) when is_binary(error), do: {:refused, error}

  defp answer(%{"ok" => true, "pane_identity" => identity} = reply, pane_ref) do
    cond do
      not identity?(identity, pane_ref) -> reply_identity("pane_identity_invalid")
      not status_fields?(reply) -> reply_identity("status_fields")
      true -> {:ok, reply |> Map.take(@status_keys) |> Map.put("pane_identity", Map.take(identity, @identity_keys))}
    end
  end

  defp answer(%{"ok" => true}, _pane_ref), do: reply_identity("pane_identity_missing")
  defp answer(_reply, _pane_ref), do: reply_identity("reply_shape")

  defp identity?(%{"pane_id" => pane_ref, "registration_id" => registration_id, "generation" => generation}, pane_ref)
       when is_binary(registration_id) and is_binary(generation),
       do: Regex.match?(@registration_id, registration_id) and Regex.match?(@generation, generation)

  defp identity?(_identity, _pane_ref), do: false

  # ipc-v3.org Status: an ok status answers all four of state, quarantined, queue_depth and pane_pid
  defp status_fields?(%{"state" => state, "quarantined" => quarantined, "queue_depth" => depth, "pane_pid" => pid})
       when is_binary(state) and state != "" and is_boolean(quarantined) and is_integer(depth) and depth >= 0 and
              is_integer(pid) and pid > 0,
       do: true

  defp status_fields?(_reply), do: false

  defp reply_identity(detail), do: {:error, :reply_identity, detail}
end
