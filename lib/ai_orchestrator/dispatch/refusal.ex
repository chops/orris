defmodule AiOrchestrator.Dispatch.Refusal do
  @moduledoc false

  # NS-42 rules 6, 8, 10 and 11 on the consumer side of a v2 reply that is not ok.
  #
  # A refusal is never an answer about delivery -- none of these may be read as `absent`,
  # and every one of them exits through the same failed observation to attention -- but it
  # is not one undifferentiated failure either. The daemon's word is reflected only when it
  # is in the closed vocabulary the contract names, split the way the contract splits it:
  #
  #   * a typed refusal (ipc-v1.org "Reply fixtures": pane_not_found, pane_dead, queue_full,
  #     send_timeout, paste_failed; ipc-v2.org "Send replies": conflict) is a fact about the
  #     pane or about admission, and becomes `dispatch_refused_<word>` with the word beside
  #     it as `refusal`. It is about THIS send only if both echoes match (rule 3), so a
  #     refusal about another message or pane is an identity error, not a refusal.
  #   * a request error (ipc-v1.org: missing_pane_id, missing_text, oversize; ipc-v2.org:
  #     missing_msg_id, missing_payload_hash, invalid_msg_id, invalid_pane_id) is a fact
  #     about the request this consumer built, and becomes `dispatch_request_rejected` with
  #     the word as `error`. The daemon echoes only the identities it could read, so no echo
  #     is required here -- and for the two `invalid_*` words that is the whole point: an
  #     identity that failed the daemon's grammar is one it cannot safely echo, so it drops
  #     that echo. Without these two words in the vocabulary, a malformed identity degraded
  #     into `reply_not_ok`, which does not repeat the word, leaving the operator with no
  #     name for a failure whose fix is in the request this consumer built.
  #   * any other word -- including words the daemon may emit that the contract does not
  #     name -- and any refusal that does not name version 2 stays `reply_not_ok`, and the
  #     word is not repeated.
  #
  # The detector names the operation whose reply this was; the reducer routes on `reason`.

  @send_refusals ~w(conflict pane_not_found pane_dead queue_full send_timeout paste_failed)
  @send_request_errors ~w(missing_msg_id missing_pane_id missing_text oversize invalid_msg_id invalid_pane_id)
  @reconcile_refusals []
  @reconcile_request_errors ~w(missing_payload_hash missing_msg_id missing_pane_id invalid_msg_id invalid_pane_id)

  @type operation :: :send | :reconcile

  @doc false
  @spec vocabulary(operation()) :: %{refusals: [String.t()], request_errors: [String.t()]}
  def vocabulary(:send), do: %{refusals: @send_refusals, request_errors: @send_request_errors}
  def vocabulary(:reconcile), do: %{refusals: @reconcile_refusals, request_errors: @reconcile_request_errors}

  @doc false
  @spec classify(term(), operation(), String.t(), String.t()) :: {:error, map()}
  def classify(%{"ok" => false, "protocol_version" => 2} = reply, operation, pane_ref, message_id)
      when operation in [:send, :reconcile] do
    word = Map.get(reply, "error")
    %{refusals: refusals, request_errors: request_errors} = vocabulary(operation)

    cond do
      word in refusals -> refused(reply, operation, word, pane_ref, message_id)
      word in request_errors -> {:error, rejected(operation, word)}
      true -> not_ok()
    end
  end

  def classify(_reply, _operation, _pane_ref, _message_id), do: not_ok()

  defp refused(reply, operation, word, pane_ref, message_id) do
    with :ok <- bound(reply, pane_ref, message_id) do
      {:error, %{"reason" => "dispatch_refused_" <> word, "detector" => detector(operation), "refusal" => word}}
    end
  end

  defp rejected(operation, word),
    do: %{"reason" => "dispatch_request_rejected", "detector" => detector(operation), "error" => word}

  defp detector(:send), do: "dispatch_send"
  defp detector(:reconcile), do: "dispatch_reconcile"

  defp not_ok, do: {:error, %{"reason" => "reply_not_ok"}}

  # The same identity clauses PaneClient and LocalPane apply to an ok reply, in the same
  # order: the message first, then the pane.
  defp bound(%{"msg_id" => message_id, "pane_id" => pane_ref}, pane_ref, message_id), do: :ok
  defp bound(%{"msg_id" => message_id, "pane_id" => _other}, _pane_ref, message_id), do: identity("reply_pane_mismatch")
  defp bound(%{"msg_id" => message_id}, _pane_ref, message_id), do: identity("reply_pane_missing")
  defp bound(%{"msg_id" => _other}, _pane_ref, _message_id), do: identity("reply_identity_mismatch")
  defp bound(_reply, _pane_ref, _message_id), do: identity("reply_identity_missing")

  defp identity(reason), do: {:error, %{"reason" => reason}}
end
