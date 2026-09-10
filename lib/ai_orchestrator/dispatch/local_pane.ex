defmodule AiOrchestrator.Dispatch.LocalPane do
  @moduledoc false

  @behaviour AiOrchestrator.Dispatch

  alias AiOrchestrator.Contract.ArtifactBaseline
  alias AiOrchestrator.Dispatch.PaneClient

  @default_observe_timeout_ms 900_000
  @default_poll_interval_ms 250
  @zero_hash "sha256:0000000000000000000000000000000000000000000000000000000000000000"

  @impl true
  @max_delivery_attempts 2

  def deliver(command, opts \\ []) when is_map(command) do
    pane_ref = fetch!(command, "pane_ref")
    prompt = Map.get(command, "prompt", "")
    send_message_id = fetch!(command, "send_message_id")
    pane_client = Keyword.get(opts, :pane_client, PaneClient)
    send_opts = opts |> Keyword.put(:message_id, send_message_id) |> Keyword.put(:protocol_version, 2)

    # R2: the daemon is asked before EVERY attempted dispatch, fresh runs included, so there
    # is one path and not a resume special case. Its five-valued answer decides everything:
    # absent is the only case that may send; delivered and queued are reconstructed from the
    # receipt and never resent; ambiguous and conflict are refused for attention, because the
    # daemon cannot prove the bytes did not land and a guess either duplicates or drops work.
    with :ok <- proven(pane_client, opts),
         {:ok, %{"outcome" => outcome, "delivery_attempt" => attempt}} <- reconcile(command, opts),
         {:ok, artifact_baseline} <- durable_baseline(command, outcome) do
      case outcome do
        # The attempt budget is refused on this path as well as in the lifecycle: a stored
        # absence that already closed the second attempt is not an invitation to a third
        # paste, whoever is asking. The lifecycle's own bound reads the same number.
        "absent" when attempt >= @max_delivery_attempts ->
          {:error,
           %{
             "reason" => "dispatch_attempts_exhausted",
             "detector" => "dispatch_reconcile",
             "delivery_attempt" => attempt
           }}

        "absent" ->
          send_now(command, pane_client, pane_ref, prompt, send_message_id, send_opts, artifact_baseline)

        replayed when replayed in ["delivered", "queued"] ->
          reconstructed(replayed, command, pane_ref, send_message_id, artifact_baseline)

        unproven ->
          {:error, refusal(unproven)}
      end
    end
  end

  defp send_now(command, pane_client, pane_ref, prompt, send_message_id, send_opts, artifact_baseline) do
    with {:ok, response} <- pane_client.send(pane_ref, prompt, send_opts),
         :ok <- send_answered(response, send_message_id, pane_ref),
         {:ok, send_status} <- send_status(response) do
      case send_status do
        # D5: a duplicate is a receipt view, and it must reach the same conclusion a
        # reconciliation would from the same fact, so it is routed as one.
        {:duplicate, view} ->
          duplicate(view, command, pane_ref, send_message_id, artifact_baseline)

        status when is_binary(status) ->
          {:ok, dispatch_data(command, pane_ref, send_message_id, artifact_baseline, status, false)}
      end
    end
  end

  # A receipt the daemon reports as delivered is reconstructed as `reconciled`: rebuilt from
  # the receipt, and the daemon's proof that the bytes landed. A receipt the daemon reports
  # as queued is exactly as unpasted as a fresh queued send, so it is journaled as `queued`
  # (replayed) and the lifecycle converges it the same way -- collapsing it to `reconciled`
  # would start observation for a prompt nobody has pasted yet.
  defp reconstructed("delivered", command, pane_ref, send_message_id, artifact_baseline),
    do: {:ok, dispatch_data(command, pane_ref, send_message_id, artifact_baseline, "reconciled", true)}

  defp reconstructed("queued", command, pane_ref, send_message_id, artifact_baseline),
    do: {:ok, dispatch_data(command, pane_ref, send_message_id, artifact_baseline, "queued", true)}

  # NS-42 rule 11: a duplicate is the receipt view -- status, delivery_attempt and the
  # identities -- and it is read exactly as a reconcile answer of the same status. A
  # delivered receipt reconstructs; a queued receipt reconstructs as queued; a pending
  # receipt is admitted and not pasted, which from here is the same fact as queued, so it
  # is journaled as queued and the lifecycle's bounded, budgeted convergence owns what
  # happens next -- deliver neither waits nor admits a second attempt on its own. An
  # ambiguous receipt is refused: the bytes may have landed, and a second paste is a
  # duplicate prompt. The view must name this command's payload: a different hash under
  # the same id is not a duplicate of this send, whatever the daemon called it.
  defp duplicate(%{"status" => status} = view, command, pane_ref, send_message_id, artifact_baseline) do
    with :ok <- same_payload(view, command) do
      case status do
        "delivered" -> reconstructed("delivered", command, pane_ref, send_message_id, artifact_baseline)
        "queued" -> reconstructed("queued", command, pane_ref, send_message_id, artifact_baseline)
        "pending" -> reconstructed("queued", command, pane_ref, send_message_id, artifact_baseline)
        "ambiguous" -> {:error, refusal("ambiguous")}
      end
    end
  end

  defp same_payload(%{"payload_hash" => hash}, %{"payload_hash" => hash}), do: :ok

  defp same_payload(_view, _command),
    do: {:error, %{"reason" => "duplicate_identity_mismatch", "detector" => "dispatch_reconcile"}}

  # M3: the refusal names the concrete class and the check that fired -- a fact about the
  # run -- and never a region of the implementation.
  defp refusal(outcome),
    do: %{"reason" => "dispatch_reconcile_" <> outcome, "detector" => "dispatch_reconcile", "outcome" => outcome}

  # EJ-7: `reconciled` names an event rebuilt from a receipt rather than from a send this
  # process performed; `replayed` carries the same fact under its existing name.
  defp dispatch_data(command, pane_ref, send_message_id, artifact_baseline, send_status, replayed?) do
    maybe_put_hash(
      %{
        "assignment_id" => fetch!(command, "assignment_id"),
        "artifact_baseline" => artifact_baseline,
        "backend" => "local_pane",
        "pane_ref" => pane_ref,
        "send_status" => send_status,
        "send_message_id" => send_message_id,
        "replayed" => replayed? or Map.get(command, "replayed", false)
      },
      Map.get(command, "payload_hash")
    )
  end

  # The digest the send was bound to travels with the event, so a reconstructed dispatch is
  # readable as being about the same bytes the projection names.
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_hash(data, hash) when is_binary(hash), do: Map.put(data, "prompt_hash", hash)
  defp maybe_put_hash(data, _none), do: data

  @receipt_outcomes ~w(delivered queued ambiguous)
  @reconcile_outcomes ~w(delivered queued absent ambiguous conflict)

  @doc """
  Asks the daemon what became of a previously journaled dispatch.

  The daemon is the idempotency authority (ruling R3): it is the only party that
  observed whether the bytes reached the pane, so it — not the orchestrator's own
  journal — answers the question replay cannot. The answer is five-valued;
  anything outside that set is an error rather than a guess, because a
  misread outcome either loses work or duplicates a paste.
  """
  @impl true
  @spec reconcile(map(), keyword()) :: {:ok, map()} | {:error, map()}
  def reconcile(command, opts \\ []) when is_map(command) do
    pane_ref = fetch!(command, "pane_ref")
    send_message_id = fetch!(command, "send_message_id")
    pane_client = Keyword.get(opts, :pane_client, PaneClient)

    # MUST-2: the bound hash is the one already journaled on the projection, carried on the
    # command as payload_hash. A command with none cannot ask a bound question at all.
    with {:ok, payload_hash} <- payload_hash(command),
         reconcile_opts = opts |> Keyword.put(:payload_hash, payload_hash) |> Keyword.put(:protocol_version, 2),
         {:ok, response} <- pane_client.reconcile(pane_ref, send_message_id, reconcile_opts),
         :ok <- versioned(response),
         :ok <- bound(response, send_message_id, pane_ref),
         {:ok, outcome} <- validate_outcome(response),
         {:ok, attempt} <- delivery_attempt(response, outcome) do
      {:ok,
       maybe_put(
         %{
           "assignment_id" => fetch!(command, "assignment_id"),
           "backend" => "local_pane",
           "delivery_attempt" => attempt,
           "outcome" => outcome,
           "pane_ref" => pane_ref,
           "send_message_id" => send_message_id
         },
         "status",
         Map.get(response, "status")
       )}
    end
  end

  # The declared capability is obtained from the daemon on this invocation. Unknown
  # well-formed wire capabilities are additive; Dispatch validates the result before
  # deciding whether its required capability is present.
  @impl true
  def capabilities(opts) do
    pane_client = Keyword.get(opts, :pane_client, PaneClient)

    with {:ok, tokens} <- pane_client.capabilities(Keyword.put(opts, :protocol_version, 2)) do
      if AiOrchestrator.Dispatch.valid_capabilities?(tokens),
        do: {:ok, Enum.filter(["delivery_reconcile"], &(&1 in tokens))},
        else: {:error, %{"reason" => "dispatch_capabilities_invalid"}}
    end
  end

  defp proven(_pane_client, opts), do: AiOrchestrator.Dispatch.preflight(__MODULE__, opts)

  defp payload_hash(%{"payload_hash" => hash}) when is_binary(hash), do: {:ok, hash}
  defp payload_hash(_command), do: {:error, %{"reason" => "reconcile_payload_hash_missing"}}

  # D2 / M5 / MUST-8, applied to the reducer-facing double exactly as PaneClient applies them
  # to the real daemon: a v2 answer is ok, names version 2, and echoes BOTH the message and
  # the pane. The request decides the protocol; the reply never does.
  defp versioned(%{"ok" => true, "protocol_version" => 2}), do: :ok
  defp versioned(%{"ok" => true}), do: {:error, %{"reason" => "protocol_version_unsupported"}}
  defp versioned(%{}), do: {:error, %{"reason" => "reply_not_ok"}}
  defp versioned(_not_a_map), do: {:error, %{"reason" => "send_reply_invalid"}}

  defp bound(%{"msg_id" => message_id, "pane_id" => pane_ref}, message_id, pane_ref), do: :ok

  defp bound(%{"msg_id" => message_id, "pane_id" => _other}, message_id, _pane_ref),
    do: {:error, %{"reason" => "reply_pane_mismatch"}}

  defp bound(%{"msg_id" => message_id}, message_id, _pane_ref), do: {:error, %{"reason" => "reply_pane_missing"}}
  defp bound(%{"msg_id" => _other}, _message_id, _pane_ref), do: {:error, %{"reason" => "reply_identity_mismatch"}}
  defp bound(%{}, _message_id, _pane_ref), do: {:error, %{"reason" => "reply_identity_missing"}}

  # M6: deliver always requests version 2, so a v2 reply is never allowed to become legacy
  # by omitting a field. Every send reply is ok, names version 2, echoes both identities,
  # and carries a status this adapter maps. The pre-receipt, no-message-id compatibility
  # lives in PaneClient.send/3's decode-only path, not here.
  defp send_answered(reply, message_id, pane_ref) do
    with :ok <- versioned(reply), do: bound(reply, message_id, pane_ref)
  end

  defp validate_outcome(%{"outcome" => outcome}) when outcome in @reconcile_outcomes, do: {:ok, outcome}

  # The outcome is the daemon's word; a word outside the closed set is not repeated into a
  # diagnostic, it is named as outside the set.
  defp validate_outcome(_response), do: {:error, %{"reason" => "reconcile_outcome_invalid"}}

  # The receipt's attempt count is authoritative for the retry bound. Only a genuine
  # no-record answer -- absent or conflict with no stored status -- may read as 0; every
  # answer that speaks for a stored receipt (any `status`, or a delivered / queued /
  # ambiguous outcome) must name the positive attempt it is about. A missing or
  # non-positive count there is refused, not defaulted: a defaulted 0 would reopen the
  # allowance the daemon just said was spent.
  defp delivery_attempt(%{"status" => _stored} = response, _outcome), do: stored_attempt(response)
  defp delivery_attempt(response, outcome) when outcome in @receipt_outcomes, do: stored_attempt(response)

  defp delivery_attempt(%{"delivery_attempt" => attempt}, _no_record) when is_integer(attempt) and attempt >= 0,
    do: {:ok, attempt}

  defp delivery_attempt(%{"delivery_attempt" => _not_a_count}, _no_record),
    do: {:error, %{"reason" => "reconcile_attempt_invalid"}}

  defp delivery_attempt(_response, _no_record), do: {:ok, 0}

  defp stored_attempt(%{"delivery_attempt" => attempt}) when is_integer(attempt) and attempt > 0, do: {:ok, attempt}
  defp stored_attempt(_response), do: {:error, %{"reason" => "reconcile_attempt_invalid"}}

  # EJ-7 ratified `send_status` as `ok | queued | reconciled`, and the journal keeps that
  # vocabulary: `ap`'s own `sent` is normalized to `ok`, `queued` stays `queued`, and a
  # typed duplicate is a receipt view. Anything else -- including no status at all -- is
  # refused rather than guessed into the enum -- reflecting the daemon's word would widen
  # ratified journal data without an event_version bump, and it is the daemon's word, so
  # it is not repeated in the refusal either. `reconciled` is never produced here: it
  # names an event rebuilt from a receipt, which is the reconcile path's to say.
  # The duplicate flag is routed before any ordinary status (NS-42 rule 11): a view with
  # the flag is a receipt, whatever its status field says, and a status string of
  # "duplicate" is not a v2 shape at all. The view is validated here for its own shape --
  # a closed stored status, a positive attempt, and an outcome (if it carries one) that
  # agrees with that status -- and against the command's identity in duplicate/5.
  defp send_status(%{"duplicate" => true} = view), do: duplicate_view(view)
  defp send_status(%{"status" => "sent"}), do: {:ok, "ok"}
  defp send_status(%{"status" => "queued"}), do: {:ok, "queued"}
  defp send_status(%{"status" => _unmapped}), do: {:error, %{"reason" => "send_status_unmapped"}}
  # A reply with no status at all is not a v2 send reply, whatever else it echoes.
  defp send_status(%{} = _no_status), do: {:error, %{"reason" => "send_status_unmapped"}}

  @duplicate_statuses ~w(pending queued delivered ambiguous)

  defp duplicate_view(%{"status" => status, "delivery_attempt" => attempt, "payload_hash" => hash} = view)
       when status in @duplicate_statuses and is_integer(attempt) and attempt > 0 and is_binary(hash) do
    expected = outcome_of(status)

    if Map.get(view, "outcome", expected) == expected,
      do: {:ok, {:duplicate, view}},
      else: {:error, %{"reason" => "duplicate_view_invalid"}}
  end

  # not_delivered is never a duplicate (admission opens a new attempt instead); anything
  # else outside the closed set, or a view missing its attempt or identity, is not a
  # receipt this adapter will act on.
  defp duplicate_view(_view), do: {:error, %{"reason" => "duplicate_view_invalid"}}

  defp outcome_of("pending"), do: "ambiguous"
  defp outcome_of(status), do: status

  @impl true
  def observe(command, opts \\ []) when is_map(command) do
    clock = Keyword.get(opts, :monotonic_ms, fn -> System.monotonic_time(:millisecond) end)
    timeout_ms = Keyword.get(opts, :observe_timeout_ms, @default_observe_timeout_ms)
    deadline_ms = clock.() + timeout_ms

    poll_observation(command, opts, clock, deadline_ms, nil)
  end

  defp poll_observation(command, opts, clock, deadline_ms, candidate) do
    case observe_once(command, opts) do
      {:pending, reason} ->
        next_candidate = if Map.has_key?(reason, "pane_state"), do: candidate
        retry_observation(command, opts, clock, deadline_ms, next_candidate, reason)

      {:candidate, artifact, fingerprint} ->
        stabilize_candidate(command, opts, clock, deadline_ms, candidate, artifact, fingerprint)

      result ->
        result
    end
  end

  defp stabilize_candidate(command, opts, clock, deadline_ms, candidate, artifact, fingerprint) do
    now_ms = clock.()
    required_ms = Map.get(command, "stable_for_ms", 5_000)

    case candidate do
      {^fingerprint, first_seen_ms} when now_ms - first_seen_ms >= required_ms ->
        {:ok, Map.put(artifact, "stable_for_ms", now_ms - first_seen_ms)}

      {^fingerprint, first_seen_ms} ->
        retry_observation(
          command,
          opts,
          clock,
          deadline_ms,
          {fingerprint, first_seen_ms},
          %{"reason" => "artifact_not_stable", "stable_for_ms" => now_ms - first_seen_ms}
        )

      _new_candidate when required_ms == 0 ->
        {:ok, Map.put(artifact, "stable_for_ms", 0)}

      _new_candidate ->
        retry_observation(
          command,
          opts,
          clock,
          deadline_ms,
          {fingerprint, now_ms},
          %{"reason" => "artifact_not_stable", "stable_for_ms" => 0}
        )
    end
  end

  defp retry_observation(command, opts, clock, deadline_ms, candidate, reason) do
    if clock.() >= deadline_ms do
      {:error, %{"reason" => "observation_timeout", "last_observation" => reason}}
    else
      sleeper = Keyword.get(opts, :sleeper, &Process.sleep/1)
      sleeper.(Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms))
      poll_observation(command, opts, clock, deadline_ms, candidate)
    end
  end

  defp observe_once(command, opts) do
    pane_ref = fetch!(command, "pane_ref")
    pane_client = Keyword.get(opts, :pane_client, PaneClient)

    with {:ok, pane_status} <- pane_client.status(pane_ref, opts),
         :ok <- ensure_idle(pane_status) do
      read_artifact(command, opts)
    end
  end

  defp ensure_idle(%{"state" => "idle"}), do: :ok

  defp ensure_idle(%{"state" => "blocked"} = status) do
    {:blocked,
     %{
       "reason" => "agent_auth_blocked",
       "pane_ref" => status["pane_id"] || status["pane_ref"],
       "pane_state" => "blocked",
       "pending_count" => Map.get(status, "pending_count", 0)
     }}
  end

  defp ensure_idle(status), do: {:pending, %{"pane_state" => status["state"], "pending_count" => status["pending_count"]}}

  defp read_artifact(command, opts) do
    artifact_reader = Keyword.get(opts, :artifact_reader, &read_file_artifact/1)
    artifact_reader.(command)
  end

  defp read_file_artifact(command) do
    path = Path.join(fetch!(command, "repo_root"), fetch!(command, "expected_artifact"))

    with {:ok, baseline} <- fetch_artifact_baseline(command) do
      case artifact_snapshot(path) do
        {:ok, fingerprint} ->
          observed_artifact(command, baseline, fingerprint)

        :missing ->
          {:pending, %{"reason" => "artifact_missing", "path" => fetch!(command, "expected_artifact")}}

        {:pending, reason} ->
          {:pending, Map.put(reason, "path", fetch!(command, "expected_artifact"))}

        {:error, reason} ->
          {:error, %{"reason" => "artifact_read_failed", "detail" => inspect(reason)}}
      end
    end
  end

  defp observed_artifact(command, baseline, fingerprint) do
    if modified_after_dispatch?(baseline, fingerprint) do
      {:candidate,
       %{
         "assignment_id" => fetch!(command, "assignment_id"),
         "artifact_id" => fetch!(command, "artifact_id"),
         "path" => fetch!(command, "expected_artifact"),
         "match_kind" => "exact",
         "bytes" => fingerprint["bytes"],
         "sha256" => fingerprint["sha256"],
         "modified_after_dispatch" => true
       }, fingerprint}
    else
      {:pending, %{"reason" => "artifact_not_modified", "path" => fetch!(command, "expected_artifact")}}
    end
  end

  @doc """
  MUST-7: the artifact baseline, taken before the projection is committed. `{"exists" =>
  false}` when there is no file; otherwise the fingerprint with `"exists" => true`. An
  unstable or unreadable file is an error the lifecycle turns into attention.
  """
  @impl true
  def snapshot(command, _opts \\ []) when is_map(command), do: artifact_baseline(command)

  # The baseline deliver acts on is the durable one the command carries, in the closed
  # grammar. The explicit `unrecorded` view of a version-1 projection blocks for attention
  # on every outcome: a snapshot taken now would land only in the post-paste event, which
  # is exactly the crash window MUST-7 closes, and a daemon-proven absence cannot make a new
  # snapshot durable before the next paste (any legacy promotion is a separately reviewed
  # pre-send event, not a carve-out here). A command with no baseline at all is a direct
  # caller with no projection: it may snapshot only when the daemon proves nothing was
  # pasted; delivered and queued cannot be reconstructed from a fresh snapshot.
  defp durable_baseline(%{"artifact_baseline" => baseline}, _outcome) do
    cond do
      ArtifactBaseline.recorded?(baseline) -> {:ok, baseline}
      ArtifactBaseline.unrecorded?(baseline) -> baseline_refusal("artifact_baseline_unrecorded")
      true -> baseline_refusal("artifact_baseline_invalid")
    end
  end

  defp durable_baseline(command, "absent"), do: artifact_baseline(command)
  defp durable_baseline(_command, _receipt), do: baseline_refusal("artifact_baseline_unrecorded")

  defp baseline_refusal(reason), do: {:error, %{"reason" => reason, "detector" => "artifact_baseline"}}

  defp artifact_baseline(command) do
    path = Path.join(fetch!(command, "repo_root"), fetch!(command, "expected_artifact"))

    case artifact_snapshot(path) do
      {:ok, fingerprint} -> {:ok, Map.put(fingerprint, "exists", true)}
      :missing -> {:ok, %{"exists" => false}}
      {:pending, reason} -> {:error, %{"reason" => "artifact_baseline_unstable", "detail" => reason}}
      {:error, reason} -> {:error, %{"reason" => "artifact_baseline_failed", "detail" => inspect(reason)}}
    end
  end

  defp artifact_snapshot(path) do
    with {:ok, before_stat} <- File.stat(path, time: :posix),
         {:ok, contents} <- File.read(path),
         {:ok, after_stat} <- File.stat(path, time: :posix) do
      if before_stat.size == after_stat.size and before_stat.mtime == after_stat.mtime do
        {:ok,
         %{
           "bytes" => byte_size(contents),
           "mtime_unix" => after_stat.mtime,
           "sha256" => sha256(contents)
         }}
      else
        {:pending, %{"reason" => "artifact_changing"}}
      end
    else
      {:error, :enoent} -> :missing
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_artifact_baseline(%{"artifact_baseline" => %{"exists" => false} = baseline}), do: {:ok, baseline}

  defp fetch_artifact_baseline(%{
         "artifact_baseline" =>
           %{"exists" => true, "bytes" => bytes, "mtime_unix" => mtime_unix, "sha256" => sha256} = baseline
       })
       when is_integer(bytes) and is_integer(mtime_unix) and is_binary(sha256), do: {:ok, baseline}

  defp fetch_artifact_baseline(%{"artifact_baseline" => %{}}) do
    {:error, %{"reason" => "artifact_baseline_malformed"}}
  end

  defp fetch_artifact_baseline(_command) do
    {:error, %{"reason" => "artifact_baseline_missing"}}
  end

  defp modified_after_dispatch?(%{"exists" => false}, _fingerprint), do: true

  defp modified_after_dispatch?(%{"exists" => true} = baseline, fingerprint) do
    baseline["bytes"] != fingerprint["bytes"] or baseline["sha256"] != fingerprint["sha256"]
  end

  defp modified_after_dispatch?(_baseline, _fingerprint), do: false

  defp fetch!(map, key), do: Map.fetch!(map, key)

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  def zero_hash, do: @zero_hash
end
