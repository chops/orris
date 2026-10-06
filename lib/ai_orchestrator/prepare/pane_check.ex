defmodule AiOrchestrator.Prepare.PaneCheck do
  @moduledoc """
  The claim-time daemon check (NS-15.G.005, B3a G4, design r2). With the pane claim held, one status read per claimed
  pane through the run's dispatch module (`pane_status/2`; the product adapter `Dispatch.LocalPane` is the default,
  as in Effects, which is why Prepare's Boundary declares the Dispatch dependency):

  - a dispatch module without `pane_status/2` is refused (daemon_unavailable, "pane_status_unsupported"): the check
    is never skipped;
  - `{:ok, %{"state" => state}}` with a non-empty string state is healthy, except state "dead";
  - a typed `pane_dead` / `pane_not_found` is dead / unregistered; every other answer, including a raise inside the
    status read, is daemon_unavailable with only the reason name kept (no daemon text).

  A refusal is answered as a function the caller runs AFTER releasing the claim; it opens the diagnosis and returns
  the public reason map. A healthy check resolves, under one lock per pane, the pane's open diagnoses the read
  verifies (dead, unregistered, daemon_unavailable by the status read; live_holder by the claim itself).
  """

  alias AiOrchestrator.Dispatch.V3Status
  alias AiOrchestrator.PaneRegistry.Diagnosis
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.PaneRegistry.PaneIdentity

  # the dispatch default Effects uses (effects.ex dispatch_module/1)
  @default_dispatch AiOrchestrator.Dispatch.LocalPane
  @refused "pane_claim_refused"
  @next_actions %{
    "dead" => %{"code" => "reattach_pane", "text" => "reattach the pane to the daemon, then retry"},
    "unregistered" => %{"code" => "reattach_pane", "text" => "register the pane with the daemon, then retry"},
    "daemon_unavailable" => %{"code" => "reconcile_daemon", "text" => "check the daemon and the pane, then retry"},
    "contradictory" => %{
      "code" => "inspect_identity_mismatch",
      "text" => "the daemon's pane identity differs from the claim's; inspect the pane registration, then retry"
    },
    "live_holder" => %{"code" => "wait_for_holder", "text" => "wait for the holding run to release the pane"},
    # NS-15.G.002 B1c: release of a quarantined pane is a held, separately reviewed step, so nothing here offers one
    "quarantined" => %{
      "code" => "quarantine_held",
      "text" =>
        "the daemon holds this pane quarantined after a restart; no release is available (release is a held, " <>
          "separately reviewed step); do not retry until an operator review clears the pane"
    }
  }

  @type refusal :: (-> map())

  @doc """
  The check after the claim. The path is chosen from the CLAIM (B3b scope r2 D4): a claim without a recorded
  daemon_identity takes the version 1 read above, unchanged; a claim with one is checked by its version 3 status
  only (never `pane_status/2`): no `status_v3/2` -> daemon_unavailable "status_v3_unavailable"; a transport error,
  raise or exit -> daemon_unavailable typed; a reply-identity error -> daemon_unavailable "reply_identity:<detail>";
  pane_not_found -> unregistered; a valid identity differing from the claim's -> contradictory
  (inspect_identity_mismatch, with the observed identity and the differing fields); an equal identity -> dead when
  its state is "dead", else healthy. A claim file that cannot be read back as this claim refuses (fail closed).
  A healthy version 3 read resolves the pane's open dead, unregistered, daemon_unavailable and contradictory
  diagnoses (check "status_v3"); a healthy version 1 read never resolves contradictory.
  """
  @spec check([String.t()], String.t(), keyword()) :: :ok | {:refuse, refusal()}
  def check(pane_refs, claim_token, opts) do
    root = Keyword.fetch!(opts, :pane_registry_root)
    dispatch = Keyword.get(opts, :dispatch, @default_dispatch)
    dispatch_opts = Keyword.get(opts, :dispatch_opts, [])
    observations = Enum.map(pane_refs, &observe_claimed(root, claim_token, dispatch, &1, dispatch_opts))

    case Enum.find(observations, &(not healthy?(&1))) do
      {pane_ref, {trigger, observed}, daemon_pane_id} ->
        {:refuse, fn -> open(root, attrs(pane_ref, trigger, observed, nil, daemon_pane_id), diagnosis_opts(opts)) end}

      nil ->
        resolve(root, observations, claim_token, diagnosis_opts(opts))
    end
  end

  defp healthy?({_pane_ref, {:healthy, _check, _state}, _daemon_pane_id}), do: true
  defp healthy?(_observation), do: false

  defp observe_claimed(root, claim_token, dispatch, pane_ref, dispatch_opts) do
    case FileRegistry.claimed_identity(root, pane_ref, claim_token) do
      {:ok, nil} -> {pane_ref, observe(dispatch, pane_ref, dispatch_opts), nil}
      {:ok, identity} -> {pane_ref, observe_v3(dispatch, pane_ref, identity, dispatch_opts), identity["pane_id"]}
      {:error, reason} -> {pane_ref, {"daemon_unavailable", unavailable(reason)}, nil}
    end
  end

  defp observe_v3(dispatch, pane_ref, identity, dispatch_opts) do
    if Code.ensure_loaded?(dispatch) and function_exported?(dispatch, :status_v3, 2) do
      dispatch |> read_status_v3(pane_ref, dispatch_opts) |> classify_claimed(pane_ref, identity)
    else
      {"daemon_unavailable", unavailable("status_v3_unavailable")}
    end
  end

  defp classify_claimed({:ok, bytes}, pane_ref, identity) when is_binary(bytes) do
    case V3Status.decode(bytes, pane_ref) do
      {:ok, status} -> compare_identity(status, identity)
      other -> refused_v3(other)
    end
  end

  defp classify_claimed({:error, %{"reason" => reason}}, _pane_ref, _identity) when is_binary(reason),
    do: {"daemon_unavailable", unavailable(reason)}

  defp classify_claimed(_invalid, _pane_ref, _identity), do: {"daemon_unavailable", unavailable("status_v3_invalid")}

  defp compare_identity(status, identity) do
    case PaneIdentity.compare(identity, status["pane_identity"]) do
      :match -> matched(status)
      {:mismatch, fields} -> {"contradictory", Map.put(observed_v3(status), "mismatch", fields)}
      {:error, _incomplete} -> {"daemon_unavailable", unavailable("identity_incomplete")}
    end
  end

  # after an identity match: dead, then quarantined (B1 design r5 precedence: mismatch > dead > quarantined)
  defp matched(%{"state" => "dead"} = status), do: {"dead", observed_v3(status)}
  defp matched(%{"quarantined" => true} = status), do: {"quarantined", observed_v3(status)}
  defp matched(%{"state" => state}), do: {:healthy, "status_v3", state}

  defp observed_v3(status) do
    status
    |> Map.take(["state", "quarantined", "pane_identity"])
    |> Map.merge(%{"source" => "status_v3", "observed_at" => now()})
  end

  @doc """
  The claim-time version 3 read (B3b scope r4 D5), run BEFORE any claim. A dispatch module without `status_v3/2`
  answers `{:ok, nil}`: no identities, a legacy claim, B3a's version 1 path. Otherwise each pane's version 3 status
  is read and decoded in turn; when all decode to a valid identity the answer is `{:ok, %{pane_ref => identity}}`
  for the claim. The first pane that does not refuses the run before any claim: a transport error, raise or exit
  is daemon_unavailable with its typed reason ("ap_unavailable" for a raise or exit), a reply-identity error is
  daemon_unavailable "reply_identity:<detail>", a typed pane_not_found is unregistered (observed source status_v3)
  and any other typed refusal is daemon_unavailable with its reason. The refusal reads the pane's holder ONCE through
  the configured registry's `holder/2` (FileRegistry's when the registry has none) and opens the diagnosis with it.
  """
  @spec claim_time([String.t()], module(), keyword()) :: {:ok, map() | nil} | {:refuse, refusal()}
  def claim_time(pane_refs, registry, opts) do
    dispatch = Keyword.get(opts, :dispatch, @default_dispatch)

    if Code.ensure_loaded?(dispatch) and function_exported?(dispatch, :status_v3, 2) do
      read_identities(pane_refs, dispatch, registry, opts)
    else
      {:ok, nil}
    end
  end

  # NS-15.G.002 B1c: a daemon PROVEN not to serve the identity core ("status_v3_unavailable", only from the dispatch's
  # capability check) on the FIRST pane read leaves the claim a legacy one ({:ok, nil}), as before version 3 existed.
  # That is the only downgrade: the same answer after an earlier pane read as capable, an indeterminate capability, and
  # every other error or refusal refuse the run.
  defp read_identities(pane_refs, dispatch, registry, opts) do
    dispatch_opts = Keyword.get(opts, :dispatch_opts, [])

    pane_refs
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, fn {pane_ref, index}, {:ok, identities} ->
      case claim_time_status(dispatch, pane_ref, dispatch_opts) do
        {:identity, identity} ->
          {:cont, {:ok, Map.put(identities, pane_ref, identity)}}

        :not_capable when index == 0 ->
          {:halt, {:ok, nil}}

        :not_capable ->
          refuse_preclaim(pane_ref, {"daemon_unavailable", unavailable("status_v3_unavailable")}, registry, opts)

        refusal ->
          refuse_preclaim(pane_ref, refusal, registry, opts)
      end
    end)
  end

  defp refuse_preclaim(pane_ref, {trigger, observed}, registry, opts),
    do: {:halt, {:refuse, fn -> preclaim(pane_ref, trigger, observed, registry, opts) end}}

  defp claim_time_status(dispatch, pane_ref, dispatch_opts) do
    case read_status_v3(dispatch, pane_ref, dispatch_opts) do
      {:ok, bytes} when is_binary(bytes) -> claim_time_identity(V3Status.decode(bytes, pane_ref))
      {:error, %{"reason" => "status_v3_unavailable"}} -> :not_capable
      {:error, %{"reason" => reason}} when is_binary(reason) -> {"daemon_unavailable", unavailable(reason)}
      _invalid -> {"daemon_unavailable", unavailable("status_v3_invalid")}
    end
  end

  # no prior identity to compare at claim time: a dead pane, then a quarantined one, refuses before any identity is kept
  defp claim_time_identity({:ok, %{"state" => "dead"} = status}), do: {"dead", observed_v3(status)}
  defp claim_time_identity({:ok, %{"quarantined" => true} = status}), do: {"quarantined", observed_v3(status)}
  defp claim_time_identity({:ok, %{"pane_identity" => identity}}), do: {:identity, identity}
  defp claim_time_identity(other), do: refused_v3(other)

  defp read_status_v3(dispatch, pane_ref, dispatch_opts) do
    dispatch.status_v3(pane_ref, dispatch_opts)
  rescue
    _error -> {:error, %{"reason" => "ap_unavailable"}}
  catch
    :exit, _reason -> {:error, %{"reason" => "ap_unavailable"}}
  end

  # a decoded version 3 answer without a usable identity, the same at claim time and after the claim
  defp refused_v3({:refused, "pane_not_found"}), do: {"unregistered", status_v3("not_found")}
  defp refused_v3({:refused, reason}), do: {"daemon_unavailable", unavailable(reason)}

  defp refused_v3({:error, :reply_identity, detail}), do: {"daemon_unavailable", unavailable("reply_identity:" <> detail)}

  defp status_v3(state), do: %{"source" => "status_v3", "observed_at" => now(), "state" => state}

  defp preclaim(pane_ref, trigger, observed, registry, opts) do
    root = Keyword.fetch!(opts, :pane_registry_root)
    open(root, attrs(pane_ref, trigger, observed, holder(registry, root, pane_ref), nil), diagnosis_opts(opts))
  end

  # exactly one read-only snapshot of the pane's existing claim file, through the configured registry
  defp holder(registry, root, pane_ref) do
    if Code.ensure_loaded?(registry) and function_exported?(registry, :holder, 2) do
      registry.holder(root, pane_ref)
    else
      FileRegistry.holder(root, pane_ref)
    end
  end

  @doc """
  The refusal for a live-holder claim rejection: its diagnosis opened, the registry's reason kept under "rejection".
  The holder is the rejection's owner fields, or null when the rejection names no owner (design r2 D2).
  """
  @spec live_holder(map(), keyword()) :: map()
  def live_holder(%{"pane_ref" => pane_ref} = rejection, opts) do
    holder =
      case rejection do
        %{"owner" => owner} when is_map(owner) -> Map.take(owner, ~w(run_id run_dir pid pid_start acquired_at_unix))
        _no_owner -> nil
      end

    observed = %{"source" => "unavailable", "error" => "claim_held"}

    opts
    |> Keyword.fetch!(:pane_registry_root)
    |> open(attrs(pane_ref, "live_holder", observed, holder, nil), diagnosis_opts(opts))
    |> Map.put("rejection", rejection)
  end

  defp observe(dispatch, pane_ref, dispatch_opts) do
    if Code.ensure_loaded?(dispatch) and function_exported?(dispatch, :pane_status, 2) do
      classify(read_status(dispatch, pane_ref, dispatch_opts))
    else
      {"daemon_unavailable", unavailable("pane_status_unsupported")}
    end
  end

  # a raise or exit inside the status read itself (for example no `ap` executable) is this check's
  # daemon_unavailable, normalized here; anything else keeps the caller's legacy rescue/catch
  defp read_status(dispatch, pane_ref, dispatch_opts) do
    dispatch.pane_status(pane_ref, dispatch_opts)
  rescue
    _error -> {:error, %{"reason" => "ap_unavailable"}}
  catch
    :exit, _reason -> {:error, %{"reason" => "ap_unavailable"}}
  end

  defp classify({:ok, %{"state" => "dead"}}), do: {"dead", status_v1("dead")}

  defp classify({:ok, %{"state" => state}}) when is_binary(state) and state != "", do: {:healthy, "pane_status_v1", state}

  defp classify({:error, %{"reason" => "pane_dead"}}), do: {"dead", status_v1("dead")}
  defp classify({:error, %{"reason" => "pane_not_found"}}), do: {"unregistered", status_v1("not_found")}

  defp classify({:error, %{"reason" => reason}}) when is_binary(reason), do: {"daemon_unavailable", unavailable(reason)}

  defp classify(_invalid), do: {"daemon_unavailable", unavailable("pane_status_invalid")}

  defp status_v1(state), do: %{"source" => "pane_status_v1", "observed_at" => now(), "state" => state}
  defp unavailable(reason), do: %{"source" => "unavailable", "error" => reason}

  defp attrs(pane_ref, trigger, observed, holder, daemon_pane_id) do
    %{
      "trigger" => trigger,
      "pane_ref" => pane_ref,
      "daemon_pane_id" => daemon_pane_id,
      "holder" => holder,
      "observed_daemon_state" => observed,
      "next_action" => Map.fetch!(@next_actions, trigger)
    }
  end

  defp open(root, attrs, opts) do
    case Diagnosis.open(root, attrs, opts) do
      {:ok, diagnosis} -> refusal(diagnosis)
      {:error, %{"persistence" => persistence}} -> %{"reason" => @refused, "persistence" => persistence}
      {:error, reason} -> %{"reason" => @refused, "diagnosis_error" => reason}
    end
  end

  defp refusal(diagnosis) do
    {removed, diagnosis} = Map.pop(diagnosis, "removed", [])
    base = %{"reason" => @refused, "diagnosis" => diagnosis}
    if removed == [], do: base, else: Map.put(base, "removed", removed)
  end

  defp resolve(root, observations, claim_token, opts) do
    claim = %{"check" => "file_registry_claim", "observed_daemon_state" => nil, "claim_token" => claim_token}

    Enum.reduce_while(observations, :ok, fn {pane_ref, {:healthy, check, state}, _daemon_pane_id}, :ok ->
      status = %{"check" => check, "observed_daemon_state" => %{"state" => state}, "claim_token" => nil}
      resolutions = Enum.map(verified_triggers(check), &{&1, status}) ++ [{"live_holder", claim}]

      case Diagnosis.resolve_verified(root, pane_ref, resolutions, opts) do
        {:ok, _resolved} ->
          {:cont, :ok}

        {:error, %{"persistence" => persistence}} ->
          {:halt, {:refuse, unpersisted(persistence)}}
      end
    end)
  end

  # the triggers a healthy read verifies gone: only a version 3 read can verify an identity (contradictory)
  defp verified_triggers("status_v3"), do: ["dead", "unregistered", "daemon_unavailable", "contradictory", "quarantined"]

  defp verified_triggers("pane_status_v1"), do: ["dead", "unregistered", "daemon_unavailable"]

  defp unpersisted(persistence), do: fn -> %{"reason" => @refused, "persistence" => persistence} end

  defp diagnosis_opts(opts) do
    Enum.reject([diagnosis_fs: opts[:diagnosis_fs], resolved_bound: opts[:diagnosis_resolved_bound]], fn {_key, value} ->
      is_nil(value)
    end)
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
end
