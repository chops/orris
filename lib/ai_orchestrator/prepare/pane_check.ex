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

  alias AiOrchestrator.PaneRegistry.Diagnosis

  # the dispatch default Effects uses (effects.ex dispatch_module/1)
  @default_dispatch AiOrchestrator.Dispatch.LocalPane
  @refused "pane_claim_refused"
  @next_actions %{
    "dead" => %{"code" => "reattach_pane", "text" => "reattach the pane to the daemon, then retry"},
    "unregistered" => %{"code" => "reattach_pane", "text" => "register the pane with the daemon, then retry"},
    "daemon_unavailable" => %{"code" => "reconcile_daemon", "text" => "check the daemon and the pane, then retry"},
    "live_holder" => %{"code" => "wait_for_holder", "text" => "wait for the holding run to release the pane"}
  }

  @type refusal :: (-> map())

  @spec check([String.t()], String.t(), keyword()) :: :ok | {:refuse, refusal()}
  def check(pane_refs, claim_token, opts) do
    root = Keyword.fetch!(opts, :pane_registry_root)
    dispatch = Keyword.get(opts, :dispatch, @default_dispatch)
    dispatch_opts = Keyword.get(opts, :dispatch_opts, [])
    observations = Enum.map(pane_refs, &{&1, observe(dispatch, &1, dispatch_opts)})

    case Enum.find(observations, fn {_pane_ref, observation} -> not match?({:healthy, _state}, observation) end) do
      {pane_ref, {trigger, observed}} ->
        {:refuse, fn -> open(root, attrs(pane_ref, trigger, observed, nil), diagnosis_opts(opts)) end}

      nil ->
        resolve(root, observations, claim_token, diagnosis_opts(opts))
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
    |> open(attrs(pane_ref, "live_holder", observed, holder), diagnosis_opts(opts))
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
  defp classify({:ok, %{"state" => state}}) when is_binary(state) and state != "", do: {:healthy, state}
  defp classify({:error, %{"reason" => "pane_dead"}}), do: {"dead", status_v1("dead")}
  defp classify({:error, %{"reason" => "pane_not_found"}}), do: {"unregistered", status_v1("not_found")}

  defp classify({:error, %{"reason" => reason}}) when is_binary(reason), do: {"daemon_unavailable", unavailable(reason)}

  defp classify(_invalid), do: {"daemon_unavailable", unavailable("pane_status_invalid")}

  defp status_v1(state), do: %{"source" => "pane_status_v1", "observed_at" => now(), "state" => state}
  defp unavailable(reason), do: %{"source" => "unavailable", "error" => reason}

  defp attrs(pane_ref, trigger, observed, holder) do
    %{
      "trigger" => trigger,
      "pane_ref" => pane_ref,
      "daemon_pane_id" => nil,
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

    Enum.reduce_while(observations, :ok, fn {pane_ref, {:healthy, state}}, :ok ->
      status = %{"check" => "pane_status_v1", "observed_daemon_state" => %{"state" => state}, "claim_token" => nil}
      resolutions = [{"dead", status}, {"unregistered", status}, {"daemon_unavailable", status}, {"live_holder", claim}]

      case Diagnosis.resolve_verified(root, pane_ref, resolutions, opts) do
        {:ok, _resolved} ->
          {:cont, :ok}

        {:error, %{"persistence" => persistence}} ->
          {:halt, {:refuse, fn -> %{"reason" => @refused, "persistence" => persistence} end}}
      end
    end)
  end

  defp diagnosis_opts(opts) do
    Enum.reject([diagnosis_fs: opts[:diagnosis_fs], resolved_bound: opts[:diagnosis_resolved_bound]], fn {_key, value} ->
      is_nil(value)
    end)
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
end
