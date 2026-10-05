defmodule AiOrchestrator.PaneRegistry.FileRegistry do
  @moduledoc """
  Cross-process ownership for local agent panes.

  Claims are published as complete files via hard links, acquired in canonical
  order, and held for the invoking process lifetime. Journal pane-lease events
  remain the run-local record; this registry prevents independent supervisors
  from both acquiring delivery authority for the same pane.
  """

  alias AiOrchestrator.PaneRegistry.PaneIdentity
  alias AiOrchestrator.PaneRegistry.RootLock
  alias AiOrchestrator.ProcessIdentity

  @schema "ai-orchestrator/pane-claim"
  @schema_version 1
  @lock_opts [:wait_ms, :grace_ms, :root_lock_helper, :on_contend, :helper_observer]
  @required_owner_fields ["run_id", "run_dir", "supervisor_instance"]
  @identity_keys ["pane_id", "registration_id", "generation"]
  @holder_fields ["run_id", "run_dir", "pid", "pid_start", "acquired_at_unix"]

  @type claim :: %{
          required(:root) => String.t(),
          required(:token) => String.t(),
          required(:pane_refs) => [String.t()]
        }

  @spec pane_refs(map()) :: [String.t()]
  def pane_refs(%{"agents" => agents}) when is_list(agents) do
    # Keep the role fallback aligned with the lifecycle pane_ref/1 (RunFSM) until pane binding
    # becomes a shared public value object.
    agents
    |> Enum.map(fn
      %{"pane_hint" => %{"pane_ref" => pane_ref}} -> pane_ref
      %{"role" => role} -> "pane_" <> role
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @spec claim([String.t()], map(), keyword()) :: {:ok, claim()} | {:error, map()}
  def claim(pane_refs, owner, opts) when is_list(pane_refs) and is_map(owner) and is_list(opts) do
    with {:ok, root} <- registry_root(opts),
         {:ok, refs} <- normalize_pane_refs(pane_refs),
         :ok <- validate_owner(owner),
         :ok <- validate_daemon_identities(Keyword.get(opts, :daemon_identities, %{}), refs),
         :ok <- ensure_registry_root(root),
         {:ok, metadata} <- claim_metadata(owner, opts) do
      acquire_all(refs, metadata, root, opts)
    end
  end

  @spec release(claim()) :: :ok | {:error, map()}
  def release(%{root: root, token: token, pane_refs: pane_refs}) do
    errors =
      pane_refs
      |> Enum.reverse()
      |> Enum.reduce([], fn pane_ref, errors ->
        case release_path(claim_path(root, pane_ref), token) do
          :ok -> errors
          {:error, reason} -> [Map.put(reason, "pane_ref", pane_ref) | errors]
        end
      end)

    case errors do
      [] -> :ok
      [reason] -> {:error, reason}
      reasons -> {:error, %{"reason" => "pane_claim_release_failed", "failures" => Enum.reverse(reasons)}}
    end
  end

  @doc """
  Runs `fun.(lock)` while holding the one claims-root lock on `root` (a kernel flock on `<root>/.root-lock`).
  Answers `{:ok, fun_result}`, or `{:error, reason}` with reason `"lock_busy"`, `"lock_unavailable"` or
  `"lock_unidentified"`, or `{:error, %{"lock" => "release_unconfirmed", "result" => fun_result}}` when `fun` ran
  but the helper's exit was not confirmed, or `{:error, %{"lock" => "lost", "result" => fun_result}}` when the
  helper ended while `fun` ran (the lock was not exclusive for all of it). opts: `:wait_ms`, `:root_lock_helper`, `:on_contend` (called once,
  after the helper reports an actual contended attempt), `:grace_ms`.
  """
  @spec with_root_lock(String.t(), String.t(), keyword(), (map() -> result)) ::
          {:ok, result} | {:error, String.t() | map()}
        when result: term()
  def with_root_lock(root, owner_token, opts, fun), do: RootLock.run(root, owner_token, opts, fun)

  @doc """
  Whether `pane_ref` is held, read from its claim file alone (B2, NS-15.G.004): no file is not held; a valid claim is
  held unless its owner classifies as dead; a malformed or unreadable claim, or an owner whose status is unknown, is
  held (a pane that cannot be proved free is held). It never writes, reclaims, removes or locks.

  An observational snapshot only: it is never authority to dispatch, to skip the exclusive claim or to bypass it.
  Being lock-free it can race a concurrent publication or removal, and its answer may be stale when used; only
  `claim/3` grants ownership.
  """
  @spec held?(String.t(), String.t(), keyword()) :: boolean()
  def held?(root, pane_ref, opts) when is_binary(root) and is_binary(pane_ref) and is_list(opts) do
    case File.read(claim_path(root, pane_ref)) do
      {:error, :enoent} -> false
      {:ok, bytes} -> claim_held?(bytes, pane_ref, opts)
      {:error, _unreadable} -> true
    end
  end

  defp claim_held?(bytes, pane_ref, opts) do
    with {:ok, metadata} <- Jason.decode(bytes),
         :ok <- validate_claim(metadata, pane_ref) do
      owner_status(metadata, opts) != :dead
    else
      _malformed -> true
    end
  end

  @doc """
  One read-only snapshot of the existing claim file for `pane_ref` (NS-15.G.005 B3b, scope r4 D5): `nil` when there
  is no file; the claim's run_id, run_dir, pid, pid_start and acquired_at_unix when the file is a valid claim;
  `%{"claim_file" => "malformed"}` or `%{"claim_file" => "unreadable"}` otherwise. One file read: no lock, write,
  reclaim or liveness probe, and like `held?/3` it may be stale when used.
  """
  @spec holder(String.t(), String.t()) :: map() | nil
  def holder(root, pane_ref) when is_binary(root) and is_binary(pane_ref) do
    case File.read(claim_path(root, pane_ref)) do
      {:error, :enoent} -> nil
      {:ok, bytes} -> holder_fields(bytes, pane_ref)
      {:error, _unreadable} -> %{"claim_file" => "unreadable"}
    end
  end

  defp holder_fields(bytes, pane_ref) do
    with {:ok, metadata} <- Jason.decode(bytes),
         :ok <- validate_claim(metadata, pane_ref) do
      Map.take(metadata, @holder_fields)
    else
      _malformed -> %{"claim_file" => "malformed"}
    end
  end

  @doc """
  The daemon identity recorded in this run's own claim for `pane_ref` (B3b scope r2 D4), read back from the claim
  file: `{:ok, identity}` when the claim with `token` records one, `{:ok, nil}` when it records none or no claim file
  exists (a registry that keeps no files), and `{:error, reason}` when the file is unreadable, malformed or now
  carries another token; the caller treats an error as fail closed.
  """
  @spec claimed_identity(String.t(), String.t(), String.t()) :: {:ok, map() | nil} | {:error, String.t()}
  def claimed_identity(root, pane_ref, token) when is_binary(root) and is_binary(pane_ref) and is_binary(token) do
    case File.read(claim_path(root, pane_ref)) do
      {:error, :enoent} -> {:ok, nil}
      {:ok, bytes} -> identity_of(bytes, pane_ref, token)
      {:error, _unreadable} -> {:error, "claim_unreadable"}
    end
  end

  defp identity_of(bytes, pane_ref, token) do
    with {:ok, metadata} <- Jason.decode(bytes),
         :ok <- validate_claim(metadata, pane_ref) do
      if metadata["token"] == token,
        do: {:ok, Map.get(metadata, "daemon_identity")},
        else: {:error, "claim_changed"}
    else
      _malformed -> {:error, "claim_malformed"}
    end
  end

  @spec claim_path(String.t(), String.t()) :: String.t()
  def claim_path(root, pane_ref) when is_binary(root) and is_binary(pane_ref) do
    digest = :sha256 |> :crypto.hash(pane_ref) |> Base.encode16(case: :lower)
    Path.join(Path.expand(root), "pane-#{digest}.json")
  end

  defp registry_root(opts) do
    case Keyword.get(opts, :root) do
      root when is_binary(root) and byte_size(root) > 0 -> {:ok, Path.expand(root)}
      _root -> {:error, %{"reason" => "pane_registry_unavailable", "detail" => "registry root is missing"}}
    end
  end

  defp normalize_pane_refs(pane_refs) do
    if Enum.all?(pane_refs, &(is_binary(&1) and String.trim(&1) != "")) do
      {:ok, pane_refs |> Enum.uniq() |> Enum.sort()}
    else
      {:error, %{"reason" => "pane_registry_unavailable", "detail" => "pane refs must be non-empty strings"}}
    end
  end

  defp validate_owner(owner) do
    if Enum.all?(@required_owner_fields, &(is_binary(owner[&1]) and owner[&1] != "")) do
      :ok
    else
      {:error, %{"reason" => "pane_registry_unavailable", "detail" => "claim owner metadata is incomplete"}}
    end
  end

  # claim-time version 3 identities (B3b D2): a map of claimed pane_ref => valid identity naming that pane
  defp validate_daemon_identities(identities, pane_refs) when is_map(identities) do
    if Enum.all?(identities, fn {pane_ref, identity} -> identity_for?(identity, pane_ref, pane_refs) end),
      do: :ok,
      else: registry_error("daemon identities must be valid identities of claimed panes")
  end

  defp validate_daemon_identities(_identities, _pane_refs),
    do: registry_error("daemon identities must be valid identities of claimed panes")

  defp identity_for?(identity, pane_ref, pane_refs),
    do: pane_ref in pane_refs and PaneIdentity.valid?(identity) and identity["pane_id"] == pane_ref

  defp ensure_registry_root(root) do
    case File.mkdir_p(root) do
      :ok ->
        case File.chmod(root, 0o700) do
          :ok -> :ok
          {:error, reason} -> registry_error("cannot secure registry root: #{inspect(reason)}")
        end

      {:error, reason} ->
        registry_error("cannot create registry root: #{inspect(reason)}")
    end
  end

  defp claim_metadata(owner, opts) do
    pid = opts |> Keyword.get(:pid, System.pid()) |> to_string()

    with {:ok, pid_start} <- own_pid_start(pid, opts),
         token when is_binary(token) and byte_size(token) > 0 <- token(opts) do
      {:ok,
       Map.merge(owner, %{
         "schema" => @schema,
         "schema_version" => @schema_version,
         "token" => token,
         "pid" => pid,
         "pid_start" => pid_start,
         "erlang_pid" => self() |> :erlang.pid_to_list() |> List.to_string(),
         "acquired_at_unix" => now_unix(opts)
       })}
    else
      {:error, reason} -> {:error, reason}
      _invalid_token -> registry_error("claim token generation failed")
    end
  end

  defp own_pid_start(pid, opts) do
    case Keyword.fetch(opts, :pid_start) do
      {:ok, pid_start} when is_binary(pid_start) and byte_size(pid_start) > 0 -> {:ok, pid_start}
      {:ok, _invalid} -> registry_error("process start identity is invalid")
      :error -> own_process_identity(pid, opts)
    end
  end

  defp own_process_identity(pid, opts) do
    case ProcessIdentity.current(pid, opts) do
      {:ok, pid_start} -> {:ok, pid_start}
      :dead -> registry_error("current process is not visible to ps")
      {:error, reason} -> {:error, reason}
    end
  end

  defp acquire_all(pane_refs, metadata, root, opts) do
    pane_refs
    |> Enum.reduce_while({:ok, []}, fn pane_ref, {:ok, acquired} ->
      pane_metadata = metadata |> Map.put("pane_ref", pane_ref) |> put_daemon_identity(pane_ref, opts)

      case acquire_one(root, pane_ref, pane_metadata, opts) do
        :ok -> {:cont, {:ok, [pane_ref | acquired]}}
        {:error, reason} -> {:halt, {:error, reason, acquired}}
      end
    end)
    |> case do
      {:ok, acquired} ->
        {:ok, %{root: root, token: metadata["token"], pane_refs: Enum.reverse(acquired)}}

      {:error, reason, acquired} ->
        rollback(root, acquired, metadata["token"])
        {:error, reason}
    end
  end

  defp put_daemon_identity(metadata, pane_ref, opts) do
    case opts |> Keyword.get(:daemon_identities, %{}) |> Map.fetch(pane_ref) do
      {:ok, identity} ->
        recorded = %{"verified_at_unix" => now_unix(opts), "source" => "status_v3"}
        Map.put(metadata, "daemon_identity", identity |> Map.take(@identity_keys) |> Map.merge(recorded))

      :error ->
        metadata
    end
  end

  defp acquire_one(root, pane_ref, metadata, opts) do
    path = claim_path(root, pane_ref)

    case publish(path, metadata) do
      :ok -> :ok
      {:error, :eexist} -> resolve_existing(path, pane_ref, metadata, opts)
      {:error, reason} -> registry_error("claim publication failed: #{inspect(reason)}")
    end
  end

  defp publish(path, metadata) do
    temp = Path.join(Path.dirname(path), ".claim-#{metadata["token"]}-#{System.unique_integer([:positive])}.tmp")

    case write_complete_temp(temp, Jason.encode!(metadata)) do
      :ok ->
        result = File.ln(temp, path)
        File.rm(temp)
        result

      {:error, reason} ->
        File.rm(temp)
        {:error, reason}
    end
  end

  defp write_complete_temp(path, contents) do
    case File.open(path, [:write, :binary, :exclusive]) do
      {:ok, io} ->
        result =
          with :ok <- File.chmod(path, 0o600),
               :ok <- IO.binwrite(io, contents) do
            :file.sync(io)
          end

        File.close(io)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_existing(path, pane_ref, metadata, opts) do
    with {:ok, existing} <- read_claim(path, pane_ref) do
      case owner_status(existing, opts) do
        :live -> {:error, rejected(pane_ref, existing)}
        :dead -> reclaim(path, pane_ref, metadata, opts)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp reclaim(path, pane_ref, metadata, opts) do
    with_reclaim_lock(path, metadata["token"], opts, fn ->
      reclaim_current(path, pane_ref, metadata, opts)
    end)
  end

  defp reclaim_current(path, pane_ref, metadata, opts) do
    with {:ok, current} <- read_claim(path, pane_ref) do
      reclaim_by_status(owner_status(current, opts), path, pane_ref, current, metadata, opts)
    end
  end

  defp reclaim_by_status(:live, _path, pane_ref, current, _metadata, _opts), do: {:error, rejected(pane_ref, current)}

  defp reclaim_by_status(:dead, path, pane_ref, current, metadata, opts) do
    with :ok <- remove_stale_claim(path, current["token"]) do
      publish_replacement(path, pane_ref, metadata, opts)
    end
  end

  defp reclaim_by_status({:error, reason}, _path, _pane_ref, _current, _metadata, _opts), do: {:error, reason}

  defp publish_replacement(path, pane_ref, metadata, opts) do
    case publish(path, metadata) do
      :ok -> :ok
      {:error, :eexist} -> resolve_existing(path, pane_ref, metadata, opts)
      {:error, reason} -> registry_error("claim publication failed: #{inspect(reason)}")
    end
  end

  defp read_claim(path, pane_ref) do
    with {:ok, bytes} <- File.read(path),
         {:ok, metadata} <- Jason.decode(bytes),
         :ok <- validate_claim(metadata, pane_ref) do
      {:ok, metadata}
    else
      {:error, :enoent} -> {:error, %{"reason" => "pane_claim_changed", "path" => path}}
      _malformed -> {:error, %{"reason" => "pane_claim_malformed", "path" => path}}
    end
  end

  defp validate_claim(metadata, pane_ref) when is_map(metadata) do
    required = [
      "schema",
      "schema_version",
      "token",
      "pane_ref",
      "run_id",
      "run_dir",
      "supervisor_instance",
      "pid",
      "pid_start"
    ]

    valid? =
      metadata["schema"] == @schema and metadata["schema_version"] == @schema_version and
        metadata["pane_ref"] == pane_ref and
        Enum.all?(required -- ["schema_version"], &(is_binary(metadata[&1]) and metadata[&1] != "")) and
        daemon_identity?(metadata, pane_ref)

    if valid?, do: :ok, else: {:error, :invalid}
  end

  defp validate_claim(_metadata, _pane_ref), do: {:error, :invalid}

  # absent: a claim made without a version 3 read (valid as before); present: a complete recorded identity of this
  # pane, or the whole claim is malformed (fail closed)
  defp daemon_identity?(%{"daemon_identity" => identity}, pane_ref) do
    PaneIdentity.valid?(identity) and identity["pane_id"] == pane_ref and is_integer(identity["verified_at_unix"]) and
      identity["source"] == "status_v3"
  end

  defp daemon_identity?(_metadata, _pane_ref), do: true

  defp owner_status(metadata, opts) do
    case Keyword.get(opts, :owner_status) do
      fun when is_function(fun, 1) -> fun.(metadata)
      nil -> metadata |> ProcessIdentity.owner_status(opts) |> local_owner(metadata, opts)
    end
  rescue
    error -> registry_error("owner liveness check failed: #{Exception.message(error)}")
  end

  # B2 (NS-15.G.004): a claim lives for the invoking Erlang process, not the whole BEAM. A claim that names THIS OS
  # process (same pid and start identity) and records the claiming "erlang_pid" is live only while that process is.
  # A claim without the key keeps the OS-only reading; a key that is not exactly a local pid is unknown, never dead.
  # ProcessIdentity.owner_status has already matched the claim's pid and start identity to a live process, so a claim
  # whose pid is this BEAM's names this exact OS process.
  defp local_owner(:live, %{"erlang_pid" => erlang_pid, "pid" => pid}, _opts) do
    if pid == System.pid(), do: erlang_status(erlang_pid), else: :live
  end

  defp local_owner(status, _metadata, _opts), do: status

  defp erlang_status(erlang_pid) when is_binary(erlang_pid) do
    with true <- Regex.match?(~r/\A<\d+\.\d+\.\d+>\z/, erlang_pid),
         pid = erlang_pid |> String.to_charlist() |> :erlang.list_to_pid(),
         true <- node(pid) == node() do
      if Process.alive?(pid), do: :live, else: :dead
    else
      _not_a_local_pid -> registry_error("owner_status_unknown")
    end
  rescue
    _error -> registry_error("owner_status_unknown")
  end

  defp erlang_status(_not_a_string), do: registry_error("owner_status_unknown")

  defp rejected(pane_ref, metadata) do
    %{
      "reason" => "pane_claim_rejected",
      "pane_ref" => pane_ref,
      "owner" => Map.take(metadata, ["run_id", "run_dir", "supervisor_instance", "pid", "pid_start", "acquired_at_unix"])
    }
  end

  # The claim is re-read immediately before the unlink and removed only while it still carries the stale owner's
  # token. This narrows but does not eliminate the race with a successor's claim: one published between this read
  # and the unlink would still be removed. Under the root lock no successor can publish there; the gap is reachable
  # only after the lock was lost to an external kill of its helper (scope r5).
  defp remove_stale_claim(path, stale_token) do
    with {:ok, bytes} <- File.read(path),
         {:ok, %{"token" => ^stale_token}} <- Jason.decode(bytes) do
      case File.rm(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} -> registry_error("stale claim removal failed: #{inspect(reason)}")
      end
    else
      {:error, :enoent} -> :ok
      _changed -> {:error, %{"reason" => "pane_claim_changed", "path" => path}}
    end
  end

  defp rollback(root, pane_refs, token) do
    Enum.each(pane_refs, &release_path(claim_path(root, &1), token))
  end

  defp release_path(path, token) do
    case File.read(path) do
      {:ok, bytes} -> release_decoded_path(path, token, Jason.decode(bytes))
      {:error, :enoent} -> :ok
      {:error, reason} -> registry_error("claim release read failed: #{inspect(reason)}")
    end
  end

  defp release_decoded_path(path, token, {:ok, %{"token" => token}}) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> registry_error("claim release failed: #{inspect(reason)}")
    end
  end

  defp release_decoded_path(path, _token, {:ok, %{"token" => _other}}),
    do: {:error, %{"reason" => "pane_claim_not_owned", "path" => path}}

  defp release_decoded_path(path, _token, _decoded), do: {:error, %{"reason" => "pane_claim_malformed", "path" => path}}

  # B3a G2: reclaim runs under the one claims-root lock (RootLock, a kernel flock held by a helper). Removing a dead
  # owner's claim and publishing the replacement is the registry's only read-modify-write; a fresh claim needs no
  # lock because hard-link publication is already exclusive. Lock failures are registry failures naming the lock
  # reason. A release the runtime did not confirm does not undo the reclaim that ran: its real result is answered
  # (the lock module has logged the unconfirmed helper), never a claim that did not happen. A lock lost while the
  # reclaim ran (its helper killed from outside) makes the reclaim indeterminate: it answers lock_lost with the
  # claim path, never success, and removes nothing. Without the lock no unlink here can be told apart from removing a
  # successor's claim, so recovery is explicit; a claim this caller left behind names a process that will be dead
  # when it ends, and is then reclaimed by the ordinary dead-owner path.
  defp with_reclaim_lock(path, claim_token, opts, fun) do
    case RootLock.run(Path.dirname(path), claim_token, Keyword.take(opts, @lock_opts), fn _lock -> fun.() end) do
      {:ok, result} ->
        result

      {:error, %{"lock" => "release_unconfirmed", "result" => result}} ->
        result

      {:error, %{"lock" => "lost"}} ->
        {:error,
         %{
           "reason" => "pane_registry_unavailable",
           "detail" => "lock_lost",
           "reclaim" => "indeterminate",
           "path" => path
         }}

      {:error, reason} when is_binary(reason) ->
        registry_error(reason)
    end
  end

  defp token(opts) do
    case Keyword.get(opts, :token_fun) do
      fun when is_function(fun, 0) -> fun.()
      nil -> 12 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
    end
  end

  defp now_unix(opts), do: Keyword.get(opts, :now_unix, fn -> System.os_time(:second) end).()

  defp registry_error(detail), do: {:error, %{"reason" => "pane_registry_unavailable", "detail" => detail}}
end
