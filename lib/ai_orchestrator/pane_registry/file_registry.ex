defmodule AiOrchestrator.PaneRegistry.FileRegistry do
  @moduledoc """
  Cross-process ownership for local agent panes.

  Claims are published as complete files via hard links, acquired in canonical
  order, and held for the invoking process lifetime. Journal pane-lease events
  remain the run-local record; this registry prevents independent supervisors
  from both acquiring delivery authority for the same pane.
  """

  alias AiOrchestrator.ProcessIdentity

  @schema "ai-orchestrator/pane-claim"
  @schema_version 1
  @default_mutex_ttl_s 5
  @default_mutex_wait_ms 1_000
  @required_owner_fields ["run_id", "run_dir", "supervisor_instance"]

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
      pane_metadata = Map.put(metadata, "pane_ref", pane_ref)

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
    with_reclaim_mutex(Path.dirname(path), metadata["token"], opts, fn ->
      reclaim_current(path, pane_ref, metadata, opts)
    end)
  end

  defp reclaim_current(path, pane_ref, metadata, opts) do
    with {:ok, current} <- read_claim(path, pane_ref) do
      reclaim_by_status(owner_status(current, opts), path, pane_ref, current, metadata, opts)
    end
  end

  defp reclaim_by_status(:live, _path, pane_ref, current, _metadata, _opts), do: {:error, rejected(pane_ref, current)}

  defp reclaim_by_status(:dead, path, pane_ref, _current, metadata, opts) do
    with :ok <- remove_stale_claim(path) do
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
        Enum.all?(required -- ["schema_version"], &(is_binary(metadata[&1]) and metadata[&1] != ""))

    if valid?, do: :ok, else: {:error, :invalid}
  end

  defp validate_claim(_metadata, _pane_ref), do: {:error, :invalid}

  defp owner_status(metadata, opts) do
    case Keyword.get(opts, :owner_status) do
      fun when is_function(fun, 1) -> fun.(metadata)
      nil -> ProcessIdentity.owner_status(metadata, opts)
    end
  rescue
    error -> registry_error("owner liveness check failed: #{Exception.message(error)}")
  end

  defp rejected(pane_ref, metadata) do
    %{
      "reason" => "pane_claim_rejected",
      "pane_ref" => pane_ref,
      "owner" => Map.take(metadata, ["run_id", "run_dir", "supervisor_instance", "pid", "pid_start", "acquired_at_unix"])
    }
  end

  defp remove_stale_claim(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> registry_error("stale claim removal failed: #{inspect(reason)}")
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

  defp with_reclaim_mutex(root, claim_token, opts, fun) do
    # The mutex limits stale-claim reclaim thrash. Safety still comes from
    # hard-link publication plus re-reading the current claim under contention.
    mutex_path = Path.join(root, ".reclaim-lock")
    mutex_token = claim_token <> "-" <> Integer.to_string(System.unique_integer([:positive]))

    case acquire_mutex(mutex_path, mutex_token, opts) do
      :ok ->
        try do
          fun.()
        after
          release_mutex(mutex_path, mutex_token)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire_mutex(path, token, opts) do
    deadline = monotonic_ms(opts) + Keyword.get(opts, :mutex_wait_ms, @default_mutex_wait_ms)
    acquire_mutex_until(path, token, opts, deadline)
  end

  defp acquire_mutex_until(path, token, opts, deadline) do
    case File.mkdir(path) do
      :ok ->
        case File.chmod(path, 0o700) do
          :ok ->
            initialize_mutex_token(path, token)

          {:error, reason} ->
            File.rm_rf(path)
            registry_error("reclaim mutex permissions failed: #{inspect(reason)}")
        end

      {:error, :eexist} ->
        maybe_break_stale_mutex(path, opts)

        if monotonic_ms(opts) < deadline do
          sleep(opts, 10)
          acquire_mutex_until(path, token, opts, deadline)
        else
          registry_error("reclaim mutex is busy")
        end

      {:error, reason} ->
        registry_error("reclaim mutex failed: #{inspect(reason)}")
    end
  end

  defp initialize_mutex_token(path, token) do
    token_path = Path.join(path, "token")

    case File.write(token_path, token, [:exclusive]) do
      :ok ->
        case File.chmod(token_path, 0o600) do
          :ok ->
            :ok

          {:error, reason} ->
            File.rm_rf(path)
            registry_error("reclaim mutex permissions failed: #{inspect(reason)}")
        end

      {:error, reason} ->
        File.rm_rf(path)
        registry_error("reclaim mutex initialization failed: #{inspect(reason)}")
    end
  end

  defp maybe_break_stale_mutex(path, opts) do
    ttl_s = Keyword.get(opts, :mutex_ttl_s, @default_mutex_ttl_s)

    case File.stat(path, time: :posix) do
      {:ok, %{mtime: mtime}} ->
        if now_unix(opts) - mtime > ttl_s, do: File.rm_rf(path), else: :ok

      _fresh_or_unreadable ->
        :ok
    end
  end

  defp release_mutex(path, token) do
    # Token matching keeps an old holder from intentionally releasing a
    # successor's mutex; claim publication remains the final ownership arbiter.
    case File.read(Path.join(path, "token")) do
      {:ok, ^token} -> File.rm_rf(path)
      _not_owned -> :ok
    end
  end

  defp token(opts) do
    case Keyword.get(opts, :token_fun) do
      fun when is_function(fun, 0) -> fun.()
      nil -> 12 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
    end
  end

  defp now_unix(opts), do: Keyword.get(opts, :now_unix, fn -> System.os_time(:second) end).()
  defp monotonic_ms(opts), do: Keyword.get(opts, :monotonic_ms, fn -> System.monotonic_time(:millisecond) end).()
  defp sleep(opts, milliseconds), do: Keyword.get(opts, :sleep, &Process.sleep/1).(milliseconds)

  defp registry_error(detail), do: {:error, %{"reason" => "pane_registry_unavailable", "detail" => detail}}
end
