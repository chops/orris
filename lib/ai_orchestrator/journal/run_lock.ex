defmodule AiOrchestrator.Journal.RunLock do
  @moduledoc """
  Single-writer lock for a run directory (crash matrix CM-03), as a family of
  generation files `run.lock.<gen>`.

  The holder is the highest generation present. Every state change publishes
  a NEW generation: a claim links `run.lock.<highest+1>`, a release links a
  `released` tombstone at the next generation. Publication is atomic and
  complete: the metadata is written to a private temp file (exclusive
  create, write, fsync, close) and hard-linked under the generation name,
  which never replaces. Because a generation number is never reused at the
  top, a claimant acting on a stale verdict always collides on the link and
  rescans; nothing that might be live is ever unlinked. Only this claimant's
  own token-verified file, or a strictly lower generation proved dead or
  released, is ever removed, and a removal failure is reported, never used to
  infer ownership.

  Claim: list the generations (a listing error fails closed); classify the
  highest: live → `run_locked` naming the owner; released tombstone or dead
  holder → publish the next generation; unreadable or malformed → fail closed
  untouched; vanished → rescan. After a successful link the claimant rescans
  before anything is held: if a higher generation exists it withdraws its own
  file and resolves against the new highest (a stale claimant that linked a
  freed lower number therefore never wins while a higher holder exists, and
  becomes the holder only once that holder has released). A link loser
  always rescans from the highest, never trusting its old target. Attempts
  are bounded and exhaustion fails closed.

  Liveness is OS pid plus `ps` start time (`AiOrchestrator.ProcessIdentity`);
  a live holder read as dead is outside this module's model. All IO goes
  through the `AiOrchestrator.Journal.Fs` seam.
  """

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.ProcessIdentity

  @schema "ai-orchestrator/run-lock"
  @schema_version 1
  @prefix "run.lock."
  # Exact grammars: a bounded positive generation, or a private candidate temp (ignored).
  # Any other name carrying the family prefix is malformed lock state and fails closed.
  @generation ~r/^run\.lock\.([1-9][0-9]{0,11})$/
  @candidate ~r/^run\.lock\.[1-9][0-9]{0,11}\.[A-Za-z0-9_-]{1,64}\.tmp$/
  @attempts 8
  @required_fields ~w(schema schema_version state pid pid_start supervisor_instance token acquired_at)
  @states ~w(held released)

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type holder :: %{generation: pos_integer(), metadata: map()}
  @type held :: %{
          path: Path.t(),
          dir: Path.t(),
          generation: pos_integer(),
          token: String.t(),
          owner: map()
        }

  @spec acquire(Fs.t(), Path.t(), keyword()) :: {:ok, held()} | {:error, rejection()}
  def acquire(fs, run_dir, opts \\ []) do
    with {:ok, owner} <- own_identity(opts) do
      claim(fs, run_dir, owner, opts, @attempts)
    end
  end

  @doc """
  Releases by publishing a `released` tombstone at the next generation (so
  the number is never reused at the top), then removes this holder's own
  file. `release_incomplete` means the tombstone is published but the old
  file could not be removed; `release_failed` means the lock is still held.
  """
  @spec release(Fs.t(), held()) :: :ok | {:error, rejection()}
  def release(fs, %{path: path, dir: dir, generation: gen, token: token, owner: owner}) do
    case read_lock(fs, path) do
      {:ok, %{"token" => ^token}} -> publish_tombstone(fs, dir, gen, path, owner)
      _ -> {:error, %{clause: "not_owner"}}
    end
  end

  @spec owner(Fs.t(), Path.t()) :: {:ok, map()} | :none | {:error, rejection()}
  def owner(fs, run_dir) do
    case holder(fs, run_dir) do
      {:ok, %{metadata: metadata}} -> {:ok, metadata}
      other -> other
    end
  end

  @doc """
  Reports the current holder with the generation it holds.

  The generation is what distinguishes a lock still held by a known claimant
  from one a later claimant has already superseded, which `owner/2` alone
  cannot say. Same verdicts as `owner/2` otherwise: a tombstone, an empty
  family, and a vanished file all read as `:none`, and unreadable or
  malformed lock state fails closed.
  """
  @spec holder(Fs.t(), Path.t()) :: {:ok, holder()} | :none | {:error, rejection()}
  def holder(fs, run_dir) do
    case generations(fs, run_dir) do
      {:ok, []} ->
        :none

      {:ok, [{gen, path} | _]} ->
        with {:ok, metadata} <- owner_of(fs, path), do: {:ok, %{generation: gen, metadata: metadata}}

      {:error, rejection} ->
        {:error, rejection}
    end
  end

  defp owner_of(fs, path) do
    case read_lock(fs, path) do
      {:ok, %{"state" => "released"}} -> :none
      {:ok, metadata} -> {:ok, metadata}
      :unreadable -> {:error, %{clause: "lock_unreadable"}}
      {:error, :enoent} -> :none
      {:error, reason} -> unavailable(reason)
    end
  end

  defp claim(_fs, _dir, _owner, _opts, 0), do: unavailable("claim attempts exhausted")

  defp claim(fs, dir, owner, opts, attempts) do
    case highest(fs, dir, opts) do
      {:free, gen} -> publish_generation(fs, dir, owner, gen + 1, opts, attempts)
      {:live, metadata} -> {:error, %{clause: "run_locked", owner: metadata}}
      :retry -> claim(fs, dir, owner, opts, attempts - 1)
      {:error, rejection} -> {:error, rejection}
    end
  end

  defp highest(fs, dir, opts) do
    case generations(fs, dir) do
      {:ok, []} -> {:free, 0}
      {:ok, [{gen, path} | _]} -> classify(fs, path, gen, opts)
      {:error, rejection} -> {:error, rejection}
    end
  end

  defp classify(fs, path, gen, opts) do
    case read_lock(fs, path) do
      {:ok, %{"state" => "released"}} -> {:free, gen}
      {:ok, metadata} -> classify_holder(metadata, gen, opts)
      :unreadable -> unavailable("lock_unreadable")
      {:error, :enoent} -> :retry
      {:error, reason} -> unavailable(reason)
    end
  end

  defp classify_holder(metadata, gen, opts) do
    case status(metadata, opts) do
      :live -> {:live, metadata}
      :dead -> {:free, gen}
      {:error, reason} -> unavailable(reason)
    end
  end

  defp publish_generation(fs, dir, owner, gen, opts, attempts) do
    path = generation_path(dir, gen)

    case publish(fs, dir, path, owner) do
      :ok -> post_check(fs, dir, path, gen, owner, opts, attempts)
      {:error, :eexist} -> claim(fs, dir, owner, opts, attempts - 1)
      {:error, {:rolled_back, how, detail}} -> {:error, %{clause: "lock_unavailable", rollback: how, detail: detail}}
      {:error, {:cleanup_required, lock, at_path, detail}} -> cleanup_required(lock, at_path, detail)
      {:error, {:candidate_conflict, lock, at_path, detail}} -> candidate_conflict(lock, at_path, detail)
      {:error, reason} -> unavailable(reason)
    end
  end

  # Nothing is held until this rescan completes: a higher generation means this claimant lost a
  # race it could not see; it withdraws its own file (truthfully) and resolves against the new
  # highest. A withdrawal failure is returned instead of the original outcome, because a
  # live-looking file this process never holds would otherwise be left behind.
  defp post_check(fs, dir, path, gen, owner, opts, attempts) do
    case generations(fs, dir) do
      {:ok, gens} -> resolve_rescan(fs, dir, path, gen, owner, opts, attempts, gens)
      {:error, rejection} -> withdraw_then(fs, dir, path, owner, fn -> {:error, rejection} end)
    end
  end

  defp resolve_rescan(fs, dir, path, gen, owner, opts, attempts, gens) do
    if Enum.any?(gens, fn {g, _} -> g > gen end) do
      withdraw_then(fs, dir, path, owner, fn -> claim(fs, dir, owner, opts, attempts - 1) end)
    else
      confirm_own(fs, dir, path, gen, owner, opts, gens)
    end
  end

  defp withdraw_then(fs, dir, path, owner, continue) do
    case withdraw(fs, dir, path, owner) do
      :ok -> continue.()
      {:error, rejection} -> {:error, rejection}
    end
  end

  # The file just linked must still carry this claimant's token; anything else fails closed and
  # never abandons a published file silently.
  defp confirm_own(fs, dir, path, gen, owner, opts, gens) do
    token = owner["token"]

    case read_lock(fs, path) do
      {:ok, %{"token" => ^token}} -> compact_then_hold(fs, dir, path, gen, owner, opts, gens)
      {:ok, _foreign} -> {:error, %{clause: "ownership_lost", path: path, token: token}}
      {:error, :enoent} -> {:error, %{clause: "ownership_lost", path: path, token: token}}
      :unreadable -> cleanup_required(owner, path, "own generation file unreadable")
      {:error, reason} -> cleanup_required(owner, path, inspect(reason))
    end
  end

  # Lower generations proved released or dead are removed before the lock is held. A removal that
  # fails is not tolerated silently: the claimant withdraws and reports cleanup_required naming the
  # lower file with THAT file's decoded metadata and token as evidence (never the claimant's), so
  # the operator surface describes the file actually left on disk (fail-closed contract).
  defp compact_then_hold(fs, dir, path, gen, owner, opts, gens) do
    case compact(fs, dir, gens, gen, opts) do
      [] ->
        {:ok, %{path: path, dir: dir, generation: gen, token: owner["token"], owner: owner}}

      [%{outcome: outcome, path: lower, owner: lower_owner, detail: detail} | _] ->
        withdraw_then(fs, dir, path, owner, fn -> compaction_failure(outcome, lower, lower_owner, detail) end)
    end
  end

  # The lower file is still on disk (removal failed) versus removed but its directory fsync failed
  # (the path is absent; durability of the removal is unproven): two different truths.
  defp compaction_failure(:cleanup_required, lower, lower_owner, detail), do: cleanup_required(lower_owner, lower, detail)

  defp compaction_failure(:removed_unsynced, lower, lower_owner, detail) do
    {:error,
     %{
       clause: "lock_unavailable",
       rollback: "removed_unsynced",
       path: lower,
       owner: lower_owner,
       token: lower_owner["token"],
       detail: detail
     }}
  end

  defp compact(fs, dir, gens, gen, opts) do
    gens
    |> Enum.filter(fn {g, _} -> g < gen end)
    |> Enum.flat_map(fn {_g, lower} -> compact_one(fs, dir, lower, opts) end)
  end

  defp compact_one(fs, dir, lower, opts) do
    case read_lock(fs, lower) do
      {:ok, %{"state" => "released"} = metadata} -> remove_reporting(fs, dir, lower, metadata)
      {:ok, metadata} -> if status(metadata, opts) == :dead, do: remove_reporting(fs, dir, lower, metadata), else: []
      _ -> []
    end
  end

  defp remove_reporting(fs, dir, lower, metadata) do
    case remove_synced(fs, dir, lower) do
      :ok ->
        []

      {:removed_unsynced, reason} ->
        [%{outcome: :removed_unsynced, path: lower, owner: metadata, detail: inspect(reason)}]

      {:remains, reason} ->
        [%{outcome: :cleanup_required, path: lower, owner: metadata, detail: inspect(reason)}]
    end
  end

  # Removal truth for a path this claimant intends to remove: :ok once the absence is durably
  # synced (a competing actor may have removed it first; :enoent is cleanup already achieved),
  # {:removed_unsynced, reason} when the path is absent but its directory fsync failed, and
  # {:remains, reason} when the path is still on disk.
  defp remove_synced(fs, dir, path) do
    case Fs.rm(fs, path) do
      :ok -> sync_absence(fs, dir)
      {:error, :enoent} -> sync_absence(fs, dir)
      {:error, reason} -> {:remains, reason}
    end
  end

  defp sync_absence(fs, dir) do
    case Fs.dir_sync(fs, dir) do
      :ok -> :ok
      {:error, reason} -> {:removed_unsynced, reason}
    end
  end

  # Truthful withdrawal of this claimant's own published file: token-verified removal, then a
  # directory fsync. Removal failure => cleanup_required (a live-looking file remains); removal
  # success with a failed directory fsync => removed_unsynced; foreign bytes => ownership_lost.
  defp withdraw(fs, dir, path, owner) do
    token = owner["token"]

    case read_lock(fs, path) do
      {:ok, %{"token" => ^token}} -> remove_own(fs, dir, path, owner)
      {:ok, _foreign} -> {:error, %{clause: "ownership_lost", path: path, token: token}}
      {:error, :enoent} -> :ok
      :unreadable -> cleanup_required(owner, path, "own generation file unreadable")
      {:error, reason} -> cleanup_required(owner, path, inspect(reason))
    end
  end

  defp remove_own(fs, dir, path, owner) do
    case remove_synced(fs, dir, path) do
      :ok ->
        :ok

      {:removed_unsynced, reason} ->
        {:error, %{clause: "lock_unavailable", rollback: "removed_unsynced", detail: inspect(reason)}}

      {:remains, reason} ->
        cleanup_required(owner, path, inspect(reason))
    end
  end

  defp publish_tombstone(fs, dir, gen, path, owner) do
    tombstone = Map.put(owner, "state", "released")
    final = generation_path(dir, gen + 1)

    case publish(fs, dir, final, tombstone) do
      :ok ->
        remove_released(fs, dir, path, owner)

      {:error, :eexist} ->
        {:error, %{clause: "release_failed", detail: "a higher generation exists"}}

      {:error, {:rolled_back, how, detail}} ->
        {:error, %{clause: "release_failed", rollback: how, path: final, detail: detail}}

      {:error, {:cleanup_required, lock, at_path, detail}} ->
        release_cleanup(final, lock, at_path, detail, "cleanup_required")

      {:error, {:candidate_conflict, lock, at_path, detail}} ->
        release_cleanup(nil, lock, at_path, detail, "candidate_conflict")

      {:error, reason} ->
        {:error, %{clause: "release_failed", detail: inspect(reason)}}
    end
  end

  # Release cleanup truth, with the cleanup class passed structurally from the branch that produced
  # it (never inferred from wording): when the tombstone final itself is what remains (its
  # retraction failed) the release stands and the old file plus the temp need cleanup
  # (release_incomplete); when the named path is the temp or a conflicting candidate, the tombstone
  # was never left visible and the lock is still held (release_failed). For cleanup_required the
  # named file's decoded metadata is carried as owner; for candidate_conflict nothing was decoded
  # from that file, so the metadata this attempt was about to publish is carried as claimant.
  defp release_cleanup(final, %{"token" => token} = lock, at_path, detail, "cleanup_required" = cleanup) do
    clause = if at_path == final, do: "release_incomplete", else: "release_failed"
    {:error, %{clause: clause, cleanup: cleanup, path: at_path, owner: lock, token: token, detail: detail}}
  end

  defp release_cleanup(_final, %{"token" => token} = claimant, at_path, detail, "candidate_conflict" = cleanup) do
    {:error,
     %{clause: "release_failed", cleanup: cleanup, path: at_path, claimant: claimant, token: token, detail: detail}}
  end

  # The tombstone is authoritative once published. Old file still present (removal failed) =>
  # release_incomplete with its evidence; removed but the directory fsync failed => the old path
  # is absent and only the removal's durability is unproven => release_removed_unsynced.
  defp remove_released(fs, dir, path, owner) do
    case remove_synced(fs, dir, path) do
      :ok ->
        :ok

      {:removed_unsynced, reason} ->
        {:error, %{clause: "release_removed_unsynced", path: path, token: owner["token"], detail: inspect(reason)}}

      {:remains, reason} ->
        {:error,
         %{clause: "release_incomplete", path: path, owner: owner, token: owner["token"], detail: inspect(reason)}}
    end
  end

  # Atomic, complete publication: private temp (exclusive, write, fsync, close), hard link under the
  # generation name (never replaces), remove the temp, directory fsync. A failure after the name is
  # visible rolls back and reports the rollback's own outcome; a candidate temp that cannot be
  # removed is never hidden: it is reported as cleanup_required naming the temp, and a final that
  # became visible in the same attempt is retracted token-safely first.
  defp publish(fs, dir, final, owner) do
    tmp = final <> "." <> owner["token"] <> ".tmp"

    case Fs.open(fs, tmp, [:exclusive]) do
      {:ok, fd} ->
        with :ok <- write_created_temp(fs, fd, tmp, Jason.encode!(owner) <> "\n", owner) do
          link_and_sync(fs, dir, tmp, final, owner)
        end

      # A candidate this attempt did not create is never touched: named conflict, fail closed. The
      # metadata carried is this attempt's claimant identity; nothing is decoded from the candidate.
      {:error, :eexist} ->
        {:error, {:candidate_conflict, owner, tmp, "candidate present before this attempt opened it; left untouched"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_created_temp(fs, fd, tmp, bytes, owner) do
    case write_all(fs, fd, bytes) do
      :ok -> :ok
      {:error, reason} -> discard_temp(fs, tmp, owner, {:error, reason})
    end
  end

  defp discard_temp(fs, tmp, owner, result) do
    case remove_temp(fs, tmp) do
      :ok ->
        result

      {:error, rm_reason} ->
        temp_cleanup_required(owner, tmp, "not removed: #{inspect(rm_reason)}; after #{inspect(result)}")
    end
  end

  # An already-absent temp is cleanup achieved, whichever way the link went.
  defp remove_temp(fs, tmp) do
    case Fs.rm(fs, tmp) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp link_and_sync(fs, dir, tmp, final, owner) do
    linked = Fs.link(fs, tmp, final)

    case {linked, remove_temp(fs, tmp)} do
      {:ok, :ok} ->
        sync_or_retract(fs, dir, final, owner["token"])

      {:ok, {:error, rm_reason}} ->
        retract_after_temp_failure(fs, dir, final, tmp, owner, rm_reason)

      {{:error, reason}, :ok} ->
        {:error, reason}

      {{:error, reason}, {:error, rm_reason}} ->
        temp_cleanup_required(owner, tmp, "not removed: #{inspect(rm_reason)}; link: #{inspect(reason)}")
    end
  end

  # The final became visible but its temp could not be removed: retract the final (token-safe rm
  # plus directory fsync) so no held result hides a cleanup failure; report both outcomes.
  defp retract_after_temp_failure(fs, dir, final, tmp, owner, rm_reason) do
    case withdraw(fs, dir, final, owner) do
      :ok ->
        temp_cleanup_required(owner, tmp, "not removed: #{inspect(rm_reason)}; final #{Path.basename(final)} retracted")

      {:error, retraction} ->
        {:error,
         {:cleanup_required, owner, final,
          "candidate temp #{Path.basename(tmp)} not removed: #{inspect(rm_reason)}; final retraction failed: #{inspect(retraction)}"}}
    end
  end

  defp temp_cleanup_required(owner, tmp, detail) do
    {:error, {:cleanup_required, owner, tmp, "candidate temp " <> detail}}
  end

  defp sync_or_retract(fs, dir, final, token) do
    case Fs.dir_sync(fs, dir) do
      :ok -> :ok
      {:error, reason} -> {:error, rollback(fs, dir, final, token, reason)}
    end
  end

  defp rollback(fs, dir, final, token, reason) do
    case read_lock(fs, final) do
      {:ok, %{"token" => ^token} = owner} -> remove_and_sync(fs, dir, final, owner, reason)
      _ -> {:rolled_back, "not_ours", inspect(reason)}
    end
  end

  defp remove_and_sync(fs, dir, final, owner, reason) do
    case remove_synced(fs, dir, final) do
      :ok -> {:rolled_back, "removed", inspect(reason)}
      {:removed_unsynced, sync_reason} -> {:rolled_back, "removed_unsynced", inspect({reason, sync_reason})}
      {:remains, rm_reason} -> {:cleanup_required, owner, final, inspect({reason, rm_reason})}
    end
  end

  defp candidate_conflict(%{"token" => token} = claimant, path, detail) do
    {:error, %{clause: "candidate_conflict", path: path, claimant: claimant, token: token, detail: detail}}
  end

  defp cleanup_required(%{"token" => token} = lock, path, detail) do
    {:error, %{clause: "cleanup_required", path: path, owner: lock, token: token, detail: detail}}
  end

  defp write_all(fs, fd, bytes) do
    with :ok <- Fs.write(fs, fd, bytes),
         :ok <- Fs.sync(fs, fd) do
      Fs.close(fs, fd)
    else
      {:error, reason} ->
        _ = Fs.close(fs, fd)
        {:error, reason}
    end
  end

  # Every name carrying the family prefix is classified: a bounded positive generation, a private
  # candidate temp (ignored), or malformed lock state, which fails closed; unrelated names are
  # ignored. A listing error fails closed.
  defp generations(fs, dir) do
    case Fs.list_dir(fs, dir) do
      {:ok, names} -> classify_names(names, dir)
      {:error, reason} -> unavailable({:list_dir, reason})
    end
  end

  defp classify_names(names, dir) do
    classified = Enum.map(names, &classify_name/1)

    case for {:malformed, name} <- classified, do: name do
      [] ->
        gens = for {:generation, gen, name} <- classified, do: {gen, Path.join(dir, name)}
        {:ok, Enum.sort_by(gens, fn {gen, _path} -> -gen end)}

      malformed ->
        {:error, %{clause: "malformed_lock_family", entries: Enum.sort(malformed)}}
    end
  end

  defp classify_name(name) do
    cond do
      match = Regex.run(@generation, name) -> {:generation, String.to_integer(Enum.at(match, 1)), name}
      Regex.match?(@candidate, name) -> :candidate
      String.starts_with?(name, @prefix) -> {:malformed, name}
      true -> :other
    end
  end

  defp generation_path(dir, gen), do: Path.join(dir, @prefix <> Integer.to_string(gen))

  # The lock is one complete, closed object; anything else is unreadable and fails closed.
  defp read_lock(fs, path) do
    case Fs.read(fs, path) do
      {:ok, bytes} -> decode_lock(bytes)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_lock(bytes) do
    case Jason.decode(bytes) do
      {:ok, %{} = metadata} -> if complete_lock?(metadata), do: {:ok, metadata}, else: :unreadable
      _ -> :unreadable
    end
  end

  defp complete_lock?(%{"schema" => @schema, "schema_version" => @schema_version, "state" => state} = metadata)
       when state in @states do
    map_size(metadata) == length(@required_fields) and
      Enum.all?(@required_fields -- ~w(schema schema_version state), fn field ->
        case Map.fetch(metadata, field) do
          {:ok, value} -> is_binary(value) and byte_size(value) > 0
          :error -> false
        end
      end)
  end

  defp complete_lock?(_metadata), do: false

  defp status(metadata, opts) do
    case Keyword.fetch(opts, :owner_status) do
      {:ok, fun} when is_function(fun, 1) -> fun.(metadata)
      :error -> ProcessIdentity.owner_status(metadata, opts)
    end
  end

  defp own_identity(opts) do
    pid = opts |> Keyword.get(:pid, System.pid()) |> to_string()
    clock = Keyword.get(opts, :clock, SystemClock)

    with {:ok, supervisor_instance} <- fetch_binary(opts, :supervisor_instance),
         {:ok, pid_start} <- own_pid_start(pid, opts) do
      {:ok,
       %{
         "schema" => @schema,
         "schema_version" => @schema_version,
         "state" => "held",
         "pid" => pid,
         "pid_start" => pid_start,
         "supervisor_instance" => supervisor_instance,
         "token" => Keyword.get_lazy(opts, :token, &random_token/0),
         "acquired_at" => clock.wall_ts()
       }}
    end
  end

  defp own_pid_start(pid, opts) do
    case Keyword.fetch(opts, :pid_start) do
      {:ok, start} when is_binary(start) and byte_size(start) > 0 ->
        {:ok, start}

      _ ->
        case ProcessIdentity.current(pid, opts) do
          {:ok, start} -> {:ok, start}
          :dead -> unavailable("own process identity not found")
          {:error, reason} -> unavailable(reason)
        end
    end
  end

  defp fetch_binary(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _ -> unavailable("#{key} is required")
    end
  end

  defp random_token, do: 16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

  defp unavailable(reason), do: {:error, %{clause: "lock_unavailable", detail: inspect(reason)}}
end
