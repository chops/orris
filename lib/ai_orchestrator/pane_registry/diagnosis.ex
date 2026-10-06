defmodule AiOrchestrator.PaneRegistry.Diagnosis do
  @moduledoc """
  Durable claim-refusal diagnoses (NS-15.G.005, B3a): one JSON file per open (pane_ref, trigger) pair under
  `<claims_root>/diagnoses/<diagnosis_id>.json`, written only through the six primitives of `:diagnosis_fs`
  (default `Diagnosis.LocalFs`) and only while holding the one claims-root lock.

  - `open/3` repeats the open diagnosis of the same pair (seen_count + 1, last_seen_at, observed_daemon_state) or
    creates a new one; before a create, while the resolved count is at the bound, the oldest resolved files (by
    resolved_at) are removed and their ids reported in "removed" (in the return value only, never in a file).
  - `resolve/5` resolves the open diagnosis of exactly that pair, only by the check that can verify its trigger (a
    claim resolves only live_holder), setting status, resolved_at and resolved_by and nothing else.

  Every persistence failure, a lock failure included, answers `{:error, %{"persistence" => %{"ok" => false,
  "error" => reason}}}`. A lock lost while the sequence ran (its helper killed from outside) answers "lock_lost":
  the sequence may have overlapped another holder, so it is never reported as done.
  """

  alias AiOrchestrator.PaneRegistry.Diagnosis.LocalFs
  alias AiOrchestrator.PaneRegistry.RootLock

  @default_resolved_bound 256
  @lock_opts [:wait_ms, :grace_ms, :root_lock_helper, :on_contend, :helper_observer]
  @resolving_checks %{
    "dead" => ["pane_status_v1", "status_v3"],
    "unregistered" => ["pane_status_v1", "status_v3"],
    "contradictory" => ["status_v3"],
    "daemon_unavailable" => ["pane_status_v1", "status_v3"],
    "live_holder" => ["file_registry_claim"],
    "quarantined" => ["status_v3"]
  }
  @attr_keys ~w(trigger pane_ref daemon_pane_id holder observed_daemon_state next_action)

  @spec open(String.t(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def open(root, attrs, opts) when is_binary(root) and is_map(attrs) and is_list(opts) do
    attrs = Map.take(attrs, @attr_keys)

    # attrs are checked before the lock and before any write: a file this module would not admit on its next read
    # is never written (that would turn every repeat into a new open diagnosis)
    if attrs?(attrs),
      do: locked(root, opts, fn dir, fs -> open_locked(dir, fs, attrs, opts) end),
      else: {:error, %{"reason" => "diagnosis_attrs_invalid"}}
  end

  defp attrs?(attrs) do
    map_size(attrs) == length(@attr_keys) and attrs["trigger"] in Map.keys(@resolving_checks) and
      is_binary(attrs["pane_ref"]) and attrs["pane_ref"] != ""
  end

  @spec resolve(String.t(), String.t(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def resolve(root, pane_ref, trigger, resolved_by, opts \\ [])
      when is_binary(root) and is_binary(pane_ref) and is_binary(trigger) and is_map(resolved_by) do
    locked(root, opts, fn dir, fs -> resolve_locked(dir, fs, pane_ref, trigger, resolved_by, opts) end)
  end

  @doc """
  Resolves, under ONE lock and one directory read, each open diagnosis of `pane_ref` named in `resolutions`
  (`[{trigger, resolved_by}]`) whose check verifies its trigger; pairs with no open diagnosis are skipped. Answers
  the resolved ids, or the first persistence failure (diagnoses resolved before it stay resolved).
  """
  @spec resolve_verified(String.t(), String.t(), [{String.t(), map()}], keyword()) ::
          {:ok, [String.t()]} | {:error, map()}
  def resolve_verified(root, pane_ref, resolutions, opts) when is_binary(pane_ref) and is_list(resolutions) do
    locked(root, opts, fn dir, fs ->
      with {:ok, docs} <- read_all(dir, fs) do
        Enum.reduce_while(resolutions, {:ok, []}, &resolve_pair(&1, &2, dir, fs, docs, pane_ref, opts))
      end
    end)
  end

  defp resolve_pair({trigger, resolved_by}, {:ok, resolved}, dir, fs, docs, pane_ref, opts) do
    with {:ok, doc} <- open_doc(docs, pane_ref, trigger),
         :ok <- resolving_check(trigger, resolved_by),
         {:ok, updated} <- resolve_doc(dir, fs, doc, resolved_by, opts) do
      {:cont, {:ok, resolved ++ [updated["diagnosis_id"]]}}
    else
      {:error, %{"persistence" => _failure}} = error -> {:halt, error}
      {:error, %{"reason" => _not_open_or_unverified}} -> {:cont, {:ok, resolved}}
    end
  end

  defp locked(root, opts, fun) do
    root = Path.expand(root)
    fs = Keyword.get(opts, :diagnosis_fs, LocalFs)

    with :ok <- ensure_root(root) do
      root
      |> RootLock.run("diagnosis", Keyword.take(opts, @lock_opts), fn _lock -> fun.(Path.join(root, "diagnoses"), fs) end)
      |> lock_result()
    end
  end

  defp lock_result({:ok, result}), do: result
  defp lock_result({:error, %{"lock" => "release_unconfirmed", "result" => result}}), do: result
  defp lock_result({:error, %{"lock" => "lost"}}), do: persistence("lock_lost")
  defp lock_result({:error, reason}) when is_binary(reason), do: persistence(reason)

  # The lock file lives in the claims root, so the root must exist before the lock; it is the registry's own
  # directory, created 0700 as FileRegistry creates it.
  defp ensure_root(root) do
    with :ok <- File.mkdir_p(root),
         :ok <- File.chmod(root, 0o700) do
      :ok
    else
      _failed -> persistence("dir_unavailable")
    end
  end

  defp open_locked(dir, fs, attrs, opts) do
    with :ok <- fs_call(fs.ensure_dir(dir)),
         {:ok, docs} <- read_all(dir, fs) do
      case find_open(docs, attrs["pane_ref"], attrs["trigger"]) do
        {_name, doc} -> repeat(dir, fs, doc, attrs, opts)
        nil -> create(dir, fs, docs, attrs, opts)
      end
    end
  end

  defp repeat(dir, fs, doc, attrs, opts) do
    updated =
      doc
      |> Map.update!("seen_count", &(&1 + 1))
      |> Map.put("last_seen_at", now(opts))
      |> Map.put("observed_daemon_state", attrs["observed_daemon_state"])

    with :ok <- fs_call(fs.replace(dir, file_name(updated), Jason.encode!(updated))) do
      {:ok, Map.put(updated, "removed", [])}
    end
  end

  defp create(dir, fs, docs, attrs, opts) do
    with {:ok, removed} <- retain(dir, fs, docs, Keyword.get(opts, :resolved_bound, @default_resolved_bound)) do
      at = now(opts)

      doc =
        Map.merge(attrs, %{
          "diagnosis_id" => diagnosis_id(opts),
          "opened_at" => at,
          "last_seen_at" => at,
          "seen_count" => 1,
          "status" => "open"
        })

      with :ok <- fs_call(fs.publish_new(dir, file_name(doc), Jason.encode!(doc))) do
        {:ok, Map.put(doc, "removed", removed)}
      end
    end
  end

  # Before a create only: while the resolved count is at the bound, remove the oldest resolved file. Open files are
  # never removed. The first failed removal refuses the create.
  defp retain(dir, fs, docs, bound) do
    resolved =
      docs
      |> Enum.filter(fn {_name, doc} -> doc["status"] == "resolved" end)
      |> Enum.sort_by(fn {name, doc} -> {doc["resolved_at"], name} end)

    excess = max(length(resolved) - bound + 1, 0)

    resolved
    |> Enum.take(excess)
    |> Enum.reduce_while({:ok, []}, fn {name, doc}, {:ok, removed} ->
      case fs_call(fs.remove(dir, name)) do
        :ok -> {:cont, {:ok, removed ++ [doc["diagnosis_id"]]}}
        {:error, _persistence} = error -> {:halt, error}
      end
    end)
  end

  defp resolve_locked(dir, fs, pane_ref, trigger, resolved_by, opts) do
    with {:ok, docs} <- read_all(dir, fs),
         {:ok, doc} <- open_doc(docs, pane_ref, trigger),
         :ok <- resolving_check(trigger, resolved_by) do
      resolve_doc(dir, fs, doc, resolved_by, opts)
    end
  end

  defp resolve_doc(dir, fs, doc, resolved_by, opts) do
    updated =
      doc
      |> Map.put("status", "resolved")
      |> Map.put("resolved_at", now(opts))
      |> Map.put("resolved_by", resolved_by)

    with :ok <- fs_call(fs.replace(dir, file_name(updated), Jason.encode!(updated))) do
      {:ok, updated}
    end
  end

  defp open_doc(docs, pane_ref, trigger) do
    case find_open(docs, pane_ref, trigger) do
      {_name, doc} -> {:ok, doc}
      nil -> {:error, %{"reason" => "diagnosis_not_open"}}
    end
  end

  defp resolving_check(trigger, %{"check" => check}) do
    if check in Map.get(@resolving_checks, trigger, []),
      do: :ok,
      else: {:error, %{"reason" => "resolution_check_mismatch"}}
  end

  defp resolving_check(_trigger, _resolved_by), do: {:error, %{"reason" => "resolution_check_mismatch"}}

  # Every diagnosis file in the directory, decoded. A file that is not a complete diagnosis object (unparseable, or
  # failing diagnosis?/2) is never matched and never changed; a read failure is a persistence failure.
  defp read_all(dir, fs) do
    with {:ok, names} <- fs_call(fs.list(dir)) do
      Enum.reduce_while(names, {:ok, []}, &read_one(&1, &2, dir, fs))
    end
  end

  defp read_one(name, {:ok, docs}, dir, fs) do
    case fs_call(fs.read(dir, name)) do
      {:ok, bytes} -> {:cont, {:ok, decoded(name, bytes, docs)}}
      {:error, _persistence} = error -> {:halt, error}
    end
  end

  defp decoded(name, bytes, docs) do
    case Jason.decode(bytes) do
      {:ok, doc} -> if diagnosis?(name, doc), do: docs ++ [{name, doc}], else: docs
      {:error, _not_json} -> docs
    end
  end

  # The full schema this module writes, with types; anything else is not a diagnosis and is never matched, repeated,
  # resolved or evicted (it stays exactly as found for an operator).
  defp diagnosis?(name, %{"diagnosis_id" => id, "status" => status} = doc) when is_binary(id) do
    name == id <> ".json" and status in ["open", "resolved"] and fields?(doc) and status_fields?(status, doc)
  end

  defp diagnosis?(_name, _doc), do: false

  defp fields?(doc) do
    Map.get(doc, "trigger") in Map.keys(@resolving_checks) and
      Enum.all?(~w(pane_ref opened_at last_seen_at), &non_empty_string?(Map.get(doc, &1))) and
      positive_integer?(Map.get(doc, "seen_count")) and
      Enum.all?(~w(daemon_pane_id holder observed_daemon_state next_action), &Map.has_key?(doc, &1))
  end

  defp non_empty_string?(value), do: is_binary(value) and value != ""

  defp status_fields?("open", doc), do: not Map.has_key?(doc, "resolved_at") and not Map.has_key?(doc, "resolved_by")

  defp status_fields?("resolved", doc),
    do:
      is_binary(Map.get(doc, "resolved_at")) and Map.get(doc, "resolved_at") != "" and is_map(Map.get(doc, "resolved_by"))

  defp positive_integer?(value), do: is_integer(value) and value > 0

  defp find_open(docs, pane_ref, trigger) do
    Enum.find(docs, fn {_name, doc} ->
      doc["status"] == "open" and doc["pane_ref"] == pane_ref and doc["trigger"] == trigger
    end)
  end

  defp fs_call(:ok), do: :ok
  defp fs_call({:ok, value}), do: {:ok, value}
  defp fs_call({:error, reason}) when is_binary(reason), do: persistence(reason)

  defp persistence(reason), do: {:error, %{"persistence" => %{"ok" => false, "error" => reason}}}
  defp file_name(%{"diagnosis_id" => id}), do: id <> ".json"

  defp diagnosis_id(opts) do
    case Keyword.get(opts, :id_fun) do
      fun when is_function(fun, 0) -> fun.()
      nil -> "dgn_" <> (16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower))
    end
  end

  defp now(opts) do
    case Keyword.get(opts, :now_fun) do
      fun when is_function(fun, 0) -> fun.()
      nil -> DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
    end
  end
end
