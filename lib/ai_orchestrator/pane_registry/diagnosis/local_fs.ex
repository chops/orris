defmodule AiOrchestrator.PaneRegistry.Diagnosis.LocalFs do
  @moduledoc """
  The six filesystem primitives of the claim-refusal diagnosis protocol (B3a design r7), on the local filesystem.
  Each answers `:ok` / `{:ok, value}` or `{:error, typed_reason}`; nothing else is used by `Diagnosis`.

  - `ensure_dir/1`: mkdir -p, mode 0700, then sync the parent directory.
  - `list/1`: the `*.json` names (never temp names), sorted; a missing directory lists as empty.
  - `read/2`: the full bytes of one file.
  - `publish_new/3`: a temp file opened exclusive 0600, written, synced, hard-linked to the name (fails when the name
    exists, so a create never overwrites), the temp removed, the directory synced; `create_failed` when no file was
    created, `create_uncertain` when the file was linked but its cleanup or sync failed.
  - `replace/3`: a temp file opened exclusive 0600, written, synced, renamed over the name, the directory synced.
  - `remove/2`: unlink one file, then sync the directory.
  """

  @spec ensure_dir(String.t()) :: :ok | {:error, String.t()}
  def ensure_dir(dir) do
    with :ok <- File.mkdir_p(dir),
         :ok <- File.chmod(dir, 0o700),
         :ok <- dir_sync(Path.dirname(dir)) do
      :ok
    else
      _failed -> {:error, "dir_unavailable"}
    end
  end

  @spec list(String.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def list(dir) do
    case File.ls(dir) do
      {:ok, names} -> {:ok, names |> Enum.filter(&json_name?/1) |> Enum.sort()}
      {:error, :enoent} -> {:ok, []}
      {:error, _reason} -> {:error, "list_failed"}
    end
  end

  @spec read(String.t(), String.t()) :: {:ok, binary()} | {:error, String.t()}
  def read(dir, name) do
    case File.read(Path.join(dir, name)) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, :enoent} -> {:error, "file_missing"}
      {:error, _reason} -> {:error, "file_unreadable"}
    end
  end

  @spec publish_new(String.t(), String.t(), iodata()) :: :ok | {:error, String.t()}
  def publish_new(dir, name, bytes), do: publish_new(dir, name, bytes, [])

  @doc """
  `publish_new/3` with one test seam: `unlink:` replaces the temp-name removal (default `File.rm/1`). When the link
  succeeded but the temp name could not be removed, or the directory sync after the link failed, the diagnosis file
  exists while the create was not completed cleanly: that answers `create_uncertain`, never `:ok` and never
  `create_failed` (which means no file was created).
  """
  @spec publish_new(String.t(), String.t(), iodata(), keyword()) :: :ok | {:error, String.t()}
  def publish_new(dir, name, bytes, opts) do
    unlink = Keyword.get(opts, :unlink, &File.rm/1)

    case write_temp(dir, name, bytes) do
      {:ok, temp} ->
        case File.ln(temp, Path.join(dir, name)) do
          :ok ->
            if unlink.(temp) == :ok and dir_sync(dir) == :ok, do: :ok, else: {:error, "create_uncertain"}

          {:error, _reason} ->
            unlink.(temp)
            {:error, "create_failed"}
        end

      :error ->
        {:error, "create_failed"}
    end
  end

  @spec replace(String.t(), String.t(), iodata()) :: :ok | {:error, String.t()}
  def replace(dir, name, bytes) do
    with {:ok, temp} <- write_temp(dir, name, bytes),
         :ok <- rename_or_drop(temp, Path.join(dir, name)),
         :ok <- dir_sync(dir) do
      :ok
    else
      _failed -> {:error, "update_failed"}
    end
  end

  @spec remove(String.t(), String.t()) :: :ok | {:error, String.t()}
  def remove(dir, name) do
    with :ok <- File.rm(Path.join(dir, name)),
         :ok <- dir_sync(dir) do
      :ok
    else
      _failed -> {:error, "remove_failed"}
    end
  end

  # Temp names start with "." and end in ".tmp", so list/1 never returns one.
  defp json_name?(name), do: String.ends_with?(name, ".json") and not String.starts_with?(name, ".")

  defp write_temp(dir, name, bytes) do
    temp = Path.join(dir, ".#{name}.#{System.unique_integer([:positive])}.tmp")

    case File.open(temp, [:write, :binary, :exclusive]) do
      {:ok, io} ->
        result =
          with :ok <- File.chmod(temp, 0o600),
               :ok <- IO.binwrite(io, bytes) do
            :file.sync(io)
          end

        File.close(io)
        if result == :ok, do: {:ok, temp}, else: drop(temp)

      {:error, _reason} ->
        :error
    end
  end

  defp rename_or_drop(temp, path) do
    case File.rename(temp, path) do
      :ok -> :ok
      {:error, _reason} -> drop(temp)
    end
  end

  defp drop(temp) do
    File.rm(temp)
    :error
  end

  defp dir_sync(dir) do
    with {:ok, fd} <- :file.open(dir, [:read, :raw, :binary, :directory]) do
      result = :file.sync(fd)
      _ = :file.close(fd)
      result
    end
  end
end
