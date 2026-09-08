defmodule AiOrchestrator.Journal.Fs.SystemFs do
  @moduledoc """
  Production `AiOrchestrator.Journal.Fs`: raw binary files via `:file`, with
  `fsync` on data files and on directories (opened with the `:directory` mode
  so the entry rename itself is made durable, as OPEN-19(a) requires).

  Known limit: `:file.sync/1` issues `fsync(2)`; on macOS that does not force
  the drive cache (`F_FULLFSYNC`), which Erlang cannot request without a NIF.
  """

  @behaviour AiOrchestrator.Journal.Fs

  @spec new() :: AiOrchestrator.Journal.Fs.t()
  def new, do: {__MODULE__, nil}

  @impl true
  def mkdir_p(_state, dir), do: File.mkdir_p(dir)

  @impl true
  def mkdir(_state, dir), do: File.mkdir(dir)

  @impl true
  def rm(_state, path), do: File.rm(path)

  @impl true
  def rmdir(_state, dir), do: File.rmdir(dir)

  @impl true
  def link(_state, existing, new), do: :file.make_link(existing, new)

  @impl true
  def chmod(_state, path, mode), do: File.chmod(path, mode)

  @impl true
  def lstat(_state, path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: type, mode: mode, links: links, size: size}} ->
        {:ok, %{type: normalize(type), mode: Bitwise.band(mode, 0o7777), links: links, size: size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize(type) when type in [:regular, :directory, :symlink], do: type
  defp normalize(_type), do: :other

  @impl true
  def list_dir(_state, dir), do: File.ls(dir)

  @impl true
  def open(_state, path, modes) do
    :file.open(path, [:raw, :binary | Enum.flat_map(modes, &translate/1)])
  end

  @impl true
  def write(_state, fd, iodata), do: :file.write(fd, iodata)

  @impl true
  def sync(_state, fd), do: :file.sync(fd)

  @impl true
  def close(_state, fd), do: :file.close(fd)

  @impl true
  def rename(_state, from, to), do: :file.rename(from, to)

  @impl true
  def dir_sync(_state, dir) do
    with {:ok, fd} <- :file.open(dir, [:read, :raw, :binary, :directory]) do
      result = :file.sync(fd)
      _ = :file.close(fd)
      result
    end
  end

  @impl true
  def read(_state, path), do: :file.read_file(path)

  @impl true
  def exists?(_state, path), do: File.exists?(path)

  defp translate(:append), do: [:append]
  defp translate(:exclusive), do: [:write, :exclusive]
  defp translate(:write), do: [:write]
  defp translate(:read), do: [:read]
end
