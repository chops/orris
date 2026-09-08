defmodule AiOrchestrator.Journal.Fs do
  @moduledoc """
  Filesystem seam for the journal writer and reader (MC-12).

  Every durable byte the journal persists goes through this behaviour so tests
  can inject faults at each append boundary. A seam value is `{module, state}`;
  `AiOrchestrator.Journal.Fs.SystemFs` carries no state, a test double carries
  its fault plan. Descriptors are raw files owned by the opening process, so a
  descriptor must be used and closed by the process that opened it.

  Errors are returned, never raised; the writer decides what is fail-closed.
  """

  @type t :: {module(), term()}
  @type fd :: term()
  @type mode :: :append | :exclusive | :write | :read
  @type result :: :ok | {:error, term()}

  @type stat :: %{
          type: :regular | :directory | :symlink | :other,
          mode: non_neg_integer(),
          links: pos_integer(),
          size: non_neg_integer()
        }

  @callback mkdir_p(state :: term(), Path.t()) :: result()
  @callback mkdir(state :: term(), Path.t()) :: result()
  @callback rm(state :: term(), Path.t()) :: result()
  @callback rmdir(state :: term(), Path.t()) :: result()
  @callback link(state :: term(), Path.t(), Path.t()) :: result()
  @callback chmod(state :: term(), Path.t(), non_neg_integer()) :: result()
  @callback lstat(state :: term(), Path.t()) :: {:ok, stat()} | {:error, term()}
  @callback list_dir(state :: term(), Path.t()) :: {:ok, [String.t()]} | {:error, term()}
  @callback open(state :: term(), Path.t(), [mode()]) :: {:ok, fd()} | {:error, term()}
  @callback write(state :: term(), fd(), iodata()) :: result()
  @callback sync(state :: term(), fd()) :: result()
  @callback close(state :: term(), fd()) :: result()
  @callback rename(state :: term(), Path.t(), Path.t()) :: result()
  @callback dir_sync(state :: term(), Path.t()) :: result()
  @callback read(state :: term(), Path.t()) :: {:ok, binary()} | {:error, term()}
  @callback exists?(state :: term(), Path.t()) :: boolean()

  @spec mkdir_p(t(), Path.t()) :: result()
  def mkdir_p({mod, state}, dir), do: mod.mkdir_p(state, dir)

  @doc "Atomic create of one directory: `{:error, :eexist}` when it already exists."
  @spec mkdir(t(), Path.t()) :: result()
  def mkdir({mod, state}, dir), do: mod.mkdir(state, dir)

  @spec rm(t(), Path.t()) :: result()
  def rm({mod, state}, path), do: mod.rm(state, path)

  @spec rmdir(t(), Path.t()) :: result()
  def rmdir({mod, state}, dir), do: mod.rmdir(state, dir)

  @doc """
  Hard-links an existing, fully written file under a new name: atomic and never
  replacing (`{:error, :eexist}`). The publication primitive for locks.
  """
  @spec link(t(), Path.t(), Path.t()) :: result()
  def link({mod, state}, existing, new), do: mod.link(state, existing, new)

  @doc """
  Sets the permission bits of an existing path. Retained evidence -- prompts, and
  anything else holding the exact bytes an agent was given -- is mode 0600 inside a
  0700 directory, so the mode is part of what publication has to succeed at rather
  than something applied hopefully afterwards.
  """
  @spec chmod(t(), Path.t(), non_neg_integer()) :: result()
  def chmod({mod, state}, path, mode), do: mod.chmod(state, path, mode)

  @doc """
  Reads a path's own type, mode and link count without following a final symlink.

  Containment computed from a path string answers "does this name stay under the
  root", which is a different question from "does this name reach a file under the
  root". A symlink is a legal name that answers the first question and fails the
  second, so retained evidence is admitted only when the entry it names is itself a
  regular file inside a real directory.

  The link count is part of the answer for the same reason the type is. A regular
  file with more than one link is reachable under a name the run does not control,
  so its mode and its bytes are not the run's to reason about and a repair applied
  through this name changes an inode that another name still reaches. Publication
  here ends with the temporary removed, so an object this code produced has exactly
  one link; anything else was linked from outside.
  """
  @spec lstat(t(), Path.t()) :: {:ok, stat()} | {:error, term()}
  def lstat({mod, state}, path), do: mod.lstat(state, path)

  @spec list_dir(t(), Path.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_dir({mod, state}, dir), do: mod.list_dir(state, dir)

  @spec open(t(), Path.t(), [mode()]) :: {:ok, fd()} | {:error, term()}
  def open({mod, state}, path, modes), do: mod.open(state, path, modes)

  @spec write(t(), fd(), iodata()) :: result()
  def write({mod, state}, fd, iodata), do: mod.write(state, fd, iodata)

  @spec sync(t(), fd()) :: result()
  def sync({mod, state}, fd), do: mod.sync(state, fd)

  @spec close(t(), fd()) :: result()
  def close({mod, state}, fd), do: mod.close(state, fd)

  @spec rename(t(), Path.t(), Path.t()) :: result()
  def rename({mod, state}, from, to), do: mod.rename(state, from, to)

  @spec dir_sync(t(), Path.t()) :: result()
  def dir_sync({mod, state}, dir), do: mod.dir_sync(state, dir)

  @spec read(t(), Path.t()) :: {:ok, binary()} | {:error, term()}
  def read({mod, state}, path), do: mod.read(state, path)

  @spec exists?(t(), Path.t()) :: boolean()
  def exists?({mod, state}, path), do: mod.exists?(state, path)
end
