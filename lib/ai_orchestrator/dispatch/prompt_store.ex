defmodule AiOrchestrator.Dispatch.PromptStore do
  @moduledoc """
  The durable home of a rendered prompt: content-addressed, create-only, and
  verified again on the way back out.

  `prompt_bundle/5` renders from the whole accumulated event prefix, so a render
  repeated after a resume is a different function of a longer input than the one
  whose hash the journal committed. Reconciliation binds a send to its payload by
  hash, so a payload the product cannot reproduce is a payload it cannot
  reconcile: the daemon would answer `conflict` for a prompt that was never in
  conflict, in exactly the resume path the rule exists to make safe.
  Reproducibility is not retention, so the bytes are kept.

  ## Publication is create-only

  An object is named by what is in it -- `prompts/<assignment_id>-<digest>.org`
  -- and it is published by `link/2`, never by `rename/2`. A rename replaces
  silently; a link refuses, and refusing is the whole guarantee. Two renders of
  the same bytes agree on the name, so the second publication is the first one's
  outcome observed again rather than a second write of the same bytes.

  The chain is: settle the directory, decide the entry, stage a temporary, make it
  durable, link it into place, remove the temporary, and make the new directory
  entry durable. A failure at any step names the step. What it never does is leave
  a name that claims bytes the disk does not hold.

  ## Modes are set while the file is still empty

  `open/3` creates under the process umask, so a `chmod` after the write leaves a
  window in which a world-readable file holds the whole prompt. The temporary is
  narrowed to `0600` before a single byte reaches it, and the directory to `0700`
  before anything is created inside it.

  ## Directories are repaired; objects fail closed

  A `prompts/` directory that is already there at a wider mode is narrowed, and
  the narrowing is made durable by syncing that same directory -- honest, because
  the directory is the inode whose mode changed. The same chain on an object would
  not be: a directory sync persists the directory's entries, not another inode's
  metadata. So an object at a mode this store never writes is refused rather than
  repaired. It is evidence that something outside the run holds the entry, and the
  answer is to say so rather than erase it.

  Link count is judged before mode, for the same reason. An inode a second name
  reaches is not this run's to narrow or to trust, and narrowing it would chmod
  an operator's file that merely shares the inode.

  ## Rejections

  A rejection is a `{reason, class}` pair, and the class never carries the prompt,
  the operator's layout, or the caller's string. Two reasons carry a contained
  relative path in the class slot instead -- `:prompt_object_conflict` and
  `:prompt_object_missing` -- because the name of the object that went wrong is
  the one thing that sends an operator to the right file, and it is metadata the
  journal already holds. `Diagnostic.describe_rejection/1` publishes an atom class
  and drops a path, so the pair is safe to carry either way.
  """

  import Bitwise

  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.PromptRejection
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Journal.Fs

  @dir "prompts"
  @ext ".org"
  @dir_mode 0o700
  @object_mode 0o600
  @version 2

  @typedoc """
  A named refusal. The class is an atom from this release's closed set, or the
  contained relative path for the two reasons that carry one.
  """
  @type rejection :: {atom(), atom() | String.t()}

  @doc """
  Publish the rendered bytes for `assignment_id` under `root`, and return the object
  that names them.

  Publishing the same bytes twice returns the same object: the second call still
  stages a temporary, but the publication it attempts is refused as already done and
  reconciled against what is on disk. Publishing different bytes under a name that is
  already taken is a conflict, and the bytes that are there are left exactly as they
  are.

  `scheme: 1` publishes under the bare legacy name instead, and exists for one purpose:
  restoring the object a pre-retention journal already names, so that journal can be
  made true in place. Everything else is the same operation -- the same staging, the
  same create-only link, the same reconciliation of an identical object and refusal of
  a different one -- because a legacy name that could be overwritten is the bug the
  content-addressed name exists to remove.
  """
  @spec put(Fs.t(), Path.t(), term(), SensitiveBytes.t(), scheme: PromptObject.version()) ::
          {:ok, PromptObject.t()} | {:error, rejection()}
  def put(fs, root, assignment_id, %SensitiveBytes{} = sensitive, opts \\ []) do
    with :ok <- checked_id(assignment_id),
         {:ok, object} <- object(assignment_id, sensitive, Keyword.get(opts, :scheme, @version)),
         :ok <- settled_dir(fs, root),
         :ok <- entry(fs, Path.join(root, object.path), object.path) do
      staged(fs, root, object, sensitive)
    end
  end

  @doc """
  Read back the object a journal committed and prove it is still the object.

  The name is decided before it is used, the object is checked against itself before
  the disk is touched, and the bytes are checked against the digest before they are
  returned. What comes back is wrapped, because unwrapping here would put a bare
  prompt into every caller above this one.
  """
  @spec fetch_verified(Fs.t(), Path.t(), PromptObject.t()) :: {:ok, SensitiveBytes.t()} | {:error, rejection()}
  def fetch_verified(fs, root, %PromptObject{} = object) do
    with :ok <- contained(object.path),
         :ok <- PromptObject.verify(object),
         :ok <- verified_dir(fs, root, object.path),
         path = Path.join(root, object.path),
         :ok <- present(fs, path, object.path),
         {:ok, bytes} <- bytes(fs, path),
         :ok <- matching(bytes, object) do
      {:ok, SensitiveBytes.new(bytes, :prompt)}
    end
  end

  # ----- the name -----

  # The id is half the object's name, so it is judged before the name exists. Nothing
  # below this line has touched the seam yet, and a rejection here has left no trace.
  defp checked_id(id) do
    case PromptObject.classify_assignment_id(id) do
      :ok -> :ok
      {:error, class} -> {:error, {:prompt_assignment_id_invalid, class}}
    end
  end

  defp object(id, sensitive, scheme) do
    "sha256:" <> hex = SensitiveBytes.hash(sensitive)

    name =
      case scheme do
        1 -> id <> @ext
        _content_addressed -> id <> "-" <> hex <> @ext
      end

    PromptObject.new(%{
      assignment_id: id,
      path: Path.join(@dir, name),
      hash: SensitiveBytes.hash(sensitive),
      byte_size: SensitiveBytes.byte_size(sensitive),
      version: scheme
    })
  end

  # `Path.expand` answers whether a name stays under the root, which is not whether
  # it reaches a file under the root, and neither question is asked of a term that is
  # not a name at all. A non-binary path belongs to `verify/1`, which reports it as
  # the missing field it is.
  defp contained(path) when is_binary(path) do
    cond do
      Path.type(path) != :relative -> {:error, {:prompt_path_escapes_root, :absolute}}
      escapes?(path) -> {:error, {:prompt_path_escapes_root, :traversal}}
      true -> :ok
    end
  end

  defp contained(_path), do: :ok

  defp escapes?(path) do
    path
    |> Path.split()
    |> Enum.reduce_while(0, fn
      "..", 0 -> {:halt, :escaped}
      "..", depth -> {:cont, depth - 1}
      ".", depth -> {:cont, depth}
      _segment, depth -> {:cont, depth + 1}
    end) == :escaped
  end

  # ----- the directory -----

  # `mkdir` is not called unconditionally: a second publication under a settled
  # directory must add no directory operations at all, and a directory already at the
  # mode this store writes needs neither a chmod nor a sync to say so.
  defp settled_dir(fs, root) do
    dir = Path.join(root, @dir)

    case Fs.lstat(fs, dir) do
      {:ok, %{type: :directory, mode: @dir_mode}} -> :ok
      {:ok, %{type: :directory}} -> narrowed_dir(fs, dir)
      {:ok, %{type: :regular}} -> {:error, {:prompt_dir_not_directory, :regular}}
      {:ok, %{type: :symlink}} -> {:error, {:prompt_dir_not_directory, :symlink}}
      {:ok, %{type: :other}} -> created_dir(fs, root, dir)
      {:error, :enoent} -> created_dir(fs, root, dir)
      {:error, reason} -> {:error, {:prompt_dir_create_failed, reason}}
    end
  end

  # The read path asks the same question of `prompts/` that the write path does, minus
  # creation, and it asks BEFORE the object is touched: a symlink standing where the
  # directory should be resolves the object's own name to somewhere outside the root, and
  # every check on the object -- type, link count, mode, digest -- would then be a check on
  # a file the store never wrote, passed. The answers match the write path's policy: the
  # directory is the entry the store repairs, so a wide one is narrowed and made durable
  # exactly as `put/5` narrows it; a symlink or a regular file is refused as not a
  # directory; an absent directory is an absent object, which is the one absence that
  # licenses a legacy restore; anything else the kernel says about it is reported as the
  # object being unreadable, with the errno.
  defp verified_dir(fs, root, relative) do
    dir = Path.join(root, @dir)

    case Fs.lstat(fs, dir) do
      {:ok, %{type: :directory, mode: @dir_mode}} -> :ok
      {:ok, %{type: :directory}} -> narrowed_dir(fs, dir)
      {:ok, %{type: :regular}} -> {:error, {:prompt_dir_not_directory, :regular}}
      {:ok, %{type: :symlink}} -> {:error, {:prompt_dir_not_directory, :symlink}}
      {:ok, %{type: :other}} -> {:error, {:prompt_object_conflict, relative}}
      {:error, :enoent} -> {:error, {:prompt_object_missing, relative}}
      {:error, reason} -> {:error, {:prompt_object_unreadable, reason}}
    end
  end

  # A device, a fifo or a socket standing in for the directory is not a type this
  # release has a class for, and inventing one would report a distinction the closed
  # set does not make. `mkdir` is asked instead, and the kernel's own answer -- an
  # errno a syscall actually returned -- is what gets reported.
  defp created_dir(fs, root, dir) do
    with :ok <- made_dir(fs, dir),
         :ok <- chmod_dir(fs, dir) do
      synced_dir(fs, root, :prompt_dir_sync_failed)
    end
  end

  # The narrowing is durable because the directory is the inode whose mode changed.
  defp narrowed_dir(fs, dir) do
    with :ok <- chmod_dir(fs, dir) do
      synced_dir(fs, dir, :prompt_dir_sync_failed)
    end
  end

  defp made_dir(fs, dir) do
    case Fs.mkdir(fs, dir) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_dir_create_failed, reason}}
    end
  end

  defp chmod_dir(fs, dir) do
    case Fs.chmod(fs, dir, @dir_mode) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_dir_chmod_failed, reason}}
    end
  end

  defp synced_dir(fs, dir, reason) do
    case Fs.dir_sync(fs, dir) do
      :ok -> :ok
      {:error, errno} -> {:error, {reason, errno}}
    end
  end

  # ----- the entry -----

  defp entry(fs, path, relative) do
    case Fs.lstat(fs, path) do
      {:error, :enoent} -> :ok
      {:ok, stat} -> usable(stat, relative)
      {:error, reason} -> {:error, {:prompt_object_unreadable, reason}}
    end
  end

  defp present(fs, path, relative) do
    case Fs.lstat(fs, path) do
      {:ok, stat} -> usable(stat, relative)
      {:error, :enoent} -> {:error, {:prompt_object_missing, relative}}
      {:error, reason} -> {:error, {:prompt_object_unreadable, reason}}
    end
  end

  # Link count before mode: an inode a second name reaches is not this run's to
  # narrow, and a wide reused object is refused rather than repaired.
  defp usable(%{type: :regular, links: 1, mode: @object_mode}, _relative), do: :ok

  defp usable(%{type: :regular, links: 1, mode: mode}, _relative),
    do: {:error, {:prompt_object_mode_unexpected, mode_class(mode)}}

  defp usable(%{type: :regular}, _relative), do: {:error, {:prompt_object_multiply_linked, :link_count}}
  defp usable(%{type: :directory}, _relative), do: {:error, {:prompt_object_not_regular, :directory}}
  defp usable(%{type: :symlink}, _relative), do: {:error, {:prompt_object_not_regular, :symlink}}
  defp usable(%{type: :other}, relative), do: {:error, {:prompt_object_conflict, relative}}

  # Bits this store never sets are what make a mode wider, which is the question an
  # operator is asking. A numeric comparison would call `0o060` narrower than `0o600`.
  defp mode_class(mode) do
    if band(mode, bnot(@object_mode)) == 0, do: :narrower, else: :wider
  end

  # ----- the object -----

  defp staged(fs, root, object, sensitive) do
    dir = Path.join(root, @dir)
    temp = Path.join(dir, temp_name())

    case written(fs, temp, sensitive) do
      :ok -> linked(fs, temp, Path.join(root, object.path), dir, object, sensitive)
      {:error, _reason} = failure -> failure
    end
  end

  # Not `.org`, so a temporary that outlives a halted seam is never mistaken for a
  # published object by anything that lists the directory.
  defp temp_name, do: "." <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower) <> ".tmp"

  defp written(fs, temp, sensitive) do
    case Fs.open(fs, temp, [:exclusive]) do
      {:ok, fd} -> discarded(fs, temp, filled(fs, fd, temp, sensitive))
      {:error, reason} -> {:error, {:prompt_open_failed, reason}}
    end
  end

  defp filled(fs, fd, temp, sensitive) do
    outcome =
      with :ok <- chmod_object(fs, temp),
           :ok <- wrote(fs, fd, SensitiveBytes.reveal(sensitive)) do
        synced(fs, fd)
      end

    case outcome do
      :ok -> closed(fs, fd)
      {:error, _reason} = failure -> closing(fs, fd, failure)
    end
  end

  defp chmod_object(fs, temp) do
    case Fs.chmod(fs, temp, @object_mode) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_chmod_failed, reason}}
    end
  end

  defp wrote(fs, fd, bytes) do
    case Fs.write(fs, fd, bytes) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_write_failed, write_class(reason)}}
    end
  end

  # A short write is not observable at this seam -- `write/3` answers `:ok` or an
  # error, never a byte count -- so a reason that is not an errno some syscall
  # returned is the seam having stopped part way through, which is what a torn write
  # is.
  defp write_class(reason) when is_atom(reason) do
    if PromptRejection.allows?(:prompt_write_failed, reason), do: reason, else: :torn_write
  end

  defp write_class(_reason), do: :torn_write

  defp synced(fs, fd) do
    case Fs.sync(fs, fd) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_sync_failed, reason}}
    end
  end

  defp closed(fs, fd) do
    case Fs.close(fs, fd) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_close_failed, reason}}
    end
  end

  defp closing(fs, fd, failure) do
    _ = Fs.close(fs, fd)
    failure
  end

  defp discarded(_fs, _temp, :ok), do: :ok

  defp discarded(fs, temp, {:error, _reason} = failure) do
    _ = Fs.rm(fs, temp)
    failure
  end

  # ----- the publication -----

  defp linked(fs, temp, final, dir, object, sensitive) do
    case Fs.link(fs, temp, final) do
      :ok -> published(fs, temp, dir, object)
      {:error, :eexist} -> reconciled(fs, temp, final, dir, object, sensitive)
      {:error, reason} -> discarded(fs, temp, {:error, {:prompt_link_failed, reason}})
    end
  end

  defp published(fs, temp, dir, object) do
    with :ok <- removed(fs, temp),
         :ok <- synced_dir(fs, dir, :prompt_publication_sync_failed) do
      {:ok, object}
    end
  end

  # The name was already taken, which is the answer this store publishes by. Reading
  # it back is the only way to tell the same render from a different one, and the
  # directory is synced again because the only safe reading of "the object is already
  # there" is "its durability is still unproven".
  defp reconciled(fs, temp, final, dir, object, sensitive) do
    with :ok <- removed(fs, temp),
         {:ok, existing} <- bytes(fs, final),
         :ok <- same(existing, sensitive, object.path),
         :ok <- synced_dir(fs, dir, :prompt_publication_sync_failed) do
      {:ok, object}
    end
  end

  defp removed(fs, temp) do
    case Fs.rm(fs, temp) do
      :ok -> :ok
      {:error, reason} -> {:error, {:prompt_temp_cleanup_failed, reason}}
    end
  end

  defp same(existing, sensitive, relative) do
    if existing == SensitiveBytes.reveal(sensitive) do
      :ok
    else
      {:error, {:prompt_object_conflict, relative}}
    end
  end

  # ----- the bytes -----

  defp bytes(fs, path) do
    case Fs.read(fs, path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:prompt_object_unreadable, reason}}
    end
  end

  # Bytes that do not hash to the committed digest are simply not the committed bytes,
  # and their length is a fact about the damage rather than about the claim. A size
  # that disagrees while the digest agrees is the journal's own claim being wrong, and
  # that is the one the digest matching must not be allowed to excuse.
  defp matching(bytes, object) do
    cond do
      digest(bytes) != object.hash -> {:error, {:prompt_hash_mismatch, :digest}}
      byte_size(bytes) != object.byte_size -> {:error, {:prompt_size_mismatch, :byte_size}}
      true -> :ok
    end
  end

  defp digest(bytes), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
