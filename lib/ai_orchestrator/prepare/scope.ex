defmodule AiOrchestrator.Prepare.Scope do
  @moduledoc """
  The ONE resolver from a public run handle (`run_ref`: a directory NAME under a server-configured root) to a run
  directory, shared by reads and mutations (docs/contracts/public-console-seam.org, F-5/F-6).

  Closed behaviour: the configured root is canonicalised PHYSICALLY (made absolute without lexical collapse, every
  component must exist, each symlink resolved before a later ".." applies to the resolved parent); an absent or
  non-directory root answers `runs_root_missing`; a handle must be one non-empty printable path segment (no separators, not "." or "..",
  at most 255 bytes) or it is `run_ref_invalid`; an absent target is `run_directory_missing`, a non-directory is
  `run_directory_invalid`, and a directory whose canonical path leaves the root is `run_ref_outside_root`.

  TRUSTED LOCAL FILESYSTEM ASSUMPTION: the checks are point in time; a hostile concurrent rename between resolve
  and use is not defended (the CLI makes the same assumption for its run_dir argument).
  """

  @max_ref_bytes 255
  @max_links 32

  @type rejection :: %{clause: String.t(), detail: map() | nil}

  @spec resolve(term(), keyword()) :: {:ok, Path.t()} | {:error, rejection()}
  def resolve(run_ref, opts) when is_list(opts) do
    with :ok <- valid_ref(run_ref),
         {:ok, root} <- canonical_root(opts) do
      target(Path.join(root, run_ref), root)
    end
  end

  def resolve(_run_ref, _opts), do: {:error, %{clause: "run_ref_invalid", detail: nil}}

  @doc "The canonical (symlink-resolved) root, or `runs_root_missing`."
  @spec canonical_root(keyword()) :: {:ok, Path.t()} | {:error, rejection()}
  def canonical_root(opts) do
    case Keyword.get(opts, :root) do
      root when is_binary(root) and root != "" -> canonical_directory(root)
      _absent -> {:error, %{clause: "runs_root_missing", detail: nil}}
    end
  end

  defp canonical_directory(root) do
    with {:ok, canonical} <- canonical(root),
         true <- File.dir?(canonical) do
      {:ok, canonical}
    else
      _missing -> {:error, %{clause: "runs_root_missing", detail: %{root: root}}}
    end
  end

  @doc "Whether `path`'s canonical (physically traversed) form lies strictly inside the canonical `root`."
  @spec inside?(Path.t(), Path.t()) :: boolean()
  def inside?(path, root) do
    case canonical(path) do
      {:ok, canonical} -> contained?(canonical, root)
      :error -> false
    end
  end

  @doc """
  The canonical form of a path: made absolute WITHOUT lexical collapse, then every component walked physically
  (each must exist; symlinks resolved in traversal order; `..` applied to the RESOLVED parent); `:error` when the
  operating system could not walk it.
  """
  @spec canonical(Path.t()) :: {:ok, Path.t()} | :error
  def canonical(path), do: walk(Path.split(absolute(path)), "/", @max_links)

  defp absolute("/" <> _rest = path), do: path
  defp absolute(path), do: Path.join(File.cwd!(), path)

  # strict: the root itself is never inside the root
  defp contained?(path, root), do: path != root and String.starts_with?(path, prefix(root))

  defp prefix("/"), do: "/"
  defp prefix(root), do: root <> "/"

  defp valid_ref(ref) when is_binary(ref) do
    cond do
      ref == "" or ref in [".", ".."] -> {:error, %{clause: "run_ref_invalid", detail: nil}}
      byte_size(ref) > @max_ref_bytes -> {:error, %{clause: "run_ref_invalid", detail: nil}}
      String.contains?(ref, ["/", "\\", <<0>>]) -> {:error, %{clause: "run_ref_invalid", detail: nil}}
      not String.printable?(ref) -> {:error, %{clause: "run_ref_invalid", detail: nil}}
      true -> :ok
    end
  end

  defp valid_ref(_other), do: {:error, %{clause: "run_ref_invalid", detail: nil}}

  defp target(path, root) do
    case File.lstat(path) do
      {:error, :enoent} ->
        {:error, %{clause: "run_directory_missing", detail: nil}}

      {:error, reason} ->
        {:error, %{clause: "run_directory_invalid", detail: %{detail: inspect(reason)}}}

      {:ok, _stat} ->
        cond do
          not File.dir?(path) -> {:error, %{clause: "run_directory_invalid", detail: nil}}
          inside?(path, root) -> {:ok, path}
          true -> {:error, %{clause: "run_ref_outside_root", detail: nil}}
        end
    end
  end

  # Physical traversal: each component is joined to the RESOLVED accumulator and must exist (only a directory may
  # precede further components; a regular final leaf is canonicalisable); a symlink's target
  # components are pushed back onto the queue (never collapsed lexically) so a later ".." applies to the resolved
  # parent, exactly as the operating system walks the path. Bounded in link depth; unwalkable paths answer :error.
  defp walk(_parts, _acc, 0), do: :error
  defp walk([], acc, _links), do: {:ok, acc}
  defp walk(["/" | rest], _acc, links), do: walk(rest, "/", links)
  defp walk(["." | rest], acc, links), do: walk(rest, acc, links)
  defp walk([".." | rest], acc, links), do: walk(rest, parent(acc), links)

  defp walk([part | rest], acc, links) do
    candidate = Path.join(acc, part)

    case File.lstat(candidate) do
      {:ok, %File.Stat{type: :symlink}} -> follow(candidate, rest, acc, links)
      {:ok, %File.Stat{type: :directory}} -> walk(rest, candidate, links)
      {:ok, _present} when rest == [] -> {:ok, candidate}
      {:ok, _not_a_directory} -> :error
      {:error, _absent_or_unreadable} -> :error
    end
  end

  defp follow(candidate, rest, acc, links) do
    case File.read_link(candidate) do
      {:ok, "/" <> _target = link} -> walk(Enum.reject(Path.split(link), &(&1 == "/")) ++ rest, "/", links - 1)
      {:ok, link} -> walk(Path.split(link) ++ rest, acc, links - 1)
      {:error, _unreadable} -> :error
    end
  end

  defp parent("/"), do: "/"
  defp parent(path), do: Path.dirname(path)
end
