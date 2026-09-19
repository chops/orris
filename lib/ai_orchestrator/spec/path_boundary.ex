defmodule AiOrchestrator.Spec.PathBoundary do
  @moduledoc """
  The ONE containment judgement for a changed path against a worktree root and its allowed roots
  (NS-20.D.001: an out-of-scope write or a worktree escape cannot complete; `allowed_paths` is
  enforced by changed-path comparison around every mutating attempt).

  Two layers share one closed vocabulary:

  - `lexical/2` is pure. A changed path must be relative (`path_absolute`), carry no `..` segment
    (`path_traversal`), and lie under one allowed root once expanded (`path_outside_roots`). It is
    the layer a pure admission check (`Spec.Plan`) runs.
  - `check/3` adds the physical layer against a worktree on disk. The worktree root is canonicalised
    (every component walked, symlinks resolved); every EXISTING ancestor of a path inside it is
    `lstat`ed, a symlink is followed (bounded, its target's own `..` applied to the RESOLVED parent),
    and a resolution that leaves the canonical worktree root or every canonical allowed root is
    `path_symlink_escape`. A component the operating system will not describe, other than by absence,
    is `path_unreadable` (fail closed: containment is unproven). An absent component is not an escape:
    nothing exists to write through yet, so the remainder is judged lexically. An absent worktree root
    leaves only the lexical layer.

  A change is a path or a rename pair `{:rename, from, to}`; both sides of a rename are changed paths
  (renaming an in-scope file out of scope, or an out-of-scope file in, is a mutation of both names).
  The rejection carries the clause and the offending path exactly as it was given -- nothing else.

  `overlapping?/2` is the same lexical expansion applied to two allowed roots: `Spec.Plan`'s
  concurrent-writer refusal judges whether two roots name one directory, or one under the other,
  through it, so every spelling this module admits as one directory is one directory to it too. The
  journal fold's workspace-lease overlap repeats the expansion as a pure segment walk (it may reach
  neither this boundary nor `Path`, which consults `:os`); test/spec/path_spellings_overlap_test.exs
  holds the two in step.

  TRUSTED LOCAL FILESYSTEM ASSUMPTION: the physical layer is point in time; a symlink planted after the
  check is not defended by it (the same assumption `Prepare.Scope` makes for run directories).
  """

  @clauses ~w(path_absolute path_traversal path_outside_roots path_symlink_escape path_unreadable)
  @max_links 32

  @type change :: Path.t() | {:rename, Path.t(), Path.t()}
  @type rejection :: %{clause: String.t(), path: String.t()}

  @doc "The closed rejection vocabulary, in the order the layers judge it."
  @spec clauses() :: [String.t()]
  def clauses, do: @clauses

  @doc """
  The pure layer: every change must be relative, `..`-free, and under one of `allowed_roots` (each
  taken as a worktree-relative name) once lexically expanded. The first offending path is named.
  """
  @spec lexical([Path.t()], [change()]) :: :ok | {:error, rejection()}
  def lexical(allowed_roots, changes) when is_list(allowed_roots) and is_list(changes) do
    roots = Enum.map(allowed_roots, &expanded/1)
    first_rejection(paths(changes), &lexical_rejection(&1, roots))
  end

  @doc """
  Whether two allowed roots, each expanded exactly as `lexical/2` expands a worktree-relative name,
  are one directory or one lies under the other. `lib`, `./lib`, `lib/` and `lib//x/./y` are
  spellings, not distinct roots; a sibling is not under either.
  """
  @spec overlapping?(Path.t(), Path.t()) :: boolean()
  def overlapping?(left, right) when is_binary(left) and is_binary(right) do
    left = expanded(left)
    right = expanded(right)
    contained?(left, right) or contained?(right, left)
  end

  @doc """
  The lexical layer, then the physical layer against `worktree_root` on disk: a symlink met while
  walking a path's existing ancestors (or standing at its leaf) must resolve inside the canonical
  worktree root AND under one canonical allowed root.
  """
  @spec check(Path.t(), [Path.t()], [change()]) :: :ok | {:error, rejection()}
  def check(worktree_root, allowed_roots, changes) when is_binary(worktree_root) do
    with :ok <- lexical(allowed_roots, changes) do
      case canonical_root(worktree_root) do
        {:ok, root} ->
          roots = Enum.map(allowed_roots, &canonical_allowed_root(root, &1))
          first_rejection(paths(changes), &physical_rejection(&1, root, roots))

        :absent ->
          :ok

        :unreadable ->
          {:error, reject("path_unreadable", ".")}
      end
    end
  end

  # ---- changes ----

  defp paths(changes) do
    Enum.flat_map(changes, fn
      path when is_binary(path) -> [path]
      {:rename, from, to} when is_binary(from) and is_binary(to) -> [from, to]
      other -> raise ArgumentError, "a change is a path or {:rename, from, to}, got: #{inspect(other)}"
    end)
  end

  defp first_rejection(paths, judge) do
    case Enum.find_value(paths, judge) do
      nil -> :ok
      rejection -> {:error, rejection}
    end
  end

  defp reject(clause, path), do: %{clause: clause, path: path}

  # ---- the lexical layer ----

  defp lexical_rejection(path, roots) do
    cond do
      Path.type(path) != :relative -> reject("path_absolute", path)
      ".." in Path.split(path) -> reject("path_traversal", path)
      not under_any?(expanded(path), roots) -> reject("path_outside_roots", path)
      true -> nil
    end
  end

  # a worktree-relative name expanded under a fixed anchor: `.`, `//` and a trailing `/` collapse
  defp expanded(path), do: Path.expand(path, "/")

  defp under_any?(path, roots), do: Enum.any?(roots, &contained?(path, &1))

  defp contained?(path, root), do: path == root or String.starts_with?(path, prefix(root))

  defp prefix("/"), do: "/"
  defp prefix(root), do: root <> "/"

  # ---- the physical layer ----

  # the worktree root, walked physically; absent (or not a directory) leaves nothing to write through
  defp canonical_root(worktree_root) do
    case walk(Path.split(absolute(worktree_root)), "/", @max_links) do
      {:ok, canonical} -> if File.dir?(canonical), do: {:ok, canonical}, else: :absent
      {:error, :enoent} -> :absent
      {:error, _unreadable} -> :unreadable
    end
  end

  # an allowed root that exists is its resolved directory; one that does not is its lexical place
  defp canonical_allowed_root(root, allowed_root) do
    lexical = Path.expand(allowed_root, root)

    case walk(Path.split(lexical), "/", @max_links) do
      {:ok, canonical} -> canonical
      {:error, _absent_or_unreadable} -> lexical
    end
  end

  defp physical_rejection(path, root, roots) do
    case resolve(Path.split(path), root, @max_links) do
      {:ok, resolved} ->
        if contained?(resolved, root) and under_any?(resolved, roots), do: nil, else: reject("path_symlink_escape", path)

      {:error, :unreadable} ->
        reject("path_unreadable", path)
    end
  end

  defp absolute("/" <> _rest = path), do: path
  defp absolute(path), do: Path.join(File.cwd!(), path)

  # Physical traversal of a changed path from the canonical root: each component is joined to the
  # RESOLVED accumulator; a symlink's target components are pushed back onto the queue (never
  # collapsed lexically) so its `..` applies to the resolved parent, exactly as the kernel walks it;
  # an absent component (or a non-directory ancestor) ends the walk and the remainder is completed
  # lexically, which is where a later `mkdir -p` would put it.
  defp resolve(_parts, _acc, 0), do: {:error, :unreadable}
  defp resolve([], acc, _links), do: {:ok, acc}
  defp resolve(["/" | rest], _acc, links), do: resolve(rest, "/", links)
  defp resolve(["." | rest], acc, links), do: resolve(rest, acc, links)
  defp resolve([".." | rest], acc, links), do: resolve(rest, parent(acc), links)

  defp resolve([part | rest], acc, links) do
    candidate = Path.join(acc, part)

    case File.lstat(candidate) do
      {:ok, %File.Stat{type: :symlink}} -> follow(candidate, rest, acc, links)
      {:ok, %File.Stat{type: :directory}} -> resolve(rest, candidate, links)
      {:ok, _not_a_directory} -> {:ok, completed(candidate, rest)}
      {:error, absent} when absent in [:enoent, :enotdir] -> {:ok, completed(candidate, rest)}
      {:error, _unreadable} -> {:error, :unreadable}
    end
  end

  defp follow(candidate, rest, acc, links) do
    case File.read_link(candidate) do
      {:ok, "/" <> _target = link} -> resolve(Enum.reject(Path.split(link), &(&1 == "/")) ++ rest, "/", links - 1)
      {:ok, link} -> resolve(Path.split(link) ++ rest, acc, links - 1)
      {:error, _unreadable} -> {:error, :unreadable}
    end
  end

  defp completed(candidate, []), do: candidate
  defp completed(candidate, rest), do: Path.expand(Path.join(rest), candidate)

  # the canonical form of an existing path: every component must exist (Prepare.Scope's walk)
  defp walk(_parts, _acc, 0), do: {:error, :unreadable}
  defp walk([], acc, _links), do: {:ok, acc}
  defp walk(["/" | rest], _acc, links), do: walk(rest, "/", links)
  defp walk(["." | rest], acc, links), do: walk(rest, acc, links)
  defp walk([".." | rest], acc, links), do: walk(rest, parent(acc), links)

  defp walk([part | rest], acc, links) do
    candidate = Path.join(acc, part)

    case File.lstat(candidate) do
      {:ok, %File.Stat{type: :symlink}} -> walk_link(candidate, rest, acc, links)
      {:ok, %File.Stat{type: :directory}} -> walk(rest, candidate, links)
      {:ok, _present} when rest == [] -> {:ok, candidate}
      {:ok, _not_a_directory} -> {:error, :enoent}
      {:error, :enoent} -> {:error, :enoent}
      {:error, _unreadable} -> {:error, :unreadable}
    end
  end

  defp walk_link(candidate, rest, acc, links) do
    case File.read_link(candidate) do
      {:ok, "/" <> _target = link} -> walk(Enum.reject(Path.split(link), &(&1 == "/")) ++ rest, "/", links - 1)
      {:ok, link} -> walk(Path.split(link) ++ rest, acc, links - 1)
      {:error, _unreadable} -> {:error, :unreadable}
    end
  end

  defp parent("/"), do: "/"
  defp parent(path), do: Path.dirname(path)
end
