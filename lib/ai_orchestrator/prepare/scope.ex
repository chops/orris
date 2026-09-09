defmodule AiOrchestrator.Prepare.Scope do
  @moduledoc """
  The ONE resolver from a public run handle (`run_ref`: a directory NAME under a server-configured root) to a run
  directory, shared by reads and mutations (docs/contracts/public-console-seam.org, F-5/F-6).

  Closed behaviour: the configured root is canonicalised (symlinks followed); an absent root answers
  `runs_root_missing`; a handle must be one non-empty printable path segment (no separators, not "." or "..",
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
    with {:ok, canonical} <- canonical(Path.expand(root)),
         true <- File.dir?(canonical) do
      {:ok, canonical}
    else
      _missing -> {:error, %{clause: "runs_root_missing", detail: %{root: root}}}
    end
  end

  @doc "Whether `path`'s canonical form lies strictly inside the canonical `root`."
  @spec inside?(Path.t(), Path.t()) :: boolean()
  def inside?(path, root) do
    case canonical(Path.expand(path)) do
      {:ok, canonical} -> String.starts_with?(canonical, root <> "/")
      :error -> false
    end
  end

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

  # symlink-resolving canonical path, component by component, bounded in link depth; :error when unresolvable
  defp canonical(path), do: canonical(Path.split(path), "/", @max_links)

  defp canonical(_parts, _acc, 0), do: :error
  defp canonical([], acc, _links), do: {:ok, acc}
  defp canonical(["/" | rest], _acc, links), do: canonical(rest, "/", links)

  defp canonical([part | rest], acc, links) do
    candidate = Path.join(acc, part)

    case File.read_link(candidate) do
      {:ok, link} ->
        resolved = if String.starts_with?(link, "/"), do: link, else: Path.join(acc, link)
        canonical(Path.split(Path.expand(resolved)) ++ rest, "/", links - 1)

      {:error, _not_a_link_or_absent} ->
        canonical(rest, candidate, links)
    end
  end
end
