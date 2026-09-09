defmodule AiOrchestrator.Query do
  @moduledoc """
  The PUBLIC read seam (docs/contracts/public-console-seam.org): projection-derived views over a run handle under
  a server-configured root. Every read uses the journal's verified prefix through `AiOrchestrator.Journal.Reader`,
  exposes `pending_repair` as data and never writes; host observations run under ONE finite budget applied after
  filesystem resolution and the verified read, and every fact the host cannot answer is `:unknown`, never inferred.
  Views carry no pids, references or other directories' paths.
  """

  use Boundary,
    deps: [AiOrchestrator.Host, AiOrchestrator.Journal, AiOrchestrator.Prepare, AiOrchestrator.Projection],
    exports: []

  alias AiOrchestrator.Host
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Prepare.Scope
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary

  @default_budget_ms 1_000

  @type rejection :: %{clause: String.t(), detail: map() | nil}

  @spec resolve(term(), keyword()) :: {:ok, Path.t()} | {:error, rejection()}
  defdelegate resolve(run_ref, server_opts), to: Scope

  @doc "The runs under the configured root; entries escaping the root are skipped and counted."
  @spec list_runs(keyword()) :: {:ok, %{runs: [map()], skipped_outside_root: non_neg_integer()}} | {:error, rejection()}
  def list_runs(server_opts) do
    with {:ok, root} <- Scope.canonical_root(server_opts),
         {:ok, names} <- list_root(root) do
      {runs, skipped} = Enum.reduce(Enum.sort(names), {[], 0}, &classify_entry(&1, root, server_opts, &2))
      {:ok, %{runs: Enum.reverse(runs), skipped_outside_root: skipped}}
    end
  end

  defp list_root(root) do
    case File.ls(root) do
      {:ok, names} -> {:ok, names}
      {:error, reason} -> {:error, %{clause: "runs_root_unreadable", detail: %{detail: inspect(reason)}}}
    end
  end

  # a non-directory is skipped silently; a directory escaping the canonical root is skipped and counted
  defp classify_entry(name, root, server_opts, {runs, skipped}) do
    dir = Path.join(root, name)

    cond do
      not File.dir?(dir) -> {runs, skipped}
      not Scope.inside?(dir, root) -> {runs, skipped + 1}
      true -> {[entry(name, dir, server_opts) | runs], skipped}
    end
  end

  @doc "The verified-prefix summary of a run and its rendered summary projection."
  @spec run_summary(term(), keyword()) :: {:ok, map()} | {:error, rejection()}
  def run_summary(run_ref, server_opts) do
    with {:ok, run_dir} <- Scope.resolve(run_ref, server_opts),
         {:ok, loaded, state} <- verified_state(run_dir, server_opts) do
      summary = Fold.summary(state)

      {:ok,
       %{
         run_ref: run_ref,
         run_id: summary["run_id"],
         status: summary["status"],
         last_seq: summary["last_seq"],
         summary: summary,
         rendered: RunSummary.render(state),
         pending_repair: loaded.pending_repair
       }}
    end
  end

  @doc "The rendered context projection of a run."
  @spec run_context(term(), keyword()) :: {:ok, map()} | {:error, rejection()}
  def run_context(run_ref, server_opts) do
    with {:ok, run_dir} <- Scope.resolve(run_ref, server_opts),
         {:ok, loaded, state} <- verified_state(run_dir, server_opts) do
      {:ok, %{run_ref: run_ref, rendered: RunContext.render(state), pending_repair: loaded.pending_repair}}
    end
  end

  @doc """
  Host observations for a run under ONE budget (`budget_ms`, default #{@default_budget_ms}) taken after the
  verified journal read: registration/liveness/generation (Host.status), the count of OTHER registered directories
  under the same root carrying this run id (Host.lookup_run_id, this directory excluded explicitly), and the mounted
  owner phase. Unanswered legs are `:unknown` with an entry in `errors`.
  """
  @spec host_view(term(), keyword()) :: {:ok, map()} | {:error, rejection()}
  def host_view(run_ref, server_opts) do
    with {:ok, run_dir} <- Scope.resolve(run_ref, server_opts),
         {:ok, root} <- Scope.canonical_root(server_opts) do
      {run_id, journal_error} =
        case verified_state(run_dir, server_opts) do
          {:ok, _loaded, state} -> {Fold.summary(state)["run_id"], nil}
          {:error, %{clause: clause}} -> {nil, %{leg: :lookup, clause: clause}}
        end

      deadline = System.monotonic_time(:millisecond) + Keyword.get(server_opts, :budget_ms, @default_budget_ms)
      remaining = fn -> max(deadline - System.monotonic_time(:millisecond), 1) end
      host_opts = Keyword.take(server_opts, [:monitor, :ownership])

      {status, status_errors} = status_leg(run_dir, host_opts, remaining)
      {count, lookup_errors} = lookup_leg(run_id, journal_error, run_dir, root, host_opts, remaining)
      {phase, mounted_errors} = mounted_leg(run_dir, server_opts, remaining)

      {:ok,
       Map.merge(status, %{
         run_ref: run_ref,
         run_id: run_id,
         other_registered_directories: count,
         mounted_phase: phase,
         errors: status_errors ++ lookup_errors ++ mounted_errors
       })}
    end
  end

  # ---- legs ----

  defp status_leg(run_dir, host_opts, remaining) do
    case Host.status(run_dir, Keyword.put(host_opts, :timeout, remaining.())) do
      {:ok, %{registered: false}} ->
        {%{registered: false, live: false, generation: nil}, []}

      {:ok, %{registered: true, live: true, generation: generation}} ->
        {%{registered: true, live: true, generation: generation}, []}

      {:ok, %{clause: clause} = inconsistent} ->
        {%{registered: true, live: :unknown, generation: Map.get(inconsistent, :generation, :unknown)},
         [%{leg: :status, clause: clause}]}

      {:error, %{clause: clause}} ->
        {%{registered: :unknown, live: :unknown, generation: :unknown}, [%{leg: :status, clause: clause}]}
    end
  end

  defp lookup_leg(nil, journal_error, _run_dir, _root, _host_opts, _remaining), do: {:unknown, [journal_error]}

  defp lookup_leg(run_id, _journal_error, run_dir, root, host_opts, remaining) do
    own = Path.expand(run_dir)

    case Host.lookup_run_id(run_id, Keyword.put(host_opts, :timeout, remaining.())) do
      {:ok, entries} ->
        count =
          Enum.count(entries, fn entry ->
            dir = Path.expand(entry.run_dir)
            dir != own and String.starts_with?(dir <> "/", root <> "/")
          end)

        {count, []}

      {:error, %{clause: clause}} ->
        {:unknown, [%{leg: :lookup, clause: clause}]}
    end
  end

  defp mounted_leg(run_dir, server_opts, remaining) do
    host = %{supervisor: Keyword.get(server_opts, :host_supervisor, Host.Supervisor)}
    own = Path.expand(run_dir)

    case Host.mounted(host, remaining.()) do
      {:ok, views} ->
        case Enum.find(views, &(is_binary(&1.run_dir) and Path.expand(&1.run_dir) == own)) do
          nil -> {:unknown, []}
          view -> {view.phase, []}
        end

      {:error, %{clause: clause}} ->
        {:unknown, [%{leg: :mounted, clause: clause}]}
    end
  end

  # ---- verified journal read (never writes) ----

  defp verified_state(run_dir, server_opts) do
    reader_opts = Keyword.take(server_opts, [:fs])

    case Reader.load(run_dir, reader_opts) do
      {:ok, %{lines: []}} ->
        {:error, %{clause: "journal_empty", detail: nil}}

      {:ok, %{lines: lines} = loaded} ->
        case Fold.fold_lines(lines) do
          {:ok, state} -> {:ok, loaded, state}
          {:error, rejection} -> {:error, %{clause: "journal_invalid", detail: rejection}}
        end

      {:error, %{clause: "journal_missing"} = rejection} ->
        {:error, %{clause: "journal_missing", detail: rejection}}

      {:error, rejection} ->
        {:error, %{clause: "journal_invalid", detail: rejection}}
    end
  end

  defp entry(name, dir, server_opts) do
    case verified_state(dir, server_opts) do
      {:ok, _loaded, state} ->
        summary = Fold.summary(state)
        %{run_ref: name, run_id: summary["run_id"], status: summary["status"], last_seq: summary["last_seq"], error: nil}

      {:error, rejection} ->
        %{run_ref: name, run_id: nil, status: "invalid", last_seq: nil, error: rejection}
    end
  end
end
