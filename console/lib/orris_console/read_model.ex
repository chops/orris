defmodule OrrisConsole.ReadModel do
  @moduledoc """
  The only path from the console to the core (C1-06/10): the session's root id is mapped to the configured directory
  and exactly the public Query functions are called with server-only options. Answers safe view data or one of
  :not_found | :invalid | :unavailable; nested core rejections are sanitized; no path, clause or inspected failure
  reaches a page.
  """
  alias AiOrchestrator.Query
  alias OrrisConsole.Config

  @type error :: :not_found | :invalid | :unavailable

  @spec valid_ref?(term()) :: boolean()
  def valid_ref?(ref) when is_binary(ref),
    do:
      ref != "" and ref not in [".", ".."] and byte_size(ref) <= 255 and not String.contains?(ref, ["/", "\\"]) and
        String.printable?(ref)

  def valid_ref?(_), do: false

  @spec list(Config.t(), map(), term()) :: {:ok, map()} | {:error, error()}
  def list(%Config{} = config, session, root_id) do
    with {:ok, root} <- root(config, session, root_id) do
      case safely(fn -> Query.list_runs(opts(config, root)) end) do
        {:ok, %{runs: runs, skipped_outside_root: skipped}} ->
          {:ok, %{root_id: root_id, runs: Enum.map(runs, &entry/1), skipped: skipped}}

        _ ->
          {:error, :unavailable}
      end
    end
  end

  @spec summary(Config.t(), map(), term(), term()) :: {:ok, map()} | {:error, error()}
  def summary(%Config{} = config, session, root_id, run_ref) do
    with {:ok, root} <- root(config, session, root_id),
         :ok <- ref(run_ref),
         {:ok, summary} <- core(fn -> Query.run_summary(run_ref, opts(config, root)) end) do
      host =
        case safely(fn -> Query.host_view(run_ref, opts(config, root)) end) do
          {:ok, view} ->
            Map.take(view, [:registered, :live, :generation, :mounted_phase, :other_registered_directories, :errors])

          _ ->
            %{
              registered: :unknown,
              live: :unknown,
              generation: :unknown,
              mounted_phase: :unknown,
              other_registered_directories: :unknown,
              errors: []
            }
        end

      {:ok,
       summary
       |> Map.take([:run_ref, :run_id, :status, :last_seq, :summary, :rendered, :pending_repair])
       |> Map.put(:host, host)}
    end
  end

  @spec context(Config.t(), map(), term(), term()) :: {:ok, map()} | {:error, error()}
  def context(%Config{} = config, session, root_id, run_ref) do
    with {:ok, root} <- root(config, session, root_id),
         :ok <- ref(run_ref),
         {:ok, context} <- core(fn -> Query.run_context(run_ref, opts(config, root)) end) do
      {:ok, Map.take(context, [:run_ref, :rendered, :pending_repair])}
    end
  end

  defp root(config, session, root_id) do
    allowed = Map.get(session, :root_ids) || Map.get(session, "root_ids") || []

    if is_binary(root_id) and root_id in allowed and Map.has_key?(config.roots, root_id),
      do: {:ok, Map.fetch!(config.roots, root_id)},
      else: {:error, :not_found}
  end

  defp ref(run_ref), do: if(valid_ref?(run_ref), do: :ok, else: {:error, :invalid})

  defp opts(config, root), do: Keyword.merge(config.query_opts, root: root)

  defp entry(run),
    do: %{
      run_ref: run.run_ref,
      run_id: run.run_id,
      status: run.status,
      last_seq: run.last_seq,
      error: if(is_nil(run.error), do: nil, else: :unavailable)
    }

  defp core(fun) do
    case safely(fun) do
      {:ok, value} ->
        {:ok, value}

      {:error, %{clause: clause}} when clause in ["run_directory_missing", "run_ref_outside_root"] ->
        {:error, :not_found}

      {:error, %{clause: clause}} when clause in ["run_ref_invalid", "run_directory_invalid"] ->
        {:error, :invalid}

      _ ->
        {:error, :unavailable}
    end
  end

  defp safely(fun) do
    fun.()
  rescue
    _ -> {:error, :unavailable}
  catch
    _, _ -> {:error, :unavailable}
  end
end
