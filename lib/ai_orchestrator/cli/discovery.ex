defmodule AiOrchestrator.CLI.Discovery do
  @moduledoc """
  One-shot explicit-root discovery through Query. The directory listing chooses
  membership; each displayed row comes from one subsequent verified summary.
  This is journal observation, with no host calls, cache or repair execution.
  """

  alias AiOrchestrator.Query

  @spec run([String.t()], keyword()) :: AiOrchestrator.CLI.result() | :usage
  def run(args, opts) do
    case arguments(args, nil, false) do
      {:ok, root, json?} -> discover(root, json?, opts)
      :usage -> :usage
    end
  end

  defp arguments([], root, json?) when is_binary(root), do: {:ok, root, json?}
  defp arguments(["--json" | rest], root, false), do: arguments(rest, root, true)

  defp arguments(["--root", root | rest], nil, json?) do
    if root != "" and not String.starts_with?(root, "-"),
      do: arguments(rest, root, json?),
      else: :usage
  end

  defp arguments(_args, _root, _json?), do: :usage

  defp discover(root, json?, opts) do
    # Do not lexically collapse symlink/.. components before Query walks them.
    root = if Path.type(root) == :absolute, do: root, else: Path.join(Keyword.get(opts, :cwd, File.cwd!()), root)
    query_opts = opts |> Keyword.take([:fs]) |> Keyword.put(:root, root)

    case list_runs(root, query_opts) do
      {:ok, %{runs: runs, skipped_outside_root: skipped}} ->
        if Enum.all?(runs, &String.valid?(&1.run_ref)) do
          view = %{
            "runs" => runs |> Enum.map(&row(&1.run_ref, query_opts)) |> Enum.sort_by(& &1["run_ref"]),
            "skipped_outside_root" => skipped
          }

          %{status: 0, stdout: if(json?, do: Jason.encode!(view) <> "\n", else: render(view)), stderr: ""}
        else
          root_error("runs_root_unavailable")
        end

      {:error, %{clause: clause}} when clause in ["runs_root_missing", "runs_root_unreadable"] ->
        root_error(clause)

      _unavailable ->
        root_error("runs_root_unavailable")
    end
  rescue
    _ -> root_error("runs_root_unavailable")
  catch
    _, _ -> root_error("runs_root_unavailable")
  end

  defp list_runs(root, opts) do
    # File.ls/1 uses list_dir/1, which silently omits raw filenames on Linux.
    # Refuse these before Query can omit them and log their raw bytes.
    case :file.list_dir_all(root) do
      {:ok, names} ->
        if Enum.all?(names, &String.valid?(IO.chardata_to_string(&1))),
          do: Query.list_runs(opts),
          else: {:error, %{clause: "runs_root_unavailable"}}

      {:error, _reason} ->
        # Preserve Query's root classification, but never accept a listing
        # whose lossless observation failed.
        case Query.list_runs(opts) do
          {:error, _rejection} = error -> error
          _unexpected_success -> {:error, %{clause: "runs_root_unavailable"}}
        end
    end
  end

  defp row(ref, opts) do
    case Query.run_summary(ref, opts) do
      {:ok, summary} ->
        %{
          "run_ref" => ref,
          "run_id" => summary.run_id,
          "status" => summary.status,
          "last_seq" => summary.last_seq,
          "pending_repair" => not is_nil(summary.pending_repair),
          "error" => nil
        }

      {:error, %{clause: clause}} when clause in ["journal_missing", "journal_empty", "journal_invalid"] ->
        invalid(ref, clause)

      _unavailable ->
        invalid(ref, "run_unavailable")
    end
  rescue
    _ -> invalid(ref, "run_unavailable")
  catch
    _, _ -> invalid(ref, "run_unavailable")
  end

  defp invalid(ref, reason),
    do: %{
      "run_ref" => ref,
      "run_id" => nil,
      "status" => "invalid",
      "last_seq" => nil,
      "pending_repair" => nil,
      "error" => reason
    }

  defp root_error(reason), do: %{status: 74, stdout: "", stderr: Jason.encode!(%{"reason" => reason}) <> "\n"}

  defp render(view) do
    rows =
      Enum.map(view["runs"], fn row ->
        cells = Enum.map(~w(run_ref run_id status last_seq pending_repair error), &cell(row[&1]))
        "| " <> Enum.join(cells, " | ") <> " |"
      end)

    [
      "#+title: Runs in explicit root",
      "",
      "Recorded status comes from the journal; it does not establish process liveness.",
      "",
      "| Ref | Run | Recorded status | Verified seq | Repair needed | Read error |",
      "|-----+-----+-----------------+--------------+---------------+------------|",
      rows,
      if(rows == [], do: "- none", else: []),
      "",
      "- Entries outside the root skipped :: #{view["skipped_outside_root"]}"
    ]
    |> List.flatten()
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp cell(nil), do: "-"
  defp cell(true), do: "yes"
  defp cell(false), do: "no"

  defp cell(value) do
    value
    |> to_string()
    |> String.to_charlist()
    |> Enum.map_join(fn char ->
      if char < 32 or char in 127..159 or char in [?[, ?], ?|, ?\\, 0x2028, 0x2029] or char in 0x202A..0x202E or
           char in 0x2066..0x2069 do
        "\\u" <> String.pad_leading(Integer.to_string(char, 16), 4, "0")
      else
        <<char::utf8>>
      end
    end)
  end
end
