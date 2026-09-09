defmodule AiOrchestrator.CLI do
  @moduledoc false

  use Boundary,
    deps: [
      AiOrchestrator.Commands,
      AiOrchestrator.Config,
      AiOrchestrator.Id,
      AiOrchestrator.Journal,
      AiOrchestrator.PaneRegistry,
      AiOrchestrator.Prepare,
      AiOrchestrator.Projection,
      AiOrchestrator.Run,
      AiOrchestrator.Spec
    ],
    exports: []

  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Prepare.Prepared
  alias AiOrchestrator.Prepare.Trusted
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary
  alias AiOrchestrator.Run

  @runs_root Path.join([".ai-orchestrator", "runs"])

  @type result :: %{
          required(:status) => non_neg_integer(),
          required(:stdout) => String.t(),
          required(:stderr) => String.t()
        }

  @spec main([String.t()]) :: no_return()
  def main(argv) do
    %{status: status, stdout: stdout, stderr: stderr} = run(argv)

    IO.write(stdout)

    if stderr != "" do
      IO.write(:stderr, stderr)
    end

    System.halt(status)
  end

  @spec run([String.t()], keyword()) :: result()
  def run(argv, opts \\ [])

  def run(["validate", run_dir], _opts), do: validate_run_dir(run_dir)

  def run(["run" | args], opts) do
    case gate_guardian_flag(args, opts) do
      {["--resume", run_dir], opts} when binary_part(run_dir, 0, 1) != "-" -> resume_run_dir(run_dir, opts)
      {[run_dir], opts} when binary_part(run_dir, 0, 1) != "-" -> run_run_dir(run_dir, opts)
      _other -> error(64, %{"reason" => "usage", "usage" => usage()})
    end
  end

  def run(["status", "--json", run_dir], _opts), do: status_json(run_dir)
  def run(["status", run_dir], _opts), do: status_org(run_dir)
  def run(["list", "--json"], opts), do: list_json(opts)
  def run(["list"], opts), do: list_org(opts)
  def run(["cancel", run_dir], opts), do: cancel_run_dir(run_dir, opts)
  def run(_argv, _opts), do: error(64, %{"reason" => "usage", "usage" => usage()})

  # `--gate-guardian <absolute path>` names the gate guardian for this invocation (the flag
  # outranks the environment and the config file in RuntimeConfig); it is accepted once, first
  defp gate_guardian_flag(["--gate-guardian", path | rest], opts) when is_binary(path),
    do: {rest, Keyword.put(opts, :gate_guardian, path)}

  defp gate_guardian_flag(args, opts), do: {args, opts}

  defp validate_run_dir(run_dir) do
    case Trusted.validate(run_dir) do
      {:ok, _inputs} -> ok("valid\n")
      {:error, %{} = reason} -> error(65, reason)
    end
  end

  # Every lifecycle verb enters through Commands.invoke/4 with Run.Executor (NS-43) via the SHARED trusted
  # preparation (AiOrchestrator.Prepare.Trusted): it reads the inputs, hashes the exact bytes it consumed, resolves
  # the run identity, claims panes, and hands the executor its CONTEXT (never command arguments). The Writer, its
  # lock and the journal are owned inside the run subtree; there is no filesystem preflight here. The CLI's outcome
  # (fold, projection files, close surfacing) runs INSIDE the claimed operation, before the claims are released.
  defp run_run_dir(run_dir, opts) do
    case Trusted.start(run_dir, opts) do
      {:ok, prepared} -> invoke_prepared(prepared)
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  defp resume_run_dir(run_dir, opts) do
    case Trusted.resume(run_dir, opts) do
      {:ok, prepared} -> invoke_prepared(prepared)
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  defp cancel_run_dir(run_dir, opts) do
    case Trusted.cancel(run_dir, opts) do
      {:ok, prepared} -> invoke_prepared(prepared)
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  # the operator actor is authorization, not authentication: its id is the CLI's existing operator value
  defp invoke_prepared(prepared) do
    actor = %{"class" => "operator", "id" => Keyword.get(Prepared.context(prepared), :operator, "operator")}

    case Trusted.invoke(actor, prepared, Run.Executor, &command_outcome(&1, Prepared.run_dir(prepared))) do
      {:ok, result} -> result
      {:error, %{reason: reason}} -> error(70, reason)
    end
  end

  defp command_outcome(result, run_dir) do
    with {:ok, result} <- result,
         {:ok, state} <- state_from_events(result.events),
         :ok <- write_projections(run_dir, state) do
      surface_close(ok(RunSummary.render(state)), Map.get(result, :close, :ok))
    else
      {:error, %{} = reason} -> error(70, command_rejection(reason))
    end
  end

  # a command's own closed clauses are reported by name; everything else is a journal rejection
  @command_clauses ~w(command_verb_unsupported command_stamp_invalid command_context_invalid command_inputs_mismatch
                      command_run_mismatch command_superseded command_continuation_unsupported acceptance_ambiguous
                      idempotency_conflict run_server_down run_executor_down run_executor_teardown_incomplete
                      run_discovery_failed run_supervisor_start_failed run_config_invalid run_inputs_missing)

  defp command_rejection(%{clause: clause} = rejection) when clause in @command_clauses do
    rejection
    |> Map.delete(:clause)
    |> Map.new(fn {key, value} -> {to_string(key), value} end)
    |> Map.put("reason", clause)
  end

  defp command_rejection(%{clause: _} = rejection), do: journal_rejection(rejection)
  defp command_rejection(%{} = reason), do: reason

  defp status_json(run_dir) do
    case load_state(run_dir) do
      {:ok, state} -> json(0, Fold.summary(state), :stdout)
      {:error, reason} -> error(66, reason)
    end
  end

  defp status_org(run_dir) do
    case load_state(run_dir) do
      {:ok, state} -> ok(RunSummary.render(state))
      {:error, reason} -> error(66, reason)
    end
  end

  defp list_json(opts) do
    case list_entries(opts) do
      {:ok, entries} -> json(0, entries, :stdout)
      {:error, reason} -> error(74, reason)
    end
  end

  defp list_org(opts) do
    case list_entries(opts) do
      {:ok, entries} -> ok(render_list(entries))
      {:error, reason} -> error(74, reason)
    end
  end

  defp list_entries(opts) do
    root = runs_root(opts)

    case File.ls(root) do
      {:ok, names} ->
        entries =
          names
          |> Enum.sort()
          |> Enum.flat_map(&list_entry(root, &1))

        {:ok, entries}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, %{"reason" => "runs_root_read_failed", "detail" => inspect(reason)}}
    end
  end

  defp list_entry(root, name) do
    run_dir = Path.join(root, name)

    if File.dir?(run_dir) do
      [
        case load_state(run_dir) do
          {:ok, state} -> state |> Fold.summary() |> Map.put("run_ref", name)
          {:error, reason} -> %{"run_ref" => name, "status" => "invalid", "error" => reason}
        end
      ]
    else
      []
    end
  end

  defp load_state(run_dir) do
    with {:ok, lines} <- read_journal_lines(run_dir) do
      Fold.fold_lines(lines)
    end
  end

  defp surface_close(result, :ok), do: result

  defp surface_close(%{status: 0, stdout: stdout}, {:error, rejection}) do
    %{status: 70, stdout: stdout, stderr: Jason.encode!(journal_rejection(rejection)) <> "\n"}
  end

  defp surface_close(result, {:error, _rejection}), do: result

  defp read_journal_lines(run_dir), do: Trusted.read_journal_lines(run_dir)

  defp journal_rejection(rejection), do: Trusted.journal_rejection(rejection)

  defp write_projections(run_dir, state) do
    with :ok <- write_file(run_dir, "run-summary.org", RunSummary.render(state)) do
      write_file(run_dir, "run-context.org", RunContext.render(state))
    end
  end

  defp write_file(run_dir, file, contents) do
    with :ok <- File.mkdir_p(run_dir),
         :ok <- File.write(Path.join(run_dir, file), contents) do
      :ok
    else
      {:error, reason} -> {:error, %{"reason" => "output_write_failed", "file" => file, "detail" => inspect(reason)}}
    end
  end

  defp state_from_events(events) do
    events
    |> Enum.map(&Jason.encode!/1)
    |> Fold.fold_lines()
  end

  defp runs_root(opts) do
    opts
    |> Keyword.get(:cwd, File.cwd!())
    |> Path.join(@runs_root)
  end

  defp render_list([]), do: "#+title: Runs\n\n* Runs\n- none\n"

  defp render_list(entries) do
    rows =
      Enum.map(entries, fn entry ->
        "| #{entry["run_ref"]} | #{entry["run_id"] || "-"} | #{entry["status"]} | #{entry["last_seq"] || "-"} |"
      end)

    (["#+title: Runs", "", "* Runs", "| Ref | Run | Status | Seq |", "|-----+-----+--------+-----|"] ++ rows)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp ok(stdout), do: %{status: 0, stdout: stdout, stderr: ""}

  defp json(status, data, stream) do
    output = Jason.encode!(data) <> "\n"

    case stream do
      :stdout -> %{status: status, stdout: output, stderr: ""}
      :stderr -> %{status: status, stdout: "", stderr: output}
    end
  end

  defp error(status, reason), do: json(status, normalize_rejection(reason), :stderr)

  defp normalize_rejection(%{} = reason), do: Trusted.normalize_rejection(reason)

  defp usage do
    """
    usage: ai-orchestrator validate <run-dir>
           ai-orchestrator run [--gate-guardian <path>] <run-dir>
           ai-orchestrator run [--gate-guardian <path>] --resume <run-dir>
           ai-orchestrator status [--json] <run-dir>
           ai-orchestrator list [--json]
           ai-orchestrator cancel <run-dir>
    """
  end
end
