defmodule AiOrchestrator.CLI do
  @moduledoc false

  use Boundary,
    deps: [
      AiOrchestrator.Commands,
      AiOrchestrator.Config,
      AiOrchestrator.Id,
      AiOrchestrator.Journal,
      AiOrchestrator.PaneRegistry,
      AiOrchestrator.Projection,
      AiOrchestrator.Run,
      AiOrchestrator.Spec
    ],
    exports: []

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Config.Runtime, as: RuntimeConfig
  alias AiOrchestrator.Id.SystemId
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary
  alias AiOrchestrator.Run
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Spec.RunSpec

  @journal_file "events.jsonl"
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
    case read_run_inputs(run_dir) do
      {:ok, _inputs} -> ok("valid\n")
      {:error, %{} = reason} -> error(65, reason)
    end
  end

  # Every lifecycle verb enters through Commands.invoke/4 with Run.Executor (NS-43): the CLI reads the inputs,
  # hashes the exact bytes it consumed, resolves the run identity, claims panes, and hands the executor its
  # CONTEXT (never command arguments). The Writer, its lock and the journal are owned inside the run subtree.
  # Absence is proven only by the Writer's exclusive create under the lock through the injected Fs (NS-43 item 4);
  # there is no filesystem preflight here. Consequences (ruled m_1788676500000): unreadable inputs and a refused pane
  # claim are reported before the Writer decides.
  defp run_run_dir(run_dir, opts) do
    with {:ok, inputs} <- read_run_inputs(run_dir),
         {:ok, fsm_opts} <- fsm_opts(run_dir, opts),
         {:ok, fsm_opts} <- fresh_identity(fsm_opts) do
      args = %{"spec_hash" => inputs.spec_hash, "plan_hash" => inputs.plan_hash}
      with_pane_claims(inputs.spec, run_dir, fsm_opts, &invoke_command("start", args, inputs, run_dir, &1))
    else
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  defp resume_run_dir(run_dir, opts) do
    with {:ok, inputs} <- read_run_inputs(run_dir),
         {:ok, fsm_opts} <- fsm_opts(run_dir, opts),
         {:ok, prior_lines} <- read_resume_journal_lines(run_dir),
         :ok <- verify_input_provenance(prior_lines, inputs),
         {:ok, fsm_opts} <- resume_identity(prior_lines, fsm_opts) do
      # a journal killed before its first event has no run to resume: the command is a fresh start
      # the preflight saw an empty journal: REQUEST the explicit restart (unit D); the locked verified prefix decides
      {verb, args, fsm_opts} =
        case prior_lines do
          [] ->
            {"start", %{"spec_hash" => inputs.spec_hash, "plan_hash" => inputs.plan_hash},
             Keyword.put(fsm_opts, :restart_empty, true)}

          _ ->
            {"resume", %{"recovery_reason" => Keyword.get(fsm_opts, :recovery_reason, "crash_recovery")}, fsm_opts}
        end

      with_pane_claims(inputs.spec, run_dir, fsm_opts, &invoke_command(verb, args, inputs, run_dir, &1))
    else
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  defp cancel_run_dir(run_dir, opts) do
    with {:ok, fsm_opts} <- fsm_opts(run_dir, opts),
         {:ok, prior_lines} <- read_journal_lines(run_dir),
         {:ok, fsm_opts} <- resume_identity(prior_lines, fsm_opts) do
      args = %{"reason" => Keyword.get(fsm_opts, :cancel_reason, "operator_cancel")}
      invoke_command("cancel", args, nil, run_dir, fsm_opts)
    else
      {:error, %{} = reason} -> error(70, reason)
    end
  end

  # the operator actor is authorization, not authentication: its id is the CLI's existing operator value
  defp invoke_command(verb, args, inputs, run_dir, fsm_opts) do
    actor = %{"class" => "operator", "id" => Keyword.get(fsm_opts, :operator, "operator")}

    context =
      fsm_opts
      |> Keyword.put(:run_dir, run_dir)
      |> Keyword.merge(input_context(inputs))

    result =
      Commands.invoke(actor, verb, args,
        run_id: Keyword.fetch!(fsm_opts, :run_id),
        executor: Run.Executor,
        executor_opts: context
      )

    with {:ok, result} <- result,
         {:ok, state} <- state_from_events(result.events),
         :ok <- write_projections(run_dir, state) do
      surface_close(ok(RunSummary.render(state)), Map.get(result, :close, :ok))
    else
      {:error, %{} = reason} -> error(70, command_rejection(reason))
    end
  end

  defp input_context(nil), do: []

  defp input_context(inputs),
    do: [spec: inputs.spec, plan: inputs.plan, spec_hash: inputs.spec_hash, plan_hash: inputs.plan_hash]

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

  defp read_run_inputs(run_dir) do
    with {:ok, spec_bytes} <- read_input_bytes(run_dir, "spec.json"),
         {:ok, spec} <- decode_input(spec_bytes, "spec.json"),
         {:ok, validated_spec} <- RunSpec.validate(spec),
         {:ok, plan_bytes} <- read_input_bytes(run_dir, "plan.json"),
         {:ok, plan} <- decode_input(plan_bytes, "plan.json"),
         {:ok, _validated_plan} <- Plan.validate(plan, validated_spec) do
      {:ok, %{spec: spec, plan: plan, spec_hash: sha256(spec_bytes), plan_hash: sha256(plan_bytes)}}
    end
  end

  defp read_input_bytes(run_dir, file) do
    case File.read(Path.join(run_dir, file)) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, :enoent} -> {:error, %{"reason" => "file_not_found", "file" => file}}
      {:error, reason} -> {:error, %{"reason" => "file_read_failed", "file" => file, "detail" => inspect(reason)}}
    end
  end

  defp decode_input(bytes, file) do
    case Jason.decode(bytes) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, %Jason.DecodeError{}} -> {:error, %{"reason" => "invalid_json", "file" => file}}
    end
  end

  @provenance_slots [
    {"run_created", "spec_hash", "spec.json"},
    {"run_spec_loaded", "spec_hash", "spec.json"},
    {"plan_recorded", "plan_hash", "plan.json"}
  ]

  defp verify_input_provenance(prior_lines, inputs) do
    with {:ok, events} <- decode_journal_events(prior_lines) do
      current = %{"spec.json" => inputs.spec_hash, "plan.json" => inputs.plan_hash}
      check_provenance_slots(events, current)
    end
  end

  defp decode_journal_events(lines) do
    lines
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {line, line_number}, {:ok, acc} ->
      case Event.validate_line(line) do
        {:ok, event} ->
          {:cont, {:ok, [event | acc]}}

        {:error, rejection} ->
          {:halt, {:error, rejection |> normalize_rejection() |> Map.put("line", line_number)}}
      end
    end)
    |> case do
      {:ok, events} -> {:ok, Enum.reverse(events)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_provenance_slots(events, current) do
    by_type = Enum.group_by(events, & &1["type"])

    with {:ok, recorded_by_file} <- collect_recorded_hashes(by_type) do
      Enum.reduce_while(recorded_by_file, :ok, &halt_on_file_mismatch(&1, &2, current))
    end
  end

  defp halt_on_file_mismatch({file, hashes}, :ok, current) do
    case check_file_provenance(file, hashes, current) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp collect_recorded_hashes(by_type) do
    Enum.reduce_while(@provenance_slots, {:ok, %{}}, fn {type, field, file}, {:ok, acc} ->
      events = Map.get(by_type, type, [])
      hashes = Enum.map(events, & &1["data"][field])

      if Enum.any?(hashes, &(not is_binary(&1))) do
        {:halt, {:error, %{"reason" => "journal_provenance_incomplete", "event_type" => type, "field" => field}}}
      else
        {:cont, {:ok, Map.update(acc, file, hashes, &(&1 ++ hashes))}}
      end
    end)
  end

  defp check_file_provenance(file, recorded_hashes, current) do
    case Enum.uniq(recorded_hashes) do
      [] ->
        :ok

      [recorded] ->
        if recorded == current[file] do
          :ok
        else
          {:error,
           %{
             "reason" => "input_provenance_mismatch",
             "file" => file,
             "expected" => recorded,
             "actual" => current[file]
           }}
        end

      conflicting ->
        {:error, %{"reason" => "journal_provenance_conflict", "file" => file, "recorded" => Enum.sort(conflicting)}}
    end
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  defp read_journal_lines(run_dir) do
    case Reader.load(run_dir) do
      {:ok, %{lines: lines}} -> nonempty_lines(lines)
      {:error, %{clause: "journal_missing"}} -> {:error, %{"reason" => "journal_not_found", "file" => @journal_file}}
      {:error, rejection} -> {:error, journal_rejection(rejection)}
    end
  end

  defp nonempty_lines([]), do: {:error, %{"reason" => "journal_empty", "file" => @journal_file}}
  defp nonempty_lines(lines), do: {:ok, lines}

  # Pre-claim read of the prior lines (provenance, identity); the writer re-verifies under the lock.
  defp read_resume_journal_lines(run_dir) do
    case read_journal_lines(run_dir) do
      {:error, %{"reason" => "journal_empty"}} -> {:ok, []}
      result -> result
    end
  end

  defp surface_close(result, :ok), do: result

  defp surface_close(%{status: 0, stdout: stdout}, {:error, rejection}) do
    %{status: 70, stdout: stdout, stderr: Jason.encode!(journal_rejection(rejection)) <> "\n"}
  end

  defp surface_close(result, {:error, _rejection}), do: result

  # A rejection that already names an operator-facing reason (Event's journal_provenance_incomplete)
  # keeps it; otherwise the reason is the clause under the journal_ prefix.
  defp journal_rejection(%{clause: clause} = rejection) do
    reason =
      cond do
        is_binary(Map.get(rejection, :reason)) -> Map.get(rejection, :reason)
        String.starts_with?(clause, "journal_") -> clause
        true -> "journal_" <> clause
      end

    rejection
    |> Map.drop([:clause, :reason])
    |> normalize_rejection()
    |> Map.merge(%{"reason" => reason, "file" => @journal_file})
  end

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

  defp fsm_opts(run_dir, opts) do
    with {:ok, config} <- resolve_runtime_config(opts) do
      base =
        opts
        |> Keyword.take([
          :agent_roster_hash,
          :clock,
          :context_initial_hash,
          :dispatch,
          :dispatch_opts,
          :fs,
          :gate_executor,
          :gate_helper,
          :gate_opts,
          :gate_runner,
          :id,
          :operator,
          :pane_registry,
          :pane_registry_opts,
          :plan_hash,
          :plan_path,
          :project,
          :recovery_reason,
          :review_reader,
          :run_id,
          :run_lock_path,
          :spec_hash,
          :spec_path,
          :supervisor_instance
        ])
        |> Keyword.put_new(:run_dir, run_dir)
        |> Keyword.put_new(:default_assignment_timeout_s, config[:default_assignment_timeout_s])
        |> Keyword.put_new(:pane_registry_root, config[:pane_registry_root])
        |> Keyword.put_new(:gate_helper, config[:gate_guardian])
        |> Keyword.update(
          :dispatch_opts,
          [ap_path: config[:ap_path], poll_interval_ms: config[:poll_interval_ms]],
          fn dispatch_opts ->
            dispatch_opts
            |> Keyword.put_new(:ap_path, config[:ap_path])
            |> Keyword.put_new(:poll_interval_ms, config[:poll_interval_ms])
          end
        )

      {:ok, base}
    end
  end

  defp resolve_runtime_config(opts) do
    opts
    |> Keyword.take([
      :env,
      :config_file,
      :file_reader,
      :ap_path,
      :poll_interval_ms,
      :default_assignment_timeout_s,
      :pane_registry_root,
      :gate_guardian
    ])
    |> RuntimeConfig.resolve()
  end

  defp runs_root(opts) do
    opts
    |> Keyword.get(:cwd, File.cwd!())
    |> Path.join(@runs_root)
  end

  defp fresh_identity(opts) do
    id = Keyword.get(opts, :id, SystemId)

    {:ok,
     opts
     |> Keyword.put_new_lazy(:run_id, &id.run_id/0)
     |> Keyword.put_new_lazy(:supervisor_instance, &id.supervisor_instance/0)}
  end

  defp resume_identity([], opts), do: fresh_identity(opts)

  defp resume_identity(prior_lines, opts) do
    id = Keyword.get(opts, :id, SystemId)

    with {:ok, state} <- Fold.fold_lines(prior_lines) do
      {:ok,
       opts
       |> Keyword.put_new(:run_id, state.run_id)
       |> Keyword.put_new_lazy(:supervisor_instance, &id.supervisor_instance/0)}
    end
  end

  defp with_pane_claims(spec, run_dir, opts, fun) do
    registry = Keyword.get(opts, :pane_registry, FileRegistry)
    pane_refs = registry.pane_refs(spec)

    owner = %{
      "run_id" => Keyword.fetch!(opts, :run_id),
      "run_dir" => Path.expand(run_dir),
      "supervisor_instance" => Keyword.fetch!(opts, :supervisor_instance)
    }

    claim_opts =
      opts
      |> Keyword.get(:pane_registry_opts, [])
      |> Keyword.put(:root, Keyword.fetch!(opts, :pane_registry_root))

    case registry.claim(pane_refs, owner, claim_opts) do
      {:ok, claim} ->
        claimed_opts = Keyword.put(opts, :pane_claim_tokens, Map.new(pane_refs, &{&1, claim.token}))
        run_with_claim(registry, claim, claimed_opts, fun)

      {:error, reason} ->
        error(70, reason)
    end
  end

  defp run_with_claim(registry, claim, claimed_opts, fun) do
    result = fun.(claimed_opts)

    case registry.release(claim) do
      :ok -> result
      {:error, reason} -> error(70, reason)
    end
  rescue
    error ->
      registry.release(claim)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      registry.release(claim)
      :erlang.raise(kind, reason, __STACKTRACE__)
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

  defp normalize_rejection(%{} = reason) do
    Map.new(reason, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

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
