defmodule AiOrchestrator.Prepare.Trusted do
  @moduledoc """
  The TRUSTED, directory-based tier of command preparation: the CLI's admission rules moved here verbatim so the
  CLI and the public seam share ONE implementation (docs/contracts/public-console-seam.org, R1/A2).

  `run_dir` is any absolute or relative directory, exactly as `AiOrchestrator.CLI.run/2` accepts it; nothing is
  resolved against a root here. Every reason map is returned UNCHANGED (the CLI keeps its own exit codes and
  rendering). Inputs are read with `File.read` (not an injected Fs); pane claims are taken around ONE claimed
  operation whose outcome callback runs while the claim is held, then released under the legacy rescue/catch
  semantics; a primary failure and a release failure keep their legacy precedence (release failure last).
  """

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Config.Runtime, as: RuntimeConfig
  alias AiOrchestrator.Id.SystemId
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Prepare.Prepared
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Spec.RunSpec

  @journal_file "events.jsonl"
  @verbs ["start", "resume", "cancel"]

  @type inputs :: %{spec: map(), plan: map(), spec_hash: String.t(), plan_hash: String.t()}
  @type reason :: map()
  @type outcome :: (term() -> term())

  @doc "The verbs the built-in executor runs (exactly `Run.Executor`'s)."
  @spec supported_verbs() :: [String.t()]
  def supported_verbs, do: @verbs

  @doc "Reads, decodes and validates spec.json/plan.json, hashing the exact bytes consumed (the CLI's validate)."
  @spec validate(Path.t()) :: {:ok, inputs()} | {:error, reason()}
  def validate(run_dir), do: read_run_inputs(run_dir)

  @doc "The CLI's `run`: inputs, server options, fresh identity."
  @spec start(Path.t(), keyword()) :: {:ok, Prepared.t()} | {:error, reason()}
  def start(run_dir, opts) do
    with {:ok, inputs} <- read_run_inputs(run_dir),
         {:ok, fsm_opts} <- options(run_dir, opts),
         {:ok, fsm_opts} <- fresh_identity(fsm_opts) do
      args = %{"spec_hash" => inputs.spec_hash, "plan_hash" => inputs.plan_hash}
      {:ok, prepared("start", args, run_dir, fsm_opts, inputs)}
    end
  end

  @doc "The CLI's `run --resume`: inputs, options, prior journal lines, provenance, resumed identity; an empty prior journal requests an explicit restart."
  @spec resume(Path.t(), keyword()) :: {:ok, Prepared.t()} | {:error, reason()}
  def resume(run_dir, opts) do
    with {:ok, inputs} <- read_run_inputs(run_dir),
         {:ok, fsm_opts} <- options(run_dir, opts),
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

      {:ok, prepared(verb, args, run_dir, fsm_opts, inputs)}
    end
  end

  @doc "The CLI's `cancel`: options, prior journal lines, resumed identity; no pane claims."
  @spec cancel(Path.t(), keyword()) :: {:ok, Prepared.t()} | {:error, reason()}
  def cancel(run_dir, opts) do
    with {:ok, fsm_opts} <- options(run_dir, opts),
         {:ok, prior_lines} <- read_journal_lines(run_dir),
         {:ok, fsm_opts} <- resume_identity(prior_lines, fsm_opts) do
      args = %{"reason" => Keyword.get(fsm_opts, :cancel_reason, "operator_cancel")}
      # nil inputs already builds the term without pane claims; the opaque term is never updated outside its module
      {:ok, prepared("cancel", args, run_dir, fsm_opts, nil)}
    end
  end

  @doc """
  ONE claimed operation: the pane claims (when the verb has inputs) are taken, `Commands.invoke/4` runs with the
  given executor, `outcome.(commands_result)` runs WHILE THE CLAIM IS HELD, then the claim is released. Answers
  `{:ok, outcome_value}`, `{:error, %{stage: :claim, reason: map}}` before any execution when the registry refuses,
  or `{:error, %{stage: :release, reason: map}}` after the outcome when the release fails (legacy precedence).
  """
  @spec invoke(map(), Prepared.t(), module(), outcome()) ::
          {:ok, term()} | {:error, %{stage: :claim | :release, reason: reason()}}
  def invoke(actor, prepared, executor, outcome) when is_atom(executor) and is_function(outcome, 1) do
    run = fn claimed_opts -> outcome.(commands_invoke(actor, prepared, executor, claimed_opts)) end

    case Prepared.claims(prepared) do
      :none -> {:ok, run.(Prepared.context(prepared))}
      {:panes, spec} -> with_pane_claims(spec, Prepared.run_dir(prepared), Prepared.context(prepared), run)
    end
  end

  @doc "The CLI's server-side options for a run directory (Config.Runtime-derived seams); never request data."
  @spec options(Path.t(), keyword()) :: {:ok, keyword()} | {:error, reason()}
  def options(run_dir, opts) do
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

  @doc "The verified, non-empty prior journal lines of a directory (the CLI's read)."
  @spec read_journal_lines(Path.t()) :: {:ok, [binary()]} | {:error, reason()}
  def read_journal_lines(run_dir) do
    case Reader.load(run_dir) do
      {:ok, %{lines: lines}} -> nonempty_lines(lines)
      {:error, %{clause: "journal_missing"}} -> {:error, %{"reason" => "journal_not_found", "file" => @journal_file}}
      {:error, rejection} -> {:error, journal_rejection(rejection)}
    end
  end

  @doc "A rejection that already names an operator-facing reason keeps it; otherwise the clause under the journal_ prefix."
  @spec journal_rejection(map()) :: map()
  def journal_rejection(%{clause: clause} = rejection) do
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

  @doc "String-keyed rejection map."
  @spec normalize_rejection(map()) :: map()
  def normalize_rejection(%{} = reason) do
    Map.new(reason, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  # ---- extraction of the CLI's private rules (cli.ex at 2c33d78), behaviour unchanged ----

  defp prepared(verb, args, run_dir, fsm_opts, inputs) do
    Prepared.new(%{
      verb: verb,
      args: args,
      run_id: Keyword.fetch!(fsm_opts, :run_id),
      run_dir: run_dir,
      context: fsm_opts |> Keyword.put(:run_dir, run_dir) |> Keyword.merge(input_context(inputs)),
      claims: if(is_nil(inputs), do: :none, else: {:panes, inputs.spec}),
      inputs: if(is_nil(inputs), do: nil, else: Map.take(inputs, [:spec_hash, :plan_hash]))
    })
  end

  defp commands_invoke(actor, prepared, executor, claimed_opts) do
    Commands.invoke(actor, Prepared.verb(prepared), Prepared.args(prepared),
      run_id: Prepared.run_id(prepared),
      executor: executor,
      executor_opts: claimed_opts
    )
  end

  defp input_context(nil), do: []

  defp input_context(inputs),
    do: [spec: inputs.spec, plan: inputs.plan, spec_hash: inputs.spec_hash, plan_hash: inputs.plan_hash]

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
           %{"reason" => "input_provenance_mismatch", "file" => file, "expected" => recorded, "actual" => current[file]}}
        end

      conflicting ->
        {:error, %{"reason" => "journal_provenance_conflict", "file" => file, "recorded" => Enum.sort(conflicting)}}
    end
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  defp nonempty_lines([]), do: {:error, %{"reason" => "journal_empty", "file" => @journal_file}}
  defp nonempty_lines(lines), do: {:ok, lines}

  defp read_resume_journal_lines(run_dir) do
    case read_journal_lines(run_dir) do
      {:error, %{"reason" => "journal_empty"}} -> {:ok, []}
      result -> result
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
        {:error, %{stage: :claim, reason: reason}}
    end
  end

  defp run_with_claim(registry, claim, claimed_opts, fun) do
    result = fun.(claimed_opts)

    case registry.release(claim) do
      :ok -> {:ok, result}
      {:error, reason} -> {:error, %{stage: :release, reason: reason}}
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
end
