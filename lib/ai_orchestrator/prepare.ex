defmodule AiOrchestrator.Prepare do
  @moduledoc """
  The PUBLIC mutation seam (docs/contracts/public-console-seam.org): every function takes a run handle (`run_ref`,
  one directory name under the server-configured `root`) and server-only options, resolves the handle through
  `AiOrchestrator.Prepare.Scope`, and delegates to the trusted tier the CLI uses. `invoke/3` executes with the
  core's built-in foreground executor (`AiOrchestrator.Run.Executor`), chosen here, never by a caller.

  The public request is exactly a verb and a handle: no argument document is accepted. Rejections are
  `%{clause: String.t(), detail: map() | nil}` with the trusted tier's original reason map carried in `detail`.
  """

  use Boundary,
    deps: [
      AiOrchestrator.Commands,
      AiOrchestrator.Config,
      AiOrchestrator.Id,
      AiOrchestrator.Journal,
      AiOrchestrator.PaneRegistry,
      AiOrchestrator.Run,
      AiOrchestrator.Spec
    ],
    exports: [Prepared, Scope, Trusted]

  alias AiOrchestrator.Prepare.Prepared
  alias AiOrchestrator.Prepare.Scope
  alias AiOrchestrator.Prepare.Trusted
  alias AiOrchestrator.Run

  @type rejection :: %{clause: String.t(), detail: map() | nil}

  @doc "The verbs the built-in executor runs."
  @spec supported_verbs() :: [String.t()]
  def supported_verbs, do: Trusted.supported_verbs()

  @doc "Whether `verb` is one of the supported verbs."
  @spec verb(term()) :: {:ok, String.t()} | {:error, rejection()}
  def verb(verb) when is_binary(verb) do
    if verb in Trusted.supported_verbs(),
      do: {:ok, verb},
      else: {:error, %{clause: "command_verb_unsupported", detail: %{verb: verb}}}
  end

  def verb(_verb), do: {:error, %{clause: "command_verb_unsupported", detail: nil}}

  @doc "Validates the run's inputs; answers the two input digests only."
  @spec validate(term(), keyword()) :: {:ok, %{spec_hash: String.t(), plan_hash: String.t()}} | {:error, rejection()}
  def validate(run_ref, server_opts) do
    with {:ok, run_dir} <- Scope.resolve(run_ref, server_opts),
         {:ok, inputs} <- reject(Trusted.validate(run_dir)) do
      {:ok, Map.take(inputs, [:spec_hash, :plan_hash])}
    end
  end

  @spec start(term(), keyword()) :: {:ok, Prepared.t()} | {:error, rejection()}
  def start(run_ref, server_opts), do: prepare(run_ref, server_opts, &Trusted.start/2)

  @spec resume(term(), keyword()) :: {:ok, Prepared.t()} | {:error, rejection()}
  def resume(run_ref, server_opts), do: prepare(run_ref, server_opts, &Trusted.resume/2)

  @spec cancel(term(), keyword()) :: {:ok, Prepared.t()} | {:error, rejection()}
  def cancel(run_ref, server_opts), do: prepare(run_ref, server_opts, &Trusted.cancel/2)

  @doc "Executes an admitted command with the built-in foreground executor; pane claims are taken around it."
  @spec invoke(map(), Prepared.t(), keyword()) :: {:ok, %{events: [map()], close: term()}} | {:error, rejection()}
  def invoke(actor, %Prepared{} = prepared, _server_opts) do
    case Trusted.invoke(actor, prepared, Run.Executor, & &1) do
      {:ok, {:ok, result}} -> {:ok, %{events: Map.get(result, :events, []), close: Map.get(result, :close, :ok)}}
      {:ok, {:error, rejection}} -> {:error, public_rejection(rejection)}
      {:error, %{stage: :claim, reason: reason}} -> {:error, %{clause: "pane_claim_refused", detail: reason}}
      {:error, %{stage: :release, reason: reason}} -> {:error, %{clause: "pane_release_failed", detail: reason}}
    end
  end

  def invoke(_actor, _prepared, _server_opts), do: {:error, %{clause: "invalid_prepared", detail: nil}}

  defp prepare(run_ref, server_opts, trusted) do
    with {:ok, run_dir} <- Scope.resolve(run_ref, server_opts) do
      reject(trusted.(run_dir, server_opts))
    end
  end

  defp reject({:ok, value}), do: {:ok, value}
  defp reject({:error, reason}), do: {:error, public_rejection(reason)}

  # the lossless closed mapping (A3): the trusted reason map is carried untouched in detail
  defp public_rejection(%{clause: clause} = rejection) when is_binary(clause), do: %{clause: clause, detail: rejection}

  defp public_rejection(%{"reason" => reason} = detail) do
    clause =
      case reason do
        "file_not_found" -> "run_inputs_missing"
        "file_read_failed" -> "run_inputs_invalid"
        "invalid_json" -> "run_inputs_invalid"
        "journal_not_found" -> "journal_missing"
        "journal_empty" -> "journal_empty"
        other when is_binary(other) -> other
        _ -> "run_inputs_invalid"
      end

    %{clause: clause, detail: detail}
  end

  defp public_rejection(%{} = detail), do: %{clause: "run_inputs_invalid", detail: detail}
end
