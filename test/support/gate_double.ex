defmodule AiOrchestrator.Test.GateDouble do
  @moduledoc """
  Deterministic gate executor for the scenario harness.

  The orchestrated route runs unchanged (prepare -> gate_started v2 -> receipt -> Ack -> one release ->
  await -> terminal) against fixed identities; only the gate's OUTCOME is scripted, by a legacy
  `gate_runner`-style function carried as `gate_opts[:runner]` (arity 1: gate; arity 2: gate,
  gate_opts), so every scenario keeps the pass/fail story it was written with. `ack/2` is the REAL
  checked constructor.
  """

  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Gate.Execution.Ack
  alias AiOrchestrator.Journal.Event

  @hash "sha256:" <> String.duplicate("ab", 32)
  @start "1756728000.123456"
  @zero "sha256:" <> String.duplicate("0", 64)

  @doc "An executable regular file: the Host's helper presence check is satisfied without a guardian."
  def helper, do: System.find_executable("true")

  @doc """
  An in-memory journal receipt: every appendable event comes back stamped the way the Writer stamps
  it. A gate releases only against such a receipt (a nil or bare `:ok` sink never releases).
  """
  def receipt_sink, do: receipt(fn _event -> :ok end)

  @doc """
  Wrap a recording sink: an answer of `:ok`, or an unstamped `{:ok, event}`, becomes a stamped receipt
  for the event the sink saw; every other answer (rejections, garbage) passes through unchanged.
  """
  def receipt(sink) when is_function(sink, 1) do
    fn event ->
      case sink.(event) do
        :ok -> stamp(event)
        {:ok, %{"prev_line_sha256" => _}} = stamped -> stamped
        {:ok, %{} = seen} -> stamp(seen)
        other -> other
      end
    end
  end

  defp stamp(event) do
    case Event.validate_append(event) do
      {:ok, _} -> {:ok, Map.merge(event, %{"schema_version" => 2, "prev_line_sha256" => @zero})}
      {:error, rejection} -> {:error, rejection}
    end
  end

  def prepare(_fs, request, _opts) do
    started = %{
      "gate_run_id" => request.gate_run_id,
      "command_argv" => request.command_argv,
      "stdout_path" => "gates/#{request.gate_run_id}.#{request.attempt}.out",
      "stderr_path" => "gates/#{request.gate_run_id}.#{request.attempt}.err",
      "attempt" => request.attempt,
      "deadline_unix" => request.deadline_unix,
      "execution" => %{"pid" => 4242, "pgid" => 4242, "start" => @start, "claim_hash" => @hash}
    }

    {:ok,
     %{
       started_data: started,
       identity: %{guardian: 4241, worker: 4242, pgid: 4242, start: @start},
       request: request
     }}
  end

  def started_data(%{started_data: data}), do: data
  def identity(%{identity: identity}), do: identity

  def ack(prepared, persisted), do: Execution.ack(prepared, persisted)

  def release(prepared, %Ack{}, _opts \\ []), do: {:ok, Map.put(prepared, :released, true)}

  def abandon(_handle), do: :ok

  def expire(_handle), do: {:timeout, %{kind: "timeout", settled: true, leftovers: "0", proof: "gone", duration_ms: 1}}

  # the legacy script decides the outcome; a settled exit carries the script's evidence fields
  def await(%{request: req}, opts \\ []) do
    gate = %{
      "gate_run_id" => req.gate_run_id,
      "command_argv" => req.command_argv,
      "attempt" => req.attempt,
      "deadline_unix" => req.deadline_unix
    }

    gate_opts = [repo_root: req.repo_root, run_dir: req.run_dir] ++ opts

    case run_script(Keyword.get(opts, :runner), gate, gate_opts) do
      {:ok, data} -> {:exit, outcome(0, data)}
      {:failed, data} -> {:exit, outcome(data["exit_status"], data)}
      # a raw term: the Host's value-domain mapper reports it as invalid_return, never its contents
      other -> {:error, other}
    end
  end

  def evidence(_run_dir, _gate_run_id, _attempt), do: {:ok, %{"stdout_hash" => @hash, "stderr_hash" => @hash}}

  def reconcile(_fs, _run_dir, _expected, _opts \\ []),
    do: {:dead, %{"leader" => "gone", "group" => "gone", "members" => 0}}

  def pass?(%{"exit_status" => 0, "settled" => true, "proof" => "gone"}), do: true
  def pass?(_), do: false

  defp run_script(nil, _gate, _opts), do: {:ok, %{}}
  defp run_script(fun, gate, _opts) when is_function(fun, 1), do: fun.(gate)
  defp run_script(fun, gate, opts) when is_function(fun, 2), do: fun.(gate, opts)

  defp outcome(status, data) do
    base = %{
      "kind" => "exited",
      "exit_status" => status,
      "settled" => true,
      "leftovers" => "0",
      "proof" => "gone",
      "escaped" => "unknown",
      "stdout_hash" => data["stdout_hash"] || @hash,
      "stderr_hash" => data["stderr_hash"] || @hash,
      "stderr_merged" => false,
      "duration_ms" => data["duration_ms"] || 12
    }

    case data["failure_summary"] do
      nil -> base
      summary -> Map.put(base, "failure_summary", summary)
    end
  end
end
