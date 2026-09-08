defmodule AiOrchestrator.Gate.Runner do
  @moduledoc """
  Executes deterministic local gates from argv, preserving output artifacts.
  """

  alias AiOrchestrator.Clock.SystemClock

  @failure_line_limit 5
  @summary_byte_cap 4096
  @summary_char_limit 160

  @type result :: {:ok, map()} | {:failed, map()} | {:error, map()}

  @spec started_data(map(), keyword()) :: map()
  def started_data(%{"gate_run_id" => gate_run_id, "command_argv" => command_argv}, opts \\ []) do
    %{
      "gate_run_id" => gate_run_id,
      "command_argv" => command_argv,
      "stdout_path" => output_path(gate_run_id, "out", opts),
      "stderr_path" => output_path(gate_run_id, "err", opts)
    }
  end

  @spec run(map(), keyword()) :: result()
  def run(gate, opts \\ [])

  def run(%{"command_argv" => [executable | args]} = gate, opts) when is_binary(executable) and is_list(args) do
    if Enum.all?(args, &is_binary/1) do
      run_argv(gate, executable, args, opts)
    else
      {:error, %{"reason" => "invalid_gate_command"}}
    end
  end

  def run(_gate, _opts), do: {:error, %{"reason" => "invalid_gate_command"}}

  defp run_argv(gate, executable, args, opts) do
    started_at = monotonic_ms(opts)
    runner = Keyword.get(opts, :runner, &System.cmd/3)
    command_opts = [cd: Keyword.get(opts, :repo_root, "."), stderr_to_stdout: true]

    case runner.(executable, args, command_opts) do
      {output, exit_status} when is_binary(output) and is_integer(exit_status) ->
        result = persist_result(gate, output, exit_status, duration_ms(opts, started_at), opts)

        if exit_status == 0 do
          {:ok, result}
        else
          {:failed, Map.put(result, "failure_summary", failure_summary(output, exit_status))}
        end

      other ->
        {:error, describe_error("gate_runner_invalid_return", other)}
    end
  rescue
    exception ->
      {:error, describe_error("gate_runner_crashed", exception)}
  end

  defp persist_result(gate, output, exit_status, duration_ms, opts) do
    gate_run_id = Map.fetch!(gate, "gate_run_id")
    stdout_path = output_path(gate_run_id, "out", opts)
    stdout_abs = absolute_output_path(stdout_path, opts)

    File.mkdir_p!(Path.dirname(stdout_abs))
    File.write!(stdout_abs, output)

    %{
      "exit_status" => exit_status,
      "duration_ms" => duration_ms,
      "stdout_hash" => sha256(output),
      "stderr_merged" => true
    }
  end

  @doc """
  The bounded, designed failure evidence (headline, up to five failure lines, suggestion) for a
  gate that did not pass. `ending` is the exit status, or `{:signal, n}` for a signaled gate.
  """
  @spec failure_summary(binary(), non_neg_integer() | {:signal, pos_integer()}) :: map()
  def failure_summary(output, ending) do
    lines = output |> String.split("\n") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

    bound_failure_summary(%{
      "headline" => headline(lines, ending),
      "failures" => failure_lines(lines),
      "suggestion" => "inspect gate output and retry after fixing the failing check"
    })
  end

  defp bound_failure_summary(summary) do
    if summary |> Jason.encode!() |> byte_size() <= @summary_byte_cap do
      summary
    else
      %{
        "headline" => truncate_bytes(summary["headline"], 120),
        "failures" => [],
        "suggestion" => "inspect gate output"
      }
    end
  end

  defp headline([], {:signal, signal}), do: "gate killed by signal #{signal}"
  defp headline([], exit_status), do: "gate exited with status #{exit_status}"
  defp headline([line | _rest], _exit_status), do: truncate(line)

  defp failure_lines(lines) do
    lines
    |> Enum.filter(&failure_line?/1)
    |> Enum.take(@failure_line_limit)
    |> Enum.map(&%{"line" => truncate(&1)})
  end

  defp failure_line?(line) do
    line =~ "failed" or line =~ "FAILED" or line =~ "Error" or line =~ "error" or line =~ "Assertion"
  end

  defp output_path(gate_run_id, extension, _opts), do: "gates/#{gate_run_id}.#{extension}"

  defp absolute_output_path(path, opts) do
    opts |> Keyword.get(:run_dir, ".") |> Path.join(path)
  end

  defp duration_ms(opts, started_at) do
    Keyword.get(opts, :duration_ms, monotonic_ms(opts) - started_at)
  end

  defp monotonic_ms(opts) do
    Keyword.get(opts, :monotonic_ms, &SystemClock.monotonic_ms/0).()
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  defp describe_error(reason, value) do
    %{
      "reason" => reason,
      "result_class" => result_class(value),
      "digest" => value |> :erlang.term_to_binary() |> sha256()
    }
  end

  # Adapter-controlled atom and tuple tags are deliberately not reflected.
  defp result_class(value) when is_tuple(value), do: "tuple"
  defp result_class(value) when is_atom(value), do: "atom"
  defp result_class(value) when is_binary(value), do: "binary"
  defp result_class(value) when is_map(value), do: "map"
  defp result_class(value) when is_list(value), do: "list"
  defp result_class(value) when is_integer(value), do: "integer"
  defp result_class(_value), do: "other"

  defp truncate(text) do
    if String.length(text) > @summary_char_limit do
      String.slice(text, 0, @summary_char_limit)
    else
      text
    end
  end

  defp truncate_bytes(text, byte_limit) when byte_size(text) <= byte_limit, do: text

  defp truncate_bytes(text, byte_limit) do
    text
    |> binary_part(0, byte_limit)
    |> trim_partial_utf8()
  end

  defp trim_partial_utf8(text) do
    if String.valid?(text) do
      text
    else
      text |> binary_part(0, byte_size(text) - 1) |> trim_partial_utf8()
    end
  end
end
