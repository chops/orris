defmodule AiOrchestrator.CLI.Watch do
  @moduledoc """
  The read loop behind `status --watch` and `list --root --watch`.

  Each cycle runs EXACTLY the one-shot read the same verb runs without the flag, and the loop renders only when
  that rendering CHANGES. It is the invoking CLI process's own loop: no GenServer, no supervised worker, no
  `Process.send_after/3` and no timer owned by a run, so nothing survives the process and nothing can outlive a
  run it observes. It never writes, repairs or advances a receipt: `Journal.Reader.load/2` is read-only.

  The loop ends cleanly on a terminal recorded status, on the bounded `--for-ms` horizon, after a bounded cycle
  count, or when the operator interrupts the foreground process: the runtime exits on the signal itself (the
  packaged escript answers 130, measured by `test/contracts/production_escript_test.exs` PE-7, which also holds
  the run directory byte-identical across the interrupt). No signal handler is installed here, and none is
  needed, because the loop holds no lock, owns no timer and writes nothing. A read failure is rendered as the
  verb's own error and the loop continues, so a torn tail or a vanished receipt during a live run does not end
  the watch.
  """

  alias AiOrchestrator.CLI

  @default_interval_ms 1_000
  @max_interval_ms 3_600_000
  @max_for_ms 86_400_000

  @type cycle :: %{stdout: String.t(), stderr: String.t(), terminal?: boolean()}
  @type parsed :: %{
          json?: boolean(),
          watch?: boolean(),
          interval_ms: pos_integer() | nil,
          for_ms: non_neg_integer() | nil,
          root: String.t() | nil,
          positional: [String.t()]
        }

  @doc """
  Parses the flags the watch forms accept (`--json`, `--watch`, `--interval-ms N`, `--for-ms N`, `--root R`)
  and collects the positional arguments. Every flag is accepted at most once; anything else is `:usage`.
  """
  @spec arguments([String.t()]) :: {:ok, parsed()} | :usage
  def arguments(args) do
    arguments(args, %{json?: false, watch?: false, interval_ms: nil, for_ms: nil, root: nil, positional: []})
  end

  defp arguments([], acc), do: {:ok, acc}
  defp arguments(["--json" | rest], %{json?: false} = acc), do: arguments(rest, %{acc | json?: true})
  defp arguments(["--watch" | rest], %{watch?: false} = acc), do: arguments(rest, %{acc | watch?: true})

  defp arguments(["--interval-ms", value | rest], %{interval_ms: nil} = acc) do
    with_bounded(value, 1, @max_interval_ms, rest, acc, :interval_ms)
  end

  defp arguments(["--for-ms", value | rest], %{for_ms: nil} = acc) do
    with_bounded(value, 0, @max_for_ms, rest, acc, :for_ms)
  end

  defp arguments(["--root", root | rest], %{root: nil} = acc) do
    if root != "" and not String.starts_with?(root, "-"),
      do: arguments(rest, %{acc | root: root}),
      else: :usage
  end

  defp arguments([value | rest], acc) do
    if value != "" and not String.starts_with?(value, "-"),
      do: arguments(rest, %{acc | positional: acc.positional ++ [value]}),
      else: :usage
  end

  defp with_bounded(value, low, high, rest, acc, key) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= low and parsed <= high -> arguments(rest, Map.put(acc, key, parsed))
      _other -> :usage
    end
  end

  @doc """
  Runs `read` once per cycle until the loop ends, writing a cycle's rendering only when it differs from the
  one before it. Answers the CLI result of the loop itself: the renderings went to the writers as they were
  produced, so a watch streams instead of buffering.
  """
  @spec run((-> cycle()), parsed(), keyword()) :: CLI.result()
  def run(read, parsed, opts) when is_function(read, 0) do
    settings = settings(parsed, opts)
    cycle(read, settings, %{last: :none, count: 1, started: settings.now.()})
  end

  defp settings(parsed, opts) do
    %{
      interval_ms: parsed.interval_ms || Keyword.get(opts, :watch_interval_ms, @default_interval_ms),
      for_ms: parsed.for_ms,
      max_cycles: Keyword.get(opts, :watch_max_cycles),
      sleep: Keyword.get(opts, :watch_sleep, &Process.sleep/1),
      writer: Keyword.get(opts, :watch_writer, &IO.write/1),
      error_writer: Keyword.get(opts, :watch_error_writer, &IO.write(:stderr, &1)),
      now: Keyword.get(opts, :watch_clock, fn -> System.monotonic_time(:millisecond) end)
    }
  end

  defp cycle(read, settings, state) do
    observed = read.()
    state = emit(observed, settings, state)

    if observed.terminal? or done?(settings, state) do
      %{status: 0, stdout: "", stderr: ""}
    else
      settings.sleep.(settings.interval_ms)
      cycle(read, settings, %{state | count: state.count + 1})
    end
  end

  # the rendered value is the pair, so an error repeated every cycle is reported once, not once per cycle
  defp emit(observed, settings, state) do
    current = {observed.stdout, observed.stderr}

    if current == state.last do
      state
    else
      if observed.stdout != "", do: settings.writer.(observed.stdout)
      if observed.stderr != "", do: settings.error_writer.(observed.stderr)
      %{state | last: current}
    end
  end

  defp done?(settings, state) do
    cycles_done?(settings.max_cycles, state.count) or horizon_done?(settings, state)
  end

  defp cycles_done?(nil, _count), do: false
  defp cycles_done?(max, count), do: count >= max

  defp horizon_done?(%{for_ms: nil}, _state), do: false
  defp horizon_done?(%{for_ms: for_ms} = settings, state), do: settings.now.() - state.started >= for_ms
end
