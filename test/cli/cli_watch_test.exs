defmodule AiOrchestrator.CLIWatchTest do
  @moduledoc """
  S2 (`status --watch`, `list --root --watch`): the fourth acceptance leg of NS-28.H.001, "delete index then
  replay/list/watch/status". Each cycle re-runs exactly the one-shot read the same verb runs; the loop renders
  only on change, never writes, never repairs, holds no lock and ends on a terminal recorded status, on the
  bounded horizon or after the bounded cycle count. The loop is driven here with an injected sleeper, clock and
  writers, so no test waits on wall-clock time.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Chain

  @projections ["run-summary.org", "run-context.org"]
  @canary "PRIVATE_WATCH_INDEX_CANARY"

  setup do
    base = Path.join(System.tmp_dir!(), "orris-cli-watch-#{System.unique_integer([:positive, :monotonic])}")
    File.mkdir_p!(base)
    on_exit(fn -> File.rm_rf!(base) end)
    %{base: base}
  end

  test "W1 the watch renders the successive status values as the journal grows and stops on the terminal status",
       %{base: base} do
    lines = F.lines("scenarios", "gated_run_seed")
    {prefix, suffix} = Enum.split(lines, 30)
    dir = journal(Path.join(base, "growing"), prefix)
    collector = collector()

    # each sleep appends the next recorded line, so cycle N reads a journal one event longer than cycle N-1
    appender = fn _ms ->
      case Agent.get_and_update(collector, &next_pending/1) do
        nil -> :ok
        line -> File.write!(Path.join(dir, "events.jsonl"), line <> "\n", [:append])
      end
    end

    Agent.update(collector, &Map.put(&1, :pending, suffix))

    assert %{status: 0, stdout: "", stderr: ""} =
             CLI.run(["status", "--json", "--watch", dir], watch_opts(collector, appender, 12))

    rendered = Enum.map(stdout(collector), &Jason.decode!/1)
    assert Enum.map(rendered, & &1["last_seq"]) == [30, 31, 32]
    assert List.last(rendered)["status"] == "completed"
    # the loop ended on the terminal status, not on the cycle bound: fewer renders than the 12 cycles allowed
    assert stderr(collector) == []
  end

  test "W2 an unchanged journal renders once, and the deleted or lagging projections change nothing", %{base: base} do
    dir = journal(Path.join(base, "still"), F.lines("scenarios", "auth_blocked_pane"))
    for file <- @projections, do: File.write!(Path.join(dir, file), "#{@canary}\n* Status: failed\n")
    collector = collector()

    # blocked is NOT terminal: the loop runs its full bound while the recorded value stays the same
    lagging = fn _ms -> :ok end

    assert %{status: 0, stdout: "", stderr: ""} =
             CLI.run(["status", "--watch", dir], watch_opts(collector, lagging, 4))

    assert length(stdout(collector)) == 1
    assert hd(stdout(collector)) =~ "* Status: BLOCKED"
    refute hd(stdout(collector)) =~ @canary

    # with the projections DELETED the same four cycles produce the identical single rendering
    deleted = collector()
    for file <- @projections, do: File.rm!(Path.join(dir, file))
    assert %{status: 0} = CLI.run(["status", "--watch", dir], watch_opts(deleted, lagging, 4))
    assert stdout(deleted) == stdout(collector)
    refute Enum.any?(@projections, &File.exists?(Path.join(dir, &1)))
  end

  test "W3 a read failure is reported once and the loop keeps running and reading", %{base: base} do
    dir = chained(Path.join(base, "receipt"), "auth_blocked_pane")
    receipt = Path.join(dir, "events.head")
    kept = File.read!(receipt)
    bytes = File.read!(Path.join(dir, "events.jsonl"))
    collector = collector()

    # cycle 1 reads cleanly, the receipt vanishes before cycle 2, and is restored before cycle 4
    steps = fn _ms ->
      case Agent.get_and_update(collector, fn state -> {state.cycle, %{state | cycle: state.cycle + 1}} end) do
        1 -> File.rm!(receipt)
        3 -> File.write!(receipt, kept)
        _other -> :ok
      end
    end

    assert %{status: 0, stdout: "", stderr: ""} = CLI.run(["status", "--watch", dir], watch_opts(collector, steps, 5))

    assert [first, restored] = stdout(collector)
    assert first == restored
    assert first =~ "* Status: BLOCKED"
    assert [failure] = stderr(collector)
    assert Jason.decode!(failure)["reason"] == "journal_receipt_missing"
    # nothing was rebuilt or rewritten by the watch: the writer, not the reader, repairs
    assert File.read!(Path.join(dir, "events.jsonl")) == bytes
  end

  test "W4 list --root --watch re-reads the explicit root and ends on the bounded horizon", %{base: base} do
    root = Path.join(base, "root")
    journal(Path.join(root, "alpha"), F.lines("scenarios", "gated_run_seed"))
    collector = collector()
    appears = fn _ms -> journal(Path.join(root, "beta"), F.lines("scenarios", "auth_blocked_pane")) end

    assert %{status: 0, stdout: "", stderr: ""} =
             CLI.run(["list", "--root", root, "--json", "--watch"], watch_opts(collector, appears, 6))

    listings = Enum.map(stdout(collector), &Jason.decode!/1)
    assert Enum.map(listings, fn view -> Enum.map(view["runs"], & &1["run_ref"]) end) == [["alpha"], ["alpha", "beta"]]
    assert Enum.all?(listings, fn view -> view["skipped_outside_root"] == 0 end)
  end

  test "W5 the bounded --for-ms horizon ends the loop without a terminal status", %{base: base} do
    dir = journal(Path.join(base, "horizon"), F.lines("scenarios", "auth_blocked_pane"))
    collector = collector()
    ticking = fn ms -> Agent.update(collector, fn state -> %{state | clock: state.clock + ms} end) end

    opts =
      collector
      |> watch_opts(ticking, 1_000)
      |> Keyword.put(:watch_clock, fn -> Agent.get(collector, & &1.clock) end)

    assert %{status: 0, stdout: "", stderr: ""} =
             CLI.run(["status", "--watch", "--interval-ms", "50", "--for-ms", "120", dir], opts)

    # 0, 50 and 100 ms are inside the horizon; the fourth reading of the clock is 150 and ends the loop
    assert Agent.get(collector, & &1.clock) == 150
    assert length(stdout(collector)) == 1
  end

  test "W6 malformed watch argv is refused before any read", %{base: base} do
    dir = journal(Path.join(base, "argv"), F.lines("scenarios", "gated_run_seed"))

    # `["status", "--watch"]` is NOT in this table: the pre-existing two-argument clause reads any second
    # argument as a run directory, and this slice does not change what the verb already accepted
    for args <- [
          ["status", "--watch", "--json"],
          ["status", "--watch", "--watch", dir],
          ["status", "--watch", dir, dir],
          ["status", "--watch", "--interval-ms", dir],
          ["status", "--watch", "--interval-ms", "0", dir],
          ["status", "--watch", "--for-ms", "-1", dir],
          ["status", "--watch", "--unknown", dir],
          ["status", "--json", "--json", "--watch", dir],
          ["status", "--watch", "--root", dir]
        ] do
      assert %{status: 64, stdout: "", stderr: stderr} = CLI.run(args)
      assert Jason.decode!(stderr)["reason"] == "usage"
    end

    assert %{status: 64, stdout: "", stderr: stderr} = CLI.run(["list", "--watch"])
    assert Jason.decode!(stderr)["reason"] == "usage"
  end

  # ---- helpers ----

  defp collector do
    {:ok, agent} = Agent.start_link(fn -> %{stdout: [], stderr: [], pending: [], cycle: 1, clock: 0} end)
    agent
  end

  defp next_pending(%{pending: []} = state), do: {nil, state}
  defp next_pending(%{pending: [line | rest]} = state), do: {line, %{state | pending: rest}}

  defp stdout(agent), do: agent |> Agent.get(& &1.stdout) |> Enum.reverse()
  defp stderr(agent), do: agent |> Agent.get(& &1.stderr) |> Enum.reverse()

  defp watch_opts(agent, sleep, max_cycles) do
    [
      watch_max_cycles: max_cycles,
      watch_sleep: sleep,
      watch_writer: fn text -> Agent.update(agent, fn s -> %{s | stdout: [text | s.stdout]} end) end,
      watch_error_writer: fn text -> Agent.update(agent, fn s -> %{s | stderr: [text | s.stderr]} end) end
    ]
  end

  defp journal(dir, lines) do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    dir
  end

  # a schema_version 2 journal with its chained hashes and head receipt (as test/cli/cli_discovery_test.exs)
  defp chained(dir, name) do
    File.mkdir_p!(dir)

    {lines, hash} =
      Enum.map_reduce(F.lines("scenarios", name), Chain.anchor(), fn line, prior ->
        bytes =
          line |> Jason.decode!() |> Map.put("schema_version", 2) |> Map.put("prev_line_sha256", prior) |> Jason.encode!()

        {bytes, Chain.line_sha256(bytes <> "\n")}
      end)

    File.write!(Path.join(dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")

    File.write!(
      Path.join(dir, "events.head"),
      Chain.encode_receipt(%{seq: length(lines), line_sha256: hash, updated_at: "2026-01-01T00:01:00Z"})
    )

    dir
  end
end
