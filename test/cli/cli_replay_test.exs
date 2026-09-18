defmodule AiOrchestrator.CLIReplayTest do
  @moduledoc """
  S3 (`replay <run-dir> [--json] [--to-seq N]`): the operator affordance for the fold NS-28.H.001's replay leg
  already exercises. With no `--to-seq` it is provably the `status` answer; with `--to-seq N` it answers the
  run's recorded state as of sequence N, folded from the SAME verified prefix. It reads only the journal, never
  a projection, and writes nothing.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold

  @projections ["run-summary.org", "run-context.org"]
  @canary "PRIVATE_REPLAY_INDEX_CANARY"

  setup do
    base = Path.join(System.tmp_dir!(), "orris-cli-replay-#{System.unique_integer([:positive, :monotonic])}")
    File.mkdir_p!(base)
    on_exit(fn -> File.rm_rf!(base) end)
    %{base: base}
  end

  test "R1 replay without --to-seq is the status answer, in both renderings", %{base: base} do
    dir = journal(Path.join(base, "seed"), F.lines("scenarios", "gated_run_seed"))

    assert CLI.run(["replay", dir, "--json"]) == CLI.run(["status", "--json", dir])
    assert CLI.run(["replay", dir]) == CLI.run(["status", dir])
    assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["replay", dir, "--json"])
    assert Jason.decode!(json) == F.json("scenarios", "gated_run_seed", "expected.json")
  end

  test "R2 each --to-seq N reproduces the fold of the first N recorded lines", %{base: base} do
    lines = F.lines("scenarios", "gated_run_seed")
    dir = journal(Path.join(base, "prefixes"), lines)

    for n <- [1, 2, 7, 18, 31, 32] do
      {:ok, state} = lines |> Enum.take(n) |> Fold.fold_lines()
      assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["replay", dir, "--json", "--to-seq", Integer.to_string(n)])
      assert Jason.decode!(json) == Jason.decode!(Jason.encode!(Fold.summary(state)))
      assert Jason.decode!(json)["last_seq"] == n
    end

    # the full prefix and the whole journal are the same answer
    assert CLI.run(["replay", dir, "--json", "--to-seq", "32"]) == CLI.run(["replay", dir, "--json"])
    assert Jason.decode!(CLI.run(["replay", dir, "--json", "--to-seq", "31"]).stdout)["status"] != "completed"
  end

  test "R3 a sequence beyond the verified prefix is refused and nothing is written", %{base: base} do
    dir = journal(Path.join(base, "range"), F.lines("scenarios", "gated_run_seed"))
    before = File.read!(Path.join(dir, "events.jsonl"))

    assert %{status: 66, stdout: "", stderr: stderr} = CLI.run(["replay", dir, "--to-seq", "33"])

    assert Jason.decode!(stderr) == %{
             "reason" => "replay_seq_out_of_range",
             "to_seq" => 33,
             "last_seq" => 32
           }

    assert File.read!(Path.join(dir, "events.jsonl")) == before
    refute Enum.any?(@projections, &File.exists?(Path.join(dir, &1)))
    refute File.exists?(Path.join(dir, "events.head"))
  end

  test "R4 a deleted or lagging projection changes no replay answer", %{base: base} do
    dir = journal(Path.join(base, "index"), F.lines("scenarios", "gated_run_seed"))
    current = replays(dir)

    for file <- @projections, do: File.write!(Path.join(dir, file), "#{@canary}\n* Status: failed\n")
    assert replays(dir) == current
    for file <- @projections, do: File.rm!(Path.join(dir, file))
    assert replays(dir) == current
    refute inspect(current, limit: :infinity, printable_limit: :infinity) =~ @canary
  end

  test "R5 a torn tail replays its verified prefix and names the pending repair", %{base: base} do
    dir = journal(Path.join(base, "torn"), F.lines("scenarios", "gated_run_seed"))
    File.write!(Path.join(dir, "events.jsonl"), ~s({"schema":"ai-orch), [:append])

    assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["replay", dir, "--json"])
    assert Jason.decode!(json)["last_seq"] == 32
    assert Jason.decode!(json)["pending_repair"]["action"] == "truncate_tail"
    # a prefix of a torn journal still reports the plan for the WHOLE file; the prefix is not a repair
    assert %{status: 0, stdout: partial, stderr: ""} = CLI.run(["replay", dir, "--json", "--to-seq", "5"])
    assert Jason.decode!(partial)["last_seq"] == 5
    assert Jason.decode!(partial)["pending_repair"]["truncate_bytes"] == 18
  end

  test "R6 malformed replay argv is refused, and an unreadable journal keeps the status verb's failure", %{base: base} do
    dir = journal(Path.join(base, "argv"), F.lines("scenarios", "gated_run_seed"))

    for args <- [
          ["replay"],
          ["replay", dir, dir],
          ["replay", dir, "--to-seq"],
          ["replay", dir, "--to-seq", "0"],
          ["replay", dir, "--to-seq", "two"],
          ["replay", dir, "--to-seq", "1", "--to-seq", "2"],
          ["replay", dir, "--json", "--json"],
          ["replay", dir, "--unknown"]
        ] do
      assert %{status: 64, stdout: "", stderr: stderr} = CLI.run(args)
      assert Jason.decode!(stderr)["reason"] == "usage"
    end

    bad = Path.join(base, "bad")
    File.mkdir_p!(bad)
    File.write!(Path.join(bad, "events.jsonl"), "not-json\n")
    assert %{status: 66, stdout: "", stderr: stderr} = CLI.run(["replay", bad, "--json"])
    assert Jason.decode!(stderr)["reason"] == "journal_undecodable_line"
  end

  defp replays(dir) do
    %{
      whole: CLI.run(["replay", dir, "--json"]),
      org: CLI.run(["replay", dir]),
      prefix: CLI.run(["replay", dir, "--json", "--to-seq", "12"])
    }
  end

  defp journal(dir, lines) do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    dir
  end
end
