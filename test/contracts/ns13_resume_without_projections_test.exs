defmodule AiOrchestrator.Contracts.NS13ResumeWithoutProjectionsTest do
  @moduledoc """
  NS-13.B.001, "SQL/Oban never own execution or retry": a run is recovered from the journal alone, with no
  derived projection present and none consulted.

  Row 1 seeds an interrupted run (the kill9 pre-gate prefix, provenance re-anchored as `test/cli/cli_test.exs`
  does), proves BOTH projection files are absent by exact path, and resumes it through `run --resume`. The resume
  must reach a positive BLOCKED result with exactly one `run_resumed` event. The projections the command then
  writes are deleted, and every read (`status`, `status --json`, `replay --json`) must be byte-identical with and
  without them.

  Row 2 is the control: the same prefix resumed with FALSE projection content already in place. If any step of
  execution or retry read a projection, the false files would change the outcome; the event kinds and stdout must
  instead equal row 1's exactly, and the rewritten projections must not carry the false text.

  Limits, stated rather than hidden:

    * No analytical projector exists at this head, and OPEN-07 (a) forbids adding one. The acceptance half
      "Remove analytical projector and recover run" can therefore only close if a reviewer accepts it as vacuous.
      What this file proves is the executable part: recovery with the derived projections removed, and
      recovery that ignores false ones.
    * `lib/ai_orchestrator/lifecycle.ex:4-13` declares `AiOrchestrator.Projection` as a Boundary dependency of
      the lifecycle. The compile-time boundary therefore does not prove non-use; only this behavioural test does.
    * The SQL/Oban dependency half of the row is `test/contracts/dependency_prohibition_test.exs`; the NS-11
      runtime closure is `test/contracts/ns11_projection_independence_test.exs`.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Test.ConsoleSeamDoubles, as: Doubles
  alias AiOrchestrator.Test.ConsoleSeamRows, as: Rows
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.FixedId

  @moduletag timeout: 120_000

  @summary "run-summary.org"
  @context "run-context.org"
  @false_text "ZZ-NS13-FALSE-PROJECTION"

  test "NS-13.B.001 a run resumes from the journal alone: no projection present, none consulted" do
    %{dir: dir, result: result} = resume!("absent")

    assert %{status: 0, stdout: stdout, stderr: ""} = result
    assert stdout =~ "* Status: BLOCKED"
    assert Enum.count(Rows.event_kinds(dir), &(&1 == "run_resumed")) == 1

    # the command wrote both projections after journaling; the reads must not depend on them
    assert File.exists?(Path.join(dir, @summary)) and File.exists?(Path.join(dir, @context))
    with_projections = reads(dir)
    journal = File.read!(Path.join(dir, "events.jsonl"))

    File.rm!(Path.join(dir, @summary))
    File.rm!(Path.join(dir, @context))

    assert reads(dir) == with_projections
    assert File.read!(Path.join(dir, "events.jsonl")) == journal
    refute File.exists?(Path.join(dir, @summary)) or File.exists?(Path.join(dir, @context))

    # the equality above is between two SUCCESSFUL reads, pinned positively here
    assert %{status: 0, stdout: status, stderr: ""} = with_projections.status
    assert status =~ "* Status: BLOCKED"
    assert %{status: 0, stderr: ""} = with_projections.status_json
    assert %{status: 0, stderr: ""} = with_projections.replay_json
  end

  test "NS-13.B.001 control: false projections in place change nothing and are overwritten" do
    %{dir: clean_dir, result: clean} = resume!("clean")
    %{dir: false_dir, result: seeded} = resume!("false", false_projections: true)

    assert %{status: 0, stderr: ""} = clean
    assert seeded.stdout == clean.stdout
    assert seeded.status == clean.status and seeded.stderr == clean.stderr
    assert Rows.event_kinds(false_dir) == Rows.event_kinds(clean_dir)

    for file <- [@summary, @context] do
      rewritten = File.read!(Path.join(false_dir, file))
      refute rewritten =~ @false_text, "#{file} still carries the false projection text"
      assert rewritten == File.read!(Path.join(clean_dir, file)), "#{file} differs from the clean resume's"
    end
  end

  # ---- helpers ----

  # a fresh interrupted run: inputs, the re-anchored kill9 pre-gate prefix, and (optionally) false projections;
  # both projection paths are proven absent (or false) by exact path BEFORE the resume
  defp resume!(label, opts \\ []) do
    dir = Rows.fresh("ns13_#{label}")
    Rows.write_inputs(dir, "kill9_resume")
    Rows.write_journal(dir, Rows.reanchored(String.split(Rows.kill9("events_pre_gate.jsonl"), "\n", trim: true), dir))

    if Keyword.get(opts, :false_projections, false) do
      File.write!(Path.join(dir, @summary), "* Status: completed\n#{@false_text}\n")
      File.write!(Path.join(dir, @context), "- item_a :: DONE\n#{@false_text}\n")
      assert File.read!(Path.join(dir, @summary)) =~ @false_text
      assert File.read!(Path.join(dir, @context)) =~ @false_text
    else
      refute File.exists?(Path.join(dir, @summary))
      refute File.exists?(Path.join(dir, @context))
    end

    FixedId.reset()
    FixedClock.reset()
    result = CLI.run(["run", "--resume", dir], Doubles.seams(Rows.fresh("ns13_registry_#{label}"), self()))
    %{dir: dir, result: result}
  end

  defp reads(dir) do
    %{
      status: CLI.run(["status", dir]),
      status_json: CLI.run(["status", "--json", dir]),
      replay_json: CLI.run(["replay", dir, "--json"])
    }
  end
end
