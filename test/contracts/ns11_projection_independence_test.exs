defmodule AiOrchestrator.Contracts.NS11ProjectionIndependenceTest do
  @moduledoc """
  NS-11.H.001 / NS-11.K.001 negative control: "SQL or Ash state required for execution fails. Execution and
  resume must work with the optional analytical projector absent."

  The closure half is bound to a MEASURED oracle, not to the absence of a `deps` line: the runtime application
  closure the compiled application declares (the `applications` key `docs/contracts/production-escript.org`
  PE-2 reads out of the packaged `.app`) and Mix's own production dependency graph (the PE-4a oracle,
  `mix deps.tree --only prod`). That is deliberately a different oracle from NS-13.B.001's
  `test/contracts/dependency_prohibition_test.exs`, which reads what mix.exs DECLARES, what the two locks
  RESOLVE and what the delivered sources NAME: declaring nothing and starting nothing are separate claims, and
  this row makes the second one. The execution half re-asserts under this row's own name that every operator
  read and a command still work with both projection files absent.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Query
  alias AiOrchestrator.Test.ConsoleSeamRows, as: Rows

  @moduletag timeout: 300_000

  @projections ["run-summary.org", "run-context.org"]
  # a stored-state technology is named by one of these segments of an application name; the segment rule keeps
  # an unrelated application whose name merely contains the letters (for example `ssl_verify_fun`) out of it
  @stored_state_segments ~w(ash ecto oban sql sqlite postgrex myxql tds mnesia)
  # one application that must be in every measured closure, so an empty or unparsed closure cannot pass
  @witness :jason

  setup do
    base = Path.join(System.tmp_dir!(), "orris-ns11-#{System.unique_integer([:positive, :monotonic])}")
    File.mkdir_p!(base)
    on_exit(fn -> File.rm_rf!(base) end)
    %{base: base}
  end

  test "NS-11 the declared runtime application closure carries no SQL, Ash or Oban application" do
    applications = Application.spec(:ai_orchestrator, :applications)

    assert @witness in applications, "the measured runtime closure is empty or unreadable: #{inspect(applications)}"
    assert stored_state(applications) == []
  end

  test "NS-11 Mix's own production dependency graph carries no SQL, Ash or Oban application" do
    graph = production_graph()

    assert @witness in graph, "the measured production graph is empty or unparsed: #{inspect(graph)}"
    assert stored_state(graph) == []
  end

  test "NS-11 control: the same predicate names a stored-state application in a closure that carries one" do
    assert stored_state([:jason, :telemetry]) == []
    assert stored_state([:jason, :ash_postgres, :ecto_sql, :oban]) == [:ash_postgres, :ecto_sql, :oban]
    # the segment rule is precise: an application whose name merely contains the letters is not named
    assert stored_state([:ssl_verify_fun, :tls_certificate_check, :chatterbox, :hpack]) == []
  end

  test "NS-11 every operator read and a command work with the optional projections absent", %{base: base} do
    root = Path.join([base, ".ai-orchestrator", "runs"])
    dir = fixture(Path.join(root, "alpha"))
    refute Enum.any?(@projections, &File.exists?(Path.join(dir, &1)))

    reads = %{
      status: CLI.run(["status", "--json", dir]),
      replay: CLI.run(["replay", dir, "--json"]),
      list_root: CLI.run(["list", "--root", root, "--json"]),
      list_legacy: CLI.run(["list", "--json"], cwd: base),
      summary: Query.run_summary("alpha", root: root),
      context: Query.run_context("alpha", root: root)
    }

    assert %{status: 0, stderr: ""} = reads.status
    assert Jason.decode!(reads.status.stdout) == F.json("scenarios", "gated_run_seed", "expected.json")
    assert {:ok, %{status: "completed", pending_repair: nil}} = reads.summary
    assert {:ok, %{rendered: rendered}} = reads.context
    assert rendered =~ "#+title:"

    # a command executes with no projection present, appends the journal first, and only then writes them
    cancel = Path.join(base, "cancel")
    File.mkdir_p!(cancel)
    File.write!(Path.join(cancel, "events.jsonl"), Rows.kill9("events_pre_dispatch.jsonl"))
    assert %{status: 0, stdout: out, stderr: ""} = CLI.run(["cancel", cancel])
    assert out =~ "* Status: cancelled"
    assert "run_cancel_requested" in Rows.event_kinds(cancel)

    # deleting them again leaves every read byte-identical: no read consults a derived file
    for file <- @projections, do: File.rm!(Path.join(cancel, file))
    assert %{status: 0, stdout: ^out, stderr: ""} = CLI.run(["status", cancel])
    assert CLI.run(["status", "--json", dir]) == reads.status
    assert CLI.run(["replay", dir, "--json"]) == reads.replay
    assert CLI.run(["list", "--root", root, "--json"]) == reads.list_root
    assert CLI.run(["list", "--json"], cwd: base) == reads.list_legacy
  end

  # ---- helpers ----

  defp stored_state(applications), do: applications |> Enum.filter(&stored_state?/1) |> Enum.sort()

  defp stored_state?(app) do
    app |> Atom.to_string() |> String.split("_") |> Enum.any?(&(&1 in @stored_state_segments))
  end

  # Mix's own production dependency graph, read through the CLI exactly as production_escript_test.exs does
  defp production_graph do
    {out, 0} = System.cmd("mix", ["deps.tree", "--only", "prod", "--format", "plain"], stderr_to_stdout: true)

    for line <- String.split(out, "\n"),
        [_, app] <- [Regex.run(~r/^[\s|`-]*-- ([a-z][a-z0-9_]*) /, line)],
        uniq: true,
        do: String.to_atom(app)
  end

  defp fixture(dir) do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(F.lines("scenarios", "gated_run_seed"), "\n") <> "\n")
    dir
  end
end
