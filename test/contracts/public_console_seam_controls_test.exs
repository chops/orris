defmodule AiOrchestrator.Contracts.PublicConsoleSeamControlsTest do
  @moduledoc """
  docs/contracts/public-console-seam.org, CONTROLS C-0..C-8: they hold at the base 2c33d78 and must keep holding.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.ConsoleConsumerHarness, as: Harness
  alias AiOrchestrator.Test.ConsoleSeamDoubles, as: Doubles
  alias AiOrchestrator.Test.ConsoleSeamRows, as: Rows

  @moduletag :public_console_seam
  @moduletag timeout: 600_000

  setup_all do
    dir = Harness.build!()
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, consumer: dir}
  end

  test "C-0 the consumer harness is valid: a public-only reference compiles under WAE", %{consumer: dir} do
    {exit, out} = Harness.compile(dir, "defmodule ConsoleConsumer.Probe do\n  def v, do: AiOrchestrator.version()\nend\n")
    assert exit == 0, out
  end

  for {row, module, call} <- [
        {"C-1", "AiOrchestrator.Journal.Writer", "&AiOrchestrator.Journal.Writer.start_link/1"},
        {"C-2", "AiOrchestrator.Run.Executor", "&AiOrchestrator.Run.Executor.prepare/2"},
        {"C-3", "AiOrchestrator.Host", "&AiOrchestrator.Host.stop/2"}
      ] do
    test "#{row} a private reference to #{module} is rejected under WAE with the forbidden-reference diagnostic", %{
      consumer: dir
    } do
      {exit, out} = Harness.compile(dir, "defmodule ConsoleConsumer.Probe do\n  def p, do: #{unquote(call)}\nend\n")
      assert exit == 1, out
      assert out =~ "forbidden reference to #{unquote(module)}", out
    end
  end

  test "C-4 the console actor class is admitted for the three verbs; an agent actor is refused" do
    console = %{"class" => "console", "id" => "session_abc"}
    for verb <- ~w(start resume cancel), do: assert(match?({:ok, _}, Policy.authorize(console, verb)), verb)
    agent = %{"class" => "agent", "id" => "agent_1", "run_id" => "run_x", "assignment_id" => "as_1"}
    for verb <- ~w(start resume cancel), do: assert(match?({:error, %{clause: _}}, Policy.authorize(agent, verb)), verb)
  end

  test "C-5 Reader: a torn tail loads with a truncate plan and unchanged bytes; a hard-invalid journal fails closed" do
    dir = Rows.fresh("reader")
    lines = F.lines("scenarios", "gated_run_seed")
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n" <> ~s({"schema":"ai-orch))
    before = File.read!(Path.join(dir, "events.jsonl"))
    assert {:ok, %{pending_repair: %{action: :truncate_tail, truncate_bytes: 18}, lines: verified}} = Reader.load(dir)
    assert length(verified) == length(lines)
    assert File.read!(Path.join(dir, "events.jsonl")) == before
    bad = Rows.fresh("reader_bad")
    File.write!(Path.join(bad, "events.jsonl"), "not-json\n")
    assert match?({:error, %{clause: _}}, Reader.load(bad))
  end

  test "C-6 CLI preservation: validate, status, cancel and list with absolute and relative directories" do
    project = Rows.fresh("cli_project")
    runs = Path.join([project, ".ai-orchestrator", "runs"])
    dir = Path.join(runs, "seed")
    File.mkdir_p!(dir)
    Rows.write_inputs(dir, "gated_run_seed")
    assert CLI.run(["validate", dir]) == %{status: 0, stdout: "valid\n", stderr: ""}
    relative = Path.relative_to(dir, File.cwd!())
    refute String.starts_with?(relative, "/")
    assert CLI.run(["validate", relative]) == %{status: 0, stdout: "valid\n", stderr: ""}
    completed = Path.join(runs, "completed")
    File.mkdir_p!(completed)
    Rows.write_journal(completed, F.lines("scenarios", "gated_run_seed"))
    assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["status", "--json", completed])
    assert Jason.decode!(json) == F.json("scenarios", "gated_run_seed", "expected.json")

    assert %{status: 0, stdout: ^json, stderr: ""} =
             CLI.run(["status", "--json", Path.relative_to(completed, File.cwd!())])

    assert %{status: 0, stdout: listed, stderr: ""} = CLI.run(["list", "--json"], cwd: project)
    entries = Jason.decode!(listed)
    assert Enum.map(entries, & &1["run_ref"]) == ["completed", "seed"]
    assert Enum.find(entries, &(&1["run_ref"] == "seed"))["status"] == "invalid"
    cancel = Path.join(runs, "cancel")
    File.mkdir_p!(cancel)
    File.write!(Path.join(cancel, "events.jsonl"), Rows.kill9("events_pre_dispatch.jsonl"))
    assert %{status: 0, stdout: out, stderr: ""} = CLI.run(["cancel", Path.relative_to(cancel, File.cwd!())])
    assert out =~ "* Status: cancelled"
    assert File.exists?(Path.join(cancel, "run-summary.org"))
    assert %{status: 66, stdout: "", stderr: _} = CLI.run(["status", "--json", Rows.fresh("cli_missing")])
  end

  defmodule ObservingRegistry do
    @moduledoc false
    def pane_refs(spec), do: FileRegistry.pane_refs(spec)

    def claim(pane_refs, _owner, opts) do
      {:ok,
       %{
         root: Keyword.fetch!(opts, :root),
         token: "observing",
         pane_refs: pane_refs,
         test: Keyword.fetch!(opts, :test_pid),
         run_dir: Keyword.fetch!(opts, :run_dir)
       }}
    end

    # release observes whether the outcome (projection files) already exists, then fails as the legacy double does
    def release(%{test: test, run_dir: run_dir}) do
      send(test, {:released, File.exists?(Path.join(run_dir, "run-summary.org"))})
      {:error, %{"reason" => "pane_claim_release_failed"}}
    end
  end

  test "C-7 claim lifetime: the outcome precedes release; a release failure keeps the legacy exit 70 with projections written" do
    dir = Rows.fresh("claim")
    Rows.write_inputs(dir, "gated_run_seed")
    refute File.exists?(Path.join(dir, "run-summary.org"))

    result =
      CLI.run(
        ["run", dir],
        "registry"
        |> Rows.fresh()
        |> Doubles.seams(self())
        |> Keyword.merge(pane_registry: ObservingRegistry, pane_registry_opts: [test_pid: self(), run_dir: dir])
      )

    assert_receive {:released, projections_existed_at_release}, 60_000
    assert projections_existed_at_release, "release ran before the outcome wrote the projections"
    refute_receive {:released, _}, 200
    assert %{status: 70, stderr: stderr} = result
    assert stderr =~ "pane_claim_release_failed"
    assert File.exists?(Path.join(dir, "run-summary.org"))
  end

  defp witness_fixture do
    root = Rows.fresh("witness_root")
    ref = "run_w"
    File.mkdir_p!(Path.join(root, ref))
    File.write!(Path.join([root, ref, "events.jsonl"]), Rows.kill9("events_pre_dispatch.jsonl"))
    {root, ref, Rows.run_id(Path.join(root, ref))}
  end

  test "C-8 the labelled FAITHFUL witness passes every counting step" do
    {root, ref, run_id} = witness_fixture()
    assert Rows.counting_outcome(Doubles.DisposableFaithful, root, ref, run_id) == :ok
  end

  for {row, impl, step} <- [
        {"C-8a", Doubles.DisposableFixedZero, :own_present},
        {"C-8b", Doubles.DisposableSubtractOnly, :own_absent},
        {"C-8c", Doubles.DisposableUnfiltered, :own_present}
      ] do
    test "#{row} the counting rows reject #{inspect(impl)} at step #{inspect(step)}" do
      {root, ref, run_id} = witness_fixture()
      assert Rows.counting_outcome(unquote(impl), root, ref, run_id) == {:failed, unquote(step)}
    end
  end

  test "C-9 a per-leg budget reset witness fails the single-budget elapsed bound" do
    {root, ref, _run_id} = witness_fixture()
    mon = Rows.monitor!(:budget_witness)
    :ok = :sys.suspend(mon)

    try do
      started = System.monotonic_time(:millisecond)

      assert {:ok, %{errors: errors}} =
               Doubles.DisposablePerLegReset.host_view(ref, root: root, monitor: mon, budget_ms: 300)

      elapsed = System.monotonic_time(:millisecond) - started
      assert errors |> Enum.map(& &1.leg) |> Enum.sort() == [:lookup, :status]
      assert elapsed >= 600, "a per-leg reset must exhaust each leg's full budget (got #{elapsed} ms)"
      refute elapsed < 550, "the F-7 bound (< 550 ms) must reject this witness"
    after
      :ok = :sys.resume(mon)
    end
  end
end
