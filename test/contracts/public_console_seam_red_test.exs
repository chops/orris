defmodule AiOrchestrator.Contracts.PublicConsoleSeamRedTest do
  @moduledoc """
  docs/contracts/public-console-seam.org, FEATURE rows F-1..F-12: RED at 2c33d78. Each row names the public module and
  function it exercises; a RED failure is an UndefinedFunctionError on AiOrchestrator.Prepare / AiOrchestrator.Query (the
  modules do not exist yet) or, for F-1, the consumer's WAE exit 1 with the forbidden-reference and undefined-module
  diagnostics. Controls C-0..C-3 in the companion file prove the harness itself is valid.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Test.ConsoleConsumerHarness, as: Harness

  @moduletag :public_console_seam
  @moduletag timeout: 600_000

  @prepare AiOrchestrator.Prepare
  @query AiOrchestrator.Query
  @console %{"class" => "console", "id" => "session_abc"}
  @public_consumer """
  defmodule ConsoleConsumer.Probe do
    def build(actor), do: AiOrchestrator.Commands.build(actor, "cancel", %{"reason" => "operator_cancel"}, run_id: "run_x", command_id: "cmd_x")
    def cancel(run_ref, opts), do: AiOrchestrator.Prepare.cancel(run_ref, opts)
    def invoke(actor, prepared, opts), do: AiOrchestrator.Prepare.invoke(actor, prepared, opts)
    def summary(run_ref, opts), do: AiOrchestrator.Query.run_summary(run_ref, opts)
    def host(run_ref, opts), do: AiOrchestrator.Query.host_view(run_ref, opts)
  end
  """

  setup_all do
    dir = Harness.build!()
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, consumer: dir}
  end

  setup do
    root = tmp("root")
    {:ok, root: root, opts: [root: root]}
  end

  test "F-1 the public seam compiles for a consumer that references only exported modules", %{consumer: dir} do
    {exit, out} = Harness.compile(dir, @public_consumer)
    assert exit == 0, out
  end

  test "F-1b a public consumer executes a real cancel with public exports only", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, prepared} = @prepare.cancel(ref, opts)
    assert {:ok, %{events: events, close: :ok}} = @prepare.invoke(@console, prepared, opts)
    assert Enum.any?(events, &(&1["type"] == "run_cancelled"))
    assert File.exists?(Path.join([root, ref, "events.head"]))
  end

  test "F-2 cancel parity with the CLI writer probe", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    cli_dir = fixture_dir("events_pre_dispatch.jsonl")
    assert %{status: 0} = CLI.run(["cancel", cli_dir])
    assert {:ok, prepared} = @prepare.cancel(ref, opts)
    assert {:ok, _} = @prepare.invoke(@console, prepared, opts)
    assert kinds(Path.join(root, ref)) == kinds(cli_dir)
    assert summary(Path.join(root, ref)) == summary(cli_dir)
  end

  test "F-3 start parity and real input failures", %{root: root, opts: opts} do
    ref = inputs_run(root)
    assert {:ok, %{spec_hash: spec_hash, plan_hash: plan_hash}} = @prepare.validate(ref, opts)
    assert {:ok, prepared} = @prepare.start(ref, opts)
    assert prepared.args == %{"spec_hash" => spec_hash, "plan_hash" => plan_hash}
    File.rm!(Path.join([root, ref, "spec.json"]))
    assert {:error, %{clause: "run_inputs_missing", detail: %{"reason" => "file_not_found"}}} = @prepare.start(ref, opts)
    ref2 = inputs_run(root)
    File.chmod!(Path.join([root, ref2, "plan.json"]), 0o000)

    assert {:error, %{clause: "run_inputs_invalid", detail: %{"reason" => "file_read_failed"}}} =
             @prepare.start(ref2, opts)
  end

  test "F-5 the resolver is closed", %{root: root, opts: opts} do
    for bad <- ["a/b", "..", ".", "", "/abs", String.duplicate("x", 256), %{}, [a: 1], 42] do
      assert match?({:error, %{clause: "run_ref_invalid"}}, @query.resolve(bad, opts)), inspect(bad)
    end

    assert {:error, %{clause: "runs_root_missing"}} = @query.resolve("run", root: Path.join(root, "absent"))
    assert {:error, %{clause: "run_directory_missing"}} = @query.resolve("absent", opts)
    File.write!(Path.join(root, "file"), "x")
    assert {:error, %{clause: "run_directory_invalid"}} = @query.resolve("file", opts)
    outside = tmp("outside")
    File.ln_s!(outside, Path.join(root, "escape"))
    assert {:error, %{clause: "run_ref_outside_root"}} = @query.resolve("escape", opts)
    link_root = Path.join(tmp("linkroot"), "root")
    File.ln_s!(root, link_root)
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, resolved} = @query.resolve(ref, root: link_root)
    assert String.starts_with?(resolved, Path.expand(root))
  end

  test "F-6 discovery applies the scope filter", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    File.write!(Path.join(root, "file"), "x")
    File.ln_s!(tmp("outside2"), Path.join(root, "escape"))
    assert {:ok, %{runs: runs, skipped_outside_root: 1}} = @query.list_runs(opts)
    assert Enum.map(runs, & &1.run_ref) == [ref]
  end

  test "F-7 unknown states are reported, never inferred", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, view} = @query.host_view(ref, opts)
    assert view.registered == false
    assert {:ok, view} = @query.host_view(ref, Keyword.merge(opts, monitor: :no_such_monitor, budget_ms: 50))
    assert view.registered == :unknown and Enum.any?(view.errors, &(&1.leg == :status))
  end

  test "F-8 counting excludes this directory and leaks no identity", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, view} = @query.host_view(ref, opts)
    assert view.other_registered_directories in [0, :unknown]
    refute inspect(view) =~ ~r/#PID|#Reference|#{Regex.escape(root)}/
  end

  test "F-9 pending_repair is exposed, not executed", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    path = Path.join([root, ref, "events.jsonl"])
    File.write!(path, File.read!(path) <> "{\"torn\"")
    before = File.read!(path)
    assert {:ok, %{pending_repair: plan, summary: %{"run_id" => _}}} = @query.run_summary(ref, opts)
    assert plan != nil and File.read!(path) == before
    File.write!(path, "not-json\n")
    assert {:error, %{clause: "journal_invalid"}} = @query.run_summary(ref, opts)
  end

  test "F-10 projection files are outputs, never inputs", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert %{status: 0} = CLI.run(["status", Path.join(root, ref)])
    assert {:ok, first} = @query.run_summary(ref, opts)
    for f <- ["run-summary.org", "run-context.org"], do: File.rm(Path.join([root, ref, f]))
    assert {:ok, ^first} = @query.run_summary(ref, opts)
    refute File.exists?(Path.join([root, ref, "run-summary.org"]))
  end

  test "F-11 unsupported verbs are refused with the existing clause" do
    assert @prepare.supported_verbs() == ["start", "resume", "cancel"]

    for verb <- ~w(pause repair update_context ratify_plan resolve_attention) do
      assert {:error, %{clause: "command_verb_unsupported"}} = @prepare.verb(verb)
    end
  end

  test "F-12 the public request is a verb and a handle only", %{opts: opts} do
    for bad <- [%{"run_ref" => "x"}, [run_ref: "x"], 1] do
      assert match?({:error, %{clause: "run_ref_invalid"}}, @prepare.cancel(bad, opts)), inspect(bad)
    end

    assert function_exported?(@prepare, :cancel, 2) and function_exported?(@prepare, :invoke, 3)
  end

  defp fixture_run(root, events_file) do
    ref = "run_#{System.unique_integer([:positive])}"
    dir = Path.join(root, ref)
    File.mkdir_p!(dir)

    File.write!(
      Path.join(dir, "events.jsonl"),
      kill9(events_file)
    )

    ref
  end

  defp fixture_dir(events_file) do
    dir = tmp("cli")

    File.write!(
      Path.join(dir, "events.jsonl"),
      kill9(events_file)
    )

    dir
  end

  defp inputs_run(root) do
    ref = "inputs_#{System.unique_integer([:positive])}"
    dir = Path.join(root, ref)
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    ref
  end

  defp kinds(dir),
    do:
      dir
      |> Path.join("events.jsonl")
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode!(&1)["type"])

  defp summary(dir) do
    {:ok, state} = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Fold.fold_lines()
    Fold.summary(state)
  end

  defp kill9(file) do
    [File.cwd!(), "test", "fixtures", "contracts", "scenarios", "kill9_resume", file] |> Path.join() |> File.read!()
  end

  defp tmp(name) do
    dir = Path.join(Mix.Project.build_path(), "console_seam_#{name}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end
end
