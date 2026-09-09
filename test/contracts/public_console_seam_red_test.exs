defmodule AiOrchestrator.Contracts.PublicConsoleSeamRedTest do
  @moduledoc """
  docs/contracts/public-console-seam.org, FEATURE rows F-1..F-12: RED at 2c33d78. Every row fails on the undefined
  `AiOrchestrator.Prepare` / `AiOrchestrator.Query` modules (F-1/F-1b through the external consumer's compile), never
  on the harness: controls C-0..C-8 in the companion file prove the harness, the doubles and the counting rows.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary
  alias AiOrchestrator.Test.ConsoleConsumerHarness, as: Harness
  alias AiOrchestrator.Test.ConsoleSeamDoubles, as: Doubles
  alias AiOrchestrator.Test.ConsoleSeamRows, as: Rows
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.FixedId

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

    # external execution entry (F-1b): cancel a run under `root` and print ONE JSON line with the observations
    def main(root, run_ref) do
      actor = %{"class" => "console", "id" => "external_session"}
      result =
        with {:ok, prepared} <- AiOrchestrator.Prepare.cancel(run_ref, root: root),
             {:ok, %{events: events, close: close}} <- AiOrchestrator.Prepare.invoke(actor, prepared, root: root) do
          %{"ok" => true, "kinds" => Enum.map(events, & &1["type"]), "close" => inspect(close),
            "head" => File.exists?(Path.join([root, run_ref, "events.head"]))}
        else
          other -> %{"ok" => false, "error" => inspect(other)}
        end
      IO.puts(Jason.encode!(result))
    end
  end
  """

  setup_all do
    dir = Harness.build!()
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, consumer: dir}
  end

  setup do
    root = Rows.fresh("root")
    FixedId.reset()
    FixedClock.reset()
    {:ok, root: root, opts: [root: root]}
  end

  test "F-1 the public seam compiles for a consumer that references only exported modules", %{consumer: dir} do
    {exit, out} = Harness.compile(dir, @public_consumer)
    assert exit == 0, out
  end

  test "F-1b an EXTERNAL consumer process executes a real cancel with public exports only", %{consumer: dir, root: root} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    {_, _} = Harness.compile(dir, @public_consumer)
    {exit, out} = Harness.run(dir, "ConsoleConsumer.Probe.main(#{inspect(root)}, #{inspect(ref)})")
    assert exit == 0, out
    assert %{"ok" => true, "kinds" => kinds, "head" => true} = Harness.json_line(out) || %{"raw" => out}
    assert "run_cancelled" in kinds
    assert File.exists?(Path.join([root, ref, "events.head"]))
  end

  test "F-2 cancel parity with the CLI writer probe", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    cli_dir = Rows.fresh("cli_cancel")
    File.write!(Path.join(cli_dir, "events.jsonl"), Rows.kill9("events_pre_dispatch.jsonl"))
    assert %{status: 0} = CLI.run(["cancel", cli_dir])
    assert {:ok, prepared} = @prepare.cancel(ref, opts)
    assert {:ok, %{events: _, close: :ok}} = @prepare.invoke(@console, prepared, opts)
    assert Rows.event_kinds(Path.join(root, ref)) == Rows.event_kinds(cli_dir)
    assert Fold.summary(Rows.state(Path.join(root, ref))) == Fold.summary(Rows.state(cli_dir))
    assert File.exists?(Path.join([root, ref, "events.head"]))
  end

  test "F-3 start parity with the CLI on equivalent fixtures, input bytes hashed independently", %{root: root} do
    cli_dir = Rows.fresh("cli_start")
    hashes = Rows.write_inputs(cli_dir, "gated_run_seed")
    registry = Rows.fresh("registry")
    seams = Doubles.seams(registry, self())
    FixedId.reset()
    FixedClock.reset()
    assert %{status: 0} = CLI.run(["run", cli_dir], seams)
    ref = "start_run"
    File.mkdir_p!(Path.join(root, ref))
    assert ^hashes = Rows.write_inputs(Path.join(root, ref), "gated_run_seed")
    FixedId.reset()
    FixedClock.reset()
    server_opts = Keyword.merge(seams, root: root, pane_registry_root: Rows.fresh("registry2"))
    assert {:ok, %{spec_hash: spec_hash, plan_hash: plan_hash}} = @prepare.validate(ref, server_opts)
    assert %{spec_hash: ^spec_hash, plan_hash: ^plan_hash} = hashes
    assert {:ok, prepared} = @prepare.start(ref, server_opts)
    assert prepared.args == %{"spec_hash" => hashes.spec_hash, "plan_hash" => hashes.plan_hash}
    assert {:ok, %{events: _}} = @prepare.invoke(@console, prepared, server_opts)
    assert Rows.event_kinds(Path.join(root, ref)) == Rows.event_kinds(cli_dir)

    first =
      root
      |> Path.join(ref)
      |> Path.join("events.jsonl")
      |> File.read!()
      |> String.split("\n", trim: true)
      |> hd()
      |> Jason.decode!()

    assert first["type"] == "run_created" and first["data"]["spec_hash"] == hashes.spec_hash
  end

  test "F-3b resume parity, input drift and the explicit empty-journal restart", %{root: root} do
    seams = Doubles.seams(Rows.fresh("registry"), self())
    cli_dir = Rows.fresh("cli_resume")
    Rows.write_inputs(cli_dir, "kill9_resume")

    Rows.write_journal(
      cli_dir,
      Rows.reanchored(String.split(Rows.kill9("events_pre_gate.jsonl"), "\n", trim: true), cli_dir)
    )

    FixedId.reset()
    FixedClock.reset()
    assert %{status: 0} = CLI.run(["run", "--resume", cli_dir], seams)
    ref = "resume_run"
    dir = Path.join(root, ref)
    File.mkdir_p!(dir)
    Rows.write_inputs(dir, "kill9_resume")
    Rows.write_journal(dir, Rows.reanchored(String.split(Rows.kill9("events_pre_gate.jsonl"), "\n", trim: true), dir))
    FixedId.reset()
    FixedClock.reset()
    server_opts = Keyword.merge(seams, root: root, pane_registry_root: Rows.fresh("registry2"))
    assert {:ok, prepared} = @prepare.resume(ref, server_opts)
    assert prepared.verb == "resume"
    assert {:ok, _} = @prepare.invoke(@console, prepared, server_opts)
    assert Rows.event_kinds(dir) == Rows.event_kinds(cli_dir)
    # input drift: the plan bytes no longer match the journal's provenance
    drift = Path.join(root, "drift")
    File.mkdir_p!(drift)
    Rows.write_inputs(drift, "kill9_resume")
    Rows.write_journal(drift, Rows.reanchored(String.split(Rows.kill9("events_pre_gate.jsonl"), "\n", trim: true), drift))
    File.write!(Path.join(drift, "plan.json"), File.read!(Path.join(drift, "plan.json")) <> " ")
    assert %{status: 70, stderr: stderr} = CLI.run(["run", "--resume", drift], seams)
    cli_reason = Jason.decode!(stderr)
    assert {:error, %{detail: ^cli_reason}} = @prepare.resume("drift", server_opts)
    # explicit empty-journal restart
    empty = Path.join(root, "empty")
    File.mkdir_p!(empty)
    Rows.write_inputs(empty, "gated_run_seed")
    File.write!(Path.join(empty, "events.jsonl"), "")
    assert {:ok, %{verb: "start", context: context}} = @prepare.resume("empty", server_opts)
    assert Keyword.get(context, :restart_empty) == true
  end

  test "F-3c a refused pane claim is reported before the Writer decides, as the CLI reports it", %{root: root} do
    seams = "registry" |> Rows.fresh() |> Doubles.seams(self()) |> Keyword.put(:pane_registry, Doubles.RefusingRegistry)
    cli_dir = Rows.fresh("cli_refused")
    Rows.write_inputs(cli_dir, "gated_run_seed")
    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", cli_dir], seams)
    refute File.exists?(Path.join(cli_dir, "events.jsonl"))
    ref = "refused"
    File.mkdir_p!(Path.join(root, ref))
    Rows.write_inputs(Path.join(root, ref), "gated_run_seed")
    server_opts = Keyword.put(seams, :root, root)
    assert {:ok, prepared} = @prepare.start(ref, server_opts)
    assert {:error, %{clause: "pane_claim_refused", detail: detail}} = @prepare.invoke(@console, prepared, server_opts)
    assert detail == Jason.decode!(stderr)
    refute File.exists?(Path.join([root, ref, "events.jsonl"]))
  end

  test "F-3d real input failures map to the CLI reasons", %{root: root, opts: opts} do
    ref = "inputs"
    File.mkdir_p!(Path.join(root, ref))
    Rows.write_inputs(Path.join(root, ref), "gated_run_seed")
    File.rm!(Path.join([root, ref, "spec.json"]))

    assert {:error, %{clause: "run_inputs_missing", detail: %{"reason" => "file_not_found", "file" => "spec.json"}}} =
             @prepare.start(ref, opts)

    ref2 = "inputs2"
    File.mkdir_p!(Path.join(root, ref2))
    Rows.write_inputs(Path.join(root, ref2), "gated_run_seed")
    File.chmod!(Path.join([root, ref2, "plan.json"]), 0o000)
    on_exit(fn -> File.chmod(Path.join([root, ref2, "plan.json"]), 0o644) end)

    assert {:error, %{clause: "run_inputs_invalid", detail: %{"reason" => "file_read_failed", "file" => "plan.json"}}} =
             @prepare.start(ref2, opts)
  end

  test "F-5 the resolver is closed", %{root: root, opts: opts} do
    for bad <- ["a/b", "..", ".", "", "/abs", String.duplicate("x", 256), %{}, [a: 1], 42] do
      assert match?({:error, %{clause: "run_ref_invalid"}}, @query.resolve(bad, opts)), inspect(bad)
    end

    assert match?({:error, %{clause: "runs_root_missing"}}, @query.resolve("run", root: Path.join(root, "absent")))
    assert match?({:error, %{clause: "run_directory_missing"}}, @query.resolve("absent", opts))
    File.write!(Path.join(root, "file"), "x")
    assert match?({:error, %{clause: "run_directory_invalid"}}, @query.resolve("file", opts))
    outside = Rows.fresh("outside")
    File.ln_s!(outside, Path.join(root, "escape"))
    assert match?({:error, %{clause: "run_ref_outside_root"}}, @query.resolve("escape", opts))
    link_root = Path.join(Rows.fresh("linkroot"), "root")
    File.ln_s!(root, link_root)
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, resolved} = @query.resolve(ref, root: link_root)
    assert String.starts_with?(resolved, Path.expand(root))
    refute File.exists?(Path.join(outside, "events.jsonl"))
  end

  test "F-6 discovery applies the scope filter", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    File.write!(Path.join(root, "file"), "x")
    File.ln_s!(Rows.fresh("outside2"), Path.join(root, "escape"))
    assert {:ok, %{runs: runs, skipped_outside_root: 1}} = @query.list_runs(opts)
    assert Enum.map(runs, & &1.run_ref) == [ref]
  end

  test "F-7 unknown states under ONE total budget with blocking host legs", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    mon = Rows.monitor!(:f7)

    assert {:ok, %{registered: false, other_registered_directories: 0}} =
             @query.host_view(ref, Keyword.put(opts, :monitor, mon))

    :ok = :sys.suspend(mon)

    try do
      started = System.monotonic_time(:millisecond)
      assert {:ok, view} = @query.host_view(ref, Keyword.merge(opts, monitor: mon, budget_ms: 300))
      elapsed = System.monotonic_time(:millisecond) - started
      assert view.registered == :unknown and view.other_registered_directories == :unknown
      legs = view.errors |> Enum.map(& &1.leg) |> Enum.sort()
      # the two observed failures are required; a closed :mounted budget exhaustion may also be reported
      assert legs in [[:lookup, :status], [:lookup, :mounted, :status]], inspect(legs)
      assert elapsed < 550, "per-leg budgets would take >= 600 ms; one total budget took #{elapsed} ms"
      assert view.run_id == Rows.run_id(Path.join(root, ref))
    after
      :ok = :sys.resume(mon)
    end
  end

  test "F-8 counting excludes this directory, ignores other roots, reports unknown lookups and leaks no identity", %{
    root: root
  } do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    Rows.counting_rows(@query, root, ref, Rows.run_id(Path.join(root, ref)))
  end

  test "F-9 pending_repair is exposed exactly as the Reader plans it; missing and empty journals map to their clauses", %{
    root: root,
    opts: opts
  } do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    path = Path.join([root, ref, "events.jsonl"])
    File.write!(path, File.read!(path) <> ~s({"schema":"ai-orch))
    before = File.read!(path)
    {:ok, %{pending_repair: expected_plan, lines: verified}} = Reader.load(Path.join(root, ref))
    {:ok, expected_state} = Fold.fold_lines(verified)
    assert {:ok, %{pending_repair: ^expected_plan, summary: summary}} = @query.run_summary(ref, opts)
    assert summary == Fold.summary(expected_state) and expected_plan.action == :truncate_tail
    assert File.read!(path) == before
    File.write!(path, "not-json\n")
    assert match?({:error, %{clause: "journal_invalid"}}, @query.run_summary(ref, opts))
    File.write!(path, "")
    assert match?({:error, %{clause: "journal_empty"}}, @query.run_summary(ref, opts))
    File.rm!(path)
    assert match?({:error, %{clause: "journal_missing"}}, @query.run_summary(ref, opts))
  end

  test "F-10 projection files are outputs, never inputs: canaries deleted, summary AND context unchanged, nothing recreated",
       %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    dir = Path.join(root, ref)
    for f <- ["run-summary.org", "run-context.org"], do: File.write!(Path.join(dir, f), "CANARY #{f} MUST NOT BE READ\n")
    expected_state = Rows.state(dir)
    expected_summary = RunSummary.render(expected_state)
    expected_context = RunContext.render(expected_state)
    assert {:ok, %{rendered: ^expected_summary}} = @query.run_summary(ref, opts)
    assert {:ok, %{rendered: ^expected_context}} = @query.run_context(ref, opts)
    for f <- ["run-summary.org", "run-context.org"], do: File.rm!(Path.join(dir, f))
    assert {:ok, %{rendered: ^expected_summary}} = @query.run_summary(ref, opts)
    assert {:ok, %{rendered: ^expected_context}} = @query.run_context(ref, opts)
    for f <- ["run-summary.org", "run-context.org"], do: refute(File.exists?(Path.join(dir, f)), f)
  end

  test "F-11 unsupported verbs are refused with the existing clause" do
    assert @prepare.supported_verbs() == ["start", "resume", "cancel"]

    for verb <- ~w(pause repair update_context ratify_plan resolve_attention) do
      assert match?({:error, %{clause: "command_verb_unsupported"}}, @prepare.verb(verb)), verb
    end
  end

  test "F-12 the public request is a verb and a handle only", %{opts: opts} do
    for bad <- [%{"run_ref" => "x"}, [run_ref: "x"], 1] do
      assert match?({:error, %{clause: "run_ref_invalid"}}, @prepare.cancel(bad, opts)), inspect(bad)
    end

    assert function_exported?(@prepare, :cancel, 2) and function_exported?(@prepare, :invoke, 3)
  end

  test "F-13 the supplied console actor is journaled by the built-in path (class and id preserved)", %{
    root: root,
    opts: opts
  } do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    assert {:ok, prepared} = @prepare.cancel(ref, opts)
    assert {:ok, _} = @prepare.invoke(@console, prepared, opts)
    requested = root |> Path.join(ref) |> cancel_event() |> get_in(["data", "requested_by"])
    assert %{"class" => "console", "id" => "session_abc", "verb" => "cancel"} = requested
  end

  test "F-14 a valid agent-class actor is refused for cancel and the journal bytes and head stay unchanged", %{
    root: root,
    opts: opts
  } do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    journal = Path.join([root, ref, "events.jsonl"])
    before = File.read!(journal)

    agent = %{
      "class" => "agent",
      "id" => "agent_1",
      "run_id" => Rows.run_id(Path.join(root, ref)),
      "assignment_id" => "as_0001"
    }

    assert {:ok, prepared} = @prepare.cancel(ref, opts)
    assert match?({:error, %{clause: "command_not_authorized"}}, @prepare.invoke(agent, prepared, opts))
    assert File.read!(journal) == before
    refute File.exists?(Path.join([root, ref, "events.head"]))
  end

  test "F-15 an invalid actor shape is refused before any execution", %{root: root, opts: opts} do
    ref = fixture_run(root, "events_pre_dispatch.jsonl")
    journal = Path.join([root, ref, "events.jsonl"])
    before = File.read!(journal)
    assert {:ok, prepared} = @prepare.cancel(ref, opts)

    for bad <- [
          %{"class" => "console"},
          %{"id" => "x"},
          "console",
          %{"class" => "console", "id" => "session_abc", "extra" => 1}
        ] do
      assert match?(
               {:error, %{clause: clause}}
               when clause in ["invalid_command_actor", "command_actor_fields", "command_actor_id"],
               @prepare.invoke(bad, prepared, opts)
             ),
             inspect(bad)
    end

    assert File.read!(journal) == before
  end

  defp cancel_event(dir) do
    dir
    |> Path.join("events.jsonl")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
    |> Enum.find(&(&1["type"] == "run_cancel_requested"))
  end

  defp fixture_run(root, events_file) do
    ref = "run_#{System.unique_integer([:positive])}"
    dir = Path.join(root, ref)
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Rows.kill9(events_file))
    ref
  end
end
