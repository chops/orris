defmodule AiOrchestrator.Contracts.NS28DiscoveryIndexTest do
  @moduledoc """
  NS-28.H.001 (projection loss or lag never blocks a read or an execution) and NS-28.H.002 (discovery roots are
  configuration; identity is the durable run directory), pinned at the source as it stands: there is no
  `Projection.Index` module, no ETS table and no cache (docs/contracts/cli-discovery.org, "Every invocation reads
  again"). The only derived artefacts are `run-summary.org` and `run-context.org`, written by the CLI after a
  command and read by nothing; every operator read folds the verified journal prefix through `Journal.Reader`.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Host
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Projection.RunContext
  alias AiOrchestrator.Projection.RunSummary
  alias AiOrchestrator.Query
  alias AiOrchestrator.Test.ConsoleSeamRows, as: Rows

  @projections ["run-summary.org", "run-context.org"]
  @canary "PRIVATE_STALE_INDEX_CANARY"
  @root_missing {:error, %{clause: "runs_root_missing", detail: nil}}

  setup do
    base = Path.join(System.tmp_dir!(), "orris-ns28-#{System.unique_integer([:positive, :monotonic])}")
    File.mkdir_p!(base)
    on_exit(fn -> File.rm_rf!(base) end)
    %{base: base}
  end

  # ---- NS-28.H.001: the index is rebuildable; projection loss or lag never blocks ----

  test "H.001 reads: status, list, replay and the Query seam answer identically with current, deleted and lagging projections",
       %{base: base} do
    root = Path.join([base, ".ai-orchestrator", "runs"])
    dir = fixture(Path.join(root, "alpha"))
    {:ok, state} = Fold.fold_lines(F.lines("scenarios", "gated_run_seed"))
    # a CURRENT index: exactly the projections the CLI writes after a command, rebuilt here from the same fold
    File.write!(Path.join(dir, "run-summary.org"), RunSummary.render(state))
    File.write!(Path.join(dir, "run-context.org"), RunContext.render(state))

    current = reads(base, root, "alpha")
    assert %{status: 0, stderr: ""} = current.status_json
    assert Jason.decode!(current.status_json.stdout) == F.json("scenarios", "gated_run_seed", "expected.json")
    # replay (a fresh fold of the verified prefix) IS the status answer
    assert current.replay == Jason.decode!(current.status_json.stdout)
    assert {:ok, %{run_ref: "alpha", status: "completed", last_seq: 32, pending_repair: nil}} = current.summary
    # the two `reads/3` comparisons below are equality only, so every listing is pinned POSITIVELY
    # here first: equal refusals would otherwise satisfy them
    assert Enum.map(Jason.decode!(current.list_root_json.stdout)["runs"], & &1["run_ref"]) == ["alpha"]
    assert Enum.map(Jason.decode!(current.list_relative_json.stdout)["runs"], & &1["run_ref"]) == ["alpha"]
    assert %{status: 0, stderr: ""} = current.list_root_org
    assert %{status: 0, stderr: ""} = current.list_relative_org
    assert current.list_root_org.stdout =~ "| alpha | run_scenario_0001 | completed | 32 | no | - |"
    assert current.list_relative_org.stdout =~ "| alpha | run_scenario_0001 | completed | 32 | no | - |"

    # index DELETED: every read is unchanged, and no read rebuilds the files
    for file <- @projections, do: File.rm!(Path.join(dir, file))
    assert reads(base, root, "alpha") == current
    refute Enum.any?(@projections, &File.exists?(Path.join(dir, &1)))

    # index LAGGING: projections that claim another outcome are neither read nor believed
    for file <- @projections, do: File.write!(Path.join(dir, file), "#{@canary}\n* Status: failed\n")
    assert reads(base, root, "alpha") == current
    refute inspect(current, limit: :infinity, printable_limit: :infinity) =~ @canary
  end

  test "H.001 the head receipt is journal state, not an index: its loss on a chained journal fails closed and is never rebuilt",
       %{base: base} do
    root = Path.join(base, "root")
    dir = chained_fixture(Path.join(root, "chained"))
    assert %{status: 0, stderr: ""} = CLI.run(["status", "--json", dir])

    File.rm!(Path.join(dir, "events.head"))
    before = File.read!(Path.join(dir, "events.jsonl"))

    assert %{status: 66, stdout: "", stderr: stderr} = CLI.run(["status", "--json", dir])
    assert Jason.decode!(stderr)["reason"] == "journal_receipt_missing"

    assert {:error, %{clause: "journal_invalid", detail: %{clause: "receipt_missing"}}} =
             Query.run_summary("chained", root: root)

    assert {:ok, %{runs: [%{run_ref: "chained", status: "invalid", run_id: nil}]}} = Query.list_runs(root: root)
    refute File.exists?(Path.join(dir, "events.head"))
    assert File.read!(Path.join(dir, "events.jsonl")) == before
  end

  test "H.001 execution: a stale or lost projection never blocks a command; the journal is appended before any projection",
       %{base: base} do
    dir = Path.join(base, "cancel")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Rows.kill9("events_pre_dispatch.jsonl"))
    for file <- @projections, do: File.write!(Path.join(dir, file), "#{@canary}\n* Status: completed\n")

    assert %{status: 0, stdout: out, stderr: ""} = CLI.run(["cancel", dir])
    assert out =~ "* Status: cancelled"
    assert "run_cancel_requested" in Rows.event_kinds(dir)
    # the projections are rewritten from the journal the command just appended: the stale index is gone
    assert File.read!(Path.join(dir, "run-summary.org")) == out
    for file <- @projections, do: refute(File.read!(Path.join(dir, file)) =~ @canary)

    # an UNWRITABLE projection target: the command still executes (the journal carries the cancel); the failed
    # projection write is reported afterwards as output_write_failed, never as a refused command
    blocked = Path.join(base, "blocked")
    File.mkdir_p!(Path.join(blocked, "run-summary.org"))
    File.write!(Path.join(blocked, "events.jsonl"), Rows.kill9("events_pre_dispatch.jsonl"))

    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["cancel", blocked])
    assert "run_cancel_requested" in Rows.event_kinds(blocked)
    assert Jason.decode!(stderr)["reason"] == "output_write_failed"
    assert %{status: 0, stdout: after_out} = CLI.run(["status", blocked])
    assert after_out =~ "* Status: cancelled"
  end

  # ---- NS-28.H.002: roots are explicit; identity is the durable run directory ----

  test "H.002 roots are configuration: the public seam discovers nothing under the working directory without a root",
       %{base: base} do
    fixture(Path.join([base, ".ai-orchestrator", "runs", "ambient"]))
    configured = Path.join(base, "configured")
    fixture(Path.join(configured, "alpha"))

    # no root, or an unrelated option, is runs_root_missing on every seam function: nothing is scanned
    assert Query.list_runs([]) == @root_missing
    assert Query.list_runs(cwd: base) == @root_missing
    assert Query.resolve("ambient", cwd: base) == @root_missing
    assert Query.run_summary("ambient", cwd: base) == @root_missing
    assert Query.run_context("ambient", []) == @root_missing
    assert Query.host_view("ambient", []) == @root_missing

    assert {:ok, %{runs: [%{run_ref: "alpha"}], skipped_outside_root: 0}} = Query.list_runs(root: configured)
    # a RELATIVE root is explicit configuration resolved against the given working directory, never a scan of it
    assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["list", "--root", "configured", "--json"], cwd: base)
    assert Enum.map(Jason.decode!(json)["runs"], & &1["run_ref"]) == ["alpha"]
    refute json =~ "ambient"
    assert %{status: 74, stdout: "", stderr: stderr} = CLI.run(["list", "--root", "absent", "--json"], cwd: base)
    assert Jason.decode!(stderr) == %{"reason" => "runs_root_missing"}
  end

  test "H.002 identity is the durable run directory: equal run ids across roots stay distinct, a linked root names the same directory, a live collision is typed",
       %{base: base} do
    root_a = Path.join(base, "root_a")
    root_b = Path.join(base, "root_b")
    same_a = fixture(Path.join(root_a, "same"))
    same_b = fixture(Path.join(root_b, "same"))
    other_a = fixture(Path.join(root_a, "other"))
    run_id = Rows.run_id(same_a)
    assert run_id == Rows.run_id(same_b) and run_id == Rows.run_id(other_a)

    # two rows per root, never a merge by run id; the same ref under another root is another directory
    assert {:ok, %{runs: [%{run_ref: "other", run_id: ^run_id}, %{run_ref: "same", run_id: ^run_id}]}} =
             Query.list_runs(root: root_a)

    assert {:ok, %{runs: [%{run_ref: "same", run_id: ^run_id}]}} = Query.list_runs(root: root_b)
    assert {:ok, dir_a} = Query.resolve("same", root: root_a)
    assert {:ok, dir_b} = Query.resolve("same", root: root_b)
    assert dir_a != dir_b
    assert {:ok, %{run_ref: "same", run_id: ^run_id, status: "completed"}} = Query.run_summary("same", root: root_a)
    assert {:ok, %{run_ref: "same", run_id: ^run_id, status: "completed"}} = Query.run_summary("same", root: root_b)

    # a symlinked root reaches the SAME physical directory: one identity, not a second
    link = Path.join(base, "root_link")
    File.ln_s!(root_a, link)
    assert Query.resolve("same", root: link) == {:ok, dir_a}

    # the live registry names a collision by clause and counts every directory carrying the id; the root-scoped
    # view counts only OTHER directories inside its own root (another root's directory is not its peer)
    mon = Rows.monitor!(:ns28_collision)
    for dir <- [same_a, same_b], do: Rows.registered!(mon, Rows.record(dir, run_id))
    assert Host.collision(run_id, monitor: mon) == {:ok, %{clause: "host_registry_collision", count: 2}}
    assert {:ok, %{other_registered_directories: 0}} = Query.host_view("same", root: root_a, monitor: mon)
    Rows.registered!(mon, Rows.record(other_a, run_id))
    assert Host.collision(run_id, monitor: mon) == {:ok, %{clause: "host_registry_collision", count: 3}}
    assert {:ok, %{other_registered_directories: 1}} = Query.host_view("same", root: root_a, monitor: mon)
    assert Host.collision("run_nobody", monitor: mon) == {:ok, %{clause: "host_registry_unique", count: 0}}
  end

  # every operator read of one run, taken together so the three index states can be compared as one value
  defp reads(base, root, ref) do
    dir = Path.join(root, ref)
    {:ok, loaded} = Reader.load(dir)
    {:ok, replayed} = Fold.fold_lines(loaded.lines)

    %{
      status_json: CLI.run(["status", "--json", dir]),
      status_org: CLI.run(["status", dir]),
      list_root_json: CLI.run(["list", "--root", root, "--json"]),
      list_root_org: CLI.run(["list", "--root", root]),
      # D-14: the retired no-root renderings are replaced by the SUPPLIED relative root, resolved
      # against the given working directory by discovery.ex. Resolution is not inference.
      list_relative_json: CLI.run(["list", "--root", Path.relative_to(root, base), "--json"], cwd: base),
      list_relative_org: CLI.run(["list", "--root", Path.relative_to(root, base)], cwd: base),
      summary: Query.run_summary(ref, root: root),
      context: Query.run_context(ref, root: root),
      replay: replayed |> Fold.summary() |> Jason.encode!() |> Jason.decode!()
    }
  end

  defp fixture(dir, name \\ "gated_run_seed") do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(F.lines("scenarios", name), "\n") <> "\n")
    dir
  end

  # a schema_version 2 journal with its chained hashes and head receipt (as test/cli/cli_discovery_test.exs)
  defp chained_fixture(dir) do
    File.mkdir_p!(dir)

    {lines, hash} =
      Enum.map_reduce(F.lines("scenarios", "gated_run_seed"), Chain.anchor(), fn line, prior ->
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
