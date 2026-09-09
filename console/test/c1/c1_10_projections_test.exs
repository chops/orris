defmodule C1.ProjectionsTest do
  use ExUnit.Case, async: false
  alias AiOrchestrator.Query
  alias C1.Harness
  alias OrrisConsole.ReadModel

  @mods [OrrisConsole.ReadModel, OrrisConsole.Config]

  defp setup!(overrides \\ []) do
    Harness.red!(@mods)
    {root, ids} = Harness.fixture_root(["one", "two"])
    {:ok, config} = OrrisConsole.Config.load(Harness.config(Harness.merged([roots: %{"alpha" => root}], overrides)))
    session = %{actor_id: "operator", root_ids: ["alpha"]}
    %{root: root, ids: ids, config: config, session: session}
  end

  defp dead_monitor! do
    {:ok, pid} = AiOrchestrator.Host.Monitor.start_link(name: nil)
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    pid
  end

  test "C1-10a list, summary and context equal the public Query facts for real fixture runs and expose no path" do
    %{root: root, ids: ids, config: config, session: session} = setup!()
    assert {:ok, %{root_id: "alpha", runs: runs, skipped: 0}} = ReadModel.list(config, session, "alpha")
    assert Enum.map(runs, & &1.run_ref) == ["one", "two"]
    assert Enum.map(runs, & &1.run_id) == [ids["one"], ids["two"]]
    {:ok, expected} = Query.run_summary("one", root: root)
    assert {:ok, summary} = ReadModel.summary(config, session, "alpha", "one")

    assert summary.run_id == expected.run_id and summary.status == expected.status and
             summary.last_seq == expected.last_seq

    assert summary.pending_repair == expected.pending_repair
    assert {:ok, context} = ReadModel.context(config, session, "alpha", "one")
    assert context.rendered == elem(Query.run_context("one", root: root), 1).rendered
    refute inspect(summary) =~ root or inspect(context) =~ root
  end

  test "C1-10b a torn tail is exact pending_repair data on every read; the bytes never change" do
    %{root: root, config: config, session: session} = setup!()
    dir = Path.join(root, "one")
    Harness.torn!(dir)
    before = Harness.journal_sha(dir)
    {:ok, %{pending_repair: expected}} = Query.run_summary("one", root: root)
    assert %{action: :truncate_tail} = expected

    for _ <- 1..3 do
      assert {:ok, %{pending_repair: ^expected}} = ReadModel.summary(config, session, "alpha", "one")
      assert {:ok, %{pending_repair: ^expected}} = ReadModel.context(config, session, "alpha", "one")
    end

    assert Harness.journal_sha(dir) == before
  end

  test "C1-10c host facts are preserved: a live Monitor answers registered false / live false (not unknown); a dead monitor yields :unknown with the precise legs" do
    %{root: root, config: config, session: session} = setup!()
    assert {:ok, %{host: %{registered: false, live: false}}} = ReadModel.summary(config, session, "alpha", "two")

    {:ok, unknown_config} =
      OrrisConsole.Config.load(
        Harness.config(roots: %{"alpha" => root}, query_opts: [monitor: dead_monitor!(), budget_ms: 200])
      )

    assert {:ok, %{host: host}} = ReadModel.summary(unknown_config, session, "alpha", "two")
    assert host.registered == :unknown and host.live == :unknown and host.other_registered_directories == :unknown
    assert Enum.sort(Enum.map(host.errors, & &1.leg)) == [:lookup, :status]
    File.write!(Path.join(root, "one/events.jsonl"), "{not json\n")
    assert {:error, :unavailable} = ReadModel.summary(config, session, "alpha", "one")
    assert {:error, :not_found} = ReadModel.summary(config, session, "alpha", "absent")
    assert {:error, :not_found} = ReadModel.summary(config, session, "beta", "one")
    assert {:error, :invalid} = ReadModel.summary(config, session, "alpha", "../one")
  end

  test "C1-10d seeded projection files are never the source of a read and their deletion changes nothing; no read or failure writes the journal" do
    %{root: root, config: config, session: session} = setup!()
    dir = Path.join(root, "two")
    before = Harness.journal_sha(dir)
    for file <- ["run-summary.org", "run-context.org"], do: File.write!(Path.join(dir, file), "CACHE_CANARY_#{file}\n")
    assert {:ok, first} = ReadModel.summary(config, session, "alpha", "two")
    assert {:ok, context} = ReadModel.context(config, session, "alpha", "two")
    refute inspect(first) =~ "CACHE_CANARY" or context.rendered =~ "CACHE_CANARY"
    for file <- ["run-summary.org", "run-context.org"], do: File.rm!(Path.join(dir, file))
    assert {:ok, ^first} = ReadModel.summary(config, session, "alpha", "two")
    assert {:ok, ^context} = ReadModel.context(config, session, "alpha", "two")
    assert {:ok, %{runs: _}} = ReadModel.list(config, session, "alpha")
    File.write!(Path.join(dir, "events.jsonl"), "{not json\n")
    assert {:error, :unavailable} = ReadModel.summary(config, session, "alpha", "two")
    assert File.read!(Path.join(dir, "events.jsonl")) == "{not json\n"
    File.write!(Path.join(dir, "events.jsonl"), Harness.fixture("events_pre_dispatch.jsonl"))
    assert Harness.journal_sha(dir) == before
    refute File.exists?(Path.join(dir, "run-summary.org"))
  end

  test "C1-10e a list entry's nested core rejection is sanitized to the closed vocabulary; the index renders no inspected failure" do
    %{root: root, config: config, session: session} = setup!()
    File.write!(Path.join(root, "one/events.jsonl"), "{not json\n")
    {:ok, %{runs: raw}} = Query.list_runs(root: root)

    assert %{error: %{clause: "journal_invalid", detail: %{clause: "undecodable_line"}}} =
             Enum.find(raw, &(&1.run_ref == "one"))

    assert {:ok, %{runs: runs}} = ReadModel.list(config, session, "alpha")
    assert %{error: :unavailable} = Enum.find(runs, &(&1.run_ref == "one"))
    assert %{error: nil} = Enum.find(runs, &(&1.run_ref == "two"))
    refute inspect(runs) =~ "clause" or inspect(runs) =~ "undecodable" or inspect(runs) =~ root
  end

  test "C1-10f the index invents no repair state: list entries carry no pending_repair, only the detail read reports it" do
    %{root: root, config: config, session: session} = setup!()
    Harness.torn!(Path.join(root, "one"))
    assert {:ok, %{runs: runs}} = ReadModel.list(config, session, "alpha")
    refute Enum.any?(runs, &Map.has_key?(&1, :pending_repair))
    assert {:ok, %{pending_repair: %{action: :truncate_tail}}} = ReadModel.summary(config, session, "alpha", "one")
  end
end
