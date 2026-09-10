defmodule AiOrchestrator.CLIDiscoveryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Chain

  defmodule ReadOnlyFs do
    @moduledoc false
    def read({owner, mode}, path) do
      send(owner, {:read, path})
      key = {__MODULE__, path}
      count = Process.get(key, 0) + 1
      Process.put(key, count)

      case {mode, Path.basename(path), count} do
        {:vanish, "events.jsonl", 2} ->
          {:error, :enoent}

        {:raise, "events.jsonl", 2} ->
          if Path.basename(Path.dirname(path)) == "private",
            do: raise("PRIVATE_NESTED_CANARY /private/elsewhere"),
            else: File.read(path)

        _ ->
          File.read(path)
      end
    end
  end

  setup do
    base = Path.join(System.tmp_dir!(), "orris-cli-discovery-#{System.unique_integer([:positive, :monotonic])}")
    File.mkdir_p!(base)
    on_exit(fn -> File.rm_rf!(base) end)
    %{base: base}
  end

  test "D1 explicit root selection, flag order and relative cwd agree", %{base: base} do
    fixture(Path.join(base, ".ai-orchestrator/runs/default"))
    root = Path.join(base, "selected")
    fixture(Path.join(root, "zeta"))
    fixture(Path.join(root, "alpha"), "auth_blocked_pane")
    File.write!(Path.join(root, "ordinary-file"), "ignored")

    assert %{status: 0, stdout: old} = CLI.run(["list", "--json"], cwd: base)
    assert Enum.map(Jason.decode!(old), & &1["run_ref"]) == ["default"]

    first = CLI.run(["list", "--root", root, "--json"])
    assert first == CLI.run(["list", "--json", "--root", "selected"], cwd: base)
    assert %{status: 0, stdout: stdout, stderr: ""} = first
    assert Enum.map(Jason.decode!(stdout)["runs"], & &1["run_ref"]) == ["alpha", "zeta"]
    assert %{status: 0, stdout: org, stderr: ""} = CLI.run(["list", "--root", root])
    assert org =~ "| alpha | run_scenario_0002 | blocked |"
    refute org =~ "default"
    assert org =~ "does not establish process liveness"
  end

  test "D2 legacy empty output and closed explicit-root failures", %{base: base} do
    assert CLI.run(["list"], cwd: base) == %{status: 0, stdout: "#+title: Runs\n\n* Runs\n- none\n", stderr: ""}

    for args <- [["list", "--root", base <> "/PRIVATE_ROOT_CANARY"], ["list", "--root", ""]] do
      result = CLI.run(args)
      assert result.status in [64, 74]
      assert result.stdout == ""
      refute result.stderr =~ "PRIVATE_ROOT_CANARY"
    end

    File.write!(Path.join(base, "file"), "not a root")
    assert %{status: 74, stdout: "", stderr: stderr} = CLI.run(["list", "--root", Path.join(base, "file")])
    assert Jason.decode!(stderr) == %{"reason" => "runs_root_missing"}
  end

  test "D2 unreadable configured root is a closed failure", %{base: base} do
    root = Path.join(base, "unreadable")
    File.mkdir!(root)
    File.chmod!(root, 0o000)

    try do
      assert %{status: 74, stdout: "", stderr: stderr} = CLI.run(["list", "--root", root])
      assert Jason.decode!(stderr) == %{"reason" => "runs_root_unreadable"}
      refute stderr =~ base
    after
      File.chmod!(root, 0o700)
    end
  end

  test "D3 projections are ignored and changed chained bytes fail integrity", %{base: base} do
    good = fixture(Path.join(base, "good"))
    bad = chained_fixture(Path.join(base, "changed"))
    File.write!(Path.join(good, "run-summary.org"), "PRIVATE_PROJECTION_CANARY status: failed")
    File.write!(Path.join(good, "run-context.org"), "PRIVATE_CONTEXT_CANARY")
    corrupt_first_line(bad)

    rows = listing(base)["runs"]
    assert %{"status" => "completed", "last_seq" => 32} = Enum.find(rows, &(&1["run_ref"] == "good"))
    assert %{"status" => "invalid", "error" => "journal_invalid", "run_id" => nil} = hd(rows)
    refute Jason.encode!(rows) =~ "PRIVATE_"
  end

  test "D4 and D9 repair is visible while all fixture bytes and files stay unchanged", %{base: base} do
    fixture(Path.join(base, "clean"))
    torn = fixture(Path.join(base, "torn"))
    File.write!(Path.join(torn, "events.jsonl"), ~s({"schema":"ai-orch), [:append])
    before = snapshot(base)
    view = listing(base, fs: {ReadOnlyFs, {self(), :normal}})

    assert [%{"pending_repair" => false}, %{"pending_repair" => true, "last_seq" => 32}] = view["runs"]
    assert snapshot(base) == before
    assert_received {:read, _}
    refute Enum.any?(Map.keys(before), &(Path.basename(&1) in ["events.head", "run.lock", "run-summary.org"]))
  end

  test "D5 corrupt siblings retain valid rows with closed per-row errors", %{base: base} do
    fixture(Path.join(base, "valid"))
    File.mkdir!(Path.join(base, "missing"))
    File.mkdir!(Path.join(base, "empty"))
    File.write!(Path.join(base, "empty/events.jsonl"), "")
    File.mkdir!(Path.join(base, "malformed"))
    File.write!(Path.join(base, "malformed/events.jsonl"), "PRIVATE_BAD_LINE_CANARY\n")
    corrupt_first_line(chained_fixture(Path.join(base, "corrupt")))

    rows = listing(base)["runs"]
    assert length(rows) == 5
    assert Enum.find(rows, &(&1["run_ref"] == "valid"))["status"] == "completed"

    for row <- Enum.reject(rows, &(&1["run_ref"] == "valid")) do
      assert row["status"] == "invalid"
      assert row["run_id"] == nil
      assert row["last_seq"] == nil
      assert row["pending_repair"] == nil
      assert row["error"] in ~w(journal_missing journal_empty journal_invalid)
    end

    refute Jason.encode!(rows) =~ "PRIVATE_BAD_LINE_CANARY"
  end

  test "D6 direct and compound directory escapes are skipped independently", %{base: base} do
    root = Path.join(base, "root")
    outside = Path.join(base, "outside")
    fixture(Path.join(root, "inside"))
    fixture(Path.join(outside, "actual"))
    File.mkdir_p!(Path.join(outside, "deep"))
    File.ln_s!(Path.join(outside, "actual"), Path.join(root, "direct"))
    File.ln_s!(Path.join(outside, "deep"), Path.join(root, "bridge"))
    File.ln_s!("bridge/../actual", Path.join(root, "compound"))

    assert %{"runs" => [%{"run_ref" => "inside"}], "skipped_outside_root" => 3} = listing(root)
  end

  test "D6 relative configured root traverses a symlink before its parent component", %{base: base} do
    fixture(Path.join(base, "actual/lexical_wrong"))
    fixture(Path.join(base, "outside/actual/physical_right"))
    File.mkdir_p!(Path.join(base, "outside/deep"))
    File.ln_s!(Path.join(base, "outside/deep"), Path.join(base, "bridge"))
    assert %{"runs" => [%{"run_ref" => "physical_right"}]} = listing("bridge/../actual", cwd: base)
    assert %{status: 74} = CLI.run(["list", "--root", "missing/../actual"], cwd: base)
  end

  test "D7 duplicate run identities remain distinct and the next invocation reads fresh bytes", %{base: base} do
    fixture(Path.join(base, "one"))
    two = fixture(Path.join(base, "two"))
    assert [one, second] = listing(base)["runs"]
    assert one["run_id"] == second["run_id"]
    assert one["run_ref"] != second["run_ref"]
    fixture(two, "auth_blocked_pane")
    assert Enum.at(listing(base)["runs"], 1)["status"] == "blocked"
    File.rm_rf!(two)
    assert [%{"run_ref" => "one"}] = listing(base)["runs"]
  end

  test "D7 failed summary cannot retain the successful discovery identity", %{base: base} do
    fixture(Path.join(base, "vanishing"))

    assert [%{"run_id" => nil, "last_seq" => nil, "error" => "journal_missing"}] =
             listing(base, fs: {ReadOnlyFs, {self(), :vanish}})["runs"]
  end

  test "D8 nested exceptions are sanitized and do not hide unaffected siblings", %{base: base} do
    fixture(Path.join(base, "private"))
    fixture(Path.join(base, "good"))
    result = CLI.run(["list", "--root", base, "--json"], fs: {ReadOnlyFs, {self(), :raise}})
    assert result.status == 0

    assert [%{"run_ref" => "good", "status" => "completed"}, %{"run_id" => nil, "error" => "run_unavailable"}] =
             Jason.decode!(result.stdout)["runs"]

    refute result.stdout <> result.stderr =~ "PRIVATE_NESTED_CANARY"
    refute result.stdout <> result.stderr =~ "/private/elsewhere"
  end

  test "D8 JSON preserves valid refs while Org encodes links, pipes, backslashes and controls", %{base: base} do
    names = ["[[file:target]]|cell", "literal\\u005B", "line\nnext", "escape\e[31m"]
    for name <- names, do: fixture(Path.join(base, name))
    assert Enum.map(listing(base)["runs"], & &1["run_ref"]) == Enum.sort(names)
    assert %{status: 0, stdout: org} = CLI.run(["list", "--root", base])
    refute org =~ "[["
    refute org =~ "\e"
    refute org =~ "line\nnext"
    assert org =~ "\\u005B\\u005Bfile:target\\u005D\\u005D\\u007Ccell"
    assert org =~ "literal\\u005Cu005B"
    assert org =~ "line\\u000Anext"

    rows = org |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "| "))
    assert length(rows) == 5
    assert Enum.all?(rows, &(length(String.split(&1, "|")) == 8))
  end

  test "D8 non-UTF8 paths are sanitized, including invalid names where the filesystem permits them", %{base: base} do
    invalid = Path.join(base, <<255>>)

    case File.mkdir(invalid) do
      :ok ->
        fixture(invalid)
        assert %{status: 74, stdout: "", stderr: stderr} = CLI.run(["list", "--root", base, "--json"])
        assert Jason.decode!(stderr) == %{"reason" => "runs_root_unavailable"}

      {:error, :eilseq} ->
        # APFS refuses this fixture name; exercise the rejected explicit path
        # instead and leave actual invalid directory discovery to Linux CI.
        IO.puts("D8 filesystem rejects non-UTF8 directory names (eilseq); checking explicit invalid root")
        assert %{status: 74, stdout: "", stderr: stderr} = CLI.run(["list", "--root", invalid, "--json"])
        assert Jason.decode!(stderr)["reason"] in ~w(runs_root_missing runs_root_unavailable)
    end
  end

  test "D10 malformed explicit-root argv is refused", %{base: base} do
    for args <- [
          ["--root"],
          ["--root", base, "--root", base],
          ["--root", base, "--json", "--json"],
          ["--json", "--json", "--root", base],
          ["--root", base, "--unknown"],
          ["--root", base, "extra"],
          ["--root", "--json"],
          ["--no-json", "--root", base]
        ] do
      assert %{status: 64, stdout: "", stderr: stderr} = CLI.run(["list" | args])
      assert Jason.decode!(stderr)["reason"] == "usage"
    end
  end

  defp listing(root, opts \\ []) do
    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["list", "--root", root, "--json"], opts)
    Jason.decode!(stdout)
  end

  defp fixture(dir, name \\ "gated_run_seed") do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(F.lines("scenarios", name), "\n") <> "\n")
    dir
  end

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

  defp corrupt_first_line(dir) do
    path = Path.join(dir, "events.jsonl")
    [first | rest] = path |> File.read!() |> String.split("\n", trim: true)
    changed = first |> Jason.decode!() |> put_in(["data", "project"], "changed") |> Jason.encode!()
    File.write!(path, Enum.join([changed | rest], "\n") <> "\n")
  end

  defp snapshot(root) do
    root
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      path = Path.join(root, name)
      stat = File.lstat!(path)

      case stat.type do
        :directory -> [{path, {:directory, stat.mode}} | Map.to_list(snapshot(path))]
        :regular -> [{path, {:regular, stat.mode, File.read!(path)}}]
        :symlink -> [{path, {:symlink, stat.mode, File.read_link!(path)}}]
      end
    end)
    |> Map.new()
  end
end
