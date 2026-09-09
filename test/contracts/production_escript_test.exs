defmodule AiOrchestrator.Contracts.ProductionEscriptTest do
  @moduledoc """
  docs/contracts/production-escript.org: the production escript is built ONCE, for real, in place at the project
  root into a freshly created private build directory, then measured: exit, packaged inventory against an exact
  oracle, the build-only exception, and a launch that opens a writer. A pre-existing operator artifact in bin/ is
  preserved byte for byte and restored.
  """

  use ExUnit.Case, async: false

  @moduletag :production_escript
  @moduletag timeout: 300_000

  @fixture "test/fixtures/contracts/scenarios/kill9_resume/events_pre_gate.jsonl"
  @artifact_path "bin/ai-orchestrator"

  # The exact expected packaged application set (docs/contracts/production-escript.org, PE-4): the application,
  # the runtime applications the escript carries (elixir, logger), the production dependency closure, and the one
  # explicit BUILD-ONLY exception (boundary: its macros are needed to compile 22 production modules; it is not a
  # runtime application). A dependency change must update this pin AND the contract together.
  @build_only [:boundary]
  @pinned_production_closure ~w(acceptor_pool chatterbox ctx gproc grpcbox hpack jason opentelemetry
    opentelemetry_api opentelemetry_exporter ssl_verify_fun telemetry tls_certificate_check zoi)a
  @expected_packaged MapSet.new([:ai_orchestrator, :elixir, :logger] ++ @build_only ++ @pinned_production_closure)

  setup_all do
    root = File.cwd!()
    original = preserve(Path.join(root, @artifact_path))
    build_path = fresh_directory(Path.dirname(Mix.Project.build_path()), "prod-clean")
    work = fresh_directory(System.tmp_dir!(), "prod_escript")
    evidence = Path.join(Mix.Project.build_path(), "production_escript")
    File.mkdir_p!(evidence)
    # cleanup is registered BEFORE the build so a failing build never leaves the build directory or the artifact
    on_exit(fn ->
      restore(Path.join(root, @artifact_path), original)
      File.rm_rf!(work)
      File.rm_rf!(build_path)
    end)

    assert File.ls!(build_path) == [], "the fresh build directory is not empty"
    env = [{"MIX_ENV", "prod"}, {"MIX_BUILD_PATH", build_path}]
    {output, exit} = System.cmd("mix", ["escript.build"], cd: root, env: env, stderr_to_stdout: true)
    built = Path.join(root, @artifact_path)
    artifact = Path.join(work, "ai-orchestrator")
    if File.regular?(built) and original == nil, do: File.rename!(built, artifact)
    if File.regular?(built) and original != nil, do: File.cp!(built, artifact)
    File.write!(Path.join(evidence, "build.exit"), Integer.to_string(exit))
    File.write!(Path.join(evidence, "build.log"), output)
    {:ok, build: %{exit: exit, output: output, artifact: artifact, work: work, evidence: evidence}}
  end

  # ---- rows ----

  test "PE-1 a clean production escript build succeeds and produces the artifact", %{build: build} do
    assert build.exit == 0, "production escript build failed (exit #{build.exit}):\n#{tail(build.output)}"
    assert File.regular?(build.artifact)
  end

  test "PE-2 build-only Boundary is packaged but is not a runtime application dependency", %{build: build} do
    assert artifact?(build), "no production artifact to inspect (build exit #{build.exit})"
    {:ok, applications} = runtime_applications(build.artifact)
    refute :boundary in applications, "boundary is started as a runtime application: #{inspect(applications)}"
    assert :boundary in packaged_apps(build.artifact), "the build-only exception is not packaged as measured"
  end

  test "PE-3 the production artifact launches as an OS process and opens a writer", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    run_dir = Path.join(build.work, "probe_run")
    File.mkdir_p!(run_dir)
    File.cp!(@fixture, Path.join(run_dir, "events.jsonl"))
    stderr = Path.join(build.evidence, "launch.stderr")
    script = ~s("$0" cancel "$1" 2>"$2")
    {out, status} = System.cmd("sh", ["-c", script, build.artifact, run_dir, stderr])
    File.write!(Path.join(build.evidence, "launch.stdout"), out)
    File.write!(Path.join(build.evidence, "launch.exit"), Integer.to_string(status))
    assert status == 0, "probe command failed (exit #{status}): #{tail(out)}\nstderr: #{tail(File.read!(stderr))}"
    assert String.starts_with?(out, "#+title: Run summary"), "stdout is not the structured run summary: #{tail(out)}"
    assert out =~ ~r/^\* Status: cancelled$/m
    assert File.regular?(Path.join(run_dir, "events.head")), "no writer ever opened the probe run directory"
  end

  test "PE-4 the packaged inventory equals the exact expected set", %{build: build} do
    assert artifact?(build), "no production artifact to inspect (build exit #{build.exit})"
    assert oracle_verdict(@expected_packaged, packaged_apps(build.artifact)) == :ok
  end

  test "PE-4a the pinned expected set agrees with Mix's own production dependency graph" do
    derived = MapSet.new([:ai_orchestrator, :elixir, :logger] ++ @build_only ++ production_graph())
    assert oracle_verdict(@expected_packaged, derived) == :ok
  end

  test "PE-5 the dev/test Boundary compiler gate is preserved" do
    assert :boundary in Mix.Project.config()[:compilers]
  end

  # ---- oracle controls: the comparison rejects both directions ----

  test "CO-1 an extra dev-only transitive application is rejected by the oracle" do
    actual = MapSet.put(@expected_packaged, :sourceror)
    assert {:error, %{extra: [:sourceror], missing: []}} = oracle_verdict(@expected_packaged, actual)
  end

  test "CO-2 a missing required transitive application is rejected by the oracle" do
    actual = MapSet.delete(@expected_packaged, :grpcbox)
    assert {:error, %{extra: [], missing: [:grpcbox]}} = oracle_verdict(@expected_packaged, actual)
  end

  # ---- helpers ----

  defp tail(output), do: output |> String.split("\n") |> Enum.take(-12) |> Enum.join("\n")

  defp artifact?(build), do: build.exit == 0 and File.regular?(build.artifact)

  defp oracle_verdict(expected, actual) do
    extra = actual |> MapSet.difference(expected) |> Enum.sort()
    missing = expected |> MapSet.difference(actual) |> Enum.sort()
    if extra == [] and missing == [], do: :ok, else: {:error, %{extra: extra, missing: missing}}
  end

  # Mix's own production dependency graph, read through the CLI (independent of the archive under test)
  defp production_graph do
    {out, 0} = System.cmd("mix", ["deps.tree", "--only", "prod", "--format", "plain"], stderr_to_stdout: true)

    for line <- String.split(out, "\n"),
        [_, app] <- [Regex.run(~r/^[\s|`-]*-- ([a-z][a-z0-9_]*) /, line)],
        uniq: true,
        do: String.to_atom(app)
  end

  # an escript is a shebang header followed by a zip archive; every packaged application appears as <app>/ebin/
  defp archive(artifact) do
    bytes = File.read!(artifact)
    {offset, _} = :binary.match(bytes, "PK\x03\x04")
    binary_part(bytes, offset, byte_size(bytes) - offset)
  end

  defp packaged_apps(artifact) do
    {:ok, entries} = :zip.list_dir(archive(artifact))

    for {:zip_file, name, _info, _comment, _offset, _size} <- entries,
        [app, "ebin" | _] <- [name |> to_string() |> Path.split()],
        into: MapSet.new(),
        do: String.to_atom(app)
  end

  # the runtime startup closure is what the packaged .app file declares, separate from package contents
  defp runtime_applications(artifact) do
    entry = ~c"ai_orchestrator/ebin/ai_orchestrator.app"

    with {:ok, [{^entry, bytes}]} <- :zip.extract(archive(artifact), [:memory, {:file_list, [entry]}]),
         {:ok, tokens, _} <- :erl_scan.string(String.to_charlist(bytes)),
         {:ok, {:application, :ai_orchestrator, props}} <- :erl_parse.parse_term(tokens) do
      {:ok, Keyword.get(props, :applications, [])}
    end
  end

  # exclusive creation with collision retry: a repeatable name never establishes a fresh directory
  defp fresh_directory(parent, prefix, attempt \\ 0) do
    path = Path.join(parent, "#{prefix}-#{Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)}")

    case File.mkdir(path) do
      :ok -> path
      {:error, :eexist} when attempt < 8 -> fresh_directory(parent, prefix, attempt + 1)
      {:error, reason} -> raise "cannot create a fresh directory under #{parent}: #{inspect(reason)}"
    end
  end

  defp preserve(path) do
    if File.regular?(path), do: {File.read!(path), File.stat!(path).mode}
  end

  defp restore(path, nil), do: File.rm(path)

  defp restore(path, {bytes, mode}) do
    File.write!(path, bytes)
    File.chmod!(path, mode)
  end
end
