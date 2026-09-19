defmodule AiOrchestrator.Contracts.ProductionEscriptTest do
  @moduledoc """
  docs/contracts/production-escript.org: the production escript is built ONCE, for real, in place at the project
  root into a freshly created private build directory, then measured: exit, packaged inventory against an exact
  oracle, the build-only exception, and a launch that opens a writer. A pre-existing operator artifact in bin/ is
  preserved byte for byte and restored.

  The IE rows (NS-32.M.001) ask that same artifact what it IS -- source revision, IPC protocol version, build
  identity -- and hold each answer to something measured independently of the artifact: the repository's own HEAD,
  the pinned v2 fixture bytes, and the running toolchain. They reuse the one build above; no second build, and no
  new machinery.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Test.StoredState

  @moduletag :production_escript
  @moduletag timeout: 300_000

  @fixture "test/fixtures/contracts/scenarios/kill9_resume/events_pre_gate.jsonl"
  # a recorded status that is NOT terminal, so `status --watch` keeps looping instead of ending on its own
  @blocked_fixture "test/fixtures/contracts/scenarios/auth_blocked_pane/events.jsonl"
  @artifact_path "bin/ai-orchestrator"

  # The exact expected packaged application set (docs/contracts/production-escript.org, PE-4): the application,
  # the runtime applications the escript carries (elixir, logger), the production dependency closure, and the one
  # explicit BUILD-ONLY exception (boundary: its macros are needed to compile 22 production modules; it is not a
  # runtime application). A dependency change must update this pin AND the contract together.
  @build_only [:boundary]
  @pinned_production_closure ~w(acceptor_pool chatterbox ctx gproc grpcbox hpack jason opentelemetry
    opentelemetry_api opentelemetry_exporter ssl_verify_fun telemetry tls_certificate_check zoi)a
  @expected_packaged MapSet.new([:ai_orchestrator, :elixir, :logger] ++ @build_only ++ @pinned_production_closure)

  # The exact identity surface the artifact publishes. A field added or dropped without review fails IE-1.
  @identity_keys ~w(application artifact build_elixir build_env build_otp ipc_protocol_version product
    source_revision source_worktree version)
  @v2_fixtures "test/fixtures/contracts/ipc/v2/*.json"

  setup_all do
    root = File.cwd!()
    # taken BEFORE the build: PE-8 holds this module to leaving the working tree as it found it,
    # which is the control bin/verify used to perform around its own duplicate escript build
    {dirt_before, 0} = System.cmd("git", ["status", "--porcelain"], cd: root)
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

    build = %{
      exit: exit,
      output: output,
      artifact: artifact,
      work: work,
      evidence: evidence,
      dirt_before: dirt_before,
      root: root
    }

    {:ok, build: build}
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
    {out, status} = System.cmd("bash", ["-c", script, build.artifact, run_dir, stderr])
    File.write!(Path.join(build.evidence, "launch.stdout"), out)
    File.write!(Path.join(build.evidence, "launch.exit"), Integer.to_string(status))
    assert status == 0, "probe command failed (exit #{status}): #{tail(out)}\nstderr: #{tail(File.read!(stderr))}"
    assert String.starts_with?(out, "#+title: Run summary"), "stdout is not the structured run summary: #{tail(out)}"
    assert out =~ ~r/^\* Status: cancelled$/m
    assert File.regular?(Path.join(run_dir, "events.head")), "no writer ever opened the probe run directory"

    diagnostics = File.read!(stderr)

    # stderr may carry third-party diagnostics; it may never carry the result
    refute diagnostics =~ ~r/^#\+title:|^\{"/m,
           "stderr carries result output, which belongs on stdout alone: #{tail(diagnostics)}"

    # One diagnostic is never legitimate: it means the OTLP exporter reached initialisation before `:inets` was
    # started and the whole process is running with no telemetry at all, from identical inputs and
    # nondeterministically. test/application_test.exs is the deterministic guard -- it pins the application order
    # that makes the race impossible. This is the end-to-end backstop, in the only process that actually starts
    # the application the way an operator does.
    refute diagnostics =~ "OTLP exporter failed to initialize",
           "the OTLP exporter lost its startup race with :inets"
  end

  # Building and launching a real artifact in place at the project root is the one thing in this suite that could
  # leave something behind. bin/verify used to assert this around its own duplicate build; the assertion belongs
  # with the build it is about.
  test "PE-8 building and launching the production artifact leaves the working tree as it found it", %{build: build} do
    assert artifact?(build), "no production artifact was built (build exit #{build.exit})"
    {dirt_now, 0} = System.cmd("git", ["status", "--porcelain"], cd: build.root)

    assert dirt_now == build.dirt_before,
           "the production escript build or launch changed the working tree:\nbefore:\n#{build.dirt_before}after:\n#{dirt_now}"
  end

  test "PE-4 the packaged inventory equals the exact expected set", %{build: build} do
    assert artifact?(build), "no production artifact to inspect (build exit #{build.exit})"
    assert oracle_verdict(@expected_packaged, packaged_apps(build.artifact)) == :ok
  end

  # NS-11.H.001 / NS-11.K.001, the packaged-archive leg: test/contracts/ns11_projection_independence_test.exs binds
  # that row to what the application DECLARES it starts and to what the production dependency graph CONTAINS; this
  # one binds it to the bytes actually shipped, reusing the single build above. It is not PE-4 restated: PE-4 pins an
  # exact set, and a deliberate dependency change updates that pin and stays green. This row says what such an update
  # may never contain, so adding ecto, Ash or Oban fails here even with the pin and the contract updated together.
  test "PE-6 the packaged archive carries no SQL, Ash or Oban application", %{build: build} do
    assert artifact?(build), "no production artifact to inspect (build exit #{build.exit})"
    packaged = packaged_apps(build.artifact)

    # a witness first, so an empty or unreadable archive cannot satisfy the emptiness that follows
    assert :jason in packaged, "the measured packaged set is empty or unreadable: #{inspect(Enum.sort(packaged))}"
    assert StoredState.named(packaged) == []
  end

  test "PE-4a the pinned expected set agrees with Mix's own production dependency graph" do
    derived = MapSet.new([:ai_orchestrator, :elixir, :logger] ++ @build_only ++ production_graph())
    assert oracle_verdict(@expected_packaged, derived) == :ok
  end

  test "PE-5 the dev/test Boundary compiler gate is preserved" do
    assert :boundary in Mix.Project.config()[:compilers]
  end

  # The interrupt leg of `status --watch` (lib/ai_orchestrator/cli/watch.ex): the loop ends on a terminal recorded
  # status, on the `--for-ms` horizon, after a bounded cycle count -- each of which test/cli/cli_watch_test.exs
  # drives with injected seams -- "or when the operator interrupts the foreground process", which no row asserted.
  # It has to be THIS artifact. Measured on 2026-09-18 with the same Port shape, run directory and signal:
  #   elixir <script>                  STILL RUNNING 12s after SIGINT   (the ERTS break handler takes the signal
  #   elixir --erl "-noinput" <script> STILL RUNNING 12s after SIGINT    and waits for a keystroke that a pipe
  #   elixir --erl "+Bi" <script>      STILL RUNNING 12s after SIGINT    never delivers)
  #   elixir --erl "+Bd" <script>      EXITED status=130
  #   bin/ai-orchestrator (escript)    EXITED status=130
  # So the row is a property of the packaged escript, which starts its emulator with the break handler off, and a
  # version of it run under bare `elixir` would hang rather than pass. 130 is 128 + SIGINT: the runtime exits on
  # the signal itself. The loop installs no handler -- that would be exactly the run-owned machinery this verb
  # must not add -- and needs none, because it holds no lock, owns no timer and writes nothing.
  test "PE-7 an interrupt ends the foreground watch process, which has written nothing", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    run_dir = Path.join(build.work, "watch_run")
    File.mkdir_p!(run_dir)
    File.cp!(@blocked_fixture, Path.join(run_dir, "events.jsonl"))
    before = directory_digest(run_dir)

    # a ten-minute horizon against the bounded waits below: an end inside them is the signal, never the horizon
    port =
      Port.open(
        {:spawn_executable, build.artifact},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: ["status", "--watch", "--interval-ms", "50", "--for-ms", "600000", run_dir]
        ]
      )

    # the loop has rendered, so the process is inside it rather than still starting the runtime
    rendered = await(port, "* Status: BLOCKED", 120_000)
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    assert {_out, 0} = System.cmd("kill", ["-INT", Integer.to_string(os_pid)], stderr_to_stdout: true)
    {status, output} = drain(port, rendered, 30_000)

    assert status == 130, "the interrupted watch exited #{status}:\n#{tail(output)}"
    # an interrupted read leaves the run exactly as it found it: no repair, no receipt, no projection
    assert directory_digest(run_dir) == before
  end

  # ---- build identity: the artifact says what it is, and every answer is held to an outside measurement ----

  test "IE-1 the artifact answers `version --json` with exactly the declared identity keys", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    assert Enum.sort(Map.keys(identity(build))) == Enum.sort(@identity_keys)
  end

  # The strong row: the artifact's claim about its own provenance is compared against this repository's HEAD,
  # read here rather than taken from the artifact. A hard-coded, stale or fabricated revision fails here.
  test "IE-2 the reported source revision is the revision the artifact was built from", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    report = identity(build)
    {head, 0} = System.cmd("git", ["rev-parse", "HEAD"])

    assert report["source_revision"] == String.trim(head)
    assert report["source_revision"] =~ ~r/\A[0-9a-f]{40}\z/
    assert report["source_worktree"] in ["clean", "modified"]
  end

  # The protocol version is not a free-standing literal: it is held to the number the pinned v2 fixture bytes
  # declare, so bumping the wire version without the contract fixtures (or the reverse) fails here.
  test "IE-3 the reported protocol version is the version the pinned v2 fixtures declare", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    paths = Path.wildcard(@v2_fixtures)
    assert paths != [], "the v2 fixture set is missing, so this row would be vacuous"

    declared =
      for path <- paths,
          into: MapSet.new(),
          do: path |> File.read!() |> Jason.decode!() |> Map.fetch!("protocol_version")

    assert MapSet.size(declared) == 1, "the v2 fixtures disagree on protocol_version: #{inspect(declared)}"
    assert identity(build)["ipc_protocol_version"] == declared |> MapSet.to_list() |> List.first()
  end

  # `build_env` is not decoration: it is what makes PE-4's packaged inventory mean anything. An artifact built in
  # dev carries a different closure, so a dev build reaching this row is a finding rather than a passing test.
  test "IE-4 the reported build identity is the toolchain and environment that built it", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    report = identity(build)

    assert report["build_elixir"] == System.version()
    assert report["build_otp"] == otp_version()
    assert report["build_env"] == "prod"
    assert report["version"] == to_string(Mix.Project.config()[:version])
    assert report["application"] == to_string(Mix.Project.config()[:app])
    assert report["artifact"] == to_string(Mix.Project.config()[:escript][:name])
    assert report["product"] == "orris"
  end

  test "IE-5 the org rendering carries every field the JSON reports", %{build: build} do
    assert artifact?(build), "no production artifact to launch (build exit #{build.exit})"
    report = identity(build)
    {out, 0} = System.cmd(build.artifact, ["version"])

    assert String.starts_with?(out, "#+title: Build identity")
    for {key, value} <- report, do: assert(out =~ "- #{key}: #{value}")
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

  # bounded wait for a marker in a running process's output; an early exit or a silent process is a failure here
  defp await(port, marker, timeout, acc \\ "") do
    if String.contains?(acc, marker) do
      acc
    else
      receive do
        {^port, {:data, chunk}} -> await(port, marker, timeout, acc <> chunk)
        {^port, {:exit_status, status}} -> flunk("the watch exited (#{status}) before rendering #{marker}: #{acc}")
      after
        timeout -> flunk("the watch never rendered #{marker}: #{acc}")
      end
    end
  end

  defp drain(port, acc, timeout) do
    receive do
      {^port, {:data, chunk}} -> drain(port, acc <> chunk, timeout)
      {^port, {:exit_status, status}} -> {status, acc}
    after
      timeout -> flunk("the interrupted watch did not exit: #{acc}")
    end
  end

  defp directory_digest(dir) do
    dir
    |> Path.join("**")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.sort()
    |> Enum.map(fn path ->
      {Path.relative_to(path, dir), :sha256 |> :crypto.hash(File.read!(path)) |> Base.encode16(case: :lower)}
    end)
  end

  defp artifact?(build), do: build.exit == 0 and File.regular?(build.artifact)

  # the artifact answers as an OS process; a non-zero exit or any non-JSON stdout is the row's failure
  defp identity(build) do
    {out, status} = System.cmd(build.artifact, ["version", "--json"])
    assert status == 0, "the artifact refused `version --json` (exit #{status}): #{tail(out)}"
    Jason.decode!(out)
  end

  # the running OTP version, read the way bin/verify reads it, independent of what the artifact claims
  defp otp_version do
    release = List.to_string(:erlang.system_info(:otp_release))
    path = Path.join([List.to_string(:code.root_dir()), "releases", release, "OTP_VERSION"])

    case File.read(path) do
      {:ok, contents} -> String.trim(contents)
      {:error, _reason} -> release
    end
  end

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
