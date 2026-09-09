defmodule AiOrchestrator.Contracts.ProductionEscriptTest do
  @moduledoc """
  docs/contracts/production-escript.org: the production escript is built ONCE, for real, in a copy of the project
  tree with a clean build path, then measured: exit, attribution, launch that opens a writer, packaged inventory.
  """

  use ExUnit.Case, async: false

  @moduletag :production_escript
  @moduletag timeout: 300_000

  @fixture "test/fixtures/contracts/scenarios/kill9_resume/events_pre_gate.jsonl"

  # The build runs in place (project root, fetched dependency sources reused) into a FRESH build path, so every
  # dependency and the application are compiled from clean; the artifact is moved out of bin/ at once. A copied
  # project tree was tried first and is not usable: rebar3 dependencies with include files (gproc first) fail
  # include resolution when compiled from a tree outside the project root.
  setup_all do
    root = File.cwd!()
    unique = System.unique_integer([:positive])
    work = Path.join(System.tmp_dir!(), "prod_escript_#{unique}")
    build_path = Path.join(Path.dirname(Mix.Project.build_path()), "prod-clean-#{unique}")
    evidence = Path.join(Mix.Project.build_path(), "production_escript")
    File.mkdir_p!(work)
    File.mkdir_p!(evidence)
    env = [{"MIX_ENV", "prod"}, {"MIX_BUILD_PATH", build_path}]
    {output, exit} = System.cmd("mix", ["escript.build"], cd: root, env: env, stderr_to_stdout: true)
    built = Path.join(root, "bin/ai-orchestrator")
    artifact = Path.join(work, "ai-orchestrator")
    if exit == 0 and File.regular?(built), do: File.rename!(built, artifact)
    File.write!(Path.join(evidence, "build.exit"), Integer.to_string(exit))
    File.write!(Path.join(evidence, "build.log"), output)
    build = %{exit: exit, output: output, artifact: artifact, work: work}

    on_exit(fn ->
      File.rm_rf!(work)
      File.rm_rf!(build_path)
    end)

    {:ok, build: build}
  end

  defp tail(output), do: output |> String.split("\n") |> Enum.take(-12) |> Enum.join("\n")

  test "PE-1 a clean production escript build succeeds and produces the artifact", %{build: build} do
    assert build.exit == 0, "production escript build failed (exit #{build.exit}):\n#{tail(build.output)}"
    assert File.regular?(build.artifact)
  end

  test "PE-2 the production build never invokes the dev/test-only Boundary compiler", %{build: build} do
    refute build.output =~ "compile.boundary",
           "the production build invoked a dev/test-only compiler:\n#{tail(build.output)}"
  end

  test "PE-3 the production artifact launches as an OS process and opens a writer", %{build: build} do
    assert build.exit == 0 and File.regular?(build.artifact),
           "no production artifact to launch (build exit #{build.exit})"

    run_dir = Path.join(build.work, "probe_run")
    File.mkdir_p!(run_dir)
    File.cp!(@fixture, Path.join(run_dir, "events.jsonl"))
    {out, status} = System.cmd(build.artifact, ["cancel", run_dir], stderr_to_stdout: false)
    assert status == 0, "probe command failed (exit #{status}): #{tail(out)}"
    assert String.starts_with?(out, "#+title: Run summary"), "stdout is not the structured run summary: #{tail(out)}"
    assert out =~ ~r/^\* Status: cancelled$/m
    assert File.regular?(Path.join(run_dir, "events.head")), "no writer ever opened the probe run directory"
  end

  test "PE-4 the packaged inventory is the application, its runtime and its production closure only", %{build: build} do
    assert build.exit == 0 and File.regular?(build.artifact),
           "no production artifact to inspect (build exit #{build.exit})"

    packaged = packaged_apps(build.artifact)
    deps = Mix.Project.config()[:deps]
    forbidden = for {name, _, opts} <- deps, only = opts[:only], :prod not in List.wrap(only), do: name
    required = for dep <- deps, is_nil(dep_opts(dep)[:only]), do: elem(dep, 0)

    assert forbidden != [] and required != []
    assert MapSet.subset?(MapSet.new([:ai_orchestrator, :elixir, :logger | required]), packaged), inspect(packaged)

    assert MapSet.disjoint?(MapSet.new(forbidden), packaged),
           "dev/test-only dependencies are packaged: #{inspect(Enum.filter(forbidden, &(&1 in packaged)))}"
  end

  test "PE-5 the dev/test Boundary compiler gate is preserved" do
    assert :boundary in Mix.Project.config()[:compilers]
  end

  defp dep_opts({_name, opts}) when is_list(opts), do: opts
  defp dep_opts({_name, _req, opts}), do: opts
  defp dep_opts(_), do: []

  # an escript is a shebang header followed by a zip archive; every packaged application appears as <app>/ebin/
  defp packaged_apps(artifact) do
    bytes = File.read!(artifact)
    {offset, _} = :binary.match(bytes, "PK\x03\x04")
    {:ok, entries} = :zip.list_dir(binary_part(bytes, offset, byte_size(bytes) - offset))

    for {:zip_file, name, _info, _comment, _offset, _size} <- entries,
        [app, "ebin" | _] <- [name |> to_string() |> Path.split()],
        into: MapSet.new(),
        do: String.to_atom(app)
  end
end
