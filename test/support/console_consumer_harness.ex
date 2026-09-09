defmodule AiOrchestrator.Test.ConsoleConsumerHarness do
  @moduledoc """
  DISPOSABLE TEST WITNESS (test support only, never product code): generates a throwaway consumer Mix project in a
  temporary directory with a path dependency on this repository, the Boundary compiler enabled and
  `use Boundary, deps: [AiOrchestrator], exports: []`, then compiles a given consumer module with
  `--warnings-as-errors` and returns the numeric exit plus the diagnostics. docs/contracts/public-console-seam.org.
  """

  @doc "Builds the consumer project once; returns its directory. Dependencies are fetched into the project itself."
  @spec build!() :: Path.t()
  def build! do
    root = File.cwd!()
    # under the project's own build directory, not the shell's TMPDIR: rebar3 dependency compiles (opentelemetry,
    # gproc) fail include resolution when the consumer lives under the nix-shell temporary directory (measured)
    dir = Path.join(Mix.Project.build_path(), "console_consumer_#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(dir, "lib"))

    File.write!(Path.join(dir, "mix.exs"), """
    defmodule ConsoleConsumer.MixProject do
      use Mix.Project
      def project do
        [app: :console_consumer, version: "0.0.0", elixir: "~> 1.19",
         compilers: [:boundary] ++ Mix.compilers(), deps: deps()]
      end
      def application, do: [extra_applications: [:logger]]
      defp deps do
        [{:ai_orchestrator, path: #{inspect(root)}}, {:boundary, "~> 0.10.4", runtime: false}]
      end
    end
    """)

    File.write!(Path.join(dir, "lib/console_consumer.ex"), """
    defmodule ConsoleConsumer do
      use Boundary, deps: [AiOrchestrator], exports: []
      @moduledoc false
    end
    """)

    File.cp!(Path.join(root, "mix.lock"), Path.join(dir, "mix.lock"))
    {_, 0} = System.cmd("mix", ["deps.get"], cd: dir, env: isolated_env(dir), stderr_to_stdout: true)
    dir
  end

  @doc "Writes `source` (a `ConsoleConsumer.Probe` module, inside the consumer boundary) and compiles with --warnings-as-errors; {exit, output}."
  @spec compile(Path.t(), String.t()) :: {non_neg_integer(), String.t()}
  def compile(dir, source) do
    File.write!(Path.join(dir, "lib/probe.ex"), source)

    {out, exit} =
      System.cmd("mix", ["compile", "--force", "--warnings-as-errors"],
        cd: dir,
        env: isolated_env(dir),
        stderr_to_stdout: true
      )

    {exit, out}
  end

  @doc """
  Runs `mix run -e expression` INSIDE the external consumer project (its own BEAM; `mix run` starts the consumer
  application and its dependencies, including the core), returning {exit, stdout}. The probe module is expected to
  print exactly one line of JSON; the caller decodes it. Nothing private is referenced: the expression calls only
  `ConsoleConsumer.Probe` functions.
  """
  @spec run(Path.t(), String.t()) :: {non_neg_integer(), String.t()}
  def run(dir, expression) do
    {out, exit} =
      System.cmd("mix", ["run", "-e", expression], cd: dir, env: isolated_env(dir), stderr_to_stdout: true)

    {exit, out}
  end

  @doc "The last line of `out` that decodes as JSON, or nil."
  @spec json_line(String.t()) :: map() | nil
  def json_line(out) do
    out
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find_value(fn line ->
      case Jason.decode(line) do
        {:ok, %{} = map} -> map
        _ -> nil
      end
    end)
  end

  # the consumer owns its build and deps directories; the flake's cwd-derived paths of the repository are not inherited
  defp isolated_env(dir) do
    [
      {"MIX_ENV", "dev"},
      {"MIX_BUILD_ROOT", Path.join(dir, "_build")},
      {"MIX_DEPS_PATH", Path.join(dir, "deps")},
      {"MIX_BUILD_PATH", nil}
    ]
  end
end
