defmodule OrrisConsole.MixProject do
  use Mix.Project

  # C1 scaffold (docs/contracts/console-readonly.org): a separate console project inside the Orris repository (OPEN-02),
  # path dependency on the core, its own lock/deps/build. Dependency versions are the B2-measured lock, pinned exactly.
  def project do
    [
      app: :orris_console,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      compilers: [:boundary] ++ Mix.compilers(),
      boundary: [default: [check: [aliases: true]]],
      elixirc_paths: elixirc_paths(Mix.env()),
      releases: releases(),
      # rows start the console themselves under a disposable configuration (fail-closed startup is itself a row)
      aliases: [test: "test --no-start"],
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger, :crypto], mod: {OrrisConsole.Application, []}]

  # the production release boots the console alone: configuration comes from the closed JSON file named by
  # ORRIS_CONSOLE_CONFIG_FILE (config/runtime.exs); no listener is enabled by the release itself
  def releases, do: [orris_console: [include_executables_for: [:unix], applications: [orris_console: :permanent]]]

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ai_orchestrator, path: ".."},
      {:phoenix, "1.8.13"},
      {:phoenix_live_view, "1.2.11"},
      {:phoenix_html, "4.3.0"},
      {:bandit, "1.12.5"},
      {:jason, "1.4.5"},
      {:boundary, "0.10.4", runtime: false},
      # test only: Phoenix.LiveViewTest's HTML parser (never in the release; recorded in the console lock)
      {:lazy_html, "~> 0.1", only: :test}
    ]
  end
end
