defmodule AiOrchestrator.MixProject do
  use Mix.Project

  def project do
    [
      app: :ai_orchestrator,
      version: "0.1.0-dev",
      package: [licenses: ["Apache-2.0"]],
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      compilers: [:boundary] ++ Mix.compilers(),
      boundary: [
        default: [
          check: [aliases: true]
        ]
      ],
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mix],
        ignore_warnings: ".dialyzer_ignore.exs",
        list_unused_filters: true
      ],
      # The escript starts the application because the host root owns the
      # ownership arbiter every journal writer acquires through; without it the
      # CLI would open no writer at all.
      escript: [
        main_module: AiOrchestrator.CLI,
        name: "ai-orchestrator",
        path: "bin/ai-orchestrator",
        app: :ai_orchestrator
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        "boundary.spec": :test,
        credo: :test,
        "deps.audit": :test
      ]
    ]
  end

  def application do
    [
      # `:inets` is a startup prerequisite, not a preference. The generated
      # application list starts `:opentelemetry` before
      # `:opentelemetry_exporter`, and the SDK can initialise the configured
      # OTLP exporter before the exporter application has started the `:inets`
      # that exporter needs. When it loses that race the exporter dies with
      # `{:error, :inets_not_started}` and the process runs on with no
      # telemetry at all -- from identical inputs, nondeterministically.
      # Starting `:inets` here removes the race rather than the symptom, and
      # `test/application_test.exs` pins the resulting order.
      extra_applications: [:logger, :inets],
      mod: {AiOrchestrator.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:zoi, "~> 0.18.7"},
      {:telemetry, "~> 1.4"},
      {:opentelemetry_api, "~> 1.5"},
      {:opentelemetry, "~> 1.7"},
      {:opentelemetry_exporter, "~> 1.10"},
      # available in every build environment: production modules use Boundary macros at compile time;
      # runtime: false keeps it out of the application startup list (docs/contracts/production-escript.org)
      {:boundary, "~> 0.10.4", runtime: false},
      {:usage_rules, "~> 1.2", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.4", only: :test},
      {:styler, "~> 1.12", only: [:dev, :test], runtime: false}
    ]
  end
end
