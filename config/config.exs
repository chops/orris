import Config

config :ai_orchestrator,
  fingerprint_policy: :escalate,
  interface_mode: :ai_pair_local_pane

# The escript starts the OTP application, so every application this one
# depends on now starts inside the operator's CLI process too. Nothing here
# emits a log line, and every span it emits ends in the no-op exporter selected
# at the bottom of this file, so all of that output is third-party startup noise
# arriving on the stream the CLI writes JSON to.
#
# stdout belongs to that JSON. Warnings and errors still reach the operator,
# on the stream that cannot corrupt a parsed result.
config :logger, :default_handler, config: %{type: :standard_error}
config :logger, level: :warning

# The application-boundary handler now produces spans for every instrumented
# lifecycle boundary (docs/contracts/lifecycle-telemetry.org), so the SDK is
# reached by domain code for the first time. Export stays OFF until an operator
# turns it on, because the alternative is that every CLI invocation tries to
# reach a collector nobody configured and reports the failure on the operator's
# stderr -- an observability default that changes what the operator sees.
#
# `:none` selects the SDK's no-op exporter: spans are still created, sampled and
# ended, so the producer is exercised, and nothing leaves the process. An
# operator with a collector sets the ordinary `OTEL_TRACES_EXPORTER` /
# `OTEL_EXPORTER_OTLP_ENDPOINT` environment, which the SDK merges over this.
config :opentelemetry, traces_exporter: :none
