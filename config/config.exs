import Config

config :ai_orchestrator,
  fingerprint_policy: :escalate,
  interface_mode: :ai_pair_local_pane

# The escript starts the OTP application, so every application this one
# depends on now starts inside the operator's CLI process too. Nothing here
# emits a log line or a span yet, so all of that output is third-party
# startup noise arriving on the stream the CLI writes JSON to.
#
# stdout belongs to that JSON. Warnings and errors still reach the operator,
# on the stream that cannot corrupt a parsed result.
config :logger, :default_handler, config: %{type: :standard_error}
config :logger, level: :warning
