import Config

config :phoenix, :json_library, Jason
# the framework's request/params logger is off: parameters, cookies and connect params are sensitive (contract R6)
config :phoenix, :logger, false
# supervisor (SASL) reports stay visible: a crashed child is a diagnostic the operator needs; OrrisConsole.Redaction
# scrubs every configured path and secret-shaped token from them
config :logger, level: :warning, handle_sasl_reports: true

# static endpoint configuration; runtime values (bind, port, secret, server) are set by OrrisConsole.Application from
# the trusted configuration before the endpoint starts
config :orris_console, OrrisConsole.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [html: OrrisConsole.ErrorHTML], layout: false],
  check_origin: false,
  code_reloader: false,
  debug_errors: false

# Test-only settings: the pinned headless browser path for the C1-14/H-4 rows comes from an explicit setting.
if config_env() == :test do
  config :orris_console, :c1_browser, System.get_env("C1_BROWSER")
end

# Trusted server configuration is read by OrrisConsole.Config.load/1 from `config :orris_console, :config`
# (config/runtime.exs in GREEN); tests put a disposable keyword there. Nothing here is a browser parameter.
