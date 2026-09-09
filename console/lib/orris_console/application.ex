defmodule OrrisConsole.Application do
  @moduledoc """
  Fail-closed startup (C1-01): the trusted configuration is loaded and installed, the endpoint's runtime settings
  (loopback bind, port, fresh signing secret, server flag) are set, then the supervision tree starts in this order:
  SessionStore (loads the credential; no implicit generation), QueryRegistry, QueryControllers, QueryWorkers, Endpoint.
  """
  use Application
  alias OrrisConsole.{Config, Redaction}

  @impl true
  def start(_type, _args) do
    with {:ok, config} <- load_config() do
      Config.install(config)
      Redaction.install(config)
      configure_endpoint(config)

      children = [
        {OrrisConsole.SessionStore, config},
        Supervisor.child_spec({Registry, keys: :unique, name: OrrisConsole.QueryRegistry},
          id: OrrisConsole.QueryRegistry
        ),
        {OrrisConsole.QueryControllers, config},
        {OrrisConsole.QueryWorkers, config},
        OrrisConsole.Endpoint
      ]

      Supervisor.start_link(children, strategy: :one_for_one, name: OrrisConsole.Supervisor)
    else
      {:error, reason} -> {:error, {:config, reason}}
    end
  end

  @impl true
  def stop(_state) do
    Redaction.uninstall()
    :ok
  end

  defp load_config do
    case {Application.get_env(:orris_console, :config), Application.get_env(:orris_console, :config_file)} do
      {input, _} when is_list(input) -> Config.load(input)
      {nil, path} when is_binary(path) -> Config.load_file(path)
      _ -> {:error, %{clause: "config_missing", detail: nil}}
    end
  end

  defp configure_endpoint(%Config{} = config) do
    existing = Application.get_env(:orris_console, OrrisConsole.Endpoint, [])

    runtime = [
      # HTTP/1.1 + websocket only in C1: HTTP/2 stays disabled until separately measured (contract)
      http: [ip: config.bind, port: config.port, http_2_options: [enabled: false]],
      server: config.server,
      url: [host: config.host, port: config.port, scheme: "http"],
      secret_key_base: Base.encode64(:crypto.strong_rand_bytes(48)),
      live_view: [signing_salt: Base.encode64(:crypto.strong_rand_bytes(16))]
    ]

    Application.put_env(:orris_console, OrrisConsole.Endpoint, Keyword.merge(existing, runtime))
  end
end
