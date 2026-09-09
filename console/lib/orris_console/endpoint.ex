defmodule OrrisConsole.Endpoint do
  @moduledoc """
  The console endpoint: `call/2` runs the ingress guard BEFORE the framework (C1-03), then the compiled pipeline
  (static assets, body limit, parsers, session, router). The LiveView socket is compiled at "/live"; a differently
  configured mount is rewritten to it by the guard after admission. Signed, unencrypted cookie session (HttpOnly,
  SameSite=Strict, host-only, path "/"); the signing secret is fresh at every boot.
  """
  use Phoenix.Endpoint, otp_app: :orris_console

  @session_options [
    store: :cookie,
    key: "_orris_console_key",
    signing_salt: "orris-console-session",
    same_site: "Strict",
    http_only: true,
    path: "/"
  ]

  @doc "The Plug session options (public configuration; the raw session id lives inside the signed cookie)."
  def session_options, do: @session_options

  socket "/live", OrrisConsole.Socket, websocket: [connect_info: [session: @session_options]], longpoll: false

  plug Plug.Static, at: "/assets", from: {:orris_console, "priv/static/assets"}, gzip: false
  plug OrrisConsole.Plugs.BodyLimit
  # the login body limit is enforced on the bytes actually read (declared or chunked) by the runtime-bound parser plug
  plug OrrisConsole.Plugs.Parsers
  plug Plug.MethodOverride
  plug Plug.Head
  plug Plug.Session, @session_options
  plug OrrisConsole.Router

  @impl true
  def call(conn, opts) do
    conn = OrrisConsole.Ingress.call(conn, OrrisConsole.Config.current())

    if conn.halted do
      conn
    else
      super(conn, opts)
    end
  rescue
    # the framework rendered the error page (404/413/...) and re-raised the wrapper: the sent response stands
    e in Plug.Conn.WrapperError ->
      if e.conn.state in [:sent, :chunked], do: e.conn, else: reraise(e, __STACKTRACE__)
  end
end
