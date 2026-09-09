defmodule OrrisConsole.Router do
  @moduledoc "Exactly the read-only routes (C1-12c): index, run detail, login (GET/POST) and logout (POST)."
  use Phoenix.Router
  import Phoenix.LiveView.Router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {OrrisConsole.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug OrrisConsole.Plugs.Headers
  end

  pipeline :authenticated do
    plug OrrisConsole.Plugs.RequireSession
    plug OrrisConsole.Plugs.ScopeGuard
  end

  scope "/", OrrisConsole do
    pipe_through :browser

    get "/login", LoginController, :new
    post "/login", LoginController, :create
    post "/logout", LoginController, :delete
  end

  scope "/", OrrisConsole do
    pipe_through [:browser, :authenticated]

    live_session :console, on_mount: OrrisConsole.AuthHook, layout: {OrrisConsole.Layouts, :app} do
      live "/", RunIndexLive
      live "/runs/:root_id/:run_ref", RunDetailLive
    end
  end
end
