defmodule OrrisConsole.Router do
  @moduledoc "Exactly seven routes (C1-12c as amended by U1): index, run detail, login (GET/POST), logout (POST), cancel intent (POST) and cancel confirm (POST)."
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

    post "/runs/:root_id/:run_ref/cancel", MutationController, :intent
    post "/runs/:root_id/:run_ref/cancel/confirm", MutationController, :confirm

    live_session :console, on_mount: OrrisConsole.AuthHook, layout: {OrrisConsole.Layouts, :app} do
      live "/", RunIndexLive
      live "/runs/:root_id/:run_ref", RunDetailLive
    end
  end
end
