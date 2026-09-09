defmodule OrrisConsole.Layouts do
  @moduledoc "Root and app layouts: pinned local assets only, CSRF meta, the configured socket mount for the client."
  use Phoenix.Component

  def root(assigns) do
    assigns = assign(assigns, :mount, hd(OrrisConsole.Config.current().socket_mounts))

    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={Plug.CSRFProtection.get_csrf_token()} />
        <meta name="orris-console-socket" content={@mount <> "/websocket"} />
        <title>Orris console</title>
        <link rel="stylesheet" href="/assets/app.css" />
        <script type="module" src="/assets/app.js"></script>
      </head>
      <body>
        {@inner_content}
      </body>
    </html>
    """
  end

  def app(assigns) do
    ~H"""
    <header class="bar">
      <span class="brand">Orris console</span>
      <form method="post" action="/logout" class="logout">
        <input type="hidden" name="_csrf_token" value={Plug.CSRFProtection.get_csrf_token()} />
        <button type="submit">Log out</button>
      </form>
    </header>
    {@inner_content}
    """
  end
end
