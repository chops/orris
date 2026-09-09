defmodule OrrisConsole.Plugs do
  @moduledoc "Request-scope plugs: body limit before parsing, CSP, session requirement, root/run scope guard."
  import Plug.Conn
  alias OrrisConsole.{Config, SessionStore}

  @csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
  def csp, do: @csp

  defmodule BodyLimit do
    @moduledoc "A declared body above max_login_body is refused (413) before any parsing or credential comparison."
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, _opts) do
      limit = Config.current().max_login_body

      case get_req_header(conn, "content-length") do
        [length] ->
          case Integer.parse(length) do
            {n, ""} when n > limit ->
              conn |> put_resp_content_type("text/plain") |> send_resp(413, "too large") |> halt()

            _ ->
              conn
          end

        _ ->
          conn
      end
    end
  end

  defmodule Parsers do
    @moduledoc """
    Form parsing bounded by the VALIDATED runtime limit on the bytes actually consumed (Content-Length or chunked):
    an oversized body is 413 before any field is parsed or any credential compared; the limiter is never charged.
    """
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, _opts) do
      limit = Config.current().max_login_body

      Plug.Parsers.call(
        conn,
        Plug.Parsers.init(parsers: [:urlencoded], pass: [], length: limit, read_length: limit + 1)
      )
    rescue
      Plug.Parsers.RequestTooLargeError ->
        conn |> put_resp_content_type("text/plain") |> send_resp(413, "too large") |> halt()
    end
  end

  defmodule Headers do
    @moduledoc false
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, _opts) do
      conn
      |> put_resp_header("content-security-policy", OrrisConsole.Plugs.csp())
      |> put_resp_header("x-content-type-options", "nosniff")
      # same-origin: referrers never leave the console; a no-referrer policy would make browsers send "Origin: null"
      # on form navigations, which the ingress guard rightly refuses
      |> put_resp_header("referrer-policy", "same-origin")
      |> put_resp_header("x-frame-options", "DENY")
    end
  end

  defmodule RequireSession do
    @moduledoc "A valid session (observed, never renewed by navigation) or a redirect to /login with the cookie dropped."
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, _opts) do
      id = get_session(conn, "session_id")

      case is_binary(id) and SessionStore.validate(SessionStore, id, :observe) do
        {:ok, session} ->
          conn |> assign(:console_session, session) |> assign(:session_id, id)

        _ ->
          conn
          |> configure_session(drop: true)
          |> Phoenix.Controller.redirect(to: "/login")
          |> halt()
      end
    end
  end

  defmodule ScopeGuard do
    @moduledoc "Root id and run handle from the path are checked against the session BEFORE any view mounts; unknown, disallowed or invalid selectors are one generic not-found."
    import Plug.Conn
    def init(opts), do: opts

    def call(%{path_params: %{"root_id" => root_id, "run_ref" => run_ref}} = conn, _opts) do
      session = conn.assigns.console_session

      if root_id in session.root_ids and Map.has_key?(Config.current().roots, root_id) and
           OrrisConsole.ReadModel.valid_ref?(run_ref),
         do: conn,
         else:
           conn
           |> put_resp_content_type("text/html")
           |> send_resp(404, OrrisConsole.ErrorHTML.render("404.html", %{}))
           |> halt()
    end

    def call(conn, _opts), do: conn
  end
end
