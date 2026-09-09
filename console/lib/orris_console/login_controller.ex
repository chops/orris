defmodule OrrisConsole.LoginController do
  @moduledoc """
  Explicit local login (C1-05): the credential is submitted hex-encoded in a password field with the CSRF token;
  the Store compares digests and answers a fresh opaque id that becomes the renewed cookie session. Wrong credential
  and capacity answer one generic message; rate limiting answers 429; logout revokes before dropping the cookie.
  """
  use Phoenix.Controller, formats: [:html]
  import Plug.Conn
  alias OrrisConsole.SessionStore

  def new(conn, _params), do: form(conn, nil)

  def create(conn, params) do
    submitted =
      case decode(Map.get(params, "credential")) do
        {:ok, secret} -> secret
        _ -> :invalid
      end

    with {:ok, id} <- SessionStore.login(SessionStore, submitted) do
      conn
      |> configure_session(renew: true)
      |> put_session("session_id", id)
      |> redirect(to: "/")
    else
      {:error, :rate_limited} ->
        conn
        |> put_status(429)
        |> put_resp_content_type("text/html")
        |> send_resp(429, OrrisConsole.ErrorHTML.render("429.html", %{}))

      _invalid_or_capacity ->
        form(conn, "Credential not accepted.")
    end
  end

  def delete(conn, _params) do
    case get_session(conn, "session_id") do
      id when is_binary(id) -> SessionStore.revoke(SessionStore, id)
      _ -> :ok
    end

    conn |> configure_session(drop: true) |> redirect(to: "/login")
  end

  defp decode(hex) when is_binary(hex) and byte_size(hex) == 64 do
    case Base.decode16(hex, case: :mixed) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> {:error, :invalid}
    end
  end

  defp decode(_), do: {:error, :invalid}

  defp form(conn, notice) do
    token = Plug.CSRFProtection.get_csrf_token()

    notice_html = if notice, do: "<p class=\"notice\">" <> Plug.HTML.html_escape(notice) <> "</p>", else: ""

    body =
      "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"csrf-token\" content=\"#{token}\"><title>Orris console</title><link rel=\"stylesheet\" href=\"/assets/app.css\"></head><body>" <>
        "<main class=\"login\"><h1>Orris console</h1>#{notice_html}<form method=\"post\" action=\"/login\"><input type=\"hidden\" name=\"_csrf_token\" value=\"#{token}\">" <>
        "<label>Credential (hex of the private credential file)<input type=\"password\" name=\"credential\" autocomplete=\"off\"></label><button type=\"submit\">Log in</button></form></main></body></html>"

    conn |> put_resp_content_type("text/html") |> send_resp(200, body)
  end
end
