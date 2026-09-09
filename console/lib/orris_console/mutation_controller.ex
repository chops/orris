defmodule OrrisConsole.MutationController do
  @moduledoc """
  The two mutation routes (docs/contracts/console-mutations.org §transport): POST .../cancel issues an intent and
  renders the no-JS confirmation page; POST .../cancel/confirm accepts (authorization inside the Store), awaits at
  most mutation_wait_ms and redirects to the detail page. Both run behind CSRF-by-form, RequireSession (:observe)
  and ScopeGuard. Request fields naming an actor, reason, root, path or executor are never read.
  """
  use Phoenix.Controller, formats: [:html]
  import Plug.Conn
  alias OrrisConsole.{Config, SessionStore}

  def intent(conn, %{"root_id" => root_id, "run_ref" => run_ref}) do
    case SessionStore.issue_intent(SessionStore, conn.assigns.session_id, root_id, run_ref) do
      {:ok, token} ->
        conn |> put_resp_content_type("text/html") |> send_resp(200, confirmation(root_id, run_ref, token))

      {:error, :in_progress} ->
        plain(conn, 409, "operation in progress")

      {:error, reason} when reason in [:fenced, :unavailable] ->
        plain(conn, 503, "not available")

      {:error, _invalid_or_expired} ->
        conn |> configure_session(drop: true) |> redirect(to: "/login")
    end
  end

  def confirm(conn, %{"root_id" => root_id, "run_ref" => run_ref} = params) do
    case SessionStore.accept(SessionStore, conn.assigns.session_id, Map.get(params, "intent"), root_id, run_ref) do
      {:accepted, op_ref} ->
        _ = SessionStore.await(SessionStore, op_ref, Config.current().mutation_wait_ms)
        redirect(conn, to: "/runs/#{root_id}/#{run_ref}")

      {:error, reason} when reason in [:intent_invalid, :intent_expired, :in_progress] ->
        plain(conn, 409, "confirmation not accepted")

      {:error, reason} when reason in [:busy, :fenced, :unavailable] ->
        plain(conn, 503, "not available")

      {:error, :scope} ->
        conn |> put_resp_content_type("text/html") |> send_resp(404, OrrisConsole.ErrorHTML.render("404.html", %{}))

      {:error, _invalid_or_expired} ->
        conn |> configure_session(drop: true) |> redirect(to: "/login")
    end
  end

  defp plain(conn, status, text), do: conn |> put_resp_content_type("text/plain") |> send_resp(status, text)

  # the confirmation page: exactly the logout form and the confirm form (CSRF by form, the opaque intent hidden)
  defp confirmation(root_id, run_ref, token) do
    csrf = Plug.CSRFProtection.get_csrf_token()
    escape = &Plug.HTML.html_escape/1
    action = "/runs/#{escape.(root_id)}/#{escape.(run_ref)}/cancel/confirm"

    "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Orris console</title>" <>
      "<link rel=\"stylesheet\" href=\"/assets/app.css\"></head><body><header class=\"bar\"><span class=\"brand\">Orris console</span>" <>
      "<form method=\"post\" action=\"/logout\" class=\"logout\"><input type=\"hidden\" name=\"_csrf_token\" value=\"#{csrf}\"><button type=\"submit\">Log out</button></form></header>" <>
      "<main class=\"confirm\"><h1>Cancel run #{escape.(run_ref)}</h1><p>The run under root #{escape.(root_id)} will be cancelled as the console operator. This cannot be undone.</p>" <>
      "<form method=\"post\" action=\"#{action}\"><input type=\"hidden\" name=\"_csrf_token\" value=\"#{csrf}\"><input type=\"hidden\" name=\"intent\" value=\"#{escape.(token)}\">" <>
      "<button type=\"submit\">Confirm cancel</button></form></main></body></html>"
  end
end
