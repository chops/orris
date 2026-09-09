defmodule OrrisConsole.Socket do
  @moduledoc """
  The LiveView socket with admission AT CONNECT (C1-07a): the signed cookie session must carry a session id the
  Store validates (:observe), and the `_csrf_token` connect parameter must match the session's CSRF state; otherwise
  the connect is refused (403) before any channel join. A refused connect never reaches Query.
  """
  use Phoenix.LiveView.Socket

  def connect(params, socket, connect_info) do
    session = connect_info[:session] || %{}
    id = session["session_id"]
    csrf = params["_csrf_token"]

    with true <- is_binary(id) and is_binary(csrf),
         true <- Plug.CSRFProtection.valid_state_and_csrf_token?(session["_csrf_token"], csrf),
         {:ok, _} <- OrrisConsole.SessionStore.validate(OrrisConsole.SessionStore, id, :observe) do
      {:ok, socket}
    else
      _ -> :error
    end
  end
end
