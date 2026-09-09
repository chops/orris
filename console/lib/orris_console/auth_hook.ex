defmodule OrrisConsole.AuthHook do
  @moduledoc """
  LiveView admission (C1-06/07/09): every mount (disconnected and connected) validates the session id from the signed
  cookie session (:observe); a connected mount registers the view with the Store and monitors the Store itself, so a
  revocation notice or a Store restart terminates the view; events and refreshes revalidate at their cut points.
  """
  import Phoenix.LiveView
  import Phoenix.Component
  alias OrrisConsole.SessionStore

  def on_mount(:default, _params, session, socket) do
    id = session["session_id"]

    case is_binary(id) and SessionStore.validate(SessionStore, id, :observe) do
      {:ok, console_session} ->
        socket = assign(socket, console_session: console_session, session_id: id)

        if connected?(socket) do
          case SessionStore.register_view(SessionStore, id, self()) do
            :ok ->
              # the Store's own monitor reference: only ITS loss closes the view; a controller DOWN reaches the view
              store = Process.whereis(SessionStore)
              ref = if store, do: Process.monitor(store)
              socket = assign(socket, :console_store_monitor, ref)
              {:cont, attach_hook(socket, :console_revocation, :handle_info, &revocation/2)}

            _ ->
              {:halt, redirect(socket, to: "/login")}
          end
        else
          {:cont, socket}
        end

      _ ->
        {:halt, redirect(socket, to: "/login")}
    end
  end

  defp revocation({:session_revoked, _}, socket), do: {:halt, redirect(socket, to: "/login")}

  defp revocation({:DOWN, ref, :process, _pid, _reason}, socket) do
    if ref == socket.assigns[:console_store_monitor],
      do: {:halt, redirect(socket, to: "/login")},
      else: {:cont, socket}
  end

  defp revocation(_other, socket), do: {:cont, socket}

  @doc "Whether the mounted session is still valid (observe: no idle renewal)."
  def valid?(id), do: match?({:ok, _}, SessionStore.validate(SessionStore, id, :observe))
end
