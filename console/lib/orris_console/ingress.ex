defmodule OrrisConsole.Ingress do
  @moduledoc """
  Strict pre-dispatch admission (C1-03), called from `Endpoint.call/2` before the framework: exactly one raw Host equal
  to the configured authority (case-sensitive); parsed host/port/scheme equal to the configuration (an absolute
  request target cannot override them); no Forwarded or x-forwarded-* header; any Upgrade only on a configured socket
  mount; exactly one configured Origin on socket mounts, on any Upgrade and on every non-GET/HEAD method; a present
  wrong or duplicate Origin is always refused. A refused request is answered 403 "refused" and halted.
  """
  import Plug.Conn
  alias OrrisConsole.Config

  @spec call(Plug.Conn.t(), Config.t()) :: Plug.Conn.t()
  def call(conn, %Config{} = config) do
    case decide(conn, config) do
      :pass -> rewrite_mount(conn, config)
      {:refuse, _reason} -> conn |> put_resp_content_type("text/plain") |> send_resp(403, "refused") |> halt()
    end
  end

  @doc "The admission decision without side effects."
  def decide(conn, %Config{} = config) do
    socket? = socket_path?(conn.request_path, config.socket_mounts)
    upgrade? = get_req_header(conn, "upgrade") != []
    requires_origin? = socket? or upgrade? or conn.method not in ["GET", "HEAD"]

    cond do
      not printable_target?(conn.request_path) ->
        {:refuse, :malformed_target}

      get_req_header(conn, "host") != [config.authority] ->
        {:refuse, :host}

      conn.host != config.host or conn.port != config.port or conn.scheme != config.scheme ->
        {:refuse, :parsed_authority}

      Enum.any?(conn.req_headers, fn {name, _} -> name == "forwarded" or String.starts_with?(name, "x-forwarded-") end) ->
        {:refuse, :forwarded}

      upgrade? and not socket? ->
        {:refuse, :upgrade_outside_mount}

      not origin_ok?(get_req_header(conn, "origin"), requires_origin?, config.origin) ->
        {:refuse, :origin}

      true ->
        :pass
    end
  end

  # a request target with control characters (a NUL, for instance) is malformed and never reaches routing
  defp printable_target?(path),
    do: is_binary(path) and String.printable?(path) and not String.contains?(path, [<<0>>, "\n", "\r", "\t"])

  defp origin_ok?([], requires?, _origin), do: not requires?
  defp origin_ok?([origin], _requires?, origin), do: true
  defp origin_ok?(_, _, _), do: false

  @doc "Whether `path` is one of the configured socket mounts or below it (segment boundary, never a prefix)."
  def socket_path?(path, mounts), do: Enum.any?(mounts, &(path == &1 or String.starts_with?(path, &1 <> "/")))

  # the endpoint's socket is compiled at "/live"; a differently configured mount is rewritten to it after admission
  defp rewrite_mount(conn, config) do
    case Enum.find(
           config.socket_mounts,
           &(conn.request_path == &1 or String.starts_with?(conn.request_path, &1 <> "/"))
         ) do
      nil ->
        conn

      "/live" ->
        conn

      mount ->
        rest = String.replace_prefix(conn.request_path, mount, "")

        %{
          conn
          | request_path: "/live" <> rest,
            path_info: ["live" | Enum.drop(conn.path_info, length(String.split(mount, "/", trim: true)))]
        }
    end
  end
end
