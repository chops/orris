defmodule C1.EchoPlug do
  @moduledoc "TEST SUPPORT ONLY: a plug for harness controls (listener, raw TCP client, browser client, delayed same-URL reload)."
  import Plug.Conn
  def init(opts), do: opts

  def call(conn, _opts) do
    conn = fetch_query_params(conn)

    case Integer.parse(conn.query_params["delay"] || "") do
      {ms, _} -> Process.sleep(ms)
      :error -> :ok
    end

    nonce = Integer.to_string(System.unique_integer([:positive]))
    hosts = get_req_header(conn, "host")

    body =
      ~s(<html><body><p id="echo">echo ) <>
        conn.method <>
        " " <>
        conn.request_path <>
        " host=" <> Enum.join(hosts, ",") <> ~s(</p><p id="nonce">) <> nonce <> "</p></body></html>"

    conn |> put_resp_header("x-c1-nonce", nonce) |> put_resp_content_type("text/html") |> send_resp(200, body)
  end
end
