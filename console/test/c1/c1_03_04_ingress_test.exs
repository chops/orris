defmodule C1.IngressTest do
  @moduledoc """
  C1-03 (ingress) and C1-04 (parser) over a REAL loopback listener: every cell is bytes on the wire against the product
  endpoint. Refusal = 403 BEFORE framework dispatch (router dispatch count 0, socket connect count 0). Acceptance is the
  dispatch witness (router dispatch count 1), never a bare status: a later layer (CSRF, routing) decides the status.
  """
  use ExUnit.Case, async: false
  alias C1.Harness

  @mods [OrrisConsole.Endpoint, OrrisConsole.Ingress, OrrisConsole.Config]

  defp server!(overrides \\ []) do
    Harness.red!(@mods)
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(Harness.merged([roots: %{"alpha" => root}, server: true], overrides))
    Harness.credential!(config)
    Harness.start_app!(config)
    assert Harness.endpoint_port() == config[:port]
    config
  end

  defp cell(config, method, target, headers) do
    {resp, counts} = Harness.telemetry_counts(fn -> Harness.tcp_request(config[:port], method, target, headers) end)
    {resp.status, counts}
  end

  defp refused!(config, label, method, target, headers) do
    {status, counts} = cell(config, method, target, headers)
    assert status == 403, "#{label}: expected 403 got #{status}"
    assert counts == %{router: 0, socket: 0}, "#{label}: reached the framework #{inspect(counts)}"
  end

  # ingress passed: the request reached router dispatch; the returned status is that later layer's decision
  defp dispatched!(config, label, method, target, headers) do
    {status, counts} = cell(config, method, target, headers)
    assert counts.router == 1, "#{label}: did not reach the router (status #{status}) #{inspect(counts)}"
    status
  end

  defp upgrade(headers) do
    [
      {"connection", "Upgrade"},
      {"upgrade", "websocket"},
      {"sec-websocket-version", "13"},
      {"sec-websocket-key", Base.encode64("0123456789abcdef")} | headers
    ]
  end

  test "C1-03a Host cells: absent, duplicate, wrong, wrong port, uppercase (even with a matching absolute target); exact lowercase passes" do
    c = server!()
    a = Harness.authority(c)
    # a request without any Host is refused by the HTTP/1.1 parser (400) before the guard can answer 403: either way
    # nothing reaches the framework
    {status, counts} = cell(c, "GET", "/login", [])
    assert status in [400, 403] and counts == %{router: 0, socket: 0}, "absent host: #{status} #{inspect(counts)}"
    # a duplicated Host is refused by the HTTP/1.1 parser (400) before the guard; nothing reaches the framework
    {status, counts} = cell(c, "GET", "/login", [{"host", a}, {"host", a}])
    assert status in [400, 403] and counts == %{router: 0, socket: 0}, "duplicate host: #{status} #{inspect(counts)}"
    refused!(c, "wrong host", "GET", "/login", [{"host", "other.invalid:#{c[:port]}"}])
    refused!(c, "uppercase host", "GET", "/login", [{"host", String.upcase(a)}])
    refused!(c, "uppercase host + matching absolute target", "GET", "http://#{a}/login", [{"host", String.upcase(a)}])
    refused!(c, "wrong port", "GET", "/login", [{"host", "#{c[:host]}:#{c[:port] + 1}"}])
    assert 200 == dispatched!(c, "exact host", "GET", "/login", [{"host", a}])
  end

  test "C1-03b parsed authority: an absolute target with another host or port is refused; a matching absolute target passes" do
    c = server!()
    a = Harness.authority(c)
    refused!(c, "absolute other host", "GET", "http://other.invalid/login", [{"host", a}])
    refused!(c, "absolute other port", "GET", "http://#{c[:host]}:#{c[:port] + 1}/login", [{"host", a}])
    assert 200 == dispatched!(c, "absolute matching", "GET", "http://#{a}/login", [{"host", a}])
  end

  test "C1-03c the raw target scheme cannot claim TLS: over plaintext the session cookie is never Secure" do
    c = server!()
    a = Harness.authority(c)
    resp = Harness.tcp_request(c[:port], "GET", "https://#{a}/login", [{"host", a}])
    assert resp.status == 200
    cookies = Harness.header(resp, "set-cookie")
    assert cookies != []
    refute Enum.any?(cookies, &(&1 =~ ~r/;\s*secure/i)), "Secure attribute over plaintext: #{inspect(cookies)}"
    assert Enum.all?(cookies, &(&1 =~ ~r/httponly/i and &1 =~ ~r/samesite=strict/i and &1 =~ ~r/path=\//i))
  end

  test "C1-03d forwarded headers are refused everywhere (Forwarded, every x-forwarded-*), including static and error paths" do
    c = server!()
    a = Harness.authority(c)

    for name <- ["forwarded", "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-anything"],
        path <- ["/login", "/assets/app.css", "/nonexistent"] do
      refused!(c, "#{name} on #{path}", "GET", path, [{"host", a}, {name, "value"}])
    end
  end

  test "C1-03e Origin cells: socket mount, any Upgrade and non-GET/HEAD methods require exactly one configured Origin; accepted requests reach dispatch and the CSRF layer decides" do
    c = server!()
    a = Harness.authority(c)
    o = Harness.origin(c)
    refused!(c, "mount GET without origin", "GET", "/live/websocket?vsn=2.0.0", [{"host", a}])

    refused!(c, "mount wrong origin", "GET", "/live/websocket?vsn=2.0.0", [
      {"host", a},
      {"origin", "http://other.invalid"}
    ])

    refused!(c, "mount duplicate origin", "GET", "/live/websocket?vsn=2.0.0", [
      {"host", a},
      {"origin", o},
      {"origin", o}
    ])

    refused!(c, "mount origin case", "GET", "/live/websocket?vsn=2.0.0", [{"host", a}, {"origin", String.upcase(o)}])
    refused!(c, "upgrade outside mount", "GET", "/login", upgrade([{"host", a}, {"origin", o}]))
    refused!(c, "upgrade without origin", "GET", "/live/websocket?vsn=2.0.0", upgrade([{"host", a}]))
    refused!(c, "POST without origin", "POST", "/login", [{"host", a}, {"content-length", "0"}])
    refused!(c, "OPTIONS without origin", "OPTIONS", "/login", [{"host", a}])
    refused!(c, "GET navigation with wrong origin", "GET", "/login", [{"host", a}, {"origin", "http://other.invalid"}])
    assert 200 == dispatched!(c, "GET navigation without origin", "GET", "/login", [{"host", a}])
    # ingress passes a POST with the right Origin; the CSRF layer refuses it (no token): 403 AFTER dispatch (C1-05b)
    assert 403 ==
             dispatched!(c, "POST with origin, no CSRF", "POST", "/login", [
               {"host", a},
               {"origin", o},
               {"content-length", "0"}
             ])
  end

  test "C1-03f the socket mount is the configured path with segment boundaries: a sibling path is ordinary navigation (404 after dispatch) unless it carries Upgrade" do
    c = server!(socket_mounts: ["/alt-live"])
    a = Harness.authority(c)
    o = Harness.origin(c)

    refused!(
      c,
      "old /live mount is not a mount",
      "GET",
      "/live/websocket?vsn=2.0.0",
      upgrade([{"host", a}, {"origin", o}])
    )

    # ordinary navigation to a sibling path passes the guard (which only ever answers 403) and reaches routing, where
    # no route matches: the framework's 404 (no dispatch telemetry exists for an unmatched path)
    {status, counts} = cell(c, "GET", "/alt-livex/websocket", [{"host", a}])
    assert status == 404 and counts.socket == 0, "prefix sibling navigation: #{status} #{inspect(counts)}"
    refused!(c, "prefix sibling with Upgrade", "GET", "/alt-livex/websocket", upgrade([{"host", a}, {"origin", o}]))
    {status, counts} = cell(c, "GET", "/alt-live/websocket?vsn=2.0.0", upgrade([{"host", a}, {"origin", o}]))
    assert status in [101, 403], "configured mount handshake answered #{status}"
    assert counts.socket == 1, "configured mount did not reach Socket.connect: #{inspect(counts)}"
  end

  test "C1-04a malformed requests never reach the router or Socket.connect" do
    c = server!()
    a = Harness.authority(c)

    cells = [
      {"nul in target", "GET /\x00 HTTP/1.1\r\nHost: #{a}\r\n\r\n"},
      {"header without colon", "GET /login HTTP/1.1\r\nHost: #{a}\r\nBroken\r\n\r\n"},
      {"duplicate content-length",
       "POST /login HTTP/1.1\r\nHost: #{a}\r\nOrigin: #{Harness.origin(c)}\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx"},
      {"oversized header", "GET /login HTTP/1.1\r\nHost: #{a}\r\nX-Big: #{String.duplicate("a", 70_000)}\r\n\r\n"},
      {"http/0.9 line", "GET /login\r\n\r\n"},
      {"bad version", "GET /login HTTP/9.9\r\nHost: #{a}\r\n\r\n"}
    ]

    for {label, bytes} <- cells do
      {resp, counts} = Harness.telemetry_counts(fn -> Harness.parse_response(Harness.tcp_raw(c[:port], bytes)) end)
      assert resp.status in [0, 400, 403, 413, 431], "#{label}: #{resp.status}"
      assert counts == %{router: 0, socket: 0}, "#{label}: reached the framework #{inspect(counts)}"
    end
  end

  test "C1-04b a well-formed upgrade on the configured mount reaches Socket.connect (unauthenticated: refused there, not before)" do
    c = server!()
    a = Harness.authority(c)

    {status, counts} =
      cell(c, "GET", "/live/websocket?vsn=2.0.0", upgrade([{"host", a}, {"origin", Harness.origin(c)}]))

    assert status == 403
    assert counts.socket == 1, "handshake did not reach Socket.connect: #{inspect(counts)}"
  end
end
