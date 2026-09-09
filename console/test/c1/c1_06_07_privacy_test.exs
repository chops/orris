defmodule C1.PrivacyTest do
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias C1.Harness

  @mods [
    OrrisConsole.Endpoint,
    OrrisConsole.SessionStore,
    OrrisConsole.ReadModel,
    OrrisConsole.RunIndexLive,
    OrrisConsole.RunDetailLive
  ]

  defp app!(overrides \\ []) do
    Harness.red!(@mods)
    {alpha, alpha_ids} = Harness.fixture_root(["a1", "a2"])
    {beta, _} = Harness.fixture_root(["b1"])
    config = Harness.config(Harness.merged([roots: %{"alpha" => alpha, "beta" => beta}], overrides))
    secret = Harness.credential!(config)
    Harness.start_app!(config)
    %{config: config, secret: secret, alpha: alpha, beta: beta, ids: alpha_ids}
  end

  defp workers_idle?,
    do:
      DynamicSupervisor.count_children(OrrisConsole.QueryWorkers).active == 0 and
        Registry.count(OrrisConsole.QueryRegistry) == 0

  test "C1-06a unauthorized index, detail and root selectors never invoke Query and leak no root name or path" do
    %{config: c, alpha: alpha, beta: beta} = app!()

    {conns, calls} =
      Harness.query_calls(fn ->
        for path <- ["/", "/runs/alpha/a1", "/runs/beta/b1", "/?root=beta"], do: Harness.conn(c, :get, path)
      end)

    assert calls == []

    for conn <- conns do
      assert conn.status == 302 and Plug.Conn.get_resp_header(conn, "location") == ["/login"]
      refute conn.resp_body =~ alpha or conn.resp_body =~ beta or conn.resp_body =~ "alpha"
    end
  end

  test "C1-06b a valid session sees only its allowed roots on the connected view; a disconnected render starts no unowned read; disallowed/unknown/invalid selectors are one generic not-found without Query" do
    %{config: c, secret: s, beta: beta, ids: ids} = app!()
    cookie = Harness.login!(c, s)
    {index, calls} = Harness.query_calls(fn -> Harness.conn(c, :get, "/", [{"cookie", cookie}]) end)
    assert index.status == 200
    assert workers_idle?(), "the disconnected render left an unowned read running"
    if calls != [], do: assert(Enum.all?(calls, fn {_f, args} -> is_list(List.last(args)) end))
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    html = Harness.await_read(view)
    assert html =~ ids["a1"] and html =~ ids["a2"]
    refute html =~ "beta" or html =~ beta

    {[forbidden, unknown, invalid], calls} =
      Harness.query_calls(fn ->
        for path <- ["/runs/beta/b1", "/runs/gamma/a1", "/runs/alpha/..%2Fb1"],
            do: Harness.conn(c, :get, path, [{"cookie", cookie}])
      end)

    assert calls == []
    assert forbidden.status == 404 and unknown.status == 404 and invalid.status == 404
    strip = &Regex.replace(~r/_csrf_token[^>]*/, &1.resp_body, "")
    assert strip.(forbidden) == strip.(unknown), "forbidden and unknown roots are distinguishable"
    for conn <- [forbidden, unknown, invalid], do: refute(conn.resp_body =~ beta or conn.resp_body =~ "beta")
  end

  test "C1-06c an authorized connected detail read maps the root id to server config and calls Query with exactly that root; no path reaches the page" do
    %{config: c, secret: s, alpha: alpha, ids: ids} = app!()
    cookie = Harness.login!(c, s)

    {{:ok, view, _shell}, calls} =
      Harness.query_calls(fn ->
        {:ok, v, shell} = live(Harness.conn(c, :get, "/runs/alpha/a1", [{"cookie", cookie}]))
        Harness.await_read(v)
        {:ok, v, shell}
      end)

    html = render(view)
    assert html =~ ids["a1"]
    refute html =~ alpha
    assert calls != []

    assert Enum.all?(calls, fn {_f, args} ->
             is_list(List.last(args)) and Keyword.get(List.last(args), :root) == alpha
           end)
  end

  test "C1-07a real socket joins over a disposable listener: valid cookie + page CSRF connects; missing or forged socket CSRF, no cookie, and a revoked session are refused at Socket.connect without Query" do
    %{config: c, secret: s} = app!(server: true)
    assert Harness.endpoint_port() == c[:port]
    cookie = Harness.login!(c, s)
    csrf = Harness.meta_csrf(Harness.conn(c, :get, "/", [{"cookie", cookie}]).resp_body)
    {status, counts} = Harness.socket_upgrade(c, cookie, csrf)
    assert status == 101 and counts.socket == 1, "accepted control: #{status} #{inspect(counts)}"

    for {label, cookie_cell, csrf_cell} <- [
          {"missing csrf", cookie, nil},
          {"forged csrf", cookie, "forged"},
          {"no cookie", nil, csrf},
          {"garbage cookie", "_orris_console_key=garbage", csrf}
        ] do
      {{status, counts}, calls} = Harness.query_calls(fn -> Harness.socket_upgrade(c, cookie_cell, csrf_cell) end)
      assert status == 403 and counts.socket == 1, "#{label}: #{status} #{inspect(counts)}"
      assert calls == [], "#{label}: Query invoked"
    end

    :ok = OrrisConsole.SessionStore.revoke_all(OrrisConsole.SessionStore)
    {{status, counts}, calls} = Harness.query_calls(fn -> Harness.socket_upgrade(c, cookie, csrf) end)
    assert status == 403 and counts.socket == 1 and calls == [], "reconnect after revocation: #{status}"
  end

  test "C1-07b a connected view reads real data; a cross-root selector event is refused without Query" do
    %{config: c, secret: s, ids: ids, beta: beta} = app!()
    cookie = Harness.login!(c, s)
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    assert Harness.await_read(view) =~ ids["a1"]
    {rendered, calls} = Harness.query_calls(fn -> render_click(view, "select_root", %{"root_id" => "beta"}) end)
    assert calls == []
    refute rendered =~ beta or rendered =~ "b1"
    assert rendered =~ "not available"
  end

  test "C1-07c an event or refresh after revocation is refused before Query and the view terminates" do
    %{config: c, secret: s} = app!()
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    ref = Process.monitor(view.pid)
    assert :ok = OrrisConsole.SessionStore.revoke_all(OrrisConsole.SessionStore)

    {_, calls} =
      Harness.query_calls(fn ->
        send(view.pid, :refresh)
        assert_receive {:DOWN, ^ref, _, _, _}
      end)

    assert calls == []
  end
end
