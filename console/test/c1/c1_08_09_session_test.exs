defmodule C1.SessionTest do
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias C1.{Harness, Oracles}
  alias OrrisConsole.SessionStore

  @mods [OrrisConsole.SessionStore, OrrisConsole.Endpoint, OrrisConsole.RunIndexLive, OrrisConsole.Config]

  defp app!(overrides \\ []) do
    Harness.red!(@mods)
    clock = C1.Clock.start!()
    {root, _} = Harness.fixture_root(["a"])

    config =
      Harness.config(
        Harness.merged(
          [roots: %{"alpha" => root}, clock: C1.Clock.fun(clock), login_capacity: 500, login_refill_ms: 1],
          overrides
        )
      )

    secret = Harness.credential!(config)
    Harness.start_app!(config)
    %{config: config, secret: secret, clock: clock, store: OrrisConsole.SessionStore, root: root}
  end

  defp form(c, cookie, path, fields) do
    Harness.conn(
      c,
      :post,
      path,
      [{"origin", Harness.origin(c)}, {"cookie", cookie}, {"content-type", "application/x-www-form-urlencoded"}],
      URI.encode_query(fields)
    )
  end

  test "C1-08a idle expiry is exact: valid one millisecond before the deadline, expired at it; :observe never renews, :action does" do
    %{secret: s, clock: clock, store: store} = app!(idle_ms: 10_000)
    {:ok, id} = SessionStore.login(store, s)
    C1.Clock.advance(clock, 9_999)
    assert {:ok, %{idle_deadline_ms: d1}} = SessionStore.validate(store, id, :observe)
    assert {:ok, %{idle_deadline_ms: ^d1}} = SessionStore.validate(store, id, :observe)
    assert {:ok, %{idle_deadline_ms: d2}} = SessionStore.validate(store, id, :action)
    assert d2 == d1 + 9_999
    C1.Clock.advance(clock, 10_000)
    assert {:error, :expired} = SessionStore.validate(store, id, :action)
    assert SessionStore.counts(store).sessions == 0
  end

  test "C1-08b absolute expiry ends a session regardless of actions" do
    %{secret: s, clock: clock, store: store} = app!(idle_ms: 10_000, absolute_ms: 25_000)
    {:ok, id} = SessionStore.login(store, s)

    for _ <- 1..4 do
      C1.Clock.advance(clock, 6_000)
      assert {:ok, _} = SessionStore.validate(store, id, :action)
    end

    C1.Clock.advance(clock, 1_000)
    assert {:error, :expired} = SessionStore.validate(store, id, :action)
  end

  test "C1-08c at most 8 views per session; a view DOWN reclaims its slot; unknown ids allocate nothing" do
    %{secret: s, store: store} = app!()
    {:ok, id} = SessionStore.login(store, s)
    views = for _ <- 1..8, do: spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> for v <- views, do: Process.exit(v, :kill) end)
    for v <- views, do: assert(:ok = SessionStore.register_view(store, id, v))
    assert {:error, :view_capacity} = SessionStore.register_view(store, id, self())
    Process.exit(hd(views), :kill)
    Process.sleep(50)
    assert :ok = SessionStore.register_view(store, id, self())
    assert SessionStore.counts(store) == %{sessions: 1, views: 8}

    for _ <- 1..1_000,
        do: assert({:error, :invalid} = SessionStore.validate(store, :crypto.strong_rand_bytes(32), :observe))

    assert SessionStore.counts(store) == %{sessions: 1, views: 8}
  end

  test "C1-08d an automatic refresh never keeps an idle session alive" do
    %{config: c, secret: s, clock: clock, store: store} = app!(idle_ms: 10_000)
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    Harness.await_read(view)

    for _ <- 1..3 do
      C1.Clock.advance(clock, 4_000)
      send(view.pid, :refresh)
      Process.sleep(20)
    end

    ref = Process.monitor(view.pid)
    send(view.pid, :refresh)
    assert_receive {:DOWN, ^ref, _, _, _}
    assert SessionStore.counts(store).sessions == 0
  end

  test "C1-09a logout revokes: the cookie no longer authorizes HTTP, and registered views get the notice before revoke returns" do
    %{config: c, secret: s, store: store} = app!()
    {:ok, id} = SessionStore.login(store, s)
    :ok = SessionStore.register_view(store, id, self())
    :ok = SessionStore.revoke(store, id)
    assert_received {:session_revoked, ^id}
    assert {:error, :invalid} = SessionStore.validate(store, id, :observe)
    cookie = Harness.login!(c, s)
    token = Harness.csrf_token(Harness.conn(c, :get, "/", [{"cookie", cookie}]).resp_body)
    assert form(c, cookie, "/logout", %{"_csrf_token" => token}).status == 302
    after_logout = Harness.conn(c, :get, "/", [{"cookie", cookie}])
    assert after_logout.status == 302 and Plug.Conn.get_resp_header(after_logout, "location") == ["/login"]
  end

  test "C1-09b a SessionStore restart invalidates every session and terminates open views (real process witnesses)" do
    %{config: c, secret: s, store: store} = app!()
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    ref = Process.monitor(view.pid)
    old = Process.whereis(store)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
    Process.sleep(50)
    assert Process.whereis(store) not in [nil, old]
    assert SessionStore.counts(store) == %{sessions: 0, views: 0}
    assert Harness.conn(c, :get, "/", [{"cookie", cookie}]).status == 302
  end

  test "C1-09c a full application restart and an explicit credential rotation refuse the old cookie and the old secret" do
    %{config: c, secret: s} = app!()
    cookie = Harness.login!(c, s)
    :ok = Application.stop(:orris_console)
    {:ok, _} = Application.ensure_all_started(:orris_console)
    assert Harness.conn(c, :get, "/", [{"cookie", cookie}]).status == 302
    :ok = Application.stop(:orris_console)
    File.rm!(c[:credential_path])
    new_secret = Harness.credential!(c)
    {:ok, _} = Application.ensure_all_started(:orris_console)
    assert new_secret != s
    get = Harness.conn(c, :get, "/login")
    token = Harness.csrf_token(get.resp_body)

    old =
      form(c, Harness.cookie(get), "/login", %{"_csrf_token" => token, "credential" => Base.encode16(s, case: :lower)})

    assert old.status in [200, 401]
    assert is_binary(Harness.login!(c, new_secret))
  end

  test "C1-09d the view's CURRENT admitted read: the mounted read is released first; a refresh read whose correlation is applied (server-side witness) adds the exact run entry" do
    %{config: c, secret: s, store: store, root: root} = app!(read_gate: self(), read_witness: self())
    cookie = Harness.login!(c, s)
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    {_mount_worker, mount_corr} = Harness.release_read!()
    html = Harness.await_applied(view, mount_corr)
    assert html =~ ~s(href="/runs/alpha/a") and html =~ ~s(data-c1-read="complete")
    refute html =~ ~s(href="/runs/alpha/b")
    refute html =~ "correlation", "internal identities never reach the browser"
    # accepted control: the exact new run entry appears only after the released read's correlation is applied
    Harness.write_run(Path.join(root, "b"), "events_pre_dispatch.jsonl")
    send(view.pid, :refresh)
    {worker, corr} = Harness.release_read!()
    assert is_pid(worker) and corr != mount_corr
    html = Harness.await_applied(view, corr)

    assert html =~ ~s(href="/runs/alpha/b"),
           "the admitted read did not add the run entry: #{String.slice(html, 0, 300)}"

    # revoke while the next read is held: the result is dropped (no witness) and the view terminates
    ref = Process.monitor(view.pid)
    send(view.pid, :refresh)
    assert_receive {:read_gate, worker2, corr2}
    :ok = SessionStore.revoke_all(store)
    send(worker2, :go)
    assert_receive {:DOWN, ^ref, _, _, _}
    refute_received {:read_applied, _, ^corr2}
    Oracles.down(worker2, :worker_survived)
    # Store restart with a read in flight
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    {_, mount2} = Harness.release_read!()
    Harness.await_applied(view, mount2)
    ref = Process.monitor(view.pid)
    send(view.pid, :refresh)
    assert_receive {:read_gate, worker3, corr3}
    Process.exit(Process.whereis(store), :kill)
    send(worker3, :go)
    assert_receive {:DOWN, ^ref, _, _, _}
    refute_received {:read_applied, _, ^corr3}
  end

  test "C1-09e both views deliver through revalidating, identity-checking delivery: RunIndexLive.deliver/2 and RunDetailLive.deliver/2 pass the oracle the doubles prove discriminating (H-13)" do
    Harness.red!([OrrisConsole.RunIndexLive, OrrisConsole.RunDetailLive])
    assert :ok = Oracles.outcome(fn -> Oracles.delivery(&OrrisConsole.RunIndexLive.deliver/2) end)
    assert :ok = Oracles.outcome(fn -> Oracles.delivery(&OrrisConsole.RunDetailLive.deliver/2) end)
  end
end
