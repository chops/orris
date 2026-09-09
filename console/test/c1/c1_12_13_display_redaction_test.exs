defmodule C1.DisplayRedactionTest do
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias C1.Harness

  @mods [
    OrrisConsole.Endpoint,
    OrrisConsole.Router,
    OrrisConsole.RunDetailLive,
    OrrisConsole.Redaction,
    OrrisConsole.SessionStore,
    OrrisConsole.QueryJob
  ]

  defp app! do
    Harness.red!(@mods)
    clock = C1.Clock.start!()
    root = Harness.fresh("root")
    Harness.canary_run(Path.join(root, "a"))
    {:ok, %{runs: [%{run_id: run_id}]}} = AiOrchestrator.Query.list_runs(root: root)
    config = Harness.config(roots: %{"alpha" => root}, clock: C1.Clock.fun(clock))
    secret = Harness.credential!(config)
    Harness.start_app!(config)
    %{config: config, secret: secret, root: root, run_id: run_id, clock: clock}
  end

  test "C1-12a journal text renders escaped on the connected view: the canary is visible as text and never as markup" do
    %{config: c, secret: s} = app!()
    cookie = Harness.login!(c, s)
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    html = Harness.await_read(view)
    assert html =~ "CTX_CANARY_7f3a"
    assert html =~ "&lt;script&gt;"
    refute html =~ "<script>window.C1_XSS"
  end

  test "C1-12b pending-repair, host and stale labels with the last-success timestamp are accurate" do
    %{config: c, secret: s, root: root, clock: clock} = app!()
    cookie = Harness.login!(c, s)
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    html = Harness.await_read(view)
    assert html =~ "host: not registered"
    refute html =~ "pending repair"
    Harness.torn!(Path.join(root, "a"))
    send(view.pid, :refresh)
    Process.sleep(100)
    assert render(view) =~ "pending repair"
    File.write!(Path.join(root, "a/events.jsonl"), "{corrupt\n")
    C1.Clock.advance(clock, 5_000)
    send(view.pid, :refresh)
    Process.sleep(100)
    stale = render(view)
    assert stale =~ "stale"
    assert stale =~ "last successful read: #{C1.Clock.now(clock) - 5_000}"
  end

  # C1-12c (U1 amendment) the router has exactly the five read-only routes plus the two cancel routes; the index carries only logout; the detail of an allowed non-terminal run carries logout and the cancel form

  test "C1-12c (U1 amendment) exactly the five read-only routes plus the two cancel routes; the index carries only logout; the detail of an allowed non-terminal run carries logout and the cancel form" do
    %{config: c, secret: s} = app!()
    routes = OrrisConsole.Router.__routes__() |> Enum.map(&{&1.verb, &1.path}) |> Enum.sort()

    assert routes ==
             Enum.sort([
               {:get, "/"},
               {:get, "/runs/:root_id/:run_ref"},
               {:get, "/login"},
               {:post, "/login"},
               {:post, "/logout"},
               {:post, "/runs/:root_id/:run_ref/cancel"},
               {:post, "/runs/:root_id/:run_ref/cancel/confirm"}
             ]),
           "RED (U1 amendment C1-12c): routes #{inspect(routes)}"

    cookie = Harness.login!(c, s)

    for {path, expected_forms, expected_buttons} <- [
          {"/", ["/logout"], ["Log out"]},
          {"/runs/alpha/a", ["/logout", "/runs/alpha/a/cancel"], ["Log out", "Cancel run"]}
        ] do
      {:ok, view, _shell} = live(Harness.conn(c, :get, path, [{"cookie", cookie}]))
      html = Harness.await_read(view)
      forms = Regex.scan(~r/<form[^>]*action="([^"]+)"/, html) |> Enum.map(&List.last/1)
      assert forms == expected_forms, "#{path}: forms #{inspect(forms)}"
      buttons = Regex.scan(~r/<button[^>]*>([^<]*)</, html) |> Enum.map(&List.last/1)
      assert buttons == expected_buttons, "#{path}: controls #{inspect(buttons)}"
      refute html =~ ~r/phx-click="(?!select_root)/
    end
  end

  test "C1-13a (U1 amendment: run_dir canary + the unauthorized cancel path) the observed request's own bearer material (its cookie value and the raw session id decoded from it), the secret, the root path" do
    %{config: c, secret: s, root: root} = app!()
    hex = Base.encode16(s, case: :lower)
    run_dir = Path.join(root, "a")

    # a pre-login cookie carries its own CSRF token but no session; a logged-in page carries a session-bound token
    prelogin_cancel = fn ->
      get = Harness.conn(c, :get, "/login")
      token = Harness.csrf_token(get.resp_body)

      Harness.conn(
        c,
        :post,
        "/runs/alpha/a/cancel",
        [
          {"origin", Harness.origin(c)},
          {"cookie", Harness.cookie(get)},
          {"content-type", "application/x-www-form-urlencoded"}
        ],
        URI.encode_query(%{"_csrf_token" => token})
      )
    end

    scoped_cancel = fn session_cookie ->
      page = Harness.conn(c, :get, "/", [{"cookie", session_cookie}])
      token = Harness.csrf_token(page.resp_body)

      Harness.conn(
        c,
        :post,
        "/runs/beta/a/cancel",
        [
          {"origin", Harness.origin(c)},
          {"cookie", session_cookie},
          {"content-type", "application/x-www-form-urlencoded"}
        ],
        URI.encode_query(%{"_csrf_token" => token})
      )
    end

    {{unauthorized, authorized, cookie}, logs} =
      Harness.capture_logs(fn ->
        cookie = Harness.login!(c, s)

        unauthorized = [
          Harness.conn(c, :get, "/runs/alpha/a"),
          Harness.conn(c, :get, "/runs/alpha/../a"),
          Harness.conn(c, :get, "/", [{"cookie", "_orris_console_key=garbage"}]),
          Harness.conn(c, :get, "/runs/alpha/missing", [{"cookie", cookie}]),
          Harness.conn(c, :get, "/nonexistent", [{"cookie", cookie}]),
          # U1 (review R5): the :browser pipeline's CSRF protection runs BEFORE :authenticated, so a tokenless POST is
          # 403 with or without a session; a valid session-bound token then reaches the session/scope checks
          Harness.conn(c, :post, "/runs/alpha/a/cancel", [{"origin", Harness.origin(c)}], ""),
          Harness.conn(
            c,
            :post,
            "/runs/beta/a/cancel/confirm",
            [{"origin", Harness.origin(c)}, {"cookie", cookie}],
            ""
          ),
          prelogin_cancel.(),
          scoped_cancel.(cookie)
        ]

        {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
        {unauthorized, Harness.await_read(view), cookie}
      end)

    {raw_id, cookie_value} = Harness.raw_session_id(cookie)

    assert {:ok, _} = OrrisConsole.SessionStore.validate(OrrisConsole.SessionStore, raw_id, :observe),
           "the decoded id is not the session of the observed request"

    for conn <- unauthorized,
        canary <- [hex, root, run_dir, "CTX_CANARY_7f3a", raw_id, cookie_value],
        do: refute(conn.resp_body =~ canary, "leak of #{canary} in #{conn.request_path}")

    assert authorized =~ "CTX_CANARY_7f3a"

    for canary <- [hex, root, run_dir, raw_id, cookie_value, "CTX_CANARY_7f3a"],
        do: refute(logs =~ canary, "log leak of #{canary}")

    # CSRF-before-auth is measured TODAY on an existing protected route (never weakened): a tokenless POST is 403
    # with and without the session cookie
    tokenless = [
      Harness.conn(c, :post, "/logout", [{"origin", Harness.origin(c)}], ""),
      Harness.conn(c, :post, "/logout", [{"origin", Harness.origin(c)}, {"cookie", cookie}], "")
    ]

    assert Enum.map(tokenless, & &1.status) == [403, 403], "CSRF-before-auth weakened on /logout"
    # the cancel routes must behave the same once they exist: tokenless 403/403; a pre-login token without a
    # session 302 to /login; a session-bound token on a cross-root selector the generic 404
    cancel_statuses = unauthorized |> Enum.drop(5) |> Enum.map(& &1.status)

    assert cancel_statuses == [403, 403, 302, 404],
           "RED (U1 amendment C1-13a): cancel routes answered #{inspect(cancel_statuses)}"
  end

  test "C1-13b (U1 amendment: run_dir canary) the credential digest is present by value in the Store state at the cut point, and neither it (hex, base64, inspect form), the session id digest, the raw id" do
    %{config: c, secret: s, root: root} = app!()
    run_dir = Path.join(root, "a")
    digest = :crypto.hash(:sha256, s)
    store = OrrisConsole.SessionStore
    {:ok, raw_id} = OrrisConsole.SessionStore.login(store, s)
    id_digest = :crypto.hash(:sha256, raw_id)
    state = :sys.get_state(store)

    assert Harness.term_contains?(state, digest),
           "presence cut point: the credential digest is not in the Store state by value"

    status = inspect(:sys.get_status(store), limit: :infinity, printable_limit: :infinity)

    for canary <- Harness.renderings(digest) ++ Harness.renderings(id_digest) ++ [raw_id],
        do: refute(status =~ canary, "format_status leak of #{canary}")

    {_, logs} =
      Harness.capture_logs(fn ->
        cookie = Harness.login!(c, s)
        {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
        Harness.await_read(view)
        assert {:ok, job} = OrrisConsole.QueryJob.start(view.pid, fn -> raise "exception_canary_#{root}" end)
        ref = Process.monitor(job)
        assert_receive {:DOWN, ^ref, _, _, _}
        Process.exit(Process.whereis(store), {:crash_canary_exit, root})
        Process.sleep(200)
      end)

    for canary <-
          Harness.renderings(digest) ++ Harness.renderings(id_digest) ++ [raw_id, root, run_dir, "exception_canary"],
        do: refute(logs =~ canary, "crash log leak of #{canary}")

    assert logs =~ "crash_canary_exit" or logs =~ "SessionStore"
    # U1 amendment: the Store's formatted status carries no mutation record content (op refs, intents, paths)
    assert Harness.term_contains?(:sys.get_state(store), digest)
    status = inspect(:sys.get_status(store), limit: :infinity, printable_limit: :infinity)
    refute status =~ run_dir

    assert status =~ "sessions",
           "RED (U1 amendment C1-13b): format_status changed shape: #{String.slice(status, 0, 200)}"
  end
end
