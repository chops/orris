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

  test "C1-12c the router has exactly the read-only routes and pages carry no control except logout" do
    %{config: c, secret: s} = app!()
    routes = OrrisConsole.Router.__routes__() |> Enum.map(&{&1.verb, &1.path}) |> Enum.sort()

    assert routes ==
             Enum.sort([
               {:get, "/"},
               {:get, "/runs/:root_id/:run_ref"},
               {:get, "/login"},
               {:post, "/login"},
               {:post, "/logout"}
             ])

    cookie = Harness.login!(c, s)

    for path <- ["/", "/runs/alpha/a"] do
      {:ok, view, _shell} = live(Harness.conn(c, :get, path, [{"cookie", cookie}]))
      html = Harness.await_read(view)
      forms = Regex.scan(~r/<form[^>]*action="([^"]+)"/, html) |> Enum.map(&List.last/1)
      assert forms == ["/logout"], "#{path}: forms #{inspect(forms)}"
      buttons = Regex.scan(~r/<button[^>]*>([^<]*)</, html) |> Enum.map(&List.last/1)
      assert buttons == ["Log out"], "#{path}: controls #{inspect(buttons)}"
      refute html =~ ~r/phx-click="(?!select_root)/
    end
  end

  test "C1-13a the observed request's own bearer material (its cookie value and the raw session id decoded from it), the secret, the root path" do
    %{config: c, secret: s, root: root} = app!()
    hex = Base.encode16(s, case: :lower)

    {{unauthorized, authorized, cookie}, logs} =
      Harness.capture_logs(fn ->
        cookie = Harness.login!(c, s)

        unauthorized = [
          Harness.conn(c, :get, "/runs/alpha/a"),
          Harness.conn(c, :get, "/runs/alpha/../a"),
          Harness.conn(c, :get, "/", [{"cookie", "_orris_console_key=garbage"}]),
          Harness.conn(c, :get, "/runs/alpha/missing", [{"cookie", cookie}]),
          Harness.conn(c, :get, "/nonexistent", [{"cookie", cookie}])
        ]

        {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
        {unauthorized, Harness.await_read(view), cookie}
      end)

    {raw_id, cookie_value} = Harness.raw_session_id(cookie)

    assert {:ok, _} = OrrisConsole.SessionStore.validate(OrrisConsole.SessionStore, raw_id, :observe),
           "the decoded id is not the session of the observed request"

    for conn <- unauthorized,
        canary <- [hex, root, "CTX_CANARY_7f3a", raw_id, cookie_value],
        do: refute(conn.resp_body =~ canary, "leak of #{canary} in #{conn.request_path}")

    assert authorized =~ "CTX_CANARY_7f3a"

    for canary <- [hex, root, raw_id, cookie_value, "CTX_CANARY_7f3a"],
        do: refute(logs =~ canary, "log leak of #{canary}")
  end

  test "C1-13b the credential digest is present by value in the Store state at the cut point, and neither it (hex, base64, inspect form), the session id digest, the raw id" do
    %{config: c, secret: s, root: root} = app!()
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

    for canary <- Harness.renderings(digest) ++ Harness.renderings(id_digest) ++ [raw_id, root, "exception_canary"],
        do: refute(logs =~ canary, "crash log leak of #{canary}")

    assert logs =~ "crash_canary_exit" or logs =~ "SessionStore"
  end
end
