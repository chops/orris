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

  # C1-07c fixture: exactly the assigns a mounted RunIndexLive holds when it processes :refresh. AuthHook assigns
  # console_session and session_id (auth_hook.ex 16); mount/3 assigns root_id, notice and state (run_index_live.ex 10).
  # handle_info(:refresh, socket) reads only state.valid?, console_session and root_id (run_index_live.ex 33-36) and
  # records its refusal by putting {:redirect, ...} on the struct, so this socket is sufficient for that cut point.
  # It deliberately models NO transport, NO on_mount hook chain, NO registered view, NO mount self-send and NO timer.
  defp mounted_socket(id) do
    {:ok, session} = OrrisConsole.SessionStore.validate(OrrisConsole.SessionStore, id, :observe)

    Phoenix.Component.assign(%Phoenix.LiveView.Socket{},
      console_session: session,
      session_id: id,
      root_id: List.first(session.root_ids),
      notice: nil,
      state: OrrisConsole.Reader.initial(id)
    )
  end

  # cleanup for work that must NOT have been admitted: runs even when the assertion that found it failed. A refused
  # admission arms a retry timer in THIS process (reader.ex 61 -> 110-113), so the timer is cancelled here too.
  # SCOPE OF THE CLAIM: Process.exit(job, :kill) only INITIATES termination and this function joins nothing, so on
  # return neither controller nor worker is established dead. What bounds their lifetime is the start_app! on_exit
  # Application.stop(:orris_console) (c1_harness.ex 388-390), not this call. Cancel_timer likewise only stops a
  # future send; it does not retract a :refresh already in the mailbox.
  defp discard_admitted(socket) do
    state = socket.assigns.state

    case state.current do
      {job, _correlation} when is_pid(job) -> Process.exit(job, :kill)
      _ -> :ok
    end

    if state.timer, do: Process.cancel_timer(state.timer)
  end

  test "C1-07c an event or refresh after revocation is refused before Query and the view terminates" do
    %{config: c, secret: s} = app!()
    cookie = Harness.login!(c, s)
    {id, _value} = Harness.raw_session_id(cookie)
    # built while the session is valid, exactly as a mounted view holds its assigns from mount time; validity is
    # re-read live at every cut point through state.valid? (reader.ex 15 -> auth_hook.ex 50 -> the Store)
    control_socket = mounted_socket(id)
    refresh_socket = mounted_socket(id)
    event_socket = mounted_socket(id)
    root_id = control_socket.assigns.root_id

    # ACCEPTED CONTROL, session still valid: this same call admits a read AND that read enters Query. Without it the
    # refusals below could pass because the oracle is blind rather than because nothing was admitted.
    {{admitted, result}, control_calls} =
      Harness.query_calls(fn ->
        {:noreply, admitted} = OrrisConsole.RunIndexLive.handle_info(:refresh, control_socket)

        try do
          # inside the cleanup scope: a refusal here fails this match with a retry timer already armed on the
          # returned socket, and cleanup reaches it through the socket rather than through an unbound `job`
          {job, correlation} = admitted.assigns.state.current
          jref = Process.monitor(job)
          # the controller delivers ONLY after the worker's actual DOWN (query_job.ex 87-89), so receiving this joins
          # the whole admitted read: worker dead, read finished, any Query call already traced
          assert_receive {:query_result, ^job, ^correlation, result}, 2_000
          assert_receive {:DOWN, ^jref, :process, ^job, _}, 2_000
          {admitted, result}
        after
          discard_admitted(admitted)
        end
      end)

    assert admitted.redirected == nil, "a valid session was refused"
    assert admitted.assigns.state.timer == nil, "the admitted read armed a retry timer"
    assert match?({:ok, _}, result), "the control read did not succeed: #{inspect(result)}"
    assert control_calls != [], "the admitted read never entered Query: the Query oracle cannot discriminate"

    assert :ok = OrrisConsole.SessionStore.revoke_all(OrrisConsole.SessionStore)

    # THE ROW'S CLAIM, decided synchronously in this process: no view process, no mount self-send, no worker and no
    # retry timer exist here, so nothing can be admitted between the revocation and the observation.
    {{:noreply, refused}, calls} =
      Harness.query_calls(fn -> OrrisConsole.RunIndexLive.handle_info(:refresh, refresh_socket) end)

    try do
      assert refused.redirected == {:redirect, %{to: "/login", status: 302}}
      assert refused.assigns.state.current == nil, "a read was admitted under a revoked session"
      assert refused.assigns.state.timer == nil, "the refusal armed a retry timer"
      assert calls == []
    after
      discard_admitted(refused)
    end

    # the event cut point and the refresh it schedules, in one window: the event itself admits nothing, and the
    # refresh it sends to the view is refused (run_index_live.ex 18-27 defers validity to :refresh)
    {{:noreply, after_event}, event_calls} =
      Harness.query_calls(fn ->
        # query_calls runs this closure in the test process (c1_harness.ex 182), so the event's send(self(), :refresh)
        # lands in THIS mailbox. No earlier phase left one there: every socket above was asserted timer == nil.
        refute_received :refresh

        {:noreply, selected} =
          OrrisConsole.RunIndexLive.handle_event("select_root", %{"root_id" => root_id}, event_socket)

        try do
          assert selected.assigns.state.current == nil, "the event itself admitted a read"
          # the event's OWN scheduling, asserted and consumed before the dispatch below: a direct self-send is in the
          # mailbox by the time handle_event returns, so deleting send(self(), :refresh) (run_index_live.ex 22) or
          # taking the not-allowed branch fails here instead of passing on a refresh this test supplied itself
          assert_received :refresh
          OrrisConsole.RunIndexLive.handle_info(:refresh, selected)
        after
          discard_admitted(selected)
        end
      end)

    try do
      assert after_event.redirected == {:redirect, %{to: "/login", status: 302}}
      assert after_event.assigns.state.current == nil, "the event path admitted a read under a revoked session"
      assert after_event.assigns.state.timer == nil, "the event path armed a retry timer"
      assert event_calls == []
    after
      discard_admitted(after_event)
    end

    # connected-view termination, deliberately with NO trace window: a pre-revocation mount read is PERMITTED
    # (docs/contracts/console-readonly.org 171 is C1-09's contract) and so cannot decide this claim either way
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    ref = Process.monitor(view.pid)
    assert :ok = OrrisConsole.SessionStore.revoke_all(OrrisConsole.SessionStore)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
