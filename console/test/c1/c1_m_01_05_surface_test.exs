defmodule C1.MutationSurfaceTest do
  @moduledoc "M-01..M-05 (docs/contracts/console-mutations.org): routes and controls, actor binding, CSRF/session, authorization + grant, scope."
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias C1.{Harness, Mutations}
  alias C1.Mutations, as: Mut

  @routes Enum.sort([
            {:get, "/"},
            {:get, "/runs/:root_id/:run_ref"},
            {:get, "/login"},
            {:post, "/login"},
            {:post, "/logout"},
            {:post, "/runs/:root_id/:run_ref/cancel"},
            {:post, "/runs/:root_id/:run_ref/cancel/confirm"}
          ])

  test "M-01 the router has exactly the seven routes; the index carries only logout; the detail of an allowed NON-TERMINAL run carries logout and cancel; a terminal run or no data carries logout only" do
    %{config: c, secret: s, root: root} = Mut.app!()
    Mut.completed!(Path.join(root, "done"))
    routes = OrrisConsole.Router.__routes__() |> Enum.map(&{&1.verb, &1.path}) |> Enum.sort()
    assert routes == @routes, "RED (U1 M-01): routes #{inspect(routes)}"
    cookie = Harness.login!(c, s)
    {:ok, index, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    html = Harness.await_read(index)

    assert Mut.forms(html) == ["/logout"] and Mut.buttons(html) == ["Log out"],
           "index controls: #{inspect(Mut.forms(html))}"

    {:ok, detail, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    html = Harness.await_read(detail)

    assert Mut.forms(html) == ["/logout", "/runs/alpha/a/cancel"],
           "RED (U1 M-01): detail forms #{inspect(Mut.forms(html))}"

    assert Mut.buttons(html) == ["Log out", "Cancel run"]
    refute html =~ ~r/phx-click="(?!select_root)/
    {:ok, done, _} = live(Harness.conn(c, :get, "/runs/alpha/done", [{"cookie", cookie}]))
    html = Harness.await_read(done)
    assert html =~ "completed"
    assert Mut.forms(html) == ["/logout"] and Mut.buttons(html) == ["Log out"], "a terminal run carries a control"
    # no data: the disconnected shell (loading) carries no cancel control
    shell = Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}])
    assert Mut.forms(shell.resp_body) -- ["/logout"] == [], "the loading shell carries a control"
  end

  # M-02 the traced Prepare.invoke receives exactly the session-bound console actor and Prepare.cancel the configured root; request fields naming class/id/actor/reason/root/path/run_dir/executor/operator change nothing (mutant request)

  test "M-02 the traced Prepare.invoke receives exactly the session-bound console actor and Prepare.cancel the configured root; mutant request fields (class/id/actor/reason/root/path/run_dir/executor/operator) change nothing" do
    %{config: c, secret: s, root: root} = Mut.app!(operator: %{id: "op.console-1", root_ids: ["alpha"]})
    cookie = Harness.login!(c, s)

    mutant = %{
      "class" => "operator",
      "id" => "evil",
      "actor" => "system",
      "reason" => "sabotage",
      "root" => "/etc",
      "path" => "/etc/passwd",
      "run_dir" => "/tmp",
      "executor" => "Elixir.System",
      "operator" => "root"
    }

    {{_intent, confirm}, calls} = Mut.prepare_calls(fn -> Mut.cancel!(c, cookie, "alpha", "a", mutant) end)
    assert confirm.status == 302 and Plug.Conn.get_resp_header(confirm, "location") == ["/runs/alpha/a"]

    assert [[actor, _prepared, server_opts]] = Mut.invokes(calls),
           "RED (U1 M-02): invokes #{inspect(Mut.invokes(calls))}"

    assert actor == %{"class" => "console", "id" => "op.console-1"}
    assert [["a", cancel_opts]] = Mut.cancels(calls)
    assert Keyword.get(cancel_opts, :root) == root and Keyword.get(server_opts, :root) == root
    refute Keyword.get(cancel_opts, :cancel_reason) == "sabotage"
    assert [%{"data" => %{"reason" => "operator_cancel", "requested_by" => by}}] = Mut.cancel_requests(root <> "/a")
    assert by["class"] == "console" and by["id"] == "op.console-1"
  end

  test "M-03 missing or wrong CSRF, no cookie, a session expired for :action, a revoked session and a foreign session's intent never reach Prepare; the correct path invokes exactly once" do
    clock = C1.Clock.start!()
    %{config: c, secret: s, root: root} = Mut.app!(clock: C1.Clock.fun(clock), idle_ms: 10_000)
    cookie = Harness.login!(c, s)
    intent_page = Mut.intent(c, cookie, "alpha", "a")
    assert intent_page.status == 200, "RED (U1 M-03): intent answered #{intent_page.status}"
    token = Harness.csrf_token(intent_page.resp_body)
    intent = Mut.intent_token(intent_page.resp_body)
    before = Harness.journal_sha(root <> "/a")

    {statuses, calls} =
      Mut.prepare_calls(fn ->
        [
          Mut.form(c, cookie, Mut.confirm_path("alpha", "a"), %{"intent" => intent}).status,
          Mut.form(c, cookie, Mut.confirm_path("alpha", "a"), %{"_csrf_token" => "forged", "intent" => intent}).status,
          Harness.conn(c, :post, Mut.confirm_path("alpha", "a"), [{"origin", Harness.origin(c)}], "").status
        ]
      end)

    assert Enum.all?(statuses, &(&1 in [302, 403])), "unexpected statuses #{inspect(statuses)}"
    assert calls == [], "a refused confirm reached Prepare: #{inspect(calls)}"
    assert Harness.journal_sha(root <> "/a") == before
    # foreign session: another login confirms with the first session's intent
    other = Harness.login!(c, s)
    other_page = Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", other}])
    other_token = Harness.csrf_token(other_page.resp_body)

    {foreign, calls} =
      Mut.prepare_calls(fn ->
        Mut.form(c, other, Mut.confirm_path("alpha", "a"), %{"_csrf_token" => other_token, "intent" => intent})
      end)

    assert foreign.status in [302, 409, 422] and calls == [], "a foreign session's intent was accepted"
    # expired for :action at the exact idle boundary: refused, no invoke
    C1.Clock.advance(clock, 10_000)

    {expired, calls} =
      Mut.prepare_calls(fn ->
        Mut.form(c, cookie, Mut.confirm_path("alpha", "a"), %{"_csrf_token" => token, "intent" => intent})
      end)

    assert expired.status == 302 and calls == []
    # revoked: refused, no invoke
    cookie = Harness.login!(c, s)
    page = Mut.intent(c, cookie, "alpha", "a")
    :ok = Mut.revoke_all()
    {revoked, calls} = Mut.prepare_calls(fn -> Mut.confirm(c, cookie, "alpha", "a", page.resp_body) end)
    assert revoked.status == 302 and calls == []
    # the correct path: exactly one invoke
    cookie = Harness.login!(c, s)
    {{_, confirm}, calls} = Mut.prepare_calls(fn -> Mut.cancel!(c, cookie, "alpha", "a") end)
    assert confirm.status == 302 and length(Mut.invokes(calls)) == 1
  end

  # M-04 authorization and grant: one acceptance per intent, exact TTL, session+root+run binding, scope at acceptance, revoke before accept and between accept and grant, completion after grant, fenced admission

  # M-04 (review R6): the Prepare trace covers the whole no-invoke window through the terminal transition; the
  # post-grant revocation happens while the work is REALLY held (a gated Writer append), then completes after release
  test "M-04 authorization and grant: one acceptance per intent, exact TTL, session+root+run binding, scope at acceptance, revoke before accept / between accept and grant / after grant, fenced admission" do
    clock = C1.Clock.start!()

    %{config: c, secret: s, root: root, owned: owned} =
      Mut.app!(
        clock: C1.Clock.fun(clock),
        intent_ttl_ms: 5_000,
        mutation_witness: self(),
        operation_gate: self(),
        mutation_opts: [fs: Mutations.GateFs.new(self(), :write)]
      )

    Mut.in_flight!(Path.join(root, "b"))
    {_cookie, id} = Mut.session!(c, s)
    # simultaneous confirms of ONE intent: one acceptance, one :intent_invalid
    {:ok, intent} = Mut.issue_intent(id, "alpha", "a")
    me = self()
    racers = for _ <- 1..2, do: spawn_link(fn -> send(me, {:raced, Mut.accept(id, intent, "alpha", "a")}) end)
    Mutations.Owned.add_all(owned, racers, :racer)
    answers = for _ <- 1..2, do: receive(do: ({:raced, a} -> a), after: (2_000 -> :none))
    assert Enum.count(answers, &match?({:accepted, _}, &1)) == 1, "RED (U1 M-04): #{inspect(answers)}"
    assert {:error, :intent_invalid} in answers
    [{:accepted, op1}] = Enum.filter(answers, &match?({:accepted, _}, &1))
    {^op1, op1_pid} = Mut.witness!(:operation_init, op1)
    Mutations.Owned.add(owned, op1_pid, :operation)
    # in progress: no second intent for this session while op1 is non-terminal
    assert {:error, :in_progress} = Mut.issue_intent(id, "alpha", "b")
    send(op1_pid, :proceed)
    writer1 = Mut.gated!()
    {subtree1, monitors1} = Mut.capture_subtree!(writer1)
    Mutations.Owned.add_all(owned, subtree1, :core_subtree)
    Mut.release(writer1)
    assert {:finished, _} = Mut.await(op1, 5_000)
    assert Mut.join(monitors1, 20_000) == []
    # binding: an intent for run a is not accepted for run b, nor for another root, nor by another session
    {:ok, intent_a} = Mut.issue_intent(id, "alpha", "a")
    assert {:error, :intent_invalid} = Mut.accept(id, intent_a, "alpha", "b")
    assert {:error, :intent_invalid} = Mut.accept(id, intent_a, "beta", "a")
    {:ok, other} = Mut.login(s)
    assert {:error, :intent_invalid} = Mut.accept(other, intent_a, "alpha", "a")
    # exact TTL at the clock seam: valid at ttl - 1 (still pending: a new intent is :in_progress), expired at ttl
    C1.Clock.advance(clock, 4_999)
    assert {:error, :in_progress} = Mut.issue_intent(id, "alpha", "a")
    C1.Clock.advance(clock, 1)
    assert {:error, :intent_expired} = Mut.accept(id, intent_a, "alpha", "a")
    # revoke before accept: refused at authorization
    {:ok, intent2} = Mut.issue_intent(id, "alpha", "b")
    :ok = Mut.revoke(id)
    assert {:error, :invalid} = Mut.accept(id, intent2, "alpha", "b")
    # revoke between accept and grant (the child held in init): refused_at_grant, no invoke, bytes unchanged; the
    # trace window covers proceed AND the terminal transition (a late asynchronous invoke would be attributed)
    {:ok, id2} = Mut.login(s)
    before = Harness.journal_sha(root <> "/b")
    {op2, _} = Mut.accept_now!(id2, "alpha", "b")
    {^op2, op2_pid} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, op2_pid, :operation)
    :ok = Mut.revoke(id2)

    {_, calls} =
      Mut.prepare_calls(fn ->
        send(op2_pid, :proceed)
        assert {:refused_at_grant, :session_revoked} = Mut.await(op2, 3_000)
        ref = Process.monitor(op2_pid)
        assert_receive {:DOWN, ^ref, :process, ^op2_pid, _}, 5_000
      end)

    assert calls == [] and Harness.journal_sha(root <> "/b") == before
    assert :none = Mut.outcome(id2)
    # after grant: revocation while the work is REALLY held (gated append) does not cancel the accepted command; it
    # completes after release and its outcome is unreadable without the session
    {:ok, id3} = Mut.login(s)
    {op3, _} = Mut.accept_now!(id3, "alpha", "b")
    {^op3, op3_pid} = Mut.witness!(:operation_init, op3)
    Mutations.Owned.add(owned, op3_pid, :operation)
    send(op3_pid, :proceed)
    Mut.witness!(:granted, op3)
    writer3 = Mut.gated!()
    {subtree3, monitors3} = Mut.capture_subtree!(writer3)
    Mutations.Owned.add_all(owned, subtree3, :core_subtree)
    :ok = Mut.revoke(id3)
    assert Process.alive?(op3_pid) and Mut.status().occupied >= 1, "revocation aborted an accepted command"
    Mut.release(writer3)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op3, 10_000)
    assert :none = Mut.outcome(id3)
    assert Mut.join(monitors3, 20_000) == []
  end

  test "M-05 a root outside the session or the configuration is refused before Prepare (generic not-found), and Prepare receives the configured directory, never the root id" do
    %{config: c, secret: s, root: alpha} = Mut.app!()
    beta = Harness.fresh("beta")
    Mut.in_flight!(Path.join(beta, "b"))
    :ok = Application.stop(:orris_console)

    config =
      Mut.config(
        roots: %{"alpha" => alpha, "beta" => beta},
        credential_path: c[:credential_path],
        port: c[:port],
        operator: %{id: "operator", root_ids: ["alpha"]}
      )

    Harness.start_app!(config)
    cookie = Harness.login!(config, s)
    page = Harness.conn(config, :get, "/runs/alpha/a", [{"cookie", cookie}])
    token = Harness.csrf_token(page.resp_body)

    {conns, calls} =
      Mut.prepare_calls(fn ->
        for path <- [Mut.cancel_path("beta", "b"), Mut.cancel_path("gamma", "a"), Mut.cancel_path("alpha", "..%2Fb")],
            do: Mut.form(config, cookie, path, %{"_csrf_token" => token})
      end)

    assert Enum.map(conns, & &1.status) == [404, 404, 404], "RED (U1 M-05): #{inspect(Enum.map(conns, & &1.status))}"
    assert calls == []
    for conn <- conns, do: refute(conn.resp_body =~ beta or conn.resp_body =~ "beta")
    {{_, confirm}, calls} = Mut.prepare_calls(fn -> Mut.cancel!(config, cookie, "alpha", "a") end)
    assert confirm.status == 302
    assert [["a", opts]] = Mut.cancels(calls)
    assert Keyword.get(opts, :root) == alpha
    refute Enum.any?(calls, fn {_f, args} -> Enum.any?(args, &(&1 == "alpha")) end), "a root id reached Prepare"
  end
end
