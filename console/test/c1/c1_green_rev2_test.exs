defmodule C1.GreenRev2Test do
  @moduledoc "G1-G4 regression rows adopted from the GREEN review (logs/console-c1-green/codex/review-4c11648)."
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest
  @endpoint OrrisConsole.Endpoint
  alias C1.Harness
  alias OrrisConsole.Reader

  defp app!(overrides) do
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(Harness.merged([roots: %{"alpha" => root}], overrides))
    secret = Harness.credential!(config)
    Harness.start_app!(config)
    %{config: config, secret: secret, root: root}
  end

  test "G1a Reader seam: an accepted read failure keeps the last successful value, marks stale and schedules one retry" do
    app!([])

    state =
      Reader.initial("seam")
      |> Map.merge(%{valid?: fn -> true end, data: {:ok, :last_success}, current: {self(), :current}})

    {after_failure, :applied} = Reader.receive_result(state, {:query_result, self(), :current, {:error, :deadline}})
    Process.cancel_timer(after_failure.timer)
    assert after_failure.stale? and after_failure.delayed?
    assert Reader.value(after_failure) == :last_success

    {recovered, :applied} =
      Reader.receive_result(%{after_failure | current: {self(), :next}}, {:query_result, self(), :next, {:ok, :fresh}})

    Process.cancel_timer(recovered.timer)
    refute recovered.stale?
    assert Reader.value(recovered) == :fresh
  end

  test "G1b the detail view keeps its last successful data through a failed read (stale with its timestamp) and recovers" do
    clock = C1.Clock.start!()
    %{config: c, secret: s, root: root} = app!(clock: C1.Clock.fun(clock), retry_ms: 100)
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    html = Harness.await_read(view)
    assert html =~ "run_scenario_0001"
    good = File.read!(Path.join(root, "a/events.jsonl"))
    File.write!(Path.join(root, "a/events.jsonl"), "{corrupt\n")
    C1.Clock.advance(clock, 3_000)
    send(view.pid, :refresh)
    Process.sleep(150)
    stale = render(view)

    assert stale =~ "stale" and stale =~ "run_scenario_0001",
           "the last successful data vanished: #{String.slice(stale, 0, 200)}"

    assert stale =~ "last successful read: #{C1.Clock.now(clock) - 3_000}"
    File.write!(Path.join(root, "a/events.jsonl"), good)
    assert Harness.eventually(fn -> not (render(view) =~ "stale") end, 60), "the view never recovered"
    assert render(view) =~ "run_scenario_0001"
  end

  test "G2a a controller killed during the first admitted read: the worker dies, the view survives and retries through bounded admission" do
    %{config: c, secret: s} = app!(read_gate: self(), retry_ms: 50, read_deadline_ms: 2_000)
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    assert_receive {:read_gate, worker, _}
    [{job, :controller}] = Registry.lookup(OrrisConsole.QueryRegistry, {:controller, view.pid})
    ref = Process.monitor(worker)
    Process.exit(job, :kill)
    assert_receive {:DOWN, ^ref, :process, ^worker, _}
    assert Process.alive?(view.pid)
    assert_receive {:read_gate, replacement, _}, 500
    assert replacement != worker
    assert render(view) =~ "refresh delayed"
    send(replacement, :go)
    assert Harness.await_read(view) =~ ~s(href="/runs/alpha/a")
  end

  test "G2b a controller lost after a success keeps the data stale and retries; a stale DOWN is ignored; Store loss still closes the view" do
    %{config: c, secret: s} = app!(read_gate: self(), retry_ms: 50, read_deadline_ms: 2_000)
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    Harness.release_read!()
    assert Harness.await_read(view) =~ ~s(href="/runs/alpha/a")
    send(view.pid, :refresh)
    assert_receive {:read_gate, worker, _}
    [{job, :controller}] = Registry.lookup(OrrisConsole.QueryRegistry, {:controller, view.pid})
    Process.exit(job, :kill)
    assert_receive {:read_gate, replacement, _}, 500
    assert replacement != worker
    stale = render(view)

    assert stale =~ "stale" and stale =~ ~s(href="/runs/alpha/a"),
           "after controller loss: #{stale |> String.split("<main", parts: 2) |> List.last() |> String.slice(0, 700)}"

    # a DOWN that is not the current controller's monitor changes nothing
    send(view.pid, {:DOWN, make_ref(), :process, self(), :stale})
    Process.sleep(30)
    assert Process.alive?(view.pid)
    send(replacement, :go)
    assert Harness.eventually(fn -> not (render(view) =~ "stale") end, 60)
    ref = Process.monitor(view.pid)
    Process.exit(Process.whereis(OrrisConsole.SessionStore), :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
  end

  defp login_bytes(c, secret, pad) do
    pre = Harness.tcp_request(c[:port], "GET", "/login", [{"host", Harness.authority(c)}])
    assert pre.status == 200
    cookie = pre |> Harness.header("set-cookie") |> Enum.map(&(&1 |> String.split(";") |> hd())) |> Enum.join("; ")
    token = Harness.csrf_token(pre.body)

    form =
      URI.encode_query(%{"_csrf_token" => token, "credential" => Base.encode16(secret, case: :lower), "pad" => pad})

    {cookie, form}
  end

  test "G3 the validated login body limit applies to the bytes actually read: chunked and declared oversized bodies are 413, the limiter stays uncharged, and an ordinary login within the limit succeeds" do
    %{config: c, secret: s} = app!(server: true, max_login_body: 256)
    a = Harness.authority(c)
    o = Harness.origin(c)
    {cookie, form} = login_bytes(c, s, String.duplicate("x", 512))
    chunked = Integer.to_string(byte_size(form), 16) <> "\r\n" <> form <> "\r\n0\r\n\r\n"
    headers = [{"host", a}, {"origin", o}, {"cookie", cookie}, {"content-type", "application/x-www-form-urlencoded"}]

    assert Harness.tcp_request(c[:port], "POST", "/login", headers ++ [{"transfer-encoding", "chunked"}], chunked).status ==
             413

    assert Harness.tcp_request(
             c[:port],
             "POST",
             "/login",
             headers ++ [{"content-length", Integer.to_string(byte_size(form))}],
             form
           ).status == 413

    assert OrrisConsole.SessionStore.counts(OrrisConsole.SessionStore).sessions == 0
    {cookie2, small} = login_bytes(c, s, "")
    assert byte_size(small) < 256

    ok =
      Harness.tcp_request(
        c[:port],
        "POST",
        "/login",
        [
          {"host", a},
          {"origin", o},
          {"cookie", cookie2},
          {"content-type", "application/x-www-form-urlencoded"},
          {"content-length", Integer.to_string(byte_size(small))}
        ],
        small
      )

    assert ok.status == 302
  end

  test "G4 HTTP/2 prior knowledge is not negotiated by the C1 listener; HTTP/1.1 keeps working" do
    %{config: c} = app!(server: true)
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, c[:port], [:binary, active: false], 1_000)

    try do
      :ok = :gen_tcp.send(socket, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" <> <<0::24, 4, 0, 0::32>>)
      response = :gen_tcp.recv(socket, 0, 1_000)
      refute match?({:ok, <<_len::24, 4, _rest::binary>>}, response), "server SETTINGS received: HTTP/2 negotiated"
    after
      :gen_tcp.close(socket)
    end

    assert Harness.tcp_request(c[:port], "GET", "/login", [{"host", Harness.authority(c)}]).status == 200
  end
end
