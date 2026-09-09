defmodule C1.Harness do
  @moduledoc """
  TEST SUPPORT ONLY (never product code). Disposable witnesses for the C1 rows: RED attribution, real core fixtures,
  raw TCP client, disposable Bandit listener, Query-call tracing, log capture and framework telemetry counters.
  """
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias AiOrchestrator.Query

  @doc "Flunks with the exact list of missing production modules (RED attribution separated from harness failures)."
  def red!(modules) do
    missing = Enum.reject(modules, &Code.ensure_loaded?/1)

    if missing != [] do
      flunk("RED (missing production implementation): " <> Enum.map_join(missing, ", ", &inspect/1))
    end

    :ok
  end

  def core_path, do: Mix.Project.deps_paths()[:ai_orchestrator]

  def fixture(file),
    do: [core_path(), "test", "fixtures", "contracts", "scenarios", "kill9_resume", file] |> Path.join() |> File.read!()

  @doc "A fresh exclusive directory under the console build path (removed on exit)."
  def fresh(label) do
    dir =
      Path.join(Mix.Project.build_path(), "c1_#{label}_#{Base.encode16(:crypto.strong_rand_bytes(5), case: :lower)}")

    :ok = File.mkdir_p!(Path.dirname(dir))
    :ok = File.mkdir(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  @doc "A configured root holding real fixture runs; {root, %{run_ref => run_id}} from the public listing."
  def fixture_root(refs) do
    root = fresh("root")
    for ref <- refs, do: write_run(Path.join(root, ref), "events_pre_dispatch.jsonl")
    {:ok, %{runs: runs}} = Query.list_runs(root: root)
    {root, Map.new(runs, &{&1.run_ref, &1.run_id})}
  end

  @doc """
  Overrides win over defaults (Keyword.merge keeps the LAST occurrence; a `defaults ++ overrides` list keeps the
  first: review R1). Every app!/config helper merges through here.
  """
  def merged(defaults, overrides), do: Keyword.merge(defaults, overrides)

  @doc """
  A VALID fixture journal whose plan work item id IS the canary (rendered by the context fold as text): the bytes are
  the kill9_resume pre-dispatch journal with `item_a` replaced consistently (plan_recorded and assignment_requested).
  """
  def canary_run(dir) do
    File.mkdir_p!(dir)
    encoded = canary() |> Jason.encode!() |> String.slice(1..-2//1)
    File.write!(Path.join(dir, "events.jsonl"), String.replace(fixture("events_pre_dispatch.jsonl"), "item_a", encoded))
    dir
  end

  def write_run(dir, file) do
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), fixture(file))
    dir
  end

  def journal_sha(dir), do: dir |> Path.join("events.jsonl") |> File.read!() |> then(&:crypto.hash(:sha256, &1))

  def torn!(dir), do: File.write!(Path.join(dir, "events.jsonl"), ~s({"type":"torn_tail","seq":9), [:append])

  # ---- raw HTTP/1.1 over a loopback socket (no client library: the bytes on the wire are the test) ----
  def tcp_raw(port, bytes, timeout \\ 2_000) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], timeout)

    try do
      :ok = :gen_tcp.send(socket, bytes)
      recv_all(socket, "", timeout)
    after
      :gen_tcp.close(socket)
    end
  end

  def tcp_request(port, method, target, headers, body \\ "") do
    header_text = Enum.map_join(headers, "", fn {k, v} -> k <> ": " <> v <> "\r\n" end)
    text = "#{method} #{target} HTTP/1.1\r\n" <> header_text <> "\r\n" <> body
    parse_response(tcp_raw(port, text))
  end

  defp recv_all(socket, acc, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, bytes} -> if complete?(acc <> bytes), do: acc <> bytes, else: recv_all(socket, acc <> bytes, timeout)
      {:error, _closed} -> acc
    end
  end

  defp complete?(text) do
    case String.split(text, "\r\n\r\n", parts: 2) do
      [head, body] ->
        case Regex.run(~r/content-length: (\d+)/i, head) do
          [_, n] -> byte_size(body) >= String.to_integer(n)
          nil -> String.contains?(head, "101") or String.contains?(body, "\r\n0\r\n\r\n")
        end

      _ ->
        false
    end
  end

  def parse_response(text) do
    case String.split(text, "\r\n\r\n", parts: 2) do
      [head, body] ->
        [status_line | header_lines] = String.split(head, "\r\n")
        status = status_line |> String.split(" ") |> Enum.at(1, "0") |> String.to_integer()

        headers =
          Enum.map(header_lines, fn l ->
            [k, v] = String.split(l, ":", parts: 2)
            {String.downcase(k), String.trim(v)}
          end)

        %{status: status, headers: headers, body: body}

      _ ->
        %{status: 0, headers: [], body: text}
    end
  end

  def header(%{headers: headers}, name), do: for({^name, v} <- headers, do: v)

  @doc "A free loopback port (bound and released; used for the console's fixed authority in tests)."
  def free_port do
    {:ok, s} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(s)
    :gen_tcp.close(s)
    port
  end

  @doc "Runs `fun.(port)` against a disposable Bandit listener for `plug` on an ephemeral loopback port; stops it after."
  def with_listener(plug, fun) do
    {:ok, pid} = Bandit.start_link(plug: plug, ip: {127, 0, 0, 1}, port: 0, startup_log: false)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)

    try do
      fun.(port)
    after
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
      wait_closed(port)
    end
  end

  def wait_closed(port, tries \\ 50) do
    case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary], 100) do
      {:ok, s} when tries > 0 ->
        :gen_tcp.close(s)
        Process.sleep(20)
        wait_closed(port, tries - 1)

      {:ok, s} ->
        :gen_tcp.close(s)
        flunk("listener on #{port} still accepting")

      {:error, _} ->
        :ok
    end
  end

  # ---- witnesses ----
  @doc """
  Every call into AiOrchestrator.Query made by ANY process while `fun` runs: [{fun, args}]. A process never sees its
  own calls, so the TRACER is a separate process and `fun` runs in the caller (monitors, LiveView test ownership and
  mailbox stay where the row expects them: review R1). Delivery barrier: :erlang.trace_delivered before reading.
  """
  def query_calls(fun) do
    tracer = spawn_link(fn -> tracer_loop([]) end)
    :erlang.trace_pattern({Query, :_, :_}, true, [:global])
    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    try do
      result = fun.()
      ref = :erlang.trace_delivered(:all)

      receive do
        {:trace_delivered, :all, ^ref} -> :ok
      end

      send(tracer, {:calls, self()})

      receive do
        {:calls, calls} -> {result, calls}
      after
        2_000 -> flunk("tracer did not answer")
      end
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Query, :_, :_}, false, [:global])
      Process.unlink(tracer)
      Process.exit(tracer, :kill)
    end
  end

  defp tracer_loop(acc) do
    receive do
      {:trace, _pid, :call, {Query, f, args}} -> tracer_loop([{f, args} | acc])
      {:calls, to} -> send(to, {:calls, Enum.reverse(acc)})
      _other -> tracer_loop(acc)
    end
  end

  @doc "Counts framework telemetry while `fun` runs: %{router: n, socket: n} (router dispatch starts, socket connects)."
  def telemetry_counts(fun) do
    {:ok, agent} = Agent.start_link(fn -> %{router: 0, socket: 0} end)
    id = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      id,
      [[:phoenix, :router_dispatch, :start], [:phoenix, :socket_connected]],
      fn event, _m, _meta, _ ->
        key = if event == [:phoenix, :socket_connected], do: :socket, else: :router
        Agent.update(agent, &Map.update!(&1, key, fn n -> n + 1 end))
      end,
      nil
    )

    try do
      result = fun.()
      Process.sleep(30)
      {result, Agent.get(agent, & &1)}
    after
      :telemetry.detach(id)
      Agent.stop(agent)
    end
  end

  @doc "Captures every log event (all levels, crash reports included) emitted while `fun` runs; {result, text}."
  def capture_logs(fun) do
    id = :"c1_capture_#{System.unique_integer([:positive])}"
    {:ok, agent} = Agent.start_link(fn -> [] end)
    formatter = {:logger_formatter, %{single_line: true, template: [:msg, "\n"]}}
    config = %{level: :all, config: %{agent: agent}, formatter: formatter}
    :ok = :logger.add_handler(id, C1.Harness.LogSink, config)
    old = Logger.level()
    Logger.configure(level: :all)

    try do
      result = fun.()
      Process.sleep(100)
      {result, agent |> Agent.get(& &1) |> Enum.reverse() |> IO.iodata_to_binary()}
    after
      Logger.configure(level: old)
      :logger.remove_handler(id)
      Agent.stop(agent)
    end
  end

  defmodule LogSink do
    @moduledoc false
    def log(event, %{config: %{agent: agent}, formatter: {mod, fconf}}) do
      Agent.update(agent, &[mod.format(event, fconf) | &1])
    end
  end

  # ---- pinned application configuration (trusted server config; never request data) ----
  @doc "Disposable trusted configuration for one test: credential set up, one or more fixture roots, fixed authority."
  def config(overrides \\ []) do
    port = Keyword.get(overrides, :port, free_port())

    Keyword.merge(
      [
        bind: {127, 0, 0, 1},
        port: port,
        host: "localhost",
        scheme: :http,
        socket_mounts: ["/live"],
        credential_path:
          Keyword.get_lazy(overrides, :credential_path, fn -> Path.join(fresh("cred"), "console/credential") end),
        operator: %{id: "operator", root_ids: ["alpha"]},
        roots: %{},
        idle_ms: 1_800_000,
        absolute_ms: 43_200_000,
        login_capacity: 5,
        login_refill_ms: 6_000,
        session_capacity: 128,
        views_per_session: 8,
        read_deadline_ms: 2_000,
        retry_ms: 1_000,
        worker_capacity: 128,
        controller_capacity: 128,
        max_login_body: 4_096,
        sweep_ms: 1_000,
        server: false,
        # trusted server-only Query seam keys (never request data): budget_ms and monitor for the :unknown rows
        query_opts: [],
        # TEST SEAM: when a pid, every product read worker sends {:read_gate, self(), correlation} and waits for :go
        read_gate: nil,
        # TEST SEAM: when a pid, a view sends {:read_applied, self(), correlation} after applying a result
        read_witness: nil
      ],
      overrides
    )
  end

  @doc """
  The closed on-disk configuration schema for the release (review R2/R7 choice): a JSON object of plain data, no
  serialized terms, atoms or functions; secret bytes never appear in it. Returns the file path.
  """
  def config_file!(config) do
    path = Path.join(fresh("cfg"), "console.json")

    data = %{
      "bind" => config[:bind] |> :inet.ntoa() |> to_string(),
      "port" => config[:port],
      "host" => config[:host],
      "scheme" => to_string(config[:scheme]),
      "socket_mounts" => config[:socket_mounts],
      "credential_path" => config[:credential_path],
      "operator" => %{"id" => config[:operator].id, "root_ids" => config[:operator].root_ids},
      "roots" => config[:roots],
      "limits" => %{
        "idle_ms" => config[:idle_ms],
        "absolute_ms" => config[:absolute_ms],
        "login_capacity" => config[:login_capacity],
        "login_refill_ms" => config[:login_refill_ms],
        "session_capacity" => config[:session_capacity],
        "views_per_session" => config[:views_per_session],
        "read_deadline_ms" => config[:read_deadline_ms],
        "retry_ms" => config[:retry_ms],
        "worker_capacity" => config[:worker_capacity],
        "controller_capacity" => config[:controller_capacity],
        "max_login_body" => config[:max_login_body]
      }
    }

    File.write!(path, Jason.encode!(data))
    path
  end

  @doc "Raw websocket upgrade on the configured mount with the given cookie/csrf; {status, telemetry counts}."
  def socket_upgrade(config, cookie, csrf) do
    query = if csrf, do: "?vsn=2.0.0&_csrf_token=" <> URI.encode_www_form(csrf), else: "?vsn=2.0.0"

    headers =
      [
        {"host", authority(config)},
        {"origin", origin(config)},
        {"connection", "Upgrade"},
        {"upgrade", "websocket"},
        {"sec-websocket-version", "13"},
        {"sec-websocket-key", Base.encode64("0123456789abcdef")}
      ] ++
        if(cookie, do: [{"cookie", cookie}], else: [])

    {resp, counts} = telemetry_counts(fn -> tcp_request(config[:port], "GET", "/live/websocket" <> query, headers) end)
    {resp.status, counts}
  end

  def meta_csrf(html) do
    case Regex.run(~r/<meta name="csrf-token" content="([^"]+)"/, html) do
      [_, token] -> token
      nil -> flunk("no csrf meta in page")
    end
  end

  @doc "Sets up a disposable credential at the configured path and returns its secret bytes (read back from the file)."
  def credential!(config) do
    path = Keyword.fetch!(config, :credential_path)
    # product modules are reached dynamically: this support code compiles warning-free while the product is RED
    :ok = apply(OrrisConsole.Credential, :setup, [path])
    File.read!(path)
  end

  @doc "Starts the console application under `config` (stopped on exit; a real listener is proven closed); returns the config."
  def start_app!(config) do
    Application.put_env(:orris_console, :config, config)
    {:ok, _} = Application.ensure_all_started(:orris_console)

    on_exit(fn ->
      Application.stop(:orris_console)
      if config[:server], do: wait_closed(config[:port])
    end)

    config
  end

  @doc """
  Waits until the connected view has applied a successful read: the main element carries the generic marker
  `data-c1-read="complete"` (no internal identity is ever rendered). Returns the rendered HTML.
  """
  def await_read(view, tries \\ 40) do
    html = Phoenix.LiveViewTest.render(view)

    cond do
      html =~ ~s(data-c1-read="complete") ->
        html

      tries > 0 ->
        Process.sleep(50)
        await_read(view, tries - 1)

      true ->
        flunk("the view never applied a successful read: #{String.slice(html, 0, 400)}")
    end
  end

  @doc """
  Server-side witness (trusted test seam `read_witness: pid`): the view sends {:read_applied, view_pid, correlation}
  after applying a result. Waits for the given correlation and answers the rendered HTML.
  """
  def await_applied(view, correlation) do
    assert_receive {:read_applied, pid, ^correlation}, 2_000, "no applied-read witness for #{inspect(correlation)}"
    assert pid == view.pid
    Phoenix.LiveViewTest.render(view)
  end

  @doc "With `read_gate: self()` configured: receives the next gated read and releases it; {worker, correlation}."
  def release_read! do
    receive do
      {:read_gate, worker, correlation} ->
        send(worker, :go)
        {worker, correlation}
    after
      2_000 -> flunk("no gated read arrived")
    end
  end

  @doc """
  The RAW session id carried by an observed request's cookie: decoded with Plug's cookie session store using the
  endpoint's own session options and secret (public configuration, no product debug interface).
  """
  def raw_session_id(cookie) do
    key = OrrisConsole.Endpoint |> apply(:session_options, []) |> Keyword.fetch!(:key)

    value =
      cookie
      |> String.split("; ")
      |> Enum.find_value(fn kv ->
        case String.split(kv, "=", parts: 2) do
          [^key, v] -> v
          _ -> nil
        end
      end)

    assert is_binary(value), "no #{key} cookie in #{cookie}"
    opts = Plug.Session.COOKIE.init(apply(OrrisConsole.Endpoint, :session_options, []))
    conn = %{Plug.Test.conn(:get, "/") | secret_key_base: apply(OrrisConsole.Endpoint, :config, [:secret_key_base])}
    {_sid, session} = Plug.Session.COOKIE.get(conn, value, opts)
    id = session["session_id"]
    assert is_binary(id) and byte_size(id) >= 16, "no raw session id in the cookie session: #{inspect(session)}"
    {id, value}
  end

  @doc "Bounded wait for a condition that settles asynchronously (Registry key removal after a proven death)."
  def eventually(fun, tries \\ 40) do
    cond do
      fun.() ->
        true

      tries > 0 ->
        Process.sleep(25)
        eventually(fun, tries - 1)

      true ->
        false
    end
  end

  @doc "Whether `term` contains `needle` (a binary) anywhere in its structure (maps, lists, tuples)."
  def term_contains?(needle, needle), do: true
  def term_contains?(term, needle) when is_map(term), do: term |> Map.to_list() |> term_contains?(needle)
  def term_contains?(term, needle) when is_list(term), do: Enum.any?(term, &term_contains?(&1, needle))
  def term_contains?(term, needle) when is_tuple(term), do: term |> Tuple.to_list() |> term_contains?(needle)
  def term_contains?(_term, _needle), do: false

  @doc "Every textual rendering a log could carry for a binary secret: hex, base64 and its inspect form."
  def renderings(binary),
    do: [
      Base.encode16(binary, case: :lower),
      Base.encode16(binary, case: :upper),
      Base.encode64(binary),
      inspect(binary)
    ]

  def authority(config), do: "#{config[:host]}:#{config[:port]}"
  def origin(config), do: "http://" <> authority(config)

  @doc "Endpoint listener port when `server: true` (Phoenix server_info)."
  def endpoint_port do
    {:ok, {_ip, port}} = apply(OrrisConsole.Endpoint, :server_info, [:http])
    port
  end

  # ---- in-process HTTP against the endpoint (Plug.Test; cookies carried by the caller) ----
  def conn(config, method, path, headers \\ [], body \\ nil) do
    base = Plug.Test.conn(method, origin(config) <> path, body)
    # the raw Host header is set directly (Plug's header helper refuses "host" on test conns); host/port/scheme
    # come from the URL exactly as a real listener would parse them
    conn = %{base | req_headers: [{"host", authority(config)} | headers] ++ base.req_headers}

    # the framework renders error pages (404, 413, invalid CSRF) and then re-raises the exception; the response it
    # already sent is recovered from the test adapter, exactly what a real client observed
    try do
      apply(OrrisConsole.Endpoint, :call, [conn, apply(OrrisConsole.Endpoint, :init, [[]])])
    rescue
      e ->
        try do
          {status, resp_headers, body} = Plug.Test.sent_resp(conn)
          %{conn | status: status, resp_headers: resp_headers, resp_body: body, state: :sent}
        rescue
          _ -> reraise(e, __STACKTRACE__)
        end
    end
  end

  def cookie(conn) do
    conn
    |> Plug.Conn.get_resp_header("set-cookie")
    |> Enum.map(&(&1 |> String.split(";") |> hd()))
    |> Enum.join("; ")
  end

  def csrf_token(html) do
    case Regex.run(~r/name="_csrf_token"[^>]*value="([^"]+)"/, html) do
      [_, token] -> token
      nil -> flunk("no _csrf_token in login form")
    end
  end

  @doc "Full login through the real endpoint: GET /login (prelogin cookie + CSRF), POST /login; returns the session cookie."
  def login!(config, secret) do
    get = conn(config, :get, "/login")
    assert get.status == 200
    token = csrf_token(get.resp_body)
    form = URI.encode_query(%{"_csrf_token" => token, "credential" => Base.encode16(secret, case: :lower)})

    post =
      conn(
        config,
        :post,
        "/login",
        [{"origin", origin(config)}, {"cookie", cookie(get)}, {"content-type", "application/x-www-form-urlencoded"}],
        form
      )

    assert post.status == 302, "login did not redirect: #{post.status} #{String.slice(post.resp_body, 0, 200)}"
    cookie(post)
  end

  @doc "XSS/context canary written into a run's context event; must render escaped, never executable."
  def canary, do: ~s(<script>window.C1_XSS=1</script>CTX_CANARY_7f3a)
end
