defmodule C1.CDP do
  @moduledoc """
  TEST SUPPORT ONLY: drives the pinned chrome-headless-shell over the DevTools protocol with a minimal websocket client
  (no browser automation dependency). The browser path is an explicit test setting (C1_BROWSER); the test owns the OS
  process and its disposable profile: kill, wait for exit, then remove.
  """
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  @doc "The pinned browser from the explicit test setting; absence is a clear failure, never a skip."
  def binary do
    case Application.get_env(:orris_console, :c1_browser) do
      path when is_binary(path) and path != "" -> path
      _absent -> flunk("browser prerequisite absent: set C1_BROWSER to the pinned chrome-headless-shell path")
    end
  end

  # Chrome for Testing 149: the existing Playwright macOS shell and the upstream
  # linux64 headless-shell archive pinned by the console qualification CI job.
  @pinned %{
    {:unix, :darwin} =>
      {"Google Chrome for Testing 149.0.7827.55", "11e393326c7d20a7c56641a7c65def33ea9c280da3b0b74cf8563b07989a0ee3"},
    {:unix, :linux} =>
      {"Google Chrome for Testing 149.0.7827.55", "670ba079b75107746ba41abad131180a31a7c7219aa1bd4061fb471f4535d541"}
  }

  @doc "Measured identity of the browser at the setting, asserted equal to the pinned identity for this platform."
  def identity! do
    path = binary()
    assert File.exists?(path), "pinned browser missing at #{path}"
    {out, 0} = System.cmd(path, ["--version"], stderr_to_stdout: true)

    measured =
      {String.trim(out), path |> File.read!() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)}

    expected = Map.fetch!(@pinned, :os.type())
    assert measured == expected, "browser identity mismatch: measured #{inspect(measured)}, pinned #{inspect(expected)}"
    measured
  end

  @doc "Starts the browser with a disposable profile; returns %{port, os_pid, os_port, profile}."
  def launch! do
    profile = Path.join(System.tmp_dir!(), "c1-profile-#{System.unique_integer([:positive])}")
    File.mkdir_p!(profile)
    port = C1.Harness.free_port()

    args = [
      "--headless=new",
      "--disable-gpu",
      "--no-first-run",
      "--no-default-browser-check",
      "--user-data-dir=#{profile}",
      "--remote-debugging-port=#{port}",
      "about:blank"
    ]

    os_port = Port.open({:spawn_executable, binary()}, [:binary, :exit_status, :stderr_to_stdout, args: args])
    {:os_pid, os_pid} = Port.info(os_port, :os_pid)
    on_exit(fn -> stop!(os_port, os_pid, profile) end)
    wait_ready(port, 100)
    %{port: port, os_pid: os_pid, os_port: os_port, profile: profile}
  end

  defmodule OS do
    @moduledoc "The OS commands teardown uses (injectable for mock negative controls)."
    def pgrep_profile(profile),
      do: pids(System.cmd("pgrep", ["-f", "user-data-dir=#{profile}"], stderr_to_stdout: true))

    def children(pid), do: pids(System.cmd("pgrep", ["-P", Integer.to_string(pid)], stderr_to_stdout: true))

    def command_line(pid) do
      case System.cmd("ps", ["-o", "command=", "-p", Integer.to_string(pid)], stderr_to_stdout: true) do
        {out, 0} -> String.trim(out)
        _ -> ""
      end
    end

    def kill9(pid), do: System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true)
    def alive?(pid), do: match?({_, 0}, System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true))

    defp pids({out, 0}), do: out |> String.split() |> Enum.map(&String.to_integer/1)
    defp pids(_), do: []
  end

  @doc """
  Ownership-verified, idempotent teardown. Ownership is never a retained numeric pid: a process is owned only if its
  CURRENT command line names this test's unique profile (a main) or it is a live descendant of such a main whose
  command line is the browser binary. Descendants are captured BEFORE the mains are signalled and each signalled
  pid is proven gone by its own exit witness (kill -0), not by its parent's or the profile's disappearance. A second
  call finds no owned process and issues no signal. Answers {:ok, signalled_pids}.
  """
  def stop!(os_port, _os_pid, profile, os \\ OS) do
    mains = owned_mains(profile, os)

    descendants =
      mains
      |> Enum.flat_map(&descendants(&1, os))
      |> Enum.filter(&browser_executable?(call(os, :command_line, [&1])))
      |> Enum.uniq()

    targets = Enum.uniq(mains ++ descendants)
    for pid <- targets, do: call(os, :kill9, [pid])
    for pid <- targets, do: wait_exit(pid, os, 100)
    if Port.info(os_port) != nil, do: Port.close(os_port)
    File.rm_rf!(profile)
    {:ok, targets}
  end

  @doc "Whether an owned browser process for `profile` is still alive: the SAME ownership predicate as stop!/4."
  def alive?(profile, os \\ OS) do
    mains = owned_mains(profile, os)

    mains != [] or
      Enum.any?(mains, fn m ->
        Enum.any?(descendants(m, os), &(browser_executable?(call(os, :command_line, [&1])) and call(os, :alive?, [&1])))
      end)
  end

  # ownership model (test-only, current command line): a main is owned when its executable IS the configured
  # browser binary AND one of its arguments is exactly `--user-data-dir=<profile>` (argument boundary, so a profile
  # that is a prefix of another, or the option text inside a foreign argument, never matches)
  defp owned_mains(profile, os) do
    profile
    |> then(&call(os, :pgrep_profile, [&1]))
    |> Enum.filter(&owned_main?(call(os, :command_line, [&1]), profile))
  end

  def owned_main?(command_line, profile) do
    browser_executable?(command_line) and ("--user-data-dir=" <> profile) in tl(String.split(command_line, " "))
  end

  # executable identity: the command's program is the configured browser binary (helpers run the same binary)
  def browser_executable?(command_line), do: List.first(String.split(command_line, " ")) == binary()

  # the real OS layer is a module; a mock is a struct whose module takes the struct as its first argument
  defp call(os, fun, args) when is_atom(os), do: apply(os, fun, args)
  defp call(%{__struct__: mod} = os, fun, args), do: apply(mod, fun, [os | args])

  defp descendants(pid, os), do: pid |> then(&call(os, :children, [&1])) |> Enum.flat_map(&[&1 | descendants(&1, os)])

  defp wait_exit(pid, os, tries) do
    cond do
      not call(os, :alive?, [pid]) ->
        :ok

      tries > 0 ->
        Process.sleep(50)
        wait_exit(pid, os, tries - 1)

      true ->
        flunk("owned browser process #{pid} still alive after its signal")
    end
  end

  defp wait_ready(_port, 0), do: flunk("browser debugging port not ready")

  defp wait_ready(port, n) do
    case http_json(port, "/json/version") do
      {:ok, _} ->
        :ok

      _ ->
        Process.sleep(100)
        wait_ready(port, n - 1)
    end
  end

  def http_json(port, path, method \\ "GET") do
    resp = C1.Harness.tcp_request(port, method, path, [{"host", "127.0.0.1:#{port}"}])
    if resp.status == 200, do: Jason.decode(resp.body), else: {:error, resp}
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  @doc "Opens a page (new target) and a websocket to it; returns the session map."
  def open_page!(%{port: port}) do
    {:ok, target} = http_json(port, "/json/new?about:blank", "PUT")
    ws = URI.parse(target["webSocketDebuggerUrl"])
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, ws.port, [:binary, active: false], 2_000)
    key = Base.encode64(:crypto.strong_rand_bytes(16))

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{ws.path} HTTP/1.1\r\nHost: 127.0.0.1:#{ws.port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" <>
          "Sec-WebSocket-Key: #{key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
      )

    {:ok, reply} = :gen_tcp.recv(socket, 0, 2_000)
    assert String.starts_with?(reply, "HTTP/1.1 101"), "websocket handshake refused: #{inspect(reply)}"
    [_, rest] = String.split(reply, "\r\n\r\n", parts: 2)
    on_exit(fn -> :gen_tcp.close(socket) end)
    %{socket: socket, buffer: rest, id: 0, target: target["id"], events: []}
  end

  @doc "Sends one CDP command and returns {result, session}; events seen meanwhile are kept in session.events."
  def call!(session, method, params \\ %{}) do
    id = session.id + 1
    payload = Jason.encode!(%{id: id, method: method, params: params})
    :ok = :gen_tcp.send(session.socket, frame(payload))
    await(%{session | id: id}, id)
  end

  defp await(session, id) do
    {msg, session} = next_message(session)

    case Jason.decode!(msg) do
      %{"id" => ^id, "result" => result} -> {result, session}
      %{"id" => ^id, "error" => error} -> flunk("CDP error: #{inspect(error)}")
      %{"method" => _} = event -> await(%{session | events: [event | session.events]}, id)
      _other -> await(session, id)
    end
  end

  defp next_message(%{buffer: buffer} = session) do
    case parse_frame(buffer) do
      {:ok, payload, rest} ->
        {payload, %{session | buffer: rest}}

      :more ->
        {:ok, bytes} = :gen_tcp.recv(session.socket, 0, 10_000)
        next_message(%{session | buffer: buffer <> bytes})
    end
  end

  # client frames are masked; server frames arrive unmasked (text opcode 1, no fragmentation for CDP replies)
  defp frame(payload) do
    mask = :crypto.strong_rand_bytes(4)
    len = byte_size(payload)

    header =
      cond do
        len < 126 -> <<0x81, 1::1, len::7>>
        len < 65_536 -> <<0x81, 1::1, 126::7, len::16>>
        true -> <<0x81, 1::1, 127::7, len::64>>
      end

    header <> mask <> mask_bytes(payload, mask)
  end

  defp mask_bytes(payload, <<m::binary-size(4)>>) do
    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {b, i} -> Bitwise.bxor(b, :binary.at(m, rem(i, 4))) end)
    |> :binary.list_to_bin()
  end

  defp parse_frame(<<_fin_rsv_op::8, 0::1, len::7, rest::binary>>) when len < 126, do: take(rest, len)
  defp parse_frame(<<_fin_rsv_op::8, 0::1, 126::7, len::16, rest::binary>>), do: take(rest, len)
  defp parse_frame(<<_fin_rsv_op::8, 0::1, 127::7, len::64, rest::binary>>), do: take(rest, len)
  defp parse_frame(_), do: :more

  defp take(rest, len) when byte_size(rest) >= len do
    <<payload::binary-size(^len), tail::binary>> = rest
    {:ok, payload, tail}
  end

  defp take(_, _), do: :more

  @doc """
  Navigates and waits for THAT navigation: the document location must equal the requested URL (or `expect`, the
  redirect target) AND readyState complete; a stale complete state of the previous page never counts.
  """
  def navigate!(session, url, expect \\ nil) do
    {_, session} = call!(session, "Page.enable")
    {_, session} = call!(session, "Network.enable")
    {_, session} = call!(session, "Page.navigate", %{url: url})
    wait_loaded(session, expect || url, 50)
  end

  defp wait_loaded(session, expect, 0) do
    {value, session} = eval!(session, "document.readyState + ' ' + location.href")
    {text, _} = eval!(session, "document.body ? document.body.innerText.slice(0, 300) : ''")
    flunk("navigation to #{expect} never completed; browser shows: #{value}; page text: #{inspect(text)}")
  end

  defp wait_loaded(session, expect, n) do
    {value, session} = eval!(session, "document.readyState + ' ' + location.href")

    case String.split(value, " ", parts: 2) do
      ["complete", href] when href == expect ->
        session

      _ ->
        Process.sleep(100)
        wait_loaded(session, expect, n - 1)
    end
  end

  @doc "Waits (bounded) until a navigation the page started itself (a form submit) has landed on `expected`."
  def await_location!(session, expected), do: wait_loaded(session, expected, 50)

  @doc "Response headers (lowercased names) of the most recent document response seen on this session."
  def document_headers(session) do
    session.events
    |> Enum.find(&(&1["method"] == "Network.responseReceived" and get_in(&1, ["params", "type"]) == "Document"))
    |> case do
      nil -> flunk("no document response observed")
      event -> event |> get_in(["params", "response", "headers"]) |> Map.new(fn {k, v} -> {String.downcase(k), v} end)
    end
  end

  def eval!(session, expression) do
    {result, session} =
      call!(session, "Runtime.evaluate", %{expression: expression, returnByValue: true, awaitPromise: true})

    {get_in(result, ["result", "value"]), session}
  end
end
