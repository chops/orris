defmodule C1.BrowserTest do
  @moduledoc "C1-14: the pinned headless browser against the disposable production release server (built by bin/c1-release) configured through the closed JSON file."
  use ExUnit.Case, async: false
  alias C1.Harness

  @release Path.expand("../../_build/prod/rel/orris_console/bin/orris_console", __DIR__)
  # the page text once the connected view applied its first read (the HTTP render is a loading shell); bounded wait
  @read_complete "new Promise((r, x) => { const t0 = Date.now(); const poll = () => { if (document.querySelector('[data-c1-read=\"complete\"]')) r(document.body.innerText); else if (Date.now() - t0 > 5000) r('TIMEOUT ' + document.body.innerText); else setTimeout(poll, 100) }; poll() })"
  @csp "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"

  test "C1-14 login, list, detail, live refresh, CSP enforcement, logout and reconnect in the pinned headless browser; release, browser and profile owned and gone afterwards" do
    Harness.red!([OrrisConsole.Application, OrrisConsole.Endpoint, OrrisConsole.RunIndexLive])
    assert File.exists?(@release), "RED (C1-01 release stage): production release not built at #{@release}"
    {version, sha} = C1.CDP.identity!()
    IO.puts("\n[C1-14] browser #{version} sha256 #{sha} (matches the pinned identity)")
    root = Harness.fresh("root")
    Harness.canary_run(Path.join(root, "a"))
    {:ok, %{runs: [%{run_id: run_id}]}} = AiOrchestrator.Query.list_runs(root: root)
    # the browser is pointed at the loopback address itself: "localhost" resolves to ::1 first in the browser while
    # the console binds one loopback address (127.0.0.1), and the acceptance must not depend on that fallback
    config = Harness.config(roots: %{"alpha" => root}, server: true, host: "127.0.0.1")
    secret = Harness.credential!(config)
    file = Harness.config_file!(config)
    env = [{~c"ORRIS_CONSOLE_CONFIG_FILE", String.to_charlist(file)}]

    # every resource installs its cleanup at acquisition, BEFORE the next fallible startup; cleanups run in reverse
    # on every path and a failing cleanup never skips the others (C1.Resources, H-15)
    C1.Resources.run(fn ->
      C1.Resources.acquire(:credential, fn -> config[:credential_path] end, &File.rm!/1)

      release =
        C1.Resources.acquire(
          :release,
          fn ->
            Port.open({:spawn_executable, @release}, [
              :binary,
              :exit_status,
              :stderr_to_stdout,
              args: ["start"],
              env: env
            ])
          end,
          fn port ->
            {:os_pid, pid} = Port.info(port, :os_pid) || {:os_pid, nil}
            System.cmd(@release, ["stop"], env: [{"ORRIS_CONSOLE_CONFIG_FILE", file}], stderr_to_stdout: true)
            if pid, do: gone!(pid, 100)
            Harness.wait_closed(config[:port])
          end
        )

      assert Port.info(release) != nil
      ready!(config[:port], 100)

      browser =
        C1.Resources.acquire(:browser, fn -> C1.CDP.launch!() end, fn b ->
          {:ok, _} = C1.CDP.stop!(b.os_port, b.os_pid, b.profile)
          refute(File.exists?(b.profile))
        end)

      browse!(config, browser, root, secret, run_id)
    end)
  end

  defp browse!(config, browser, root, secret, run_id) do
    session = C1.CDP.open_page!(browser)
    url = Harness.origin(config)
    session = C1.CDP.navigate!(session, url <> "/", url <> "/login")
    assert %{"content-security-policy" => @csp} = C1.CDP.document_headers(session)

    {_, session} =
      C1.CDP.eval!(
        session,
        "document.querySelector('input[name=credential]').value = #{Jason.encode!(Base.encode16(secret, case: :lower))}; document.querySelector('form').submit(); 'submitted'"
      )

    # the submit starts a navigation the page owns: wait for it to land (the redirect sets the renewed cookie)
    session = C1.CDP.await_location!(session, url <> "/")
    session = C1.CDP.navigate!(session, url <> "/")
    {index, session} = C1.CDP.eval!(session, @read_complete)
    assert index =~ run_id
    session = C1.CDP.navigate!(session, url <> "/runs/alpha/a")
    {detail, session} = C1.CDP.eval!(session, @read_complete)
    assert detail =~ "CTX_CANARY_7f3a"
    {xss, session} = C1.CDP.eval!(session, "String(window.C1_XSS)")
    assert xss == "undefined"

    {connected, session} =
      C1.CDP.eval!(
        session,
        "new Promise(r => setTimeout(() => r(document.querySelector('[data-phx-main]').classList.contains('phx-connected')), 1500))"
      )

    assert connected == true
    # CSP enforcement: a controlled inline script is blocked (violation event) while the LiveSocket stays connected
    {blocked, session} =
      C1.CDP.eval!(
        session,
        "new Promise(r => { let hit = false; document.addEventListener('securitypolicyviolation', () => { hit = true }); const s = document.createElement('script'); s.textContent = 'window.C1_INLINE = 1'; document.body.appendChild(s); setTimeout(() => r(String(hit) + ' ' + String(window.C1_INLINE) + ' ' + document.querySelector('[data-phx-main]').classList.contains('phx-connected')), 300) })"
      )

    assert blocked == "true undefined true"
    Harness.torn!(Path.join(root, "a"))
    {repair, session} = C1.CDP.eval!(session, "new Promise(r => setTimeout(() => r(document.body.innerText), 1500))")
    assert repair =~ "pending repair"
    session = C1.CDP.navigate!(session, url <> "/runs/alpha/a")

    {reconnected, session} =
      C1.CDP.eval!(
        session,
        "new Promise(r => setTimeout(() => r(document.querySelector('[data-phx-main]').classList.contains('phx-connected')), 1500))"
      )

    assert reconnected == true
    {_, session} = C1.CDP.eval!(session, "document.querySelector('form[action=\"/logout\"]').submit(); 'out'")
    session = C1.CDP.await_location!(session, url <> "/login")
    session = C1.CDP.navigate!(session, url <> "/runs/alpha/a", url <> "/login")
    {after_logout, _session} = C1.CDP.eval!(session, "location.pathname")
    assert after_logout == "/login"
  end

  defp ready!(port, tries) do
    case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary], 200) do
      {:ok, s} ->
        :gen_tcp.close(s)

      {:error, _} when tries > 0 ->
        Process.sleep(100)
        ready!(port, tries - 1)

      {:error, reason} ->
        flunk("release server never listened: #{inspect(reason)}")
    end
  end

  defp gone!(os_pid, tries) do
    case System.cmd("kill", ["-0", Integer.to_string(os_pid)], stderr_to_stdout: true) do
      {_, 0} when tries > 0 ->
        Process.sleep(100)
        gone!(os_pid, tries - 1)

      {_, 0} ->
        flunk("release process #{os_pid} still alive")

      _ ->
        :ok
    end
  end
end
