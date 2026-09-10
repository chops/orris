defmodule OrrisConsole.GateControlsTest do
  use ExUnit.Case, async: false

  import Bitwise

  @moduletag timeout: 90_000
  @source Path.expand("../..", __DIR__)
  @toolchain "elixir-1.20.4-otp-29.0.5"

  setup do
    root = Path.join(System.tmp_dir!(), "orris-gate-controls-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    # Register independent cleanup before creating any subprocess or fallible fixture.
    on_exit(fn ->
      for mark <- ["pid", "kid", "runner.pid", "sentinel.pid"],
          {:ok, contents} <- [File.read(Path.join(root, mark))],
          {pid, _} <- [Integer.parse(contents)] do
        if alive?(pid) do
          System.cmd("kill", ["-KILL", Integer.to_string(pid)], stderr_to_stdout: true)
          await_gone(pid)
        end
      end

      File.rm_rf!(root)
    end)

    {:ok, root: root}
  end

  defp script(path, contents) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, contents)
    File.chmod!(path, 0o755)
  end

  defp copy(root, name) do
    dest = Path.join(root, "console/bin/#{name}")
    File.mkdir_p!(Path.dirname(dest))
    File.cp!(Path.join(@source, "bin/#{name}"), dest)
    File.chmod!(dest, 0o755)
  end

  defp quote_sh(s), do: "'" <> String.replace(s, "'", "'\\''") <> "'"

  defp fixture(root, packaging_exit \\ 0) do
    for path <- [
          "console/bin",
          "mockbin",
          "bin",
          "_build/#{@toolchain}",
          "console/priv/static/assets",
          "console/_build"
        ] do
      File.mkdir_p!(Path.join(root, path))
    end

    for name <- ["verify", "c1-core-escript-check", "c1-escript-app-count"] do
      copy(root, name)
    end

    for name <- ["c1-assets", "c1-release", "c1-release-smoke"] do
      script(Path.join(root, "console/bin/#{name}"), "#!/bin/sh\nexit 0\n")
    end

    records =
      if packaging_exit == 0 do
        ~S"""
        echo 1 > "$out/negative-writer.exit"
        echo 0 > "$out/positive-query.exit"
        echo 0 > "$out/clean.exit"
        echo 'warning: forbidden reference to AiOrchestrator.Journal.Writer' > "$out/negative-writer.log"
        """
      else
        ""
      end

    script(
      Path.join(root, "console/bin/c1-packaging-controls"),
      "#!/bin/sh\nout=$1\nmkdir -p \"$out\"\n" <> records <> "\nexit #{packaging_exit}\n"
    )

    real_elixir = System.find_executable("elixir")

    script(
      Path.join(root, "mockbin/elixir"),
      "#!/bin/sh\ncase \"$*\" in *System.version*) printf 1.20.4;; *OTP_VERSION*) printf 29.0.5;; *) exec #{quote_sh(real_elixir)} \"$@\";; esac\n"
    )

    for name <- ["git", "shasum", "browser"] do
      script(Path.join(root, "mockbin/#{name}"), "#!/bin/sh\nexit 0\n")
    end

    entries = for i <- 1..18, do: {String.to_charlist("app#{i}/ebin/x.beam"), <<>>}
    {:ok, {_, archive}} = :zip.create(~c"mock.zip", entries, [:memory])
    :ok = :escript.create(String.to_charlist(Path.join(root, "mock.escript")), shebang: :default, archive: archive)

    script(
      Path.join(root, "mockbin/mix"),
      ~s{#!/bin/sh\ncase "$*" in *escript.build*) cp "$root/mock.escript" "$root/bin/ai-orchestrator";; esac\nexit 0\n}
    )

    [
      {"PATH", Path.join(root, "mockbin") <> ":" <> System.get_env("PATH")},
      {"root", root},
      {"C1_BROWSER", Path.join(root, "mockbin/browser")},
      {"OUT", Path.join(root, "evidence")}
    ]
  end

  defp run(root, name, env) do
    # The wrapper becomes the script without creating another child. Its identity
    # is available to the outer cleanup even if an ExUnit assertion or timeout fires.
    result =
      System.cmd("bash", ["-c", ~S(echo $$ > "$root/runner.pid"; exec bash "$1"), "gate-control", "bin/#{name}"],
        cd: Path.join(root, "console"),
        env: env,
        stderr_to_stdout: true
      )

    File.rm!(Path.join(root, "runner.pid"))
    result
  end

  defp summary(root), do: File.read!(Path.join(root, "evidence/SUMMARY.txt"))
  defp scratches(root), do: Path.wildcard(Path.join(root, "_build/#{@toolchain}/verify-escript.*"))

  defp artifact(root, contents) do
    path = Path.join(root, "bin/ai-orchestrator")
    File.write!(path, contents)
    File.chmod!(path, 0o640)
    path
  end

  test "C-1 refuses stale evidence and fails a crashed packaging runner", %{root: root} do
    env = fixture(root, 42)
    stale = Path.join(root, "evidence/06-packaging")
    File.mkdir_p!(stale)

    for {name, code} <- [{"negative-writer", 1}, {"positive-query", 0}, {"clean", 0}] do
      File.write!(Path.join(stale, "#{name}.exit"), "#{code}\n")
    end

    assert {output, 3} = run(root, "verify", env)
    assert output =~ "refusing nonempty"
    File.rm_rf!(Path.join(root, "evidence"))
    assert {_, rc} = run(root, "verify", env)
    assert rc != 0
    assert summary(root) =~ "06-packaging: exit 1"
    assert summary(root) =~ "verify: FAIL"
    File.rm_rf!(Path.join(root, "evidence"))
    File.mkdir_p!(Path.join(root, "evidence"))
    assert {_, rc} = run(root, "verify", env)
    assert rc != 0
  end

  test "C-2 preserves an existing escript's bytes and mode, and removes a newly built artifact", %{root: root} do
    env = fixture(root)
    path = artifact(root, "original")
    assert {output, 0} = run(root, "verify", env)
    assert summary(root) =~ "verify: PASS", output
    assert summary(root) =~ "12-core-escript-18: exit 0"
    assert File.read!(path) == "original"
    assert (File.stat!(path).mode &&& 0o777) == 0o640
    assert scratches(root) == []
    File.rm!(path)
    assert {_, 0} = run(root, "c1-core-escript-check", env)
    refute File.exists?(path)
    assert scratches(root) == []
  end

  for {label, injection} <- [{"backup failure", :backup}, {"build failure", :build}, {"restore failure", :restore}] do
    test "R1 #{label} keeps the original artifact available", %{root: root} do
      env = fixture(root)
      path = artifact(root, "original")

      case unquote(injection) do
        :backup ->
          script(Path.join(root, "mockbin/cp"), ~S"""
          #!/bin/sh
          case "$3" in */preserved) exit 42;; esac
          exec /bin/cp "$@"
          """)

        :restore ->
          script(Path.join(root, "mockbin/cp"), ~S"""
          #!/bin/sh
          case "$2" in */preserved) exit 42;; esac
          exec /bin/cp "$@"
          """)

        :build ->
          script(Path.join(root, "mockbin/mix"), "#!/bin/sh\necho BUILD_FAILED >&2\nexit 1\n")
      end

      assert {output, rc} = run(root, "c1-core-escript-check", env)
      assert rc != 0

      if unquote(injection) == :restore do
        assert output =~ "kept at"
        assert [scratch] = scratches(root)
        assert File.read!(Path.join(scratch, "preserved")) == "original"
        assert (File.stat!(Path.join(scratch, "preserved")).mode &&& 0o777) == 0o640
      else
        assert File.read!(path) == "original"
        assert (File.stat!(path).mode &&& 0o777) == 0o640
        assert scratches(root) == []
      end
    end
  end

  @curl ~S"""
  #!/bin/sh
  prev=""; out=""; hdr=""; w=0
  for a in "$@"; do case "$prev" in -o) out=$a;; -D) hdr=$a;; -w) w=1;; esac; prev=$a; done
  [ -f "$STOPPED_MARK" ] && exit 7
  if [ -n "$out" ] && [ "$out" != /dev/null ]; then echo '<form><input name="credential"></form>' > "$out"; fi
  if [ -n "$hdr" ]; then echo 'content-security-policy: default-src none' > "$hdr"; fi
  [ "$w" = 1 ] && printf 200
  exit 0
  """

  defp release(stop, child \\ false) do
    child_line = if child, do: "/bin/sleep 60 & echo $! > \"$KID_MARK\"", else: ""

    """
    #!/bin/sh
    case "$1" in
    start) echo $$ > "$PID_MARK"; dirname "$ORRIS_CONSOLE_CONFIG_FILE" > "$WORK_MARK"
      echo "${RELEASE_NODE:-default}|${RELEASE_COOKIE:-default}" > "$PID_MARK.start-identity"
      #{child_line}
      exec /bin/sleep 30;;
    stop) touch "$STOPPED_MARK"
      echo "${RELEASE_NODE:-default}|${RELEASE_COOKIE:-default}" > "$PID_MARK.stop-identity"
      #{stop};;
    esac
    """
  end

  defp smoke(root, opts) do
    env = fixture(root)
    copy(root, "c1-release-smoke")
    copy(root, "c1-smoke-port")
    script(Path.join(root, "console/bin/c1-setup"), Keyword.get(opts, :setup, "#!/bin/sh\nexit 0\n"))
    rel = Path.join(root, "console/_build/#{@toolchain}/prod/rel/orris_console/bin/orris_console")
    script(rel, Keyword.get(opts, :release, release("exit #{Keyword.get(opts, :stop, 0)}")))
    script(Path.join(root, "mockbin/curl"), Keyword.get(opts, :curl, @curl))
    if date = Keyword.get(opts, :date), do: script(Path.join(root, "mockbin/date"), date)
    File.mkdir_p!(Path.join(root, "test/fixtures/contracts/scenarios/kill9_resume"))
    File.write!(Path.join(root, "test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl"), "fixture")
    File.mkdir_p!(Path.join(root, "temps"))

    env ++
      [
        {"TMPDIR", Path.join(root, "temps")},
        {"SMOKE_BUDGET", to_string(Keyword.get(opts, :budget, 8))},
        {"SMOKE_CLEANUP_RESERVE", "1"},
        {"PID_MARK", Path.join(root, "pid")},
        {"KID_MARK", Path.join(root, "kid")},
        {"WORK_MARK", Path.join(root, "workpath")},
        {"STOPPED_MARK", Path.join(root, "stopped")},
        {"SMOKE_LOG_DIR", Path.join(root, "smoke-logs")}
      ]
  end

  defp alive?(pid) do
    case System.cmd("ps", ["-o", "stat=", "-p", to_string(pid)], stderr_to_stdout: true) do
      {s, 0} -> String.trim(s) != "" && !String.starts_with?(String.trim(s), "Z")
      _ -> false
    end
  end

  defp await_gone(pid) do
    deadline = System.monotonic_time(:millisecond) + 3_000
    await_gone(pid, deadline)
  end

  defp await_gone(pid, deadline) do
    if alive?(pid) do
      assert System.monotonic_time(:millisecond) < deadline, "owned fixture process #{pid} survived cleanup"
      Process.sleep(20)
      await_gone(pid, deadline)
    end
  end

  defp assert_smoke(root, opts, success) do
    env = smoke(root, opts)
    before = System.monotonic_time(:millisecond)
    {output, rc} = run(root, "c1-release-smoke", env)
    assert rc == 0 == success, output
    assert System.monotonic_time(:millisecond) - before < (Keyword.get(opts, :budget, 8) + 15) * 1_000

    if !Keyword.has_key?(opts, :setup),
      do: assert(File.exists?(Path.join(root, "pid")), "release fixture never started: #{output}")

    for mark <- Keyword.get(opts, :required_marks, []) do
      setup_log = File.read!(Path.join(root, "smoke-logs/setup.log"))

      assert File.exists?(Path.join(root, mark)),
             "required fixture #{mark} never started; runner: #{output}; setup: #{setup_log}"
    end

    for mark <- ["pid", "kid"], {:ok, text} <- [File.read(Path.join(root, mark))] do
      refute alive?(String.to_integer(String.trim(text))), "#{mark} survived: #{output}"
      File.rename!(Path.join(root, mark), Path.join(root, mark <> ".joined"))
    end

    assert Path.wildcard(Path.join(root, "temps/orris-console-smoke.*")) == []
    assert File.exists?(Path.join(root, "smoke-logs/stop.log"))
    {env, output}
  end

  test "C-3 failed HTTP and stop are contained", %{root: root} do
    assert_smoke(root, [stop: 1, curl: "#!/bin/sh\nprintf 000\nexit 7\n"], false)
  end

  test "C-4 successful smoke cleans up its release and retains logs", %{root: root} do
    assert_smoke(root, [], true)
  end

  test "R2 hanging stop is actually exercised and bounded", %{root: root} do
    assert_smoke(root, [release: release("echo STOP_HUNG; exec /bin/sleep 60")], false)
    assert File.read!(Path.join(root, "smoke-logs/stop.log")) =~ "STOP_HUNG"
  end

  test "R2 hanging HTTP client is bounded", %{root: root} do
    assert_smoke(root, [curl: "#!/bin/sh\nexec /bin/sleep 60\n"], false)
  end

  test "R2 partial startup fails and cleans up", %{root: root} do
    early = ~s{#!/bin/sh\ncase "$1" in start) echo $$ > "$PID_MARK"; exit 3;; stop) exit 0;; esac\n}
    assert_smoke(root, [release: early, curl: "#!/bin/sh\nexit 7\n"], false)
  end

  test "R2 release descendants are contained when stop fails", %{root: root} do
    assert_smoke(root, [release: release("exit 1", true)], false)
    assert File.exists?(Path.join(root, "kid.joined"))
  end

  test "R2 setup descendants are contained at the bound", %{root: root} do
    setup = "#!/bin/sh\n/bin/sleep 60 &\necho $! > \"$KID_MARK\"\necho SETUP_CHILD_STARTED\nwait\n"

    # Cross one wall-clock second during the preamble deterministically. A two-second
    # total budget minus the one-second cleanup reserve leaves no setup allowance.
    # Use the ordinary eight-second fixture budget; run_owned must still bound the
    # sixty-second child, and the monotonic elapsed check and JOIN witness stay live.
    date = """
    #!/bin/sh
    if [ -f "$root/date.started" ]; then
      echo 101
    else
      touch "$root/date.started"
      echo 100
    fi
    """

    {_env, output} = assert_smoke(root, [setup: setup, date: date, budget: 8, required_marks: ["kid"]], false)
    assert output =~ "bootstrap failed or timed out"
    assert File.read!(Path.join(root, "smoke-logs/setup.log")) =~ "SETUP_CHILD_STARTED"
    assert File.exists?(Path.join(root, "kid.joined"))
  end

  test "R2 start and stop share a unique unlogged identity and preserve a foreign sentinel", %{root: root} do
    # A Port owns and reaps the sentinel; cleanup is registered before assertions.
    sentinel = Port.open({:spawn_executable, ~c"/bin/sleep"}, [:exit_status, {:args, [~c"60"]}])
    {:os_pid, pid} = Port.info(sentinel, :os_pid)
    File.write!(Path.join(root, "sentinel.pid"), to_string(pid))
    {_env, output} = assert_smoke(root, [], true)
    start_id = root |> Path.join("pid.start-identity") |> File.read!() |> String.trim()
    assert start_id == root |> Path.join("pid.stop-identity") |> File.read!() |> String.trim()
    [node, cookie] = String.split(start_id, "|", parts: 2)
    assert String.starts_with?(node, "orris_console_smoke_")
    assert byte_size(cookie) >= 16
    logs = root |> Path.join("smoke-logs/*.log") |> Path.wildcard() |> Enum.map_join(&File.read!/1)
    refute logs <> output =~ cookie
    assert alive?(pid)
    System.cmd("kill", ["-KILL", to_string(pid)], stderr_to_stdout: true)
    assert_receive {^sentinel, {:exit_status, _}}, 3_000
    File.rm!(Path.join(root, "sentinel.pid"))
  end
end
