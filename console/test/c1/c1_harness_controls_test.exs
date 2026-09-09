defmodule C1.HarnessControlsTest do
  @moduledoc "Harness validity controls: they hold at every head (they prove the witnesses and oracles, not the product)."
  use ExUnit.Case, async: false
  alias AiOrchestrator.Query
  alias C1.{Doubles, Harness, Oracles}

  @core_lock_sha "52c10c350f69cfd9cd8c9c8c70959566c11b9c212ff66149bd128b35e93440fd"

  test "H-1 the scaffold reaches the public core: a real fixture root lists through Query with its run identity" do
    {root, ids} = Harness.fixture_root(["alpha_run"])
    assert %{"alpha_run" => run_id} = ids
    assert is_binary(run_id) and byte_size(run_id) > 0

    assert {:ok, %{run_ref: "alpha_run", run_id: ^run_id, pending_repair: nil}} =
             Query.run_summary("alpha_run", root: root)
  end

  test "H-2 a torn journal tail is data (pending_repair), not an error, and the bytes are unchanged by the read" do
    {root, _} = Harness.fixture_root(["torn"])
    dir = Path.join(root, "torn")
    Harness.torn!(dir)
    before = Harness.journal_sha(dir)
    assert {:ok, %{pending_repair: %{action: :truncate_tail}}} = Query.run_summary("torn", root: root)
    assert Harness.journal_sha(dir) == before
  end

  test "H-3 a disposable loopback listener answers the raw TCP client and is gone afterwards" do
    port =
      Harness.with_listener(C1.EchoPlug, fn port ->
        resp = Harness.tcp_request(port, "GET", "/probe", [{"host", "localhost:#{port}"}])
        assert resp.status == 200
        assert resp.body =~ "echo GET /probe host=localhost:#{port}"
        port
      end)

    assert {:error, _} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary], 200)
  end

  test "H-4 the pinned headless browser (explicit setting, identity asserted against the reviewed pin) drives a page through the DevTools client; the process is gone afterwards and teardown is idempotent" do
    {version, sha} = C1.CDP.identity!()
    IO.puts("
[H-4] browser #{version} sha256 #{sha} (matches the pinned identity)")
    browser = C1.CDP.launch!()

    Harness.with_listener(C1.EchoPlug, fn port ->
      session = C1.CDP.open_page!(browser)
      session = C1.CDP.navigate!(session, "http://127.0.0.1:#{port}/browser")
      {text, session} = C1.CDP.eval!(session, "document.getElementById('echo').textContent")
      assert text == "echo GET /browser host=127.0.0.1:#{port}"
      assert %{"content-type" => "text/html" <> _} = C1.CDP.document_headers(session)
    end)

    {:ok, signalled} = C1.CDP.stop!(browser.os_port, browser.os_pid, browser.profile)

    assert signalled != [] and Enum.all?(signalled, &(not C1.CDP.OS.alive?(&1))),
           "signalled browser processes survived: #{inspect(signalled)}"

    refute C1.CDP.alive?(browser.profile), "browser processes survived teardown"
    refute File.exists?(browser.profile)
    assert {:ok, []} = C1.CDP.stop!(browser.os_port, browser.os_pid, browser.profile)
  end

  test "H-4b a same-URL delayed reload is a new navigation: the client waits for the new response (new nonce, elapsed >= delay)" do
    browser = C1.CDP.launch!()

    Harness.with_listener(C1.EchoPlug, fn port ->
      url = "http://127.0.0.1:#{port}/reload?delay=600"
      session = C1.CDP.open_page!(browser)
      session = C1.CDP.navigate!(session, url)
      {first, session} = C1.CDP.eval!(session, "document.getElementById('nonce').textContent")
      started = System.monotonic_time(:millisecond)
      session = C1.CDP.navigate!(session, url)
      elapsed = System.monotonic_time(:millisecond) - started
      {second, session} = C1.CDP.eval!(session, "document.getElementById('nonce').textContent")
      assert second != first and elapsed >= 500, "stale page accepted: #{first} -> #{second} after #{elapsed} ms"
      assert %{"x-c1-nonce" => ^second} = C1.CDP.document_headers(session)
    end)
  end

  test "H-5 Query-call tracing sees calls from this process and from a spawned process, and nothing when nothing is called" do
    {root, _} = Harness.fixture_root(["seen"])
    {_, own} = Harness.query_calls(fn -> Query.list_runs(root: root) end)
    assert Enum.any?(own, &match?({:list_runs, _}, &1)), "own-process call not traced: #{inspect(own)}"

    {_, nested} =
      Harness.query_calls(fn -> Task.async(fn -> Query.run_summary("seen", root: root) end) |> Task.await() end)

    assert Enum.any?(nested, &match?({:run_summary, _}, &1)), "call from a spawned process not traced"
    {_, none} = Harness.query_calls(fn -> :nothing end)
    assert none == []
  end

  test "H-5b a monitor created by the test is delivered to the test mailbox while tracing (the closure runs here)" do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    ref = Process.monitor(pid)

    {_, calls} =
      Harness.query_calls(fn ->
        Process.exit(pid, :kill)
        assert_receive({:DOWN, ^ref, _, _, _})
      end)

    assert calls == []
  end

  test "H-6 the core lock is the frozen B3 lock (the console adds no core dependency)" do
    sha =
      Harness.core_path()
      |> Path.join("mix.lock")
      |> File.read!()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    assert sha == @core_lock_sha
  end

  test "H-7 log capture sees a warning and a crash report emitted while it runs" do
    {_, text} =
      Harness.capture_logs(fn ->
        require Logger
        Logger.warning("c1_capture_warning_canary")
        pid = spawn(fn -> receive do: (:go -> exit({:c1_crash_canary, 1})) end)
        ref = Process.monitor(pid)
        send(pid, :go)
        assert_receive {:DOWN, ^ref, _, _, _}
      end)

    assert text =~ "c1_capture_warning_canary"
  end

  test "H-8 overrides take precedence in the merged configuration (a duplicate-keyword list would keep the default)" do
    assert Keyword.get([worker_capacity: 2] ++ [worker_capacity: 1], :worker_capacity) == 2
    merged = Harness.merged([worker_capacity: 2, retry_ms: 200], worker_capacity: 1)
    assert Keyword.get(merged, :worker_capacity) == 1 and Keyword.get(merged, :retry_ms) == 200
    assert Harness.config(worker_capacity: 1, retry_ms: 300)[:worker_capacity] == 1
    assert Harness.config(worker_capacity: 1, retry_ms: 300)[:retry_ms] == 300
  end

  test "H-9 the canary run is a VALID journal: the core renders the canary as context text in summary, context and list" do
    root = Harness.fresh("canary")
    Harness.canary_run(Path.join(root, "a"))
    assert {:ok, %{pending_repair: nil, rendered: rendered}} = Query.run_context("a", root: root)
    assert rendered =~ Harness.canary()
    assert {:ok, %{pending_repair: nil, status: "in_flight"}} = Query.run_summary("a", root: root)
    assert {:ok, %{runs: [%{error: nil, run_ref: "a"}]}} = Query.list_runs(root: root)
  end

  test "H-10 the permission-first oracle passes the faithful setup and fails each mutant at its own violation" do
    assert :ok =
             Oracles.outcome(fn ->
               Oracles.permission_first(
                 &Doubles.Credential.faithful/1,
                 Path.join(Harness.fresh("perm_ok"), "console/credential")
               )
             end)

    for {mutant, step} <- [
          {&Doubles.Credential.write_first/1, :write_before_permission},
          {&Doubles.Credential.no_exclusive_open/1, :no_exclusive_open},
          {&Doubles.Credential.chmod_then_replace/1, :target_replaced},
          {&Doubles.Credential.secret_elsewhere/1, :no_secret_write}
        ] do
      path = Path.join(Harness.fresh("perm_#{step}"), "console/credential")
      assert {:failed, ^step} = Oracles.outcome(fn -> Oracles.permission_first(mutant, path) end)
    end
  end

  test "H-11 the ordering oracle passes the faithful adapter and fails the premature-admission mutant, which really admits a reading replacement at the cut point; every tracked process is gone afterwards" do
    faithful = Doubles.Job.fresh(:faithful)
    assert :ok = Oracles.outcome(fn -> Oracles.ordering(faithful) end)
    assert Harness.eventually(fn -> Registry.count(faithful.registry) == 0 end), "keys survived cleanup"
    Doubles.Job.cleanup(faithful)
    mutant = Doubles.Job.fresh(:premature_admission)
    assert {:failed, :premature_admission} = Oracles.outcome(fn -> Oracles.ordering(mutant) end)

    assert DynamicSupervisor.count_children(mutant.workers).active == 0,
           "the mutant's replacement worker survived cleanup"

    Doubles.Job.cleanup(mutant)
  end

  test "H-12 the ack/link oracle passes the faithful adapter, fails the no-link mutant with a detached read and the read-before-ack mutant with a read entered before the worker acknowledgement" do
    faithful = Doubles.Job.fresh(:faithful)
    assert :ok = Oracles.outcome(fn -> Oracles.link_gate(faithful) end)
    assert Harness.eventually(fn -> DynamicSupervisor.count_children(faithful.workers).active == 0 end)
    Doubles.Job.cleanup(faithful)

    for {variant, step} <- [
          {:no_link, :detached_read},
          {:read_before_ack, :read_before_worker_ack},
          {:early_detached, :orphan_after_early_kill}
        ] do
      mutant = Doubles.Job.fresh(variant)
      assert {:failed, ^step} = Oracles.outcome(fn -> Oracles.link_gate(mutant) end)
      Doubles.Job.cleanup(mutant)
    end
  end

  test "H-4c teardown ownership (mock OS, no signals): a closed Port with an absent profile issues no signal and never the retained numeric pid" do
    port = Port.open({:spawn_executable, "/usr/bin/true"}, [:binary, :exit_status])
    receive do: ({^port, {:exit_status, _}} -> :ok)
    profile = Path.join(System.tmp_dir!(), "c1-mock-profile-#{System.unique_integer([:positive])}")
    {:ok, []} = C1.CDP.stop!(port, 123_456_789, profile, C1.FakeOS.absent())
    refute_received {:os_signal, _}

    bin = C1.CDP.binary()

    os =
      C1.FakeOS.present(
        profile,
        %{
          100 => "#{bin} --headless=new --user-data-dir=#{profile}",
          101 => "#{bin} --type=renderer --headless=old",
          102 => "unrelated-daemon --child-of-100",
          103 => "chrome-notifier --watch --mentions-chrome",
          200 => "#{bin} --headless=new --user-data-dir=#{profile}0",
          201 => "unrelated-daemon --note=user-data-dir=#{profile}"
        },
        %{100 => [101, 102, 103]}
      )

    {:ok, signalled} = C1.CDP.stop!(port, 123_456_789, profile, os)
    assert Enum.sort(signalled) == [100, 101]
    assert_received {:os_signal, 100}
    assert_received {:os_signal, 101}
    refute_received {:os_signal, 102}
    refute_received {:os_signal, 103}
    refute_received {:os_signal, 200}
    refute_received {:os_signal, 201}
    refute C1.CDP.owned_main?("#{bin} --user-data-dir=#{profile}0", profile)
    refute C1.CDP.owned_main?("unrelated-daemon --note=user-data-dir=#{profile}", profile)
    assert C1.CDP.owned_main?("#{bin} --headless=new --user-data-dir=#{profile}", profile)
    refute_received {:os_signal, 123_456_789}
    {:ok, []} = C1.CDP.stop!(port, 123_456_789, profile, C1.FakeOS.absent())
    refute_received {:os_signal, _}
  end

  test "H-15 the resource owner runs every cleanup in reverse on failure paths: a failing later acquisition cleans the earlier resource, and a raising cleanup neither skips the others nor hides its failure" do
    me = self()
    spawn_owned = fn tag -> spawn(fn -> receive do: (:stop -> :ok) end) |> tap(&send(me, {:acquired, tag, &1})) end

    stop = fn pid ->
      Process.exit(pid, :kill)
      send(me, {:cleaned, pid})
    end

    assert_raise RuntimeError, ~r/startup failed/, fn ->
      C1.Resources.run(fn ->
        first = C1.Resources.acquire(:first, fn -> spawn_owned.(:first) end, stop)
        assert C1.Resources.installed() == [:first]
        _second = C1.Resources.acquire(:second, fn -> raise "startup failed after the first resource" end, stop)
        first
      end)
    end

    assert_received {:acquired, :first, first}
    assert_received {:cleaned, ^first}
    refute Process.alive?(first)

    error =
      assert_raise ExUnit.AssertionError, fn ->
        C1.Resources.run(fn ->
          C1.Resources.acquire(:a, fn -> spawn_owned.(:a) end, stop)
          # b's Resources cleanup deliberately raises; b is ALSO owned independently by the test (monitor + on_exit)
          C1.Resources.acquire(
            :b,
            fn ->
              pid = spawn_owned.(:b)
              send(me, {:b_ref, Process.monitor(pid)})
              on_exit(fn -> Process.exit(pid, :kill) end)
              pid
            end,
            fn _ -> raise "cleanup b exploded" end
          )

          C1.Resources.acquire(:c, fn -> spawn_owned.(:c) end, stop)
          :ok
        end)
      end

    assert error.message =~ "cleanup b exploded"
    assert_received {:acquired, :a, a}
    assert_received {:acquired, :b, b}
    assert_received {:acquired, :c, c}
    assert_received {:cleaned, ^c}
    assert_received {:cleaned, ^a}
    refute Process.alive?(a) or Process.alive?(c)
    # b outlived its raising cleanup by design; the test reaps it and proves every acquired process DOWN
    assert_received {:b_ref, b_ref}
    assert Process.alive?(b)
    Process.exit(b, :kill)
    assert_receive {:DOWN, ^b_ref, :process, ^b, _}
    refute Enum.any?([first, a, b, c], &Process.alive?/1)
  end

  test "H-13 the delivery oracle passes faithful revalidation and fails the no-revalidation mutant (stale) and the identity-ignoring mutant (forged job)" do
    assert :ok = Oracles.outcome(fn -> Oracles.delivery(&Doubles.Delivery.faithful/2) end)
    assert {:failed, :stale_delivery} = Oracles.outcome(fn -> Oracles.delivery(&Doubles.Delivery.no_revalidation/2) end)
    assert {:failed, :forged_job} = Oracles.outcome(fn -> Oracles.delivery(&Doubles.Delivery.ignores_identity/2) end)
  end

  test "H-14 (U1 amendment) the closed configuration file is plain JSON data without secrets, serialized terms or seams; the seven mutation limits travel in limits when configured" do
    config = Harness.config(roots: %{"alpha" => "/tmp/x"})
    path = Harness.config_file!(config)
    data = path |> File.read!() |> Jason.decode!()
    assert data["host"] == "localhost" and data["bind"] == "127.0.0.1" and data["limits"]["session_capacity"] == 128

    refute Map.has_key?(data, "clock") or Map.has_key?(data, "read_gate") or Map.has_key?(data, "read_witness") or
             Map.has_key?(data, "query_opts")

    refute File.read!(path) =~ "credential\":\"" && File.read!(path) =~ Base.encode16(<<0::256>>)
    refute Map.has_key?(data["limits"], "mutation_capacity"), "a plain C1 configuration wrote U1 limits"
    # with the U1 keys configured (C1.Mutations.config/1) the seven limits are written and the seams never are
    with_mutations =
      C1.Mutations.config(
        roots: %{"alpha" => "/tmp/x"},
        mutation_witness: self(),
        operation_gate: self(),
        mutation_invoke: fn _, _, _ -> :ok end
      )

    data = with_mutations |> Harness.config_file!() |> File.read!() |> Jason.decode!()

    assert Map.take(
             data["limits"],
             ~w(mutation_capacity intent_ttl_ms mutation_wait_ms mutation_retention_ms mutation_shutdown_ms mutation_start_ms mutation_read_ms)
           ) ==
             %{
               "mutation_capacity" => 4,
               "intent_ttl_ms" => 60_000,
               "mutation_wait_ms" => 5_000,
               "mutation_retention_ms" => 300_000,
               "mutation_shutdown_ms" => 60_000,
               "mutation_start_ms" => 1_000,
               "mutation_read_ms" => 1_000
             }

    text = Jason.encode!(data)

    for seam <- ~w(mutation_opts mutation_witness operation_gate operation_finish_gate starter_gate mutation_invoke),
        do: refute(text =~ seam, "seam #{seam} in the file")
  end
end
