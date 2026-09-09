defmodule C1.MutationPreservationTest do
  @moduledoc """
  M-13 redaction on the mutation path, M-15 gate shape, M-16 configuration, M-17 cleanup oracle. Controls that hold
  at every head: H-16 the owned-pid tracker's REAL ExUnit lifecycle (teardown after the test process exited, passing
  and failing bodies, a held operation and a deliberate leak: review R2/R6) and H-20 the Prepare-trace window
  covering an asynchronous late call (review R6).
  """
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias AiOrchestrator.Prepare
  alias C1.{Harness, Mutations}
  alias C1.Mutations, as: Mut

  @verify_stages ~w(00-toolchain 00-browser 01-deps-locked 02-hex-audit 03-format 04-compile-wae 05-test-compile-wae 06-packaging 07-assets 07-assets-manifest 08-release 09-release-smoke 10-rows-wae 11-core-unchanged 12-core-escript-18)
  @limits ~w(mutation_capacity intent_ttl_ms mutation_wait_ms mutation_retention_ms mutation_shutdown_ms mutation_start_ms mutation_read_ms)a

  # H-16: the REAL lifecycle. A nested ExUnit module (run with ExUnit.run/1) has three tests that call owned!/1 exactly
  # as the rows do: (a) a passing body that leaves a held operation and a suspended process alive, (b) a failing body
  # that leaves a deliberate leak, (c) a clean body. Every teardown runs in on_exit AFTER its test process exited and
  # reports its result here; the leaks are recorded before the emergency kill, and the kill leaves no survivor.
  test "H-16 (control) owned! teardown runs after the test process exited, on passing and failing bodies: a held operation is reported as a leak and killed; a suspended process is resumed first; a clean body reports nothing" do
    :persistent_term.put({__MODULE__, :h16_parent}, self())

    defmodule H16Nested do
      use ExUnit.Case, async: false

      defp parent, do: :persistent_term.get({C1.MutationPreservationTest, :h16_parent})

      test "passing body with a held operation and a suspended process" do
        owned = C1.Mutations.owned!(expect_leaks: true, witness: parent())
        held = spawn(fn -> receive do: (:never -> :ok) end)
        C1.Mutations.Owned.add(owned, held, :held_operation)
        {:ok, agent} = Agent.start(fn -> 0 end)
        C1.Mutations.Owned.add(owned, agent, :suspended_agent)
        :ok = :sys.suspend(agent)
        C1.Mutations.Owned.suspended(owned, agent)
        send(parent(), {:h16_pids, held, agent})
        assert Process.alive?(held)
      end

      test "failing body with a deliberate leak" do
        owned = C1.Mutations.owned!(expect_leaks: true, witness: parent())
        leak = spawn(fn -> receive do: (:never -> :ok) end)
        C1.Mutations.Owned.add(owned, leak, :deliberate_leak)
        send(parent(), {:h16_leak, leak})
        flunk("deliberate failure: the teardown must still run")
      end

      test "clean body" do
        _owned = C1.Mutations.owned!(witness: parent())
        done = spawn(fn -> :ok end)
        ref = Process.monitor(done)
        assert_receive {:DOWN, ^ref, :process, ^done, _}
        assert true
      end
    end

    # the nested run must not inherit the outer invocation's include/exclude/location filters (review S1): save
    # them, run the nested module unfiltered, restore them on every path (whole-file and line-selected runs agree)
    saved = ExUnit.configuration() |> Keyword.take([:include, :exclude, :only_test_ids])

    stats =
      try do
        ExUnit.configure(include: [], exclude: [], only_test_ids: nil)
        ExUnit.run([H16Nested])
      after
        ExUnit.configure(saved)
      end

    assert stats.total == 3 and stats.failures == 1 and stats.excluded == 0, inspect(stats)
    assert_receive {:h16_pids, held, agent}
    assert_receive {:h16_leak, leak}
    results = for _ <- 1..3, do: receive(do: ({:owned_teardown, r} -> r), after: (2_000 -> :none))
    assert Enum.all?(results, &(&1 != :none)), "a teardown did not report: #{inspect(results)}"
    leak_sets = Enum.map(results, &Enum.sort(&1.leaks))
    assert Enum.sort([{:held_operation, held}, {:suspended_agent, agent}]) in leak_sets, inspect(leak_sets)
    assert [{:deliberate_leak, leak}] in leak_sets
    assert [] in leak_sets
    assert Enum.all?(results, &(&1.survivors == []))
    refute Process.alive?(held) or Process.alive?(agent) or Process.alive?(leak)
    :persistent_term.erase({__MODULE__, :h16_parent})
  end

  # H-20: the Prepare trace window covers an asynchronous call that lands after a send and before the closure ends,
  # and does not attribute a call made after the window closed (the oracle the lifecycle rows rely on).
  test "H-20 (control) the Prepare trace window catches a late asynchronous forbidden call inside the closure and none after it" do
    me = self()

    {_, inside} =
      Mut.prepare_calls(fn ->
        late = spawn(fn -> receive do: (:go -> send(me, {:late, Prepare.verb("cancel")})) end)
        send(late, :go)
        assert_receive {:late, {:ok, "cancel"}}
      end)

    assert Enum.any?(inside, &match?({:verb, ["cancel"]}, &1)),
           "the late asynchronous call escaped the window: #{inspect(inside)}"

    after_window = spawn(fn -> receive do: (:go -> send(me, {:after, Prepare.verb("start")})) end)
    {_, outside} = Mut.prepare_calls(fn -> :nothing end)
    send(after_window, :go)
    assert_receive {:after, {:ok, "start"}}
    assert outside == []
  end

  # M-13: the confirm runs in an OWNED task while the operation's init is held; the observed init is released
  # BEFORE the visible start budget (1000 ms); the terminal outcome is awaited; the view refresh is awaited. The
  # crashed-operation window uses the same ordering and proves the crash (monitored DOWN with the exact reason).
  test "M-13 the run directory path, rejection detail, intent token, op_ref and session material are absent from HTML and captured logs on the mutation path (C1-13 canaries + run_dir); the sentence stays visible" do
    %{config: c, secret: s, root: root, owned: owned} =
      Mut.app!(mutation_witness: self(), operation_gate: self(), operation_finish_gate: self())

    Harness.canary_run(Path.join(root, "a"))
    hex = Base.encode16(s, case: :lower)
    run_dir = Path.join(root, "a")

    {{cookie, raw_id, cookie_value, intent, op_ref, html, refused, crash_reason}, logs} =
      Harness.capture_logs(fn ->
        cookie = Harness.login!(c, s)
        {raw_id, cookie_value} = Harness.raw_session_id(cookie)
        page = Mut.intent(c, cookie, "alpha", "a")
        assert page.status == 200, "RED (U1 M-13): intent answered #{page.status}"
        intent = Mut.intent_token(page.resp_body)
        task = Mut.confirm_async(c, cookie, "alpha", "a", page.resp_body)
        Mutations.Owned.add(owned, task.pid, :confirm_task)
        {op_ref, pid} = Mut.witness!(:operation_init, nil, 1_000)
        Mutations.Owned.add(owned, pid, :operation)
        send(pid, :proceed)
        confirm = Task.await(task, 10_000)
        assert confirm.status == 302
        assert {:finished, _} = Mut.await(op_ref, 5_000)
        # the finished operation waits RESPONSIVELY at the finish gate: release it and join it
        {^op_ref, ^pid} = Mut.witness!(:operation_finish, op_ref)
        first_mref = Process.monitor(pid)
        send(pid, :proceed)
        assert_receive {:DOWN, ^first_mref, :process, ^pid, _}, 5_000
        {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
        html = Harness.await_read(view)
        # a refused cancel (vanished directory) whose operation is crashed at a DETERMINISTIC RESPONSIVE point
        # (review S4): its init is released, it finishes (pre-write refusal) and waits at the finish gate where
        # it processes messages; the canary-bearing exit signal then stops it with EXACTLY that reason (its crash
        # report carries the run directory and must be scrubbed)
        Mut.in_flight!(Path.join(root, "gone"))
        File.rm_rf!(Path.join(root, "gone"))
        gone_page = Mut.intent(c, cookie, "alpha", "gone")
        assert gone_page.status == 200
        gone_task = Mut.confirm_async(c, cookie, "alpha", "gone", gone_page.resp_body)
        Mutations.Owned.add(owned, gone_task.pid, :confirm_task)
        {gone_ref, gone_pid} = Mut.witness!(:operation_init, nil, 1_000)
        Mutations.Owned.add(owned, gone_pid, :operation)
        send(gone_pid, :proceed)
        refused = Task.await(gone_task, 10_000)
        assert {:finished, %{phase: :pre_admission_refused}} = Mut.await(gone_ref, 5_000)
        {^gone_ref, ^gone_pid} = Mut.witness!(:operation_finish, gone_ref)
        mref = Process.monitor(gone_pid)
        crash_reason = {:crash_canary_exit, run_dir}
        Process.exit(gone_pid, crash_reason)
        assert_receive {:DOWN, ^mref, :process, ^gone_pid, ^crash_reason}, 2_000
        Process.sleep(200)
        {cookie, raw_id, cookie_value, intent, op_ref, html, refused, crash_reason}
      end)

    assert refused.status == 302
    assert elem(crash_reason, 0) == :crash_canary_exit
    canaries = [hex, root, run_dir, raw_id, cookie_value, intent, inspect(op_ref), "CTX_CANARY_7f3a"]
    for canary <- canaries -- ["CTX_CANARY_7f3a"], do: refute(html =~ canary, "page leak of #{canary}")
    assert html =~ "CTX_CANARY_7f3a" and Mut.outcome_line(html) =~ "verified journal"
    for canary <- canaries, do: refute(logs =~ canary, "log leak of #{canary}")
    assert logs =~ "crash_canary_exit" or logs =~ "MutationOperation"
    after_page = Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}])
    for canary <- [root, run_dir, intent, inspect(op_ref)], do: refute(after_page.resp_body =~ canary)
  end

  test "M-15 the console gate keeps its shape: bin/verify stage list unchanged, stage 11 compares the core paths, the release smoke is byte-identical to public a59d945, committed core paths equal a59d945" do
    verify = File.read!(Path.expand("../../bin/verify", __DIR__))
    # every numbered stage or record call, in order (record calls sit inside the packaging if/else on one line)
    stages = Regex.scan(~r/\b(?:stage|record) (\d\d-[a-z0-9-]+)/, verify) |> Enum.map(&List.last/1) |> Enum.uniq()
    assert stages == @verify_stages, "verify stages: #{inspect(stages)}"
    assert verify =~ ~s(git diff --quiet HEAD -- mix.exs mix.lock flake.nix flake.lock lib test bin)

    smoke_sha =
      Path.expand("../../bin/c1-release-smoke", __DIR__)
      |> File.read!()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    {base_smoke, 0} =
      System.cmd("git", ["show", "a59d945be772061d1968fd6ea02449abe3ce9f81:console/bin/c1-release-smoke"],
        cd: Harness.core_path()
      )

    assert smoke_sha == base_smoke |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
    core_paths = ~w(mix.exs mix.lock flake.nix flake.lock lib test bin)
    diff_args = ["diff", "--stat", "a59d945be772061d1968fd6ea02449abe3ce9f81", "HEAD", "--"] ++ core_paths
    {diff, 0} = System.cmd("git", diff_args, cd: Harness.core_path())
    assert diff == "", "committed core paths differ from public a59d945:\n#{diff}"
  end

  test "M-16 configuration: operator id outside the actor grammar refuses startup; the seven limits default, bound and refuse out-of-range values (config_mutation_limits_invalid); the file accepts exactly them; seams never load" do
    Harness.red!([OrrisConsole.Config])
    load = fn overrides -> OrrisConsole.Config.load(Mut.config(overrides)) end
    assert {:ok, config} = load.(roots: %{"alpha" => "/tmp"})

    assert Map.take(Map.from_struct(config), @limits) == %{
             mutation_capacity: 4,
             intent_ttl_ms: 60_000,
             mutation_wait_ms: 5_000,
             mutation_retention_ms: 300_000,
             mutation_shutdown_ms: 60_000,
             mutation_start_ms: 1_000,
             mutation_read_ms: 1_000
           },
           "RED (U1 M-16): limits #{inspect(Map.take(Map.from_struct(config), @limits))}"

    for bad <- ["op erator", "", "x" <> String.duplicate("y", 64), "op/1", "op:1"] do
      assert {:error, %{clause: "config_operator_invalid"}} =
               load.(operator: %{id: bad, root_ids: ["alpha"]}, roots: %{"alpha" => "/tmp"}),
             inspect(bad)
    end

    assert {:ok, %{operator: %{id: "op.console_1-x"}}} =
             load.(operator: %{id: "op.console_1-x", root_ids: ["alpha"]}, roots: %{"alpha" => "/tmp"})

    for {key, value} <- [
          mutation_capacity: 0,
          mutation_capacity: 65,
          intent_ttl_ms: 600_001,
          mutation_wait_ms: 60_001,
          mutation_retention_ms: 3_600_001,
          mutation_shutdown_ms: 300_001,
          mutation_start_ms: 0,
          mutation_read_ms: "1"
        ] do
      assert {:error, %{clause: "config_mutation_limits_invalid"}} = load.([{key, value}]),
             "#{key}=#{inspect(value)} accepted"
    end

    # the closed file: the seven names are accepted inside limits; unknown keys and seams are refused
    base = Mut.config(roots: %{"alpha" => "/tmp/x"})
    path = Harness.config_file!(base)
    data = path |> File.read!() |> Jason.decode!()

    assert Enum.all?(@limits, &Map.has_key?(data["limits"], Atom.to_string(&1))),
           "H-14 schema without the U1 limits: #{inspect(Map.keys(data["limits"]))}"

    assert {:ok, loaded} = OrrisConsole.Config.load_file(path)
    assert Map.take(Map.from_struct(loaded), @limits) == Map.take(Map.from_struct(config), @limits)
    File.write!(path, Jason.encode!(put_in(data, ["limits", "mutation_capacity"], 3)))
    assert {:ok, %{mutation_capacity: 3}} = OrrisConsole.Config.load_file(path)
    File.write!(path, Jason.encode!(Map.put(data, "mutation_invoke", "System.cmd")))
    assert {:error, %{clause: "config_file_invalid"}} = OrrisConsole.Config.load_file(path)
    File.write!(path, Jason.encode!(put_in(data, ["limits", "mutation_witness"], 1)))
    assert {:error, %{clause: "config_file_invalid"}} = OrrisConsole.Config.load_file(path)

    for seam <- [:mutation_opts, :mutation_witness, :operation_gate, :mutation_invoke],
        do: assert(Map.from_struct(loaded)[seam] in [nil, []], "#{seam} loaded from the file")

    # fail closed at startup
    {root, _} = Harness.fixture_root(["a"])
    bad = Mut.config(roots: %{"alpha" => root}, operator: %{id: "op erator", root_ids: ["alpha"]})
    Harness.credential!(bad)
    Application.put_env(:orris_console, :config, bad)

    assert {:error, {:orris_console, {{:config, %{clause: "config_operator_invalid"}}, _}}} =
             Application.ensure_all_started(:orris_console)
  end

  # M-17: every owned pid of one full operation is captured while alive (the core subtree by its ORIGINAL identities
  # and monitors: review R6), the held operation is reported by the differential while gated, and every identity is
  # joined by its own monitor before the row ends; the row's teardown then reports nothing.
  test "M-17 the cleanup oracle on the product: every owned pid of one full operation (operation, starter, invoker, read task, core subtree) tracked with monitored DOWN and gone; the differential reports a held operation" do
    %{secret: s, owned: owned} =
      Mut.app!(
        mutation_witness: self(),
        operation_gate: self(),
        read_gate: self(),
        mutation_opts: [fs: Mutations.GateFs.new(self(), :write)]
      )

    {:ok, id} = Mut.login(s)
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    starter = Mut.starter!(op)
    Mutations.Owned.add(owned, starter, :starter)
    {^op, pid} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, pid, :operation)
    send(pid, :proceed)
    Mut.witness!(:granted, op)
    {_, invoker} = Mut.witness!(:invoker, op)
    Mutations.Owned.add(owned, invoker, :invoker)
    writer = Mut.gated!()
    {subtree, monitors} = Mut.capture_subtree!(writer)
    Mutations.Owned.add_all(owned, subtree, :core_subtree)
    own_monitors = Map.new([pid, invoker, starter], &{&1, Process.monitor(&1)})
    # differential: the held operation IS reported alive while gated
    assert Enum.any?(Mutations.Owned.alive(owned), fn {p, _} -> p == pid end),
           "RED (U1 M-17): the held operation is not reported"

    Mut.release(writer)
    assert_receive {:read_gate, read_task, _}, 10_000
    Mutations.Owned.add(owned, read_task, :read_task)
    read_monitor = %{read_task => Process.monitor(read_task)}
    send(read_task, :go)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op, 10_000)

    assert Mut.join(Map.merge(own_monitors, read_monitor), 5_000) == [],
           "an owned console process outlived the operation"

    assert Mut.join(monitors, 20_000) == [], "the core subtree outlived the operation (original identities)"
    # before any emergency cleanup: nothing owned is alive
    assert Mutations.Owned.alive(owned) == []
    assert Mut.status() |> Map.take([:fenced, :occupied]) == %{fenced: false, occupied: 0}
  end
end
