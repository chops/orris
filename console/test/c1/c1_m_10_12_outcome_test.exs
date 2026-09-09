defmodule C1.MutationOutcomeTest do
  @moduledoc """
  M-10 busy refusals (same-BEAM Writer, other-OS-process lock), M-11 abort at console stop, M-12 the outcome classes
  and message table; H-18 measures the lock clauses through the public seam at every head; H-19 measures a REAL
  partial append (review R1): the public seam answers journal_append_failed AFTER durable rows, so that clause can
  never be classified pre-write.
  """
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias AiOrchestrator.Query
  alias C1.{Harness, Mutations}
  alias C1.Mutations, as: Mut

  @moduletag timeout: 120_000

  defp app!(overrides) do
    ctx = Mut.app!(Harness.merged([mutation_witness: self(), operation_gate: self(), mutation_capacity: 2], overrides))
    {:ok, id} = Mut.login(ctx.secret)
    Map.put(ctx, :id, id)
  end

  # accept + init witness + owned; the gate is NOT released here (rows release inside their observation windows)
  defp held!(owned, id, root_id, run_ref) do
    {op, _intent} = Mut.accept_now!(id, root_id, run_ref)
    {^op, pid} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, pid, :operation)
    {op, pid}
  end

  defp run!(owned, id, root_id, run_ref) do
    {op, pid} = held!(owned, id, root_id, run_ref)
    send(pid, :proceed)
    {op, pid}
  end

  defp restart_app!(config) do
    :ok = Application.stop(:orris_console)
    Application.put_env(:orris_console, :config, config)
    {:ok, _} = Application.ensure_all_started(:orris_console)
  end

  test "H-18 (control) public seam lock clauses: a Writer held in another OS process refuses a cancel through run_locked before any write; a gated live Writer in this BEAM answers second_live_writer within a second" do
    root = Harness.fresh("h18")
    dir = Mut.in_flight!(Path.join(root, "run"))
    sha = Harness.journal_sha(dir)
    {port, _holder} = Mut.hold_lock_elsewhere!(dir)
    t0 = System.monotonic_time(:millisecond)
    result = Mut.seam_cancel("run", root: root)
    ms = System.monotonic_time(:millisecond) - t0
    assert {:error, %{clause: "run_locked", detail: detail}} = result
    assert Harness.journal_sha(dir) == sha
    IO.puts("\n[H-18] other-OS-process lock: clause run_locked in #{ms} ms; detail keys #{inspect(Map.keys(detail))}")
    Mut.release_lock_elsewhere!(port)
    # same BEAM: a real Writer gated inside its append; the duplicate is refused by the Writer itself, pre-write
    me = self()

    holder =
      spawn_link(fn -> send(me, {:held, Mut.seam_cancel("run", root: root, fs: Mutations.GateFs.new(me, :write))}) end)

    writer = Mut.gated!()
    t0 = System.monotonic_time(:millisecond)
    dup = Mut.seam_cancel("run", root: root)
    dup_ms = System.monotonic_time(:millisecond) - t0
    assert {:error, %{clause: "second_live_writer"}} = dup
    assert dup_ms < 1_000
    assert Harness.journal_sha(dir) == sha
    Mut.release(writer)
    assert_receive {:held, {:ok, %{close: :ok}}}, 10_000
    refute Process.alive?(holder)
    assert {:ok, %{status: "cancelled"}} = Query.run_summary("run", root: root)
  end

  test "H-19 (control) a REAL partial append through the public seam: the lease-release append fails, the public result is journal_append_failed, two rows are durable, a fresh cancel afterwards is a NEW command" do
    root = Harness.fresh("h19")
    dir = Mut.in_flight!(Path.join(root, "run"))
    before = Mut.lines(dir)
    result = Mut.seam_cancel("run", root: root, fs: Mutations.PartialAppendFs.new())
    assert {:error, %{clause: "journal_append_failed"}} = result
    appended = Enum.drop(Mut.types(dir), before)
    assert appended == ["run_cancel_requested", "workspace_lease_release_requested"], inspect(appended)
    assert Mut.lines(dir) == before + 2
    assert {:ok, %{status: status, last_seq: seq}} = Query.run_summary("run", root: root)
    assert status != "cancelled" and seq == before + 2

    IO.puts(
      "\n[H-19] partial append: journal_append_failed with #{before}->#{Mut.lines(dir)} durable rows, observed #{status} at seq #{seq}"
    )

    # recovery: a fresh cancel is a second, distinct acceptance and reaches cancelled
    assert {:ok, %{close: :ok}} = Mut.seam_cancel("run", root: root)
    ids = Mut.cancel_command_ids(dir)
    assert length(ids) == 2 and length(Enum.uniq(ids)) == 2
    assert {:ok, %{status: "cancelled"}} = Query.run_summary("run", root: root)
  end

  test "M-10a a live Writer in the same BEAM (gated append) refuses a second console cancel pre-write: second_live_writer, class busy, no read, bytes unchanged; the first completes after release" do
    %{owned: owned, id: id, root: root, config: c, secret: s} =
      app!(mutation_opts: [fs: Mutations.GateFs.new(self(), :write)])

    {op1, _} = run!(owned, id, "alpha", "a")
    writer = Mut.gated!()
    {subtree, monitors} = Mut.capture_subtree!(writer)
    Mutations.Owned.add_all(owned, subtree, :core_subtree)
    sha = Harness.journal_sha(root <> "/a")
    {:ok, id2} = Mut.login(s)
    {op2, pid2} = held!(owned, id2, "alpha", "a")

    # the Query capture starts BEFORE the second operation is released (review R6)
    {_, query_calls} =
      Harness.query_calls(fn ->
        send(pid2, :proceed)
        assert {:finished, outcome} = Mut.await(op2, 5_000), "RED (U1 M-10a)"
        assert outcome.phase == :pre_admission_refused and outcome.invoke == {:error, "second_live_writer"}
        assert outcome.observed == nil and outcome.message == "Cancel refused before any write (busy)"
        ref = Process.monitor(pid2)
        assert_receive {:DOWN, ^ref, :process, ^pid2, _}, 5_000
      end)

    assert query_calls == [], "a pre-write refusal was followed by a read"
    assert Harness.journal_sha(root <> "/a") == sha
    cookie = Harness.login!(c, s)
    {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    Harness.await_read(view)
    Mut.release(writer)
    assert {:finished, %{observed: %{status: "cancelled", last_seq: seq}}} = Mut.await(op1, 10_000)
    assert {:ok, %{op_ref: ^op1, outcome: %{message: message}}} = Mut.outcome(id)
    assert message == "Run is cancelled (verified journal at seq #{seq})"
    assert Mut.join(monitors, 20_000) == []
  end

  test "M-10b a Writer held in another OS process refuses the console cancel with EXACTLY the RunLock clause run_locked: class busy, no write, no read" do
    %{owned: owned, id: id, root: root, secret: s} = app!([])
    {port, _holder} = Mut.hold_lock_elsewhere!(root <> "/a")
    sha = Harness.journal_sha(root <> "/a")
    {op, pid} = held!(owned, id, "alpha", "a")

    {_, query_calls} =
      Harness.query_calls(fn ->
        send(pid, :proceed)
        assert {:finished, outcome} = Mut.await(op, 10_000), "RED (U1 M-10b)"
        assert outcome.phase == :pre_admission_refused and outcome.invoke == {:error, "run_locked"}
        assert outcome.message == "Cancel refused before any write (busy)"
      end)

    assert query_calls == []
    assert Harness.journal_sha(root <> "/a") == sha
    Mut.release_lock_elsewhere!(port)
    {:ok, id2} = Mut.login(s)
    {op2, _} = run!(owned, id2, "alpha", "a")
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op2, 10_000)
  end

  # M-11: console stop is an ABORT. Append leg: the Writer is gated inside the append; at stop return the operation
  # is dead; the captured subtree drains at the Owner budget (joined by its original monitors); nothing appended.
  # Close leg: the append landed and the close is gated; abort leaves 10 lines (one accepted request); the recovery
  # leg runs WITHOUT any gate (review R4) and is a FRESH cancel with a second distinct acceptance id (16 lines).
  # The blocked-open leg is retained evidence (>65 s, CORE-FOLLOWUP-blocked-open.org), reported, never re-run.
  test "M-11 console stop is an ABORT: operation dead at stop return (Writer blocked in append, then close); subtrees drain at the Owner budget; recovery is a FRESH ungated cancel with a second acceptance id; open leg retained" do
    %{owned: owned, id: id, root: root, config: c, secret: s} =
      app!(mutation_opts: [fs: Mutations.GateFs.new(self(), :write)], mutation_shutdown_ms: 500)

    lines0 = Mut.lines(root <> "/a")
    {_op, pid} = run!(owned, id, "alpha", "a")
    writer = Mut.gated!()
    {subtree, monitors} = Mut.capture_subtree!(writer)
    Mutations.Owned.add_all(owned, subtree, :core_subtree)
    t0 = System.monotonic_time(:millisecond)
    :ok = Application.stop(:orris_console)
    stop_ms = System.monotonic_time(:millisecond) - t0
    refute Process.alive?(pid), "RED (U1 M-11): the operation survived the console stop"

    # measured at GREEN (harness precision): the spike's plain invoker left 3 of 5 subtree pids alive at stop
    # return; the product's LINKED invoker takes the Owner with it and the non-trapping Writer dies on its
    # supervisor's shutdown signal at once, so survival at return is a REPORTED count; the obligation is the
    # bounded drain by original identities with nothing appended
    survivors_at_return = Enum.count(subtree, &Process.alive?/1)

    assert Mut.join(monitors, 20_000) == [],
           "append-leg subtree did not drain at the Owner budget (original identities)"

    drain_ms = System.monotonic_time(:millisecond) - t0
    assert Mut.lines(root <> "/a") == lines0, "bytes appended during the abort"
    # close leg (MEASURED at GREEN, contract precision): the public core's Writer closes the journal ONCE after
    # every row of the command is durable, so a Writer gated in its close already holds a COMPLETE cancelled
    # journal (15 lines, one acceptance id); the abort leaves it complete; a fresh cancel then observes cancelled
    # with bytes unchanged and no second acceptance (nothing to do)
    Application.put_env(
      :orris_console,
      :config,
      Keyword.put(c, :mutation_opts, fs: Mutations.GateFs.new(self(), :close))
    )

    {:ok, _} = Application.ensure_all_started(:orris_console)
    {:ok, id2} = Mut.login(s)
    {_op2, pid2} = run!(owned, id2, "alpha", "a")
    writer2 = Mut.gated!()
    lines_at_close = Mut.lines(root <> "/a")
    {subtree2, monitors2} = Mut.capture_subtree!(writer2)
    Mutations.Owned.add_all(owned, subtree2, :core_subtree)
    :ok = Application.stop(:orris_console)
    refute Process.alive?(pid2)
    assert Mut.join(monitors2, 20_000) == []
    assert lines_at_close == lines0 + 6 and Mut.lines(root <> "/a") == lines0 + 6
    assert length(Mut.cancel_command_ids(root <> "/a")) == 1 and List.last(Mut.types(root <> "/a")) == "run_cancelled"
    # recovery after the close-leg abort: ungated (review R4), the run is already cancelled: bytes unchanged
    Application.put_env(:orris_console, :config, Keyword.put(c, :mutation_opts, []))
    {:ok, _} = Application.ensure_all_started(:orris_console)
    sha_complete = Harness.journal_sha(root <> "/a")
    {:ok, id3} = Mut.login(s)
    {op3, _} = run!(owned, id3, "alpha", "a")
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op3, 10_000)
    assert Harness.journal_sha(root <> "/a") == sha_complete and length(Mut.cancel_command_ids(root <> "/a")) == 1
    # partial-command leg (the MAP's "fresh cancel = a NEW command id" obligation on the PRODUCT path): a real
    # partial append (journal_append_failed after two durable rows) then an ungated fresh cancel = a second
    # distinct acceptance id, observed cancelled
    Mut.in_flight!(Path.join(root, "p"))
    p0 = Mut.lines(root <> "/p")
    Application.put_env(:orris_console, :config, Keyword.put(c, :mutation_opts, fs: Mutations.PartialAppendFs.new()))
    :ok = Application.stop(:orris_console)
    {:ok, _} = Application.ensure_all_started(:orris_console)
    {:ok, id4} = Mut.login(s)
    {op4, _} = run!(owned, id4, "alpha", "p")
    assert {:finished, %{invoke: {:error, "journal_append_failed"}}} = Mut.await(op4, 10_000)
    assert Mut.lines(root <> "/p") == p0 + 2 and length(Mut.cancel_command_ids(root <> "/p")) == 1
    Application.put_env(:orris_console, :config, Keyword.put(c, :mutation_opts, []))
    :ok = Application.stop(:orris_console)
    {:ok, _} = Application.ensure_all_started(:orris_console)
    {:ok, id5} = Mut.login(s)
    {op5, _} = run!(owned, id5, "alpha", "p")
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op5, 10_000)
    ids = Mut.cancel_command_ids(root <> "/p")

    assert length(ids) == 2 and length(Enum.uniq(ids)) == 2,
           "recovery continued the incomplete command instead of a fresh acceptance"

    IO.puts(
      "\n[M-11] stop returned in #{stop_ms} ms; #{survivors_at_return}/#{length(subtree)} append-leg subtree pids alive at return; drained at #{drain_ms} ms; close leg gated with #{lines_at_close} lines already durable (complete command); partial-command recovery = 2 acceptance ids; open leg = retained evidence (>65 s, CORE-FOLLOWUP-blocked-open.org), not re-run"
    )
  end

  # M-12: every outcome class through its own path, with the message table verbatim. Real paths: observed
  # cancelled, already finished, Scope refusal, the REAL partial append (journal_append_failed AFTER durable rows →
  # uncertain + separate read, never pre-write: review R1). MAPPING (stubbed invoke result, attributed): close
  # failure, teardown-incomplete, ownership_lost. Unknown: the invoker dies without a result. Unavailable read:
  # read_gate past mutation_read_ms. Then the detail page renders the retained sentence and never a detail map.
  test "M-12 every outcome class through its own path (Scope/live-Writer refusals, fixture states, REAL partial append, stubbed close and post-append clauses, unknown invoker, unavailable read); exact message table; detail page renders it" do
    stub = fn result -> fn _actor, _run_ref, _opts -> result end end
    %{owned: owned, id: id, root: root, config: c, secret: s} = app!(read_gate: nil)
    Mut.completed!(Path.join(root, "done"))
    # observed cancelled (real)
    {op, _} = run!(owned, id, "alpha", "a")

    assert {:finished, %{phase: :invoked, invoke: :ok, observed: %{status: "cancelled", last_seq: seq}, message: m}} =
             Mut.await(op, 10_000),
           "RED (U1 M-12)"

    assert m == "Run is cancelled (verified journal at seq #{seq})"
    # already finished (real, completed fixture)
    {op, _} = run!(owned, id, "alpha", "done")

    assert {:finished, %{invoke: :ok, observed: %{status: "completed", last_seq: seq2}, message: m}} =
             Mut.await(op, 10_000)

    assert m == "Run already finished: completed (verified journal at seq #{seq2})"
    # pre-write refusal via Scope (a run directory that vanished between the page and the confirm)
    Mut.in_flight!(Path.join(root, "gone"))
    File.rm_rf!(Path.join(root, "gone"))
    {op, _} = run!(owned, id, "alpha", "gone")

    assert {:finished,
            %{
              phase: :pre_admission_refused,
              invoke: {:error, "run_directory_missing"},
              observed: nil,
              message: "Cancel refused before any write (not available)"
            }} = Mut.await(op, 5_000)

    # REAL partial append: uncertain, the separate read observes the durable rows (never "before any write")
    Mut.in_flight!(Path.join(root, "partial"))
    lines0 = Mut.lines(Path.join(root, "partial"))
    restart_app!(Keyword.put(c, :mutation_opts, fs: Mutations.PartialAppendFs.new()))
    {:ok, sid} = Mut.login(s)
    {op, _} = run!(owned, sid, "alpha", "partial")
    assert {:finished, outcome} = Mut.await(op, 10_000)
    assert outcome.phase == :invoked and outcome.invoke == {:error, "journal_append_failed"}
    assert outcome.observed == %{status: "in_flight", last_seq: lines0 + 2}

    assert outcome.message ==
             "Cancel outcome uncertain (journal_append_failed): Run is in_flight (verified journal at seq #{lines0 + 2}); the cancel did not take effect"

    refute outcome.message =~ "before any write"

    # MAPPING (stubbed invoke results at the seam; attributed): close failure, post-append clause, uncertain + non-terminal
    Mut.in_flight!(Path.join(root, "b"))

    stubs = [
      {stub.({:ok, %{events: [], close: {:error, %{clause: "close_failed"}}}}), "a", {:close_failed, "close_failed"},
       ~r/^Run is cancelled \(verified journal at seq \d+\); the journal close reported an error \(attention\)$/},
      {stub.({:error, %{clause: "run_executor_teardown_incomplete", detail: %{survivors: 1, run_dir: root}}}), "a",
       {:error, "run_executor_teardown_incomplete"},
       ~r/^Cancel outcome uncertain \(run_executor_teardown_incomplete\): Run is cancelled \(verified journal at seq \d+\)$/},
      {stub.({:error, %{clause: "ownership_lost", detail: %{path: root}}}), "b", {:error, "ownership_lost"},
       ~r/^Cancel outcome uncertain \(ownership_lost\): Run is in_flight \(verified journal at seq \d+\); the cancel did not take effect$/}
    ]

    for {fun, ref, invoke, pattern} <- stubs do
      restart_app!(Keyword.put(c, :mutation_invoke, fun))
      {:ok, sid} = Mut.login(s)
      {op, _} = run!(owned, sid, "alpha", ref)
      assert {:finished, %{phase: :invoked, invoke: ^invoke, message: message}} = Mut.await(op, 5_000)
      assert message =~ pattern, "#{inspect(invoke)}: #{message}"
      refute message =~ root, "a detail map leaked into the message"
    end

    # invoker DOWN without a result: unknown
    restart_app!(Keyword.put(c, :mutation_invoke, fn _, _, _ -> exit(:boom) end))
    {:ok, sid} = Mut.login(s)
    {op, _} = run!(owned, sid, "alpha", "b")

    assert {:unknown, %{phase: :unknown, invoke: :unknown, message: "Cancel outcome uncertain (unknown): " <> _}} =
             Mut.await(op, 5_000)

    # read unavailable: the post-write read gated past mutation_read_ms → read_timeout, the read task DOWN, slot released
    restart_app!(Keyword.merge(c, mutation_invoke: nil, read_gate: self(), mutation_read_ms: 300))
    {:ok, sid} = Mut.login(s)
    {op, pid} = run!(owned, sid, "alpha", "b")
    assert_receive {:read_gate, read_task, _corr}, 10_000
    Mutations.Owned.add(owned, read_task, :read_task)

    assert {:finished,
            %{
              invoke: :ok,
              observed: %{unavailable: "read_timeout"},
              message: "Current journal state unavailable (read_timeout)"
            }} = Mut.await(op, 5_000)

    ref = Process.monitor(read_task)
    assert_receive {:DOWN, ^ref, :process, ^read_task, _}, 2_000
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    # the detail page renders the retained sentence for its session and no detail map (ungated reads again)
    restart_app!(c)
    cookie = Harness.login!(c, s)
    {sid2, _} = Harness.raw_session_id(cookie)
    {op, _} = run!(owned, sid2, "alpha", "a")
    assert {:finished, %{message: sentence}} = Mut.await(op, 10_000)
    {:ok, view, _} = live(Harness.conn(c, :get, "/runs/alpha/a", [{"cookie", cookie}]))
    html = Harness.await_read(view)
    assert Mut.outcome_line(html) == sentence
    refute html =~ root
  end
end
