defmodule C1.MutationLifetimeTest do
  @moduledoc """
  M-08 occupancy and M-09 lifetime on the CURRENT rest_for_one topology (MAP §3, spike2 R1/R2/R3-a, spike3 T4,
  spike5 A1-A3/B1/B2). Every process the row creates or captures is owned (M-17); the actual message ordering at the
  starter/barrier seam is exercised with the authority or the supervisor suspended, as the reviewer's probes did.
  Seams (contract): operation_gate holds a child in init; starter_gate holds the starter before it submits;
  operation_finish_gate holds a finished operation alive; mutation_witness receives the pinned witnesses.
  Review R3: sessions and intents that must exist while the Store is suspended are created BEFORE the suspension;
  a refused accept is RETRIED ON THE SAME INTENT (the contract retains it); the queued start request is OBSERVED in
  the suspended supervisor's mailbox before the starter is killed; every :sys.suspend is failure-safe (tracked).
  """
  use ExUnit.Case, async: false
  alias C1.{Harness, Mutations}
  alias C1.Mutations, as: Mut

  @moduletag timeout: 120_000

  defp app!(overrides) do
    ctx = Mut.app!(Harness.merged([mutation_witness: self(), mutation_start_ms: 300, mutation_capacity: 1], overrides))
    {:ok, id} = Mut.login(ctx.secret)
    Map.put(ctx, :id, id)
  end

  defp held_operation!(owned, id, root_id, run_ref) do
    {op, _intent} = Mut.accept_now!(id, root_id, run_ref)
    {^op, pid} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, pid, :operation)
    {op, pid}
  end

  defp down!(pid, ms \\ 5_000) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, ms, "#{inspect(pid)} did not exit"
  end

  defp suspend!(owned, pid) do
    :ok = :sys.suspend(pid)
    Mutations.Owned.suspended(owned, pid)
  end

  defp resume!(owned, pid) do
    :ok = :sys.resume(pid)
    Mutations.Owned.resumed(owned, pid)
  end

  # M-08a/d a finished-but-alive operation occupies its slot until its DOWN (cap 1): a second accept is busy without
  # invoke and its intent is RETAINED; the slot frees only on the observed DOWN; a stale DOWN changes nothing; the
  # retried accept on the SAME intent is admitted and that second operation is owned and released too
  test "M-08a/d finished-but-alive occupancy (cap 1): busy without invoke, retained intent, freed only on observed DOWN, stale DOWN ignored" do
    %{owned: owned, id: id, root: root, secret: s} = app!(operation_finish_gate: self())
    Mut.in_flight!(Path.join(root, "b"))
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    Mut.witness!(:granted, op)
    {^op, pid} = Mut.witness!(:operation_finish, op)
    Mutations.Owned.add(owned, pid, :operation)
    assert {:finished, _} = Mut.await(op, 5_000), "RED (U1 M-08a)"
    assert Process.alive?(pid), "the operation stopped before the finish gate released it"
    assert Mut.status().occupied == 1
    {:ok, id2} = Mut.login(s)
    before = Harness.journal_sha(root <> "/b")
    {refused, calls} = Mut.prepare_calls(fn -> Mut.accept_now(id2, "alpha", "b") end)
    assert {:error, :busy, intent2} = refused
    assert calls == [] and Harness.journal_sha(root <> "/b") == before
    # the refused intent is retained: a new intent for the session is :in_progress, the SAME intent can be retried
    assert {:error, :in_progress} = Mut.issue_intent(id2, "alpha", "b")
    assert {:error, :busy} = Mut.accept(id2, intent2, "alpha", "b")
    # a stale DOWN (a monitor the store never created) changes nothing
    send(Process.whereis(Mut.store()), {:DOWN, make_ref(), :process, pid, :stale})
    Process.sleep(50)
    assert Mut.status().occupied == 1
    send(pid, :proceed)
    down!(pid)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end), "the slot did not free on the observed DOWN"
    assert {:accepted, op2} = Mut.accept(id2, intent2, "alpha", "b")
    Mut.witness!(:granted, op2)
    {^op2, pid2} = Mut.witness!(:operation_finish, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    assert {:finished, _} = Mut.await(op2, 5_000)
    send(pid2, :proceed)
    down!(pid2)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
  end

  test "M-08b/c an expired start with the child held in init answers failed_to_start but occupies until the child is gone; no grant, no invoke; the starter counts until the attempt report" do
    %{owned: owned, id: id, root: root, secret: s} = app!(operation_gate: self())
    before = Harness.journal_sha(root <> "/a")
    {op, pid} = held_operation!(owned, id, "alpha", "a")
    {_, calls} = Mut.prepare_calls(fn -> assert {:failed_to_start, _} = Mut.await(op, 2_000), "RED (U1 M-08b)" end)
    assert calls == [], "a start beyond its budget invoked"
    assert Process.alive?(pid) and Mut.status().occupied == 1
    Mut.refute_witness!(:granted, op)
    {:ok, id2} = Mut.login(s)
    assert {:error, :busy, intent2} = Mut.accept_now(id2, "alpha", "a")
    send(pid, :proceed)
    down!(pid)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert Harness.journal_sha(root <> "/a") == before
    assert {:accepted, op2} = Mut.accept(id2, intent2, "alpha", "a")
    {^op2, pid2} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    send(pid2, :proceed)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op2, 5_000)
  end

  # M-09a A1 reviewer adoption ordering: the second session and ITS INTENT exist before the authority is suspended;
  # starter killed while the registered child is held in init; the second ACCEPT (not an intent) is queued behind the
  # DOWN in the suspended authority's mailbox (observed there) → busy, occupancy 1, the child adopted, granted, cancelled
  test "M-09a A1 adoption ordering: authority suspended, starter killed with the child held in init, a queued second accept is busy; the child is adopted, granted and cancelled" do
    %{owned: owned, id: id, root: root, secret: s} = app!(operation_gate: self(), mutation_start_ms: 5_000)
    Mut.in_flight!(Path.join(root, "b"))
    store = Process.whereis(Mut.store())
    {:ok, id2} = Mut.login(s)
    intent2 = Mut.intent!(id2, "alpha", "b")
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    starter = Mut.starter!(op)
    {^op, child} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, child, :operation)
    suspend!(owned, store)
    Process.exit(starter, :kill)
    down!(starter)
    me = self()
    racer = spawn_link(fn -> send(me, {:second, Mut.accept(id2, intent2, "alpha", "b")}) end)
    Mutations.Owned.add(owned, racer, :racer)
    # the accept call sits in the suspended authority's mailbox AFTER the starter's DOWN
    queued? = fn ->
      {:messages, m} = Process.info(store, :messages)
      Enum.any?(m, &match?({:"$gen_call", _, {:accept, _, _, _, _}}, &1))
    end

    assert Harness.eventually(queued?), "RED (U1 M-09a): no queued accept observed in the suspended authority's mailbox"
    {:messages, mailbox} = Process.info(store, :messages)
    down_index = Enum.find_index(mailbox, &match?({:DOWN, _, :process, ^starter, _}, &1))
    accept_index = Enum.find_index(mailbox, &match?({:"$gen_call", _, {:accept, _, _, _, _}}, &1))
    assert is_integer(down_index) and is_integer(accept_index) and down_index < accept_index, inspect(mailbox)
    resume!(owned, store)
    assert_receive {:second, second}, 3_000
    assert second == {:error, :busy}, "the queued accept answered #{inspect(second)}"
    assert Mut.status().occupied == 1
    send(child, :proceed)
    Mut.witness!(:granted, op)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op, 5_000)
    down!(child)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert {:accepted, op2} = Mut.accept(id2, intent2, "alpha", "b")
    {^op2, pid2} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    send(pid2, :proceed)
    assert {:finished, _} = Mut.await(op2, 5_000)
  end

  # M-09b A2 reviewer queued start: the supervisor is suspended; the starter's start_child request is OBSERVED in its
  # mailbox before the starter is killed; the visible timeout answers failed_to_start; the reservation is KEPT; on
  # resume the supervisor's own report counts the late child (no grant, no invoke); freed on its DOWN; bytes unchanged;
  # the busy session retries the SAME intent afterwards
  test "M-09b A2 queued start: supervisor suspended, queued request observed, starter killed, visible timeout answered, reservation KEPT; late child counted, no grant/invoke, freed on DOWN" do
    %{owned: owned, id: id, root: root, secret: s} = app!(operation_gate: self())
    before = Harness.journal_sha(root <> "/a")
    sup = Mut.workers()
    {:ok, id2} = Mut.login(s)
    suspend!(owned, sup)
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    starter = Mut.starter!(op)

    assert Harness.eventually(fn -> Mut.queued_start_child?(sup) end),
           "RED (U1 M-09b): no start_child request queued at the suspended supervisor"

    Process.exit(starter, :kill)
    down!(starter)
    assert Mut.queued_start_child?(sup), "the queued request vanished with the starter"
    assert {:failed_to_start, _} = Mut.await(op, 2_000), "the visible timeout did not answer"
    assert Mut.status().occupied == 1, "the reservation was released while the start request was still queued"
    assert {:error, :busy, intent2} = Mut.accept_now(id2, "alpha", "a")

    {_, calls} =
      Mut.prepare_calls(fn ->
        resume!(owned, sup)
        {^op, child} = Mut.witness!(:operation_init, op)
        Mutations.Owned.add(owned, child, :operation)
        send(child, :proceed)
        Mut.witness!(:counted_late, op)
        assert Mut.status().occupied == 1
        assert {:error, :busy} = Mut.accept(id2, intent2, "alpha", "a")
        Mut.refute_witness!(:granted, op, 500)
        # no grant: the child exits on its own grant timeout
        down!(child)
      end)

    assert calls == [] and Harness.journal_sha(root <> "/a") == before
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert {:accepted, op2} = Mut.accept(id2, intent2, "alpha", "a")
    {^op2, pid2} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    send(pid2, :proceed)
    assert {:finished, _} = Mut.await(op2, 5_000)
  end

  test "M-09c A3 the starter killed before submitting: the correlated barrier finds no attempt report, the reservation is released well before the visible timeout, nothing invoked, no child" do
    %{owned: owned, id: id, root: root} = app!(starter_gate: self(), mutation_start_ms: 5_000)
    before = Harness.journal_sha(root <> "/a")
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, starter} = Mut.witness!(:starter_ready, op)
    Process.exit(starter, :kill)
    down!(starter)
    t0 = System.monotonic_time(:millisecond)
    {_, {_attempt, helper}} = Mut.witness!(:barrier_helper, op)
    Mutations.Owned.add(owned, helper, :barrier_helper)

    {_, calls} =
      Mut.prepare_calls(fn -> assert {:failed_to_start, :no_start_request} = Mut.await(op, 4_000), "RED (U1 M-09c)" end)

    assert System.monotonic_time(:millisecond) - t0 < 2_000, "released only at the visible timeout"
    assert calls == [] and Harness.journal_sha(root <> "/a") == before
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end), "residual reservation after the barrier"
    assert Mut.operation_pid(op) == [] and DynamicSupervisor.count_children(Mut.workers()).active == 0
  end

  test "M-09d/e B1 barrier helper killed: reservation retained, authority-owned retry, at most one live helper, released once answered with nothing invoked; B2 a forged stale barrier result is ignored" do
    %{owned: owned, id: id} = app!(starter_gate: self(), mutation_start_ms: 5_000)
    sup = Mut.workers()
    store = Process.whereis(Mut.store())
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, starter} = Mut.witness!(:starter_ready, op)
    suspend!(owned, sup)
    Process.exit(starter, :kill)
    down!(starter)
    {_, {attempt1, helper1}} = Mut.witness!(:barrier_helper, op)
    Mutations.Owned.add(owned, helper1, :barrier_helper)
    Process.exit(helper1, :kill)
    down!(helper1)
    assert Mut.status().occupied == 1, "RED (U1 M-09d): the reservation was released on the helper's DOWN"
    {_, {attempt2, helper2}} = Mut.witness!(:barrier_helper, op)
    Mutations.Owned.add(owned, helper2, :barrier_helper)
    assert attempt2 != attempt1
    assert Enum.count([helper1, helper2], &Process.alive?/1) <= 1
    # B2: a forged result for the STALE attempt is ignored; the reservation stays
    send(store, {:barrier_result, op, attempt1, :answered})
    Process.sleep(50)
    assert Mut.status().occupied == 1, "a stale barrier result released the reservation"

    {_, calls} =
      Mut.prepare_calls(fn ->
        resume!(owned, sup)
        assert {:failed_to_start, :no_start_request} = Mut.await(op, 4_000)
      end)

    assert calls == []
    Mut.refute_witness!(:granted, op)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert DynamicSupervisor.count_children(sup).active == 0
  end

  test "M-09f a waiter (the HTTP caller) that dies after acceptance changes nothing: the operation completes and its outcome is retained for the session" do
    %{owned: owned, id: id} = app!(operation_gate: self())
    {op, pid} = held_operation!(owned, id, "alpha", "a")
    me = self()

    waiter =
      spawn(fn ->
        send(me, {:waiting, self()})
        Mut.await(op, 10_000)
      end)

    Mutations.Owned.add(owned, waiter, :waiter)
    assert_receive {:waiting, ^waiter}
    Process.sleep(50)
    Process.exit(waiter, :kill)
    down!(waiter)
    send(pid, :proceed)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op, 5_000), "RED (U1 M-09f)"
    assert {:ok, %{op_ref: ^op, state: :finished}} = Mut.outcome(id)
  end

  test "M-09g/j the authority killed before the grant: no invoke (the child exits on its grant timeout); every terminal transition wakes a waiter (a waiter never sleeps to its timeout)" do
    %{owned: owned, id: id, root: root, secret: s} = app!(operation_gate: self())
    before = Harness.journal_sha(root <> "/a")
    {_op, pid} = held_operation!(owned, id, "alpha", "a")
    store = Process.whereis(Mut.store())

    {_, calls} =
      Mut.prepare_calls(fn ->
        Process.exit(store, :kill)
        down!(store)
        send(pid, :proceed)
        down!(pid, 5_000)
      end)

    assert calls == [] and Harness.journal_sha(root <> "/a") == before, "RED (U1 M-09g): invoked without a grant"
    assert Harness.eventually(fn -> is_pid(Process.whereis(Mut.store())) end)
    # waiter wake-up: a failed_to_start transition answers a long await promptly (queued request observed first)
    {:ok, id2} = Mut.login(s)
    sup = Mut.workers()
    suspend!(owned, sup)
    {op2, _} = Mut.accept_now!(id2, "alpha", "a")
    starter = Mut.starter!(op2)
    assert Harness.eventually(fn -> Mut.queued_start_child?(sup) end)
    Process.exit(starter, :kill)
    down!(starter)
    t0 = System.monotonic_time(:millisecond)
    me = self()
    waiter = spawn_link(fn -> send(me, {:woke, Mut.await(op2, 20_000), System.monotonic_time(:millisecond)}) end)
    Mutations.Owned.add(owned, waiter, :waiter)
    Process.sleep(100)
    resume!(owned, sup)
    assert_receive {:woke, {:failed_to_start, _}, t1}, 5_000
    assert t1 - t0 < 3_000, "the waiter slept to its timeout"
    {^op2, late} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, late, :operation)
    send(late, :proceed)
    down!(late)
  end

  # M-09h workers loss (rest_for_one): a granted operation blocked inside a real Writer append stops on its parent's
  # EXIT and its invoker dies by the link (the core drains by caller death); an init-held operation of another session
  # survives as an orphan seen by the successor census, admission is fenced (issue_intent answers :fenced, asserted
  # explicitly), reopened on the orphan's DOWN; the captured core subtree is joined by its ORIGINAL monitors
  test "M-09h workers loss (rest_for_one): granted operation stops on parent EXIT (invoker dies by link); init-held orphan seen by the census, fenced, reopened on DOWN" do
    %{owned: owned, id: id, root: root, secret: s} =
      app!(operation_gate: self(), mutation_capacity: 2, mutation_opts: [fs: Mutations.GateFs.new(self(), :write)])

    Mut.in_flight!(Path.join(root, "b"))
    {:ok, id2} = Mut.login(s)
    {:ok, id3} = Mut.login(s)
    {op1, pid1} = held_operation!(owned, id, "alpha", "a")
    send(pid1, :proceed)
    Mut.witness!(:granted, op1)
    {_, invoker} = Mut.witness!(:invoker, op1)
    Mutations.Owned.add(owned, invoker, :invoker)
    writer = Mut.gated!()
    {subtree, monitors} = Mut.capture_subtree!(writer)
    Mutations.Owned.add_all(owned, subtree, :core_subtree)
    {op2, pid2} = held_operation!(owned, id2, "alpha", "b")
    sup = Mut.workers()
    old_store = Process.whereis(Mut.store())
    Process.exit(sup, :kill)
    down!(pid1)
    down!(invoker, 2_000)
    assert Process.alive?(pid2), "RED (U1 M-09h): the init-held operation did not survive its parent"
    assert Harness.eventually(fn -> Process.whereis(Mut.store()) not in [nil, old_store] end)
    assert Mut.status().fenced == true and Mut.operation_pid(op2) == [pid2]
    # the restarted authority holds no session (C1-09b preserved): every earlier id is invalid; a fresh login
    # against the NEW authority is fenced while the orphan lives (review S2)
    for old <- [id, id2, id3], do: assert({:error, :invalid} = Mut.issue_intent(old, "alpha", "a"))
    {:ok, id4} = Mut.login(s)
    assert {:error, :fenced} = Mut.issue_intent(id4, "alpha", "a")
    Mut.release(writer)
    send(pid2, :proceed)
    down!(pid2, 10_000)
    assert Harness.eventually(fn -> Mut.status().fenced == false end, 200)
    assert Mut.join(monitors, 20_000) == [], "the core subtree did not drain (original identities)"
    # reopened: a fresh operation is admitted, owned, its NEW gated Writer observed and released, awaited and joined
    {op4, pid4} = held_operation!(owned, id4, "alpha", "a")
    send(pid4, :proceed)
    Mut.witness!(:granted, op4)
    writer4 = Mut.gated!()
    {subtree4, monitors4} = Mut.capture_subtree!(writer4)
    Mutations.Owned.add_all(owned, subtree4, :core_subtree)
    Mut.release(writer4)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op4, 10_000)
    down!(pid4)
    assert Mut.join(monitors4, 20_000) == []
  end

  test "M-09i Registry loss: drain then restart, successor census empty and open; a same-run cancel during the core drain is refused pre-write (second_live_writer) and succeeds after it" do
    %{owned: owned, id: id, secret: s} =
      app!(operation_gate: self(), mutation_opts: [fs: Mutations.GateFs.new(self(), :write)], mutation_shutdown_ms: 500)

    {op, pid} = held_operation!(owned, id, "alpha", "a")
    send(pid, :proceed)
    Mut.witness!(:granted, op)
    writer = Mut.gated!()
    {subtree, monitors} = Mut.capture_subtree!(writer)
    Mutations.Owned.add_all(owned, subtree, :core_subtree)
    registry = Process.whereis(Mut.registry())
    old_store = Process.whereis(Mut.store())
    t0 = System.monotonic_time(:millisecond)
    Process.exit(registry, :kill)
    down!(pid, 5_000)

    assert Harness.eventually(
             fn ->
               Process.whereis(Mut.store()) not in [nil, old_store] and
                 Process.whereis(Mut.registry()) not in [nil, registry]
             end,
             200
           ),
           "RED (U1 M-09i): the subtree did not restart"

    restart_ms = System.monotonic_time(:millisecond) - t0
    assert Mut.status() |> Map.take([:fenced, :occupied]) == %{fenced: false, occupied: 0}
    # the core subtree still drains: a same-run cancel is refused pre-write by the Writer
    {:ok, id2} = Mut.login(s)
    {op2, pid2} = held_operation!(owned, id2, "alpha", "a")
    send(pid2, :proceed)
    assert {:finished, %{phase: :pre_admission_refused, invoke: {:error, "second_live_writer"}}} = Mut.await(op2, 5_000)
    Mut.release(writer)
    assert Mut.join(monitors, 20_000) == []
    {:ok, id3} = Mut.login(s)
    {op3, pid3} = held_operation!(owned, id3, "alpha", "a")
    send(pid3, :proceed)
    # the retained :write seam gates op3's NEW Writer too (review S3): observe it, own its subtree, release, join
    Mut.witness!(:granted, op3)
    writer3 = Mut.gated!()
    {subtree3, monitors3} = Mut.capture_subtree!(writer3)
    Mutations.Owned.add_all(owned, subtree3, :core_subtree)
    Mut.release(writer3)
    assert {:finished, %{observed: %{status: "cancelled"}}} = Mut.await(op3, 10_000)
    down!(pid3)
    assert Mut.join(monitors3, 20_000) == []
    IO.puts("\n[M-09i] Mutations subtree restart after Registry loss: #{restart_ms} ms")
  end
end
