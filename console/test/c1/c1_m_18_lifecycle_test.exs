defmodule C1.MutationLifecycleRegressionTest do
  @moduledoc """
  M-18 regression rows for review G1-G3 on 16ad3df (each fails on that head): retention is separated from
  occupancy (expiry hides the outcome while the slot is still occupied; the record is collected after the last owned
  DOWN; the retention origin is the first terminal transition for every terminal state; replacement and session end
  never leave an unreachable record), a dead waiter is consumed by its exact DOWN, and the grant revalidates without
  a second idle renewal.
  """
  use ExUnit.Case, async: false
  alias C1.{Clock, Harness, Mutations}
  alias C1.Mutations, as: Mut
  alias OrrisConsole.SessionStore

  @moduletag timeout: 60_000

  defp app!(overrides) do
    ctx = Mut.app!(Harness.merged([mutation_witness: self(), mutation_retention_ms: 50], overrides))
    {:ok, id} = Mut.login(ctx.secret)
    Map.put(ctx, :id, id)
  end

  defp down!(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  defp records, do: map_size(:sys.get_state(SessionStore).ops)
  defp waiters, do: :sys.get_state(SessionStore).waiters

  test "M-18a retention expires while the finished operation is still alive (slot occupied), stays expired, and the record is collected after its DOWN" do
    %{owned: owned, id: id} = app!(operation_finish_gate: self())
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, pid} = Mut.witness!(:operation_finish, op)
    Mutations.Owned.add(owned, pid, :operation)
    assert {:finished, _} = Mut.await(op, 2_000)
    assert {:ok, %{op_ref: ^op}} = Mut.outcome(id)
    Process.sleep(160)
    assert Mut.outcome(id) == :none, "G1: the expired outcome is still readable while the operation is alive"
    assert Mut.status().occupied == 1, "retention released a live reservation"
    assert Process.alive?(pid)
    send(pid, :proceed)
    down!(pid)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert Mut.outcome(id) == :none
    assert Harness.eventually(fn -> records() == 0 end), "the expired record was not collected after its DOWN"
  end

  test "M-18b failed_to_start (no start request), unknown and refused_at_grant outcomes expire from their first terminal transition and their records are collected" do
    %{owned: owned, id: id, config: c, secret: s} = app!(starter_gate: self(), mutation_start_ms: 5_000)
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, starter} = Mut.witness!(:starter_ready, op)
    Process.exit(starter, :kill)
    down!(starter)
    assert {:failed_to_start, :no_start_request} = Mut.await(op, 2_000)
    assert Harness.eventually(fn -> Mut.status().occupied == 0 end)
    assert {:ok, %{state: :failed_to_start}} = Mut.outcome(id)
    Process.sleep(160)
    assert Mut.outcome(id) == :none, "G1: a failed_to_start outcome never expired"
    assert records() == 0
    # unknown: the invoker dies without a result
    :ok = Application.stop(:orris_console)

    Application.put_env(
      :orris_console,
      :config,
      Keyword.merge(c, starter_gate: nil, mutation_invoke: fn _, _, _ -> exit(:boom) end)
    )

    {:ok, _} = Application.ensure_all_started(:orris_console)
    {:ok, id2} = Mut.login(s)
    {op2, _} = Mut.accept_now!(id2, "alpha", "a")
    assert {:unknown, _} = Mut.await(op2, 5_000)
    assert {:ok, %{state: :unknown}} = Mut.outcome(id2)
    Process.sleep(160)
    assert Mut.outcome(id2) == :none, "G1: an unknown outcome never expired"
    assert Harness.eventually(fn -> records() == 0 end)
    # refused_at_grant: revoked between accept and grant; the child exits on its grant timeout; the record is collected
    :ok = Application.stop(:orris_console)

    Application.put_env(
      :orris_console,
      :config,
      Keyword.merge(c, starter_gate: nil, operation_gate: self(), mutation_start_ms: 300)
    )

    {:ok, _} = Application.ensure_all_started(:orris_console)
    {:ok, id3} = Mut.login(s)
    {op3, _} = Mut.accept_now!(id3, "alpha", "a")
    {^op3, pid3} = Mut.witness!(:operation_init, op3)
    Mutations.Owned.add(owned, pid3, :operation)
    :ok = Mut.revoke(id3)
    send(pid3, :proceed)
    assert {:refused_at_grant, :session_revoked} = Mut.await(op3, 3_000)
    down!(pid3)
    assert Harness.eventually(fn -> records() == 0 end, 40), "a refused_at_grant record was never collected"
  end

  test "M-18c replacement by the next acceptance and session end while the previous operation is still alive never leave an unreachable record; a live reservation is never released" do
    %{owned: owned, id: id, root: root, secret: s} =
      app!(operation_finish_gate: self(), mutation_capacity: 2, mutation_retention_ms: 60_000)

    Mut.in_flight!(Path.join(root, "b"))
    {op1, _} = Mut.accept_now!(id, "alpha", "a")
    {^op1, pid1} = Mut.witness!(:operation_finish, op1)
    Mutations.Owned.add(owned, pid1, :operation)
    assert {:finished, _} = Mut.await(op1, 2_000)
    # replacement: op2 accepted while op1 is finished-but-alive; op1 is no longer presented
    {op2, _} = Mut.accept_now!(id, "alpha", "b")
    {^op2, pid2} = Mut.witness!(:operation_finish, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    assert {:finished, _} = Mut.await(op2, 2_000)
    assert {:ok, %{op_ref: ^op2}} = Mut.outcome(id)
    assert Mut.status().occupied == 2 and records() == 2
    send(pid1, :proceed)
    down!(pid1)

    assert Harness.eventually(fn -> records() == 1 and Mut.status().occupied == 1 end),
           "G1: the replaced record was not collected at its DOWN"

    # session end while op2 is still alive: unreadable now, collected at its DOWN
    {:ok, other} = Mut.login(s)
    :ok = Mut.revoke(id)
    assert Mut.outcome(id) == :none
    assert Mut.status().occupied == 1, "session end released a live reservation"
    send(pid2, :proceed)
    down!(pid2)

    assert Harness.eventually(fn -> records() == 0 and Mut.status().occupied == 0 end),
           "G1: the ended session's record was not collected"

    assert {:accepted, _} = Mut.accept(other, Mut.intent!(other, "alpha", "a"), "alpha", "a")
  end

  test "M-18d a dead waiter is consumed by its exact DOWN (timer cancelled) before the operation completes; a stale DOWN never disturbs the replacement waiter, which receives the terminal reply" do
    %{owned: owned, id: id} = app!(operation_gate: self(), mutation_start_ms: 10_000)
    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, child} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, child, :operation)
    dead = spawn(fn -> Mut.await(op, 20_000) end)
    Mutations.Owned.add(owned, dead, :waiter)
    assert Harness.eventually(fn -> map_size(waiters()) == 1 end)
    Process.exit(dead, :kill)
    down!(dead)
    assert Harness.eventually(fn -> map_size(waiters()) == 0 end), "G2: the dead waiter was not consumed by its DOWN"
    # replacement: waiter A then waiter B (A answered :pending); A's death must not remove B
    me = self()
    a = spawn(fn -> send(me, {:a, Mut.await(op, 20_000)}) end)
    Mutations.Owned.add(owned, a, :waiter)
    assert Harness.eventually(fn -> map_size(waiters()) == 1 end)
    b = spawn(fn -> send(me, {:b, Mut.await(op, 20_000)}) end)
    Mutations.Owned.add(owned, b, :waiter)
    assert_receive {:a, :pending}, 2_000
    assert Harness.eventually(fn -> match?(%{^op => %{from: {^b, _}}}, waiters()) end)
    Process.exit(a, :kill)
    down!(a)
    Process.sleep(50)
    assert match?(%{^op => %{from: {^b, _}}}, waiters()), "G2: a stale DOWN removed the replacement waiter"
    send(child, :proceed)
    assert_receive {:b, {:finished, _}}, 10_000
    down!(child)
  end

  test "M-18e a delayed grant revalidates without renewing idle: the deadline set at acceptance is unchanged; an idle expiry before the grant is refused_at_grant" do
    clock = Clock.start!()

    %{owned: owned, id: id, secret: s} =
      app!(
        clock: Clock.fun(clock),
        idle_ms: 1_000,
        absolute_ms: 10_000,
        operation_gate: self(),
        mutation_start_ms: 5_000,
        mutation_retention_ms: 60_000
      )

    {op, _} = Mut.accept_now!(id, "alpha", "a")
    {^op, pid} = Mut.witness!(:operation_init, op)
    Mutations.Owned.add(owned, pid, :operation)
    {:ok, at_accept} = SessionStore.validate(SessionStore, id, :observe)
    Clock.advance(clock, 400)
    send(pid, :proceed)
    Mut.witness!(:granted, op)
    assert {:finished, _} = Mut.await(op, 5_000)
    down!(pid)
    {:ok, after_grant} = SessionStore.validate(SessionStore, id, :observe)

    assert after_grant.idle_deadline_ms == at_accept.idle_deadline_ms,
           "G3: the grant renewed idle by #{after_grant.idle_deadline_ms - at_accept.idle_deadline_ms} ms"

    # expiry before the grant: refused, no invoke
    {:ok, id2} = Mut.login(s)
    {op2, _} = Mut.accept_now!(id2, "alpha", "a")
    {^op2, pid2} = Mut.witness!(:operation_init, op2)
    Mutations.Owned.add(owned, pid2, :operation)
    Clock.advance(clock, 1_000)

    {_, calls} =
      Mut.prepare_calls(fn ->
        send(pid2, :proceed)
        assert {:refused_at_grant, :session_revoked} = Mut.await(op2, 3_000)
      end)

    assert calls == []
    Mut.refute_witness!(:granted, op2)
    down!(pid2)
  end
end
