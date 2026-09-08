defmodule WorkerGreenReviewProbesTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Executor.Owner
  alias AiOrchestrator.Test.OwnedHarness, as: O
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  setup do
    Process.flag(:trap_exit, true)
    O.setup_owned()
    dir = Path.join(System.tmp_dir!(), "worker-review-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    O.track_dir!(dir)
    H.reset_seams()
    {:ok, dir: dir}
  end

  defp config(dir, extra \\ []) do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    opts = Keyword.drop(make.(), [:event_sink, :run_dir, :run_lock_path, :tail_repair])

    %{
      run_dir: dir,
      mode: :run,
      command: nil,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: Keyword.merge(opts, extra),
      trace: O.collector()
    }
  end

  defp only_ok(:ok), do: :ok

  test "handoff failure stays closed and reaps already-known suspended children", %{dir: dir} do
    collector = O.collector()

    barrier = fn
      :handoff_received, facts ->
        send(collector, {:facts, facts})

        receive do
          :crash_now -> :ok
        after
          5_000 -> exit(:review_permit_missing)
        end

        :ok = :sys.suspend(facts.supervisor)
        :ok = :sys.suspend(facts.writer)
        raise "HANDOFF-RAW-REVIEW-SENTINEL"

      :subtree_started, _ ->
        :ok
    end

    ctx = config(dir)

    log =
      capture_log(fn ->
        O.spawn_caller!(fn -> Owner.run(ctx, barrier) end)
        assert_receive {:facts, facts}, 10_000
        monitor = Process.monitor(facts.owner)
        send(facts.owner, :crash_now)
        assert_receive {:DOWN, ^monitor, :process, _, reason}, 25_000
        assert_receive {:result, result}, 25_000
        Process.put(:review_facts, facts)
        Process.put(:review_result, result)
        Process.put(:review_down, reason)
      end)

    facts = Process.get(:review_facts)

    refute Process.alive?(facts.writer)
    refute Process.alive?(facts.supervisor)
    refute log =~ "HANDOFF-RAW-REVIEW-SENTINEL"
  end

  test "nested FunctionClauseError is not mistaken for an absent optional callback", %{dir: dir} do
    barrier = fn
      :handoff_received, facts -> only_ok(Map.get(facts, :review_value, :bad))
      :subtree_started, _ -> :ok
    end

    ctx = config(dir)
    O.spawn_caller!(fn -> Owner.run(ctx, barrier) end)
    assert_receive {:result, result}, 30_000
    assert {:error, %{clause: "run_executor_down"}} = result
  end

  test "birth barrier cannot fabricate a statem terminal by throwing a callback-shaped value", %{dir: dir} do
    barrier = fn
      :before_birth, _ -> throw({:next_state, :finished, %{result: {:ok, %{summary: %{"status" => "fabricated"}}}}})
      _, _ -> :ok
    end

    assert {:ok, root} = Run.Supervisor.start_link(config(dir, birth_barrier: barrier))
    O.track!(root)
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    result = Run.Server.await(server, 10_000)
    refute match?({:ok, %{summary: %{"status" => "fabricated"}}}, result)
    assert {:error, %{clause: "run_server_down"}} = result
  end

  test "worker death already observed before dequeue defeats a queued effect reply", %{dir: dir} do
    collector = O.collector()
    ctx = config(dir, gate_opts: [runner: OwnerDoubles.held_gate(collector)])
    assert {:ok, root} = Run.Supervisor.start_link(ctx)
    O.track!(root)
    assert_receive {:run_child_started, ^root, :writer, writer}, 10_000
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, _, :worker, worker}, 10_000
    assert_receive {:gate_entered, ^worker}, 10_000
    # U1b-0b-L shape migration (docs/contracts/owner-loss-generation.org): independent registration capture
    assert {:ok, %{writer: ^writer, generation: generation, state: :live}} = Ownership.status(dir)
    :ok = :sys.suspend(server)
    send(worker, :release_gate)
    assert wait(fn -> Enum.any?(mailbox(server), &match?({:effect_result, _, _, _, ^worker, _}, &1)) end)
    mon = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^mon, :process, ^worker, :killed}, 5_000
    assert wait(fn -> Enum.any?(mailbox(server), &match?({:DOWN, _, :process, ^worker, _}, &1)) end)
    O.flush!()
    request = Enum.find(Enum.reverse(mailbox(self())), &match?({:run_effect_requested, ^server, %{op: :execute}}, &1))
    assert {:run_effect_requested, ^server, %{cap: cap, gen: gen, ref: ref}} = request
    :ok = :sys.resume(server)

    assert Run.Server.await(server, 10_000) ==
             {:error, %{clause: "run_effect_owner_down", writer_generation: generation}}

    O.flush!()
    refute_received {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^ref}}
  end

  defp mailbox(pid) do
    {:messages, messages} = Process.info(pid, :messages)
    messages
  end

  defp wait(fun), do: wait(fun, System.monotonic_time(:millisecond) + 5_000)

  defp wait(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(5)
        wait(fun, deadline)
    end
  end
end
