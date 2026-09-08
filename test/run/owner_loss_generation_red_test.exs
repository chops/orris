defmodule AiOrchestrator.Run.OwnerLossGenerationRedTest do
  @moduledoc """
  U1b-0b-L RED/interface, revision 1 (docs/contracts/owner-loss-generation.org): the post-admission owner-loss
  result gains the exact Writer generation captured at discovery. Controls measure the unchanged 5eeb50f
  subtree with real pids, monitors and lock files; RED rows fail only on the missing `writer_generation`.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Executor.Owner
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_owner_loss_gen"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @canary "OWNER-LOSS-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

  # a worker whose START fails before any admission: the pre-admission loss path (clause-only today and after)
  defmodule StartFailWorker do
    @moduledoc false
    def child_spec(server), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [server]}, restart: :temporary}
    def start_link(_server), do: {:error, :injected_start_failure}
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "owner-loss-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  defp gated_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp config(dir, extra_opts) do
    opts =
      gated_opts()
      |> Keyword.drop(@owned)
      |> Keyword.put(:supervisor_instance, @instance)
      |> Keyword.merge(extra_opts)

    %{
      run_dir: dir,
      mode: :run,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: opts,
      trace: collector()
    }
  end

  defp held(dir), do: config(dir, gate_opts: [runner: OwnerDoubles.held_gate(collector())])

  # a supported EXISTING-journal acquisition: mode :resume opens (never creates) the journal already written
  defp resumed(dir), do: %{held(dir) | mode: :resume}

  # the canary travels through two REAL exercised inputs: an option key and the run directory name
  defp canary_dir(dir), do: Path.join(dir, "run-" <> @canary)

  defp canary_held(dir) do
    canary = canary_dir(dir)
    File.mkdir_p!(canary)
    config(canary, gate_opts: [runner: OwnerDoubles.held_gate(collector())], review_canary_opt: @canary)
  end

  # a direct RunLock claim + release on the run dir BEFORE any subtree: the next claimant's generation is > 1
  defp advance_lock!(dir) do
    fs = SystemFs.new()

    opts = [
      supervisor_instance: "sup_advance",
      pid: "41001",
      pid_start: "start_41001",
      owner_status: fn _ -> :live end
    ]

    assert {:ok, held} = RunLock.acquire(fs, dir, opts)
    assert :ok = RunLock.release(fs, held)
    held.generation
  end

  defp holder_generation!(dir) do
    assert {:ok, %{generation: generation}} = RunLock.holder(SystemFs.new(), dir)
    generation
  end

  defp start!(config) do
    assert {:ok, root} = Run.Supervisor.start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :writer, writer}, 10_000
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, writer: writer, server: server, work: work}
  end

  # the exact registration of the discovered Writer sibling, captured independently while it is alive
  defp registration!(dir, facts) do
    assert {:ok, %{writer: writer, generation: generation, state: :live}} = Ownership.status(dir)
    assert writer == facts.writer
    generation
  end

  defp held_worker!(facts) do
    assert_receive {:run_child_started, _, :worker, worker}, 10_000
    assert_receive {:gate_entered, ^worker}, 10_000
    assert Server.status(facts.server) == :driving
    worker
  end

  defp alive!(facts, names), do: for(name <- names, do: assert(Process.alive?(facts[name]), "#{name} alive"))

  defp stop!(root) do
    if Process.alive?(root), do: Supervisor.stop(root, :shutdown, 10_000)
    :ok
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

  # queued-DOWN setup on ONE server/worker pair: the correlated execute request (bound to this server AND worker)
  # is captured, the Server is suspended, the worker's reply is queued FIRST, then the worker is killed so its
  # real DOWN is queued SECOND; both are observable in the mailbox before dequeue. Returns {cap, gen, ref}.
  defp queue_reply_then_down!(server, worker) do
    :ok = :sys.suspend(server)
    send(worker, :release_gate)
    OwnedHarness.flush!()

    # the LATEST execute request bound to this exact server AND worker (earlier executes completed before the gate)
    request =
      self()
      |> mailbox()
      |> Enum.reverse()
      |> Enum.find(&match?({:run_effect_requested, ^server, %{op: :execute, worker: ^worker}}, &1))

    assert match?({:run_effect_requested, ^server, %{cap: _, gen: _, ref: _}}, request)
    {:run_effect_requested, ^server, %{cap: cap, gen: gen, ref: ref}} = request

    assert wait(fn -> Enum.any?(mailbox(server), &match?({:effect_result, ^cap, ^gen, ^ref, ^worker, _}, &1)) end)
    kill_and_join!(worker)
    assert wait(fn -> Enum.any?(mailbox(server), &match?({:DOWN, _, :process, ^worker, _}, &1)) end)
    {cap, gen, ref}
  end

  defp refute_applied!(server, {cap, gen, ref}) do
    OwnedHarness.flush!()
    refute_received {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^ref}}
  end

  defp kill_and_join!(worker) do
    mon = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^mon, :process, ^worker, :killed}, 5_000
  end

  defp owner_loss?(result), do: match?({:error, %{clause: "run_effect_owner_down"}}, result)
  defp generation_of({:error, map}), do: Map.get(map, :writer_generation)

  defp assert_no_canary(term), do: refute(inspect(term, limit: :infinity, printable_limit: :infinity) =~ @canary)

  describe "controls: registration/RunLock binding, effect-protocol gen and lifetimes on unchanged 5eeb50f" do
    test "C-1 the discovered Writer's registration generation is the held RunLock generation and not 1", %{dir: dir} do
      advanced = advance_lock!(dir)
      assert advanced >= 1
      facts = start!(held(dir))
      _worker = held_worker!(facts)
      registration = registration!(dir, facts)
      assert registration == holder_generation!(dir)
      assert registration > advanced
      assert registration != 1
      stop!(facts.root)
    end

    test "C-2 the effect-protocol gen traced on the request is 1 while the registration generation is above 1", %{
      dir: dir
    } do
      _ = advance_lock!(dir)
      facts = start!(held(dir))
      _worker = held_worker!(facts)
      assert registration!(dir, facts) > 1
      OwnedHarness.flush!()
      assert_received {:run_effect_requested, server, %{gen: 1}} when server == facts.server
      stop!(facts.root)
    end

    test "C-3 post-admission owner loss: subtree alive, no rebirth, Work empty, :failed, cached await stable", %{
      dir: dir
    } do
      facts = start!(held(dir))
      worker = held_worker!(facts)
      kill_and_join!(worker)
      first = Server.await(facts.server, 10_000)
      assert owner_loss?(first)
      assert Server.status(facts.server) == :failed
      alive!(facts, [:root, :writer, :server, :work])
      assert DynamicSupervisor.which_children(facts.work) == []
      refute_receive {:run_child_started, _, :worker, _}, 200
      assert Server.await(facts.server, 10_000) == first
      stop!(facts.root)
    end

    test "C-4 baseline before-shape observation: the owner-loss result is clause-only today (migration start)", %{
      dir: dir
    } do
      facts = start!(held(dir))
      worker = held_worker!(facts)
      kill_and_join!(worker)
      result = Server.await(facts.server, 10_000)
      assert owner_loss?(result)
      # evidence of the starting point; the enriched shape is the RED target, so this is match-not-equality
      assert match?({:error, %{clause: "run_effect_owner_down"}}, result)
      stop!(facts.root)
    end

    test "C-5 Executor.Owner passes a Server result through unchanged after teardown and the owner's DOWN", %{
      dir: dir
    } do
      ctx = config(dir, [])
      OwnedHarness.spawn_caller!(fn -> Owner.run(ctx, nil) end)
      assert_receive {:result, result}, 30_000
      assert match?({:ok, %{summary: %{"status" => "completed"}}}, result)
    end

    test "C-6 pre-admission loss and wait timeout keep their clauses today", %{dir: dir} do
      facts = start!(held(dir))
      assert Server.await(facts.server, 0) == {:error, %{clause: "await_timeout"}}
      _worker = held_worker!(facts)
      assert Server.await(facts.server, 0) == {:error, %{clause: "await_timeout"}}
      stop!(facts.root)

      failing = config(Path.join(dir, "pre"), worker_module: StartFailWorker)
      File.mkdir_p!(failing.run_dir)
      facts2 = start!(failing)
      assert Server.await(facts2.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
      stop!(facts2.root)
    end
  end

  describe "controls: the RED harness paths are executable today (queued DOWN, Owner pass-through)" do
    test "C-7 reply queued first, real DOWN second: clause-only owner loss today, the correlated reply never applied", %{
      dir: dir
    } do
      _ = advance_lock!(dir)
      facts = start!(held(dir))
      worker = held_worker!(facts)
      assert registration!(dir, facts) != 1
      op = queue_reply_then_down!(facts.server, worker)
      assert elem(op, 1) == 1
      :ok = :sys.resume(facts.server)
      assert owner_loss?(Server.await(facts.server, 10_000))
      refute_applied!(facts.server, op)
      stop!(facts.root)
    end

    test "C-8 Executor.Owner returns today's clause-only owner loss after its teardown and the Writer's DOWN", %{
      dir: dir
    } do
      parent = self()

      barrier = fn
        :subtree_started, facts_map ->
          send(parent, {:owned_facts, facts_map})
          :ok

        _, _ ->
          :ok
      end

      ctx = held(dir)
      OwnedHarness.spawn_caller!(fn -> Owner.run(ctx, barrier) end)
      assert_receive {:owned_facts, owned}, 10_000
      assert_receive {:run_child_started, _, :worker, owned_worker}, 10_000
      assert_receive {:gate_entered, ^owned_worker}, 10_000
      assert registration!(dir, %{writer: owned.writer}) >= 1
      writer_mon = Process.monitor(owned.writer)
      kill_and_join!(owned_worker)
      assert_receive {:result, result}, 30_000
      assert_receive {:DOWN, ^writer_mon, :process, _, _}, 10_000
      assert owner_loss?(result)
    end
  end

  describe "controls: existing-journal acquisition and the privacy surface on unchanged 5eeb50f" do
    test "C-9 a :resume subtree on the journal of a lost run acquires a strictly greater registration generation", %{
      dir: dir
    } do
      facts = start!(held(dir))
      worker = held_worker!(facts)
      first_gen = registration!(dir, facts)
      kill_and_join!(worker)
      assert owner_loss?(Server.await(facts.server, 10_000))
      stop!(facts.root)
      facts2 = start!(resumed(dir))
      second_gen = registration!(dir, facts2)
      assert second_gen > first_gen
      stop!(facts2.root)
    end

    test "C-10 today's owner-loss result, sys status and log carry neither the canary option nor the run dir", %{
      dir: dir
    } do
      ctx = canary_held(dir)
      canary = ctx.run_dir

      log =
        capture_log(fn ->
          facts = start!(ctx)
          worker = held_worker!(facts)
          assert File.dir?(canary) and String.contains?(canary, @canary)
          kill_and_join!(worker)
          result = Server.await(facts.server, 10_000)
          assert owner_loss?(result)
          assert_no_canary(result)
          status = inspect(:sys.get_status(facts.server, 5_000), limit: :infinity, printable_limit: :infinity)
          refute status =~ @canary
          refute status =~ canary
          assert status =~ ":failed"
          stop!(facts.root)
        end)

      refute log =~ @canary
    end
  end

  describe "RED: writer_generation on the post-admission owner-loss result" do
    test "L1 held worker killed after a RunLock advance: writer_generation is the captured registration and holder gen",
         %{dir: dir} do
      advanced = advance_lock!(dir)
      facts = start!(held(dir))
      worker = held_worker!(facts)
      registration = registration!(dir, facts)
      holder = holder_generation!(dir)
      assert registration == holder and registration > advanced and registration != 1
      kill_and_join!(worker)
      result = Server.await(facts.server, 10_000)
      assert result == {:error, %{clause: "run_effect_owner_down", writer_generation: registration}}
      stop!(facts.root)
    end

    test "L2 direct DOWN and reply-queued-then-DOWN produce the SAME enriched result; nothing stale applied", %{
      dir: dir
    } do
      # direct path
      _ = advance_lock!(dir)
      facts = start!(held(dir))
      worker = held_worker!(facts)
      registration = registration!(dir, facts)
      assert registration == holder_generation!(dir) and registration != 1
      kill_and_join!(worker)
      direct = Server.await(facts.server, 10_000)
      assert direct == {:error, %{clause: "run_effect_owner_down", writer_generation: registration}}
      stop!(facts.root)

      # queued path on its own advanced lock: generation bound to holder and exact Writer, distinct from gen 1
      queued_dir = Path.join(dir, "queued")
      File.mkdir_p!(queued_dir)
      _ = advance_lock!(queued_dir)
      facts2 = start!(held(queued_dir))
      worker2 = held_worker!(facts2)
      registration2 = registration!(queued_dir, facts2)
      assert registration2 == holder_generation!(queued_dir) and registration2 != 1
      op = queue_reply_then_down!(facts2.server, worker2)
      assert elem(op, 1) == 1 and registration2 != elem(op, 1)
      :ok = :sys.resume(facts2.server)
      queued = Server.await(facts2.server, 10_000)
      assert queued == {:error, %{clause: "run_effect_owner_down", writer_generation: registration2}}
      refute_applied!(facts2.server, op)
      assert Map.delete(elem(direct, 1), :writer_generation) == Map.delete(elem(queued, 1), :writer_generation)
      stop!(facts2.root)
    end

    test "L3 a :resume acquisition has a distinct generation; the first run's returned evidence keeps its ORIGINAL g",
         %{dir: dir} do
      facts = start!(held(dir))
      worker = held_worker!(facts)
      first_gen = registration!(dir, facts)
      kill_and_join!(worker)
      first = Server.await(facts.server, 10_000)
      assert generation_of(first) == first_gen
      # cached await proven BEFORE teardown
      assert Server.await(facts.server, 10_000) == first
      stop!(facts.root)
      facts2 = start!(resumed(dir))
      second_gen = registration!(dir, facts2)
      assert second_gen > first_gen
      # the saved immutable return from the first run is compared, not a new await on the stopped Server
      assert generation_of(first) == first_gen
      refute generation_of(first) == second_gen
      stop!(facts2.root)
    end

    test "L4 the enriched result survives cached await and Executor.Owner teardown to the caller unchanged", %{
      dir: dir
    } do
      facts = start!(held(dir))
      worker = held_worker!(facts)
      registration = registration!(dir, facts)
      kill_and_join!(worker)
      first = Server.await(facts.server, 10_000)
      assert generation_of(first) == registration
      assert Server.await(facts.server, 10_000) == first
      stop!(facts.root)

      owner_dir = Path.join(dir, "owner")
      File.mkdir_p!(owner_dir)
      parent = self()

      barrier = fn
        :subtree_started, facts_map ->
          send(parent, {:owned_facts, facts_map})
          :ok

        _, _ ->
          :ok
      end

      owner_ctx = held(owner_dir)
      OwnedHarness.spawn_caller!(fn -> Owner.run(owner_ctx, barrier) end)
      assert_receive {:owned_facts, owned}, 10_000
      assert_receive {:run_child_started, _, :worker, owned_worker}, 10_000
      assert_receive {:gate_entered, ^owned_worker}, 10_000
      owned_registration = registration!(owner_dir, %{writer: owned.writer})
      writer_mon = Process.monitor(owned.writer)
      kill_and_join!(owned_worker)
      assert_receive {:result, result}, 30_000
      assert_receive {:DOWN, ^writer_mon, :process, _, _}, 10_000
      assert result == {:error, %{clause: "run_effect_owner_down", writer_generation: owned_registration}}
    end

    test "L5 pre-admission loss, wait timeout and a forged reply carry no owner-loss generation evidence", %{dir: dir} do
      facts = start!(held(dir))
      assert Server.await(facts.server, 0) == {:error, %{clause: "await_timeout"}}
      worker = held_worker!(facts)
      registration = registration!(dir, facts)
      # forged reply with the wrong cap: dropped, no loss evidence, the run is still driving
      send(facts.server, {:effect_result, make_ref(), 1, make_ref(), worker, :forged})
      assert Server.status(facts.server) == :driving
      kill_and_join!(worker)
      assert generation_of(Server.await(facts.server, 10_000)) == registration
      stop!(facts.root)

      failing = config(Path.join(dir, "pre"), worker_module: StartFailWorker)
      File.mkdir_p!(failing.run_dir)
      facts2 = start!(failing)
      assert Server.await(facts2.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
      stop!(facts2.root)
    end

    test "L6 whole enriched result equals the captured registration; canary option/run dir absent from result, status, log",
         %{dir: dir} do
      ctx = canary_held(dir)
      canary = ctx.run_dir

      log =
        capture_log(fn ->
          facts = start!(ctx)
          worker = held_worker!(facts)
          registration = dir |> canary_dir() |> registration!(facts)
          assert String.contains?(canary, @canary)
          kill_and_join!(worker)
          result = Server.await(facts.server, 10_000)
          assert result == {:error, %{clause: "run_effect_owner_down", writer_generation: registration}}
          returned = generation_of(result)
          assert is_integer(returned) and returned >= 1 and returned <= 999_999_999_999
          assert result |> elem(1) |> Map.keys() |> Enum.sort() == [:clause, :writer_generation]
          assert_no_canary(result)
          text = inspect(result, limit: :infinity)
          refute text =~ "#PID" or text =~ "#Reference" or text =~ canary or text =~ "killed"
          status = inspect(:sys.get_status(facts.server, 5_000), limit: :infinity, printable_limit: :infinity)
          refute status =~ @canary
          refute status =~ canary
          assert status =~ ":failed"
          stop!(facts.root)
        end)

      refute log =~ @canary
      refute log =~ canary
    end
  end
end
