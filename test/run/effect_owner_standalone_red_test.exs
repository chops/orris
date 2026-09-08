defmodule AiOrchestrator.Run.EffectOwnerStandaloneRedTest do
  @moduledoc """
  RED/interface: standalone `Run.Supervisor` lifetime semantics with a worker (contract revision 2, rows L-*). The
  test starts the subtree itself, so the Server's closed result is observed through `Run.Server.await/2` BEFORE any
  teardown: who closes (the Server) and who tears down (the starter) are separated. Worker-side doubles implement
  the admission/release protocol from the worker's side so each outstanding stage can be stalled and killed.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.OwnerDoubles.AbandonGate
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_owner_standalone"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @sentinel "STANDALONE-SENTINEL-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

  defp worker, do: Module.concat(["AiOrchestrator", "Run", "Worker"])
  defp require_worker!, do: assert(Code.ensure_loaded?(worker()), "AiOrchestrator.Run.Worker does not exist")
  defp run_sup, do: AiOrchestrator.Run.Supervisor

  # ---- worker-side doubles: the protocol from the worker's side, stalled at one stage ----
  defmodule ProtocolWorker do
    @moduledoc false
    # a protocol-FAITHFUL worker double: admit builds the runtime from the seams; release/execute/settle run the
    # real Effects functions locally; every reply carries (cap, gen, op_ref, self()). One stage can be stalled
    # ({:stall, stage}) or answered with ONE field corrupted ({:corrupt, stage, field}); everything else is faithful.
    use GenServer

    alias AiOrchestrator.Effects
    alias AiOrchestrator.Effects.Runtime

    def child_spec(server),
      do: %{id: :worker, start: {__MODULE__, :start_link, [server]}, restart: :temporary, shutdown: 1_000}

    def start_link(server), do: GenServer.start_link(__MODULE__, server)

    @impl true
    def init(server) do
      {collector, mode} = :persistent_term.get({__MODULE__, :control})
      {:ok, %{server: server, collector: collector, mode: mode, runtime: nil, seams: []}}
    end

    @impl true
    def handle_info({:admit, cap, gen, seams}, state) do
      state = %{state | runtime: Runtime.new(seams), seams: seams}
      answer(:admit, {:admitted, cap, gen, self()}, state)
    end

    def handle_info({:release_terminal, cap, gen, ref, suffix}, state) do
      state = %{state | runtime: Effects.release_terminal(state.runtime, suffix)}
      answer(:release, {:released, cap, gen, ref, self()}, state)
    end

    def handle_info({:execute, cap, gen, ref, intent, receipt}, state) do
      {observation, runtime} = Effects.execute(intent, state.runtime, opts: state.seams, receipt: receipt)
      answer(:execute, {:effect_result, cap, gen, ref, self(), observation}, %{state | runtime: runtime})
    end

    def handle_info({:settle, cap, gen, ref}, state) do
      {cleanup, runtime} = Effects.settle(state.runtime)
      answer(:settle, {:settled, cap, gen, ref, self(), cleanup}, %{state | runtime: runtime})
    end

    def handle_info(_other, state), do: {:noreply, state}

    defp answer(stage, _reply, %{mode: {:stall, stage}} = state) do
      send(state.collector, {:stalled, stage, self()})
      {:noreply, state}
    end

    defp answer(stage, reply, %{mode: {:corrupt, stage, field}} = state) do
      send(state.server, corrupt(reply, field))
      send(state.collector, {:corrupted, stage, field, self()})
      {:noreply, state}
    end

    defp answer(_stage, reply, state) do
      send(state.server, reply)
      {:noreply, state}
    end

    defp corrupt(reply, :cap), do: put_elem(reply, 1, make_ref())
    defp corrupt(reply, :gen), do: put_elem(reply, 2, elem(reply, 2) + 1)
    defp corrupt(reply, :ref), do: put_elem(reply, 3, make_ref())
  end

  defmodule FailingWorker do
    @moduledoc false
    def child_spec(server),
      do: %{id: :worker, start: {__MODULE__, :start_link, [server]}, restart: :temporary, shutdown: 1_000}

    def start_link(_server), do: {:error, {:injected, :persistent_term.get({__MODULE__, :secret})}}
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "owner-standalone-#{System.unique_integer([:positive])}")
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

  defp stalling(dir, stage), do: with_worker(dir, {:stall, stage})
  defp corrupting(dir, stage, field), do: with_worker(dir, {:corrupt, stage, field})

  defp with_worker(dir, mode, extra \\ []) do
    :persistent_term.put({ProtocolWorker, :control}, {collector(), mode})
    config(dir, [worker_module: ProtocolWorker] ++ extra)
  end

  # a Server-stage failure (observer raise on the PrepareGate observation) with the handle live in the double
  defp settling_via(dir, mode) do
    AbandonGate.control(collector(), :ok)
    parent = collector()

    with_worker(dir, mode,
      gate_executor: AbandonGate,
      effect_observer: fn effect, _observation ->
        send(parent, {:observed, effect.__struct__})
        if match?(%Effect.PrepareGate{}, effect), do: raise("observer failure")
        :ok
      end
    )
  end

  defp held(dir), do: config(dir, gate_opts: [runner: OwnerDoubles.held_gate(collector())])

  # a Server-stage failure with a live handle in the worker: the observer raises on the PrepareGate observation
  # the L-2 failure: the observer (it runs IN the Server, Host.notify_observer) raises on the PrepareGate observation.
  # With a `gate` controller the observer first reports {:failure_ready, server, ref} and BLOCKS until {:release, ref}:
  # nothing on the failure/settle path can run before the release, so a monitor installed behind this gate is
  # provably installed before the Server can die (LC-M1). Every L-2c row now gates; the raise itself is unchanged.
  defp settling(dir, abandon_mode, gate) do
    AbandonGate.control(collector(), abandon_mode)
    parent = collector()

    config(dir,
      gate_executor: AbandonGate,
      effect_observer: fn effect, _observation ->
        send(parent, {:observed, effect.__struct__})

        if match?(%Effect.PrepareGate{}, effect) do
          if is_pid(gate) do
            ref = make_ref()
            send(gate, {:failure_ready, self(), ref})

            receive do
              {:release, ^ref} -> :ok
            after
              15_000 -> raise("failure gate never released")
            end
          end

          raise("observer failure")
        end

        :ok
      end
    )
  end

  # the positive ordering checker (LC-M2): the kill is only meaningful while the Server is still waiting, and the
  # whole post-release sequence (release -> abandon entry -> kill -> diagnostic DOWN) must end well inside the
  # settle budget; a Server already dead at the kill, any other DOWN, or a sequence beyond the bound is REJECTED
  defp early_death(server, smon, worker, started) do
    alive_at_kill = Process.alive?(server)
    Process.exit(worker, :kill)

    receive do
      {:DOWN, ^smon, :process, ^server, {:run_step_failed, diagnostic}} ->
        elapsed = System.monotonic_time(:millisecond) - started

        cond do
          not alive_at_kill -> {:rejected, :server_dead_before_kill}
          elapsed >= 2_000 -> {:rejected, {:beyond_bound_ms, elapsed}}
          true -> {:ok, diagnostic, elapsed}
        end

      {:DOWN, ^smon, :process, ^server, other} ->
        {:rejected, {:down, other}}
    after
      5_000 -> {:rejected, :no_down}
    end
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(10) && false)
      end)

    fun.()
  end

  # non-consuming: the exact monitor's DOWN is already queued for this process
  defp down_queued?(smon) do
    {:messages, msgs} = Process.info(self(), :messages)
    Enum.any?(msgs, &match?({:DOWN, ^smon, :process, _, _}, &1))
  end

  # the subtree is started by the TEST (linked, trapping); every child is registered through the traces
  defp start!(config) do
    assert {:ok, root} = run_sup().start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :writer, writer}, 10_000
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, writer: writer, server: server, work: work}
  end

  defp alive!(facts, names), do: for(name <- names, do: assert(Process.alive?(facts[name]), "#{name} alive"))
  defp gone!(facts, names), do: for(name <- names, do: refute(Process.alive?(facts[name]), "#{name} gone"))

  defp stop!(root) do
    if Process.alive?(root), do: Supervisor.stop(root, :shutdown, 10_000)
    :ok
  end

  # =================================================================================================
  test "control: L-0 a standalone run completes, the root outlives the result and is stopped by the starter", %{dir: dir} do
    facts = start!(config(dir, []))
    assert {:ok, %{summary: %{"status" => "completed"}}} = Server.await(facts.server, 30_000)
    alive!(facts, [:root, :writer, :server, :work])
    # intended lifetime: an owner born for the run lives at :finished until the starter's shutdown (no retirement)
    OwnedHarness.flush!()
    births = for {:run_child_started, _, :worker, w} <- messages(), do: w
    assert Enum.map(DynamicSupervisor.which_children(facts.work), &elem(&1, 1)) == births
    stop!(facts.root)
    gone!(facts, [:root, :writer, :server, :work])
  end

  test "control: L-4 killing Work exhausts the root (permanent child): Server and Writer gone, await answers run_server_down",
       %{dir: dir} do
    facts = start!(held(dir))
    assert_receive {:gate_entered, _executing}, 10_000
    rmon = Process.monitor(facts.root)
    Process.exit(facts.work, :kill)
    assert_receive {:DOWN, ^rmon, :process, _, :shutdown}, 10_000
    gone!(facts, [:root, :writer, :server, :work])
    assert Server.await(facts.server, 1_000) == {:error, %{clause: "run_server_down"}}
  end

  test "control: L-5 killing the Server exhausts the root; nothing restarts", %{dir: dir} do
    facts = start!(held(dir))
    assert_receive {:gate_entered, _executing}, 10_000
    # acknowledged drain of the INITIAL births (a worker's, when one exists) before the death
    OwnedHarness.flush!()
    drain_births()
    rmon = Process.monitor(facts.root)
    Process.exit(facts.server, :kill)
    assert_receive {:DOWN, ^rmon, :process, _, :shutdown}, 10_000
    gone!(facts, [:root, :writer, :server, :work])
    refute_receive {:run_child_started, _, _, _}, 200, "no post-death birth of any kind"
  end

  defp drain_births do
    receive do
      {:run_child_started, _, _, _} -> drain_births()
    after
      0 -> :ok
    end
  end

  describe "L-1 / L-3 birth and admission" do
    test "L-1 worker dies while admission is pending: run_worker_start_failed; root/Writer/Work alive; no rebirth",
         %{dir: dir} do
      require_worker!()
      facts = start!(stalling(dir, :admit))
      assert_receive {:run_child_started, work, :worker, w}, 10_000
      assert work == facts.work
      assert_receive {:stalled, :admit, ^w}, 10_000
      assert Server.status(facts.server) == :opening, "admission pending: still opening"
      Process.exit(w, :kill)
      assert Server.await(facts.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
      alive!(facts, [:root, :writer, :server, :work])
      assert DynamicSupervisor.which_children(facts.work) == []
      refute_receive {:run_child_started, _, :worker, _}, 200
      stop!(facts.root)
    end

    test "L-3a a worker whose start fails: run_worker_start_failed, no child, injected term in no log", %{
      dir: dir
    } do
      require_worker!()
      :persistent_term.put({FailingWorker, :secret}, @sentinel)

      log =
        capture_log(fn ->
          facts = start!(config(dir, worker_module: FailingWorker))
          assert Server.await(facts.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
          assert DynamicSupervisor.which_children(facts.work) == []
          refute_receive {:run_child_started, _, :worker, _}, 100
          stop!(facts.root)
        end)

      refute log =~ @sentinel
    end

    test "L-3b a worker that never admits: run_worker_start_failed within the admit-ack budget; :opening meanwhile", %{
      dir: dir
    } do
      require_worker!()
      facts = start!(stalling(dir, :admit))
      assert_receive {:stalled, :admit, w}, 10_000
      assert Server.status(facts.server) == :opening
      started = System.monotonic_time(:millisecond)
      assert Server.await(facts.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
      assert System.monotonic_time(:millisecond) - started < 7_000
      refute Process.alive?(w), "an un-admitted worker is not kept"
      stop!(facts.root)
    end
  end

  describe "L-6 per-op correlation from the worker side (admit: cap+gen+phase, no op_ref)" do
    for field <- [:cap, :gen] do
      test "L-6a admitted with the wrong #{field}: dropped; run_worker_start_failed within the budget",
           %{dir: dir} do
        require_worker!()
        facts = start!(corrupting(dir, :admit, unquote(field)))
        assert_receive {:corrupted, :admit, unquote(field), _w}, 10_000
        assert_receive {:run_effect_reply_dropped, server, reason}, 5_000
        assert server == facts.server and reason in [:cap_mismatch, :generation_stale]
        assert Server.status(facts.server) == :opening
        assert Server.await(facts.server, 10_000) == {:error, %{clause: "run_worker_start_failed"}}
        stop!(facts.root)
      end
    end

    test "L-6b released with the wrong ref: dropped; no execute requested; Server stays :driving (no release budget)",
         %{dir: dir} do
      require_worker!()
      facts = start!(corrupting(dir, :release, :ref))
      assert_receive {:corrupted, :release, :ref, _w}, 10_000
      assert_receive {:run_effect_reply_dropped, server, :ref_mismatch}, 5_000
      assert server == facts.server
      refute_receive {:run_effect_requested, ^server, %{op: :execute}}, 1_000
      assert Server.status(facts.server) == :driving
      stop!(facts.root)
    end

    test "L-6c settled with the wrong ref after a REAL Server-stage failure with a transferred handle: dropped; settle budget ends with cleanup UNKNOWN",
         %{dir: dir} do
      require_worker!()
      facts = start!(settling_via(dir, {:corrupt, :settle, :ref}))
      server = facts.server
      smon = Process.monitor(server)
      assert_receive {:run_child_started, _, :worker, w}, 10_000
      assert_receive {:abandoned, ^w, :ok}, 15_000, "the double settled its own handle faithfully"
      assert_receive {:corrupted, :settle, :ref, ^w}, 5_000
      assert_receive {:run_effect_reply_dropped, ^server, :ref_mismatch}, 5_000
      assert_receive {:DOWN, ^smon, :process, _, {:run_step_failed, diagnostic}}, 10_000
      assert diagnostic.cleanup == %{attempts: :unknown, settled: 0, unproven: :unknown}
    end

    test "L-6d duplicate of an applied reply at :finished (no outstanding): dropped no_outstanding; result unchanged",
         %{dir: dir} do
      require_worker!()
      facts = start!(config(dir, []))
      assert {:ok, result} = Server.await(facts.server, 30_000)
      OwnedHarness.flush!()
      server = facts.server
      applied = for {:run_effect_applied, ^server, {:execute, c, g, r}} <- messages(), do: {c, g, r}
      assert applied != [], "a completed gated run applies many executes; replay ONE specific already-applied one"
      {cap, gen, ref} = List.last(applied)
      assert {:run_child_started, _, :worker, w} = Enum.find(messages(), &match?({:run_child_started, _, :worker, _}, &1))
      send(server, {:effect_result, cap, gen, ref, w, %{"forged" => true}})
      assert_receive {:run_effect_reply_dropped, ^server, :no_outstanding}, 5_000
      assert Server.status(server) == :finished
      assert Server.await(server, 1_000) == {:ok, result}
      stop!(facts.root)
    end
  end

  describe "F-1 subunit: closed five-key diagnostic domain (a responsive Server at :finished)" do
    @sentinel_cleanup %{attempts: "STANDALONE-FORGED-SENTINEL", settled: "1", unproven: [1]}
    defp forged(cleanup),
      do: %{kind: :error, class: "map", digest: "sha256:" <> String.duplicate("a", 64), frames: 0, cleanup: cleanup}

    defp stop_with!(facts, reason) do
      capture_log(fn ->
        smon = Process.monitor(facts.server)
        :gen_statem.stop(facts.server, reason, 5_000)
        assert_receive {:DOWN, ^smon, :process, _, ^reason}, 5_000
      end)
    end

    test "control (invariant): a forged five-key tag with a MALFORMED cleanup is re-diagnosed in the report: no forged value printed",
         %{dir: dir} do
      facts = start!(config(dir, []))
      assert {:ok, _} = Server.await(facts.server, 30_000)
      log = stop_with!(facts, {:run_step_failed, forged(@sentinel_cleanup)})
      # the tag is not trusted: the report carries a re-diagnosed closed reason (kind/class/digest/frames) only
      assert log =~ "digest:" and log =~ "frames:"
      refute log =~ "STANDALONE-FORGED-SENTINEL"
      refute log =~ "cleanup"
    end

    @valid_cleanup %{attempts: :unknown, settled: 0, unproven: :unknown}
    for {label, reason} <- [
          {"an EXTRA cleanup key",
           %{
             kind: :error,
             class: "map",
             digest: "sha256:" <> String.duplicate("a", 64),
             frames: 0,
             cleanup: Map.put(@valid_cleanup, :extra, 1)
           }},
          {"a MISSING cleanup key",
           %{
             kind: :error,
             class: "map",
             digest: "sha256:" <> String.duplicate("a", 64),
             frames: 0,
             cleanup: Map.delete(@valid_cleanup, :unproven)
           }},
          {"a NEGATIVE count",
           %{
             kind: :error,
             class: "map",
             digest: "sha256:" <> String.duplicate("a", 64),
             frames: 0,
             cleanup: %{@valid_cleanup | settled: -1}
           }},
          {"a FLOAT count",
           %{
             kind: :error,
             class: "map",
             digest: "sha256:" <> String.duplicate("a", 64),
             frames: 0,
             cleanup: %{@valid_cleanup | attempts: 1.5}
           }},
          {"an OUTER extra key",
           %{
             kind: :error,
             class: "map",
             digest: "sha256:" <> String.duplicate("a", 64),
             frames: 0,
             cleanup: @valid_cleanup,
             extra: "x"
           }}
        ] do
      test "control (invariant): a five-key tag with #{label} is re-diagnosed; the report prints no cleanup", %{dir: dir} do
        facts = start!(config(dir, []))
        assert {:ok, _} = Server.await(facts.server, 30_000)
        log = stop_with!(facts, {:run_step_failed, unquote(Macro.escape(reason))})
        assert log =~ "digest:" and log =~ "frames:"
        refute log =~ "cleanup"
        refute log =~ "extra"
      end
    end

    test "F-1c a WELL-FORMED five-key diagnostic (integer or :unknown counts) is printed intact by the report", %{
      dir: dir
    } do
      require_worker!()
      facts = start!(config(dir, []))
      assert {:ok, _} = Server.await(facts.server, 30_000)
      log = stop_with!(facts, {:run_step_failed, forged(%{attempts: :unknown, settled: 0, unproven: :unknown})})
      assert log =~ "cleanup" and log =~ "unknown", "the closed cleanup summary is part of the closed domain"
    end
  end

  defp messages do
    {:messages, messages} = Process.info(self(), :messages)
    messages
  end

  describe "L-2 owner DOWN with a stage outstanding" do
    test "L-2a release outstanding: run_effect_owner_down; root/Writer/Work alive; no replacement", %{dir: dir} do
      require_worker!()
      facts = start!(stalling(dir, :release))
      assert_receive {:stalled, :release, w}, 10_000

      assert_receive {:run_child_started, _, :worker, ^w},
                     5_000,
                     "the initial birth, consumed before any rebirth is refuted"

      assert Server.status(facts.server) == :driving
      # U1b-0b-L shape migration (docs/contracts/owner-loss-generation.org): the loss result carries the exact
      # Writer sibling's registration generation, captured independently BEFORE the kill
      assert {:ok, %{writer: writer, generation: generation, state: :live}} = Ownership.status(dir)
      assert writer == facts.writer
      Process.exit(w, :kill)

      assert Server.await(facts.server, 10_000) ==
               {:error, %{clause: "run_effect_owner_down", writer_generation: generation}}

      alive!(facts, [:root, :writer, :server, :work])
      assert DynamicSupervisor.which_children(facts.work) == []
      refute_receive {:run_child_started, _, :worker, _}, 200
      stop!(facts.root)
    end

    test "L-2b execute outstanding (held gate): run_effect_owner_down; the root outlives the result",
         %{dir: dir} do
      require_worker!()
      facts = start!(held(dir))
      assert_receive {:run_child_started, _, :worker, w}, 10_000
      assert_receive {:gate_entered, ^w}, 10_000
      # U1b-0b-L shape migration: independent registration capture before the kill
      assert {:ok, %{writer: writer, generation: generation, state: :live}} = Ownership.status(dir)
      assert writer == facts.writer
      Process.exit(w, :kill)

      assert Server.await(facts.server, 10_000) ==
               {:error, %{clause: "run_effect_owner_down", writer_generation: generation}}

      alive!(facts, [:root, :writer, :server, :work])
      assert DynamicSupervisor.which_children(facts.work) == []
      refute_receive {:gate_entered, _}, 200
      stop!(facts.root)
      gone!(facts, [:root, :writer, :server, :work])
    end

    # CI 34155623540 (exact 92df1dd) failed this row with the monitor's DOWN reason :noproc: the Server had died on its
    # own settle budget BEFORE the test installed its monitor (the kill was scheduled by a collector-forwarded hop).
    # Corrected (rulings m_1788810030000, m_1788811177000): the monitor is installed behind the observer's pre-failure
    # gate, the timing origin is captured BEFORE the release, the abandon entry is acknowledged directly by the
    # Worker, and the positive checker rejects a Server that is not alive at the kill.
    test "L-2c settle outstanding (abandon hangs): the worker's death ends the wait early; cleanup UNKNOWN",
         %{dir: dir} do
      require_worker!()
      facts = start!(settling(dir, {:hang_ack, self()}, self()))
      server = facts.server
      assert_receive {:run_child_started, _, :worker, w}, 10_000
      # the Server itself reports it is about to fail and waits: the failure path has not started
      assert_receive {:failure_ready, ^server, ref}, 15_000
      smon = Process.monitor(server)
      assert Process.alive?(server)
      started = System.monotonic_time(:millisecond)
      send(server, {:release, ref})
      # the exact Worker reports its own entry into the hanging abandon (no collector hop)
      assert_receive {:abandon_entered, ^w}, 15_000
      assert {:ok, diagnostic, elapsed} = early_death(server, smon, w, started)
      assert elapsed < 2_000, "release -> entry -> kill -> DOWN ends the settle wait, no budget is waited out"
      assert diagnostic.cleanup == %{attempts: :unknown, settled: 0, unproven: :unknown}, "no reply: nothing known"
      assert diagnostic.kind == :error
      # the collector-forwarded report still arrives for the same Worker (ordinary trace, not the ordering signal)
      assert_receive {:abandoned, ^w, :hang}, 5_000
    end

    test "L-2c negative (deterministic late kill): a Server already dead on its budget before the kill is REJECTED by the checker",
         %{dir: dir} do
      require_worker!()
      facts = start!(settling(dir, {:hang_ack, self()}, self()))
      server = facts.server
      assert_receive {:run_child_started, _, :worker, w}, 10_000
      assert_receive {:failure_ready, ^server, ref}, 15_000
      smon = Process.monitor(server)
      started = System.monotonic_time(:millisecond)
      send(server, {:release, ref})
      assert_receive {:abandon_entered, ^w}, 15_000
      # deliberately let the Server's own settle budget kill it; the exact DOWN is witnessed queued, not consumed
      assert wait_until(fn -> down_queued?(smon) end, 15_000), "the budget death is queued before any kill"
      refute Process.alive?(server)
      assert {:rejected, :server_dead_before_kill} = early_death(server, smon, w, started)
    end

    test "L-2c control (missed monitor): a monitor installed only after the budget death sees :noproc, never the diagnostic",
         %{dir: dir} do
      require_worker!()
      facts = start!(settling(dir, {:hang_ack, self()}, self()))
      server = facts.server
      assert_receive {:run_child_started, _, :worker, w}, 10_000
      assert_receive {:failure_ready, ^server, ref}, 15_000
      early = Process.monitor(server)
      send(server, {:release, ref})
      assert_receive {:abandon_entered, ^w}, 15_000
      assert_receive {:DOWN, ^early, :process, ^server, {:run_step_failed, budget_diagnostic}}, 15_000
      assert budget_diagnostic.cleanup == %{attempts: :unknown, settled: 0, unproven: :unknown}
      late = Process.monitor(server)
      assert_receive {:DOWN, ^late, :process, ^server, :noproc}, 1_000
      refute_receive {:DOWN, ^late, :process, ^server, {:run_step_failed, _}}, 0
      Process.exit(w, :kill)
    end
  end
end
