defmodule AiOrchestrator.Run.GateAsyncPendingControlTest do
  @moduledoc """
  R1 controls for the owner-resident AwaitGate (docs/contracts/gate-async-await-proposal.org, map review
  m_1788888135086): the opaque pending accessor refuses incomplete carriers and never answers `:none` for a corrupt
  runtime; the Worker re-reads the accessor from its LATEST runtime before resuming or settling, so cached pending
  metadata that no longer matches the runtime routes nothing, resurrects nothing and is answered ONCE as a closed
  `effect_failed` (the executor's resume/settle primitive is never entered, the runtime is settled exactly once).
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.StepClock

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)
  @run_id "run_async_pending"
  @gate "gr_0001"
  @now 1_700_000_000
  @far @now + 600
  @loop 5_000
  @exit_ok "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"

  # the complete protocol delegated to the real executor; resume/settle/abandon entries are witnessed to the test
  defmodule WitnessExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: Execution
    defdelegate started_data(prepared), to: Execution
    defdelegate identity(prepared), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate await(running, opts), to: Execution
    defdelegate expire(running), to: Execution
    defdelegate stage(handle, message), to: Execution
    defdelegate evidence(run_dir, gate_run_id, attempt), to: Execution
    defdelegate pass?(outcome), to: Execution
    defdelegate begin_await(running, opts), to: Execution

    def resume_await(%{running: %{opts: opts}} = waiting, message) do
      witness(opts, {:resume_entered, self()})
      Execution.resume_await(waiting, message)
    end

    def settle_await(%{running: %{opts: opts}} = waiting) do
      witness(opts, {:settle_entered, self()})
      Execution.settle_await(waiting)
    end

    def abandon(%{opts: opts} = handle) do
      witness(opts, {:abandon_entered, self()})
      Execution.abandon(handle)
    end

    defp witness(opts, fact), do: send(Keyword.fetch!(opts, :witness), fact)
  end

  # ================= pure: the opaque accessor over carriers =================

  setup_all do
    dir = Path.join(System.tmp_dir!(), "async-pending-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    StepClock.set(@now, 0)
    on_exit(fn -> StepClock.clear() end)
    run_dir = Path.join(System.tmp_dir!(), "async-pending-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    {:ok, run_dir: run_dir, helper: helper}
  end

  describe "Effects.pending/1 carrier validation (R1)" do
    test "exactly one COMPLETE carrier names its key and port; every incomplete carrier is a closed refusal" do
      port = Port.open({:spawn, "cat"}, [:binary])
      other = Port.open({:spawn, "cat"}, [:binary])
      on_exit(fn -> Enum.each([port, other], &safe_close/1) end)

      effect = %Effect.AwaitGate{gate_run_id: "gr_a", attempt: 1, deadline_unix: @far}
      complete = %{phase: :awaiting, handle: %{port: port}, waiting: %{port: port, ref: make_ref()}, effect: effect}
      base = Runtime.new([])

      assert {:ok, %{key: {"gr_a", 1}, port: ^port}} = Effects.pending(gates(base, %{{"gr_a", 1} => complete}))

      refused = [
        missing_waiting: Map.delete(complete, :waiting),
        missing_effect: Map.delete(complete, :effect),
        descriptor_on_other_port: put_in(complete, [:waiting, :port], other),
        descriptor_without_ref: %{complete | waiting: %{port: port}},
        handle_without_port: %{complete | handle: %{}},
        non_port_handle: %{complete | handle: %{port: nil}},
        foreign_effect: %{complete | effect: %{effect | gate_run_id: "gr_b"}},
        wrong_effect_kind: %{complete | effect: %Effect.ReconcileGate{gate_run_id: "gr_a", attempt: 1, expected: %{}}},
        extra_carrier_key: Map.put(complete, :extra, true)
      ]

      for {label, entry} <- refused do
        assert match?({:error, %{clause: "pending_malformed"}}, Effects.pending(gates(base, %{{"gr_a", 1} => entry}))),
               "#{label} must be a closed refusal"
      end

      # a corrupt retained entry is never :none, whatever else the runtime holds
      assert {:error, %{clause: "runtime_malformed"}} =
               Effects.pending(gates(base, %{{"gr_c", 1} => %{phase: :bogus, handle: %{port: port}}}))

      assert {:error, %{clause: "runtime_malformed"}} = Effects.pending(gates(base, %{{"gr_c", 1} => %{phase: :running}}))

      assert {:error, %{clause: "runtime_malformed"}} =
               Effects.pending(gates(base, %{{"gr_a", 1} => complete, {"gr_c", 1} => %{phase: :running}}))

      assert {:error, %{clause: "runtime_malformed"}} = Effects.pending(:not_a_runtime)
      assert {:error, %{clause: "runtime_malformed"}} = Effects.pending(%{gates: %{}})

      second = %{complete | effect: %{effect | gate_run_id: "gr_b"}}

      assert {:error, %{clause: "pending_ambiguous"}} =
               Effects.pending(gates(base, %{{"gr_a", 1} => complete, {"gr_b", 1} => second}))

      # the well-formed retained shapes stay :none
      retained =
        base |> Runtime.put({"gr_a", 1}, :prepared, %{port: nil}) |> Runtime.put({"gr_b", 1}, :running, %{port: port})

      assert :none = Effects.pending(retained)
      assert :none = Effects.pending(base)
    end
  end

  describe "Worker pending revalidation (R1)" do
    # {label, mutation of the awaiting runtime, the completion message}
    # ================= the real Worker: stale cached metadata on every completion path =================
    for {label, disposition, path} <- [
          {"the runtime no longer awaits (accessor :none), record path", :none, :record},
          {"the runtime no longer awaits (accessor :none), wake path", :none, :wake},
          {"the runtime awaits ANOTHER key (accessor mismatch), wake path", :mismatch, :wake},
          {"the runtime awaits ANOTHER key (accessor mismatch), record path", :mismatch, :record},
          {"the awaiting carrier is malformed (closed refusal), wake path", :malformed, :wake}
        ] do
      test "#{label}: one closed effect_failed on the cached ref, the primitive never entered, settled once, pending cleared",
           ctx do
        test = self()
        {pid, cap} = worker(seams(ctx, WitnessExecutor, witness: test))
        {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"])
        ref = pending!(pid, cap)

        # the LATEST runtime is mutated behind the cached %{ref, key, port}: the accessor no longer names it
        :sys.replace_state(pid, fn state -> %{state | runtime: mutate(state.runtime, unquote(disposition))} end)
        assert %{pending: %{ref: ^ref, key: {@gate, 1}, port: ^port}} = loop!(pid)

        case unquote(path) do
          :record -> send(pid, {port, {:data, {:eol, @exit_ok}}})
          :wake -> send(pid, {:gate_deadline, cap, 1, ref})
        end

        assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, closed}, 20_000
        assert %{kind: :error, class: class, digest: "sha256:" <> _, cleanup: %{attempts: a, settled: s}} = closed
        assert is_binary(class) and is_integer(a) and is_integer(s)
        refute_received {:resume_entered, ^pid}
        refute_received {:settle_entered, ^pid}
        # R2: a purported primitive-entry fact can never coexist with the no-entry verdict
        refute_received {:gate_deadline, :worker, _, :resume}
        refute_received {:gate_deadline, :worker, _, :settle_await}
        assert_receive {:abandon_entered, ^pid}, 2_000
        refute_receive {:abandon_entered, ^pid}, 300
        refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, 300
        refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, 300

        assert %{pending: nil, runtime: %Runtime{gates: %{}}} = loop!(pid)
        assert :none = Effects.pending(loop!(pid).runtime)

        # nothing is resurrected afterwards: a late record and a late wake are facts only
        send(pid, {port, {:data, {:eol, @exit_ok}}})
        send(pid, {:gate_deadline, cap, 1, ref})
        assert %{pending: nil} = loop!(pid)
        refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, 300
        refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, 300
      end
    end
  end

  # ============ observer identity and entry facts: direct callback probes (reviewer m_1788891476967) ============

  defmodule ProbeExecutor do
    @moduledoc false
    def resume_await(_waiting, _message) do
      send(self(), {:primitive_entered, :resume})
      {:done, {:error, %{clause: "probe"}}}
    end

    def settle_await(_waiting) do
      send(self(), {:primitive_entered, :settle_await})
      {:done, {:error, %{clause: "probe"}}}
    end

    def abandon(_handle), do: :ok
  end

  describe "Worker observer identity and entry facts (D-10; R1/R2 of m_1788891476967)" do
    test "a malformed foreign wake never copies claim bytes into the observer identity (nil or a plain identity)" do
      s = probe_state()
      marker = "SYNTHETIC_OBSERVER_PAYLOAD"
      assert {:noreply, ^s} = Worker.handle_info({:gate_deadline, %{payload: marker}, [marker], marker}, s)
      assert_receive {:gate_deadline, :worker, identity, {:stale, reason}}
      assert reason in [:foreign_cap, :foreign_generation, :foreign_ref, :not_pending]
      refute inspect(identity, limit: :infinity) =~ marker
      assert identity == nil
      refute_received {:effect_result, _, _, _, _, _}
      refute_received {:effect_failed, _, _, _, _, _}
    end

    test "a well-formed foreign wake is a closed stale fact carrying exactly its plain identity" do
      s = probe_state()
      id = %{cap: make_ref(), gen: 1, ref: make_ref()}
      assert {:noreply, ^s} = Worker.handle_info({:gate_deadline, id.cap, id.gen, id.ref}, s)
      assert_receive {:gate_deadline, :worker, ^id, {:stale, :foreign_cap}}
      refute_received {:primitive_entered, _}
    end

    for path <- [:resume, :settle_await] do
      test "a stale cached pending op on the #{path} path: no entry fact, no primitive entry, one closed effect_failed" do
        port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary])

        try do
          s = probe_state()
          ref = make_ref()
          s = %{s | pending: %{ref: ref, key: {"gr_probe", 1}, port: port}}
          message = probe_message(unquote(path), s, port, ref)
          assert {:noreply, returned} = Worker.handle_info(message, s)
          assert returned.pending == nil
          assert_receive {:effect_failed, _, 1, ^ref, _, %{kind: :error}}
          refute_received {:primitive_entered, _}
          refute_received {:gate_deadline, :worker, _, unquote(path)}
        after
          Port.close(port)
        end
      end

      test "a validated pending op on the #{path} path: the entry fact precedes the primitive, then one result" do
        port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary])

        try do
          s = probe_state()
          key = {"gr_probe", 1}
          ref = make_ref()
          effect = %Effect.AwaitGate{gate_run_id: "gr_probe", attempt: 1, deadline_unix: @far}

          runtime =
            s.runtime
            |> Runtime.put(key, :running, %{port: port})
            |> Runtime.awaiting(key, %{port: port, ref: make_ref()}, effect)

          s = %{s | runtime: runtime, pending: %{ref: ref, key: key, port: port}}
          message = probe_message(unquote(path), s, port, ref)
          assert {:noreply, returned} = Worker.handle_info(message, s)
          assert returned.pending == nil
          assert %{phase: :running, handle: %{port: ^port}} = returned.runtime.gates[key]
          id = %{cap: s.cap, gen: s.gen, ref: ref}
          assert_receive {:gate_deadline, :worker, ^id, unquote(path)}
          assert_receive {:primitive_entered, unquote(path)}
          assert_receive {:effect_result, _, 1, ^ref, _, %Observation.GateError{}}
          refute_receive {:effect_result, _, 1, ^ref, _, _}, 100
        after
          Port.close(port)
        end
      end
    end
  end

  defp probe_state do
    %{
      server: self(),
      cap: make_ref(),
      gen: 1,
      pending: nil,
      runtime: Runtime.new(gate_executor: ProbeExecutor),
      seams: [gate_deadline_observer: self(), gate_executor: ProbeExecutor, clock: StepClock]
    }
  end

  defp probe_message(:resume, _s, port, _ref), do: {port, {:data, {:eol, "probe"}}}
  defp probe_message(:settle_await, s, _port, ref), do: {:gate_deadline, s.cap, s.gen, ref}

  # ================= harness lifetime (R3): the reaper runs and completes, before and after release =================

  describe "control harness lifetime (R3)" do
    test "a harness failure BEFORE release: the identity registered at READY is reclaimed by force with evidence", ctx do
      {pid, cap} = worker(seams(ctx, WitnessExecutor, witness: self()))
      ref = make_ref()

      prepare = %Effect.PrepareGate{
        gate_run_id: @gate,
        attempt: 1,
        requested: %{"command_argv" => ["/bin/sleep", "30"]},
        deadline_unix: @far,
        repo_root: ctx.run_dir,
        run_dir: ctx.run_dir
      }

      send(pid, {:execute, cap, 1, ref, prepare, nil})
      assert_receive {:ready, identity, owner, ready_ref}, 30_000
      track!(identity)
      send(owner, {:ready_ack, ready_ref})
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GatePrepared{}}, 20_000
      # the harness "fails" here: nothing is released or permitted; the guardian group is live and owned
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
      assert {:gone, :forced, %{live_at_branch: :alive, kill: {_, 0}}} = reap(identity, 300)
      assert dead?(identity)
    end

    test "a harness failure AFTER release while the await is pending: forced reclaim with evidence, once", ctx do
      {pid, cap} = worker(seams(ctx, WitnessExecutor, witness: self()))
      {identity, _port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"])
      _ref = pending!(pid, cap)
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
      assert {:gone, :forced, %{live_at_branch: :alive, kill: {_, 0}}} = reap(identity, 300)
      assert dead?(identity)
      # the registered on_exit reaper then finds the group already gone (idempotent, natural)
      assert {:gone, :natural} = reap(identity, 300)
    end

    test "a completed run is reclaimed naturally: no force, the group settled by the guardian", ctx do
      {pid, cap} = worker(seams(ctx, WitnessExecutor, witness: self()))
      argv = ["/bin/sh", "-c", "while [ ! -f '#{Path.join(ctx.run_dir, "go")}' ]; do sleep 0.02; done; exit 0"]
      {identity, _port} = released!(pid, cap, ctx.run_dir, argv)
      ref = pending!(pid, cap)
      File.write!(Path.join(ctx.run_dir, "go"), "")
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
      assert {:gone, :natural} = reap(identity)
    end
  end

  # :none = the entry is back on its retained running handle; :mismatch = a complete carrier under another key;
  # :malformed = the carrier lost its effect
  defp mutate(runtime, :none), do: Runtime.resumed(runtime, {@gate, 1})

  defp mutate(%Runtime{gates: gates} = runtime, :mismatch) do
    entry = Map.fetch!(gates, {@gate, 1})
    moved = %{entry | effect: %{entry.effect | gate_run_id: "gr_other"}}
    %{runtime | gates: gates |> Map.delete({@gate, 1}) |> Map.put({"gr_other", 1}, moved)}
  end

  defp mutate(%Runtime{gates: gates} = runtime, :malformed),
    do: %{runtime | gates: Map.update!(gates, {@gate, 1}, &Map.delete(&1, :effect))}

  defp gates(%Runtime{} = runtime, gates), do: %{runtime | gates: gates}

  defp safe_close(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp seams(ctx, executor, gate_extra) do
    [
      gate_executor: executor,
      gate_helper: ctx.helper,
      gate_opts: [settle_ms: 200, rounds: 2, clock: StepClock, barrier: ready_barrier(self())] ++ gate_extra,
      # ---- the Worker harness (the AW rows' shape: register-then-ACK barrier, durable start, release) ----
      run_id: @run_id,
      supervisor_instance: "sup_async_pending",
      clock: StepClock,
      fs: {SystemFs, nil},
      run_dir: ctx.run_dir
    ]
  end

  defp ready_barrier(parent) do
    fn
      :after_ready, identity ->
        ref = make_ref()
        send(parent, {:ready, identity, self(), ref})

        receive do
          {:ready_ack, ^ref} -> true
        after
          30_000 -> exit({:ready_not_acknowledged, ref})
        end

      _name, _info ->
        true
    end
  end

  defp worker(seams) do
    pid = start_supervised!(Supervisor.child_spec({Worker, self()}, restart: :temporary))
    cap = make_ref()
    send(pid, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    {pid, cap}
  end

  defp released!(pid, cap, run_dir, argv) do
    ref = make_ref()

    prepare = %Effect.PrepareGate{
      gate_run_id: @gate,
      attempt: 1,
      requested: %{"command_argv" => argv},
      deadline_unix: @far,
      repo_root: run_dir,
      run_dir: run_dir
    }

    send(pid, {:execute, cap, 1, ref, prepare, nil})
    assert_receive {:ready, identity, owner, ready_ref}, 30_000
    # the native identity is owned by the test BEFORE the owner is released past READY (every later step is fallible)
    track!(identity)
    send(owner, {:ready_ack, ready_ref})
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GatePrepared{started: started}}, 20_000
    persisted = persist!(run_dir, started)
    rref = make_ref()
    send(pid, {:execute, cap, 1, rref, %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}, persisted})
    assert_receive {:effect_result, ^cap, 1, ^rref, ^pid, %Observation.GateReleased{}}, 20_000
    [port] = pid |> Process.info(:links) |> elem(1) |> Enum.filter(&is_port/1)
    {identity, port}
  end

  defp pending!(pid, cap) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: @far}, nil})
    state = loop!(pid)
    assert {:ok, %{key: {@gate, 1}}} = Effects.pending(state.runtime)
    ref
  end

  defp loop!(pid), do: :sys.get_state(pid, @loop)

  defp persist!(run_dir, data) do
    lock = [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
    {:ok, w, _} = Writer.open(run_dir, create: true, clock: FixedClock, lock: lock)

    event = %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 2,
      "seq" => 1,
      "event_id" => "ev_0001",
      "type" => "gate_started",
      "ts" => "2026-01-01T00:00:01Z",
      "run_id" => @run_id,
      "actor" => "run_supervisor",
      "data" => data
    }

    try do
      assert {:ok, persisted} = Writer.append(w, event)
      persisted
    after
      :ok = Writer.close(w)
    end
  end

  # ---- the reaper (R3; the AR-M16 shape): one proven outcome per owned identity, registered before fallible work ----
  # {:gone, :natural} within the bound; {:gone, :forced, %{live_at_branch, kill}} after a -9 on the OWNED group only;
  # :unproven fails closed (raised from on_exit) - a kill command's exit is never taken as proof of cleanup
  defp track!(identity) do
    on_exit(fn ->
      case reap(identity) do
        :unproven -> raise("owned group #{identity.pgid} survived the reaper")
        _proven -> :ok
      end
    end)
  end

  defp reap(identity, bound \\ 15_000) do
    if wait_until(fn -> dead?(identity) end, bound), do: {:gone, :natural}, else: reap_forced(identity)
  end

  # the forced branch: liveness measured immediately before the -9 on the OWNED group, the kill's own result, recheck
  defp reap_forced(identity) do
    live = signal_zero("-" <> Integer.to_string(identity.pgid))
    kill = System.cmd("kill", ["-9", "-" <> Integer.to_string(identity.pgid)], stderr_to_stdout: true)

    if wait_until(fn -> dead?(identity) end, 5_000),
      do: {:gone, :forced, %{live_at_branch: live, kill: kill}},
      else: :unproven
  end

  # ---- OS oracles (as in gate_ownership_baseline_test / worker_spike_native_test) ----
  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n", trim: true) |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
      {"", 1} -> []
      {out, status} -> flunk("ps proved nothing (#{status}): #{inspect(out)}")
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do:
      signal_zero(Integer.to_string(pid)) == :gone and signal_zero("-" <> Integer.to_string(pgid)) == :gone and
        members(pgid) == []

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    fun.()
  end
end
