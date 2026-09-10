defmodule AiOrchestrator.Run.GateAsyncAwaitRedTest do
  @moduledoc """
  RED/interface + controls for the owner-resident asynchronous AwaitGate (docs/contracts/gate-async-await-proposal.org
  rev 3; scope m_1788828811174, corrections m_1788831011600 AR-M1..8). Execution-level rows (SA-*) own a REAL guardian
  in this process and name the proposed `Execution.settle_await/1`; Worker-level rows (AW-*) run the REAL `Run.Worker`
  with the REAL native executor. Rows titled "control" pass today. Server-owned actuation rows live in
  run_server_foundation_red_test.exs (tag :aw_server). Harness: native identities are registered BEFORE the READY ack
  (register-then-ack barrier), probes are monitored and joined, the bounded poll is fail-closed, the Writer is closed
  in `after`, and a reaper kills the process group and RETAINS the run directory when settlement is unproven.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.StepClock

  @moduletag :native

  @source Path.expand("../../bin/build-guardian", __DIR__)
  @run_id "run_async_await"
  @gate "gr_0001"
  @now 1_700_000_000
  @far @now + 600
  @probe_ms 300
  @loop 5_000
  @bogus "BOGUS kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"
  @exit_missing_field "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone"
  @dead_ok "DEAD reason=command kind=signaled signal=15 settled=1 leftovers=0 proof=gone escaped=unknown"
  @exit_ok "EXIT kind=exited status=0 settled=1 leftovers=0 proof=gone escaped=unknown"
  @gate_passed [%{"type" => "gate_passed", "data" => %{"gate_run_id" => "gr_0001"}}]

  defmodule Clock do
    @moduledoc false
    def unix_now, do: Process.get(:aa_now, 1_700_000_000)
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  # tracked native identities per test pid (unlinked agent: the reaper reads it after the test process exited)
  defmodule Tracked do
    @moduledoc false
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def put(test, identity), do: Agent.update(__MODULE__, &Map.update(&1, test, [identity], fn l -> [identity | l] end))
    def take(test), do: Agent.get_and_update(__MODULE__, &Map.pop(&1, test, []))
    def list(test), do: Agent.get(__MODULE__, &Map.get(&1, test, []))
  end

  # counts every adapter entry so the duplicate matrix can prove ZERO adapter starts (AR-M5)
  defmodule CountingDispatch do
    @moduledoc false
    def deliver(command, opts), do: witness(:deliver, command, opts)
    def observe(command, opts), do: witness(:observe, command, opts)
    def reconcile(command, opts), do: witness(:reconcile, command, opts)

    defp witness(kind, _command, opts) do
      send(Keyword.fetch!(opts, :witness), {:adapter_started, kind})
      {:error, %{clause: "counting_double"}}
    end
  end

  # the executor protocol Effects calls today, delegated verbatim, plus the proposed async protocol; the modes are
  # configured OWNER-SIDE through the handle's own opts
  defmodule ModeExecutor do
    @moduledoc false
    defdelegate started_data(prepared), to: Execution
    defdelegate identity(handle), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate await(running, opts), to: Execution
    defdelegate expire(running), to: Execution
    defdelegate stage(handle, message), to: Execution
    defdelegate evidence(run_dir, gate_run_id, attempt), to: Execution
    defdelegate pass?(outcome), to: Execution
    defdelegate reconcile(fs, run_dir, expected, opts), to: Execution

    # entry counters (AR-M12 / AR-M11): every prepare and abandon entry is witnessed to the owner-side recipient
    def prepare(fs, request, opts) do
      witness(opts, {:prepare_entered, request.gate_run_id})
      Execution.prepare(fs, request, opts)
    end

    def abandon(%{opts: opts} = handle) do
      witness(opts, {:abandon_entered, self()})
      Execution.abandon(handle)
    end

    defp witness(opts, fact) do
      case Keyword.get(opts, :witness) do
        pid when is_pid(pid) -> send(pid, fact)
        _ -> :ok
      end
    end

    def begin_await(%{opts: opts} = running, gate_opts),
      do: mode(opts, :begin_mode, fn -> Execution.begin_await(running, gate_opts) end)

    def resume_await(%{running: %{opts: opts}} = waiting, message),
      do: mode(opts, :resume_mode, fn -> Execution.resume_await(waiting, message) end)

    def settle_await(%{running: %{opts: opts}} = waiting), do: mode(opts, :settle_mode, fn -> settle(waiting) end)

    # RED: the primitive does not exist on Execution yet; resolved at runtime on purpose
    defp settle(waiting), do: Module.concat(["AiOrchestrator", "Gate", "Execution"]).settle_await(waiting)

    defp mode(opts, key, real) do
      case Keyword.get(opts, key, :real) do
        :real -> real.()
        :invalid -> :not_an_answer
        # a structurally invalid but well-typed answer: the mapping AFTER the runtime transition must fail closed
        :bad_answer -> {:done, {:exit, :not_an_outcome}}
        :raise -> raise("#{key} escaped")
        :throw -> throw({key, :thrown})
        :exit -> exit({key, :exited})
      end
    end
  end

  # the EXISTING synchronous path's trappable failure (AR-M15c control LR-C1): a fully synchronous executor whose
  # await raises inside execute/3, so the Worker settles the runtime an Interrupted carries TODAY; abandon witnessed
  defmodule LrRaisingExecutor do
    @moduledoc false
    defdelegate started_data(prepared), to: Execution
    defdelegate identity(handle), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate expire(running), to: Execution
    defdelegate stage(handle, message), to: Execution
    defdelegate evidence(run_dir, gate_run_id, attempt), to: Execution
    defdelegate pass?(outcome), to: Execution
    defdelegate reconcile(fs, run_dir, expected, opts), to: Execution
    defdelegate prepare(fs, request, opts), to: Execution

    def await(_running, _opts), do: raise("synchronous await failure (LR-C1 control)")

    def abandon(%{opts: opts} = handle) do
      if pid = Keyword.get(opts, :witness), do: send(pid, {:abandon_entered, self()})
      Execution.abandon(handle)
    end
  end

  # partial capability: begin/resume only (no settle_await) -> the synchronous fallback must stay in force
  defmodule PartialExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: Execution
    defdelegate started_data(prepared), to: Execution
    defdelegate identity(handle), to: Execution
    defdelegate ack(prepared, persisted), to: Execution
    defdelegate release(prepared, ack, opts), to: Execution
    defdelegate await(running, opts), to: Execution
    defdelegate abandon(handle), to: Execution
    defdelegate expire(running), to: Execution
    defdelegate stage(handle, message), to: Execution
    defdelegate begin_await(running, opts), to: Execution
    defdelegate resume_await(waiting, message), to: Execution
    defdelegate evidence(run_dir, gate_run_id, attempt), to: Execution
    defdelegate pass?(outcome), to: Execution
    defdelegate reconcile(fs, run_dir, expected, opts), to: Execution
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "async-await-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    _ = Tracked.start()
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup %{helper: helper} do
    Process.put(:aa_now, @now)
    StepClock.set(@now, 0)
    on_exit(fn -> StepClock.clear() end)
    run_dir = Path.join(System.tmp_dir!(), "async-await-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    test = self()
    # the reaper (AR-M8): every tracked group must be gone; a survivor is killed (-9 on the group) and rechecked; an
    # unproven settlement keeps the run directory for inspection and fails the test (reap_all!/3, AR-M16 HF rows)
    on_exit(fn -> reap_all!(test, run_dir, []) end)

    {:ok, run_dir: run_dir, helper: helper, opts: [helper: helper, settle_ms: 200, rounds: 2, clock: Clock]}
  end

  # ---- the reaper as an observable function (AR-M16 HF-1/HF-3..HF-5): one outcome per identity ----
  # {:gone, :natural}                       gone within the bound, no force
  # {:gone, :forced, %{live_at_branch, kill}} the forced branch: liveness of the group measured IMMEDIATELY before
  #                                         the kill (-9 on the group), the kill's own output/exit, then gone
  # :unproven                               still not gone after the force: settlement unproven
  # `opts`: :bound (ms before the forced branch, default 15_000), :dead (the settlement predicate; a test-only
  # surrogate may model an unproven settlement without any live native work)
  defp reap(identity, opts) do
    bound = Keyword.get(opts, :bound, 15_000)
    dead = Keyword.get(opts, :dead, &dead?/1)

    if wait_until(fn -> dead.(identity) end, bound), do: {:gone, :natural}, else: reap_forced(identity, dead)
  end

  # the forced branch: liveness measured immediately before the kill, the kill's own result, then the recheck
  defp reap_forced(identity, dead) do
    live = signal_zero("-" <> Integer.to_string(identity.pgid))
    kill = kill_group(identity)

    if wait_until(fn -> dead.(identity) end, 5_000),
      do: {:gone, :forced, %{live_at_branch: live, kill: kill}},
      else: :unproven
  end

  # every identity this test registered: reaped, then the directory removed ONLY when every settlement is proven;
  # otherwise the directory is RETAINED for inspection and the failure raised (fail closed)
  defp reap_all!(test, dir, opts) do
    outcomes = for identity <- Tracked.take(test), do: {identity, reap(identity, opts)}
    survivors = for {identity, :unproven} <- outcomes, do: identity.pgid

    if survivors == [] do
      File.rm_rf(dir)
      {:removed, outcomes}
    else
      raise("owned groups survived the reaper: #{inspect(survivors)}; #{dir} retained")
    end
  end

  defp kill_group(%{pgid: pgid}), do: System.cmd("kill", ["-9", "-" <> Integer.to_string(pgid)], stderr_to_stdout: true)

  # ---- in-process owner driver ----
  defp request(run_dir, argv, deadline) do
    %{
      run_id: @run_id,
      gate_run_id: @gate,
      attempt: 1,
      command_argv: argv,
      repo_root: run_dir,
      run_dir: run_dir,
      deadline_unix: deadline,
      supervisor_instance: "sup_0001"
    }
  end

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

  # the identity is registered with the reaper BEFORE the first fallible step (ack / release)
  defp running!(run_dir, argv, deadline, opts) do
    assert {:ok, prepared} = Execution.prepare({SystemFs, nil}, request(run_dir, argv, deadline), opts)
    identity = Execution.identity(prepared)
    Tracked.put(self(), identity)
    started = Execution.started_data(prepared)
    assert {:ok, ack} = Execution.ack(prepared, persist!(run_dir, started))
    assert {:ok, running} = Execution.release(prepared, ack, opts)
    {running, identity, started}
  end

  defp gated(run_dir), do: ["/bin/sh", "-c", "while [ ! -f '#{Path.join(run_dir, "go")}' ]; do sleep 0.02; done; exit 0"]
  defp permit!(run_dir), do: File.write!(Path.join(run_dir, "go"), "")
  defp line(port, text), do: {port, {:data, {:eol, text}}}
  # RED: the primitive does not exist yet; runtime module values keep --warnings-as-errors honest
  defp execution, do: Module.concat(["AiOrchestrator", "Gate", "Execution"])
  defp effects, do: Module.concat(["AiOrchestrator", "Effects"])
  defp memo(port, key), do: Process.get({Execution, port, key})

  defp queued_record?(port, prefix) do
    {:messages, msgs} = Process.info(self(), :messages)
    Enum.any?(msgs, &match?({^port, {:data, {:eol, l}}} when binary_part(l, 0, byte_size(prefix)) == prefix, &1))
  end

  defp drain_status(port) do
    receive do
      {^port, {:exit_status, _}} -> :ok
    after
      2_000 -> :none
    end
  end

  # ---- OS oracles ----
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

  # fail-closed bounded poll: no true is accepted after the bound (the deadline is checked before an observation and
  # again after a true one); the predicate's own duration is not bounded
  defp wait_until(fun, timeout_ms), do: poll(fun, System.monotonic_time(:millisecond) + timeout_ms)

  defp poll(fun, deadline) do
    cond do
      System.monotonic_time(:millisecond) >= deadline ->
        false

      fun.() ->
        System.monotonic_time(:millisecond) < deadline

      true ->
        Process.sleep(20)
        poll(fun, deadline)
    end
  end

  # ================= harness controls (AR-M8) =================

  describe "harness controls" do
    test "HC-1 control: the bounded poll answers false for an always-false condition (no after-deadline success)" do
      refute wait_until(fn -> false end, 60)
      assert wait_until(fn -> true end, 60)
    end

    test "HC-2 control: a true observed only PAST the deadline is rejected (late-true negative)" do
      refute wait_until(fn -> Process.sleep(120) && true end, 60)
    end

    test "HF-1 control: the reaper's FORCED branch: group live right before the kill, kill acts, group and worker gone",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf1")
      File.mkdir_p!(dir)
      {_running, identity, _} = running!(dir, ["/bin/sleep", "30"], @far, o)
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
      # a short bound: `sleep 30` cannot complete naturally, so the reaper MUST take the forced branch
      assert {:gone, :forced, %{live_at_branch: :alive, kill: {_out, 0}}} = reap(identity, bound: 300)
      assert signal_zero(Integer.to_string(identity.worker)) == :gone, "the worker process is gone"
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :gone, "the group is gone"
      assert dead?(identity)
    end

    test "HF-1n natural-exit contrast (passes today): a self-completing command is {:gone, :natural}, not forced cleanup",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf1n")
      File.mkdir_p!(dir)
      {_running, identity, _} = running!(dir, gated(dir), @far, o)
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
      permit!(dir)
      assert {:gone, :natural} = reap(identity, bound: 10_000)
      assert dead?(identity)
    end

    # ---- real failing owners (AR-M16; m_1788879223657300542): a SURROGATE owner process registers the identity under
    # its OWN pid (running!/4, before the fallible steps), reports it to the test (which registers the same identity
    # under itself as the independent FALLBACK recovery, before any further native work), then FAILS at the boundary.
    # Measured fact (pinned below): when the owner dies its Port closes and the guardian settles the group itself,
    # so a dead owner's group is reclaimed as a NATURAL settlement; the forced branch is for a LIVE owner that
    # neglects its group (HF-3h) or a lost guardian (HF-1). ----
    # the failing-owner fixture command stays LIVE after the permit (it never completes on its own within the test),
    # so an after-permit failure provably happens while native work is live; the guardian's EOF-driven settlement
    # after the owner's death is then a legitimate natural reclaim
    defp live_after_permit(run_dir),
      do: ["/bin/sh", "-c", "while [ ! -f '#{Path.join(run_dir, "go")}' ]; do sleep 0.02; done; exec /bin/sleep 30"]

    defp failing_owner!(dir, o, boundary) do
      test = self()

      owner =
        spawn(fn ->
          # START HANDSHAKE: no native work until the test owns this child's teardown
          send(test, {:owner_ready, self()})

          receive do
            :start -> :ok
          after
            30_000 -> exit(:owner_never_started)
          end

          {_running, identity, _} = running!(dir, live_after_permit(dir), @far, o)
          send(test, {:owner_registered, identity, self()})

          receive do
            :proceed -> :ok
          after
            30_000 -> exit(:owner_never_released)
          end

          if boundary == :after_permit, do: permit!(dir)
          # the native work is still LIVE at the moment of failure (witnessed by the owner itself, just before)
          send(test, {:live_at_failure, self(), signal_zero("-" <> Integer.to_string(identity.pgid))})
          exit({:owner_failure, boundary})
        end)

      mref = Process.monitor(owner)
      assert_receive {:owner_ready, ^owner}, 5_000
      # owned child teardown, registered BEFORE the child may start native work: if this test fails at any later
      # point, the owner is killed (its Port closes, the guardian settles the group), JOINED through the callback's
      # own monitor (the kill is asynchronous; the child could still be registering), and only then reaped
      on_exit(fn ->
        join = Process.monitor(owner)
        if Process.alive?(owner), do: Process.exit(owner, :kill)

        receive do
          {:DOWN, ^join, :process, ^owner, _reason} -> :ok
        after
          10_000 -> raise("failing owner did not terminate within the join bound")
        end

        _ = reap_all!(owner, dir, [])
      end)

      send(owner, :start)
      assert_receive {:owner_registered, identity, ^owner}, 30_000
      # the owner's own registration, witnessed without consuming it; the test's fallback registration
      assert Tracked.list(owner) == [identity]
      Tracked.put(test, identity)
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
      send(owner, :proceed)
      assert_receive {:live_at_failure, ^owner, :alive}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^owner, {:owner_failure, ^boundary}}, 5_000
      {owner, identity}
    end

    test "HF-3 before-permit failure (passes today): a real owner registers, then dies before permitting; its group is settled and reclaimed through ITS registration",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf3")
      File.mkdir_p!(dir)
      {owner, identity} = failing_owner!(dir, o, :before_permit)
      # the same teardown path, acting for the DEAD owner: reclaim through the owner's registration
      assert {:removed, [{^identity, {:gone, :natural}}]} = reap_all!(owner, dir, bound: 10_000)
      assert dead?(identity) and Tracked.list(owner) == []
      refute File.exists?(dir)
      # the fallback registration is still the test's; the on_exit reaper will find the group already gone
      assert Tracked.list(self()) == [identity]
    end

    test "HF-4 after-permit failure (passes today): a real owner permits the gate and dies at once; its group is settled and reclaimed through ITS registration",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf4")
      File.mkdir_p!(dir)
      {owner, identity} = failing_owner!(dir, o, :after_permit)
      assert {:removed, [{^identity, {:gone, :natural}}]} = reap_all!(owner, dir, bound: 10_000)
      assert dead?(identity) and Tracked.list(owner) == []
      refute File.exists?(dir)
      assert Tracked.list(self()) == [identity]
    end

    test "HF-3h live-owner neglect (passes today): the owner stays alive and never permits nor abandons; the reaper reclaims the LIVE group by force and removes the directory",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf3h")
      File.mkdir_p!(dir)
      {_running, identity, _} = running!(dir, gated(dir), @far, o)
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive

      assert {:removed, [{^identity, {:gone, :forced, %{live_at_branch: :alive, kill: {_, 0}}}}]} =
               reap_all!(self(), dir, bound: 300)

      assert dead?(identity)
      refute File.exists?(dir)
      assert Tracked.list(self()) == []
    end

    test "HF-5 unproven-retain (passes today): unproven settlement fails closed and RETAINS the evidence dir; own teardown",
         %{run_dir: d, opts: o} do
      dir = Path.join(d, "hf5")
      File.mkdir_p!(dir)
      evidence = Path.join(dir, "evidence.txt")
      File.write!(evidence, "retain me")
      # a REAL identity whose group has ALREADY settled (nothing can leak), judged by a SURROGATE predicate that
      # never proves settlement: models the unproven case without any live native work
      {_running, identity, _} = running!(dir, gated(dir), @far, o)
      permit!(dir)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      unproven = fn _identity -> false end

      error =
        assert_raise(RuntimeError, fn -> reap_all!(self(), dir, bound: 100, dead: unproven) end)

      assert error.message =~ "survived the reaper" and error.message =~ "#{dir} retained"
      # the concrete retention outcome: the directory AND its evidence are still there
      assert File.exists?(evidence) and File.read!(evidence) == "retain me"
      assert Tracked.take(self()) == [], "the identities were consumed by the reaper even when unproven"
      # the negative's own reliable teardown (the surrogate lied; the real group is dead)
      assert dead?(identity)
      File.rm_rf!(dir)
      refute File.exists?(dir)
    end

    test "HF-2 control: the Writer is closed even when the durable append fails (primary failure not masked)",
         %{run_dir: d} do
      dir = Path.join(d, "hf2")
      File.mkdir_p!(dir)
      assert_raise ExUnit.AssertionError, fn -> persist!(dir, %{"not" => "a started record"}) end
      assert wait_until(fn -> Ownership.status(dir) == :none end, 5_000), "lock released"
    end
  end

  # ================= SA: Execution.settle_await/1 (owner = this process, real guardian) =================

  describe "SA settle_await (RED: Execution.settle_await/1)" do
    test "SA-1 memo first: an expired handle's active descriptor settles from the termination memo", %{
      run_dir: d,
      opts: o
    } do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert {:timeout, termination} = Execution.expire(running)
      assert {:done, {:timeout, ^termination}} = execution().settle_await(waiting)
      assert :finalized == memo(running.port, :descriptor)
    end

    test "SA-2 FIRST record: a real EXIT queued behind the descriptor settles as the exit (no expire)", %{
      run_dir: d,
      opts: o
    } do
      {running, identity, _} = running!(d, gated(d), @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      permit!(d)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert wait_until(fn -> queued_record?(running.port, "EXIT ") end, 5_000)
      assert {:done, {:exit, %{"kind" => "exited", "exit_status" => 0}}} = execution().settle_await(waiting)
      assert nil == memo(running.port, :termination), "no expire ran"
      assert :finalized == memo(running.port, :descriptor)
    end

    test "SA-3 FIRST record unknown head (staged) ahead of a real EXIT: await_failed \"malformed\", no terminal search",
         %{run_dir: d, opts: o} do
      {running, identity, _} = running!(d, gated(d), @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, line(running.port, @bogus))
      permit!(d)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert wait_until(fn -> queued_record?(running.port, "EXIT ") end, 5_000)
      assert {:done, {:error, %{clause: "await_failed", record: "malformed"}}} = execution().settle_await(waiting)
      assert :finalized == memo(running.port, :descriptor)
      assert queued_record?(running.port, "EXIT "), "the later EXIT was not searched for"
    end

    test "SA-4 FIRST record exit_status ahead: await_failed \"exited 7\"", %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, {running.port, {:exit_status, 7}})
      assert {:done, {:error, %{clause: "await_failed", record: "exited 7"}}} = execution().settle_await(waiting)
      _ = Execution.expire(running)
    end

    test "SA-5 empty scan: expire through the owner channel; memo == the answer; finalized; native settlement; NOT backstop",
         %{run_dir: d, opts: o} do
      {running, identity, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert {:done, {:timeout, %{kind: "timeout"} = termination}} = execution().settle_await(waiting)
      refute Map.has_key?(termination, :backstop)
      assert memo(running.port, :termination) == termination
      assert :finalized == memo(running.port, :descriptor)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "SA-6 expire error on the wake path: GUARDIAN killed, Port closed, witnessed empty queue: guardian_gone, NO memo",
         %{run_dir: d, opts: o} do
      {running, identity, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      {_, 0} = System.cmd("kill", ["-9", Integer.to_string(identity.guardian)])
      assert wait_until(fn -> Port.info(running.port) == nil end, 5_000)
      _ = drain_status(running.port)
      refute queued_record?(running.port, "EXIT ")
      refute queued_record?(running.port, "DEAD ")
      assert {:done, {:error, %{clause: "guardian_gone"}}} = execution().settle_await(waiting)
      assert nil == memo(running.port, :termination)
      assert :finalized == memo(running.port, :descriptor)
      # the orphaned group is reaped by the harness; prove it is still ours to reap
      assert signal_zero("-" <> Integer.to_string(identity.pgid)) == :alive
    end

    test "SA-6c control: killing the command LEADER makes the guardian report a signaled EXIT (existing await/2)",
         %{run_dir: d, opts: o} do
      {running, identity, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      {_, 0} = System.cmd("kill", ["-9", Integer.to_string(identity.worker)])
      assert {:exit, %{"kind" => "signaled"}} = Execution.await(running, o)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "SA-7 expired-memo reuse: settle_await on the finalized descriptor raises; begin_await answers :done from the memo",
         %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert {:done, {:timeout, _}} = execution().settle_await(waiting)
      assert_raise ArgumentError, fn -> execution().settle_await(waiting) end
      assert {:done, {:timeout, _}} = Execution.begin_await(running, o)
      assert_raise ArgumentError, fn -> execution().settle_await(waiting) end
    end

    test "SA-7b fresh activation: a new begin is a NEW ref; the unchanged old descriptor and an unknown ref are refused; the active duplicate is reused",
         %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, w1} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, line(running.port, @bogus))
      assert {:done, {:error, %{clause: "await_failed"}}} = execution().settle_await(w1)
      assert :finalized == memo(running.port, :descriptor)
      assert {:pending, w2} = Execution.begin_await(running, o)
      assert w2.ref != w1.ref
      assert_raise ArgumentError, fn -> execution().settle_await(w1) end
      # an unknown activation ref is refused; a value equal to the active descriptor IS the active descriptor
      assert_raise ArgumentError, fn -> execution().settle_await(%{w1 | ref: make_ref()}) end
      assert %{w1 | ref: w2.ref} == w2
      assert {:pending, ^w2} = Execution.begin_await(running, o)
      assert {:done, {:timeout, _}} = execution().settle_await(w2)
    end

    test "SA-9 FIRST record valid DEAD (staged, synthetic): timeout via the DEAD memo; memo == answer", %{
      run_dir: d,
      opts: o
    } do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, line(running.port, @dead_ok))
      assert {:done, {:timeout, %{kind: "timeout"} = t}} = execution().settle_await(waiting)
      assert memo(running.port, :termination) == t
      _ = Execution.expire(running)
    end

    test "SA-10 FIRST record known head with a missing field: \"malformed EXIT\"", %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, line(running.port, @exit_missing_field))
      assert {:done, {:error, %{clause: "await_failed", record: "malformed EXIT"}}} = execution().settle_await(waiting)
      _ = Execution.expire(running)
    end

    test "SA-11 FIRST record a valid non-terminal head (RELEASED): \"RELEASED\"", %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, line(running.port, "RELEASED"))
      assert {:done, {:error, %{clause: "await_failed", record: "RELEASED"}}} = execution().settle_await(waiting)
      _ = Execution.expire(running)
    end

    test "SA-12 FIRST record overlong (noeol): \"malformed\"", %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      assert :staged = Execution.stage(running, {running.port, {:data, {:noeol, "x"}}})
      assert {:done, {:error, %{clause: "await_failed", record: "malformed"}}} = execution().settle_await(waiting)
      _ = Execution.expire(running)
    end

    test "SA-13 overflow: four staged records then the sentinel: the FIRST record answers, the sentinel is not searched for",
         %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      for _ <- 1..4, do: assert(:staged = Execution.stage(running, line(running.port, @bogus)))
      assert :overflow = Execution.stage(running, line(running.port, @exit_ok))
      assert {:done, {:error, %{clause: "await_failed", record: "malformed"}}} = execution().settle_await(waiting)
      assert length(Process.get({Execution, running.port, :staged})) == 4
      _ = Execution.expire(running)
    end

    test "SA-14 evidence error on the exit path RETURNS the same closed error await/2 returns today (differential); finalized",
         %{run_dir: d, opts: o} do
      # control gate: the existing await/2 on a real EXIT whose stdout evidence file was removed
      {c_running, c_identity, c_started} = running!(d, gated(d), @far, o)
      permit!(d)
      assert wait_until(fn -> dead?(c_identity) end, 10_000)
      File.rm!(Path.expand(c_started["stdout_path"], d))
      assert {:error, %{clause: clause}} = Execution.await(c_running, o)
      # the primitive on an identical setup in a sibling directory
      d2 = Path.join(d, "b")
      File.mkdir_p!(d2)
      {running, identity, started} = running!(d2, gated(d2), @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      permit!(d2)
      assert wait_until(fn -> dead?(identity) end, 10_000)
      assert wait_until(fn -> queued_record?(running.port, "EXIT ") end, 5_000)
      File.rm!(Path.expand(started["stdout_path"], d2))
      assert {:done, {:error, %{clause: ^clause}}} = execution().settle_await(waiting)
      assert :finalized == memo(running.port, :descriptor)
    end

    test "SA-8 not the owner: a monitored probe is refused, joined, and mutates NOTHING in the owner", %{
      run_dir: d,
      opts: o
    } do
      {running, identity, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      assert {:pending, waiting} = Execution.begin_await(running, o)
      test = self()

      {probe, mref} =
        spawn_monitor(fn ->
          send(test, {:other, try(do: execution().settle_await(waiting), rescue: (e -> {:raised, e.__struct__}))})
        end)

      assert_receive {:other, {:raised, ArgumentError}}, 5_000
      assert_receive {:DOWN, ^mref, :process, ^probe, :normal}, 2_000
      assert memo(running.port, :descriptor) == waiting
      assert nil == memo(running.port, :termination)
      assert signal_zero(Integer.to_string(identity.guardian)) == :alive
      _ = Execution.expire(running)
    end
  end

  # ================= AW: the REAL Worker with the REAL native executor =================

  defp seams(ctx, executor, gate_extra, extra) do
    [
      gate_executor: executor,
      gate_helper: ctx.helper,
      gate_opts: [settle_ms: 200, rounds: 2, clock: StepClock, barrier: ready_barrier(self())] ++ gate_extra,
      run_id: @run_id,
      supervisor_instance: "sup_async",
      clock: StepClock,
      fs: {SystemFs, nil},
      run_dir: ctx.run_dir
    ] ++ extra
  end

  # register-then-ACK (AR-M8): the executing owner is HELD at READY until the test has registered the identity
  defp ready_barrier(parent, ack_ms \\ 30_000) do
    fn
      :after_ready, identity ->
        ref = make_ref()
        send(parent, {:ready, identity, self(), ref})

        receive do
          {:ready_ack, ^ref} -> true
        after
          ack_ms -> exit({:ready_not_acknowledged, ref})
        end

      _name, _info ->
        true
    end
  end

  defp ready_ack! do
    assert_receive {:ready, identity, owner, ref}, 30_000
    Tracked.put(self(), identity)
    send(owner, {:ready_ack, ref})
    identity
  end

  defp worker(seams) do
    pid = start_supervised!(Supervisor.child_spec({Worker, self()}, restart: :temporary))
    cap = make_ref()
    send(pid, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    {pid, cap}
  end

  defp prepare_effect(run_dir, argv, deadline) do
    %Effect.PrepareGate{
      gate_run_id: @gate,
      attempt: 1,
      requested: %{"command_argv" => argv},
      deadline_unix: deadline,
      repo_root: run_dir,
      run_dir: run_dir
    }
  end

  # prepare (identity registered before the ack releases READY) + durable start + release
  defp released!(pid, cap, run_dir, argv, deadline) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, prepare_effect(run_dir, argv, deadline), nil})
    identity = ready_ack!()
    assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GatePrepared{started: started}}, 20_000
    persisted = persist!(run_dir, started)
    release = %Effect.ReleaseGate{gate_run_id: @gate, attempt: 1, started_seq: 1}
    rref = make_ref()
    send(pid, {:execute, cap, 1, rref, release, persisted})
    assert_receive {:effect_result, ^cap, 1, ^rref, ^pid, %Observation.GateReleased{}}, 20_000
    [port] = pid |> Process.info(:links) |> elem(1) |> Enum.filter(&is_port/1)
    {identity, port}
  end

  defp await_gate(deadline), do: %Effect.AwaitGate{gate_run_id: @gate, attempt: 1, deadline_unix: deadline}
  defp loop!(pid), do: :sys.get_state(pid, @loop)
  defp dictionary(pid), do: pid |> Process.info(:dictionary) |> elem(1)
  defp worker_memo(pid, port, key), do: List.keyfind(dictionary(pid), {Execution, port, key}, 0)

  # the pending wait is entered: the loop serves a bounded call (today the call times out: the Worker blocks)
  defp pending!(pid, cap, deadline) do
    ref = make_ref()
    send(pid, {:execute, cap, 1, ref, await_gate(deadline), nil})
    state = loop!(pid)
    assert {:ok, %{key: {@gate, 1}}} = effects().pending(state.runtime)
    ref
  end

  # the failing primitive is reached through the pending wait: resume by a record, settle_await by a wake
  defp trigger_failure(:begin_mode, _pid, _cap, _ref, _port), do: :ok

  defp trigger_failure(:resume_mode, pid, _cap, _ref, port) do
    loop!(pid)
    send(pid, line(port, @exit_ok))
  end

  defp trigger_failure(:settle_mode, pid, cap, ref, _port) do
    loop!(pid)
    send(pid, {:gate_deadline, cap, 1, ref})
  end

  # ---- AW-R helpers ----
  defmodule DownstreamHandler do
    @moduledoc false
    alias AiOrchestrator.Run.GateAsyncAwaitRedTest, as: Labels

    def log(%{meta: %{pid: pid}} = event, %{config: %{test: test, pid: pid, run_dir: run_dir}}) do
      label = Labels.event_label(event)
      send(test, {:downstream_event, label, inspect(event, limit: :infinity) =~ run_dir, self()})
    end

    def log(_event, _config), do: :ok
  end

  @doc false
  def event_label(%{msg: {:report, %{label: label}}}), do: label
  def event_label(%{msg: {:string, _}}), do: :string
  def event_label(%{msg: {fmt, _}}) when is_binary(fmt) or is_list(fmt), do: :format
  def event_label(_), do: :other

  # ---- AR-M16 AW-R3 surfaces (m_1788841674810; U2 m_1788878711715) ----
  # Three surfaces are kept DISTINCT: (1) RAW = the event as logged, observed by a primary filter placed FIRST in
  # the chain (before :logger_translator), in the LOGGING process; (2) DOWNSTREAM = what a handler receives after
  # the primary filters (post-translation); (3) RENDERED = the default handler's formatted output (capture_log).
  # The crash event is CORRELATED, never positional: label {:proc_lib, :crash} whose crash report names THIS pid and
  # THIS exit reason. A raw event is recorded as {:raw_event, label, carries_run_dir?, level, domain, correlation}.
  @aw_r3_chain [:logger_translator, :logger_process_level]

  # the concrete logging configuration these facts are measured under (pinned, not disclosed as "any of")
  defp aw_r3_config! do
    %{level: level, filters: filters} = :logger.get_primary_config()
    assert level == :warning
    assert Enum.map(filters, &elem(&1, 0)) == @aw_r3_chain
    assert {_fun, %{otp: true, sasl: false}} = Keyword.fetch!(filters, :logger_translator)
    assert :default in :logger.get_handler_ids()
    assert Application.get_env(:logger, :handle_otp_reports) == true
    assert Application.get_env(:logger, :handle_sasl_reports) == false
    filters
  end

  # the raw filter installed at the HEAD of the primary chain; the exact prior chain is restored by the caller
  # the caller pins the prior chain FIRST and registers its restoration BEFORE this first global mutation
  defp aw_r3_prior!, do: aw_r3_config!()

  defp aw_r3_install!(prior, pid, run_dir, test) do
    raw_filter = fn event, _ ->
      if match?(%{meta: %{pid: ^pid}}, event) do
        carries = inspect(event, limit: :infinity, printable_limit: :infinity) =~ run_dir

        send(
          test,
          {:raw_event, event_label(event), carries, event.level, event.meta[:domain], aw_r3_correlation(event), self()}
        )
      end

      :ignore
    end

    :ok = :logger.set_primary_config(:filters, [{:aw_r3_raw, {raw_filter, nil}} | prior])
    :ok = :logger.add_handler(:aw_r3_down, DownstreamHandler, %{config: %{test: test, pid: pid, run_dir: run_dir}})
    :ok
  end

  defp aw_r3_restore!(prior) do
    :ok = :logger.set_primary_config(:filters, prior)
    _ = :logger.remove_handler(:aw_r3_down)
    :ok
  end

  # what identifies the event: the crash report's own pid and error_info, or the terminate report's reason
  defp aw_r3_correlation(%{msg: {:report, %{label: {:proc_lib, :crash}, report: [crashed | _]}}}) when is_list(crashed) do
    case Keyword.get(crashed, :error_info) do
      {_kind, reason, _stack} -> %{pid: Keyword.get(crashed, :pid), reason: reason}
      _other -> %{pid: Keyword.get(crashed, :pid), reason: :unknown}
    end
  end

  # the gen_server terminate report is a FLAT map (label, name, reason, log, state, last_message, ...)
  defp aw_r3_correlation(%{msg: {:report, %{label: {:gen_server, :terminate}, reason: reason}}, meta: %{pid: pid}}),
    do: %{pid: pid, reason: reason}

  defp aw_r3_correlation(_event), do: nil

  # the correlated crash event: exact label, this pid, this reason (a first-same-pid pick is NOT this)
  defp aw_r3_crash_event(raw_events, pid, reason) do
    case Enum.filter(raw_events, &aw_r3_crash_event?(&1, pid, reason)) do
      [event] -> {:ok, event}
      [] -> {:error, {:crash_event_missing, Enum.map(raw_events, &elem(&1, 1))}}
      many -> {:error, {:crash_event_ambiguous, length(many)}}
    end
  end

  defp aw_r3_crash_event?(
         {:raw_event, {:proc_lib, :crash}, _carries, :error, [:otp, :sasl], %{pid: pid, reason: reason}},
         pid,
         reason
       ), do: true

  defp aw_r3_crash_event?(_event, _pid, _reason), do: false

  # collection-completion boundary: primary filters and our handler's log/2 run IN THE LOGGING PROCESS (the Worker),
  # so every {:raw_event, ...} / {:downstream_event, ...} the Worker sent us precedes its DOWN (per-pair signal order);
  # the rendered output is complete when capture_log returns (its handler is removed and flushed). No sleeps.
  defp aw_r3_crash!(pid, reason) do
    mref = Process.monitor(pid)

    rendered =
      ExUnit.CaptureLog.capture_log(fn ->
        :sys.terminate(pid, reason)
        assert_receive {:DOWN, ^mref, :process, ^pid, ^reason}, 5_000
      end)

    raw = collect({:raw_event, :_, :_, :_, :_, :_, :_})
    downstream = collect({:downstream_event, :_, :_, :_})
    # the sender of every raw/downstream witness is the crashing Worker itself (inline primary filters / handler log/2)
    assert Enum.all?(raw, &match?({:raw_event, _, _, _, _, _, ^pid}, &1)), "raw witnesses sent by the Worker"
    assert Enum.all?(downstream, &match?({:downstream_event, _, _, ^pid}, &1)), "downstream witnesses sent by the Worker"
    %{raw: strip_sender(raw), downstream: Enum.map(downstream, &Tuple.delete_at(&1, 3)), rendered: rendered}
  end

  defp strip_sender(raw), do: Enum.map(raw, &Tuple.delete_at(&1, 6))

  # the AW-R3 oracle on the three surfaces; `resident?` = run_dir bytes in the Worker's dictionary BEFORE the crash
  defp aw_r3_surfaces!(%{raw: raw, downstream: downstream, rendered: rendered}, pid, reason, run_dir, resident?) do
    assert {:ok, {:raw_event, {:proc_lib, :crash}, carries, :error, [:otp, :sasl], _}} =
             aw_r3_crash_event(raw, pid, reason)

    # residency <-> carry: the raw crash report carries run_dir bytes exactly when the dictionary held them
    assert carries == resident?, "raw crash report carries #{carries}, dictionary residency #{resident?}"
    # the terminate report (format_status-governed) never carries
    assert [{:raw_event, {:gen_server, :terminate}, false, :error, [:otp], %{pid: ^pid, reason: ^reason}}] =
             Enum.filter(raw, &match?({:raw_event, {:gen_server, :terminate}, _, _, _, _}, &1))

    # downstream (post primary filters): the SASL-domain crash report is STOPPED by the translator (sasl: false), the
    # terminate report arrives, and nothing downstream carries run_dir
    labels = Enum.map(downstream, fn {:downstream_event, label, _} -> label end)
    assert {:gen_server, :terminate} in labels, "downstream labels: #{inspect(labels)}"
    refute {:proc_lib, :crash} in labels, "the SASL crash report is stopped before any handler"
    refute Enum.any?(downstream, fn {:downstream_event, _label, carries} -> carries end)
    # rendered: the terminate report for THIS pid with its redacted last message, no run_dir bytes
    assert rendered =~ "#{inspect(pid)} terminating" and rendered =~ inspect(reason)
    assert rendered =~ "Last message: :redacted"
    refute rendered =~ run_dir
    :ok
  end

  defp aw_r3_resident?(pid, run_dir),
    do: inspect(Process.info(pid, :dictionary), limit: :infinity, printable_limit: :infinity) =~ run_dir

  # drain every queued message matching the shape (order preserved)
  defp collect(shape) do
    {:messages, msgs} = Process.info(self(), :messages)
    matching = Enum.filter(msgs, &shape_match?(&1, shape))
    for m <- matching, do: receive(do: (^m -> m))
    matching
  end

  defp shape_match?(m, shape) when is_tuple(m) and is_tuple(shape) and tuple_size(m) == tuple_size(shape),
    do: Enum.all?(Enum.zip(Tuple.to_list(m), Tuple.to_list(shape)), fn {a, b} -> b == :_ or a == b end)

  defp shape_match?(_, _), do: false

  defp assert_status_fields_redacted!(pid, run_dir) do
    {:status, ^pid, {:module, :gen_server}, [_pdict, _sys, _parent, _dbg, misc]} = :sys.get_status(pid, @loop)
    fields = for {:data, kv} <- misc, {k, v} <- kv, do: {to_string(k), v}
    assert {"State", :redacted} in fields
    for {k, v} <- fields, do: refute(inspect(v, limit: :infinity) =~ run_dir, "field #{k}")
  end

  defp closed_diagnostic!(closed, kind) do
    assert %{kind: ^kind, class: class, digest: "sha256:" <> _, cleanup: %{settled: s, attempts: a, unproven: u}} = closed
    assert is_binary(class) and is_integer(s) and is_integer(a) and is_integer(u)
  end

  describe "AW Worker pending wait (RED unless titled control)" do
    test "AW-2 pending then a real EXIT resumes: one answer; the entry is RETAINED :running until release_terminal drops it",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @far)
      ref = pending!(pid, cap, @far)
      permit!(ctx.run_dir)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      %{runtime: runtime} = loop!(pid)
      assert :none == effects().pending(runtime)
      assert %{phase: :running} = Map.fetch!(runtime.gates, {@gate, 1})
      rel = make_ref()
      send(pid, {:release_terminal, cap, 1, rel, @gate_passed})
      assert_receive {:released, ^cap, 1, ^rel, ^pid}, 5_000
      assert %Runtime{gates: gates} = loop!(pid).runtime
      assert gates == %{}
    end

    test "AW-11 GB-6 INVERTED (native, complete capability): a correlated settle probe is answered with the EXACT cleanup while pending",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {identity, _port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      ref = pending!(pid, cap, @far)
      settle = make_ref()
      send(pid, {:settle, cap, 1, settle})

      assert_receive {:settled, ^cap, 1, ^settle, ^pid,
                      [%{"gate_run_id" => @gate, "attempt" => 1, "settle" => %{"settled" => true, "proof" => "gone"}}]},
                     @probe_ms

      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      assert :none == effects().pending(loop!(pid).runtime)
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "AW-3w Worker actuation on {:gate_deadline, cap, gen, ref} (injected; Server rows prove the send): GateFailed timeout, memo witnessed, no backstop",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      ref = pending!(pid, cap, @far)
      send(pid, {:gate_deadline, cap, 1, ref})
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFailed{} = observation}, 20_000
      assert inspect(observation) =~ "timeout"
      assert {{Execution, ^port, :termination}, %{kind: "timeout"} = termination} = worker_memo(pid, port, :termination)
      refute Map.has_key?(termination, :backstop)
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      assert wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "AW-4f forced order: the wake is dequeued BEFORE a record already queued behind it: settle_await answers the EXIT",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      ref = pending!(pid, cap, @far)
      :ok = :sys.suspend(pid)
      send(pid, {:gate_deadline, cap, 1, ref})
      send(pid, line(port, @exit_ok))
      :ok = :sys.resume(pid)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
      assert nil == worker_memo(pid, port, :termination), "no expire ran: the queued record won"
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
    end

    test "AW-5 stale wakes (foreign cap, foreign gen, unknown ref) and a late wake after completion are facts only",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @far)
      ref = pending!(pid, cap, @far)
      send(pid, {:gate_deadline, make_ref(), 1, ref})
      send(pid, {:gate_deadline, cap, 2, ref})
      send(pid, {:gate_deadline, cap, 1, make_ref()})
      loop!(pid)
      refute_received {:effect_result, ^cap, 1, ^ref, ^pid, _}
      assert {:ok, _} = effects().pending(loop!(pid).runtime)
      permit!(ctx.run_dir)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
      send(pid, {:gate_deadline, cap, 1, ref})
      loop!(pid)
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
      refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, @probe_ms
    end

    test "AW-D9 every execute class while pending is DROPPED (same ref / other ref / foreign cap / foreign gen): zero adapter starts, zero replies; original once; no revival",
         ctx do
      test = self()
      extra = [dispatch: CountingDispatch, dispatch_opts: [witness: test]]
      {pid, cap} = worker(seams(ctx, ModeExecutor, [witness: test], extra))
      {_identity, port} = released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @far)
      assert_receive {:prepare_entered, @gate}, 1_000
      ref = pending!(pid, cap, @far)
      other = make_ref()
      observe = %Effect.Observe{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}, deadline_unix: @far}

      dispatch = %Effect.Dispatch{
        assignment_id: "as_0001",
        command: %{"assignment_id" => "as_0001"},
        message_id: "m1",
        deadline_unix: @far
      }

      reconcile = %Effect.ReconcileSend{
        assignment_id: "as_0001",
        command: %{"assignment_id" => "as_0001"},
        deadline_unix: @far
      }

      never_dir = Path.join(ctx.run_dir, "never")
      File.mkdir_p!(never_dir)
      prepare = prepare_effect(never_dir, gated(never_dir), @far)

      for intent <- [await_gate(@far), prepare, observe, dispatch, reconcile] do
        send(pid, {:execute, cap, 1, ref, intent, nil})
        send(pid, {:execute, cap, 1, other, intent, nil})
        send(pid, {:execute, make_ref(), 1, other, intent, nil})
        send(pid, {:execute, cap, 2, other, intent, nil})
      end

      loop!(pid)
      # every side-effect and reply channel is silent at the loop barrier
      refute_received {:adapter_started, _}
      refute_received {:prepare_entered, _}
      refute_received {:abandon_entered, _}
      refute_received {:effect_result, _, _, ^other, ^pid, _}
      refute_received {:effect_failed, _, _, ^other, ^pid, _}
      refute_received {:effect_result, ^cap, 1, ^ref, ^pid, _}
      refute_received {:effect_failed, ^cap, 1, ^ref, ^pid, _}
      refute_received {:settled, _, _, _, ^pid, _}
      refute_received {:released, _, _, _, ^pid}
      assert {:ok, %{key: {@gate, 1}, port: ^port}} = effects().pending(loop!(pid).runtime)
      permit!(ctx.run_dir)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
      refute_receive {:effect_result, _, _, _, ^pid, _}, @probe_ms
      refute_receive {:effect_failed, _, _, _, ^pid, _}, @probe_ms
      refute_received {:adapter_started, _}
      refute_received {:prepare_entered, _}
      refute_received {:abandon_entered, _}
      # positive control: the same counters DO fire for a non-dropped execute after completion
      pos_dir = Path.join(ctx.run_dir, "positive")
      File.mkdir_p!(pos_dir)
      positive = make_ref()
      prepare_pos = %{prepare_effect(pos_dir, gated(pos_dir), @far) | gate_run_id: "gr_0002"}
      send(pid, {:execute, cap, 1, positive, prepare_pos, nil})
      _ = ready_ack!()
      assert_receive {:prepare_entered, "gr_0002"}, 20_000
      assert_receive {:effect_result, ^cap, 1, ^positive, ^pid, %Observation.GatePrepared{}}, 20_000
      # no revival: stale metadata accepts neither a record for the old Port nor a wake for the old ref
      send(pid, line(port, @exit_ok))
      send(pid, {:gate_deadline, cap, 1, ref})
      loop!(pid)
      assert :none == effects().pending(loop!(pid).runtime)
      refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
    end

    for stage <- [:begin_mode, :resume_mode, :settle_mode],
        {mode, kind} <- [invalid: :error, bad_answer: :error, raise: :error, throw: :throw, exit: :exit] do
      test "AW-M3 #{stage} #{mode}: one closed effect_failed (kind #{kind}), abandon once, latest runtime settled, pending :none",
           ctx do
        stage = unquote(stage)
        mode = unquote(mode)
        test = self()
        {pid, cap} = worker(seams(ctx, ModeExecutor, [{stage, mode}, {:witness, test}], []))
        {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
        assert_receive {:prepare_entered, @gate}, 1_000
        ref = make_ref()
        send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
        trigger_failure(stage, pid, cap, ref, port)
        assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, closed}, 20_000
        closed_diagnostic!(closed, unquote(kind))
        # the closed failure settles the LATEST runtime exactly once: one abandon entry, the entry gone after
        assert_receive {:abandon_entered, ^pid}, 2_000
        refute_receive {:abandon_entered, ^pid}, @probe_ms
        refute_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _}, @probe_ms
        refute_receive {:effect_result, ^cap, 1, ^ref, ^pid, _}, @probe_ms
        %{runtime: runtime} = loop!(pid)
        assert :none == effects().pending(runtime)
        assert runtime.gates == %{}
      end
    end

    # ---- AR-M15c latest-runtime distinction (ruling m_1788841674810 AR-M15; assignment m_1788876080770 U1) ----
    # The witness is INDEPENDENT of the abandon count: a call trace on the Worker's own `Effects.settle/1` invocation
    # (test-only :erlang.trace on that pid; the global pattern is reset in on_exit) delivers the EXACT runtime the
    # Worker settles, i.e. the runtime the Interrupted carrier holds on the failure path. The pure oracle then asks
    # whether that runtime is the RETURNED :running one (the entry back on its retained running handle, waiting and
    # effect removed) or the OLD :awaiting one (phase :awaiting, or a waiting/effect carrier still present). AW-M3's
    # "abandon once, pending :none" cannot tell them apart because the same handle is abandoned either way.
    defp lr_trace_settle!(pid) do
      :erlang.trace_pattern({Effects, :settle, 1}, true, [])
      on_exit(fn -> :erlang.trace_pattern({Effects, :settle, 1}, false, []) end)
      1 = :erlang.trace(pid, true, [:call])
      :ok
    end

    # the runtime the Worker actually settled (its own call), consumed once
    defp lr_settled_runtime!(pid) do
      assert_receive {:trace, ^pid, :call, {Effects, :settle, [runtime]}}, 20_000
      runtime
    end

    # pure verdict: :ok only for the RETURNED :running runtime whose entry for `key` sits on a retained running handle
    defp lr_latest_verdict(%Runtime{gates: gates}, key) do
      case Map.get(gates, key) do
        nil -> {:error, {:entry_missing, key, Map.keys(gates)}}
        %{phase: :awaiting} = entry -> {:error, {:stale_awaiting_runtime, key, lr_shape(entry)}}
        %{waiting: _} = entry -> {:error, {:stale_awaiting_carrier, key, lr_shape(entry)}}
        %{effect: _} = entry -> {:error, {:stale_awaiting_carrier, key, lr_shape(entry)}}
        %{phase: :running, handle: %{port: port}} = entry when map_size(entry) == 2 and is_port(port) -> :ok
        entry -> {:error, {:not_a_running_entry, key, lr_shape(entry)}}
      end
    end

    defp lr_latest_verdict(other, key), do: {:error, {:runtime_malformed, key, other}}

    # payload-free rendering for messages: phase and key names only
    defp lr_shape(entry) when is_map(entry),
      do: %{phase: Map.get(entry, :phase), keys: entry |> Map.keys() |> Enum.sort()}

    # the PRESTATE pin (R2 of m_1788877342034): before the resume/settle failure is injected, the loop state must
    # carry the correlated pending op (this op's ref, the key, the released port) and the entry for the key must be
    # :awaiting on the retained port (waiting/effect carriers allowed there, per the Effects grammar). Without this
    # the post-state oracle would also accept a runtime that never left :running (LR-C1's own path).
    defp lr_awaiting_verdict(%{runtime: %Runtime{gates: gates}} = state, key, ref, port) do
      pending = Map.get(state, :pending)

      cond do
        pending == nil ->
          {:error, {:pending_missing, key}}

        pending != %{ref: ref, key: key, port: port} ->
          {:error, {:pending_mismatch, key, lr_pending_shape(pending, ref, port)}}

        true ->
          case Map.get(gates, key) do
            nil -> {:error, {:entry_missing, key, Map.keys(gates)}}
            %{phase: :awaiting, handle: %{port: ^port}} -> :ok
            %{phase: :awaiting} = entry -> {:error, {:awaiting_on_wrong_port, key, lr_shape(entry)}}
            entry -> {:error, {:not_awaiting, key, lr_shape(entry)}}
          end
      end
    end

    defp lr_awaiting_verdict(other, key, _ref, _port), do: {:error, {:state_malformed, key, lr_state_keys(other)}}

    defp lr_state_keys(state) when is_map(state), do: state |> Map.keys() |> Enum.sort()
    defp lr_state_keys(_other), do: :not_a_map

    # payload-free: which of ref/key/port disagree
    defp lr_pending_shape(%{ref: r, key: k, port: p}, ref, port),
      do: %{ref_matches: r == ref, port_matches: p == port, key: k}

    defp lr_pending_shape(other, _ref, _port), do: %{malformed: lr_state_keys(other)}

    defp lr_prestate!(state, key, ref, port) do
      verdict = lr_awaiting_verdict(state, key, ref, port)
      assert :ok == verdict, "prestate verdict: #{inspect(verdict)}"
      state
    end

    defp lr_latest!(pid, key, port) do
      runtime = lr_settled_runtime!(pid)
      verdict = lr_latest_verdict(runtime, key)
      assert :ok == verdict, "latest-runtime verdict: #{inspect(verdict)}"

      assert match?(%{handle: %{port: ^port}}, runtime.gates[key]),
             "the retained running handle (its port) is the one settled: #{inspect(lr_shape(runtime.gates[key]))}"

      runtime
    end

    test "LR-C1 control (passes today): the sync execute/3 failure settles the Interrupted runtime: retained :running entry on its port",
         ctx do
      test = self()
      {pid, cap} = worker(seams(ctx, LrRaisingExecutor, [{:witness, test}], []))
      {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      lr_trace_settle!(pid)
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
      assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, closed}, 20_000
      closed_diagnostic!(closed, :error)
      runtime = lr_latest!(pid, {@gate, 1}, port)
      assert map_size(runtime.gates) == 1
      assert_receive {:abandon_entered, ^pid}, 2_000
      refute_receive {:abandon_entered, ^pid}, @probe_ms
      # settled exactly once: the loop's runtime is empty afterwards (the RED pending/1 accessor is not needed here)
      assert loop!(pid).runtime.gates == %{}
      refute_receive {:trace, ^pid, :call, {Effects, :settle, _}}, @probe_ms
    end

    test "LR-C2 negatives (pass today): oracle refuses :awaiting/carrier shapes; settle witness fails on a stale mutation of the real runtime",
         ctx do
      test = self()
      {pid, cap} = worker(seams(ctx, LrRaisingExecutor, [{:witness, test}], []))
      {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      lr_trace_settle!(pid)
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
      assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, _closed}, 20_000
      key = {@gate, 1}
      real = lr_settled_runtime!(pid)
      assert :ok == lr_latest_verdict(real, key)
      %{handle: handle} = real.gates[key]
      # (p) the PRESTATE helper on states derived from a REAL loop state (mutated in the test, never in production):
      # the unchanged :running state with no pending op is refused, as are a mismatched pending ref/port and an
      # awaiting entry on a wrong port; a constructed awaiting prestate with the correlated pending op is accepted
      idle = %{runtime: %{real | gates: %{key => %{phase: :running, handle: handle}}}}
      assert {:error, {:pending_missing, ^key}} = lr_awaiting_verdict(idle, key, ref, port)
      with_pending = Map.put(idle, :pending, %{ref: ref, key: key, port: port})
      assert {:error, {:not_awaiting, ^key, _}} = lr_awaiting_verdict(with_pending, key, ref, port)

      awaiting_state =
        %{
          with_pending
          | runtime: %{
              real
              | gates: %{key => %{phase: :awaiting, handle: handle, waiting: %{running: handle}, effect: nil}}
            }
        }

      assert :ok == lr_awaiting_verdict(awaiting_state, key, ref, port)
      assert ^awaiting_state = lr_prestate!(awaiting_state, key, ref, port)

      assert {:error, {:pending_mismatch, ^key, %{ref_matches: false}}} =
               lr_awaiting_verdict(awaiting_state, key, make_ref(), port)

      wrong_port = Port.open({:spawn, "/bin/sleep 0"}, [])

      assert {:error, {:pending_mismatch, ^key, %{port_matches: false}}} =
               lr_awaiting_verdict(awaiting_state, key, ref, wrong_port)

      other_pending = %{awaiting_state | pending: %{ref: ref, key: key, port: wrong_port}}
      assert {:error, {:pending_mismatch, ^key, _}} = lr_awaiting_verdict(other_pending, key, ref, port)

      wrong_entry_port =
        %{
          awaiting_state
          | runtime: %{real | gates: %{key => %{phase: :awaiting, handle: %{handle | port: wrong_port}}}}
        }

      assert {:error, {:awaiting_on_wrong_port, ^key, _}} = lr_awaiting_verdict(wrong_entry_port, key, ref, port)

      assert {:error, {:entry_missing, ^key, []}} =
               lr_awaiting_verdict(%{with_pending | runtime: %{real | gates: %{}}}, key, ref, port)

      assert {:error, {:state_malformed, ^key, _}} = lr_awaiting_verdict(%{pending: nil}, key, ref, port)

      for bad <- [idle, with_pending, other_pending] do
        error = assert_raise(ExUnit.AssertionError, fn -> lr_prestate!(bad, key, ref, port) end)
        assert error.message =~ "prestate verdict"
      end

      # (a) pure: stale shapes derived from the REAL settled runtime (mutated in the test, never in production)
      awaiting =
        %{real | gates: %{key => %{phase: :awaiting, handle: handle, waiting: %{running: handle}, effect: nil}}}

      assert {:error, {:stale_awaiting_runtime, ^key, _}} = lr_latest_verdict(awaiting, key)
      carrier = %{real | gates: %{key => %{phase: :running, handle: handle, waiting: %{running: handle}}}}
      assert {:error, {:stale_awaiting_carrier, ^key, _}} = lr_latest_verdict(carrier, key)
      effect_left = %{real | gates: %{key => %{phase: :running, handle: handle, effect: await_gate(@far)}}}
      assert {:error, {:stale_awaiting_carrier, ^key, _}} = lr_latest_verdict(effect_left, key)
      assert {:error, {:entry_missing, ^key, []}} = lr_latest_verdict(%{real | gates: %{}}, key)

      assert {:error, {:not_a_running_entry, ^key, _}} =
               lr_latest_verdict(%{real | gates: %{key => %{phase: :prepared, handle: handle}}}, key)

      assert {:error, {:not_a_running_entry, ^key, _}} =
               lr_latest_verdict(%{real | gates: %{key => %{phase: :running, handle: nil}}}, key)

      assert {:error, {:runtime_malformed, ^key, _}} = lr_latest_verdict(%{gates: real.gates}, key)

      # (b) the settle witness through the SAME assertion helper: a stale-runtime mutation requeued as the traced
      # call FAILS
      for stale <- [awaiting, carrier] do
        send(self(), {:trace, pid, :call, {Effects, :settle, [stale]}})
        error = assert_raise(ExUnit.AssertionError, fn -> lr_latest!(pid, key, port) end)
        assert error.message =~ "latest-runtime verdict"
      end

      # a wrong port on an otherwise returned runtime also fails (the handle must be the retained one)
      send(self(), {:trace, pid, :call, {Effects, :settle, [real]}})
      other_port = Port.open({:spawn, "/bin/sleep 0"}, [])
      error = assert_raise(ExUnit.AssertionError, fn -> lr_latest!(pid, key, other_port) end)
      assert error.message =~ "retained running handle"
      # and the unmodified real runtime passes the same helper
      send(self(), {:trace, pid, :call, {Effects, :settle, [real]}})
      assert ^real = lr_latest!(pid, key, port)
      assert_receive {:abandon_entered, ^pid}, 2_000
    end

    # LR-1 resume (a real EXIT record), LR-2 settle (a wake)
    for stage <- [:resume_mode, :settle_mode] do
      test "LR-#{if stage == :resume_mode, do: 1, else: 2} #{stage} bad_answer (AR-M15c, RED): mapping failure after the transition settles the RETURNED :running runtime",
           ctx do
        stage = unquote(stage)
        test = self()
        {pid, cap} = worker(seams(ctx, ModeExecutor, [{stage, :bad_answer}, {:witness, test}], []))
        {_identity, port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
        assert_receive {:prepare_entered, @gate}, 1_000
        lr_trace_settle!(pid)
        ref = make_ref()
        send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
        # PRESTATE (R2): this op is pending on the released port and the entry is :awaiting there BEFORE the fault
        _ = lr_prestate!(loop!(pid), {@gate, 1}, ref, port)
        trigger_failure(stage, pid, cap, ref, port)
        assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, closed}, 20_000
        closed_diagnostic!(closed, :error)
        # the distinguishing witness: the runtime the Worker settled is the RETURNED :running one on the retained port
        _ = lr_latest!(pid, {@gate, 1}, port)
        assert_receive {:abandon_entered, ^pid}, 2_000
        refute_receive {:abandon_entered, ^pid}, @probe_ms
        assert :none == effects().pending(loop!(pid).runtime)
      end
    end

    test "AW-P accessor: malformed or multiple :awaiting entries are a closed refusal, never :none", _ctx do
      base = Runtime.new([])
      two = base |> Runtime.put({"gr_a", 1}, :running, %{port: nil}) |> Runtime.put({"gr_b", 1}, :running, %{port: nil})
      two_awaiting = %{two | gates: Map.new(two.gates, fn {k, e} -> {k, Map.put(e, :phase, :awaiting)} end)}
      assert {:error, %{clause: clause}} = effects().pending(two_awaiting)
      assert is_binary(clause)
      malformed = %{base | gates: %{{"gr_c", 1} => %{phase: :awaiting}}}
      assert {:error, %{clause: clause2}} = effects().pending(malformed)
      assert is_binary(clause2)
      assert :none == effects().pending(base)
    end

    test "AW-9p control: a PARTIALLY capable executor (no settle_await) keeps the synchronous path (blocks; answers on exit)",
         ctx do
      {:module, _} = Code.ensure_loaded(PartialExecutor)
      refute function_exported?(PartialExecutor, :settle_await, 1)
      {pid, cap} = worker(seams(ctx, PartialExecutor, [], []))
      released!(pid, cap, ctx.run_dir, gated(ctx.run_dir), @far)
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
      assert catch_exit(:sys.get_state(pid, @probe_ms)), "the synchronous fallback blocks the owner, as today"
      permit!(ctx.run_dir)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^pid, %Observation.GateFinished{}}, 20_000
    end

    test "AW-9e control: AwaitGate with NO retained entry reaches the executor as today (nil handle -> closed failure)",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      ref = make_ref()
      send(pid, {:execute, cap, 1, ref, await_gate(@far), nil})
      assert_receive {:effect_failed, ^cap, 1, ^ref, ^pid, closed}, 5_000
      closed_diagnostic!(closed, :error)
      assert is_map(loop!(pid))
    end

    # AR-M7 (pending the redaction ruling, option A): format_status-governed surfaces are pinned with a LIVE handle
    # today; the raw :sys dictionary is named DISTINCTLY and measured as it is (a known pre-existing limitation)
    test "AW-R1 Worker format_status fields with an ACTIVE pending native descriptor: State :redacted, no run_dir in any field",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      _ref = pending!(pid, cap, @far)
      assert_status_fields_redacted!(pid, ctx.run_dir)
    end

    test "AW-R1c control: the same fields with a live (non-pending) handle today", ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      assert_status_fields_redacted!(pid, ctx.run_dir)
    end

    # AW-R3 (qualified A): a REAL pending native descriptor resident in the Worker, then a monitor-correlated abnormal
    # termination; the RENDERED crash report must carry no run_dir bytes; the RAW pre-filter event is recorded apart
    # (the frozen primitive already stores the full descriptor in the dictionary; this unit newly makes it resident in
    # the production Worker, which proc_lib's crash event includes independently of format_status: known limitation)
    # AR-M16: exact crash label + concrete configuration, correlated (not positional) crash event, monitor/pid and
    # residency pinned before the failure, a real completion boundary, filters/handlers restored on every exit.
    test "AW-R3 crash report with a resident pending descriptor: correlated raw crash event carries (known limitation); downstream + rendered do not",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {identity, _port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      _ref = pending!(pid, cap, @far)
      run_dir = ctx.run_dir
      # residency pinned BEFORE the failure: the pending descriptor is resident in the Worker's dictionary
      assert aw_r3_resident?(pid, run_dir), "the pending descriptor is resident before the crash"
      prior = aw_r3_prior!()
      on_exit(fn -> aw_r3_restore!(prior) end)
      aw_r3_install!(prior, pid, run_dir, self())

      try do
        surfaces = aw_r3_crash!(pid, :aw_r3_probe_failure)
        assert :ok == aw_r3_surfaces!(surfaces, pid, :aw_r3_probe_failure, run_dir, true)
      after
        aw_r3_restore!(prior)
      end

      _ = wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "AW-R3c control (passes today): same surfaces on the real sync path (no residency): exact labels, order, stopped SASL report",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {identity, _port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      run_dir = ctx.run_dir
      refute aw_r3_resident?(pid, run_dir), "no descriptor is resident on the synchronous path"
      prior = aw_r3_prior!()
      on_exit(fn -> aw_r3_restore!(prior) end)
      aw_r3_install!(prior, pid, run_dir, self())

      try do
        surfaces = aw_r3_crash!(pid, :aw_r3_control_failure)
        assert :ok == aw_r3_surfaces!(surfaces, pid, :aw_r3_control_failure, run_dir, false)
        # order of the Worker's own raw events: the terminate report precedes the crash report
        labels = for {:raw_event, label, _, _, _, _} <- surfaces.raw, do: label
        assert labels == [{:gen_server, :terminate}, {:proc_lib, :crash}], "raw labels: #{inspect(labels)}"
      after
        aw_r3_restore!(prior)
      end

      # restored: the exact prior chain, our handler gone
      assert Enum.map(:logger.get_primary_config().filters, &elem(&1, 0)) == @aw_r3_chain
      refute :aw_r3_down in :logger.get_handler_ids()
      _ = wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "AW-R3n wrong-first negative (passes today): a same-pid decoy is the FIRST raw event; the oracle selects the correlated crash event",
         ctx do
      {pid, cap} = worker(seams(ctx, Execution, [], []))
      {identity, _port} = released!(pid, cap, ctx.run_dir, ["/bin/sleep", "30"], @far)
      run_dir = ctx.run_dir
      prior = aw_r3_prior!()
      on_exit(fn -> aw_r3_restore!(prior) end)
      aw_r3_install!(prior, pid, run_dir, self())

      try do
        # the decoy: an unrelated event carrying the Worker's pid in its metadata, logged from the test BEFORE the crash
        :logger.log(:warning, "aw_r3 decoy", %{pid: pid})
        test = self()
        # the decoy's own witness is sent by the TEST process (the logging process), not the Worker
        assert [{:raw_event, :string, false, :warning, nil, nil, ^test}] = collect({:raw_event, :_, :_, :_, :_, :_, :_})

        assert [{:downstream_event, :string, false, ^test}] = collect({:downstream_event, :_, :_, :_})
        decoy = {:raw_event, :string, false, :warning, nil, nil}
        crash = aw_r3_crash!(pid, :aw_r3_negative_failure)
        surfaces = %{crash | raw: [decoy | crash.raw]}
        assert decoy == hd(surfaces.raw), "the decoy is the first same-pid event"

        refute aw_r3_crash_event?(hd(surfaces.raw), pid, :aw_r3_negative_failure),
               "a first-same-pid pick is not the crash event"

        assert {:ok,
                {:raw_event, {:proc_lib, :crash}, false, :error, [:otp, :sasl],
                 %{pid: ^pid, reason: :aw_r3_negative_failure}}} =
                 aw_r3_crash_event(surfaces.raw, pid, :aw_r3_negative_failure)

        # the same oracle refuses a wrong reason, a wrong pid, and a set without the crash event
        assert {:error, {:crash_event_missing, _}} = aw_r3_crash_event(surfaces.raw, pid, :some_other_reason)
        assert {:error, {:crash_event_missing, _}} = aw_r3_crash_event(surfaces.raw, self(), :aw_r3_negative_failure)
        without = Enum.reject(surfaces.raw, &aw_r3_crash_event?(&1, pid, :aw_r3_negative_failure))
        assert {:error, {:crash_event_missing, _}} = aw_r3_crash_event(without, pid, :aw_r3_negative_failure)

        assert {:error, {:crash_event_ambiguous, 2}} =
                 aw_r3_crash_event(surfaces.raw ++ surfaces.raw, pid, :aw_r3_negative_failure)

        # the full surface oracle still passes with the decoy present (it is not positional) and FAILS the same chain
        # when the residency claim is corrupted
        assert :ok == aw_r3_surfaces!(surfaces, pid, :aw_r3_negative_failure, run_dir, false)

        error =
          assert_raise(ExUnit.AssertionError, fn ->
            aw_r3_surfaces!(surfaces, pid, :aw_r3_negative_failure, run_dir, true)
          end)

        assert error.message =~ "dictionary residency"
      after
        aw_r3_restore!(prior)
      end

      _ = wait_until(fn -> dead?(identity) end, 10_000)
    end

    test "AW-R4 measured: the raw :sys dictionary contains run_dir once begin_await stored a descriptor (pre-existing; frozen primitive)",
         %{run_dir: d, opts: o} do
      {running, _, _} = running!(d, ["/bin/sleep", "30"], @far, o)
      before = inspect(Process.info(self(), :dictionary), limit: :infinity, printable_limit: :infinity)
      refute before =~ d
      assert {:pending, _} = Execution.begin_await(running, o)
      after_ = inspect(Process.info(self(), :dictionary), limit: :infinity, printable_limit: :infinity)
      assert after_ =~ d
      _ = Execution.expire(running)
    end
  end
end
