defmodule AiOrchestrator.Run.TimerFenceRedTest do
  @moduledoc """
  R08 G4 (NS-18.D.002 "Run.Server owns timers; workers report and terminate", failure control "Hidden sleep-poll
  scheduler in worker fails"; NS-19.D.001 monotonic live timers): the `Effect.Timer` wait leaves the Worker's
  `Process.sleep`. The Worker arms the same `Run.DeadlineFence` it arms for Observe (one wall and one monotonic read
  of the configured clock) and waits LOOP-RESIDENT in bounded chunks, so a correlated settle, a gate_deadline wake and a
  system probe are all answered mid-wait, and the deadline is answered at or after the due instant of the COHERENT
  clock the test drives. Controls pin the direct no-runner `Effects.execute` path, which still sleeps and returns.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [track!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.DeadlineFence
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_timer_fence"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @canary "TIMER-FENCE-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @cap_ms 50
  @unix 1_800_000_000
  @mono 1_000_000
  @timer_s 600

  # ---- the coherent clock: wall AND monotonic set by the test, every read reported with its reader ---------------

  defmodule CoherentClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    def set(unix, mono_ms), do: :persistent_term.put({__MODULE__, :now}, {unix, mono_ms})
    def set_mono(mono_ms), do: set(elem(now(), 0), mono_ms)
    def sink(pid), do: :persistent_term.put({__MODULE__, :sink}, pid)

    def clear do
      :persistent_term.erase({__MODULE__, :now})
      :persistent_term.erase({__MODULE__, :sink})
    end

    @impl true
    def unix_now, do: report(:unix_now, elem(now(), 0))
    @impl true
    def monotonic_ms, do: report(:monotonic_ms, elem(now(), 1))
    @impl true
    def wall_ts, do: unix_now() |> DateTime.from_unix!() |> DateTime.to_iso8601()

    defp now, do: :persistent_term.get({__MODULE__, :now}, {0, 0})

    defp report(name, value) do
      case :persistent_term.get({__MODULE__, :sink}, nil) do
        pid when is_pid(pid) -> send(pid, {:clock_read, name, value, self()})
        _ -> :ok
      end

      value
    end
  end

  # a clock that fails RAW at the wall read (the arm), every other read coherent
  defmodule ArmFailingClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    def script(canary), do: :persistent_term.put({__MODULE__, :canary}, canary)
    def clear, do: :persistent_term.erase({__MODULE__, :canary})

    @impl true
    def unix_now do
      case :persistent_term.get({__MODULE__, :canary}, nil) do
        nil -> CoherentClock.unix_now()
        canary -> raise canary
      end
    end

    @impl true
    def monotonic_ms, do: CoherentClock.monotonic_ms()
    @impl true
    def wall_ts, do: CoherentClock.wall_ts()
  end

  # ---- harness ----------------------------------------------------------------------------------------------------

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    CoherentClock.set(@unix, @mono)
    CoherentClock.sink(self())
    ArmFailingClock.clear()

    on_exit(fn ->
      CoherentClock.clear()
      ArmFailingClock.clear()
    end)

    :ok
  end

  defp scenario_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  # the standalone product Worker's seams: the scenario's, with the Timer opted into the fence observer, the small
  # chunk cap and the coherent clock; the gate_deadline observer receives the Worker's stale-wake facts
  defp seams(extra \\ []) do
    scenario_opts()
    |> Keyword.drop(@owned)
    |> Keyword.merge(
      supervisor_instance: @instance,
      run_id: "run_fixture_0001",
      observe_fence_observer: self(),
      observe_fence_kinds: [Effect.Timer],
      observe_fence_cap_ms: @cap_ms,
      gate_deadline_observer: self(),
      clock: CoherentClock
    )
    |> Keyword.merge(extra)
  end

  defp standalone_worker!(seams) do
    {:ok, worker} = Run.Worker.start_link(self())
    track!(worker)
    cap = make_ref()
    send(worker, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^worker}, 5_000
    {worker, cap}
  end

  defp timer(deadline_unix), do: %Effect.Timer{purpose: "queued_send_poll", deadline_unix: deadline_unix}

  # the arm snapshot in order (clock reads and facts share the Worker sender): reads BETWEEN :arming and :armed
  defp arm_snapshot!(worker), do: await_arming!(worker, System.monotonic_time(:millisecond) + 10_000)

  defp await_arming!(worker, deadline) do
    receive do
      {:clock_read, _name, _value, ^worker} ->
        await_arming!(worker, deadline)

      {:observe_fence, ^worker, %{op: op, task: nil, kind: Effect.Timer, fact: :arming}} ->
        await_armed!(worker, op, [], deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated Timer arming fact")
    end
  end

  defp await_armed!(worker, op, reads, deadline) do
    receive do
      {:clock_read, name, value, ^worker} ->
        await_armed!(worker, op, [{name, value} | reads], deadline)

      {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: {:armed, armed}}} ->
        {op, armed, Enum.reverse(reads)}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated Timer armed fact")
    end
  end

  # the next fence fact of this op paired with the monotonic read that preceded it
  defp clocked_fact!(worker, op), do: clocked_fact!(worker, op, nil, System.monotonic_time(:millisecond) + 10_000)

  defp clocked_fact!(worker, op, at, deadline) do
    receive do
      {:clock_read, :monotonic_ms, value, ^worker} -> clocked_fact!(worker, op, value, deadline)
      {:clock_read, _name, _value, ^worker} -> clocked_fact!(worker, op, at, deadline)
      {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: {:early, _} = fact}} -> {fact, at}
      {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: :due}} -> {:due, at}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated clocked Timer fact")
    end
  end

  defp mailbox(pid) do
    case Process.info(pid, :messages) do
      {:messages, messages} -> messages
      nil -> []
    end
  end

  defp pending_ref(worker) do
    case :sys.get_state(worker, 1_000) do
      %{pending: %{ref: ref}} -> ref
      %{pending: nil} -> nil
    end
  end

  describe "Worker Timer fence" do
    # armed once from one wall and one monotonic read; capped early wakes; mid-wait service (system probe, stale
    # gate_deadline, foreign wake); expiry at or after due on the coherent clock; no task; a retired wake is stale
    test "T-1 600 s Timer, 50 ms cap: capped chunks, mid-wait service, expiry at or after due, no task" do
      {worker, cap} = standalone_worker!(seams())
      ref = make_ref()
      deadline = @unix + @timer_s
      send(worker, {:execute, cap, 1, ref, timer(deadline), nil})

      assert {%{cap: ^cap, gen: 1, ref: ^ref} = op, %{deadline_unix: ^deadline, due_ms: due, unix_now: @unix}, reads} =
               arm_snapshot!(worker)

      assert reads == [unix_now: @unix, monotonic_ms: @mono], "exactly one wall and one monotonic read at the arm"
      assert {:ok, fence} = DeadlineFence.arm(op, deadline, @unix, @mono, @cap_ms)
      assert due == fence.due_ms and due == @mono + @timer_s * 1_000
      refute_receive {:observe_fence, ^worker, %{op: ^op, fact: {:armed, _}}}, 200, "armed exactly once"

      # the first wait comes from the arm's own monotonic sample; later chunks each read the clock: the clock does not
      # move, so every wake is early by the full cap
      assert {{:early, %{wait_ms: @cap_ms, due_ms: ^due}}, nil} = clocked_fact!(worker, op)
      assert {{:early, %{wait_ms: @cap_ms, due_ms: ^due}}, @mono} = clocked_fact!(worker, op)
      assert {{:early, %{wait_ms: @cap_ms, due_ms: ^due}}, @mono} = clocked_fact!(worker, op)

      # ---- mid-wait: the loop is serviced (a sleeping Worker answers none of these before its deadline)
      assert pending_ref(worker) == ref, "the system probe is answered while the Timer is pending"
      foreign = make_ref()
      send(worker, {:gate_deadline, cap, 1, foreign})
      assert_receive {:gate_deadline, :worker, %{cap: ^cap, gen: 1, ref: ^foreign}, {:stale, :foreign_ref}}, 1_000
      send(worker, {:gate_deadline, cap, 1, ref})
      assert_receive {:gate_deadline, :worker, %{cap: ^cap, gen: 1, ref: ^ref}, {:stale, :foreign_ref}}, 1_000
      send(worker, {:observe_fence_wake, %{op | ref: make_ref()}})
      assert_receive {:observe_fence, ^worker, %{fact: {:stale, :foreign}}}, 1_000
      assert pending_ref(worker) == ref, "neither probe disturbed the pending Timer"
      refute_received {:effect_result, ^cap, 1, ^ref, ^worker, _}
      refute_received {:effect_failed, ^cap, 1, ^ref, ^worker, _}

      # ---- the coherent clock reaches due only when the test says so: one early chunk of the remaining 1 ms, then due
      CoherentClock.set_mono(due - 1)
      assert {{:early, %{wait_ms: 1, due_ms: ^due}}, at_early} = wait_for_clocked!(worker, op, {:early, %{wait_ms: 1}})
      assert at_early == due - 1 and DeadlineFence.next(fence, at_early) == {:wait, 1}
      CoherentClock.set_mono(due)
      assert {:due, at_due} = wait_for_clocked!(worker, op, :due)
      assert at_due >= due and DeadlineFence.next(fence, at_due) == :due, "expiry only at or after the due instant"
      assert_receive {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: :expiry_selected}}, 1_000

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker,
                      %Observation.Deadline{
                        purpose: "queued_send_poll",
                        deadline_unix: ^deadline,
                        now: %Moment{unix: @unix}
                      }},
                     1_000

      assert pending_ref(worker) == nil

      # no task, no GO, no kill: the fence IS the wait
      facts = for {:observe_fence, ^worker, %{op: ^op, fact: fact}} <- mailbox(self()), do: fact
      refute Enum.any?(facts, &match?({:task_allocated, _}, &1))
      refute Enum.any?(facts, &match?({:task_started, _}, &1))
      refute :kill_requested in facts

      # a late wake for the retired op is a fact only: no second answer
      send(worker, {:observe_fence_wake, op})
      assert_receive {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: {:stale, :retired}}}, 1_000
      refute_receive {:effect_result, ^cap, 1, ^ref, ^worker, _}, 100
      assert Process.alive?(worker)
    end

    test "T-2 a correlated settle mid-wait is answered at once and retires the Timer (never answered after)" do
      {worker, cap} = standalone_worker!(seams())
      ref = make_ref()
      deadline = @unix + @timer_s
      send(worker, {:execute, cap, 1, ref, timer(deadline), nil})
      assert {op, %{due_ms: due}, _reads} = arm_snapshot!(worker)
      assert {{:early, %{wait_ms: @cap_ms}}, _} = clocked_fact!(worker, op)
      assert {{:early, %{wait_ms: @cap_ms}}, _} = clocked_fact!(worker, op)

      settle_ref = make_ref()
      send(worker, {:settle, cap, 1, settle_ref})
      assert_receive {:settled, ^cap, 1, ^settle_ref, ^worker, []}, 1_000, "answered mid-wait, not after 600 s"
      assert_receive {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: :cancelled}}, 1_000
      assert pending_ref(worker) == nil

      # the clock reaches due afterwards: nothing is answered; a wake for the retired op is stale
      CoherentClock.set_mono(due)
      refute_receive {:effect_result, ^cap, 1, ^ref, ^worker, _}, 300
      refute_receive {:observe_fence, ^worker, %{op: ^op, fact: :due}}, 100
      send(worker, {:observe_fence_wake, op})
      assert_receive {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: {:stale, :retired}}}, 1_000
      refute_receive {:effect_result, ^cap, 1, ^ref, ^worker, _}, 100
      assert Process.alive?(worker)
    end

    test "T-3 already due at dequeue: answered from the arm's own reads (one each), no wake, no early chunk" do
      {worker, cap} = standalone_worker!(seams())
      ref = make_ref()
      deadline = @unix - 1
      send(worker, {:execute, cap, 1, ref, timer(deadline), nil})
      assert {op, %{deadline_unix: ^deadline, due_ms: @mono, unix_now: @unix}, reads} = arm_snapshot!(worker)
      assert reads == [unix_now: @unix, monotonic_ms: @mono]

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker,
                      %Observation.Deadline{purpose: "queued_send_poll", deadline_unix: ^deadline}},
                     1_000

      refute_receive {:observe_fence, ^worker, %{op: ^op, fact: {:early, _}}}, 100
      assert pending_ref(worker) == nil
      send(worker, {:observe_fence_wake, op})
      assert_receive {:observe_fence, ^worker, %{op: ^op, kind: Effect.Timer, fact: {:stale, :retired}}}, 1_000
    end

    test "T-4 RAW clock failure at the arm fails closed (canary absent); a negative deadline is refused before any read" do
      log =
        capture_log(fn ->
          {worker, cap} = standalone_worker!(seams(clock: ArmFailingClock))
          ArmFailingClock.script(@canary)
          ref = make_ref()
          send(worker, {:execute, cap, 1, ref, timer(@unix + @timer_s), nil})
          assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, diagnostic}, 5_000
          exception = %RuntimeError{message: @canary}
          assert diagnostic.kind == :error and diagnostic.class == Diagnostic.result_class(exception)
          assert diagnostic.digest == Diagnostic.describe(exception)["digest"]
          assert diagnostic.cleanup == %{attempts: 0, settled: 0, unproven: 0}
          refute inspect(diagnostic, limit: :infinity) =~ @canary
          assert Process.alive?(worker) and pending_ref(worker) == nil
          ArmFailingClock.clear()

          bad = make_ref()
          send(worker, {:execute, cap, 1, bad, %Effect.Timer{purpose: "queued_send_poll", deadline_unix: -1}, nil})
          assert_receive {:effect_failed, ^cap, 1, ^bad, ^worker, refused}, 5_000
          assert refused.digest == Diagnostic.describe(:timer_deadline_invalid)["digest"]
          refute_received {:clock_read, _, _, ^worker}
          assert Process.alive?(worker)
        end)

      refute log =~ @canary
    end

    test "T-5 an execute while the Timer is pending is dropped (D-9 parity); the Timer still answers at due" do
      {worker, cap} = standalone_worker!(seams())
      ref = make_ref()
      deadline = @unix + @timer_s
      send(worker, {:execute, cap, 1, ref, timer(deadline), nil})
      assert {_op, %{due_ms: due}, _reads} = arm_snapshot!(worker)
      other = make_ref()
      send(worker, {:execute, cap, 1, other, timer(@unix - 1), nil})
      assert_receive {:gate_deadline, :worker, %{cap: ^cap, gen: 1, ref: ^ref}, :pending_execute}, 1_000
      refute_receive {:effect_result, ^cap, 1, ^other, ^worker, _}, 100
      CoherentClock.set_mono(due)
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.Deadline{deadline_unix: ^deadline}}, 2_000
      refute_receive {:effect_result, ^cap, 1, ^other, ^worker, _}, 100
    end
  end

  # drain early facts until the named one arrives: the clock moves between real chunks, so the facts before the
  # move are the waits of the previous instant; a :due before its target is the one fact that must not be skipped
  defp wait_for_clocked!(worker, op, target) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    fn -> clocked_fact!(worker, op, nil, deadline) end
    |> Stream.repeatedly()
    |> Enum.find(fn
      {{:early, %{wait_ms: wait}}, _} -> target == {:early, %{wait_ms: wait}}
      {:due, _} -> true
    end)
    |> case do
      {{:early, _}, _} = found -> found
      {:due, _} = found when target == :due -> found
      other -> flunk("unexpected clocked fact #{inspect(other)} while waiting for #{inspect(target)}")
    end
  end

  describe "controls at Effects" do
    test "C-1 direct execute WITHOUT a runner still sleeps to the deadline in the caller and returns Deadline" do
      CoherentClock.set(@unix, @mono)
      deadline = @unix + 1
      intent = timer(deadline)
      started = System.monotonic_time(:millisecond)
      {observation, runtime} = Effects.execute(intent, Runtime.new([]), opts: [clock: CoherentClock])
      elapsed = System.monotonic_time(:millisecond) - started
      assert elapsed >= 1_000, "the direct path sleeps (deadline - now) seconds: #{elapsed} ms"

      assert %Observation.Deadline{purpose: "queued_send_poll", deadline_unix: ^deadline, now: %Moment{unix: @unix}} =
               observation

      assert runtime == Runtime.new([])
      test = self()
      assert_received {:clock_read, :unix_now, @unix, ^test}
    end

    # the closure is `:ok`, Effects never arms nor sleeps; :expired and {:ok, :ok} answer Deadline; unknown fails closed
    test "C-2 direct execute WITH a runner: no arm, no sleep; expiry grammar; unknown envelope fails closed" do
      test = self()
      intent = timer(@unix + @timer_s)

      expiring = fn closure, %{deadline_unix: d} when is_function(closure, 0) ->
        send(test, {:runner_called, d, closure.()})
        :expired
      end

      started = System.monotonic_time(:millisecond)
      {observation, _} = Effects.execute(intent, Runtime.new([]), opts: [clock: CoherentClock, adapter_runner: expiring])
      assert System.monotonic_time(:millisecond) - started < 1_000, "no sleep with a runner"
      assert_received {:runner_called, d, :ok}
      assert d == @unix + @timer_s
      assert %Observation.Deadline{deadline_unix: ^d, now: %Moment{unix: @unix}} = observation
      refute_received {:clock_read, :monotonic_ms, _, ^test}, "Effects reads no monotonic time: the owner arms"

      running = fn closure, _ -> {:ok, closure.()} end
      {observation2, _} = Effects.execute(intent, Runtime.new([]), opts: [clock: CoherentClock, adapter_runner: running])
      assert %Observation.Deadline{deadline_unix: ^d} = observation2

      for bad <- [
            fn _c, _d -> :garbage end,
            fn _c, _d -> {:ok, :not_ok} end,
            fn _c, _d -> {:failed, %{not: :a_diagnostic}} end
          ] do
        interrupted =
          try do
            Effects.execute(intent, Runtime.new([]), opts: [clock: CoherentClock, adapter_runner: bad])
            :returned
          rescue
            e in Effects.Interrupted -> e
          end

        assert %Effects.Interrupted{kind: :error, reason: :timer_invalid_runner_return} = interrupted
      end
    end
  end
end
