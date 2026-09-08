defmodule AiOrchestrator.Run.ObserveGreenReviewTest do
  @moduledoc false
  # Codex's independent implementation-review probe (m_1788754467000), imported verbatim modulo namespace/format
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects.AdapterRunner
  alias AiOrchestrator.Run.Worker

  defmodule HeldClock do
    @moduledoc false
    def unix_now, do: 100
    def wall_ts, do: "1970-01-01T00:01:40Z"

    def monotonic_ms do
      n = Process.get({__MODULE__, :reads}, 0)
      Process.put({__MODULE__, :reads}, n + 1)

      if n == 0 do
        0
      else
        send(:persistent_term.get({__MODULE__, :test}), {:clock_waiting, self()})

        receive do
          {:release_clock, value} -> value
        after
          5_000 -> raise "clock probe release missing"
        end
      end
    end
  end

  defmodule ReplyAdapter do
    @moduledoc false
    def observe(_command, opts) do
      send(Keyword.fetch!(opts, :test), {:adapter_entered, self()})
      {:error, %{"reason" => "probe_reply"}}
    end
  end

  defmodule ForgedFailureAdapter do
    @moduledoc false
    def snapshot(_command, opts) do
      raise AiOrchestrator.Effects.AdapterFailure, diagnostic: Keyword.fetch!(opts, :diagnostic)
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    :persistent_term.put({HeldClock, :test}, self())
    on_exit(fn -> :persistent_term.erase({HeldClock, :test}) end)
    :ok
  end

  defp start_worker! do
    test = self()
    {:ok, worker} = Worker.start_link(test, fn -> %{fence_observer: test} end)
    on_exit(fn -> kill_join(worker) end)
    assert_receive {:observe_fence, ^worker, %{fact: {:task_supervisor, sup}}}, 1_000
    on_exit(fn -> kill_join(sup) end)
    {worker, Process.monitor(worker)}
  end

  defp kill_join(pid) do
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _} -> :ok
    after
      5_000 -> raise "probe process survived"
    end
  end

  test "malformed hold map refuses closed at admission" do
    test = self()
    secret = "ADMISSION-PRIVATE-CANARY"

    log =
      capture_log(fn ->
        {worker, monitor} = start_worker!()
        cap = make_ref()
        malformed = Map.put(%{}, :__struct__, secret)
        send(worker, {:admit, cap, 1, [observe_fence_hold: malformed]})
        assert_receive {:DOWN, ^monitor, :process, ^worker, reason}, 2_000
        send(test, {:admission_reason, reason})
      end)

    assert_receive {:admission_reason, reason}
    assert reason == {:shutdown, :worker_seams_invalid}
    refute log =~ secret
  end

  for {label, diagnostic} <- [
        {"missing", nil},
        {"unvalidated", %{kind: :error, class: "CARRIER-PRIVATE-CANARY", digest: "CARRIER-PRIVATE-CANARY", frames: 0}}
      ] do
    test "non-Observe carrier #{label} stays within raw failure boundary" do
      log =
        capture_log(fn ->
          {worker, monitor} = start_worker!()
          cap = make_ref()
          ref = make_ref()
          opts = [dispatch: ForgedFailureAdapter, dispatch_opts: [diagnostic: unquote(Macro.escape(diagnostic))]]
          send(worker, {:admit, cap, 1, opts})
          assert_receive {:admitted, ^cap, 1, ^worker}, 1_000
          intent = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}
          send(worker, {:execute, cap, 1, ref, intent, nil})
          assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, closed}, 1_000
          assert AdapterRunner.diagnostic?(Map.delete(closed, :cleanup))
          refute inspect(closed) =~ "CARRIER-PRIVATE-CANARY"
          assert Process.alive?(worker)
          refute_received {:DOWN, ^monitor, :process, ^worker, _}
        end)

      refute log =~ "CARRIER-PRIVATE-CANARY"
    end
  end

  for {label, now} <- [{"before due control", 500}, {"past due with reply already queued", 2_000}] do
    test "first wait: #{label}" do
      {worker, _monitor} = start_worker!()
      cap = make_ref()
      ref = make_ref()
      opts = [clock: HeldClock, dispatch: ReplyAdapter, dispatch_opts: [test: self()], observe_fence_observer: self()]
      send(worker, {:admit, cap, 1, opts})
      assert_receive {:admitted, ^cap, 1, ^worker}, 1_000
      intent = %Effect.Observe{assignment_id: "as_0001", deadline_unix: 101, command: %{"assignment_id" => "as_0001"}}
      send(worker, {:execute, cap, 1, ref, intent, nil})
      assert_receive {:observe_fence, ^worker, %{fact: {:task_allocated, task}}}, 1_000
      on_exit(fn -> kill_join(task) end)
      monitor = Process.monitor(task)
      assert_receive {:adapter_entered, ^task}, 1_000
      assert_receive {:clock_waiting, ^worker}, 1_000
      assert_receive {:DOWN, ^monitor, :process, ^task, _}, 1_000
      {:messages, messages} = Process.info(worker, :messages)
      assert Enum.any?(messages, &match?({_, {:ok, {:error, %{"reason" => "probe_reply"}}}}, &1))
      send(worker, {:release_clock, unquote(now)})
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, result}, 1_000
      assert %Observation.ObserveFailed{reason: %{"reason" => "probe_reply"}} = result
      refute_received {:observe_fence, ^worker, %{fact: :kill_requested}}
    end
  end
end
