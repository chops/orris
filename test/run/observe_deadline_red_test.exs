defmodule AiOrchestrator.Run.ObserveDeadlineRedTest do
  @moduledoc """
  U2a-1O RED/interface, revision 6 (docs/contracts/observe-deadline.org; reviewer trace/hold corrections): Observe-only
  deadline actuation through a runner seam, a Worker-owned fence and a Worker-internal task, mapped to the FIRST
  producer of the admissible Observation.TimedOut (ruling B). Controls C-1..C-10 target unchanged 012086b
  with only the observe adapter, the artifact reader or the assignment timeout seam changed (never the pane
  client); RED rows fail today because the seam, fence, task runner and test-only seams do not exist.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.DeadlineFence
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_observe_deadline"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @canary "OBSERVE-DEADLINE-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @kill9_dir Path.expand("../fixtures/contracts/scenarios/kill9_resume", __DIR__)
  @timeout_s 2
  @cap_ms 400

  # ---- doubles ------------------------------------------------------------------------------------------------------

  # a NON-cooperative observe adapter: reports entry, ignores observe_timeout_ms, blocks until instructed (RAW
  # failures on demand, no self-catching); every other dispatch method is the real LocalPane
  defmodule BlockingObserve do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate deliver(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def observe(command, opts) do
      collector = Keyword.fetch!(opts, :collector)
      send(collector, {:observe_entered, self(), command["assignment_id"], Keyword.get(opts, :observe_timeout_ms)})

      receive do
        :release_ok -> LocalPane.observe(command, Keyword.drop(opts, [:collector, :canary]))
        :release_error -> {:error, %{"reason" => "adapter_error_for_parity"}}
        :fail_raise -> raise Keyword.fetch!(opts, :canary)
        :fail_throw -> throw(Keyword.fetch!(opts, :canary))
        :fail_exit -> exit(Keyword.fetch!(opts, :canary))
        # the SAME raw failure site wrapped by an independent test catch that records the real depth and re-raises
        # the ORIGINAL stack: the production diagnostic's frames must equal this observed length
        {:fail_measured, kind, sink} -> measured_raw(kind, Keyword.fetch!(opts, :canary), sink)
      end
    end

    defp measured_raw(kind, canary, sink) do
      case kind do
        :error -> raise canary
        :throw -> throw(canary)
        :exit -> exit(canary)
      end
    catch
      k, r ->
        stack = __STACKTRACE__
        send(sink, {:observed_raw_depth, kind, length(stack)})
        :erlang.raise(k, r, stack)
    end
  end

  # counts every dispatch call with its assignment id (resume controls) while delegating to LocalPane
  defmodule CountingDispatch do
    @moduledoc false
    for name <- [:deliver, :snapshot, :observe, :reconcile] do
      def unquote(name)(command, opts) do
        send(Keyword.fetch!(opts, :collector), {:dispatch_called, unquote(name), command["assignment_id"], self()})
        apply(LocalPane, unquote(name), [command, Keyword.delete(opts, :collector)])
      end
    end
  end

  # a snapshot double that reports which process ran it (default direct invocation control)
  defmodule PidWitness do
    @moduledoc false
    def snapshot(_command, opts) do
      send(Keyword.fetch!(opts, :collector), {:ran_in, self()})
      {:ok, %{"exists" => false}}
    end
  end

  # the Port/memo-owning gate wrapper (same semantics as the imported observe_port_retention_control_test.exs)
  defmodule PortGate do
    @moduledoc false
    alias AiOrchestrator.Test.GateDouble

    def prepare(fs, request, opts) do
      {:ok, handle} = GateDouble.prepare(fs, request, opts)
      port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary, :stream, :exit_status])
      memo = make_ref()
      Process.put({__MODULE__, memo}, :live)
      {:ok, Map.merge(handle, %{owner: self(), port: port, memo: memo, witness: Keyword.fetch!(opts, :witness)})}
    end

    def started_data(handle) do
      witness!(handle, :prepared)
      GateDouble.started_data(handle)
    end

    def abandon(handle) do
      witness!(handle, :settling)
      true = Port.close(handle.port)
      nil = Port.info(handle.port)
      :live = Process.delete({__MODULE__, handle.memo})
      send(handle.witness, {:gate_closed, self(), handle.port, handle.memo})
      :ok
    end

    def witness!(handle, stage) do
      if !(handle.owner == self() and Process.get({__MODULE__, handle.memo}) == :live and
             Port.info(handle.port, :connected) == {:connected, self()}) do
        raise "gate owner or memo changed"
      end

      port = handle.port
      bytes = Atom.to_string(stage) <> "\n"
      true = Port.command(port, bytes)

      receive do
        {^port, {:data, ^bytes}} -> :ok
      after
        2_000 -> raise "owner Port echo did not arrive"
      end

      send(handle.witness, {:gate_owned, stage, self(), port, handle.memo})
      :ok
    end
  end

  # a NON-Observe adapter that raises the public carrier with whatever diagnostic the test supplies (OG-M1)
  defmodule ForgedSnapshot do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate deliver(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def snapshot(_command, opts),
      do: raise(AiOrchestrator.Effects.AdapterFailure, diagnostic: Keyword.fetch!(opts, :diagnostic))
  end

  # a positive leaking source: a GenServer whose state holds the canary (the status probe must see it)
  defmodule LeakingServer do
    @moduledoc false
    use GenServer

    def start(secret), do: GenServer.start(__MODULE__, secret)
    @impl true
    def init(secret), do: {:ok, %{secret: secret}}
  end

  # the owner Clock: wall time scripted exactly like the scenario's FixedClock (the reducer computes deadlines
  # from it), monotonic time REAL (the Worker's timer must fire), and EVERY read reported to the test with its
  # ---- harness ----------------------------------------------------------------------------------------------------

  # returned value so the oracle can recompute the expected due instant and waits
  defmodule ReportingClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    def sink(pid), do: :persistent_term.put({__MODULE__, :sink}, pid)

    @impl true
    def unix_now, do: report(:unix_now, FixedClock.unix_now())
    @impl true
    def monotonic_ms, do: report(:monotonic_ms, System.monotonic_time(:millisecond))
    @impl true
    def wall_ts, do: FixedClock.wall_ts()

    defp report(name, value) do
      case :persistent_term.get({__MODULE__, :sink}, nil) do
        pid when is_pid(pid) -> send(pid, {:clock_read, name, value, self()})
        _ -> :ok
      end

      value
    end
  end

  # a clock that fails RAW (raise/throw/exit with the canary) at one scripted read: the first unix read (the arm)
  # or the n-th monotonic read (2 = the first chunk after GO); every other read is FixedClock / real monotonic
  defmodule FailingClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    def script(mode) do
      :persistent_term.put({__MODULE__, :mode}, mode)
      :persistent_term.put({__MODULE__, :mono_reads}, 0)
    end

    @impl true
    def unix_now do
      case :persistent_term.get({__MODULE__, :mode}, nil) do
        {:unix_now, kind, canary} -> fail(kind, canary)
        _ -> FixedClock.unix_now()
      end
    end

    @impl true
    def monotonic_ms do
      n = :persistent_term.get({__MODULE__, :mono_reads}, 0) + 1
      :persistent_term.put({__MODULE__, :mono_reads}, n)

      case :persistent_term.get({__MODULE__, :mode}, nil) do
        {:monotonic, ^n, kind, canary} -> fail(kind, canary)
        _ -> System.monotonic_time(:millisecond)
      end
    end

    @impl true
    def wall_ts, do: FixedClock.wall_ts()

    defp fail(:error, canary), do: raise(canary)
    defp fail(:throw, canary), do: throw(canary)
    defp fail(:exit, canary), do: exit(canary)
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "observe-deadline-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    ReportingClock.sink(self())
    {:ok, dir: dir}
  end

  defp scenario_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp resume_opts do
    {_, :resume, "kill9_resume", _lines, make} =
      Enum.find(H.cases(), fn {label, _, _, _, _} -> label == "kill9 resume awaiting_artifact" end)

    H.reset_seams()
    make.()
  end

  # the scenario's opts with ONLY the dispatch module replaced (its dispatch_opts kept) plus collector/canary; the
  # assignment deadline is shortened through the reducer's own seam so the Worker's REAL timer stays short
  defp config(dir, dispatch, extra_opts \\ [], extra_dispatch_opts \\ []) do
    base = scenario_opts()

    dispatch_opts =
      base
      |> Keyword.get(:dispatch_opts, [])
      |> Keyword.put(:collector, collector())
      |> Keyword.put(:canary, @canary)
      |> Keyword.merge(extra_dispatch_opts)

    opts =
      base
      |> Keyword.drop(@owned)
      |> Keyword.put(:supervisor_instance, @instance)
      |> Keyword.put(:dispatch, dispatch)
      |> Keyword.put(:dispatch_opts, dispatch_opts)
      |> Keyword.put(:default_assignment_timeout_s, @timeout_s)
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

  # the RED subtree config: fence observer, held point, explicit chunk cap and the reporting owner Clock
  defp red_config(dir, extra \\ []) do
    config(
      dir,
      BlockingObserve,
      [
        observe_fence_observer: self(),
        observe_fence_hold: %{after_expiry: self()},
        observe_fence_cap_ms: @cap_ms,
        clock: ReportingClock
      ] ++
        extra
    )
  end

  defp start!(config) do
    assert {:ok, root} = Run.Supervisor.start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :writer, writer}, 10_000
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, writer: writer, server: server, work: work}
  end

  defp stop!(root) do
    if Process.alive?(root), do: Supervisor.stop(root, :shutdown, 10_000)
    :ok
  end

  defp journal(dir),
    do: dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp types(events), do: Enum.map(events, & &1["type"])

  defp after_observation_start(dir),
    do: dir |> journal() |> types() |> Enum.drop_while(&(&1 != "assignment_observation_started")) |> Enum.drop(1)

  defp mailbox(pid) do
    case Process.info(pid, :messages) do
      {:messages, messages} -> messages
      nil -> []
    end
  end

  defp wait_for(fun, timeout_ms), do: wait(fun, System.monotonic_time(:millisecond) + timeout_ms)

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

  # the identity the Server minted for the Observe execute request (traced), correlating every observer fact
  defp observe_request!(server) do
    OwnedHarness.flush!()

    request =
      self()
      |> mailbox()
      |> Enum.reverse()
      |> Enum.find(&match?({:run_effect_requested, ^server, %{op: :execute, kind: Effect.Observe}}, &1))

    assert match?({:run_effect_requested, ^server, %{cap: _, gen: _, ref: _, worker: _}}, request),
           "the Observe execute request was traced"

    {:run_effect_requested, ^server, %{cap: cap, gen: gen, ref: ref, worker: worker}} = request
    %{op: %{cap: cap, gen: gen, ref: ref}, worker: worker}
  end

  # a bounded status probe: absence is RECORDED (:unavailable), never counted as clean
  defp status_text(pid) do
    inspect(:sys.get_status(pid, 500), limit: :infinity, printable_limit: :infinity)
  catch
    :exit, _ -> :unavailable
  end

  defp observe_effect(deadline_unix, id \\ "as_0001") do
    %Effect.Observe{
      assignment_id: id,
      command: %{"assignment_id" => id, "pane_ref" => "pane:0", "expected_artifact" => "out.txt"},
      deadline_unix: deadline_unix
    }
  end

  # ---- controls -----------------------------------------------------------------------------------------------------

  defp expiry_result(deadline_unix), do: {:error, %{"reason" => "observation_timeout", "deadline_unix" => deadline_unix}}

  # the independent expected closed diagnostic for a raw failure carrying the canary (kind/class/digest)
  defp expected_diagnostic(:error) do
    exception = %RuntimeError{message: @canary}
    %{kind: :error, class: Diagnostic.result_class(exception), digest: Diagnostic.describe(exception)["digest"]}
  end

  defp expected_diagnostic(kind) when kind in [:throw, :exit],
    do: %{kind: kind, class: Diagnostic.result_class(@canary), digest: Diagnostic.describe(@canary)["digest"]}

  defp now_unix, do: System.os_time(:second)

  # a standalone admitted PRODUCT Worker with this test as its controlled server; the bootstrap is a zero-arity
  # closure (D2 + correction 1): no payload-bearing map or MFA reaches supervisor start/crash reports
  defp standalone_worker!(seams, bootstrap \\ nil) do
    {:ok, worker} =
      case bootstrap do
        nil -> Run.Worker.start_link(self())
        fun -> worker_module().start_link(self(), fun)
      end

    track!(worker)
    cap = make_ref()
    send(worker, {:admit, cap, 1, seams})
    assert_receive {:admitted, ^cap, 1, ^worker}, 5_000
    {worker, cap}
  end

  # the interface under RED: start_link/2 does not exist yet; a dynamic module call keeps the RED file
  # compiling under warnings-as-errors
  defp worker_module, do: Module.concat(["AiOrchestrator", "Run", "Worker"])

  defp standalone_seams(extra \\ []) do
    base = scenario_opts()
    test = self()

    base
    |> Keyword.drop(@owned)
    |> Keyword.put(:dispatch, BlockingObserve)
    |> Keyword.put(:dispatch_opts, Keyword.merge(Keyword.get(base, :dispatch_opts, []), collector: test, canary: @canary))
    |> Keyword.merge(supervisor_instance: @instance, run_id: "run_fixture_0001")
    |> Keyword.merge(extra)
  end

  defp port_gate_seams(extra), do: standalone_seams([gate_executor: PortGate, gate_opts: [witness: self()]] ++ extra)

  defp prepare_gate(dir) do
    %Effect.PrepareGate{
      gate_run_id: "gr_0001",
      attempt: 1,
      requested: %{"command_argv" => ["true"], "timeout_s" => 600},
      deadline_unix: FixedClock.unix_now() + 600,
      repo_root: dir,
      run_dir: dir
    }
  end

  # the durable accepted intent deadline from the journal (O-M9): never Worker unix + timeout
  defp durable_deadline!(dir) do
    event = dir |> journal() |> Enum.find(&(&1["type"] == "assignment_observation_started"))
    assert event, "assignment_observation_started is durable"
    event["data"]["deadline_unix"]
  end

  # Clock reports and fence facts have the same Worker sender. Consume them in order:
  # prior reads are not arm reads, and a later wake cannot supply an earlier wake's time.
  defp arm_snapshot!(worker), do: await_arming!(worker, System.monotonic_time(:millisecond) + 30_000)

  defp await_arming!(worker, deadline) do
    receive do
      {:clock_read, _name, _value, ^worker} -> await_arming!(worker, deadline)
      {:observe_fence, ^worker, %{op: op, fact: :arming}} -> await_armed!(worker, op, [], deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated arming fact")
    end
  end

  defp await_armed!(worker, op, reads, deadline) do
    receive do
      {:clock_read, name, value, ^worker} -> await_armed!(worker, op, [{name, value} | reads], deadline)
      {:observe_fence, ^worker, %{op: ^op, fact: {:armed, armed}}} -> {op, armed, Enum.reverse(reads)}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated armed fact")
    end
  end

  defp clocked_fact!(worker, op), do: clocked_fact!(worker, op, nil, System.monotonic_time(:millisecond) + 30_000)

  defp clocked_fact!(worker, op, at, deadline) do
    receive do
      {:clock_read, :monotonic_ms, value, ^worker} -> clocked_fact!(worker, op, value, deadline)
      {:clock_read, _name, _value, ^worker} -> clocked_fact!(worker, op, at, deadline)
      {:observe_fence, ^worker, %{op: ^op, fact: {:early, _} = fact}} -> {fact, at}
      {:observe_fence, ^worker, %{op: ^op, fact: :due}} -> {:due, at}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no correlated clocked fence fact")
    end
  end

  defp verify_wakes!(worker, op, task, fence, due, count, deadline) do
    case clocked_fact!(worker, op, nil, deadline) do
      {{:early, %{wait_ms: wait, due_ms: ^due}}, at} when is_integer(at) ->
        assert Process.alive?(task)
        assert DeadlineFence.next(fence, at) == {:wait, wait}
        refute_received {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}
        verify_wakes!(worker, op, task, fence, due, count + 1, deadline)

      {:due, at} when is_integer(at) ->
        assert DeadlineFence.next(fence, at) == :due
        assert count >= 2, "multiple capped early wakes must be exercised"
    end
  end

  describe "controls: the unchanged 012086b observe path, measured end to end" do
    test "C-10 ordered trace reader: delayed consumer excludes old reads, retains duplicate arm reads, pairs each wake" do
      test = self()
      op = %{cap: make_ref(), gen: 1, ref: make_ref()}
      armed = %{deadline_unix: 11, due_ms: 1_000}

      for duplicate <- [false, true] do
        {sender, monitor} =
          OwnedHarness.spawn_caller!(fn ->
            send(test, {:clock_read, :unix_now, -1, self()})
            send(test, {:observe_fence, self(), %{op: op, fact: :arming}})
            send(test, {:clock_read, :unix_now, 10, self()})
            if duplicate, do: send(test, {:clock_read, :unix_now, 10, self()})
            send(test, {:clock_read, :monotonic_ms, 0, self()})
            send(test, {:observe_fence, self(), %{op: op, fact: {:armed, armed}}})

            for {at, wait} <- [{400, 400}, {800, 200}] do
              send(test, {:clock_read, :monotonic_ms, at, self()})
              send(test, {:observe_fence, self(), %{op: op, fact: {:early, %{due_ms: 1_000, wait_ms: wait}}}})
            end

            send(test, {:clock_read, :monotonic_ms, 1_000, self()})
            send(test, {:observe_fence, self(), %{op: op, fact: :due}})
            :trace_queued
          end)

        assert_receive {:result, :trace_queued}, 5_000
        assert_receive {:DOWN, ^monitor, :process, ^sender, :normal}, 5_000
        assert {^op, ^armed, reads} = arm_snapshot!(sender)
        expected = [unix_now: 10, monotonic_ms: 0]
        expected = if duplicate, do: [{:unix_now, 10} | expected], else: expected
        assert reads == expected
        assert {{:early, %{due_ms: 1_000, wait_ms: 400}}, 400} = clocked_fact!(sender, op)
        assert {{:early, %{due_ms: 1_000, wait_ms: 200}}, 800} = clocked_fact!(sender, op)
        assert {:due, 1_000} = clocked_fact!(sender, op)
      end
    end

    test "C-1 SYNCHRONOUS blocked observe: Worker inside Effects.execute; settle-shaped message waits; releases complete",
         %{dir: dir} do
      facts = start!(config(dir, BlockingObserve))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      assert_receive {:observe_entered, entered_in, first, cap_ms}, 30_000
      # GREEN (named transition C-1): the adapter runs in the TASK, never in the Worker; the Worker still cannot
      # dequeue settle while inside the runner receive; releases go to the task
      refute entered_in == worker
      assert is_integer(cap_ms), "observe_opts caps observe_timeout_ms to the remaining window"
      settle = {:settle, make_ref(), 1, make_ref()}
      send(worker, settle)
      assert wait_for(fn -> settle in mailbox(worker) end, 2_000), "blocked in the runner receive: nothing dequeues"
      assert Process.alive?(worker)
      send(entered_in, :release_ok)
      assert_receive {:observe_entered, second_task, second, _}, 30_000
      assert second != first and second_task != worker
      send(second_task, :release_ok)
      assert {:ok, %{summary: %{"status" => "completed"}}} = Server.await(facts.server, 30_000)
      stop!(facts.root)
    end

    test "C-2 COOPERATIVE timeout baseline, MEASURED: real LocalPane poll entered and expired (only the artifact reader replaced)",
         %{dir: dir} do
      test = self()

      pending_reader = fn command ->
        send(test, {:poll_entered, self(), command["assignment_id"]})
        {:pending, %{"reason" => "artifact_absent"}}
      end

      facts =
        start!(
          config(dir, LocalPane, [], observe_timeout_ms: 10, sleeper: fn _ -> :ok end, artifact_reader: pending_reader)
        )

      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      server_mon = Process.monitor(facts.server)
      # GREEN (named transition C-2): the poll runs in the TASK, never in the Worker; the result shape is unchanged
      assert_receive {:poll_entered, poller, "as_0001"}, 30_000
      refute poller == worker
      result = Server.await(facts.server, 30_000)
      polls = self() |> mailbox() |> Enum.count(&match?({:poll_entered, _, _}, &1))
      IO.puts("\n[C-2 measured] result=#{inspect(result)} polls_queued=#{polls}")
      assert "assignment_observation_started" in types(journal(dir))
      refute "artifact_observed" in types(journal(dir))

      assert result ==
               {:error, %{"reason" => "observation_timeout", "last_observation" => %{"reason" => "artifact_absent"}}}

      assert Server.status(facts.server) == :failed
      refute_receive {:DOWN, ^server_mon, :process, _, _}, 200
      assert after_observation_start(dir) == []
      stop!(facts.root)
    end

    test "C-3 D1 transition: the UNCHANGED expired kill9 awaiting prefix resumes to the exact expiry error, zero adapter entries, prefix preserved",
         %{dir: dir} do
      seeded =
        @kill9_dir |> Path.join("events_awaiting_artifact.jsonl") |> File.read!() |> String.split("\n", trim: true)

      File.write!(Path.join(dir, "events.jsonl"), Enum.join(seeded, "\n") <> "\n")
      base = resume_opts()
      dispatch_opts = base |> Keyword.get(:dispatch_opts, []) |> Keyword.put(:collector, collector())

      opts =
        base
        |> Keyword.drop(@owned)
        |> Keyword.put(:supervisor_instance, @instance)
        |> Keyword.put(:dispatch, CountingDispatch)
        |> Keyword.put(:dispatch_opts, dispatch_opts)
        |> Keyword.put(:observe_fence_observer, self())

      cfg = %{
        run_dir: dir,
        mode: :resume,
        spec: H.spec("kill9_resume"),
        plan: H.plan("kill9_resume"),
        opts: opts,
        trace: collector()
      }

      facts = start!(cfg)
      result = Server.await(facts.server, 60_000)
      OwnedHarness.flush!()
      calls = for {:dispatch_called, name, assignment_id, _} <- mailbox(self()), do: {name, assignment_id}
      # the fixture's deadline (2026-01-01T00:00:00Z, before its own lifecycle timestamps) is already due against
      # FixedClock (2026-09-01T12:00Z): D1 answers the expiry from the retained intent with NO adapter look (the
      # unchanged product took one; measured 2026-09-07). This is an explicit D1 transition, not a control.
      assert result == {:error, %{"reason" => "observation_timeout", "deadline_unix" => 1_767_225_600}}

      assert calls == [],
             "zero adapter entries for the due awaiting assignment: nothing observed, delivered or reconciled"

      assert_receive {:observe_fence, worker, %{fact: {:armed, %{deadline_unix: 1_767_225_600}}}}, 5_000
      refute_receive {:observe_fence, ^worker, %{fact: {:task_allocated, _}}}, 100
      refute_receive {:observe_fence, ^worker, %{fact: {:task_started, _}}}, 100

      assert String.starts_with?(File.read!(Path.join(dir, "events.jsonl")), Enum.join(seeded, "\n") <> "\n"),
             "the input prefix bytes are preserved"

      suffix = dir |> journal() |> Enum.drop(length(seeded)) |> types()

      # the legitimate resume-acceptance suffix, MEASURED (run_resumed + stale-lease repair pairs), nothing else
      assert suffix == [
               "run_resumed",
               "workspace_lease_release_requested",
               "workspace_lease_released",
               "workspace_lease_acquired",
               "pane_lease_release_requested",
               "pane_lease_released",
               "pane_lease_acquired"
             ]

      assert Server.status(facts.server) == :failed
      stop!(facts.root)
    end

    test "C-4 default direct invocation: a SnapshotArtifact runs in the calling process with no runner supplied" do
      snapshot = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}

      {observation, _} =
        Effects.execute(snapshot, Runtime.new([]), opts: [dispatch: PidWitness, dispatch_opts: [collector: self()]])

      test = self()
      assert_receive {:ran_in, ^test}
      assert is_struct(observation)
      assert observation.__struct__ in Effect.admissible_observations(snapshot)
    end

    test "C-5 raw raise/throw/exit inside observe today, MEASURED per kind: closed effect_failed, Server exits run_step_failed",
         %{dir: dir} do
      for {signal, kind} <- [fail_raise: :error, fail_throw: :throw, fail_exit: :exit] do
        sub = Path.join(dir, Atom.to_string(kind))
        File.mkdir_p!(sub)

        log =
          capture_log(fn ->
            facts = start!(config(sub, BlockingObserve))
            assert_receive {:observe_entered, worker, _, _}, 30_000
            server_mon = Process.monitor(facts.server)
            send(worker, signal)
            assert Server.await(facts.server, 30_000) == {:error, %{clause: "run_server_down"}}
            assert_receive {:DOWN, ^server_mon, :process, _, {:run_step_failed, diagnostic}}, 10_000
            expected = expected_diagnostic(kind)

            assert diagnostic.kind == expected.kind and diagnostic.class == expected.class and
                     diagnostic.digest == expected.digest

            # ---- RED
            assert is_integer(diagnostic.frames) and is_map(diagnostic.cleanup)
            assert Enum.sort(Map.keys(diagnostic)) == [:class, :cleanup, :digest, :frames, :kind]
            Process.put({__MODULE__, :baseline_frames, kind}, diagnostic.frames)
            stop!(facts.root)
          end)

        refute log =~ @canary
      end
    end

    test "C-6 synthetic direct Effects retention control (NOT the product retention witness)" do
      port = Port.open({:spawn_executable, "/bin/cat"}, [:binary, :stream])
      Process.put({__MODULE__, :memo}, :kept)

      runtime =
        [] |> Runtime.new() |> Runtime.put({"gr_0001", 1}, :prepared, %{label: :fake_handle_witness, port: port})

      {observation, runtime2} =
        Effects.execute(observe_effect(now_unix() + 60), runtime, opts: [dispatch: H.OkDispatch, dispatch_opts: []])

      assert %Observation.ArtifactObserved{} = observation
      assert Runtime.handle(runtime2, {"gr_0001", 1}) == %{label: :fake_handle_witness, port: port}
      assert Port.info(port, :connected) == {:connected, self()}
      assert Process.get({__MODULE__, :memo}) == :kept
    end

    test "C-8 a real PrepareGate alone succeeds under the standalone admitted product Worker (scenario gate seams)" do
      base = scenario_opts()
      seams = base |> Keyword.drop(@owned) |> Keyword.merge(supervisor_instance: @instance, run_id: "run_fixture_0001")
      {:ok, worker} = Run.Worker.start_link(self())
      track!(worker)
      cap = make_ref()
      send(worker, {:admit, cap, 1, seams})
      assert_receive {:admitted, ^cap, 1, ^worker}, 5_000
      gate_ref = make_ref()

      prepare = %Effect.PrepareGate{
        gate_run_id: "gr_0001",
        attempt: 1,
        requested: %{"command_argv" => ["true"], "timeout_s" => 600},
        deadline_unix: FixedClock.unix_now() + 600,
        repo_root: System.tmp_dir!(),
        run_dir: System.tmp_dir!()
      }

      send(worker, {:execute, cap, 1, gate_ref, prepare, nil})
      assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, prepared}, 10_000
      assert is_struct(prepared)
      %{runtime: %Runtime{gates: gates}} = :sys.get_state(worker)
      assert Map.has_key?(gates, {"gr_0001", 1})
      settle_ref = make_ref()
      send(worker, {:settle, cap, 1, settle_ref})
      assert_receive {:settled, ^cap, 1, ^settle_ref, ^worker, cleanup}, 5_000
      assert length(cleanup) == 1
    end

    test "C-9 failure-path latest-Runtime settlement with PortGate, MEASURED today (adapter runs IN the Worker)",
         %{dir: dir} do
      test = self()
      seams = port_gate_seams(observe_fence_observer: test, clock: ReportingClock)

      log =
        capture_log(fn ->
          {worker, cap} = standalone_worker!(seams)
          gate_ref = make_ref()
          send(worker, {:execute, cap, 1, gate_ref, prepare_gate(dir), nil})
          assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
          assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
          obs_ref = make_ref()
          send(worker, {:execute, cap, 1, obs_ref, observe_effect(FixedClock.unix_now() + 600), nil})
          assert_receive {:observe_entered, task, _, _}, 5_000
          send(task, :fail_raise)
          assert_receive {:gate_owned, :settling, ^worker, ^port, ^memo}, 10_000
          assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000
          assert_receive {:effect_failed, ^cap, 1, ^obs_ref, ^worker, %{cleanup: %{attempts: 1, settled: 1}}}, 10_000
          assert Port.info(port) == nil
        end)

      refute log =~ @canary
    end

    test "C-7 positive leaking-source control: the bounded status probe DOES see a canary held in a GenServer state" do
      {:ok, leaking} = LeakingServer.start(@canary)
      track!(leaking)
      text = status_text(leaking)
      assert text != :unavailable and text =~ @canary
    end
  end

  describe "RED: seam boundary and already-due (O-1, O-3)" do
    test "O-1 direct Effects: runner invoked for Observe only (future AND already-due deadlines); Effects never arms" do
      test = self()

      runner = fn closure, %{deadline_unix: d} when is_function(closure, 0) ->
        send(test, {:runner_called, d})
        {:ok, closure.()}
      end

      opts = [dispatch: H.OkDispatch, dispatch_opts: [], adapter_runner: runner]
      {observation, _} = Effects.execute(observe_effect(now_unix() + 60), Runtime.new([]), opts: opts)
      assert_receive {:runner_called, d1}, 1_000
      assert is_integer(d1)
      assert %Observation.ArtifactObserved{} = observation
      # already due at DIRECT Effects: the runner is still invoked (arming is the Worker's, not Effects')
      {_observation2, _} = Effects.execute(observe_effect(now_unix() - 10), Runtime.new([]), opts: opts)
      assert_receive {:runner_called, _d2}, 1_000
      snapshot = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}
      Effects.execute(snapshot, Runtime.new([]), opts: opts)
      refute_receive {:runner_called, _}, 200
    end

    test "O-3 already due at the PRODUCT Worker: TimedOut, zero task starts, zero adapter entries, runner not invoked, arm-snapshot reads once each" do
      test = self()

      seams =
        standalone_seams(
          observe_fence_observer: test,
          clock: ReportingClock,
          adapter_runner: fn _c, _d ->
            send(test, :runner_invoked)
            :never
          end
        )

      {worker, cap} = standalone_worker!(seams)
      ref = make_ref()
      due_deadline = FixedClock.unix_now() - 1
      send(worker, {:execute, cap, 1, ref, observe_effect(due_deadline), nil})
      assert {%{cap: ^cap, gen: 1, ref: ^ref}, %{deadline_unix: ^due_deadline}, reads} = arm_snapshot!(worker)

      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker,
                      %Observation.TimedOut{assignment_id: "as_0001", deadline_unix: ^due_deadline}},
                     5_000

      refute_receive {:observe_fence, ^worker, %{fact: {:task_started, _}}}, 200
      refute_receive {:observe_entered, _, _, _}, 200
      refute_receive :runner_invoked, 200

      assert Enum.count(reads, &match?({:unix_now, _}, &1)) == 1 and
               Enum.count(reads, &match?({:monotonic_ms, _}, &1)) == 1
    end

    test "O-3b late admission consumes the REMAINING time: a deadline 1 s ahead expires after ~1 s at the product Worker" do
      test = self()
      seams = standalone_seams(observe_fence_observer: test, clock: ReportingClock, observe_fence_cap_ms: @cap_ms)
      {worker, cap} = standalone_worker!(seams)
      ref = make_ref()
      deadline = FixedClock.unix_now() + 1
      started = System.monotonic_time(:millisecond)
      send(worker, {:execute, cap, 1, ref, observe_effect(deadline), nil})
      assert {%{ref: ^ref}, %{deadline_unix: ^deadline, due_ms: due}, reads} = arm_snapshot!(worker)
      assert [unix_now: unix, monotonic_ms: mono] = reads
      assert deadline - unix == 1 and due == mono + 1_000
      assert_receive {:observe_entered, task, _, _}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.TimedOut{deadline_unix: ^deadline}}, 10_000
      finished = System.monotonic_time(:millisecond)
      assert finished >= due, "the result cannot precede the independently measured due instant"
      assert finished - started < 5_000, "remaining time, not a fresh timeout_s"
      assert is_integer(due) and not Process.alive?(task)
    end
  end

  describe "RED: arming, chunks and actual timer expiry at the real subtree (O-2, O-4, O-5, O-14)" do
    test "O-2/O-4/O-5 armed once (arm-snapshot reads, durable deadline), early chunks vs the clock trace, real timer expiry, exact result and prefix",
         %{dir: dir} do
      facts = start!(red_config(dir))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      {op, %{deadline_unix: d, due_ms: due}, arm_reads} = arm_snapshot!(worker)
      request = observe_request!(facts.server)
      assert op == request.op and worker == request.worker
      refute_receive {:observe_fence, ^worker, %{fact: {:armed, _}}}, 200, "armed exactly once"
      assert d == durable_deadline!(dir), "the expected deadline is the DURABLE accepted intent"

      assert Enum.count(arm_reads, &match?({:unix_now, _}, &1)) == 1 and
               Enum.count(arm_reads, &match?({:monotonic_ms, _}, &1)) == 1

      {:unix_now, u} = Enum.find(arm_reads, &match?({:unix_now, _}, &1))
      {:monotonic_ms, m} = Enum.find(arm_reads, &match?({:monotonic_ms, _}, &1))
      assert {:ok, fence} = DeadlineFence.arm(op, d, u, m, @cap_ms)
      assert due == fence.due_ms
      assert_receive {:observe_entered, task, _, _}, 30_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:task_started, ^task}}}, 5_000
      refute task == worker

      verify_wakes!(worker, op, task, fence, due, 0, System.monotonic_time(:millisecond) + 30_000)

      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 5_000
      assert_receive {:observe_fence_held, ^worker, token, %{op: ^op, task: %{pid: ^task}}}, 5_000
      send(worker, {:observe_fence_proceed, token})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:shutdown_return, nil}}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, task: %{pid: ^task}, fact: {:death, :killed}}}, 5_000
      server_mon = Process.monitor(facts.server)
      assert Server.await(facts.server, 30_000) == expiry_result(d)
      assert Server.status(facts.server) == :failed
      refute_receive {:DOWN, ^server_mon, :process, _, _}, 200
      assert after_observation_start(dir) == []
      # O-14 expiry-produced prefix resume: exact result, prefix preserved, re-observation policy RECORDED
      stop!(facts.root)
      seeded = journal(dir)
      base = resume_opts()
      dispatch_opts = base |> Keyword.get(:dispatch_opts, []) |> Keyword.put(:collector, collector())

      opts =
        base
        |> Keyword.drop(@owned)
        |> Keyword.put(:supervisor_instance, @instance)
        |> Keyword.put(:dispatch, CountingDispatch)
        |> Keyword.put(:dispatch_opts, dispatch_opts)

      cfg = %{
        run_dir: dir,
        mode: :resume,
        spec: H.spec("gated_run_seed"),
        plan: H.plan("gated_run_seed"),
        opts: opts,
        trace: collector()
      }

      facts2 = start!(cfg)
      resume_result = Server.await(facts2.server, 60_000)
      OwnedHarness.flush!()
      calls = for {:dispatch_called, name, assignment_id, _} <- mailbox(self()), do: {name, assignment_id}
      IO.puts("\n[O-14 measured] resume ok?: #{inspect(match?({:ok, _}, resume_result))}; calls: #{inspect(calls)}")
      assert Enum.take(journal(dir), length(seeded)) == seeded, "the expired assignment's prefix is preserved"
      refute {:deliver, "as_0001"} in calls
      refute {:reconcile, "as_0001"} in calls
      assert match?({:ok, _}, resume_result)
      stop!(facts2.root)
    end
  end

  describe "RED: dequeue orders, held raced result and wakes (O-6, O-6b, O-7, O-8)" do
    test "O-6 result first, FORCED (long deadline, controlled owner): reply wins, no kill; a retired wake is stale" do
      test = self()
      seams = standalone_seams(observe_fence_observer: test, clock: ReportingClock)
      {worker, cap} = standalone_worker!(seams)
      ref_a = make_ref()
      send(worker, {:execute, cap, 1, ref_a, observe_effect(FixedClock.unix_now() + 600, "as_0001"), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref_a} = op_a, fact: {:armed, _}}}, 5_000
      assert_receive {:observe_entered, task, "as_0001", _}, 5_000
      send(task, :release_error)

      assert_receive {:effect_result, ^cap, 1, ^ref_a, ^worker,
                      %Observation.ObserveFailed{reason: %{"reason" => "adapter_error_for_parity"}}},
                     5_000

      refute_receive {:observe_fence, ^worker, %{op: ^op_a, fact: :kill_requested}}, 100
      ref_b = make_ref()
      send(worker, {:execute, cap, 1, ref_b, observe_effect(FixedClock.unix_now() + 600, "as_0002"), nil})
      assert_receive {:observe_entered, task_b, "as_0002", _}, 5_000
      send(worker, {:observe_fence_wake, op_a})
      assert_receive {:observe_fence, ^worker, %{op: ^op_a, fact: {:stale, :retired}}}, 5_000
      assert Process.alive?(task_b)
      send(task_b, :release_error)
      assert_receive {:effect_result, ^cap, 1, ^ref_b, ^worker, %Observation.ObserveFailed{}}, 5_000
      kills = for {:observe_fence, ^worker, %{fact: :kill_requested}} <- mailbox(self()), do: :kill
      assert kills == []
    end

    test "O-6b raw error first, FORCED (controlled owner): closed failure wins; a later wake for that op is stale" do
      test = self()
      seams = standalone_seams(observe_fence_observer: test, clock: ReportingClock)

      log =
        capture_log(fn ->
          {worker, cap} = standalone_worker!(seams)
          ref = make_ref()
          send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})
          assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref} = op, fact: {:armed, _}}}, 5_000
          assert_receive {:observe_entered, task, _, _}, 5_000
          send(task, :fail_raise)
          assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, :normal}}}, 10_000
          assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, diagnostic}, 10_000
          expected = expected_diagnostic(:error)
          assert diagnostic.kind == :error and diagnostic.class == expected.class and diagnostic.digest == expected.digest
          assert Enum.sort(Map.keys(diagnostic)) == [:class, :cleanup, :digest, :frames, :kind]
          send(worker, {:observe_fence_wake, op})
          assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:stale, :retired}}}, 5_000
          refute_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 100
        end)

      refute log =~ @canary
    end

    test "O-7 genuine raced result via the HELD point: expiry selected, held (token correlated), reply queued on the current ref, then kill: ok_reply + late_result REQUIRED",
         %{dir: dir} do
      facts = start!(red_config(dir))
      assert_receive {:observe_fence, worker, %{op: op, fact: {:armed, %{deadline_unix: d}}}}, 30_000
      assert_receive {:observe_entered, task, _, _}, 30_000

      assert_receive {:observe_fence, ^worker,
                      %{op: ^op, task: %{ref: task_ref, pid: ^task}, fact: {:task_started, ^task}}},
                     5_000

      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 30_000
      assert_receive {:observe_fence_held, ^worker, token, %{op: ^op, task: %{ref: ^task_ref, pid: ^task}}}, 5_000
      refute_received {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}
      send(task, :release_ok)
      assert wait_for(fn -> Enum.any?(mailbox(worker), &match?({^task_ref, {:ok, _}}, &1)) end, 5_000)
      send(worker, {:observe_fence_proceed, token})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:shutdown_return, :ok_reply}}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:late_result, :stale}}}, 5_000
      assert Server.await(facts.server, 30_000) == expiry_result(d)
      assert Server.status(facts.server) == :failed
      assert after_observation_start(dir) == []
      stop!(facts.root)
    end

    test "O-8 foreign and duplicate wakes: foreign ignored with the task untouched; a duplicate never kills twice (count over the complete op)",
         %{dir: dir} do
      facts = start!(red_config(dir))
      assert_receive {:observe_fence, worker, %{op: op, fact: {:armed, _}}}, 30_000
      assert_receive {:observe_entered, task, _, _}, 30_000
      send(worker, {:observe_fence_wake, %{op | ref: make_ref()}})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:stale, :foreign}}}, 5_000
      assert Process.alive?(task)
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :expiry_selected}}, 30_000
      assert_receive {:observe_fence_held, ^worker, token, _}, 5_000
      send(worker, {:observe_fence_wake, op})
      send(worker, {:observe_fence_proceed, token})
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:stale, :duplicate}}}, 5_000
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, :killed}}}, 5_000
      _ = Server.await(facts.server, 30_000)
      OwnedHarness.flush!()
      kills = for {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}} <- mailbox(self()), do: :kill
      assert kills == [], "the one asserted kill was consumed; no additional kill may remain"
      stop!(facts.root)
    end

    test "O-8b hold controller death AFTER expiry_selected: the Worker proceeds only to shutdown (no stranded hold)" do
      test = self()

      controller =
        spawn(fn ->
          receive do
            :never -> :ok
          end
        end)

      track!(controller)

      seams =
        standalone_seams(
          observe_fence_observer: test,
          observe_fence_hold: %{after_expiry: controller},
          observe_fence_cap_ms: @cap_ms,
          clock: ReportingClock
        )

      {worker, cap} = standalone_worker!(seams)
      ref = make_ref()
      send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 1), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{ref: ^ref} = op, fact: :expiry_selected}}, 10_000
      assert wait_for(fn -> Enum.any?(mailbox(controller), &match?({:observe_fence_held, ^worker, _, _}, &1)) end, 5_000)
      Process.exit(controller, :kill)
      assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^ref, ^worker, %Observation.TimedOut{}}, 10_000
    end

    test "O-8c hold controller death at the pre-GO hold: the Worker fails CLOSED and joins the task; GO is never granted" do
      test = self()

      controller =
        spawn(fn ->
          receive do
            :never -> :ok
          end
        end)

      track!(controller)

      seams =
        standalone_seams(
          observe_fence_observer: test,
          observe_fence_hold: %{before_go: controller},
          clock: ReportingClock
        )

      log =
        capture_log(fn ->
          {worker, cap} = standalone_worker!(seams)
          ref = make_ref()
          send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})

          assert_receive {:observe_fence, ^worker,
                          %{op: %{ref: ^ref} = op, task: %{pid: task}, fact: {:task_allocated, task}}},
                         5_000

          assert wait_for(
                   fn ->
                     Enum.any?(mailbox(controller), &match?({:observe_fence_held, ^worker, _, %{stage: :before_go}}, &1))
                   end,
                   5_000
                 )

          task_mon = Process.monitor(task)
          Process.exit(controller, :kill)
          assert_receive {:DOWN, ^task_mon, :process, ^task, _}, 5_000
          refute_receive {:observe_entered, _, _, _}, 100, "GO was never granted"
          assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, %{kind: :exit}}, 10_000
          assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, _}}}, 5_000
        end)

      refute log =~ @canary
    end
  end

  describe "RED: failure parity, grammar and death (O-9)" do
    test "O-9 RAW raise/throw/exit under the runner: exact C-5 kind/class/digest; frames EQUAL the independently observed real depth",
         %{dir: dir} do
      for kind <- [:error, :throw, :exit] do
        sub = Path.join(dir, Atom.to_string(kind))
        File.mkdir_p!(sub)
        test = self()

        log =
          capture_log(fn ->
            facts = start!(red_config(sub))
            assert_receive {:observe_fence, worker, %{op: op, fact: {:armed, _}}}, 30_000
            assert_receive {:observe_entered, task, _, _}, 30_000
            server_mon = Process.monitor(facts.server)
            send(task, {:fail_measured, kind, test})
            assert_receive {:observed_raw_depth, ^kind, observed_depth}, 10_000
            assert_receive {:observe_fence, ^worker, %{op: ^op, task: %{pid: ^task}, fact: {:death, :normal}}}, 10_000
            assert Server.await(facts.server, 30_000) == {:error, %{clause: "run_server_down"}}
            assert_receive {:DOWN, ^server_mon, :process, _, {:run_step_failed, diagnostic}}, 10_000
            expected = expected_diagnostic(kind)

            assert diagnostic.kind == expected.kind and diagnostic.class == expected.class and
                     diagnostic.digest == expected.digest

            assert diagnostic.frames == observed_depth, "frames equal the independently observed real depth"
            stop!(facts.root)
          end)

        refute log =~ @canary
      end
    end

    test "O-9 grammar: {:ok, {:error, map}} parity; malformed envelopes fail closed; task killed without reply is a closed failure",
         %{dir: dir} do
      # {:ok, {:error, map}} -> ObserveFailed parity at direct Effects with the runner
      runner_ok = fn closure, _ -> {:ok, closure.()} end
      opts = [dispatch: BlockingObserve, dispatch_opts: [collector: self(), canary: @canary], adapter_runner: runner_ok]

      {caller, caller_mon} =
        OwnedHarness.spawn_caller!(fn ->
          {observation, _} = Effects.execute(observe_effect(now_unix() + 60), Runtime.new([]), opts: opts)
          observation
        end)

      assert_receive {:observe_entered, adapter, _, _}, 5_000
      send(adapter, :release_error)

      assert_receive {:result, %Observation.ObserveFailed{reason: %{"reason" => "adapter_error_for_parity"}}},
                     5_000

      assert_receive {:DOWN, ^caller_mon, :process, ^caller, :normal}, 5_000

      # malformed runner envelope and malformed {:failed, diagnostic}: closed as ObserveFailed(invalid_return)
      bad_runners = [
        fn _c, _d -> :garbage end,
        fn _c, _d -> {:failed, %{not: :a_diagnostic}} end,
        fn _c, _d -> {:failed, %{kind: :error, class: @canary, digest: @canary, frames: -1}} end,
        # isolated (m_1788751778000): a valid depth with a class outside the closed vocabulary; a valid class with a
        # raw digest; a valid class with an upper-case digest
        fn _c, _d ->
          {:failed, %{kind: :error, class: @canary, digest: Diagnostic.describe(:probe)["digest"], frames: 0}}
        end,
        fn _c, _d -> {:failed, %{kind: :error, class: "atom", digest: @canary, frames: 0}} end,
        fn _c, _d ->
          {:failed, %{kind: :error, class: "atom", digest: "SHA256:" <> String.duplicate("A", 64), frames: 0}}
        end,
        fn _c, _d -> %URI{} end,
        fn _c, _d -> {:ok} end
      ]

      for bad <- bad_runners do
        {observation, _} =
          Effects.execute(observe_effect(now_unix() + 60), Runtime.new([]),
            opts: [dispatch: H.OkDispatch, dispatch_opts: [], adapter_runner: bad]
          )

        assert %Observation.ObserveFailed{reason: %{"reason" => "observe_invalid_return"} = reason} = observation
        assert Enum.sort(Map.keys(reason)) == ["digest", "reason", "result_class"]
        refute inspect(observation, limit: :infinity, printable_limit: :infinity) =~ @canary
      end

      # a task killed WITHOUT reply under the real subtree: closed failure with kind :exit, observed join, no hang
      log =
        capture_log(fn ->
          facts = start!(red_config(dir))
          assert_receive {:observe_fence, worker, %{op: op, fact: {:armed, _}}}, 30_000
          assert_receive {:observe_entered, task, _, _}, 30_000
          server_mon = Process.monitor(facts.server)
          Process.exit(task, :kill)
          assert_receive {:observe_fence, ^worker, %{op: ^op, task: %{pid: ^task}, fact: {:death, :killed}}}, 10_000
          assert Server.await(facts.server, 30_000) == {:error, %{clause: "run_server_down"}}
          assert_receive {:DOWN, ^server_mon, :process, _, {:run_step_failed, %{kind: :exit}}}, 10_000
          stop!(facts.root)
        end)

      refute log =~ @canary
    end
  end

  describe "RED: lifetime, retention, redaction (O-10, O-11b, O-12, O-13)" do
    test "O-10 orderly stop, HARD Worker kill and task-supervisor kill: third-party joins; supervisor death -> owner loss",
         %{dir: dir} do
      for mode <- [:orderly, :hard_worker, :task_supervisor] do
        sub = Path.join(dir, Atom.to_string(mode))
        File.mkdir_p!(sub)
        facts = start!(red_config(sub))
        assert_receive {:observe_fence, worker, %{fact: {:task_supervisor, task_sup}}}, 30_000
        assert_receive {:observe_fence, ^worker, %{op: _, fact: {:armed, _}}}, 30_000
        assert_receive {:observe_entered, task, _, _}, 30_000
        monitors = for pid <- [worker, task_sup, task], do: {pid, Process.monitor(pid)}

        case mode do
          :orderly -> stop!(facts.root)
          :hard_worker -> Process.exit(worker, :kill)
          :task_supervisor -> Process.exit(task_sup, :kill)
        end

        for {pid, mon} <- monitors, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, 10_000)

        if mode != :orderly do
          assert match?(
                   {:error, %{clause: "run_effect_owner_down", writer_generation: _}},
                   Server.await(facts.server, 10_000)
                 )

          stop!(facts.root)
        end
      end
    end

    test "O-11b PortGate under the product Worker: owner Port + memo + handle survive an interrupted observe; settle exactly once",
         %{dir: dir} do
      test = self()
      seams = port_gate_seams(observe_fence_observer: test, observe_fence_cap_ms: @cap_ms, clock: ReportingClock)
      {worker, cap} = standalone_worker!(seams)
      gate_ref = make_ref()
      send(worker, {:execute, cap, 1, gate_ref, prepare_gate(dir), nil})
      assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
      %{runtime: runtime} = :sys.get_state(worker, 2_000)
      handle = Runtime.handle(runtime, {"gr_0001", 1})
      assert handle.port == port and handle.memo == memo
      obs_ref = make_ref()
      send(worker, {:execute, cap, 1, obs_ref, observe_effect(FixedClock.unix_now() + 1), nil})
      assert_receive {:observe_fence, ^worker, %{op: %{cap: ^cap, gen: 1, ref: ^obs_ref}, fact: {:armed, _}}}, 5_000
      assert_receive {:observe_entered, task, _, _}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^obs_ref, ^worker, %Observation.TimedOut{}}, 15_000
      refute Process.alive?(task)
      %{runtime: retained} = :sys.get_state(worker, 2_000)
      assert Runtime.handle(retained, {"gr_0001", 1}) == handle, "the retained handle survived the interruption"
      assert Port.info(port, :connected) == {:connected, worker}
      settle_ref = make_ref()
      send(worker, {:settle, cap, 1, settle_ref})
      assert_receive {:gate_owned, :settling, ^worker, ^port, ^memo}, 5_000
      assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000

      assert_receive {:settled, ^cap, 1, ^settle_ref, ^worker,
                      [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true}}]},
                     5_000

      again = make_ref()
      send(worker, {:settle, cap, 1, again})
      assert_receive {:settled, ^cap, 1, ^again, ^worker, []}, 5_000
    end

    test "O-12 redaction on a controlled owner: supervisor status required, post-return Worker status required and type-valid" do
      test = self()
      seams = standalone_seams(observe_fence_observer: test, clock: ReportingClock)

      log =
        capture_log(fn ->
          {worker, cap} = standalone_worker!(seams, fn -> %{fence_observer: test} end)
          assert_receive {:observe_fence, ^worker, %{fact: {:task_supervisor, task_sup}}}, 5_000
          ref = make_ref()
          send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})

          assert_receive {:observe_fence, ^worker,
                          %{op: %{ref: ^ref} = op, task: %{pid: task}, fact: {:task_started, task}}},
                         5_000

          assert_receive {:observe_entered, ^task, _, _}, 5_000
          sup_status = status_text(task_sup)
          assert sup_status != :unavailable, "the task supervisor is a required status source"
          refute sup_status =~ @canary
          task_status = status_text(task)

          IO.puts(
            "\n[O-12 recorded] task (arbitrary closure) status: #{if task_status == :unavailable, do: :unavailable, else: :returned}"
          )

          if task_status != :unavailable, do: refute(task_status =~ @canary)
          mon = Process.monitor(task)
          send(task, :fail_throw)
          assert_receive {:DOWN, ^mon, :process, ^task, reason}, 10_000
          refute inspect(reason, limit: :infinity) =~ @canary
          assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, :normal}}}, 10_000
          assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, _diagnostic}, 10_000
          assert Process.alive?(worker)
          {:status, ^worker, {:module, :gen_server}, items} = :sys.get_status(worker, 2_000)
          rendered = inspect(items, limit: :infinity, printable_limit: :infinity)
          refute rendered =~ @canary
          assert rendered =~ ":redacted"
        end)

      refute log =~ @canary
    end

    test "O-13 bootstrap closure: partial start fails Worker.init under the closed boundary; canary never in start logs; bootstrap redacted" do
      Process.flag(:trap_exit, true)

      log =
        capture_log(fn ->
          bootstrap = fn -> %{task_supervisor_start: fn -> {:error, @canary} end} end
          result = worker_module().start_link(self(), bootstrap)
          assert match?({:error, _}, result), "init fails under the closed failure boundary"
          refute inspect(result, limit: :infinity) =~ @canary
          # MEASURED at GREEN: no EXIT reaches the linked, trapping caller (the start protocol answers the
          # nack and flushes it), so the witnesses are the start result and the captured log
          refute_receive {:EXIT, _pid, _reason}, 200
        end)

      refute log =~ @canary
      {worker, _cap} = standalone_worker!(standalone_seams(), fn -> %{fence_observer: self()} end)
      status = status_text(worker)
      assert status != :unavailable and not (status =~ "fence_observer"), "the bootstrap is redacted from status"
    end

    test "O-13b before-GO kills: pre-allocation and allocated-before-GO are distinct; independent joins of Worker/task/supervisor" do
      test = self()

      seams =
        standalone_seams(observe_fence_observer: test, observe_fence_hold: %{before_go: test}, clock: ReportingClock)

      {worker, _cap} = standalone_worker!(seams, fn -> %{fence_observer: test} end)
      assert_receive {:observe_fence, ^worker, %{fact: {:task_supervisor, task_sup}}}, 5_000
      sup_mon = Process.monitor(task_sup)
      worker_mon = Process.monitor(worker)
      Process.exit(worker, :kill)
      assert_receive {:DOWN, ^worker_mon, :process, ^worker, :killed}, 5_000
      assert_receive {:DOWN, ^sup_mon, :process, ^task_sup, _}, 5_000
      {worker2, cap2} = standalone_worker!(seams, fn -> %{fence_observer: test} end)
      assert_receive {:observe_fence, ^worker2, %{fact: {:task_supervisor, task_sup2}}}, 5_000
      ref = make_ref()
      send(worker2, {:execute, cap2, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})

      assert_receive {:observe_fence_held, ^worker2, _token, held}, 5_000
      assert %{op: %{ref: ^ref}, task: %{pid: task2}, stage: :before_go} = held

      refute_receive {:observe_entered, _, _, _}, 100, "the adapter has not run: GO withheld"
      monitors = for pid <- [worker2, task_sup2, task2], do: {pid, Process.monitor(pid)}
      Process.exit(worker2, :kill)
      for {pid, mon} <- monitors, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, 5_000)
      refute_receive {:observe_entered, _, _, _}, 100
    end

    test "O-13c bootstrap rejects bare hold pid, unknown stage, invalid controller and unknown bootstrap key" do
      invalid = [
        %{fence_hold: self()},
        %{fence_hold: %{unreviewed_stage: self()}},
        %{fence_hold: %{before_go: @canary}},
        %{unreviewed_key: @canary}
      ]

      for value <- invalid do
        log =
          capture_log(fn ->
            result = worker_module().start_link(self(), fn -> value end)

            case result do
              {:ok, pid} -> track!(pid)
              _ -> :ok
            end

            assert match?({:error, _}, result)
            refute inspect(result, limit: :infinity) =~ @canary
            # MEASURED at GREEN: no EXIT reaches the linked, trapping caller (see O-13)
            refute_receive {:EXIT, _pid, _reason}, 200
          end)

        refute log =~ @canary
      end
    end
  end

  describe "GREEN witnesses under the boundary findings (O-15, O-16) and the resume ruling (O-17)" do
    test "O-15 RAW clock failure at the arm (each kind): closed failure, retained Port settled, no task, canary absent",
         %{dir: dir} do
      for kind <- [:error, :throw, :exit] do
        test = self()
        seams = port_gate_seams(observe_fence_observer: test, clock: FailingClock)
        FailingClock.script(nil)
        sub = Path.join(dir, Atom.to_string(kind))
        File.mkdir_p!(sub)

        log =
          capture_log(fn ->
            {worker, cap} = standalone_worker!(seams)
            gate_ref = make_ref()
            send(worker, {:execute, cap, 1, gate_ref, prepare_gate(sub), nil})
            assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
            assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
            FailingClock.script({:unix_now, kind, @canary})
            ref = make_ref()
            send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})
            assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, diagnostic}, 10_000
            expected = expected_diagnostic(kind)
            assert diagnostic.kind == kind and diagnostic.class == expected.class and diagnostic.digest == expected.digest
            assert diagnostic.cleanup == %{attempts: 1, settled: 1, unproven: 0}
            assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000
            refute_receive {:observe_fence, ^worker, %{fact: {:task_allocated, _}}}, 100
            refute_receive {:observe_entered, _, _, _}, 100
            assert Process.alive?(worker)
            refute inspect(diagnostic, limit: :infinity) =~ @canary
            FailingClock.script(nil)
          end)

        refute log =~ @canary
      end
    end

    test "O-16 RAW clock failure after GO (each kind): live task killed and joined, then the closed failure settles the Port",
         %{dir: dir} do
      for kind <- [:error, :throw, :exit] do
        test = self()
        seams = port_gate_seams(observe_fence_observer: test, clock: FailingClock)
        FailingClock.script(nil)
        sub = Path.join(dir, Atom.to_string(kind))
        File.mkdir_p!(sub)

        log =
          capture_log(fn ->
            {worker, cap} = standalone_worker!(seams)
            gate_ref = make_ref()
            send(worker, {:execute, cap, 1, gate_ref, prepare_gate(sub), nil})
            assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
            assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
            # monotonic read 1 is the arm; read 2 is the first chunk after GO
            FailingClock.script({:monotonic, 2, kind, @canary})
            ref = make_ref()
            send(worker, {:execute, cap, 1, ref, observe_effect(FixedClock.unix_now() + 600), nil})

            assert_receive {:observe_fence, ^worker,
                            %{op: %{ref: ^ref} = op, task: %{pid: task}, fact: {:task_started, task}}},
                           5_000

            task_mon = Process.monitor(task)
            assert_receive {:observe_fence, ^worker, %{op: ^op, fact: :kill_requested}}, 10_000
            assert_receive {:observe_fence, ^worker, %{op: ^op, fact: {:death, _}}}, 5_000
            assert_receive {:DOWN, ^task_mon, :process, ^task, _}, 5_000
            assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, diagnostic}, 10_000
            expected = expected_diagnostic(kind)
            assert diagnostic.kind == kind and diagnostic.class == expected.class and diagnostic.digest == expected.digest
            assert diagnostic.cleanup == %{attempts: 1, settled: 1, unproven: 0}
            assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000
            assert Process.alive?(worker)
            refute inspect(diagnostic, limit: :infinity) =~ @canary
            FailingClock.script(nil)
          end)

        refute log =~ @canary
      end
    end

    test "O-17 positive unexpired resume from a GENERATED awaiting prefix: deadline ahead at arm, observed first, never re-delivered",
         %{dir: dir} do
      facts = start!(config(dir, BlockingObserve, default_assignment_timeout_s: 600))
      assert_receive {:observe_entered, _task, "as_0001", _}, 30_000
      Process.exit(facts.root, :kill)
      assert wait_for(fn -> not Process.alive?(facts.root) end, 5_000)
      # the killed tree's Writer ownership is released by the arbiter's DOWN handling; a resume before that is
      # refused as second_live_writer (measured once); wait for :none, never retry blindly
      assert wait_for(fn -> Ownership.status(dir) == :none end, 5_000)
      seeded = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)
      assert "assignment_observation_started" in types(journal(dir))
      base = resume_opts()
      dispatch_opts = base |> Keyword.get(:dispatch_opts, []) |> Keyword.put(:collector, collector())

      opts =
        base
        |> Keyword.drop(@owned)
        |> Keyword.put(:supervisor_instance, @instance)
        |> Keyword.put(:dispatch, CountingDispatch)
        |> Keyword.put(:dispatch_opts, dispatch_opts)
        |> Keyword.merge(observe_fence_observer: self(), clock: ReportingClock)

      cfg = %{
        run_dir: dir,
        mode: :resume,
        spec: H.spec("gated_run_seed"),
        plan: H.plan("gated_run_seed"),
        opts: opts,
        trace: collector()
      }

      facts2 = start!(cfg)
      work2 = facts2.work
      # pinned to the RESUMED tree's Work supervisor: the generation tree's own :worker start is still queued here
      assert_receive {:run_child_started, ^work2, :worker, worker}, 10_000
      {_op, %{deadline_unix: d}, reads} = arm_snapshot!(worker)
      assert [unix_now: unix, monotonic_ms: _] = reads
      assert d > unix, "the retained deadline is still ahead at the owner arm"
      assert d == durable_deadline!(dir)
      assert_receive {:observe_fence, ^worker, %{fact: {:task_started, _}}}, 5_000
      result = Server.await(facts2.server, 60_000)
      OwnedHarness.flush!()
      calls = for {:dispatch_called, name, assignment_id, _} <- mailbox(self()), do: {name, assignment_id}
      assert match?([{:observe, "as_0001"} | _], calls), "the pending assignment is observed first"
      refute {:deliver, "as_0001"} in calls
      refute {:reconcile, "as_0001"} in calls
      assert Enum.take(journal(dir), length(seeded)) == Enum.map(seeded, &Jason.decode!/1)
      assert match?({:ok, _}, result)
      stop!(facts2.root)
    end
  end

  describe "OG-M1 companions (m_1788754467000): the carrier is validated at the Worker boundary with a retained Port" do
    for {label, diagnostic} <- [
          {"nil carrier", nil},
          {"unvalidated carrier",
           %{kind: :error, class: "OBSERVE-DEADLINE-CARRIER-CANARY", digest: "OBSERVE-DEADLINE-CARRIER-CANARY", frames: 0}}
        ] do
      test "O-18 a non-Observe #{label} is an ordinary raw failure: closed diagnostic, Port settled, Worker alive",
           %{dir: dir} do
        test = self()
        seams = port_gate_seams(dispatch: ForgedSnapshot, dispatch_opts: [diagnostic: unquote(Macro.escape(diagnostic))])

        log =
          capture_log(fn ->
            {worker, cap} = standalone_worker!(seams)
            gate_ref = make_ref()
            send(worker, {:execute, cap, 1, gate_ref, prepare_gate(dir), nil})
            assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
            assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
            ref = make_ref()
            snapshot = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}
            send(worker, {:execute, cap, 1, ref, snapshot, nil})
            assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, closed}, 10_000
            assert Effects.AdapterRunner.diagnostic?(Map.delete(closed, :cleanup))
            assert closed.kind == :error and closed.cleanup == %{attempts: 1, settled: 1, unproven: 0}
            refute inspect(closed, limit: :infinity) =~ "CARRIER-CANARY"
            assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000
            assert Process.alive?(worker)
            send(test, :companion_done)
          end)

        assert_received :companion_done
        refute log =~ "CARRIER-CANARY"
      end
    end

    test "O-18 control: a VALID carrier from a non-Observe adapter is honoured unchanged plus the cleanup summary",
         %{dir: dir} do
      valid = %{kind: :throw, class: "atom", digest: Diagnostic.describe(:probe)["digest"], frames: 7}
      seams = port_gate_seams(dispatch: ForgedSnapshot, dispatch_opts: [diagnostic: valid])
      {worker, cap} = standalone_worker!(seams)
      gate_ref = make_ref()
      send(worker, {:execute, cap, 1, gate_ref, prepare_gate(dir), nil})
      assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 5_000
      assert_receive {:effect_result, ^cap, 1, ^gate_ref, ^worker, %Observation.GatePrepared{}}, 5_000
      ref = make_ref()
      snapshot = %Effect.SnapshotArtifact{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}
      send(worker, {:execute, cap, 1, ref, snapshot, nil})
      assert_receive {:effect_failed, ^cap, 1, ^ref, ^worker, closed}, 10_000
      assert closed == Map.put(valid, :cleanup, %{attempts: 1, settled: 1, unproven: 0})
      assert_receive {:gate_closed, ^worker, ^port, ^memo}, 5_000
      assert Process.alive?(worker)
    end
  end
end
