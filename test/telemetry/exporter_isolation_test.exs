defmodule AiOrchestrator.Telemetry.ExporterIsolationTest do
  @moduledoc """
  NS-26.F.001 (north-star-architecture.org:113 and 526-529): observability failure never changes
  journal facts, retries or exit status. Pinned over the FULL command path -- `Commands.invoke/4` ->
  `Run.Executor` -> owned run subtree -> `Journal.Writer` -> `events.jsonl` -- by running the same
  start command with and without a failing span exporter and requiring byte-identical journal bytes
  and an identical command result term.

  Spans are created by a test handler on the command lifecycle telemetry events through a NAMED
  OpenTelemetry tracer provider that this test starts with its own exporter, so the perturbation
  never depends on `OTEL_*` environment and the global provider is untouched. The handler also
  force-flushes a probe span on `:start`, so the first export attempt (and its failure) runs while
  the domain path is executing, not after it. Every perturbation proves it was actually exercised
  (a timestamped message from inside the failing path, or the runner's death under the export
  timeout); a row whose failure never happened cannot pass.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @start [:ai_orchestrator, :commands, :invoke, :start]
  @stop [:ai_orchestrator, :commands, :invoke, :stop]
  @exception [:ai_orchestrator, :commands, :invoke, :exception]
  @events [@start, @stop, @exception]

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-18T00:00:00Z", unix: 1_789_603_200}
  @command_id "cmd_01J9X3T2QF5G7H8K1N3P"
  @run_id "run_ns26_f001"
  @instance "sup_ns26_f001"
  # Server-owned bindings a caller never supplies (exactly `Run.Executor`'s list)
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]

  # ---- the failing exporter: one module, one failure class per provider ----
  #
  # `:otel_exporter_traces` callbacks (the SDK's `otel_exporter_traces:export/3` dispatch). `export/3`
  # runs in the batch processor's runner process, never in a domain process. The switch lets the test
  # disarm the failure once the row has measured it, so the processor's terminate-time export does
  # not repeat the failure outside the measured window.
  defmodule FailingExporter do
    @moduledoc false
    @behaviour :otel_exporter_traces

    @impl true
    def init(%{mode: :init_raise, test: test, switch: switch} = state) do
      if Agent.get(switch, & &1) do
        send(test, {:init_attempted, :init_raise, self(), System.monotonic_time()})
        raise "exporter refused to initialise"
      else
        {:ok, state}
      end
    end

    def init(%{} = state), do: {:ok, state}

    @impl true
    def export(tab, _resource, %{mode: mode, test: test, switch: switch}) do
      if Agent.get(switch, & &1) do
        send(test, {:export_attempted, mode, self(), :ets.info(tab, :size), System.monotonic_time()})
        # the hang row asserts this runner is KILLED; it must be watched before it hangs (see below)
        if mode == :hang, do: watch_runner!(test)
        fail(mode)
      else
        :ok
      end
    end

    @impl true
    def shutdown(_state), do: :ok

    # Reports how this runner dies, to a watcher established BEFORE it hangs.
    #
    # The hang row cannot learn that from a monitor the TEST sets up after the perturbed run: the
    # processor kills the runner `exporting_timeout_ms` after the export begins, the test reaches its
    # monitor only after the rest of that run and a journal byte comparison, and `Process.monitor/1`
    # on an already-dead process answers `{:DOWN, ref, :process, pid, :noproc}` -- `:noproc` records
    # that the process is gone and NOTHING about how it went, so a slow enough machine turns a
    # correct kill into a failure. That is not hypothetical: the window between the export attempt
    # and that monitor measures ~33 ms idle and 216-1941 ms under CPU oversubscription, either side
    # of the 200 ms timeout.
    #
    # The watcher is therefore created here, by the runner itself, in the same breath as the export
    # and before it hangs, so it is already watching whatever the machine does afterwards, and it
    # reports the real exit reason. It must NOT be linked: `:kill` propagates along links and would
    # take the watcher down with its subject before it could report.
    #
    # The runner then WAITS for the watcher to confirm the monitor is up before hanging. Spawning is
    # asynchronous, so without this handshake a runner that died promptly could still outrun its own
    # watcher and reproduce the very `:noproc` this exists to eliminate. The watcher monitors before
    # it confirms, so even a kill delivered mid-handshake is observed rather than lost.
    defp watch_runner!(test) do
      runner = self()
      watching = make_ref()

      spawn(fn ->
        ref = Process.monitor(runner)
        send(runner, watching)

        receive do
          {:DOWN, ^ref, :process, ^runner, reason} -> send(test, {:runner_down, runner, reason})
        end
      end)

      receive do
        ^watching -> :ok
      after
        5_000 -> raise "the export runner watcher never established its monitor"
      end
    end

    # a soft failure the processor reports and drops
    defp fail(:not_retryable), do: :failed_not_retryable
    # a raise inside the exporter: caught by the processor's export guard
    defp fail(:raise), do: raise("exporter raised during export")
    # an exit inside the exporter: caught by the same guard
    defp fail(:exit), do: exit(:exporter_exited_during_export)
    # the runner process itself dies: the processor traps the EXIT and completes the export
    defp fail(:kill), do: Process.exit(self(), :kill)
    # an export that never returns: killed by the processor's exporting timeout
    defp fail(:hang), do: Process.sleep(:infinity)
    # after a disarmed init succeeded there is nothing left to fail
    defp fail(:init_raise), do: :ok
  end

  # ---- harness ----

  setup do
    dir = Path.join(System.tmp_dir!(), "ns26-f001-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, run_dir: Path.join(dir, "run")}
  end

  defp sha(term), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, Jason.encode!(term)), case: :lower)

  # The full command path, exactly as the executor tests drive it: the gated_run_seed scenario
  # under FixedClock/FixedId (reset before every run), a fixed instance and command id, the
  # journal written by the real Writer into `run_dir`. The directory is recreated so every run
  # starts from the same absent state and the same path bytes in `run_created`. Answers the
  # command result, the journal bytes and the monotonic instant the run finished.
  defp run_full_command_path!(run_dir) do
    File.rm_rf!(run_dir)
    File.mkdir_p!(run_dir)
    {_name, :run, "gated_run_seed", [], make} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    H.reset_seams()
    spec = H.spec("gated_run_seed")
    plan = H.plan("gated_run_seed")

    ctx =
      make.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: run_dir,
        spec: spec,
        plan: plan,
        spec_hash: sha(spec),
        plan_hash: sha(plan),
        supervisor_instance: @instance
      )

    result =
      Commands.invoke(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: @run_id,
        command_id: @command_id,
        now: @now,
        executor: AiOrchestrator.Run.Executor,
        executor_opts: ctx
      )

    {result, File.read!(Path.join(run_dir, "events.jsonl")), System.monotonic_time()}
  end

  # a perturbation under captured logging: the SDK reports exporter failures through Logger and
  # that report must not reach the test output; the value the function computed is what matters
  defp captured!(fun) when is_function(fun, 0) do
    parent = self()
    ref = make_ref()
    _log = capture_log(fn -> send(parent, {ref, fun.()}) end)

    receive do
      {^ref, value} -> value
    after
      0 -> flunk("the captured perturbation produced no value")
    end
  end

  defp run_perturbed!(run_dir), do: captured!(fn -> run_full_command_path!(run_dir) end)

  defp assert_completed_run!({result, bytes, _finished_at}) do
    assert {:ok, %{summary: %{"status" => "completed"}}} = result
    lines = String.split(bytes, "\n", trim: true)
    types = Enum.map(lines, &(&1 |> Jason.decode!() |> Map.fetch!("type")))
    assert "run_started" in types and "run_completed" in types
    assert length(lines) > 4
  end

  defp assert_same_outcome!({baseline_result, baseline_bytes, _}, {result, bytes, _}) do
    assert bytes == baseline_bytes, "journal bytes changed under the perturbation"
    assert result == baseline_result, "the command result changed under the perturbation"
  end

  # A named tracer provider with ONE batch processor over the given exporter. The base config is the
  # SDK's own merge (so the sampler, id generator and limits are the ordinary ones); only the
  # processors are replaced, after the merge, so no OTEL_* variable can substitute another exporter.
  # The processor is named explicitly: a lone builtin processor is otherwise registered as `global`,
  # which collides with the global provider's processor and would leave this provider processor-less.
  defp start_provider!(exporter) do
    suffix = System.unique_integer([:positive])
    name = :"ns26_f001_provider_#{suffix}"
    processor = %{name: :"ns26_f001_processor_#{suffix}", exporter: exporter, scheduled_delay_ms: 20}

    config =
      :opentelemetry
      |> Application.get_all_env()
      |> :otel_configuration.merge_with_os()
      |> Map.put(:processors, [{:otel_batch_processor, Map.put(processor, :exporting_timeout_ms, 200)}])

    assert {:ok, pid} = :otel_tracer_provider_sup.start(name, config)
    on_exit(fn -> :supervisor.terminate_child(:otel_tracer_provider_sup, pid) end)
    tracer = :otel_tracer_provider.get_tracer(name, :ns26_f001, "0", :undefined)
    refute match?({:otel_tracer_noop, _}, tracer), "the named provider answered no tracer"
    {name, pid, tracer}
  end

  # The span-creating handler, in the invoking process, through the named provider's tracer: on
  # `:start` a probe span is opened, ended and force-flushed (so an export runs while the domain
  # path executes) and the invocation span is opened; on `:stop`/`:exception` it is ended. This is
  # the shape the application-boundary handler (NS-26.F.000) will have; here it is the perturbation.
  defp attach_span_handler!(name, tracer) do
    id = "ns26-f001-spans-#{System.unique_integer([:positive])}"
    config = %{provider: name, tracer: tracer, test: self()}
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.span_handler/4, config)
    on_exit(fn -> :telemetry.detach(id) end)
    id
  end

  def span_handler(event, _measurements, %{invocation_ref: ref}, %{provider: name, tracer: tracer, test: test}) do
    case List.last(event) do
      :start ->
        :otel_span.end_span(:otel_tracer.start_span(tracer, :"ai_orchestrator.commands.probe", %{}))
        :ok = :otel_tracer_provider.force_flush(name)
        Process.put({__MODULE__, ref}, :otel_tracer.start_span(tracer, :"ai_orchestrator.commands.invoke", %{}))

      _terminal ->
        case Process.delete({__MODULE__, ref}) do
          nil -> :ok
          span -> :otel_span.end_span(span)
        end

        send(test, {:span_ended, ref})
    end
  end

  defp failing_exporter!(mode) do
    {:ok, switch} = Agent.start_link(fn -> true end)
    {{FailingExporter, %{mode: mode, test: self(), switch: switch}}, switch}
  end

  defp disarm!(switch), do: Agent.update(switch, fn _ -> false end)

  # A local endpoint the test controls: it accepts every connection and closes it at once, so the
  # real OTLP exporter reaches a peer that refuses the export. Every accepted connection is reported.
  defp refusing_endpoint! do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    test = self()
    spawn_link(fn -> accept_loop(listen, test) end)
    on_exit(fn -> :gen_tcp.close(listen) end)
    port
  end

  defp accept_loop(listen, test) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        send(test, {:endpoint_hit, self(), System.monotonic_time()})
        :gen_tcp.close(socket)
        accept_loop(listen, test)

      {:error, _closed} ->
        :ok
    end
  end

  # The runner's own watcher (FailingExporter.watch_runner!/1) was monitoring before the export could
  # time out, so this reads the reason the runner ACTUALLY died of rather than racing to observe it.
  # `^runner` pins the report to the runner that made the measured attempt, so a later export cycle's
  # runner cannot answer for it. The row still fails if the timeout never kills the runner (nothing
  # arrives within 5 s) and if it dies of anything other than the kill.
  defp assert_runner_killed!(runner) do
    assert_receive {:runner_down, ^runner, reason}, 5_000
    assert reason == :killed, "the hung export runner was not killed by the exporting timeout: #{inspect(reason)}"
  end

  # ---- rows ----

  describe "control" do
    test "R1 the full command path is byte-deterministic without any perturbation", %{run_dir: run_dir} do
      first = run_full_command_path!(run_dir)
      second = run_full_command_path!(run_dir)
      assert_completed_run!(first)
      assert_same_outcome!(first, second)
    end
  end

  # one exporter failure class end to end: baseline run, named provider over the failing exporter,
  # span handler, perturbed run, proof of the attempt inside the run, identical outcome, provider
  # still alive; answers the runner pid that made the first attempt
  defp exporter_failure_row!(run_dir, mode) do
    baseline = run_full_command_path!(run_dir)
    assert_completed_run!(baseline)

    {exporter, switch} = failing_exporter!(mode)
    {name, provider, tracer} = start_provider!(exporter)
    attach_span_handler!(name, tracer)

    {_result, _bytes, finished_at} = perturbed = run_perturbed!(run_dir)

    assert_receive {:span_ended, _ref}, 5_000
    assert_receive {:export_attempted, ^mode, runner, size, attempted_at}, 5_000
    assert is_integer(size) and size >= 1, "the export table carried no span"
    assert attempted_at < finished_at, "the first export attempt did not overlap the domain run"

    assert_same_outcome!(baseline, perturbed)
    assert Process.alive?(provider), "the failing exporter took its provider down"
    disarm!(switch)
    runner
  end

  describe "exporter failure classes (NS-26.F.001)" do
    for mode <- [:not_retryable, :raise, :exit, :kill] do
      test "R2 #{mode}: a failing exporter leaves the journal bytes and the command result identical",
           %{run_dir: run_dir} do
        _runner = exporter_failure_row!(run_dir, unquote(mode))
      end
    end

    test "R2 hang: an export that never returns is killed by the exporting timeout and leaves everything identical",
         %{run_dir: run_dir} do
      runner = exporter_failure_row!(run_dir, :hang)
      assert_runner_killed!(runner)
    end

    test "R2 init_raise: an exporter that cannot initialise leaves the journal bytes and the result identical",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      {exporter, switch} = failing_exporter!(:init_raise)

      # the exporter initialises at provider start, so the start is inside the captured window too
      {provider, perturbed} =
        captured!(fn ->
          {name, provider, tracer} = start_provider!(exporter)
          attach_span_handler!(name, tracer)
          {provider, run_full_command_path!(run_dir)}
        end)

      {_result, _bytes, finished_at} = perturbed

      assert_receive {:init_attempted, :init_raise, _processor, attempted_at}, 5_000
      assert attempted_at < finished_at, "the first initialisation attempt did not overlap the domain run"
      assert_receive {:span_ended, _ref}, 5_000
      assert_same_outcome!(baseline, perturbed)
      assert Process.alive?(provider), "the uninitialisable exporter took its provider down"
      disarm!(switch)
    end

    test "R2 endpoint: the real OTLP exporter against a refusing local endpoint leaves everything identical",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      port = refusing_endpoint!()
      exporter = {:opentelemetry_exporter, %{endpoints: ["http://127.0.0.1:#{port}"], protocol: :http_protobuf}}
      {name, provider, tracer} = start_provider!(exporter)
      attach_span_handler!(name, tracer)

      {_result, _bytes, finished_at} = perturbed = run_perturbed!(run_dir)

      assert_receive {:span_ended, _ref}, 5_000
      assert_receive {:endpoint_hit, _acceptor, hit_at}, 5_000
      assert hit_at < finished_at, "the first refused export did not overlap the domain run"
      assert_same_outcome!(baseline, perturbed)
      assert Process.alive?(provider), "the refused export took its provider down"
    end
  end

  describe "handler failure (the product's own telemetry boundary)" do
    test "R3 a raising lifecycle handler is detached by :telemetry and leaves the journal bytes identical",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      id = "ns26-f001-raising-#{System.unique_integer([:positive])}"
      test = self()

      :ok =
        :telemetry.attach_many(
          id,
          @events,
          fn _event, _measurements, _metadata, _config ->
            send(test, {:handler_raising, id})
            raise "observer failure"
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)

      perturbed = run_perturbed!(run_dir)

      assert_receive {:handler_raising, ^id}, 5_000
      assert_same_outcome!(baseline, perturbed)
      refute Enum.any?(:telemetry.list_handlers(@start), &(&1.id == id)), "the raising handler was not detached"
    end
  end

  # NS-03.F.000 / NS-03.F.001. The raising-handler row above pins the FACT half of "observability
  # failure changes no domain behavior". These rows pin what is left, and they exist because
  # `commands/telemetry.ex:21-24` states the rest as an EXPECTATION that nothing enforces:
  # `:telemetry` runs handlers synchronously in the invoking process with no timeout and no
  # supervision, and `Commands.Telemetry.span/4` calls `:telemetry.execute/3` directly. So the honest
  # statement of this boundary's guarantee is "an observability failure changes no FACT", never "a
  # handler has no effect": a handler demonstrably CAN change timing, and R4 asserts the delay rather
  # than asserting its absence. Whether a handler-induced delay may consume a recorded gate or
  # observation deadline is a separate question for NS-18 / NS-19 and is not answered here.
  describe "handler effects that are real (NS-03.F.001, stated rather than assumed)" do
    @blocking_ms 300

    test "R4 a blocking handler delays the invocation itself and leaves the journal bytes and the result identical",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      test = self()
      observer = "ns26-f001-observer-#{System.unique_integer([:positive])}"
      blocking = "ns26-f001-blocking-#{System.unique_integer([:positive])}"

      # the product's OWN measurement, read off the terminal event: `emit/3` computes
      # `duration = now - start_mono` with `start_mono` taken before the `:start` handlers ran, so a
      # handler that held the calling process during `:start` is inside this number by construction --
      # and is outside it if handlers ever stop running in the calling process.
      :ok =
        :telemetry.attach_many(
          observer,
          [@stop, @exception],
          fn _event, %{duration: duration}, %{invocation_ref: ref}, _config ->
            send(test, {:observed_duration, ref, duration})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(observer) end)

      :ok =
        :telemetry.attach(
          blocking,
          @start,
          fn _event, _measurements, %{invocation_ref: ref}, _config ->
            entered = System.monotonic_time()
            Process.sleep(@blocking_ms)
            send(test, {:handler_blocked, ref, System.monotonic_time() - entered})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(blocking) end)

      perturbed = run_perturbed!(run_dir)

      assert_receive {:handler_blocked, ref, held}, 5_000
      assert_receive {:observed_duration, ^ref, duration}, 5_000

      held_ms = System.convert_time_unit(held, :native, :millisecond)
      duration_ms = System.convert_time_unit(duration, :native, :millisecond)

      assert held_ms >= @blocking_ms, "the blocking handler did not hold the process: #{held_ms} ms"

      assert duration_ms >= held_ms,
             "the handler did not block the invocation: the invocation measured #{duration_ms} ms " <>
               "while its own handler held the calling process for #{held_ms} ms"

      # the fact half: the invocation was slower and nothing else about it moved
      assert_same_outcome!(baseline, perturbed)
    end

    test "R5 a handler that returns a large term changes no fact: a handler's return value is discarded",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      id = "ns26-f001-large-term-#{System.unique_integer([:positive])}"
      test = self()

      :ok =
        :telemetry.attach_many(
          id,
          @events,
          fn _event, _measurements, %{invocation_ref: ref}, _config ->
            send(test, {:large_term_handler, ref})
            :binary.copy("x", 1_000_000)
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)

      perturbed = run_perturbed!(run_dir)

      assert_receive {:large_term_handler, _ref}, 5_000
      assert_same_outcome!(baseline, perturbed)

      assert Enum.any?(:telemetry.list_handlers(@start), &(&1.id == id)),
             "a handler that answered a value rather than raising must stay attached"
    end

    test "R6 a handler that detaches itself mid-invocation is not called again and changes no fact",
         %{run_dir: run_dir} do
      baseline = run_full_command_path!(run_dir)
      assert_completed_run!(baseline)

      id = "ns26-f001-self-detaching-#{System.unique_integer([:positive])}"
      test = self()

      :ok =
        :telemetry.attach_many(
          id,
          @events,
          fn event, _measurements, %{invocation_ref: ref}, _config ->
            :telemetry.detach(id)
            send(test, {:self_detached, ref, List.last(event)})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)

      perturbed = run_perturbed!(run_dir)

      assert_receive {:self_detached, _ref, :start}, 5_000
      assert_same_outcome!(baseline, perturbed)

      refute Enum.any?(:telemetry.list_handlers(@stop), &(&1.id == id)),
             "the self-detaching handler is still attached to the terminal event"

      refute_received {:self_detached, _ref, :stop}
    end
  end
end
