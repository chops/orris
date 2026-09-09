defmodule AiOrchestrator.Commands.CommandTelemetryRedTest do
  @moduledoc """
  RED rows for the command lifecycle telemetry contract at `Commands.invoke/4`
  (docs/contracts/command-lifecycle-telemetry.org, NS-26 slice T1): exactly one
  `:start` per invocation and exactly one `:stop` (normal return) or `:exception`
  (trappable escape), correlated by one `invocation_ref`, with a closed metadata
  allowlist that never carries args, ids, replies, reasons or stack frames, while
  every domain return term, exception, throw and exit is preserved byte for byte and
  the executor is invoked exactly once.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Moment

  @start [:ai_orchestrator, :commands, :invoke, :start]
  @stop [:ai_orchestrator, :commands, :invoke, :stop]
  @exception [:ai_orchestrator, :commands, :invoke, :exception]
  @events [@start, @stop, @exception]

  @command_id "cmd_01J9X3T2QF5G7H8K1N3P"
  @hash_a "sha256:" <> String.duplicate("a", 64)
  @hash_b "sha256:" <> String.duplicate("b", 64)
  @now %Moment{wall_ts: "2026-09-03T20:00:00Z", unix: 1_788_400_000}
  @operator %{"class" => "operator", "id" => "local_operator"}
  @marker "POISON_MARKER_7f3a9c"

  @start_keys [:invocation_ref, :verb, :actor_class]
  @stop_keys @start_keys ++ [:outcome, :stage, :clause, :result_class, :command_id_digest]
  @exception_keys @start_keys ++ [:kind, :class, :stack_depth, :command_id_digest]

  defmodule Executor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(command, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:executed, command})
      {:ok, %{accepted: true}}
    end
  end

  defmodule InvalidExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: {:unexpected, "POISON_MARKER_7f3a9c-provider-output"}
  end

  defmodule RejectingExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: {:error, %{clause: "journal_exists", detail: "POISON_MARKER_7f3a9c-detail"}}
  end

  defmodule RaisingExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: raise(RuntimeError, "POISON_MARKER_7f3a9c raised in the executor")
  end

  defmodule ThrowingExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: throw({:executor_threw, "POISON_MARKER_7f3a9c"})
  end

  defmodule ExitingExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: exit({:executor_exited, "POISON_MARKER_7f3a9c"})
  end

  # R2: controlled escapes with an EXACT reason and a synthetic stack, so preservation is byte-for-byte
  defmodule ControlledEscapeExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @stack [
      {:synthetic_mod, :synthetic_fun, 1, [file: ~c"synthetic.ex", line: 7]},
      {AiOrchestrator.Commands.CommandTelemetryRedTest, :marker, 0, [file: ~c"POISON_MARKER_7f3a9c.ex", line: 9]}
    ]
    def stack, do: @stack
    @impl true
    def execute(_command, opts), do: :erlang.raise(Keyword.fetch!(opts, :kind), Keyword.fetch!(opts, :reason), @stack)
  end

  # R2: an arbitrary nested return term must come back exactly
  defmodule NestedResultExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, opts), do: {:ok, Keyword.fetch!(opts, :result)}
  end

  # R3: collision controls -- a VALID error map mimicking the normalizer's own diagnostic, or spoofing a build clause
  defmodule CollidingExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, opts), do: {:error, Keyword.fetch!(opts, :rejection)}
  end

  # R1: both executors signal entry and wait for the release, so overlap is proven by protocol, not by sleeps
  defmodule BarrierExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, opts) do
      test = Keyword.fetch!(opts, :test_pid)
      send(test, {:entered, self(), Keyword.fetch!(opts, :tag)})

      receive do
        :release -> {:ok, %{accepted: true, tag: Keyword.fetch!(opts, :tag)}}
      after
        5_000 -> exit(:barrier_never_released)
      end
    end
  end

  # R3: trappable failures inside build-stage seams (clock, id generator) escape before any command exists
  defmodule RaisingClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    @impl true
    def wall_ts, do: "2026-09-03T20:00:00Z"
    @impl true
    def unix_now, do: raise("POISON_MARKER_7f3a9c clock seam failure")
    @impl true
    def monotonic_ms, do: 0
  end

  defmodule RaisingIdGenerator do
    @moduledoc false
    def generate, do: throw({:id_seam, "POISON_MARKER_7f3a9c"})
  end

  defmodule SlowExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, opts) do
      Process.sleep(Keyword.get(opts, :sleep_ms, 20))
      {:ok, %{accepted: true, tag: Keyword.get(opts, :tag)}}
    end
  end

  # ---- harness ----

  setup do
    id = "command-telemetry-#{System.unique_integer([:positive])}"
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  # the TEST message carries the emitting process (self() inside the handler); production metadata never does
  def forward(event, measurements, metadata, pid), do: send(pid, {:telemetry, event, measurements, metadata, self()})

  defp invoke(actor, verb, args, opts), do: Commands.invoke(actor, verb, args, opts)

  # later keys override the defaults (Keyword.fetch reads the first occurrence, so plain ++ would not)
  defp ok_opts(extra \\ []) do
    Keyword.merge(
      [run_id: "run_0001", command_id: @command_id, now: @now, executor: Executor, executor_opts: [test_pid: self()]],
      extra
    )
  end

  # exactly one start and one stop, correlated; returns {start_meta, stop_measurements, stop_meta}
  defp one_start_one_stop! do
    assert_receive {:telemetry, @start, %{monotonic_time: t0, system_time: st} = start_measurements, start_meta,
                    _emitter},
                   1_000

    assert is_integer(t0) and is_integer(st)

    assert_receive {:telemetry, @stop, %{monotonic_time: t1, duration: duration} = stop_measurements, stop_meta,
                    _emitter},
                   1_000

    assert is_integer(t1) and is_integer(duration) and duration >= 0
    assert duration == t1 - t0, "duration is terminal minus start monotonic time"
    assert Enum.sort(Map.keys(start_measurements)) == [:monotonic_time, :system_time]
    assert Enum.sort(Map.keys(stop_measurements)) == [:duration, :monotonic_time]
    assert start_meta.invocation_ref == stop_meta.invocation_ref
    assert is_reference(start_meta.invocation_ref)
    refute_receive {:telemetry, @start, _, _, _}, 50
    refute_receive {:telemetry, @stop, _, _, _}, 50
    refute_receive {:telemetry, @exception, _, _, _}, 50
    assert Enum.sort(Map.keys(start_meta)) == Enum.sort(@start_keys)
    assert Enum.sort(Map.keys(stop_meta)) == Enum.sort(@stop_keys)
    {start_meta, stop_measurements, stop_meta}
  end

  defp one_start_one_exception! do
    assert_receive {:telemetry, @start, %{monotonic_time: t0} = start_measurements, start_meta, _emitter}, 1_000

    assert_receive {:telemetry, @exception, %{monotonic_time: t1, duration: duration} = exception_measurements,
                    exception_meta, _emitter},
                   1_000

    assert is_integer(t1) and is_integer(duration) and duration >= 0
    assert duration == t1 - t0, "duration is terminal minus start monotonic time"
    assert Enum.sort(Map.keys(start_measurements)) == [:monotonic_time, :system_time]
    assert Enum.sort(Map.keys(exception_measurements)) == [:duration, :monotonic_time]
    assert start_meta.invocation_ref == exception_meta.invocation_ref
    refute_receive {:telemetry, @stop, _, _, _}, 50
    refute_receive {:telemetry, @exception, _, _, _}, 50
    refute_receive {:telemetry, @start, _, _, _}, 50
    assert Enum.sort(Map.keys(exception_meta)) == Enum.sort(@exception_keys)
    exception_meta
  end

  defp drain_events(acc \\ []) do
    receive do
      {:telemetry, event, measurements, metadata, emitter} ->
        drain_events([{event, measurements, metadata, emitter} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp no_marker!(events) do
    rendered = inspect(events, limit: :infinity, printable_limit: :infinity)
    refute rendered =~ @marker, "poisoned bytes reached telemetry: #{rendered}"
    rendered
  end

  # ---- rows ----

  describe "cardinality and correlation (RED)" do
    test "CT-1 an accepted invocation emits exactly one start and one stop with one correlation identity, the result and the executor call unchanged" do
      assert {:ok, %{accepted: true}} = invoke(@operator, "cancel", %{"reason" => "operator_cancel"}, ok_opts())
      assert_received {:executed, %Command{requested_by: %{"verb" => "cancel"}}}
      refute_received {:executed, _}

      {start_meta, _measurements, stop_meta} = one_start_one_stop!()
      assert start_meta.verb == "cancel" and start_meta.actor_class == "operator"
      assert stop_meta.outcome == :accepted and stop_meta.stage == :executor
      assert stop_meta.clause == nil and stop_meta.result_class == nil
      assert is_binary(stop_meta.command_id_digest) and byte_size(stop_meta.command_id_digest) == 16
      refute stop_meta.command_id_digest == @command_id
      refute inspect(stop_meta, limit: :infinity) =~ @command_id
    end

    test "CT-2 a build rejection emits one start and one stop with the closed clause and never reaches the executor" do
      agent = %{"class" => "agent", "id" => "writer", "run_id" => "run_0001", "assignment_id" => "as_0001"}

      assert {:error, %{clause: "command_not_authorized"}} =
               invoke(agent, "cancel", %{"reason" => "stop"}, ok_opts())

      refute_received {:executed, _}
      {start_meta, _m, stop_meta} = one_start_one_stop!()
      assert start_meta.actor_class == "agent" and start_meta.verb == "cancel"
      assert stop_meta.outcome == :rejected and stop_meta.stage == :build
      assert stop_meta.clause == "command_not_authorized"
      assert stop_meta.command_id_digest == nil
    end

    test "CT-3 malformed inputs still get exactly one start and one stop with the closed clause, and never the raw verb" do
      assert {:error, %{clause: "invalid_command_options"}} = invoke(@operator, "cancel", %{}, :not_a_keyword)
      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.clause == "invalid_command_options" and stop_meta.outcome == :rejected

      assert {:error, %{clause: "invalid_command_verb"}} = invoke(@operator, {:verb, @marker}, %{}, ok_opts())
      {start_meta, _m, stop_meta} = one_start_one_stop!()
      assert start_meta.verb == :invalid and stop_meta.clause == "invalid_command_verb"
      no_marker!([start_meta, stop_meta])
    end

    test "CT-16 non-list options keep the supplied actor's class label; only an unknown or malformed actor is :invalid" do
      for actor <- [
            @operator,
            %{"class" => "console", "id" => "console_1"},
            %{"class" => "agent", "id" => "writer", "run_id" => "run_0001", "assignment_id" => "a1"},
            %{"class" => "system", "id" => "boot", "reason" => "tail_truncate"}
          ],
          bad_opts <- [nil, false, %{}, :bad, "bad", 7] do
        assert {:error, %{clause: "invalid_command_options"}} = invoke(actor, "cancel", %{}, bad_opts)
        {start_meta, _m, stop_meta} = one_start_one_stop!()
        assert start_meta.actor_class == actor["class"] and stop_meta.actor_class == actor["class"]
        assert stop_meta.clause == "invalid_command_options" and stop_meta.stage == :build
      end

      for actor <- [nil, %{}, %{"class" => "root"}, %{"class" => :operator}, %{"class" => @marker}, "operator"] do
        assert {:error, %{clause: "invalid_command_options"}} = invoke(actor, "cancel", %{}, :not_a_keyword)
        {start_meta, _m, stop_meta} = one_start_one_stop!()
        assert start_meta.actor_class == :invalid and stop_meta.actor_class == :invalid
        no_marker!([start_meta, stop_meta])
      end
    end

    test "CT-4 executor port refusals are stop events at the :executor_port stage" do
      opts = Keyword.delete(ok_opts(), :executor)
      assert {:error, %{clause: "command_executor_required"}} = invoke(@operator, "cancel", %{"reason" => "x"}, opts)
      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.stage == :executor_port and stop_meta.clause == "command_executor_required"

      opts = [executor: __MODULE__] |> ok_opts() |> Keyword.delete(:executor) |> Keyword.put(:executor, __MODULE__)
      assert {:error, %{clause: "invalid_command_executor"}} = invoke(@operator, "cancel", %{"reason" => "x"}, opts)
      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.stage == :executor_port and stop_meta.clause == "invalid_command_executor"
    end

    test "CT-5 an invalid executor reply is an :invalid_executor_result stop carrying only the closed result class" do
      assert {:error, %{clause: "invalid_executor_result", result_class: "tuple"}} =
               invoke(@operator, "cancel", %{"reason" => "x"}, ok_opts(executor: InvalidExecutor))

      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :invalid_executor_result and stop_meta.stage == :executor
      assert stop_meta.result_class == "tuple" and stop_meta.clause == nil
      no_marker!([stop_meta])
    end

    test "CT-6 an executor rejection is reported as executor_rejected; the rejection map is returned unchanged and never emitted" do
      assert {:error, %{clause: "journal_exists", detail: detail}} =
               invoke(@operator, "cancel", %{"reason" => "x"}, ok_opts(executor: RejectingExecutor))

      assert detail =~ @marker
      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :rejected and stop_meta.stage == :executor
      assert stop_meta.clause == "executor_rejected"
      no_marker!([stop_meta])
    end
  end

  describe "exception preservation (RED)" do
    test "CT-7 a raising executor re-raises the same exception with the original stack; one start and one exception, no stop" do
      error =
        assert_raise RuntimeError, ~r/raised in the executor/, fn ->
          invoke(@operator, "cancel", %{"reason" => "x"}, ok_opts(executor: RaisingExecutor))
        end

      assert error.message =~ @marker

      # the original stacktrace is preserved: the executor frame is present
      stack =
        try do
          invoke(@operator, "cancel", %{"reason" => "x"}, ok_opts(executor: RaisingExecutor))
        rescue
          _e -> __STACKTRACE__
        end

      assert Enum.any?(stack, &match?({RaisingExecutor, :execute, 2, _}, &1))

      # two invocations above: two start/exception pairs, each correlated
      events = drain_events()
      starts = for {@start, _m, meta, _e} <- events, do: meta
      exceptions = for {@exception, _m, meta, _e} <- events, do: meta
      assert length(starts) == 2 and length(exceptions) == 2
      assert Enum.map(starts, & &1.invocation_ref) == Enum.map(exceptions, & &1.invocation_ref)
      assert [] == for({@stop, _m, meta, _e} <- events, do: meta)

      for meta <- exceptions do
        assert Enum.sort(Map.keys(meta)) == Enum.sort(@exception_keys)
        assert meta.kind == :error and meta.class == "map" and is_integer(meta.stack_depth) and meta.stack_depth > 0
      end

      no_marker!(events)
    end

    for {kind, reason} <- [
          error: {:exact_error_reason, "POISON_MARKER_7f3a9c"},
          throw: {:exact_throw_reason, "POISON_MARKER_7f3a9c"},
          exit: {:exact_exit_reason, "POISON_MARKER_7f3a9c"}
        ] do
      test "CT-8 #{kind}: an executor escape re-raises the identical kind, reason and full original stack; one start, one exception" do
        kind = unquote(kind)
        reason = unquote(Macro.escape(reason))
        opts = ok_opts(executor: ControlledEscapeExecutor, executor_opts: [kind: kind, reason: reason])

        caught =
          try do
            invoke(@operator, "cancel", %{"reason" => "x"}, opts)
          catch
            k, r -> {k, r, __STACKTRACE__}
          end

        assert {^kind, ^reason, stack} = caught
        assert stack == ControlledEscapeExecutor.stack(), "the synthetic stack must pass through unchanged"

        meta = one_start_one_exception!()

        assert meta.kind == kind and meta.class == "tuple" and
                 meta.stack_depth == length(ControlledEscapeExecutor.stack())

        assert meta.command_id_digest
        no_marker!([meta])
      end
    end

    test "CT-9 an arbitrary nested return term and an arbitrary rejection map come back exactly" do
      result = %{accepted: true, nested: %{marker: @marker, list: [1, 2, 3]}, tuple: {:a, "b"}}

      assert {:ok, ^result} =
               invoke(
                 @operator,
                 "cancel",
                 %{"reason" => "x"},
                 ok_opts(executor: NestedResultExecutor, executor_opts: [result: result])
               )

      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :accepted
      no_marker!([stop_meta])

      rejection = %{clause: "journal_exists", detail: @marker, nested: %{ids: ["run_" <> @marker]}}

      assert {:error, ^rejection} =
               invoke(
                 @operator,
                 "cancel",
                 %{"reason" => "x"},
                 ok_opts(executor: CollidingExecutor, executor_opts: [rejection: rejection])
               )

      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :rejected and stop_meta.stage == :executor and stop_meta.clause == "executor_rejected"
      no_marker!([stop_meta])
    end
  end

  describe "classification origin (RED)" do
    test "CT-14 an executor that mimics the normalizer's own diagnostic is still classified from the raw-reply boundary" do
      rejection = %{
        clause: "invalid_executor_result",
        result_class: "POISON_MARKER_7f3a9c-class",
        digest: "sha256:" <> @marker
      }

      assert {:error, ^rejection} =
               invoke(
                 @operator,
                 "cancel",
                 %{"reason" => "x"},
                 ok_opts(executor: CollidingExecutor, executor_opts: [rejection: rejection])
               )

      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :rejected and stop_meta.stage == :executor
      assert stop_meta.clause == "executor_rejected" and stop_meta.result_class == nil
      no_marker!([stop_meta])
    end

    test "CT-15 an executor that spoofs a build clause is classified at the executor stage, never as a build rejection" do
      rejection = %{clause: "command_not_authorized", class: "operator", verb: "cancel"}

      assert {:error, ^rejection} =
               invoke(
                 @operator,
                 "cancel",
                 %{"reason" => "x"},
                 ok_opts(executor: CollidingExecutor, executor_opts: [rejection: rejection])
               )

      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.stage == :executor and stop_meta.clause == "executor_rejected"
    end
  end

  describe "isolation and leakage (RED)" do
    test "CT-10 a raising handler cannot change the command outcome, and another handler still observes the lifecycle" do
      bad = "command-telemetry-bad-#{System.unique_integer([:positive])}"
      :ok = :telemetry.attach(bad, @start, fn _e, _m, _meta, _c -> raise "observer failure" end, nil)
      on_exit(fn -> :telemetry.detach(bad) end)

      assert {:ok, %{accepted: true}} = invoke(@operator, "cancel", %{"reason" => "x"}, ok_opts())
      assert_received {:executed, _}
      {_s, _m, stop_meta} = one_start_one_stop!()
      assert stop_meta.outcome == :accepted
    end

    test "CT-11 two overlapping invocations keep distinct correlation identities under one global observer" do
      # the setup handler is process-global: it forwards EVERY emission (with the emitter pid) to this test process
      test = self()

      tasks =
        for tag <- [:a, :b] do
          Task.async(fn ->
            invoke(
              @operator,
              "cancel",
              %{"reason" => "x"},
              ok_opts(executor: BarrierExecutor, executor_opts: [test_pid: test, tag: tag])
            )
          end)
        end

      # overlap proven by protocol: both executors are inside execute/2 before either is released
      assert_receive {:entered, pid_a, :a}, 5_000
      assert_receive {:entered, pid_b, :b}, 5_000
      assert pid_a != pid_b
      send(pid_a, :release)
      send(pid_b, :release)

      assert [{:ok, %{accepted: true, tag: :a}}, {:ok, %{accepted: true, tag: :b}}] =
               Enum.map(tasks, &Task.await(&1, 5_000))

      events = drain_events()
      assert length(events) == 4, "exactly four emissions (two starts, two stops): #{inspect(events)}"
      by_emitter = Enum.group_by(events, fn {_e, _m, _meta, emitter} -> emitter end)
      assert map_size(by_emitter) == 2

      refs =
        for {_emitter, evs} <- by_emitter do
          assert [{@start, _, s, _}, {@stop, _, e, _}] = Enum.sort_by(evs, fn {ev, _, _, _} -> ev end)
          assert s.invocation_ref == e.invocation_ref and e.outcome == :accepted
          s.invocation_ref
        end

      assert length(Enum.uniq(refs)) == 2
    end

    test "CT-11b trappable failures inside build-stage seams are one start and one exception with no command digest" do
      # the clock seam raises inside build/4: the escape is preserved and no command was built
      assert_raise RuntimeError, ~r/clock seam failure/, fn ->
        invoke(
          @operator,
          "cancel",
          %{"reason" => "x"},
          ok_opts() |> Keyword.delete(:now) |> Keyword.put(:clock, RaisingClock)
        )
      end

      meta = one_start_one_exception!()
      assert meta.kind == :error and meta.command_id_digest == nil
      no_marker!([meta])

      # the id generator seam throws inside build/4
      assert {:id_seam, thrown} =
               catch_throw(
                 invoke(
                   @operator,
                   "cancel",
                   %{"reason" => "x"},
                   ok_opts() |> Keyword.delete(:command_id) |> Keyword.put(:command_id_generator, RaisingIdGenerator)
                 )
               )

      assert thrown =~ @marker
      meta = one_start_one_exception!()
      assert meta.kind == :throw and meta.command_id_digest == nil
      refute_received {:executed, _}
      no_marker!([meta])
    end

    test "CT-12 poisoned actor, args, options and identities never appear in any event" do
      actor = %{"class" => "operator", "id" => "local_operator_" <> @marker}
      args = %{"reason" => "reason_" <> @marker}

      opts =
        ok_opts(
          run_id: "run_" <> @marker,
          executor_opts: [test_pid: self(), secret: @marker],
          note: @marker
        )

      opts = Keyword.put(opts, :run_id, "run_" <> @marker)
      assert {:ok, %{accepted: true}} = invoke(actor, "cancel", args, opts)
      events = drain_events()
      assert length(events) == 2
      rendered = no_marker!(events)
      refute rendered =~ @command_id
      refute rendered =~ "run_0001"
      refute rendered =~ "local_operator"
    end

    test "CT-13 Commands.build/4 alone emits no telemetry" do
      assert {:ok, %Command{}} =
               Commands.build(@operator, "start", %{"spec_hash" => @hash_a, "plan_hash" => @hash_b},
                 run_id: "run_0001",
                 command_id: @command_id,
                 now: @now
               )

      assert [] == drain_events()
    end
  end
end
