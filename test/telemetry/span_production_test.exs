defmodule AiOrchestrator.Telemetry.SpanProductionTest do
  @moduledoc """
  NS-26.F.000 (the emit sites) and NS-26.F.002 (only propagated identity joins spans), for the
  application-boundary span producer `AiOrchestrator.Telemetry.Handler` and the emit vocabulary
  `AiOrchestrator.Telemetry.Events` (docs/contracts/lifecycle-telemetry.org).

  Before this slice there was NO span producer in the product, so NS-26.F.002 could be neither
  satisfied nor falsified: `docs/contracts/command-lifecycle-telemetry.org:99` recorded that no
  production module attached a handler or created a span, and the exporter-isolation row's own
  handler described itself as "the shape the application-boundary handler will have". These rows
  exist so that what the producer actually does is MEASURED rather than described.

  What the rows deliberately do NOT do is assert a connected trace. The measured shape of one full
  command path is mostly ROOT spans, because the run's effects, journal appends and gates run in the
  owned subtree's own processes while the `run` span is open in the caller's, and this product
  propagates no trace context across process boundaries (the envelope's `traceparent` field exists
  and has no producer). Rows C2 and C3 pin that shape as it is -- including the roots -- so a future
  propagation slice makes them FAIL rather than inherit a claim nobody re-measured.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Query
  alias AiOrchestrator.Telemetry.Events
  alias AiOrchestrator.Telemetry.Handler
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  require Record

  Record.defrecord(:span, Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl"))

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-18T00:00:00Z", unix: 1_789_603_200}
  @command_id "cmd_01J9X3T2QF5G7H8K1N3P"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]

  # MEASURED: the span operations a sweep of every shipped scenario reaches (row B1). The residual is
  # declared in @not_swept rather than left silent: an operation nobody drives is an emit site that
  # could be wrong without anything noticing, and naming it is the difference between a measured
  # remainder and an unmeasured one.
  @swept [
    {:assignment, :dispatch},
    {:assignment, :observe},
    {:assignment, :read_review},
    {:assignment, :retain_prompt},
    {:assignment, :snapshot},
    {:dispatch, :deliver},
    {:dispatch, :observe},
    {:dispatch, :snapshot},
    {:gate, :await},
    {:gate, :prepare},
    {:gate, :release},
    {:host, :mount},
    {:journal, :append},
    {:run, :execute}
  ]

  # Declared, driven directly by rows A3/A4, and NOT reached by any shipped scenario. Each is a real
  # emit site in `lib/`; what is missing is a fixture that exercises it, not the producer. The
  # projection pair is reached by the read seam instead (row B2), which no scenario runs.
  @not_swept [
    {:assignment, :fetch_prompt},
    {:assignment, :reconcile_send},
    {:dispatch, :reconcile},
    {:gate, :reconcile},
    {:gate, :run},
    {:projection, :run_context},
    {:projection, :run_summary}
  ]

  # The command boundary's own span is not one of this module's operations; the sweep produces it
  # because every scenario runs through `Commands.invoke/4`.
  @command_pair {:commands, :invoke}

  # ---- a provider whose spans this test can read ----

  defmodule Collector do
    @moduledoc false
    @behaviour :otel_exporter_traces

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def export(tab, _resource, %{test: test, provider: provider}) do
      :ets.foldl(
        fn record, acc ->
          send(test, {:span_record, provider, record})
          acc
        end,
        :ok,
        tab
      )

      :ok
    end

    @impl true
    def shutdown(_state), do: :ok
  end

  # A NAMED provider with a simple processor over the collecting exporter. The processors are
  # replaced AFTER the SDK's own merge, exactly as the exporter-isolation rows do it, so no `OTEL_*`
  # variable can substitute another exporter and the global provider is untouched.
  defp collecting_handler! do
    suffix = System.unique_integer([:positive])
    name = :"span_production_provider_#{suffix}"

    config =
      :opentelemetry
      |> Application.get_all_env()
      |> :otel_configuration.merge_with_os()
      # The processor is named explicitly for the reason exporter_isolation_test.exs records: a lone
      # builtin processor is otherwise registered as `global` and collides with any other provider
      # this test starts, leaving the second one processor-less.
      |> Map.put(:processors, [
        {:otel_simple_processor,
         %{name: :"span_production_processor_#{suffix}", exporter: {Collector, %{test: self(), provider: name}}}}
      ])

    {:ok, provider} = :otel_tracer_provider_sup.start(name, config)
    on_exit(fn -> :supervisor.terminate_child(:otel_tracer_provider_sup, provider) end)

    tracer = :otel_tracer_provider.get_tracer(name, :span_production, "0", :undefined)
    refute match?({:otel_tracer_noop, _}, tracer), "the named provider answered no tracer"

    attach_handler!(tracer, suffix)
    {name, tracer}
  end

  defp attach_handler!(tracer, suffix) do
    id = "span-production-#{suffix}"
    assert {:ok, ^id} = Handler.attach(id: id, tracer: tracer)
    on_exit(fn -> :telemetry.detach(id) end)
    id
  end

  # Drains every span the collector has forwarded. The simple processor exports in its own process,
  # so the first message is waited for generously and the rest are taken until the stream goes quiet.
  # Messages are tagged with the provider that produced them, so two handler instances attached in
  # one test (D4) each drain only their own spans.
  defp spans(name) do
    :otel_tracer_provider.force_flush(name)
    drain(name, [], 2_000)
  end

  defp drain(name, acc, wait) do
    receive do
      {:span_record, ^name, record} -> drain(name, [record | acc], 200)
    after
      wait -> Enum.reverse(acc)
    end
  end

  defp rows(records) do
    by_id = Map.new(records, fn r -> {span(r, :span_id), span(r, :name)} end)

    Enum.map(records, fn r ->
      %{
        name: span(r, :name),
        trace: span(r, :trace_id),
        id: span(r, :span_id),
        parent: span(r, :parent_span_id),
        parent_name: if(span(r, :parent_span_id) == :undefined, do: nil, else: Map.get(by_id, span(r, :parent_span_id))),
        attributes: attributes(r)
      }
    end)
  end

  defp attributes(record) do
    case span(record, :attributes) do
      {:attributes, _limit, _value_limit, _dropped, map} -> map
      %{} = map -> map
    end
  end

  defp pairs(rows) do
    rows
    |> Enum.map(fn %{name: name} ->
      ["ai_orchestrator", domain, operation] = String.split(Atom.to_string(name), ".")
      {String.to_atom(domain), String.to_atom(operation)}
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp named(rows, domain, operation), do: Enum.filter(rows, &(&1.name == :"ai_orchestrator.#{domain}.#{operation}"))

  defp sha(term), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, Jason.encode!(term)), case: :lower)

  # One full command path through the Host route, which is the route that mounts the owned subtree
  # and therefore the one that produces a `host` span at all.
  defp run_case!(run_dir, {_label, mode, scenario, prior, make}, run_id) do
    File.rm_rf!(run_dir)
    File.mkdir_p!(run_dir)
    H.reset_seams()
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    ctx =
      make.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: run_dir,
        spec: spec,
        plan: plan,
        spec_hash: sha(spec),
        plan_hash: sha(plan),
        supervisor_instance: "sup_#{run_id}"
      )

    seed_prior!(run_dir, prior)

    Commands.invoke(@operator, verb(mode), %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
      run_id: run_id,
      command_id: @command_id,
      now: @now,
      # ---- A. the vocabulary is closed, and it is the vocabulary the product actually emits ----
      executor: AiOrchestrator.Host.Executor,
      executor_opts: ctx
    )
  end

  defp verb(:run), do: "start"
  defp verb(:resume), do: "resume"
  defp verb(:cancel), do: "cancel"

  defp seed_prior!(_run_dir, []), do: :ok

  defp seed_prior!(run_dir, lines) when is_list(lines),
    do: File.write!(Path.join(run_dir, "events.jsonl"), Enum.map_join(lines, "", &(&1 <> "\n")))

  setup do
    dir = Path.join(System.tmp_dir!(), "span-production-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, dir: dir, run_dir: Path.join(dir, "run")}
  end

  describe "A the emit vocabulary" do
    test "A1 the declared operations are exactly the swept ones plus the declared remainder" do
      assert Enum.sort(Events.operations()) == Enum.sort(@swept ++ @not_swept),
             "a declared operation is neither swept by a scenario nor listed in @not_swept"

      assert @swept -- @not_swept == @swept, "an operation cannot be both swept and unswept"
    end

    test "A2 events/0 is exactly the command boundary's three events plus start/stop/exception per operation " <>
           "plus one name per point event" do
      assert Events.events() == Events.command_events() ++ Events.span_events() ++ Events.point_events()
      assert length(Events.span_events()) == 3 * length(Events.operations())
      assert length(Events.point_events()) == length(Events.points())

      # The literal is asserted here; that it names the events `Commands.Telemetry` REALLY emits is
      # proved by B1, where the handler attached to exactly these names observes a real invocation.
      # A wrong literal would leave the handler attached to nothing and drop `{:commands, :invoke}`
      # out of B1's observed set.
      assert Events.command_events() == [
               [:ai_orchestrator, :commands, :invoke, :start],
               [:ai_orchestrator, :commands, :invoke, :stop],
               [:ai_orchestrator, :commands, :invoke, :exception]
             ]
    end

    test "A3 every declared span operation produces exactly one span carrying its own identifiers" do
      {name, _tracer} = collecting_handler!()

      for {domain, operation} <- Events.operations() do
        assert Events.span(domain, operation, %{run_id: "run_a3"}, fn -> {:ok, {domain, operation}} end) ==
                 {:ok, {domain, operation}}
      end

      rows = rows(spans(name))

      assert pairs(rows) == Enum.sort(Events.operations()),
             "a declared operation produced no span, or an undeclared one produced a span"

      assert length(rows) == length(Events.operations()), "an operation produced more than one span"

      for row <- rows do
        assert row.attributes[:"ai_orchestrator.run_id"] == "run_a3"
        assert row.attributes[:"ai_orchestrator.outcome"] == :ok
      end
    end

    test "A4 every declared point event produces exactly one span and answers :ok" do
      {name, _tracer} = collecting_handler!()

      for {domain, operation} <- Events.points() do
        assert Events.emit(domain, operation, %{run_id: "run_a4"}) == :ok
      end

      rows = rows(spans(name))

      assert Enum.sort(Enum.map(rows, & &1.name)) ==
               Enum.sort(for({d, o} <- Events.points(), do: :"ai_orchestrator.#{d}.#{o}"))

      for row <- rows, do: assert(row.attributes[:"ai_orchestrator.run_id"] == "run_a4")
    end

    # ---- B. production over the real command path (T2 boundary, T3 emit sites) ----

    test "A5 an undeclared pair emits nothing and still runs the function" do
      # The observer attaches to the UNDECLARED event names directly. Watching the span collector
      # instead would be a check that cannot fail: the handler is attached only to `Events.events/0`,
      # so an undeclared name produces no span even when it IS emitted, and the row would pass for
      # the wrong reason. Measured: with the emit guard removed, this row fails and the collector one
      # does not.
      undeclared = [
        [:ai_orchestrator, :not_a_domain, :not_an_operation, :start],
        [:ai_orchestrator, :not_a_domain, :not_an_operation, :stop],
        [:ai_orchestrator, :not_a_domain, :not_an_operation, :exception],
        [:ai_orchestrator, :not_a_domain, :not_an_operation]
      ]

      declared = [hd(Events.span_events()), hd(Events.point_events())]
      test = self()
      id = "span-production-undeclared-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach_many(
          id,
          undeclared ++ declared,
          fn event, _measurements, _metadata, _config -> send(test, {:emitted, event}) end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)

      assert Events.span(:not_a_domain, :not_an_operation, %{run_id: "run_a5"}, fn -> :sentinel end) == :sentinel
      assert Events.emit(:not_a_domain, :not_an_operation, %{run_id: "run_a5"}) == :ok

      refute_received {:emitted, [:ai_orchestrator, :not_a_domain, :not_an_operation, _phase]}
      refute_received {:emitted, [:ai_orchestrator, :not_a_domain, :not_an_operation]}

      # the control: the same observer DOES hear a declared pair, so the refutations above are about
      # the emit guard and not about an observer that was never listening
      [_prefix, domain, operation, _phase] = hd(Events.span_events())
      assert Events.span(domain, operation, %{run_id: "run_a5"}, fn -> :ok end) == :ok
      assert_receive {:emitted, [:ai_orchestrator, ^domain, ^operation, :start]}, 1_000

      [_p, point_domain, point_operation] = hd(Events.point_events())
      assert Events.emit(point_domain, point_operation, %{run_id: "run_a5"}) == :ok
      assert_receive {:emitted, [:ai_orchestrator, ^point_domain, ^point_operation]}, 1_000
    end
  end

  describe "B production over the full command path" do
    test "B1 a sweep of every shipped scenario produces exactly the swept operations", %{run_dir: run_dir} do
      {name, _tracer} = collecting_handler!()

      H.cases()
      |> Enum.with_index()
      |> Enum.each(fn {kase, index} -> run_case!(run_dir, kase, "run_sweep_#{index}") end)

      observed = name |> spans() |> rows() |> pairs()
      spans_observed = Enum.filter(observed, &(&1 in Events.operations()))

      assert spans_observed == Enum.sort(@swept),
             "the scenario sweep reached a different set of emit sites than @swept declares: " <>
               "unswept-but-reached #{inspect(spans_observed -- @swept)}, " <>
               "declared-but-unreached #{inspect(@swept -- spans_observed)}"

      # the command boundary's span and the attention point event ride the same sweep
      assert @command_pair in observed
      assert {:attention, :raised} in observed

      # the recovery point event is NOT reached by any shipped scenario: the writer's bounded tail
      # repair needs a truncated journal, which no fixture carries. Row A4 drives it directly.
      refute {:recovery, :repaired} in observed

      # and nothing outside the declared vocabulary appeared
      assert observed -- ([@command_pair] ++ Events.operations() ++ Events.points()) == []
    end

    test "B2 the run span nests under the host span that encloses it, and both name the same run", %{
      run_dir: run_dir,
      dir: dir
    } do
      {name, _tracer} = collecting_handler!()
      assert {:ok, _} = run_case!(run_dir, hd(H.cases()), "run_b2")

      rows = rows(spans(name))
      assert [host] = named(rows, :host, :mount)
      assert [run] = named(rows, :run, :execute)

      assert run.parent == host.id, "the run span is not a child of the host span that encloses it"
      assert run.trace == host.trace
      assert host.parent == :undefined, "the host span is not a root"
      assert host.attributes[:"ai_orchestrator.run_id"] == "run_b2"
      assert run.attributes[:"ai_orchestrator.run_id"] == "run_b2"

      # the command boundary cannot be a parent -- its digest exists only on the terminal event --
      # so command and run join on the identifier both carry
      assert [command] = named(rows, :commands, :invoke)
      assert command.parent == :undefined
      assert command.trace != run.trace

      assert command.attributes[:"ai_orchestrator.command_id_digest"] ==
               run.attributes[:"ai_orchestrator.command_id_digest"]

      assert is_binary(run.attributes[:"ai_orchestrator.command_id_digest"])

      # the projection spans are produced by the read seam, not by the run
      assert {:ok, _} = Query.run_summary("run", root: dir)
      assert {:ok, _} = Query.run_context("run", root: dir)
      projections = rows(spans(name))

      assert pairs(projections) == [{:projection, :run_context}, {:projection, :run_summary}]
      for row <- projections, do: assert(row.attributes[:"ai_orchestrator.run_id"] == "run_b2")
    end
  end

  describe "C correlation" do
    test "C1 every span of a run carries that run's identifier, which is how the rule says to follow it", %{
      run_dir: run_dir
    } do
      {name, _tracer} = collecting_handler!()
      assert {:ok, _} = run_case!(run_dir, hd(H.cases()), "run_c1")

      rows = rows(spans(name))
      assert rows != []

      {with_run, without_run} =
        Enum.split_with(rows, &(&1.attributes[:"ai_orchestrator.run_id"] == "run_c1"))

      # the command boundary is the one span that legitimately has no run id: it is emitted before a
      # command (and therefore a run id) exists
      assert Enum.map(without_run, & &1.name) == [:"ai_orchestrator.commands.invoke"]
      assert length(with_run) > 10

      # ---- C. correlation: only propagated identity joins spans (NS-26.F.002) ----

      # each domain carries the identifier of its own scope as well
      for row <- named(rows, :assignment, :dispatch),
          do: assert(is_binary(row.attributes[:"ai_orchestrator.assignment_id"]))

      for row <- named(rows, :gate, :prepare),
          do: assert(is_binary(row.attributes[:"ai_orchestrator.gate_run_id"]))
    end

    test "C2 the measured parentage: what nests, and what stays a root because it crossed a process", %{
      run_dir: run_dir
    } do
      {name, _tracer} = collecting_handler!()
      assert {:ok, _} = run_case!(run_dir, hd(H.cases()), "run_c2")

      rows = rows(spans(name))

      # what nests: dispatch under the assignment effect it belongs to, when the adapter runs in the
      # effect's own process (the snapshot path has no adapter_runner)
      snapshots = named(rows, :dispatch, :snapshot)
      assert snapshots != []

      for row <- snapshots,
          do: assert(row.parent_name == :"ai_orchestrator.assignment.snapshot", "dispatch.snapshot did not nest")

      # what does NOT nest, and the reason: deliver and observe go through an adapter_runner, which
      # places the closure in another process, and nothing about the trace was propagated there
      for operation <- [:deliver, :observe], row <- named(rows, :dispatch, operation) do
        assert row.parent == :undefined,
               "dispatch.#{operation} acquired a parent: the adapter_runner boundary now propagates context, " <>
                 "so this row and the handler moduledoc must be re-measured rather than inherited"
      end

      # every journal append is a root for the same reason: the sink runs in the owned subtree while
      # the run span is open in the caller's process. They join to the run by identifier instead.
      appends = named(rows, :journal, :append)
      assert length(appends) > 5

      for row <- appends do
        assert row.parent == :undefined, "a journal append acquired a parent; re-measure C2 rather than inherit it"
        assert row.attributes[:"ai_orchestrator.run_id"] == "run_c2"
      end

      # and the count of distinct traces is therefore large, which is the honest headline
      assert length(Enum.uniq(Enum.map(rows, & &1.trace))) > 5
    end

    test "C3 temporal proximity cannot manufacture parentage" do
      {name, _tracer} = collecting_handler!()

      # all three inner spans run INSIDE an open run scope, in the same process, at the same instant.
      # Only the one whose identifier names that scope may be its child.
      Events.span(:run, :execute, %{run_id: "run_c3"}, fn ->
        Events.span(:journal, :append, %{run_id: "run_c3"}, fn -> :same end)
        Events.span(:journal, :append, %{run_id: "run_other"}, fn -> :different end)
        Events.span(:journal, :append, %{}, fn -> :none end)
        :ok
      end)

      rows = rows(spans(name))
      assert [outer] = named(rows, :run, :execute)
      appends = named(rows, :journal, :append)
      assert length(appends) == 3

      {children, roots} = Enum.split_with(appends, &(&1.parent == outer.id))

      assert length(children) == 1,
             "exactly one of three temporally nested spans names the open scope, so exactly one may be its child"

      assert hd(children).attributes[:"ai_orchestrator.run_id"] == "run_c3"

      assert length(roots) == 2
      assert Enum.all?(roots, &(&1.parent == :undefined)), "a span that named no open scope was still parented"

      assert Enum.sort(Enum.map(roots, & &1.attributes[:"ai_orchestrator.run_id"])) == [nil, "run_other"]
    end

    test "C4 an identifier outside the closed shape is dropped, so it can neither label nor parent a span" do
      {name, _tracer} = collecting_handler!()

      Events.span(:run, :execute, %{run_id: "run_c4"}, fn ->
        # each of these is outside the admitted identity shape and must be dropped whole
        Events.span(:journal, :append, %{run_id: String.duplicate("a", 129)}, fn -> :too_long end)
        Events.span(:journal, :append, %{run_id: "run c4 with spaces"}, fn -> :bad_charset end)
        Events.span(:journal, :append, %{run_id: :run_c4}, fn -> :not_a_binary end)
        # a key outside the allowlist is never copied, whatever its value
        Events.span(:journal, :append, %{secret: "run_c4"}, fn -> :unlisted_key end)
        :ok
      end)

      rows = rows(spans(name))
      appends = named(rows, :journal, :append)
      assert length(appends) == 4

      for row <- appends do
        assert row.parent == :undefined, "a dropped identifier still bought a parent"
        assert row.attributes[:"ai_orchestrator.run_id"] == nil
        refute Map.has_key?(row.attributes, :"ai_orchestrator.secret")
      end

      # the control: the SAME value in the admitted shape does parent, so the rows above fail for the
      # shape and not because parenting never works here
      assert [outer] = named(rows, :run, :execute)

      Events.span(:run, :execute, %{run_id: "run_c4b"}, fn ->
        Events.span(:journal, :append, %{run_id: "run_c4b"}, fn -> :ok end)
      end)

      second = rows(spans(name))
      assert [inner] = named(second, :journal, :append)
      assert [outer2] = named(second, :run, :execute)
      assert inner.parent == outer2.id
      assert outer.id != outer2.id
    end
  end

  describe "D non-interference" do
    test "D1 the wrapped term passes through unchanged for every answer shape" do
      {name, _tracer} = collecting_handler!()

      for {answer, outcome} <- [
            {:ok, :ok},
            {{:ok, %{a: 1}}, :ok},
            {:error, :error},
            {{:error, %{clause: "x"}}, :error},
            {{:refused, :nope}, :refused},
            {:something_else, :other}
          ] do
        assert Events.span(:run, :execute, %{run_id: "run_d1"}, fn -> answer end) == answer
        rows = rows(spans(name))
        assert [row] = named(rows, :run, :execute)
        assert row.attributes[:"ai_orchestrator.outcome"] == outcome
      end
    end

    test "D2 an escape is re-raised with its kind, reason and stacktrace, and emits exactly one terminal event" do
      {name, _tracer} = collecting_handler!()
      test = self()
      id = "span-production-terminals-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach_many(
          id,
          [
            [:ai_orchestrator, :run, :execute, :start],
            [:ai_orchestrator, :run, :execute, :stop],
            [:ai_orchestrator, :run, :execute, :exception]
          ],
          fn event, _measurements, _metadata, _config -> send(test, {:phase, List.last(event)}) end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)

      raised =
        try do
          Events.span(:run, :execute, %{run_id: "run_d2"}, fn -> raise ArgumentError, "domain failure" end)
        rescue
          error -> error
        end

      assert %ArgumentError{message: "domain failure"} = raised

      assert_receive {:phase, :start}, 1_000
      assert_receive {:phase, :exception}, 1_000
      refute_received {:phase, :stop}
      refute_received {:phase, :exception}

      rows = rows(spans(name))
      assert [row] = named(rows, :run, :execute)
      assert row.attributes[:"ai_orchestrator.kind"] == :error
      assert row.attributes[:"ai_orchestrator.stack_depth"] > 0
      refute Map.has_key?(row.attributes, :"ai_orchestrator.outcome")
    end

    test "D3 a tracer that raises neither reaches the caller nor gets the handler detached" do
      # This is the property that distinguishes the production handler from the raising handler of
      # exporter_isolation_test R3: `:telemetry` detaches a handler that raises, which would silently
      # disable telemetry for the whole VM. This one catches its own escapes, so it stays attached.
      id = "span-production-raising-#{System.unique_integer([:positive])}"
      assert {:ok, ^id} = Handler.attach(id: id, tracer: {__MODULE__.NoSuchTracer, :config})
      on_exit(fn -> :telemetry.detach(id) end)

      assert Events.span(:run, :execute, %{run_id: "run_d3"}, fn -> {:ok, :survived} end) == {:ok, :survived}
      assert Events.emit(:attention, :raised, %{run_id: "run_d3"}) == :ok

      attached =
        [:ai_orchestrator, :run, :execute, :start]
        |> :telemetry.list_handlers()
        |> Enum.map(& &1.id)

      assert id in attached, "the production handler was detached by :telemetry when its tracer raised"
    end

    test "D4 two handler instances keep separate state and neither sees the other's scopes" do
      # ---- D. the producer cannot change domain behaviour ----
      {name_a, _tracer_a} = collecting_handler!()
      {name_b, _tracer_b} = collecting_handler!()

      Events.span(:run, :execute, %{run_id: "run_d4"}, fn ->
        Events.span(:journal, :append, %{run_id: "run_d4"}, fn -> :ok end)
      end)

      for name <- [name_a, name_b] do
        rows = rows(spans(name))
        assert [outer] = named(rows, :run, :execute)
        assert [inner] = named(rows, :journal, :append)
        assert inner.parent == outer.id, "an instance lost its own scope, or read another instance's"
      end
    end
  end

  describe "E attributes" do
    test "E1 only allowlisted metadata keys become attributes, and every attribute is prefixed" do
      {name, _tracer} = collecting_handler!()

      Events.span(:run, :execute, %{run_id: "run_e1"}, fn -> :ok end)
      rows = rows(spans(name))
      assert [row] = named(rows, :run, :execute)

      allowed =
        MapSet.new(
          [
            :domain,
            :operation,
            :verb,
            :actor_class,
            :outcome,
            :stage,
            :clause,
            :result_class,
            :kind,
            :class,
            :stack_depth
          ] ++ Events.identity_keys(),
          &:"ai_orchestrator.#{&1}"
        )

      assert MapSet.subset?(MapSet.new(Map.keys(row.attributes)), allowed),
             "a span carried an attribute outside the allowlist: #{inspect(Map.keys(row.attributes))}"

      assert Enum.all?(Map.keys(row.attributes), &String.starts_with?(Atom.to_string(&1), "ai_orchestrator."))
    end

    test "E2 a metadata key outside the allowlist never becomes an attribute, even handed straight to the handler" do
      {name, tracer} = collecting_handler!()

      metadata = %{
        span_ref: make_ref(),
        domain: :run,
        operation: :execute,
        run_id: "run_e2",
        prompt_bytes: "a secret the event should never carry",
        rejection: %{clause: "nope"}
      }

      config = %{id: "span-production-direct", tracer: tracer}
      assert Handler.handle([:ai_orchestrator, :run, :execute, :start], %{}, metadata, config) == :ok
      assert Handler.handle([:ai_orchestrator, :run, :execute, :stop], %{}, metadata, config) == :ok

      rows = rows(spans(name))
      assert [row] = named(rows, :run, :execute)
      assert row.attributes[:"ai_orchestrator.run_id"] == "run_e2"
      refute Map.has_key?(row.attributes, :"ai_orchestrator.prompt_bytes")
      refute Map.has_key?(row.attributes, :"ai_orchestrator.rejection")
      refute Map.has_key?(row.attributes, :prompt_bytes)
    end
  end

  # ---- E. attributes are a closed allowlist ----
end
