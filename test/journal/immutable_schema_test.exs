defmodule AiOrchestrator.Journal.ImmutableSchemaTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Journal.Schemas.EventDataSchemas
  alias AiOrchestrator.Journal.Schemas.LiteralSchema
  alias Zoi.Types.Literal

  @constructors [{EventData, :build_schema, 3}, {EventData, :event_variant, 4}, {Zoi, :map, 2}]
  @fixture Path.expand("../fixtures/contracts/scenarios/gated_run_seed/events.jsonl", __DIR__)

  test "every admitted literal equals both the builder and the public union variant" do
    counts =
      for mode <- [:read, :append, :view] do
        %Zoi.Types.Union{schemas: schemas} = EventData.schema(mode)

        keys =
          for %Zoi.Types.Map{fields: fields} = schema <- schemas do
            fields = Map.new(fields)
            %Literal{value: type} = Map.fetch!(fields, "type")
            %Literal{value: version} = Map.fetch!(fields, "event_version")
            assert EventDataSchemas.fetch!(type, version, mode) === schema
            assert EventData.build_schema(type, version, mode) === schema
            assert LiteralSchema.check!(schema) === schema
            {type, version}
          end

        expected =
          for type <- EventData.typed_types(), version <- versions(type, mode), do: {type, version}

        assert Enum.sort(keys) == Enum.sort(expected)
        length(keys)
      end

    assert counts == [48, 45, 45]
  end

  test "unknown types and inadmissible versions retain named refusals before lookup" do
    for mode <- [:read, :append, :view] do
      assert {:error, %{clause: "event_schema_unavailable", event_type: "unknown"}} =
               EventData.parse(%{"type" => "unknown", "event_version" => 1, "data" => %{}}, mode)

      for type <- EventData.typed_types(), version <- [0, EventData.current_version(type) + 1] do
        assert {:error, %{clause: "unsupported_event_version", event_type: ^type, event_version: ^version}} =
                 EventData.parse(%{"type" => type, "event_version" => version, "data" => %{}}, mode)
      end
    end
  end

  test "the literal scanner rejects nested runtime terms and names their path" do
    for term <- [fn -> :ok end, self(), make_ref()] do
      assert_raise ArgumentError, ~r/nonliteral schema term.*witness/, fn ->
        LiteralSchema.check!(%{witness: {:nested, [term]}})
      end
    end

    assert_raise ArgumentError, ~r/nested Regex violates schema premise.*witness/, fn ->
      LiteralSchema.check!(%{witness: ~r/probe/})
    end

    assert_raise ArgumentError, ~r/nonliteral schema term.*map_key/, fn ->
      LiteralSchema.check!(%{self() => :value})
    end

    assert_raise ArgumentError, ~r/nonliteral schema term.*tail/, fn ->
      LiteralSchema.check!([:head | self()])
    end

    safe = %{nested: %Literal{value: :value}, sequence: [1, {"text", nil}]}
    assert LiteralSchema.check!(safe) === safe
  end

  test "known older versions are refused by name in append and view modes" do
    rejected =
      for type <- EventData.typed_types(),
          mode <- [:append, :view],
          version <- EventData.known_versions(type),
          version != EventData.current_version(type) do
        assert {:error, %{clause: "unsupported_event_version", event_type: ^type, event_version: ^version}} =
                 EventData.parse(%{"type" => type, "event_version" => version, "data" => %{}}, mode)

        assert_raise KeyError, fn -> EventDataSchemas.fetch!(type, version, mode) end
        {type, version, mode}
      end

    assert length(rejected) == 6
  end

  test "real fixture parsing bypasses constructors after loading the modules" do
    events = @fixture |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
    assert length(events) == 32

    assert_no_construction(fn ->
      for event <- events do
        assert {:ok, _} = Event.validate_read(event)
      end
    end)
  end

  test "the same bypass witness fails when lookup delegates to construction" do
    assert_raise ExUnit.AssertionError, ~r/constructor_calls/, fn ->
      assert_no_construction(fn -> EventData.build_schema("run_completed", 1, :read) end)
    end
  end

  # A single synchronous worker is traced, not arbitrary descendant processes.
  # Join its exit and fence trace delivery before interpreting an empty call list.
  defp assert_no_construction(fun) do
    for {module, _, _} <- @constructors, do: Code.ensure_loaded!(module)
    parent = self()
    tag = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        receive do
          ^tag -> send(parent, {tag, fun.()})
        end
      end)

    try do
      for pattern <- @constructors do
        assert :erlang.trace_pattern(pattern, true, [:local]) == 1
      end

      assert :erlang.trace(pid, true, [:call]) == 1
      send(pid, tag)
      assert_receive {^tag, result}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 2_000
      fence = :erlang.trace_delivered(pid)
      deadline = System.monotonic_time(:millisecond) + 2_000
      calls = collect_calls(pid, fence, deadline, [])
      assert calls == [], "constructor_calls: #{inspect(calls)}"
      result
    after
      try do
        cleanup = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^cleanup, :process, ^pid, _reason}, 2_000
        Process.demonitor(monitor, [:flush])
      after
        for pattern <- @constructors, do: :erlang.trace_pattern(pattern, false, [:local])
      end
    end
  end

  defp collect_calls(pid, fence, deadline, calls) do
    remaining = deadline - System.monotonic_time(:millisecond)
    assert remaining > 0, "constructor trace delivery budget exhausted"

    receive do
      {:trace, ^pid, :call, {module, function, arguments}} ->
        collect_calls(pid, fence, deadline, [{module, function, length(arguments)} | calls])

      {:trace_delivered, ^pid, ^fence} ->
        Enum.reverse(calls)
    after
      remaining -> flunk("constructor trace delivery fence missing")
    end
  end

  defp versions(type, :read), do: EventData.known_versions(type)
  defp versions(type, _mode), do: [EventData.current_version(type)]
end
