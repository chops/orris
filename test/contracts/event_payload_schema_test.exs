defmodule AiOrchestrator.Contracts.EventPayloadSchemaTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Schemas.EventData

  @fixture_root Path.expand("../fixtures/contracts", __DIR__)
  @schema_path Path.expand("../../docs/contracts/journal-event.schema.json", __DIR__)

  @envelope_rejection_fixtures ~w(
    reject_chain_field_on_version_1
    reject_invalid_prev_line_sha256
    reject_missing_chain_on_version_2
    reject_missing_event_id
    reject_schema_version_string
    reject_unknown_event_type
    reject_unsupported_schema_version
  )

  @evidence_backed_reserved ~w(
    assignment_failed
    context_patch_accepted
    context_patch_proposed
    contract_change_proposed
    contract_change_ratified
    notification_failed
    notification_requested
    notification_sent
    run_budget_exhausted
    run_failed
    run_recovery_reserved
    stop_policy_evaluated
  )

  test "typed vocabulary covers every appendable type and every reserved historical type" do
    expected = MapSet.union(Event.appendable_types(), MapSet.new(@evidence_backed_reserved))

    assert EventData.typed_types() == expected
    assert MapSet.size(EventData.typed_types()) == 45
    assert MapSet.size(MapSet.difference(Event.reserved_types(), EventData.typed_types())) == 9
  end

  test "every fixture-backed typed event validates through the discriminated union" do
    events = fixture_events()

    for event <- events, MapSet.member?(EventData.typed_types(), event["type"]) do
      result = EventData.parse(event)
      assert match?({:ok, _}, result), failure_label(event)
      {:ok, parsed} = result
      assert parsed.__struct__ == EventData
      assert parsed.type == event["type"]
      assert parsed.event_version == event["event_version"]
      assert parsed.data == event["data"]
    end
  end

  # MUST-7 versioning: assignment_prompt_projected is at version 2; the positive fixture
  # set still carries it at version 1, so its upcaster is the one real transform (pinned
  # in test/journal/event_version_test.exs); every other type is at 1 with an identity.
  @current_versions %{"assignment_prompt_projected" => 2, "gate_started" => 2, "gate_failed" => 2}

  test "every appendable type has a read upcaster: identity at its current version, a transform below it" do
    events = fixture_events()

    for type <- Event.appendable_types() do
      event = Enum.find(events, &(&1["type"] == type)) || flunk("#{type}: no positive fixture")
      current = Map.get(@current_versions, type, 1)

      assert EventData.current_version(type) == current, type

      case event["event_version"] do
        ^current -> assert {:ok, ^event} = EventData.upcast(event)
        _older -> assert {:ok, %{"event_version" => ^current}} = EventData.upcast(event)
      end
    end
  end

  test "negative fixture removes one required field from every appendable payload" do
    rules = negative_rules()
    events = fixture_events()

    assert MapSet.new(rules, & &1["type"]) == Event.appendable_types()

    for %{"type" => type, "remove" => field, "expected_clause" => expected_clause} <- rules do
      # Append mode admits only the current version, so the fixture removed from must be at it.
      valid =
        Enum.find(events, fn event ->
          event["type"] == type and Map.has_key?(event["data"], field) and
            event["event_version"] == EventData.current_version(type)
        end) || flunk("#{type}: no positive fixture at the current version contains required field #{field}")

      valid = append_ready(valid)

      malformed = update_in(valid, ["data"], &Map.delete(&1, field))

      current = EventData.current_version(type)

      assert {:error, %{clause: ^expected_clause, event_type: ^type, event_version: ^current}} =
               Event.validate_append(malformed)
    end
  end

  test "every appendable payload rejects unknown keys" do
    events = fixture_events()

    for type <- Event.appendable_types() do
      valid =
        Enum.find(events, &(&1["type"] == type and &1["event_version"] == EventData.current_version(type))) ||
          flunk("#{type}: no positive fixture")

      valid = append_ready(valid)
      malformed = update_in(valid, ["data"], &Map.put(&1, "unexpected", true))

      assert {:error, %{clause: "invalid_event_data", event_type: ^type}} =
               Event.validate_append(malformed)
    end
  end

  test "legacy string authorship is readable but never appendable" do
    event =
      Enum.find(fixture_events(), fn event ->
        event["type"] == "run_cancel_requested" and is_binary(event["data"]["requested_by"])
      end)

    assert {:ok, view} = Event.validate_line(Jason.encode!(event))
    assert {:ok, ^view} = Event.validate_view(view)

    assert {:error, %{clause: "requested_by_object_required", event_type: "run_cancel_requested"}} =
             Event.validate_append(event)
  end

  test "requested_by value bounds apply to appends without invalidating historical reads" do
    event = requested_by_event(String.duplicate("x", 4_097))

    assert {:ok, view} = Event.validate_line(Jason.encode!(event))
    assert {:ok, ^view} = Event.validate_view(view)

    at_limit = put_in(event, ["data", "requested_by", "reason"], String.duplicate("x", 4_096))
    assert {:ok, _event} = Event.validate_append(at_limit)

    assert {:error,
            %{
              clause: "requested_by_value_too_large",
              event_type: "run_created",
              field: "reason",
              bytes: 4_097,
              max_bytes: 4_096
            }} = Event.validate_append(event)
  end

  test "requested_by append bounds reject invalid UTF-8 without reflecting the value" do
    assert {:error, %{clause: "requested_by_invalid_utf8", event_type: "run_created", field: "reason"} = rejection} =
             <<255>>
             |> requested_by_event()
             |> EventData.parse(:append)

    refute inspect(rejection) =~ <<255>>
  end

  test "requested_by bound rejection selects the first field deterministically" do
    event =
      fixture_events()
      |> Enum.find(&(&1["type"] == "run_created"))
      |> put_in(
        ["data", "requested_by"],
        %{
          "class" => "agent",
          "id" => "writer",
          "command_id" => "cmd_01J9X3T2QF5G7H8K1N3P",
          "verb" => "propose_plan",
          "args_hash" => "sha256:" <> String.duplicate("0", 64),
          "run_id" => String.duplicate("r", 4_097),
          "assignment_id" => String.duplicate("a", 4_097)
        }
      )

    assert {:error, %{clause: "requested_by_value_too_large", field: "assignment_id"}} =
             EventData.parse(event, :append)
  end

  test "declared reserved types without evidence have no invented schema" do
    untyped = MapSet.difference(Event.reserved_types(), EventData.typed_types())
    source = Enum.find(fixture_events(), &(&1["type"] == "run_failed"))

    for type <- untyped do
      event = %{source | "type" => type}

      assert {:error, %{clause: "event_schema_unavailable", event_type: ^type}} =
               Event.validate_line(Jason.encode!(event))

      assert {:error, %{clause: "reserved_event_type", event_type: ^type}} =
               Event.validate_append(event)
    end
  end

  test "tail repair metadata accepts the zero-receipt legacy boundary on resume and cancel" do
    repair = %{
      "action" => "advance_and_truncate",
      "truncated_bytes" => 17,
      "receipt_seq_before" => 0,
      "receipt_seq_after" => 0
    }

    for type <- ["run_resumed", "run_cancel_requested"] do
      event =
        fixture_events()
        |> Enum.find(&(&1["type"] == type))
        |> append_ready()
        |> put_in(["data", "tail_repair"], repair)

      assert {:ok, _event} = Event.validate_append(event)

      malformed = put_in(event, ["data", "tail_repair", "receipt_seq_after"], -1)

      assert {:error, %{clause: "invalid_event_data", event_type: ^type}} =
               Event.validate_append(malformed)
    end
  end

  test "validation rejections contain only JSON-safe diagnostics" do
    event = Enum.find(fixture_events(), &(&1["type"] == "plan_recorded"))
    malformed = update_in(event, ["data"], &Map.delete(&1, "plan_hash"))

    assert {:error, rejection} = Event.validate_line(Jason.encode!(malformed))
    assert {:ok, _json} = Jason.encode(rejection)
    assert rejection.reason == "journal_provenance_incomplete"
  end

  test "fold-facing validation accepts all immutable fixtures with valid envelopes" do
    for {line, path, position} <- fixture_lines(),
        Path.basename(Path.dirname(path)) not in @envelope_rejection_fixtures do
      case Jason.decode(line) do
        {:ok, %{"type" => type}} when type in ["task_created"] ->
          :ok

        {:ok, %{"type" => type}} when is_binary(type) ->
          assert match?({:ok, _}, Event.validate_line(line)), "#{path}:#{position} #{type}"

        _other ->
          :ok
      end
    end
  end

  test "the exported JSON Schema and generated typespec come from the public union" do
    expected = @schema_path |> File.read!() |> Jason.decode!()
    generated = EventData.json_schema() |> Jason.encode!() |> Jason.decode!() |> normalize_schema()

    assert generated == normalize_schema(expected)
    assert Enum.all?(generated["anyOf"], &(&1["properties"]["schema_version"]["const"] == 2))
    assert Enum.all?(generated["anyOf"], &("prev_line_sha256" in &1["required"]))
    assert Macro.to_string(EventData.type_spec()) =~ "event_version"
  end

  defp negative_rules do
    @fixture_root
    |> Path.join("journals/reject_data_required_fields/spec.json")
    |> File.read!()
    |> Jason.decode!()
  end

  defp fixture_events do
    for {line, path, _position} <- fixture_lines(),
        Path.basename(Path.dirname(path)) not in @envelope_rejection_fixtures,
        {:ok, %{"schema" => "ai-orchestrator/journal-event", "type" => type} = event} <- [Jason.decode(line)],
        type != "task_created" do
      event
    end
  end

  defp fixture_lines do
    @fixture_root
    |> Path.join("**/*.jsonl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.flat_map(fn path ->
      path
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.with_index(1)
      |> Enum.map(fn {line, position} -> {line, path, position} end)
    end)
  end

  defp failure_label(event), do: "#{event["type"]} event #{event["event_id"]}"

  defp requested_by_event(reason) do
    fixture_events()
    |> Enum.find(&(&1["type"] == "run_created"))
    |> put_in(
      ["data", "requested_by"],
      %{
        "class" => "system",
        "id" => "system",
        "command_id" => "cmd_01J9X3T2QF5G7H8K1N3P",
        "verb" => "repair",
        "args_hash" => "sha256:" <> String.duplicate("0", 64),
        "reason" => reason
      }
    )
  end

  defp append_ready(%{"data" => %{"requested_by" => value}} = event) when is_binary(value),
    do: update_in(event, ["data"], &Map.delete(&1, "requested_by"))

  defp append_ready(event), do: event

  defp normalize_schema(value) when is_map(value) do
    value
    |> Map.drop(["description", "example", "examples", "x-zoi"])
    |> Map.new(fn {key, nested} -> {key, normalize_schema(nested)} end)
  end

  defp normalize_schema(value) when is_list(value), do: Enum.map(value, &normalize_schema/1)
  defp normalize_schema(value), do: value
end
