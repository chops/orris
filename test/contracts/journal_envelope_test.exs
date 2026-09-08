defmodule AiOrchestrator.Contracts.JournalEnvelopeTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Event

  test "valid_minimal: every line validates as a journal event" do
    for line <- F.lines("journals", "valid_minimal") do
      assert {:ok, event} = Event.validate_line(line)
      assert is_map(event)
    end
  end

  test "valid_envelope_version_2_minimal: chain fields are present and well-shaped" do
    [first_line, second_line] = lines = F.lines("journals", "valid_envelope_version_2_minimal")

    for line <- lines do
      assert {:ok, %{"schema_version" => 2, "prev_line_sha256" => "sha256:" <> digest}} =
               Event.validate_line(line)

      assert digest =~ ~r/^[0-9a-f]{64}$/
    end

    assert {:ok, second_event} = Event.validate_line(second_line)
    assert second_event["prev_line_sha256"] == "sha256:" <> sha256(first_line <> "\n")
  end

  test "reject_chain_field_on_version_1: version 1 cannot carry a chain field" do
    [line] = F.lines("journals", "reject_chain_field_on_version_1")

    assert {:error, %{clause: "chain_field_forbidden_on_v1"}} = Event.validate_line(line)
  end

  test "reject_missing_chain_on_version_2: version 2 requires a chain field" do
    [line] = F.lines("journals", "reject_missing_chain_on_version_2")

    assert {:error, %{clause: "missing_prev_line_sha256"}} = Event.validate_line(line)
  end

  test "reject_invalid_prev_line_sha256: malformed chain hashes have a named clause" do
    [line] = F.lines("journals", "reject_invalid_prev_line_sha256")

    assert {:error, %{clause: "invalid_prev_line_sha256"}} = Event.validate_line(line)
  end

  test "reject_unsupported_schema_version: unknown numeric versions have a named clause" do
    [line] = F.lines("journals", "reject_unsupported_schema_version")

    assert {:error, %{clause: "unsupported_schema_version", schema_version: 3}} =
             Event.validate_line(line)
  end

  test "append validation accepts produced types and refuses every reserved type" do
    line = "journals" |> F.lines("valid_minimal") |> hd()
    event = Jason.decode!(line)

    assert {:ok, %{"type" => "run_created"}} = Event.validate_append(event)

    for type <- Event.reserved_types() do
      assert {:error, %{clause: "reserved_event_type", event_type: ^type}} =
               Event.validate_append(Map.put(event, "type", type))
    end
  end

  defp sha256(bytes) do
    :sha256
    |> :crypto.hash(bytes)
    |> Base.encode16(case: :lower)
  end

  for name <- ["reject_schema_version_string", "reject_missing_event_id"] do
    test "#{name}: envelope violation is rejected with the named clause" do
      name = unquote(name)
      [line | _] = F.lines("journals", name)
      expected = F.json("journals", name, "expected_rejection.json")
      assert {:error, rejection} = Event.validate_line(line)
      F.assert_rejection_matches(rejection, expected)
    end
  end
end
