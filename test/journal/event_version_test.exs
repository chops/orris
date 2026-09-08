defmodule AiOrchestrator.Journal.EventVersionTest do
  @moduledoc """
  RED for MUST-7's versioning half (ruling `m_1788571429536242416_dca73dd4`): a bounded,
  per-event-type versioned unit, not a silent read-time transformation at a fixed version.

  `assignment_prompt_projected` moves to version 2, where `artifact_baseline` is required.
  Historical version-1 lines stay exactly as written; they are read, and their read-side
  view is upcast to version 2 with an explicit `{"status" => "unrecorded"}` baseline, which
  is a shape no journal line may carry, in either mode; only the reducer's view mode admits
  it. Append requires the current version. A version
  from the future is refused by name. Every other type remains at version 1, with the
  identity upcaster it has today.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Schemas.EventData

  @dir Path.expand("../fixtures/contracts/journal/versions", __DIR__)
  @projection "assignment_prompt_projected"

  defp fixture(name), do: @dir |> Path.join("#{@projection}.#{name}.json") |> File.read!() |> Jason.decode!()

  # Since unit 2 of gate-process-ownership (D3, m_1788583146000) the gate start and failure are
  # also at version 2; their discipline is pinned in gate_event_version_test.exs.
  @versioned [@projection, "gate_started", "gate_failed"]

  test "the projection and the two gate types are at version 2; every other appendable type stays at 1" do
    assert EventData.current_version(@projection) == 2

    for type <- Event.appendable_types(), type not in @versioned do
      assert EventData.current_version(type) == 1, type
    end
  end

  test "a version-1 projection is read but may not be appended" do
    v1 = fixture("v1")

    assert {:ok, %EventData{event_version: 1}} = EventData.parse(v1, :read)
    assert {:ok, _} = Event.validate_read(v1)

    assert {:error, %{clause: "unsupported_event_version", event_version: 1}} = EventData.parse(v1, :append)
  end

  test "a version-2 projection with a recorded or absent baseline is read and appended" do
    for name <- ["v2.absent", "v2.recorded"] do
      v2 = fixture(name)
      assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(v2, :read)), name
      assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(v2, :append)), name
      assert match?({:ok, _}, Event.validate_read(v2)), name
    end
  end

  test "a version from the future is refused by name in both modes" do
    v3 = fixture("v3.future")

    for mode <- [:read, :append] do
      assert {:error, %{clause: "unsupported_event_version", event_version: 3}} = EventData.parse(v3, mode)
    end

    assert {:error, %{clause: "unsupported_event_version"}} = EventData.upcast(v3)
  end

  test "malformed versions are refused: a v2 without its baseline, a v1 with one, a v2 claiming unrecorded" do
    for mode <- [:read, :append] do
      assert match?(
               {:error, %{clause: "invalid_event_data"}},
               EventData.parse(fixture("v2.missing_baseline.malformed"), mode)
             ),
             "#{mode}"
    end

    # The unrecorded shape is in-memory vocabulary: a line on disk may never carry it, in
    # either mode, and Event.validate_line refuses it; only the reducer's view mode admits it.
    for mode <- [:read, :append] do
      assert match?({:error, %{clause: "invalid_event_data"}}, EventData.parse(fixture("v2.unrecorded.malformed"), mode)),
             "#{mode}"
    end

    assert match?({:error, _}, Event.validate_line(Jason.encode!(fixture("v2.unrecorded.malformed"))))
    assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(fixture("v2.unrecorded.malformed"), :view))

    # A version-1 line is only ever read (append refuses its version before its shape), and
    # read at version 1 a baseline is an unrecognized key.
    assert match?(
             {:error, %{clause: "invalid_event_data"}},
             EventData.parse(fixture("v1.with_baseline.malformed"), :read)
           )
  end

  test "a shapeless event is a named rejection at the upcaster, never a crash" do
    assert {:error, %{clause: "invalid_event_data"}} = EventData.upcast(%{"type" => @projection, "event_version" => 1})
    assert {:error, _} = Event.validate_read(%{"type" => @projection, "event_version" => 1})
  end

  test "the read-side view of a version-1 projection is version 2 with an explicit unrecorded baseline" do
    v1 = fixture("v1")

    assert {:ok, view} = EventData.upcast(v1)
    assert view["event_version"] == 2
    assert match?({:ok, ^view}, Event.validate_view(view)), "the view validates in view mode"
    assert view["data"]["artifact_baseline"] == %{"status" => "unrecorded"}
    assert Map.delete(view["data"], "artifact_baseline") == v1["data"], "every other field is carried unchanged"
    assert Map.drop(view, ["event_version", "data"]) == Map.drop(v1, ["event_version", "data"])
  end

  test "the read-side view of a current-version event is the event itself" do
    v2 = fixture("v2.recorded")
    assert {:ok, ^v2} = EventData.upcast(v2)
  end

  test "the public JSON schema admits the projection at version 2 and requires its baseline there" do
    schema = Jason.encode!(EventData.json_schema())
    assert schema =~ "artifact_baseline"
    assert schema =~ ~s("const":2) or schema =~ ~s("enum":[1,2]) or schema =~ ~s("enum":[2])
  end
end
