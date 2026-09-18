defmodule AiOrchestrator.Journal.GoldenVersionBytesTest do
  @moduledoc """
  NS-08.B.001 controls: upcasting preserves historical bytes, and a required field added
  without an event-version increment is refused.

  The row's acceptance says "load every supported golden version and COMPARE ORIGINAL
  BYTES". The delivered payload-schema contract compares decoded maps, never the file, so
  a read path that rewrote a line while upcasting it would pass. These rows compare the
  bytes, over a disposable copy of every fixture journal, after the reader and every fold
  entry point have run.

  The row's failure control ("required field without event-version increment rejected")
  had no assertion at all: it held only because such a change would incidentally break the
  positive corpus. `every golden line still validates at the version it was written at`
  makes that incidental breakage the control itself.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.Schemas.EventData

  @fixture_root Path.expand("../fixtures/contracts", __DIR__)

  # The seven corpora that deliberately carry an INVALID envelope, and the one prototype
  # type the delivered contract already excludes. Everything else is golden history.
  @envelope_rejection_fixtures ~w(
    reject_chain_field_on_version_1
    reject_invalid_prev_line_sha256
    reject_missing_chain_on_version_2
    reject_missing_event_id
    reject_schema_version_string
    reject_unknown_event_type
    reject_unsupported_schema_version
  )

  @prototype_types ~w(task_created)

  test "reading and folding every fixture journal leaves the bytes on disk untouched" do
    root = temp_dir()

    for source <- journal_dirs() do
      name = Path.basename(source)
      copy = Path.join(root, name)
      File.cp_r!(source, copy)

      before = digest_tree(copy)
      assert before != %{}, "#{name}: nothing was copied"

      _loaded = Reader.load(copy)
      lines = copy |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)
      _folded_lines = Fold.fold_lines(lines)
      _folded_events = Fold.fold_events(decoded(lines))
      _folded_views = Fold.fold_views(views(lines))

      assert digest_tree(copy) == before,
             "#{name}: reading or folding the journal changed bytes on disk"
    end
  end

  test "every golden line still validates at the version it was written at" do
    pairs = golden_pairs()

    assert pairs != [], "the golden corpus is empty, so this control would pass vacuously"

    for {type, version, line, label} <- pairs do
      assert match?({:ok, _view}, Event.validate_line(line)),
             """
             #{label}: the golden line for #{type} at event_version #{version} no longer validates.

             A payload field that became required without an event-version increment rejects
             history written before it existed (NS-08.B.001). Add the field at a NEW version
             with an upcaster instead, and leave the recorded version admitting the old shape.

             #{inspect(Event.validate_line(line))}
             """
    end
  end

  test "the golden corpus really exercises more than one version of a type" do
    below_current =
      for {type, version, _line, _label} <- golden_pairs(),
          current = EventData.current_version(type),
          is_integer(current),
          version < current,
          do: {type, version}

    assert below_current != [],
           "no golden line sits below its type's current version, so upcasting is never exercised"
  end

  test "an event version beyond a type's current version is refused by name" do
    for {type, version, line, label} <- golden_pairs(), version == EventData.current_version(type) do
      ahead = line |> Jason.decode!() |> Map.put("event_version", version + 1)

      assert match?(
               {:error, %{clause: "unsupported_event_version", event_type: ^type}},
               Event.validate_line(Jason.encode!(ahead))
             ),
             "#{label}: #{type} admitted an event_version above its declared current version"
    end
  end

  test "every golden (type, version) has at least one field whose absence is refused on read" do
    typed_pairs =
      golden_pairs()
      |> Enum.filter(fn {type, _version, _line, _label} -> MapSet.member?(EventData.typed_types(), type) end)
      |> Enum.uniq_by(fn {type, version, _line, _label} -> {type, version} end)

    for {type, version, line, label} <- typed_pairs do
      event = Jason.decode!(line)

      required =
        for {field, _value} <- event["data"],
            refused?(event, field),
            do: field

      assert required != [],
             """
             #{label}: #{type} at event_version #{version} has no field whose removal is refused.
             A payload every shape satisfies cannot witness the version rule this row names.
             """
    end
  end

  defp refused?(event, field) do
    stripped = update_in(event, ["data"], &Map.delete(&1, field))
    match?({:error, _rejection}, Event.validate_line(Jason.encode!(stripped)))
  end

  defp golden_pairs do
    for {line, path, position} <- fixture_lines(),
        {:ok, %{"schema" => "ai-orchestrator/journal-event", "type" => type, "event_version" => version}} <-
          [Jason.decode(line)],
        type not in @prototype_types,
        do: {type, version, line, "#{Path.relative_to(path, @fixture_root)}:#{position}"}
  end

  defp fixture_lines do
    @fixture_root
    |> Path.join("**/*.jsonl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.reject(&(Path.basename(Path.dirname(&1)) in @envelope_rejection_fixtures))
    |> Enum.flat_map(fn path ->
      path
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.with_index(1)
      |> Enum.map(fn {line, position} -> {line, path, position} end)
    end)
  end

  defp journal_dirs do
    @fixture_root
    |> Path.join("**/events.jsonl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(&Path.dirname/1)
  end

  defp decoded(lines) do
    for line <- lines, {:ok, event} <- [Jason.decode(line)], do: event
  end

  defp views(lines) do
    for line <- lines, {:ok, view} <- [Event.validate_line(line)], do: view
  end

  defp digest_tree(dir) do
    dir
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Map.new(&{Path.relative_to(&1, dir), :crypto.hash(:sha256, File.read!(&1))})
  end

  defp temp_dir do
    dir = Path.join(System.tmp_dir!(), "golden_bytes_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end
end
