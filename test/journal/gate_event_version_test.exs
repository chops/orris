defmodule AiOrchestrator.Journal.GateEventVersionTest do
  @moduledoc """
  RED (D3, rulings m_1788583146000, corrections MUST-9 m_1788584540000): `gate_started` and
  `gate_failed` move to version 2 with closed shapes; version-1 lines stay exactly as written
  on read (valid under TODAY's rules, including the stderr evidence union) with explicit
  unrecorded views; append requires version 2; the future is refused by name; the exported
  JSON Schema carries the new variants; Fold parity holds on a full valid prefix.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Schemas.EventData

  @dir Path.expand("../fixtures/contracts/journal/versions", __DIR__)
  @parity Path.expand("../fixtures/contracts/parity/06_concurrency_cap_run.jsonl", __DIR__)
  @schema_path Path.expand("../../docs/contracts/journal-event.schema.json", __DIR__)

  defp fixture(name), do: @dir |> Path.join(name <> ".json") |> File.read!() |> Jason.decode!()

  # the first 26 lines of a real archived run: run_created through gate_requested gr_0001
  defp valid_prefix do
    @parity |> File.stream!() |> Enum.take(26) |> Enum.map(&Jason.decode!/1)
  end

  test "every gate fixture that is not marked malformed is valid under the current read rules" do
    for name <- ["gate_started.v1", "gate_failed.v1"] do
      assert match?({:ok, _}, Event.validate_read(fixture(name))), name
    end

    assert match?({:error, _}, Event.validate_read(fixture("gate_failed.v1.no_stderr_evidence.malformed")))
  end

  test "both gate types are at version 2; the projection stays at 2; everything else at 1" do
    assert EventData.current_version("gate_started") == 2
    assert EventData.current_version("gate_failed") == 2
    assert EventData.current_version("assignment_prompt_projected") == 2

    for type <- Event.appendable_types(), type not in ~w(gate_started gate_failed assignment_prompt_projected) do
      assert EventData.current_version(type) == 1, type
    end
  end

  test "version-1 gate lines are read exactly as written but may not be appended" do
    for name <- ["gate_started.v1", "gate_failed.v1"] do
      v1 = fixture(name)
      assert match?({:ok, %EventData{event_version: 1}}, EventData.parse(v1, :read)), name

      assert match?({:error, %{clause: "unsupported_event_version", event_version: 1}}, EventData.parse(v1, :append)),
             name
    end
  end

  test "version-2 gate lines with closed shapes are read and appended; the stderr evidence union is preserved" do
    for name <- [
          "gate_started.v2",
          "gate_failed.v2.exit",
          "gate_failed.v2.timeout",
          "gate_failed.v2.signal",
          "gate_failed.v2.recovery"
        ] do
      v2 = fixture(name)
      assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(v2, :read)), name
      assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(v2, :append)), name
      assert match?({:ok, _}, Event.validate_read(v2)), name
    end

    only_hash = update_in(fixture("gate_failed.v2.exit"), ["data"], &Map.delete(&1, "stderr_merged"))
    assert match?({:ok, %EventData{event_version: 2}}, EventData.parse(only_hash, :append))
    assert match?({:error, _}, EventData.parse(fixture("gate_failed.v2.no_stderr_evidence.malformed"), :read))
  end

  test "malformed version-2 shapes are refused in every mode, including view" do
    for name <- [
          "gate_started.v2.missing_execution.malformed",
          "gate_started.v2.unrecorded.malformed",
          "gate_started.v1.with_execution.malformed",
          "gate_failed.v2.null_without_termination.malformed",
          "gate_failed.v2.exit_with_termination.malformed",
          "gate_failed.v2.timeout_with_signal.malformed",
          "gate_failed.v2.signal_without_signal.malformed",
          "gate_failed.v2.unrecorded.malformed",
          "gate_failed.v2.missing_summary.malformed"
        ] do
      bad = fixture(name)

      for mode <- [:read, :append] do
        assert match?({:error, _}, EventData.parse(bad, mode)), "#{name} in #{mode}"
      end

      if !String.contains?(name, "unrecorded") do
        assert match?({:error, _}, Event.validate_view(bad)), "#{name} as a view"
      end
    end
  end

  test "the view of a version-1 gate line carries the explicit unrecorded claim, which no line may carry" do
    assert {:ok, %{"event_version" => 2, "data" => %{"execution" => %{"status" => "unrecorded"}} = started}} =
             EventData.upcast(fixture("gate_started.v1"))

    view = "gate_started.v1" |> fixture() |> Map.put("event_version", 2) |> Map.put("data", started)
    assert match?({:ok, _}, Event.validate_view(view))
    assert match?({:error, _}, Event.validate_read(fixture("gate_started.v2.unrecorded.malformed")))

    assert {:ok, %{"event_version" => 2, "data" => %{"termination" => %{"status" => "unrecorded"}, "exit_status" => 3}}} =
             EventData.upcast(fixture("gate_failed.v1"))

    assert match?({:error, _}, Event.validate_read(fixture("gate_failed.v2.unrecorded.malformed")))
  end

  # one field at a time, from an accepted v2 line: refused in read, append AND view (the view-only
  # unrecorded arms are separate, so a legacy view keeps exactly its historical admission)
  for {fixture, path, value} <- [
        {"gate_started.v2", ["attempt"], 0},
        {"gate_started.v2", ["attempt"], 3},
        {"gate_started.v2", ["execution", "pid"], 0},
        {"gate_started.v2", ["execution", "pgid"], 2_147_483_648},
        {"gate_started.v2", ["execution", "start"], String.duplicate("X", 10_000)},
        {"gate_started.v2", ["execution", "start"], "yesterday"},
        {"gate_started.v2", ["execution", "start"], ""},
        {"gate_failed.v2.exit", ["exit_status"], 0},
        {"gate_failed.v2.exit", ["exit_status"], 256},
        {"gate_failed.v2.exit", ["exit_status"], -1},
        {"gate_failed.v2.timeout", ["termination", "leftovers"], "some text"},
        {"gate_failed.v2.timeout", ["termination", "leftovers"], "01"},
        {"gate_failed.v2.timeout", ["termination", "leftovers"], ""},
        {"gate_failed.v2.timeout", ["termination", "proof"], "maybe"},
        {"gate_failed.v2.signal", ["termination", "signal"], 0}
      ] do
    label = if is_binary(value) and byte_size(value) > 40, do: "#{byte_size(value)}-byte string", else: inspect(value)

    test "#{fixture}: #{Enum.join(path, ".")} = #{label} is outside the v2 domain in every mode" do
      accepted = fixture(unquote(fixture))
      assert match?({:ok, _}, EventData.parse(accepted, :read)), "positive control"
      assert match?({:ok, _}, EventData.parse(accepted, :view)), "positive control (view)"
      bad = put_in(accepted, ["data" | unquote(path)], unquote(Macro.escape(value)))

      for mode <- [:read, :append, :view] do
        assert match?({:error, _}, EventData.parse(bad, mode)), "#{inspect(unquote(path))} in #{mode}"
      end
    end
  end

  test "the exported schema is the generator's, so the domains above are in the public export too" do
    exported = @schema_path |> File.read!() |> Jason.decode!()
    generated = EventData.json_schema() |> Jason.encode!() |> Jason.decode!()
    assert exported == generated
  end

  # executable PUBLIC-schema controls, read from the committed export (equality to the generator
  # cannot catch a shape no JSON value can satisfy): the numeric domains are integer + bounds, the
  # string domains are portable patterns (no PCRE-only anchors, compilable), and each pattern
  # admits the native positives and refuses a trailing newline
  defp exported_data_properties(type, version) do
    exported = @schema_path |> File.read!() |> Jason.decode!()

    variant =
      Enum.find(exported["anyOf"], fn v ->
        get_in(v, ["properties", "type", "const"]) == type and
          get_in(v, ["properties", "event_version", "const"]) == version
      end) || flunk("no exported variant for #{type} v#{version}")

    get_in(variant, ["properties", "data"])
  end

  defp field_schemas(%{"anyOf" => arms}, path), do: Enum.flat_map(arms, &field_schemas(&1, path))
  defp field_schemas(%{"properties" => props}, [key]), do: List.wrap(props[key])

  defp field_schemas(%{"properties" => props}, [key | rest]),
    do: if(props[key], do: field_schemas(props[key], rest), else: [])

  defp field_schemas(_, _), do: []

  test "exported attempt is an integer bounded to 1..2, never a string enum" do
    data = exported_data_properties("gate_started", 2)
    schemas = field_schemas(data, ["attempt"])
    assert schemas != []

    for schema <- schemas do
      assert schema["type"] == "integer", inspect(schema)
      assert schema["minimum"] == 1 and schema["maximum"] == 2, inspect(schema)
      refute Map.has_key?(schema, "enum")
    end
  end

  for {type, version, path, positives, negatives} <- [
        {"gate_started", 2, ["execution", "start"], ["1756728000.123456", "ticks:123"],
         ["1756728000.123456\n", "yesterday", "", "ticks:"]},
        {"gate_started", 2, ["execution", "claim_hash"], ["sha256:" <> String.duplicate("ab", 32)],
         ["sha256:" <> String.duplicate("ab", 32) <> "\n", "sha256:" <> String.duplicate("AB", 32)]},
        {"gate_failed", 2, ["termination", "leftovers"], ["0", "17", "unknown"], ["0\n", "01", "", "some text"]}
      ] do
    test "exported #{Enum.join(path, ".")} pattern is portable and byte-exact" do
      data = exported_data_properties(unquote(type), unquote(version))
      schemas = field_schemas(data, unquote(path))
      patterns = schemas |> Enum.map(& &1["pattern"]) |> Enum.reject(&is_nil/1) |> Enum.uniq()
      assert patterns != [], "no exported pattern for #{inspect(unquote(path))}"

      for pattern <- patterns do
        refute pattern =~ ~r/\\[AzZG]/, "PCRE-only anchor in the public export: #{pattern}"
        compiled = Regex.compile(pattern)
        assert match?({:ok, _}, compiled), "the exported pattern must compile: #{pattern}"
        {:ok, regex} = compiled

        for good <- unquote(positives), do: assert(Regex.match?(regex, good), "#{inspect(good)} must match #{pattern}")
        for bad <- unquote(negatives), do: refute(Regex.match?(regex, bad), "#{inspect(bad)} must not match #{pattern}")
      end
    end
  end

  test "execution.claim_hash with a trailing newline is refused in every mode (byte-exact binding)" do
    accepted = fixture("gate_started.v2")
    bad = put_in(accepted, ["data", "execution", "claim_hash"], accepted["data"]["execution"]["claim_hash"] <> "\n")

    for mode <- [:read, :append, :view] do
      assert match?({:error, _}, EventData.parse(bad, mode)), "#{mode}"
    end
  end

  test "the future is refused by name in both modes and in upcast" do
    v3 = fixture("gate_started.v3.future")

    for mode <- [:read, :append] do
      assert match?({:error, %{clause: "unsupported_event_version", event_version: 3}}, EventData.parse(v3, mode))
    end

    assert match?({:error, %{clause: "unsupported_event_version"}}, EventData.upcast(v3))
  end

  test "the exported JSON Schema carries version-2 variants for both gate types and stays append-only" do
    exported = @schema_path |> File.read!() |> Jason.decode!()
    generated = EventData.json_schema() |> Jason.encode!() |> Jason.decode!()

    variants = fn schema ->
      for variant <- schema["anyOf"],
          do:
            {get_in(variant, ["properties", "type", "const"]), get_in(variant, ["properties", "event_version", "const"])}
    end

    # the public export is APPEND-only: version 2 present, the historical version 1 absent from it
    # (version 1 stays readable through the read/view API, tested above)
    for schema <- [exported, generated] do
      present = variants.(schema)
      assert {"gate_started", 2} in present
      assert {"gate_failed", 2} in present
      refute {"gate_started", 1} in present
      refute {"gate_failed", 1} in present
    end
  end

  test "on a full valid prefix, the raw v1 line and its view fold to the same gate status as a v2 line; a v2 timeout folds failed" do
    prefix = valid_prefix()
    # the raw fold takes lines; the view fold takes upgraded views, so the prefix is upcast the
    # way the reducer upcasts every event (its version-1 projections included)
    prefix_views =
      Enum.map(prefix, fn line ->
        {:ok, view} = EventData.upcast(line)
        view
      end)

    v1 = fixture("gate_started.v1")
    {:ok, v1_view} = EventData.upcast(v1)
    assert {:ok, raw} = Fold.fold_events(prefix ++ [v1])
    assert {:ok, views} = Fold.fold_views(prefix_views ++ [v1_view])
    assert raw.gate_runs["gr_0001"].status == "started"
    assert views.gate_runs["gr_0001"].status == "started"
    assert {:ok, v2} = Fold.fold_views(prefix_views ++ [fixture("gate_started.v2")])
    assert v2.gate_runs["gr_0001"].status == "started"

    assert {:ok, timeout} =
             Fold.fold_views(prefix_views ++ [fixture("gate_started.v2"), fixture("gate_failed.v2.timeout")])

    assert timeout.gate_runs["gr_0001"].status == "failed"
  end
end
