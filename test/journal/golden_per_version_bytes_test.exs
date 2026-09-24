defmodule AiOrchestrator.Journal.GoldenPerVersionBytesTest do
  @moduledoc """
  NS-08.B.001: golden bytes for EACH supported event type and version.

  `golden_version_bytes_test.exs` proves that SOME golden line sits below its type's current
  version. These rows take the supported list from the code -- every type in
  `EventData.typed_types/0` crossed with `EventData.known_versions/1` -- and require one golden
  line per pair under `test/fixtures/contracts/journal/golden/`, pinned by `golden.sha256`.
  Each line must still validate at the version it was written at, upcast to its type's current
  version, and survive the read, upcast and every fold entry point with its bytes unchanged.

  Every check is a function returning its problems, so the inert controls at the bottom run
  the SAME checks over a disposable copy of the corpus that violates the clause and show
  that each one reports it.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Journal.Vocabulary

  @golden_root Path.expand("../fixtures/contracts/journal/golden", __DIR__)
  @manifest "golden.sha256"

  describe "the supported (type, version) list" do
    test "is derived from the schema module and covers every vocabulary type it types" do
      pairs = supported_pairs()

      assert pairs != [], "EventData declares no typed type, so every row below would pass vacuously"
      assert Enum.any?(pairs, fn {_type, version} -> version > 1 end), "no type is past version 1"

      for {type, _entry} <- Vocabulary.entries() do
        typed? = MapSet.member?(EventData.typed_types(), type)
        versions = EventData.known_versions(type)

        assert typed? == (versions != []),
               "#{type}: typed #{typed?} but known versions #{inspect(versions)}"
      end

      for type <- EventData.typed_types() do
        assert Map.has_key?(Vocabulary.entries(), type), "#{type} is typed but not in the vocabulary"
      end
    end
  end

  describe "golden bytes per supported version" do
    test "exactly one golden file exists per supported (type, version), and no other" do
      assert coverage_problems(@golden_root, supported_pairs()) == []
    end

    test "every golden file's bytes match the pinned digest" do
      assert digest_problems(@golden_root) == []
    end

    test "every golden line validates at its own version and upcasts to the current version" do
      assert read_problems(@golden_root, supported_pairs()) == []
    end

    test "reading, upcasting and folding every golden line leaves its bytes untouched" do
      copy = copy_corpus()
      before = File.read!(Path.join(copy, @manifest))

      assert exercise_and_compare(copy, supported_pairs()) == []
      assert digest_problems(copy) == []
      assert File.read!(Path.join(copy, @manifest)) == before
    end
  end

  # ---- inert controls: the same checks over a test-local corpus that violates the clause ----

  describe "inert controls" do
    test "a supported version without a golden file is reported missing" do
      copy = copy_corpus()
      File.rm!(Path.join(copy, "gate_started.v1.jsonl"))
      assert coverage_problems(copy, supported_pairs()) == [{:missing, "gate_started.v1.jsonl"}]

      # a version the code does not declare (as if @versions were bumped without a golden)
      assert {:missing, "gate_passed.v2.jsonl"} in coverage_problems(@golden_root, [{"gate_passed", 2}])
    end

    test "a golden file for an undeclared version is reported extra" do
      copy = copy_corpus()
      File.cp!(Path.join(copy, "gate_started.v2.jsonl"), Path.join(copy, "gate_started.v3.jsonl"))
      assert coverage_problems(copy, supported_pairs()) == [{:extra, "gate_started.v3.jsonl"}]
    end

    test "one rewritten byte is reported against the pinned digest" do
      copy = copy_corpus()
      path = Path.join(copy, "gate_failed.v1.jsonl")
      File.write!(path, path |> File.read!() |> String.replace(~s("actor":), ~s("actor" :)))
      assert digest_problems(copy) == [{:digest, "gate_failed.v1.jsonl"}]
    end

    test "a golden line that no longer validates at its version is reported" do
      copy = copy_corpus()
      path = Path.join(copy, "gate_started.v1.jsonl")
      # a v1 line that lost a field v1 requires: what a required field added without an
      # event-version increment does to history
      event = path |> File.read!() |> Jason.decode!() |> update_in(["data"], &Map.delete(&1, "command_argv"))
      File.write!(path, Jason.encode!(event) <> "\n")

      assert [{:invalid, "gate_started.v1.jsonl", _rejection}] = read_problems(copy, [{"gate_started", 1}])
    end

    test "a read path that rewrites the line it upcasts is reported" do
      copy = copy_corpus()
      path = Path.join(copy, "gate_started.v1.jsonl")
      rewriting_reader = fn line -> File.write!(path, line <> " \n") end
      assert [{:rewritten, "gate_started.v1.jsonl"}] = exercise_and_compare(copy, [{"gate_started", 1}], rewriting_reader)
    end
  end

  # ---- the checks ----

  defp supported_pairs do
    for type <- Enum.sort(EventData.typed_types()),
        version <- EventData.known_versions(type),
        do: {type, version}
  end

  defp file_name({type, version}), do: "#{type}.v#{version}.jsonl"

  defp coverage_problems(dir, pairs) do
    expected = MapSet.new(pairs, &file_name/1)
    present = dir |> Path.join("*.jsonl") |> Path.wildcard() |> MapSet.new(&Path.basename/1)

    (expected |> MapSet.difference(present) |> Enum.sort() |> Enum.map(&{:missing, &1})) ++
      (present |> MapSet.difference(expected) |> Enum.sort() |> Enum.map(&{:extra, &1}))
  end

  defp digest_problems(dir) do
    pinned =
      for line <- dir |> Path.join(@manifest) |> File.read!() |> String.split("\n", trim: true),
          [digest, name] = String.split(line, "  ", parts: 2),
          into: %{},
          do: {name, digest}

    present = dir |> Path.join("*.jsonl") |> Path.wildcard() |> Enum.map(&Path.basename/1) |> Enum.sort()

    unpinned = for name <- present, not Map.has_key?(pinned, name), do: {:unpinned, name}

    changed =
      for {name, digest} <- Enum.sort(pinned),
          sha256(Path.join(dir, name)) != digest,
          do: {:digest, name}

    unpinned ++ changed
  end

  defp read_problems(dir, pairs) do
    Enum.flat_map(pairs, fn {type, version} = pair ->
      name = file_name(pair)

      with {:ok, bytes} <- File.read(Path.join(dir, name)),
           [line] <- String.split(bytes, "\n", trim: true),
           true <- bytes == line <> "\n" || {:framing, name},
           {:ok, %{"type" => ^type, "event_version" => ^version} = event} <- Jason.decode(line),
           {:ok, _view} <- Event.validate_line(line),
           {:ok, %{"event_version" => upcast_version} = upcast} <- Event.upcast(event),
           true <- upcast_version == EventData.current_version(type) || {:upcast_version, name, upcast_version},
           {:ok, _view} <- Event.validate_view(upcast) do
        []
      else
        {:error, rejection} -> [{:invalid, name, rejection}]
        {:framing, _name} = problem -> [problem]
        {:upcast_version, _name, _version} = problem -> [problem]
        other -> [{:unreadable, name, other}]
      end
    end)
  end

  # every read-side entry point over each golden line, then the bytes on disk compared with the
  # bytes read before; `extra_reader` lets the control stand in for a read path that rewrites
  defp exercise_and_compare(dir, pairs, extra_reader \\ fn _line -> :ok end) do
    Enum.flat_map(pairs, fn pair ->
      name = file_name(pair)
      path = Path.join(dir, name)
      bytes = File.read!(path)
      line = String.trim_trailing(bytes, "\n")
      {:ok, event} = Jason.decode(line)

      _ = Event.validate_line(line)
      _ = Event.validate_read(event)
      _ = Event.upcast(event)
      _ = Fold.fold_lines([line])
      _ = Fold.fold_events([event])
      _ = for {:ok, view} <- [Event.validate_line(line)], do: Fold.fold_views([view])
      _ = extra_reader.(line)

      if File.read!(path) == bytes, do: [], else: [{:rewritten, name}]
    end)
  end

  defp sha256(path), do: Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower)

  defp copy_corpus do
    dir = Path.join(System.tmp_dir!(), "golden_per_version_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    File.cp_r!(@golden_root, dir)
    dir
  end
end
