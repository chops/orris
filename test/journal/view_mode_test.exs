defmodule AiOrchestrator.Journal.ViewModeTest do
  @moduledoc """
  The reducer folds upgraded views (Fold.fold_views / Event.validate_view). A view is a
  validated line plus the upcaster's additions, so every field a line may carry in read mode
  must fold as a view too -- including the structured `requested_by` on run_created,
  run_resumed and run_cancel_requested, which append mode requires as an object and read
  mode also admits as the historical literal "operator". Only the artifact baseline has a
  view-only shape (the unrecorded claim), and an arbitrary string is refused everywhere.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold

  @run_id "run_fixture_0001"

  # The shape the fixtures carry (Journal.Schemas.RequestedBy): a command-bound operator.
  @requested_by %{
    "class" => "operator",
    "id" => "operator",
    "command_id" => "cmd_01J9X3T2QF5G7H8K1N3P",
    "verb" => "start",
    "args_hash" => "sha256:" <> String.duplicate("ab", 32)
  }

  defp event(seq, type, data) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 1,
      "seq" => seq,
      "event_id" => "ev_#{String.pad_leading(Integer.to_string(seq), 4, "0")}",
      "type" => type,
      "ts" => "2026-01-01T00:00:0#{seq}Z",
      "run_id" => @run_id,
      "actor" => "run_supervisor",
      "data" => data
    }
  end

  defp run_created(requested_by) do
    data = %{
      "project" => "example-repo",
      "repo_root" => "/tmp/example-repo",
      "run_dir" => "/tmp/example-run",
      "operator" => "operator",
      "spec_path" => "run.json",
      "spec_hash" => "sha256:" <> String.duplicate("00", 32)
    }

    event(1, "run_created", if(requested_by, do: Map.put(data, "requested_by", requested_by), else: data))
  end

  test "a structured requested_by validates and folds as a view" do
    created = run_created(@requested_by)
    assert {:ok, ^created} = Event.validate_view(created)
    assert {:ok, _state} = Fold.fold_views([created])
  end

  test "the historical literal operator validates and folds as a view, as it reads" do
    created = run_created("operator")
    assert {:ok, ^created} = Event.validate_read(created)
    assert {:ok, ^created} = Event.validate_view(created)
    assert {:ok, _state} = Fold.fold_views([created])
  end

  test "an arbitrary requested_by string is refused in every mode" do
    created = run_created("someone-else")

    for validate <- [&Event.validate_read/1, &Event.validate_view/1, &Event.validate_append/1] do
      assert match?({:error, _}, validate.(created))
    end

    assert match?({:error, _}, Fold.fold_views([created]))
  end
end
