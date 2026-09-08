defmodule AiOrchestrator.Contract.ArtifactBaselineTest do
  @moduledoc """
  The baseline grammar is owned once (Contract.ArtifactBaseline) and the journal schema
  must agree with it: every value is accepted or refused identically by both, so the host's
  boundary, the pane adapter, and the journal cannot drift apart.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.ArtifactBaseline
  alias AiOrchestrator.Journal.Schemas.EventData

  @hash "sha256:" <> String.duplicate("ab", 32)

  @recorded [
    %{"exists" => false},
    %{"exists" => true, "bytes" => 0, "mtime_unix" => 0, "sha256" => @hash},
    %{"exists" => true, "bytes" => 23, "mtime_unix" => 1_767_225_000, "sha256" => @hash}
  ]

  @refused [
    %{"exists" => false, "unexpected" => "private baseline payload"},
    %{"exists" => true},
    %{"exists" => true, "bytes" => -1, "mtime_unix" => 0, "sha256" => @hash},
    %{"exists" => true, "bytes" => 1, "mtime_unix" => 0, "sha256" => "sha256:short"},
    # M5: a trailing newline on an otherwise valid hash -- the shared line pattern's `$`
    # would admit it; the baseline grammar is anchored absolutely on both sides.
    %{"exists" => true, "bytes" => 1, "mtime_unix" => 0, "sha256" => @hash <> "\n"},
    %{"exists" => true, "bytes" => 1, "mtime_unix" => 0, "sha256" => @hash, "path" => "lib/x.ex"},
    %{"status" => "unrecorded"},
    %{"status" => "unrecorded", "exists" => false},
    %{},
    nil,
    "sha256:not-a-map"
  ]

  test "recorded shapes are accepted by the contract and by the journal's line schemas" do
    for value <- @recorded, mode <- [:read, :append] do
      assert ArtifactBaseline.recorded?(value), inspect(value)
      assert match?({:ok, _}, Zoi.parse(EventData.artifact_baseline_schema(mode), value)), "#{mode} #{inspect(value)}"
    end
  end

  test "everything else is refused by the contract and by the journal's line schemas alike" do
    # View mode admits exactly one non-recorded shape, the unrecorded claim, pinned below.
    for value <- @refused,
        mode <- [:read, :append, :view],
        not (mode == :view and ArtifactBaseline.unrecorded?(value)) do
      refute ArtifactBaseline.recorded?(value), inspect(value)
      assert match?({:error, _}, Zoi.parse(EventData.artifact_baseline_schema(mode), value)), "#{mode} #{inspect(value)}"
    end
  end

  test "the unrecorded claim is a view shape: known to the contract, admitted only by view mode" do
    assert ArtifactBaseline.unrecorded?(%{"status" => "unrecorded"})
    refute ArtifactBaseline.recorded?(%{"status" => "unrecorded"})
    assert match?({:ok, _}, Zoi.parse(EventData.artifact_baseline_schema(:view), %{"status" => "unrecorded"}))
    refute ArtifactBaseline.unrecorded?(%{"status" => "unrecorded", "exists" => false})
  end
end
