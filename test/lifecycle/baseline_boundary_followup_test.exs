defmodule AiOrchestrator.Lifecycle.BaselineBoundaryFollowupTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.ArtifactBaseline
  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Test.ScenarioHarness

  test "line grammar agrees with adapter on trailing newline in baseline hash" do
    value = %{
      "exists" => true,
      "bytes" => 1,
      "mtime_unix" => 1,
      "sha256" => "sha256:" <> String.duplicate("a", 64) <> "\n"
    }

    refute ArtifactBaseline.recorded?(value)

    for mode <- [:read, :append] do
      assert match?({:error, _}, Zoi.parse(EventData.artifact_baseline_schema(mode), value))
    end
  end

  defmodule BadSnapshot do
    @moduledoc false
    def snapshot(_command, _opts), do: {:error, %{"reason" => "private-adapter-marker"}}
  end

  test "snapshot errors cannot journal arbitrary adapter strings as classes" do
    alias AiOrchestrator.Lifecycle.Host
    alias ScenarioHarness, as: H

    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    # The two shapes LocalPane actually returns reach attention as their class and nothing
    # else; a known class with extra adapter data keeps only the class; anything outside the
    # closed set is an invalid return (class/digest), never an event.
    H.reset_seams()
    result = Host.run(H.spec(scenario), H.plan(scenario), Keyword.put(opts_fun.(), :dispatch, BadSnapshot))
    refute inspect(result, limit: :infinity) =~ "private-adapter-marker"
  end

  defmodule ClassifiedSnapshot do
    @moduledoc false
    def snapshot(_command, opts), do: {:error, Keyword.fetch!(opts, :snapshot_error)}
  end

  for {error, expected} <- [
        {%{"reason" => "artifact_baseline_unstable", "detail" => %{"reason" => "artifact_changing"}},
         "artifact_baseline_unstable"},
        {%{"reason" => "artifact_baseline_failed", "detail" => "eacces PRIVATE_PATH_MARKER"}, "artifact_baseline_failed"}
      ] do
    test "snapshot error #{expected} reaches attention as its class only" do
      alias AiOrchestrator.Lifecycle.Host
      alias ScenarioHarness, as: H

      {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
      H.reset_seams()

      opts =
        opts_fun.()
        |> Keyword.put(:dispatch, ClassifiedSnapshot)
        |> Keyword.put(:dispatch_opts, snapshot_error: unquote(Macro.escape(error)))

      assert {:ok, result} = Host.run(H.spec(scenario), H.plan(scenario), opts)
      attention = Enum.find(result.events, &(&1["type"] == "human_attention_required"))
      assert attention["data"]["reason"] == "artifact_baseline_failed"
      assert attention["data"]["detail"] == %{"error" => unquote(expected), "stage" => "snapshot"}
      refute Enum.any?(result.events, &(&1["type"] == "agent_wedge_detected")), "nothing about the pane was measured"
      refute inspect(result, limit: :infinity) =~ "PRIVATE_PATH_MARKER"
      refute inspect(result, limit: :infinity) =~ "artifact_changing"
    end
  end

  test "a snapshot error outside the closed set is an invalid return, not an event" do
    alias AiOrchestrator.Lifecycle.Host
    alias ScenarioHarness, as: H

    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()

    opts =
      opts_fun.()
      |> Keyword.put(:dispatch, ClassifiedSnapshot)
      |> Keyword.put(:dispatch_opts, snapshot_error: %{"reason" => "private-adapter-marker", "extra" => "MORE_PRIVATE"})

    assert {:error, %{"reason" => "dispatch_snapshot_invalid_return"}} =
             Host.run(H.spec(scenario), H.plan(scenario), opts)
  end
end
