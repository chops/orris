defmodule AiOrchestrator.Lifecycle.RunFSMSinkTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Lifecycle.RunFSM

  @repair %{"action" => "truncate_tail", "truncated_bytes" => 9, "receipt_seq_before" => 0, "receipt_seq_after" => 0}

  defp prior_lines do
    [__DIR__, "..", "fixtures", "contracts", "scenarios", "kill9_resume", "events_pre_dispatch.jsonl"]
    |> Path.join()
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  test "a sink that returns the persisted event replaces the in-memory event" do
    anchor = Chain.anchor()
    sink = fn event -> {:ok, event |> Map.put("schema_version", 2) |> Map.put("prev_line_sha256", anchor)} end
    assert {:ok, %{appended_events: appended}} = RunFSM.cancel(prior_lines(), event_sink: sink)
    assert length(appended) >= 2
    assert Enum.all?(appended, &(&1["schema_version"] == 2 and &1["prev_line_sha256"] == anchor))
  end

  test "a plain :ok sink keeps the built event" do
    assert {:ok, %{appended_events: appended}} = RunFSM.cancel(prior_lines(), event_sink: fn _ -> :ok end)
    assert Enum.all?(appended, &(&1["schema_version"] == 1))
  end

  test "a sink rejection becomes a named result instead of a raise" do
    sink = fn _ -> {:error, %{clause: "append_failed", stage: "write", detail: ":enospc"}} end

    assert {:error, %{"reason" => "journal_append_failed", "clause" => "append_failed", "stage" => "write"}} =
             RunFSM.cancel(prior_lines(), event_sink: sink)
  end

  test "an invalid sink result is also a named result" do
    assert {:error, %{"reason" => "journal_append_failed", "clause" => "invalid_sink_result"}} =
             RunFSM.cancel(prior_lines(), event_sink: fn _ -> :nope end)
  end

  test "cancel journals the tail repair the writer performed on open" do
    assert {:ok, %{appended_events: [requested | _]}} = RunFSM.cancel(prior_lines(), tail_repair: @repair)
    assert %{"type" => "run_cancel_requested", "data" => %{"tail_repair" => @repair}} = requested
    assert {:ok, %{appended_events: [plain | _]}} = RunFSM.cancel(prior_lines())
    refute Map.has_key?(plain["data"], "tail_repair")
    _ = F
  end
end
