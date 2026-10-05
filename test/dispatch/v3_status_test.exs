defmodule AiOrchestrator.Dispatch.V3StatusTest do
  @moduledoc """
  NS-15.G.005 B3b GREEN G1: an ok version 3 status must carry all four status fields of ipc-v3.org "Status" with
  their contract types; a valid identity alone is not enough.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.V3Status

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @pane "pane_writer"

  defp ok_status do
    %{"<pane_id>" => @pane, "<registration_id>" => "reg_" <> String.duplicate("a", 32), "<generation>" => "7"}
    |> Enum.reduce(File.read!(Path.join(@fixtures, "status.ok.json")), fn {placeholder, value}, bytes ->
      String.replace(bytes, placeholder, value)
    end)
    |> Jason.decode!()
  end

  test "control: the substituted ok fixture decodes" do
    assert match?({:ok, _status}, V3Status.decode(Jason.encode!(ok_status()), @pane))
  end

  test "an ok status missing any of state, quarantined, queue_depth or pane_pid is a reply-identity error" do
    for field <- ~w(state quarantined queue_depth pane_pid) do
      bytes = ok_status() |> Map.delete(field) |> Jason.encode!()
      assert V3Status.decode(bytes, @pane) == {:error, :reply_identity, "status_fields"}, field
    end
  end

  test "an ok status whose status fields have the wrong type or range is a reply-identity error" do
    for {field, value} <- [
          {"state", ""},
          {"state", 1},
          {"quarantined", "false"},
          {"queue_depth", -1},
          {"queue_depth", "0"},
          {"pane_pid", 0},
          {"pane_pid", "4242"}
        ] do
      bytes = ok_status() |> Map.put(field, value) |> Jason.encode!()
      assert V3Status.decode(bytes, @pane) == {:error, :reply_identity, "status_fields"}, inspect({field, value})
    end
  end
end
