defmodule AiOrchestrator.Lifecycle.HostClockTest do
  @moduledoc """
  Contract truth for time (Phase A4): lifecycle wall-clock deadline values in
  event data derive only from correlated Clock observations, while durations and
  stability intervals are executor measurements or configured bounds; the
  envelope `ts` is the append stamp the host writes at commit and is never a
  reducer input.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @contract Path.expand("../../docs/contracts/lifecycle-effect-contract.org", __DIR__)
  @placeholder_ts "0000-00-00T00:00:00Z"

  defmodule ShiftedWallClock do
    @moduledoc "Same unix facts as FixedClock, wall_ts shifted by one hour: only the envelope stamp differs."
    @behaviour AiOrchestrator.Clock

    @impl true
    def wall_ts do
      {:ok, datetime, 0} = DateTime.from_iso8601(FixedClock.wall_ts())
      datetime |> DateTime.shift(hour: 1) |> DateTime.to_iso8601()
    end

    @impl true
    def unix_now, do: FixedClock.unix_now()

    @impl true
    def monotonic_ms, do: FixedClock.monotonic_ms()
  end

  test "the contract names the envelope ts as host append metadata, never reducer input" do
    # The document is hard-wrapped; compare on collapsed whitespace.
    doc = @contract |> File.read!() |> String.replace(~r/\s+/, " ")

    assert doc =~
             "Lifecycle wall-clock deadline values recorded in event data derive only from " <>
               "correlated =Effect.Clock= observations delivered after preceding intent events are durable."

    assert doc =~
             "Durations and stability intervals are effect-executor monotonic measurements or " <>
               "configured bounds; they are never treated as wall-clock facts."

    assert doc =~ "envelope =ts= is the append stamp the host writes at commit"
    assert doc =~ "never a reducer input"
    # The superseded claims: neither the pre-A4 wording nor A4's overbroad one.
    refute doc =~ "Journal-visible times and lifecycle deadlines derive only from"
    refute doc =~ "every time value inside event data derive only from"
  end

  test "the reducer emits placeholder timestamps; the host stamps them at commit" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_clock_0001", supervisor_instance: "sup_clock_0001")

    assert {:effect, %Effect.Clock{}, _state, events} = Reducer.init(H.spec(scenario), H.plan(scenario), opts)
    assert events != []
    assert Enum.all?(events, &(&1["ts"] == @placeholder_ts))
  end

  test "two clocks that differ only in wall_ts produce identical events modulo ts and identical summaries" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)

    H.reset_seams()

    assert {:ok, %{events: fixed_events, summary: fixed_summary}} =
             Host.run(H.spec(scenario), H.plan(scenario), opts_fun.())

    H.reset_seams()
    shifted_opts = Keyword.put(opts_fun.(), :clock, ShiftedWallClock)

    assert {:ok, %{events: shifted_events, summary: shifted_summary}} =
             Host.run(H.spec(scenario), H.plan(scenario), shifted_opts)

    assert length(fixed_events) == length(shifted_events) and fixed_events != []
    assert Enum.map(fixed_events, &Map.delete(&1, "ts")) == Enum.map(shifted_events, &Map.delete(&1, "ts"))
    assert fixed_summary == shifted_summary
    # The stamps themselves differ, so the comparison above is not vacuous.
    assert Enum.map(fixed_events, & &1["ts"]) != Enum.map(shifted_events, & &1["ts"])
    # And deadlines (event-data time values) come from the shared unix facts, so they agree.
    deadlines = fn events -> for %{"data" => %{"deadline_unix" => d}} <- events, do: d end
    assert deadlines.(fixed_events) == deadlines.(shifted_events) and deadlines.(fixed_events) != []
  end
end
