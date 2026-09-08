defmodule AiOrchestrator.Lifecycle.ReducerParityTest do
  @moduledoc """
  Gate C stop condition: for every scenario case, the one execution path
  (`RunFSM` façade -> `Host` -> pure `Reducer`) reproduces, byte for byte, the
  events and fold summary captured from the sequential supervisor at greenfield
  601010f (the committed oracle under FixedClock/FixedId), or the identical
  rejection. The façade and the host are asserted to be the same engine.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @oracle_dir Path.expand("../fixtures/contracts/parity", __DIR__)

  for {{name, _kind, _scenario, _prior, _opts_fun}, index} <- Enum.with_index(H.cases()) do
    test "oracle parity: #{name}" do
      {_name, kind, scenario, prior, opts_fun} = Enum.at(H.cases(), unquote(index))

      via_facade = drive(RunFSM, kind, scenario, prior, opts_fun)
      via_host = drive(Host, kind, scenario, prior, opts_fun)

      assert normalize(via_host) == oracle(unquote(index), unquote(name))
      assert normalize(via_facade) == normalize(via_host)
      assert_receipts(via_host, length(prior))
    end
  end

  defp drive(engine, kind, scenario, prior, opts_fun) do
    H.reset_seams()
    opts = opts_fun.()

    case kind do
      :run -> engine.run(H.spec(scenario), H.plan(scenario), opts)
      :resume -> engine.resume(H.spec(scenario), H.plan(scenario), prior, opts)
      :cancel -> engine.cancel(prior, opts)
    end
  end

  # Events compare as persisted bytes; the fold summary must agree too. The comparison is
  # stamp-agnostic: the harness journals through an in-memory receipt sink that stamps every line
  # the way the Writer does (`schema_version`, `prev_line_sha256`), because a gate releases only
  # against a persisted receipt; the oracle pins the reducer's output, not the envelope stamps.
  @stamps ~w(prev_line_sha256 schema_version)

  defp normalize({:ok, %{events: events} = result}) do
    {:ok, Enum.map(events, &unstamped/1), Map.get(result, :summary)}
  end

  defp normalize(other), do: other

  # every event this run appended carries the receipt stamps of a version-2 envelope; prior lines
  # are carried as read
  defp assert_receipts({:ok, %{events: events}}, prior_count) do
    for %{"seq" => seq} = event <- Enum.drop(events, prior_count) do
      assert event["schema_version"] == 2, "appended seq #{seq} lacks a version-2 receipt"
      assert is_binary(event["prev_line_sha256"]) and event["prev_line_sha256"] =~ ~r/\Asha256:[0-9a-f]{64}\z/
    end
  end

  defp assert_receipts(_other, _prior_count), do: :ok

  defp unstamped(event) when is_map(event), do: event |> Map.drop(@stamps) |> Jason.encode!()
  defp unstamped(line) when is_binary(line), do: line |> Jason.decode!() |> unstamped()

  # The committed oracle pins the pre-split supervisor's output so a drifting engine cannot
  # hide behind itself: the bytes captured before the split remain the reference.
  defp oracle(index, name) do
    base = Path.join(@oracle_dir, oracle_basename(index, name))
    lines = base |> Kernel.<>(".jsonl") |> File.read!() |> String.split("\n", trim: true)
    summary = base |> Kernel.<>(".summary.json") |> File.read!() |> Jason.decode!()
    {:ok, Enum.map(lines, &unstamped/1), summary}
  end

  defp oracle_basename(index, name) do
    String.pad_leading(Integer.to_string(index + 1), 2, "0") <> "_" <> String.replace(name, " ", "_")
  end
end
