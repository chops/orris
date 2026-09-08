defmodule C4Rev2ReviewTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  defmodule Executor do
    @moduledoc false
    def prepare(fs, request, opts) do
      Process.put(:prepared, [request | Process.get(:prepared, [])])
      GateDouble.prepare(fs, request, opts)
    end

    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    def release(handle, ack, opts), do: Process.get(:release_result) || GateDouble.release(handle, ack, opts)
    def await(handle, opts), do: Process.get(:await_result) || GateDouble.await(handle, opts)
    def abandon(_handle), do: Process.get(:abandon_result, :ok)
    def evidence(dir, id, attempt), do: Process.get(:evidence_result) || GateDouble.evidence(dir, id, attempt)
    def reconcile(_, _, _, _), do: {:dead, %{"leader" => "gone", "group" => "gone", "members" => 0}}
  end

  setup do
    H.reset_seams()
    :ok
  end

  defp opts do
    {_, :run, "gated_run_seed", [], make_opts} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    Keyword.put(make_opts.(), :gate_executor, Executor)
  end

  defp run, do: Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts())

  test "control: ordinary run succeeds" do
    assert {:ok, %{summary: %{"status" => "completed"}}} = run()
  end

  test "release rejection preserves unsuccessful abandonment evidence" do
    Process.put(:release_result, {:error, %{clause: "ack_mismatch"}})
    Process.put(:abandon_result, {:error, %{clause: "settle_unproven"}})
    assert {:ok, result} = run()
    found = Jason.encode!(result) =~ "settle_unproven"
    assert found, "release failure must not discard cleanup failure"
  end

  for {label, evidence} <- [
        {"missing", %{"stderr_hash" => "sha256:" <> String.duplicate("ab", 32)}},
        {"nonhex",
         %{
           "stdout_hash" => "sha256:" <> String.duplicate("z", 64),
           "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
         }}
      ] do
    test "#{label} timeout hash is attention, not an invalid terminal" do
      Process.put(:evidence_result, {:ok, unquote(Macro.escape(evidence))})

      Process.put(
        :await_result,
        {:timeout, %{kind: "timeout", settled: true, proof: "gone", leftovers: "0", duration_ms: 3}}
      )

      assert {:ok, %{events: events}} = run()

      assert Enum.any?(
               events,
               &(&1["type"] == "human_attention_required" and &1["data"]["reason"] == "gate_evidence_unreadable")
             )

      refute Enum.any?(events, &(&1["type"] == "gate_failed"))
    end
  end

  defp resume_at(start_ts) do
    events = "scenarios" |> F.lines("gated_run_seed") |> Enum.map(&Jason.decode!/1)
    {prefix, [start | _]} = Enum.split_while(events, &(&1["type"] != "gate_started"))

    data =
      Map.merge(start["data"], %{
        "attempt" => 1,
        "deadline_unix" => 4_102_444_800,
        "stdout_path" => "gates/gr_0001.1.out",
        "stderr_path" => "gates/gr_0001.1.err",
        "execution" => %{
          "pid" => 4242,
          "pgid" => 4242,
          "start" => "1756728000.123456",
          "claim_hash" => "sha256:" <> String.duplicate("ab", 32)
        }
      })

    start = Map.merge(start, %{"event_version" => 2, "ts" => start_ts, "data" => data})
    assert {:ok, _} = Event.validate_read(start)
    prior = Enum.map(prefix ++ [start], &Jason.encode!/1)
    Host.resume(H.spec("gated_run_seed"), H.plan("gated_run_seed"), prior, opts())
  end

  test "control: dead first attempt before deadline may retry without clock skew" do
    assert {:ok, _} = resume_at("2026-09-01T11:00:00Z")
    assert Enum.any?(Process.get(:prepared, []), &(&1.gate_run_id == "gr_0001" and &1.attempt == 2))
  end

  test "clock skew is attention before the first-attempt retry decision" do
    assert {:ok, %{events: events}} = resume_at("2026-09-01T12:30:00Z")
    refute Enum.any?(Process.get(:prepared, []), &(&1.gate_run_id == "gr_0001")), "clock skew must not authorize a retry"

    assert Enum.any?(
             events,
             &(&1["type"] == "human_attention_required" and &1["data"]["reason"] == "gate_recovery_clock_skew")
           )
  end
end
