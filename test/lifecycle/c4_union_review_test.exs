defmodule C4UnionReviewTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  defmodule Executor do
    @moduledoc false
    def prepare(fs, req, opts) do
      case Process.get(:probe_prepare) do
        nil -> GateDouble.prepare(fs, req, opts)
        result -> result
      end
    end

    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble

    def release(handle, ack, opts) do
      if Process.get(:probe_raise_release), do: raise("release probe")
      GateDouble.release(handle, ack, opts)
    end

    def await(handle, opts), do: Process.get(:probe_await) || GateDouble.await(handle, opts)

    def abandon(_handle) do
      Process.put(:probe_abandoned, true)
      Process.get(:probe_abandon_result, :ok)
    end
  end

  setup do
    H.reset_seams()
    :ok
  end

  defp run(extra \\ []) do
    {_, :run, "gated_run_seed", [], make_opts} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    opts = make_opts.() |> Keyword.put(:gate_executor, Executor) |> Keyword.merge(extra)
    Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
  end

  # (assertion revised for Effects extraction, disclosed: the registry leaves the process dictionary,
  # so `Process.get(:host_gate_registry, %{}) == %{}` would pass vacuously; the observable facts are
  # that nothing was left to settle - no gate_cleanup on the result - and abandon never ran)
  test "control: the unmodified executor completes with nothing left to settle" do
    assert {:ok, %{summary: %{"status" => "completed"}} = result} = run()
    refute Map.has_key?(result, :gate_cleanup)
    assert Process.get(:probe_abandoned) == nil
  end

  test "control: valid settlement facts survive diagnostics" do
    Process.put(
      :probe_prepare,
      {:error, %{clause: "claim_unpublished", settle: %{settled: true, proof: "gone", leftovers: "0"}}}
    )

    assert {:ok, %{events: events}} = run()
    attention = Enum.find(events, &(&1["type"] == "human_attention_required"))
    assert get_in(attention, ["data", "detail", "settle"]) == %{"settled" => true, "proof" => "gone", "leftovers" => "0"}
  end

  test "release exception still abandons the prepared handle" do
    Process.put(:probe_raise_release, true)
    assert_raise RuntimeError, "release probe", fn -> run() end
    assert Process.get(:probe_abandoned) == true
  end

  test "attention return settles and clears the retained handle" do
    Process.put(:probe_await, {:error, %{clause: "output_unreadable"}})
    assert {:ok, %{summary: %{"status" => "blocked"}} = result} = run()
    assert Process.get(:probe_abandoned) == true
    # revised for Effects extraction (disclosed): the settled handle is reported, not merely gone
    assert [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{}}] = result.gate_cleanup
  end

  test "false settlement remains false in journal diagnostics" do
    Process.put(
      :probe_prepare,
      {:error, %{clause: "claim_unpublished", settle: %{settled: false, proof: "unknown", leftovers: "unknown"}}}
    )

    assert {:ok, %{events: events}} = run()
    attention = Enum.find(events, &(&1["type"] == "human_attention_required"))
    assert get_in(attention, ["data", "detail", "settle", "settled"]) == false
  end

  test "off-domain settlement text cannot enter journal diagnostics" do
    Process.put(
      :probe_prepare,
      {:error,
       %{clause: "claim_unpublished", settle: %{settled: false, proof: "unknown", leftovers: "PROBE_PRIVATE_MARKER"}}}
    )

    assert {:ok, %{events: events}} = run()
    leaked = Jason.encode!(events) =~ "PROBE_PRIVATE_MARKER"
    refute leaked, "off-domain value reached the journal"
  end

  test "cleanup rejection cannot bypass the closed diagnostic mapper" do
    Process.put(:probe_abandon_result, {:error, %{clause: "PROBE_PRIVATE_MARKER"}})

    sink =
      GateDouble.receipt(fn event ->
        if event["type"] == "gate_passed", do: {:error, %{clause: "sink_probe"}}, else: :ok
      end)

    assert {:error, rejection} = run(event_sink: sink)
    leaked = Jason.encode!(rejection) =~ "PROBE_PRIVATE_MARKER"
    refute leaked, "off-domain value reached cleanup diagnostics"
  end

  test "missing timeout duration becomes attention rather than a malformed terminal" do
    Process.put(:probe_await, {:timeout, %{kind: "timeout", settled: true, proof: "gone", leftovers: "0"}})
    assert {:ok, %{events: events}} = run()

    assert Enum.any?(
             events,
             &(&1["type"] == "human_attention_required" and &1["data"]["reason"] == "gate_evidence_unreadable")
           )

    refute Enum.any?(events, &(&1["type"] == "gate_failed"))
  end
end
