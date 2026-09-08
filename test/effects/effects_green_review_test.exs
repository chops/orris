defmodule AiOrchestrator.Effects.GreenReviewTest do
  # Imported assertion-preserving from Codex's review probes (/tmp/effects_green_review_test.exs,
  # m_1788634618000): total cleanup, primary-failure preservation, post-abandon runtime, exactly-once cleanup.
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Interrupted
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule Executor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate await(handle, opts), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    def ack(_handle, _receipt), do: {:error, %{clause: "ack_mismatch"}}

    def abandon(handle) do
      attempts = Process.get(:review_attempts, [])
      Process.put(:review_attempts, attempts ++ [handle])

      case Process.get(:review_mode) do
        :bad_return ->
          if attempts == [], do: {:error, %RuntimeError{message: "cleanup sentinel"}}, else: :ok

        :clock_after_abandon ->
          Process.put(:review_clock_fault, true)
          :ok

        _ ->
          :ok
      end
    end
  end

  defmodule Clock do
    @moduledoc false
    defdelegate wall_ts(), to: FixedClock

    def unix_now do
      if Process.delete(:review_clock_fault), do: raise("post-abandon clock")
      FixedClock.unix_now()
    end
  end

  defp caught(fun) do
    {:return, fun.()}
  catch
    kind, reason -> {kind, reason, __STACKTRACE__}
  end

  defp opts do
    [
      gate_executor: Executor,
      gate_helper: GateDouble.helper(),
      clock: Clock,
      run_id: "run_fixture_0001",
      supervisor_instance: "sup_0001"
    ]
  end

  defp prepared do
    effect = %Effect.PrepareGate{
      gate_run_id: "gr_0001",
      attempt: 1,
      requested: %{"command_argv" => ["true"]},
      deadline_unix: 4_102_444_800,
      repo_root: "/tmp",
      run_dir: "/tmp"
    }

    {_, rt} = Effects.execute(effect, Runtime.new(opts()), opts: opts())
    rt
  end

  test "settlement catches normalization failures and still attempts every handle" do
    Process.put(:review_mode, :bad_return)

    rt =
      opts()
      |> Runtime.new()
      |> Runtime.put({"gr_0001", 1}, :prepared, :first)
      |> Runtime.put({"gr_0002", 1}, :running, :second)

    result = caught(fn -> Effects.settle(rt) end)
    assert {:return, {reports, %Runtime{gates: gates}}} = result
    assert gates == %{}
    assert length(Process.get(:review_attempts, [])) == 2
    assert Enum.any?(reports, &(&1["settle"]["clause"] == "settle_unproven"))
  end

  test "Host preserves original observer throw if cleanup returns a struct" do
    Process.put(:review_mode, :bad_return)

    {_, :run, "gated_run_seed", [], make_opts} =
      Enum.find(ScenarioHarness.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

    ScenarioHarness.reset_seams()

    observer = fn
      %Effect.PrepareGate{}, _ ->
        try do
          throw(:primary_review_failure)
        catch
          kind, reason ->
            Process.put(:review_original, {kind, reason, __STACKTRACE__})
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      _, _ ->
        :ok
    end

    options = make_opts.() |> Keyword.put(:gate_executor, Executor) |> Keyword.put(:effect_observer, observer)

    result =
      caught(fn -> Host.run(ScenarioHarness.spec("gated_run_seed"), ScenarioHarness.plan("gated_run_seed"), options) end)

    assert Process.get(:review_original)
    assert result == Process.get(:review_original)
  end

  test "failure after rejected release carries the post-abandon runtime" do
    rt = prepared()
    Process.put(:review_mode, :clock_after_abandon)
    effect = %Effect.ReleaseGate{gate_run_id: "gr_0001", attempt: 1, started_seq: 27}
    result = caught(fn -> Effects.execute(effect, rt, opts: opts(), receipt: nil) end)
    assert {:error, %Interrupted{runtime: latest, reason: %RuntimeError{message: "post-abandon clock"}}, _} = result
    assert latest.gates == %{}
    assert length(Process.get(:review_attempts, [])) == 1
  end

  test "Host does not repeat abandonment after rejection observation clock fails" do
    Process.put(:review_mode, :clock_after_abandon)

    {_, :run, "gated_run_seed", [], make_opts} =
      Enum.find(ScenarioHarness.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

    ScenarioHarness.reset_seams()
    options = make_opts.() |> Keyword.put(:gate_executor, Executor) |> Keyword.put(:clock, Clock)

    result =
      caught(fn -> Host.run(ScenarioHarness.spec("gated_run_seed"), ScenarioHarness.plan("gated_run_seed"), options) end)

    assert {:error, %RuntimeError{message: "post-abandon clock"}, _} = result
    assert length(Process.get(:review_attempts, [])) == 1
  end
end
