defmodule AiOrchestrator.Lifecycle.HostTest do
  @moduledoc """
  Gate C host properties beyond parity: intent events are durable before the
  effect they describe executes, and an observation that does not answer the
  effect the machine is suspended on is a first-class error, never a silent skip.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Lifecycle.Core.Diagnostic
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  defmodule RecordingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, opts) do
      record(opts, {:dispatch, command["assignment_id"]})
      opts[:inner].deliver(command, opts[:inner_opts])
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, opts) do
      record(opts, {:observe, command["assignment_id"]})
      opts[:inner].observe(command, opts[:inner_opts])
    end

    defp record(opts, entry), do: Agent.update(opts[:trace], &(&1 ++ [entry]))
  end

  test "every intent event is journaled before its effect executes" do
    {:ok, trace} = Agent.start_link(fn -> [] end)
    {name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    assert name == "gated_run_seed run"

    H.reset_seams()
    opts = opts_fun.()

    recording_opts =
      Keyword.merge(opts,
        dispatch: RecordingDispatch,
        dispatch_opts: [inner: opts[:dispatch], inner_opts: Keyword.get(opts, :dispatch_opts, []), trace: trace],
        event_sink:
          GateDouble.receipt(fn event ->
            Agent.update(trace, &(&1 ++ [{:sink, event["type"], event["data"]["assignment_id"]}]))
            {:ok, event}
          end)
      )

    assert {:ok, %{summary: %{"status" => "completed"}}} = Host.run(H.spec(scenario), H.plan(scenario), recording_opts)

    entries = Agent.get(trace, & &1)
    dispatches = for {{:dispatch, _} = entry, index} <- Enum.with_index(entries), do: {entry, index}
    assert dispatches != []

    for {{:dispatch, assignment_id}, index} <- dispatches do
      before = Enum.take(entries, index)
      assert {:sink, "assignment_prompt_projected", assignment_id} in before
      refute {:sink, "assignment_dispatch_sent", assignment_id} in before
      assert {:sink, "assignment_dispatch_sent", assignment_id} in Enum.drop(entries, index + 1)
    end
  end

  test "an observation that does not answer the pending effect is an error step" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_divergence_0001", supervisor_instance: "sup_divergence_0001")

    assert {:effect, effect, state, _events} = Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    assert {:error, %{"reason" => "observation_mismatch", "expected" => expected, "observed" => observed}} =
             Reducer.step(state, {:bogus, "never"})

    assert expected == Diagnostic.describe(effect)
    assert %{"result_class" => "tuple", "digest" => "sha256:" <> _} = observed
    refute inspect(expected) =~ "purpose"
  end
end
