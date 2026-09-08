defmodule AiOrchestrator.Lifecycle.ReducerContractTest do
  @moduledoc """
  The reducer speaks AiOrchestrator.Contract: every yielded effect is a Contract
  effect struct, every observation the host feeds back is one of that effect's
  admissible observation structs, and the typed correlation field agrees.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @effect_modules [
    Effect.Clock,
    Effect.RetainPrompt,
    Effect.FetchPrompt,
    Effect.SnapshotArtifact,
    Effect.Dispatch,
    Effect.Observe,
    Effect.ReadReview,
    Effect.RunGate,
    Effect.PrepareGate,
    Effect.ReleaseGate,
    Effect.AwaitGate,
    Effect.ReconcileGate
  ]
  @contract Path.expand("../../docs/contracts/lifecycle-effect-contract.org", __DIR__)
  @host_test Path.expand("host_test.exs", __DIR__)

  test "the first yield of a fresh run is a 1-based clock read for the assignment deadline" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_contract_0001", supervisor_instance: "sup_contract_0001")

    assert {:effect, %Effect.Clock{read_index: 1, purpose: purpose}, state, _events} =
             Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    assert is_binary(purpose) and purpose != ""

    now = %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

    # Blob strictly precedes event: the first assignment-correlated effect asks the host to
    # retain the rendered bytes, and only the object it answers with can name the projection.
    assert {:effect, %Effect.RetainPrompt{assignment_id: "as_0001", bytes: %SensitiveBytes{} = bytes}, at_retain, _events} =
             Reducer.step(state, %Observation.Clock{read_index: 1, now: now})

    # MUST-7: the retained object is followed by the artifact snapshot, and only then by the dispatch.
    assert {:effect, %Effect.SnapshotArtifact{assignment_id: "as_0001"}, at_snapshot, _events} =
             Reducer.step(at_retain, %Observation.PromptRetained{object: retained("as_0001", bytes), now: now})

    assert {:effect, %Effect.Dispatch{assignment_id: "as_0001", command: command, message_id: message_id}, _state,
            _events} =
             Reducer.step(at_snapshot, %Observation.ArtifactSnapshot{
               assignment_id: "as_0001",
               baseline: %{"exists" => false},
               now: now
             })

    assert message_id == command["send_message_id"]
  end

  for {{name, _kind, _scenario, _prior, _opts_fun}, index} <- Enum.with_index(H.cases()) do
    test "every effect/observation pair is typed, admissible, and correlated: #{name}" do
      {_name, kind, scenario, prior, opts_fun} = Enum.at(H.cases(), unquote(index))
      {:ok, trace} = Agent.start_link(fn -> [] end)
      H.reset_seams()

      opts =
        Keyword.put(opts_fun.(), :effect_observer, fn effect, observation ->
          Agent.update(trace, &(&1 ++ [{effect, observation}]))
        end)

      result =
        case kind do
          :run -> Host.run(H.spec(scenario), H.plan(scenario), opts)
          :resume -> Host.resume(H.spec(scenario), H.plan(scenario), prior, opts)
          :cancel -> Host.cancel(prior, opts)
        end

      assert {:ok, _} = result
      pairs = Agent.get(trace, & &1)
      if kind == :run, do: assert(pairs != [])

      for {effect, observation} <- pairs do
        assert effect.__struct__ in @effect_modules
        assert observation.__struct__ in Effect.admissible_observations(effect)
        assert %Moment{} = observation.now
        assert correlation(effect) == correlation(observation)
      end
    end
  end

  test "a suspended state carries the pending effect and its continuation, and no observation transcript" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_contract_0002", supervisor_instance: "sup_contract_0002")

    assert {:effect, %Effect.Clock{} = clock, state, _events} = Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    assert %Reducer{pending: ^clock, stack: [_ | _]} = state

    # The machine answers "what is pending" from the struct, not from a log of what it has seen.
    fields = state |> Map.from_struct() |> Map.keys()
    refute :observations in fields
  end

  test "a terminal outcome returns without suspending and names no pending effect" do
    {_name, :cancel, _scenario, prior_lines, opts_fun} = Enum.find(H.cases(), &(elem(&1, 1) == :cancel))
    H.reset_seams()
    prior = Enum.map(prior_lines, &Jason.decode!/1)

    opts =
      opts_fun.()
      |> Keyword.merge(run_id: "run_contract_0005", supervisor_instance: "sup_contract_0005")
      |> Keyword.put(:prior_count, length(prior))

    # Suspension is one of two outcomes, not the only one: cancel reaches a terminal outcome directly.
    assert {:done, %Reducer{pending: nil, stack: []}, _events} = Reducer.cancel(prior, opts)
  end

  test "an admissible observation carrying the wrong correlation id is rejected without consuming the state" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_contract_0003", supervisor_instance: "sup_contract_0003")
    now = %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

    assert {:effect, %Effect.Clock{read_index: 1} = clock, at_clock, _events} =
             Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    # Right struct type, wrong read index: only the correlation conjunct can reject this.
    stale_clock = %Observation.Clock{read_index: 2, now: now}
    assert stale_clock.__struct__ in Effect.admissible_observations(clock)

    assert {:error,
            %{
              "reason" => "observation_mismatch",
              "expected" => %{"kind" => "clock", "correlation" => "1"},
              "observed" => %{"kind" => "clock", "correlation" => "2"}
            }} = Reducer.step(at_clock, stale_clock)

    # The rejected step neither consumed nor mutated the suspension: the same state still steps.
    assert {:effect, %Effect.RetainPrompt{assignment_id: "as_0001", bytes: bytes} = retain, at_retain, _events} =
             Reducer.step(at_clock, %Observation.Clock{read_index: 1, now: now})

    # Same shape one boundary down, over an assignment-correlated effect whose observation
    # carries the assignment inside the object rather than beside it.
    other_retained = %Observation.PromptRetained{object: retained("as_not_this_one", bytes), now: now}
    assert other_retained.__struct__ in Effect.admissible_observations(retain)

    assert {:error,
            %{
              "reason" => "observation_mismatch",
              "expected" => %{"kind" => "retain_prompt", "correlation" => "as_0001"},
              "observed" => %{"kind" => "prompt_retained", "correlation" => "as_not_this_one"}
            }} = Reducer.step(at_retain, other_retained)

    assert {:effect, %Effect.SnapshotArtifact{assignment_id: "as_0001"}, at_snapshot, _events} =
             Reducer.step(at_retain, %Observation.PromptRetained{object: retained("as_0001", bytes), now: now})

    assert {:effect, %Effect.Dispatch{assignment_id: "as_0001"} = dispatch, at_dispatch, _events} =
             Reducer.step(at_snapshot, %Observation.ArtifactSnapshot{
               assignment_id: "as_0001",
               baseline: %{"exists" => false},
               now: now
             })

    other_dispatched = %Observation.Dispatched{assignment_id: "as_not_this_one", result: %{}, now: now}
    assert other_dispatched.__struct__ in Effect.admissible_observations(dispatch)

    assert {:error,
            %{
              "reason" => "observation_mismatch",
              "expected" => %{"kind" => "dispatch", "correlation" => "as_0001"},
              "observed" => %{"kind" => "dispatched", "correlation" => "as_not_this_one"}
            }} = Reducer.step(at_dispatch, other_dispatched)

    assert {:effect, %Effect.Observe{assignment_id: "as_0001"}, _state, _events} =
             Reducer.step(at_dispatch, %Observation.Dispatched{
               assignment_id: "as_0001",
               result: %{"send_status" => "ok"},
               now: now
             })
  end

  test "one step advances exactly one effect/observation boundary and one gapless event suffix" do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    {:ok, trace} = Agent.start_link(fn -> [] end)
    H.reset_seams()

    identity = [run_id: "run_contract_0004", supervisor_instance: "sup_contract_0004"]

    observed_opts =
      opts_fun.()
      |> Keyword.merge(identity)
      |> Keyword.put(:effect_observer, fn effect, observation ->
        Agent.update(trace, &(&1 ++ [{effect, observation}]))
      end)

    assert {:ok, %{events: events}} = Host.run(H.spec(scenario), H.plan(scenario), observed_opts)
    pairs = Agent.get(trace, & &1)
    assert pairs != []

    # Feed the host's observations back into a bare machine drive: same effects, one step each.
    H.reset_seams()
    drive_opts = Keyword.merge(opts_fun.(), identity)
    {suffixes, steps} = drive(Reducer.init(H.spec(scenario), H.plan(scenario), drive_opts), pairs, [])

    assert steps == length(pairs)

    # No committed event is ever re-emitted: the suffixes concatenate to the journal exactly once.
    seqs = suffixes |> Enum.reverse() |> List.flatten() |> Enum.map(& &1["seq"])
    assert seqs == Enum.to_list(1..length(events))
  end

  test "the checked-in contract describes the machine this checkpoint actually ships" do
    doc = @contract |> File.read!() |> String.replace(~r/\s+/, " ")

    assert doc =~ "an explicit suspension machine, not an observation-log replay coroutine"
    # NS-02: dropping the observation log did not weaken journal replay, which stays real and pure.
    assert doc =~ "Journal replay itself remains real and pure"
    assert doc =~ "that fold is the only way a run is rehydrated"
    assert doc =~ "run until an effect site or a terminal outcome"
    assert doc =~ "only the event suffix emitted since the previous suspension"
    assert doc =~ "returns without suspending and names no pending effect"
    assert doc =~ "a suffix must continue the journal by sequence number with neither a gap nor an overlap"
    assert doc =~ "=Reducer.step/2= then consumes exactly one observation and advances exactly one"
    assert doc =~ "its typed correlation id equals the pending effect's"
    assert doc =~ "a rejected step neither consumes nor mutates the suspended state"
    assert doc =~ "The machine retains no observation transcript."

    # The superseded mechanism: the host no longer compares a regenerated prefix.
    refute doc =~ "prefix comparison"
    refute doc =~ "regenerated"
    refute doc =~ "replay reuses"

    host_test = @host_test |> File.read!() |> String.replace(~r/\s+/, " ")
    assert host_test =~ "an observation that does not answer the effect the machine is suspended on"
    refute host_test =~ "observation log"
    refute host_test =~ "regenerated effect key"
  end

  defp drive({:effect, effect, state, suffix}, [{expected, observation} | rest], suffixes) do
    assert effect == expected
    {suffixes, steps} = drive(Reducer.step(state, observation), rest, [suffix | suffixes])
    {suffixes, steps + 1}
  end

  defp drive({:done, _state, suffix}, [], suffixes), do: {[suffix | suffixes], 0}

  # The object a store would answer with for these bytes: version 2, named by the digest.
  defp retained(assignment_id, %SensitiveBytes{} = bytes) do
    "sha256:" <> hex = hash = SensitiveBytes.hash(bytes)

    {:ok, object} =
      PromptObject.new(%{
        assignment_id: assignment_id,
        path: "prompts/#{assignment_id}-#{hex}.org",
        hash: hash,
        byte_size: SensitiveBytes.byte_size(bytes),
        version: 2
      })

    object
  end

  defp correlation(%{read_index: index}), do: {:read_index, index}
  defp correlation(%{gate_run_id: id}), do: {:gate_run_id, id}
  defp correlation(%{assignment_id: id}), do: {:assignment_id, id}
  defp correlation(%{object: %PromptObject{assignment_id: id}}), do: {:assignment_id, id}
end
