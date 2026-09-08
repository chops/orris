defmodule AiOrchestrator.Lifecycle.RunFSMPromptStateTest do
  @moduledoc """
  What a suspended machine prints. `Diagnostic.describe/1` being payload-free does not help
  when a crashed `Run.Server` prints its state: the redaction has to live on the value, and
  the value has to be the only copy. M1 of the 8cb72c5 review found the bare render held in
  a continuation frame beside the wrapped effect; these tests pin the suspension itself.
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

  @inspect [limit: :infinity, printable_limit: :infinity]
  @now %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}
  @reducer Path.expand("../../lib/ai_orchestrator/lifecycle/core/reducer.ex", __DIR__)
  @kill9 Path.expand("../fixtures/contracts/scenarios/kill9_resume", __DIR__)

  # Codex's review probe, kept as written in spirit: the state suspended on RetainPrompt.
  test "the state suspended on retention prints the wrapper's facts, never the render" do
    {_name, :run, scenario, [], opts_fun} = hd(H.cases())
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), run_id: "run_state_0001", supervisor_instance: "sup_state_0001")

    {:effect, %Effect.Clock{}, at_clock, _} = Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    {:effect, %Effect.RetainPrompt{bytes: wrapped}, at_retain, _} =
      Reducer.step(at_clock, %Observation.Clock{read_index: 1, now: @now})

    refute printed?(at_retain, wrapped), "the continuation frame still prints the full rendered prompt"

    # ...and the state suspended one boundary later, on the dispatch that carries the command.
    # MUST-7: the baseline is taken before the projection is committed, so the retained
    # object is followed by a snapshot, and only then by the dispatch.
    {:effect, %Effect.SnapshotArtifact{}, at_snapshot, _} =
      Reducer.step(at_retain, %Observation.PromptRetained{object: retained(wrapped), now: @now})

    {:effect, %Effect.Dispatch{command: command}, at_dispatch, _} =
      Reducer.step(at_snapshot, %Observation.ArtifactSnapshot{
        assignment_id: "as_0001",
        baseline: %{"exists" => false},
        now: @now
      })

    assert is_struct(command["prompt"], SensitiveBytes)
    refute printed?(at_dispatch, wrapped)
  end

  test "the states suspended around a resumed fetch print neither the journaled nor the fetched bytes" do
    {lines, wrapped, spec, plan, opts} = journal_through_projection()

    # Drive until the fetch is asked for; every suspension on the way is inspected.
    {%Effect.FetchPrompt{object: object}, at_fetch} =
      drive_to(Reducer.resume(spec, plan, decode(lines), opts), Effect.FetchPrompt, wrapped)

    # ---- M3: a render happens only where a render is licensed ----
    #
    # Two kinds of evidence, because neither alone is enough. The structural guard pins WHERE a
    # render can be reached from at all. The behavioural tests then show that on the two
    # branches that must not render, no outcome of any rendering clause occurred: every
    # rendering clause either yields RetainPrompt, emits attention, or returns an error, so a
    # branch that produced none of those did not enter one. A render computed and discarded
    # inside a non-rendering clause would be invisible to the behavioural tests; that is what
    # the structural guard is for, and why both are here.
    refute printed?(at_fetch, wrapped)

    fetched = SensitiveBytes.new(SensitiveBytes.reveal(wrapped), :prompt)

    {:effect, %Effect.Dispatch{command: command}, at_dispatch, _} =
      Reducer.step(at_fetch, %Observation.PromptFetched{object: object, bytes: fetched, now: @now})

    assert is_struct(command["prompt"], SensitiveBytes)
    refute printed?(at_dispatch, wrapped), "the fetched bytes are held bare in the state that carries the command"
  end

  test "prompt_bundle is reachable only through render/5, and render/5 only from the three licensed sites" do
    clauses = @reducer |> File.read!() |> String.split(~r/\n(?=  defp? )/)

    callers = fn needle ->
      for clause <- clauses, String.contains?(clause, needle), do: clause |> String.split("(") |> hd() |> String.trim()
    end

    assert Enum.sort(callers.("prompt_bundle(")) == Enum.sort(["defp render", "defp prompt_bundle"]), """
    The bare render exists only between prompt_bundle/5 and the wrapper inside render/5.
    A second caller of prompt_bundle/5 is a second place a binary can escape.
    Found: #{inspect(callers.("prompt_bundle("))}
    """

    assert Enum.sort(callers.(" render(")) ==
             Enum.sort([
               "defp assignment_requested",
               "defp continue_assignment",
               "defp resumed_prompt_fetched",
               "defp render"
             ]),
           """
           render/5 is licensed at exactly three sites: a fresh assignment, a resumed
           assignment with no projection, and a validated legacy object that is missing.
           Found: #{inspect(callers.(" render("))}
           """
  end

  # The probe is a string only a render produces. The spec's goal would be a poor one: the
  # machine stores the spec, so a goal appears in every suspended state whether or not a
  # render happened. `#+title: Assignment <id>` is written by render_prompt/6 and by nothing
  # else -- no spec, plan or event carries it -- so its presence in a suspended state is a
  # render held in that state, and nothing else.
  @render_marker "#+title: Assignment as_0001"

  test "an already-sent assignment resumes to observation without rendering or fetching" do
    {spec, plan, prior, opts} = kill9("events_awaiting_artifact.jsonl")

    {%Effect.Observe{assignment_id: "as_0001"}, _state, seen} =
      drive_until(Reducer.resume(spec, plan, prior, opts), Effect.Observe, @render_marker, [])

    refute Enum.any?(seen, &match?(%Effect.RetainPrompt{}, &1)), "an already-sent assignment retained again"

    refute Enum.any?(seen, &match?(%Effect.FetchPrompt{}, &1)),
           "an already-sent assignment fetched bytes it will not send"
  end

  test "a retained send resumes to its fetch without rendering" do
    {lines, wrapped, spec, plan, opts} = journal_through_projection()
    assert SensitiveBytes.reveal(wrapped) =~ @render_marker, "the marker must be something a render really produces"

    {%Effect.FetchPrompt{}, _state, seen} =
      drive_until(Reducer.resume(spec, plan, decode(lines), opts), Effect.FetchPrompt, @render_marker, [])

    # ---- helpers ----

    refute Enum.any?(seen, &match?(%Effect.RetainPrompt{}, &1)), "a retained send rendered and retained again"
  end

  defp printed?(state, %SensitiveBytes{} = wrapped) do
    String.contains?(inspect(state, @inspect), inspect(SensitiveBytes.reveal(wrapped), @inspect)) or
      String.contains?(inspect(state, @inspect), SensitiveBytes.reveal(wrapped))
  end

  # A fresh run through the host, so the store holds a real object, truncated before the send.
  defp journal_through_projection do
    {_name, :run, scenario, [], opts_fun} = hd(H.cases())
    H.reset_seams()
    test_pid = self()

    opts =
      opts_fun.()
      |> Keyword.merge(run_id: "run_state_0002", supervisor_instance: "sup_state_0002")
      |> Keyword.put(:effect_observer, fn
        %Effect.RetainPrompt{bytes: bytes}, _observation -> send(test_pid, {:retained_bytes, bytes})
        _effect, _observation -> :ok
      end)

    assert {:ok, %{events: events}} = Host.run(H.spec(scenario), H.plan(scenario), opts)
    assert_received {:retained_bytes, wrapped}

    lines =
      events
      |> Enum.take_while(&(&1["type"] != "assignment_dispatch_sent"))
      |> Enum.map(&Jason.encode!/1)

    {lines, wrapped, H.spec(scenario), H.plan(scenario), Keyword.delete(opts, :effect_observer)}
  end

  # Answers clock reads with a fixed moment until the wanted effect is yielded; refuses any
  # other effect, and inspects every suspended state on the way.
  defp drive_to({:effect, %Effect.Clock{read_index: index}, state, _}, wanted, wrapped) do
    refute printed?(state, wrapped)
    drive_to(Reducer.step(state, %Observation.Clock{read_index: index, now: @now}), wanted, wrapped)
  end

  defp drive_to({:effect, %wanted{} = effect, state, _}, wanted, wrapped) do
    refute printed?(state, wrapped)
    {effect, state}
  end

  defp drive_to(other, wanted, _wrapped), do: flunk("expected #{inspect(wanted)}, got #{inspect(elem(other, 0))}")

  defp decode(lines), do: Enum.map(lines, &Jason.decode!/1)

  # A legacy kill9 journal, resumed through the bare machine with the harness's seams.
  defp kill9(file) do
    {_name, _kind, _scenario, _prior, opts_fun} = hd(H.cases())
    H.reset_seams()
    prior = @kill9 |> Path.join(file) |> File.read!() |> String.split("\n", trim: true) |> decode()
    spec = @kill9 |> Path.join("spec.json") |> File.read!() |> Jason.decode!()
    plan = @kill9 |> Path.join("plan.json") |> File.read!() |> Jason.decode!()
    {spec, plan, prior, Keyword.merge(opts_fun.(), run_id: "run_scenario_0001", supervisor_instance: "sup_state_0003")}
  end

  # Answers clock reads until the wanted effect appears, refusing attention and errors on the
  # way, and asserting at every suspension that no render is held in state.
  defp drive_until({:effect, %Effect.Clock{read_index: index} = effect, state, suffix}, wanted, sentinel, seen) do
    assert_no_render(state, suffix, sentinel)
    drive_until(Reducer.step(state, %Observation.Clock{read_index: index, now: @now}), wanted, sentinel, [effect | seen])
  end

  defp drive_until({:effect, %wanted{} = effect, state, suffix}, wanted, sentinel, seen) do
    assert_no_render(state, suffix, sentinel)
    {effect, state, Enum.reverse(seen)}
  end

  defp drive_until({:effect, effect, _state, _suffix}, wanted, _sentinel, seen) do
    all = Enum.reverse([effect | seen])

    flunk(
      "yielded #{inspect(effect.__struct__)} before #{inspect(wanted)}; effects: #{inspect(Enum.map(all, & &1.__struct__))}"
    )
  end

  defp drive_until(other, wanted, _sentinel, _seen),
    do: flunk("expected #{inspect(wanted)}, got #{inspect(elem(other, 0))}")

  defp assert_no_render(state, suffix, marker) do
    refute inspect(state, @inspect) =~ marker, "a suspended state carries a render made on this resume"
    refute Enum.any?(suffix, &(&1["type"] == "human_attention_required")), "a rendering clause emitted attention"
  end

  defp retained(%SensitiveBytes{} = bytes) do
    "sha256:" <> hex = hash = SensitiveBytes.hash(bytes)

    {:ok, object} =
      PromptObject.new(%{
        assignment_id: "as_0001",
        path: "prompts/as_0001-#{hex}.org",
        hash: hash,
        byte_size: SensitiveBytes.byte_size(bytes),
        version: 2
      })

    object
  end
end
