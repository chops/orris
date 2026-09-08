defmodule AiOrchestrator.Contracts.CommandContractTest do
  @moduledoc """
  Wave 1 contract for the actor-aware command path (ADR-0001, ledger NS-09).

  Command-originated events carry `data.requested_by` = {class, id, command_id}; the
  envelope `actor` stays `run_supervisor` (EJ-1). Events the runtime derives from a
  command carry no `requested_by`. The fixture `journals/valid_requested_by` pins the
  shape; the per-type data schema that enforces it in `Journal.Event` lands with Z1 in
  Wave 2.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Commands.Arguments
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Schemas.RequestedBy

  @fixture "valid_requested_by"

  # ADR-0001 item 4: types a command originates in the Wave 1 vocabulary.
  @command_originated ~w(
    run_created
    run_resumed
    run_cancel_requested
    run_pause_requested
    context_patch_proposed
  )

  # ADR-0001 item 3: closed command set per actor class.
  @operator_verbs ~w(start resume resolve_attention repair cancel pause update_context ratify_plan)
  @agent_verbs ~w(propose_plan propose_context_change)
  @system_verbs ~w(repair)
  @verbs Enum.uniq(@operator_verbs ++ @agent_verbs ++ @system_verbs)

  # ADR-0001 item 3: verb -> acceptance event in the Wave 1 vocabulary (nil = Wave 9 decision).
  @acceptance_event %{
    "start" => "run_created",
    "resume" => "run_resumed",
    "resolve_attention" => "run_resumed",
    "repair" => "run_resumed",
    "cancel" => "run_cancel_requested",
    "pause" => "run_pause_requested",
    "update_context" => "context_patch_proposed",
    "propose_context_change" => "context_patch_proposed",
    "propose_plan" => nil,
    "ratify_plan" => nil
  }

  # Same grammar ai-pair enforces for peer msg_id (lib/ai_pair/cli/consult.ex).
  @command_id_grammar ~r/^[A-Za-z0-9_-]{16,64}$/

  @base_example %{
    "id" => "local_operator",
    "command_id" => "cmd_01J9X3T2QF5G7H8K1N3P",
    "verb" => "start",
    "args_hash" => "sha256:" <> String.duplicate("0", 64)
  }

  # ADR-0001 item 3: one positive example per actor class.
  @positive_examples [
    Map.put(@base_example, "class", "operator"),
    Map.merge(@base_example, %{"class" => "console", "id" => "console_session_7", "verb" => "pause"}),
    Map.merge(@base_example, %{
      "class" => "agent",
      "id" => "writer",
      "run_id" => "run_fixture_0004",
      "assignment_id" => "as_0001",
      "verb" => "propose_context_change"
    }),
    Map.merge(@base_example, %{
      "class" => "system",
      "id" => "runs_monitor",
      "reason" => "supervision_exhausted",
      "verb" => "repair"
    })
  ]

  # ADR-0001 item 3: negative examples the union must reject.
  @negative_examples [
    {"agent without assignment_id",
     Map.merge(@base_example, %{"class" => "agent", "id" => "writer", "run_id" => "run_fixture_0004"})},
    {"system without reason", Map.merge(@base_example, %{"class" => "system", "id" => "runs_monitor"})},
    {"operator carrying run_id", Map.merge(@base_example, %{"class" => "operator", "run_id" => "run_fixture_0004"})},
    {"console carrying reason", Map.merge(@base_example, %{"class" => "console", "reason" => "x"})},
    {"unknown verb", Map.merge(@base_example, %{"class" => "operator", "verb" => "delete_everything"})},
    {"extra key", Map.merge(@base_example, %{"class" => "operator", "note" => "hi"})},
    {"unknown class", Map.put(@base_example, "class", "root")},
    {"bad command_id grammar", Map.merge(@base_example, %{"class" => "operator", "command_id" => "short"})},
    {"empty operator id", Map.merge(@base_example, %{"class" => "operator", "id" => ""})},
    {"agent id outside roster grammar",
     Map.merge(@base_example, %{
       "class" => "agent",
       "id" => "Writer",
       "run_id" => "run_fixture_0004",
       "assignment_id" => "as_0001"
     })},
    {"system with empty reason",
     Map.merge(@base_example, %{"class" => "system", "id" => "runs_monitor", "reason" => ""})},
    {"agent invoking an operator verb",
     Map.merge(@base_example, %{
       "class" => "agent",
       "id" => "writer",
       "run_id" => "run_fixture_0004",
       "assignment_id" => "as_0001",
       "verb" => "start"
     })},
    {"operator invoking an agent verb", Map.merge(@base_example, %{"class" => "operator", "verb" => "propose_plan"})},
    {"system invoking cancel",
     Map.merge(@base_example, %{"class" => "system", "id" => "runs_monitor", "reason" => "x", "verb" => "cancel"})}
  ]

  test "fixture folds to its expected summary with the envelope actor unchanged" do
    lines = F.lines("journals", @fixture)
    expected = F.json("journals", @fixture, "expected.json")

    assert {:ok, state} = Fold.fold_lines(lines)
    assert Fold.summary(state) == expected

    for event <- decode(lines) do
      assert event["actor"] == "run_supervisor", "seq #{event["seq"]}: envelope actor must stay run_supervisor"
    end
  end

  test "the per-class union accepts one example of every actor class" do
    for example <- @positive_examples do
      assert match?({:ok, _}, RequestedBy.parse(example)), "#{example["class"]} example must parse"
    end
  end

  test "the per-class union rejects scope, verb, key, and grammar violations" do
    for {label, example} <- @negative_examples do
      assert match?({:error, _}, RequestedBy.parse(example)), "#{label} must be rejected"
    end
  end

  test "every command-originated event carries a well-formed requested_by object" do
    events = @fixture |> fixture_lines() |> decode()
    command_events = Enum.filter(events, &(&1["type"] in @command_originated))

    assert command_events != [], "fixture must contain at least one command-originated event"

    for event <- command_events do
      assert match?({:ok, _}, RequestedBy.parse(event["data"]["requested_by"])),
             "seq #{event["seq"]} (#{event["type"]}): requested_by must match ADR-0001 item 3"
    end
  end

  test "args_hash on every command event recomputes from its argument document (ARGS-CANON-1)" do
    for event <- @fixture |> fixture_lines() |> decode(), event["type"] in @command_originated do
      doc = F.json("journals", @fixture, "args/seq_#{String.pad_leading(Integer.to_string(event["seq"]), 4, "0")}.json")
      stamp = event["data"]["requested_by"]

      assert doc["verb"] == stamp["verb"], "seq #{event["seq"]}: argument document verb must match the stamp"

      assert Arguments.hash(doc["verb"], doc["args"]) == stamp["args_hash"],
             "seq #{event["seq"]}: args_hash must equal sha256 over ARGS-CANON-1 bytes"
    end
  end

  test "ARGS-CANON-1 is order-independent, verb-bound, and structurally unambiguous" do
    args = %{"spec_hash" => "sha256:" <> String.duplicate("a", 64), "plan_hash" => "sha256:" <> String.duplicate("b", 64)}
    reordered = args |> Enum.reverse() |> Map.new()

    assert Arguments.hash("start", args) == Arguments.hash("start", reordered)
    refute Arguments.hash("start", args) == Arguments.hash("resume", args)

    assert Arguments.bytes("cancel", %{"reason" => "operator_cancel"}) ==
             "ARGS-CANON-1\n" <> <<6::32>> <> "cancel" <> <<6::32>> <> "reason" <> <<15::32>> <> "operator_cancel"

    # Pairs that collide under a newline/"=" delimited encoding do not collide here.
    refute Arguments.hash("x", %{"a" => "1\nb=2"}) == Arguments.hash("x", %{"a" => "1", "b" => "2"})
    refute Arguments.hash("x", %{"a" => "b=c"}) == Arguments.hash("x", %{"a" => "b", "" => "c"})
    refute Arguments.hash("x", %{"ab" => "c"}) == Arguments.hash("x", %{"a" => "bc"})
    refute Arguments.hash("x", %{"a" => <<0, 1>>}) == Arguments.hash("x", %{"a" => <<0>>, "b" => <<1>>})
    refute Arguments.hash("x", %{"k" => "v"}) == Arguments.hash("xk", %{"" => "v"})
  end

  test "every verb belongs to a class and maps to a documented acceptance event" do
    assert MapSet.new(@verbs) == MapSet.new(Map.keys(@acceptance_event))

    for {verb, event} <- @acceptance_event, event != nil do
      assert event in @command_originated, "#{verb}: acceptance event #{event} must be a command-originated type"
    end

    assert Enum.sort(@agent_verbs) == ~w(propose_context_change propose_plan)
    assert @system_verbs == ~w(repair)
  end

  test "command_id matches the shared opaque id grammar on every command event" do
    for event <- @fixture |> fixture_lines() |> decode(), event["type"] in @command_originated do
      command_id = get_in(event, ["data", "requested_by", "command_id"])

      assert is_binary(command_id) and Regex.match?(@command_id_grammar, command_id),
             "seq #{event["seq"]}: #{inspect(command_id)}"
    end
  end

  test "the fixture carries no retired run_lock_path locator" do
    for event <- @fixture |> fixture_lines() |> decode() do
      refute Map.has_key?(event["data"], "run_lock_path"), "seq #{event["seq"]}: run_lock_path is retired"
    end
  end

  test "events the runtime derives from a command carry no requested_by" do
    for event <- @fixture |> fixture_lines() |> decode(), event["type"] not in @command_originated do
      refute Map.has_key?(event["data"], "requested_by"),
             "seq #{event["seq"]} (#{event["type"]}): derived events must not carry requested_by"
    end
  end

  test "the fixture exercises both the start and the cancel commands" do
    types = @fixture |> fixture_lines() |> decode() |> MapSet.new(& &1["type"])
    assert MapSet.subset?(MapSet.new(["run_created", "run_cancel_requested"]), types)
  end

  defp fixture_lines(name), do: F.lines("journals", name)

  defp decode(lines), do: Enum.map(lines, &Jason.decode!/1)
end
