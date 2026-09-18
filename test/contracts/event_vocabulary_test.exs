defmodule AiOrchestrator.Contracts.EventVocabularyTest do
  @moduledoc """
  Wave 1 / Gate A contract: every declared journal event type is either produced by
  current runtime code or explicitly reserved with a target wave (ledger NS-40).

  This test reads runtime sources and fixture journals on purpose: the vocabulary is a
  protocol fact, so the table in `AiOrchestrator.Journal.Vocabulary` must match what the
  code actually emits and what the fixtures actually contain. Reserved means "not
  appendable by current production code"; it never makes historical fixtures invalid.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FoldTypeCollector
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Vocabulary

  @lib_root Path.expand("../../lib", __DIR__)
  @fixture_root Path.expand("../fixtures/contracts", __DIR__)

  # Inventory pinned at greenfield@46e8217 (NS-40). Changing these numbers requires a
  # ledger amendment, not a test edit.
  @declared_count 54
  @produced_count 33
  @reserved_count 21

  @reserved_types ~w(
    assignment_failed
    context_conflict_detected
    context_patch_accepted
    context_patch_proposed
    context_patch_rejected
    contract_change_proposed
    contract_change_ratified
    contract_change_rejected
    notification_failed
    notification_requested
    notification_sent
    pane_lease_failed
    run_budget_exhausted
    run_failed
    run_pause_requested
    run_paused
    run_recovery_reserved
    scheduler_stalled
    stop_policy_evaluated
    work_item_failed
    workspace_lease_failed
  )

  # Closed wave set as the north star names it (7a is the foundational operator surface).
  @allowed_target_waves [:w4, :w6, :w7a, :w9]
  @wave_labels %{w4: "4", w6: "6", w7a: "7a", w9: "9"}

  # The one function in fold.ex that returns a new Fold.State.
  @state_transition :apply_type_update

  # The functions that CLASSIFY an event without transitioning anything. A type named only
  # here satisfies the naming check below with no disposition at all, which is the weakness
  # the fold check alone cannot see.
  @membership_predicates [:blocks_on_attention?, :cleanup_event?, :terminal_with_completed_ids?]

  # Produced `:clause` types fold.ex names but gives no `apply_type_update/2` clause,
  # measured at the head this list was written against. Each is validated or admitted by
  # name without changing the folded state. Extending this list is a FINDING, not a fix: a
  # promotion whose type lands here added a name to fold.ex and no disposition.
  @produced_without_state_transition ~w(
    assignment_dispatch_sent
    assignment_observation_started
    assignment_prompt_projected
    pane_lease_release_requested
    review_received
    workspace_lease_release_requested
  )

  # Produced `:clause` types whose ONLY naming is inside a membership predicate: both are
  # release acknowledgements whose state change is carried by the matching released event.
  # A new entry means a promoted type that fold.ex neither validates nor dispositions.
  @produced_named_only_in_a_predicate ~w(
    pane_lease_release_requested
    workspace_lease_release_requested
  )

  test "vocabulary table covers exactly the declared event types" do
    assert Vocabulary.entries() |> Map.keys() |> MapSet.new() == Event.declared_types()
    assert MapSet.size(Event.declared_types()) == @declared_count
  end

  test "produced and reserved partition the vocabulary with the pinned counts" do
    produced = types_with_status(:produced)
    reserved = types_with_status(:reserved)

    assert MapSet.size(produced) == @produced_count
    assert MapSet.size(reserved) == @reserved_count
    assert MapSet.disjoint?(produced, reserved)
    assert MapSet.union(produced, reserved) == Event.declared_types()
  end

  test "reserved set is exactly the NS-40 list and is exposed by Event" do
    expected = MapSet.new(@reserved_types)

    assert types_with_status(:reserved) == expected
    assert Event.reserved_types() == expected
    assert Event.appendable_types() == MapSet.difference(Event.declared_types(), expected)

    for type <- @reserved_types, do: assert(Event.reserved?(type), "#{type} should be reserved")
    for type <- Event.appendable_types(), do: refute(Event.reserved?(type), "#{type} should be appendable")
  end

  test "every produced type names a producer whose source emits it" do
    for {type, entry} <- Vocabulary.entries(), entry.status == :produced do
      assert is_binary(entry.producer), "#{type}: produced entries name a producer module"
      assert is_binary(entry.source), "#{type}: produced entries name a source path"

      source = Path.join(@lib_root, entry.source)
      assert File.exists?(source), "#{type}: #{entry.source} does not exist"

      assert MapSet.member?(emitted_types(source), type),
             "#{type}: no emit/2+ call with that literal in #{entry.source}; update the vocabulary table"
    end
  end

  test "no reserved type has an emit site anywhere in lib/" do
    emitted_by_file =
      @lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.map(&{Path.relative_to(&1, @lib_root), emitted_types(&1)})

    for type <- @reserved_types, {path, emitted} <- emitted_by_file do
      refute MapSet.member?(emitted, type),
             "#{type} is reserved but #{path} emits it; move it to :produced with its fixtures"
    end
  end

  test "every reserved type carries a closed target wave and no producer" do
    for {type, entry} <- Vocabulary.entries(), entry.status == :reserved do
      assert entry.producer == nil, "#{type}: reserved entries have no producer"
      assert entry.target_wave in @allowed_target_waves, "#{type}: target_wave #{inspect(entry.target_wave)}"
      assert Vocabulary.wave_label(entry.target_wave) == @wave_labels[entry.target_wave]
    end

    for {type, entry} <- Vocabulary.entries(), entry.status == :produced do
      assert entry.target_wave == nil, "#{type}: produced entries carry no target wave"
    end
  end

  test "every produced (appendable) type appears in at least one fixture journal" do
    fixture_types = fixture_event_types()

    for type <- Event.appendable_types() do
      assert MapSet.member?(fixture_types, type), "#{type}: appendable but absent from every fixture journal"
    end
  end

  test "fold classification in the table matches how fold.ex names types" do
    named = FoldTypeCollector.named_types_from_source(fold_source())

    for {type, entry} <- Vocabulary.entries() do
      named? = MapSet.member?(named, type)

      case entry.fold do
        :clause -> assert named?, "#{type}: marked :clause but fold.ex never matches or compares it"
        :generic -> refute named?, "#{type}: marked :generic but fold.ex names it in a pattern, list, or comparison"
      end
    end
  end

  # The naming check above proves only that fold.ex mentions a type in one of three
  # syntactic positions. Four reserved types (work_item_failed, and the three notification
  # types) are named ONLY inside a membership predicate, so promoting one would satisfy the
  # fold half of NS-40 with no state transition, no validation and no terminal handling.
  # These two rows close that: the sets are pinned, so a promotion that adds a bare naming
  # fails here by name instead of passing.
  test "every produced type marked :clause is dispositioned by a fold state transition" do
    participation = FoldTypeCollector.named_types_by_function(fold_source())

    without =
      for {type, entry} <- Vocabulary.entries(),
          entry.status == :produced,
          entry.fold == :clause,
          not MapSet.member?(participation_of(participation, type), @state_transition),
          do: type

    assert Enum.sort(without) == Enum.sort(@produced_without_state_transition),
           """
           A produced :clause type must take part in the fold's state transition, not merely
           be named somewhere in fold.ex. `apply_type_update/2` is the only function that
           returns a new Fold.State, and these produced types have no clause in it:

           #{Enum.map_join(Enum.sort(without), "\n", &"  #{&1}")}

           If a promotion put a type here, the promotion is incomplete: give it a real
           disposition (ledger NS-40; architecture 307-310). Do not extend the pinned list.
           """
  end

  test "no produced type marked :clause is named only inside a membership predicate" do
    participation = FoldTypeCollector.named_types_by_function(fold_source())
    predicates = MapSet.new(@membership_predicates)

    membership_only =
      for {type, entry} <- Vocabulary.entries(),
          entry.status == :produced,
          entry.fold == :clause,
          functions = participation_of(participation, type),
          MapSet.size(functions) > 0,
          MapSet.subset?(functions, predicates),
          do: type

    assert Enum.sort(membership_only) == Enum.sort(@produced_named_only_in_a_predicate),
           """
           These produced :clause types are named in fold.ex only by a membership predicate
           (#{Enum.map_join(@membership_predicates, ", ", &to_string/1)}), which classifies an
           event without folding it:

           #{Enum.map_join(Enum.sort(membership_only), "\n", &"  #{&1}")}

           Membership in such a list is not a fold rule. Do not extend the pinned list.
           """
  end

  test "the participation collector attributes each naming to the function that encloses it" do
    # Without this the two rows above report the same green against a collector that has
    # stopped distinguishing positions, which is exactly the defect they exist to close.
    source = """
    defmodule Synthetic do
      defp apply_type_update(%{"type" => "run_created"}, state), do: state
      defp validate_domain(state, %{"type" => "gate_passed"}), do: {:ok, state}
      defp blocks_on_attention?(type), do: type in ["work_item_failed"]
      defp cleanup_event?(type), do: type in ["notification_sent"]
      defp preamble_allowed?(:run_created, type), do: type == "run_spec_loaded"
    end
    """

    assert FoldTypeCollector.participation_from_string(source) == %{
             "run_created" => MapSet.new([:apply_type_update]),
             "gate_passed" => MapSet.new([:validate_domain]),
             "work_item_failed" => MapSet.new([:blocks_on_attention?]),
             "notification_sent" => MapSet.new([:cleanup_event?]),
             "run_spec_loaded" => MapSet.new([:preamble_allowed?])
           }
  end

  test "the fold-type collector ignores event-name-looking values compared to other variables" do
    source = """
    defmodule Synthetic do
      def a(%{"type" => "run_created"}), do: :ok
      def b(type), do: type == "run_started"
      def c(type), do: type in ["run_completed", "run_failed"]
      def d(status), do: status == "run_cancelled"
      def e(disposition), do: disposition in ["assignment_failed"]
      def f(%{"role" => "gate_failed"}), do: :ok
      @doc "mentions work_item_failed in prose"
      def g, do: "notification_sent"
    end
    """

    assert FoldTypeCollector.named_types_from_string(source) ==
             MapSet.new(["run_created", "run_started", "run_completed", "run_failed"])
  end

  test "unknown event types are still rejected at the envelope boundary" do
    line =
      Jason.encode!(%{
        "schema" => "ai-orchestrator/journal-event",
        "schema_version" => 1,
        "event_version" => 1,
        "seq" => 1,
        "event_id" => "ev_0001",
        "type" => "not_a_declared_type",
        "ts" => "2026-01-01T00:00:00Z",
        "run_id" => "run_fixture_0001",
        "actor" => "run_supervisor",
        "data" => %{}
      })

    assert {:error, %{clause: "unknown_event_type"}} = Event.validate_line(line)
  end

  defp fold_source, do: Path.join(@lib_root, "ai_orchestrator/journal/fold.ex")

  defp participation_of(participation, type), do: Map.get(participation, type, MapSet.new())

  defp types_with_status(status) do
    Vocabulary.entries()
    |> Enum.filter(fn {_type, entry} -> entry.status == status end)
    |> MapSet.new(fn {type, _entry} -> type end)
  end

  # Structural detection: parse the source and collect the string-literal arguments of every
  # `emit` call (`emit(fsm, "type", ...)`, `|> emit("type", ...)`, and multi-line forms all
  # produce the same AST node). Comments and unrelated literals cannot register as emit sites.
  defp emitted_types(path) do
    path
    |> File.read!()
    |> Code.string_to_quoted!(file: path)
    |> Macro.prewalk(MapSet.new(), fn
      {:emit, _meta, args} = node, acc when is_list(args) ->
        {node, Enum.reduce(args, acc, fn arg, acc -> if is_binary(arg), do: MapSet.put(acc, arg), else: acc end)}

      node, acc ->
        {node, acc}
    end)
    |> elem(1)
  end

  defp fixture_event_types do
    @fixture_root
    |> Path.join("**/*.jsonl")
    |> Path.wildcard()
    |> Enum.flat_map(&(&1 |> File.read!() |> String.split("\n", trim: true)))
    |> Enum.flat_map(fn line ->
      case Jason.decode(line) do
        {:ok, %{"type" => type}} when is_binary(type) -> [type]
        _other -> []
      end
    end)
    |> MapSet.new()
  end
end
