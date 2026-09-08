defmodule AiOrchestrator.Journal.RecoveryReservationRedTest do
  @moduledoc """
  U1a-R RED/interface, revision 2 (docs/contracts/recovery-reservation.org rev 2): the reserved event
  `run_recovery_reserved` - schema per mode, reserved membership (refused at append reserved-first while INCLUDED in
  the typed envelope export like every typed reserved type), the one Fold.State count with run-wide history, placement
  under the existing rules, unchanged projections proven against a MEASURED baseline file. Controls first, on the
  unchanged lib; pre-transition observations of the new type are recorded as evidence, never as an invariant.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Journal.Vocabulary
  alias AiOrchestrator.Projection.RunSummary

  @type_name "run_recovery_reserved"
  @canary "RECOVERY-RESERVATION-PRIVATE-CANARY"
  @fixture "fold_recovery_reserved"
  @baseline_file Path.expand("../fixtures/contracts/journals/BASELINE-FOLD-PROJECTIONS.json", __DIR__)
  @baseline @baseline_file |> File.read!() |> Jason.decode!()
  @historical Map.keys(@baseline)
  @fields ~w(attempt limit cause_class lost_generation authority)
  @causes ~w(writer_exit writer_timeout owner_down)
  @synthetic_unknown "private_synthetic_unknown_type"
  # the inventory transition (ledger amendment at GREEN): {baseline, after}
  @transition %{declared: {53, 54}, produced: {33, 33}, reserved: {20, 21}, typed: {44, 45}, reserved_untyped: {9, 9}}

  defp lines, do: F.lines("journals", @fixture)
  defp decoded, do: Enum.map(lines(), &Jason.decode!/1)
  defp reservation?(event), do: event["type"] == @type_name
  defp reservation, do: Enum.find(decoded(), &reservation?/1)
  defp with_data(event, data), do: Map.put(event, "data", data)
  defp encode(events), do: Enum.map(events, &Jason.encode!/1)

  defp fold_ok!(lines),
    do:
      (fn ->
         {:ok, state} = Fold.fold_lines(lines)
         state
       end).()

  defp without_reservations, do: decoded() |> Enum.reject(&reservation?/1) |> renumber()

  defp renumber(events) do
    events
    |> Enum.with_index(1)
    |> Enum.map(fn {e, seq} ->
      e |> Map.put("seq", seq) |> Map.put("event_id", "ev_#{String.pad_leading(Integer.to_string(seq), 4, "0")}")
    end)
  end

  # the pending new field, read through a plain map: at RED the struct does not declare it yet
  defp reservations(state), do: state |> Map.from_struct() |> Map.get(:recovery_reservations)

  # HISTORICAL projection exactly as BASELINE-FOLD-PROJECTIONS.json records it: the full canonical Fold.State with
  # ONLY the pending new field removed (event_ids RETAINED), an order-stable JSON-shaped value (atoms -> strings,
  # MapSets -> sorted lists, nested maps with string keys) - never `inspect`, whose map key order is not stable
  defp historical_projection(state) do
    state
    |> Map.from_struct()
    |> Map.delete(:recovery_reservations)
    |> Map.new(fn {k, v} -> {Atom.to_string(k), canon(v)} end)
  end

  # the with-versus-without comparison of R-6 additionally drops event_ids (the stripped journal is renumbered)
  defp comparison_projection(state), do: state |> historical_projection() |> Map.delete("event_ids")

  defp canon(%MapSet{} = s), do: s |> MapSet.to_list() |> Enum.map(&canon/1) |> Enum.sort()
  defp canon(v) when is_map(v) and not is_struct(v), do: Map.new(v, fn {k, vv} -> {canon_key(k), canon(vv)} end)
  defp canon(v) when is_list(v), do: Enum.map(v, &canon/1)
  defp canon(v) when is_atom(v) and not is_boolean(v) and not is_nil(v), do: Atom.to_string(v)
  defp canon(v), do: v
  defp canon_key(k) when is_atom(k), do: Atom.to_string(k)
  defp canon_key(k) when is_binary(k), do: k
  defp canon_key(k), do: inspect(k)

  defp assert_no_canary(term), do: refute(inspect(term, limit: :infinity, printable_limit: :infinity) =~ @canary)

  defp unknown_line(type) do
    reservation() |> Map.put("type", type) |> Map.put("data", %{})
  end

  describe "controls (unchanged lib)" do
    test "C-1 a stable synthetic unknown type is refused by name at every entry point; the fold names no at_seq" do
      event = unknown_line(@synthetic_unknown)
      assert {:error, %{clause: "unknown_event_type", event_type: @synthetic_unknown}} = Event.validate_read(event)

      assert {:error, %{clause: "unknown_event_type", event_type: @synthetic_unknown}} =
               Event.validate_line(Jason.encode!(event))

      assert {:error, %{clause: "unknown_event_type", event_type: @synthetic_unknown}} = Event.validate_append(event)

      assert {:error, %{clause: "event_schema_unavailable", event_type: @synthetic_unknown}} =
               EventData.parse(event, :read)

      {pre, _} = Enum.split(lines(), 5)
      assert {:error, %{clause: "unknown_event_type"} = rejection} = Fold.fold_lines(pre ++ [Jason.encode!(event)])
      refute Map.has_key?(rejection, :at_seq)
    end

    test "C-2 reserved-first: an existing typed reserved type is refused at append before any data check" do
      exhausted =
        reservation()
        |> Map.put("type", "run_budget_exhausted")
        |> with_data(%{"budget_kind" => "restart_attempts", "limit" => 3, "consumed" => 3, "blocking_entity" => "gen_1"})

      assert {:error, %{clause: "reserved_event_type", event_type: "run_budget_exhausted"}} =
               Event.validate_append(exhausted)

      assert {:error, %{clause: "reserved_event_type", event_type: "run_budget_exhausted"}} =
               Event.validate_append(with_data(exhausted, %{}))

      # the same line is readable: reserved is not fold-illegal history
      assert {:ok, _} = Event.validate_read(exhausted)
      assert {:ok, %EventData{type: "run_budget_exhausted"}} = EventData.parse(exhausted, :append)
    end

    test "C-3 (P2 imported) the typed envelope export INCLUDES typed reserved types today" do
      assert Event.reserved?("run_budget_exhausted") and Event.reserved?("run_failed")
      schema = Jason.encode!(Event.json_schema())
      assert schema =~ "run_budget_exhausted" and schema =~ "run_failed"
      # and excludes an untyped reserved type: the export follows TYPED current versions, not reserved membership
      refute schema =~ "run_pause_requested"
      refute MapSet.member?(EventData.typed_types(), "run_pause_requested")
    end

    test "C-4 the MEASURED baseline projections of all nine historical fold fixtures hold on the unchanged lib" do
      assert length(@historical) == 9
      assert "valid_envelope_version_2_minimal" in @historical

      for name <- @historical do
        %{"outcome" => "ok", "projection" => expected} = @baseline[name]
        state = fold_ok!(F.lines("journals", name))
        assert historical_projection(state) == expected, name
      end
    end

    test "C-4b the comparator is not vacuous: unchanged compares equal; a status mutation and an event_ids mutation compare unequal" do
      [name | _] = @historical
      selected = @baseline[name]["projection"]
      measured = historical_projection(fold_ok!(F.lines("journals", name)))
      assert measured == selected
      assert measured == Map.put(selected, "status", selected["status"]), "a no-op mutation is still equal"
      refute measured == Map.put(selected, "status", @canary), "a non-count field mutation is detected"
      refute measured == Map.put(selected, "event_ids", []), "an event_ids mutation is detected"
      refute measured == Map.delete(selected, "event_ids"), "event_ids is part of the historical comparison"
    end

    test "C-5 fixture composition on the unchanged lib: the reservation-less renumbered journal folds to the measured summary" do
      state = fold_ok!(encode(without_reservations()))

      assert Fold.summary(state) == %{
               "run_id" => "run_fixture_0031",
               "status" => "in_flight",
               "last_seq" => 7,
               "open_attention_ids" => []
             }

      assert state.attention_seen? == true and MapSet.size(state.open_attention_ids) == 0
      # the pinned expected.json for the fixture differs from this summary ONLY by last_seq (9 vs 7)
      expected = F.json("journals", @fixture, "expected.json")
      assert Map.delete(expected, "last_seq") == Map.delete(Fold.summary(state), "last_seq") and expected["last_seq"] == 9
    end

    test "C-6 (P3 imported) a v1 reservation line after a real v2 prefix is refused by the chain before its type is consulted" do
      {prefix, _} =
        decoded()
        |> Enum.take(4)
        |> Enum.map_reduce(Chain.anchor(), fn event, prev ->
          line = event |> Map.put("schema_version", 2) |> Map.put("prev_line_sha256", prev) |> Jason.encode!()
          {line, Chain.line_sha256(line <> "\n")}
        end)

      assert {:ok, %{envelope_version: 2, count: 4}} = Chain.verify(Enum.join(prefix, "\n") <> "\n")
      res = Map.put(reservation(), "seq", 5)
      mixed = Enum.join(prefix ++ [Jason.encode!(res)], "\n") <> "\n"
      assert {:error, %{clause: "mixed_envelope_versions", at_seq: 5}} = Chain.verify(mixed)
    end

    test "C-7 R-3's reconstruction is well-formed: the fixture rebuilt around its own reservation line is byte-identical" do
      {pre, [res | rest]} = Enum.split(lines(), 5)
      assert pre ++ [res] ++ rest == lines()
      assert length(rest) == 3 and Enum.all?(rest, &is_binary/1)
    end
  end

  describe "R-0 vocabulary membership" do
    test "reserved in Event and Vocabulary (:w4), typed at version 1, never appendable, INCLUDED in the typed export" do
      assert Event.reserved?(@type_name)
      assert MapSet.member?(Event.declared_types(), @type_name)
      refute MapSet.member?(Event.appendable_types(), @type_name)
      assert %{status: :reserved, target_wave: :w4, producer: nil} = Vocabulary.entries()[@type_name]
      assert MapSet.member?(EventData.typed_types(), @type_name)
      assert EventData.current_version(@type_name) == 1
      assert EventData.known_versions(@type_name) == [1]
      # the typed envelope export follows typed current versions (C-3): the new typed reserved type is INCLUDED
      assert Jason.encode!(Event.json_schema()) =~ @type_name
    end

    test "entry points pinned separately: read/line/view/direct parse accept; Event.validate_append refuses reserved-first" do
      res = reservation()
      assert {:ok, view} = Event.validate_read(res)
      assert view["type"] == @type_name and view["event_version"] == 1
      assert {:ok, _} = Event.validate_line(Jason.encode!(res))
      assert {:ok, _} = Event.validate_view(res)
      assert {:ok, %EventData{type: @type_name, event_version: 1}} = EventData.parse(res, :read)
      assert {:ok, %EventData{type: @type_name, event_version: 1}} = EventData.parse(res, :append)
      assert {:ok, %EventData{type: @type_name, event_version: 1}} = EventData.parse(res, :view)
      assert {:error, %{clause: "reserved_event_type", event_type: @type_name}} = Event.validate_append(res)

      assert {:error, %{clause: "reserved_event_type", event_type: @type_name}} =
               Event.validate_append(with_data(res, %{}))
    end

    test "not an acceptance type and not a terminal type (measured through the fold)" do
      state = fold_ok!(lines())
      assert state.phase == :run_started and state.terminal? == false and state.status == "in_flight"
    end
  end

  describe "R-1 positive corpus" do
    test "the fixture folds to expected.json (in_flight, last_seq 9, open_attention_ids []) with recovery_reservations 2" do
      state = fold_ok!(lines())
      assert Fold.summary(state) == F.json("journals", @fixture, "expected.json")
      assert reservations(state) == 2 and state.last_seq == 9
    end
  end

  describe "R-2 structural domain per entry point" do
    for mode <- [:read, :append, :view] do
      test "EventData.parse(#{mode}): missing fields, extra key, malformed data, wrong types and boundaries -> exact structured errors" do
        mode = unquote(mode)
        base = reservation()
        data = base["data"]

        for field <- @fields do
          actual = EventData.parse(with_data(base, Map.delete(data, field)), mode)

          assert match?(
                   {:error, %{clause: "invalid_event_data", event_type: @type_name, event_version: 1, errors: [_ | _]}},
                   actual
                 ),
                 field

          {:error, %{errors: errors}} = actual
          assert Enum.any?(errors, &(&1 == %{"code" => "required", "path" => ["data", field]})), field
        end

        actual = EventData.parse(with_data(base, Map.put(data, "private_extra", @canary)), mode)
        assert match?({:error, %{clause: "invalid_event_data", errors: [_ | _]}}, actual)
        assert_no_canary(actual)

        for malformed <- [
              nil,
              "data",
              1,
              [],
              %{"attempt" => 1, :limit => 3},
              %Version{major: 1, minor: 0, patch: 0},
              %URI{path: @canary}
            ] do
          actual = EventData.parse(with_data(base, malformed), mode)
          assert match?({:error, %{clause: "invalid_event_data"}}, actual), inspect(malformed)
          assert_no_canary(actual)
        end

        # an OTHERWISE-VALID data map with an extra ATOM key: refused as unknown, no masking required-field error,
        # no echo
        actual = EventData.parse(with_data(base, Map.put(data, :private_atom_extra, @canary)), mode)
        assert match?({:error, %{clause: "invalid_event_data", errors: [_ | _]}}, actual)
        {:error, %{errors: errors}} = actual
        refute Enum.any?(errors, &(&1["code"] == "required")), "no required-field error masks the unknown atom key"
        assert_no_canary(actual)

        wrong = [
          {"attempt", 0},
          {"attempt", -1},
          {"attempt", 1.0},
          {"attempt", "1"},
          {"attempt", true},
          {"attempt", nil},
          {"attempt", [1]},
          {"attempt", %{}},
          {"limit", -1},
          {"limit", 1.0},
          {"limit", "3"},
          {"limit", false},
          {"limit", nil},
          {"limit", [3]},
          {"limit", %{}},
          {"lost_generation", 0},
          {"lost_generation", 1_000_000_000_000},
          {"lost_generation", 1.0},
          {"lost_generation", "1"},
          {"lost_generation", true},
          {"lost_generation", nil},
          {"lost_generation", [1]},
          {"lost_generation", %{}},
          {"cause_class", "operator"},
          {"cause_class", "os_group_lost"},
          {"cause_class", @canary},
          {"cause_class", "writer_exit\n"},
          {"cause_class", nil},
          {"cause_class", 1},
          {"authority", "daemon"},
          {"authority", @canary},
          {"authority", "host\n"},
          {"authority", nil},
          {"authority", 2}
        ]

        for {field, value} <- wrong do
          actual = EventData.parse(with_data(base, Map.put(data, field, value)), mode)

          assert match?({:error, %{clause: "invalid_event_data", event_type: @type_name, event_version: 1}}, actual),
                 "#{field}=#{inspect(value)}"

          {:error, %{errors: errors}} = actual
          assert Enum.any?(errors, &(&1["path"] == ["data", field])), "#{field}: the error path names the field"
          assert_no_canary(actual)
        end

        assert EventData.parse(Map.put(base, "event_version", 2), mode) ==
                 {:error, %{clause: "unsupported_event_version", event_type: @type_name, event_version: 2}}
      end
    end

    test "Event entry points carry the same structured refusal; unsupported version verdict per entry point" do
      base = reservation()
      bad = with_data(base, Map.put(base["data"], "attempt", 0))

      assert {:error, %{clause: "invalid_event_data", errors: [%{"code" => _, "path" => ["data", "attempt"]} | _]}} =
               Event.validate_read(bad)

      assert {:error, %{clause: "invalid_event_data"}} = Event.validate_line(Jason.encode!(bad))
      assert {:error, %{clause: "invalid_event_data"}} = Event.validate_view(bad)
      # reserved-first: at append the reserved refusal precedes the data refusal
      assert {:error, %{clause: "reserved_event_type"}} = Event.validate_append(bad)
      v2 = Map.put(base, "event_version", 2)

      assert {:error, %{clause: "unsupported_event_version", event_type: @type_name, event_version: 2}} =
               Event.validate_read(v2)

      assert {:error, %{clause: "unsupported_event_version", event_type: @type_name, event_version: 2}} =
               Event.validate_view(v2)
    end

    test "boundary accepts: attempt 1 with limit 1; lost_generation 999999999999; every cause and authority" do
      base = reservation()

      for cause <- @causes, authority <- ~w(executor host) do
        data = %{
          "attempt" => 1,
          "limit" => 1,
          "cause_class" => cause,
          "lost_generation" => 999_999_999_999,
          "authority" => authority
        }

        assert match?({:ok, _}, Event.validate_read(with_data(base, data))), "#{cause}/#{authority}"
      end
    end
  end

  describe "R-3 cross-field (fold, after the structural pass)" do
    test "limit 0 and attempt > limit are readable lines refused by the fold with recovery_reservation_invalid" do
      {pre, [res | rest]} = Enum.split(lines(), 5)
      base = Jason.decode!(res)

      for data <- [
            %{
              "attempt" => 1,
              "limit" => 0,
              "cause_class" => "writer_exit",
              "lost_generation" => 1,
              "authority" => "executor"
            },
            %{
              "attempt" => 2,
              "limit" => 1,
              "cause_class" => "writer_exit",
              "lost_generation" => 1,
              "authority" => "executor"
            }
          ] do
        line = Jason.encode!(with_data(base, data))
        assert match?({:ok, _}, Event.validate_read(with_data(base, data)))

        assert {:error, %{clause: "recovery_reservation_invalid", at_seq: 6, field: "attempt"}} =
                 Fold.fold_lines(pre ++ [line] ++ rest)
      end

      # the reconstruction with the ORIGINAL line folds: the split is well-formed (C-7)
      assert {:ok, _} = Fold.fold_lines(pre ++ [res] ++ rest)
    end
  end

  describe "R-4 history (run-wide count, never reset)" do
    test "attempt must be count + 1; the count survives generation, authority and an accepted resume" do
      {pre, [first | rest]} = Enum.split(lines(), 5)

      skip =
        first
        |> Jason.decode!()
        |> with_data(%{
          "attempt" => 2,
          "limit" => 3,
          "cause_class" => "writer_exit",
          "lost_generation" => 1,
          "authority" => "executor"
        })

      assert {:error, %{clause: "recovery_reservation_out_of_order", at_seq: 6, expected: 1}} =
               Fold.fold_lines(pre ++ [Jason.encode!(skip)] ++ rest)

      {pre8, [second | rest8]} = Enum.split(lines(), 7)

      repeat =
        second
        |> Jason.decode!()
        |> with_data(%{
          "attempt" => 1,
          "limit" => 3,
          "cause_class" => "owner_down",
          "lost_generation" => 7,
          "authority" => "host"
        })

      assert {:error, %{clause: "recovery_reservation_out_of_order", at_seq: 8, expected: 2}} =
               Fold.fold_lines(pre8 ++ [Jason.encode!(repeat)] ++ rest8)

      assert reservations(fold_ok!(lines())) == 2
    end
  end

  describe "R-5 placement through the existing rules (with valid controls)" do
    test "before run_started -> preamble_violation; after a terminal -> events_after_terminal; while blocked -> legal" do
      res = reservation()
      {pre, _} = Enum.split(lines(), 3)
      early = res |> Map.put("seq", 4) |> Map.put("event_id", "ev_0004")
      assert {:error, %{clause: "preamble_violation", at_seq: 4}} = Fold.fold_lines(pre ++ [Jason.encode!(early)])
      # valid control for the same prefix: run_started IS allowed there
      started = Enum.at(decoded(), 3)
      assert {:ok, _} = Fold.fold_lines(pre ++ [Jason.encode!(started)])

      completed =
        res
        |> Map.put("seq", 10)
        |> Map.put("event_id", "ev_0010")
        |> Map.put("type", "run_completed")
        |> with_data(%{"completed_work_item_ids" => []})

      late =
        res
        |> Map.put("seq", 11)
        |> Map.put("event_id", "ev_0011")
        |> with_data(%{
          "attempt" => 3,
          "limit" => 3,
          "cause_class" => "writer_exit",
          "lost_generation" => 9,
          "authority" => "executor"
        })

      assert {:error, %{clause: "events_after_terminal", at_seq: 11}} =
               Fold.fold_lines(lines() ++ [Jason.encode!(completed), Jason.encode!(late)])

      # valid control: the terminal alone folds
      assert {:ok, %{terminal?: true}} = Fold.fold_lines(lines() ++ [Jason.encode!(completed)])

      {blocked_prefix, _} = Enum.split(lines(), 6)
      state = fold_ok!(blocked_prefix)

      assert state.status == "blocked" and MapSet.member?(state.open_attention_ids, "att_0001") and
               reservations(state) == 1
    end
  end

  describe "R-6 unchanged projections" do
    test "the same journal without reservations: identical behavioural projection; summary/render differ only by last_seq" do
      with_res = fold_ok!(lines())
      without = fold_ok!(encode(without_reservations()))

      render_lines = fn state ->
        state |> RunSummary.render() |> String.split("\n") |> Enum.reject(&(&1 =~ "last_seq"))
      end

      rendered_with = render_lines.(with_res)
      rendered_without = render_lines.(without)
      summary_with = Fold.summary(with_res)
      summary_without = Fold.summary(without)

      assert Map.delete(comparison_projection(with_res), "last_seq") ==
               Map.delete(comparison_projection(without), "last_seq")

      assert Map.get(with_res, :last_seq) == 9 and Map.get(without, :last_seq) == 7
      assert reservations(with_res) == 2 and reservations(without) == 0
      assert Map.delete(summary_with, "last_seq") == Map.delete(summary_without, "last_seq")
      assert rendered_with == rendered_without
      assert MapSet.size(Map.get(with_res, :event_ids)) == 9
    end
  end

  describe "R-8 behavioural projection equality against the measured baseline" do
    test "every historical fold fixture: projection with only recovery_reservations removed equals the pre-change baseline; the field is 0" do
      for name <- @historical do
        %{"outcome" => "ok", "projection" => expected} = @baseline[name]
        state = fold_ok!(F.lines("journals", name))
        assert historical_projection(state) == expected, name
        assert reservations(state) == 0, "#{name}: no historical fixture carries a reservation"
      end
    end
  end

  describe "R-9 inventory transition" do
    test "the pinned counts move exactly as the transition table says (ledger amendment at GREEN)" do
      %{declared: {_, d}, produced: {_, p}, reserved: {_, r}, typed: {_, t}, reserved_untyped: {_, u}} = @transition
      assert MapSet.size(Event.declared_types()) == d
      assert MapSet.size(Vocabulary.produced_types()) == p
      assert MapSet.size(Event.reserved_types()) == r
      assert MapSet.size(EventData.typed_types()) == t
      assert MapSet.size(MapSet.difference(Event.reserved_types(), EventData.typed_types())) == u
      lib = "lib/**/*.ex" |> Path.wildcard() |> Enum.filter(&(File.read!(&1) =~ ~s("run_recovery_reserved")))

      assert Enum.reject(lib, &String.contains?(&1, "journal/")) == [],
             "only the journal vocabulary/schema/fold may name the type"
    end
  end
end
