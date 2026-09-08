defmodule AiOrchestrator.Run.ExhaustionJudgeRedTest do
  @moduledoc """
  U1b-0a RED/interface, revision 1 (docs/contracts/exhaustion-judge.org): a PURE budget judge over supplied facts.
  Controls measure the unchanged Spec/Fold surfaces at 742bc7c; RED rows address the absent
  `Run.Recovery.Exhaustion` through `Module.concat` and fail only because it does not exist.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Run.Recovery
  alias AiOrchestrator.Spec.Budgets
  alias AiOrchestrator.Spec.RunSpec

  @canary "EXHAUSTION-JUDGE-PRIVATE-CANARY"
  @big Integer.pow(2, 70)
  @v1_full "test/fixtures/contracts/run_specs/valid_full/spec.json" |> Path.expand() |> File.read!() |> Jason.decode!()

  defp judge_mod, do: Module.concat(["AiOrchestrator", "Run", "Recovery", "Exhaustion"])
  defp judge(view, consumed), do: judge_mod().judge(view, consumed)

  defp typed(map), do: {:typed, map}
  defp v2(budgets), do: %{"schema_version" => 2, "budgets" => budgets}
  defp v1(budgets), do: Map.put(@v1_full, "budgets", budgets)

  defp assert_no_canary(term), do: refute(inspect(term, limit: :infinity, printable_limit: :infinity) =~ @canary)

  defp refusal?(result, clause), do: match?({:error, %{clause: ^clause}}, result) and refusal_keys_exact?(result)
  defp refusal_keys_exact?({:error, map}), do: Map.keys(map) == [:clause]

  @malformed_views [
    {:typed, nil},
    {:typed, %URI{}},
    {:typed, %{"restart_attempts" => true}},
    {:typed, %{"restart_attempts" => 1.0}},
    {:typed, %{"restart_attempts" => -1}},
    {:typed, %{"restart_attempts" => nil}},
    {:typed, %{"restart_attempts" => "3"}},
    {:typed, %{"restart_attempts" => 3, "max_attempts_default" => 0}},
    {:typed, %{"restart_attempts" => 3, "max_wall_clock_s" => 0}},
    {:typed, %{"restart_attempts" => 3, "gate_attempts" => 0}},
    {:typed, %{"restart_attempts" => 3, :restart_attempts => 3}},
    {:typed, %{"restart_attempts" => 3, "EXHAUSTION-JUDGE-PRIVATE-CANARY" => 1}},
    {:typed, []},
    {:typed, "EXHAUSTION-JUDGE-PRIVATE-CANARY"},
    {:legacy, nil},
    {:legacy, "EXHAUSTION-JUDGE-PRIVATE-CANARY"},
    {:legacy, [restart_attempts: 3]},
    {:error, %{clause: "EXHAUSTION-JUDGE-PRIVATE-CANARY"}},
    {:error, "EXHAUSTION-JUDGE-PRIVATE-CANARY"},
    %{"restart_attempts" => 3},
    %{},
    nil,
    :typed,
    {:typed},
    {:typed, %{"restart_attempts" => 3}, :extra},
    {:frobnicate, %{"restart_attempts" => 3}},
    {"typed", %{"restart_attempts" => 3}},
    3,
    "EXHAUSTION-JUDGE-PRIVATE-CANARY"
  ]

  @invalid_counts [-1, -@big, 1.0, 0.0, true, false, nil, [], [1], %{}, %URI{}, "1", :one, {1}, self()]

  describe "controls: the unchanged Spec and Fold surfaces at 742bc7c" do
    test "C-1 RunSpec.budgets/1 typed / typed-empty / legacy / never-typed-v1 / unsupported" do
      assert RunSpec.budgets(v2(%{"restart_attempts" => 3})) == {:typed, %{"restart_attempts" => 3}}
      assert RunSpec.budgets(%{"schema_version" => 2}) == {:typed, %{}}
      assert RunSpec.budgets(v2(%{})) == {:typed, %{}}
      assert match?({:legacy, %{}}, RunSpec.budgets(@v1_full))
      assert RunSpec.budgets(v1(%{"restart_attempts" => 3})) == {:legacy, %{"restart_attempts" => 3}}
      refute match?({:typed, _}, RunSpec.budgets(v1(%{"restart_attempts" => 3})))
      assert RunSpec.budgets(Map.put(@v1_full, "schema_version", 99)) == {:error, %{clause: "unsupported_schema_version"}}

      assert RunSpec.budgets(v2(%{"restart_attempts" => 1.0})) ==
               {:error, %{clause: "budget_invalid", field: "restart_attempts", reason: "not_integer"}}
    end

    test "C-2 Budgets.validate/1 malformed domains refuse with budget_invalid and never echo user content" do
      assert Budgets.validate(nil) == {:error, %{clause: "budget_invalid", field: "budgets", reason: "not_map"}}
      assert Budgets.validate([]) == {:error, %{clause: "budget_invalid", field: "budgets", reason: "not_map"}}
      assert Budgets.validate(%URI{}) == {:error, %{clause: "budget_invalid", field: "budgets", reason: "unknown_key"}}

      assert Budgets.validate(%{"restart_attempts" => true}) ==
               {:error, %{clause: "budget_invalid", field: "restart_attempts", reason: "not_integer"}}

      assert Budgets.validate(%{"restart_attempts" => 1.0}) ==
               {:error, %{clause: "budget_invalid", field: "restart_attempts", reason: "not_integer"}}

      assert Budgets.validate(%{"restart_attempts" => -1}) ==
               {:error, %{clause: "budget_invalid", field: "restart_attempts", reason: "below_minimum"}}

      assert Budgets.validate(%{"restart_attempts" => nil}) ==
               {:error, %{clause: "budget_invalid", field: "restart_attempts", reason: "null"}}

      assert Budgets.validate(%{"max_attempts_default" => 0}) ==
               {:error, %{clause: "budget_invalid", field: "max_attempts_default", reason: "below_minimum"}}

      assert Budgets.validate(%{"max_wall_clock_s" => 0}) ==
               {:error, %{clause: "budget_invalid", field: "max_wall_clock_s", reason: "below_minimum"}}

      assert Budgets.validate(%{"gate_attempts" => 0}) ==
               {:error, %{clause: "budget_invalid", field: "gate_attempts", reason: "below_minimum"}}

      assert Budgets.validate(%{"restart_attempts" => 0}) == {:ok, %{"restart_attempts" => 0}}
      assert Budgets.validate(%{"restart_attempts" => @big}) == {:ok, %{"restart_attempts" => @big}}
      canary = Budgets.validate(%{@canary => 1})
      assert canary == {:error, %{clause: "budget_invalid", field: "budgets", reason: "unknown_key"}}
      assert_no_canary(canary)
    end

    test "C-3 the fold_recovery_reserved fixture folds on the unchanged Fold to its measured reservation count" do
      lines = F.lines("journals", "fold_recovery_reserved")
      reserved = Enum.count(lines, &(Jason.decode!(&1)["type"] == "run_recovery_reserved"))
      assert {:ok, state} = Fold.fold_lines(lines)
      count = state |> Map.from_struct() |> Map.get(:recovery_reservations)
      assert count == reserved
      assert count == 2
    end

    test "C-4 Run.Recovery public surface today is acquire/2, evidence/1, evidence/2, release/1" do
      assert Enum.sort(Recovery.__info__(:functions)) == [
               acquire: 2,
               evidence: 1,
               evidence: 2,
               release: 1
             ]
    end
  end

  describe "RED: the pure judge (absent today)" do
    test "R-1 below / equal / above the configured limit" do
      view = typed(%{"restart_attempts" => 3})
      assert judge(view, 0) == {:reserve, 1}
      assert judge(view, 2) == {:reserve, 3}
      assert judge(view, 3) == {:exhausted, %{limit: 3, consumed: 3}}
      assert judge(view, 5) == {:exhausted, %{limit: 3, consumed: 5}}
    end

    test "R-2 limit 0 is exhausted at 0/0 and above, never a reservation" do
      view = typed(%{"restart_attempts" => 0})
      assert judge(view, 0) == {:exhausted, %{limit: 0, consumed: 0}}
      assert judge(view, 4) == {:exhausted, %{limit: 0, consumed: 4}}
    end

    test "R-3 R-e: typed view without restart_attempts refuses unset; max_attempts_default is never a fallback" do
      view = typed(%{"max_attempts_default" => 7})
      assert judge(view, 0) == {:error, %{clause: "restart_budget_unset"}}
      assert judge(typed(%{}), 0) == {:error, %{clause: "restart_budget_unset"}}
      assert judge(view, 6) == {:error, %{clause: "restart_budget_unset"}}
      refute judge(view, 0) == {:reserve, 1}
    end

    test "R-4 R-e: a legacy budget map refuses untyped even when it carries the numeric key" do
      assert judge({:legacy, %{"restart_attempts" => 5}}, 0) == {:error, %{clause: "restart_budget_untyped"}}
      assert judge({:legacy, %{}}, 0) == {:error, %{clause: "restart_budget_untyped"}}
      assert judge(RunSpec.budgets(v1(%{"restart_attempts" => 5})), 0) == {:error, %{clause: "restart_budget_untyped"}}
    end

    test "R-5 every malformed budget view refuses restart_budget_invalid with a clause-only map and no echo" do
      for view <- @malformed_views do
        result = judge(view, 0)

        assert refusal?(result, "restart_budget_invalid"),
               "view #{inspect(view, printable_limit: 40)} gave #{inspect(result)}"

        assert_no_canary(result)
      end
    end

    test "R-6 every invalid count refuses recovery_count_invalid against a valid typed view" do
      view = typed(%{"restart_attempts" => 3})

      for consumed <- @invalid_counts do
        result = judge(view, consumed)
        assert refusal?(result, "recovery_count_invalid"), "count #{inspect(consumed)} gave #{inspect(result)}"
      end
    end

    test "R-7 two-error precedence: budget classification first, then count, then unset" do
      assert judge(typed(%{}), -1) == {:error, %{clause: "recovery_count_invalid"}}
      assert judge(typed(%{"max_attempts_default" => 7}), 1.0) == {:error, %{clause: "recovery_count_invalid"}}
      assert judge({:typed, nil}, -1) == {:error, %{clause: "restart_budget_invalid"}}
      assert judge({:typed, %{@canary => 1}}, nil) == {:error, %{clause: "restart_budget_invalid"}}
      assert judge({:legacy, %{"restart_attempts" => 5}}, -1) == {:error, %{clause: "restart_budget_untyped"}}
    end

    test "R-8 large integers compare exactly with no coercion, overflow or default" do
      assert judge(typed(%{"restart_attempts" => @big}), @big - 1) == {:reserve, @big}
      assert judge(typed(%{"restart_attempts" => @big}), @big) == {:exhausted, %{limit: @big, consumed: @big}}
      assert judge(typed(%{"restart_attempts" => 1}), @big) == {:exhausted, %{limit: 1, consumed: @big}}
      assert judge(typed(%{"restart_attempts" => @big + 1}), @big) == {:reserve, @big + 1}
    end

    test "R-9 purity and closure: identical results, closed shapes, clause-only refusals, no capabilities" do
      views = [typed(%{"restart_attempts" => 2}), typed(%{}), {:legacy, %{}}, {:typed, nil}]
      counts = [0, 1, 2, 3, -1, nil, @big]

      for view <- views, consumed <- counts do
        first = judge(view, consumed)
        assert judge(view, consumed) == first

        assert match?({:reserve, n} when is_integer(n) and n >= 1, first) or
                 match?(
                   {:exhausted, %{limit: l, consumed: c}} when is_integer(l) and is_integer(c) and c >= l,
                   first
                 ) or
                 (match?({:error, %{clause: c}} when is_binary(c), first) and refusal_keys_exact?(first)),
               "open shape #{inspect(first)}"

        text = inspect(first, limit: :infinity)
        refute text =~ "#PID" or text =~ "#Reference" or text =~ "#Function"
      end

      assert judge(typed(%{"restart_attempts" => 2}), 1) == {:reserve, 2}
      assert Map.keys(elem(judge(typed(%{}), 0), 1)) == [:clause]
    end

    test "R-10 the module's public surface is exactly judge/2" do
      assert judge_mod().__info__(:functions) == [judge: 2]
    end
  end
end
