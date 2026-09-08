defmodule AiOrchestrator.Spec.TypedBudgetsRedTest do
  @moduledoc """
  U2b-min RED/interface, revision 2 (docs/contracts/typed-budgets-v2.org): a standalone strict `Spec.Budgets`
  schema, run-spec `schema_version 2` as a separately supported version with optional strict budgets, and an
  accessor that never calls a v1 map typed. Controls measure the IMMUTABLE v1 admission first; version 2 is the
  one deliberate baseline-to-GREEN transition and no control asserts it refused.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Spec.RunSpec

  @v1 "test/fixtures/contracts/plans/invalid_undeclared_agent/spec.json"
      |> Path.expand()
      |> File.read!()
      |> Jason.decode!()
  @fixture_specs "test/fixtures/**/spec.json" |> Path.wildcard() |> Enum.sort()
  @canary "TYPED-BUDGETS-PRIVATE-CANARY"
  @fields ["max_attempts_default", "restart_attempts", "max_wall_clock_s", "gate_attempts"]

  # the corpus outcomes MEASURED on the unchanged RunSpec at 306c629 (path, schema_version, outcome); one fixture is
  # a JSON array (the journal negative), refused by shape; one carries version 99
  @measured [
    {"test/fixtures/contracts/journals/reject_data_required_fields/spec.json", :list, "invalid_run_spec_shape"},
    {"test/fixtures/contracts/plans/invalid_cycle/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_duplicate_ids/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_effort_hint_value/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_missing_dep/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_nonpositive_timeout/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_paths_outside_roots/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_stretch_overlap/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/invalid_undeclared_agent/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/valid_diamond/spec.json", 1, "ok"},
    {"test/fixtures/contracts/plans/valid_linear/spec.json", 1, "ok"},
    {"test/fixtures/contracts/run_specs/invalid_agent_grammar/spec.json", 1, "agent_name_grammar"},
    {"test/fixtures/contracts/run_specs/invalid_allowed_roots_escape/spec.json", 1, "allowed_root_outside_repo"},
    {"test/fixtures/contracts/run_specs/invalid_missing_gate/spec.json", 1, "empty_gates"},
    {"test/fixtures/contracts/run_specs/invalid_oracle_freeform/spec.json", 1, "gate_not_argv"},
    {"test/fixtures/contracts/run_specs/invalid_reserved_agent/spec.json", 1, "reserved_agent_name"},
    {"test/fixtures/contracts/run_specs/invalid_schema_version/spec.json", 99, "unsupported_schema_version"},
    {"test/fixtures/contracts/run_specs/valid_full/spec.json", 1, "ok"},
    {"test/fixtures/contracts/run_specs/valid_minimal/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/auth_blocked_pane/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/concurrency_cap/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/fingerprint_drift/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/gate_failure_summary_feedback/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/gated_run_seed/spec.json", 1, "ok"},
    {"test/fixtures/contracts/scenarios/kill9_resume/spec.json", 1, "ok"}
  ]

  defp budgets_mod, do: Module.concat(["AiOrchestrator", "Spec", "Budgets"])
  defp run_spec_mod, do: Module.concat(["AiOrchestrator", "Spec", "RunSpec"])
  defp v2(overrides \\ %{}), do: @v1 |> Map.put("schema_version", 2) |> Map.merge(overrides)
  defp invalid(field, reason), do: {:error, %{clause: "budget_invalid", field: field, reason: reason}}
  defp outcome({:ok, _}), do: "ok"
  defp outcome({:error, %{clause: clause}}), do: clause
  defp sha256(bytes), do: :sha256 |> :crypto.hash(bytes) |> Base.encode16(case: :lower)

  describe "controls: run-spec v1 is immutable (measured on the unchanged RunSpec)" do
    test "V-1a budgets deleted -> invalid_run_spec_shape" do
      assert {:error, %{clause: "invalid_run_spec_shape"}} = RunSpec.validate(Map.delete(@v1, "budgets"))
    end

    test "V-1b arbitrary budget keys and values are ACCEPTED and returned verbatim" do
      budgets = %{"private_unknown" => @canary, "restart_attempts" => -5}
      assert {:ok, %{"budgets" => ^budgets}} = RunSpec.validate(Map.put(@v1, "budgets", budgets))
    end

    # version 2 is deliberately NOT in this control: it is refused today and ADMITTED after GREEN (row V-2 is the
    # transition witness); the immutable refusals are everything else
    test ~s(V-1c unsupported versions: 3, "1", "2", 1.0, 2.0, nil -> unsupported; missing -> shape) do
      for version <- [3, "1", "2", 1.0, 2.0, nil] do
        actual = RunSpec.validate(Map.put(@v1, "schema_version", version))
        assert match?({:error, %{clause: "unsupported_schema_version"}}, actual), inspect(version)
      end

      assert {:error, %{clause: "invalid_run_spec_shape"}} = RunSpec.validate(Map.delete(@v1, "schema_version"))
    end

    test "V-1d every fixture spec keeps its MEASURED outcome; no historical fixture is v2; the corpus is complete" do
      assert Enum.map(@measured, &elem(&1, 0)) == @fixture_specs, "the fixture corpus changed: re-measure"

      for {path, version, expected} <- @measured do
        spec = path |> File.read!() |> Jason.decode!()
        assert outcome(RunSpec.validate(spec)) == expected, path

        case version do
          :list -> assert is_list(spec), path
          number -> assert spec["schema_version"] == number, path
        end
      end
    end
  end

  describe "B interface: the standalone strict schema" do
    test "B-0 Spec.Budgets.validate/1 and RunSpec.budgets/1 exist" do
      Code.ensure_loaded(budgets_mod())
      Code.ensure_loaded(run_spec_mod())
      assert function_exported?(budgets_mod(), :validate, 1), "Spec.Budgets.validate/1 does not exist"
      assert function_exported?(run_spec_mod(), :budgets, 1), "RunSpec.budgets/1 does not exist"
    end

    test "B-1 the empty map and each single valid field are accepted verbatim; restart_attempts 0 valid; nothing injected" do
      assert {:ok, %{}} = budgets_mod().validate(%{})

      for {key, value} <- [
            {"max_attempts_default", 1},
            {"max_attempts_default", 3},
            {"restart_attempts", 0},
            {"restart_attempts", 2},
            {"max_wall_clock_s", 1},
            {"max_wall_clock_s", 3600},
            {"gate_attempts", 1}
          ] do
        assert {:ok, %{^key => ^value} = typed} = budgets_mod().validate(%{key => value})
        assert map_size(typed) == 1, "nothing injected for #{key}"
      end

      assert {:ok, typed} = budgets_mod().validate(@v1["budgets"])
      assert typed == @v1["budgets"]
    end

    for {field, minimum, below} <- [
          {"max_attempts_default", 1, 0},
          {"restart_attempts", 0, -1},
          {"max_wall_clock_s", 1, 0},
          {"gate_attempts", 1, 0}
        ] do
      test "B-2 #{field}: below #{minimum}, float, integral float, string, boolean, list, map, null -> the closed reason" do
        field = unquote(field)
        assert budgets_mod().validate(%{field => unquote(below)}) == invalid(field, "below_minimum")

        for not_integer <- [1.5, 1.0, 3.0, "1", true, [1], %{}, %{"n" => 1}] do
          assert budgets_mod().validate(%{field => not_integer}) == invalid(field, "not_integer"), inspect(not_integer)
        end

        assert budgets_mod().validate(%{field => nil}) == invalid(field, "null")
      end
    end

    test "B-2b an unknown key is refused as field budgets (user content is never echoed); mixed-type keys; no atom created; not-a-map" do
      unknown = invalid("budgets", "unknown_key")
      assert budgets_mod().validate(%{"private_unknown" => 1}) == unknown

      for key <- [@canary, :private_atom, 42, {:t, @canary}] do
        actual = budgets_mod().validate(%{key => @canary})
        assert actual == unknown, inspect(key)
        refute inspect(actual, limit: :infinity) =~ @canary
      end

      assert_raise ArgumentError, fn -> String.to_existing_atom(@canary) end

      for other <- [nil, [], "budgets", 1, 1.0, true] do
        assert budgets_mod().validate(other) == invalid("budgets", "not_map"), inspect(other)
      end
    end

    test "B-3 precedence: any unknown key beats bound violations; then the fields in declared order" do
      assert budgets_mod().validate(%{"zzz" => 1, "aaa" => 1, "restart_attempts" => -1}) ==
               invalid("budgets", "unknown_key")

      assert budgets_mod().validate(%{"gate_attempts" => 0, "restart_attempts" => -1, "max_attempts_default" => 0}) ==
               invalid("max_attempts_default", "below_minimum")

      assert budgets_mod().validate(%{"gate_attempts" => 0, "max_wall_clock_s" => 0}) ==
               invalid("max_wall_clock_s", "below_minimum")
    end
  end

  describe "V run-spec version 2 and the accessor" do
    test "V-2 (the transition) a v2 spec with typed budgets is accepted; budgets/1 is {:typed, map}" do
      budgets = %{"max_wall_clock_s" => 3600, "max_attempts_default" => 3, "restart_attempts" => 0}
      assert {:ok, validated} = run_spec_mod().validate(v2(%{"budgets" => budgets}))
      assert validated["schema_version"] == 2
      assert run_spec_mod().budgets(validated) == {:typed, budgets}
    end

    test "V-3 a v2 spec with budgets ABSENT is accepted; budgets/1 is {:typed, %{}} (not configured, nothing injected)" do
      assert {:ok, validated} = run_spec_mod().validate(Map.delete(v2(), "budgets"))
      refute Map.has_key?(validated, "budgets")
      assert run_spec_mod().budgets(validated) == {:typed, %{}}
    end

    test "V-4 a v2 invalid budgets section is budget_invalid (never the shape clause); present null / non-map reach the standalone refusal; top-level strictness kept" do
      assert run_spec_mod().validate(v2(%{"budgets" => %{"restart_attempts" => -1}})) ==
               invalid("restart_attempts", "below_minimum")

      actual = run_spec_mod().validate(v2(%{"budgets" => %{"private_unknown" => @canary}}))
      assert actual == invalid("budgets", "unknown_key")
      refute inspect(actual, limit: :infinity) =~ @canary
      assert run_spec_mod().validate(v2(%{"budgets" => nil})) == invalid("budgets", "not_map")
      assert run_spec_mod().validate(v2(%{"budgets" => "x"})) == invalid("budgets", "not_map")
      assert run_spec_mod().validate(v2(%{"budgets" => [1]})) == invalid("budgets", "not_map")
      assert {:error, %{clause: "invalid_run_spec_shape"}} = run_spec_mod().validate(v2(%{"private_top" => 1}))
      assert {:error, %{clause: "invalid_run_spec_shape"}} = run_spec_mod().validate(v2(%{"goal" => 1}))
    end

    test "V-5 a v1 spec is never typed: {:legacy, map} whether or not its budgets would satisfy the schema" do
      {:ok, strict_valid} = run_spec_mod().validate(@v1)
      assert run_spec_mod().budgets(strict_valid) == {:legacy, @v1["budgets"]}

      arbitrary = %{"private_unknown" => @canary}
      {:ok, legacy} = run_spec_mod().validate(Map.put(@v1, "budgets", arbitrary))
      assert run_spec_mod().budgets(legacy) == {:legacy, arbitrary}
    end

    test "V-6 unsupported versions stay refused with v2 present; V-6b the accessor's limited promise and unvalidated maps" do
      for version <- [3, "1", "2", 1.0, 2.0, nil] do
        actual = run_spec_mod().validate(Map.put(@v1, "schema_version", version))
        assert match?({:error, %{clause: "unsupported_schema_version"}}, actual), inspect(version)
      end

      assert {:error, %{clause: "invalid_run_spec_shape"}} = run_spec_mod().validate(Map.delete(@v1, "schema_version"))

      for other <- [%{"schema_version" => 3}, %{"schema_version" => "2"}, %{}, nil, [], "spec"] do
        assert run_spec_mod().budgets(other) == {:error, %{clause: "unsupported_schema_version"}}, inspect(other)
      end

      # an UNVALIDATED map carrying version 2 with a malformed section is never labelled typed: the section is judged
      assert run_spec_mod().budgets(%{"schema_version" => 2, "budgets" => %{"restart_attempts" => -1}}) ==
               invalid("restart_attempts", "below_minimum")

      assert run_spec_mod().budgets(%{"schema_version" => 2, "budgets" => %{@canary => 1}}) ==
               invalid("budgets", "unknown_key")

      assert run_spec_mod().budgets(%{"schema_version" => 2, "budgets" => "x"}) == invalid("budgets", "not_map")
      assert run_spec_mod().budgets(%{"schema_version" => 2, "budgets" => nil}) == invalid("budgets", "not_map")
      # an UNVALIDATED map carrying version 1 whose budgets is not a map is the v1 shape refusal, never legacy
      assert run_spec_mod().budgets(%{"schema_version" => 1}) == {:error, %{clause: "invalid_run_spec_shape"}}

      assert run_spec_mod().budgets(%{"schema_version" => 1, "budgets" => "x"}) ==
               {:error, %{clause: "invalid_run_spec_shape"}}
    end

    test "V-7 precedence in v2: gates before budgets; allowed_roots before budgets" do
      bad = %{"restart_attempts" => -1}
      assert {:error, %{clause: "empty_gates"}} = run_spec_mod().validate(v2(%{"gates" => %{}, "budgets" => bad}))

      assert {:error, %{clause: "allowed_root_outside_repo"}} =
               run_spec_mod().validate(v2(%{"allowed_roots" => ["../outside"], "budgets" => bad}))
    end

    test "V-8 preservation on real v1 fixtures: validated map == input, bytes untouched, legacy never typed, no migrate/1" do
      for {path, 1, "ok"} <- @measured do
        bytes = File.read!(path)
        digest = sha256(bytes)
        spec = Jason.decode!(bytes)
        assert match?({:ok, _}, run_spec_mod().validate(spec)), path
        {:ok, validated} = run_spec_mod().validate(spec)
        assert validated == spec, "#{path}: the validated v1 map is the input map"
        assert run_spec_mod().budgets(validated) == {:legacy, spec["budgets"]}, path
        assert sha256(File.read!(path)) == digest, "#{path}: bytes unchanged"
      end

      # limits stated: the modules are pure by source review (no Fs seam, no migrate/1); this is not a runtime guard
      refute function_exported?(budgets_mod(), :migrate, 1)
      refute function_exported?(run_spec_mod(), :migrate, 1)
    end

    test "V-9 paired v1/v2 non-budget rejections are identical (sharing schemas neither tightens v1 nor weakens v2)" do
      pairs = [
        {%{"agents" => [%{"name" => "Bad Name", "role" => "writer"}]}, "agent_name_grammar"},
        {%{"agents" => [%{"name" => "user", "role" => "writer"}]}, "reserved_agent_name"},
        {%{"gates" => %{}}, "empty_gates"},
        {%{"gates" => %{"tests" => "mix test"}}, "gate_not_argv"},
        {%{"allowed_roots" => ["../outside"]}, "allowed_root_outside_repo"},
        {%{"private_top" => 1}, "invalid_run_spec_shape"},
        {%{"agents" => [%{"name" => "ok_agent", "role" => "writer", "private_agent_key" => 1}]}, "invalid_run_spec_shape"}
      ]

      for {overrides, clause} <- pairs do
        assert match?({:error, %{clause: ^clause}}, run_spec_mod().validate(Map.merge(@v1, overrides))), "v1 #{clause}"
        assert match?({:error, %{clause: ^clause}}, run_spec_mod().validate(v2(overrides))), "v2 #{clause}"
      end

      assert length(@fields) == 4
    end
  end
end
