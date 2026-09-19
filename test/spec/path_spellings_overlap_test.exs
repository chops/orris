defmodule AiOrchestrator.Spec.PathSpellingsOverlapTest do
  @moduledoc """
  Invariant: two concurrent writers naming the same directory are refused whatever spelling each
  uses for it. The admission layer (`PathBoundary.lexical/2`, path_boundary.ex:102) canonicalises a
  worktree-relative name with `Path.expand(name, "/")`, so `lib`, `./lib`, `lib/` and `lib//x/./y`
  are ONE directory to the containment rule. The two overlap predicates that guard concurrent
  writers -- `Spec.Plan.validate_stretch_overlap/2` (clause `stretch_paths_overlap`) and the fold's
  `workspace_lease_overlap` clause -- must judge the same directory the same way, or a pair the
  admission layer admits as equivalent spellings passes the refusal that exists to stop it.

  Each row here names one spelling the admission layer already admits and asserts the refusal.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Spec.PathBoundary
  alias AiOrchestrator.Spec.Plan

  @plan_fixture "invalid_stretch_overlap"
  @lease_fixture "reject_workspace_lease_overlap"

  # one directory, four spellings the admission layer canonicalises to the same place
  @spellings [
    {"lib", "./lib"},
    {"lib", "lib/"},
    {"lib/nested", "lib//nested/"},
    {"lib/nested", "lib/./nested"}
  ]

  setup do
    {:ok,
     plan: F.json("plans", @plan_fixture, "plan.json"),
     spec: F.json("plans", @plan_fixture, "spec.json"),
     lease_lines: F.lines("journals", @lease_fixture)}
  end

  test "the premise: every spelling pair is admitted as one directory by the containment rule" do
    for {left, right} <- @spellings do
      assert PathBoundary.lexical(["lib"], [left, right]) == :ok
      assert Path.expand(left, "/") == Path.expand(right, "/"), "#{inspect({left, right})} are not one directory"
    end
  end

  describe "stretch_paths_overlap (Spec.Plan)" do
    test "the fixture pair is DAG-independent and opts in, so the refusal is reachable", %{plan: plan, spec: spec} do
      assert spec["stretch_worktrees"] == true
      assert Enum.map(plan["work_items"], & &1["deps"]) == [[], []]
    end

    for {left, right} <- @spellings do
      test "writers on #{inspect(left)} and #{inspect(right)} are refused", %{plan: plan, spec: spec} do
        plan = with_writer_paths(plan, [unquote(left)], [unquote(right)])

        assert Plan.validate(plan, spec) == {:error, %{clause: "stretch_paths_overlap"}}
      end
    end

    test "a spelling of a sibling directory is still admitted", %{plan: plan, spec: spec} do
      assert {:ok, _validated} = Plan.validate(with_writer_paths(plan, ["lib"], ["./test/"]), spec)
    end
  end

  describe "workspace_lease_overlap (Journal.Fold)" do
    for {left, right} <- @spellings do
      test "an active lease on #{inspect(left)} refuses a second lease on #{inspect(right)}", %{lease_lines: lines} do
        lines = with_lease_paths(lines, "wsl_as_0001", [unquote(left)], "wsl_as_0002", [unquote(right)])

        assert {:error, rejection} = Fold.fold_lines(lines)
        F.assert_rejection_matches(rejection, %{"clause" => "workspace_lease_overlap", "entity_id" => "wsl_as_0002"})
      end
    end

    test "a spelling of a sibling directory is still admitted", %{lease_lines: lines} do
      lines = with_lease_paths(lines, "wsl_as_0001", ["lib"], "wsl_as_0002", ["./test/"])

      assert {:ok, _state} = Fold.fold_lines(lines)
    end
  end

  # ---- helpers ----

  defp with_writer_paths(plan, left_paths, right_paths) do
    [left, right] = plan["work_items"]

    Map.put(plan, "work_items", [Map.put(left, "allowed_paths", left_paths), Map.put(right, "allowed_paths", right_paths)])
  end

  # the fixture's two `workspace_lease_requested` events, each given the allowed paths for its lease id
  defp with_lease_paths(lines, left_id, left_paths, right_id, right_paths) do
    Enum.map(lines, fn line ->
      event = Jason.decode!(line)

      case event do
        %{"type" => "workspace_lease_requested", "data" => %{"workspace_lease_id" => ^left_id}} ->
          Jason.encode!(put_in(event, ["data", "allowed_paths"], left_paths))

        %{"type" => "workspace_lease_requested", "data" => %{"workspace_lease_id" => ^right_id}} ->
          Jason.encode!(put_in(event, ["data", "allowed_paths"], right_paths))

        _other ->
          line
      end
    end)
  end
end
