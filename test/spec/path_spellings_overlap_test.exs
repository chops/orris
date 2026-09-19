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
  use ExUnitProperties

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

    # The fold may reach neither `Spec.PathBoundary` nor `Path` (replay purity: the boundary is
    # pinned and `Path.expand` consults `:os`), so it repeats the expansion as a pure segment walk.
    # This is the control that the walk and `PathBoundary.overlapping?/2` never drift apart: over
    # generated spellings (dotted, doubled, trailing, `..`-climbing, absolute), the lease overlap
    # refuses exactly the pairs the containment rule calls one directory or nested.
    property "the lease overlap and PathBoundary.overlapping?/2 agree on every generated spelling pair",
             %{lease_lines: lines} do
      check all(left <- spelling(), right <- spelling()) do
        lines = with_lease_paths(lines, "wsl_as_0001", [left], "wsl_as_0002", [right])

        case Fold.fold_lines(lines) do
          {:error, rejection} ->
            assert PathBoundary.overlapping?(left, right),
                   "refused a pair the rule keeps apart: #{inspect({left, right})}"

            F.assert_rejection_matches(rejection, %{"clause" => "workspace_lease_overlap", "entity_id" => "wsl_as_0002"})

          {:ok, _state} ->
            refute PathBoundary.overlapping?(left, right),
                   "admitted a pair the rule calls one directory: #{inspect({left, right})}"
        end
      end
    end
  end

  @segments ["lib", "test", "nested", ".", "..", ""]

  # every spelling the event schema admits as an allowed path (a non-empty string); the empty
  # spelling is the schema's own refusal (`invalid_event_data`), not the overlap's
  defp spelling do
    gen all(
          segments <- list_of(member_of(@segments), min_length: 1, max_length: 4),
          lead <- member_of(["", "./", "/"]),
          trail <- member_of(["", "/"]),
          path = lead <> Enum.join(segments, "/") <> trail,
          path != ""
        ) do
      path
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
