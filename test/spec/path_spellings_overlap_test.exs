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

  The converse holds too: a name the admission layer takes LITERALLY must stay literal to the
  overlap. `Path.expand` does not trim, so ` . ` (spaces around a dot) is a directory named ` . `,
  not the repository root, and ` ` is a directory named ` `, not the empty name; a writer on either
  is disjoint from a writer on `lib`. The refusal must not trim a literal name into the root and
  refuse a pair the containment rule keeps apart (refusal correctness, both layers).

  Named narrowing (pre-existing, not repaired here): a tilde-led name. `Path.expand` turns a leading
  `~` into the home directory, so `~/lib` under the root `.` is admitted and judged as `$HOME/lib`
  by the containment rule, while the fold's pure walk (which may not consult the environment) keeps
  `~` as a literal segment. The two layers agree on every tilde-led pair in the table below and
  disagree on the one pinned in the narrowing test, which is why tilde-led names are excluded from
  the parity generator. Follow-up: judge `~` literally at every `Path.expand` site of
  `Spec.PathBoundary` (`expanded/1`, `canonical_allowed_root/2`, `completed/2`), then admit tilde-led
  names to the generator and invert the narrowing test.
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

  # the repository root, spelled as admission spells it, against a directory under it: genuine
  # root overlap, refused under the root `.` (the rows a trimmed literal name must NOT be confused with)
  @root_overlaps [
    {".", "lib"},
    {"./", "lib/"},
    {"lib", "."}
  ]

  # literal names: whitespace is part of the name, a leading `~` is a segment to the fold; each pair
  # is two distinct directories under the root `.` to BOTH layers
  @literal_disjoint [
    {" . ", "lib"},
    {" ", "lib"},
    {" . ", " "},
    {" lib", "lib"},
    {"lib ", "lib"},
    {"~/lib", "lib"}
  ]

  # the same literal name twice, or two spellings of one literal name, is one directory
  @literal_overlaps [
    {" . ", " . "},
    {" . ", "./ . /"},
    {" ", " /"},
    {"lib ", "./lib /"},
    {"~/lib", "~/lib"},
    {"~/lib", "~/./lib/"}
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

  test "the premise: a literal name is admitted under the root `.` and is NOT the root or `lib`" do
    for {left, right} <- @literal_disjoint ++ @literal_overlaps do
      assert PathBoundary.lexical(["."], [left, right]) == :ok, "#{inspect({left, right})} not admitted under `.`"
    end

    for {left, right} <- @literal_disjoint do
      refute Path.expand(left, "/") == Path.expand(right, "/"), "#{inspect({left, right})} expand to one directory"
    end

    for {left, right} <- @literal_overlaps do
      assert Path.expand(left, "/") == Path.expand(right, "/"), "#{inspect({left, right})} are not one directory"
    end

    # the bytes the trim used to discard are the name
    assert Path.expand(" . ", "/") == "/ . "
    assert Path.expand(" ", "/") == "/ "
    assert Path.expand(" . ", "/") != Path.expand(".", "/")
  end

  # the narrowing, measured: the earlier record said a tilde-led name is refused outside every root;
  # that holds for the root `lib` and NOT for the root `.`, where `~/lib` is admitted as `$HOME/lib`
  test "the premise of the narrowing: a tilde-led name is the home directory to admission under `.`" do
    assert PathBoundary.lexical(["."], ["~/lib"]) == :ok
    assert PathBoundary.lexical(["lib"], ["~/lib"]) == {:error, %{clause: "path_outside_roots", path: "~/lib"}}
    assert Path.expand("~/lib", "/") == Path.join(System.user_home!(), "lib")
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

    for {left, right} <- @root_overlaps do
      test "under the root `.`, writers on #{inspect(left)} and #{inspect(right)} are refused", %{plan: plan, spec: spec} do
        plan = with_writer_paths(plan, [unquote(left)], [unquote(right)])

        assert Plan.validate(plan, under_repository_root(spec)) == {:error, %{clause: "stretch_paths_overlap"}}
      end
    end

    for {left, right} <- @literal_overlaps do
      test "under the root `.`, writers on the literal names #{inspect(left)} and #{inspect(right)} are refused",
           %{plan: plan, spec: spec} do
        plan = with_writer_paths(plan, [unquote(left)], [unquote(right)])

        assert Plan.validate(plan, under_repository_root(spec)) == {:error, %{clause: "stretch_paths_overlap"}}
      end
    end

    for {left, right} <- @literal_disjoint do
      test "under the root `.`, writers on the literal names #{inspect(left)} and #{inspect(right)} are admitted",
           %{plan: plan, spec: spec} do
        plan = with_writer_paths(plan, [unquote(left)], [unquote(right)])

        assert {:ok, _validated} = Plan.validate(plan, under_repository_root(spec))
      end
    end

    # NARROWING (pre-existing, follow-up item): the containment rule reads `~/lib` as the home
    # directory, so a writer spelling that directory relative to `/` is refused as overlapping it.
    # This pins what the rule does today so the follow-up that judges `~` literally must revisit it.
    test "narrowing: a tilde-led name overlaps the home directory spelled relative to `/`", %{plan: plan, spec: spec} do
      plan = with_writer_paths(plan, ["~/lib"], [home_relative("lib")])

      assert PathBoundary.overlapping?("~/lib", home_relative("lib"))
      assert Plan.validate(plan, under_repository_root(spec)) == {:error, %{clause: "stretch_paths_overlap"}}
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

    for {left, right} <- @root_overlaps ++ @literal_overlaps do
      test "an active lease on #{inspect(left)} refuses a second lease on the same directory #{inspect(right)}",
           %{lease_lines: lines} do
        lines = with_lease_paths(lines, "wsl_as_0001", [unquote(left)], "wsl_as_0002", [unquote(right)])

        assert {:error, rejection} = Fold.fold_lines(lines)
        F.assert_rejection_matches(rejection, %{"clause" => "workspace_lease_overlap", "entity_id" => "wsl_as_0002"})
      end
    end

    for {left, right} <- @literal_disjoint do
      test "an active lease on the literal name #{inspect(left)} admits a second lease on #{inspect(right)}",
           %{lease_lines: lines} do
        lines = with_lease_paths(lines, "wsl_as_0001", [unquote(left)], "wsl_as_0002", [unquote(right)])

        assert {:ok, _state} = Fold.fold_lines(lines)
      end
    end

    # NARROWING (pre-existing, follow-up item): the pair the Plan narrowing test refuses is admitted
    # here, because the walk keeps `~` literal. This is the one measured disagreement between the
    # layers; it is pinned so the follow-up must flip both tests together.
    test "narrowing: the fold keeps `~` literal, so the pair the containment rule overlaps is admitted",
         %{lease_lines: lines} do
      lines = with_lease_paths(lines, "wsl_as_0001", ["~/lib"], "wsl_as_0002", [home_relative("lib")])

      assert PathBoundary.overlapping?("~/lib", home_relative("lib"))
      assert {:ok, _state} = Fold.fold_lines(lines)
    end

    # The fold may reach neither `Spec.PathBoundary` nor `Path` (replay purity: the boundary is
    # pinned and `Path.expand` consults `:os`), so it repeats the expansion as a pure segment walk.
    # This is the control that the walk and `PathBoundary.overlapping?/2` never drift apart: over
    # generated spellings (dotted, doubled, trailing, `..`-climbing, absolute, and whitespace-bearing
    # segments that must stay literal), the lease overlap refuses exactly the pairs the containment
    # rule calls one directory or nested. Passing generated samples is evidence over the samples
    # drawn, not a proof over every name; the explicit tables above carry the named cases.
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

  # the three whitespace-bearing segments are literal names: ` . ` is not `.`, ` ` is not the empty
  # segment, `lib ` is not `lib`. A `~` segment is deliberately absent: at the head of a name the
  # containment rule reads it as the home directory and the walk as a literal segment (the pinned
  # narrowing above), so a generator carrying it would report that known disagreement, not a new one.
  @segments ["lib", "test", "nested", ".", "..", "", " . ", " ", "lib "]

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

  # the spec with the repository itself as the one allowed root, so a literal name under it is admitted
  defp under_repository_root(spec), do: Map.put(spec, "allowed_roots", ["."])

  # the home directory as a name relative to `/`, the directory `~/<name>` expands to under admission
  defp home_relative(name), do: Path.join(Path.relative_to(System.user_home!(), "/"), name)

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
