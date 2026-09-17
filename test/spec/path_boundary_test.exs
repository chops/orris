defmodule AiOrchestrator.Spec.PathBoundaryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Spec.PathBoundary

  # A real worktree with a nested allowed root, a sibling directory outside every allowed root, and a
  # directory outside the worktree altogether; every symlink case is planted by the test that needs it.
  setup do
    base =
      Path.join([
        System.tmp_dir!(),
        "ai_orchestrator_path_boundary_test",
        Integer.to_string(System.unique_integer([:positive]))
      ])

    root = Path.join(base, "worktree")
    outside = Path.join(base, "outside")
    File.mkdir_p!(Path.join(root, "src/nested"))
    File.mkdir_p!(Path.join(root, "lib"))
    File.mkdir_p!(outside)
    File.write!(Path.join(root, "src/nested/file.ex"), "inside\n")
    File.write!(Path.join(outside, "secret.txt"), "outside\n")
    on_exit(fn -> File.rm_rf!(base) end)
    {:ok, base: base, root: root, outside: outside}
  end

  describe "accepted in-root changes" do
    test "existing files, absent descendants, the root itself and dotted segments under an allowed root",
         %{root: root} do
      changes = ["src/nested/file.ex", "src/new/deep/file.ex", "src", "src/./nested", "src//nested/"]
      assert PathBoundary.check(root, ["src"], changes) == :ok
      assert PathBoundary.lexical(["src"], changes) == :ok
    end

    test "a rename whose both sides stay under an allowed root", %{root: root} do
      assert PathBoundary.check(root, ["src", "lib"], [{:rename, "src/nested/file.ex", "lib/moved.ex"}]) == :ok
    end

    test "a symlink that resolves inside the worktree and under an allowed root", %{root: root} do
      File.ln_s!("nested", Path.join(root, "src/alias"))
      assert PathBoundary.check(root, ["src"], ["src/alias/file.ex", "src/alias"]) == :ok
    end

    test "an absolute symlink target that climbs back into the allowed root through its own ..",
         %{root: root, outside: outside} do
      File.ln_s!(Path.join(outside, "../worktree/src/nested"), Path.join(root, "src/roundtrip"))
      assert PathBoundary.check(root, ["src"], ["src/roundtrip/file.ex"]) == :ok
    end

    test "a worktree root reached through a symlink is judged by its canonical form", %{base: base, root: root} do
      File.ln_s!(root, Path.join(base, "root_link"))
      assert PathBoundary.check(Path.join(base, "root_link"), ["src"], ["src/nested/file.ex", "src/new.ex"]) == :ok
    end

    test "an absent worktree root leaves the lexical layer only", %{base: base} do
      assert PathBoundary.check(Path.join(base, "missing"), ["src"], ["src/x.ex"]) == :ok

      assert PathBoundary.check(Path.join(base, "missing"), ["src"], ["lib/x.ex"]) ==
               {:error, %{clause: "path_outside_roots", path: "lib/x.ex"}}
    end
  end

  describe "traversal" do
    test "a .. segment anywhere is refused before any root comparison", %{root: root} do
      for path <- ["src/../lib/x.ex", "../x.ex", "src/nested/../../../x", "src/.."] do
        assert PathBoundary.lexical(["src"], [path]) == {:error, %{clause: "path_traversal", path: path}}
        assert PathBoundary.check(root, ["src"], [path]) == {:error, %{clause: "path_traversal", path: path}}
      end
    end

    test "a .. that would still land under the allowed root is refused by form", %{root: root} do
      assert PathBoundary.check(root, ["src"], ["src/nested/../nested/file.ex"]) ==
               {:error, %{clause: "path_traversal", path: "src/nested/../nested/file.ex"}}
    end
  end

  describe "absolute paths" do
    test "an absolute path is refused even when it names a place under the worktree", %{root: root} do
      inside = Path.join(root, "src/nested/file.ex")

      for path <- ["/etc/passwd", inside] do
        assert PathBoundary.lexical(["src"], [path]) == {:error, %{clause: "path_absolute", path: path}}
        assert PathBoundary.check(root, ["src"], [path]) == {:error, %{clause: "path_absolute", path: path}}
      end
    end
  end

  describe "outside every allowed root" do
    test "a sibling root, a prefix trap and the empty name are outside", %{root: root} do
      for path <- ["lib/x.ex", "srcx/y.ex", "", "."] do
        assert PathBoundary.lexical(["src"], [path]) == {:error, %{clause: "path_outside_roots", path: path}}
        assert PathBoundary.check(root, ["src"], [path]) == {:error, %{clause: "path_outside_roots", path: path}}
      end
    end

    test "a rename names the side that leaves the roots", %{root: root} do
      assert PathBoundary.check(root, ["src"], [{:rename, "src/nested/file.ex", "lib/moved.ex"}]) ==
               {:error, %{clause: "path_outside_roots", path: "lib/moved.ex"}}

      assert PathBoundary.check(root, ["src"], [{:rename, "lib/orig.ex", "src/moved.ex"}]) ==
               {:error, %{clause: "path_outside_roots", path: "lib/orig.ex"}}
    end
  end

  describe "symlink escapes" do
    test "an absolute symlink to a directory outside the worktree", %{root: root, outside: outside} do
      File.ln_s!(outside, Path.join(root, "src/link"))

      assert PathBoundary.check(root, ["src"], ["src/link/secret.txt"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/link/secret.txt"}}
    end

    test "a relative symlink whose .. climbs out of the worktree", %{root: root} do
      File.ln_s!("../../outside", Path.join(root, "src/up"))

      assert PathBoundary.check(root, ["src"], ["src/up/new.txt"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/up/new.txt"}}
    end

    test "a symlink that stays inside the worktree but leaves every allowed root", %{root: root} do
      File.ln_s!("../lib", Path.join(root, "src/tolib"))

      assert PathBoundary.check(root, ["src"], ["src/tolib/x.ex"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/tolib/x.ex"}}
    end

    test "a symlink standing at the changed leaf itself", %{root: root, outside: outside} do
      File.ln_s!(Path.join(outside, "secret.txt"), Path.join(root, "src/leaf"))

      assert PathBoundary.check(root, ["src"], ["src/leaf"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/leaf"}}
    end

    test "a chain of symlinks is followed to its end", %{root: root, outside: outside} do
      File.ln_s!(outside, Path.join(root, "lib/hop"))
      File.ln_s!("../lib/hop", Path.join(root, "src/chain"))

      assert PathBoundary.check(root, ["src"], ["src/chain/x"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/chain/x"}}
    end

    test "a dangling symlink whose target would be created outside the worktree", %{root: root, outside: outside} do
      File.ln_s!(Path.join(outside, "missing/dir"), Path.join(root, "src/dangling"))

      assert PathBoundary.check(root, ["src"], ["src/dangling/x"]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/dangling/x"}}
    end

    test "a symlink loop cannot prove containment and is refused as unreadable", %{root: root} do
      File.ln_s!("loop", Path.join(root, "src/loop"))

      assert PathBoundary.check(root, ["src"], ["src/loop/x"]) ==
               {:error, %{clause: "path_unreadable", path: "src/loop/x"}}
    end

    test "a rename names the side that escapes", %{root: root, outside: outside} do
      File.ln_s!(outside, Path.join(root, "src/link"))

      assert PathBoundary.check(root, ["src"], [{:rename, "src/link/x", "src/y"}]) ==
               {:error, %{clause: "path_symlink_escape", path: "src/link/x"}}
    end

    test "the lexical layer alone cannot see a symlink, which is why check/3 exists", %{root: root, outside: outside} do
      File.ln_s!(outside, Path.join(root, "src/link"))
      assert PathBoundary.lexical(["src"], ["src/link/secret.txt"]) == :ok
    end
  end

  describe "the vocabulary" do
    test "every rejection carries exactly the clause and the offending path, from the closed set",
         %{root: root, outside: outside} do
      File.ln_s!(outside, Path.join(root, "src/link"))
      File.ln_s!("loop", Path.join(root, "src/loop"))

      rejections =
        for path <- ["/abs", "../x", "lib/x", "src/link/x", "src/loop/x"] do
          {:error, rejection} = PathBoundary.check(root, ["src"], [path])
          rejection
        end

      assert Enum.map(rejections, & &1.clause) == PathBoundary.clauses()
      assert Enum.all?(rejections, &(&1 |> Map.keys() |> Enum.sort() == [:clause, :path]))
    end

    test "the first offending change is named, in the order given", %{root: root} do
      assert PathBoundary.check(root, ["src"], ["src/ok.ex", "lib/first.ex", "/second"]) ==
               {:error, %{clause: "path_outside_roots", path: "lib/first.ex"}}
    end

    test "a change that is neither a path nor a rename pair is a caller error, never a verdict", %{root: root} do
      assert_raise ArgumentError, fn -> PathBoundary.check(root, ["src"], [:not_a_path]) end
      assert_raise ArgumentError, fn -> PathBoundary.lexical(["src"], [{:rename, "src/a", 1}]) end
    end
  end
end
