defmodule AiOrchestrator.Dispatch.PromptStoreTest do
  @moduledoc """
  RED for the minimum durable PromptStore, ruled into this slice by
  `m_1788513216000000000_a93fb78e` as a correctness prerequisite of NS-42 rule 5.

  The reason it is a prerequisite and not an enhancement is a property of the renderer.
  `prompt_bundle/5` renders from the whole accumulated event prefix, so a resumed render
  is a different function of a longer input than the one whose hash the journal committed.
  Reconciliation binds the payload by hash, so a payload the product cannot reproduce is a
  payload it cannot reconcile: the daemon would answer `conflict` for a prompt that was
  never in conflict, in exactly the resume path NS-42 exists to make safe. Reproducibility
  is not retention, so the bytes are kept.

  The store is therefore an effect boundary over the existing `Journal.Fs` seam rather than
  a pure function, and the reducer stays pure. What these tests pin:

    * publication is create-only. Bytes are written and fsynced to a temporary file, hard
      linked into their final name, the temporary is removed, and the directory is fsynced.
      Nothing is ever renamed over an existing prompt.
    * names are content addressed, so a second identical render publishes the same object
      and a second *different* render under the same name is a corruption report, never a
      silent overwrite.
    * an object is mode 0600 inside a 0700 directory under a contained relative path. The
      file holds the exact bytes handed to an agent, so it is not opened up to fail soft.
    * every failure is named. A caller may not distinguish "not durable" from "durable" by
      inspecting the disk after the call returned, so faults are injected at the seam.

  Ordering against the journal (blob before event) is pinned by the dispatch RED, not here;
  this file is the store's own contract.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.PromptRejection
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Dispatch.PromptStore
  alias AiOrchestrator.Lifecycle.Core.Diagnostic
  alias AiOrchestrator.Test.FaultFs

  @assignment "as_0001"
  @other_assignment "as_0002"
  @bytes "* Assignment\n\nDo the thing.\n"
  @other_bytes "* Assignment\n\nDo the other thing.\n"

  # Each id is paired with the class the store may report about it. Pinning the mapping
  # rather than mere membership is what keeps the classes from collapsing into one
  # uninformative atom the moment a caller finds them inconvenient.
  @invalid_ids [
    {"", :empty},
    {".", :dot_name},
    {"..", :dot_name},
    {"as/0001", :separator},
    {"../as_0001", :separator},
    {"as_0001/../../etc", :separator},
    {<<"as_", 0, "0001">>, :byte},
    {String.duplicate("a", 65), :too_long},
    {"as 0001", :byte},
    {"as\t0001", :byte},
    {"as_0001\n", :byte},
    {"as\u00e90001", :byte},
    {"as*0001", :byte}
  ]

  # The store's whole rejection vocabulary, seam faults and object-level refusals alike.
  # It is one set because an operator reads it as one: a reason that exists only at the
  # call site that invented it is a reason no runbook, no alert rule and no `Diagnostic`
  # clause knows about.
  #
  # It is the table's set less the two `PromptObject.new/1` raises while parsing an
  # object's fields, which no store operation can reach and which
  # `AiOrchestrator.LifecycleContractTest` produces instead. Naming those two here rather
  # than writing out the other twenty-eight is what keeps this honest in both directions:
  # a reason added to the table and reachable by no store operation fails below, and so
  # does one this module reaches that the table does not list.
  @not_store_produced [:prompt_object_unknown_field, :prompt_object_duplicate_field]
  @reasons PromptRejection.reasons() -- @not_store_produced

  setup do
    root = Path.join(System.tmp_dir!(), "prompt_store_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root, fs: FaultFs.new()}
  end

  describe "an object is named by what is in it" do
    test "the path carries the assignment and the full digest", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      digest = Base.encode16(:crypto.hash(:sha256, @bytes), case: :lower)

      assert is_struct(object, PromptObject), """
      The store answers in the vocabulary the contract carries. A bare map here would
      be a second, open shape for the same fact, and the effect that names the object
      would be free to arrive holding a decoded journal fragment instead.
      """

      assert object.version == 2, "retention only ever publishes content-addressed names"

      assert object.path == "prompts/#{@assignment}-#{digest}.org"
      assert object.hash == "sha256:" <> digest
      assert object.byte_size == byte_size(@bytes)

      assert File.read!(Path.join(ctx.root, object.path)) == @bytes
    end

    test "a truncated digest is not a name", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert [_assignment, digest] = String.split(Path.basename(object.path, ".org"), "-")

      assert String.length(digest) == 64,
             "a prefix collides on purpose under an attacker and by accident under enough runs"
    end

    test "the path is relative, so an operator may move the run directory", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      refute String.starts_with?(object.path, "/")
      refute object.path =~ ".."
    end
  end

  describe "publication is create-only" do
    test "the bytes are linked into place, never renamed over", ctx do
      assert {:ok, _object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      ops = ctx.fs |> FaultFs.trace() |> Enum.map(&elem(&1, 0))

      assert :link in ops,
             "rename replaces silently; link refuses, and refusing is the whole guarantee"

      refute :rename in ops
    end

    test "the temporary is fully written, fsynced and closed before it is linked", ctx do
      assert {:ok, _object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      ops = ctx.fs |> FaultFs.trace() |> Enum.map(&elem(&1, 0))

      assert index_of(ops, :write) < index_of(ops, :sync)
      assert index_of(ops, :sync) < index_of(ops, :close)

      assert index_of(ops, :close) < index_of(ops, :link), """
      `close` is where buffered bytes are flushed and, on some filesystems, the only
      place a write error is ever reported. Linking a still-open file publishes a name
      for bytes whose write has not been answered yet.
      """
    end

    test "the object is already 0600 when the first byte reaches it", ctx do
      assert {:ok, _object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      trace = FaultFs.trace(ctx.fs)

      assert at(trace, &match?({:chmod, _name, 0o600}, &1)) < at(trace, &match?({:write, _size}, &1)), """
      `open` creates the temporary under the process umask, so a chmod that runs after
      the write leaves a window in which a world-readable file holds the whole prompt.
      The mode is a property the bytes must never have been seen without, so it is set
      while the file is still empty.
      """
    end

    test "the temporary is removed on success", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert leftovers(ctx.root) == [],
             "a half-named file in the prompt directory is indistinguishable from an object"

      assert File.exists?(Path.join(ctx.root, object.path))
    end

    test "the directory is created 0700 and the object 0600", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert mode(Path.join(ctx.root, "prompts")) == 0o700
      assert mode(Path.join(ctx.root, object.path)) == 0o600
    end

    test "the prompt directory is narrowed before its own entry is made durable", ctx do
      assert {:ok, _object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      trace = FaultFs.trace(ctx.fs)
      root = Path.basename(ctx.root)

      assert at(trace, &(&1 == {:mkdir, "prompts"})) < at(trace, &(&1 == {:chmod, "prompts", 0o700})), """
      A directory that is made durable at the umask's mode and narrowed afterwards is
      a directory that can survive a crash at the umask's mode, holding prompts.
      """

      assert at(trace, &(&1 == {:chmod, "prompts", 0o700})) < at(trace, &(&1 == {:dir_sync, root}))

      assert at(trace, &(&1 == {:dir_sync, root})) < at(trace, &match?({:open, _name, _modes}, &1)), """
      `prompts/` is itself an entry in the run root, and an unsynced entry can vanish
      whole -- taking a published, fsynced object with it. The root is synced once,
      when the directory first appears.
      """
    end

    test "the object's own directory entry is fsynced after the temporary is gone", ctx do
      assert {:ok, _object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      trace = FaultFs.trace(ctx.fs)

      assert at(trace, &match?({:link, _from, _to}, &1)) < at(trace, &match?({:rm, _name}, &1))

      assert at(trace, &match?({:rm, _name}, &1)) < at(trace, &(&1 == {:dir_sync, "prompts"})), """
      One sync covers both changes this publication made to the directory: the name
      that appeared and the temporary that left. Counting syncs cannot tell either of
      those apart from a sync of the run root, which is why these assertions name the
      directory instead.
      """
    end

    test "a second put does not sync the run root again", ctx do
      assert {:ok, _first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      before = FaultFs.trace(ctx.fs)

      assert {:ok, _second} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@other_bytes))

      added = Enum.drop(FaultFs.trace(ctx.fs), length(before))

      refute {:dir_sync, Path.basename(ctx.root)} in added, """
      The run root's entry for `prompts/` became durable when the directory was
      created. Re-proving it on every prompt is cost that buys no claim, and a cost
      per prompt is the one that grows.
      """

      refute {:mkdir, "prompts"} in added
    end
  end

  describe "an identical render is the same object" do
    test "a second put of the same bytes reuses the object", ctx do
      assert {:ok, first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      writes = count(ctx.fs, :write)

      assert {:ok, ^first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert File.read!(Path.join(ctx.root, first.path)) == @bytes

      assert count(ctx.fs, :write) > writes,
             "the temporary is still written; what is idempotent is the publication"
    end

    test "a retry that lost its answer publishes nothing new", ctx do
      assert {:ok, first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      assert {:ok, ^first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      assert {:ok, ^first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == [Path.basename(first.path)]
      assert leftovers(ctx.root) == []
    end

    test "different bytes at an existing name are corruption, not an update", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      # Content addressing makes this unreachable by a correct renderer, which is exactly
      # why reaching it is evidence of damage rather than of a new version.
      File.write!(Path.join(ctx.root, object.path), @other_bytes)

      assert {:error, {:prompt_object_conflict, path}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert path == object.path

      assert File.read!(Path.join(ctx.root, object.path)) == @other_bytes,
             "the damaged bytes are preserved for an operator to look at, not repaired"

      assert leftovers(ctx.root) == []
    end

    test "a different render is a different object beside the first", ctx do
      assert {:ok, first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      assert {:ok, second} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@other_bytes))

      refute first.path == second.path
      assert File.read!(Path.join(ctx.root, first.path)) == @bytes
      assert File.read!(Path.join(ctx.root, second.path)) == @other_bytes
    end
  end

  describe "durability is proven at the seam, not by looking at the disk afterwards" do
    test "a short write publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :write, count(ctx.fs, :write) + 1, {:torn, 7})

      assert {:error, {:prompt_write_failed, :torn_write}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == [],
             "a short write must not acquire a name that claims the whole prompt is there"

      assert FaultFs.halted?(ctx.fs), """
      The seam stopped answering mid-write, so the store could not remove what it staged.
      A temporary that outlives a halted seam is not a leak the store can close; what it
      must never do is give those bytes a name.
      """
    end

    test "a failed write publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :write, count(ctx.fs, :write) + 1, {:error, :enospc})

      assert {:error, {:prompt_write_failed, :enospc}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
    end

    test "a failed file fsync publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :sync, count(ctx.fs, :sync) + 1, {:error, :eio})

      assert {:error, {:prompt_sync_failed, :eio}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == [],
             "bytes the kernel has not acknowledged must not acquire a name that claims it has"

      assert leftovers(ctx.root) == []
    end

    test "a failed link publishes nothing and leaves no temporary", ctx do
      FaultFs.inject(ctx.fs, :link, count(ctx.fs, :link) + 1, {:error, :eacces})

      assert {:error, {:prompt_link_failed, :eacces}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
    end

    test "a failed object chmod publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :chmod, fn [_name, mode] -> mode == 0o600 end, {:error, :eperm})

      assert {:error, {:prompt_chmod_failed, :eperm}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == [],
             "the object holds the exact bytes handed to an agent; a mode failure fails closed"

      assert leftovers(ctx.root) == []
    end

    test "a caller is never told an object is durable before the seam says so", ctx do
      FaultFs.inject(ctx.fs, :sync, count(ctx.fs, :sync) + 1, :halt)

      assert {:error, _reason} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert FaultFs.halted?(ctx.fs),
             "the fault latched, so no later operation could have quietly finished the job"
    end

    test "a prompt directory that cannot be created publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :mkdir, 1, {:error, :eacces})

      assert {:error, {:prompt_dir_create_failed, :eacces}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert File.ls!(ctx.root) == []
    end

    test "a directory chmod failure is not an object chmod failure", ctx do
      FaultFs.inject(ctx.fs, :chmod, fn [name, _mode] -> name == "prompts" end, {:error, :eperm})

      assert {:error, {:prompt_dir_chmod_failed, :eperm}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == []

      assert leftovers(ctx.root) == [], """
      Two failures that both read `prompt_chmod_failed` send an operator to the wrong
      file. The directory's mode governs every object it will ever hold; the object's
      governs one.
      """
    end

    test "a failed root fsync after the directory appears publishes nothing", ctx do
      root = Path.basename(ctx.root)
      FaultFs.inject(ctx.fs, :dir_sync, fn [dir] -> dir == root end, {:error, :eio})

      assert {:error, {:prompt_dir_sync_failed, :eio}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
    end

    test "a failed open publishes nothing", ctx do
      FaultFs.inject(ctx.fs, :open, 1, {:error, :emfile})

      assert {:error, {:prompt_open_failed, :emfile}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
    end

    test "a failed close publishes nothing and leaves no temporary", ctx do
      FaultFs.inject(ctx.fs, :close, 1, {:error, :eio})

      assert {:error, {:prompt_close_failed, :eio}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      ops = ctx.fs |> FaultFs.trace() |> Enum.map(&elem(&1, 0))

      refute :link in ops, "a name published for bytes whose close failed is a name for bytes nobody can vouch for"

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
    end

    test "a temporary that cannot be removed is reported, never swallowed", ctx do
      FaultFs.inject(ctx.fs, :rm, 1, {:error, :eperm})

      assert {:error, {:prompt_temp_cleanup_failed, :eperm}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert leftovers(ctx.root) != [], """
      The failure is reported precisely because the temporary survived, and this is
      the one shape the ruling forbids hiding: an `:ok` that leaves a file in the
      prompt directory which the next reader cannot tell from an object.
      """
    end

    test "a failed cleanup fsync is reported even though the object exists", ctx do
      FaultFs.inject(ctx.fs, :dir_sync, fn [dir] -> dir == "prompts" end, {:error, :eio})

      assert {:error, {:prompt_publication_sync_failed, :eio}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert objects(ctx.root) != [],
             "the link landed; what is unproven is that the entry survives a crash"

      assert leftovers(ctx.root) == []
    end

    test "a retry after a failed cleanup fsync re-verifies and syncs before acknowledging", ctx do
      FaultFs.inject(ctx.fs, :dir_sync, fn [dir] -> dir == "prompts" end, {:error, :eio})

      assert {:error, {:prompt_publication_sync_failed, :eio}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      # A retry is a new process against the same disk: it inherits the object and none of
      # the knowledge of how it got there.
      retry = FaultFs.new()

      assert {:ok, object} = PromptStore.put(retry, ctx.root, @assignment, wrapped(@bytes))

      trace = FaultFs.trace(retry)

      assert Enum.any?(trace, &match?({:read, _name}, &1)), """
      The name is a claim about the bytes and this retry did not make it. Reusing an
      object found in place without reading it back lets a half-written or foreign
      file inherit a digest it does not have.
      """

      assert {:dir_sync, "prompts"} in trace, """
      The first attempt left the entry unsynced and nothing on disk records that. The
      only safe reading of "the object is already there" is "its durability is still
      unproven", so the retry proves it again before returning `:ok`.
      """

      assert File.read!(Path.join(ctx.root, object.path)) == @bytes
    end
  end

  describe "a name is only a name, so the entry it reaches is checked too" do
    test "an assignment id that is not a single safe name never touches the disk", ctx do
      for {id, class} <- @invalid_ids do
        assert PromptStore.put(ctx.fs, ctx.root, id, wrapped(@bytes)) ==
                 {:error, {:prompt_assignment_id_invalid, class}},
               "an id that is not a name is rejected before it becomes half of one: #{inspect(id)}"
      end

      assert FaultFs.trace(ctx.fs) == [], """
      Containment checked after the object is written is containment checked after the
      damage. The id is half the object's name, so it is validated before the name
      exists.
      """

      assert File.ls!(ctx.root) == []
    end

    test "a symlink standing in for the prompt directory is refused", ctx do
      elsewhere = Path.join(ctx.root, "elsewhere")
      File.mkdir_p!(elsewhere)
      File.ln_s!(elsewhere, Path.join(ctx.root, "prompts"))

      assert {:error, {:prompt_dir_not_directory, :symlink}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert File.ls!(elsewhere) == [], """
      `Path.expand` answers whether a name stays under the root, which is not whether
      it reaches a file under the root. A symlink is a legal name that passes the
      first question and fails the second, and following this one would write prompt
      bytes wherever it points.
      """
    end

    test "a regular file standing in for the prompt directory is refused", ctx do
      File.write!(Path.join(ctx.root, "prompts"), "not a directory\n")

      assert {:error, {:prompt_dir_not_directory, :regular}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert File.read!(Path.join(ctx.root, "prompts")) == "not a directory\n"
    end

    test "a symlink is not reused as an object even when it resolves to the right bytes", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      decoy = Path.join(ctx.root, "decoy.org")
      File.write!(decoy, @bytes)
      File.rm!(Path.join(ctx.root, object.path))
      File.ln_s!(decoy, Path.join(ctx.root, object.path))

      assert PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes)) ==
               {:error, {:prompt_object_not_regular, :symlink}},
             "the digest matches through the link, which is exactly why the digest is not the check"

      assert {:error, {:prompt_object_not_regular, :symlink}} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, object)
    end

    test "the object's type is decided before it is read", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      File.rm!(Path.join(ctx.root, object.path))
      File.mkdir!(Path.join(ctx.root, object.path))

      probe = FaultFs.new()

      assert {:error, {:prompt_object_not_regular, :directory}} =
               PromptStore.fetch_verified(probe, ctx.root, object)

      refute Enum.any?(FaultFs.trace(probe), &match?({:read, _name}, &1)), """
      Whatever a directory read fails with is not evidence about a prompt. Naming the
      type tells an operator that something replaced the object, rather than that the
      object went bad.
      """
    end
  end

  describe "an object that disagrees with itself never reaches the disk" do
    setup ctx do
      {:ok, mine} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      {:ok, theirs} = PromptStore.put(ctx.fs, ctx.root, @other_assignment, wrapped(@other_bytes))
      {:ok, mine: mine, theirs: theirs}
    end

    test "an object naming my assignment and another assignment's file is refused", ctx do
      substituted = %{ctx.theirs | assignment_id: @assignment}

      # Every check the store makes about the *entry* passes here, and the setup proves
      # it rather than asserting it: the path is inside `prompts/`, the entry is a
      # regular file the store itself published, its bytes hash to `hash` and their
      # length is `byte_size`. It is another assignment's real object wearing my id.
      assert File.regular?(Path.join(ctx.root, substituted.path))
      assert "sha256:" <> Base.encode16(:crypto.hash(:sha256, @other_bytes), case: :lower) == substituted.hash
      assert byte_size(@other_bytes) == substituted.byte_size

      probe = FaultFs.new()

      assert PromptStore.fetch_verified(probe, ctx.root, substituted) ==
               {:error, {:prompt_object_name_mismatch, :assignment}},
             """
             Containment and the digest both answer questions about the bytes at a name.
             Neither answers whose bytes they are. The name is derived from the id, so
             the id and the name are one claim that can be checked against itself --
             and a fetch that skips that check hands `#{@assignment}` the prompt written
             for `#{@other_assignment}`.
             """

      assert FaultFs.trace(probe) == [], """
      A rejection that arrives after an `lstat` or a `read` has already answered the
      question the substitution was asking: whether that object exists, and what is in
      it. The refusal is a decision about the object's own fields, so it is taken
      before anything reaches the seam. `Fs.exists?/2` is not traced, so routing the
      lookup through it would evade this assertion rather than satisfy it.
      """
    end

    test "the substituted object's bytes are not returned by any path", ctx do
      substituted = %{ctx.theirs | assignment_id: @assignment}

      assert {:error, reason} = PromptStore.fetch_verified(FaultFs.new(), ctx.root, substituted)

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ "Do the other thing", """
      The refusal is about an object that names another assignment's prompt. Quoting
      any part of what it reached is the disclosure the refusal exists to prevent.
      """
    end

    # `{name, override, expected}`. Each inconsistency is paired with the class the store
    # may report, in the same discipline `@invalid_ids` applies to caller-supplied ids:
    # a reducer branches on the class, so two different inconsistencies arriving under
    # one name cannot be told apart by the code deciding whether to re-render.
    @self_inconsistencies [
      {"a legacy name at the content-addressed version", %{version: 1}, {:prompt_object_name_mismatch, :scheme}},
      {"a digest the hash does not name", %{hash: "sha256:" <> String.duplicate("b", 64)},
       {:prompt_object_name_mismatch, :digest}},
      {"a hash with no algorithm", %{hash: String.duplicate("c", 64)}, {:prompt_object_hash_malformed, :algorithm}},
      {"a hash that is not lowercase hex", %{hash: "sha256:" <> String.duplicate("Z", 64)},
       {:prompt_object_hash_malformed, :digest}},
      {"a naming scheme that does not exist", %{version: 3}, {:prompt_object_version_unsupported, :unknown}}
    ]

    for {name, override, expected} <- @self_inconsistencies do
      test "the store refuses #{name} without reading anything", ctx do
        override = unquote(Macro.escape(override))
        expected = unquote(Macro.escape(expected))

        probe = FaultFs.new()

        assert {:error, reason} =
                 PromptStore.fetch_verified(probe, ctx.root, Map.merge(ctx.mine, override))

        assert reason == expected, """
        #{unquote(name)} was refused as `#{inspect(reason)}` rather than `#{inspect(expected)}`.
        """

        assert FaultFs.trace(probe) == [], """
        A malformed hash cannot be compared against anything, and a name that is not the
        one the fields derive is not this object's name. Both are decided from the object
        alone, so neither costs an `lstat` -- and an `lstat` taken first is an answer
        about the filesystem given to a caller whose object was never valid.
        """

        assert elem(reason, 0) in @reasons, """
        The object-level rejections join the same closed set as the seam's. An operator
        reads all of these from one place, and a reason invented at a call site is a
        reason no runbook and no `Diagnostic` clause knows about.
        """
      end
    end

    # `PromptObject.new/1` is not the only door into a `%PromptObject{}`. A struct that
    # arrives over a process boundary ran no constructor, and a struct always has every
    # key, so the shapes a consumer can actually receive include ones the constructor
    # could never return. Each of these is a `verify/1` rejection the store propagates
    # rather than re-classifies: the store's job is to refuse, and inventing a second
    # name for a refusal the contract already names is how one failure becomes two
    # vocabularies. The constructor-only classes -- a duplicate field, an unknown field --
    # are unreachable from a struct and are deliberately absent.
    @struct_reachable [
      {"a size below zero", %{byte_size: -1}, {:prompt_object_byte_size_invalid, :negative}},
      {"a size that is not a number", %{byte_size: "6"}, {:prompt_object_byte_size_invalid, :not_an_integer}},
      {"a nil assignment id", %{assignment_id: nil}, {:prompt_object_incomplete, :assignment_id}},
      {"a nil path", %{path: nil}, {:prompt_object_incomplete, :path}},
      {"a nil hash", %{hash: nil}, {:prompt_object_incomplete, :hash}},
      {"a nil size", %{byte_size: nil}, {:prompt_object_incomplete, :byte_size}},
      {"a nil version", %{version: nil}, {:prompt_object_incomplete, :version}}
    ]

    for {name, override, expected} <- @struct_reachable do
      test "a forged object carrying #{name} is refused before the seam", ctx do
        override = unquote(Macro.escape(override))
        expected = unquote(Macro.escape(expected))

        probe = FaultFs.new()
        forged = Map.merge(ctx.mine, override)

        assert forged.__struct__ == PromptObject, """
        The point of this table is the struct, not a map that resembles one. A map would
        be refused by a function head and prove nothing about what crosses a boundary.
        """

        assert PromptStore.fetch_verified(probe, ctx.root, forged) == {:error, expected}, """
        #{unquote(name)} is a `PromptObject.verify/1` rejection, and the store's answer is
        that rejection unchanged. A store that re-reports it under a name of its own gives
        an operator two vocabularies for one fault and a reducer two clauses to keep in
        step; a store that does not check at all reaches the disk on a claim nobody made.
        """

        assert FaultFs.trace(probe) == [], """
        A nil field or a negative length is decided from the object alone. Deriving a path
        from a nil id, or asking the filesystem about a length that cannot exist, spends a
        seam call to learn something the fields already said.
        """

        assert elem(expected, 0) in @reasons
      end
    end

    test "a forged id is judged as an id before it is judged as a name", ctx do
      # The id and the path are one claim, so a bad id usually arrives with a path that
      # does not derive from it -- two rules broken at once, and which one is reported is
      # not a matter of taste. `prompt_object_name_mismatch` says the object is internally
      # inconsistent, which sends an operator looking for the object it was confused with.
      # The id is the rule that is broken here, and no such object exists.
      probe = FaultFs.new()
      forged = %{ctx.mine | assignment_id: "as/0001"}

      refute forged.path =~ "as/0001", "the setup is only meaningful while the path still names the old id"

      assert PromptStore.fetch_verified(probe, ctx.root, forged) ==
               {:error, {:prompt_object_assignment_id_invalid, :separator}}

      assert FaultFs.trace(probe) == []
    end

    for {id, class} <- @invalid_ids do
      test "an id refused at #{inspect(id)} is refused the same way whoever presents it", ctx do
        id = unquote(id)
        class = unquote(class)

        # One rule, two doors. `put` takes the id from a caller and `new` takes it from a
        # decoded map, and they are the same sixty-four bytes of grammar or they are two
        # rules that will drift apart the first time one of them is relaxed. The reasons
        # differ by prefix on purpose -- a bad id in a request and a bad id in an object
        # that already exists are different situations -- so it is the class that has to
        # match, because the class is what says *why*.
        assert {:error, {:prompt_assignment_id_invalid, ^class}} =
                 PromptStore.put(FaultFs.new(), ctx.root, id, wrapped(@bytes))

        "sha256:" <> digest = ctx.mine.hash
        fields = %{Map.from_struct(ctx.mine) | assignment_id: id, path: "prompts/#{id}-#{digest}.org"}

        assert PromptObject.new(fields) == {:error, {:prompt_object_assignment_id_invalid, class}}, """
        The path is derived from the bad id, so this object is internally perfect and the
        grammar is the only rule it breaks. Derivation is not a filter -- an id of any
        shape derives a name that agrees with it -- so an id checked only after the name
        is built is an id that was never checked.
        """
      end
    end

    test "a well-formed object still fetches, so the checks are not refusing everything", ctx do
      assert {:ok, sensitive} = PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.mine)
      assert SensitiveBytes.reveal(sensitive) == @bytes
    end
  end

  describe "a reused entry is narrowed or refused before it is trusted" do
    # The store claims 0700 on the directory and 0600 on the object. That claim is about
    # the entry it uses, not about the entry it happens to have created: a run resumed
    # against a `prompts/` left at the umask's mode holds prompts at 0755 for as long as
    # the run lasts, and the creation-time proof above says nothing about it.
    #
    # The two entries get different answers, because the repair a directory admits is one
    # a file does not. Narrowing `prompts/` is repaired and made durable the same way
    # creation makes it durable -- chmod, then a sync of `prompts/` -- and that sync is
    # honest because the directory is the inode whose mode changed. The same chain
    # applied to an object would not be: a directory sync persists the directory's own
    # entries, not another inode's metadata, so a store that chmods an object and syncs
    # the parent has narrowed the mode in memory and proved nothing about what survives a
    # crash. Making it honest would mean opening the object, syncing that descriptor and
    # naming two more failure modes on the read path -- a write, and two new ways to
    # fail, added to the path whose whole job is to read.
    #
    # So the object fails closed instead. An object at a mode the store never publishes
    # is not a wide entry the store can tidy: the store writes 0600 and only 0600, so a
    # different mode is evidence that something outside this run has held the entry, and
    # the useful answer is to say so rather than to erase the evidence and continue. The
    # same reasoning refuses an object with more than one link before the mode is even
    # considered -- an inode a second name reaches is not this run's to narrow or to
    # trust, and its mode is not a fact about this run at all.

    test "an existing wide prompt directory is narrowed before the temporary is opened", ctx do
      prompts = Path.join(ctx.root, "prompts")
      File.mkdir_p!(prompts)
      File.chmod!(prompts, 0o755)

      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      assert mode(prompts) == 0o700
      assert mode(Path.join(ctx.root, object.path)) == 0o600

      trace = FaultFs.trace(ctx.fs)

      assert at(trace, &(&1 == {:chmod, "prompts", 0o700})) < at(trace, &match?({:open, _name, _modes}, &1)), """
      Prompt bytes written into a 0755 directory are readable by every account on the
      machine for the window between the write and the narrowing, and if the process
      dies in that window there is no narrowing at all.
      """

      assert at(trace, &(&1 == {:chmod, "prompts", 0o700})) < at(trace, &(&1 == {:dir_sync, "prompts"}))
    end

    test "a wide prompt directory that cannot be narrowed fails the publication", ctx do
      prompts = Path.join(ctx.root, "prompts")
      File.mkdir_p!(prompts)
      File.chmod!(prompts, 0o755)

      fs = FaultFs.new()
      FaultFs.inject(fs, :chmod, &match?(["prompts", _mode], &1), {:error, :eperm})

      assert PromptStore.put(fs, ctx.root, @assignment, wrapped(@bytes)) ==
               {:error, {:prompt_dir_chmod_failed, :eperm}},
             """
             Accepting a directory the store could not narrow is the silent acceptance
             MUST-10 refuses: the call returns `:ok` and the 0700 claim in this module's
             own docs is false for every prompt the run writes afterwards.
             """

      assert objects(ctx.root) == []
      assert leftovers(ctx.root) == []
      assert mode(prompts) == 0o755, "the store reports what it could not change rather than pretending"
    end

    test "a narrowing that cannot be made durable fails the publication", ctx do
      prompts = Path.join(ctx.root, "prompts")
      File.mkdir_p!(prompts)
      File.chmod!(prompts, 0o755)

      fs = FaultFs.new()
      FaultFs.inject(fs, :dir_sync, &match?(["prompts"], &1), {:error, :eio})

      assert PromptStore.put(fs, ctx.root, @assignment, wrapped(@bytes)) ==
               {:error, {:prompt_dir_sync_failed, :eio}},
             """
             A repair that is not durable is a repair that a crash undoes, leaving the
             directory wide and the run believing it is narrow.
             """
    end

    test "an existing wide object is refused rather than narrowed, on the publication path", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      path = Path.join(ctx.root, object.path)
      File.chmod!(path, 0o644)

      fs = FaultFs.new()

      assert PromptStore.put(fs, ctx.root, @assignment, wrapped(@bytes)) ==
               {:error, {:prompt_object_mode_unexpected, :wider}},
             """
             The reuse path is the one a retry takes. Answering `:ok` here hands back an
             object the store never published at a mode it never writes, and the caller
             has no way left to learn that the entry it was given was reachable by every
             account on the machine before it arrived.
             """

      trace = FaultFs.trace(fs)

      refute Enum.any?(trace, &match?({:chmod, _name, _mode}, &1)), """
      A chmod here would be the dishonest repair: the mode changes in memory, the store
      syncs `prompts/` because that is the descriptor it has, and nothing has proved the
      object's own inode metadata survives the crash the sync was there to survive.
      """

      refute Enum.any?(trace, &match?({:read, _name}, &1))
      assert mode(path) == 0o644, "the store reports what it found rather than erasing it"
    end

    test "an object narrowed past 0600 is refused too, and named as narrower", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      path = Path.join(ctx.root, object.path)
      File.chmod!(path, 0o400)

      fs = FaultFs.new()

      assert PromptStore.fetch_verified(fs, ctx.root, object) ==
               {:error, {:prompt_object_mode_unexpected, :narrower}},
             """
             0400 is not a danger, and that is the point of naming it apart from 0644: an
             operator who tightened a file by hand and an attacker who loosened one are
             two different mornings, and a single `mode_unexpected` reason that cannot
             tell them apart sends both to the same runbook. What they share is that the
             store did not write this mode, so the entry is not the one it published.
             """

      assert mode(path) == 0o400
    end

    test "a wide object is refused on the fetch path before the bytes are read", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      path = Path.join(ctx.root, object.path)
      File.chmod!(path, 0o666)

      fs = FaultFs.new()

      assert {:error, {:prompt_object_mode_unexpected, :wider}} =
               PromptStore.fetch_verified(fs, ctx.root, object)

      trace = FaultFs.trace(fs)

      assert Enum.any?(trace, &match?({:lstat, _name}, &1))

      refute Enum.any?(trace, &match?({:read, _name}, &1)), """
      Reading first and refusing afterwards is not refusing: the bytes are in the BEAM,
      the hash matches, and the only thing standing between them and the caller is a
      branch that a later refactor reads as redundant.
      """

      refute Enum.any?(trace, &match?({:chmod, _name, _mode}, &1))
    end

    test "an object reachable under a second name is refused before its mode is judged", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      path = Path.join(ctx.root, object.path)

      outside_dir = ctx.root <> "-outside"
      File.mkdir_p!(outside_dir)
      on_exit(fn -> File.rm_rf!(outside_dir) end)
      outside = Path.join(outside_dir, "operator-notes.org")

      :ok = :file.make_link(String.to_charlist(path), String.to_charlist(outside))
      File.chmod!(outside, 0o644)

      fs = FaultFs.new()

      assert PromptStore.fetch_verified(fs, ctx.root, object) ==
               {:error, {:prompt_object_multiply_linked, :link_count}},
             """
             The entry passes every check that looks at the name: it is a regular file,
             its name derives from its hash, its bytes hash correctly. What it is not is
             this run's, and no check that reads type and mode can see that.
             """

      assert {:error, {:prompt_object_multiply_linked, :link_count}} =
               PromptStore.put(fs, ctx.root, @assignment, wrapped(@bytes))

      trace = FaultFs.trace(fs)

      refute Enum.any?(trace, &match?({:chmod, _name, _mode}, &1)), """
      This is the concrete harm the link count exists to prevent. A store that narrowed a
      wide reused object would chmod this inode, and the operator's file -- named outside
      the run, owned by a decision the run knows nothing about -- would change mode
      because a prompt happened to share its inode.
      """

      refute Enum.any?(trace, &match?({:read, _name}, &1))

      assert mode(outside) == 0o644, "the name outside the run keeps the mode its owner set"
      assert File.read!(outside) == @bytes, "and its bytes are not the store's to touch either"
    end

    test "an entry already at the right mode is left alone", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))

      fs = FaultFs.new()

      assert {:ok, _sensitive} = PromptStore.fetch_verified(fs, ctx.root, object)

      trace = FaultFs.trace(fs)

      refute Enum.any?(trace, &match?({:chmod, _name, _mode}, &1)), """
      The read path never repairs an object, so there is no mode at which it chmods one.
      Stating that as an absence rather than as a condition is the point: a chmod that
      appears here later is a write on the read path, and a write on the read path is a
      failure mode the read path did not have.
      """

      assert at(trace, &match?({:lstat, _name}, &1)) < at(trace, &match?({:read, _name}, &1)), """
      The entry is admitted on the strength of what it is, so what it is has to be
      established before the bytes are taken.
      """
    end
  end

  describe "a failure names itself without quoting the prompt" do
    test "every seam failure is reported from a closed set, and leaves its own disk as named", ctx do
      for {op, injection, fault, post} <- seam_faults() do
        {case_root, result} = failing_put(ctx.root, op, injection, fault)

        assert {:error, reason} = result

        assert elem(reason, 0) in @reasons, """
        `#{inspect(op)}` failed with `#{inspect(reason)}`, which is not in the closed
        set. An operator reads these; a reason invented at a call site is a reason no
        runbook, no alert rule and no `Diagnostic` clause knows about.
        """

        assert_post_state(case_root, op, post)
      end
    end

    test "every rejection is a pair, so one shape leaves this module", ctx do
      for {label, reason} <- every_rejection(ctx.root) do
        assert tuple_size(reason) == 2, """
        `#{inspect(label)}` reported `#{inspect(reason)}`. Arity is the contract here.
        `elem(reason, 0) in @reasons` is true of a tuple of any size, so the closed-set
        test beside this one is satisfied by a three-element rejection that no consumer
        can match and that `Diagnostic.describe_rejection/1` cannot recognise -- it falls
        through to a result class and the reason is gone. A rejection carries two closed
        sets and nothing else; a value that is neither is not part of the term.
        """

        assert is_atom(elem(reason, 0)), "a reason is a name, and `#{inspect(label)}` gave `#{inspect(reason)}`"
      end
    end

    test "every rejection family reaches the normalization boundary as its own two names", ctx do
      produced = every_rejection(ctx.root)

      for {label, reason} <- produced do
        described = Diagnostic.describe_rejection(reason)

        assert Enum.sort(Map.keys(described)) == ["class", "reason"], """
        `#{inspect(label)}` reported `#{inspect(reason)}` and was described as
        `#{inspect(described)}`. Two keys is the whole shape. A `result_class` or a
        `digest` key here means the pair fell through to `Diagnostic.describe/1`, which
        is the degradation this test exists to catch: the reason is gone, and nothing in
        what an operator is shown says it was ever there.
        """

        assert described["reason"] == Atom.to_string(elem(reason, 0)), """
        `#{inspect(reason)}` is an in-process shape and it is not the shape that leaves
        the process: `Jason.Encoder` is not implemented for tuples, so this pair reaches
        a protocol reply or a journal event only through this function. A reason this
        module produces and `Diagnostic`'s closed set does not list is not an encoder
        crash and not a leak. It is worse than either, because it degrades silently to a
        result class and the vocabulary a runbook is written against is gone.
        """

        assert {:ok, encoded} = Jason.encode(described)
        assert {:ok, ^described} = Jason.decode(encoded)

        case elem(reason, 1) do
          class when is_atom(class) ->
            assert described["class"] == Atom.to_string(class), """
            `#{inspect(label)}` classified its failure `#{inspect(class)}` and the boundary
            reported `#{inspect(described["class"])}`. The class half is gated against its own
            closed set, so a class this module produces and that set omits reads as `nil` --
            an operator sees the reason and loses the one field that tells `:eperm` from
            `:enospc`, or a mode that was widened from one that was tightened.
            """

            assert PromptRejection.allows?(elem(reason, 0), class), """
            `#{inspect(label)}` produced `#{inspect(reason)}` and the table does not admit
            that class under that reason. This is the direction the reason set below cannot
            check: both halves can be names this release owns while the pair is one no
            producer should be able to emit. Two membership lists admit their product, which
            is a rectangle rather than a closed set; the table is the set, and it is what
            `Diagnostic` gates against and what a consumer branches on. A pair it omits
            reads as `nil` at the boundary and the class half is gone. Either this call site
            classified wrongly or the table is missing a row -- the pair is what says which,
            and neither list alone can.
            """

          path when is_binary(path) ->
            assert described["class"] == nil, """
            `#{inspect(label)}` carries the contained relative path the journal already
            committed. In process that path is the only thing naming which object went
            missing, and the ruling makes it safe metadata. The class slot is not where it
            is published: a slot a consumer branches on cannot also be free text.
            """

            assert PromptRejection.classes(elem(reason, 0)) == [], """
            `#{inspect(label)}` returned a contained path where a class goes, and the table
            lists classes under `#{inspect(elem(reason, 0))}`. Which arm a rejection takes is
            decided by the table's empty class list, not by the shape of what arrives, so a
            reason that is both would be normalized by whichever branch happened to read it.
            """

            refute encoded =~ path
        end

        refute encoded =~ "Do the thing", "the bytes are the one thing a failure about the bytes must not carry"
        refute encoded =~ ctx.root
        refute encoded =~ "prompts/"
      end

      assert MapSet.new(produced, fn {_label, reason} -> elem(reason, 0) end) == MapSet.new(@reasons), """
      The closed set and the set the code reaches are one set or they are neither. A
      reason listed here and never produced is a runbook entry for a morning that cannot
      happen; a reason produced and never listed is the morning with no runbook. This is
      the assertion the helper above is long for: the families are produced, not recalled.
      """

      # The reasons are asserted complete; the classes are not, and deliberately. A seam
      # reason's classes are the operating system's `errno` values, and which of the
      # twenty-seven a run reaches is a fact about the kernel it ran on rather than about
      # this module -- a completeness assertion over them would fail on a host that never
      # returns `:edquot` and would be right to. Soundness is the half this module owes and
      # it is asserted on every pair above: what the store emits is a pair the table admits.
      # The table's own arms and classes are pinned against an independently written literal
      # in `AiOrchestrator.Contracts.PromptRejectionTest`.

      path_carrying =
        produced
        |> Enum.filter(fn {_label, reason} -> is_binary(elem(reason, 1)) end)
        |> Enum.map(fn {_label, reason} -> elem(reason, 0) end)
        |> Enum.uniq()
        |> Enum.sort()

      assert path_carrying == [:prompt_object_conflict, :prompt_object_missing], """
      These two name an object that is on disk and wrong, and the committed relative path
      is what sends an operator to it. Every other rejection classifies instead. Pinning
      the exception as a list rather than as a rule is what keeps it an exception: the
      next reason that wants to carry text has to change this line to do it.
      """
    end

    test "no seam failure carries an absolute path or the prompt itself", ctx do
      for {op, injection, fault, _post} <- seam_faults() do
        {case_root, {:error, reason}} = failing_put(ctx.root, op, injection, fault)

        text = inspect(reason, limit: :infinity, printable_limit: :infinity)

        refute text =~ case_root, """
        `#{inspect(op)}` reported `#{text}`. An absolute path is a payload: it names the
        operator's filesystem layout, and it is the thing that makes an error message
        unusable as an artifact anyone may read.
        """

        refute text =~ "Do the thing", "the bytes are the one thing a failure about the bytes must not contain"
      end
    end

    test "an invalid id is classified, never echoed back", ctx do
      for {id, _class} <- @invalid_ids, id =~ "0001" do
        assert {:error, reason} = PromptStore.put(ctx.fs, ctx.root, id, wrapped(@bytes))

        text = inspect(reason, limit: :infinity, printable_limit: :infinity)

        refute text =~ "0001", """
        `#{inspect(id)}` came from a caller. Quoting it back puts attacker-chosen bytes --
        a traversal, a null byte, anything at all -- into the journal and the log line
        that records the rejection. The class is the whole of what an operator needs.
        """
      end
    end
  end

  describe "a legacy-name publication (scheme 1) is the same operation under another name" do
    test "scheme 1 publishes create-only at the bare legacy name as a version 1 object", ctx do
      assert {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes), scheme: 1)

      assert object.version == 1
      assert object.path == "prompts/#{@assignment}.org"
      assert mode(Path.join(ctx.root, object.path)) == 0o600
      assert File.read!(Path.join(ctx.root, object.path)) == @bytes
      assert {:ok, sensitive} = PromptStore.fetch_verified(ctx.fs, ctx.root, object)
      assert SensitiveBytes.reveal(sensitive) == @bytes
    end

    # Ruling A asked for the eexist behaviour to be stated: a concurrent resume that
    # restored the same legacy object between this run's lstat and its link meets eexist at
    # the link, and the store answers as it does for every publication -- the bytes on
    # disk are compared to the bytes in hand, an identical object is reconciled and returned
    # unchanged, a different one is a conflict. Nothing unverified is ever published over.
    test "two identical scheme 1 publications are one object, and the second one rewrites nothing", ctx do
      assert {:ok, first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes), scheme: 1)
      path = Path.join(ctx.root, first.path)
      %{mtime: mtime} = File.stat!(path, time: :posix)

      assert {:ok, second} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes), scheme: 1)

      assert second == first
      assert File.read!(path) == @bytes
      assert File.stat!(path, time: :posix).mtime == mtime, "reconciliation reads; it never rewrites the object"
    end

    test "a different render at the legacy name is a conflict, and the bytes there are left alone", ctx do
      assert {:ok, first} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes), scheme: 1)

      assert {:error, {:prompt_object_conflict, path}} =
               PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@other_bytes), scheme: 1)

      assert path == first.path
      assert File.read!(Path.join(ctx.root, first.path)) == @bytes
    end
  end

  describe "a resumed send fetches the committed bytes and verifies them" do
    setup ctx do
      {:ok, object} = PromptStore.put(ctx.fs, ctx.root, @assignment, wrapped(@bytes))
      {:ok, object: object}
    end

    test "the committed object is returned verbatim, and still wrapped", ctx do
      assert {:ok, sensitive} = PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)

      assert is_struct(sensitive, SensitiveBytes), """
      Unwrapping at the store's edge puts a bare prompt back into every caller above
      it -- the host, the effect executor, a test stub's mailbox -- which is the exact
      distance the wrapper exists to cover. The bytes are revealed once, at the
      adapter that writes them.
      """

      assert SensitiveBytes.class(sensitive) == :prompt
      assert SensitiveBytes.reveal(sensitive) == @bytes
    end

    test "a missing object is named as missing, never re-rendered", ctx do
      File.rm!(Path.join(ctx.root, ctx.object.path))

      assert {:error, {:prompt_object_missing, path}} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)

      assert path == ctx.object.path, """
      This is the one reason that carries a path, and it is the *contained relative*
      path the journal already committed -- safe metadata by the ruling, and the only
      thing that tells an operator which object went missing. A path that failed
      containment is a different thing: it came from the caller, so it is reported as
      a class.
      """
    end

    test "bytes that do not hash to the committed digest are refused", ctx do
      File.write!(Path.join(ctx.root, ctx.object.path), @other_bytes)

      assert {:error, {:prompt_hash_mismatch, :digest} = reason} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~
               Base.encode16(:crypto.hash(:sha256, @other_bytes), case: :lower),
             """
             The committed digest is already in the caller's hand -- it is the object it passed
             in -- and the digest of what is on disk is a fact about damaged bytes that an
             operator learns by looking at the file. Carrying either one here would put a third
             element into a rejection whose other two are the closed sets a consumer branches
             on, and a value in that position is a value the normalization boundary either
             publishes or silently drops. There is no third outcome, which is why the mismatch
             names itself and stops.
             """
    end

    test "a size that disagrees with the journal is refused before the digest is trusted", ctx do
      claim = %{ctx.object | byte_size: ctx.object.byte_size + 1}

      assert {:error, {:prompt_size_mismatch, :byte_size}} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, claim)
    end

    test "an absolute path is refused without being read", ctx do
      claim = %{ctx.object | path: Path.join(ctx.root, ctx.object.path)}

      assert {:error, {:prompt_path_escapes_root, :absolute} = reason} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, claim)

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ ctx.root, """
      The path came from outside and the answer goes to a journal, a protocol reply or
      a log. Echoing it turns a containment decision into a disclosure of both the
      operator's layout and whatever the caller chose to send.
      """
    end

    test "a traversing path is refused without being read", ctx do
      claim = %{ctx.object | path: "prompts/../../etc/passwd"}

      assert {:error, {:prompt_path_escapes_root, :traversal} = reason} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, claim)

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ "passwd"

      reads = ctx.fs |> FaultFs.trace() |> Enum.filter(&(elem(&1, 0) == :read))

      refute Enum.any?(reads, &(elem(&1, 1) == "passwd")),
             "containment is a decision about the name, taken before the name is used"
    end

    # M2 (Codex review of 8cb72c5): the parent is judged before the object is read.
    test "a symlinked prompt directory is refused before anything under it is read", ctx do
      File.rename!(Path.join(ctx.root, "prompts"), Path.join(ctx.root, "moved"))
      File.ln_s!(Path.join(ctx.root, "moved"), Path.join(ctx.root, "prompts"))
      reads_before = count(ctx.fs, :read)

      assert {:error, {:prompt_dir_not_directory, :symlink}} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)

      assert count(ctx.fs, :read) == reads_before, """
      A symlink standing where `prompts/` should be resolves the object's own name to
      somewhere outside the root, and every check on the object -- type, links, mode,
      digest -- would then pass against a file the store never wrote. The verdict on the
      parent has to come first, and it has to come before a single byte is read.
      """
    end

    test "a regular file standing in for the prompt directory is refused on the read path too", ctx do
      File.rm_rf!(Path.join(ctx.root, "prompts"))
      File.write!(Path.join(ctx.root, "prompts"), "not a directory")

      assert {:error, {:prompt_dir_not_directory, :regular}} =
               PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)
    end

    test "a wide prompt directory is narrowed on the read path exactly as on the write path", ctx do
      prompts = Path.join(ctx.root, "prompts")
      File.chmod!(prompts, 0o755)

      assert {:ok, sensitive} = PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)
      assert SensitiveBytes.reveal(sensitive) == @bytes
      assert mode(prompts) == 0o700

      trace = FaultFs.trace(ctx.fs)

      assert at(trace, &(&1 == {:chmod, "prompts", 0o700})) < at(trace, &match?({:read, _name}, &1)),
             "the directory is the entry the store repairs, and the repair precedes the read"
    end

    test "an absent prompt directory is an absent object, which licenses a legacy restore", ctx do
      File.rm_rf!(Path.join(ctx.root, "prompts"))

      assert {:error, {:prompt_object_missing, path}} = PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object)
      assert path == ctx.object.path
    end

    test "an unreadable object is a fault, not an absence", ctx do
      FaultFs.inject(ctx.fs, :read, count(ctx.fs, :read) + 1, {:error, :eacces})

      assert PromptStore.fetch_verified(ctx.fs, ctx.root, ctx.object) ==
               {:error, {:prompt_object_unreadable, :eacces}},
             "absence licenses a v1 migration re-render; a read fault licenses nothing"
    end
  end

  # ----- helpers -----

  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))

  defp index_of(ops, op), do: Enum.find_index(ops, &(&1 == op))

  # Ordering is asserted over the traced call *and its arguments*, because two calls to the
  # same operation on different directories are two different guarantees.
  defp at(trace, matcher) do
    index = Enum.find_index(trace, matcher)
    assert index != nil, "no traced operation matched; the trace was #{inspect(trace)}"
    index
  end

  # `{operation, injection, fault, post_state}`. `chmod` and `dir_sync` each happen twice
  # in one publication, so those two are targeted by matcher rather than by ordinal: an
  # ordinal that silently starts landing on the other call is a test that keeps passing
  # while asserting something else.
  defp seam_faults do
    [
      {:mkdir, 1, {:error, :eacces}, :nothing},
      {:chmod, &match?(["prompts", _mode], &1), {:error, :eperm}, :nothing},
      {:open, 1, {:error, :emfile}, :nothing},
      {:write, 1, {:error, :enospc}, :nothing},
      {:sync, 1, {:error, :eio}, :nothing},
      {:close, 1, {:error, :eio}, :nothing},
      {:link, 1, {:error, :eacces}, :nothing},
      {:rm, 1, {:error, :eperm}, :temporary_retained},
      {:dir_sync, &match?(["prompts"], &1), {:error, :eio}, :object_published}
    ]
  end

  # One root per case. Sharing a root lets an earlier failure leave a directory, an
  # object or a temporary that changes which call a later case's fault lands on, and
  # then neither the closed-set result nor the post-state means anything per operation.
  defp failing_put(root, op, injection, fault), do: failing_put(root, op, op, injection, fault)

  defp failing_put(root, label, op, injection, fault) do
    case_root = Path.join(root, "case-#{label}")
    File.mkdir_p!(case_root)

    fs = FaultFs.new()
    FaultFs.inject(fs, op, injection, fault)

    {case_root, PromptStore.put(fs, case_root, @assignment, wrapped(@bytes))}
  end

  defp assert_post_state(root, op, :nothing) do
    assert objects(root) == [], "`#{inspect(op)}` failed and published an object anyway"
    assert leftovers(root) == [], "`#{inspect(op)}` failed and left a temporary in the prompt directory"
  end

  defp assert_post_state(root, op, :object_published) do
    assert objects(root) != [],
           "`#{inspect(op)}` fails after the link lands; what is unproven is that the entry survives a crash"

    assert leftovers(root) == [], "`#{inspect(op)}` failed after the temporary was already gone"
  end

  defp assert_post_state(root, op, :temporary_retained) do
    assert leftovers(root) != [],
           "`#{inspect(op)}` is reported precisely because the temporary survived; an empty directory would mean it did not"
  end

  defp wrapped(bytes), do: SensitiveBytes.new(bytes, :prompt)

  # Every rejection the store can produce, produced by producing it. A literal list would
  # be a list of the families someone remembered, and the exhaustiveness assertion above
  # is what makes the difference worth the length: a family added to `@reasons` and never
  # reached fails there, and so does a family reached and never listed.
  defp every_rejection(root), do: seam_rejections(root) ++ object_rejections(root)

  defp seam_rejections(root) do
    faults =
      Enum.map(seam_faults(), fn {op, injection, fault, _post} -> {op, op, injection, fault} end) ++
        [
          # Two seam failures `seam_faults/0` cannot express, because it keys its injections
          # by operation and these two share an operation with a row already in it: the run
          # root's fsync is a `dir_sync` like the prompt directory's, and the object's chmod
          # is a `chmod` like the directory's. Telling them apart is the whole of what
          # `prompt_dir_sync_failed` and `prompt_chmod_failed` are for.
          {:dir_sync_root, :dir_sync, &match?(["case-dir_sync_root"], &1), {:error, :eio}},
          {:chmod_object, :chmod, fn [_name, mode] -> mode == 0o600 end, {:error, :eperm}}
        ]

    for {label, op, injection, fault} <- faults do
      {_case_root, {:error, reason}} = failing_put(root, label, op, injection, fault)
      {label, reason}
    end
  end

  defp object_rejections(root) do
    [
      in_case(root, :assignment_id, fn r, fs -> PromptStore.put(fs, r, "", wrapped(@bytes)) end),
      in_case(root, :dir_not_directory, fn r, fs ->
        File.write!(Path.join(r, "prompts"), "not a directory\n")
        PromptStore.put(fs, r, @assignment, wrapped(@bytes))
      end),
      in_case(root, :conflict, fn r, fs ->
        object = published(r)
        File.write!(Path.join(r, object.path), @other_bytes)
        PromptStore.put(fs, r, @assignment, wrapped(@bytes))
      end),
      in_case(root, :missing, fn r, fs ->
        object = published(r)
        File.rm!(Path.join(r, object.path))
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :not_regular, fn r, fs ->
        object = published(r)
        File.rm!(Path.join(r, object.path))
        File.mkdir!(Path.join(r, object.path))
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :unreadable, fn r, fs ->
        object = published(r)
        FaultFs.inject(fs, :read, 1, {:error, :eacces})
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :hash_mismatch, fn r, fs ->
        object = published(r)
        File.write!(Path.join(r, object.path), @other_bytes)
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :size_mismatch, fn r, fs ->
        object = published(r)
        PromptStore.fetch_verified(fs, r, %{object | byte_size: object.byte_size + 1})
      end),
      in_case(root, :path_escapes_root, fn r, fs ->
        object = published(r)
        PromptStore.fetch_verified(fs, r, %{object | path: Path.join(r, object.path)})
      end),
      in_case(root, :mode_unexpected, fn r, fs ->
        object = published(r)
        File.chmod!(Path.join(r, object.path), 0o644)
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :multiply_linked, fn r, fs ->
        object = published(r)
        second = Path.join(r, "second-name.org")
        :ok = :file.make_link(String.to_charlist(Path.join(r, object.path)), String.to_charlist(second))
        PromptStore.fetch_verified(fs, r, object)
      end),
      in_case(root, :object_assignment_id, fn r, fs ->
        object = published(r)
        PromptStore.fetch_verified(fs, r, forged_id(object, "as 0001"))
      end)
    ] ++
      for {label, override} <- [
            {:name_mismatch, %{version: 1}},
            {:hash_malformed, %{hash: String.duplicate("c", 64)}},
            {:version_unsupported, %{version: 3}},
            {:object_byte_size, %{byte_size: -1}},
            {:object_incomplete, %{hash: nil}}
          ] do
        in_case(root, label, fn r, fs ->
          PromptStore.fetch_verified(fs, r, Map.merge(published(r), override))
        end)
      end
  end

  # One root per case, for the reason `failing_put/5` has one: a case that leaves a
  # directory, an object or a temporary behind changes which entry the next case finds,
  # and then neither its rejection nor its class is a fact about the case that produced it.
  defp in_case(root, label, fun) do
    case_root = Path.join(root, "case-#{label}")
    File.mkdir_p!(case_root)

    {:error, reason} = fun.(case_root, FaultFs.new())
    {label, reason}
  end

  defp published(root) do
    {:ok, object} = PromptStore.put(FaultFs.new(), root, @assignment, wrapped(@bytes))
    object
  end

  # The path is re-derived from the bad id rather than left naming the good one, so the
  # object is internally consistent and the id rule is the only rule it breaks. An object
  # that is *also* name-mismatched proves nothing about which check runs first, which is
  # what the ordering test states on its own.
  defp forged_id(object, id) do
    "sha256:" <> digest = object.hash
    %{object | assignment_id: id, path: "prompts/#{id}-#{digest}.org"}
  end

  defp objects(root) do
    root
    |> Path.join("prompts")
    |> File.ls()
    |> case do
      {:ok, names} -> names |> Enum.filter(&String.ends_with?(&1, ".org")) |> Enum.sort()
      {:error, :enoent} -> []
    end
  end

  defp leftovers(root) do
    root
    |> Path.join("prompts")
    |> File.ls()
    |> case do
      {:ok, names} -> names |> Enum.reject(&String.ends_with?(&1, ".org")) |> Enum.sort()
      {:error, :enoent} -> []
    end
  end

  defp mode(path) do
    %File.Stat{mode: mode} = File.stat!(path)
    Bitwise.band(mode, 0o777)
  end
end
