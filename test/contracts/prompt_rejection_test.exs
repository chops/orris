defmodule AiOrchestrator.Contracts.PromptRejectionTest do
  @moduledoc """
  The pin on the prompt rejection table.

  `PromptRejection` is the sole production source of which rejections exist, so this file
  does not import it to decide what to expect. The snapshot below is written out by hand
  from the producers -- `PromptStore`'s own contract, `PromptObject.new/1`, and the
  `@invalid_ids` table -- and compared to the runtime table for exact equality. Deriving
  the expectation from the table would let a hand edit to the table and a hand edit to
  this file travel together, which is the one failure a snapshot exists to catch. Every
  other test in the tree may derive its cases from the table, but only past this check.

  Both the table and this snapshot were written by the same author in adjacent sittings,
  which is precisely the arrangement a snapshot is supposed to rule out. What restores
  the independence is not the authorship but the review: every non-`errno` arm below was
  checked against the RED producer specifications by the reviewing agent and accepted in
  `m_1788538774000000000_6d0f58a1`. That citation, not the order the files were typed in,
  is why these mappings can be trusted.

  The `errno` half is not held that way, because a reviewer reading a list of POSIX atoms
  cannot tell a complete one from a plausible one -- and the first draft of that list was
  short by twenty-one members. So it is not pinned by hand at all. It is extracted from
  the running OTP's own `:file.posix/0` typespec and compared to the production set, in
  "the shared errno set is exactly the pinned runtime's own `:file.posix/0`" below. The
  authority for that half is the runtime, which neither the table nor this file can edit.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.PromptRejection

  # Not written out by hand. A hand-written list of POSIX atoms looks equally right at 27
  # members and at 47, so transcribing one here would pin the production set against a
  # second guess rather than against an authority. `:file.posix/0` in the pinned OTP is
  # the authority: it is what `File`/`:file` declare they return, and `SystemFs` forwards
  # those results unchanged through `Journal.Fs`'s `{:error, term()}` callbacks. Read at
  # runtime, from the typespec chunk, so the set cannot narrow without OTP narrowing.
  @errnos (case Code.Typespec.fetch_types(:file) do
             {:ok, types} ->
               {:type, {:posix, {:type, _, :union, members}, []}} =
                 Enum.find(types, &match?({:type, {:posix, _, []}}, &1))

               Enum.sort(for {:atom, _, atom} <- members, do: atom)

             :error ->
               raise "cannot read :file typespecs from the pinned OTP; the errno pin has no authority"
           end)

  # Every reason and the exact classes it may carry. A reason whose second element is a
  # contained relative path rather than a class is written with an empty list, which is
  # what makes "carries no class" a statement this snapshot can hold rather than an
  # absence a reader has to notice.
  @snapshot [
    # Object-level, from `PromptObject.new/1`.
    {:prompt_object_name_mismatch, [:assignment, :digest, :scheme]},
    {:prompt_object_hash_malformed, [:algorithm, :digest]},
    {:prompt_object_version_unsupported, [:unknown]},
    {:prompt_object_assignment_id_invalid, [:empty, :separator, :dot_name, :byte, :too_long]},
    {:prompt_object_byte_size_invalid, [:negative, :not_an_integer]},
    {:prompt_object_incomplete, [:assignment_id, :path, :hash, :byte_size, :version]},
    {:prompt_object_unknown_field, [:unknown]},
    {:prompt_object_duplicate_field, [:alias]},

    # Store refusals about what was asked for, or about what is on disk.
    {:prompt_assignment_id_invalid, [:empty, :separator, :dot_name, :byte, :too_long]},
    {:prompt_dir_not_directory, [:regular, :symlink]},
    {:prompt_object_not_regular, [:directory, :symlink]},
    {:prompt_object_mode_unexpected, [:narrower, :wider]},
    {:prompt_object_multiply_linked, [:link_count]},
    {:prompt_hash_mismatch, [:digest]},
    {:prompt_size_mismatch, [:byte_size]},
    {:prompt_path_escapes_root, [:absolute, :traversal]},

    # Path-carrying: no class at all.
    {:prompt_object_conflict, []},
    {:prompt_object_missing, []},

    # Seam failures.
    {:prompt_dir_create_failed, @errnos},
    {:prompt_dir_chmod_failed, @errnos},
    {:prompt_dir_sync_failed, @errnos},
    {:prompt_publication_sync_failed, @errnos},
    {:prompt_open_failed, @errnos},
    {:prompt_write_failed, [:torn_write | @errnos]},
    {:prompt_sync_failed, @errnos},
    {:prompt_close_failed, @errnos},
    {:prompt_link_failed, @errnos},
    {:prompt_chmod_failed, @errnos},
    {:prompt_temp_cleanup_failed, @errnos},
    {:prompt_object_unreadable, @errnos}
  ]

  @path_carrying [:prompt_object_conflict, :prompt_object_missing]

  @seam_reasons [
    :prompt_chmod_failed,
    :prompt_close_failed,
    :prompt_dir_chmod_failed,
    :prompt_dir_create_failed,
    :prompt_dir_sync_failed,
    :prompt_link_failed,
    :prompt_object_unreadable,
    :prompt_open_failed,
    :prompt_publication_sync_failed,
    :prompt_sync_failed,
    :prompt_temp_cleanup_failed,
    :prompt_write_failed
  ]

  # The seven pairs a hand-written representative table got wrong before the table
  # existed. Five name a class the reason cannot carry; two offer a class to a reason
  # whose second element is a path. Each is a plausible guess and none is producible, so
  # they are the probes worth keeping: a table that admits any of them has stopped being
  # a table of what happens and gone back to being a list of what someone remembered.
  #
  # Every row here is refused. An earlier draft carried
  # `{:prompt_object_multiply_linked, :link_count}` in this list with an escape hatch that
  # asserted it instead, which made a list of seven refusals actually check six -- and the
  # count in the sentence above was the only place that said so. A probe list that can
  # hold a passing row is not a probe list; the corrected pair is asserted below, on its
  # own, where it is a claim rather than an exception.
  @historical_false_pairs [
    {:prompt_dir_not_directory, :enotdir, "the path is a regular file or a symlink; no syscall reported anything"},
    {:prompt_object_not_regular, :scheme, "`:scheme` belongs to a hash string, not to a directory entry's type"},
    {:prompt_object_mode_unexpected, :unknown, "the mode is known and is reported as `:narrower` or `:wider`"},
    {:prompt_object_multiply_linked, :unknown,
     "the link count is known and is reported as `:link_count`; `:unknown` was the guess"},
    {:prompt_path_escapes_root, :separator, "`:separator` classifies an assignment id, not an escape"},
    {:prompt_object_conflict, :digest, "the conflict carries the committed path; a class here would be a second answer"},
    {:prompt_object_missing, :enoent, "the object is absent by `stat`, and the path is what an operator needs"}
  ]

  test "the table is exactly the vocabulary, reason by reason and class by class" do
    expected = Map.new(@snapshot, fn {reason, classes} -> {reason, Enum.sort(classes)} end)
    actual = Map.new(PromptRejection.reasons(), &{&1, Enum.sort(PromptRejection.classes(&1))})

    assert actual == expected, """
    The runtime table and this hand-written snapshot disagree. One of them is wrong and
    the other is the specification; which is which is the review, not the fix.

      only in the table:    #{inspect(Map.keys(actual) -- Map.keys(expected))}
      only in the snapshot: #{inspect(Map.keys(expected) -- Map.keys(actual))}
      differing classes:    #{inspect(for k <- Map.keys(actual) -- (Map.keys(actual) -- Map.keys(expected)), actual[k] != expected[k], do: {k, actual[k], expected[k]})}
    """
  end

  test "thirty reasons, because the vocabulary is one set and not two halves" do
    assert length(PromptRejection.reasons()) == 30
    assert PromptRejection.reasons() == Enum.sort(Enum.map(@snapshot, &elem(&1, 0)))
  end

  test "the errno set is shared by the seam reasons and reachable from no others" do
    assert Enum.sort(PromptRejection.errnos()) == Enum.sort(@errnos)
    assert PromptRejection.seam_reasons() == @seam_reasons

    for reason <- PromptRejection.reasons(), reason not in @seam_reasons, errno <- @errnos do
      refute PromptRejection.allows?(reason, errno), """
      `#{inspect({reason, errno})}` is admitted. Sharing one `errno` set among the seam
      reasons is deliberate; letting it reach a reason that never performs a syscall is
      how a membership list turns back into a rectangle.
      """
    end

    for reason <- @seam_reasons, errno <- @errnos do
      assert PromptRejection.allows?(reason, errno), "`#{inspect({reason, errno})}` is a seam failure and was refused"
    end
  end

  test "a short write is named in the store's own word, which no errno can say" do
    assert PromptRejection.allows?(:prompt_write_failed, :torn_write)

    for reason <- PromptRejection.reasons(), reason != :prompt_write_failed do
      refute PromptRejection.allows?(reason, :torn_write), """
      `:torn_write` reports a `write` that returned a byte count smaller than the buffer.
      Only the write seam can observe that, so only `prompt_write_failed` may say it.
      """
    end
  end

  test "a path-carrying reason admits no class at all" do
    assert PromptRejection.path_carrying() == @path_carrying

    for reason <- @path_carrying do
      assert PromptRejection.classes(reason) == [], """
      `#{inspect(reason)}` names an object that is on disk and wrong, and carries the
      contained relative path the journal already committed. A class beside the path
      would be a second answer to the question the path already answers.
      """

      for class <- PromptRejection.classes() do
        refute PromptRejection.allows?(reason, class)
      end

      refute PromptRejection.allows?(reason, "prompts/as_0001-#{String.duplicate("a", 64)}.org"), """
      The path is not a class. `allows?/2` answering true here is how it would reach a
      `"class"` slot, and a slot a consumer branches on cannot also hold free text.
      """
    end
  end

  test "every pair a producer can make is admitted, and `pairs/0` is exactly those" do
    expected =
      for {reason, classes} <- Enum.sort(@snapshot),
          class <- Enum.sort(classes),
          do: {reason, class}

    assert PromptRejection.pairs() == expected

    for {reason, class} <- expected do
      assert PromptRejection.allows?(reason, class), "`#{inspect({reason, class})}` is in the table and was refused"
    end
  end

  test "a known reason crossed with a known class of another reason is not a pair" do
    admitted = MapSet.new(PromptRejection.pairs())

    crossed =
      for reason <- PromptRejection.reasons(),
          class <- PromptRejection.classes(),
          not MapSet.member?(admitted, {reason, class}),
          do: {reason, class}

    assert length(crossed) > 1000, """
    The cross product of the two closed sets is far larger than the set of real pairs,
    which is the whole reason the pair is the boundary. A `crossed` list this small means
    one of the two sets has quietly collapsed.
    """

    for {reason, class} <- crossed do
      refute PromptRejection.allows?(reason, class), """
      `#{inspect({reason, class})}` is admitted. Both halves are names this release owns,
      which is exactly what makes the pair plausible enough to be typed by hand and
      impossible to produce.
      """
    end
  end

  test "the seven pairs a hand-written table got wrong stay refused" do
    assert length(@historical_false_pairs) == 7,
           "the test's name counts the rows; a row added or removed has to move the name with it"

    for {reason, class, note} <- @historical_false_pairs do
      refute PromptRejection.allows?(reason, class), "`#{inspect({reason, class})}`: #{note}"
    end
  end

  test "the pair the hand-written table should have named is the one that is real" do
    # The corrected half of the `:prompt_object_multiply_linked` row above. Refusing the
    # guess proves the table is not credulous; admitting the real pair proves it did not
    # buy that by refusing the whole reason. Neither claim is worth much without the other,
    # and asserting an acceptance inside a list of refusals is how the count went wrong.
    assert PromptRejection.allows?(:prompt_object_multiply_linked, :link_count)
  end

  test "the shared errno set is exactly the pinned runtime's own `:file.posix/0`" do
    # The completeness half, which the table-equality test above cannot state on its own:
    # that test proves the table matches `@errnos`, and `@errnos` is the extraction, so a
    # failure there reports a class diff across twelve reasons rather than the one fact a
    # reader needs. This says the fact. It is also the test that fails if a future OTP
    # widens or narrows the union, which is the right time to find out.
    declared = MapSet.new(@errnos)
    actual = MapSet.new(PromptRejection.classes(:prompt_open_failed))

    assert MapSet.equal?(actual, declared), """
    The store's errno vocabulary is not the runtime's.

      OTP #{:erlang.system_info(:otp_release)} declares: #{MapSet.size(declared)}
      the table carries:      #{MapSet.size(actual)}
      declared but missing:   #{inspect(Enum.sort(MapSet.difference(declared, actual)))}
      carried but undeclared: #{inspect(Enum.sort(MapSet.difference(actual, declared)))}

    `Journal.Fs` callbacks return `{:error, term()}` and `SystemFs` forwards `File`/`:file`
    results unchanged, so anything the runtime can return and this set omits leaves the
    normalization open at exactly the seam it claims to close.
    """

    refute :enotempty in @errnos, """
    OTP has started declaring `:enotempty`. The table excludes it on a measurement -- a
    `Fs.rmdir/2` against a non-empty directory answers `{:error, :eexist}`, the file driver
    having already mapped ENOTEMPTY -- and that measurement is now worth repeating rather
    than trusting.
    """
  end

  test "every seam reason carries that same set, and nothing else does" do
    # `@errnos` is shared per seam reason by ruling, so the pin has to be that the sharing
    # is total: one reason quietly given its own subset is the two-lists mistake at the
    # scale where it is hardest to see.
    for reason <- @seam_reasons do
      carried = MapSet.new(PromptRejection.classes(reason))
      extra = if reason == :prompt_write_failed, do: MapSet.new([:torn_write]), else: MapSet.new()

      assert MapSet.equal?(carried, MapSet.union(MapSet.new(@errnos), extra)),
             "`#{reason}` does not carry the shared errno set: " <>
               inspect(Enum.sort(MapSet.difference(MapSet.union(MapSet.new(@errnos), extra), carried)))
    end

    for reason <- PromptRejection.reasons(), reason not in @seam_reasons do
      refute Enum.any?(PromptRejection.classes(reason), &(&1 in @errnos)),
             "`#{reason}` is not a seam reason but carries an errno class"
    end
  end

  test "an unknown reason has no classes and admits nothing" do
    for reason <- [:prompt_object_missing_x, :SECRET_ATOM_TAG, :error, nil, "prompt_open_failed", 42] do
      refute PromptRejection.reason?(reason)
      assert PromptRejection.classes(reason) == []
      refute PromptRejection.allows?(reason, :eacces)
    end
  end

  test "a class that is not an atom is not a class, whatever it holds" do
    for class <- [
          "/srv/runs/run_0001/prompts/as_0001.org",
          "* Assignment\n\nSECRET_PROMPT_BYTES\n",
          %{"path" => "/abs/SECRET"},
          {:nested, "SECRET"},
          123,
          nil
        ] do
      refute PromptRejection.allows?(:prompt_open_failed, class)
      refute PromptRejection.allows?(:prompt_object_unreadable, class)
    end
  end
end
