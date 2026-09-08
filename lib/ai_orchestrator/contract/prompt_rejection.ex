defmodule AiOrchestrator.Contract.PromptRejection do
  @moduledoc """
  The closed rejection vocabulary of the prompt store and the prompt object, as one
  table of reason-to-allowed-classes rather than as two lists of names.

  A rejection is a pair, so the fact worth closing is a pair. Two independent
  membership lists -- one of reasons, one of classes -- cannot state that fact. Their
  product admits `{:prompt_object_duplicate_field, :emfile}`, which no code produces and
  no runbook can act on, and it just as quietly omits a class the store does emit. Both
  happened here: the declared rectangle was simultaneously far too wide and, in nine
  classes, too narrow. A consumer branching on the pair has nothing to be exhaustive
  against when the declared set is a rectangle, and a normalizer gated on one will
  reflect a class this release never chose while dropping one it did.

  The table has three arms, because producers answer in three shapes.

    * Most reasons carry a class from their own small set. `prompt_object_not_regular`
      is `:directory` or `:symlink` -- the two things the path turned out to be instead,
      and a different morning apiece.

    * Twelve reasons name a seam failure and carry the operating system's `errno`. They
      share one set because the operating system does, and the `errno` is the half an
      operator acts on: `prompt_open_failed` is one problem at `:emfile` and another at
      `:eacces`. Sharing is the point, and so is the boundary: the set is reachable from
      these twelve reasons and from no others. `prompt_write_failed` also carries
      `:torn_write`, which is not an `errno` at all -- a short write returns a byte
      count rather than an error, so the store has to say so in its own word.

    * Two reasons carry no class. `prompt_object_conflict` and `prompt_object_missing`
      name an object that is on disk and wrong, and the contained relative path the
      journal already committed is what sends an operator to it. A path is a binary, not
      a member of any closed set, so these two have an empty class set here and
      normalize with `"class" => nil` and no path anywhere in the result. It is an
      exception, and it is pinned as a list rather than derived from a rule so that the
      next reason wanting to carry text has to edit that line to do it.

  What this module is not: it is not a normalizer, a validator, or a place to describe a
  rejection. `Diagnostic.describe_rejection/1` asks `allows?/2` and then reflects or
  drops. This module says only which pairs exist, and says it once.

  The object-level arm is the same set `PromptObject.rejection/0` declares. The unions
  below are derived from the table rather than written beside it, because a hand-written
  union next to a hand-written table is the two-lists mistake again at a different
  altitude. The table is pinned in turn by an independently written literal in
  `AiOrchestrator.Contracts.PromptRejectionTest`, so a table edited by hand and a test
  edited to match cannot drift together.
  """

  alias AiOrchestrator.Contract.FileError

  # The seam reasons' shared vocabulary: the POSIX `errno` values that can reach a caller
  # from the file operations the store performs.
  #
  # It is `FileError.errnos/0`, consumed rather than transcribed. There is one closed
  # file-error vocabulary in this system, and it is `:file.posix()` as the pinned OTP
  # declares it, whole and in the runtime's own order -- not the subset someone expected
  # these operations to produce. `Journal.Fs`'s callbacks return `{:error, term()}` and
  # `SystemFs` forwards whatever `File`/`:file` hands back, so the seam's vocabulary is the
  # runtime's vocabulary; any narrower list is a guess about a kernel, and a legitimate
  # result outside it would fall out of a normalization this module promises is closed.
  # A second copy of that list here, however faithful on the day it was written, is a
  # second place for it to be wrong, and the two would drift silently because nothing
  # compares them. So this module owns no errno list of its own.
  #
  # `:enotempty` is deliberately absent from that vocabulary, and the one store operation
  # that could plausibly raise it does not: `Fs.rmdir/2` on a non-empty directory returns
  # `{:error, :eexist}`, because the Erlang file driver has already mapped ENOTEMPTY. That
  # is measured on a supported platform, not assumed from the type's silence.
  #
  # Pinned twice, independently: `AiOrchestrator.Contracts.FileErrorTest` checks the
  # vocabulary against the running toolchain, and `AiOrchestrator.Contracts.PromptRejectionTest`
  # checks the table's seam arm against its own extraction of the same type. Neither pin
  # is derived from the other, so the list cannot silently narrow again.
  @errnos FileError.errnos()

  # An assignment id is rejected the same way wherever it is checked, so the two reasons
  # that check one share these classes rather than each naming its own near-copy.
  @assignment_id_classes [:empty, :separator, :dot_name, :byte, :too_long]

  @path_carrying [:prompt_object_conflict, :prompt_object_missing]

  @table %{
    # Object-level refusals, mirroring `PromptObject.rejection/0`.
    prompt_object_name_mismatch: [:assignment, :digest, :scheme],
    prompt_object_hash_malformed: [:algorithm, :digest],
    prompt_object_version_unsupported: [:unknown],
    prompt_object_assignment_id_invalid: @assignment_id_classes,
    prompt_object_byte_size_invalid: [:negative, :not_an_integer],
    prompt_object_incomplete: [:assignment_id, :path, :hash, :byte_size, :version],
    prompt_object_unknown_field: [:unknown],
    prompt_object_duplicate_field: [:alias],

    # Store refusals about what was asked for, or about what is on disk.
    prompt_assignment_id_invalid: @assignment_id_classes,
    prompt_dir_not_directory: [:regular, :symlink],
    prompt_object_not_regular: [:directory, :symlink],
    prompt_object_mode_unexpected: [:narrower, :wider],
    prompt_object_multiply_linked: [:link_count],
    prompt_hash_mismatch: [:digest],
    prompt_size_mismatch: [:byte_size],
    prompt_path_escapes_root: [:absolute, :traversal],

    # The two that carry a contained relative path instead of a class.
    prompt_object_conflict: [],
    prompt_object_missing: [],

    # Seam failures. One shared `errno` set, plus the one class an `errno` cannot say.
    prompt_dir_create_failed: @errnos,
    prompt_dir_chmod_failed: @errnos,
    prompt_dir_sync_failed: @errnos,
    prompt_publication_sync_failed: @errnos,
    prompt_open_failed: @errnos,
    prompt_write_failed: [:torn_write | @errnos],
    prompt_sync_failed: @errnos,
    prompt_close_failed: @errnos,
    prompt_link_failed: @errnos,
    prompt_chmod_failed: @errnos,
    prompt_temp_cleanup_failed: @errnos,
    prompt_object_unreadable: @errnos
  }

  @reasons @table |> Map.keys() |> Enum.sort()
  @classes @table |> Map.values() |> List.flatten() |> Enum.uniq() |> Enum.sort()
  @seam_reasons @table
                |> Enum.filter(fn {_reason, classes} -> Enum.any?(classes, &(&1 in @errnos)) end)
                |> Enum.map(&elem(&1, 0))
                |> Enum.sort()
  @pairs for {reason, classes} <- Enum.sort(@table), class <- Enum.sort(classes), do: {reason, class}

  @type reason :: unquote(Enum.reduce(@reasons, &{:|, [], [&1, &2]}))
  @type class :: unquote(Enum.reduce(@classes, &{:|, [], [&1, &2]}))

  @typedoc """
  A rejection as a producer returns it. The path-carrying shape is spelled separately,
  because its second element is a contained relative path and not a member of `class/0`.
  """
  @type t :: {reason(), class()} | {:prompt_object_conflict | :prompt_object_missing, String.t()}

  @doc "Every reason in the closed set."
  @spec reasons() :: [reason()]
  def reasons, do: @reasons

  @doc """
  Every class some reason may carry.

  Membership authorizes nothing on its own; that is what `allows?/2` is for, and gating a
  class on this list rather than on the pair is the defect this module exists to close.
  """
  @spec classes() :: [class()]
  def classes, do: @classes

  @doc "The classes this reason may carry: `[]` for a path-carrying reason and for an unknown one."
  @spec classes(term()) :: [class()]
  def classes(reason) when is_atom(reason), do: Map.get(@table, reason, [])
  def classes(_reason), do: []

  @doc """
  The shared POSIX `errno` set the seam reasons carry.

  It is `FileError.errnos/0`; this delegate exists so a consumer of the table can ask the
  table what its seam vocabulary is without knowing where the vocabulary is owned. The
  binding to `seam_reasons/0` is made by the table, not by this function.
  """
  @spec errnos() :: [class()]
  defdelegate errnos(), to: FileError

  @doc "The reasons that carry an `errno`."
  @spec seam_reasons() :: [reason()]
  def seam_reasons, do: @seam_reasons

  @doc "The two reasons whose second element is a contained relative path rather than a class."
  @spec path_carrying() :: [reason()]
  def path_carrying, do: @path_carrying

  @doc "Every valid pair, sorted. The path-carrying reasons contribute none."
  @spec pairs() :: [{reason(), class()}]
  def pairs, do: @pairs

  @doc "Whether this is a reason of the closed set."
  @spec reason?(term()) :: boolean()
  def reason?(reason), do: is_atom(reason) and is_map_key(@table, reason)

  @doc """
  Whether this reason may carry this class.

  False for every class offered with a path-carrying reason, which is what makes such a
  rejection normalize to its reason and a `nil` class with its path reflected nowhere.
  """
  @spec allows?(term(), term()) :: boolean()
  def allows?(reason, class), do: is_atom(class) and class in classes(reason)
end
