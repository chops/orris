defmodule AiOrchestrator.Contract.PromptObject do
  @moduledoc """
  A durable prompt object: which assignment it belongs to, where the bytes are,
  what they hash to, how many there are, and which naming scheme produced the
  name.

  This is the closed shape that `Effect.FetchPrompt` and
  `Observation.PromptRetained` carry between the reducer and the host. It is
  deliberately not a bare map: an open map lets a caller pass a decoded journal
  fragment straight through, and then the fields the store verifies against are
  whatever happened to be in the journal rather than what the contract requires.

  ## One object names one assignment

  `assignment_id` lives inside the object rather than beside it. An effect that
  carried both an id and an object would be free to pair `as_0001` with an
  object naming `as_0002`, and a store that verified only containment and bytes
  would serve the other assignment's prompt: the path is inside `prompts/` and
  the digest matches the bytes it names, so both checks pass. Keeping the id in
  the object removes the pairing, and therefore the drift.

  ## `path` is derived, not supplied

  `assignment_id`, `hash` and `version` are authoritative. `path` is a function
  of them and MUST equal that function exactly:

      1 -> "prompts/" <> assignment_id <> ".org"
      2 -> "prompts/" <> assignment_id <> "-" <> hex <> ".org"

  where `hex` is the 64 lowercase hex characters of `hash`, which is written
  `"sha256:" <> hex`. `version` is the naming scheme, not the event version. A
  `1` object is what journals written before prompt retention projected; a `2`
  object is immutable by construction. Retention only ever produces `2`; `1`
  exists so a legacy projection can be made true in place exactly once.

  Every consumer checks that derivation, and checks it *before* it touches the
  filesystem: a rejection that arrives after an `lstat` or a `read` has already
  told an attacker whether the substituted path exists.

  `new/1` is the checked constructor and the only supported way to build an
  object from untrusted input -- a decoded journal fragment, a resumed
  projection, an operator-supplied name. It returns `{:ok, t}` or a named
  rejection, and it never returns an object whose fields disagree. A bare
  `%PromptObject{}` literal stays constructible so a test can forge an
  inconsistent one and prove that the consumer refuses it.

  `verify/1` is the same check applied to an object a consumer was handed rather
  than built. The two exist separately because the store receives a
  `%PromptObject{}` out of an effect, not a map, and the effect crossed a
  process boundary where nothing re-ran the constructor.

  ## Keys arrive from a decoder, so they are matched, never converted

  `new/1` accepts a map whose keys are the five field names as atoms or as
  strings, and it decides which field a key names by matching it against that
  closed set. It never calls `String.to_atom/1` or `String.to_existing_atom/1`
  on a key, and a key outside the set grows no atom: the atom table is a fixed
  BEAM resource that is never reclaimed, so a constructor reachable from a
  decoded journal that converts before it checks turns a malformed file into a
  node that stops scheduling. This is an availability invariant, not input
  hygiene, and it is the reason the unknown-key rejection is a refusal rather
  than a conversion that happens to fail.

  A map that names one field twice -- once by atom and once by string -- is
  refused rather than resolved. There is no correct winner to pick: the two
  values disagree about a field the whole contract is built to make
  authoritative, and a rule that silently keeps one is a rule that decides which
  `hash` a store verifies against based on map iteration order.

  `assignment_id` becomes a filename component, so it is bounded by an explicit
  grammar -- `[A-Za-z0-9_.-]`, one to sixty-four bytes -- before `path` is
  derived from it. The bound is checked first because a derivation is not a
  check: an id of any length produces a `path` that agrees with it, and an
  object whose fields agree perfectly still cannot be written on a filesystem
  that will not hold the name.

  ## Rejections

  A rejection is a `{reason, class}` pair of atoms, in the same grammar and the
  same closed set discipline `PromptStore` uses. The pair is what a reducer
  branches on, so two different inconsistencies never arrive under one name. The
  class never carries the offending value: the refused field may be the prompt.

  The pair is the in-process shape and it is not the wire shape. `Jason.Encoder`
  is not implemented for tuples, so a rejection carried unchanged into a journal
  event or a protocol reply is an encoder crash at the moment something has
  already gone wrong. Both names are drawn from this release's own closed set,
  so `Diagnostic.describe_rejection/1` reflects them into
  `%{"reason" => .., "class" => ..}`, and that map -- not this pair -- is what
  the journal, a protocol reply and a diagnostic hold.

      {:prompt_object_name_mismatch, :assignment | :digest | :scheme}
      {:prompt_object_hash_malformed, :algorithm | :digest}
      {:prompt_object_version_unsupported, :unknown}
      {:prompt_object_assignment_id_invalid, :empty | :separator | :dot_name | :byte | :too_long}
      {:prompt_object_byte_size_invalid, :negative | :not_an_integer}
      {:prompt_object_incomplete, :assignment_id | :path | :hash | :byte_size | :version}
      {:prompt_object_unknown_field, :unknown}
      {:prompt_object_duplicate_field, :alias}

  `:prompt_object_incomplete` names the missing field and
  `:prompt_object_unknown_field` does not, because the missing field's name is
  drawn from this module's own closed set and the unknown key's name is drawn
  from whoever wrote the map. Both the unknown key and the assignment id are
  caller-controlled, so neither is echoed: a rule that decides case by case
  which caller-supplied string is safe to repeat is a rule that will eventually
  decide wrong, and the same closed pair is what makes the two reasons
  branchable at all.

  The missing field is spelled out as those five atoms rather than as `atom()`,
  because a class typed `atom()` is not a closed set. The module states above
  that the vocabulary is closed; a rejection whose second element is any atom at
  all reopens it, and a consumer branching on the pair has nothing to be
  exhaustive against.

  A field that is present and `nil` is missing. A struct always has all five
  keys, so `Map.has_key?/2` answers `true` for a forged object that filled none
  of them, and presence is not the question a consumer needs answered when the
  struct arrived over a process boundary that ran no constructor.

  No payload: `hash` and `byte_size` are facts about the bytes and `path` is
  relative to the run directory.
  """

  @enforce_keys [:assignment_id, :path, :hash, :byte_size, :version]
  defstruct [:assignment_id, :path, :hash, :byte_size, :version]

  @type version :: 1 | 2
  @type t :: %__MODULE__{
          assignment_id: String.t(),
          path: String.t(),
          hash: String.t(),
          byte_size: non_neg_integer(),
          version: version()
        }

  @type rejection ::
          {:prompt_object_name_mismatch, :assignment | :digest | :scheme}
          | {:prompt_object_hash_malformed, :algorithm | :digest}
          | {:prompt_object_version_unsupported, :unknown}
          | {:prompt_object_assignment_id_invalid, :empty | :separator | :dot_name | :byte | :too_long}
          | {:prompt_object_byte_size_invalid, :negative | :not_an_integer}
          | {:prompt_object_incomplete, :assignment_id | :path | :hash | :byte_size | :version}
          | {:prompt_object_unknown_field, :unknown}
          | {:prompt_object_duplicate_field, :alias}

  @type assignment_id_class :: :empty | :separator | :dot_name | :byte | :too_long

  @fields [:assignment_id, :path, :hash, :byte_size, :version]
  @aliases Map.new(@fields, fn field -> {Atom.to_string(field), field} end)
  @id_max_bytes 64
  @dir "prompts/"
  @ext ".org"

  @doc """
  Build a checked object from a map whose keys are the five field names, as atoms or
  as strings.

  Returns `{:ok, t}` or a named rejection. It never returns an object whose fields
  disagree, and it grows no atom from a key it was handed.
  """
  @spec new(map()) :: {:ok, t()} | {:error, rejection()}
  def new(fields) when is_map(fields) do
    with {:ok, resolved} <- resolve(fields),
         {:ok, complete} <- complete(resolved),
         :ok <- check(complete) do
      {:ok, struct!(__MODULE__, complete)}
    end
  end

  @doc """
  Apply the same checks to an object a consumer was handed rather than built.

  A bare `%PromptObject{}` literal stays constructible and the effect that carries one
  crosses a process boundary where nothing re-runs `new/1`, so the shapes a consumer can
  receive include ones the constructor could never return. The two constructor-only
  rejections -- an unknown field and a duplicate field -- are unreachable from a struct,
  which has exactly the five keys and no others.
  """
  @spec verify(t()) :: :ok | {:error, rejection()}
  def verify(%__MODULE__{} = object) do
    with {:ok, complete} <- complete(Map.from_struct(object)) do
      check(complete)
    end
  end

  @doc """
  Classify an assignment id against the grammar that bounds it as a filename component.

  Public because it is one rule with two doors: this module refuses a bad id inside an
  object as `:prompt_object_assignment_id_invalid`, and `PromptStore` refuses the same id
  in a request as `:prompt_assignment_id_invalid`. The reasons differ because a bad id in
  a request and a bad id in an object that already exists are different situations; the
  class is what says why, so the class comes from here and there is nothing to drift.

  A term that is not a binary is refused as `:byte`: the grammar is a statement about
  bytes, and a term that has none cannot satisfy it.
  """
  @spec classify_assignment_id(term()) :: :ok | {:error, assignment_id_class()}
  def classify_assignment_id(id) when is_binary(id) do
    cond do
      id == "" -> {:error, :empty}
      String.contains?(id, "/") -> {:error, :separator}
      id in [".", ".."] -> {:error, :dot_name}
      not grammatical?(id) -> {:error, :byte}
      byte_size(id) > @id_max_bytes -> {:error, :too_long}
      true -> :ok
    end
  end

  def classify_assignment_id(_id), do: {:error, :byte}

  @doc """
  The inclusive upper bound, in bytes, on an assignment id used as a filename component.
  """
  @spec id_max_bytes() :: pos_integer()
  def id_max_bytes, do: @id_max_bytes

  # A key names a field or it names nothing. `Map.fetch/2` against a table built at
  # compile time is the whole mechanism: no `String.to_atom/1`, no
  # `String.to_existing_atom/1`, and so no key from a decoded journal can add to a table
  # the BEAM never reclaims.
  defp field_for(key) when is_atom(key) do
    if key in @fields, do: {:ok, key}, else: :error
  end

  defp field_for(key) when is_binary(key), do: Map.fetch(@aliases, key)
  defp field_for(_key), do: :error

  # Unknown keys are decided across the whole map before duplicates are, so a map that
  # breaks both rules gets one answer rather than an answer that depends on the order a
  # map happened to enumerate in.
  defp resolve(fields) do
    resolved = for {key, value} <- fields, do: {field_for(key), value}

    cond do
      Enum.any?(resolved, &match?({:error, _}, &1)) ->
        {:error, {:prompt_object_unknown_field, :unknown}}

      duplicated?(resolved) ->
        {:error, {:prompt_object_duplicate_field, :alias}}

      true ->
        {:ok, Map.new(resolved, fn {{:ok, field}, value} -> {field, value} end)}
    end
  end

  defp duplicated?(resolved) do
    named = for {{:ok, field}, _value} <- resolved, do: field
    length(named) != length(Enum.uniq(named))
  end

  # Absent and present-but-nil are the same answer on purpose: a struct always has all
  # five keys, so `Map.has_key?/2` is not the question a consumer needs answered when the
  # object arrived over a boundary that ran no constructor.
  defp complete(resolved) do
    case Enum.find(@fields, fn field -> is_nil(Map.get(resolved, field)) end) do
      nil -> {:ok, resolved}
      field -> {:error, {:prompt_object_incomplete, field}}
    end
  end

  # The id is judged as an id before the name is judged against it. A bad id usually
  # arrives with a path that does not derive from it, and `name_mismatch` would send an
  # operator looking for the object this one was confused with -- when no such object
  # exists and the id is the rule that is broken.
  defp check(%{assignment_id: id, path: path, hash: hash, byte_size: size, version: version}) do
    with :ok <- checked_id(id),
         :ok <- checked_hash(hash),
         :ok <- checked_version(version),
         :ok <- checked_byte_size(size) do
      checked_name(id, path, hash, version)
    end
  end

  defp checked_id(id) do
    case classify_assignment_id(id) do
      :ok -> :ok
      {:error, class} -> {:error, {:prompt_object_assignment_id_invalid, class}}
    end
  end

  defp checked_hash("sha256:" <> digest) when byte_size(digest) == 64 do
    if lower_hex?(digest), do: :ok, else: {:error, {:prompt_object_hash_malformed, :digest}}
  end

  defp checked_hash("sha256:" <> _digest), do: {:error, {:prompt_object_hash_malformed, :digest}}
  defp checked_hash(_hash), do: {:error, {:prompt_object_hash_malformed, :algorithm}}

  defp checked_version(version) when version in [1, 2], do: :ok
  defp checked_version(_version), do: {:error, {:prompt_object_version_unsupported, :unknown}}

  defp checked_byte_size(size) when is_integer(size) and size >= 0, do: :ok
  defp checked_byte_size(size) when is_integer(size), do: {:error, {:prompt_object_byte_size_invalid, :negative}}
  defp checked_byte_size(_size), do: {:error, {:prompt_object_byte_size_invalid, :not_an_integer}}

  # The hash has already been checked, so the digest is there to be taken.
  defp checked_name(id, path, "sha256:" <> digest, version) do
    if path == derive(id, digest, version) do
      :ok
    else
      {:error, {:prompt_object_name_mismatch, classify_name(id, path, digest, version)}}
    end
  end

  defp derive(id, _digest, 1), do: @dir <> id <> @ext
  defp derive(id, digest, 2), do: @dir <> id <> "-" <> digest <> @ext

  # Three classes, and which one is reported is what a reducer branches on. `:scheme` is
  # the name built by the other naming scheme, or built nowhere near the prompt
  # directory; `:assignment` is this scheme's name carrying someone else's assignment;
  # `:digest` is this assignment's name carrying the wrong content address.
  defp classify_name(id, path, digest, version) do
    case inner(path) do
      :error -> :scheme
      {:ok, inner} -> classify_inner(id, inner, digest, version)
    end
  end

  defp classify_inner(id, inner, digest, 2) do
    if inner == id do
      :scheme
    else
      case split_last(inner, "-") do
        :error -> :scheme
        {:ok, ^id, ^digest} -> :scheme
        {:ok, ^id, _other} -> :digest
        {:ok, _other, _tail} -> :assignment
      end
    end
  end

  defp classify_inner(id, inner, _digest, 1) do
    case split_last(inner, "-") do
      {:ok, ^id, _tail} -> :scheme
      _other -> :assignment
    end
  end

  # A path outside `prompts/`, or not ending in the extension, is not this scheme's name
  # at all and there is no inner name to compare.
  defp inner(path) when is_binary(path) do
    if String.starts_with?(path, @dir) and String.ends_with?(path, @ext) and
         byte_size(path) >= byte_size(@dir) + byte_size(@ext) do
      {:ok, binary_part(path, byte_size(@dir), byte_size(path) - byte_size(@dir) - byte_size(@ext))}
    else
      :error
    end
  end

  defp inner(_path), do: :error

  # Split at the last occurrence, because the id's own grammar admits the separator and
  # the digest's does not.
  defp split_last(subject, separator) do
    case :binary.matches(subject, separator) do
      [] ->
        :error

      matches ->
        {at, length} = List.last(matches)

        {:ok, binary_part(subject, 0, at), binary_part(subject, at + length, byte_size(subject) - at - length)}
    end
  end

  defp grammatical?(id) do
    id |> :binary.bin_to_list() |> Enum.all?(&allowed_byte?/1)
  end

  defp allowed_byte?(byte) when byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in [?_, ?., ?-], do: true

  defp allowed_byte?(_byte), do: false

  defp lower_hex?(digest) do
    digest |> :binary.bin_to_list() |> Enum.all?(&(&1 in ?0..?9 or &1 in ?a..?f))
  end
end
