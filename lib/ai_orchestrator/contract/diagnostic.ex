defmodule AiOrchestrator.Contract.Diagnostic do
  @moduledoc """
  Payload-free descriptions for error reports over untrusted values.

  Only the known contract effect and observation structs are described by kind
  and by their typed correlation id; every other term, including a map that
  merely carries a `__struct__` key and a struct from any other module, is
  described by a bare result class. A correlation is emitted only when the
  known field holds a value that passes the identifier grammar and its byte
  bound, so an id carrying adapter bytes is dropped rather than reflected.
  Class names never reflect adapter-controlled atoms. Every description carries
  the full sha256 of the term so two reports can be compared, and none carries
  prompt, command, artifact, provider, or path bytes. Normalization never
  raises, whatever an invalid adapter returns.

  ## A closed rejection is normalized, not described

  `describe_rejection/1` is the second entry point, and it exists because
  `describe/1` gives the right answer to the wrong question when the term is one
  of the product's own rejections. A rejection is a `{reason, class}` pair of
  atoms drawn from a closed set this release owns, so its two names are not
  adapter-controlled and are the whole content an operator reads. Passing one
  through `describe/1` reports `result_class: "tuple"`, which is a true statement
  that has thrown away everything worth journaling.

  The pair itself cannot be the wire shape. `Jason.Encoder` is not implemented
  for tuples, so a rejection carried unchanged into a journal event or a protocol
  reply is an encoder crash at the moment something has already gone wrong. The
  normalization is therefore a map with the two names as strings --
  `%{"reason" => .., "class" => ..}` -- and it is the only shape those three
  destinations hold.

  The gate is what keeps this from being a hole. Only a `reason` in the closed
  set is reflected; every other term, including a two-atom tuple an adapter
  invented that happens to look like a rejection, falls back to the same
  payload-free `result_class`/`digest` description `describe/1` gives. Without
  that gate the function would be a way to get an adapter-controlled atom name
  into a log by wrapping it in a pair, which is the precise thing `describe/1`
  refuses to do.

  The class is gated with its reason, not beside it. An earlier draft of this
  module named two sets -- every reason, every class -- and admitted their
  product. That is a rectangle rather than a closed set: it admits
  `{:prompt_dir_not_directory, :enotdir}`, whose halves are both names this
  release owns and which no producer can emit, and it leaves a consumer branching
  on it nothing to be exhaustive against.
  `AiOrchestrator.Contract.PromptRejection` carries the reason-to-classes table
  instead, and `allows?/2` asks the question two lists cannot: whether this reason
  carries this class.

  A reason is always this release's word. A class is this release's word for an
  object-level refusal and the operating system's `errno` for a seam failure, and
  an operator cannot act on `prompt_open_failed` without knowing whether it was
  `emfile` or `eacces`.

  So there are three answers, not two. A pair the table admits reflects both
  names. A pair whose reason the table names, carrying a class that is not one of
  this release's names anywhere -- an `errno` no release chose, an atom an adapter
  minted, a term that is not an atom at all -- reports its reason with
  `"class" => nil`: the half an alert rule matches survives, and the half nothing
  chose does not. A pair whose halves are both this release's names but which the
  table does not admit together is not a rejection this product can produce at
  all, and it is described rather than reflected -- a hand-written or replayed
  term can reach that shape and a producer cannot, so reflecting it would tell a
  consumer a fact about this run that did not happen.

  The cost of the closed set is real -- an `errno` the table does not name reads
  as `nil` until it names it -- and it is smaller than the cost of a normalizer
  that will print any atom it is handed.

  Two reasons carry no class at all. `prompt_object_conflict` and
  `prompt_object_missing` report a contained relative path, which is what an
  operator needs and what the store already owes its caller. It is also the one
  thing a diagnostic may not hold, so those normalize to their reason with
  `"class" => nil` and the path is reflected nowhere in the result.

  A rejection normalizes to exactly two keys. There is no digest: both halves are
  already reflected in full, so a digest would be a hash of a value the map
  beside it already prints.
  """

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.PromptRejection

  # The grammar and byte bound the journal already applies to actor ids.
  # Anchored absolutely: `$` would match before a trailing newline and reflect it.
  @identifier ~r/\A[A-Za-z0-9_.-]{1,64}\z/
  @identifier_max_bytes 64

  @by_read_index [Effect.Clock, Observation.Clock]
  @by_assignment_id [
    Effect.Dispatch,
    Effect.Observe,
    Effect.ReadReview,
    Observation.Dispatched,
    Observation.DispatchFailed,
    Effect.ReconcileSend,
    Observation.SendReconciled,
    Observation.SendReconcileFailed,
    Effect.SnapshotArtifact,
    Observation.ArtifactSnapshot,
    Observation.ArtifactSnapshotFailed,
    Observation.ArtifactObserved,
    Observation.Blocked,
    Observation.Pending,
    Observation.ObserveFailed,
    Observation.TimedOut,
    Observation.ReviewRead,
    Observation.ReviewUnreadable,
    Effect.RetainPrompt,
    Observation.PromptRetentionFailed,
    Observation.PromptFetchFailed
  ]
  @by_gate_run_id [
    Effect.RunGate,
    Effect.PrepareGate,
    Effect.ReleaseGate,
    Effect.AwaitGate,
    Effect.ReconcileGate,
    Observation.GateFinished,
    Observation.GateFailed,
    Observation.GateError,
    Observation.GatePrepared,
    Observation.GatePrepareFailed,
    Observation.GateReleased,
    Observation.GateReleaseFailed,
    Observation.GateUnsettled,
    Observation.GateReconciled,
    Observation.GateReconcileFailed
  ]
  @by_notification_id [Effect.Notify, Observation.Notified, Observation.NotifyFailed]

  # The assignment these three name is inside the object rather than beside it, which is
  # the whole point of `PromptObject`: a struct that carried both could pair one
  # assignment's id with another's object, and a diagnostic would then report the
  # correlation the pair claimed rather than the one the run acted on.
  @by_object [Effect.FetchPrompt, Observation.PromptRetained, Observation.PromptFetched]
  @uncorrelated [Effect.Timer, Observation.Deadline]
  @known @by_read_index ++
           @by_assignment_id ++ @by_gate_run_id ++ @by_notification_id ++ @by_object ++ @uncorrelated

  @spec describe(term()) :: %{String.t() => String.t() | nil}
  def describe(%{__struct__: module} = value) when is_atom(module) and module in @known do
    %{"kind" => kind(module), "correlation" => correlation(module, value), "digest" => digest(value)}
  end

  def describe(other), do: %{"result_class" => result_class(other), "digest" => digest(other)}

  @spec describe_rejection(term()) :: %{String.t() => String.t() | nil}
  def describe_rejection({reason, class}) when is_atom(reason) do
    cond do
      not PromptRejection.reason?(reason) -> describe({reason, class})
      PromptRejection.allows?(reason, class) -> reflect(reason, class)
      is_atom(class) and class in PromptRejection.classes() -> describe({reason, class})
      true -> reflect(reason, nil)
    end
  end

  def describe_rejection(other), do: describe(other)

  defp reflect(reason, nil), do: %{"reason" => Atom.to_string(reason), "class" => nil}
  defp reflect(reason, class), do: %{"reason" => Atom.to_string(reason), "class" => Atom.to_string(class)}

  defp kind(module), do: module |> Module.split() |> List.last() |> Macro.underscore()

  defp correlation(module, %{read_index: index}) when module in @by_read_index, do: read_index(index)

  defp correlation(module, %{assignment_id: id}) when module in @by_assignment_id, do: identifier(id)
  defp correlation(module, %{gate_run_id: id}) when module in @by_gate_run_id, do: identifier(id)
  defp correlation(module, %{notification_id: id}) when module in @by_notification_id, do: identifier(id)

  defp correlation(module, %{object: %PromptObject{assignment_id: id}}) when module in @by_object, do: identifier(id)

  defp correlation(_module, _value), do: nil

  defp read_index(index) when is_integer(index) and index >= 0, do: Integer.to_string(index)
  defp read_index(_index), do: nil

  # An id that carries anything but identifier bytes is dropped, never reflected.
  defp identifier(value) when is_binary(value) do
    if byte_size(value) <= @identifier_max_bytes and Regex.match?(@identifier, value), do: value
  end

  defp identifier(_value), do: nil

  @result_classes ~w(tuple atom binary map list integer other)

  @doc """
  The closed result-class vocabulary: every class `result_class/1` can return. A process
  diagnostic that carries a class must draw it from here and nowhere else.
  """
  @spec result_classes() :: [String.t()]
  def result_classes, do: @result_classes

  @doc """
  The bare class of any term: an adapter-controlled tuple tag, atom name, struct or exception
  module is never reflected (a struct or exception is a `"map"`). The same classification
  `describe/1` applies to terms outside the known contract structs.
  """
  @spec result_class(term()) :: String.t()
  def result_class(value) when is_tuple(value), do: "tuple"
  def result_class(value) when is_atom(value), do: "atom"
  def result_class(value) when is_binary(value), do: "binary"
  def result_class(value) when is_map(value), do: "map"
  def result_class(value) when is_list(value), do: "list"
  def result_class(value) when is_integer(value), do: "integer"
  def result_class(_value), do: "other"

  defp digest(term) do
    "sha256:" <> (:sha256 |> :crypto.hash(:erlang.term_to_binary(term)) |> Base.encode16(case: :lower))
  end
end
