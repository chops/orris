defmodule AiOrchestrator.Contract.SensitiveBytes do
  @moduledoc """
  A closed wrapper around bytes that may cross an internal boundary but may never
  be printed, logged, journaled or transmitted as themselves.

  Prompt bytes do cross the effect/adapter boundary: an effect is how a pure reducer
  asks the host to retain bytes, and an opaque handle cannot precede the retention
  that mints it. The no-payload rule governs journals, protocol replies, diagnostics,
  telemetry and logs. So the boundary is not the leak surface. The leak surface is
  every incidental printer that will one day be pointed at a struct holding the
  bytes: a crashed process printing its state, a `Logger.error` interpolating an
  effect, an `IO.inspect` left in a branch nobody exercises, a `Jason.encode!` of a
  command. None of those go through `Diagnostic.describe/1`, so a payload-free
  diagnostic does not help with any of them.

  What makes this wrapper closed rather than merely conventional is as much what it
  omits as what it implements:

    * `Inspect` prints the class, the digest and the byte count, and nothing derived
      from the bytes. It ignores `limit` and `printable_limit`, because a redaction
      that is really truncation is undone by the operator who raises them to read a
      large struct.
    * there is no `Jason.Encoder`. The journal writer, the protocol reply and every
      telemetry attribute go through Jason, so the one mistake that would make a
      prompt durable raises at the moment it is made instead of disclosing. Passing
      `hash/1` is the supported way past it.
    * there is no `String.Chars`, so `"\#{sensitive}"` in a log line cannot compile
      away into the payload.
    * revealing is a named function, and `reveal/1` has no clause for a bare binary,
      so a caller cannot get passthrough behaviour by handing it something that was
      never wrapped.

  The struct compares by value, because effects are compared by value in the reducer
  and in tests. A wrapper that broke equality would force callers to unwrap in order
  to compare, which is the habit this type exists to remove.
  """

  @enforce_keys [:bytes, :class, :hash, :byte_size]
  defstruct [:bytes, :class, :hash, :byte_size]

  @typedoc "What kind of secret this is. Named in diagnostics; never the bytes."
  @type class :: :prompt

  @type t :: %__MODULE__{
          bytes: binary(),
          class: class(),
          hash: String.t(),
          byte_size: non_neg_integer()
        }

  @doc """
  Wraps bytes under a class.

  The digest and size are computed once and carried, so the payload-free facts are
  available everywhere the wrapper is without touching the bytes again.
  """
  @spec new(binary(), class()) :: t()
  def new(bytes, class) when is_binary(bytes) and is_atom(class) do
    %__MODULE__{
      bytes: bytes,
      class: class,
      hash: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
      byte_size: Kernel.byte_size(bytes)
    }
  end

  @doc """
  Returns the exact bytes that were wrapped.

  There are exactly two call sites: `PromptStore` writing the object, and the final
  `PaneClient` adapter sending it. Anything else wanting to know something about the
  bytes wants `hash/1`, `byte_size/1` or `class/1`.
  """
  @spec reveal(t()) :: binary()
  def reveal(%__MODULE__{bytes: bytes}), do: bytes

  @doc "The kind of secret. Safe to journal."
  @spec class(t()) :: class()
  def class(%__MODULE__{class: class}), do: class

  @doc ~S'The `"sha256:" <> hex` digest, in the shape the reducer already journals.'
  @spec hash(t()) :: String.t()
  def hash(%__MODULE__{hash: hash}), do: hash

  @doc "How many bytes are wrapped. Safe to journal."
  @spec byte_size(t()) :: non_neg_integer()
  def byte_size(%__MODULE__{byte_size: size}), do: size

  defimpl Inspect do
    import Inspect.Algebra

    alias AiOrchestrator.Contract.SensitiveBytes

    def inspect(%SensitiveBytes{class: class, hash: hash, byte_size: size}, opts) do
      concat([
        "#SensitiveBytes<",
        to_doc(class, opts),
        " ",
        hash,
        " ",
        Integer.to_string(size),
        " bytes>"
      ])
    end
  end
end
