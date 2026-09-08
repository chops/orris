defmodule AiOrchestrator.Contract.SensitiveBytesTest do
  @moduledoc """
  RED for the redaction wrapper ruled in by `m_1788515559000000000_f7fa9369` (a).

  The ruling settles a boundary question: prompt bytes *may* cross an internal
  effect/adapter boundary, because an effect is how a pure reducer asks the host to retain
  bytes and an opaque handle cannot precede the retention that mints it. The no-payload
  rule governs journals, protocol replies, diagnostics, telemetry and logs.

  That leaves the leak surface, which is not the boundary itself but every incidental
  printer that will one day be pointed at a struct carrying the bytes: a crashed
  `Run.Server` printing its state, a `Logger.error` interpolating an effect, an `IO.inspect`
  left in a branch nobody exercises, a `Jason.encode!` of a command. `Diagnostic.describe/1`
  being payload-free does not help with any of them, because none of them go through it.

  So the bytes travel wrapped. What this file pins is that the wrapper is *closed* rather
  than merely conventional:

    * `Inspect` prints class, hash and byte count, and nothing derived from the bytes.
    * there is no `Jason.Encoder`, so journaling the wrapper raises instead of disclosing.
      This is the strongest property here: it turns the one mistake that would put a prompt
      in the durable record into a loud failure at the moment it is made.
    * there is no `String.Chars`, so `"\#{sensitive}"` in a log line does not compile away
      into the payload.
    * revealing is explicit, by a named function, from exactly two call sites.

  The call sites are pinned elsewhere -- `PromptStore` writes the bytes and the final
  `PaneClient` adapter sends them -- and the absence of any other reveal is pinned by the
  leak assertions in `run_fsm_prompt_retention_test.exs` and
  `run_fsm_dispatch_reconcile_test.exs`.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SensitiveBytes

  @payload "* Assignment\n\nUNIQUE-PROMPT-SENTINEL-6f3a for the agent to act on.\n"
  @digest "sha256:" <> Base.encode16(:crypto.hash(:sha256, @payload), case: :lower)

  describe "the wrapper carries bytes and answers only facts about them" do
    test "revealing returns the exact bytes that were wrapped" do
      assert @payload |> SensitiveBytes.new(:prompt) |> SensitiveBytes.reveal() == @payload
    end

    test "the payload-free facts are the ones a journal is allowed to keep" do
      sensitive = SensitiveBytes.new(@payload, :prompt)

      assert SensitiveBytes.class(sensitive) == :prompt
      assert SensitiveBytes.hash(sensitive) == @digest
      assert SensitiveBytes.byte_size(sensitive) == byte_size(@payload)

      assert SensitiveBytes.hash(sensitive) == "sha256:" <> Base.encode16(:crypto.hash(:sha256, @payload), case: :lower),
             """
             The digest is the same shape the reducer already journals, so a wrapper and an
             event can be compared without unwrapping either.
             """
    end

    test "two wrappers of the same bytes are the same value" do
      assert SensitiveBytes.new(@payload, :prompt) == SensitiveBytes.new(@payload, :prompt),
             """
             Effects are compared by value in tests and in the reducer's own equality
             checks. A wrapper that broke that would force callers to unwrap in order to
             compare, which is the habit this type exists to remove.
             """

      refute SensitiveBytes.new(@payload, :prompt) == SensitiveBytes.new(@payload <> "x", :prompt)
    end

    test "revealing something that was never wrapped is a crash, not a passthrough" do
      # A dynamic call: the point is the runtime refusal, and a literal wrong-type call is a
      # compile-time type warning that bin/verify counts as an error.
      # Resolved at runtime: a literal-bound module is still type-checked, and the deliberate
      # wrong-type call is a compile-time warning bin/verify counts as an error.
      wrapper = String.to_existing_atom("Elixir.AiOrchestrator.Contract.SensitiveBytes")
      assert_raise FunctionClauseError, fn -> wrapper.reveal(@payload) end
    end
  end

  describe "printing it prints the facts and never the bytes" do
    test "inspect names the class, the digest and the size" do
      printed = inspect(SensitiveBytes.new(@payload, :prompt))

      assert printed =~ "prompt"
      assert printed =~ @digest
      assert printed =~ to_string(byte_size(@payload))
    end

    test "inspect does not contain the payload" do
      sensitive = SensitiveBytes.new(@payload, :prompt)

      refute inspect(sensitive) =~ "UNIQUE-PROMPT-SENTINEL-6f3a"

      refute inspect(sensitive, limit: :infinity, printable_limit: :infinity) =~ "UNIQUE-PROMPT-SENTINEL-6f3a",
             """
             `limit` and `printable_limit` are how a redaction that is really just
             truncation gets undone. An operator raising them to read a large struct must
             not thereby read the prompt.
             """
    end

    test "inspect stays redacted when the wrapper is nested inside something else" do
      printed = inspect(%{"command" => %{"prompt" => SensitiveBytes.new(@payload, :prompt)}}, printable_limit: :infinity)

      refute printed =~ "UNIQUE-PROMPT-SENTINEL-6f3a", "a struct is usually printed as part of a larger one"
      assert printed =~ @digest
    end

    test "there is no String.Chars, so interpolation cannot silently unwrap it" do
      chars = String.to_existing_atom("Elixir.String.Chars")
      assert_raise Protocol.UndefinedError, fn -> chars.to_string(SensitiveBytes.new(@payload, :prompt)) end
    end
  end

  describe "the wrapper cannot be journaled" do
    test "encoding it raises rather than writing the payload" do
      assert_raise Protocol.UndefinedError, fn -> Jason.encode!(SensitiveBytes.new(@payload, :prompt)) end
    end

    test "encoding a command that still holds the wrapper raises too" do
      command = %{"assignment_id" => "as_0001", "prompt" => SensitiveBytes.new(@payload, :prompt)}

      assert_raise Protocol.UndefinedError, fn -> Jason.encode!(command) end

      assert match?({:ok, _}, Jason.encode(%{command | "prompt" => SensitiveBytes.hash(command["prompt"])})),
             """
             Deliberately omitting `Jason.Encoder` is the whole mechanism. The journal
             writer, the protocol reply and every telemetry attribute go through Jason, so
             the one mistake that would make a prompt durable becomes an exception at the
             moment it is made rather than a disclosure discovered later. Replacing the
             wrapper with its digest is the supported way past it.
             """
    end
  end
end
