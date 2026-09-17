defmodule AiOrchestrator.Contract.SendIdTest do
  @moduledoc """
  RED for MUST-1 (Codex review `m_1788508818157722000_82f1b70d`): the daemon receipt store
  is global, but assignment ids are only run-scoped.

  The reducer used to mint `"send_" <> assignment_id`, so every run that contained
  `as_0001` asks the daemon about the same message id. Two runs then collide in the one
  store: the second run's admission is answered from the first run's receipt, so a prompt
  that was never sent reads as `delivered` and the work is silently dropped -- or, if the
  payloads differ, an unrelated run reads as `conflict` and blocks for attention it did
  not earn.

  NS-42 rule 4 pins the replacement grammar: `snd_` followed by the full lowercase
  SHA-256 over the ASCII tag `SEND-ID-1\\n`, then `run_id` and `assignment_id` each
  encoded as a big-endian `u32` byte length followed by exact UTF-8 bytes. The full
  digest is kept rather than truncated so collision resistance is at least 128 bits, and
  the length prefixes make the encoding injective: no pair of distinct run/assignment
  values can produce the same preimage by shifting a delimiter.

  The id must stay a pure function of run and assignment, because replay has to mint the
  same id the pre-crash run used -- an id derived from a clock or a random source would
  make the receipt unfindable exactly when it matters.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SendId

  @grammar ~r/^snd_[0-9a-f]{64}$/

  test "the minted id matches the pinned grammar" do
    id = SendId.mint("run_2026_09_04_a", "as_0001")

    assert Regex.match?(@grammar, id),
           "NS-42 rule 4 pins ^snd_[0-9a-f]{64}$, got: #{inspect(id)}"
  end

  test "the id is replay-stable for one run and assignment" do
    assert SendId.mint("run_a", "as_0001") == SendId.mint("run_a", "as_0001"),
           "resume must mint the id the pre-crash run used, or the receipt cannot be found"
  end

  test "the same assignment id in two runs does not collide" do
    assert SendId.mint("run_a", "as_0001") != SendId.mint("run_b", "as_0001"),
           "a global receipt store cannot distinguish two runs whose send ids are equal"
  end

  test "distinct assignments within one run do not collide" do
    assert SendId.mint("run_a", "as_0001") != SendId.mint("run_a", "as_0002")
  end

  test "the encoding is injective across the run/assignment boundary" do
    # Without length prefixes, "run_a" <> "as_0001" and "run_" <> "aas_0001" hash alike.
    assert SendId.mint("run_a", "as_0001") != SendId.mint("run_", "aas_0001"),
           "a delimiter-free concatenation would let one boundary shift produce two equal ids"
  end

  test "the id matches an independent computation of the pinned preimage" do
    run_id = "run_2026_09_04_a"
    assignment_id = "as_0001"

    preimage =
      "SEND-ID-1\n" <>
        <<byte_size(run_id)::unsigned-big-32>> <>
        run_id <>
        <<byte_size(assignment_id)::unsigned-big-32>> <>
        assignment_id

    expected = "snd_" <> (:sha256 |> :crypto.hash(preimage) |> Base.encode16(case: :lower))

    assert SendId.mint(run_id, assignment_id) == expected,
           "the grammar is a cross-repository contract; it must not drift silently"
  end

  # NS-42.C.004 failure controls: each of these is a plausible wrong implementation of the
  # same grammar. Every one is computed independently here and must NOT equal the minted id,
  # so a drift into any of them fails a named test rather than silently changing the key the
  # daemon's receipt store is asked about.
  @run_id "run_2026_09_04_a"
  @assignment_id "as_0001"

  defp minted, do: SendId.mint(@run_id, @assignment_id)

  defp digest(preimage), do: "snd_" <> (:sha256 |> :crypto.hash(preimage) |> Base.encode16(case: :lower))

  defp u32(field), do: <<byte_size(field)::unsigned-big-32>> <> field

  describe "the pinned preimage is the only one the id answers to" do
    test "a different tag is a different id" do
      for tag <- ["SEND-ID-0\n", "SEND-ID-2\n", "SEND-ID-1", "send-id-1\n", ""] do
        refute minted() == digest(tag <> u32(@run_id) <> u32(@assignment_id)), inspect(tag)
      end
    end

    test "the digest is lowercase, never uppercase or mixed" do
      id = minted()
      assert id == String.downcase(id)
      refute id == "snd_" <> String.upcase(String.slice(id, 4..-1//1))
    end

    test "the digest is the full 64 hex characters, never truncated" do
      id = minted()
      assert byte_size(id) == 4 + 64

      for keep <- [16, 32, 40, 63] do
        refute id == "snd_" <> String.slice(id, 4, keep), "a #{keep}-character truncation"
      end
    end

    test "the length prefix is four bytes big-endian, not one or two bytes, and not little-endian" do
      alternatives = [
        {"u8", fn field -> <<byte_size(field)::unsigned-8>> <> field end},
        {"u16 big-endian", fn field -> <<byte_size(field)::unsigned-big-16>> <> field end},
        {"u32 little-endian", fn field -> <<byte_size(field)::unsigned-little-32>> <> field end},
        {"u64 big-endian", fn field -> <<byte_size(field)::unsigned-big-64>> <> field end},
        {"no prefix", fn field -> field end},
        {"NUL delimiter", fn field -> field <> <<0>> end}
      ]

      for {name, encode} <- alternatives do
        refute minted() == digest("SEND-ID-1\n" <> encode.(@run_id) <> encode.(@assignment_id)), name
      end
    end

    test "the prefix counts bytes, not characters, so multi-byte UTF-8 fields are encoded by byte length" do
      # Two e-acute characters, written as their UTF-8 bytes so the file stays ASCII.
      run_id = "run_" <> <<195, 169, 195, 169>>
      assert String.valid?(run_id) and String.length(run_id) != byte_size(run_id)

      chars = fn field -> <<String.length(field)::unsigned-big-32>> <> field end
      refute SendId.mint(run_id, @assignment_id) == digest("SEND-ID-1\n" <> chars.(run_id) <> chars.(@assignment_id))
      assert SendId.mint(run_id, @assignment_id) == digest("SEND-ID-1\n" <> u32(run_id) <> u32(@assignment_id))
    end

    test "a field longer than 255 bytes still gets one four-byte length and one id" do
      long_run = String.duplicate("r", 300)
      id = SendId.mint(long_run, @assignment_id)
      assert Regex.match?(@grammar, id)
      assert id == digest("SEND-ID-1\n" <> u32(long_run) <> u32(@assignment_id))

      refute id == digest("SEND-ID-1\n" <> <<44::unsigned-big-32>> <> long_run <> u32(@assignment_id)),
             "a length wrapped at 8 bits"
    end

    test "the field order is run then assignment, never the reverse" do
      refute minted() == digest("SEND-ID-1\n" <> u32(@assignment_id) <> u32(@run_id))
      refute SendId.mint(@run_id, @assignment_id) == SendId.mint(@assignment_id, @run_id)
    end

    test "an empty field is encoded as a zero length, so it is still distinguishable" do
      assert SendId.mint("", @assignment_id) == digest("SEND-ID-1\n" <> <<0::unsigned-big-32>> <> u32(@assignment_id))
      refute SendId.mint("", @assignment_id) == SendId.mint(@assignment_id, "")
    end
  end
end
