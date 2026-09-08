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
end
