defmodule AiOrchestrator.Contracts.EnvelopeValidationTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Protocol.Envelope

  for name <- [
        "valid_note",
        "valid_ask",
        "valid_answer",
        "valid_status",
        "valid_handoff",
        "valid_consultation",
        "valid_full_context"
      ] do
    test "#{name}: envelope validates" do
      envelope = F.json("envelopes", unquote(name), "envelope.json")
      assert {:ok, validated} = Envelope.validate(envelope)
      assert is_map(validated)
    end
  end

  for name <- [
        "invalid_unknown_kind",
        "invalid_bare_string_peer",
        "invalid_revision_without_hash",
        "invalid_schema_major"
      ] do
    test "#{name}: envelope rejected with the named clause" do
      name = unquote(name)
      envelope = F.json("envelopes", name, "envelope.json")
      expected = F.json("envelopes", name, "expected_rejection.json")
      assert {:error, rejection} = Envelope.validate(envelope)
      F.assert_rejection_matches(rejection, expected)
    end
  end
end
