defmodule AiOrchestrator.Journal.ChainTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Chain

  @empty_sha "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  defp raw(name), do: F.raw("journals", name)

  defp version_2_lines, do: "valid_envelope_version_2_minimal" |> raw() |> String.split("\n", trim: true)

  defp receipt(seq, hash), do: %{seq: seq, line_sha256: hash, updated_at: "2026-09-03T00:00:02Z"}

  defp upgrade_line(%{count: count, last_line_sha256: last, lines: lines}) do
    lines
    |> List.last()
    |> Jason.decode!()
    |> Map.merge(%{"schema_version" => 2, "seq" => count + 1, "event_id" => "ev_upgrade", "prev_line_sha256" => last})
    |> Jason.encode!()
  end

  describe "hashing" do
    test "the anchor is the hash of the empty string and lines hash with their newline" do
      assert Chain.anchor() == @empty_sha
      assert Chain.line_sha256("abc\n") == "sha256:" <> Base.encode16(:crypto.hash(:sha256, "abc\n"), case: :lower)
      refute Chain.line_sha256("abc\n") == Chain.line_sha256("abc")
    end
  end

  describe "verify/1" do
    test "an empty journal verifies to zero lines at the anchor" do
      assert {:ok, %{count: 0, envelope_version: 0, last_line_sha256: @empty_sha, incomplete_tail: nil, lines: []}} =
               Chain.verify("")
    end

    test "a version 2 journal verifies every link and reports the last line hash" do
      bytes = raw("valid_envelope_version_2_minimal")
      [_first, second] = version_2_lines()

      assert {:ok, %{count: 2, envelope_version: 2, incomplete_tail: nil, last_line_sha256: last, lines: lines}} =
               Chain.verify(bytes)

      assert last == Chain.line_sha256(second <> "\n")
      assert lines == version_2_lines()
    end

    test "a version 1 journal is accepted without chain links (legacy path)" do
      bytes = raw("valid_minimal")
      count = bytes |> String.split("\n", trim: true) |> length()
      assert {:ok, %{count: ^count, envelope_version: 1, incomplete_tail: nil}} = Chain.verify(bytes)
    end

    test "a flipped byte in a middle line fails closed at the next sequence" do
      [first, second] = version_2_lines()
      corrupted = String.replace(first, ~s("project":"example"), ~s("project":"exampls"), global: false)
      refute corrupted == first
      assert {:error, %{clause: "chain_mismatch", at_seq: 2}} = Chain.verify(corrupted <> "\n" <> second <> "\n")
    end

    test "an undecodable complete line fails closed at its own sequence" do
      [_first, second] = version_2_lines()
      assert {:error, %{clause: "undecodable_line", at_seq: 1}} = Chain.verify("{garbage\n" <> second <> "\n")
    end

    test "a version 1 line after version 2 lines fails closed at that line" do
      [version_1_line | _] = "valid_minimal" |> raw() |> String.split("\n", trim: true)
      downgraded = version_1_line |> Jason.decode!() |> Map.put("seq", 3) |> Jason.encode!()
      bytes = raw("valid_envelope_version_2_minimal") <> downgraded <> "\n"
      assert {:error, %{clause: "mixed_envelope_versions", at_seq: 3}} = Chain.verify(bytes)
    end

    test "a legacy journal may upgrade one way to version 2 at a resume point" do
      {:ok, legacy} = Chain.verify(raw("valid_minimal"))
      assert %{envelope_version: 1, version_2_from: nil, version_2_count: 0} = legacy
      upgraded_bytes = raw("valid_minimal") <> upgrade_line(legacy) <> "\n"

      assert {:ok, %{envelope_version: 2, version_2_from: from, version_2_count: 1, count: count}} =
               Chain.verify(upgraded_bytes)

      assert from == legacy.count + 1 and count == legacy.count + 1

      wrong_anchor = legacy |> upgrade_line() |> Jason.decode!() |> Map.put("prev_line_sha256", @empty_sha)
      bytes = raw("valid_minimal") <> Jason.encode!(wrong_anchor) <> "\n"
      assert {:error, %{clause: "chain_mismatch", at_seq: ^from}} = Chain.verify(bytes)
    end

    test "bytes after the final newline are reported as the incomplete tail, never verified" do
      bytes = raw("valid_envelope_version_2_minimal") <> ~s({"schema":"ai-orchestrator/journal-event","seq":3)

      assert {:ok, %{count: 2, incomplete_tail: ~s({"schema":"ai-orchestrator/journal-event","seq":3)}} =
               Chain.verify(bytes)
    end
  end

  describe "receipts" do
    test "encode and decode round-trip a head receipt as one JSON line" do
      encoded = Chain.encode_receipt(receipt(2, Chain.line_sha256("x\n")))
      assert String.ends_with?(encoded, "\n")
      assert %{"schema" => "ai-orchestrator/journal-head", "seq" => 2} = Jason.decode!(encoded)
      assert {:ok, %{seq: 2, line_sha256: hash, updated_at: "2026-09-03T00:00:02Z"}} = Chain.decode_receipt(encoded)
      assert hash == Chain.line_sha256("x\n")
    end

    test "malformed receipts are rejected with one named clause" do
      good = 1 |> receipt(@empty_sha) |> Chain.encode_receipt() |> Jason.decode!()

      for bad <- [
            "not json",
            Jason.encode!(Map.put(good, "schema", "other")),
            Jason.encode!(Map.put(good, "seq", 0)),
            Jason.encode!(Map.put(good, "line_sha256", "sha256:nope")),
            Jason.encode!(Map.delete(good, "updated_at")),
            Jason.encode!(Map.put(good, "extra", 1))
          ] do
        assert {:error, %{clause: "invalid_receipt"}} = Chain.decode_receipt(bad)
      end
    end
  end

  describe "reconcile/2" do
    setup do
      {:ok, verified} = Chain.verify(raw("valid_envelope_version_2_minimal"))
      [first, _second] = version_2_lines()
      %{verified: verified, first_hash: Chain.line_sha256(first <> "\n")}
    end

    test "receipt at the last line with the right hash needs nothing", %{verified: v} do
      assert {:ok, %{action: :none, truncate_bytes: 0, receipt_seq_before: 2, receipt_seq_after: 2}} =
               Chain.reconcile(v, receipt(2, v.last_line_sha256))
    end

    test "receipt one behind a chain-valid final line advances", %{verified: v, first_hash: h1} do
      assert {:ok, %{action: :advance_receipt, receipt_seq_before: 1, receipt_seq_after: 2, truncate_bytes: 0}} =
               Chain.reconcile(v, receipt(1, h1))
    end

    test "a missing receipt is only legal before the second line", %{verified: v} do
      {:ok, one} = Chain.verify(hd(version_2_lines()) <> "\n")

      assert {:ok, %{action: :advance_receipt, receipt_seq_before: 0, receipt_seq_after: 1}} =
               Chain.reconcile(one, nil)

      assert {:error, %{clause: "receipt_missing", count: 2}} = Chain.reconcile(v, nil)
    end

    test "a receipt beyond, stale, or hash-inconsistent fails closed", %{verified: v, first_hash: h1} do
      assert {:error, %{clause: "receipt_beyond_tail", receipt_seq: 3, count: 2}} =
               Chain.reconcile(v, receipt(3, v.last_line_sha256))

      {:ok, three} = Chain.verify(raw("valid_envelope_version_2_minimal"))
      assert {:error, %{clause: "receipt_hash_mismatch", receipt_seq: 2}} = Chain.reconcile(three, receipt(2, h1))

      assert {:error, %{clause: "receipt_hash_mismatch", receipt_seq: 1}} =
               Chain.reconcile(v, receipt(1, v.last_line_sha256))

      {:ok, long} = Chain.verify(raw("valid_envelope_version_2_minimal"))
      assert {:error, %{clause: "receipt_stale", receipt_seq: 0}} = Chain.reconcile(%{long | count: 3}, receipt(0, h1))
    end

    test "an incomplete tail is truncated exactly once and recorded", %{verified: v} do
      tail = ~s({"schema":"ai-orchestrator/journal-event","seq":3)
      {:ok, torn} = Chain.verify(raw("valid_envelope_version_2_minimal") <> tail)

      assert {:ok, %{action: :truncate_tail, truncate_bytes: bytes, receipt_seq_before: 2, receipt_seq_after: 2}} =
               Chain.reconcile(torn, receipt(2, v.last_line_sha256))

      assert bytes == byte_size(tail)

      first_hash = Chain.line_sha256(hd(version_2_lines()) <> "\n")
      assert {:ok, advanced} = Chain.reconcile(torn, receipt(1, first_hash))
      assert advanced.action == :advance_and_truncate
      assert {1, 2, ^bytes} = {advanced.receipt_seq_before, advanced.receipt_seq_after, advanced.truncate_bytes}
    end

    test "legacy version 1 journals ignore no receipt, truncate a tail, and refuse a receipt" do
      {:ok, legacy} = Chain.verify(raw("valid_minimal"))
      assert {:ok, %{action: :none, truncate_bytes: 0}} = Chain.reconcile(legacy, nil)

      {:ok, torn} = Chain.verify(raw("valid_minimal") <> "{\"partial")
      assert {:ok, %{action: :truncate_tail, truncate_bytes: 9}} = Chain.reconcile(torn, nil)
      assert {:error, %{clause: "receipt_on_legacy_journal"}} = Chain.reconcile(legacy, receipt(1, @empty_sha))
    end

    test "after an upgrade the receipt rules apply to the version 2 suffix only" do
      {:ok, legacy} = Chain.verify(raw("valid_minimal"))
      one = raw("valid_minimal") <> upgrade_line(legacy) <> "\n"
      {:ok, upgraded} = Chain.verify(one)
      n = legacy.count + 1

      assert {:ok, %{action: :advance_receipt, receipt_seq_before: 0, receipt_seq_after: ^n}} =
               Chain.reconcile(upgraded, nil)

      assert {:ok, %{action: :none}} = Chain.reconcile(upgraded, receipt(n, upgraded.last_line_sha256))

      {:ok, two} = Chain.verify(one <> upgrade_line(upgraded) <> "\n")
      assert {:error, %{clause: "receipt_missing", count: _}} = Chain.reconcile(two, nil)
    end
  end
end
