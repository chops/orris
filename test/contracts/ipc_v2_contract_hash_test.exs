defmodule AiOrchestrator.Contracts.IpcV2ContractHashTest do
  @moduledoc """
  IPC protocol version 2 reply fixtures shared with the coordination runtime:
  send replies with explicit `protocol_version`, duplicate views (`duplicate: true`
  beside the receipt status, attempt and identities -- never a status string), the
  five reconcile outcomes, and the ping capability reply. This repository pins the
  fixture bytes and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes
  NUL, byte-sorted). The v1 set is pinned separately. These tests verify the fixtures;
  they do not establish the capabilities of an installed daemon.

  The third row is the consumer half of the paired-revision requirement: the producer
  vendors `docs/contracts/ipc-v2.org` verbatim and pins the consumer revision it took,
  and this row holds the reciprocal block in that document to the producer revision,
  the two digests and the fixture hash THESE fixtures produce. It cannot read the
  producer repository -- this gate has no access to it -- so what it detects is a
  change made here: the pairing block being dropped, edited, or left naming a fixture
  hash the fixture set no longer produces.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "78c2f64240c3c5c9da60425c65c498974a2a81c8adb3e68e0bef28613c1707dc"
  @expected_fixture_count 16

  @document Path.expand("../../docs/contracts/ipc-v2.org", __DIR__)
  @paired_revision "971f0a88af0ed9496210a2da0c291eaf35937556"
  @paired_document_sha256 "a593e1eb1e5194dc0dc122fb15896240b6547a644c28ae388bb2a85a2a47a202"

  test "the IPC v2 fixture set matches the pinned cross-repository hash" do
    paths = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()

    assert length(paths) == @expected_fixture_count, "IPC v2 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v2 CONTRACT_HASH is missing"

    payload = Enum.map(paths, fn path -> [Path.basename(path), 0, File.read!(path), 0] end)
    actual = :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)

    assert actual == @pinned_hash
    assert @hash_path |> File.read!() |> String.trim() == @pinned_hash
  end

  test "every v2 reply names its protocol version and a duplicate is a flag, not a status" do
    for path <- @fixture_dir |> Path.join("*.json") |> Path.wildcard() do
      reply = path |> File.read!() |> Jason.decode!()
      assert reply["protocol_version"] == 2, Path.basename(path)
      refute reply["status"] == "duplicate", Path.basename(path)

      if reply["duplicate"] do
        assert reply["status"] in ["pending", "queued", "delivered", "ambiguous"], Path.basename(path)
        assert is_integer(reply["delivery_attempt"]) and reply["delivery_attempt"] > 0, Path.basename(path)

        assert Map.has_key?(reply, "payload_hash") and Map.has_key?(reply, "msg_id") and Map.has_key?(reply, "pane_id"),
               Path.basename(path)
      end
    end
  end

  test "the contract document names the producer copy it is paired with, and the fixture hash both repositories pin" do
    document = File.read!(@document)

    assert declared(document, "paired_repository") == "orrisd"
    assert declared(document, "paired_path") == "docs/contracts/ipc-v2.org"
    assert declared(document, "paired_revision") == @paired_revision
    assert declared(document, "paired_document_sha256") == @paired_document_sha256
    assert declared(document, "vendored_source_revision") =~ ~r/\A[0-9a-f]{40}\z/
    assert declared(document, "vendored_source_sha256") =~ ~r/\A[0-9a-f]{64}\z/

    # the document is a THIRD pin on the fixture hash, beside the CONTRACT_HASH file and this module's constant:
    # a rotation that moved the fixtures, that file and this constant together but left the document fails here
    assert declared(document, "paired_fixture_contract_hash") == @pinned_hash
    assert declared(document, "paired_fixture_count") == Integer.to_string(@expected_fixture_count)
  end

  # `- key: ~value~` in the paired block; exactly one such line per key, so a duplicated or removed key fails here
  defp declared(document, key) do
    case Regex.scan(~r/^- #{key}: ~([^~]+)~/m, document) do
      [[_line, value]] -> value
      other -> flunk("#{key} is not declared exactly once in #{@document}: #{inspect(other)}")
    end
  end
end
