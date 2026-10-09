defmodule AiOrchestrator.Contracts.IpcV2ContractHashTest do
  @moduledoc """
  IPC protocol version 2 reply fixtures shared with the coordination runtime:
  send replies with explicit `protocol_version`, duplicate views (`duplicate: true`
  beside the receipt status, attempt and identities -- never a status string), the
  five reconcile outcomes, and the ping capability reply. This repository pins the
  fixture bytes and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes
  NUL, byte-sorted). The v1 set is pinned separately. These tests verify the fixtures;
  they do not establish the capabilities of an installed daemon.

  The directory is this consumer's LOCAL set: twenty-one files. Twenty of them are the
  set the producer snapshot named below ships and pairs; they are held here, by name, to
  the bytes and hash that snapshot measured. The twenty-first, send.error.quiescing.json
  (the admission refusal, untyped `reply_not_ok`), is consumer-local and its reciprocal
  pairing is pending the producer re-vendoring (RB-3a-P P2). The local set and the
  historically paired subset are pinned separately so that neither can pass as the other.

  The document row is the consumer half of the paired-revision requirement: the producer
  vendors `docs/contracts/ipc-v2.org` verbatim and pins the consumer revision it took,
  and this row holds the reciprocal block in that document to the producer revision,
  the two digests and the hash of the HISTORICALLY PAIRED twenty, and separately to the
  consumer's local set and its pending status. It cannot read the producer repository --
  this gate has no access to it -- so what it detects is a change made here: the pairing
  block being dropped, edited, or left naming a fixture hash a set no longer produces.

  `@paired_revision` names a NAMED HISTORICAL SNAPSHOT of the producer, never the
  producer's current head, and `@paired_document_sha256` is that snapshot's whole-document
  digest. A later producer revision that re-vendors this repository and changes the
  producer document again does not invalidate this pin and does not make this row stale,
  because the pin never claimed to track a head. It is moved only by a deliberate
  re-pairing that measures a new named snapshot.

  A cross-check that is deliberately NOT here: recomputing `vendored_source_sha256`
  from `git show <vendored_source_revision>:docs/contracts/ipc-v2.org`. The verify job
  checks out at depth 1 (`.github/workflows/ci.yml`), so that object is absent in CI and
  the row would pass locally and fail hosted -- a control whose verdict depends on the
  checkout depth rather than on the bytes. It was measured once by hand at the
  re-pairing instead, and the pins below are exact so that a later edit of either value
  cannot pass as a well-shaped hash.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  # The local set: every *.json in the directory. CONTRACT_HASH pins this set.
  @local_hash "908783b6db5e4f129ad6514e59b2aed046ed1fe9868b6f95ee1af72caa7f4f9a"
  @local_fixture_count 21

  # The historically paired subset: exactly these twenty files, as the named producer snapshot measured them.
  @paired_hash "f23caceb6ae106238da82f35e469f022f18c24b1296c78c3f96021eb15dc8497"
  @paired_fixture_count 20
  @paired_files ~w(
    ping.ok.json reconcile.absent.json reconcile.absent.not_delivered.json reconcile.ambiguous.json
    reconcile.conflict.json reconcile.delivered.json reconcile.error.missing_payload_hash.json reconcile.queued.json
    send.duplicate.ambiguous.json send.duplicate.delivered.json send.duplicate.pending.json send.duplicate.queued.json
    send.error.conflict.json send.error.missing_msg_id.json send.error.pane_quarantined.json
    send.error.payload_store_full.json send.error.payload_store_unavailable.json send.error.queue_full.json
    send.queued.json send.sent.json
  )
  # Consumer-local until the producer ships them; pairing pending (RB-3a-P P2).
  @consumer_local_files ~w(send.error.quiescing.json)

  @document Path.expand("../../docs/contracts/ipc-v2.org", __DIR__)
  @paired_revision "ff96001878a94401948bb3999683bdfc84c31dff"
  @paired_document_sha256 "9a273558e34cc412e5241dc1713fb3d525fd3d891b5ed4a58359097256ca6d6b"
  @vendored_source_revision "bbd416fb516645d8eb563d8f1b8d3f7b7c544bba"
  @vendored_source_sha256 "ff75187917ead27923d2368aa86336a4036208fc9f465c240af6e1201936a5e1"
  @toolchain_source Path.expand("../../bin/verify", __DIR__)

  test "the local IPC v2 fixture set matches its pinned hash" do
    paths = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()

    assert length(paths) == @local_fixture_count, "IPC v2 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v2 CONTRACT_HASH is missing"

    assert contract_hash(paths) == @local_hash
    assert @hash_path |> File.read!() |> String.trim() == @local_hash
  end

  test "the twenty historically paired IPC v2 fixtures keep the named snapshot's bytes" do
    assert length(@paired_files) == @paired_fixture_count
    paths = Enum.map(@paired_files, &Path.join(@fixture_dir, &1))

    for path <- paths, do: assert(File.regular?(path), Path.basename(path))

    assert contract_hash(paths) == @paired_hash
  end

  test "the local set is exactly the paired twenty plus the consumer-local files" do
    names = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.map(&Path.basename/1) |> Enum.sort()

    assert MapSet.disjoint?(MapSet.new(@paired_files), MapSet.new(@consumer_local_files))
    assert names == Enum.sort(@paired_files ++ @consumer_local_files)
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

  test "the contract document names the producer copy it is paired with, the paired hash and the local set" do
    document = File.read!(@document)

    assert declared(document, "paired_repository") == "orrisd"
    assert declared(document, "paired_path") == "docs/contracts/ipc-v2.org"
    assert declared(document, "paired_revision") == @paired_revision
    assert declared(document, "paired_document_sha256") == @paired_document_sha256

    # exact, not shaped: both halves of a re-pairing move together or this row fails. The producer
    # took THESE bytes at @vendored_source_revision, so a silent edit of either value is a claim
    # about a snapshot that was never taken, which a 40-hex shape check would have admitted.
    assert declared(document, "vendored_source_revision") == @vendored_source_revision
    assert declared(document, "vendored_source_sha256") == @vendored_source_sha256

    # the paired_fixture_* keys describe the HISTORICALLY PAIRED twenty, never the local set: the document is a
    # second pin on the paired hash beside this module's constant, so a silent change of either fails here
    assert declared(document, "paired_fixture_contract_hash") == @paired_hash
    assert declared(document, "paired_fixture_count") == Integer.to_string(@paired_fixture_count)

    # the consumer_* keys describe the LOCAL set the CONTRACT_HASH file pins, and its pairing stays pending until the
    # producer ships the consumer-local files: a third pin on the local hash beside that file and this constant
    assert declared(document, "consumer_fixture_contract_hash") == @local_hash
    assert declared(document, "consumer_fixture_count") == Integer.to_string(@local_fixture_count)
    assert declared(document, "consumer_local_fixtures") == Enum.join(@consumer_local_files, " ")
    assert declared(document, "pairing_status") == "pending"
  end

  # Both products refuse to verify on anything but one Elixir/OTP pair, and each carries that pair as a literal in
  # its own bin/verify. Nothing asserted the two literals were the SAME pair, so they could drift apart one release
  # at a time with both gates green. This is the half the consumer can mechanise: what the contract declares as the
  # paired toolchain is held to what this repository actually refuses to verify without.
  test "the paired toolchain the contract declares is the toolchain this repository refuses to verify without" do
    document = File.read!(@document)
    verify = File.read!(@toolchain_source)

    elixir = pinned(verify, "required_elixir")
    otp = pinned(verify, "required_otp")

    # a witness: a bin/verify that stopped pinning versions at all would otherwise make the row vacuous
    assert elixir =~ ~r/\A\d+\.\d+\.\d+\z/
    assert otp =~ ~r/\A\d+\.\d+\.\d+\z/

    assert declared(document, "paired_elixir") == elixir
    assert declared(document, "paired_otp") == otp
  end

  # `name="value"` in bin/verify; exactly one such line per name, so a removed or duplicated pin fails rather
  # than resolving to whichever line happens to come first
  defp pinned(verify, name) do
    case Regex.scan(~r/^#{name}="([^"]+)"$/m, verify) do
      [[_line, value]] -> value
      other -> flunk("#{name} is not pinned exactly once in #{@toolchain_source}: #{inspect(other)}")
    end
  end

  # name NUL bytes NUL for each path, in byte-sorted file name order
  defp contract_hash(paths) do
    payload =
      paths
      |> Enum.sort_by(&Path.basename/1)
      |> Enum.map(fn path -> [Path.basename(path), 0, File.read!(path), 0] end)

    :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)
  end

  # `- key: ~value~` in the paired block; exactly one such line per key, so a duplicated or removed key fails here
  defp declared(document, key) do
    case Regex.scan(~r/^- #{key}: ~([^~]+)~/m, document) do
      [[_line, value]] -> value
      other -> flunk("#{key} is not declared exactly once in #{@document}: #{inspect(other)}")
    end
  end
end
