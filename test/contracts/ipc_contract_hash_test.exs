defmodule AiOrchestrator.Contracts.IPCContractHashTest do
  @moduledoc """
  IPC v1 reply fixtures shared with the coordination runtime. This repository pins
  the fixture bytes and CONTRACT_HASH. The hash rule is the one
  documented in docs/contracts/ipc-v1.org: lowercase SHA-256 over, for each *.json file in
  byte-sorted filename order, filename NUL bytes NUL.

  The directory is this consumer's LOCAL set: seventeen files. Sixteen of them are the set
  historically paired with Orrisd b79863cd, held here to the bytes and hash that pairing
  measured; the seventeenth, send.error.quiescing.json (the admission refusal), is
  consumer-local and its reciprocal pairing is pending the producer re-vendoring (RB-3a-P P2).
  The two inventories are pinned separately so that neither can pass as the other.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v1", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")

  # The local set: every *.json in the directory. CONTRACT_HASH pins this set.
  @local_hash "5361313977d1abbd24fbb88333d5c1465234c8668c112ec4f51869ebd5b6a43c"
  @local_fixture_count 17

  # Imported from the coordination runtime's IPC v1 fixture set and paired with Orrisd b79863cd.
  # Hash changes require coordinated protocol review in both repositories.
  @paired_hash "e809de8ea47339c1d6cffca65cc6dee1dc09d99d8f7b242e00296cc1f7f51a88"
  @paired_files ~w(
    pane_status.error.missing_pane_id.json pane_status.error.pane_dead.json pane_status.error.pane_not_found.json
    pane_status.ok.json ping.ok.json send.error.missing_pane_id.json send.error.missing_text.json
    send.error.oversize.json send.error.pane_dead.json send.error.pane_not_found.json
    send.error.pane_quarantined.json send.error.paste_failed.json send.error.queue_full.json
    send.error.send_timeout.json send.queued.json send.sent.json
  )
  # Consumer-local until the producer ships them; pairing pending (RB-3a-P P2).
  @consumer_local_files ~w(send.error.quiescing.json)

  test "the local IPC v1 fixture set matches its pinned hash" do
    paths = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()

    assert length(paths) == @local_fixture_count, "IPC v1 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v1 CONTRACT_HASH is missing"

    assert contract_hash(paths) == @local_hash
    assert @hash_path |> File.read!() |> String.trim() == @local_hash
  end

  test "the sixteen historically paired IPC v1 fixtures keep the paired bytes" do
    assert length(@paired_files) == 16
    paths = Enum.map(@paired_files, &Path.join(@fixture_dir, &1))

    for path <- paths, do: assert(File.regular?(path), Path.basename(path))

    assert contract_hash(paths) == @paired_hash
  end

  test "the local set is exactly the paired sixteen plus the consumer-local files" do
    names = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.map(&Path.basename/1) |> Enum.sort()

    assert MapSet.disjoint?(MapSet.new(@paired_files), MapSet.new(@consumer_local_files))
    assert names == Enum.sort(@paired_files ++ @consumer_local_files)
  end

  # name NUL bytes NUL for each path, in byte-sorted file name order
  defp contract_hash(paths) do
    payload =
      paths
      |> Enum.sort_by(&Path.basename/1)
      |> Enum.map(fn path -> [Path.basename(path), 0, File.read!(path), 0] end)

    :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)
  end
end
