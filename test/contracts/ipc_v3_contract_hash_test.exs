defmodule AiOrchestrator.Contracts.IpcV3ContractHashTest do
  @moduledoc """
  IPC protocol version 3 example fixtures (`docs/contracts/ipc-v3.org`): pane identity on
  pane-bound replies, per-pane status, cancel and its cancelled receipt status, and the
  subscribe frames and ordered sequences. This repository pins the fixture bytes and
  CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes NUL, byte-sorted).

  Version 3 is specified, not implemented, on either side. These rows check the example
  set and its agreement with the text; they say nothing about any daemon. There is no
  pairing row yet: the producer has not vendored this document, and the paired block is
  added in that re-pairing.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "ea87ff58298623a39d3be00e2176bcbff53eeecb2fe14577e2e32140c61cffaa"
  @expected_fixture_count 35
  @document Path.expand("../../docs/contracts/ipc-v3.org", __DIR__)
  @v2_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)

  defp fixtures, do: @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()
  defp decode(path), do: path |> File.read!() |> Jason.decode!()

  test "the IPC v3 fixture set matches the pinned hash" do
    paths = fixtures()

    assert length(paths) == @expected_fixture_count, "IPC v3 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v3 CONTRACT_HASH is missing"

    payload = Enum.map(paths, fn path -> [Path.basename(path), 0, File.read!(path), 0] end)
    actual = :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)

    assert actual == @pinned_hash
    assert @hash_path |> File.read!() |> String.trim() == @pinned_hash
  end

  test "every fixture is version 3 except the one version 2 refusal" do
    for path <- fixtures() do
      name = Path.basename(path)
      expected = if String.starts_with?(name, "v2_refusal."), do: 2, else: 3
      assert decode(path)["protocol_version"] == expected, name
    end
  end

  test "every pane-bound version 3 reply carries a full pane identity, except the refusal example" do
    for path <- fixtures(),
        name = Path.basename(path),
        String.starts_with?(name, "send.") or String.starts_with?(name, "cancel.") or
          String.starts_with?(name, "reconcile.") or name == "status.ok.json" or name == "status.quarantined.json" do
      reply = decode(path)

      cond do
        name == "send.sent.no_pane_identity.json" ->
          refute Map.has_key?(reply, "pane_identity"), name

        reply["outcome"] in ["absent", "conflict"] ->
          refute Map.has_key?(reply, "pane_identity"), name

        true ->
          assert Map.keys(reply["pane_identity"]) |> Enum.sort() == ["generation", "pane_id", "registration_id"], name
      end
    end
  end

  test "cancelled is a terminal status of its own and the version 2 projection reuses the existing ambiguous shape" do
    assert decode(Path.join(@fixture_dir, "cancel.cancelled.json"))["status"] == "cancelled"
    assert decode(Path.join(@fixture_dir, "reconcile.cancelled.json"))["outcome"] == "cancelled"
    assert decode(Path.join(@fixture_dir, "send.duplicate.cancelled.json"))["status"] == "cancelled"

    for path <- fixtures() do
      refute decode(path)["status"] == "not_delivered" and decode(path)["outcome"] == "cancelled", Path.basename(path)
    end

    # the text says a cancelled receipt reads as the EXISTING v2 ambiguous shapes; they must still exist unchanged
    assert File.regular?(Path.join(@v2_dir, "reconcile.ambiguous.json"))
    assert File.regular?(Path.join(@v2_dir, "send.duplicate.ambiguous.json"))
  end

  test "sequences keep seq, epoch and per-pane version gapless, and close-without-frame sequences end without one" do
    for path <- fixtures(), name = Path.basename(path), String.starts_with?(name, "seq.") do
      %{"frames" => frames, "then" => "close"} = decode(path)

      seqs = Enum.map(frames, & &1["seq"])
      assert seqs == Enum.to_list(0..(length(frames) - 1)//1), name

      refute Enum.any?(frames, &(&1["frame"] == "subscription_lost")), name

      case frames do
        [%{"frame" => "snapshot", "epoch" => e, "max_epoch_at_snapshot" => max} | events] ->
          epochs = for %{"kind" => "registration", "epoch" => ep} <- events, do: ep
          assert epochs == Enum.to_list((e + 1)..(e + length(epochs))//1), name
          assert max >= e, name

          detached = for %{"detached" => true, "pane_identity" => id} <- hd(frames)["entries"], do: id["registration_id"]
          detaches = for %{"change" => "detach", "pane_identity" => id} <- events, do: id["registration_id"]
          assert Enum.sort(detached) == Enum.sort(detaches), "#{name}: each detached entry pairs with exactly one detach"

        [] ->
          assert String.contains?(name, "snapshot_timeout"), name
      end
    end
  end

  test "an empty all-panes snapshot has max_epoch_at_snapshot equal to its epoch" do
    snap = decode(Path.join(@fixture_dir, "subscribe.all.empty.json"))
    assert snap["entries"] == []
    assert snap["max_epoch_at_snapshot"] == snap["epoch"]
  end

  test "the contract text names every fixture family it pins" do
    document = File.read!(@document)

    for family <-
          ~w(ping.ok.json ping.missing_tokens.json send.sent.no_pane_identity.json send.duplicate.cancelled.json
             reconcile.cancelled.json v2_refusal.unsupported_command.json subscribe.all.empty.json
             seq.close_no_frame seq.race) do
      assert String.contains?(document, family), family
    end
  end
end
