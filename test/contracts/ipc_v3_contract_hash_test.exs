defmodule AiOrchestrator.Contracts.IpcV3ContractHashTest do
  @moduledoc """
  IPC protocol version 3 example fixtures (`docs/contracts/ipc-v3.org`): pane identity on
  pane-bound replies, per-pane status, cancel and its cancelled receipt status, the
  subscribe frames and ordered sequences, version 2 refusals of version 3 commands, and
  the version 2 projection of a cancelled receipt. This repository pins the fixture bytes
  and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes NUL, byte-sorted).

  Version 3 is specified, not implemented, on either side. These rows check the example
  set and its agreement with the text; they say nothing about any daemon. There is no
  pairing row yet: the producer has not vendored this document, and the paired block is
  added in that re-pairing.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "5f82237167ec609ef6dd0eb6de6754a5a233375c9baa737e99f9ceed6b0c0a3b"
  @expected_fixture_count 40
  @document Path.expand("../../docs/contracts/ipc-v3.org", __DIR__)
  @v2_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)

  # Every fixture file is named here exactly once, in one of two classes, so a new file cannot pass unclassified.
  @with_identity ~w(
    send.sent.json send.queued.json send.duplicate.cancelled.json
    status.ok.json status.quarantined.json
    cancel.cancelled.json cancel.too_late.json cancel.ambiguous.json reconcile.cancelled.json
    event.pane_state.json event.receipt.json event.pane_gone.json
    event.registration.attach.json event.registration.detach.json
  )
  @without_identity ~w(
    ping.ok.json ping.missing_tokens.json send.sent.no_pane_identity.json status.error.pane_not_found.json
    cancel.absent.json cancel.conflict.json subscription_lost.overflow.json subscription_lost.snapshot_timeout.json
    v2_request.subscribe.json v2_reply.subscribe.unsupported_command.json
    v2_request.cancel.json v2_reply.cancel.unsupported_command.json
    scenario.cancelled.v2_reconcile.json scenario.cancelled.v2_send.json
  )
  @snapshots ~w(
    subscribe.pane.snapshot.json subscribe.all.snapshot.json subscribe.all.empty.json
    subscribe.all.snapshot_with_detached.json subscribe.all.snapshot_detached_unknown_state.json
  )
  @registration_id ~r/\A(reg_[0-9a-f]{32}|<registration_id(_[0-9]+)?>)\z/
  @generation ~r/\A([0-9]+|<generation>)\z/

  defp fixtures, do: @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()
  defp names, do: Enum.map(fixtures(), &Path.basename/1)
  defp decode(name), do: @fixture_dir |> Path.join(name) |> File.read!() |> Jason.decode!()
  defp sequences, do: Enum.filter(names(), &String.starts_with?(&1, "seq."))

  # the daemon's pane-intent record validates session_gen as a decimal-digit string and compares it as text
  defp identity_ok?(%{"pane_id" => p, "registration_id" => r, "generation" => g} = id)
       when map_size(id) == 3 and is_binary(p) and is_binary(r) and is_binary(g),
       do: Regex.match?(@registration_id, r) and Regex.match?(@generation, g)

  defp identity_ok?(_), do: false

  test "the IPC v3 fixture set matches the pinned hash" do
    paths = fixtures()

    assert length(paths) == @expected_fixture_count, "IPC v3 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v3 CONTRACT_HASH is missing"

    payload = Enum.map(paths, fn path -> [Path.basename(path), 0, File.read!(path), 0] end)
    actual = :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)

    assert actual == @pinned_hash
    assert @hash_path |> File.read!() |> String.trim() == @pinned_hash
  end

  test "every fixture is classified exactly once" do
    classified = @with_identity ++ @without_identity ++ @snapshots ++ sequences()
    assert Enum.sort(classified) == names()
    assert length(Enum.uniq(classified)) == length(classified)
  end

  test "version 2 bytes appear only in the version 2 request/refusal pairs and inside the scenarios" do
    for name <- names() do
      reply = decode(name)

      cond do
        String.starts_with?(name, "v2_") -> assert reply["protocol_version"] == 2, name
        String.starts_with?(name, "scenario.") -> refute Map.has_key?(reply, "protocol_version"), name
        true -> assert reply["protocol_version"] == 3, name
      end
    end
  end

  test "pane identity is present and well formed exactly where the text requires it" do
    for name <- @with_identity do
      reply = decode(name)
      identity = if name == "event.registration.attach.json", do: reply["entry"]["pane_identity"], else: reply["pane_identity"]
      assert identity_ok?(identity), name
    end

    for name <- @without_identity do
      refute Map.has_key?(decode(name), "pane_identity"), name
    end

    for name <- @snapshots, entry <- decode(name)["entries"] do
      assert identity_ok?(entry["pane_identity"]), name
    end
  end

  test "a malformed registration id or generation is not an identity" do
    # 39 decimal digits: the widest generation the daemon mints from 16 random bytes
    good = %{
      "pane_id" => "<pane_id>",
      "registration_id" => "reg_" <> String.duplicate("a", 32),
      "generation" => "340282366920938463463374607431768211455"
    }

    assert identity_ok?(good)
    refute identity_ok?(%{good | "registration_id" => "reg_" <> String.duplicate("A", 32)})
    refute identity_ok?(%{good | "registration_id" => "reg_" <> String.duplicate("a", 31)})
    refute identity_ok?(%{good | "generation" => 1}), "a JSON number is not a generation"
    refute identity_ok?(%{good | "generation" => ""})
    refute identity_ok?(%{good | "generation" => "12a"})
    refute identity_ok?(Map.delete(good, "generation"))
  end

  test "each version 3 command named in a version 2 request is refused with a typed, echoing refusal" do
    for cmd <- ["subscribe", "cancel"] do
      request = decode("v2_request.#{cmd}.json")
      reply = decode("v2_reply.#{cmd}.unsupported_command.json")

      assert request["cmd"] == cmd
      assert reply["ok"] == false and reply["error"] == "unsupported_command" and reply["cmd"] == cmd

      for key <- ["msg_id", "pane_id"], Map.has_key?(request, key) do
        assert reply[key] == request[key], "#{cmd} echoes #{key}"
      end
    end
  end

  # structural equality with the decoded v2 fixture; the v2 fixture bytes themselves stay pinned by the v2 hash test
  test "a cancelled receipt read through version 2 is structurally the existing, unchanged ambiguous shape" do
    assert decode("cancel.cancelled.json")["status"] == "cancelled"
    assert decode("reconcile.cancelled.json")["outcome"] == "cancelled"
    assert decode("send.duplicate.cancelled.json")["status"] == "cancelled"

    for name <- ["scenario.cancelled.v2_reconcile.json", "scenario.cancelled.v2_send.json"] do
      scenario = decode(name)
      assert scenario["after"] == "cancel.cancelled.json", name
      assert File.regular?(Path.join(@fixture_dir, scenario["after"])), name
      assert scenario["request"]["protocol_version"] == 2, name

      assert scenario["projection_of"] in ["reconcile.ambiguous.json", "send.duplicate.ambiguous.json"], name
      v2 = @v2_dir |> Path.join(scenario["projection_of"]) |> File.read!() |> Jason.decode!()
      assert scenario["reply"] == v2, name
      assert scenario["reply"]["status"] == "ambiguous", name
    end
  end

  test "sequences keep seq, epoch and per-pane version gapless, and end in close with no final frame" do
    for name <- sequences() do
      %{"frames" => frames, "then" => "close"} = decode(name)

      assert Enum.map(frames, & &1["seq"]) == Enum.to_list(0..(length(frames) - 1)//1), name
      refute Enum.any?(frames, &(&1["frame"] == "subscription_lost")), name

      case frames do
        [%{"frame" => "snapshot", "epoch" => e, "max_epoch_at_snapshot" => max} = snap | events] ->
          epochs = for %{"kind" => "registration", "epoch" => ep} <- events, do: ep
          assert epochs == Enum.to_list((e + 1)..(e + length(epochs))//1), name
          assert max >= e, name

          detached = for %{"detached" => true, "pane_identity" => id} <- snap["entries"], do: id["registration_id"]
          detaches = for %{"change" => "detach", "pane_identity" => id} <- events, do: id["registration_id"]
          assert Enum.sort(detached) == Enum.sort(detaches), "#{name}: each detached entry pairs with exactly one detach"

        [%{"frame" => "snapshot"} | _] ->
          :ok

        [] ->
          assert String.contains?(name, "snapshot_timeout"), name
      end
    end
  end

  test "an empty all-panes snapshot has max_epoch_at_snapshot equal to its epoch" do
    snap = decode("subscribe.all.empty.json")
    assert snap["entries"] == []
    assert snap["max_epoch_at_snapshot"] == snap["epoch"]
  end

  test "the contract text names every fixture family it pins" do
    document = File.read!(@document)

    for family <-
          ~w(ping.ok.json ping.missing_tokens.json send.sent.no_pane_identity.json send.duplicate.cancelled.json
             reconcile.cancelled.json v2_request.subscribe.json v2_reply.cancel.unsupported_command.json
             scenario.cancelled.v2_reconcile.json subscribe.all.empty.json seq.close_no_frame seq.race) do
      assert String.contains?(document, family), family
    end
  end
end
