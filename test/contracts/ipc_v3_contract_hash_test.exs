defmodule AiOrchestrator.Contracts.IpcV3ContractHashTest do
  @moduledoc """
  IPC protocol version 3 example fixtures (`docs/contracts/ipc-v3.org`): pane identity on
  pane-bound replies, per-pane status, cancel and its cancelled receipt status, the
  subscribe frames and ordered sequences, version 2 refusals of version 3 commands, and
  the version 2 projection of a cancelled receipt. This repository pins the fixture bytes
  and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes NUL, byte-sorted).

  The identity core is implemented on both sides and paired (NS-15.G.002 B1c); cancel and
  subscribe remain specified only. Release (NS-15.G.003 S3, Charles decisions 49 and 50) is
  implemented by the paired producer (S3a), whose ping is ping.ok.identity_core_release.json;
  this consumer does not send it, and its request and reply files remain examples. These
  rows check the fixture set, its agreement with the text and the paired block's claims;
  they run no daemon.

  The identity core (ping, status, send and reconcile at version 3, the
  pane_identity_unavailable refusal and the version 2 refusals) splits the set into core
  replies, client requests and examples; every file is in exactly one class, and only the
  core replies (and exercised client requests) may later be claimed by a pairing.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "56cbc3257efa181fa4c9715528c4d02eb15dcadc4c101d373035c8032f65c592"
  @expected_fixture_count 59
  @document Path.expand("../../docs/contracts/ipc-v3.org", __DIR__)
  @v2_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)

  # Every fixture file is named here exactly once, in one of two classes, so a new file cannot pass unclassified.
  @with_identity ~w(
    send.sent.json send.queued.json send.duplicate.cancelled.json
    status.ok.json status.quarantined.json reconcile.queued.json reconcile.delivered.json
    cancel.cancelled.json cancel.too_late.json cancel.ambiguous.json reconcile.cancelled.json
    event.pane_state.json event.receipt.json event.pane_gone.json
    event.registration.attach.json event.registration.detach.json
    release.released.json
  )
  @without_identity ~w(
    ping.ok.json ping.missing_tokens.json send.sent.no_pane_identity.json status.error.pane_not_found.json
    cancel.absent.json cancel.conflict.json subscription_lost.overflow.json subscription_lost.snapshot_timeout.json
    v2_request.subscribe.json v2_reply.subscribe.unsupported_command.json
    v2_request.cancel.json v2_reply.cancel.unsupported_command.json
    scenario.cancelled.v2_reconcile.json scenario.cancelled.v2_send.json
    ping.ok.identity_core.json status.error.pane_identity_unavailable.json
    send.error.pane_identity_unavailable.json reconcile.error.pane_identity_unavailable.json
    ping.ok.identity_core_release.json v3_request.release.json
    release.error.pane_identity_unavailable.json release.error.effect_unresolved.json
    release.error.release_fence_unavailable.json release.error.release_stop_failed.json
    release.error.release_failed_requarantined.json release.error.release_unstarted.json
    v2_request.release.json v2_reply.release.unsupported_command.json
    ping.ok.identity_core_release_build.json ping.ok.identity_core_release_build_dirty.json
  )
  @release_refusals ~w(
    pane_identity_unavailable effect_unresolved release_fence_unavailable release_stop_failed
    release_failed_requarantined release_unstarted
  )
  # The identity-core classes; every file not named here is an example.
  @core_replies ~w(
    ping.ok.identity_core_release.json send.sent.json send.queued.json
    status.ok.json status.quarantined.json status.error.pane_not_found.json
    reconcile.queued.json reconcile.delivered.json
    status.error.pane_identity_unavailable.json send.error.pane_identity_unavailable.json
    reconcile.error.pane_identity_unavailable.json
    v2_reply.subscribe.unsupported_command.json v2_reply.cancel.unsupported_command.json
  )
  @client_requests ~w(v2_request.subscribe.json v2_request.cancel.json)
  @build_identity_pings ~w(ping.ok.identity_core_release_build.json ping.ok.identity_core_release_build_dirty.json)
  @identity_refusals ~w(
    status.error.pane_identity_unavailable.json send.error.pane_identity_unavailable.json
    reconcile.error.pane_identity_unavailable.json
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

      identity =
        if name == "event.registration.attach.json", do: reply["entry"]["pane_identity"], else: reply["pane_identity"]

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
    for cmd <- ["subscribe", "cancel", "release"] do
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

          assert Enum.sort(detached) == Enum.sort(detaches),
                 "#{name}: each detached entry pairs with exactly one detach"

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

  test "the identity-core classes partition the set: 13 core replies, 2 client requests, 44 examples" do
    examples = names() -- (@core_replies ++ @client_requests)

    assert length(Enum.uniq(@core_replies)) == 13
    assert length(Enum.uniq(@client_requests)) == 2
    assert length(examples) == 44
    assert Enum.sort(@core_replies ++ @client_requests ++ examples) == names()

    for name <- ["ping.ok.json", "ping.missing_tokens.json", "send.sent.no_pane_identity.json"] do
      assert name in examples, name
    end

    for name <- @core_replies ++ @client_requests,
        prefix <- ~w(cancel. subscribe. event. subscription_lost. seq. scenario. release. v3_request.) do
      refute String.starts_with?(name, prefix), "#{name} is outside the identity core"
    end

    for name <- @core_replies do
      reply = decode(name)
      refute reply["outcome"] == "cancelled" or reply["status"] == "cancelled", name
    end
  end

  test "the example set table names every core reply and client request by its full file name and class" do
    rows = @document |> File.read!() |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "|"))

    named? = fn name, class ->
      Enum.any?(rows, &(String.contains?(&1, "~#{name}~") and &1 =~ ~r/\|\s*#{class}\s*\|/))
    end

    for name <- @core_replies, do: assert(named?.(name, "core reply"), name)
    for name <- @client_requests, do: assert(named?.(name, "client request"), name)
  end

  test "the pairing block claims exactly the core replies and the client requests, at one producer revision" do
    document = File.read!(@document)

    declared = fn key ->
      [[_, value]] = Regex.scan(~r/^- #{key}: =([^=\n]+)=$/m, document)
      value
    end

    assert declared.("paired_repository") == "orrisd"
    assert Regex.match?(~r/\A[0-9a-f]{40}\z/, declared.("paired_revision"))
    assert Enum.sort(String.split(declared.("paired_produced"))) == Enum.sort(@core_replies)
    assert Enum.sort(String.split(declared.("paired_exercised_requests"))) == Enum.sort(@client_requests)
  end

  test "an identity-core daemon without release (example) advertises pane_identity and neither cancel nor subscribe" do
    tokens = decode("ping.ok.identity_core.json")["capabilities"]

    assert "pane_identity" in tokens
    refute "cancel" in tokens
    refute "subscribe" in tokens
  end

  test "pane_identity_unavailable refuses status, send and reconcile with a typed, echoing, identity-free reply" do
    for name <- @identity_refusals do
      reply = decode(name)
      message_bound = not String.starts_with?(name, "status.")

      assert reply["ok"] == false and reply["error"] == "pane_identity_unavailable", name
      assert reply["protocol_version"] == 3 and reply["pane_id"] == "<pane_id>", name
      assert Map.has_key?(reply, "msg_id") == message_bound, name
      refute Map.has_key?(reply, "pane_identity"), name
    end
  end

  # structural equality with the decoded v2 fixture once the v3 additions are removed
  test "the version 3 reconcile queued and delivered replies are the version 2 replies plus pane_identity" do
    for outcome <- ["queued", "delivered"] do
      v3 = decode("reconcile.#{outcome}.json")
      v2 = @v2_dir |> Path.join("reconcile.#{outcome}.json") |> File.read!() |> Jason.decode!()

      assert v3 |> Map.delete("pane_identity") |> Map.put("protocol_version", 2) == v2, outcome
    end
  end

  test "the contract text names every fixture family it pins" do
    document = File.read!(@document)

    for family <-
          ~w(ping.ok.json ping.missing_tokens.json send.sent.no_pane_identity.json send.duplicate.cancelled.json
             reconcile.cancelled.json v2_request.subscribe.json v2_reply.cancel.unsupported_command.json
             scenario.cancelled.v2_reconcile.json subscribe.all.empty.json seq.close_no_frame seq.race
             ping.ok.identity_core.json reconcile.queued.json reconcile.delivered.json
             status.error.pane_identity_unavailable.json send.error.pane_identity_unavailable.json
             reconcile.error.pane_identity_unavailable.json ping.ok.identity_core_release.json
             v3_request.release.json release.released.json v2_request.release.json
             v2_reply.release.unsupported_command.json) do
      assert String.contains?(document, family), family
    end

    for refusal <- @release_refusals do
      assert String.contains?(document, "~release.error.#{refusal}.json~"), refusal
    end
  end

  test "a release-capable identity-core ping advertises release beside pane_identity; the identity-core ping does not" do
    tokens = decode("ping.ok.identity_core_release.json")["capabilities"]

    assert "release" in tokens and "pane_identity" in tokens
    refute "cancel" in tokens
    refute "subscribe" in tokens
    refute "release" in decode("ping.ok.identity_core.json")["capabilities"]
    assert "release" in decode("ping.ok.json")["capabilities"]
  end

  test "build_identity is present exactly when its token is, in every version 3 ping fixture" do
    for name <- names(), String.starts_with?(name, "ping."), decode(name)["protocol_version"] == 3 do
      ping = decode(name)
      advertised? = "build_identity" in ping["capabilities"]
      assert advertised? == Map.has_key?(ping, "build_identity"), name
    end

    assert Map.has_key?(decode("ping.ok.identity_core_release_build.json"), "build_identity")
    refute Map.has_key?(decode("ping.ok.identity_core_release.json"), "build_identity")
  end

  test "a build_identity object has exactly its eight typed keys" do
    for name <- @build_identity_pings do
      identity = decode(name)["build_identity"]

      assert Enum.sort(Map.keys(identity)) ==
               ~w(build_id clean ipc_protocols name rollback_eligible source_nar_hash source_revision version),
             name

      assert is_binary(identity["name"]) and is_binary(identity["version"]), name
      assert is_boolean(identity["clean"]) and is_boolean(identity["rollback_eligible"]), name
      assert identity["build_id"] =~ ~r/\A[0-9a-f]{64}\z/, name
      assert String.starts_with?(identity["source_nar_hash"], "sha256-"), name
      assert is_list(identity["ipc_protocols"]) and Enum.all?(identity["ipc_protocols"], &is_integer/1), name
      assert is_nil(identity["source_revision"]) or identity["source_revision"] =~ ~r/\A[0-9a-f]{40}\z/, name
    end
  end

  test "rollback_eligible is true exactly for a clean build with a 40-hex source revision" do
    for name <- @build_identity_pings do
      identity = decode(name)["build_identity"]
      revision = identity["source_revision"]
      expected = identity["clean"] == true and is_binary(revision) and revision =~ ~r/\A[0-9a-f]{40}\z/
      assert identity["rollback_eligible"] == expected, name
    end

    assert decode("ping.ok.identity_core_release_build.json")["build_identity"]["rollback_eligible"] == true
    assert decode("ping.ok.identity_core_release_build_dirty.json")["build_identity"]["rollback_eligible"] == false
  end

  test "the release request names only the pane, and success carries the identity and matched/held counts" do
    assert decode("v3_request.release.json") == %{"cmd" => "release", "pane_id" => "<pane_id>", "protocol_version" => 3}

    reply = decode("release.released.json")
    assert reply["ok"] == true and reply["released"] == true and reply["pane_id"] == "<pane_id>"
    assert identity_ok?(reply["pane_identity"])
    assert %{"matched" => m, "held" => h} = reply["counts"]
    assert map_size(reply["counts"]) == 2 and is_integer(m) and is_integer(h) and m >= 0 and h >= 0
  end

  test "each release refusal is typed, echoes pane_id, and carries no identity" do
    for refusal <- @release_refusals do
      reply = decode("release.error.#{refusal}.json")

      assert reply == %{
               "error" => refusal,
               "ok" => false,
               "pane_id" => "<pane_id>",
               "protocol_version" => 3
             },
             refusal
    end

    on_disk = for name <- names(), String.starts_with?(name, "release.error."), do: name
    assert Enum.sort(on_disk) == Enum.sort(Enum.map(@release_refusals, &"release.error.#{&1}.json"))
  end
end
