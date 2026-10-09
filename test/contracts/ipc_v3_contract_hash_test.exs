defmodule AiOrchestrator.Contracts.IpcV3ContractHashTest do
  @moduledoc """
  IPC protocol version 3 example fixtures (`docs/contracts/ipc-v3.org`): pane identity on
  pane-bound replies, per-pane status, cancel and its cancelled receipt status, the
  subscribe frames and ordered sequences, version 2 refusals of version 3 commands, and
  the version 2 projection of a cancelled receipt. This repository pins the fixture bytes
  and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes NUL, byte-sorted).

  The identity core is implemented on both sides and paired (NS-15.G.002 B1c); cancel and
  subscribe remain specified only. Release (NS-15.G.003 S3, Charles decisions 49 and 50) is
  implemented by the paired producer (S3a), whose ping is ping.ok.identity_core_release.json,
  or ping.ok.identity_core_release_build.json when it read a build identity record (RB-1);
  this consumer does not send it, and its request and reply files remain examples except the
  claimed release.error.quiescing.json. Quiesce and resume (NS-32.M.002 RB-3a) are implemented
  by the producer (Orrisd); the reviewed producer witnesses (W, Orrisd e9d64b4a) support the
  claimed quiesce/resume/quiescing replies and the two admitted durable pings
  ping.ok.identity_core_release_quiesce.json and ping.ok.identity_core_release_build_quiesce.json,
  whose bytes equal the reviewed W candidates. This consumer issues neither quiesce nor resume
  (the sole client is the producer's generation operator); quiesce.ok.json stays an example,
  and no installed daemon is claimed. The producer vendors the 77-file set at d5de930f, so
  vendoring of this 79-file set is pending (RB-3a-P P2). These rows check the fixture set, its
  agreement with the text and the paired block's claims; they run no daemon.

  The identity core (ping, status, send and reconcile at version 3, the
  pane_identity_unavailable refusal and the version 2 refusals) and the claimed quiesce,
  resume and quiescing replies split the set into 26 core replies, 6 client requests and 47
  examples; every file is in exactly one class, and only the core replies (and exercised
  client requests) may be claimed by a pairing.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "e86bca9b1464621d3e110b85f2d208df2b3abc030270f9a53e5cdc4f070f3800"
  @expected_fixture_count 79
  @document Path.expand("../../docs/contracts/ipc-v3.org", __DIR__)
  @v2_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @v1_dir Path.expand("../fixtures/contracts/ipc/v1", __DIR__)

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
    ping.ok.identity_core_release_quiesce.json ping.ok.identity_core_release_build_quiesce.json
    v3_request.quiesce.json quiesce.ok.json v3_request.resume.json resume.ok.json resume.error.fence_mismatch.json
    quiesce.error.quiesce_busy.json quiesce.error.quiesce_timeout.json quiesce.error.observation_incomplete.json
    quiesce.error.invalid_request.json ping.ok.quiesced.json
    send.error.quiescing.json release.error.quiescing.json v2_reply.send.quiescing.json v1_reply.quiescing.json
    v2_request.quiesce.json v2_reply.quiesce.unsupported_command.json
    v2_request.resume.json v2_reply.resume.unsupported_command.json
  )
  @release_refusals ~w(
    pane_identity_unavailable effect_unresolved release_fence_unavailable release_stop_failed
    release_failed_requarantined release_unstarted quiescing
  )
  @quiesce_refusals ~w(quiesce_busy quiesce_timeout observation_incomplete invalid_request)
  @observation_keys ~w(effects lineage pane_intent payloads receipts session_marker)
  # The identity-core classes; every file not named here is an example.
  @core_replies ~w(
    ping.ok.identity_core_release.json ping.ok.identity_core_release_build.json
    send.sent.json send.queued.json
    status.ok.json status.quarantined.json status.error.pane_not_found.json
    reconcile.queued.json reconcile.delivered.json
    status.error.pane_identity_unavailable.json send.error.pane_identity_unavailable.json
    reconcile.error.pane_identity_unavailable.json
    v2_reply.subscribe.unsupported_command.json v2_reply.cancel.unsupported_command.json
    ping.ok.identity_core_release_quiesce.json ping.ok.identity_core_release_build_quiesce.json
    quiesce.error.invalid_request.json quiesce.error.observation_incomplete.json quiesce.error.quiesce_busy.json
    quiesce.error.quiesce_timeout.json resume.ok.json resume.error.fence_mismatch.json
    send.error.quiescing.json release.error.quiescing.json
    v2_reply.quiesce.unsupported_command.json v2_reply.resume.unsupported_command.json
  )
  @client_requests ~w(
    v2_request.subscribe.json v2_request.cancel.json v3_request.quiesce.json v3_request.resume.json
    v2_request.quiesce.json v2_request.resume.json
  )
  # The only core/client names a refused identity-core prefix may admit: the claimed quiesce, resume and quiescing
  # replies and the two version 3 requests (RB-3a-P P1). Any other match is a promotion this list never reviewed.
  @guard_exceptions ~w(
    quiesce.error.invalid_request.json quiesce.error.observation_incomplete.json quiesce.error.quiesce_busy.json
    quiesce.error.quiesce_timeout.json resume.ok.json resume.error.fence_mismatch.json release.error.quiescing.json
    v3_request.quiesce.json v3_request.resume.json
  )
  @guard_prefixes ~w(
    cancel. subscribe. event. subscription_lost. seq. scenario. release. v3_request. quiesce. resume. v1_
  )
  # The reviewed producer witness candidates (RB-3a-P P2-W, Orrisd e9d64b4a) these core pings copy byte for byte.
  @w_revision "e9d64b4afa2cac9bf69113d4d9bec7ca76ad8829"
  @w_candidates %{
    "ping.ok.identity_core_release_quiesce.json" => "4c837bffcf13b243ddddf6e4159b79028f9901abad1434cd51c78587d8a906f1",
    "ping.ok.identity_core_release_build_quiesce.json" =>
      "2cbdeaaf240fdff75efa4994dbec8cdcbb3ae337e6165517b8efc24cd43d1af0"
  }
  @build_identity_pings ~w(
    ping.ok.identity_core_release_build.json ping.ok.identity_core_release_build_dirty.json
    ping.ok.identity_core_release_build_quiesce.json
  )
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
        String.starts_with?(name, "v1_") -> refute Map.has_key?(reply, "protocol_version"), name
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
    for cmd <- ["subscribe", "cancel", "release", "quiesce", "resume"] do
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

  test "the identity-core classes partition the set: 26 core replies, 6 client requests, 47 examples" do
    examples = names() -- (@core_replies ++ @client_requests)

    assert length(Enum.uniq(@core_replies)) == 26
    assert length(Enum.uniq(@client_requests)) == 6
    assert length(examples) == 47
    assert Enum.sort(@core_replies ++ @client_requests ++ examples) == names()
  end

  # pure lists, no fixture read: runs on its own so the guard is checked even when the set is incomplete
  test "only the reviewed quiesce, resume and quiescing names pass the identity-core prefix guard" do
    claimed = @core_replies ++ @client_requests
    matched = Enum.filter(claimed, fn name -> Enum.any?(@guard_prefixes, &String.starts_with?(name, &1)) end)

    assert Enum.sort(matched) == Enum.sort(@guard_exceptions)

    for name <- claimed -- @guard_exceptions, prefix <- @guard_prefixes do
      refute String.starts_with?(name, prefix), "#{name} is outside the identity core"
    end

    for name <- ["ping.ok.json", "ping.missing_tokens.json", "send.sent.no_pane_identity.json"] do
      refute name in claimed, name
    end
  end

  test "no core reply is a cancelled outcome" do
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

  test "no file name has rows of two classes, and no glob row covers a core reply or client request" do
    rows = @document |> File.read!() |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "|"))
    classes = ["core reply", "client request", "example"]

    named =
      for row <- rows,
          [_, name] <- Regex.scan(~r/~([^~]+\.json)~/, row),
          class <- classes,
          row =~ ~r/\|\s*#{class}\s*\|/,
          do: {name, class}

    for {name, entries} <- Enum.group_by(named, &elem(&1, 0), &elem(&1, 1)) do
      assert length(Enum.uniq(entries)) == 1, "#{name} has rows of classes #{inspect(Enum.uniq(entries))}"
    end

    globs = for {name, _class} <- named, String.contains?(name, "*"), do: name

    for glob <- globs, name <- @core_replies ++ @client_requests do
      pattern = ~r/\A#{glob |> Regex.escape() |> String.replace("\\*", "[^~|]*")}\z/
      refute name =~ pattern, "#{glob} covers the claimed #{name}"
    end
  end

  test "the pairing block claims exactly the core replies and the client requests, at one producer revision" do
    document = File.read!(@document)

    declared = fn key ->
      [[_, value]] = Regex.scan(~r/^- #{key}: =([^=\n]+)=$/m, document)
      value
    end

    assert declared.("paired_repository") == "orrisd"
    assert declared.("paired_revision") == @w_revision
    assert declared.("paired_source_revision") == "d5de930f4f068dc47f654e2d7db08c38ef82b777"
    assert Enum.sort(String.split(declared.("paired_produced"))) == Enum.sort(@core_replies)
    assert Enum.sort(String.split(declared.("paired_exercised_requests"))) == Enum.sort(@client_requests)
    assert declared.("paired_vendoring") == "pending"

    # the claimed admitted pings are the reviewed W candidates, byte for byte
    for {key, name} <- [
          {"paired_candidate_quiesce_sha256", "ping.ok.identity_core_release_quiesce.json"},
          {"paired_candidate_build_quiesce_sha256", "ping.ok.identity_core_release_build_quiesce.json"}
        ] do
      bytes = @fixture_dir |> Path.join(name) |> File.read!()
      assert declared.(key) == Map.fetch!(@w_candidates, name), key
      assert :sha256 |> :crypto.hash(bytes) |> Base.encode16(case: :lower) == Map.fetch!(@w_candidates, name), name
    end
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
             v2_reply.release.unsupported_command.json v3_request.quiesce.json quiesce.ok.json
             v3_request.resume.json resume.ok.json resume.error.fence_mismatch.json ping.ok.quiesced.json
             send.error.quiescing.json v2_reply.send.quiescing.json v1_reply.quiescing.json
             v2_request.quiesce.json v2_reply.quiesce.unsupported_command.json v2_request.resume.json
             v2_reply.resume.unsupported_command.json ping.ok.identity_core_release_quiesce.json
             ping.ok.identity_core_release_build_quiesce.json) do
      assert String.contains?(document, family), family
    end

    for refusal <- @release_refusals do
      assert String.contains?(document, "~release.error.#{refusal}.json~"), refusal
    end

    for refusal <- @quiesce_refusals do
      assert String.contains?(document, "~quiesce.error.#{refusal}.json~"), refusal
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

  test "the build-identity core reply is the no-record core ping plus its token and object, nothing else" do
    build = decode("ping.ok.identity_core_release_build.json")
    plain = decode("ping.ok.identity_core_release.json")

    assert "ping.ok.identity_core_release_build.json" in @core_replies
    refute "ping.ok.identity_core_release_build_dirty.json" in @core_replies

    assert build
           |> Map.delete("build_identity")
           |> Map.update!("capabilities", &(&1 -- ["build_identity"])) == plain
  end

  test "each admitted durable ping is its claimed counterpart plus the quiesce token, nothing else" do
    for {quiesce, counterpart} <- [
          {"ping.ok.identity_core_release_quiesce.json", "ping.ok.identity_core_release.json"},
          {"ping.ok.identity_core_release_build_quiesce.json", "ping.ok.identity_core_release_build.json"}
        ] do
      ping = decode(quiesce)

      assert quiesce in @core_replies and counterpart in @core_replies, quiesce
      assert "quiesce" in ping["capabilities"], quiesce
      refute Map.has_key?(ping, "quiesced") or Map.has_key?(ping, "fence_id"), quiesce
      assert Map.update!(ping, "capabilities", &(&1 -- ["quiesce"])) == decode(counterpart), quiesce
    end
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

  test "the quiesce request carries only the resume digest, and success carries the fence id and a six-key observation" do
    assert decode("v3_request.quiesce.json") == %{
             "cmd" => "quiesce",
             "protocol_version" => 3,
             "resume_hash" => "<resume_hash>"
           }

    reply = decode("quiesce.ok.json")
    assert Enum.sort(Map.keys(reply)) == ~w(fence_id observation ok protocol_version quiesced)
    assert reply["ok"] == true and reply["quiesced"] == true and reply["fence_id"] == "<fence_id>"

    observation = reply["observation"]
    assert Enum.sort(Map.keys(observation)) == @observation_keys

    assert %{"versions" => versions, "queued" => queued, "pending" => pending} = observation["receipts"]
    assert map_size(observation["receipts"]) == 3 and Enum.all?(versions, &is_integer/1)
    assert is_integer(queued) and is_integer(pending)
    assert %{"version" => intent, "live_panes" => live} = observation["pane_intent"]
    assert map_size(observation["pane_intent"]) == 2 and is_binary(intent) and is_integer(live)
    assert %{"version" => effects, "unresolved_holds" => holds} = observation["effects"]
    assert map_size(observation["effects"]) == 2 and is_integer(effects) and is_integer(holds)
    assert %{"version" => lineage, "attested" => attested} = observation["lineage"]
    assert map_size(observation["lineage"]) == 2 and is_integer(lineage) and is_boolean(attested)
    assert %{"layouts" => layouts} = observation["payloads"]
    assert map_size(observation["payloads"]) == 1 and Enum.all?(layouts, &is_integer/1)
    assert %{"version" => marker} = observation["session_marker"]
    assert map_size(observation["session_marker"]) == 1 and is_integer(marker)
  end

  test "each quiesce refusal is typed and carries only its stated detail" do
    details = %{"quiesce_timeout" => ["bound_ms"], "observation_incomplete" => ["dimension"]}

    for refusal <- @quiesce_refusals do
      reply = decode("quiesce.error.#{refusal}.json")
      extra = Map.get(details, refusal, [])

      assert Enum.sort(Map.keys(reply)) == Enum.sort(~w(error ok protocol_version) ++ extra), refusal
      assert reply["ok"] == false and reply["error"] == refusal and reply["protocol_version"] == 3, refusal
    end

    assert is_integer(decode("quiesce.error.quiesce_timeout.json")["bound_ms"])
    assert decode("quiesce.error.observation_incomplete.json")["dimension"] in @observation_keys

    on_disk = for name <- names(), String.starts_with?(name, "quiesce.error."), do: name
    assert Enum.sort(on_disk) == Enum.sort(Enum.map(@quiesce_refusals, &"quiesce.error.#{&1}.json"))
  end

  test "resume names the fence and the secret; success and the fence_mismatch refusal say nothing else" do
    assert decode("v3_request.resume.json") == %{
             "cmd" => "resume",
             "fence_id" => "<fence_id>",
             "protocol_version" => 3,
             "resume_secret" => "<resume_secret>"
           }

    assert decode("resume.ok.json") == %{"ok" => true, "protocol_version" => 3, "resumed" => true}

    assert decode("resume.error.fence_mismatch.json") == %{
             "error" => "fence_mismatch",
             "ok" => false,
             "protocol_version" => 3
           }
  end

  test "quiesced and fence_id appear together and only while quiesced; no ping carries the digest or the secret" do
    for name <- names(), String.starts_with?(name, "ping."), decode(name)["protocol_version"] == 3 do
      ping = decode(name)
      assert Map.has_key?(ping, "quiesced") == Map.has_key?(ping, "fence_id"), name
      refute Map.has_key?(ping, "resume_hash") or Map.has_key?(ping, "resume_secret"), name
    end

    quiesced = decode("ping.ok.quiesced.json")
    plain = decode("ping.ok.identity_core_release.json")

    assert quiesced["quiesced"] == true and quiesced["fence_id"] == "<fence_id>"

    assert quiesced
           |> Map.drop(["quiesced", "fence_id"])
           |> Map.update!("capabilities", &(&1 -- ["quiesce"])) == plain

    assert "quiesce" in decode("ping.ok.json")["capabilities"]
    refute "quiesce" in plain["capabilities"]
  end

  test "the version 2 quiescing rule preserves an existing receipt rather than claiming a later absent" do
    v2_text = File.read!(Path.expand("../../docs/contracts/ipc-v2.org", __DIR__))
    [_, section] = String.split(v2_text, "*** Next paired send refusal: quiescing", parts: 2)
    [section | _] = String.split(section, "\n** ", parts: 2)
    words = section |> String.split() |> Enum.join(" ")

    assert words =~ "admits no NEW receipt and never erases, changes or reclassifies a receipt that already exists"
    assert words =~ "~absent~ only if there was no receipt before the refused send"
    refute words =~ "so a later reconcile answers ~absent~"
  end

  test "the quiescing refusal has the shape of each protocol version" do
    assert decode("send.error.quiescing.json") == %{
             "error" => "quiescing",
             "msg_id" => "<msg_id>",
             "ok" => false,
             "pane_id" => "<pane_id>",
             "protocol_version" => 3
           }

    v2_shape = @v2_dir |> Path.join("send.error.pane_quarantined.json") |> File.read!() |> Jason.decode!()
    assert decode("v2_reply.send.quiescing.json") == %{v2_shape | "error" => "quiescing"}
    assert decode("v1_reply.quiescing.json") == %{"error" => "quiescing", "ok" => false}

    # the owning v1 and v2 sets hold exactly these example bytes as their own send.error.quiescing.json
    for {dir, example} <- [{@v1_dir, "v1_reply.quiescing.json"}, {@v2_dir, "v2_reply.send.quiescing.json"}] do
      owned = dir |> Path.join("send.error.quiescing.json") |> File.read!()
      assert owned == @fixture_dir |> Path.join(example) |> File.read!(), example
    end
  end
end
