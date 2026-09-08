defmodule AiOrchestrator.Contracts.IpcV2ContractHashTest do
  @moduledoc """
  IPC protocol version 2 reply fixtures shared with the coordination runtime:
  send replies with explicit `protocol_version`, duplicate views (`duplicate: true`
  beside the receipt status, attempt and identities -- never a status string), the
  five reconcile outcomes, and the ping capability reply. This repository pins the
  fixture bytes and CONTRACT_HASH under the v1 rule (sha256 over filename NUL bytes
  NUL, byte-sorted). The v1 set is pinned separately. These tests verify the fixtures;
  they do not establish the capabilities of an installed daemon.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "78c2f64240c3c5c9da60425c65c498974a2a81c8adb3e68e0bef28613c1707dc"
  @expected_fixture_count 16

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
end
