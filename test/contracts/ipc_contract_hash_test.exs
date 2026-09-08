defmodule AiOrchestrator.Contracts.IPCContractHashTest do
  @moduledoc """
  IPC v1 reply fixtures shared with the coordination runtime. This repository pins
  the fixture bytes and CONTRACT_HASH. The hash rule is the one
  documented in docs/contracts/ipc-v1.md: lowercase SHA-256 over, for each *.json file in
  byte-sorted filename order, filename NUL bytes NUL.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v1", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")

  # Imported from the coordination runtime's IPC v1 fixture set.
  # Hash changes require coordinated protocol review in both repositories.
  @pinned_hash "f1cacf8b53fdd1db37ec968e5476081250804e9c6a4d615215d47d9b77894213"
  @expected_fixture_count 15

  test "the IPC v1 fixture set matches the pinned cross-repository hash" do
    paths = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()

    assert length(paths) == @expected_fixture_count, "IPC v1 fixture set is missing or incomplete"
    assert File.regular?(@hash_path), "IPC v1 CONTRACT_HASH is missing"

    payload = Enum.map(paths, fn path -> [Path.basename(path), 0, File.read!(path), 0] end)
    actual = :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)

    assert actual == @pinned_hash
    assert @hash_path |> File.read!() |> String.trim() == @pinned_hash
  end
end
