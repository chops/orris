defmodule AiOrchestrator.Contracts.IpcV2SessionsContractHashTest do
  @moduledoc """
  The canonical examples for the `sessions` read surface specified in
  docs/contracts/ipc-v2.org, pinned under test/fixtures/contracts/ipc/v2-sessions/ with
  their own CONTRACT_HASH under the v1 hash rule.

  They are deliberately NOT in the paired v2 directory. That set is byte-identical with
  the producer snapshot the contract's pairing block names, and no producer emits a
  sessions reply yet, so an example dropped in beside the delivery fixtures would make
  the pairing block assert an equality that is false.

  The second reason belongs to the PRODUCER, not to the tests in this repository. The
  producer's delivery-fixture harness (Orrisd test/ai_pair/ipc/contract_v2_fixture_test.exs
  :94-116 at 1018ad9b) enumerates every JSON file in that directory and dispatches each
  through `Delivery.dispatch`, so a sessions example placed there would be driven down
  the delivery route on vendoring. The consumer tests here select `send.*` and
  `reconcile.*` by glob and would not do that; the rationale is cross-repository.

  This module therefore pins the sessions set on its own, and one row holds the paired
  set to the bytes and hash it had before this lane existed -- so that an edit which
  quietly advertises the new capability in the paired ping fails here too.

  Nothing in this module establishes that a daemon answers `sessions`. It measures the
  examples and the document, which is all a consumer repository can measure without the
  producer.
  """

  use ExUnit.Case, async: true

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v2-sessions", __DIR__)
  @hash_path Path.join(@fixture_dir, "CONTRACT_HASH")
  @pinned_hash "5d81dcb70a06c1219854992abeabbd13a1db709dd9fa04d3d8331cc33f01271e"
  @declared_dir "test/fixtures/contracts/ipc/v2-sessions/"

  # exact, not a count: a file added or removed without this list moving is the drift the
  # hash alone would report as an opaque mismatch
  @expected_files [
    "oversize.json",
    "sessions.error.invalid_sessions_census.json",
    "sessions.error.invalid_sessions_request.json",
    "sessions.error.sessions_unavailable.json",
    "sessions.ok.duplicate_names.json",
    "sessions.ok.empty.json",
    "sessions.ok.escaped_names.json",
    "sessions.ok.linked_window.json",
    "sessions.ok.mixed_registration.json",
    "sessions.ok.multi_address_consistency.json",
    "sessions.ok.multiple_sessions.json",
    "sessions.ok.ordering.json"
  ]

  # the paired set, held here to what it was: this lane specifies the capability and does
  # not advertise it
  @paired_dir Path.expand("../fixtures/contracts/ipc/v2", __DIR__)
  @paired_hash "78c2f64240c3c5c9da60425c65c498974a2a81c8adb3e68e0bef28613c1707dc"
  @paired_capabilities ["delivery_reconcile"]

  @document Path.expand("../../docs/contracts/ipc-v2.org", __DIR__)

  @max_payload 1_048_576

  @success_keys ~w(ok protocol_version sessions)
  @session_keys ~w(panes session_id session_name)
  @address_keys ~w(pane_id pane_index registered_at_observation window_index)
  @error_keys ~w(error ok protocol_version)
  @errors ~w(invalid_sessions_census invalid_sessions_request oversize sessions_unavailable)

  test "the sessions example set is exactly the pinned files and hashes to the pin" do
    assert names() == @expected_files
    assert File.regular?(@hash_path), "the sessions CONTRACT_HASH is missing"

    assert contract_hash(@fixture_dir) == @pinned_hash
    assert @hash_path |> File.read!() |> String.trim() == @pinned_hash
  end

  test "specifying the capability did not advertise it: the paired set is untouched" do
    assert contract_hash(@paired_dir) == @paired_hash

    ping = @paired_dir |> Path.join("ping.ok.json") |> File.read!() |> Jason.decode!()

    assert ping["capabilities"] == @paired_capabilities,
           "the paired ping example may only change with a producer that advertises it"
  end

  test "every example names version 2 and carries exactly the keys its shape allows" do
    for name <- @expected_files do
      reply = fixture(name)

      assert reply["protocol_version"] == 2, name
      assert is_boolean(reply["ok"]), name

      if reply["ok"] do
        assert sorted_keys(reply) == @success_keys, name
        assert is_list(reply["sessions"]), name
      else
        assert sorted_keys(reply) == @error_keys, name
        assert reply["error"] in @errors, name
      end
    end
  end

  test "every session and address carries exactly its fields, with numeric indices" do
    for {name, session} <- sessions() do
      assert sorted_keys(session) == @session_keys, name
      assert is_binary(session["session_name"]), name
      assert session["session_name"] != "", name
      assert String.valid?(session["session_name"]), "#{name}: session_name is not UTF-8"
      assert is_list(session["panes"]), name

      for address <- session["panes"] do
        assert sorted_keys(address) == @address_keys, name
        assert is_integer(address["window_index"]) and address["window_index"] >= 0, name
        assert is_integer(address["pane_index"]) and address["pane_index"] >= 0, name
        assert is_boolean(address["registered_at_observation"]), name
      end
    end
  end

  # grouping is by the stable id: an implementation that emitted one session twice, or
  # split its addresses across two objects, would satisfy every ordering rule above, so
  # the uniqueness of the id is its own row. The duplicate-name example is the witness
  # that this row rejects the right thing: a reused NAME is legal and must survive it.
  test "each session_id appears exactly once, so a session is never split in two" do
    for {name, reply} <- success_replies() do
      ids = Enum.map(reply["sessions"], & &1["session_id"])
      assert ids == Enum.uniq(ids), "#{name}: a session_id appears in more than one object"
    end

    duplicates = fixture("sessions.ok.duplicate_names.json")
    names = Enum.map(duplicates["sessions"], & &1["session_name"])

    assert length(duplicates["sessions"]) == 2
    assert length(Enum.uniq(names)) == 1, "the example must reuse one name across two ids"
  end

  test "sessions sort by session_id byte order and addresses by numeric index" do
    for {name, reply} <- success_replies() do
      ids = Enum.map(reply["sessions"], & &1["session_id"])
      assert ids == Enum.sort(ids), name

      for session <- reply["sessions"] do
        keys = Enum.map(session["panes"], &address_key/1)
        assert keys == Enum.sort(keys), name

        addresses = Enum.map(session["panes"], &{&1["window_index"], &1["pane_index"]})
        assert addresses == Enum.uniq(addresses), name
      end
    end
  end

  # without this the ordering row above would pass on a projection that sorted both
  # fields the same way, which is exactly the mistake the contract writes out
  test "the ordering example makes byte order and numeric order disagree" do
    reply = fixture("sessions.ok.ordering.json")
    numeric = Enum.map(reply["sessions"], &suffix(&1["session_id"]))

    refute numeric == Enum.sort(numeric), "session ids are in numeric order"

    first = reply["sessions"] |> hd() |> Map.fetch!("panes")
    windows = first |> Enum.map(& &1["window_index"]) |> as_text()
    panes = first |> Enum.filter(&(&1["window_index"] == 0)) |> Enum.map(& &1["pane_index"])

    refute windows == Enum.sort(windows), "the window indices are also in byte order"
    refute as_text(panes) == Enum.sort(as_text(panes)), "the pane indices are in byte order"
  end

  test "a linked pane holds several addresses and one replicated registration" do
    linked = fixture("sessions.ok.linked_window.json")
    consistency = fixture("sessions.ok.multi_address_consistency.json")

    assert repeated_pane_ids(linked) != [], "no example shows a pane at several addresses"
    assert repeated_pane_ids(consistency) != []

    for {name, reply} <- success_replies(), {pane_id, flags} <- registrations(reply) do
      assert length(Enum.uniq(flags)) == 1, "#{name}: #{pane_id} disagrees with itself"
    end
  end

  test "every identity is a tmux decimal identity once the placeholders are substituted" do
    # built, not written: the redaction gate refuses a literal pane id in the tree, which
    # is why the examples carry placeholders at all
    prefix = "%"

    for {name, session} <- sessions() do
      assert session["session_id"] =~ ~r/\A\$[0-9]+\z/, name

      for address <- session["panes"] do
        substituted = prefix <> suffix_digits(address["pane_id"])

        assert address["pane_id"] =~ ~r/\A<pane_id_[1-8]>\z/, name
        assert substituted =~ ~r/\A%[0-9]+\z/, name
      end
    end
  end

  # ILLUSTRATIVE ARITHMETIC, not producer evidence. This builds a local value and applies
  # the documented predicate to it on either side of the bound. It does not measure the
  # producer's threshold, its encoder, where in its reply path the check sits, or whether
  # it ever hands an oversized payload to the socket. Those controls are owed by the
  # implementation and nothing here discharges them.
  test "the documented oversize predicate decides both sides of the bound" do
    base = byte_size(candidate(""))
    exact = candidate(String.duplicate("a", @max_payload - base))
    over = candidate(String.duplicate("a", @max_payload - base + 1))

    assert byte_size(exact) == @max_payload
    refute documented_oversize?(exact), "a value at the bound is not over it"
    assert documented_oversize?(over)

    refusal = File.read!(Path.join(@fixture_dir, "oversize.json"))

    assert byte_size(refusal) < 256, "the refusal is small enough to always be sendable"
    assert refusal |> Jason.decode!() |> Map.fetch!("error") == "oversize"

    for name <- @expected_files do
      bytes = @fixture_dir |> Path.join(name) |> File.read!()
      refute bytes =~ "sessions_oversize", name
    end
  end

  test "the contract document pins this set and says it is unadopted" do
    document = File.read!(@document)
    count = Integer.to_string(length(@expected_files))

    assert declared(document, "sessions_example_dir") == @declared_dir
    assert declared(document, "sessions_example_contract_hash") == @pinned_hash
    assert declared(document, "sessions_example_count") == count
    assert declared(document, "sessions_example_status") == "unadopted"
  end

  defp json_paths(dir), do: dir |> Path.join("*.json") |> Path.wildcard() |> Enum.sort()

  defp names, do: @fixture_dir |> json_paths() |> Enum.map(&Path.basename/1)

  defp fixture(name), do: @fixture_dir |> Path.join(name) |> File.read!() |> Jason.decode!()

  defp replies, do: Enum.map(@expected_files, &{&1, fixture(&1)})

  defp success_replies, do: Enum.filter(replies(), fn {_name, reply} -> reply["ok"] end)

  defp sessions do
    for {name, reply} <- success_replies(), session <- reply["sessions"], do: {name, session}
  end

  defp sorted_keys(map), do: map |> Map.keys() |> Enum.sort()

  defp address_key(address) do
    {address["window_index"], address["pane_index"], address["pane_id"]}
  end

  defp as_text(integers), do: Enum.map(integers, &Integer.to_string/1)

  defp suffix(id), do: id |> suffix_digits() |> String.to_integer()

  defp suffix_digits(id), do: String.replace(id, ~r/\A[^0-9]+|[^0-9]+\z/, "")

  defp registrations(reply) do
    pairs =
      for session <- reply["sessions"], address <- session["panes"] do
        {address["pane_id"], address["registered_at_observation"]}
      end

    Enum.group_by(pairs, &elem(&1, 0), &elem(&1, 1))
  end

  defp repeated_pane_ids(reply) do
    repeats = Enum.filter(registrations(reply), fn {_id, flags} -> length(flags) > 1 end)

    Enum.map(repeats, &elem(&1, 0))
  end

  defp candidate(padding) do
    Jason.encode!(%{
      "ok" => true,
      "protocol_version" => 2,
      "sessions" => [%{"panes" => [], "session_id" => "$1", "session_name" => padding}]
    })
  end

  # the predicate the contract documents, applied to a locally built value: an example of
  # the rule, never a measurement of a producer
  defp documented_oversize?(encoded), do: byte_size(encoded) > @max_payload

  # the v1 rule, restated in ipc-v2.org: byte-sorted filenames, each followed by a NUL, its
  # exact bytes and another NUL
  defp contract_hash(dir) do
    payload = dir |> json_paths() |> Enum.map(&[Path.basename(&1), 0, File.read!(&1), 0])

    :sha256 |> :crypto.hash(payload) |> Base.encode16(case: :lower)
  end

  # `- key: ~value~`; exactly one such line per key, so a duplicated or removed key fails
  defp declared(document, key) do
    case Regex.scan(~r/^- #{key}: ~([^~]+)~/m, document) do
      [[_line, value]] -> value
      other -> flunk("#{key} is not declared exactly once in #{@document}: #{inspect(other)}")
    end
  end
end
