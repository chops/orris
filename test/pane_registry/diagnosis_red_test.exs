defmodule AiOrchestrator.PaneRegistry.DiagnosisRedTest do
  @moduledoc """
  B3a RED (design r7, D/B3A-RED-DESIGN-r7.org; scope r4 diagnosis contract): the durable claim-refusal diagnosis
  file protocol, at module level, against a temporary pane-registry root.

  API these rows name (absent at this head, reached through apply/3 so the suite still compiles):
  - `Diagnosis.open(root, attrs, opts)` -> `{:ok, diagnosis}` | `{:error, %{"persistence" => %{"ok" => false,
    "error" => reason}}}`; attrs carry trigger, pane_ref, daemon_pane_id, holder, observed_daemon_state, next_action;
    the returned diagnosis carries "diagnosis_id", whose file is "<diagnosis_id>.json", and "removed", the ids a
    create evicted under the retention bound (in the return value only, never written to a file).
  - `Diagnosis.resolve(root, pane_ref, trigger, resolved_by, opts)` -> `{:ok, diagnosis}` | `{:error, reason}`.
  - opts: `:id_fun`, `:now_fun`, `:resolved_bound`, `:diagnosis_fs` (the six-primitive seam).

  Expected at this head: every row fails on the named "Diagnosis module is not available" assertion (seventeen rows).
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.PaneRegistry.Diagnosis

  @diagnosis Diagnosis

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_diagnosis_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, dir: Path.join(root, "diagnoses")}
  end

  defp api! do
    assert Code.ensure_loaded?(@diagnosis) and function_exported?(@diagnosis, :open, 3) and
             function_exported?(@diagnosis, :resolve, 5),
           "Diagnosis module is not available (open/3, resolve/5)"
  end

  defp attrs(pane_ref, trigger) do
    %{
      "trigger" => trigger,
      "pane_ref" => pane_ref,
      "daemon_pane_id" => nil,
      "holder" => nil,
      "observed_daemon_state" => %{"source" => "pane_status_v1", "observed_at" => "2026-10-05T00:00:00Z"},
      "next_action" => %{"code" => "reattach_pane", "text" => "reattach the pane, then retry"}
    }
  end

  # a strictly increasing UTC clock, one second per call
  defp clock do
    counter = :counters.new(1, [])

    fn ->
      :counters.add(counter, 1, 1)
      "2026-10-05T00:#{String.pad_leading(Integer.to_string(:counters.get(counter, 1)), 2, "0")}:00Z"
    end
  end

  defp open(root, attrs, opts), do: Diagnosis.open(root, attrs, opts)
  defp resolve(root, pane, trigger, by, opts \\ []), do: Diagnosis.resolve(root, pane, trigger, by, opts)

  defp opened!(root, attrs, opts \\ []) do
    result = open(root, attrs, opts)
    assert match?({:ok, %{"diagnosis_id" => _}}, result), inspect(result)
    {:ok, %{"diagnosis_id" => id}} = result
    id <> ".json"
  end

  defp files(dir), do: dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".json")) |> Enum.sort()
  defp read(dir, name), do: dir |> Path.join(name) |> File.read!()
  defp decode(dir, name), do: dir |> read(name) |> Jason.decode!()

  defp status_read,
    do: %{"check" => "pane_status_v1", "observed_daemon_state" => %{"state" => "idle"}, "claim_token" => nil}

  defp failing(primitive, reason), do: [diagnosis_fs: __MODULE__.FailingFs.with(primitive, reason)]
  defp persistence(reason), do: {:error, %{"persistence" => %{"ok" => false, "error" => reason}}}

  test "R3.9 RED a repeat updates the open file: seen_count 2, last_seen_at advances, opened_at kept", %{
    root: root,
    dir: dir
  } do
    api!()
    opts = [now_fun: clock()]
    name = opened!(root, attrs("pane_writer", "dead"), opts)
    first = decode(dir, name)

    assert match?({:ok, _}, open(root, attrs("pane_writer", "dead"), opts))

    assert files(dir) == [name]
    second = decode(dir, name)
    assert second["seen_count"] == 2
    assert second["last_seen_at"] > first["last_seen_at"]
    assert second["opened_at"] == first["opened_at"]
  end

  test "R3.9 RED a different trigger on the same pane opens its own file", %{root: root, dir: dir} do
    api!()
    first = opened!(root, attrs("pane_writer", "dead"))
    second = opened!(root, attrs("pane_writer", "daemon_unavailable"))

    assert first != second
    assert files(dir) == Enum.sort([first, second])
  end

  test "R3.10 RED the directory is 0700 and each file 0600", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    assert Bitwise.band(File.stat!(dir).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(Path.join(dir, name)).mode, 0o777) == 0o600
  end

  test "R3.10 RED a create never overwrites an existing diagnosis file", %{root: root, dir: dir} do
    api!()
    File.mkdir_p!(dir)
    taken = "dgn_" <> String.duplicate("0", 32)
    File.write!(Path.join(dir, taken <> ".json"), "keep")

    assert open(root, attrs("pane_writer", "dead"), id_fun: fn -> taken end) == persistence("create_failed")
    assert read(dir, taken <> ".json") == "keep"
  end

  test "R3.10 RED resolving a missing file changes nothing and recreates nothing", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    File.rm!(Path.join(dir, name))

    assert match?({:error, _}, resolve(root, "pane_writer", "dead", status_read()))
    assert files(dir) == []
  end

  test "R3.10 RED resolving an unreadable file changes nothing", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    File.write!(Path.join(dir, name), "not json")

    assert match?({:error, _}, resolve(root, "pane_writer", "dead", status_read()))
    assert read(dir, name) == "not json"
  end

  test "R3.8d RED a resolved file is never modified again", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    assert match?({:ok, _}, resolve(root, "pane_writer", "dead", status_read()))
    before = read(dir, name)

    assert match?({:error, _}, resolve(root, "pane_writer", "dead", status_read()))
    assert read(dir, name) == before
  end

  test "R3.8c RED resolving one diagnosis changes exactly that file", %{root: root, dir: dir} do
    api!()
    target = opened!(root, attrs("pane_writer", "dead"))
    opened!(root, attrs("pane_writer", "daemon_unavailable"))
    opened!(root, attrs("pane_reviewer", "dead"))
    before = Map.new(files(dir), &{&1, read(dir, &1)})

    assert match?({:ok, _}, resolve(root, "pane_writer", "dead", status_read()))

    changed = for {name, bytes} <- before, read(dir, name) != bytes, do: name
    assert changed == [target]
  end

  test "R3.8e RED resolution changes only status, resolved_at and resolved_by", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    before = decode(dir, name)

    assert match?({:ok, _}, resolve(root, "pane_writer", "dead", status_read()))
    after_resolution = decode(dir, name)

    changed = for {key, value} <- after_resolution, before[key] != value, do: key
    assert Enum.sort(changed) == ["resolved_at", "resolved_by", "status"]
    assert after_resolution["status"] == "resolved"
  end

  test "R3.13 RED a live_holder resolution records the claim and keeps the original daemon state", %{
    root: root,
    dir: dir
  } do
    api!()
    name = opened!(root, attrs("pane_writer", "live_holder"))
    original = decode(dir, name)["observed_daemon_state"]

    by = %{"check" => "file_registry_claim", "observed_daemon_state" => nil, "claim_token" => "token_b"}
    assert match?({:ok, _}, resolve(root, "pane_writer", "live_holder", by))

    resolved = decode(dir, name)
    assert resolved["resolved_by"] == by
    assert resolved["observed_daemon_state"] == original
  end

  test "R3.8a RED a claim check does not resolve a dead diagnosis", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    before = read(dir, name)
    by = %{"check" => "file_registry_claim", "observed_daemon_state" => nil, "claim_token" => "token_b"}

    assert match?({:error, _}, resolve(root, "pane_writer", "dead", by))
    assert read(dir, name) == before
  end

  # a create's return also reports the diagnosis ids it evicted ("removed", in the return value only, never in a file)
  defp created!(root, attrs, opts) do
    result = open(root, attrs, opts)
    assert match?({:ok, %{"diagnosis_id" => _, "removed" => _}}, result), inspect(result)
    {:ok, %{"diagnosis_id" => id, "removed" => removed}} = result
    {id, removed}
  end

  # Bound 2. Eviction happens only BEFORE a create, and only while the resolved count is at the bound; a resolve never
  # evicts. An OPEN sentinel s is created first, so it is the oldest file of all and a live eviction candidate if an
  # implementation evicted by age alone. Step by step: s stays open; a and b are opened and resolved (2 resolved,
  # nothing evicted); opening c evicts exactly a and reports it; resolving c evicts nothing (resolved: b, c); opening d
  # evicts exactly b and reports it. Every report names the evicted id, and s is byte-identical throughout.
  test "R3.12 RED each create evicts and reports exactly the oldest resolved ids, never at resolve, never an open one", %{
    root: root,
    dir: dir
  } do
    api!()
    opts = [resolved_bound: 2, now_fun: clock()]

    {s, removed_at_s} = created!(root, attrs("pane_sentinel", "dead"), opts)
    assert removed_at_s == []
    sentinel = read(dir, s <> ".json")

    {a, removed_at_a} = created!(root, attrs("pane_a", "dead"), opts)
    assert removed_at_a == []
    assert match?({:ok, _}, resolve(root, "pane_a", "dead", status_read(), opts))
    {b, removed_at_b} = created!(root, attrs("pane_b", "dead"), opts)
    assert removed_at_b == []
    assert match?({:ok, _}, resolve(root, "pane_b", "dead", status_read(), opts))
    assert files(dir) == Enum.sort([s <> ".json", a <> ".json", b <> ".json"]), "a resolve evicts nothing"

    {c, removed_at_c} = created!(root, attrs("pane_c", "dead"), opts)
    assert removed_at_c == [a]
    assert files(dir) == Enum.sort([s <> ".json", b <> ".json", c <> ".json"])
    assert read(dir, s <> ".json") == sentinel, "the open sentinel is never evicted or changed"

    assert match?({:ok, _}, resolve(root, "pane_c", "dead", status_read(), opts))
    assert files(dir) == Enum.sort([s <> ".json", b <> ".json", c <> ".json"]), "a resolve evicts nothing"

    {d, removed_at_d} = created!(root, attrs("pane_d", "dead"), opts)
    assert removed_at_d == [b]
    assert files(dir) == Enum.sort([s <> ".json", c <> ".json", d <> ".json"])
    assert read(dir, s <> ".json") == sentinel, "the open sentinel is never evicted or changed"
    assert decode(dir, d <> ".json")["status"] == "open"
    refute Map.has_key?(decode(dir, d <> ".json"), "removed")
  end

  test "R3.11 RED create-path primitive failures are typed persistence failures", %{root: root} do
    api!()

    for {primitive, reason} <- [ensure_dir: "dir_unavailable", list: "list_failed", publish_new: "create_failed"] do
      assert open(root, attrs("pane_writer", "dead"), failing(primitive, reason)) == persistence(reason),
             inspect(primitive)
    end
  end

  test "R3.11 RED a repeat whose read fails is typed and leaves the file unchanged", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    before = read(dir, name)

    assert open(root, attrs("pane_writer", "dead"), failing(:read, "file_unreadable")) == persistence("file_unreadable")
    assert read(dir, name) == before
  end

  test "R3.11 RED a repeat whose replace fails is typed and leaves the file unchanged", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))
    before = read(dir, name)

    assert open(root, attrs("pane_writer", "dead"), failing(:replace, "update_failed")) == persistence("update_failed")
    assert read(dir, name) == before
  end

  test "R3.11 RED a resolving update that fails is typed and the diagnosis stays open", %{root: root, dir: dir} do
    api!()
    name = opened!(root, attrs("pane_writer", "dead"))

    result = resolve(root, "pane_writer", "dead", status_read(), failing(:replace, "update_failed"))

    assert result == persistence("update_failed")
    assert decode(dir, name)["status"] == "open"
  end

  test "R3.11 RED a retention removal that fails is typed, refuses the create and keeps the resolved file", %{
    root: root,
    dir: dir
  } do
    api!()
    opts = [resolved_bound: 1, now_fun: clock()]
    old = opened!(root, attrs("pane_a", "dead"), opts)
    assert match?({:ok, _}, resolve(root, "pane_a", "dead", status_read(), opts))

    result = open(root, attrs("pane_b", "dead"), opts ++ failing(:remove, "remove_failed"))

    assert result == persistence("remove_failed")
    assert files(dir) == [old]
  end

  defmodule FailingFs do
    @moduledoc false
    # A :diagnosis_fs double: the named primitive fails with its typed reason; every other primitive is the real one.
    def with(primitive, reason) do
      :persistent_term.put({__MODULE__, :failing}, {primitive, reason})
      __MODULE__
    end

    defp fail_or(primitive, fun) do
      case :persistent_term.get({__MODULE__, :failing}, nil) do
        {^primitive, reason} -> {:error, reason}
        _other -> fun.()
      end
    end

    def ensure_dir(dir), do: fail_or(:ensure_dir, fn -> local().ensure_dir(dir) end)
    def list(dir), do: fail_or(:list, fn -> local().list(dir) end)
    def read(dir, name), do: fail_or(:read, fn -> local().read(dir, name) end)
    def publish_new(dir, name, bytes), do: fail_or(:publish_new, fn -> local().publish_new(dir, name, bytes) end)
    def replace(dir, name, bytes), do: fail_or(:replace, fn -> local().replace(dir, name, bytes) end)
    def remove(dir, name), do: fail_or(:remove, fn -> local().remove(dir, name) end)

    defp local, do: Diagnosis.LocalFs
  end
end
