defmodule AiOrchestrator.PaneRegistry.DiagnosisTest do
  @moduledoc """
  B3a G3 (scope r5): files that are not complete diagnosis objects are never matched, repeated, resolved or evicted;
  attrs that would not make a complete diagnosis are refused before any write; a create whose file was linked but not
  cleaned up is `create_uncertain`, never success.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.PaneRegistry.Diagnosis
  alias AiOrchestrator.PaneRegistry.Diagnosis.LocalFs

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_diagnosis_g3_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, dir: Path.join(root, "diagnoses")}
  end

  defp attrs(pane_ref) do
    %{
      "trigger" => "dead",
      "pane_ref" => pane_ref,
      "daemon_pane_id" => nil,
      "holder" => nil,
      "observed_daemon_state" => %{"source" => "pane_status_v1"},
      "next_action" => %{"code" => "reattach_pane", "text" => "reattach the pane, then retry"}
    }
  end

  defp plant(dir, id, doc) do
    File.mkdir_p!(dir)
    bytes = Jason.encode!(Map.put(doc, "diagnosis_id", id))
    File.write!(Path.join(dir, id <> ".json"), bytes)
    bytes
  end

  test "an open file without seen_count is never repeated; the open creates its own file", %{root: root, dir: dir} do
    malformed = "dgn_" <> String.duplicate("a", 32)
    doc = "pane_writer" |> attrs() |> Map.merge(%{"status" => "open", "opened_at" => "t0", "last_seen_at" => "t0"})
    bytes = plant(dir, malformed, doc)

    assert {:ok, %{"diagnosis_id" => created, "seen_count" => 1}} = Diagnosis.open(root, attrs("pane_writer"), [])
    assert created != malformed
    assert File.read!(Path.join(dir, malformed <> ".json")) == bytes
  end

  test "a resolved file without resolved_at is never evicted", %{root: root, dir: dir} do
    malformed = "dgn_" <> String.duplicate("b", 32)

    doc =
      "pane_old"
      |> attrs()
      |> Map.merge(%{"status" => "resolved", "opened_at" => "t0", "last_seen_at" => "t0", "seen_count" => 1})
      |> Map.put("resolved_by", %{"check" => "pane_status_v1"})

    bytes = plant(dir, malformed, doc)
    opts = [resolved_bound: 1]
    assert {:ok, %{"diagnosis_id" => valid}} = Diagnosis.open(root, attrs("pane_a"), opts)
    by = %{"check" => "pane_status_v1", "observed_daemon_state" => %{"state" => "idle"}, "claim_token" => nil}
    assert {:ok, _resolved} = Diagnosis.resolve(root, "pane_a", "dead", by, opts)

    assert {:ok, %{"removed" => removed}} = Diagnosis.open(root, attrs("pane_b"), opts)
    assert removed == [valid]
    assert File.read!(Path.join(dir, malformed <> ".json")) == bytes
  end

  test "incomplete attrs and an unknown trigger are refused before any write", %{root: root, dir: dir} do
    incomplete = %{"trigger" => "dead", "pane_ref" => "pane_writer"}
    unknown = Map.put(attrs("pane_writer"), "trigger", "not_a_trigger")

    for bad <- [incomplete, unknown, Map.put(attrs("pane_writer"), "pane_ref", "")] do
      assert Diagnosis.open(root, bad, []) == {:error, %{"reason" => "diagnosis_attrs_invalid"}}
      assert Diagnosis.open(root, bad, []) == {:error, %{"reason" => "diagnosis_attrs_invalid"}}
    end

    refute File.exists?(dir)
  end

  test "a linked create whose temp name cannot be removed is create_uncertain, not success", %{dir: dir} do
    :ok = LocalFs.ensure_dir(dir)
    name = "dgn_" <> String.duplicate("c", 32) <> ".json"

    result = LocalFs.publish_new(dir, name, "{}", unlink: fn _temp -> {:error, :eacces} end)

    assert result == {:error, "create_uncertain"}
    assert File.read!(Path.join(dir, name)) == "{}"
  end
end
