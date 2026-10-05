defmodule AiOrchestrator.PaneRegistry.FileRegistryIdentityTest do
  @moduledoc """
  NS-15.G.005 B3b GREEN G2: the claim's daemon_identities option is validated before any write, and holder/2 is one
  read-only snapshot of the existing claim file.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.PaneRegistry.PaneIdentity

  @pane "pane_writer"
  @identity %{"pane_id" => @pane, "registration_id" => "reg_" <> String.duplicate("b", 32), "generation" => "12"}

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_b3b_g2_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp owner, do: %{"run_id" => "run_a", "run_dir" => "/tmp/run_a", "supervisor_instance" => "sup_a"}

  test "invalid daemon identities are refused before any claim file is written", %{root: root} do
    for identities <- [
          %{@pane => Map.put(@identity, "generation", 12)},
          %{@pane => Map.put(@identity, "pane_id", "pane_reviewer")},
          %{"pane_reviewer" => Map.put(@identity, "pane_id", "pane_reviewer")},
          [{@pane, @identity}]
        ] do
      assert {:error, %{"reason" => "pane_registry_unavailable"}} =
               FileRegistry.claim([@pane], owner(), root: root, daemon_identities: identities)

      refute File.exists?(FileRegistry.claim_path(root, @pane)), inspect(identities)
    end
  end

  test "holder/2 answers nil, the owner fields, or the claim file's state, and writes nothing", %{root: root} do
    assert FileRegistry.holder(root, @pane) == nil

    assert {:ok, claim} = FileRegistry.claim([@pane], owner(), root: root, daemon_identities: %{@pane => @identity})
    path = FileRegistry.claim_path(root, @pane)
    bytes = File.read!(path)
    file = Jason.decode!(bytes)

    assert FileRegistry.holder(root, @pane) == Map.take(file, ~w(run_id run_dir pid pid_start acquired_at_unix))
    assert File.read!(path) == bytes
    assert :ok = FileRegistry.release(claim)

    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "{\"schema\":\"other\"}")
    assert FileRegistry.holder(root, @pane) == %{"claim_file" => "malformed"}
  end

  test "PaneIdentity.valid?/1 accepts only the ipc-v3 identity grammar" do
    assert PaneIdentity.valid?(@identity)
    refute PaneIdentity.valid?(Map.put(@identity, "pane_id", ""))
    refute PaneIdentity.valid?(Map.put(@identity, "registration_id", "reg_" <> String.duplicate("B", 32)))
    refute PaneIdentity.valid?(Map.put(@identity, "generation", "1e3"))
    refute PaneIdentity.valid?(nil)
  end
end
