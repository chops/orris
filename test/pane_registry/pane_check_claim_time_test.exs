defmodule AiOrchestrator.PaneRegistry.PaneCheckClaimTimeTest do
  @moduledoc """
  NS-15.G.005 B3b GREEN G3 (scope r4 D5): PaneCheck.claim_time/3 reads every pane's version 3 identity before any
  claim, answers nil identities for a dispatch module without status_v3/2, and refuses on the first pane without a
  valid identity with one holder/2 read of that pane through the configured registry.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Prepare.PaneCheck

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @panes ["pane_reviewer", "pane_writer"]

  defmodule HolderRegistry do
    @moduledoc false
    def holder(root, pane_ref) do
      send(:persistent_term.get({__MODULE__, :test_pid}), {:holder, pane_ref})
      FileRegistry.holder(root, pane_ref)
    end
  end

  defmodule V3 do
    @moduledoc false
    def status_v3(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:status_v3, pane_ref})
      Keyword.fetch!(opts, :reply).(pane_ref)
    end
  end

  defmodule V1 do
    @moduledoc false
    def pane_status(_pane_ref, _opts), do: {:ok, %{"state" => "idle"}}
  end

  setup do
    :persistent_term.put({HolderRegistry, :test_pid}, self())
    on_exit(fn -> :persistent_term.erase({HolderRegistry, :test_pid}) end)
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_b3b_g3_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp identity(pane_ref),
    do: %{"pane_id" => pane_ref, "registration_id" => "reg_" <> String.duplicate("c", 32), "generation" => "9"}

  defp ok_reply(pane_ref) do
    Enum.reduce(identity(pane_ref), File.read!(Path.join(@fixtures, "status.ok.json")), fn {key, value}, bytes ->
      String.replace(bytes, "<#{key}>", value)
    end)
  end

  defp opts(root, dispatch, reply),
    do: [pane_registry_root: root, dispatch: dispatch, dispatch_opts: [test_pid: self(), reply: reply]]

  test "every pane's decoded identity is returned for the claim; no holder read", %{root: root} do
    assert PaneCheck.claim_time(@panes, HolderRegistry, opts(root, V3, &{:ok, ok_reply(&1)})) ==
             {:ok, Map.new(@panes, &{&1, identity(&1)})}

    assert_received {:status_v3, "pane_reviewer"}
    assert_received {:status_v3, "pane_writer"}
    refute_received {:holder, _pane_ref}
  end

  test "a dispatch module without status_v3/2 answers no identities", %{root: root} do
    assert PaneCheck.claim_time(@panes, HolderRegistry, opts(root, V1, nil)) == {:ok, nil}
  end

  test "the first pane without a valid identity refuses with one holder read of that pane", %{root: root} do
    reply = fn
      "pane_reviewer" -> {:error, %{"reason" => "ap_timeout"}}
      pane_ref -> {:ok, ok_reply(pane_ref)}
    end

    assert {:refuse, refusal} = PaneCheck.claim_time(@panes, HolderRegistry, opts(root, V3, reply))
    refute_received {:status_v3, "pane_writer"}
    object = refusal.()

    assert object["diagnosis"]["trigger"] == "daemon_unavailable"
    assert object["diagnosis"]["observed_daemon_state"] == %{"source" => "unavailable", "error" => "ap_timeout"}
    assert object["diagnosis"]["holder"] == nil
    assert_received {:holder, "pane_reviewer"}
    refute_received {:holder, _other}
  end
end
