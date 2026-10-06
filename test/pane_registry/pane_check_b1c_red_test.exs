defmodule AiOrchestrator.PaneRegistry.PaneCheckB1cRedTest do
  @moduledoc """
  NS-15.G.002 B1c (scope r3): PaneCheck's version 3 path once a dispatch module exports `status_v3/2`, through
  scripted dispatch doubles and real claim files (helpers copied from pane_check_identity_red_test.exs).

  - Claim time: only "status_v3_unavailable" (a PROVEN non-capable daemon) on the FIRST pane read keeps a legacy claim
    ({:ok, nil}); an indeterminate capability, or that answer after an earlier pane read, refuses. A decoded status
    refuses dead, then quarantined, before any identity is kept.
  - After a claim holding a daemon_identity: both capability answers refuse daemon_unavailable; precedence is identity
    mismatch (contradictory), then dead, then quarantined (trigger "quarantined", next_action quarantine_held), which a
    later healthy version 3 read resolves.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Prepare.PaneCheck

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @pane "pane_writer"
  @panes ["pane_reviewer", "pane_writer"]
  @reg "reg_" <> String.duplicate("0123456789abcdef", 2)
  @other_reg "reg_" <> String.duplicate("f", 32)
  @gen "123456789012345678901234567890123456789"

  defmodule V3Dispatch do
    @moduledoc false
    def status_v3(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:status_v3, pane_ref})
      Keyword.fetch!(opts, :v3).(pane_ref)
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_b1c_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp owner(run_id), do: %{"run_id" => run_id, "run_dir" => "/tmp/#{run_id}", "supervisor_instance" => "sup_#{run_id}"}

  defp ident(pane, overrides \\ %{}),
    do: Map.merge(%{"pane_id" => pane, "registration_id" => @reg, "generation" => @gen}, overrides)

  defp status_bytes(name, pane, edits) do
    %{"<pane_id>" => pane, "<registration_id>" => @reg, "<generation>" => @gen}
    |> Enum.reduce(File.read!(Path.join(@fixtures, name)), fn {placeholder, value}, bytes ->
      String.replace(bytes, placeholder, value)
    end)
    |> Jason.decode!()
    |> Map.merge(edits)
    |> Jason.encode!()
  end

  defp ok(pane, edits \\ %{}), do: {:ok, status_bytes("status.ok.json", pane, edits)}
  defp quarantined(pane, edits \\ %{}), do: {:ok, status_bytes("status.quarantined.json", pane, edits)}
  defp unavailable(reason), do: {:error, %{"reason" => reason}}

  defp claim_time(root, v3),
    do:
      PaneCheck.claim_time(@panes, FileRegistry,
        pane_registry_root: root,
        dispatch: V3Dispatch,
        dispatch_opts: [test_pid: self(), v3: v3]
      )

  defp claim!(root) do
    assert {:ok, claim} =
             FileRegistry.claim([@pane], owner("run_a"), root: root, daemon_identities: %{@pane => ident(@pane)})

    on_exit(fn -> FileRegistry.release(claim) end)
    claim
  end

  defp check(root, claim, v3) do
    PaneCheck.check([@pane], claim.token,
      pane_registry_root: root,
      dispatch: V3Dispatch,
      dispatch_opts: [test_pid: self(), v3: v3]
    )
  end

  defp refused!(result) do
    assert match?({:refuse, _refusal}, result), "expected a refusal, got #{inspect(result)}"
    {:refuse, refusal} = result
    object = refusal.()
    assert object["reason"] == "pane_claim_refused"
    object["diagnosis"]
  end

  # --- claim time ------------------------------------------------------------------------------

  test "CT1 a proven non-capable daemon on the first pane keeps a legacy claim and reads no other pane", c do
    assert claim_time(c.root, fn _ -> unavailable("status_v3_unavailable") end) == {:ok, nil}
    assert_received {:status_v3, "pane_reviewer"}
    refute_received {:status_v3, "pane_writer"}
  end

  test "CT2 an indeterminate capability on the first pane refuses; it is never a downgrade", c do
    for reason <- ["identity_capability_ap_failed", "identity_capability_protocol_version"] do
      diagnosis = refused!(claim_time(c.root, fn _ -> unavailable(reason) end))
      assert diagnosis["trigger"] == "daemon_unavailable"
      assert diagnosis["observed_daemon_state"]["error"] == reason
    end
  end

  test "CT3 a non-capable answer after an earlier capable pane refuses (the daemon changed)", c do
    v3 = fn
      "pane_reviewer" -> ok("pane_reviewer")
      _ -> unavailable("status_v3_unavailable")
    end

    diagnosis = refused!(claim_time(c.root, v3))
    assert diagnosis["trigger"] == "daemon_unavailable"
    assert diagnosis["pane_ref"] == "pane_writer"
    assert diagnosis["observed_daemon_state"]["error"] == "status_v3_unavailable"
  end

  test "CT4 a dead pane refuses dead at claim time", c do
    diagnosis = refused!(claim_time(c.root, fn pane -> ok(pane, %{"state" => "dead"}) end))
    assert diagnosis["trigger"] == "dead"
  end

  test "CT5 a quarantined pane refuses quarantined at claim time, offering no release", c do
    diagnosis = refused!(claim_time(c.root, &quarantined/1))
    assert diagnosis["trigger"] == "quarantined"
    assert diagnosis["next_action"]["code"] == "quarantine_held"
    refute diagnosis["next_action"]["text"] =~ "release it"
    assert diagnosis["observed_daemon_state"]["quarantined"] == true
  end

  test "CT6 dead takes precedence over quarantined at claim time", c do
    diagnosis = refused!(claim_time(c.root, fn pane -> quarantined(pane, %{"state" => "dead"}) end))
    assert diagnosis["trigger"] == "dead"
  end

  # --- after the claim -------------------------------------------------------------------------

  test "AC1 a claim holding an identity refuses both capability answers; no version 1 fallback", c do
    claim = claim!(c.root)

    for reason <- ["status_v3_unavailable", "identity_capability_ap_failed"] do
      diagnosis = refused!(check(c.root, claim, fn _ -> unavailable(reason) end))
      assert diagnosis["trigger"] == "daemon_unavailable"
      assert diagnosis["observed_daemon_state"]["error"] == reason
    end
  end

  test "AC2 a matching quarantined pane refuses quarantined after the claim", c do
    claim = claim!(c.root)
    diagnosis = refused!(check(c.root, claim, &quarantined/1))
    assert diagnosis["trigger"] == "quarantined"
    assert diagnosis["next_action"]["code"] == "quarantine_held"
  end

  test "AC3 precedence after the claim: mismatch, then dead, then quarantined", c do
    claim = claim!(c.root)

    mismatch = fn pane ->
      quarantined(pane, %{"pane_identity" => ident(pane, %{"registration_id" => @other_reg})})
    end

    assert refused!(check(c.root, claim, mismatch))["trigger"] == "contradictory"
    assert refused!(check(c.root, claim, fn pane -> quarantined(pane, %{"state" => "dead"}) end))["trigger"] == "dead"
  end

  test "AC4 the producer's pane_identity_unavailable refuses daemon_unavailable with that reason", c do
    claim = claim!(c.root)
    refusal = File.read!(Path.join(@fixtures, "status.error.pane_identity_unavailable.json"))
    diagnosis = refused!(check(c.root, claim, fn pane -> {:ok, String.replace(refusal, "<pane_id>", pane)} end))
    assert diagnosis["trigger"] == "daemon_unavailable"
    assert diagnosis["observed_daemon_state"]["error"] == "pane_identity_unavailable"
  end

  test "AC5 a quarantined diagnosis resolves on a later healthy version 3 read", c do
    claim = claim!(c.root)
    _ = refused!(check(c.root, claim, &quarantined/1))
    assert :ok = check(c.root, claim, &ok/1)

    [diagnosis] =
      c.root
      |> Path.join("diagnoses")
      |> File.ls!()
      |> Enum.map(&(c.root |> Path.join("diagnoses") |> Path.join(&1) |> File.read!() |> Jason.decode!()))

    assert diagnosis["trigger"] == "quarantined"
    assert diagnosis["status"] == "resolved"
    assert diagnosis["resolved_by"]["check"] == "status_v3"
  end
end
