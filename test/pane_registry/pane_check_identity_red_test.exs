defmodule AiOrchestrator.PaneRegistry.PaneCheckIdentityRedTest do
  @moduledoc """
  NS-15.G.005 B3b (scope r2, D/B3B-SCOPE-r2.org): RED rows R4.4-R4.8 and their controls for the pre-dispatch pane
  identity check, through `Prepare.PaneCheck.check/3` with scripted dispatch doubles and real claim files.

  Names and shapes this RED requires (absent at this head):
  - `FileRegistry.claim/3` option `daemon_identities: %{pane_ref => %{"pane_id", "registration_id", "generation"}}`
    records, per pane, `"daemon_identity" => %{"pane_id", "registration_id", "generation", "verified_at_unix",
    "source" => "status_v3"}` in the claim file (D2); a claim without it validates as today, a present but malformed
    one reads as malformed (fail closed);
  - a dispatch module's optional `status_v3(pane_ref, opts)` answering `{:ok, reply_bytes}` or
    `{:error, %{"reason" => reason}}`;
  - PaneCheck choosing its path from the CLAIM (D4 table): a claim without identity keeps the B3a version 1 path and
    never calls `status_v3/2`; a claim with identity needs a usable version 3 status and otherwise refuses;
    "contradictory" only for a valid identity that differs (next_action inspect_identity_mismatch); a reply-identity
    error or no version 3 path is daemon_unavailable; a version 3 pane_not_found is unregistered.

  Exit 70, no Writer activity and no paste on these refusals follow from B3a's existing claim-time refusal path
  (cli_pane_check_test.exs) once PaneCheck refuses; these rows witness the refusal and its diagnosis at PaneCheck.
  Controls (R4.5d, the R4.6 version 1 half, R4.8 legacy) pass at this head and must stay green. R4.8 proves only that
  FileRegistry records the identities it is given; their claim-time provenance (a decoded version 3 read in the run
  path, nothing else) is R4.9 in cli_pane_identity_red_test.exs.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.PaneRegistry.Diagnosis
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Prepare.PaneCheck

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @pane "pane_writer"
  @reg "reg_" <> String.duplicate("0123456789abcdef", 2)
  @other_reg "reg_" <> String.duplicate("f", 32)
  @gen "123456789012345678901234567890123456789"

  # version 1 status says alive; version 3 answers what the test scripted; every call is reported
  defmodule V3Dispatch do
    @moduledoc false
    def pane_status(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:pane_status, pane_ref})
      {:ok, %{"state" => "idle"}}
    end

    def status_v3(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:status_v3, pane_ref})

      case Keyword.fetch!(opts, :v3) do
        :raise -> raise "no ap"
        reply -> reply
      end
    end
  end

  # version 1 only: alive, and no status_v3/2
  defmodule V1Dispatch do
    @moduledoc false
    def pane_status(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:pane_status, pane_ref})
      {:ok, %{"state" => "idle"}}
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_b3b_identity_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  defp owner(run_id), do: %{"run_id" => run_id, "run_dir" => "/tmp/#{run_id}", "supervisor_instance" => "sup_#{run_id}"}

  defp ident(overrides \\ %{}),
    do: Map.merge(%{"pane_id" => @pane, "registration_id" => @reg, "generation" => @gen}, overrides)

  defp claim!(root, identities) do
    opts = if identities == nil, do: [root: root], else: [root: root, daemon_identities: identities]
    assert {:ok, claim} = FileRegistry.claim([@pane], owner("run_a"), opts)
    on_exit(fn -> FileRegistry.release(claim) end)
    claim
  end

  # a claim whose claiming process has exited without releasing (same OS process, dead Erlang pid)
  defp dead_claim!(root) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn -> send(parent, {:claimed, FileRegistry.claim([@pane], owner("run_a"), root: root)}) end)

    assert_receive {:claimed, {:ok, _claim}}, 5_000
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
  end

  defp claim_file(root), do: root |> FileRegistry.claim_path(@pane) |> File.read!() |> Jason.decode!()

  defp status_bytes(name, substitutions) do
    %{"<pane_id>" => @pane, "<registration_id>" => @reg, "<generation>" => @gen}
    |> Map.merge(substitutions)
    |> Enum.reduce(File.read!(Path.join(@fixtures, name)), fn {placeholder, value}, bytes ->
      String.replace(bytes, placeholder, value)
    end)
  end

  defp edit(bytes, fun), do: bytes |> Jason.decode!() |> fun.() |> Jason.encode!()

  defp check(root, claim, dispatch, v3) do
    PaneCheck.check([@pane], claim.token,
      pane_registry_root: root,
      dispatch: dispatch,
      dispatch_opts: [test_pid: self(), v3: v3]
    )
  end

  defp refused!(result) do
    assert match?({:refuse, _refusal}, result), "expected a refusal before dispatch, got #{inspect(result)}"
    {:refuse, refusal} = result
    object = refusal.()
    assert object["reason"] == "pane_claim_refused"
    object["diagnosis"]
  end

  defp diagnoses(root) do
    dir = Path.join(root, "diagnoses")

    if File.dir?(dir) do
      dir |> File.ls!() |> Enum.map(&(dir |> Path.join(&1) |> File.read!() |> Jason.decode!()))
    else
      []
    end
  end

  test "R4.8 RED a claim made with a version 3 identity records daemon_identity", %{root: root} do
    claim!(root, %{@pane => ident()})
    recorded = claim_file(root)["daemon_identity"]

    assert is_map(recorded), "the claim file carries no daemon_identity"
    assert Map.take(recorded, ~w(pane_id registration_id generation)) == ident()
    assert recorded["source"] == "status_v3"
    assert is_integer(recorded["verified_at_unix"])
  end

  test "R4.8 RED a malformed daemon_identity in a dead owner's claim reads held (fail closed)", %{root: root} do
    dead_claim!(root)

    path = FileRegistry.claim_path(root, @pane)
    doc = path |> File.read!() |> Jason.decode!() |> Map.put("daemon_identity", %{"pane_id" => 1})
    File.write!(path, Jason.encode!(doc))

    assert FileRegistry.held?(root, @pane, []) == true
  end

  test "R4.8 control: a dead owner's claim without daemon_identity reads not held, as in B2", %{root: root} do
    dead_claim!(root)

    assert FileRegistry.held?(root, @pane, []) == false
  end

  test "R4.4 RED a valid version 3 identity that differs refuses as contradictory", %{root: root} do
    claim = claim!(root, %{@pane => ident()})
    reply = {:ok, status_bytes("status.ok.json", %{"<registration_id>" => @other_reg})}

    diagnosis = refused!(check(root, claim, V3Dispatch, reply))
    assert diagnosis["trigger"] == "contradictory"
    assert diagnosis["next_action"]["code"] == "inspect_identity_mismatch"
    assert diagnosis["daemon_pane_id"] == @pane
    assert diagnosis["observed_daemon_state"]["source"] == "status_v3"
    assert diagnosis["observed_daemon_state"]["pane_identity"]["registration_id"] == @other_reg
    assert_received {:status_v3, @pane}
  end

  test "R4.5 RED a missing or malformed identity on an ok version 3 status is daemon_unavailable, not contradictory",
       %{root: root} do
    claim = claim!(root, %{@pane => ident()})
    missing = edit(status_bytes("status.ok.json", %{}), &Map.delete(&1, "pane_identity"))
    malformed = status_bytes("status.ok.json", %{"<registration_id>" => "reg_short"})

    for bytes <- [missing, malformed] do
      diagnosis = refused!(check(root, claim, V3Dispatch, {:ok, bytes}))
      assert diagnosis["trigger"] == "daemon_unavailable"
      assert diagnosis["next_action"]["code"] == "reconcile_daemon"
      assert diagnosis["observed_daemon_state"]["source"] == "unavailable"
      assert "reply_identity:" <> _detail = diagnosis["observed_daemon_state"]["error"]
    end
  end

  test "R4.5b RED a claim with identity and no version 3 status refuses even when version 1 says alive",
       %{root: root} do
    claim = claim!(root, %{@pane => ident()})

    diagnosis = refused!(check(root, claim, V1Dispatch, nil))
    assert diagnosis["trigger"] == "daemon_unavailable"
    assert diagnosis["observed_daemon_state"] == %{"source" => "unavailable", "error" => "status_v3_unavailable"}
  end

  test "R4.5b RED a version 3 transport error or raise on a claim with identity refuses with its typed reason",
       %{root: root} do
    claim = claim!(root, %{@pane => ident()})

    for {v3, reason} <- [{{:error, %{"reason" => "ap_timeout"}}, "ap_timeout"}, {:raise, "ap_unavailable"}] do
      diagnosis = refused!(check(root, claim, V3Dispatch, v3))
      assert diagnosis["trigger"] == "daemon_unavailable"
      assert diagnosis["observed_daemon_state"] == %{"source" => "unavailable", "error" => reason}
    end
  end

  test "R4.5c RED a version 3 pane_not_found on a claim with identity is unregistered", %{root: root} do
    claim = claim!(root, %{@pane => ident()})

    diagnosis = refused!(check(root, claim, V3Dispatch, {:ok, status_bytes("status.error.pane_not_found.json", %{})}))
    assert diagnosis["trigger"] == "unregistered"
    assert diagnosis["next_action"]["code"] == "reattach_pane"
    assert diagnosis["observed_daemon_state"]["source"] == "status_v3"
  end

  test "R4.5c RED a matching identity whose version 3 state is dead is dead", %{root: root} do
    claim = claim!(root, %{@pane => ident()})
    dead = edit(status_bytes("status.ok.json", %{}), &Map.put(&1, "state", "dead"))

    diagnosis = refused!(check(root, claim, V3Dispatch, {:ok, dead}))
    assert diagnosis["trigger"] == "dead"
    assert diagnosis["observed_daemon_state"]["source"] == "status_v3"
  end

  test "R4.5d control: a claim without identity takes the version 1 path and never reads version 3", %{root: root} do
    claim = claim!(root, nil)

    assert check(root, claim, V3Dispatch, :raise) == :ok
    assert_received {:pane_status, @pane}
    refute_received {:status_v3, _pane_ref}
    assert diagnoses(root) == []
  end

  test "R4.6 RED a contradictory diagnosis survives a claim and a version 1 read, resolving only on version 3",
       %{root: root} do
    attrs = %{
      "trigger" => "contradictory",
      "pane_ref" => @pane,
      "daemon_pane_id" => @pane,
      "holder" => nil,
      "observed_daemon_state" => %{
        "source" => "status_v3",
        "pane_identity" => ident(%{"registration_id" => @other_reg})
      },
      "next_action" => %{"code" => "inspect_identity_mismatch", "text" => "inspect the pane identity"}
    }

    assert {:ok, %{"diagnosis_id" => id}} = Diagnosis.open(root, attrs, [])

    legacy = claim!(root, nil)
    assert check(root, legacy, V1Dispatch, nil) == :ok
    assert [%{"diagnosis_id" => ^id, "status" => "open"}] = diagnoses(root)
    FileRegistry.release(legacy)

    claim = claim!(root, %{@pane => ident()})
    assert check(root, claim, V3Dispatch, {:ok, status_bytes("status.ok.json", %{})}) == :ok
    assert [%{"diagnosis_id" => ^id, "status" => status} = doc] = diagnoses(root)
    assert status == "resolved", "a matching version 3 read did not resolve the contradictory diagnosis"
    assert doc["resolved_by"]["check"] == "status_v3"
    assert doc["observed_daemon_state"] == attrs["observed_daemon_state"]
  end

  test "R4.7 RED a matching identity passes on the version 3 read alone and writes no diagnosis", %{root: root} do
    claim = claim!(root, %{@pane => ident()})

    assert check(root, claim, V3Dispatch, {:ok, status_bytes("status.ok.json", %{})}) == :ok
    assert_received {:status_v3, @pane}, "an identity-bearing claim was checked without its version 3 read"
    refute_received {:pane_status, @pane}, "an identity-bearing claim fell back to the version 1 read"
    assert diagnoses(root) == []
  end
end
