defmodule AiOrchestrator.CLIPaneCheckTest do
  @moduledoc """
  B3a G4 (design r2 D1): the claim-time daemon check is never skipped. A dispatch module that cannot read pane status
  (no `pane_status/2`) is refused at claim time as daemon_unavailable / pane_status_unsupported: exit 70 with the
  diagnosis, nothing delivered, no Writer activity, claims balanced.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  # a dispatch module with no pane_status/2: every delivery callback reports itself, none may be reached
  defmodule NoStatusDispatch do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}
    def deliver(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :delivered)
    def observe(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :observed)
    def reconcile(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :reconciled)
  end

  defmodule CountingRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry

    def claim(pane_refs, owner, opts) do
      result = FileRegistry.claim(pane_refs, owner, opts)
      if match?({:ok, _claim}, result), do: send(owner_test_pid(), :claimed)
      result
    end

    def release(claim) do
      send(owner_test_pid(), :released)
      FileRegistry.release(claim)
    end

    defp owner_test_pid, do: :persistent_term.get({__MODULE__, :test_pid})
  end

  # a registry rejecting the claim as held by a live run, but naming no owner
  defmodule OwnerlessRejectingRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry
    def claim(_pane_refs, _owner, _opts), do: {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_writer"}}
    def release(_claim), do: :ok
  end

  test "a live-holder rejection naming no owner is diagnosed with a null holder" do
    run_dir = Path.join(System.tmp_dir!(), "cli-pane-check-ownerless-#{System.unique_integer([:positive])}")
    root = Path.join(System.tmp_dir!(), "cli-pane-check-ownerless-registry-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf!(run_dir) end)
    on_exit(fn -> File.rm_rf!(root) end)
    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    fs = FaultFs.new()

    opts = [
      fs: fs,
      pane_registry: OwnerlessRejectingRegistry,
      pane_registry_root: root,
      dispatch: NoStatusDispatch,
      dispatch_opts: [test_pid: self()],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]

    result = CLI.run(["run", run_dir], opts)

    assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))
    object = Jason.decode!(result.stderr)
    assert object["reason"] == "pane_claim_refused"
    assert object["diagnosis"]["trigger"] == "live_holder"
    assert object["diagnosis"]["holder"] == nil
    assert object["diagnosis"]["next_action"]["code"] == "wait_for_holder"
    assert object["rejection"] == %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_writer"}
    assert [_file] = root |> Path.join("diagnoses") |> File.ls!()
    assert FaultFs.trace(fs) == [], "no Writer activity"
    refute_received :delivered
  end

  test "a dispatch module without pane_status/2 is refused at claim time, never skipped" do
    :persistent_term.put({CountingRegistry, :test_pid}, self())
    run_dir = Path.join(System.tmp_dir!(), "cli-pane-check-unsupported-#{System.unique_integer([:positive])}")
    root = Path.join(System.tmp_dir!(), "cli-pane-check-registry-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf!(run_dir) end)
    on_exit(fn -> File.rm_rf!(root) end)
    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    fs = FaultFs.new()

    opts = [
      fs: fs,
      pane_registry: CountingRegistry,
      pane_registry_root: root,
      dispatch: NoStatusDispatch,
      dispatch_opts: [test_pid: self()],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]

    result = CLI.run(["run", run_dir], opts)

    assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))
    object = Jason.decode!(result.stderr)
    assert object["reason"] == "pane_claim_refused"
    assert object["diagnosis"]["trigger"] == "daemon_unavailable"

    assert object["diagnosis"]["observed_daemon_state"] == %{
             "source" => "unavailable",
             "error" => "pane_status_unsupported"
           }

    assert FaultFs.trace(fs) == [], "no Writer activity"
    assert_received :claimed
    assert_received :released
    refute_received :delivered
    refute_received :observed
    refute_received :reconciled
  end
end
