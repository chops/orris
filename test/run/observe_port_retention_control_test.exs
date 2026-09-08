defmodule ObservePortRetentionControlTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Run.Worker
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule PortGate do
    @moduledoc false
    def prepare(fs, request, opts) do
      {:ok, handle} = GateDouble.prepare(fs, request, opts)
      port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary, :stream, :exit_status])
      memo = make_ref()
      Process.put({__MODULE__, memo}, :live)

      {:ok,
       Map.merge(handle, %{
         owner: self(),
         port: port,
         memo: memo,
         witness: Keyword.fetch!(opts, :witness)
       })}
    end

    def started_data(handle) do
      witness!(handle, :prepared)
      GateDouble.started_data(handle)
    end

    def abandon(handle) do
      witness!(handle, :settling)
      true = Port.close(handle.port)
      nil = Port.info(handle.port)
      :live = Process.delete({__MODULE__, handle.memo})
      send(handle.witness, {:gate_closed, self(), handle.port, handle.memo})
      :ok
    end

    # A foreign process cannot pass by merely retaining a copied handle.
    def witness!(handle, stage) do
      if !(handle.owner == self() and Process.get({__MODULE__, handle.memo}) == :live and
             Port.info(handle.port, :connected) == {:connected, self()}) do
        raise "gate owner or memo changed"
      end

      port = handle.port
      bytes = Atom.to_string(stage) <> "\n"
      true = Port.command(port, bytes)

      receive do
        {^port, {:data, ^bytes}} -> :ok
      after
        2_000 -> raise "owner Port echo did not arrive"
      end

      send(handle.witness, {:gate_owned, stage, self(), port, handle.memo})
      :ok
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "observe-port-control-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    OwnedHarness.track_dir!(dir)
    {:ok, dir: dir}
  end

  test "real product Worker retains an owner Port and memo through Observe and settles exactly once", %{dir: dir} do
    {:ok, worker} = Worker.start_link(self())
    OwnedHarness.track!(worker)
    monitor = Process.monitor(worker)
    cap = make_ref()

    opts = [
      gate_executor: PortGate,
      gate_helper: GateDouble.helper(),
      gate_opts: [witness: self()],
      supervisor_instance: "sup_port_retention_control",
      run_id: "run_port_retention_control",
      dispatch: ScenarioHarness.OkDispatch
    ]

    send(worker, {:admit, cap, 1, opts})
    assert_receive {:admitted, ^cap, 1, ^worker}, 2_000
    prepare_ref = make_ref()

    prepare = %Effect.PrepareGate{
      gate_run_id: "gr_0001",
      attempt: 1,
      requested: %{"command_argv" => ["true"]},
      deadline_unix: System.os_time(:second) + 60,
      repo_root: dir,
      run_dir: dir
    }

    send(worker, {:execute, cap, 1, prepare_ref, prepare, nil})
    assert_receive {:gate_owned, :prepared, ^worker, port, memo}, 3_000
    assert_receive {:effect_result, ^cap, 1, ^prepare_ref, ^worker, %Observation.GatePrepared{}}, 3_000
    %{runtime: runtime} = :sys.get_state(worker, 2_000)
    handle = Runtime.handle(runtime, {"gr_0001", 1})
    assert handle.port == port and handle.memo == memo
    assert_raise RuntimeError, "gate owner or memo changed", fn -> PortGate.witness!(handle, :foreign) end

    observe_ref = make_ref()

    observe = %Effect.Observe{
      assignment_id: "as_0001",
      command: %{"assignment_id" => "as_0001"},
      deadline_unix: System.os_time(:second) + 60
    }

    send(worker, {:execute, cap, 1, observe_ref, observe, nil})
    assert_receive {:effect_result, ^cap, 1, ^observe_ref, ^worker, %Observation.ArtifactObserved{}}, 3_000
    %{runtime: retained} = :sys.get_state(worker, 2_000)
    assert Runtime.handle(retained, {"gr_0001", 1}) == handle
    assert Port.info(port, :connected) == {:connected, worker}

    settle_ref = make_ref()
    send(worker, {:settle, cap, 1, settle_ref})
    assert_receive {:gate_owned, :settling, ^worker, ^port, ^memo}, 3_000
    assert_receive {:gate_closed, ^worker, ^port, ^memo}, 3_000

    assert_receive {:settled, ^cap, 1, ^settle_ref, ^worker,
                    [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true, "proof" => "gone"}}]},
                   3_000

    assert Port.info(port) == nil
    again_ref = make_ref()
    send(worker, {:settle, cap, 1, again_ref})
    assert_receive {:settled, ^cap, 1, ^again_ref, ^worker, []}, 2_000
    refute_receive {:gate_owned, :settling, ^worker, _, _}, 50
    GenServer.stop(worker, :normal, 2_000)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, 2_000
  end
end
