defmodule WorkerGreenSuccessorProbeTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Run.Executor.Owner
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.OwnedHarness, as: O
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.OwnerOracle
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  setup do
    Process.flag(:trap_exit, true)
    O.setup_owned()
    dir = Path.join(System.tmp_dir!(), "worker-successor-review-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    O.track_dir!(dir)
    H.reset_seams()
    {:ok, dir: dir}
  end

  defp config(dir) do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    opts = Keyword.drop(make.(), [:event_sink, :run_dir, :run_lock_path, :tail_repair])

    %{
      run_dir: dir,
      mode: :run,
      command: nil,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: opts,
      trace: O.collector()
    }
  end

  test "two distinct closures in one family do not establish optional absence", %{dir: dir} do
    nested = fn :only_this_name, _ -> :ok end

    barrier = fn
      :handoff_received, facts -> nested.(Map.get(facts, :review_name, :handoff_received), facts)
      :subtree_started, _ -> :ok
    end

    refute Function.info(nested, :name) == Function.info(barrier, :name)
    ctx = config(dir)
    O.spawn_caller!(fn -> Owner.run(ctx, barrier) end)
    assert_receive {:result, result}, 30_000

    outcome =
      case result do
        {:ok, value} -> {:ok, value.summary["status"]}
        {:error, value} -> {:error, value.clause}
      end

    assert {:error, "run_executor_down"} = outcome
  end

  test "oracle runtime owner dies when its caller dies mid-effect", %{dir: dir} do
    ctx = config(dir)

    opts =
      ctx.opts
      |> Keyword.put(:gate_opts, runner: OwnerDoubles.held_gate(O.collector()))
      |> Keyword.put(:event_sink, GateDouble.receipt_sink())
      |> Keyword.put(:run_dir, dir)

    {caller, monitor} = O.spawn_caller!(fn -> OwnerOracle.run(ctx.spec, ctx.plan, opts) end)
    assert_receive {:gate_entered, owner}, 10_000
    O.track!(owner)
    owner_monitor = Process.monitor(owner)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 5_000
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, _}, 6_000
  end
end
