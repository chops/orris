defmodule AiOrchestrator.Host.BaselineControlTest do
  @moduledoc """
  Baseline controls for the observational host slice (docs/contracts/host-observational-registry.org).

  Every row here must PASS on the unchanged source: they pin the oracles the host rows rely on
  before any host module exists. B-1 the same-BEAM second-writer refusal; B-2 orderly close
  releases the disk lock and retires the arbiter record so a later acquisition proceeds under
  ordinary journal admission; B-3 a stranded lock (arbiter registration lost AND the writer
  killed before release) fails closed as `run_locked`; B-4 the owner barrier contract the host
  wrapper must preserve; B-5 `Journal.Ownership` is the live-run authority, not any registry.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Executor
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @command_id "cmd_host_baseline_0001abcd"

  setup do
    dir = Path.join(System.tmp_dir!(), "host_baseline_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp lock_opts(overrides \\ []) do
    Keyword.merge(
      [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
      overrides
    )
  end

  defp seed_journal!(dir), do: File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])

  defp open(dir, overrides \\ []) do
    Writer.open(dir, Keyword.merge([fs: SystemFs.new(), lock: lock_opts()], overrides))
  end

  defp kill!(pid) do
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
    :ok
  end

  describe "baseline controls (must pass on the unchanged source)" do
    test "B-1 a second live writer in the same BEAM is refused with the generation", %{dir: dir} do
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)

      assert {:error, %{clause: "second_live_writer", generation: 1}} =
               open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))

      assert {:ok, %{"supervisor_instance" => "sup_0001"}} = RunLock.owner(SystemFs.new(), dir)
      :ok = Writer.close(writer)
    end

    test "B-2 an orderly close releases the disk lock and retires the record; reacquisition proceeds", %{dir: dir} do
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      assert {:ok, %{state: :live, generation: 1}} = Ownership.status(dir)

      :ok = Writer.close(writer)
      assert :none = Ownership.status(dir)
      assert :none = RunLock.owner(SystemFs.new(), dir)

      assert {:ok, replacement, _} = open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))
      assert {:ok, %{"supervisor_instance" => "sup_0002"}} = RunLock.owner(SystemFs.new(), dir)
      :ok = Writer.close(replacement)
    end

    test "B-3 a stranded lock after arbiter loss fails closed as run_locked", %{dir: dir} do
      seed_journal!(dir)
      {:ok, arbiter_a} = Ownership.start_link(name: nil)
      {:ok, writer, _} = open(dir, ownership: [server: arbiter_a])
      assert {:ok, %{state: :live}} = Ownership.status(dir, server: arbiter_a)

      # the writer dies without releasing, then the arbiter that held its record is gone
      :ok = kill!(writer)
      :ok = GenServer.stop(arbiter_a)
      {:ok, arbiter_b} = Ownership.start_link(name: nil)

      assert :none = Ownership.status(dir, server: arbiter_b)

      assert {:error, %{clause: "run_locked"}} =
               open(dir, ownership: [server: arbiter_b], lock: lock_opts(supervisor_instance: "sup_0002"))

      :ok = GenServer.stop(arbiter_b)
    end

    test "B-4 the owner barrier is called in order with the owned identities and its escape is closed", %{dir: dir} do
      test_pid = self()

      barrier = fn label, owned ->
        send(test_pid, {:barrier, label, owned})
        :ok
      end

      assert {:ok, %{}} = run_start(dir, barrier)

      assert_receive {:barrier, :handoff_received, first}, 10_000
      assert_receive {:barrier, :subtree_started, second}, 10_000
      refute_receive {:barrier, _, _}, 200

      for owned <- [first, second], key <- [:supervisor, :server, :writer, :owner] do
        assert is_pid(owned[key]), "#{key} missing from the #{inspect(owned)} barrier map"
      end

      assert is_pid(second[:worker])
    end

    test "B-4b a raising barrier collapses to the closed run_executor_down", %{dir: dir} do
      raising = fn _label, _owned -> raise "barrier failure" end
      assert {:error, %{clause: "run_executor_down"}} = run_start(dir, raising)
    end

    test "B-5 Journal.Ownership, not a registry, answers whether a run is live", %{dir: dir} do
      seed_journal!(dir)
      assert :none = Ownership.status(dir)
      {:ok, writer, _} = open(dir)
      assert {:ok, %{state: :live, writer: ^writer, generation: 1}} = Ownership.status(dir)
      :ok = Writer.close(writer)
      assert :none = Ownership.status(dir)
    end
  end

  # ---- one real start command through Run.Executor with the given barrier ----

  defp run_start(dir, barrier) do
    index = Enum.find_index(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    {_name, _kind, scenario, [], opts_fun} = Enum.at(H.cases(), index)
    H.reset_seams()
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    ctx =
      opts_fun.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: dir,
        spec: spec,
        plan: plan,
        spec_hash: hash(spec),
        plan_hash: hash(plan),
        supervisor_instance: "sup_host_0001",
        trace: self(),
        barrier: barrier
      )

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_host_0001",
        command_id: @command_id,
        now: @now
      )

    Executor.execute(command, ctx)
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))
end
