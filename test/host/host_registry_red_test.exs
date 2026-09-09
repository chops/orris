defmodule AiOrchestrator.Host.RegistryRedTest do
  @moduledoc """
  RED rows for the in-VM observational host slice (docs/contracts/host-observational-registry.org).

  The interface under test does not exist yet: `AiOrchestrator.Host.Monitor` (the observational
  monitor process), `AiOrchestrator.Host.Executor` (the `Commands.Executor` wrapper that composes
  the owner barrier) and `AiOrchestrator.Host.status/1`. Every row begins by requiring that
  interface, so on the unchanged source each row fails on exactly that missing interface and on
  nothing else. The baseline oracles the rows compare against are pinned by
  `test/host/host_baseline_control_test.exs`.

  Nothing in this file changes admission: the monitor is a hint. The clauses the rows assert
  (`second_live_writer`, `run_locked`, `journal_exists`) are the existing authorities.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]

  # ---- late-bound receivers: the interface is absent on the unchanged source ----
  defp host, do: Module.concat(["AiOrchestrator", "Host"])
  defp monitor, do: Module.concat(["AiOrchestrator", "Host", "Monitor"])
  defp host_executor, do: Module.concat(["AiOrchestrator", "Host", "Executor"])

  defp require_host! do
    for mod <- [host(), monitor(), host_executor()] do
      assert Code.ensure_loaded?(mod), "#{inspect(mod)} does not exist"
    end

    assert function_exported?(host(), :status, 1), "AiOrchestrator.Host.status/1 does not exist"
    assert function_exported?(host_executor(), :execute, 2), "AiOrchestrator.Host.Executor.execute/2 does not exist"
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "host_red_#{System.unique_integer([:positive])}")
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
  defp open(dir, overrides \\ []), do: Writer.open(dir, Keyword.merge([fs: SystemFs.new(), lock: lock_opts()], overrides))

  describe "registration and cleanup (R5)" do
    test "H-1 an entry appears after :subtree_started and is gone after completion and owner exit", %{dir: dir} do
      require_host!()
      test_pid = self()

      holding = fn
        :subtree_started, owned ->
          send(test_pid, {:status_while_live, host().status(dir), owned})
          :ok

        _label, _owned ->
          :ok
      end

      assert {:ok, %{}} = start_through_host(dir, holding)
      assert_receive {:status_while_live, {:ok, %{live: true, generation: generation} = status}, owned}, 10_000
      assert is_integer(generation) and generation >= 1
      assert status[:owner] == owned[:owner] and status[:supervisor] == owned[:supervisor]
      assert {:ok, %{live: false, registered: false}} = host().status(dir)
    end

    test "H-1c an orderly teardown leaves no monitor entry and no arbiter registration", %{dir: dir} do
      require_host!()
      assert {:ok, %{}} = start_through_host(dir, fn _, _ -> :ok end)
      assert {:ok, %{registered: false}} = host().status(dir)
      assert :none = Ownership.status(dir)
    end
  end

  describe "the monitor is a hint, never an admission authority (R4/R7)" do
    test "H-2 a second start on a live directory is refused by the existing clause, unchanged", %{dir: dir} do
      require_host!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      before = File.read!(Path.join(dir, "events.jsonl"))

      assert {:error, %{clause: "second_live_writer", generation: 1}} =
               open(dir, lock: lock_opts(supervisor_instance: "sup_0002"))

      assert File.read!(Path.join(dir, "events.jsonl")) == before
      assert {:ok, %{registered: false, live: false}} = host().status(dir)
      :ok = Writer.close(writer)
    end

    test "H-2b absence of a monitor entry never proves absence of a live run", %{dir: dir} do
      require_host!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      assert {:ok, %{registered: false}} = host().status(dir)
      assert {:ok, %{state: :live}} = Ownership.status(dir)
      :ok = Writer.close(writer)
    end

    test "H-5 same run_id under two directories is diagnosed, never used for admission", %{dir: dir} do
      require_host!()
      other = dir <> "_other"
      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf!(other) end)
      test_pid = self()

      probe = fn
        :subtree_started, _owned -> send(test_pid, {:lookup, host().lookup_run_id("run_host_0001")}) && :ok
        _l, _o -> :ok
      end

      assert {:ok, %{}} = start_through_host(dir, fn _, _ -> :ok end)
      assert {:ok, %{}} = start_through_host(other, probe)
      assert_receive {:lookup, {:ok, entries}}, 10_000
      assert is_list(entries)
    end

    test "H-7 an entry whose generation disagrees with Journal.Ownership is reported inconsistent", %{dir: dir} do
      require_host!()
      seed_journal!(dir)
      {:ok, writer, _} = open(dir)
      :ok = monitor().register(%{run_dir: dir, run_id: "run_stale", owner: self(), generation: 99})
      assert {:ok, %{clause: "host_registry_inconsistent"}} = host().status(dir)
      :ok = Writer.close(writer)
    end
  end

  describe "fail-soft composition (R6)" do
    test "H-6a a missing monitor changes nothing about the command", %{dir: dir} do
      require_host!()
      if pid = Process.whereis(monitor()), do: GenServer.stop(pid)
      assert {:ok, %{}} = start_through_host(dir, fn _, _ -> :ok end)
      assert {:error, %{clause: "host_monitor_unavailable"}} = host().status(dir)
    end

    test "H-6b the user barrier runs exactly once per label, in order, with its return preserved", %{dir: dir} do
      require_host!()
      test_pid = self()
      barrier = fn label, owned -> send(test_pid, {:barrier, label, owned}) && :ok end
      assert {:ok, %{}} = start_through_host(dir, barrier)
      assert_receive {:barrier, :handoff_received, _}, 10_000
      assert_receive {:barrier, :subtree_started, _}, 10_000
      refute_receive {:barrier, _, _}, 200
    end

    test "H-6c a raising user barrier still collapses to run_executor_down", %{dir: dir} do
      require_host!()
      assert {:error, %{clause: "run_executor_down"}} = start_through_host(dir, fn _, _ -> raise "boom" end)
    end

    test "H-6d a stalled monitor bounds status and never blocks the command", %{dir: dir} do
      require_host!()
      stalled = spawn(fn -> Process.sleep(:infinity) end)
      assert {:ok, %{}} = start_through_host(dir, fn _, _ -> :ok end, monitor: stalled)
      assert {:error, %{clause: "host_monitor_unavailable"}} = host().status(dir, monitor: stalled, timeout: 100)
    end
  end

  describe "restarts and crashes (R3/R7)" do
    test "H-9a the monitor killed mid-run: the run completes, status falls back", %{dir: dir} do
      require_host!()
      test_pid = self()

      killer = fn
        :subtree_started, _owned ->
          pid = Process.whereis(monitor())
          if pid, do: Process.exit(pid, :kill)
          send(test_pid, :monitor_killed)
          :ok

        _l, _o ->
          :ok
      end

      assert {:ok, %{}} = start_through_host(dir, killer)
      assert_receive :monitor_killed, 10_000
      assert :none = Ownership.status(dir)
    end

    test "H-9b arbiter loss with a live writer: orderly close still reacquires; a killed writer strands", %{dir: dir} do
      require_host!()
      seed_journal!(dir)
      {:ok, arbiter} = Ownership.start_link(name: nil)
      {:ok, writer, _} = open(dir, ownership: [server: arbiter])
      :ok = GenServer.stop(arbiter)
      # orderly close releases the disk lock even without a record; reacquisition proceeds
      :ok = Writer.close(writer)
      {:ok, fresh} = Ownership.start_link(name: nil)
      assert {:ok, again, _} = open(dir, ownership: [server: fresh], lock: lock_opts(supervisor_instance: "sup_0002"))
      :ok = Writer.close(again)
      :ok = GenServer.stop(fresh)
    end
  end

  # ---- one real start command routed through the host executor ----

  defp start_through_host(dir, barrier, extra \\ []) do
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
      |> Keyword.merge(extra)

    Commands.invoke(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
      run_id: "run_host_0001",
      command_id: "cmd_host_red_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower),
      now: @now,
      executor: host_executor(),
      executor_opts: ctx
    )
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))
end
