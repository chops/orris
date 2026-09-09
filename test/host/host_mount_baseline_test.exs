defmodule AiOrchestrator.Host.MountBaselineTest do
  @moduledoc """
  Facts the mounted-runs contract (docs/contracts/host-mounted-runs.org) builds on, pinned on the CURRENT source
  so the RED rows are measured against them. Every row passes today; none is a host feature row.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @deadline 10_000
  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @context_keys [:run_dir, :spec, :plan, :trace, :barrier, :restart_empty]

  defmodule ParentExitProbe do
    @moduledoc false
    @behaviour :gen_statem

    def start_link(observer), do: :gen_statem.start_link(__MODULE__, observer, [])
    def callback_mode, do: :handle_event_function

    def init(observer) do
      Process.flag(:trap_exit, true)
      {:ok, :idle, observer}
    end

    def handle_event(:info, message, :idle, observer) do
      send(observer, {:event, message})
      :keep_state_and_data
    end

    def terminate(reason, _state, observer), do: send(observer, {:terminated, reason})
  end

  defmodule SlowStop do
    @moduledoc false
    use GenServer

    def start_link(ms), do: GenServer.start_link(__MODULE__, ms)

    def init(ms) do
      Process.flag(:trap_exit, true)
      {:ok, ms}
    end

    def terminate(_reason, ms), do: Process.sleep(ms)
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "mount_base_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  test "MB-1 OTP delivers the PARENT's EXIT of a gen_statem to terminate/3, never as an event" do
    observer = self()

    parent =
      spawn(fn ->
        {:ok, pid} = ParentExitProbe.start_link(observer)
        send(observer, {:probe, pid})

        receive do
          :die -> exit(:shutdown)
        end
      end)

    assert_receive {:probe, probe}, @deadline
    ref = Process.monitor(probe)
    send(parent, :die)
    assert_receive {:terminated, :shutdown}, @deadline
    assert_receive {:DOWN, ^ref, :process, ^probe, :shutdown}, @deadline
    refute_received {:event, {:EXIT, _, _}}
  end

  test "MB-2 a trapped linked plain process keeps running with its parent's EXIT queued behind an unmatched receive" do
    observer = self()

    parent =
      spawn(fn ->
        child =
          spawn_link(fn ->
            Process.flag(:trap_exit, true)
            send(observer, {:blocked, self()})

            receive do
              :release -> :ok
            end
          end)

        send(observer, {:child, child})

        receive do
          :never -> :ok
        end
      end)

    assert_receive {:child, child}, @deadline
    assert_receive {:blocked, ^child}, @deadline
    ref = Process.monitor(parent)
    Process.exit(parent, :kill)
    assert_receive {:DOWN, ^ref, :process, ^parent, :killed}, @deadline
    Process.sleep(100)
    assert Process.alive?(child)
    assert {:messages, [{:EXIT, ^parent, :killed}]} = Process.info(child, :messages)
    Process.exit(child, :kill)
  end

  test "MB-3 a :sys-suspended GenServer still terminates on its PARENT's exit (the suspend loop handles it)" do
    observer = self()

    parent =
      spawn(fn ->
        {:ok, pid} = SlowStop.start_link(0)
        :ok = :sys.suspend(pid)
        send(observer, {:suspended, pid})

        receive do
          :die -> exit(:shutdown)
        end
      end)

    assert_receive {:suspended, pid}, @deadline
    ref = Process.monitor(pid)
    send(parent, :die)
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, @deadline
  end

  test "MB-4 DynamicSupervisor terminates its children concurrently (bounded by the child shutdown, not the sum)" do
    {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)

    for _ <- 1..3 do
      {:ok, _} =
        DynamicSupervisor.start_child(sup, %{id: SlowStop, start: {SlowStop, :start_link, [300]}, shutdown: 2_000})
    end

    started = System.monotonic_time(:millisecond)
    :ok = DynamicSupervisor.stop(sup)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < 700, "three 300 ms terminates took #{elapsed} ms: not concurrent"
  end

  test "MB-5 Run.Recovery.evidence/2 reads the journal only: it neither acquires nor disturbs a held lock", %{dir: dir} do
    arb = start_supervised!({Ownership, name: nil})
    File.write!(Path.join(dir, "events.jsonl"), "", [:exclusive])
    {:ok, writer, _} = Writer.open(dir, fs: SystemFs.new(), lock: lock_opts(), ownership: [server: arb])
    {:ok, %{"token" => token}} = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, _evidence} = Run.Recovery.evidence(dir)
    assert {:ok, %{"token" => ^token}} = RunLock.owner(SystemFs.new(), dir)
    assert {:ok, %{writer: ^writer, state: :live}} = Ownership.status(dir, server: arb)
    :ok = Writer.close(writer)
  end

  test "MB-6a (permanent preservation) Run.Supervisor without an ownership option registers with the GLOBAL arbiter",
       %{dir: dir} do
    Process.flag(:trap_exit, true)
    {:ok, sup} = Run.Supervisor.start_link(run_config(dir, []))
    assert {:ok, %{state: :live}} = Ownership.status(dir)
    :ok = Supervisor.stop(sup, :shutdown, @deadline)
    assert :none == Ownership.status(dir)
  end

  # MB-6b (historical: "an injected ownership option is ignored by Run.Supervisor") was RETIRED explicitly by the
  # passthrough delivered with the mounted-runs slice (docs/contracts/host-mounted-runs.org section 6); its
  # requirement now lives in host_mount_red_test.exs RP-1/RP-2. MB-6a above remains the permanent preservation row.

  defp lock_opts,
    do: [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]

  defp run_config(dir, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
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
        supervisor_instance: "sup_mount_0001",
        trace: nil
      )
      |> Keyword.merge(extra)

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_mount_0001",
        command_id: "cmd_mount_base_00000001",
        now: @now
      )

    %{
      run_dir: dir,
      mode: :run,
      spec: spec,
      plan: plan,
      opts: ctx |> Keyword.drop(@context_keys) |> Keyword.drop(@owned),
      trace: nil,
      command: command
    }
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))
end
