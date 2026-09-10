defmodule AiOrchestrator.Run.EffectOwnerNativeRedTest do
  @moduledoc """
  RED/interface: native boundaries A/B/C in the ACTUAL lifecycle flow (ruling m_1788681693000 item 6; EO-M3).
  The real guardian runs through Commands.invoke -> Run.Executor -> Run.Supervisor; a gate barrier kills the
  process that executes the effect (today the Server: CONTROL rows; after GREEN the worker: RED rows) at
  :after_claim / :after_ack / :after_go. One oracle serves both partitions; its expectations were DERIVED from the
  unchanged production controls (measured 2026-09-06 on 20be945) and are pinned exactly: baseline behavior wins.
  Cold recovery is an explicitly NEW subtree (executor resume, new Writer); never automatic restart.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track_dir!: 1, spawn_caller!: 1]

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @moduletag :native
  @moduletag timeout: 120_000
  @source Path.expand("../../bin/build-guardian", __DIR__)
  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-06T08:00:00Z", unix: 1_788_681_600}
  @instance "sup_owner_native"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @argv ["/bin/sh", "-c", "echo ran >> ran; echo ran; sleep 3"]

  defp worker, do: Module.concat(["AiOrchestrator", "Run", "Worker"])
  defp require_worker!, do: assert(Code.ensure_loaded?(worker()), "AiOrchestrator.Run.Worker does not exist")

  setup_all do
    dir = Path.join(System.tmp_dir!(), "owner-native-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd(@source, [bin], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "owner-native-run-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  defp scenario_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp sha(term), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, Jason.encode!(term)), case: :lower)

  defp real_gates(spec, argv, run_dir),
    do:
      spec
      |> Map.update!("gates", fn gates -> Map.new(gates, fn {id, _} -> {id, argv} end) end)
      |> Map.put("repo_root", run_dir)

  defp context(dir, helper, at) do
    spec = real_gates(H.spec("gated_run_seed"), @argv, dir)
    plan = H.plan("gated_run_seed")

    scenario_opts()
    |> Keyword.drop(@owned)
    |> Keyword.put(:gate_executor, Execution)
    |> Keyword.put(:gate_helper, helper)
    |> Keyword.put(:repo_root, dir)
    |> Keyword.put(:gate_runner, fn _gate, _opts ->
      raise "the legacy GateRunner must never run on the orchestrated route"
    end)
    |> Keyword.merge(
      run_dir: dir,
      spec: spec,
      plan: plan,
      spec_hash: sha(spec),
      plan_hash: sha(plan),
      supervisor_instance: @instance,
      trace: collector(),
      gate_opts: [barrier: barrier(collector(), at)]
    )
  end

  # two-way READY barrier: the executing process reports the identity (through the collector, so it is owned) and
  # WAITS for the test to register the OS absence oracle before the boundary may advance; then it is killed at `at`
  defp barrier(collector, at) do
    fn
      :after_ready, identity ->
        send(collector, {:identity, identity, self()})

        receive do
          :go -> true
        after
          10_000 -> exit(:ready_never_acknowledged)
        end

      name, _info when name == at ->
        Process.exit(self(), :kill)

      _name, _info ->
        true
    end
  end

  defp invoke!(verb, args, ctx) do
    spawn_caller!(fn ->
      {verb,
       Commands.invoke(@operator, verb, args,
         run_id: "run_owner_native",
         command_id: CommandId.generate(),
         now: @now,
         executor: AiOrchestrator.Run.Executor,
         executor_opts: ctx
       )}
    end)
  end

  defp start_args(ctx), do: %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]}

  # registers the OS oracle BEFORE the boundary is allowed to advance; the identity is reaped on exit
  defp track_ready! do
    assert_receive {:identity, identity, executing}, 15_000
    OwnedHarness.os_oracle!(fn -> dead?(identity) end)
    send(executing, :go)
    {identity, executing}
  end

  defp result!(verb) do
    receive do
      {:result, {^verb, r}} -> r
    after
      60_000 -> flunk("no #{verb} result")
    end
  end

  # ---- journal oracles ----
  defp journal_bytes(dir), do: File.read!(Path.join(dir, "events.jsonl"))
  defp journal(dir), do: dir |> journal_bytes() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp ran_lines(dir) do
    case File.read(Path.join(dir, "ran")) do
      {:ok, s} -> length(String.split(s, "\n", trim: true))
      _ -> 0
    end
  end

  defp starts(events, attempt),
    do: Enum.count(events, &(&1["type"] == "gate_started" and &1["data"]["attempt"] == attempt))

  defp attention_reasons(events),
    do: for(%{"type" => "human_attention_required", "data" => %{"reason" => r}} <- events, do: r)

  # ---- OS oracles ----
  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n", trim: true) |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
      {"", 1} -> []
      {out, status} -> flunk("ps proved nothing (#{status}): #{inspect(out)}")
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do:
      signal_zero(Integer.to_string(pid)) == :gone and signal_zero("-" <> Integer.to_string(pgid)) == :gone and
        members(pgid) == []

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    fun.()
  end

  # ---- the boundary oracles: expectations DERIVED from unchanged production (measured 2026-09-06) ----
  #
  # CONSISTENT clock (the real clock on BOTH runs, gate `echo ran >> ran; echo ran; sleep 3`): the crash run's gate
  # deadline is minutes ahead, so a dead attempt 1 is reconciled through the real Execution.reconcile (expected
  # fields = the journaled gate_started data + run_id + journaled) and rerun as the SAME gate at attempt 2, which
  # runs to GO and the run completes. Per-attempt GO evidence is the guardian's own stdout file per attempt.
  @consistent %{
    after_claim: %{starts: [], last: "gate_requested", recovery: :blocked_prepare_failed, reconcile: 0},
    after_ack: %{starts: [1], last: "gate_started", recovery: :rerun_attempt_2, reconcile: 1},
    after_go: %{starts: [1], last: "gate_started", recovery: :rerun_attempt_2, reconcile: 1}
  }
  # FIXED clock on the recovery (the test clock reads BEFORE the guardian's real start): a NEGATIVE control - the
  # reducer reports gate_recovery_clock_skew after the dead reconcile and blocks; nothing is rerun.
  @skew %{after_ack: "gate_recovery_clock_skew", after_go: "gate_recovery_clock_skew", after_claim: "gate_prepare_failed"}

  @real_clock AiOrchestrator.Clock.SystemClock

  defmodule FixedClockRef do
    @moduledoc false
    # the scenario harness's own fixed clock (whatever module it binds), so the skew rows use the SAME fixed clock
    def clock, do: H.cases() |> hd() |> elem(4) |> then(& &1.()) |> Keyword.fetch!(:clock)
  end

  defp crash!(dir, helper, at, clock, executing_check) do
    ctx = dir |> context(helper, at) |> Keyword.put(:clock, clock)
    invoke!("start", start_args(ctx), ctx)
    # U1b-0b-L shape migration (docs/contracts/owner-loss-generation.org): the exact Writer sibling's registration
    # is captured independently while the run is still executing, BEFORE the worker is killed
    assert_receive {:run_child_started, _root, :writer, writer1}, 15_000
    assert {:ok, %{writer: ^writer1, generation: generation, state: :live}} = Ownership.status(dir)
    {identity, executing} = track_ready!()
    executing_check.(executing)
    result = result!("start")
    assert {:error, %{clause: _}} = result
    assert wait_until(fn -> dead?(identity) end, 15_000), "the guardian settles the group on control EOF"
    {result, writer1, generation}
  end

  # the exact expected loss result for a clause: owner loss carries the captured registration generation
  defp expected_loss("run_effect_owner_down", generation),
    do: {:error, %{clause: "run_effect_owner_down", writer_generation: generation}}

  defp expected_loss(clause, _generation), do: {:error, %{clause: clause}}

  defp attempt_out(dir, attempt) do
    case File.read(Path.join(dir, "gates/gr_0001.#{attempt}.out")) do
      {:ok, s} -> s
      _ -> :absent
    end
  end

  defp recover!(dir, helper, clock, observer) do
    ctx2 = dir |> context(helper, :none) |> Keyword.put(:clock, clock) |> Keyword.put(:effect_observer, observer)
    invoke!("resume", %{"recovery_reason" => "crash_recovery"}, ctx2)
    assert_receive {:run_child_started, _root2, :writer, writer2}, 15_000

    recovered =
      receive do
        {:identity, identity2, executing2} ->
          OwnedHarness.os_oracle!(fn -> dead?(identity2) end)
          send(executing2, :go)
          result!("resume")

        {:result, {"resume", r}} ->
          r
      after
        60_000 -> flunk("recovery neither ran a gate nor answered")
      end

    {recovered, writer2}
  end

  # every ReconcileGate the recovery performs, with its expected fields, observed through the effect observer
  defp reconcile_observer(collector) do
    fn effect, _observation ->
      if match?(%AiOrchestrator.Contract.Effect.ReconcileGate{}, effect),
        do: send(collector, {:reconciled, effect.gate_run_id, effect.attempt, effect.expected})

      :ok
    end
  end

  defp reconciles do
    receive do
      {:reconciled, id, attempt, expected} -> [{id, attempt, expected} | reconciles()]
    after
      0 -> []
    end
  end

  defp gate_starts(events),
    do: for(%{"type" => "gate_started", "data" => %{"gate_run_id" => "gr_0001", "attempt" => a}} <- events, do: a)

  # the consistent-clock boundary: crash, then recovery through the unchanged production path
  defp consistent_boundary!(dir, helper, at, clause, executing_check) do
    expect = Map.fetch!(@consistent, at)
    {result, writer1, generation} = crash!(dir, helper, at, @real_clock, executing_check)
    assert result == expected_loss(clause, generation)
    before_bytes = journal_bytes(dir)
    before = journal(dir)
    assert gate_starts(before) == expect.starts
    assert List.last(before)["type"] == expect.last
    marker_at_crash = ran_lines(dir)
    assert marker_at_crash in [0, 1]
    out1_at_crash = attempt_out(dir, 1)
    if at != :after_go, do: assert(marker_at_crash == 0 and out1_at_crash in [:absent, ""], "no GO before :after_go")

    {recovered, writer2} = recover!(dir, helper, @real_clock, reconcile_observer(collector()))
    assert writer2 != writer1, "a distinct new Writer, not the crashed one"
    after_bytes = journal_bytes(dir)
    assert String.starts_with?(after_bytes, before_bytes), "the durable prefix is preserved byte for byte"
    after_events = journal(dir)
    appended = Enum.drop(after_events, length(before))
    assert appended != []

    # the reconcile's expected fields are the NEW Writer's verified prefix: the journaled gate_started data exactly
    started = Enum.find(after_events, &(&1["type"] == "gate_started" and &1["data"]["attempt"] == 1))
    OwnedHarness.flush!()

    case reconciles() do
      [] ->
        assert expect.reconcile == 0

      [{"gr_0001", 1, expected}] ->
        assert expect.reconcile == 1
        assert expected == Map.merge(started["data"], %{"run_id" => started["run_id"], "journaled" => true})
    end

    case expect.recovery do
      :blocked_prepare_failed ->
        assert {:ok, %{summary: %{"status" => "blocked"}}} = recovered
        assert attention_reasons(appended) == ["gate_prepare_failed"]
        assert gate_starts(after_events) == []
        assert ran_lines(dir) == marker_at_crash

      :rerun_attempt_2 ->
        assert {:ok, %{summary: %{"status" => "completed"}}} = recovered
        assert gate_starts(after_events) == [1, 2], "the SAME gate reruns once at attempt 2; attempt 1 never again"
        assert Enum.count(appended, &(&1["type"] == "gate_passed")) == 1
        assert attempt_out(dir, 2) == "ran\n", "attempt 2 reached GO exactly once (its own stdout)"
        assert attempt_out(dir, 1) == out1_at_crash, "attempt 1's evidence never changes after the crash"
        assert ran_lines(dir) == marker_at_crash + 1, "the shared marker grows by attempt 2 only"
    end
  end

  # the fixed-clock boundary (negative control): the recovery clock reads before the real start -> skew, blocked
  defp skew_boundary!(dir, helper, at, clause, executing_check) do
    {result, writer1, generation} = crash!(dir, helper, at, FixedClockRef.clock(), executing_check)
    assert result == expected_loss(clause, generation)
    before = journal(dir)
    before_bytes = journal_bytes(dir)
    marker_at_crash = ran_lines(dir)
    {recovered, writer2} = recover!(dir, helper, FixedClockRef.clock(), reconcile_observer(collector()))
    assert writer2 != writer1
    assert String.starts_with?(journal_bytes(dir), before_bytes)
    assert {:ok, %{summary: %{"status" => "blocked"}}} = recovered
    appended = Enum.drop(journal(dir), length(before))
    assert attention_reasons(appended) == [Map.fetch!(@skew, at)]
    assert gate_starts(journal(dir)) == gate_starts(before), "nothing is rerun under skew"
    assert ran_lines(dir) == marker_at_crash
  end

  # ---- N-0 control: the real gate through the executor path, one GO, completed ----
  test "control: N-0 a real gate through the executor completes with exactly one GO", %{dir: dir, helper: helper} do
    ctx = context(dir, helper, :none)
    ctx = Keyword.put(ctx, :spec, real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "echo ran >> ran; exit 0"], dir))
    ctx = Keyword.put(ctx, :spec_hash, sha(ctx[:spec]))
    invoke!("start", start_args(ctx), ctx)
    {_identity, _executing} = track_ready!()
    assert {:ok, %{summary: %{"status" => "completed"}}} = result!("start")
    assert ran_lines(dir) == 1
    assert starts(journal(dir), 1) == 1
  end

  for {label, at} <- [
        {"N-A before gate_started is durable (:after_claim)", :after_claim},
        {"N-B after the durable receipt before GO (:after_ack)", :after_ack},
        {"N-C after GO (:after_go)", :after_go}
      ] do
    # INVARIANT control: whoever executes today, the crash closes with a closed clause and the unchanged production
    # recovery behaves exactly as measured; the executing identity and the clause are pinned only by the RED row
    test "control: #{label}, consistent clock: closed crash; recovery through unchanged production pinned exactly",
         %{dir: dir, helper: helper} do
      {result, _writer, _generation} = crash!(dir, helper, unquote(at), @real_clock, fn _executing -> :ok end)
      assert {:error, %{clause: clause}} = result
      assert is_binary(clause)
      # re-run the full oracle from a fresh directory so the crash above is only the closed-clause witness
      dir2 = Path.join(dir, "oracle")
      File.mkdir_p!(dir2)
      consistent_boundary!(dir2, helper, unquote(at), clause, fn _executing -> :ok end)
    end

    test "control: #{label}, fixed clock (negative): skew is reported after the dead reconcile; nothing reruns",
         %{dir: dir, helper: helper} do
      {result, _writer, _generation} = crash!(dir, helper, unquote(at), FixedClockRef.clock(), fn _executing -> :ok end)
      assert {:error, %{clause: clause}} = result
      dir2 = Path.join(dir, "skew")
      File.mkdir_p!(dir2)
      skew_boundary!(dir2, helper, unquote(at), clause, fn _executing -> :ok end)
    end

    test "#{label}: the WORKER executes and is killed (run_effect_owner_down); consistent-clock recovery identical to the baseline",
         %{dir: dir, helper: helper} do
      require_worker!()

      consistent_boundary!(dir, helper, unquote(at), "run_effect_owner_down", fn executing ->
        assert_receive {:run_child_started, _work, :worker, ^executing}, 15_000
      end)
    end
  end
end
