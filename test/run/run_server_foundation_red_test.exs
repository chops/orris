defmodule AiOrchestrator.Run.ServerFoundationRedTest do
  @moduledoc """
  RED/interface for the foreground Run.Server foundation (docs/contracts/run-server-foundation.org),
  rev 4 after Codex reviews m_1788637055000, m_1788641220000 and m_1788642300000 (MUST-1..15,
  Q-1..Q-7 ruled); GREEN from 99be98b (GO m_1788643500000). The test corrections made while turning
  GREEN are listed in docs/contracts/run-server-foundation.org ("GREEN disclosures").

  The Run modules and the Host stepping API were late-bound (runtime-computed receivers) while RED so
  the tree compiled warning-free; the binding is kept as written. Tests named "control:" are
  baseline-green and never touch the missing modules: they prove every fixture, harness seam and
  OS/OTP fact the REDs rely on, including the harness's own lifetime and cross-process mechanisms.

  Receiver binding (MUST-12): every receiver-bound value (a config's `trace`, a held gate runner, a
  barrier) is built in the TEST process; only the bare start_link call crosses into the owner
  process, through `start_run!/2`, which refuses a config whose trace is not the calling test.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Diagnostic
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Run.DeadlineFence
  alias AiOrchestrator.Run.ServerFoundationRedTest.EntryClock
  alias AiOrchestrator.Run.ServerFoundationRedTest.HeldGate
  alias AiOrchestrator.Run.ServerFoundationRedTest.QueuedAdapter
  alias AiOrchestrator.Run.ServerFoundationRedTest.RaisingAwaitExecutor
  alias AiOrchestrator.Run.ServerFoundationRedTest.Seam
  alias AiOrchestrator.Run.ServerFoundationRedTest.SeamClock
  alias AiOrchestrator.Run.ServerFoundationRedTest.ThrowingAwaitExecutor
  alias AiOrchestrator.Run.ServerFoundationRedTest.UnsettledExecutor
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.OwnerOracle
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @host_src Path.expand("../../lib/ai_orchestrator/lifecycle/host.ex", __DIR__)
  @kill9 Path.expand("../fixtures/contracts/scenarios/kill9_resume", __DIR__)
  @guardian_src Path.expand("../../bin/build-guardian", __DIR__)
  @zero "sha256:" <> String.duplicate("ab", 32)

  # ---- the shared test seam (MUST-8): one named Agent, read from any process ----
  # Carries the recovery clock instant, the abandon-notification subscriber and named flags across
  # the test -> Server -> Writer -> FaultFs-Agent boundaries. Test-only; production copies nothing.
  defmodule Seam do
    @moduledoc false
    # unlinked: teardown callbacks read it after the test process has exited
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
    def put(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))
    def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))
    def take(key), do: Agent.get_and_update(__MODULE__, &Map.pop(&1, key))
  end

  defmodule SeamClock do
    @moduledoc false
    def unix_now, do: Seam.get(:now) || FixedClock.unix_now()
    def wall_ts, do: unix_now() |> DateTime.from_unix!() |> DateTime.to_iso8601()
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  # a non-waiting Host-entry witness (MUST-13): the FIRST configured-clock read by a process is
  # reported once, from that process, to the seam's recipient. The Host stamps the first committed
  # event through the configured clock before any effect executes, so for the Server process this
  # is the entry into Host stepping; the effect observer remains POST-effect evidence.
  defmodule EntryClock do
    @moduledoc false
    def unix_now, do: witness(FixedClock.unix_now())
    def wall_ts, do: witness(FixedClock.wall_ts())
    def monotonic_ms, do: witness(FixedClock.monotonic_ms())

    defp witness(value) do
      if Process.get({__MODULE__, :witnessed}) == nil do
        Process.put({__MODULE__, :witnessed}, true)
        if pid = Seam.get(:entry_recipient), do: send(pid, {:host_entry, self(), :first_clock_read})
      end

      value
    end
  end

  # ---- late-bound receivers ----
  defp host, do: Module.concat(["AiOrchestrator", "Lifecycle", "Host"])
  defp run_sup, do: Module.concat([AiOrchestrator, Run, Supervisor])
  defp run_server, do: Module.concat([AiOrchestrator, Run, Server])
  defp work_sup, do: Module.concat([AiOrchestrator, Run, Work, Supervisor])
  defp loaded?(module), do: Code.ensure_loaded?(module)

  defp require_run!,
    do:
      assert(
        loaded?(run_sup()) and loaded?(run_server()) and loaded?(work_sup()),
        "AiOrchestrator.Run.{Supervisor,Server,Work.Supervisor} do not exist"
      )

  setup do
    case Seam.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> Seam.reset()
    end

    Seam.put(:abandon_subscriber, self())
    on_exit(fn -> if Process.whereis(Seam), do: Seam.reset() end)
    :ok
  end

  # ---- fixtures on disk ----
  defp tmp_run_dir do
    dir = Path.join(System.tmp_dir!(), "run-server-red-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end

  # ---- test-owned Writer teardown (rulings m_1788814165000, m_1788814670000 WT-M1..M4) ----
  # `Writer.open/2` LINKS the holder to the opener and the Writer traps exits, so once the owning test process
  # exits the holder terminates on its own; an on_exit liveness snapshot followed by `close` races that shutdown
  # (post-push CI 34160013128, seed 958009: EXIT :shutdown inside GenServer.call). The owning test therefore closes
  # its holder in try/after BEFORE it exits and requires the real :ok (the PRIMARY body failure is preserved and a
  # close failure attached, never substituted). Process settlement and filesystem removal are ONE controlled
  # sequence: the directory is removed only after the join proved absence; otherwise it is PRESERVED and the
  # failure raised (an independent LIFO rm_rf callback would run regardless, WT-M4).

  # a run directory WITHOUT an independent rm_rf: removal is owned by settle_then_remove!/2
  defp guarded_dir! do
    dir = Path.join(System.tmp_dir!(), "run-server-red-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end

  # settlement first (raises and exits are both failures); removal ONLY on proven absence; else preserve + raise
  defp settle_then_remove!(dir, settle) do
    outcome =
      try do
        {:settled, settle.()}
      rescue
        e -> {:unsettled, Exception.message(e)}
      catch
        kind, reason -> {:unsettled, "#{kind}: #{inspect(reason)}"}
      end

    case outcome do
      {:settled, proof} ->
        File.rm_rf!(dir)
        {:removed, proof}

      {:unsettled, why} ->
        raise "directory preserved (#{dir}): settlement unproven: #{why}"
    end
  end

  defp with_holder(dir, lock, fun), do: with_holder(dir, lock, [], fun)

  defp with_holder(dir, lock, open_opts, fun) do
    {:ok, holder, opened} = Writer.open(dir, Keyword.merge([create: true, lock: lock], open_opts))
    on_exit(fn -> settle_then_remove!(dir, fn -> join_holder!(holder) end) end)

    # the PRIMARY outcome is captured for raise, throw and exit alike: the owner-local close always runs
    body =
      try do
        {:ok, fun.(holder, opened)}
      catch
        kind, value -> {:primary, kind, value, __STACKTRACE__}
      end

    closed =
      try do
        Writer.close(holder)
      catch
        :exit, reason -> {:exit, reason}
      end

    case {body, closed} do
      {{:ok, value}, :ok} -> value
      {{:ok, _}, other} -> raise "writer close failed in the owning test: #{inspect(other)}"
      {{:primary, :error, e, st}, :ok} -> reraise(e, st)
      {{:primary, :throw, value, _}, :ok} -> throw(value)
      {{:primary, :exit, reason, _}, :ok} -> exit(reason)
      {{:primary, kind, value, st}, other} -> reraise(both(kind, value, st, other), st)
    end
  end

  # the PRIMARY failure stays primary; a genuine secondary close failure is never dropped: an AssertionError keeps
  # its type with the secondary appended, any other kind is reported with its original kind, reason and stack
  # formatted verbatim plus the secondary (the original stacktrace is reraised with it)
  defp both(:error, %ExUnit.AssertionError{message: m} = e, _st, other),
    do: %{e | message: "#{m} (writer close also failed: #{inspect(other)})"}

  defp both(kind, value, st, other) do
    %RuntimeError{
      message: "primary #{kind}: #{Exception.format(kind, value, st)}\nwriter close also failed: #{inspect(other)}"
    }
  end

  @join_ms 5_000

  # monitor FIRST (the caller owns it, installed before the event whose reason is asserted), then ALWAYS attempt
  # the close: a call meeting a holder that is terminating on its owner's exit answers :noproc / :shutdown, which
  # is tolerated ONLY for those termination reasons and ONLY when the joined DOWN then proves termination (never
  # called a successful close); a live holder answers the real close result, and a REAL close failure raises
  # AFTER the termination was joined; anything else raises (no blanket catch)
  defp join_holder!(holder) do
    ref = Process.monitor(holder)

    result =
      try do
        {:closed, Writer.close(holder)}
      catch
        :exit, {reason, {GenServer, :call, _}} when reason in [:shutdown, :noproc, :normal] -> {:racing, reason}
        :exit, {{:shutdown, _}, {GenServer, :call, _}} -> {:racing, :shutdown}
      end

    receive do
      {:DOWN, ^ref, :process, ^holder, reason} when reason in [:noproc, :normal, :shutdown] ->
        case result do
          {:closed, :ok} -> {{:closed, :ok}, reason}
          {:closed, failure} -> raise "writer holder close failed: #{inspect(failure)} (terminated #{inspect(reason)})"
          {:racing, racing} -> {:already_gone, racing}
        end

      {:DOWN, ^ref, :process, ^holder, other} ->
        raise "writer holder died abnormally after #{inspect(result)}: #{inspect(other)}"
    after
      @join_ms -> raise "writer holder did not terminate within #{@join_ms} ms after #{inspect(result)}"
    end
  end

  # a linked OWNER process (not the test) that opens a Writer and exits on command. REGISTERED before it is
  # permitted to do any work (the existing harness pattern): the reaper is on_exit'ed first, then the permit is
  # sent; the owner publishes its holder into a per-owner record the reaper can read even if the test fails
  # before receiving the publication
  # Allocation registration (precision on WT-M3/M6): the owner records :opening BEFORE Writer.open and the pid after,
  # so the reaper can tell "never reached allocation" (nil) and "open in flight at death" (:opening) from a known
  # holder; both unknown states fail CLOSED (preserve, raise), never labelled absence. `mode` :die_before_publish
  # is the deterministic gap control: the owner exits after open returned but before publication, telling only the
  # TEST the orphan's pid so the control can join it; `register?: false` lets that control drive the reaper itself.
  defp spawn_owner!(dir, lock, opts \\ []) do
    test = self()
    permit = make_ref()
    mode = Keyword.get(opts, :mode, :normal)

    owner = spawn(fn -> owner_body(test, permit, dir, lock, mode) end)

    # cleanup is registered BEFORE the permit: the subject reaper for the normal mode, the test-owned control
    # cleanup for the gap controls; nothing fallible (publication, assertions) precedes either registration
    case Keyword.get(opts, :cleanup, :subject) do
      :subject -> register_reaper!(dir, owner)
      :control -> register_control_cleanup!(dir, owner)
    end

    send(owner, {:start, permit})

    case mode do
      :normal ->
        assert_receive {:holder, holder}, 5_000
        {owner, holder}

      _gap ->
        assert_receive {:holder_unpublished, holder}, 5_000
        {owner, holder}
    end
  end

  # one controlled sequence: settle the owner's actors, then remove the directory only on proven absence
  defp register_reaper!(dir, owner), do: on_exit(fn -> settle_then_remove!(dir, fn -> reap_actors!(owner) end) end)

  defp owner_body(test, permit, dir, lock, mode) do
    receive do
      {:start, ^permit} -> :ok
    after
      5_000 -> exit(:owner_never_permitted)
    end

    :persistent_term.put({__MODULE__, :holder_of, self()}, :opening)
    {:ok, holder, _} = Writer.open(dir, create: true, lock: lock)
    publish_or_die(test, holder, mode)

    receive do
      {:exit_with, reason} -> exit(reason)
    end
  end

  # gap modes: the SUBJECT record stays :opening (the subject reaper must see unknown); the orphan is published
  # into the separate TEST-OWNED registry before anything fallible, then the owner dies before its publication
  defp publish_or_die(test, holder, :die_before_publish) do
    :persistent_term.put({__MODULE__, :control_registry, self()}, holder)
    send(test, {:holder_unpublished, holder})
    exit(:shutdown)
  end

  # early-failure gap: the owner dies before even the registry knows the orphan (the test learns it only through
  # the channel); models a failure that precedes every publication
  defp publish_or_die(test, holder, :die_before_registry) do
    send(test, {:holder_unpublished, holder})
    exit(:shutdown)
  end

  defp publish_or_die(test, holder, _normal) do
    :persistent_term.put({__MODULE__, :holder_of, self()}, holder)
    send(test, {:holder, holder})
  end

  # failure-path reaper (owns its monitors): the owner's holder (if it ever opened one) is resumed if suspended
  # and closed through the real path; the owner is killed and joined; a holder that does not close cleanly is
  # killed, joined and REPORTED (raise -> the directory is preserved by settle_then_remove!)
  defp reap_actors!(owner) do
    # the OWNER is stopped and joined FIRST so no later publication can race the read; the record is then read.
    # nil or :opening after a permitted start is NOT proof of absence (a Writer can exist between open return and
    # publication, and a suspended one survives its owner): both are UNKNOWN and fail closed
    kill_join!(owner)
    holder = :persistent_term.get({__MODULE__, :holder_of, owner}, nil)

    holder_outcome =
      case holder do
        # the owner was permitted and died before allocation registration, or with an open in flight: the holder's
        # existence is UNKNOWN; absence is never labelled, the caller preserves and reports
        nil ->
          {:failed, "holder unknown: owner died before allocation registration"}

        :opening ->
          {:failed, "holder unknown: Writer.open in flight at owner death (unpublished)"}

        pid ->
          _ = try(do: :sys.resume(pid), catch: (:exit, _ -> :not_running))

          try do
            {:ok, join_holder!(pid)}
          rescue
            e -> {:failed, Exception.message(e)}
          catch
            kind, reason -> {:failed, "#{kind}: #{inspect(reason)}"}
          end
      end

    # the record is released ONLY once the ownership set is resolved: a settled holder, or a known holder killed
    # and joined; an UNKNOWN holder (nil / :opening) keeps its record so a later resolution can still find it
    case holder_outcome do
      {:ok, proof} ->
        :persistent_term.erase({__MODULE__, :holder_of, owner})
        proof

      {:failed, why} when is_pid(holder) ->
        kill_join!(holder)
        :persistent_term.erase({__MODULE__, :holder_of, owner})
        raise "reaper: holder did not close cleanly: #{why}"

      {:failed, why} ->
        raise "reaper: #{why} (record retained: unresolved)"
    end
  end

  # the test-owned control cleanup, registered BEFORE the permit: every KNOWN actor (owner; the orphan from the
  # test-owned registry) is joined; records are erased only after the known orphan's DOWN; the directory is
  # removed only with that absence proof; an orphan unknown to the registry too is preserved and reported
  defp register_control_cleanup!(dir, owner), do: on_exit(fn -> control_cleanup!(dir, owner) end)

  defp control_cleanup!(dir, owner) do
    if Process.alive?(owner), do: kill_join!(owner)
    orphan = :persistent_term.get({__MODULE__, :control_registry, owner}, nil)

    case orphan do
      pid when is_pid(pid) ->
        _ = try(do: :sys.resume(pid), catch: (:exit, _ -> :not_running))
        if Process.alive?(pid), do: kill_join!(pid)
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          @join_ms -> raise "control cleanup: orphan #{inspect(pid)} did not terminate"
        end

        :persistent_term.erase({__MODULE__, :control_registry, owner})
        :persistent_term.erase({__MODULE__, :holder_of, owner})
        File.rm_rf!(dir)
        {:resolved, pid}

      nil ->
        record = :persistent_term.get({__MODULE__, :holder_of, owner}, nil)

        # idempotent: a state already resolved by an earlier run (no records, no directory) is not unknown
        if is_nil(record) and not File.exists?(dir) do
          :already_resolved
        else
          raise "control cleanup: orphan unknown to the registry (record #{inspect(record)}); directory preserved (#{dir})"
        end
    end
  end

  # a raw test-owned holder registered BEFORE any fallible operation: the failure-path callback resumes it if it
  # was left suspended, settles it through the real path and removes the directory only on proven absence
  defp register_holder!(dir, holder) do
    on_exit(fn ->
      settle_then_remove!(dir, fn ->
        _ = try(do: :sys.resume(holder), catch: (:exit, _ -> :not_running))
        join_holder!(holder)
      end)
    end)

    holder
  end

  # a raw holder opened by the test itself (not through with_holder), registered at allocation
  defp open_registered!(dir, lock, open_opts \\ []) do
    {:ok, holder, opened} = Writer.open(dir, Keyword.merge([create: true, lock: lock], open_opts))
    {register_holder!(dir, holder), opened}
  end

  defp kill_join!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      @join_ms -> raise "reaper: #{inspect(pid)} survived :kill for #{@join_ms} ms"
    end
  end

  # a task registered at allocation and joined (killed if needed) even when later assertions fail
  # (the on_exit callback is NOT the task owner, so it never calls Task.shutdown: it kills and joins on its own monitor)
  defp registered_task!(fun) do
    task = Task.async(fun)
    on_exit(fn -> kill_join!(task.pid) end)
    task
  end

  # every way out of a call, captured as data (a normal return, a raise, a throw or an exit)
  defp capture(fun) do
    {:ok, fun.()}
  rescue
    e -> {:rescued, e}
  catch
    :throw, v -> {:thrown, v}
    :exit, r -> {:exited, r}
  end

  defp wait_until_true(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(5) && false)
      end)

    fun.()
  end

  # non-consuming: the exact caller's close call is queued in the suspended holder
  defp close_queued?(holder, caller) do
    case Process.info(holder, :messages) do
      {:messages, msgs} -> Enum.any?(msgs, &match?({:"$gen_call", {^caller, _}, :close}, &1))
      nil -> false
    end
  end

  # non-consuming: two close calls from the exact callers are queued in the suspended holder, in that order
  defp closes_queued_in_order?(holder, first, second) do
    case Process.info(holder, :messages) do
      {:messages, msgs} ->
        a = Enum.find_index(msgs, &match?({:"$gen_call", {^first, _}, :close}, &1))
        b = Enum.find_index(msgs, &match?({:"$gen_call", {^second, _}, :close}, &1))
        is_integer(a) and is_integer(b) and a < b

      nil ->
        false
    end
  end

  # non-consuming: the suspended holder's mailbox holds the owner's EXIT and, AFTER it, the exact caller's close call
  defp exit_then_close_queued?(holder, owner, caller) do
    case Process.info(holder, :messages) do
      {:messages, msgs} ->
        exit_at = Enum.find_index(msgs, &match?({:EXIT, ^owner, _}, &1))
        call_at = Enum.find_index(msgs, &match?({:"$gen_call", {^caller, _}, :close}, &1))
        is_integer(exit_at) and is_integer(call_at) and exit_at < call_at

      nil ->
        false
    end
  end

  defp seed_legacy!(run_dir, lines) do
    File.write!(Path.join(run_dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    run_dir
  end

  defp seed_v2!(run_dir, lines) do
    File.write!(Path.join(run_dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")
    last = List.last(lines)

    receipt =
      Chain.encode_receipt(%{
        seq: length(lines),
        line_sha256: Chain.line_sha256(last <> "\n"),
        updated_at: FixedClock.wall_ts()
      })

    File.write!(Path.join(run_dir, "events.head"), receipt)
    run_dir
  end

  defp kill9(file), do: @kill9 |> Path.join(file) |> File.read!() |> String.split("\n", trim: true)

  # D1 on the UNCHANGED expired kill9 prefix (m_1788751607000 / m_1788752018000): a resume answers the exact expiry
  # for the due Observe; repair rows re-pin their subject on the ACTUAL journal suffix (cancel is unaffected)
  @expired_resume {:error, %{"reason" => "observation_timeout", "deadline_unix" => 1_767_225_600}}

  # the Writer's head receipt names exactly the last journaled line: real receipt evidence, not envelope syntax
  defp assert_head_receipt!(run_dir) do
    lines = run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)
    assert {:ok, %{seq: seq, line_sha256: hash}} = Chain.decode_receipt(File.read!(Path.join(run_dir, "events.head")))
    assert seq == length(lines) and hash == Chain.line_sha256(List.last(lines) <> "\n")
  end

  # U2b GREEN transition (recorded): the expired pre_dispatch prefix now expires at the DISPATCH (D1 at the Worker):
  # the run is blocked with the attention-only dispatch_deadline_exceeded, never a wedge, never a delivery
  defp repair_suffix!(started, :resume, run_dir, prior) do
    assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => [_]}}} =
             run_server().await(Keyword.fetch!(started, :server), 30_000)

    suffix = run_dir |> journal() |> Enum.drop(prior)

    assert %{"type" => "human_attention_required", "data" => %{"reason" => "dispatch_deadline_exceeded"}} =
             List.last(suffix)

    refute Enum.any?(suffix, &(&1["type"] in ["agent_wedge_detected", "assignment_dispatch_sent"]))
    suffix
  end

  defp repair_suffix!(started, _mode, _run_dir, _prior) do
    assert {:ok, %{appended_events: appended}} = run_server().await(Keyword.fetch!(started, :server), 30_000)
    appended
  end

  defp journal(run_dir),
    do:
      run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp strip(events), do: Enum.map(events, &Map.drop(&1, ["prev_line_sha256", "schema_version"]))

  # Host-direct executions keep the harness receipt sink; Server configs drop it (the Server binds the Writer)
  defp fresh_opts(index) do
    {_name, _kind, _scenario, _prior, opts_fun} = Enum.at(H.cases(), index)
    H.reset_seams()
    opts_fun.()
  end

  defp gated_index, do: Enum.find_index(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
  defp flaky_index, do: Enum.find_index(H.cases(), &match?({_, :run, "gate_failure_summary_feedback", [], _}, &1))

  # built in the TEST process only (its trace is the caller); `start_run!/2` enforces that binding
  defp config(run_dir, mode, opts, extra \\ %{}) do
    opts = Keyword.delete(opts, :event_sink)

    Map.merge(
      %{
        run_dir: run_dir,
        mode: mode,
        spec: H.spec("gated_run_seed"),
        plan: H.plan("gated_run_seed"),
        opts: opts,
        trace: self()
      },
      extra
    )
  end

  # ---- the owner harness (MUST-7, MUST-12, MUST-15): the REAL linked parent lives for the test's lifetime ----
  # `start_owned!/2` runs `start_fun` inside a trapping owner process that stays alive until the
  # single failure-safe teardown stops it. `start_fun` runs in the OWNER: it must be the bare
  # start_link over values already built in the test (see `start_run!/2`). A startup timeout kills
  # and reaps the owner and everything linked to it before failing. Teardown: stop the owner first (a
  # bounded Supervisor.stop through the owner), reap the owned subtree, then wait for every tracked
  # native group to be gone; an escalation is reported as a failure, never as success.
  defp start_owned!(start_fun, timeout \\ 5_000) do
    parent = self()

    owner =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        result = start_fun.()
        send(parent, {:owned_start, self(), result})
        owner_loop(result)
      end)

    ref = Process.monitor(owner)

    receive do
      {:owned_start, ^owner, {:ok, sup}} ->
        Process.demonitor(ref, [:flush])
        register_teardown(owner)
        {owner, sup}

      {:owned_start, ^owner, other} ->
        Process.demonitor(ref, [:flush])
        _ = kill_and_reap!([owner])
        other

      {:DOWN, ^ref, :process, ^owner, reason} ->
        flunk("the owner crashed while starting: #{inspect(reason)}")
    after
      timeout ->
        # a :kill propagated over a link reaches a trapping supervisor-in-init as a trappable :killed, so
        # the partial subtree is killed directly: every process reachable over links, deepest first
        Process.demonitor(ref, [:flush])
        killed = kill_and_reap!(owned_subtree(owner))
        flunk("start_link did not return within #{timeout} ms: #{length(killed)} owned processes reaped")
    end
  end

  # MUST-12: the ONLY way a Run config crosses into the owner. The config and every receiver-bound
  # value inside it (trace, held runner, barrier) were built in the TEST process; the owner runs the
  # bare start_link. A config bound to another process is refused here, not discovered by a timeout.
  defp start_run!(config, timeout \\ 5_000) do
    assert config.trace == self(), "the config's trace must be the test process, not the owner (MUST-12)"
    start_owned!(fn -> run_sup().start_link(config) end, timeout)
  end

  defp owner_loop({:ok, sup} = result) do
    receive do
      {:stop, from} ->
        if Process.alive?(sup), do: Supervisor.stop(sup, :shutdown, 10_000)
        send(from, {:stopped, self()})

      {:EXIT, _pid, _reason} ->
        owner_loop(result)
    end
  end

  defp owner_loop(_other), do: :ok

  # one teardown per owned subtree: it runs in the on_exit process, so it takes its OWN monitors
  defp register_teardown(owner) do
    Seam.put({:tracked, owner}, [])

    on_exit(fn ->
      outcome = stop_owner_bounded(owner, 15_000)
      wait_tracked_gone(owner)
      if outcome != :ok, do: raise("teardown escalated - the owned subtree did not stop gracefully: #{inspect(outcome)}")
    end)
  end

  # bounded graceful stop, then the owned subtree is checked/reaped: :ok only when every owned process
  # is gone; {:escalated, killed} when the fallback had to kill (each kill reaped to DOWN); raises when
  # an owned process survives the escalation. Idempotent: fresh monitors in the calling process.
  defp stop_owner_bounded(owner, grace_ms) do
    subtree = owned_subtree(owner)
    ref = Process.monitor(owner)
    if Process.alive?(owner), do: send(owner, {:stop, self()})

    graceful =
      receive do
        {:stopped, ^owner} -> :ok
        {:DOWN, ^ref, :process, ^owner, _} -> :ok
      after
        grace_ms -> :timeout
      end

    Process.demonitor(ref, [:flush])

    case graceful do
      :ok ->
        # the owner exits right after acknowledging; every owned process must be gone with it
        _ = wait_until(fn -> Enum.all?(subtree, &(not Process.alive?(&1))) end, 5_000)

        case Enum.filter(subtree, &Process.alive?/1) do
          [] -> :ok
          alive -> {:escalated, kill_and_reap!(alive)}
        end

      :timeout ->
        {:escalated, kill_and_reap!(subtree)}
    end
  end

  # every process reachable from the owner over links (bounded depth), deepest first, never the caller
  defp owned_subtree(owner), do: owner |> collect_links([owner], 4) |> Enum.uniq() |> Enum.reverse()

  defp collect_links(_pid, seen, 0), do: seen

  defp collect_links(pid, seen, depth) do
    links =
      case Process.info(pid, :links) do
        {:links, links} -> for l <- links, is_pid(l), l != self(), l not in seen, do: l
        nil -> []
      end

    Enum.reduce(links, seen ++ links, fn l, acc -> collect_links(l, acc, depth - 1) end)
  end

  # kill each pid and reap each to DOWN with a fresh monitor; survivors are a reported failure
  defp kill_and_reap!(pids) do
    refs = for pid <- pids, do: {pid, Process.monitor(pid)}
    for pid <- pids, do: Process.exit(pid, :kill)

    survivors =
      for {pid, ref} <- refs,
          (receive do
             {:DOWN, ^ref, :process, ^pid, _} -> false
           after
             5_000 -> true
           end),
          do: pid

    if survivors != [], do: raise("owned processes survived the escalation: #{inspect(survivors)}")
    pids
  end

  defp wait_tracked_gone(owner) do
    for identity <- Agent.get(Seam, &Map.get(&1, {:tracked, owner}, [])) do
      wait_until(fn -> dead?(identity) end, 15_000) || raise("owned group #{identity.pgid} still present at teardown")
    end
  end

  defp track(owner, identity),
    do: Agent.update(Seam, &Map.update(&1, {:tracked, owner}, [identity], fn ids -> [identity | ids] end))

  defp stop_owned!(owner) do
    send(owner, {:stop, self()})
    assert_receive {:stopped, ^owner}, 15_000
  end

  # the ruled start trace (Q-6): the SUPERVISOR process records each successful real child start,
  # in order, as {:run_child_started, supervisor_pid, id, pid}; the Server reports readiness as
  # {:run_server_driving, server_pid, facts} once its post-init discovery succeeded
  defp started_children(sup) do
    for _ <- 1..3 do
      receive do
        {:run_child_started, ^sup, id, pid} -> {id, pid}
      after
        5_000 -> flunk("a child start was never recorded by the supervisor")
      end
    end
  end

  # MUST-13: ONE receive clause set over every readiness-relevant message from the Server, so the
  # returned list is the Server's own send order; separate selective receives could skip an earlier one
  defp readiness_messages(server, count, timeout) do
    for _ <- 1..count do
      receive do
        {:run_server_driving, ^server, _} = m -> m
        {:host_entry, ^server, _} = m -> m
        {:first_effect, ^server, _} = m -> m
      after
        timeout -> flunk("a readiness message from the Server never arrived")
      end
    end
  end

  # the barrier runs in the EXECUTING process. At READY it reports identity, the LIVE Port connected
  # to the guardian OS pid and its own pid under a fresh ref, then WAITS (bounded) for that exact
  # ref's acknowledgment: the recipient tracks the identity BEFORE acking; without an ack the
  # executing process fails closed (exit), which closes its Port -> guardian EOF -> group cleanup.
  # At GO it measures the group's liveness inside the held boundary; TERMINATED reports the boundary.
  defp barrier(parent, ack_ms \\ 30_000) do
    fn
      :after_ready, identity ->
        ref = make_ref()
        send(parent, {:ready, identity, self(), live_port_owner(identity.guardian), ref})

        receive do
          {:ready_ack, ^ref} -> :ok
        after
          ack_ms -> exit({:ready_not_acknowledged, ref})
        end

      :after_go, started_data ->
        send(parent, {:after_go, self(), signal_zero(Integer.to_string(started_data["execution"]["pid"]))})
        :ok

      :terminated, termination ->
        send(parent, {:terminated, termination})
        :ok

      _, _ ->
        :ok
    end
  end

  # the ratified effect owner: the process that executes effects (and owns the Port) is the run's worker, born
  # under Work by the Server and traced as {:run_child_started, work, :worker, pid}
  defp executing! do
    assert_receive {:run_child_started, _work, :worker, pid}, 10_000
    pid
  end

  defp live_port_owner(os_pid) do
    case Enum.find(Port.list(), &(Port.info(&1, :os_pid) == {:os_pid, os_pid})) do
      nil -> :no_live_port
      port -> Port.info(port, :connected)
    end
  end

  # READY acknowledgment for the given attempt: register the identity under the owner's teardown FIRST,
  # then release the executing process by acknowledging its exact ref
  defp ready!(owner, executing_pid) do
    assert_receive {:ready, identity, ^executing_pid, port_owner, ref}, 30_000
    track(owner, identity)
    send(executing_pid, {:ready_ack, ref})
    {identity, port_owner}
  end

  defp all_down!(pids, timeout \\ 10_000) do
    refs = for pid <- pids, do: Process.monitor(pid)

    for ref <- refs,
        do:
          (receive do
             {:DOWN, ^ref, :process, _, _} -> :ok
           after
             timeout -> flunk("a process survived: #{inspect(ref)}")
           end)

    :ok
  end

  # a pending gen call is a monitor from the caller on the server: the witness that a subscriber is waiting
  defp wait_until_monitoring(server, caller, ms) do
    wait_until(
      fn ->
        {:monitored_by, by} = Process.info(server, :monitored_by)
        caller in by
      end,
      ms
    )
  end

  defp wait_until(fun, ms) do
    deadline = System.monotonic_time(:millisecond) + ms
    _ = fn -> fun.() or System.monotonic_time(:millisecond) > deadline end |> Stream.repeatedly() |> Enum.find(& &1)
    fun.()
  end

  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do: signal_zero(Integer.to_string(pid)) == :gone and signal_zero("-" <> Integer.to_string(pgid)) == :gone

  # ---- doubles ----
  defmodule QueuedAdapter do
    @moduledoc false
    alias AiOrchestrator.Dispatch.LocalPane

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane

    def deliver(command, opts) do
      {:ok, result} = LocalPane.deliver(command, opts)
      {:ok, Map.put(result, "send_status", "queued")}
    end

    # a gate runner that parks the executing process (the Server) until released by message
    def reconcile(_command, _opts) do
      outcome = if Seam.get(:polled), do: "delivered", else: "queued"
      Seam.put(:polled, true)
      {:ok, %{"outcome" => outcome, "delivery_attempt" => 1}}
    end
  end

  defmodule HeldGate do
    @moduledoc false
    def runner(parent) do
      fn _gate ->
        send(parent, {:gate_entered, self()})

        receive do
          :release_gate ->
            {:ok,
             %{
               "exit_status" => 0,
               "duration_ms" => 1,
               "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
               "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
             }}
        end
      end
    end
  end

  defmodule UnsettledExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate release(handle, ack, opts), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate reconcile(fs, dir, expected, opts), to: GateDouble

    def await(_handle, _opts) do
      {:exit,
       %{
         "kind" => "exited",
         "exit_status" => 0,
         "settled" => false,
         "leftovers" => "unknown",
         "proof" => "unknown",
         "escaped" => "unknown",
         "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
         "stderr_hash" => "sha256:" <> String.duplicate("ab", 32),
         "stderr_merged" => false,
         "duration_ms" => 1
       }}
    end

    def abandon(handle) do
      if pid = Seam.get(:abandon_subscriber), do: send(pid, {:abandoned, handle})
      :ok
    end
  end

  defmodule RaisingAwaitExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate release(handle, ack, opts), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate reconcile(fs, dir, expected, opts), to: GateDouble
    def await(_handle, _opts), do: raise("red executor escape")

    def abandon(handle) do
      if pid = Seam.get(:abandon_subscriber), do: send(pid, {:abandoned, handle})
      :ok
    end
  end

  # an executor whose await THROWS an action-bearing gen_statem callback tuple carrying the seam's canary
  defmodule ThrowingAwaitExecutor do
    @moduledoc false
    defdelegate prepare(fs, request, opts), to: GateDouble
    defdelegate started_data(handle), to: GateDouble
    defdelegate ack(handle, event), to: GateDouble
    defdelegate release(handle, ack, opts), to: GateDouble
    defdelegate pass?(outcome), to: GateDouble
    defdelegate evidence(dir, id, attempt), to: GateDouble
    defdelegate reconcile(fs, dir, expected, opts), to: GateDouble

    def await(_handle, _opts),
      do: throw({:keep_state_and_data, [{:reply, {self(), make_ref()}, {:ok, %{Seam.get(:canary) => true}}}]})

    def abandon(handle) do
      if pid = Seam.get(:abandon_subscriber), do: send(pid, {:abandoned, handle})
      :ok
    end
  end

  # =================================================================================================
  describe "controls: harness lifetime and cross-process seams (baseline" do
    test "control: OTP shape - start order by trace, which_children reversed, kill at max_restarts 0 downs all" do
      observer = self()

      children =
        for id <- [:writer, :server, :work] do
          %{
            id: id,
            start:
              {Agent, :start_link,
               [
                 fn ->
                   send(observer, {:started, id, self()})
                   %{}
                 end
               ]}
          }
        end

      {owner, supervisor} =
        start_owned!(fn -> Supervisor.start_link(children, strategy: :rest_for_one, max_restarts: 0) end)

      started =
        for _ <- 1..3,
            do:
              (receive do
                 {:started, id, pid} -> {id, pid}
               after
                 1_000 -> flunk("missing child startup")
               end)

      assert Keyword.keys(started) == [:writer, :server, :work]
      assert supervisor |> Supervisor.which_children() |> Enum.map(&elem(&1, 0)) == [:work, :server, :writer]
      sup_ref = Process.monitor(supervisor)
      Process.exit(Keyword.fetch!(started, :server), :kill)
      all_down!(Keyword.values(started), 1_000)
      assert_receive {:DOWN, ^sup_ref, :process, _, :shutdown}, 1_000
      assert Process.alive?(owner), "the trapping owner survives its subtree's exhaustion"
      stop_owned!(owner)
    end

    test "control: the owner keeps a started subtree alive until explicit stop (the starter's own exit never kills" do
      {owner, sup} =
        start_owned!(fn ->
          Supervisor.start_link([{Agent, fn -> :child end}], strategy: :rest_for_one, max_restarts: 0)
        end)

      ref = Process.monitor(sup)
      refute_receive {:DOWN, ^ref, :process, ^sup, _}, 300
      assert Process.alive?(sup) and Process.alive?(owner)
      {:links, links} = Process.info(sup, :links)
      assert owner in links, "the owner is the supervisor's linked parent"
      stop_owned!(owner)
      assert_receive {:DOWN, ^ref, :process, ^sup, :shutdown}, 5_000
      assert fn -> stop_owner_bounded(owner, 1_000) end |> Task.async() |> Task.await(5_000) == :ok
    end

    test "control: a blocked startup is reaped - the owner and the partially started child are killed, then the tes" do
      parent = self()

      blocked_child = %{
        id: :blocked,
        start:
          {Task, :start_link,
           [
             fn ->
               send(parent, {:blocked_child, self()})
               Process.sleep(:infinity)
             end
           ]}
      }

      # a child whose start never returns blocks Supervisor.start_link itself
      never = %{id: :never, start: {__MODULE__, :never_returning_start, []}}

      assert_raise ExUnit.AssertionError, ~r/did not return within/, fn ->
        start_owned!(fn -> Supervisor.start_link([blocked_child, never], strategy: :one_for_one) end, 300)
      end

      assert_received {:blocked_child, child}

      assert wait_until(fn -> not Process.alive?(child) end, 2_000),
             "the partially started child died with the killed owner"
    end

    test "control: receiver binding - trace, held runner and barrier built in the test deliver to the test" do
      run_dir = tmp_run_dir()
      config = config(run_dir, :run, Keyword.put(fresh_opts(gated_index()), :gate_opts, runner: HeldGate.runner(self())))
      runner = config.opts[:gate_opts][:runner]
      barrier = barrier(self(), 5_000)
      trace = config.trace

      probe = %{
        id: :probe,
        start:
          {Task, :start_link,
           [
             fn ->
               send(trace, {:run_child_started, :probe_sup, :probe, self()})
               :ok = barrier.(:after_ready, %{guardian: -1})
               {:ok, _} = runner.(:gate)
               send(trace, {:probe_done, self()})
               Process.sleep(:infinity)
             end
           ]}
      }

      {owner, _sup} = start_owned!(fn -> Supervisor.start_link([probe], strategy: :one_for_one) end)
      # every message reaches the TEST (not the owner), although the child runs under the owner's tree
      assert_receive {:run_child_started, :probe_sup, :probe, child}, 5_000
      assert_receive {:ready, %{guardian: -1}, ^child, :no_live_port, ack}, 5_000
      refute_received {:gate_entered, ^child}, "held at READY until acknowledged"
      send(child, {:ready_ack, ack})
      assert_receive {:gate_entered, ^child}, 5_000
      send(child, :release_gate)
      assert_receive {:probe_done, ^child}, 5_000
      stop_owned!(owner)

      # a config whose receiver-bound values were built in ANOTHER process is refused before any start
      parent = self()
      spawn_link(fn -> send(parent, {:foreign, config(run_dir, :run, [])}) end)
      assert_receive {:foreign, foreign}, 1_000
      assert foreign.trace != parent
      assert_raise ExUnit.AssertionError, ~r/MUST-12/, fn -> start_run!(foreign) end
    end

    test "control: the ordered readiness receive accepts the Server's order and rejects a reversed order" do
      parent = self()
      facts = %{writer: :w, work: :k, ownership: {:ok, %{writer: :w, state: :live}}}

      ordered =
        spawn_link(fn ->
          send(parent, {:run_server_driving, self(), facts})
          send(parent, {:host_entry, self(), :first_clock_read})
          send(parent, {:first_effect, self(), Effect.Timer})
        end)

      assert [{:run_server_driving, ^ordered, ^facts}, {:host_entry, ^ordered, _}, {:first_effect, ^ordered, _}] =
               readiness_messages(ordered, 3, 1_000)

      reversed =
        spawn_link(fn ->
          send(parent, {:first_effect, self(), Effect.Timer})
          send(parent, {:host_entry, self(), :first_clock_read})
          send(parent, {:run_server_driving, self(), facts})
        end)

      messages = readiness_messages(reversed, 3, 1_000)

      assert_raise ExUnit.AssertionError, fn ->
        assert [{:run_server_driving, ^reversed, _}, {:host_entry, ^reversed, _}, {:first_effect, ^reversed, _}] =
                 messages
      end

      assert match?([{:first_effect, _, _}, {:host_entry, _, _}, {:run_server_driving, _, _}], messages)
    end

    test "control: teardown fallback - a stuck owner with a trapping linked dependent is killed, reaped and reported" do
      parent = self()

      stuck =
        spawn(fn ->
          Process.flag(:trap_exit, true)

          dependent =
            spawn_link(fn ->
              Process.flag(:trap_exit, true)

              receive do
                :never -> :ok
              end
            end)

          send(parent, {:dependent, dependent})

          receive do
            :never -> :ok
          end
        end)

      assert_receive {:dependent, dependent}, 1_000
      # the cleanup runs in ANOTHER process, exactly as on_exit does; it takes its own monitors
      {elapsed_ms, outcome} =
        fn -> :timer.tc(fn -> stop_owner_bounded(stuck, 300) end, :millisecond) end
        |> Task.async()
        |> Task.await(10_000)

      assert {:escalated, killed} = outcome
      assert Enum.sort(killed) == Enum.sort([stuck, dependent]), "deepest first, both reaped"
      assert elapsed_ms < 5_000, "bounded: the 300 ms grace, then kill + reap"
      refute Process.alive?(stuck)
      refute Process.alive?(dependent), "a trapping dependent needs its own kill, not a propagated :killed"
      # idempotent: a second pass over an already-dead owner is :ok
      assert fn -> stop_owner_bounded(stuck, 300) end |> Task.async() |> Task.await(5_000) == :ok
    end

    test "control: the seam carries the clock instant, the abandon notification and a flag across processes" do
      Seam.put(:now, 2_000_000_060)
      parent = self()

      spawn_link(fn ->
        send(parent, {:child_saw, SeamClock.unix_now(), UnsettledExecutor.abandon(%{released: :probe})})
      end)

      assert_receive {:child_saw, 2_000_000_060, :ok}, 1_000
      assert_receive {:abandoned, %{released: :probe}}, 1_000
      Seam.put(:refuse_next_write, true)
      spawn_link(fn -> send(parent, {:flag_seen, Seam.get(:refuse_next_write)}) end)
      assert_receive {:flag_seen, true}, 1_000
    end

    test "control: a Writer-side write refusal driven by a seam flag set from another process refuses exactly one l" do
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      # the matcher runs inside the FaultFs Agent; it reads the SEAM agent (a different process), never its own
      FaultFs.inject(fs, :write, fn _args -> Seam.take(:refuse_next_write) == true end, {:error, :eio})

      {:ok, w, _} = Writer.open(run_dir, create: true, fs: fs, lock: [supervisor_instance: "sup_ctl"])
      [e1, e2, e3] = "events_pre_dispatch.jsonl" |> kill9() |> Enum.take(3) |> Enum.map(&Jason.decode!/1)
      assert {:ok, _} = Writer.append(w, Map.put(e1, "ts", FixedClock.wall_ts()))
      parent = self()

      spawn_link(fn ->
        Seam.put(:refuse_next_write, true)
        send(parent, :flag_set)
      end)

      assert_receive :flag_set, 1_000

      assert {:error, %{clause: "append_failed", stage: "write"}} =
               Writer.append(w, Map.put(e2, "ts", FixedClock.wall_ts()))

      assert Seam.get(:refuse_next_write) == nil, "consumed by the one refused write"

      assert match?({:error, %{clause: "writer_failed"}}, Writer.append(w, Map.put(e3, "ts", FixedClock.wall_ts()))),
             "a failed writer stays failed (existing policy)"

      Writer.close(w)
    end

    test "control: every scenario prior seeds as a LEGACY on-disk journal Writer.open accepts; Writer.append refuse" do
      for {name, _kind, _scenario, prior, _} <- H.cases(), prior != [] do
        run_dir = seed_legacy!(tmp_run_dir(), prior)

        assert {:ok, writer, %{last_seq: seq, lines: lines}} =
                 Writer.open(run_dir, lock: [supervisor_instance: "sup_ctl"])

        assert seq == length(prior) and lines == prior, name
        :ok = Writer.close(writer)
      end

      run_dir = tmp_run_dir()
      {:ok, w, _} = Writer.open(run_dir, create: true, lock: [supervisor_instance: "sup_ctl"])
      lines = "events_awaiting_artifact.jsonl" |> kill9() |> Enum.map(&Jason.decode!/1)
      {prefix, [projection | _]} = Enum.split_while(lines, &(&1["type"] != "assignment_prompt_projected"))
      for event <- prefix, do: assert(match?({:ok, _}, Writer.append(w, event)))
      assert projection["event_version"] == 1
      assert {:error, %{clause: "unsupported_event_version"}} = Writer.append(w, projection)
      :ok = Writer.close(w)
    end

    test "control: the flaky-gate options are stateful per opts_fun call" do
      opts = fresh_opts(flaky_index())
      {_, _, scenario, _, _} = Enum.at(H.cases(), flaky_index())
      {:ok, first} = Host.run(H.spec(scenario), H.plan(scenario), opts)
      H.reset_seams()
      {:ok, second} = Host.run(H.spec(scenario), H.plan(scenario), opts)
      refute first == second
      {:ok, third} = Host.run(H.spec(scenario), H.plan(scenario), fresh_opts(flaky_index()))
      assert strip(third.events) == strip(first.events)
    end

    test "control: an 18-byte torn tail on the kill9 pre_dispatch legacy journal is repaired by Writer.open" do
      run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
      torn = ~s({"schema":"ai-orch)
      assert byte_size(torn) == 18
      File.write!(Path.join(run_dir, "events.jsonl"), File.read!(Path.join(run_dir, "events.jsonl")) <> torn)

      assert {:ok, w, %{repair: %{action: :truncate_tail, truncate_bytes: 18}}} =
               Writer.open(run_dir, lock: [supervisor_instance: "sup_ctl"])

      :ok = Writer.close(w)
    end

    test "control: rejected-open corpus at the Writer - locked, receipt on legacy, receipt hash mismatch, receipt m" do
      locked = guarded_dir!()

      with_holder(locked, [supervisor_instance: "sup_holder"], fn _holder, _opened ->
        assert {:error, %{clause: _}} = Writer.open(locked, lock: [supervisor_instance: "sup_other"])
      end)

      legacy = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))

      File.write!(
        Path.join(legacy, "events.head"),
        Chain.encode_receipt(%{seq: 1, line_sha256: Chain.line_sha256("x\n"), updated_at: FixedClock.wall_ts()})
      )

      assert {:error, %{clause: "receipt_on_legacy_journal"}} =
               Writer.open(legacy, lock: [supervisor_instance: "sup_ctl"])

      {:ok, v2_lines} = v2_journal_lines()
      mismatch = seed_v2!(tmp_run_dir(), v2_lines)

      File.write!(
        Path.join(mismatch, "events.head"),
        Chain.encode_receipt(%{
          seq: length(v2_lines),
          line_sha256: Chain.line_sha256("tampered\n"),
          updated_at: FixedClock.wall_ts()
        })
      )

      assert {:error, %{clause: "receipt_hash_mismatch"}} = Writer.open(mismatch, lock: [supervisor_instance: "sup_ctl"])
      missing = seed_v2!(tmp_run_dir(), v2_lines)
      File.rm!(Path.join(missing, "events.head"))
      assert {:error, %{clause: "receipt_missing"}} = Writer.open(missing, lock: [supervisor_instance: "sup_ctl"])
    end

    test "control: FaultFs {:after, fun} runs in the writer process after a SUCCESSFUL receipt dir_sync and before" do
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      parent = self()

      FaultFs.inject(
        fs,
        :dir_sync,
        fn _args -> true end,
        {:after,
         fn trace ->
           if match?([{:dir_sync, _}, {:rename, "events.head.tmp", "events.head"} | _], trace),
             do:
               send(
                 parent,
                 {:after_receipt, self(), File.read!(Path.join(run_dir, "events.head")), System.monotonic_time()}
               )
         end}
      )

      {:ok, w, _} = Writer.open(run_dir, create: true, fs: fs, lock: [supervisor_instance: "sup_ctl"])
      event = "events_pre_dispatch.jsonl" |> kill9() |> hd() |> Jason.decode!()
      assert {:ok, _} = Writer.append(w, Map.put(event, "ts", FixedClock.wall_ts()))
      replied_at = System.monotonic_time()
      assert_received {:after_receipt, ^w, receipt_bytes, hook_at}
      assert match?({:ok, %{seq: 1}}, Chain.decode_receipt(receipt_bytes)), "the receipt was durable when the hook ran"
      assert hook_at < replied_at, "the hook ran before the append reply"
      :ok = Writer.close(w)
    end

    test "control: a PRE-SYNC publication failure (receipt rename) never reaches the after-hook and fails the appen" do
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      parent = self()

      FaultFs.inject(
        fs,
        :dir_sync,
        fn _ -> true end,
        {:after,
         fn trace ->
           if match?([{:dir_sync, _}, {:rename, "events.head.tmp", "events.head"} | _], trace),
             do: send(parent, :must_not_fire)
         end}
      )

      FaultFs.inject(fs, :rename, fn [_from, to] -> to == "events.head" end, {:error, :eio})
      {:ok, w, _} = Writer.open(run_dir, create: true, fs: fs, lock: [supervisor_instance: "sup_ctl"])
      event = "events_pre_dispatch.jsonl" |> kill9() |> hd() |> Jason.decode!()

      assert {:error, %{clause: "append_failed", stage: "receipt"}} =
               Writer.append(w, Map.put(event, "ts", FixedClock.wall_ts()))

      refute_received :must_not_fire
      Writer.close(w)
    end

    test "control: a FAILED underlying dir_sync with {:after, fun} selected returns the error unchanged and never c" do
      fs = FaultFs.new()
      parent = self()
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:after, fn _ -> send(parent, :after_failed_sync) end})
      missing = Path.join(System.tmp_dir!(), "run-server-red-missing-#{System.unique_integer([:positive])}")
      assert {:error, _} = Fs.dir_sync(fs, missing)
      refute_received :after_failed_sync
    end
  end

  def never_returning_start, do: Process.sleep(:infinity)

  # a real v2 journal (the gated scenario run through a real Writer), as lines
  defp v2_journal_lines do
    run_dir = tmp_run_dir()

    {:ok, w, _} =
      Writer.open(run_dir,
        create: true,
        lock: [supervisor_instance: "sup_v2", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
      )

    opts = gated_index() |> fresh_opts() |> Keyword.put(:event_sink, &Writer.append(w, &1))
    {:ok, _} = Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
    :ok = Writer.close(w)
    {:ok, run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)}
  end

  # =================================================================================================
  describe "R-A topology, strategy, readiness (D-2, D-5, Q-6)" do
    test "Run.Supervisor.init pins rest_for_one, intensity 0 and the child types" do
      require_run!()
      config = config(tmp_run_dir(), :run, fresh_opts(gated_index()))
      # the init argument is the OPAQUE config thunk start_link hands to OTP (F-1 root hardening); the topology pin
      # is unchanged
      thunk = fn -> config end
      assert {:ok, {%{strategy: :rest_for_one, intensity: 0}, [writer, server, work]}} = run_sup().init(thunk)
      assert writer.id == {Writer, Path.expand(config.run_dir)} and writer.type == :worker
      assert server.type == :worker and work.type == :supervisor
    end

    test "start order is recorded by the supervisor; the Server reports :driving with the exact siblings and live o" do
      require_run!()
      run_dir = tmp_run_dir()
      parent = self()

      first_effect = fn effect, _obs ->
        if Seam.get(:first_effect_seen) == nil do
          Seam.put(:first_effect_seen, true)
          send(parent, {:first_effect, self(), effect.__struct__})
        end
      end

      Seam.put(:entry_recipient, parent)

      config =
        config(
          run_dir,
          :run,
          gated_index() |> fresh_opts() |> Keyword.merge(clock: EntryClock, effect_observer: first_effect)
        )

      {owner, sup} = start_run!(config)
      started = started_children(sup)
      assert Keyword.keys(started) == [:writer, :server, :work], "order recorded by the one starting process"
      writer = Keyword.fetch!(started, :writer)
      server = Keyword.fetch!(started, :server)
      work = Keyword.fetch!(started, :work)
      # ONE ordered receive over the Server's readiness-relevant messages (single sender, so mailbox order
      # is the Server's order): :driving FIRST, then the Host entry (the Server's first configured-clock
      # read, i.e. stamping the first committed event before any effect), then the POST-effect observer
      assert [
               {:run_server_driving, ^server,
                %{writer: ^writer, work: ^work, ownership: {:ok, %{writer: ^writer, state: :live}}}},
               {:host_entry, ^server, :first_clock_read},
               {:first_effect, ^server, _struct}
             ] = readiness_messages(server, 3, 5_000)

      # the ratified worker: Work hosts exactly the one temporary owner the Server births for the run
      assert [{_, worker, :worker, [AiOrchestrator.Run.Worker]}] = DynamicSupervisor.which_children(work)
      assert is_pid(worker)
      assert {:ok, %{summary: %{"status" => "completed"}}} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    test "rejected open (directory owned): the exact Writer rejection and zero effects of any kind" do
      require_run!()
      run_dir = tmp_run_dir()
      # the holder is linked to this test process (Writer.open links): closed explicitly below
      {:ok, holder, _} = Writer.open(run_dir, create: true, lock: [supervisor_instance: "sup_holder"])
      {:error, expected} = Writer.open(run_dir, lock: [supervisor_instance: "sup_other"])
      parent = self()

      opts =
        gated_index()
        |> fresh_opts()
        |> Keyword.put(:effect_observer, fn effect, _ -> send(parent, {:effect_ran, effect.__struct__}) end)

      assert start_run!(config(run_dir, :run, opts)) == {:error, expected}
      refute_received {:effect_ran, _}
      :ok = Writer.close(holder)
    end
  end

  describe "R-N smallest shared stepping API (D-3, D-7, Q-1)" do
    test "Host.open/3 + Host.advance/1 exist; run, resume AND cancel each equal open + advance-until-halt with fres" do
      assert function_exported?(Host, :open, 3), "Host.open/3"
      assert function_exported?(Host, :advance, 1), "Host.advance/1"

      assert function_exported?(Host, :run, 3) and function_exported?(Host, :resume, 4) and
               function_exported?(Host, :cancel, 2)

      for {{_name, kind, scenario, prior, _}, index} <- Enum.with_index(H.cases()) do
        direct =
          case kind do
            :run -> Host.run(H.spec(scenario), H.plan(scenario), fresh_opts(index))
            :resume -> Host.resume(H.spec(scenario), H.plan(scenario), prior, fresh_opts(index))
            :cancel -> Host.cancel(prior, fresh_opts(index))
          end

        {:ok, loop} =
          host().open(kind, %{spec: H.spec(scenario), plan: H.plan(scenario), prior_lines: prior}, fresh_opts(index))

        stepped =
          fn -> :tick end
          |> Stream.repeatedly()
          |> Enum.reduce_while(loop, fn _, l ->
            case host().advance(l) do
              {:continue, next} -> {:cont, next}
              {:halt, result} -> {:halt, result}
            end
          end)

        assert stepped == direct, "#{kind} #{scenario}"
      end
    end

    test "Run.Server copies no commit/receipt/cleanup/step logic; Run reaches Effects only through Run.Worker; Lifecycle" do
      require_run!()
      server_src = Path.expand("../../lib/ai_orchestrator/run/server.ex", __DIR__)
      run_src = Path.expand("../../lib/ai_orchestrator/run.ex", __DIR__)
      lifecycle_src = Path.expand("../../lib/ai_orchestrator/lifecycle.ex", __DIR__)
      assert File.exists?(server_src) and File.exists?(run_src)
      server = File.read!(server_src)

      for forbidden <- ["defp commit(", "Enum.find(committed", "Effects.settle(", "Reducer.step(", "stamp_and_sink"],
          do: refute(server =~ forbidden, forbidden)

      assert File.read!(@host_src) =~ "def advance("
      run = File.read!(run_src)
      assert run =~ ~r/use Boundary/
      # the ratified effect owner: Run depends on Effects ONLY through Run.Worker (the runtime owner); the Server,
      # the Executor and the Supervisor still reference no Effects module
      assert run =~ ~r/AiOrchestrator\.Effects\b/
      refute server =~ ~r/AiOrchestrator\.Effects\b/

      for src <- ["executor.ex", "executor/owner.ex", "supervisor.ex"],
          do:
            refute(
              File.read!(Path.expand("../../lib/ai_orchestrator/run/" <> src, __DIR__)) =~ ~r/AiOrchestrator\.Effects\b/
            )

      lifecycle = File.read!(lifecycle_src)
      assert lifecycle =~ ~r/exports:\s*\[[^\]]*Host[^\]]*\]/
      refute lifecycle =~ ~r/exports:\s*\[[^\]]*Core[^\]]*\]/
    end
  end

  # =================================================================================================
  describe "R-B / R-I / R-J parity and rework through the subtree (real" do
    for {{name, _kind, _scenario, _prior, _}, index} <- Enum.with_index(H.cases()) do
      test "case #{index + 1} #{name}: Server result == Host result; v2 receipts on appended events" do
        require_run!()
        {_name, kind, scenario, prior, _} = Enum.at(H.cases(), unquote(index))
        run_dir = tmp_run_dir()
        if prior != [], do: seed_legacy!(run_dir, prior)

        # the executor bindings (m_1788645900000 ruling): a FIXED explicit supervisor instance given to both
        # sides; run_dir from this test's setup; lock path and repair from the sibling Writer's own opening,
        # read from the Writer process (not learned from the journal under test). The oracle never sees the
        # Server's output before it runs.
        {owner, sup} = start_run!(parity_config(run_dir, kind, scenario, unquote(index)))
        started = started_children(sup)
        opened = Writer.opened(Keyword.fetch!(started, :writer))
        actual = run_server().await(Keyword.fetch!(started, :server), 30_000)
        stop_owned!(owner)
        expected = parity_oracle(kind, scenario, prior, unquote(index), run_dir, opened)
        assert {:ok, %{summary: summary_e, events: events_e}} = expected
        # D1 transition pin (m_1788751607000 / m_1788752018000) for the two EXPIRED kill9 resume cases: the independent
        # oracle (direct Effects, unchanged) still completes; the Worker path answers the exact expiry for the due
        # Observe; acceptance-first and Writer v2 receipts are asserted on the ACTUAL journal suffix
        if unquote(name) in ["kill9 resume pre_dispatch", "kill9 resume awaiting_artifact"] do
          assert summary_e["status"] == "completed", "the independent oracle still completes: " <> unquote(name)
          # U2b GREEN transition (recorded): pre_dispatch expires at the DISPATCH (blocked, attention-only);
          # awaiting_artifact still expires at the Observe (the exact observation_timeout error)
          if unquote(name) == "kill9 resume pre_dispatch" do
            assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} = actual
            last = run_dir |> journal() |> List.last()
            assert last["type"] == "human_attention_required" and last["data"]["reason"] == "dispatch_deadline_exceeded"
            refute Enum.any?(journal(run_dir), &(&1["type"] == "agent_wedge_detected"))
          else
            assert actual == @expired_resume, unquote(name)
          end

          assert String.starts_with?(File.read!(Path.join(run_dir, "events.jsonl")), Enum.join(prior, "\n") <> "\n"),
                 "prefix bytes preserved"

          appended = run_dir |> journal() |> Enum.drop(length(prior))
          assert hd(appended)["type"] == "run_resumed", unquote(name)
          for e <- appended, do: assert(e["schema_version"] == 2 and e["prev_line_sha256"] =~ ~r/\Asha256:[0-9a-f]{64}\z/)
          assert_head_receipt!(run_dir)
        else
          assert {:ok, %{summary: summary_a, events: events_a, appended_events: appended}} = actual
          assert summary_a == summary_e, unquote(name)
          assert strip(events_a) == strip(events_e), unquote(name)
          for e <- appended, do: assert(e["schema_version"] == 2 and e["prev_line_sha256"] =~ ~r/\Asha256:[0-9a-f]{64}\z/)
        end
      end
    end

    test "parity control: a wrong executor binding on the oracle side FAILS the comparison (mutation)" do
      require_run!()
      index = gated_index()
      {_, :run, scenario, [], _} = Enum.at(H.cases(), index)
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(parity_config(run_dir, :run, scenario, index))
      started = started_children(sup)
      opened = Writer.opened(Keyword.fetch!(started, :writer))
      {:ok, %{events: events_a}} = run_server().await(Keyword.fetch!(started, :server), 30_000)
      stop_owned!(owner)
      {:ok, %{events: events_e}} = parity_oracle(:run, scenario, [], index, run_dir, opened)
      assert strip(events_a) == strip(events_e), "positive: the independent oracle agrees"

      for mutation <- [
            [run_lock_path: "run.lock.9"],
            [supervisor_instance: "sup_wrong_0001"],
            [run_dir: Path.join(run_dir, "elsewhere")]
          ] do
        H.reset_seams()
        wrong = Keyword.merge(parity_opts(index, run_dir, opened), mutation)
        {:ok, %{events: mutated}} = Host.run(H.spec(scenario), H.plan(scenario), wrong)
        refute strip(mutated) == strip(events_a), inspect(mutation)
      end
    end

    test "parity control: a GENERATED instance (no explicit one) is the one in the lock file and the journal" do
      require_run!()
      index = gated_index()
      run_dir = tmp_run_dir()
      config = config(run_dir, :run, fresh_opts(index))
      refute Keyword.has_key?(config.opts, :supervisor_instance)
      {owner, sup} = start_run!(config)
      started = started_children(sup)
      opened = Writer.opened(Keyword.fetch!(started, :writer))
      {:ok, %{events: events}} = run_server().await(Keyword.fetch!(started, :server), 30_000)
      lock = run_dir |> Path.join(opened.lock_path) |> File.read!() |> Jason.decode!()
      [started_evt] = Enum.filter(events, &(&1["type"] == "run_started"))
      assert lock["supervisor_instance"] == started_evt["data"]["supervisor_instance"]
      assert started_evt["data"]["supervisor_instance"] == "sup_0001", "the id seam's first instance, resolved once"
      assert started_evt["data"]["run_lock_path"] == opened.lock_path
      stop_owned!(owner)
    end

    test "R-J reducer-owned rework: work_item_retry_scheduled journaled, child pids unchanged, no further start rec" do
      require_run!()
      run_dir = tmp_run_dir()
      {_, _, scenario, _, _} = Enum.at(H.cases(), flaky_index())

      {owner, sup} =
        start_run!(%{
          run_dir: run_dir,
          mode: :run,
          spec: H.spec(scenario),
          plan: H.plan(scenario),
          opts: Keyword.delete(fresh_opts(flaky_index()), :event_sink),
          trace: self()
        })

      started = started_children(sup)
      server = Keyword.fetch!(started, :server)
      assert {:ok, %{events: events, summary: %{"status" => "completed"}}} = run_server().await(server, 30_000)
      assert Enum.any?(events, &(&1["type"] == "work_item_retry_scheduled"))
      assert Enum.count(events, &(&1["type"] == "gate_failed")) == 1
      refute_received {:run_child_started, ^sup, _, _}

      assert sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 1)) |> Enum.sort() ==
               started |> Keyword.values() |> Enum.sort()

      stop_owned!(owner)
    end

    test "executed-effect witnesses through the Server: queued Timer, retained-prompt FetchPrompt, recovery Reconci" do
      require_run!()
      {:ok, seen} = Agent.start_link(fn -> [] end)
      observer = fn effect, observation -> Agent.update(seen, &[{effect, observation} | &1]) end

      run_dir = tmp_run_dir()

      {o1, s1} =
        start_run!(%{
          run_dir: run_dir,
          mode: :run,
          spec: H.spec("gated_run_seed"),
          plan: H.plan("gated_run_seed"),
          opts:
            gated_index()
            |> fresh_opts()
            |> Keyword.delete(:event_sink)
            |> Keyword.merge(dispatch: QueuedAdapter, effect_observer: observer),
          trace: self()
        })

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               s1 |> started_children() |> Keyword.fetch!(:server) |> run_server().await(30_000)

      stop_owned!(o1)

      {:ok, cut_dir} = projection_cut_dir()

      {o2, s2} =
        start_run!(%{
          run_dir: cut_dir,
          mode: :resume,
          spec: H.spec("kill9_resume"),
          plan: H.plan("kill9_resume"),
          opts:
            gated_index()
            |> fresh_opts()
            |> Keyword.delete(:event_sink)
            |> Keyword.merge(prompt_root: cut_dir, effect_observer: observer),
          trace: self()
        })

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               s2 |> started_children() |> Keyword.fetch!(:server) |> run_server().await(30_000)

      stop_owned!(o2)

      rec_dir = seed_legacy!(tmp_run_dir(), recovery_prior())

      {o3, s3} =
        start_run!(%{
          run_dir: rec_dir,
          mode: :resume,
          spec: H.spec("gated_run_seed"),
          plan: H.plan("gated_run_seed"),
          opts: gated_index() |> fresh_opts() |> Keyword.delete(:event_sink) |> Keyword.put(:effect_observer, observer),
          trace: self()
        })

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               s3 |> started_children() |> Keyword.fetch!(:server) |> run_server().await(30_000)

      stop_owned!(o3)

      pairs = Agent.get(seen, & &1)
      executed = pairs |> Enum.map(fn {e, _} -> e.__struct__ end) |> Enum.uniq()
      for m <- [Effect.Timer, Effect.FetchPrompt, Effect.ReconcileGate], do: assert(m in executed, inspect(m))

      for {effect, observation} <- pairs do
        assert observation.__struct__ in Effect.admissible_observations(effect)
        assert correlation(effect) == correlation(observation)
      end
    end
  end

  @parity_instance "sup_parity_0001"

  # both sides receive the same explicit instance; the Server keeps a supplied one (put_new, as the CLI)
  defp parity_config(run_dir, kind, scenario, index) do
    %{
      run_dir: run_dir,
      mode: kind,
      spec: H.spec(scenario),
      plan: H.plan(scenario),
      opts: index |> fresh_opts() |> Keyword.delete(:event_sink) |> Keyword.put(:supervisor_instance, @parity_instance),
      trace: self()
    }
  end

  defp parity_opts(index, run_dir, opened) do
    index
    |> fresh_opts()
    |> Keyword.merge(
      supervisor_instance: @parity_instance,
      run_dir: run_dir,
      run_lock_path: opened.lock_path,
      tail_repair: Writer.tail_repair_data(opened.repair)
    )
    |> Keyword.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp parity_oracle(kind, scenario, prior, index, run_dir, opened) do
    opts = parity_opts(index, run_dir, opened)

    # the oracle mirrors the ratified split (loop here, effects in a separate owner process with its own clock)
    case kind do
      :run -> OwnerOracle.run(H.spec(scenario), H.plan(scenario), opts)
      :resume -> OwnerOracle.resume(H.spec(scenario), H.plan(scenario), prior, opts)
      :cancel -> OwnerOracle.cancel(prior, opts)
    end
  end

  defp projection_cut_dir do
    src = tmp_run_dir()

    {:ok, w, _} =
      Writer.open(src,
        create: true,
        lock: [supervisor_instance: "sup_cut", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
      )

    {_, :resume, "kill9_resume", _, opts_fun} = Enum.find(H.cases(), &match?({_, :resume, "kill9_resume", _, _}, &1))
    H.reset_seams()
    opts = Keyword.merge(opts_fun.(), prompt_root: src, event_sink: &Writer.append(w, &1))
    {:ok, %{events: events}} = Host.run(H.spec("kill9_resume"), H.plan("kill9_resume"), opts)
    :ok = Writer.close(w)
    prefix = Enum.take_while(events, &(&1["type"] != "assignment_dispatch_sent"))
    requested = for %{"type" => "assignment_requested", "data" => %{"assignment_id" => id}} <- prefix, do: id
    cut = seed_v2!(tmp_run_dir(), Enum.map(prefix, &Jason.encode!/1))
    File.mkdir_p!(Path.join(cut, "prompts"))

    for path <- Path.wildcard(Path.join([src, "prompts", "*.org"])),
        Enum.any?(requested, &String.starts_with?(Path.basename(path), &1 <> "-")),
        do: File.cp!(path, Path.join([cut, "prompts", Path.basename(path)]))

    {:ok, cut}
  end

  defp recovery_prior do
    prior = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    {prefix, _} = Enum.split_while(prior, &(&1["type"] != "gate_started"))
    seq = length(prefix) + 1

    start = %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 2,
      "type" => "gate_started",
      "ts" => "2026-09-01T12:00:00Z",
      "run_id" => "run_scenario_0001",
      "actor" => "run_supervisor",
      "seq" => seq,
      "event_id" => "ev_" <> String.pad_leading(Integer.to_string(seq), 4, "0"),
      "data" => %{
        "gate_run_id" => "gr_0001",
        "command_argv" => ["mix", "test"],
        "stdout_path" => "gates/gr_0001.1.out",
        "stderr_path" => "gates/gr_0001.1.err",
        "attempt" => 1,
        "deadline_unix" => 4_102_444_800,
        "execution" => %{"pid" => 4242, "pgid" => 4242, "start" => "1756728000.123456", "claim_hash" => @zero}
      }
    }

    Enum.map(prefix ++ [start], &Jason.encode!/1)
  end

  defp correlation(%{read_index: index}), do: {:read_index, index}
  defp correlation(%{gate_run_id: id}), do: {:gate_run_id, id}
  defp correlation(%{assignment_id: id}), do: {:assignment_id, id}
  defp correlation(%{object: %{assignment_id: id}}), do: {:assignment_id, id}
  defp correlation(%{purpose: purpose, deadline_unix: deadline}), do: {:timer, purpose, deadline}
  defp correlation(_), do: :none

  # =================================================================================================
  describe "R-H attention, sink refusal and trappable failures through t" do
    test "an unsettled gate exit is attention with truthful cleanup through await; the abandon ran once inside the" do
      require_run!()
      run_dir = tmp_run_dir()

      {owner, sup} =
        start_run!(config(run_dir, :run, Keyword.put(fresh_opts(gated_index()), :gate_executor, UnsettledExecutor)))

      server = sup |> started_children() |> Keyword.fetch!(:server)
      assert {:ok, %{summary: %{"status" => "blocked"}, events: events} = result} = run_server().await(server, 30_000)

      assert Enum.any?(
               events,
               &(&1["type"] == "human_attention_required" and &1["data"]["reason"] == "gate_settlement_unknown")
             )

      assert [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true, "proof" => "gone"}}] =
               result.gate_cleanup

      assert_received {:abandoned, _handle}
      refute_received {:abandoned, _}
      stop_owned!(owner)
    end

    test "a refused write of the gate_passed line (seam-flagged FaultFs) ends the run as journal_append_failed with" do
      require_run!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()

      FaultFs.inject(fs, :write, fn _args -> Seam.take(:refuse_next_write) == true end, {:error, :eio})

      observer = fn
        %Effect.AwaitGate{}, %Observation.GateFinished{} -> Seam.put(:refuse_next_write, true)
        _, _ -> :ok
      end

      {owner, sup} =
        start_run!(
          config(run_dir, :run, gated_index() |> fresh_opts() |> Keyword.merge(fs: fs, effect_observer: observer))
        )

      server = sup |> started_children() |> Keyword.fetch!(:server)

      assert {:error, %{"reason" => "journal_append_failed", "gate_cleanup" => [%{"gate_run_id" => "gr_0001"}]}} =
               run_server().await(server, 30_000)

      stop_owned!(owner)
    end

    # GREEN review m_1788645900000 M1/M2: gen_statem takes a THROWN term as the callback's return value, and
    # a raw exit reason is printed by the supervisor's child report. Each origin below carries a canary.
    # class expectations come from the contract's closed vocabulary: an exception is a "map", never a module
    for {label, origin, class} <- [
          {"observer throw of a next_state forged success", :throw_next_state, "tuple"},
          {"observer throw of an action-bearing callback tuple", :throw_action, "tuple"},
          {"executor throw of an action-bearing callback tuple", :executor_throw, "tuple"},
          {"observer raised exception message", :raise, "map"},
          {"observer error with a __exception__-marked private module", :marked_module, "map"},
          {"observer throw of a KNOWN contract struct", :known_struct, "map"},
          {"observer exit with a private atom", :exit_atom, "atom"},
          {"observer exit with a payload", :exit, "tuple"}
        ] do
      test "#{label}: no forged terminal; run_server_down; DOWN reason, crash logs and status carry no bytes" do
        require_run!()
        canary = "PRIVATE_RUN_SERVER_CANARY_#{System.unique_integer([:positive])}"
        Seam.put(:canary, canary)
        run_dir = tmp_run_dir()
        parent = self()

        fail = fn c ->
          case unquote(origin) do
            :throw_next_state -> throw({:next_state, :finished, %{result: {:ok, %{c => true}}}})
            :throw_action -> throw({:keep_state_and_data, [{:reply, {self(), make_ref()}, {:ok, %{c => true}}}]})
            :raise -> raise(c)
            :marked_module -> :erlang.error(%{__struct__: String.to_atom(c), __exception__: true})
            :known_struct -> throw(struct(Effect.Dispatch, assignment_id: "as_probe_0001", prompt_hash: c))
            :exit_atom -> exit(String.to_atom(c))
            :exit -> exit({:private, c})
          end
        end

        # the observer HOLDS before failing so the test's monitor is registered before the death (causal witness)
        observer = fn
          %Effect.ReleaseGate{}, _ ->
            send(parent, {:observer_held, self()})

            receive do
              :release_observer -> :ok
            end

            if unquote(origin) == :executor_throw, do: :ok, else: fail.(canary)

          _, _ ->
            :ok
        end

        executor = if unquote(origin) == :executor_throw, do: ThrowingAwaitExecutor, else: UnsettledExecutor
        opts = gated_index() |> fresh_opts() |> Keyword.merge(gate_executor: executor, effect_observer: observer)
        config = config(run_dir, :run, Keyword.put(opts, :review_canary_opt, canary))

        log =
          ExUnit.CaptureLog.capture_log(fn ->
            {owner, sup} = start_run!(config)
            started = started_children(sup)
            server = Keyword.fetch!(started, :server)
            assert_receive {:observer_held, ^server}, 30_000
            ref = Process.monitor(server)
            send(server, :release_observer)
            assert run_server().await(server, 30_000) == {:error, %{clause: "run_server_down"}}
            assert_receive {:DOWN, ^ref, :process, ^server, reason}, 5_000
            assert {:run_step_failed, %{kind: kind, class: class, digest: digest, frames: frames}} = reason
            assert kind in [:throw, :error, :exit] and is_integer(frames)
            assert digest =~ ~r/\Asha256:[0-9a-f]{64}\z/
            assert class == unquote(class), "the contract's closed result class"
            refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ canary
            assert_received {:abandoned, %{released: true}}, "the Host settled before the boundary closed"
            refute_received {:abandoned, _}, "exactly once, the latest handle"
            all_down!([sup | Keyword.values(started)])
            stop_owned!(owner)
            Logger.flush()
          end)

        refute log =~ canary, "statem and supervisor crash reports carry no observer/executor bytes"
        refute log =~ "review_canary_opt", "child-start arguments (the config) are not printed by the supervisor"
        refute Enum.any?(journal(run_dir), &(&1["type"] in ["gate_passed", "run_completed"])), "no fabricated terminal"
      end
    end

    test "positive: a closed diagnostic names the class and the digest of the escaped term; sys status and format_status are closed" do
      require_run!()
      canary = "PRIVATE_RUN_SERVER_STATUS_#{System.unique_integer([:positive])}"
      run_dir = tmp_run_dir()
      error = %RuntimeError{message: canary}

      observer = fn
        %Effect.ReleaseGate{}, _ -> raise(error)
        _, _ -> :ok
      end

      opts = gated_index() |> fresh_opts() |> Keyword.merge(gate_executor: UnsettledExecutor, effect_observer: observer)
      {owner, sup} = start_run!(config(run_dir, :run, opts))
      started = started_children(sup)
      server = Keyword.fetch!(started, :server)
      ref = Process.monitor(server)
      assert run_server().await(server, 30_000) == {:error, %{clause: "run_server_down"}}
      assert_receive {:DOWN, ^ref, :process, ^server, {:run_step_failed, diagnostic}}, 5_000
      assert %{kind: :error, class: "map", digest: digest, frames: frames} = diagnostic
      assert digest == Diagnostic.describe(error)["digest"] and is_integer(frames) and frames > 0
      assert "map" in Diagnostic.result_classes() and Diagnostic.result_class(error) == "map"
      all_down!([sup | Keyword.values(started)])
      stop_owned!(owner)

      # a finished Server answers sys:get_status through format_status: neither the run directory nor an
      # option byte appears; every field keeps a callback-type-valid closed value
      run_dir2 = tmp_run_dir()
      opts2 = gated_index() |> fresh_opts() |> Keyword.put(:review_canary_opt, canary)
      {owner2, sup2} = start_run!(config(run_dir2, :run, opts2))
      server2 = sup2 |> started_children() |> Keyword.fetch!(:server)
      assert {:ok, _} = run_server().await(server2, 30_000)
      status = inspect(:sys.get_status(server2), limit: :infinity, printable_limit: :infinity)
      refute status =~ canary
      refute status =~ run_dir2
      assert status =~ ":finished"
      stop_owned!(owner2)

      sanitized =
        run_server().format_status(%{
          state: :driving,
          data: %{payload: canary},
          reason: {:adapter_failed, canary},
          queue: [{:info, canary}, {{:call, {self(), make_ref()}}, {:await, canary}}],
          postponed: [{:internal, canary}],
          timeouts: [{:state_timeout, canary}],
          log: [canary]
        })

      refute inspect(sanitized, limit: :infinity, printable_limit: :infinity) =~ canary
      assert %{state: :driving, data: :redacted, log: [], postponed: [{:internal, :redacted}]} = sanitized
      assert [{:info, :redacted}, {{:call, _from}, :redacted}] = sanitized.queue
      assert {:exit, %{kind: :exit, class: "tuple", digest: _}, []} = sanitized.reason

      # an atom exit reason is described by its result class, never by its name; a stack never survives
      private_atom = :"PRIVATE_ATOM_#{System.unique_integer([:positive])}"
      frame = [{__MODULE__, :f, [canary], [file: ~c"PRIVATE_SOURCE_FILE", line: 1]}]
      closed = run_server().format_status(%{reason: {:error, error, frame}}).reason
      assert {:error, %{kind: :error, class: "map", frames: 1}, []} = closed
      refute inspect(closed, limit: :infinity) =~ "PRIVATE_SOURCE_FILE"
      atom_closed = run_server().format_status(%{reason: {:exit, private_atom, frame}}).reason
      assert {:exit, %{kind: :exit, class: "atom"}, []} = atom_closed
      refute inspect(atom_closed, limit: :infinity) =~ Atom.to_string(private_atom)
      # a __exception__ marker is no trust boundary: the marked module name never appears
      marked = %{__struct__: private_atom, __exception__: true}
      assert {:error, %{class: "map"}, []} = run_server().format_status(%{reason: {:error, marked, frame}}).reason

      refute inspect(run_server().format_status(%{reason: {:error, marked, frame}}), limit: :infinity) =~
               Atom.to_string(private_atom)

      # a reason merely SHAPED like this process's closed diagnostic is re-described unless every field is closed
      digest_ok = "sha256:" <> String.duplicate("0", 64)
      # the closed diagnostic domain is five keys: the cleanup summary is part of it (contract rev 4)
      cleanup_ok = %{attempts: 0, settled: 0, unproven: 0}
      genuine = {:run_step_failed, %{kind: :throw, class: "tuple", digest: digest_ok, frames: 3, cleanup: cleanup_ok}}
      assert run_server().format_status(%{reason: genuine}).reason == genuine

      for spoof <- [
            {:run_step_failed, %{payload: canary}},
            {:run_step_failed, %{kind: :throw, class: "tuple", digest: digest_ok, frames: 3, extra: canary}},
            {:run_step_failed, %{kind: :throw, class: canary, digest: digest_ok, frames: 3}},
            {:run_step_failed, %{kind: :throw, class: "tuple", digest: canary, frames: 3}},
            {:run_step_failed, %{kind: String.to_atom(canary), class: "tuple", digest: digest_ok, frames: 3}},
            {:run_step_failed, %{kind: :throw, class: "tuple", digest: digest_ok, frames: canary}}
          ] do
        spoofed = run_server().format_status(%{reason: spoof})
        assert {:exit, %{kind: :exit, class: "tuple"}, []} = spoofed.reason
        refute inspect(spoofed, limit: :infinity, printable_limit: :infinity) =~ canary
      end
    end

    for {origin, kind} <- [{:observer, :throw}, {:observer, :exit}, {:executor, :error}] do
      test "a trappable #{origin} #{kind} after ReleaseGate closes inside the Server (abandon once) and is run_server" do
        require_run!()
        _ = unquote(kind)
        run_dir = tmp_run_dir()
        base = gated_index() |> fresh_opts() |> Keyword.put(:gate_executor, UnsettledExecutor)

        opts =
          case unquote(origin) do
            :observer ->
              Keyword.put(base, :effect_observer, fn
                %Effect.ReleaseGate{}, _ ->
                  case unquote(kind) do
                    :throw -> throw(:red_escape)
                    :exit -> exit(:red_escape)
                  end

                _, _ ->
                  :ok
              end)

            :executor ->
              Keyword.put(base, :gate_executor, RaisingAwaitExecutor)
          end

        {owner, sup} = start_run!(config(run_dir, :run, opts))
        started = started_children(sup)
        result = run_server().await(Keyword.fetch!(started, :server), 30_000)
        assert result == {:error, %{clause: "run_server_down"}}, "exact closed error; got #{inspect(result)}"
        assert_received {:abandoned, %{released: true}}
        refute_received {:abandoned, _}
        all_down!([sup | Keyword.values(started)])
        assert Process.alive?(owner)
        stop_owned!(owner)
      end
    end
  end

  # =================================================================================================
  describe "R-K await semantics (D-4), deterministic via a held gate and" do
    defp held_config(run_dir),
      do: config(run_dir, :run, Keyword.put(fresh_opts(gated_index()), :gate_opts, runner: HeldGate.runner(self())))

    test "await timeout only waits (work continues); the terminal result is cached and repeated reads add no effect" do
      require_run!()
      run_dir = tmp_run_dir()
      {:ok, effects} = Agent.start_link(fn -> 0 end)

      config =
        run_dir
        |> held_config()
        |> Map.update!(:opts, &Keyword.put(&1, :effect_observer, fn _, _ -> Agent.update(effects, fn n -> n + 1 end) end))

      {owner, sup} = start_run!(config)
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      assert_receive {:gate_entered, ^executing}, 30_000
      assert run_server().await(server, 50) == {:error, %{clause: "await_timeout"}}
      assert Process.alive?(server), "a caller's timeout never kills the work"
      send(executing, :release_gate)
      first = run_server().await(server, 30_000)
      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} = first
      assert run_server().status(server) == :finished, "status is answered once the sequential step is over"
      n_effects = Agent.get(effects, & &1)
      assert run_server().await(server, 1_000) == first

      assert Agent.get(effects, & &1) == n_effects and length(journal(run_dir)) == length(events),
             "cached reads rerun nothing"

      stop_owned!(owner)
    end

    test "an await SUBSCRIBER killed while its call is pending (monitor witness) does not take the work with it" do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(held_config(run_dir))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      assert_receive {:gate_entered, ^executing}, 30_000
      {caller, ref} = spawn_monitor(fn -> run_server().await(server, 60_000) end)
      assert wait_until_monitoring(server, caller, 2_000), "the subscriber's call is pending (it monitors the Server)"
      Process.exit(caller, :kill)
      assert_receive {:DOWN, ^ref, :process, ^caller, :killed}, 1_000
      assert Process.alive?(server)
      send(executing, :release_gate)
      assert {:ok, %{summary: %{"status" => "completed"}}} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    test "a Server killed while a subscriber's call is pending: exactly run_server_down (no DOWN bytes); all DOWNs" do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(held_config(run_dir))
      started = started_children(sup)
      server = Keyword.fetch!(started, :server)
      executing = executing!()
      assert_receive {:gate_entered, ^executing}, 30_000
      parent = self()
      caller = spawn_link(fn -> send(parent, {:await_result, run_server().await(server, 10_000)}) end)
      assert wait_until_monitoring(server, caller, 2_000)
      Process.exit(server, {:shutdown, :red_private_down_reason})
      assert_receive {:await_result, result}, 10_000
      assert result == {:error, %{clause: "run_server_down"}}
      refute inspect(result) =~ "red_private_down_reason"
      all_down!([sup | Keyword.values(started)])
      assert run_server().await(server, 100) == {:error, %{clause: "run_server_down"}}
      stop_owned!(owner)
    end
  end

  # =================================================================================================
  describe "R-C / R-D / R-E / R-L / R-F real guardian (D-1, D-6, Q-3)" do
    setup do
      dir = Path.join(System.tmp_dir!(), "run-server-red-build-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      bin = Path.join(dir, "gate_guardian")

      {"", 0} =
        System.cmd(@guardian_src, [bin], stderr_to_stdout: true)

      on_exit(fn -> File.rm_rf(dir) end)
      {:ok, helper: bin}
    end

    defp real_opts(helper, parent),
      do:
        gated_index()
        |> fresh_opts()
        |> Keyword.drop([:gate_executor, :gate_helper, :gate_opts, :event_sink])
        |> Keyword.merge(gate_helper: helper, gate_opts: [barrier: barrier(parent)])
        |> Keyword.update!(:dispatch_opts, &Keyword.update!(&1, :artifact_reader, fn r -> rework_tolerant(r) end))

    # a REAL gate failure (R-F's timeout) makes the reducer rework: it dispatches exactly two further
    # assignments the scenario fixture never recorded - the writer's rework (as_0003) and the reviewer's
    # re-review (as_0004) - so the fixture's artifact map has no record for them. Serve each the fixture's
    # record of the SAME role (as_0001 writer, as_0002 reviewer) under its own ids; every other assignment
    # goes to the fixture reader unchanged (its KeyError stays a real failure).
    @rework_assignments %{"as_0003" => "as_0001", "as_0004" => "as_0002"}

    defp rework_tolerant(reader) do
      fn
        %{"assignment_id" => id} when is_map_key(@rework_assignments, id) ->
          {:ok, record} = reader.(%{"assignment_id" => Map.fetch!(@rework_assignments, id)})
          {:ok, %{record | "assignment_id" => id, "artifact_id" => "art_" <> id}}

        command ->
          reader.(command)
      end
    end

    defp real_spec(run_dir, argv),
      do:
        "gated_run_seed"
        |> H.spec()
        |> Map.update!("gates", fn g -> Map.new(g, fn {id, _} -> {id, argv} end) end)
        |> Map.put("repo_root", run_dir)

    defp marker_argv(run_dir),
      do: ["/bin/sh", "-c", ~s{m="#{run_dir}/attempt1.ran"; [ -e "$m" ] && exit 0; : > "$m"; sleep 30}]

    defp real_config(run_dir, mode, argv, opts),
      do: %{
        run_dir: run_dir,
        mode: mode,
        spec: real_spec(run_dir, argv),
        plan: H.plan("gated_run_seed"),
        opts: opts,
        trace: self()
      }

    test "control: native path - Port connected to the executing process; READY is two-way; no ack fails closed", %{
      helper: helper
    } do
      run_dir = tmp_run_dir()
      parent = self()

      request = fn gate_run_id ->
        %{
          run_id: "run_fixture_0001",
          gate_run_id: gate_run_id,
          attempt: 1,
          command_argv: ["/bin/sh", "-c", "sleep 30"],
          repo_root: run_dir,
          run_dir: run_dir,
          deadline_unix: FixedClock.unix_now() + 600,
          supervisor_instance: "sup_ctl"
        }
      end

      barrier = barrier(parent, 30_000)

      {worker, ref} =
        spawn_monitor(fn ->
          {:ok, prepared} =
            Execution.prepare(SystemFs.new(), request.("gr_ctl1"), helper: helper, clock: FixedClock, barrier: barrier)

          send(parent, {:prepared, self(), Execution.identity(prepared)})

          receive do
            :abandon -> _ = Execution.abandon(prepared)
          end
        end)

      assert_receive {:ready, identity, ^worker, {:connected, ^worker}, ack}, 30_000
      refute_received {:prepared, ^worker, _}, "the executing process is held at READY until the ack"
      send(worker, {:ready_ack, ack})
      assert_receive {:prepared, ^worker, ^identity}, 30_000
      send(worker, :abandon)
      assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 10_000
      assert wait_until(fn -> dead?(identity) end, 10_000)

      # absent ack (the recipient failed an assertion first): the executing process fails closed with the
      # exact ref, its Port closes, and the guardian drives the group to EOF cleanup
      short_barrier = barrier(parent, 300)

      {worker2, ref2} =
        spawn_monitor(fn ->
          Execution.prepare(SystemFs.new(), request.("gr_ctl2"),
            helper: helper,
            clock: FixedClock,
            barrier: short_barrier
          )
        end)

      assert_receive {:ready, identity2, ^worker2, {:connected, ^worker2}, ack2}, 30_000
      assert_receive {:DOWN, ^ref2, :process, ^worker2, {:ready_not_acknowledged, ^ack2}}, 5_000
      assert wait_until(fn -> dead?(identity2) end, 10_000), "EOF cleanup after the fail-closed exit"
    end

    test "R-C the guardian Port is connected to the Run.Server process while live (Port.info at READY)", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()

      {owner, sup} =
        start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "exit 0"], real_opts(helper, self())))

      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, port_owner} = ready!(owner, executing)
      assert port_owner == {:connected, executing}
      assert {:ok, %{summary: %{"status" => "completed"}}} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    test "R-D Server kill after GO: whole subtree down, EOF settle, exact durable prefix without terminal, ownershi", %{
      helper: helper
    } do
      require_run!()
      run_dir = tmp_run_dir()
      argv = marker_argv(run_dir)

      {owner, sup} =
        start_run!(real_config(run_dir, :run, argv, real_opts(helper, self())))

      started = started_children(sup)
      writer = Keyword.fetch!(started, :writer)
      server = Keyword.fetch!(started, :server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      assert_receive {:after_go, ^executing, _liveness}, 5_000

      assert wait_until(fn -> File.exists?(Path.join(run_dir, "attempt1.ran")) end, 5_000),
             "attempt 1 ran its command before the kill"

      Process.exit(server, :kill)
      all_down!([sup | Keyword.values(started)])
      refute_received {:run_child_started, ^sup, _, _}, "no automatic restart"
      assert wait_until(fn -> dead?(identity) end, 10_000), "settled on control EOF"

      events = journal(run_dir)
      [started_evt] = Enum.filter(events, &(&1["type"] == "gate_started"))
      refute Enum.any?(events, &(&1["type"] in ["gate_passed", "gate_failed"]))
      refute match?({:ok, %{state: :live}}, Ownership.status(run_dir))

      recorded_start = started_evt["ts"] |> DateTime.from_iso8601() |> elem(1) |> DateTime.to_unix()
      deadline = started_evt["data"]["deadline_unix"]
      recovery_now = recorded_start + 60
      assert recovery_now >= recorded_start and recovery_now < deadline
      Seam.put(:now, recovery_now)
      parent = self()

      retry_observer = fn
        %Effect.PrepareGate{attempt: 2, requested: requested}, _ -> send(parent, {:retry_argv, requested["command_argv"]})
        _, _ -> :ok
      end

      {owner2, sup2} =
        start_run!(
          real_config(
            run_dir,
            :resume,
            argv,
            helper |> real_opts(self()) |> Keyword.merge(clock: SeamClock, effect_observer: retry_observer)
          )
        )

      started2 = started_children(sup2)
      server2 = Keyword.fetch!(started2, :server)
      assert Keyword.fetch!(started2, :writer) != writer, "a NEW Writer owns the run"
      {_identity2, _} = ready!(owner2, executing!())
      assert {:ok, %{summary: %{"status" => "completed"}}} = run_server().await(server2, 60_000)
      assert_received {:retry_argv, ^argv}
      [a1, a2] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))
      assert a1["data"]["attempt"] == 1 and a2["data"]["attempt"] == 2

      assert a1["data"]["gate_run_id"] == a2["data"]["gate_run_id"] and
               a1["data"]["deadline_unix"] == a2["data"]["deadline_unix"]

      assert a2["data"]["command_argv"] == argv
      stop_owned!(owner2)
    end

    test "R-E Writer kill while driving: Server and Work down, EOF settle, same recovery evidence", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      argv = marker_argv(run_dir)

      {owner, sup} =
        start_run!(real_config(run_dir, :run, argv, real_opts(helper, self())))

      started = started_children(sup)
      writer = Keyword.fetch!(started, :writer)
      _server = Keyword.fetch!(started, :server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      assert_receive {:after_go, ^executing, _liveness}, 5_000
      assert wait_until(fn -> File.exists?(Path.join(run_dir, "attempt1.ran")) end, 5_000)
      Process.exit(writer, :kill)
      all_down!([sup | Keyword.values(started)])
      assert wait_until(fn -> dead?(identity) end, 10_000)
      [started_evt] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))
      refute Enum.any?(journal(run_dir), &(&1["type"] in ["gate_passed", "gate_failed"]))
      Seam.put(:now, (started_evt["ts"] |> DateTime.from_iso8601() |> elem(1) |> DateTime.to_unix()) + 60)

      {owner2, sup2} =
        start_run!(real_config(run_dir, :resume, argv, helper |> real_opts(self()) |> Keyword.put(:clock, SeamClock)))

      started2 = started_children(sup2)
      server2 = Keyword.fetch!(started2, :server)
      assert Keyword.fetch!(started2, :writer) != writer
      {_identity2, _} = ready!(owner2, executing!())
      assert {:ok, %{summary: %{"status" => "completed"}}} = run_server().await(server2, 60_000)

      assert [%{"data" => %{"attempt" => 1}}, %{"data" => %{"attempt" => 2, "command_argv" => ^argv}}] =
               Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))

      stop_owned!(owner2)
    end

    test "R-L a supervisor shutdown after GO settles the group by EOF and journals no terminal; no owner TERM was i", %{
      helper: helper
    } do
      require_run!()
      run_dir = tmp_run_dir()

      {owner, sup} =
        start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 30"], real_opts(helper, self())))

      _server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      assert_receive {:after_go, ^executing, _liveness}, 5_000
      ref = Process.monitor(sup)
      stop_owned!(owner)
      assert_receive {:DOWN, ^ref, :process, _, _}, 20_000
      assert wait_until(fn -> dead?(identity) end, 10_000), "settled by control EOF"
      refute Enum.any?(journal(run_dir), &(&1["type"] in ["gate_passed", "gate_failed"]))
      refute_received {:terminated, _}
    end

    test "R-F (labelled inherited sequential deadline) owner-clock expiry TERMs the live group through the Server", %{
      helper: helper
    } do
      require_run!()
      run_dir = tmp_run_dir()

      opts =
        helper
        |> real_opts(self())
        |> Keyword.merge(
          clock: SeamClock,
          effect_observer: fn
            %Effect.ReleaseGate{}, _ -> Seam.put(:now, FixedClock.unix_now() + 10_000)
            _, _ -> :ok
          end
        )

      argv = ["/bin/sh", "-c", ~s{[ "$(ls gates/*.claim.* | wc -l)" -gt 1 ] && exit 0; sleep 30}]
      {owner, sup} = start_run!(real_config(run_dir, :run, argv, opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      # liveness measured INSIDE the held GO boundary by the executing process, before the clock jump
      assert_receive {:after_go, ^executing, :alive}, 30_000
      # the timed-out gate makes the reducer rework (a further assignment, then a NEW gate run whose
      # command now sees two claims and exits 0): that second gate's READY is acknowledged too (MUST-10)
      # the same worker executes the rework's gate: one owner per run
      {_identity2, _} = ready!(owner, executing)

      assert {:ok, _} = run_server().await(server, 30_000)
      # the executing process saw the group terminated for the timeout and proved it gone (Execution's shape)
      assert_received {:terminated, %{kind: "timeout", settled: true, proof: "gone"}}
      [failed | _] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_failed"))
      assert failed["data"]["termination"]["kind"] == "timeout"
      [started_evt | _] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))

      assert started_evt["data"]["deadline_unix"] -
               (started_evt["ts"] |> DateTime.from_iso8601() |> elem(1) |> DateTime.to_unix()) == 600

      stop_owned!(owner)
    end

    # ---- AW (docs/contracts/gate-async-await-proposal.org rev 3): Server-owned deadline authority, RED ----
    # REAL Run.Server, REAL Worker, REAL guardian through the same harness as R-F. Fails today: no Server timer exists
    # and no :gate_deadline_observer fact is ever emitted. Observer (D-10): {:gate_deadline, role, identity, event},
    # identity = %{cap, gen, ref}; closed events include {:armed, wait}, {:early, wait}, :request_expiration,
    # :cancelled (Server) and :pending, :settle_await, :resume (Worker).
    defmodule AwFaultClock do
      @moduledoc false
      def unix_now, do: if(Seam.get(:fault) == :unix, do: raise("unix clock fault"), else: SeamClock.unix_now())
      def wall_ts, do: SeamClock.wall_ts()
      def monotonic_ms, do: if(Seam.get(:fault) == :mono, do: raise("mono clock fault"), else: SeamClock.monotonic_ms())
    end

    # counts clock reads per calling pid (AR-M10 exact arm samples); wall from SeamClock
    defmodule AwCountingClock do
      @moduledoc false
      def unix_now do
        count(:unix)
        SeamClock.unix_now()
      end

      def wall_ts, do: SeamClock.wall_ts()

      def monotonic_ms do
        count(:mono)
        SeamClock.monotonic_ms()
      end

      defp count(kind) do
        key = {:clock_read, kind, self()}
        Seam.put(key, (Seam.get(key) || 0) + 1)
      end
    end

    # a fault keyed to ONE process (AR-M15a): raises for `kind` only when the caller IS that pid, so an arm-phase
    # fault aimed at the Server can never hit the Worker's own release/await reads (the process-agnostic AwFaultClock
    # does: that is the pre-existing T-4 Worker/Host fault, kept as its own control)
    defmodule AwPidFaultClock do
      @moduledoc false
      def unix_now do
        check(:unix)
        SeamClock.unix_now()
      end

      def wall_ts, do: SeamClock.wall_ts()

      def monotonic_ms do
        check(:mono)
        SeamClock.monotonic_ms()
      end

      # the hit is recorded BEFORE raising: the witness of which process the fault fired in, and for which sample
      defp check(kind) do
        if Seam.get(:pid_fault) == {kind, self()} do
          Seam.put(:pid_fault_hit, {kind, self()})
          raise("#{kind} clock fault in #{inspect(self())}")
        end
      end
    end

    # an executor whose synchronous await HOLDS in the executing Worker until told to fail, then raises: a
    # correlated closed effect_failed AFTER the Server armed and chunked (AR-M15b EF-1). Bounded (no sleep).
    defmodule HeldRaisingExecutor do
      @moduledoc false
      defdelegate prepare(fs, request, opts), to: GateDouble
      defdelegate started_data(handle), to: GateDouble
      defdelegate ack(handle, event), to: GateDouble
      defdelegate release(handle, ack, opts), to: GateDouble
      defdelegate pass?(outcome), to: GateDouble
      defdelegate evidence(dir, id, attempt), to: GateDouble
      defdelegate reconcile(fs, dir, expected, opts), to: GateDouble

      def await(_handle, _opts) do
        if pid = Seam.get(:held_executor_subscriber), do: send(pid, {:executor_holding, self()})

        receive do
          :fail_now -> raise("held executor failure after the arm")
        after
          60_000 -> raise("held executor released by its bound")
        end
      end

      def abandon(handle) do
        if pid = Seam.get(:abandon_subscriber), do: send(pid, {:abandoned, handle})
        :ok
      end
    end

    # a minimal statem owning one generic timeout: the control that the Time-outs parser reads a REAL scheduled
    # timer and its cancellation (TP-1)
    defmodule TimerProbeStatem do
      @moduledoc false
      @behaviour :gen_statem

      def callback_mode, do: :handle_event_function
      def init(:ok), do: {:ok, :idle, nil}

      def handle_event({:call, from}, {:arm, name}, :idle, data),
        do: {:keep_state, data, [{{:timeout, name}, 60_000, :probe_content}, {:reply, from, :ok}]}

      def handle_event({:call, from}, {:cancel, name}, :idle, data),
        do: {:keep_state, data, [{{:timeout, name}, :cancel}, {:reply, from, :ok}]}
    end

    defp aw_opts(helper, parent, extra) do
      helper
      |> real_opts(parent)
      |> Keyword.merge(clock: SeamClock, gate_deadline_observer: parent)
      |> Keyword.merge(extra)
    end

    defp aw_jump_on(effect_mod, delta) do
      fn effect, _ ->
        if effect.__struct__ == effect_mod, do: Seam.put(:now, FixedClock.unix_now() + delta)
        :ok
      end
    end

    defp aw_argv_rework, do: ["/bin/sh", "-c", ~s{[ "$(ls gates/*.claim.* | wc -l)" -gt 1 ] && exit 0; sleep 30}]
    defp aw_gated_argv(run_dir), do: ["/bin/sh", "-c", "while [ ! -f '#{run_dir}/go' ]; do sleep 0.02; done; exit 0"]

    # ---- before-arm hold (AR-M14): the EXISTING test-owned gate barrier (Execution `opts[:barrier]`, test-only)
    # blocks the executing Worker at :after_go until the test acks. :after_go runs inside the ReleaseGate effect
    # (execution.ex release_checked/2, after GO was written to the guardian), so while it holds the Server has not
    # received the release result, has not committed the AwaitGate stage and cannot have requested or armed it:
    # witnesses installed during the hold provably precede the measured action. Bounded like :after_ready (exit on
    # no ack); no extra process. The hold is scoped to ONE measured attempt by explicit correlation (AR-M14-R1):
    # the first :after_go this barrier sees records that attempt's {gate_run_id, attempt} (the gate_started data
    # the barrier receives) and holds; a later :after_go with a different identity (the rework is a NEW gate run,
    # attempt 1 again) sends {:after_go_later, executing, live, key} and continues, so a retry never waits for an
    # ack the row does not give (RC-1 is the real-retry control). ----
    defp aw_hold_barrier(parent, ack_ms \\ 30_000) do
      base = barrier(parent, ack_ms)
      token = make_ref()

      fn
        :after_go, started_data ->
          key = {started_data["gate_run_id"], started_data["attempt"]}
          live = signal_zero(Integer.to_string(started_data["execution"]["pid"]))

          case Process.get({:aw_hold, token}) do
            nil ->
              Process.put({:aw_hold, token}, key)
              ref = make_ref()
              send(parent, {:after_go_hold, self(), live, ref, key})

              receive do
                {:go_ack, ^ref} -> :ok
              after
                ack_ms -> exit({:go_not_acknowledged, ref})
              end

            _measured ->
              send(parent, {:after_go_later, self(), live, key})
              :ok
          end

        name, info ->
          base.(name, info)
      end
    end

    defp aw_hold_opts(helper, parent, extra),
      do: helper |> aw_opts(parent, extra) |> Keyword.put(:gate_opts, barrier: aw_hold_barrier(parent))

    defp held_at_go!(executing) do
      assert_receive {:after_go_hold, ^executing, :alive, ref, {gate_run_id, attempt} = key}, 30_000
      assert is_binary(gate_run_id) and attempt in [1, 2]
      %{ref: ref, key: key}
    end

    defp release_go!(executing, %{ref: ref}), do: send(executing, {:go_ack, ref})

    # ---- immutable arm snapshot (AR-M14-R2): the ORDERED trace stream of ONE process - its calls to the resolved
    # clock (SeamClock unix_now/0, monotonic_ms/0) and its sends - consumed in that process's own execution order
    # and CUT at a correlated end marker the process sends itself (the Server's own {:armed, _} fact; an actor's
    # :measured). Calls after the marker (a later chunk read) cannot change the count, however late the test reads
    # the stream. Test-only: process trace flags die with the process; the global pattern is reset in on_exit. ----
    defp aw_trace_clock!(pid) do
      :erlang.trace_pattern({SeamClock, :unix_now, 0}, true, [])
      :erlang.trace_pattern({SeamClock, :monotonic_ms, 0}, true, [])
      on_exit(fn -> :erlang.trace_pattern({SeamClock, :_, :_}, false, []) end)
      1 = :erlang.trace(pid, true, [:call, :send])
      :ok
    end

    defp aw_reads_before_marker!(pid, marker, timeout),
      do: aw_collect_reads(pid, marker, %{unix: 0, mono: 0}, System.monotonic_time(:millisecond) + timeout)

    defp aw_collect_reads(pid, marker, counts, deadline) do
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:trace, ^pid, :call, {SeamClock, :unix_now, []}} ->
          aw_collect_reads(pid, marker, %{counts | unix: counts.unix + 1}, deadline)

        {:trace, ^pid, :call, {SeamClock, :monotonic_ms, []}} ->
          aw_collect_reads(pid, marker, %{counts | mono: counts.mono + 1}, deadline)

        {:trace, ^pid, :send, message, _to} ->
          case marker.(message) do
            {:ok, value} -> {counts, value}
            :skip -> aw_collect_reads(pid, marker, counts, deadline)
          end
      after
        remaining -> flunk("no end marker in the traced stream of #{inspect(pid)}; reads so far #{inspect(counts)}")
      end
    end

    @tag :aw_server
    test "AW-S1 already-due: Server send, owner settle_await, real timeout termination, no backstop", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_hold_opts(helper, self(), effect_observer: aw_jump_on(Effect.ReleaseGate, 10_000))
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_argv_rework(), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      # AR-M14: the :send trace is installed while the Worker is HELD inside ReleaseGate, before any AwaitGate request
      hold = held_at_go!(executing)
      :erlang.trace(server, true, [:send])
      release_go!(executing, hold)
      # the exact request whose identity the fence must carry
      assert_receive {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate} = req}, 30_000
      # correlated actuation: the Server armed and sent for THIS identity; the owner entered settle_await for it
      assert_receive {:gate_deadline, :server, %{cap: cap, gen: gen, ref: ref} = id, {:armed, 0}}, 30_000
      assert id == %{cap: req.cap, gen: req.gen, ref: req.ref}
      assert_receive {:gate_deadline, :server, ^id, :request_expiration}, 30_000
      # the ACTUAL send, from the Server pid to the executing Worker pid, with this identity
      assert_receive {:trace, ^server, :send, {:gate_deadline, ^cap, ^gen, ^ref}, ^executing}, 5_000
      :erlang.trace(server, false, [:send])
      assert_receive {:gate_deadline, :worker, ^id, :settle_await}, 30_000
      # the rework attempt (AR-M14-R1): a NEW gate run, its :after_go is the plain notification (the rework command
      # may already have exited when the barrier samples liveness), never a second hold
      {_identity2, _} = ready!(owner, executing)
      assert_receive {:after_go_later, ^executing, _live2, key2}, 30_000
      assert key2 != hold.key
      assert {:ok, _} = run_server().await(server, 30_000)
      refute_received {:after_go_hold, ^executing, _, _, _}
      assert_received {:terminated, %{kind: "timeout", settled: true, proof: "gone"} = termination}
      refute Map.has_key?(termination, :backstop), "owner TERM through the Server, not the guardian backstop"
      [failed | _] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_failed"))
      assert failed["data"]["termination"]["kind"] == "timeout"
      refute Map.has_key?(failed["data"]["termination"], "backstop")
      [started | _] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))
      assert failed["data"]["gate_run_id"] == started["data"]["gate_run_id"]
      # the journaled first attempt is the exact held one (gate_failed v2 carries no attempt field of its own)
      assert {started["data"]["gate_run_id"], started["data"]["attempt"]} == hold.key
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-S2a never due: genuine early chunks (cap 50 ms) then the permit; cancelled on the result; no expiry",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_opts(helper, self(), gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      assert_receive {:gate_deadline, :server, %{ref: ref} = id, {:armed, wait0}}, 30_000
      assert is_integer(wait0) and wait0 > 0 and wait0 <= 50
      assert_receive {:gate_deadline, :server, ^id, {:early, w1}}, 5_000
      assert_receive {:gate_deadline, :server, ^id, {:early, w2}}, 5_000
      assert w1 <= 50 and w2 <= 50
      File.write!(Path.join(run_dir, "go"), "")
      assert_receive {:gate_deadline, :server, ^id, :cancelled}, 30_000
      refute_received {:gate_deadline, :server, %{ref: ^ref}, :request_expiration}
      assert {:ok, _} = run_server().await(server, 30_000)
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-S2b future arm -> genuine chunks -> due -> ONE actuation (cap 200 ms, 1 s remaining)", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      # the journaled deadline is start + 600 s; after ReleaseGate the wall jumps to 599 s past start: 1 s remains
      opts = aw_opts(helper, self(), gate_deadline_cap_ms: 200, effect_observer: aw_jump_on(Effect.ReleaseGate, 599))
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_argv_rework(), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      assert_receive {:gate_deadline, :server, %{ref: _} = id, {:armed, wait0}}, 30_000
      assert wait0 <= 200
      assert_receive {:gate_deadline, :server, ^id, {:early, _}}, 5_000
      assert_receive {:gate_deadline, :server, ^id, {:early, _}}, 5_000
      assert_receive {:gate_deadline, :server, ^id, :request_expiration}, 10_000
      refute_receive {:gate_deadline, :server, ^id, :request_expiration}, 500
      assert_receive {:gate_deadline, :worker, ^id, :settle_await}, 30_000
      {_identity2, _} = ready!(owner, executing)
      assert {:ok, _} = run_server().await(server, 30_000)
      assert_received {:terminated, %{kind: "timeout"}}
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-S4b a wall move WHILE ARMED does not shift the due instant (no re-arm from the wall)", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_opts(helper, self(), gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      assert_receive {:gate_deadline, :server, %{ref: _} = id, {:armed, _}}, 30_000
      assert_receive {:gate_deadline, :server, ^id, {:early, _}}, 5_000
      Seam.put(:now, FixedClock.unix_now() + 10_000)
      assert_receive {:gate_deadline, :server, ^id, {:early, _}}, 5_000
      refute_receive {:gate_deadline, :server, ^id, :request_expiration}, 500
      File.write!(Path.join(run_dir, "go"), "")
      assert_receive {:gate_deadline, :server, ^id, :cancelled}, 30_000
      assert {:ok, _} = run_server().await(server, 30_000)
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-S4 control: a wall move observed AFTER the AwaitGate does not expire the gate", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_opts(helper, self(), effect_observer: aw_jump_on(Effect.AwaitGate, 10_000))
      {owner, sup} = start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 2; exit 0"], opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      assert {:ok, _} = run_server().await(server, 30_000)
      refute_received {:terminated, %{kind: "timeout"}}
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-S3 owner DOWN while ARMED and pending: run_effect_owner_down with writer_generation, cancelled, no send",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_opts(helper, self(), [])
      {owner, sup} = start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 30"], opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      assert_receive {:after_go, ^executing, :alive}, 30_000
      assert_receive {:gate_deadline, :server, %{ref: _} = id, {:armed, _}}, 30_000
      assert_receive {:gate_deadline, :worker, ^id, :pending}, 30_000
      mref = Process.monitor(server)
      Process.exit(executing, :kill)
      assert_receive {:gate_deadline, :server, ^id, :cancelled}, 5_000
      refute_received {:gate_deadline, :server, ^id, :request_expiration}
      assert {:ok, %{generation: g0}} = Ownership.status(run_dir)
      assert {:error, %{clause: "run_effect_owner_down", writer_generation: g}} = run_server().await(server, 30_000)
      assert g == g0, "the exact captured Writer generation"
      refute_received {:DOWN, ^mref, :process, ^server, _}
      assert wait_until(fn -> dead?(identity) end, 15_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "T-4 control: a clock failure after release is ALREADY a controlled Server exit (run_step_failed, cleanup); the arm read must keep it",
         %{
           helper: helper
         } do
      require_run!()
      run_dir = tmp_run_dir()
      Seam.put(:fault, nil)

      opts =
        aw_opts(helper, self(),
          clock: AwFaultClock,
          effect_observer: fn
            %Effect.ReleaseGate{}, _ -> Seam.put(:fault, :unix)
            _, _ -> :ok
          end
        )

      {owner, sup} = start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 30"], opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      mref = Process.monitor(server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)

      assert_receive {:DOWN, ^mref, :process, ^server,
                      {:run_step_failed, %{kind: :error, cleanup: %{settled: s, attempts: a, unproven: u}}}},
                     30_000

      assert is_integer(s) and is_integer(a) and is_integer(u) and a >= 1
      refute_received {:gate_deadline, :server, _, {:armed, _}}
      assert wait_until(fn -> dead?(identity) end, 15_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "T-5 later clock failure on a chunk read: controlled Server exit with cleanup, no actuation", %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      Seam.put(:fault, nil)
      opts = aw_opts(helper, self(), clock: AwFaultClock, gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 30"], opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      mref = Process.monitor(server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      assert_receive {:gate_deadline, :server, %{ref: _} = id, {:armed, _}}, 30_000
      Seam.put(:fault, :mono)

      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, %{kind: :error, cleanup: %{attempts: a}}}},
                     30_000

      assert a >= 1
      refute_received {:gate_deadline, :server, ^id, :request_expiration}
      assert wait_until(fn -> dead?(identity) end, 15_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-R2 Server format_status with an ARMED gate timer and pending op: data/timeouts/queue/log rendered closed",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      test = self()
      opts = aw_hold_opts(helper, test, gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      # AR-M15c (U1, R1 of m_1788877342034): the sampled state must be the ARMED timer WITH the pending op of THIS
      # run. The executing Worker's :send trace is installed while it is HELD at :after_go inside ReleaseGate (the
      # existing before-arm hold), i.e. before any AwaitGate request can exist, so its own :pending fact for the
      # SAME cap/gen/ref cannot be emitted untraced; the Server's armed fact, then that traced send, then the sample
      hold = held_at_go!(executing)
      1 = :erlang.trace(executing, true, [:send])
      release_go!(executing, hold)
      assert_receive {:gate_deadline, :server, %{cap: _, gen: _, ref: _} = id, {:armed, _}}, 30_000
      assert_receive {:trace, ^executing, :send, {:gate_deadline, :worker, ^id, :pending}, ^test}, 30_000
      assert_receive {:gate_deadline, :worker, ^id, :pending}, 5_000
      :erlang.trace(executing, false, [:send])
      {:status, ^server, {:module, :gen_statem}, [_pdict, _sys, _parent, _dbg, misc]} = :sys.get_status(server, 5_000)
      owned = aw_owned_timeouts!(server)
      assert match?({1, [_entry]}, owned), "exactly one owned timeout while the Worker is pending: #{inspect(owned)}"
      {1, [entry]} = owned
      refute match?({:state_timeout, _}, entry)
      rendered = inspect(misc, limit: :infinity, printable_limit: :infinity)
      refute rendered =~ run_dir, "no run_dir bytes in the formatted status"
      assert rendered =~ ":redacted", "data rendered :redacted"
      refute rendered =~ "due_ms", "no fence instants in the formatted status"
      File.write!(Path.join(run_dir, "go"), "")
      assert_receive {:gate_deadline, :server, ^id, :cancelled}, 30_000
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-R2c order control (passes today): a Worker :send trace installed at the before-arm hold captures its next real send (GateReleased), pinned by the Server's applied release ref",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), aw_hold_opts(helper, self(), [])))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      hold = held_at_go!(executing)
      # the ReleaseGate request to THIS Worker precedes the hold (the Worker is held inside that effect)
      assert_received {:run_effect_requested, ^server,
                       %{op: :execute, kind: Effect.ReleaseGate, cap: cap, gen: gen, ref: rref, worker: ^executing}}

      1 = :erlang.trace(executing, true, [:send])
      release_go!(executing, hold)

      assert_receive {:trace, ^executing, :send,
                      {:effect_result, ^cap, ^gen, ^rref, ^executing, %Observation.GateReleased{}}, ^server},
                     30_000

      assert_receive {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^rref}}, 30_000
      :erlang.trace(executing, false, [:send])
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AW-R2n late-registration negative (passes today): a Worker :send trace installed only AFTER the Server applied the release cannot recover that send",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), aw_hold_opts(helper, self(), [])))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      hold = held_at_go!(executing)

      assert_received {:run_effect_requested, ^server,
                       %{op: :execute, kind: Effect.ReleaseGate, cap: cap, gen: gen, ref: rref, worker: ^executing}}

      # the OLD order (42256fb): release first, register the trace later (the ReleaseGate effect is an :execute op)
      release_go!(executing, hold)
      assert_receive {:run_effect_applied, ^server, {:execute, ^cap, ^gen, ^rref}}, 30_000
      1 = :erlang.trace(executing, true, [:send])
      refute_receive {:trace, ^executing, :send, {:effect_result, _, _, ^rref, _, _}, _}, 200
      :erlang.trace(executing, false, [:send])
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    # ---- direct gen_statem callback controls (AR-M1/AR-M10: deterministic ordering; the exported callback is RED
    # before its {:timeout, name} clause exists: today it raises FunctionClauseError) ----
    #
    # AR-M13 harness: the callback rows step a GENUINE held Server snapshot, never a hand-built map with nil
    # Loop/Stage. `aw_held_snapshot!/1` drives a REAL run (Server, Worker, guardian, the R-F harness) to the point
    # where the Server has REQUESTED the attempt-1 AwaitGate execute and the Worker holds at the gated command, and
    # copies the Server's live data with `:sys.get_state/1`: real config/siblings, the loop whose RESOLVED opts carry
    # the clock the Server handed the Worker at admission (server.ex:153), the :effect Stage holding the AwaitGate
    # intent and outstanding %{op: :execute, ref}. The real run is then released and awaited (gate_passed) so the
    # copy is stepped by the exported callback in the TEST process with nothing live behind it; the Worker slot of the
    # copy is a monitored probe so the send is witnessed here and the real Worker is never actuated by the copy.
    # The copy's `gate` is the FIXTURE's: `aw_data/3` normalizes it from the same primitives the contract names (the
    # fence armed from the copy's RESOLVED loop.opts clock and the journaled intent deadline, AW-M1), REPLACING
    # whatever the real Server carried at the held point - none (observed at this revision: evidence, not a
    # precondition), `nil`, or a contract-shaped gate on a compliant Server (FC-1 is the control for all three).
    #
    # Probe lifecycle: a probe is spawned WITHOUT a link to its owner and monitors it, so it follows the owner on a
    # :normal exit (a linked probe survives that: the AR-M13 leak) and on any failure. PC-1 witnesses that
    # PASSIVELY (a monitor on the probe taken before the owner dies, the probe's own DOWN, no kill) and keeps a
    # leaky probe as the oracle's negative.
    #
    # Ownership (AR-M13-R3): every parked owner and every probe of the PC rows is a child of ONE custodian
    # (`AwCustodian`), spawn_LINKed from inside it, so creation and ownership are one atomic step - there is no
    # registration after spawn and therefore no window; the custodian traps exits (a child's failure is a message,
    # not a cascade), ends every child through the links when it is killed, and follows its test on the test's
    # DOWN. `aw_custodian!/0` registers the on_exit reclaim BEFORE the custodian creates anything; reclaim never
    # runs inside an assertion path, so the passive oracle stays honest. PC-2 and PC-3 are the controls.
    defmodule AwCustodian do
      @moduledoc false

      def start(test) do
        spawn(fn ->
          Process.flag(:trap_exit, true)
          ref = Process.monitor(test)
          loop(%{test: test, ref: ref, children: []})
        end)
      end

      @doc "Create a child (a zero-arity body) under the custodian's link: `{:ok, pid}` | `:dead` | `:timeout`."
      def create(custodian, body, timeout), do: call(custodian, {:create, body}, timeout)

      @doc "Queue a create without waiting; the reply `{ref, pid}` arrives in the caller's mailbox."
      def cast_create(custodian, body) do
        ref = make_ref()
        send(custodian, {:call, self(), ref, {:create, body}})
        ref
      end

      def children(custodian, timeout), do: call(custodian, :children, timeout)

      defp call(custodian, request, timeout) do
        ref = Process.monitor(custodian)
        send(custodian, {:call, self(), ref, request})

        receive do
          {^ref, reply} ->
            Process.demonitor(ref, [:flush])
            {:ok, reply}

          {:DOWN, ^ref, :process, ^custodian, _} ->
            :dead
        after
          timeout ->
            Process.demonitor(ref, [:flush])
            :timeout
        end
      end

      defp loop(%{test: test, ref: test_ref} = state) do
        receive do
          {:call, from, ref, {:create, body}} ->
            pid = spawn_link(body)
            send(from, {ref, pid})
            loop(%{state | children: state.children ++ [pid]})

          {:call, from, ref, :children} ->
            send(from, {ref, state.children})
            loop(state)

          {:EXIT, _child, _reason} ->
            loop(state)

          {:DOWN, ^test_ref, :process, ^test, _reason} ->
            exit(:test_down)
        end
      end
    end

    defp aw_custodian! do
      custodian = AwCustodian.start(self())
      on_exit(fn -> aw_reclaim!(custodian, 1_000) end)
      custodian
    end

    defp aw_create!(custodian, body) do
      case AwCustodian.create(custodian, body, 1_000) do
        {:ok, pid} -> pid
        other -> flunk("the custodian did not create the child: #{inspect(other)}")
      end
    end

    # the failure-safe reclaim: the children the custodian knows, then kill + join the custodian (the links end every
    # child, known or in flight), then join each known child; idempotent on a dead custodian. A custodian that does
    # not answer is killed all the same (its children end through the links) and reported.
    defp aw_reclaim!(custodian, timeout) do
      answer = AwCustodian.children(custodian, timeout)

      children =
        case answer do
          {:ok, list} -> list
          _dead_or_unanswered -> []
        end

      aw_join_gone!(custodian, timeout)
      for pid <- children, do: aw_join!(pid, timeout)

      if answer == :timeout,
        do: flunk("the custodian did not answer before teardown (its children ended through the links)")

      {:ok, children}
    end

    # ---- scheduled-timer proof (AR-M15b): the statem's OWN Time-outs entry from :sys.get_status. gen_statem
    # computes the count from its real timers before format_status runs; the Server's format_status redacts the
    # entries (names too), so the COUNT is the authority and the entry shape only separates a state_timeout from an
    # owned generic timeout. Absence of a send is never used as proof of a cancelled timer. ----
    defp aw_owned_timeouts!(statem) do
      {:status, ^statem, {:module, :gen_statem}, [_pdict, _sys, _parent, _dbg, misc]} = :sys.get_status(statem, 5_000)

      case aw_find_timeouts(misc) do
        {count, entries} when is_integer(count) and is_list(entries) -> {count, entries}
        other -> flunk("no Time-outs entry in the statem status: #{inspect(other)}")
      end
    end

    # OTP labels the entry with an Erlang string (a charlist)
    defp aw_find_timeouts({~c"Time-outs", {count, entries}}), do: {count, entries}
    defp aw_find_timeouts({"Time-outs", {count, entries}}), do: {count, entries}
    defp aw_find_timeouts(list) when is_list(list), do: Enum.find_value(list, &aw_find_timeouts/1)
    defp aw_find_timeouts({_key, value}), do: aw_find_timeouts(value)
    defp aw_find_timeouts(_other), do: nil

    # a parked owner body: waits for :finish (then exits as told) or its principal's DOWN (the test or a surrogate)
    defp aw_park(principal, exit_reason) do
      fn ->
        ref = Process.monitor(principal)

        receive do
          :finish -> if exit_reason == :normal, do: :ok, else: exit(exit_reason)
          {:DOWN, ^ref, :process, ^principal, _reason} -> exit(:owner_abandoned)
        end
      end
    end

    # owner + probe under the custodian; the owner monitors `principal`, the probe monitors the owner
    defp aw_owner!(custodian, principal, probe_body, exit_reason) do
      owner = aw_create!(custodian, aw_park(principal, exit_reason))
      oref = Process.monitor(owner)
      probe = aw_create!(custodian, probe_body.(owner))
      {owner, oref, probe}
    end

    # the test-owned probe in the Worker slot of the callback copies: it monitors the test, so it follows the test;
    # kill + join in on_exit is failure-safe cleanup
    defp aw_probe_worker do
      pid = spawn(aw_probe_body(self()))
      on_exit(fn -> aw_join_gone!(pid, 1_000) end)
      pid
    end

    defp aw_probe_body(owner) do
      fn ->
        owner_ref = Process.monitor(owner)
        aw_probe_loop(owner, owner_ref)
      end
    end

    defp aw_probe_loop(owner, owner_ref) do
      receive do
        {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
          exit(:normal)

        msg ->
          send(owner, {:worker_received, msg})
          aw_probe_loop(owner, owner_ref)
      end
    end

    # the review's mutant (AR-M13-R2) kept in-suite as the oracle's NEGATIVE: the owner-DOWN branch loops
    defp aw_leaky_body(owner) do
      fn ->
        owner_ref = Process.monitor(owner)
        aw_leaky_loop(owner, owner_ref)
      end
    end

    defp aw_leaky_loop(owner, owner_ref) do
      receive do
        {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
          aw_leaky_loop(owner, owner_ref)

        msg ->
          send(owner, {:worker_received, msg})
          aw_leaky_loop(owner, owner_ref)
      end
    end

    # kill is idempotent on a dead pid; the join is the evidence (a monitor on a dead pid answers :noproc at once)
    defp aw_join_gone!(pid, timeout) do
      Process.exit(pid, :kill)
      aw_join!(pid, timeout)
    end

    # join only: no kill (a link or an owner's DOWN must already have ended the process)
    defp aw_join!(pid, timeout) do
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        timeout -> flunk("process #{inspect(pid)} survived")
      end
    end

    # the passive oracle: the process's OWN DOWN through a monitor taken BEFORE its owner dies; nothing is killed
    defp aw_probe_fate(probe_ref, probe, timeout) do
      receive do
        {:DOWN, ^probe_ref, :process, ^probe, _} -> :gone
      after
        timeout -> :survived
      end
    end

    defp aw_held_snapshot!(helper) do
      require_run!()
      run_dir = tmp_run_dir()
      test = self()
      go = Path.join(run_dir, "go")
      # a failed row must not leave the gated command holding for the journaled deadline: release it before teardown
      on_exit(fn -> File.write(go, "") end)

      observer = fn effect, observation -> send(test, {:effect_observed, effect, observation}) end
      opts = aw_opts(helper, self(), effect_observer: observer)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)

      assert_receive {:run_effect_requested, ^server,
                      %{op: :execute, kind: Effect.AwaitGate, cap: cap, gen: gen, ref: ref, worker: ^executing}},
                     30_000

      {:driving, snapshot} = :sys.get_state(server)

      # validity of the copy, independent of any timer behaviour: the real lifecycle data at the held point
      assert %{
               loop: %{opts: opts, step: {:effect, %Effect.AwaitGate{} = intent, _state, _appended}},
               stage: %{kind: :effect, intent: intent, loop: %{}},
               outstanding: %{op: :execute, ref: ^ref},
               cap: ^cap,
               gen: ^gen,
               worker: %{pid: ^executing, phase: :admitted},
               siblings: %{}
             } = snapshot

      assert Keyword.fetch!(opts, :clock) == SeamClock, "the RESOLVED loop.opts clock is the configured seam clock"
      assert is_integer(intent.deadline_unix) and intent.deadline_unix > SeamClock.unix_now()

      File.write!(go, "")
      assert {:ok, _} = run_server().await(server, 30_000)
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      stop_owned!(owner)
      snapshot
    end

    # the copy with a monitored probe in the Worker slot and the FIXTURE's gate map (AW-M1: fence from the RESOLVED
    # loop.opts clock and the journaled deadline; D-8 cap from the same opts, default 60_000), replacing any :gate the
    # held Server carried (absent / nil / contract-shaped: FC-1)
    defp aw_data(%{loop: %{opts: opts}, stage: %{intent: intent}} = snapshot, worker, extra) do
      %{cap: cap, gen: gen, outstanding: %{op: :execute, ref: ref}} = snapshot
      clock = Keyword.fetch!(opts, :clock)
      cap_ms = Keyword.get(opts, :gate_deadline_cap_ms, 60_000)
      identity = %{cap: cap, gen: gen, ref: ref}
      {:ok, fence} = DeadlineFence.arm(identity, intent.deadline_unix, clock.unix_now(), clock.monotonic_ms(), cap_ms)
      {:ok, outstanding} = DeadlineFence.outstanding(identity)

      snapshot
      |> Map.put(:worker, %{pid: worker, monitor: Process.monitor(worker), phase: :admitted})
      |> Map.put(:gate, %{identity: identity, kind: Effect.AwaitGate, fence: fence, outstanding: outstanding})
      |> Map.merge(extra)
    end

    defp aw_identity(%{cap: cap, gen: gen, outstanding: %{ref: ref}}), do: {cap, gen, ref}

    defp aw_timeout_event(ref, cap, gen),
      do: {{:timeout, {:gate_deadline, ref}}, {:gate_chunk, %{cap: cap, gen: gen, ref: ref}}}

    @tag :aw_server
    test "TC-1 genuine generic-timeout event, correlated and due: exactly one {:gate_deadline, cap, gen, ref} send",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      # due at the first dequeue: the RESOLVED clock (SeamClock) now reads the journaled deadline; arm samples it
      Seam.put(:now, snapshot.stage.intent.deadline_unix)
      data = aw_data(snapshot, worker, %{})
      assert :due == DeadlineFence.next(data.gate.fence, SeamClock.monotonic_ms())
      {{:timeout, name}, content} = aw_timeout_event(ref, cap, gen)
      assert {:keep_state, data2} = run_server().handle_event({:timeout, name}, content, :driving, data)
      assert_receive {:worker_received, {:gate_deadline, ^cap, ^gen, ^ref}}, 1_000
      assert data2.gate.outstanding.state == :timeout_selected
      assert {:keep_state, data3} = run_server().handle_event({:timeout, name}, content, :driving, data2)
      refute_receive {:worker_received, _}, 200
      assert data3.drops == data2.drops + 1, "duplicate due is a counted fact"
    end

    @tag :aw_server
    test "TC-2 stale generic-timeout events: wrong cap / gen / ref / op / kind, and every non-driving state: counted, no send",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      base = aw_data(snapshot, worker, %{})

      variants = [
        {base, aw_timeout_event(ref, make_ref(), gen)},
        {base, aw_timeout_event(ref, cap, gen + 1)},
        {base, aw_timeout_event(make_ref(), cap, gen)},
        {%{base | outstanding: %{op: :settle, ref: ref}}, aw_timeout_event(ref, cap, gen)},
        {%{base | gate: %{base.gate | kind: Effect.Observe}}, aw_timeout_event(ref, cap, gen)},
        {%{base | gate: nil}, aw_timeout_event(ref, cap, gen)}
      ]

      for {data, {{:timeout, name}, content}} <- variants do
        assert {:keep_state, data2} = run_server().handle_event({:timeout, name}, content, :driving, data)
        assert data2.drops == data.drops + 1
      end

      for state <- [:opening, :finished, :failed] do
        {{:timeout, name}, content} = aw_timeout_event(ref, cap, gen)
        assert {:keep_state, data2} = run_server().handle_event({:timeout, name}, content, state, base)
        assert data2.drops == base.drops + 1
      end

      refute_receive {:worker_received, _}, 200
    end

    @tag :aw_server
    test "TC-3 owner DOWN already observable takes priority over a due generic timeout: :failed, timer cancelled, no send",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      Seam.put(:now, snapshot.stage.intent.deadline_unix)
      data = aw_data(snapshot, worker, %{})
      send(self(), {:DOWN, data.worker.monitor, :process, worker, :killed})
      {{:timeout, name}, content} = aw_timeout_event(ref, cap, gen)

      assert {:next_state, :failed, data2, actions} =
               run_server().handle_event({:timeout, name}, content, :driving, data)

      assert {:error, %{clause: "run_effect_owner_down"}} = data2.result
      assert {{:timeout, {:gate_deadline, ref}}, :cancel} in actions
      assert data2.gate == nil
      refute_receive {:worker_received, _}, 200
    end

    @tag :aw_server
    test "TC-4 the correlated effect result cancels the gate timer; a chunk queued after the cancel is a counted drop",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      data = aw_data(snapshot, worker, %{})
      intent = snapshot.stage.intent
      # an admissible, CORRELATED observation for the held AwaitGate (the reducer answers a foreign gate_run_id with
      # observation_mismatch); GateError is the closed-error leg, consumed through ordinary result handling
      observation = %Observation.GateError{
        gate_run_id: intent.gate_run_id,
        reason: %{"clause" => "probe"},
        now: 1_700_000_000
      }

      result = {:effect_result, cap, gen, ref, worker, observation}
      reply = run_server().handle_event(:info, result, :driving, data)
      {data2, actions} = aw_reply_parts(reply)
      # fixture validity, independent of the missing timer: the result went through Host.resume_step (observer
      # notified with THIS intent and observation, the reducer consumed it, the stage is closed, the next step queued)
      assert_receive {:effect_observed, ^intent, ^observation}, 1_000
      assert data2.stage == nil and data2.outstanding == nil
      assert data2.loop.step != snapshot.loop.step, "the reducer stepped past the pending AwaitGate"
      refute match?({:error, %{"reason" => "observation_mismatch"}}, data2.loop.step)
      refute match?({:error, %{"reason" => "no_pending_effect"}}, data2.loop.step)
      assert {:next_event, :internal, :step} in actions
      # RED from here: no Server-owned gate timer exists today
      assert {{:timeout, {:gate_deadline, ref}}, :cancel} in actions
      assert data2.gate == nil
      {{:timeout, name}, content} = aw_timeout_event(ref, cap, gen)
      assert {:keep_state, data3} = run_server().handle_event({:timeout, name}, content, :driving, data2)
      assert data3.drops == data2.drops + 1
      refute_receive {:worker_received, {:gate_deadline, _, _, _}}, 200
    end

    defp aw_reply_parts({:keep_state, data, actions}), do: {data, actions}
    defp aw_reply_parts({:next_state, _state, data, actions}), do: {data, actions}
    defp aw_reply_parts({:keep_state, data}), do: {data, []}
    defp aw_reply_parts(other), do: flunk("unexpected callback reply #{inspect(other, limit: 20)}")

    @tag :aw_server
    test "PC-1 probe lifecycle control (AR-M13-R2): passive DOWN witness on :normal and failure owner exits; kill/join is cleanup only; a leaky probe is exposed" do
      custodian = aw_custodian!()
      # :normal owner exit - the leak a linked probe has (a link ignores :normal); the probe's DOWN is the witness
      {owner1, oref1, probe1} = aw_owner!(custodian, self(), &aw_probe_body/1, :normal)
      pref1 = Process.monitor(probe1)
      send(owner1, :finish)
      assert_receive {:DOWN, ^oref1, :process, ^owner1, :normal}, 1_000
      assert :gone == aw_probe_fate(pref1, probe1, 1_000)
      # failure owner exit - the fixture builder's owner fails after spawning
      {owner2, oref2, probe2} = aw_owner!(custodian, self(), &aw_probe_body/1, :fixture_failure)
      pref2 = Process.monitor(probe2)
      send(owner2, :finish)
      assert_receive {:DOWN, ^oref2, :process, ^owner2, :fixture_failure}, 1_000
      assert :gone == aw_probe_fate(pref2, probe2, 1_000)
      # the cleanup path on a LIVE owner: kill + join removes the probe, the owner is untouched
      {owner3, oref3, probe3} = aw_owner!(custodian, self(), &aw_probe_body/1, :normal)
      assert :ok == aw_join_gone!(probe3, 1_000)
      assert Process.alive?(owner3)
      send(owner3, :finish)
      assert_receive {:DOWN, ^oref3, :process, ^owner3, :normal}, 1_000
      # the test-owned probe forwards while its owner (this test) lives; on_exit joins it
      probe4 = aw_probe_worker()
      send(probe4, :ping)
      assert_receive {:worker_received, :ping}, 1_000
      assert Process.alive?(probe4)
      # NEGATIVE: the same passive oracle exposes a probe whose owner-DOWN branch loops (the review's mutant)
      {owner5, oref5, probe5} = aw_owner!(custodian, self(), &aw_leaky_body/1, :normal)
      pref5 = Process.monitor(probe5)
      send(owner5, :finish)
      assert_receive {:DOWN, ^oref5, :process, ^owner5, :normal}, 1_000

      assert :survived == aw_probe_fate(pref5, probe5, 200),
             "the oracle must expose a probe that ignores its owner's DOWN"

      assert :ok == aw_join_gone!(probe5, 1_000)
    end

    @tag :aw_server
    test "PC-2 failure-before-:finish control (AR-M13-R3): a surrogate test failing before :finish takes its parked owners and compliant probe down; the reclaim joins the leaky probe" do
      test = self()
      # the surrogate is a custodian child; its actors live under a second custodian whose in-test reclaim is
      # the witness (its on_exit reclaim is then idempotent); a failure anywhere here leaves nothing behind
      c_surrogate = aw_custodian!()
      c_actors = aw_custodian!()

      surrogate =
        aw_create!(c_surrogate, fn ->
          {owner, _, probe} = aw_owner!(c_actors, self(), &aw_probe_body/1, :normal)
          {leaky_owner, _, leaky} = aw_owner!(c_actors, self(), &aw_leaky_body/1, :normal)
          send(test, {:acquired, owner, probe, leaky_owner, leaky})
          receive(do: (:fail -> exit(:forced_assertion_failure)))
        end)

      sref = Process.monitor(surrogate)
      assert_receive {:acquired, owner, probe, leaky_owner, leaky}, 1_000
      assert {:ok, [^owner, ^probe, ^leaky_owner, ^leaky]} = AwCustodian.children(c_actors, 1_000)
      refs = Map.new([owner, probe, leaky_owner, leaky], &{&1, Process.monitor(&1)})
      send(surrogate, :fail)
      assert_receive {:DOWN, ^sref, :process, ^surrogate, :forced_assertion_failure}, 1_000
      # passive: both owners follow the failed surrogate and the compliant probe follows its owner; nothing killed
      assert :gone == aw_probe_fate(refs[owner], owner, 1_000)
      assert :gone == aw_probe_fate(refs[leaky_owner], leaky_owner, 1_000)
      assert :gone == aw_probe_fate(refs[probe], probe, 1_000)
      assert :survived == aw_probe_fate(refs[leaky], leaky, 200), "the leaky probe is what the reclaim exists for"
      # the reclaim an on_exit runs: kill + join the custodian (the link ends the leaky probe), join every child
      assert {:ok, [^owner, ^probe, ^leaky_owner, ^leaky]} = aw_reclaim!(c_actors, 1_000)
      assert :gone == aw_probe_fate(refs[leaky], leaky, 1_000)
    end

    @tag :aw_server
    test "PC-3 ownership control (AR-M13-R3-b): a create in flight at teardown is reclaimed; killing the custodian ends owner, probe and leaky probe through the links" do
      # (a) creation in flight: the create is queued from this process BEFORE the reclaim's children query, so the
      # custodian serves it first; the reply pid is one of the children the reclaim joins
      c1 = aw_custodian!()
      ref = AwCustodian.cast_create(c1, aw_leaky_body(self()))
      assert {:ok, [pid]} = aw_reclaim!(c1, 1_000)
      assert_receive {^ref, ^pid}, 1_000
      assert :ok == aw_join!(pid, 1_000)
      # (b) the link, with no per-child kill: ending the custodian ends every child, even the leaky probe
      c2 = aw_custodian!()
      {owner, oref, probe} = aw_owner!(c2, self(), &aw_probe_body/1, :normal)
      {leaky_owner, loref, leaky} = aw_owner!(c2, self(), &aw_leaky_body/1, :normal)
      pref = Process.monitor(probe)
      lref = Process.monitor(leaky)
      cref = Process.monitor(c2)
      Process.exit(c2, :kill)
      assert_receive {:DOWN, ^cref, :process, ^c2, :killed}, 1_000
      assert :gone == aw_probe_fate(oref, owner, 1_000)
      assert :gone == aw_probe_fate(loref, leaky_owner, 1_000)
      assert :gone == aw_probe_fate(pref, probe, 1_000)
      assert :gone == aw_probe_fate(lref, leaky, 1_000)
    end

    @tag :aw_server
    test "FC-1 fixture control (AR-M13-R1): a held snapshot without :gate, with gate: nil or a contract-shaped gate is accepted; the fixture's gate replaces it",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      identity = %{cap: cap, gen: gen, ref: ref}
      deadline = snapshot.stage.intent.deadline_unix
      {:ok, fence} = DeadlineFence.arm(identity, deadline, SeamClock.unix_now(), SeamClock.monotonic_ms(), 60_000)
      {:ok, outstanding} = DeadlineFence.outstanding(identity)
      # a compliant Server's gate at the held point (AW-M1 shape), foreign to the fixture's own arm samples
      shaped = %{
        identity: identity,
        kind: Effect.AwaitGate,
        fence: %{fence | due_ms: fence.due_ms + 1},
        outstanding: outstanding
      }

      worker = aw_probe_worker()

      for variant <- [Map.delete(snapshot, :gate), Map.put(snapshot, :gate, nil), Map.put(snapshot, :gate, shaped)] do
        data = aw_data(variant, worker, %{})

        assert %{
                 identity: ^identity,
                 kind: Effect.AwaitGate,
                 fence: %{identity: ^identity},
                 outstanding: %{identity: ^identity, state: :running}
               } =
                 data.gate

        assert Map.drop(data, [:gate, :worker]) == Map.drop(variant, [:gate, :worker])
        assert %{pid: ^worker, phase: :admitted} = data.worker
      end
    end

    @tag :aw_server
    test "TC-6 effect_failed correlation control on the held copy (AR-M15b): stale dropped with gate unchanged; correlated = callback exit only; non-closed re-diagnosed",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      data = aw_data(snapshot, worker, %{})

      closed = %{
        kind: :error,
        class: "map",
        digest: "sha256:" <> String.duplicate("0", 64),
        frames: 1,
        cleanup: %{attempts: 1, settled: 1, unproven: 0}
      }

      # NEGATIVES: every uncorrelated effect_failed is a counted drop; the outstanding gate and op stay as they were
      for {c, g, r, from} <- [
            {make_ref(), gen, ref, worker},
            {cap, gen + 1, ref, worker},
            {cap, gen, make_ref(), worker},
            {cap, gen, ref, self()}
          ] do
        assert {:keep_state, data2} =
                 run_server().handle_event(:info, {:effect_failed, c, g, r, from, closed}, :driving, data)

        assert data2.drops == data.drops + 1
        assert data2.gate == data.gate and data2.outstanding == data.outstanding
      end

      refute_receive {:worker_received, _}, 200
      # correlated: the callback applies the op and ENDS the process with the Worker's closed diagnostic unchanged;
      # the owner settled in-invocation (no Server settle). A bare callback exit here is a callback-exit/correlation
      # control ONLY: the disposal of a gen_statem-OWNED timer is witnessed by EF-1 on the real process lifecycle.
      failed = {:effect_failed, cap, gen, ref, worker, closed}
      assert {:run_step_failed, ^closed} = catch_exit(run_server().handle_event(:info, failed, :driving, data))
      assert_received {:run_effect_applied, _, {:execute, ^cap, ^gen, ^ref}}
      refute_received {:worker_received, {:settle, _, _, _}}
      # a diagnostic outside the closed domain is never passed through
      bogus = {:effect_failed, cap, gen, ref, worker, %{bogus: true}}

      assert {:run_step_failed, %{kind: :exit, class: class, digest: digest, frames: 0}} =
               catch_exit(run_server().handle_event(:info, bogus, :driving, data))

      assert is_binary(class) and digest =~ ~r/\Asha256:[0-9a-f]{64}\z/
    end

    @tag :aw_server
    test "TC-6d effect_failed with the owner's DOWN already observable (AR-M15b, RED): :failed, timer cancelled, gate cleared, nothing sent",
         %{helper: helper} do
      snapshot = aw_held_snapshot!(helper)
      {cap, gen, ref} = aw_identity(snapshot)
      worker = aw_probe_worker()
      data = aw_data(snapshot, worker, %{})

      closed = %{
        kind: :error,
        class: "map",
        digest: "sha256:" <> String.duplicate("0", 64),
        frames: 1,
        cleanup: %{attempts: 1, settled: 1, unproven: 0}
      }

      send(self(), {:DOWN, data.worker.monitor, :process, worker, :killed})

      assert {:next_state, :failed, data2, actions} =
               run_server().handle_event(:info, {:effect_failed, cap, gen, ref, worker, closed}, :driving, data)

      assert {:error, %{clause: "run_effect_owner_down"}} = data2.result
      assert {{:timeout, {:gate_deadline, ref}}, :cancel} in actions
      assert data2.gate == nil
      refute_receive {:worker_received, _}, 200
    end

    @tag :aw_server
    test "TP-1 timer-proof control (passes today): the Time-outs parser reads a real owned generic timeout and its cancellation" do
      {:ok, probe} = :gen_statem.start(TimerProbeStatem, :ok, [])
      on_exit(fn -> if Process.alive?(probe), do: :gen_statem.stop(probe) end)
      assert {0, []} = aw_owned_timeouts!(probe)
      :ok = :gen_statem.call(probe, {:arm, {:gate_deadline, make_ref()}})
      assert {1, [{{:timeout, {:gate_deadline, _}}, :probe_content}]} = aw_owned_timeouts!(probe)
      :ok = :gen_statem.call(probe, {:arm, {:gate_deadline, ref2 = make_ref()}})
      assert {2, _} = aw_owned_timeouts!(probe)
      :ok = :gen_statem.call(probe, {:cancel, {:gate_deadline, ref2}})
      assert {1, [{{:timeout, {:gate_deadline, other}}, :probe_content}]} = aw_owned_timeouts!(probe)
      assert other != ref2
      :gen_statem.stop(probe)
    end

    @tag :aw_server
    test "T-4s exact arm samples: between the release result and the {:armed,_} fact the Server reads unix once and mono once",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      # short cap kept: chunk reads follow the Server's own :armed send in its trace stream and must not count
      opts = aw_hold_opts(helper, self(), gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      # AR-M14: the Server's clock-call + send trace stream starts while the Worker is HELD inside ReleaseGate
      hold = held_at_go!(executing)
      aw_trace_clock!(server)
      release_go!(executing, hold)
      assert_receive {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate} = req}, 30_000
      # the immutable snapshot: reads strictly before the Server's own correlated {:armed, _} send
      marker = fn
        {:gate_deadline, :server, %{cap: _, gen: _, ref: _} = id, {:armed, wait}} -> {:ok, {id, wait}}
        _other -> :skip
      end

      {counts, {id, wait}} = aw_reads_before_marker!(server, marker, 30_000)
      assert id == %{cap: req.cap, gen: req.gen, ref: req.ref}
      assert is_integer(wait) and wait > 0 and wait <= 50
      assert counts == %{unix: 1, mono: 1}
      :erlang.trace(server, false, [:call, :send])
      File.write!(Path.join(run_dir, "go"), "")
      assert_receive {:gate_deadline, :server, ^id, :cancelled}, 30_000
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "RC-1 real retry control (AR-M14-R1): the hold is scoped to attempt 1; the rework attempt's :after_go is plain and the run completes",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_hold_opts(helper, self(), effect_observer: aw_jump_on(Effect.ReleaseGate, 10_000))
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_argv_rework(), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      hold = held_at_go!(executing)
      release_go!(executing, hold)
      # today's in-await enforcement times the measured attempt out at the jumped wall; the reducer reworks into a
      # NEW gate run whose :after_go is the plain notification (its command exits at once: liveness may read :gone)
      {_identity2, _} = ready!(owner, executing)
      assert_receive {:after_go_later, ^executing, _live2, key2}, 30_000
      assert key2 != hold.key
      assert {:ok, _} = run_server().await(server, 30_000)
      refute_received {:after_go_hold, ^executing, _, _, _}

      started =
        for e <- journal(run_dir), e["type"] == "gate_started", do: {e["data"]["gate_run_id"], e["data"]["attempt"]}

      assert started == [hold.key, key2]
      stop_owned!(owner)
    end

    @tag :aw_server
    test "NC-1 witness-order control (AR-M14): a :send trace installed at the before-arm hold captures the Server's next release_terminal send, pinned by the applied ref",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), aw_hold_opts(helper, self(), [])))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      hold = held_at_go!(executing)
      :erlang.trace(server, true, [:send])
      release_go!(executing, hold)
      # the first release_terminal after the hold belongs to the AwaitGate stage; its ref is confirmed by the Server's
      # own applied trace and the AwaitGate execute request follows it
      assert_receive {:trace, ^server, :send, {:release_terminal, cap, gen, ref, _suffix}, ^executing}, 30_000
      assert_receive {:run_effect_applied, ^server, {:release, ^cap, ^gen, ^ref}}, 30_000

      assert_receive {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate, cap: ^cap, gen: ^gen}},
                     30_000

      :erlang.trace(server, false, [:send])
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "NC-1n witness-order NEGATIVE (AR-M14): the old late registration (trace after the notification that follows the send) provably misses that send",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), aw_hold_opts(helper, self(), [])))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      hold = held_at_go!(executing)
      release_go!(executing, hold)
      # old order: register only after a notification that the send has already happened
      assert_receive {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate} = req}, 30_000
      %{cap: cap, gen: gen} = req
      :erlang.trace(server, true, [:send])
      refute_receive {:trace, ^server, :send, {:release_terminal, ^cap, ^gen, _, _}, ^executing}, 200
      :erlang.trace(server, false, [:send])
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "NC-2 snapshot-boundary control (AR-M14-R2): a later read forced before sampling makes the live counter say 2 vs 1; the marker-cut trace snapshot says 1/1" do
      custodian = aw_custodian!()
      test = self()

      # the measured action (one unix + one mono), the actor's own marker send, then the review's post-fact read
      reader = fn ->
        receive do
          :measure ->
            AwCountingClock.unix_now()
            AwCountingClock.monotonic_ms()
            send(test, {:measured, self()})
            AwCountingClock.monotonic_ms()
            send(test, {:later_read_done, self()})
            receive(do: (:finish -> :ok))
        end
      end

      actor = aw_create!(custodian, reader)
      aw_trace_clock!(actor)
      before_unix = Seam.get({:clock_read, :unix, actor}) || 0
      before_mono = Seam.get({:clock_read, :mono, actor}) || 0
      send(actor, :measure)
      assert_receive {:measured, ^actor}, 1_000
      # known order: the later read is DONE before the receiver samples anything
      assert_receive {:later_read_done, ^actor}, 1_000
      # NEGATIVE: the old live-counter path cannot tell the arm sample from the later read
      assert (Seam.get({:clock_read, :unix, actor}) || 0) - before_unix == 1
      assert (Seam.get({:clock_read, :mono, actor}) || 0) - before_mono == 2
      # the snapshot: the actor's ordered trace stream cut at its own marker send
      marker = fn
        {:measured, _} -> {:ok, :measured}
        _other -> :skip
      end

      assert {%{unix: 1, mono: 1}, :measured} = aw_reads_before_marker!(actor, marker, 1_000)
      # old order (baseline after the action): the reads are invisible
      actor2 = aw_create!(custodian, reader)
      send(actor2, :measure)
      assert_receive {:measured, ^actor2}, 1_000
      assert_receive {:later_read_done, ^actor2}, 1_000
      late_unix = Seam.get({:clock_read, :unix, actor2}) || 0
      late_mono = Seam.get({:clock_read, :mono, actor2}) || 0
      assert late_unix == 1 and late_mono == 2, "the reads happened"
      assert (Seam.get({:clock_read, :unix, actor2}) || 0) - late_unix == 0
      assert (Seam.get({:clock_read, :mono, actor2}) || 0) - late_mono == 0
      send(actor, :finish)
      send(actor2, :finish)
    end

    # ---- AR-M15a: initial Server arm failures / refusals (AW-M1 "Arm / clock errors", D-8). Every row holds the
    # gate command (no `go`), so once the hold is released the Server's only remaining work is the release result,
    # the AwaitGate commit and its request/arm: a closed exit in that window is the arm phase. No row requires the
    # run_effect_requested notification to precede the exit (AW-M1 fixes no such order inside request/3,
    # m_1788849293816). The witnesses that separate a Server arm failure from the pre-existing Worker fault (T-4 /
    # AF-N1): the fault clock records the exact pid and sample it raised in; the Server settles the owner ITSELF
    # through the protocol under closed/2 (its {:settle, ...} applied trace, cleanup from the reply: integer
    # counts, never the unknown shape); the Worker's effect_failed path has no Server settle. ----
    defp af_run!(helper, extra) do
      require_run!()
      run_dir = tmp_run_dir()
      on_exit(fn -> File.write(Path.join(run_dir, "go"), "") end)
      opts = aw_hold_opts(helper, self(), extra)
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      mref = Process.monitor(server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      %{run_dir: run_dir, owner: owner, server: server, mref: mref, executing: executing, identity: identity}
    end

    # the settle reply is the SOLE count authority (AW-M1): for this fixture - ONE retained gate, held, settled by
    # the Server's own settle_before_exit - the reply's cleanup is exactly one attempt, one settled, nothing
    # unproven (observed on the real Server-stage closed path, AF-C1). A pure verdict, so the oracle's negatives
    # can be exercised today (AF-C2): the @unknown_cleanup shape (:unknown atoms), a missing/non-map cleanup, any
    # other count, extra or missing keys, negative or non-integer values are all refused.
    @af_expected_cleanup %{attempts: 1, settled: 1, unproven: 0}

    defp af_cleanup_verdict(%{attempts: 1, settled: 1, unproven: 0} = cleanup) when map_size(cleanup) == 3, do: :ok
    defp af_cleanup_verdict(%{attempts: :unknown} = cleanup), do: {:error, {:unknown_cleanup, cleanup}}

    defp af_cleanup_verdict(cleanup) when is_map(cleanup),
      do: {:error, {:cleanup_mismatch, cleanup, @af_expected_cleanup}}

    defp af_cleanup_verdict(other), do: {:error, {:cleanup_malformed, other}}

    # the closed arm failure: exact Server DOWN with the closed shape carrying a cleanup, the verdict FIRST, then the
    # Server's own settle applied (the arm path settles the owner itself; the Worker's effect_failed path does not),
    # no :armed fact, guardian gone. It CONSUMES the settle applied trace and RETURNS the evidence (the closed
    # diagnostic and the applied settle triple), so a caller correlates it ONCE instead of re-asserting a consumed
    # message (AR-M15b R1).
    defp af_closed_arm_failure!(%{server: server, mref: mref, identity: identity}) do
      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, %{kind: kind} = diagnostic}}, 30_000
      assert kind in [:error, :exit, :throw]
      assert :ok == af_cleanup_verdict(Map.get(diagnostic, :cleanup)), "cleanup verdict: #{inspect(diagnostic)}"
      assert_received {:run_effect_applied, ^server, {:settle, cap, gen, ref}}
      refute_received {:gate_deadline, :server, _, {:armed, _}}
      assert wait_until(fn -> dead?(identity) end, 15_000)
      %{diagnostic: diagnostic, settle: {cap, gen, ref}}
    end

    # a REAL existing Server-stage closed/2 failure on the same fixture (the observer raises on the ReleaseGate
    # result, after the hold), the positive the oracle is calibrated on: cleanup exactly 1/1/0
    defp af_real_closed_failure!(helper) do
      observer = fn
        %Effect.ReleaseGate{}, _ -> raise("Server-stage failure exercising the existing closed/2 boundary")
        _effect, _observation -> :ok
      end

      %{executing: executing} = run = af_run!(helper, effect_observer: observer)
      hold = held_at_go!(executing)
      release_go!(executing, hold)
      run
    end

    @tag :aw_server
    test "AF-C1 positive control (passes today): a real Server-stage closed/2 failure settles the held gate with cleanup EXACTLY 1/1/0 and the oracle accepts it",
         %{helper: helper} do
      %{server: server, mref: mref, owner: owner} = run = af_real_closed_failure!(helper)
      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, %{cleanup: cleanup} = diagnostic}}, 30_000
      assert cleanup == @af_expected_cleanup
      # requeue the REAL, unmodified DOWN for the full oracle
      send(self(), {:DOWN, mref, :process, server, {:run_step_failed, diagnostic}})
      af_closed_arm_failure!(run)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-C2 negative controls (pass today): the verdict refuses unproven/unknown/malformed cleanups, and the full oracle FAILS on a modified real diagnostic",
         %{helper: helper} do
      # (a) the pure verdict
      assert :ok == af_cleanup_verdict(%{attempts: 1, settled: 1, unproven: 0})

      for bad <- [
            %{attempts: 1, settled: 1, unproven: 1},
            %{attempts: 1, settled: 1, unproven: -1},
            %{attempts: :unknown, settled: 0, unproven: :unknown},
            %{attempts: 1, settled: 0, unproven: 0},
            %{attempts: 2, settled: 1, unproven: 0},
            %{attempts: 1.0, settled: 1, unproven: 0},
            %{attempts: 1, settled: 1},
            %{attempts: 1, settled: 1, unproven: 0, extra: true},
            nil,
            [attempts: 1, settled: 1, unproven: 0]
          ] do
        assert match?({:error, _}, af_cleanup_verdict(bad)), "must refuse #{inspect(bad)}"
      end

      # (b) the full oracle on a REAL diagnostic whose cleanup is modified in the test (never in production)
      %{server: server, mref: mref, owner: owner} = run = af_real_closed_failure!(helper)
      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, %{cleanup: cleanup} = diagnostic}}, 30_000
      assert cleanup == @af_expected_cleanup

      for modified <- [%{cleanup | unproven: 1}, %{attempts: :unknown, settled: 0, unproven: :unknown}] do
        send(self(), {:DOWN, mref, :process, server, {:run_step_failed, %{diagnostic | cleanup: modified}}})
        error = assert_raise(ExUnit.AssertionError, fn -> af_closed_arm_failure!(run) end)
        assert error.message =~ "cleanup verdict"
      end

      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-1 initial WALL sample at arm fails (Server pid only): closed run_step_failed, owner settled by the Server, no arm fact",
         %{helper: helper} do
      Seam.put(:pid_fault, nil)
      %{server: server, executing: executing, owner: owner} = run = af_run!(helper, clock: AwPidFaultClock)
      hold = held_at_go!(executing)
      # enabled at the hold: the first read the Server makes after it is the arm's wall sample (pre-arm reads: 0/0)
      Seam.put(:pid_fault, {:unix, server})
      release_go!(executing, hold)
      af_closed_arm_failure!(run)
      assert Seam.get(:pid_fault_hit) == {:unix, server}, "the wall sample raised in the exact Server"
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-2 initial MONOTONIC sample at arm fails (Server pid only): closed run_step_failed, owner settled by the Server, no arm fact",
         %{helper: helper} do
      Seam.put(:pid_fault, nil)
      %{server: server, executing: executing, owner: owner} = run = af_run!(helper, clock: AwPidFaultClock)
      hold = held_at_go!(executing)
      Seam.put(:pid_fault, {:mono, server})
      release_go!(executing, hold)
      af_closed_arm_failure!(run)
      assert Seam.get(:pid_fault_hit) == {:mono, server}, "the monotonic sample raised in the exact Server"
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-3a live invalid cap (resolved gate_deadline_cap_ms 0): closed failure under the boundary (route not demonstrated live: D-8 check or fence wait_cap_invalid)",
         %{helper: helper} do
      %{executing: executing, owner: owner} = run = af_run!(helper, gate_deadline_cap_ms: 0)
      hold = held_at_go!(executing)
      release_go!(executing, hold)
      af_closed_arm_failure!(run)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-3b D-8 precondition (not a fence refusal): a test-only cap ABOVE the default (120_000) is refused under the closed boundary",
         %{helper: helper} do
      %{executing: executing, owner: owner} = run = af_run!(helper, gate_deadline_cap_ms: 120_000)
      hold = held_at_go!(executing)
      release_go!(executing, hold)
      af_closed_arm_failure!(run)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-3c control: the PRIMITIVE typed refusal (DeadlineFence.arm/5 wait_cap_invalid for 0 and a non-integer; 120_000 is accepted by the fence)" do
      identity = %{cap: make_ref(), gen: 1, ref: make_ref()}
      assert {:error, %{clause: "wait_cap_invalid"}} = DeadlineFence.arm(identity, 1_700_000_600, 1_700_000_000, 0, 0)
      assert {:error, %{clause: "wait_cap_invalid"}} = DeadlineFence.arm(identity, 1_700_000_600, 1_700_000_000, 0, 1.5)
      assert {:ok, %{wait_cap_ms: 120_000}} = DeadlineFence.arm(identity, 1_700_000_600, 1_700_000_000, 0, 120_000)
    end

    @tag :aw_server
    test "AF-N1 wrong-process negative (passes today): the same fault keyed to the WORKER pid is the pre-existing Worker fault: no AwaitGate request, no Server settle",
         %{helper: helper} do
      Seam.put(:pid_fault, nil)

      %{server: server, mref: mref, executing: executing, identity: identity, owner: owner} =
        af_run!(helper, clock: AwPidFaultClock)

      hold = held_at_go!(executing)
      # the Worker's next read after :after_go is its release monotonic sample: the effect fails in the Worker
      Seam.put(:pid_fault, {:mono, executing})
      release_go!(executing, hold)
      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, %{kind: :error, cleanup: cleanup}}}, 30_000
      refute_received {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate}}
      refute_received {:run_effect_applied, ^server, {:settle, _, _, _}}
      assert Seam.get(:pid_fault_hit) == {:mono, executing}, "the fault raised in the Worker, not the Server"
      assert is_map(cleanup)
      assert wait_until(fn -> dead?(identity) end, 15_000)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "AF-N2 healthy control (passes today): the fault keyed to an unrelated pid never fires; the run completes", %{
      helper: helper
    } do
      Seam.put(:pid_fault, nil)
      custodian = aw_custodian!()
      bystander = aw_create!(custodian, fn -> receive(do: (:finish -> :ok)) end)
      %{server: server, executing: executing, owner: owner, run_dir: run_dir} = af_run!(helper, clock: AwPidFaultClock)
      hold = held_at_go!(executing)
      Seam.put(:pid_fault, {:unix, bystander})
      release_go!(executing, hold)
      assert_receive {:run_effect_requested, ^server, %{op: :execute, kind: Effect.AwaitGate}}, 30_000
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      send(bystander, :finish)
      stop_owned!(owner)
    end

    # ---- AR-M15b real-Server lifecycle rows: a PROVEN scheduled timer (the statem's own Time-outs count 1 after the
    # :armed fact, an owned generic timeout, and a genuine {:early, _} chunk), then the exit from the outstanding
    # gate; disposal is witnessed as the process DOWN (AW-M2: gen_statem termination discards owned timers) and
    # no gate fact after it. A live gate at a settle exists only on the closed path (one op outstanding at a time:
    # the ordinary :settle request follows the gate result), so SC-1 is settle_before_exit. ----
    defp aw_proven_timer!(server) do
      assert_receive {:gate_deadline, :server, %{cap: _, gen: _, ref: _} = id, {:armed, _}}, 30_000
      owned = aw_owned_timeouts!(server)
      assert match?({1, [_entry]}, owned), "exactly one owned timeout after the arm: #{inspect(owned)}"
      {1, [entry]} = owned
      refute match?({:state_timeout, _}, entry)
      assert_receive {:gate_deadline, :server, ^id, {:early, _}}, 5_000
      id
    end

    # ---- the complete SC-1 post-fault chain (AR-M15b R1/R3): af_closed_arm_failure! consumed the Server's settle
    # applied trace and returned it; here that evidence is correlated ONCE with the actual settle ENTRY (the
    # Server's own :settle request to THIS Worker, same cap/gen, the applied ref IS the request's ref and never the
    # gate's), then the Server's :server facts for the outstanding identity are drained through its DOWN and judged
    # by the causal-boundary verdict. `gate` is the outstanding identity, or nil on a real closed path that never
    # armed (SC-C1: then no :server fact of any identity may exist). ----
    defp sc_closed_disposal!(%{server: server, executing: executing} = run, gate) do
      %{settle: {scap, sgen, sref}} = evidence = af_closed_arm_failure!(run)

      assert_received {:run_effect_requested, ^server,
                       %{op: :settle, cap: ^scap, gen: ^sgen, worker: ^executing, ref: ^sref}}

      case gate do
        %{cap: cap, gen: gen, ref: ref} = id ->
          assert {scap, sgen} == {cap, gen}, "the settle entry is for the outstanding identity's cap/gen"
          assert sref != ref, "the settle op ref is never the gate's ref"
          facts = aw_drain_server_facts(id)
          assert :ok == aw_server_facts_verdict(facts), "causal boundary: #{inspect(facts)}"

        nil ->
          assert [] == aw_drain_server_facts(:any)
      end

      evidence
    end

    @tag :aw_server
    test "SC-1 closed-path disposal at settle_before_exit (AR-M15b, RED): Worker pending, proven timer, Server chunk fault, settle entry, 1/1/0, closed exit",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      Seam.put(:pid_fault, nil)
      opts = aw_opts(helper, self(), clock: AwPidFaultClock, gate_deadline_cap_ms: 50)
      {owner, sup} = start_run!(real_config(run_dir, :run, ["/bin/sh", "-c", "sleep 30"], opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      mref = Process.monitor(server)
      executing = executing!()
      {identity, _} = ready!(owner, executing)
      %{cap: cap, gen: gen} = id = aw_proven_timer!(server)
      # the correlated Worker PENDING state on this exact outstanding gate (D-10), before the fault
      assert_receive {:gate_deadline, :worker, ^id, :pending}, 30_000
      Seam.put(:pid_fault, {:mono, server})
      run = %{server: server, mref: mref, identity: identity, executing: executing}
      # R1: the helper's evidence is asserted once, the settle entry (request + applied) pinned to THIS Worker and
      # this cap/gen; R3: the drained :server facts for `id` pass the causal-boundary verdict
      assert %{settle: {^cap, ^gen, _sref}, diagnostic: %{cleanup: cleanup}} = sc_closed_disposal!(run, id)
      assert cleanup == @af_expected_cleanup
      assert Seam.get(:pid_fault_hit) == {:mono, server}
      stop_owned!(owner)
    end

    @tag :aw_server
    test "SC-C1 control (passes today, R1): the full SC-1 post-fault chain on the real Server-stage closed path: evidence once, 1/1/0, no fact",
         %{helper: helper} do
      %{owner: owner} = run = af_real_closed_failure!(helper)
      %{diagnostic: %{cleanup: cleanup}, settle: {_cap, _gen, sref}} = sc_closed_disposal!(run, nil)
      assert cleanup == @af_expected_cleanup
      assert is_reference(sref)
      stop_owned!(owner)
    end

    @tag :aw_server
    test "SC-C1n negative (passes today, R1): after the chain the settle trace and request are consumed; re-asserting the trace FAILS",
         %{helper: helper} do
      %{owner: owner, server: server} = run = af_real_closed_failure!(helper)
      %{settle: {cap, gen, sref}} = sc_closed_disposal!(run, nil)
      refute_received {:run_effect_applied, ^server, {:settle, _, _, _}}
      refute_received {:run_effect_requested, ^server, %{op: :settle}}

      assert_raise ExUnit.AssertionError, fn ->
        assert_received {:run_effect_applied, ^server, {:settle, ^cap, ^gen, ^sref}}
      end

      stop_owned!(owner)
    end

    # ---- EF fixture (AR-M15b R2): the held-then-raising executor on the gated scenario. The executing Worker holds
    # inside its synchronous await until told, so a :send trace installed while it is provably held (its own
    # :executor_holding came from inside the await) captures its actual effect_failed send. on_exit releases a Worker
    # still held when a row fails early. ----
    defp ef_run! do
      require_run!()
      run_dir = tmp_run_dir()
      Seam.put(:held_executor_subscriber, self())
      on_exit(fn -> if pid = Seam.get(:ef_executing), do: send(pid, :fail_now) end)

      opts =
        gated_index()
        |> fresh_opts()
        |> Keyword.merge(
          gate_executor: HeldRaisingExecutor,
          clock: SeamClock,
          gate_deadline_observer: self(),
          gate_deadline_cap_ms: 50
        )

      {owner, sup} = start_run!(config(run_dir, :run, opts))
      started = started_children(sup)
      server = Keyword.fetch!(started, :server)
      mref = Process.monitor(server)
      executing = executing!()
      Seam.put(:ef_executing, executing)
      %{owner: owner, sup: sup, started: started, server: server, mref: mref, executing: executing}
    end

    defp ef_fail_held!(executing) do
      assert_receive {:executor_holding, ^executing}, 30_000
      1 = :erlang.trace(executing, true, [:send])
      send(executing, :fail_now)
      :ok
    end

    @closed_digest ~r/\Asha256:[0-9a-f]{64}\z/

    # the parity oracle (R2), pure: the diagnostic the Worker actually SENT (its own effect_failed) and the reason
    # the Server DIED with must be closed on BOTH sides and IDENTICAL terms. Closed side (m_1788853996045): exactly
    # the five keys, kind in the closed kinds, class in the EXISTING closed enum Diagnostic.result_classes(), a
    # sha256 digest, an INTEGER frames >= 0, and a cleanup the exact verdict accepts (integer 1/1/0; the Worker
    # settled its runtime in-invocation, EF-C1 observes exactly that). Identity is strict (===): 1.0 is never 1.
    # Corrupted, unknown, unproven, float or malformed values on either side are refused (EF-C2).
    defp ef_parity_verdict(worker_closed, server_diagnostic) do
      with :ok <- ef_closed_side(:worker, worker_closed),
           :ok <- ef_closed_side(:server, server_diagnostic) do
        if worker_closed === server_diagnostic,
          do: :ok,
          else: {:error, {:parity_broken, worker_closed, server_diagnostic}}
      end
    end

    defp ef_closed_side(side, diagnostic) do
      cond do
        not is_map(diagnostic) ->
          {:error, {:diagnostic_malformed, side, diagnostic}}

        not ef_closed_shape?(diagnostic) ->
          {:error, {:not_closed, side, diagnostic}}

        true ->
          case af_cleanup_verdict(Map.fetch!(diagnostic, :cleanup)) do
            :ok -> :ok
            {:error, why} -> {:error, {:cleanup, side, why}}
          end
      end
    end

    defp ef_closed_shape?(%{kind: kind, class: class, digest: digest, frames: frames, cleanup: _} = map),
      do:
        map_size(map) == 5 and kind in [:error, :exit, :throw] and is_binary(class) and
          class in Diagnostic.result_classes() and is_binary(digest) and Regex.match?(@closed_digest, digest) and
          is_integer(frames) and frames >= 0

    defp ef_closed_shape?(_other), do: false

    # the EF closed-failure chain: the executing Worker's actual correlated effect_failed :send (pinned to the
    # outstanding cap/gen/ref, from that pid, to the exact Server), the Server's DOWN, the parity verdict FIRST, the
    # in-invocation settle (abandon witnessed), NO Server settle (neither request nor applied)
    defp ef_closed_failure!(%{server: server, mref: mref, executing: executing, cap: cap, gen: gen, ref: ref}) do
      assert_receive {
                       :trace,
                       ^executing,
                       :send,
                       {:effect_failed, ^cap, ^gen, ^ref, ^executing, worker_closed},
                       ^server
                     },
                     30_000

      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, diagnostic}}, 30_000
      verdict = ef_parity_verdict(worker_closed, diagnostic)
      assert :ok == verdict, "parity verdict: #{inspect(verdict)}"
      assert_received {:abandoned, _}, "the Worker settled its runtime in-invocation"
      refute_received {:run_effect_applied, ^server, {:settle, _, _, _}}
      refute_received {:run_effect_requested, ^server, %{op: :settle}}
      %{worker_closed: worker_closed, diagnostic: diagnostic}
    end

    @tag :aw_server
    test "EF-1 effect_failed (AR-M15b, RED): correlated closed effect_failed from the Worker after a proven timer ends the Server closed, no Server settle",
         %{helper: _helper} do
      %{server: server, executing: executing, sup: sup, started: started, owner: owner} = run = ef_run!()
      id = aw_proven_timer!(server)
      ef_fail_held!(executing)
      # R2: the Worker's own send and the Server's DOWN carry the SAME closed diagnostic, cleanup exactly 1/1/0
      %{worker_closed: %{cleanup: cleanup}} = ef_closed_failure!(Map.merge(run, id))
      assert cleanup == @af_expected_cleanup
      # R3: causal boundary of the Server's own DOWN: its facts for `id`, at most one valid cancellation, nothing
      # invalid
      facts = aw_drain_server_facts(id)
      assert :ok == aw_server_facts_verdict(facts), "causal boundary: #{inspect(facts)}"
      all_down!([sup | Keyword.values(started)])
      stop_owned!(owner)
    end

    @tag :aw_server
    test "EF-C1 control (passes today, R2): the Worker's own effect_failed send equals the Server DOWN diagnostic, cleanup 1/1/0, no settle",
         %{helper: _helper} do
      %{server: server, executing: executing, sup: sup, started: started, owner: owner} = run = ef_run!()
      # the outstanding execute identity from the Server's own request to THIS Worker (no timer claim)
      assert_receive {:run_effect_requested, ^server,
                      %{op: :execute, kind: Effect.AwaitGate, cap: cap, gen: gen, ref: ref, worker: ^executing}},
                     30_000

      ef_fail_held!(executing)

      %{worker_closed: closed, diagnostic: diagnostic} =
        ef_closed_failure!(Map.merge(run, %{cap: cap, gen: gen, ref: ref}))

      assert closed == diagnostic
      assert %{kind: :error, cleanup: @af_expected_cleanup} = closed
      all_down!([sup | Keyword.values(started)])
      stop_owned!(owner)
    end

    @tag :aw_server
    test "EF-C2 negatives (pass today, R2): parity verdict refuses corrupted/unknown/unproven/malformed; full chain FAILS on a modified send or DOWN",
         %{helper: _helper} do
      closed = %{
        kind: :error,
        class: "map",
        digest: "sha256:" <> String.duplicate("0", 64),
        frames: 1,
        cleanup: @af_expected_cleanup
      }

      assert :ok == ef_parity_verdict(closed, closed)

      for bad <- [
            %{closed | cleanup: %{attempts: :unknown, settled: -1, unproven: :unknown}},
            %{closed | cleanup: %{attempts: 1, settled: 0, unproven: 1}},
            %{closed | cleanup: %{attempts: 1, settled: 1, unproven: 1}},
            %{closed | cleanup: %{attempts: :unknown, settled: 0, unproven: :unknown}},
            %{closed | cleanup: %{attempts: 1, settled: 1}},
            %{closed | cleanup: nil},
            %{closed | kind: :bogus},
            %{closed | digest: "sha256:short"},
            %{closed | frames: -1},
            Map.delete(closed, :class),
            Map.put(closed, :extra, true),
            # the d60218d review's cases: float counts/frames and a class outside Diagnostic.result_classes()
            %{closed | frames: 1.0},
            %{closed | cleanup: %{attempts: 1.0, settled: 1.0, unproven: 0.0}},
            %{closed | class: "arbitrary payload outside result_classes"},
            %{closed | class: :map}
          ] do
        assert match?({:error, _}, ef_parity_verdict(bad, bad)), "must refuse #{inspect(bad)}"
      end

      # each side is judged on its own: a malformed Server side is refused even when the Worker side is closed, and
      # a malformed Worker side even when the Server side is closed
      assert {:error, {:not_closed, :server, _}} = ef_parity_verdict(closed, %{closed | frames: 1.0})

      assert {:error, {:cleanup, :server, _}} =
               ef_parity_verdict(closed, %{closed | cleanup: %{attempts: 1.0, settled: 1.0, unproven: 0.0}})

      assert {:error, {:not_closed, :server, _}} =
               ef_parity_verdict(closed, %{closed | class: "arbitrary payload outside result_classes"})

      assert {:error, {:not_closed, :worker, _}} = ef_parity_verdict(%{closed | frames: 1.0}, closed)

      assert {:error, {:cleanup, :worker, _}} =
               ef_parity_verdict(%{closed | cleanup: %{attempts: 1.0, settled: 1.0, unproven: 0.0}}, closed)

      assert {:error, {:cleanup, :server, _}} =
               ef_parity_verdict(closed, %{closed | cleanup: %{attempts: 1, settled: 0, unproven: 1}})

      # parity itself: ANY difference between two CLOSED sides
      assert {:error, {:parity_broken, _, _}} = ef_parity_verdict(closed, %{closed | frames: 2})

      assert {:error, {:parity_broken, _, _}} =
               ef_parity_verdict(%{closed | digest: "sha256:" <> String.duplicate("f", 64)}, closed)

      assert {:error, {:parity_broken, _, _}} = ef_parity_verdict(closed, %{closed | class: "tuple"})
      assert {:error, {:diagnostic_malformed, :worker, _}} = ef_parity_verdict(nil, closed)
      assert {:error, {:diagnostic_malformed, :server, _}} = ef_parity_verdict(closed, [])

      # the full chain on the REAL path: capture the real witness pair once, then requeue it modified on either side
      %{server: server, mref: mref, executing: executing, sup: sup, started: started, owner: owner} = run = ef_run!()

      assert_receive {:run_effect_requested, ^server,
                      %{op: :execute, kind: Effect.AwaitGate, cap: cap, gen: gen, ref: ref, worker: ^executing}},
                     30_000

      ef_fail_held!(executing)
      ids = Map.merge(run, %{cap: cap, gen: gen, ref: ref})
      assert_receive {:trace, ^executing, :send, {:effect_failed, ^cap, ^gen, ^ref, ^executing, sent}, ^server}, 30_000
      assert_receive {:DOWN, ^mref, :process, ^server, {:run_step_failed, diagnostic}}, 30_000
      assert :ok == ef_parity_verdict(sent, diagnostic)

      float_cleanup = %{attempts: 1.0, settled: 1.0, unproven: 0.0}
      foreign_class = "arbitrary payload outside result_classes"

      modified = [
        {sent, %{diagnostic | cleanup: %{diagnostic.cleanup | unproven: 1}}},
        {sent, %{diagnostic | cleanup: %{attempts: :unknown, settled: 0, unproven: :unknown}}},
        {%{sent | digest: "sha256:" <> String.duplicate("f", 64)}, diagnostic},
        {sent, %{diagnostic | frames: diagnostic.frames + 1}},
        # the d60218d review's numeric-type and class corruptions, on either side and on both sides at once
        {sent, %{diagnostic | frames: diagnostic.frames * 1.0}},
        {%{sent | frames: sent.frames * 1.0}, diagnostic},
        {sent, %{diagnostic | cleanup: float_cleanup}},
        {%{sent | cleanup: float_cleanup}, %{diagnostic | cleanup: float_cleanup}},
        {%{sent | class: foreign_class}, %{diagnostic | class: foreign_class}}
      ]

      for {worker_side, server_side} <- modified do
        send(self(), {:trace, executing, :send, {:effect_failed, cap, gen, ref, executing, worker_side}, server})
        send(self(), {:DOWN, mref, :process, server, {:run_step_failed, server_side}})
        error = assert_raise(ExUnit.AssertionError, fn -> ef_closed_failure!(ids) end)
        assert error.message =~ "parity verdict"
      end

      # the unmodified pair passes the complete chain (abandon witnessed once, no Server settle)
      send(self(), {:trace, executing, :send, {:effect_failed, cap, gen, ref, executing, sent}, server})
      send(self(), {:DOWN, mref, :process, server, {:run_step_failed, diagnostic}})
      assert %{worker_closed: ^sent, diagnostic: ^diagnostic} = ef_closed_failure!(ids)
      all_down!([sup | Keyword.values(started)])
      stop_owned!(owner)
    end

    # ---- causal boundary of the Server's own DOWN (AR-M15b R3): a monitored process's DOWN is delivered after every
    # message it sent to the monitoring process, so once the DOWN is consumed every :server fact the Server ever sent
    # for the identity is already in the mailbox. The drain returns them in delivery order (other identities and other
    # senders' facts stay). The verdict allows :armed / {:early, _} and AT MOST ONE valid :cancelled (AW-M2 lets an
    # exit cancel the outstanding gate explicitly; D-10 defines :cancelled) and refuses :request_expiration, any
    # {:stale, _}, a second cancellation and any event outside the closed grammar; no order is imposed beyond the
    # contract. ----
    defp aw_drain_server_facts(:any) do
      receive do
        {:gate_deadline, :server, _id, event} -> [event | aw_drain_server_facts(:any)]
      after
        0 -> []
      end
    end

    defp aw_drain_server_facts(id) do
      receive do
        {:gate_deadline, :server, ^id, event} -> [event | aw_drain_server_facts(id)]
      after
        0 -> []
      end
    end

    # payload bounds from the existing contract (m_1788853996045): every wait is the fence's wait, an integer bounded
    # by the wait cap (DeadlineFence @max_wait_cap_ms); an already-due arm reports {:armed, 0} (AW-S1), an early chunk
    # carries the fence's {:wait, pos_integer()} (D-10 wait_ms). No other event carries a payload here.
    @max_wait_cap_ms 4_294_967_295
    defp aw_wait_ok?(:armed, wait), do: is_integer(wait) and wait >= 0 and wait <= @max_wait_cap_ms
    defp aw_wait_ok?(:early, wait), do: is_integer(wait) and wait > 0 and wait <= @max_wait_cap_ms

    defp aw_server_facts_verdict(facts) when is_list(facts) do
      facts
      |> Enum.reduce_while({:ok, 0}, fn
        {tag, wait} = fact, acc when tag in [:armed, :early] ->
          if aw_wait_ok?(tag, wait),
            do: {:cont, acc},
            else: {:halt, {:error, {:malformed_payload, fact, facts}}}

        :cancelled, {:ok, 0} ->
          {:cont, {:ok, 1}}

        :cancelled, {:ok, 1} ->
          {:halt, {:error, {:second_cancellation, facts}}}

        :request_expiration, _ ->
          {:halt, {:error, {:expiration_after_timer_proof, facts}}}

        {:stale, _} = stale, _ ->
          {:halt, {:error, {:stale_fact, stale, facts}}}

        other, _ ->
          {:halt, {:error, {:fact_outside_boundary, other, facts}}}
      end)
      |> case do
        {:ok, _cancelled} -> :ok
        error -> error
      end
    end

    defp aw_server_facts_verdict(other), do: {:error, {:facts_malformed, other}}

    @tag :aw_server
    test "FD-C1 controls (pass today, R3): drain returns the identity's :server facts in order; verdict allows at most one :cancelled, refuses invalid" do
      assert :ok == aw_server_facts_verdict([])
      assert :ok == aw_server_facts_verdict([{:armed, 50}, {:early, 20}])
      assert :ok == aw_server_facts_verdict([{:armed, 50}, {:early, 20}, :cancelled])
      # no order is imposed beyond the contract
      assert :ok == aw_server_facts_verdict([{:armed, 50}, :cancelled, {:early, 20}])
      assert :ok == aw_server_facts_verdict([:cancelled])
      # payload bounds: an already-due arm is {:armed, 0}; waits up to the fence's maximum cap are well-formed
      assert :ok == aw_server_facts_verdict([{:armed, 0}, {:early, 1}])
      assert :ok == aw_server_facts_verdict([{:armed, 4_294_967_295}, {:early, 4_294_967_295}])

      for bad <- [
            # malformed armed/early payloads (the d60218d review's [{:early, :invalid_wait}] included)
            [{:early, :invalid_wait}],
            [{:early, 0}],
            [{:early, -1}],
            [{:early, 1.0}],
            [{:early, nil}],
            [{:early, 4_294_967_296}],
            [{:armed, :invalid_wait}],
            [{:armed, -1}],
            [{:armed, 1.0}],
            [{:armed, "50"}],
            [{:armed, 4_294_967_296}],
            [{:armed, 50, 60}],
            [:armed],
            [:early],
            [{:armed, 50}, {:early, :invalid_wait}, :cancelled],
            [:request_expiration],
            [{:armed, 50}, {:early, 20}, :request_expiration],
            [{:armed, 50}, {:early, 20}, :cancelled, :request_expiration],
            [{:armed, 50}, :cancelled, :cancelled],
            [{:stale, :gen}],
            [{:armed, 50}, {:stale, %{clause: "x"}}],
            [:settle_await],
            [:resume],
            [:pending_execute],
            [:pending],
            [:bogus],
            [{:cancelled, 1}]
          ] do
        assert match?({:error, _}, aw_server_facts_verdict(bad)), "must refuse #{inspect(bad)}"
      end

      assert {:error, {:second_cancellation, _}} = aw_server_facts_verdict([:cancelled, :cancelled])

      assert {:error, {:malformed_payload, {:early, :invalid_wait}, _}} =
               aw_server_facts_verdict([{:early, :invalid_wait}])

      assert {:error, {:malformed_payload, {:armed, 1.0}, _}} = aw_server_facts_verdict([{:armed, 1.0}])
      assert {:error, {:fact_outside_boundary, :armed, _}} = aw_server_facts_verdict([:armed])
      assert {:error, {:facts_malformed, _}} = aw_server_facts_verdict(nil)

      id = %{cap: make_ref(), gen: 1, ref: make_ref()}
      other = %{id | ref: make_ref()}
      send(self(), {:gate_deadline, :server, id, {:armed, 50}})
      send(self(), {:gate_deadline, :worker, id, :pending})
      send(self(), {:gate_deadline, :server, other, :cancelled})
      send(self(), {:gate_deadline, :server, id, {:early, 20}})
      send(self(), {:gate_deadline, :server, id, :cancelled})
      assert [{:armed, 50}, {:early, 20}, :cancelled] == aw_drain_server_facts(id)
      assert [] == aw_drain_server_facts(id)
      # other senders' and other identities' facts are untouched by the drain of `id`
      assert_received {:gate_deadline, :worker, ^id, :pending}
      assert [:cancelled] == aw_drain_server_facts(other)
      assert [] == aw_drain_server_facts(:any)
    end

    @tag :aw_server
    test "T-6 control: forged wake-shaped :info tuples reaching the Server are ignored (not generic-timeout events; TC rows cover those)",
         %{helper: helper} do
      require_run!()
      run_dir = tmp_run_dir()
      opts = aw_opts(helper, self(), [])
      {owner, sup} = start_run!(real_config(run_dir, :run, aw_gated_argv(run_dir), opts))
      server = sup |> started_children() |> Keyword.fetch!(:server)
      executing = executing!()
      {_identity, _} = ready!(owner, executing)
      send(server, {:gate_chunk, %{cap: make_ref(), gen: 1, ref: make_ref()}})
      send(server, {:timeout, {:gate_deadline, make_ref()}, {:gate_chunk, %{}}})
      assert run_server().status(server) == :driving
      refute_received {:gate_deadline, :server, _, :request_expiration}
      File.write!(Path.join(run_dir, "go"), "")
      assert {:ok, _} = run_server().await(server, 30_000)
      assert Enum.any?(journal(run_dir), &(&1["type"] == "gate_passed"))
      stop_owned!(owner)
    end
  end

  # =================================================================================================
  describe "R-G / R-M rehydration authority (MUST-6, Q-7)" do
    test "R-G a config carrying raw prior lines is refused before any Writer/file/effect activity, in every mode" do
      require_run!()

      for mode <- [:run, :resume, :cancel] do
        run_dir = tmp_run_dir()
        parent = self()

        opts =
          gated_index()
          |> fresh_opts()
          |> Keyword.put(:effect_observer, fn effect, _ -> send(parent, {:effect_ran, effect.__struct__}) end)

        config = run_dir |> config(mode, opts) |> Map.put(:prior_lines, ["{}"])

        assert start_run!(config) == {:error, %{clause: "raw_lines_not_accepted"}},
               inspect(mode)

        refute File.exists?(Path.join(run_dir, "events.jsonl")), "no Writer/file activity before the refusal"
        refute_received {:effect_ran, _}
      end
    end

    # m_1788645900000 M3: repair evidence belongs to the Writer that performed it, never to the caller
    for {mode, label} <- [{:resume, "resume"}, {:cancel, "cancel"}] do
      test "M3 #{label}: a clean verified prefix journals NO tail_repair even when the caller supplies one" do
        require_run!()
        run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
        prior = length(journal(run_dir))

        forged = %{
          "action" => "truncate_tail",
          "truncated_bytes" => 777,
          "receipt_seq_before" => 0,
          "receipt_seq_after" => 0
        }

        index = Enum.find_index(H.cases(), &match?({"kill9 resume pre_dispatch", _, _, _, _}, &1))
        opts = index |> fresh_opts() |> Keyword.merge(tail_repair: forged, run_lock_path: "run.lock.99")

        config = %{
          run_dir: run_dir,
          mode: unquote(mode),
          spec: H.spec("kill9_resume"),
          plan: H.plan("kill9_resume"),
          opts: opts,
          trace: self()
        }

        {owner, sup} = start_run!(config)
        started = started_children(sup)
        assert Writer.opened(Keyword.fetch!(started, :writer)).repair == nil
        appended = repair_suffix!(started, unquote(mode), run_dir, prior)
        first = hd(appended)
        assert first["type"] in ["run_resumed", "run_cancel_requested"]
        refute Map.has_key?(first["data"], "tail_repair"), "caller-supplied repair on a clean prefix"
        assert first["data"]["run_lock_path"] == Writer.opened(Keyword.fetch!(started, :writer)).lock_path
        stop_owned!(owner)
      end

      test "M3 #{label}: the Writer's actual repair wins over a conflicting caller record" do
        require_run!()
        run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))
        prior = length(journal(run_dir))

        File.write!(
          Path.join(run_dir, "events.jsonl"),
          File.read!(Path.join(run_dir, "events.jsonl")) <> ~s({"schema":"ai-orch)
        )

        forged = %{
          "action" => "truncate_tail",
          "truncated_bytes" => 777,
          "receipt_seq_before" => 0,
          "receipt_seq_after" => 0
        }

        index = Enum.find_index(H.cases(), &match?({"kill9 resume pre_dispatch", _, _, _, _}, &1))
        opts = index |> fresh_opts() |> Keyword.put(:tail_repair, forged)

        config = %{
          run_dir: run_dir,
          mode: unquote(mode),
          spec: H.spec("kill9_resume"),
          plan: H.plan("kill9_resume"),
          opts: opts,
          trace: self()
        }

        {owner, sup} = start_run!(config)
        started = started_children(sup)
        assert %{action: :truncate_tail, truncate_bytes: 18} = Writer.opened(Keyword.fetch!(started, :writer)).repair
        appended = repair_suffix!(started, unquote(mode), run_dir, prior)
        assert %{"action" => "truncate_tail", "truncated_bytes" => 18} = hd(appended)["data"]["tail_repair"]
        stop_owned!(owner)
      end
    end

    test "R-G a torn tail is repaired by the Writer and the Server's resume sees the repaired view" do
      require_run!()
      run_dir = seed_legacy!(tmp_run_dir(), kill9("events_pre_dispatch.jsonl"))

      File.write!(
        Path.join(run_dir, "events.jsonl"),
        File.read!(Path.join(run_dir, "events.jsonl")) <> ~s({"schema":"ai-orch)
      )

      index = Enum.find_index(H.cases(), &match?({"kill9 resume pre_dispatch", _, _, _, _}, &1))

      {owner, sup} =
        start_run!(%{
          run_dir: run_dir,
          mode: :resume,
          spec: H.spec("kill9_resume"),
          plan: H.plan("kill9_resume"),
          opts: Keyword.delete(fresh_opts(index), :event_sink),
          trace: self()
        })

      server = sup |> started_children() |> Keyword.fetch!(:server)
      # D1 (m_1788751607000): the expired pre_dispatch prefix resumes to the exact expiry; the repaired view is
      # read from the ACTUAL journal (the Writer's repair, journaled by the Writer)
      # U2b GREEN transition (recorded): blocked at the dispatch with the attention-only event; the repaired view
      # is still read from the ACTUAL journal
      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => [_]}}} = run_server().await(server, 30_000)
      events = journal(run_dir)

      assert %{"type" => "human_attention_required", "data" => %{"reason" => "dispatch_deadline_exceeded"}} =
               List.last(events)

      resumed = Enum.find(events, &(&1["type"] == "run_resumed"))
      assert %{"action" => "truncate_tail", "truncated_bytes" => 18} = resumed["data"]["tail_repair"]
      stop_owned!(owner)
    end

    test "R-G the rejected-open corpus through Run.Supervisor: the exact Writer rejection and no effect" do
      require_run!()
      {:ok, v2_lines} = v2_journal_lines()
      parent = self()
      observer = fn effect, _ -> send(parent, {:effect_ran, effect.__struct__}) end

      cases = [
        {"receipt on legacy",
         tmp_run_dir()
         |> seed_legacy!(kill9("events_pre_dispatch.jsonl"))
         |> tap(
           &File.write!(
             Path.join(&1, "events.head"),
             Chain.encode_receipt(%{seq: 1, line_sha256: Chain.line_sha256("x\n"), updated_at: FixedClock.wall_ts()})
           )
         )},
        {"receipt hash mismatch",
         tmp_run_dir()
         |> seed_v2!(v2_lines)
         |> tap(
           &File.write!(
             Path.join(&1, "events.head"),
             Chain.encode_receipt(%{
               seq: length(v2_lines),
               line_sha256: Chain.line_sha256("t\n"),
               updated_at: FixedClock.wall_ts()
             })
           )
         )},
        {"receipt missing", tmp_run_dir() |> seed_v2!(v2_lines) |> tap(&File.rm!(Path.join(&1, "events.head")))}
      ]

      for {label, run_dir} <- cases do
        {:error, expected} = Writer.open(run_dir, lock: [supervisor_instance: "sup_ctl"])

        rejected =
          start_run!(config(run_dir, :resume, Keyword.put(fresh_opts(gated_index()), :effect_observer, observer)))

        assert rejected == {:error, expected}, label
      end

      refute_received {:effect_ran, _}
    end

    test "R-M Writer killed after journal line AND receipt of seq 12 (assignment_observation_started) are durable" do
      require_run!()
      run_dir = tmp_run_dir()
      fs = FaultFs.new()
      parent = self()
      # fixed scenario position, disclosed: in gated_run_seed the 12th append is assignment_observation_started
      FaultFs.inject(
        fs,
        :dir_sync,
        fn _ -> true end,
        {:after,
         fn trace ->
           receipts = Enum.count(trace, &match?({:rename, "events.head.tmp", "events.head"}, &1))

           if match?([{:dir_sync, _}, {:rename, "events.head.tmp", "events.head"} | _], trace) and receipts == 12 do
             send(parent, {:durable_before_reply, File.read!(Path.join(run_dir, "events.head"))})
             Process.exit(self(), :kill)
           end
         end}
      )

      {owner, sup} =
        start_run!(config(run_dir, :run, Keyword.put(fresh_opts(gated_index()), :fs, fs)))

      started = started_children(sup)
      writer = Keyword.fetch!(started, :writer)
      result = run_server().await(Keyword.fetch!(started, :server), 30_000)
      assert result == {:error, %{clause: "run_server_down"}}, "the interrupted invocation never observed append success"
      assert_received {:durable_before_reply, receipt_bytes}
      assert match?({:ok, %{seq: 12}}, Chain.decode_receipt(receipt_bytes))
      all_down!([sup | Keyword.values(started)])
      stop_owned!(owner)
      durable = journal(run_dir)
      assert length(durable) == 12 and Enum.at(durable, 11)["type"] == "assignment_observation_started"
      assert File.read!(Path.join(run_dir, "events.head")) == receipt_bytes

      {owner2, sup2} = start_run!(config(run_dir, :resume, fresh_opts(gated_index())))
      started2 = started_children(sup2)
      writer2 = Keyword.fetch!(started2, :writer)
      assert writer2 != writer
      opened = Writer.opened(writer2)
      assert match?(%{last_seq: 12, repair: nil}, opened), "accepted in the verified view, not repaired away"
      assert Jason.decode!(List.last(opened.lines))["type"] == "assignment_observation_started"

      assert {:ok, %{summary: %{"status" => "completed"}, events: events}} =
               run_server().await(Keyword.fetch!(started2, :server), 30_000)

      assert Enum.count(events, &(&1["seq"] == 12)) == 1
      stop_owned!(owner2)
    end
  end

  describe "writer teardown WT" do
    test "WT-1 normal close in try/after; the fallback (always attempts close) joins an already-gone holder" do
      dir = guarded_dir!()

      with_holder(dir, [supervisor_instance: "sup_wt1"], fn holder, _opened ->
        assert Process.alive?(holder)
        assert {:error, %{clause: _}} = Writer.open(dir, lock: [supervisor_instance: "sup_other"])
      end)

      dir2 = guarded_dir!()
      {holder2, _} = open_registered!(dir2, supervisor_instance: "sup_wt1b")
      mon = Process.monitor(holder2)
      assert :ok = Writer.close(holder2)
      assert_receive {:DOWN, ^mon, :process, ^holder2, :normal}, 5_000
      assert {:removed, {:already_gone, :noproc}} = settle_then_remove!(dir2, fn -> join_holder!(holder2) end)
      refute File.exists?(dir2)
    end

    test "WT-2 linked owner exits before teardown: holder terminates with it; fallback = already-gone, never success" do
      dir = guarded_dir!()
      {owner, holder} = spawn_owner!(dir, supervisor_instance: "sup_wt2")
      omon = Process.monitor(owner)
      hmon = Process.monitor(holder)
      send(owner, {:exit_with, :shutdown})
      assert_receive {:DOWN, ^omon, :process, ^owner, :shutdown}, 5_000
      assert_receive {:DOWN, ^hmon, :process, ^holder, :shutdown}, 5_000
      assert {:already_gone, :noproc} = join_holder!(holder)
      refute Process.alive?(holder)
    end

    test "WT-3 forced old ordering: exact task close queued behind the owner EXIT; alive?-then-close exits shutdown" do
      dir = guarded_dir!()
      {owner, holder} = spawn_owner!(dir, supervisor_instance: "sup_wt3")
      hmon = Process.monitor(holder)
      :ok = :sys.suspend(holder)
      omon = Process.monitor(owner)
      send(owner, {:exit_with, :shutdown})
      assert_receive {:DOWN, ^omon, :process, ^owner, :shutdown}, 5_000
      assert Process.alive?(holder), "the old snapshot sees a live holder"

      # the OLD pattern from a REGISTERED task (joined even on failure): its call queues behind the EXIT
      old =
        registered_task!(fn ->
          try do
            {:returned, if(Process.alive?(holder), do: Writer.close(holder), else: :skipped)}
          catch
            :exit, reason -> {:exited, reason}
          end
        end)

      caller = old.pid

      assert wait_until_true(fn -> exit_then_close_queued?(holder, owner, caller) end, 5_000),
             "the exact task's $gen_call(:close) is queued behind the owner's EXIT while the holder is suspended"

      :ok = :sys.resume(holder)
      assert {:exited, {:shutdown, {GenServer, :call, [^holder, :close, _]}}} = Task.await(old, 5_000)
      assert_receive {:DOWN, ^hmon, :process, ^holder, :shutdown}, 5_000
      assert {:already_gone, :noproc} = join_holder!(holder)
    end

    test "WT-4 live holder closed by the fallback with the real :ok, DOWN joined, directory removed" do
      dir = guarded_dir!()
      {holder, _} = open_registered!(dir, supervisor_instance: "sup_wt4")
      assert {:removed, {{:closed, :ok}, :normal}} = settle_then_remove!(dir, fn -> join_holder!(holder) end)
      refute Process.alive?(holder)
      refute File.exists?(dir)
    end

    test "WT-5 real FaultFs close failure RAISES after the joined termination; directory preserved" do
      dir = guarded_dir!()
      fs = FaultFs.new()
      {holder, _} = open_registered!(dir, [supervisor_instance: "sup_wt5"], fs: fs)
      FaultFs.inject(fs, :close, fn _ -> true end, {:error, :eio})
      hmon = Process.monitor(holder)

      assert_raise RuntimeError, ~r/directory preserved .* close failed/, fn ->
        settle_then_remove!(dir, fn -> join_holder!(holder) end)
      end

      # the termination WAS joined (the helper raises only after it) and the directory survived the failed settle
      assert_receive {:DOWN, ^hmon, :process, ^holder, :normal}, 0
      refute Process.alive?(holder)
      assert File.exists?(dir), "absence unproven: the directory must not be removed"
      File.rm_rf!(dir)
    end

    test "WT-7 gap: owner dies between open and publication; reaper fails closed, directory preserved" do
      dir = guarded_dir!()
      {owner, orphan} = spawn_owner!(dir, [supervisor_instance: "sup_wt7"], mode: :die_before_publish, cleanup: :control)
      # the owner exits right after telling the test: by the time it is monitored it may already be gone
      omon = Process.monitor(owner)
      assert_receive {:DOWN, ^omon, :process, ^owner, owner_reason}, 5_000
      assert owner_reason in [:shutdown, :noproc]

      assert_raise RuntimeError, ~r/directory preserved .* holder unknown: Writer.open in flight/, fn ->
        settle_then_remove!(dir, fn -> reap_actors!(owner) end)
      end

      assert File.exists?(dir), "absence unproven: the directory must not be removed"
      # the subject record is RETAINED while unresolved; the test-owned registry knows the orphan, so the control
      # cleanup registered BEFORE the permit will join it, erase both records and remove the directory
      assert :persistent_term.get({__MODULE__, :holder_of, owner}, nil) == :opening
      assert :persistent_term.get({__MODULE__, :control_registry, owner}, nil) == orphan
      hmon = Process.monitor(orphan)
      assert_receive {:DOWN, ^hmon, :process, ^orphan, _}, 5_000
      refute Process.alive?(orphan)
    end

    test "WT-7b early failure: the owner dies before ANY publication; the control cleanup registered before the permit fails closed (no absence proof, directory preserved); the test-known orphan is joined and the record resolved" do
      dir = guarded_dir!()

      {owner, orphan} =
        spawn_owner!(dir, [supervisor_instance: "sup_wt7b"], mode: :die_before_registry, cleanup: :control)

      omon = Process.monitor(owner)
      assert_receive {:DOWN, ^omon, :process, ^owner, owner_reason}, 5_000
      assert owner_reason in [:shutdown, :noproc]

      # the SAME cleanup that on_exit will run: nothing known to join, absence unproven, directory kept
      assert_raise RuntimeError, ~r/orphan unknown to the registry .*:opening.*directory preserved/, fn ->
        control_cleanup!(dir, owner)
      end

      assert File.exists?(dir)
      # the only party that knows the orphan resolves it: join its DOWN, then publish it to the registry so the
      # registered cleanup can erase the records and remove the directory with absence proof
      hmon = Process.monitor(orphan)
      assert_receive {:DOWN, ^hmon, :process, ^orphan, _}, 5_000
      :persistent_term.put({__MODULE__, :control_registry, owner}, orphan)
      assert {:resolved, ^orphan} = control_cleanup!(dir, owner)
      refute File.exists?(dir)
    end

    test "WT-6 survivor negative: an unsettleable (suspended) holder is reported and the directory preserved" do
      dir = guarded_dir!()
      {holder, _} = open_registered!(dir, supervisor_instance: "sup_wt6")
      :ok = :sys.suspend(holder)

      assert_raise RuntimeError, ~r/directory preserved .*:timeout/, fn ->
        settle_then_remove!(dir, fn -> join_holder!(holder) end)
      end

      assert Process.alive?(holder), "the survivor is still there: nothing pretended it was gone"
      assert File.exists?(dir)
      # the registered failure-path callback settles it at teardown (resume, real close, guarded removal)
    end

    test "WT-8a before-death: fallback close queued behind an earlier close; already_gone :normal, exact DOWN" do
      dir = guarded_dir!()
      {holder, _} = open_registered!(dir, supervisor_instance: "sup_wt8a")
      :ok = :sys.suspend(holder)
      earlier = registered_task!(fn -> capture(fn -> Writer.close(holder) end) end)
      first = earlier.pid
      # the FIRST caller's close is witnessed queued BEFORE the second caller is even launched
      assert wait_until_true(fn -> close_queued?(holder, first) end, 5_000),
             "the earlier close is queued in the suspended holder before the fallback is launched"

      later = registered_task!(fn -> join_holder!(holder) end)
      second = later.pid

      assert wait_until_true(fn -> closes_queued_in_order?(holder, first, second) end, 5_000),
             "both close calls are queued in order while the holder is suspended"

      hmon = Process.monitor(holder)
      :ok = :sys.resume(holder)
      assert {:ok, :ok} = Task.await(earlier, 5_000)
      assert {:already_gone, :normal} = Task.await(later, 5_000)
      assert_receive {:DOWN, ^hmon, :process, ^holder, :normal}, 5_000
    end

    test "WT-8b after-death: fallback runs after the joined DOWN; already_gone :noproc; never a success" do
      dir = guarded_dir!()
      {holder, _} = open_registered!(dir, supervisor_instance: "sup_wt8b")
      hmon = Process.monitor(holder)
      assert :ok = Writer.close(holder)
      assert_receive {:DOWN, ^hmon, :process, ^holder, :normal}, 5_000
      assert {:already_gone, :noproc} = join_holder!(holder)
    end

    test "WT-9 with_holder closes on raise/throw/exit, preserves the primary, reports a secondary close failure" do
      # raise (non-AssertionError) and throw and exit: the holder is closed on the way out
      for {name, primary} <- [
            {"raise", fn -> raise ArgumentError, "primary" end},
            {"throw", fn -> throw(:primary) end},
            {"exit", fn -> exit(:primary) end}
          ] do
        dir = guarded_dir!()
        parent = self()

        outcome =
          capture(fn ->
            with_holder(dir, [supervisor_instance: "sup_wt9_#{name}"], fn holder, _ ->
              send(parent, {:holder_seen, name, holder})
              primary.()
            end)
          end)

        assert_receive {:holder_seen, ^name, holder}, 1_000
        refute Process.alive?(holder), "#{name}: the owner-local close ran on the way out"

        case name do
          "raise" -> assert {:rescued, %ArgumentError{message: "primary"}} = outcome
          "throw" -> assert {:thrown, :primary} = outcome
          "exit" -> assert {:exited, :primary} = outcome
        end
      end

      # secondary close failure with a non-AssertionError primary: both are reported, the primary first
      dir = guarded_dir!()
      fs = FaultFs.new()

      err =
        assert_raise RuntimeError, fn ->
          with_holder(dir, [supervisor_instance: "sup_wt9_both"], [fs: fs], fn _holder, _ ->
            FaultFs.inject(fs, :close, fn _ -> true end, {:error, :eio})
            raise ArgumentError, "primary"
          end)
        end

      assert err.message =~ "primary error"
      assert err.message =~ "ArgumentError"
      assert err.message =~ "writer close also failed"
      assert err.message =~ "close_failed"
    end
  end
end
