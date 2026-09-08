defmodule AiOrchestrator.Run.DeadlineFenceRedTest do
  @moduledoc """
  U2a-0 RED/interface, revision 2 (docs/contracts/deadline-fence.org; DF-M1..4 applied): a PURE monotonic deadline
  fence with operation correlation and a winner-preserving transition. Controls measure the unchanged 9eba2ef source
  (a real blocked Worker, executable backstop refusals through Execution.prepare, shape evidence, the walk oracle
  bound); RED rows address the absent `Run.DeadlineFence` through `Module.concat`.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.OwnerDoubles
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @instance "sup_deadline_fence"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @canary "DEADLINE-FENCE-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @max_timer_ms 4_294_967_295
  @big Integer.pow(2, 62)
  @backstop_max_ms 86_400_000
  @backstop_grace_ms 2_000
  @now 1_700_000_000

  # ---- fixed clock for the executable backstop controls -------------------------------------------------------

  defmodule FixedClock do
    @moduledoc false
    def unix_now, do: Process.get(:deadline_fence_now, 1_700_000_000)
    def monotonic_ms, do: System.monotonic_time(:millisecond)
    def wall_ts, do: "2026-09-06T00:00:00Z"
  end

  # ---- mutant oracles for the walk bound (C-4) -------------------------------------------------------------------

  defmodule ZeroWaitOracle do
    @moduledoc false
    def next(_fence, _mono), do: {:wait, 0}
  end

  defmodule PrematureDueOracle do
    @moduledoc false
    def next(_fence, _mono), do: :due
  end

  defmodule CompliantOracle do
    @moduledoc false
    def next(%{due_ms: due, wait_cap_ms: cap}, mono) do
      remaining = max(due - mono, 0)
      if remaining == 0, do: :due, else: {:wait, min(remaining, cap)}
    end
  end

  defp fence_mod, do: Module.concat(["AiOrchestrator", "Run", "DeadlineFence"])
  defp arm(id, deadline, unix, mono, cap), do: fence_mod().arm(id, deadline, unix, mono, cap)
  defp next(fence, mono), do: fence_mod().next(fence, mono)
  defp outstanding(id), do: fence_mod().outstanding(id)
  defp observe(out, event), do: fence_mod().observe(out, event)

  defp identity, do: %{cap: make_ref(), gen: 1, ref: make_ref()}

  defp armed!(id, ahead_s, mono \\ 10_000, cap \\ 1_000) do
    assert {:ok, fence} = arm(id, 1_000_000 + ahead_s, 1_000_000, mono, cap)
    fence
  end

  defp running!(id) do
    assert {:ok, out} = outstanding(id)
    out
  end

  defp assert_no_canary(term), do: refute(inspect(term, limit: :infinity, printable_limit: :infinity) =~ @canary)
  defp clause_only?({:error, map}), do: is_map(map) and Map.keys(map) == [:clause]
  defp clause_only?(_), do: false

  # BOUNDED walk (DF-M4): every wait validated in 1..cap, strict advance, no overshoot, explicit step budget
  defp walk(oracle, fence, mono, budget) do
    do_walk(oracle, fence, mono, budget, [])
  end

  defp do_walk(_oracle, _fence, _mono, budget, _acc) when budget < 0, do: {:error, :budget_exhausted}

  defp do_walk(oracle, %{due_ms: due, wait_cap_ms: cap} = fence, mono, budget, acc) do
    case oracle.next(fence, mono) do
      {:wait, ms} when is_integer(ms) and ms >= 1 and ms <= cap and mono + ms <= due ->
        do_walk(oracle, fence, mono + ms, budget - 1, [ms | acc])

      {:wait, ms} ->
        {:error, {:bad_wait, ms, mono}}

      :due when mono >= due ->
        {:ok, mono, Enum.reverse(acc)}

      :due ->
        {:error, {:premature_due, mono}}

      other ->
        {:error, {:unexpected, other}}
    end
  end

  defp budget_for(remaining, cap), do: div(remaining + cap - 1, cap) + 1

  # ---- subtree harness (C-1) -----------------------------------------------------------------------------------

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "deadline-fence-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    Process.put(:deadline_fence_now, @now)
    {:ok, dir: dir}
  end

  defp gated_opts do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    make.()
  end

  defp held(dir) do
    opts =
      gated_opts()
      |> Keyword.drop(@owned)
      |> Keyword.put(:supervisor_instance, @instance)
      |> Keyword.put(:gate_opts, runner: OwnerDoubles.held_gate(collector()))

    %{
      run_dir: dir,
      mode: :run,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: opts,
      trace: collector()
    }
  end

  defp start!(config) do
    assert {:ok, root} = Run.Supervisor.start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :writer, writer}, 10_000
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, writer: writer, server: server, work: work}
  end

  defp mailbox(pid) do
    case Process.info(pid, :messages) do
      {:messages, messages} -> messages
      nil -> []
    end
  end

  defp wait(fun), do: wait(fun, System.monotonic_time(:millisecond) + 5_000)

  defp wait(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(5)
        wait(fun, deadline)
    end
  end

  # ---- executable backstop controls (C-3) -----------------------------------------------------------------------

  defp request(run_dir, deadline_unix) do
    %{
      run_id: "run_fixture_0001",
      gate_run_id: "gr_0001",
      attempt: 1,
      command_argv: ["/bin/sh", "-c", "true"],
      repo_root: run_dir,
      run_dir: run_dir,
      deadline_unix: deadline_unix,
      supervisor_instance: "sup_0001"
    }
  end

  defp prepare_opts(extra \\ []),
    do: Keyword.merge([helper: "/bin/true", clock: FixedClock, settle_ms: 200, rounds: 2], extra)

  defp fs, do: {SystemFs, nil}

  describe "controls: unchanged 9eba2ef source" do
    test "C-1 a REAL Worker blocked in Effects.execute does not read a fence-shaped message; completes; teardown observed",
         %{dir: dir} do
      facts = start!(held(dir))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      assert_receive {:gate_entered, ^worker}, 10_000
      assert Server.status(facts.server) == :driving
      worker_mon = Process.monitor(worker)
      root_mon = Process.monitor(facts.root)
      fence_message = {:deadline_fence, make_ref()}
      send(worker, fence_message)
      assert wait(fn -> fence_message in mailbox(worker) end)
      assert Process.alive?(worker)
      assert fence_message in mailbox(worker), "blocked in the synchronous effect: the message is not dequeued"
      send(worker, :release_gate)

      assert facts.server |> Server.await(30_000) |> elem(1) |> Map.fetch!(:summary) |> Map.fetch!("status") ==
               "completed"

      assert wait(fn -> fence_message not in mailbox(worker) end), "after the effect returned, the catch-all consumed it"
      Supervisor.stop(facts.root, :shutdown, 10_000)
      assert_receive {:DOWN, ^worker_mon, :process, ^worker, _}, 10_000
      assert_receive {:DOWN, ^root_mon, :process, _, _}, 10_000
    end

    test "C-2 SHAPE EVIDENCE: Timer admits only Deadline; gate observation structs are distinct from TimedOut" do
      assert Effect.admissible_observations(%Effect.Timer{purpose: "queued_send_poll", deadline_unix: 0}) ==
               [Observation.Deadline]

      assert :deadline_unix in Map.keys(Observation.TimedOut.__struct__())
      assert :result in Map.keys(Observation.GateFailed.__struct__())
      assert :result in Map.keys(Observation.GateUnsettled.__struct__())
      refute :deadline_unix in Map.keys(Observation.GateFailed.__struct__())
    end

    test "C-3a executable backstop: an expired deadline is refused by Execution.prepare before anything is spawned", %{
      dir: dir
    } do
      run_dir = Path.join(dir, "expired")
      File.mkdir_p!(run_dir)
      assert Execution.prepare(fs(), request(run_dir, @now), prepare_opts()) == {:error, %{clause: "deadline_expired"}}

      assert Execution.prepare(fs(), request(run_dir, @now - 1), prepare_opts()) ==
               {:error, %{clause: "deadline_expired"}}

      assert File.ls!(run_dir) == []
    end

    test "C-3b executable backstop: a horizon beyond the native maximum (grace included) is refused before spawn", %{
      dir: dir
    } do
      run_dir = Path.join(dir, "unsupported")
      File.mkdir_p!(run_dir)
      # remaining_ms + grace > max  <=>  remaining_s * 1000 > max - grace
      first_unsupported_s = div(@backstop_max_ms - @backstop_grace_ms, 1_000) + 1

      assert Execution.prepare(fs(), request(run_dir, @now + first_unsupported_s), prepare_opts()) ==
               {:error, %{clause: "deadline_unsupported", max_ms: @backstop_max_ms}}

      assert File.ls!(run_dir) == []
    end

    test "C-3c positive seam witness: the supported boundary passes backstop and stops at the :before_spawn barrier", %{
      dir: dir
    } do
      run_dir = Path.join(dir, "boundary")
      File.mkdir_p!(run_dir)
      parent = self()
      marker = make_ref()

      # the seam contract is `fun.(name, info) && :ok` (execution.ex 1350-1355): only a FALSY return halts prepare,
      # and prepare then returns that falsy value from its `with`; an error tuple would be truthy and NOT halt
      barrier = fn
        :before_spawn, nil ->
          send(parent, {:before_spawn, marker})
          false

        _name, _info ->
          :ok
      end

      last_supported_s = div(@backstop_max_ms - @backstop_grace_ms, 1_000)

      for ahead_s <- [1, 60, last_supported_s] do
        result = Execution.prepare(fs(), request(run_dir, @now + ahead_s), prepare_opts(barrier: barrier))
        assert result == false, "ahead #{ahead_s}s: backstop accepted and the barrier halted: #{inspect(result)}"
        assert_receive {:before_spawn, ^marker}, 1_000
      end

      assert File.ls!(run_dir) == [], "the barrier stopped prepare before the guardian spawned or a claim was published"
    end

    test "C-4 the walk oracle bound fails promptly on a zero wait and on a premature due; a compliant oracle passes" do
      fence = %{due_ms: 5_000, identity: identity(), wait_cap_ms: 1_000}
      assert walk(ZeroWaitOracle, fence, 0, budget_for(5_000, 1_000)) == {:error, {:bad_wait, 0, 0}}
      assert walk(PrematureDueOracle, fence, 0, budget_for(5_000, 1_000)) == {:error, {:premature_due, 0}}

      assert walk(CompliantOracle, fence, 0, budget_for(5_000, 1_000)) ==
               {:ok, 5_000, [1_000, 1_000, 1_000, 1_000, 1_000]}

      assert walk(CompliantOracle, fence, 0, 2) == {:error, :budget_exhausted}
    end
  end

  # ---- RED --------------------------------------------------------------------------------------------------------

  describe "RED: arm / next (R1, R2)" do
    test "F-1 exact seconds to ms: five bounded waits then due; one ms before due waits 1" do
      fence = armed!(identity(), 5, 10_000, 1_000)
      assert fence.due_ms == 15_000
      assert next(fence, 10_000) == {:wait, 1_000}
      assert next(fence, 14_999) == {:wait, 1}
      assert next(fence, 15_000) == :due
      assert next(fence, 19_000) == :due

      assert walk(fence_mod(), fence, 10_000, budget_for(5_000, 1_000)) ==
               {:ok, 15_000, [1_000, 1_000, 1_000, 1_000, 1_000]}
    end

    test "F-2 zero and past deadlines: due at arm, never a negative wait" do
      id = identity()
      assert {:ok, zero} = arm(id, 1_000_000, 1_000_000, 10_000, 1_000)
      assert zero.due_ms == 10_000 and next(zero, 10_000) == :due
      assert {:ok, past} = arm(id, 999_000, 1_000_000, 10_000, 1_000)
      assert past.due_ms == 10_000 and next(past, 10_000) == :due
      assert next(past, 9_000) == {:wait, 1_000}
    end

    test "F-3 negative monotonic epoch arms and chunks correctly" do
      fence = armed!(identity(), 3, -5_000_000, 2_000)
      assert fence.due_ms == -4_997_000
      assert walk(fence_mod(), fence, -5_000_000, budget_for(3_000, 2_000)) == {:ok, -4_997_000, [2_000, 1_000]}
    end

    test "F-4 cap boundary and the bounded walk: waits in 1..cap, strict advance, no overshoot, exact chunk count" do
      id = identity()
      assert {:ok, exact} = arm(id, 1_000_001, 1_000_000, 0, 1_000)
      assert next(exact, 0) == {:wait, 1_000}
      assert {:ok, plus} = arm(id, 1_000_001, 1_000_000, 0, 999)
      assert next(plus, 0) == {:wait, 999}
      assert next(plus, 999) == {:wait, 1}
      assert next(plus, 1_000) == :due

      for {ahead_s, cap} <- [{7, 3_000}, {100, 7}, {1, @max_timer_ms}, {3_600, 250_000}] do
        fence = armed!(id, ahead_s, 0, cap)
        remaining = ahead_s * 1_000
        assert {:ok, reached, waits} = walk(fence_mod(), fence, 0, budget_for(remaining, cap))
        assert reached == remaining and Enum.sum(waits) == remaining
        assert length(waits) == div(remaining + cap - 1, cap)
      end
    end

    test "F-5 large integers exact without walks; malformed arm domains refuse in the pinned precedence" do
      id = identity()
      assert {:ok, big} = arm(id, @big, 0, 0, @max_timer_ms)
      assert big.due_ms == @big * 1_000
      assert next(big, @big * 1_000 - 1) == {:wait, 1}
      assert next(big, @big * 1_000) == :due
      assert next(big, 0) == {:wait, @max_timer_ms}

      for bad <- [1.0, 0.0, nil, true, false, "1", :one, [1], %{}, {1}] do
        assert arm(id, bad, 0, 0, 1_000) == {:error, %{clause: "deadline_invalid"}}
        assert arm(id, 0, bad, 0, 1_000) == {:error, %{clause: "deadline_invalid"}}
        assert arm(id, 0, 0, bad, 1_000) == {:error, %{clause: "deadline_invalid"}}
      end

      for bad_cap <- [0, -1, 1.0, nil, true, @max_timer_ms + 1, "1000", :cap] do
        assert arm(id, 0, 0, 0, bad_cap) == {:error, %{clause: "wait_cap_invalid"}}
      end

      # precedence: identity before instants before cap
      assert arm(%{id | gen: 0}, nil, nil, nil, 0) == {:error, %{clause: "fence_identity_invalid"}}
      assert arm(id, nil, nil, nil, 0) == {:error, %{clause: "deadline_invalid"}}
      assert arm(id, 0, 0, 0, 0) == {:error, %{clause: "wait_cap_invalid"}}
    end

    test "F-6 no later wall-clock dependency: fence keys exact; equal offsets arm identically" do
      id = identity()
      fence = armed!(id, 5)
      assert Enum.sort(Map.keys(fence)) == [:due_ms, :identity, :wait_cap_ms]
      assert {:ok, a} = arm(id, 1_000_005, 1_000_000, 10_000, 1_000)
      assert {:ok, b} = arm(id, 5, 0, 10_000, 1_000)
      assert a == b
    end
  end

  describe "RED: observe/2 winner preservation, correlation and precedence (R3, R4)" do
    test "F-7 the winner is preserved through the whole state machine for the SAME identity" do
      id = identity()
      fence = armed!(id, 1)
      due = 11_000

      # result first
      out = running!(id)
      assert {:ok, completed, :retain_result} = observe(out, {:result, id})
      assert completed.state == :completed
      assert observe(completed, {:fence, fence, due}) == {:stale, :completed, completed}
      assert observe(completed, {:fence, fence, 10_500}) == {:stale, :completed, completed}
      assert observe(completed, {:result, id}) == {:stale, :duplicate_completion, completed}

      # fence first
      out2 = running!(id)
      assert {:ok, selected, :request_expiration} = observe(out2, {:fence, fence, due})
      assert selected.state == :timeout_selected
      assert observe(selected, {:fence, fence, due}) == {:stale, :duplicate_timeout, selected}
      assert observe(selected, {:fence, fence, 10_500}) == {:stale, :duplicate_timeout, selected}
      assert {:ok, late, :retain_late_result} = observe(selected, {:result, id})
      assert late.state == :completed_after_timeout
      assert observe(late, {:fence, fence, due}) == {:stale, :duplicate_timeout, late}
      assert observe(late, {:result, id}) == {:stale, :duplicate_completion, late}
      assert late.identity == id and selected.identity == id and completed.identity == id
    end

    test "F-8 foreign identities in EVERY state leave the outstanding unchanged; early fence waits" do
      id = identity()
      fence = armed!(id, 1)

      foreign = [
        {%{id | cap: make_ref()}, :foreign_cap},
        {%{id | gen: 2}, :foreign_generation},
        {%{id | ref: make_ref()}, :foreign_ref}
      ]

      running = running!(id)
      {:ok, completed, _} = observe(running, {:result, id})
      {:ok, selected, _} = observe(running!(id), {:fence, fence, 11_000})
      {:ok, late, _} = observe(selected, {:result, id})

      for out <- [running, completed, selected, late], {foreign_id, reason} <- foreign do
        assert observe(out, {:fence, armed!(foreign_id, 1), 11_000}) == {:stale, reason, out}
        assert observe(out, {:fence, armed!(foreign_id, 1), 10_500}) == {:stale, reason, out}
        assert observe(out, {:result, foreign_id}) == {:stale, reason, out}
      end

      assert observe(running, {:fence, fence, 10_500}) == {:early, {:wait, 500}, running}
      assert next(fence, 10_500) == {:wait, 500}
      assert observe(running, {:fence, fence, 10_999}) == {:early, {:wait, 1}, running}
    end

    test "F-9 precedence: outstanding > event shape > fence > instant > foreign > state > early/due; cap > gen > ref" do
      id = identity()
      fence = armed!(id, 1)
      out = running!(id)
      bad_fence = %{fence | due_ms: nil}
      {:ok, completed, _} = observe(out, {:result, id})

      # all malformed at once: outstanding wins
      assert observe(%{bogus: true}, {:fence, bad_fence, :nope}) == {:error, %{clause: "outstanding_invalid"}}
      # event shape before its fence/instant
      assert observe(out, {:fence, bad_fence}) == {:error, %{clause: "event_invalid"}}
      assert observe(out, :fence) == {:error, %{clause: "event_invalid"}}
      # malformed fence before malformed instant
      assert observe(out, {:fence, bad_fence, :nope}) == {:error, %{clause: "fence_invalid"}}
      # malformed instant before a foreign identity
      assert observe(out, {:fence, armed!(%{id | cap: make_ref()}, 1), :nope}) == {:error, %{clause: "instant_invalid"}}
      # malformed result identity before state
      assert observe(completed, {:result, %{id | gen: 0}}) == {:error, %{clause: "fence_identity_invalid"}}
      # foreign before state (completed) and before early
      assert observe(completed, {:fence, armed!(%{id | gen: 2}, 1), 10_500}) == {:stale, :foreign_generation, completed}
      # state before early: completed + early -> completed; selected + early -> duplicate_timeout
      assert observe(completed, {:fence, fence, 10_500}) == {:stale, :completed, completed}
      {:ok, selected, _} = observe(running!(id), {:fence, fence, 11_000})
      assert observe(selected, {:fence, fence, 10_500}) == {:stale, :duplicate_timeout, selected}
      # cap > gen > ref
      all_foreign = %{cap: make_ref(), gen: 9, ref: make_ref()}
      assert observe(out, {:fence, armed!(all_foreign, 1), 11_000}) == {:stale, :foreign_cap, out}
      assert observe(out, {:fence, armed!(%{all_foreign | cap: id.cap}, 1), 11_000}) == {:stale, :foreign_generation, out}
      assert observe(out, {:result, %{all_foreign | cap: id.cap, gen: 1}}) == {:stale, :foreign_ref, out}
    end

    test "F-10 no payload echo at all four functions: clause-only refusals, canary never returned, no exception" do
      id = identity()
      fence = armed!(id, 1)
      out = running!(id)

      results = [
        arm(id, @canary, 0, 0, 1_000),
        arm(id, 0, 0, 0, @canary),
        arm(%{id | cap: @canary}, 0, 0, 0, 1_000),
        next(%{canary: @canary}, 0),
        next(fence, @canary),
        outstanding(%{id | ref: @canary}),
        outstanding(@canary),
        observe(%{canary: @canary}, {:result, id}),
        observe(out, {:fence, %{canary: @canary}, 0}),
        observe(out, {:fence, fence, @canary}),
        observe(out, {:result, @canary}),
        observe(out, @canary)
      ]

      for result <- results do
        assert clause_only?(result), inspect(result)
        assert_no_canary(result)
      end
    end

    test "F-11 one-field malformed matrices for identity, fence, outstanding, result identity and instants" do
      good = identity()
      fence = armed!(good, 1)
      out = running!(good)

      bad_identities = [
        %{good | cap: :cap},
        %{good | cap: nil},
        %{good | ref: "ref"},
        %{good | ref: 1},
        %{good | gen: 0},
        %{good | gen: -1},
        %{good | gen: 1.0},
        %{good | gen: nil},
        %{good | gen: true},
        Map.delete(good, :ref),
        Map.delete(good, :cap),
        Map.delete(good, :gen),
        Map.put(good, :extra, 1),
        struct(URI, Map.to_list(good)),
        nil,
        [],
        %{}
      ]

      for bad <- bad_identities do
        assert outstanding(bad) == {:error, %{clause: "fence_identity_invalid"}}, inspect(bad)
        assert arm(bad, 0, 0, 0, 1_000) == {:error, %{clause: "fence_identity_invalid"}}, inspect(bad)
        assert observe(out, {:result, bad}) == {:error, %{clause: "fence_identity_invalid"}}, inspect(bad)
        assert next(%{fence | identity: bad}, 0) == {:error, %{clause: "fence_invalid"}}, inspect(bad)

        assert observe(%{out | identity: bad}, {:result, good}) == {:error, %{clause: "outstanding_invalid"}},
               inspect(bad)
      end

      bad_fences = [
        %{fence | due_ms: nil},
        %{fence | due_ms: 1.0},
        %{fence | wait_cap_ms: 0},
        %{fence | wait_cap_ms: @max_timer_ms + 1},
        %{fence | wait_cap_ms: nil},
        Map.delete(fence, :due_ms),
        Map.delete(fence, :identity),
        Map.delete(fence, :wait_cap_ms),
        Map.put(fence, :extra, 1),
        struct(URI, Map.to_list(fence)),
        %{},
        nil,
        [],
        :fence
      ]

      for bad <- bad_fences do
        assert next(bad, 0) == {:error, %{clause: "fence_invalid"}}, inspect(bad)
        assert observe(out, {:fence, bad, 0}) == {:error, %{clause: "fence_invalid"}}, inspect(bad)
      end

      bad_outstandings = [
        %{out | state: :unknown},
        %{out | state: nil},
        %{out | state: "running"},
        Map.delete(out, :state),
        Map.delete(out, :identity),
        Map.put(out, :extra, 1),
        struct(URI, Map.to_list(out)),
        %{},
        nil,
        [],
        :running
      ]

      for bad <- bad_outstandings do
        assert observe(bad, {:result, good}) == {:error, %{clause: "outstanding_invalid"}}, inspect(bad)
        assert observe(bad, {:fence, fence, 0}) == {:error, %{clause: "outstanding_invalid"}}, inspect(bad)
      end

      for bad_instant <- [nil, 1.0, true, "0", :now, [0], %{}] do
        assert next(fence, bad_instant) == {:error, %{clause: "instant_invalid"}}, inspect(bad_instant)
        assert observe(out, {:fence, fence, bad_instant}) == {:error, %{clause: "instant_invalid"}}, inspect(bad_instant)
      end

      for bad_event <- [nil, :fence, {:fence, fence}, {:result}, {:other, good}, {:fence, fence, 0, :extra}, []] do
        assert observe(out, bad_event) == {:error, %{clause: "event_invalid"}}, inspect(bad_event)
      end

      assert {:ok, %{identity: ^good, state: :running}} = outstanding(good)
    end

    test "F-12 public surface exact" do
      assert Enum.sort(fence_mod().__info__(:functions)) == [arm: 5, next: 2, observe: 2, outstanding: 1]
    end
  end
end
