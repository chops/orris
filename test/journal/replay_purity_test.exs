defmodule AiOrchestrator.Journal.ReplayPurityTest do
  @moduledoc """
  NS-02.B.002 control: replay has no side effects, with effect spies.

  The row's acceptance is "replay a golden journal with effect spies" and its failure
  control is "any dispatch/gate/notification during replay fails". Every fold entry point
  runs inside a traced process with global call tracing armed on the effect modules and on
  both filesystem modules, and ONE trace message is a failure.

  Five properties this file is responsible for, each with its own control:

  1. NAMED GOLDEN ORACLE, ATTRIBUTED CORPUS OUTCOMES, AND NAMED REFUSALS. `gated_run_seed`
     is folded through all three entry points and `Fold.summary/1` is compared to the
     committed `expected.json`. A named REJECT golden is folded inside the same traced
     process and asserted against its committed `expected_rejection.json`, so a refusal is
     required to be the NAMED refusal rather than any error. An entry whose lines do not all
     decode or validate is NOT folded over the surviving subset: the event and view entry
     points are recorded `{:inapplicable, refusals}`, carrying the refusal terms.

  2. DESCENDANT COVERAGE. Tracing is armed with `:set_on_spawn`, so an effect performed by
     a process the fold spawns is visible, and with `:procs`, so the collector is TOLD about
     the spawn. `:set_on_spawn` alone propagates flags but announces nothing.

  3. DELIVERY FENCE, PER PID, TO A FIXED POINT. Trace messages are asynchronous. The collector
     discovers, joins and fences each owned pid individually and repeats until no new pid
     appears. There is NO `:erlang.trace_delivered(:all)` sweep: that call is documented over
     currently traced processes, which would make the fence depend on nothing having been
     untraced rather than on a property of this tree, and it would assert over unrelated
     processes. See `converge/5` for why the per-pid fixed point terminates and is complete.

  4. AN ENFORCED BUDGET. Every stage carries ONE absolute monotonic deadline, and that
     deadline is checked BEFORE each receive as well as being passed to `after`. A
     `receive ... after 0` still consumes any matching message first, so a stream of trace
     events - or merely a queue of finite work - could otherwise postpone the timeout
     indefinitely or finish after expiry and report success.

  5. POSITIVE CONTROLS PER EFFECT. Each named control effect with callable functions -
     dispatch, gate, notification, pane registry - has its own control exercising one pinned,
     pure, non-operational MFA, plus a disarm witness proving the control fails when that
     module is not traced, plus a reader-arming sensitivity control.

  The reader half is observational at the injected filesystem boundary: `Reader.load/2` runs
  through a `FaultFs` whose every mutating operation is planned to fail, and its success is
  asserted rather than discarded.

  ON THE PANE REGISTRY, corrected. `AiOrchestrator.PaneRegistry` is a namespace `use Boundary`
  declaration with no functions, so arming it matches nothing. The OPERATIONAL registry is
  `AiOrchestrator.PaneRegistry.FileRegistry`, whose `claim/3` and `release/1` write directly
  through `File.mkdir_p`, `File.ln`, `File.open`, `File.write` and `File.rm_rf` - BYPASSING the
  `FaultFs` seam the reader control injects. It is therefore armed on both named-effect paths,
  and `pane_refs/1` is its pinned pure MFA. No `claim/3` or `release/1` call is made anywhere
  in this file.

  LIFECYCLE CONTROLS. Several tests here exercise the collector's own failure modes rather
  than the replay path. Successful syntax is not their acceptance: each one FIRST asserts that
  the condition it is supposed to repair is actually present, so a control that silently
  stopped reproducing that condition fails rather than passes.

  Where the property under test is a COLLECTOR TRANSITION rather than an outcome, the control
  steps the collector through individual convergence rounds with a bounded handshake and acts
  at the exact transition. Inferring a transition from a later observation - for instance
  concluding from a surviving grandchild that adoption was recursive - would be reading a
  generation as a round, which it is not.

  GLOBAL-STATE MUTATION WITNESS. The three `:persistent_term` mutations - `put/2`,
  `put_new/2` and `erase/1` - are now witnessed on the traced replay tree, armed per exact
  MFA and never as `{:persistent_term, :_, :_}`. Reads (`get/0`, `get/1`, `get/2`, `info/0`)
  are deliberately not witnessed. Whether call tracing reaches these BIF exports is shown by
  the mutation controls below, not assumed; the replay tests prove nothing about it. In a base
  tree a warm cache can hide a cold-cache write, so the controls, not the replay tests, are
  the evidence that the witness works.

  NOT COVERED HERE, recorded rather than implied: other global state - ETS, the process
  dictionary of untraced processes, application env, `:global` registrations, code loading -
  and any effect outside the three MFAs. Nothing here is a universal purity claim.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Gate.Runner
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Notify.Notifier
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs

  @fixture_root Path.expand("../fixtures/contracts", __DIR__)
  @journal_source Path.expand("../../lib/ai_orchestrator/journal.ex", __DIR__)
  @fold_source Path.expand("../../lib/ai_orchestrator/journal/fold.ex", __DIR__)

  # Every effect the row names, plus both filesystem modules. `Fold` reaching any of these
  # is the failure; they are traced globally so only external calls register.
  @spied_modules [
    AiOrchestrator.Dispatch,
    Runner,
    Notifier,
    FileRegistry,
    File,
    :file,
    :os
  ]

  # The NAMED effects alone, without the filesystem modules. Used where real reads are the
  # operation under test and must be allowed to proceed. FileRegistry belongs here in
  # particular: its writes do NOT go through the injected FaultFs, so the reader control
  # would not otherwise observe them at all.
  @effect_modules [
    AiOrchestrator.Dispatch,
    Runner,
    Notifier,
    FileRegistry
  ]

  # The ONLY mutating exports of the pinned OTP's `persistent_term` (its exports, read with
  # beam_lib, are erase/1, get/0-2, info/0, put/2 and put_new/2). Each is armed as an exact
  # MFA, so the reads stay unarmed. Arming is refused if another tracer already holds any of
  # them: cleanup removes these exact patterns, which must therefore be ours.
  @global_state_mutations [
    {:persistent_term, :put, 2},
    {:persistent_term, :put_new, 2},
    {:persistent_term, :erase, 1}
  ]

  # One pinned, pure, non-operational MFA per NAMED control effect. Each was inspected at the
  # exact head: `valid_capabilities?/1` folds `Enum.all?` over an empty list; `started_data/2`
  # builds a map whose only path work is string interpolation; `bound_payload/1` returns a
  # binary under the cap unchanged; `pane_refs/1` at file_registry.ex:26-35 is
  # `Enum.map |> Enum.uniq |> Enum.sort` and returns `[]` for an empty agent list. None
  # dispatches, gates, notifies, claims, releases or writes.
  @control_mfas [
    {AiOrchestrator.Dispatch, :valid_capabilities?, [[]]},
    {Runner, :started_data, [%{"gate_run_id" => "spy_gate", "command_argv" => []}, []]},
    {Notifier, :bound_payload, ["probe"]},
    {FileRegistry, :pane_refs, [%{"agents" => []}]}
  ]

  @mutating_ops ~w(mkdir_p mkdir rm rmdir link chmod open write sync close rename dir_sync)a

  # A reject golden whose refusal layer was READ, not inferred from its directory name:
  # `test/contracts/fold_rejection_test.exs` asserts this fixture against
  # `expected_rejection.json` through `Fold.fold_lines/1`, so the fold is where it is refused.
  @reject_golden "reject_seq_gap"

  # A reject golden refused one layer EARLIER, at `Event.validate_line/1`, per
  # `test/contracts/journal_envelope_test.exs`. Its refusal must survive corpus preparation
  # with its named clause intact, which is what makes the retention assertion load-bearing.
  @envelope_reject_golden "reject_schema_version_string"

  # ONE overall deadline for a whole discover/join/fence stage, not a per-process timeout
  # charged again for every pid found. A per-pid budget multiplies without bound as the
  # traced tree widens. Expiry of THIS budget is an explicit failure: never a green result,
  # and never a claim that cleanup completed.
  @cleanup_deadline 30_000

  # The test side always waits strictly longer than the budget it gave the collector, so a
  # collector that DID report a bounded failure is heard rather than overtaken by the test's
  # own timeout - otherwise the explicit report would be replaced by a mute one.
  @reply_grace 5_000

  # The full wait a test gives the collector for a cleanup report: the collector's own budget
  # plus the grace above.
  @cleanup_reply_deadline @cleanup_deadline + @reply_grace

  # Lifecycle controls deliberately drive workloads that never finish, and they step the
  # collector through individual transitions, so they get their own small budget. It also
  # bounds the observation handshake: an observer that has died must never be able to wedge
  # the cleanup it was watching.
  @control_deadline 2_000

  test "the named golden folds to its committed summary through every entry point, under the spies" do
    lines = F.lines("scenarios", "gated_run_seed")
    expected = F.json("scenarios", "gated_run_seed", "expected.json")

    decoded = Enum.map(lines, &Jason.decode/1)
    validated = Enum.map(lines, &Event.validate_line/1)
    events = for {:ok, event} <- decoded, do: event
    views = for {:ok, view} <- validated, do: view

    # The golden is the one journal that must fold cleanly, so its inputs must be whole.
    # Nothing below is allowed to run over a subset of it.
    assert length(events) == length(lines),
           "golden journal did not decode completely: #{length(events)} of #{length(lines)}"

    assert length(views) == length(lines),
           "golden journal did not validate completely: #{length(views)} of #{length(lines)}"

    # The oracle runs INSIDE the traced folder. An earlier form asserted the summary in the
    # untraced test process, which checked the RESULT while observing no effects during the
    # fold that produced it - the two halves of this row's claim came apart.
    arm_spies()
    {folder, armed} = spawn_owned(fn -> golden_on_command() end)
    assert armed == 1
    send(folder, {:golden, self(), lines, events, views})

    assert_receive {:golden_folded, summaries}, 60_000
    assert join_tracees([folder]) == [], "the golden fold reached an effect"

    # The oracle is the committed fixture, not another fold: a shared defect in Fold could
    # otherwise make three agreeing wrong answers look like corroboration.
    for {entry, summary} <- summaries do
      assert summary == expected, "#{entry}: summary does not match the committed golden"
    end
  end

  # Design R1, completed. Refusing a bad journal is as much a replay as accepting a good one,
  # and it must refuse for the NAMED reason while reaching no effect. The existing coverage in
  # `fold_rejection_test.exs` asserts the clause but runs UNTRACED, so it says nothing about
  # effects on the refusal path.
  test "the named reject golden is refused with its committed clause, under the spies" do
    lines = F.lines("journals", @reject_golden)
    expected = F.json("journals", @reject_golden, "expected_rejection.json")

    arm_spies()
    {folder, armed} = spawn_owned(fn -> reject_on_command() end)
    assert armed == 1
    send(folder, {:reject, self(), lines})

    assert_receive {:reject_folded, result}, 60_000
    assert join_tracees([folder]) == [], "refusing a journal reached an effect"

    assert {:error, rejection} = result
    F.assert_rejection_matches(rejection, expected)
  end

  test "an envelope-layer refusal survives corpus preparation with its named clause" do
    expected = F.json("journals", @envelope_reject_golden, "expected_rejection.json")

    {_name, _lines, _events, views, counts} =
      Enum.find(corpus(), fn {name, _l, _e, _v, _c} -> name == @envelope_reject_golden end) ||
        flunk("#{@envelope_reject_golden} is not in the corpus, so this assertion is vacuous")

    assert counts.validate_refusals > 0,
           "#{@envelope_reject_golden} validated cleanly, so its refusal cannot be retained"

    assert {:inapplicable, refusals} = views

    # The index is pinned, not taken as "whichever refused first": journal_envelope_test.exs
    # reads `[line | _]` for this fixture, so line 0 is the offending one. If the refusal ever
    # moves, this fails loudly instead of quietly comparing a different line's clause.
    assert {0, {:error, rejection}} = hd(refusals)
    F.assert_rejection_matches(rejection, expected)
  end

  test "folding every fixture journal calls no dispatch, gate, notifier, registry or filesystem function" do
    corpus = corpus()
    assert corpus != [], "the fixture corpus is empty, so this spy would report green over nothing"

    arm_spies()
    {folder, armed} = spawn_owned(fn -> fold_on_command() end)
    assert armed == 1
    send(folder, {:fold, self(), corpus})

    assert_receive {:folded, outcomes}, 60_000
    assert length(outcomes) == length(corpus)

    reached = join_tracees([folder])

    assert reached == [],
           """
           NS-02.B.002: replay reached an effect. A fold that dispatches, runs a gate,
           notifies, touches the pane registry or opens a file is not a replay.

           #{Enum.map_join(reached, "\n", &"  #{inspect(&1)}")}
           """

    # Outcomes are ATTRIBUTED, never filtered. Every entry point is either DECIDED - `{:ok, _}`
    # or a refusal - or explicitly marked INAPPLICABLE because its input was refused before the
    # fold could be asked. A subset fold is never produced and so can never be mistaken for a
    # whole-journal replay.
    for {name, counts, results} <- outcomes, {entry, result} <- results do
      assert match?({:ok, _}, result) or match?({:error, _}, result) or
               match?({:inapplicable, [_ | _]}, result),
             "#{name}/#{entry}: undecided fold result #{inspect(result)} over #{inspect(counts)}"
    end

    # The parts must reconcile against the whole. This CAN fail: a helper that dropped,
    # duplicated or fabricated a line would trip it, unlike re-asserting the predicate the
    # loop above already checked.
    for {name, counts, _results} <- outcomes do
      assert counts.decoded + counts.decode_refusals == counts.lines,
             "#{name}: decode outcomes do not account for every line: #{inspect(counts)}"

      assert counts.validated + counts.validate_refusals == counts.lines,
             "#{name}: validation outcomes do not account for every line: #{inspect(counts)}"
    end

    # An entry point marked inapplicable must carry the refusals that made it so, and an
    # entry point that was asked must have had whole input. Neither direction may be silent.
    for {name, counts, results} <- outcomes do
      applied_events? = not match?({:inapplicable, _}, Keyword.fetch!(results, :events))
      applied_views? = not match?({:inapplicable, _}, Keyword.fetch!(results, :views))

      assert applied_events? == (counts.decode_refusals == 0),
             "#{name}: fold_events applicability disagrees with its refusals #{inspect(counts)}"

      assert applied_views? == (counts.validate_refusals == 0),
             "#{name}: fold_views applicability disagrees with its refusals #{inspect(counts)}"
    end
  end

  test "an effect performed by a descendant of the folder is reported" do
    arm_spies()
    {parent, armed} = spawn_owned(fn -> spawn_then_probe() end)
    assert armed == 1
    send(parent, {:probe, self()})

    # Deterministic completion: the child reports its own pid and the parent reports the
    # child's exit. No sleep is used and no timing inference is drawn.
    assert_receive {:child_started, child}, 10_000
    assert_receive {:child_done, ^child}, 10_000

    reported = join_tracees([parent, child])

    assert Enum.any?(reported, &match?({:trace, ^child, :call, {File, :exists?, _args}}, &1)),
           "set_on_spawn is not arming descendants: the child's File call was not reported"
  end

  test "each named control effect has a spy that reports its own pinned call" do
    arm_spies()
    {prober, armed} = spawn_owned(fn -> control_on_command() end)
    assert armed == 1
    send(prober, {:controls, self(), @control_mfas})

    assert_receive {:controlled, :ok}, 10_000
    reported = join_tracees([prober])

    for {module, function, _args} <- @control_mfas do
      assert Enum.any?(reported, &match?({:trace, ^prober, :call, {^module, ^function, _}}, &1)),
             "the spy did not report #{inspect(module)}.#{function}; that effect is untraced"
    end
  end

  # The reader test arms @effect_modules ONLY and then asserts `reached == []`. That assertion
  # is worth nothing unless this narrower arming can actually report each named effect, so its
  # sensitivity is measured here under the SAME arming rather than inferred from the full-spy
  # controls above.
  test "the reader-path arming is sensitive to every named effect it claims to exclude" do
    arm_spies(@effect_modules)
    {prober, armed} = spawn_owned(fn -> control_on_command() end)
    assert armed == 1
    send(prober, {:controls, self(), @control_mfas})

    assert_receive {:controlled, :ok}, 10_000
    reported = join_tracees([prober])

    for {module, function, _args} <- @control_mfas do
      assert Enum.any?(reported, &match?({:trace, ^prober, :call, {^module, ^function, _}}, &1)),
             "arming @effect_modules alone does not report #{inspect(module)}.#{function}, " <>
               "so the reader control's `reached == []` establishes nothing about that effect"
    end
  end

  # EVERY named effect gets its own disarm witness. An earlier form took only the head of
  # @control_mfas, so it proved the control load-bearing for Dispatch alone and said nothing
  # about the others - which is where a silent tracing failure would most plausibly hide.
  for {module, function, args} <- [
        {AiOrchestrator.Dispatch, :valid_capabilities?, [[]]},
        {Runner, :started_data, [%{"gate_run_id" => "spy_gate", "command_argv" => []}, []]},
        {Notifier, :bound_payload, ["probe"]},
        {FileRegistry, :pane_refs, [%{"agents" => []}]}
      ] do
    test "#{inspect(module)} left unarmed is not reported, which is what makes its control load-bearing" do
      module = unquote(module)
      function = unquote(function)
      args = unquote(Macro.escape(args))
      armed_modules = @spied_modules -- [module]

      # This control arms a SUBSET, so it cannot call `arm_spies/1` - that would arm the very
      # module whose absence is the point. It still needs the collector, both because the
      # collector is the tracer and because pattern removal is now the collector's job: a
      # local `on_exit` removing patterns could run BEFORE the collector had finished
      # discovery, which is exactly the disarm-before-discovery order the design forbids.
      for spied <- @spied_modules, do: Code.ensure_loaded!(spied)
      start_collector(@spied_modules, [], nil)
      for spied <- armed_modules, do: :erlang.trace_pattern({spied, :_, :_}, true, [:global])

      {prober, armed} = spawn_owned(fn -> control_on_command() end)
      assert armed == 1
      send(prober, {:controls, self(), [{module, function, args}]})

      assert_receive {:controlled, :ok}, 10_000
      disarmed = join_tracees([prober])

      refute Enum.any?(disarmed, &match?({:trace, _pid, :call, {^module, ^function, _}}, &1)),
             "#{inspect(module)} was reported while unarmed, so its positive control proves nothing"
    end
  end

  test "cleanup completes promptly after a normal join and removes the patterns it armed" do
    arm_spies()
    {prober, armed} = spawn_owned(fn -> control_on_command() end)
    assert armed == 1
    send(prober, {:controls, self(), @control_mfas})

    assert_receive {:controlled, :ok}, 10_000
    assert join_tracees([prober]) != [], "the join reported nothing, so this control is vacuous"

    # The join consumed every monitor DOWN it waited for. Cleanup must not try to await those
    # same references a second time: it would find nothing and burn the whole budget.
    assert :erlang.trace_info({File, :exists?, 1}, :traced) == {:traced, :global},
           "the pattern under test was not armed, so its removal below would prove nothing"

    for mfa <- @global_state_mutations do
      assert :erlang.trace_info(mfa, :traced) == {:traced, :global},
             "#{inspect(mfa)} was not armed, so its removal below would prove nothing"
    end

    collector = collector()
    send(collector, {:cleanup, self()})
    assert_receive {:cleanup_done, ^collector, :ok}, @cleanup_reply_deadline

    assert :erlang.trace_info({File, :exists?, 1}, :traced) == {:traced, false},
           "cleanup reported success while its global trace pattern was still armed"

    assert_mutations_disarmed("cleanup after a normal join")
  end

  # FINDING 1. A `receive ... after 0` consumes matching messages before it times out, so an
  # expired budget could be consumed its way past. Here the budget is expired BEFORE the stage
  # starts and a matching `:DOWN` is PROVEN to be queued at the collector first, so a stage
  # that only honoured `after` would settle the pid and report success.
  test "an expired budget fails the stage even with a matching message already queued" do
    arm_spies()
    {prober, armed} = spawn_owned(fn -> control_on_command() end)
    assert armed == 1
    send(prober, {:controls, self(), @control_mfas})
    assert_receive {:controlled, :ok}, 10_000

    collector = collector()
    await_queued_down(collector, prober)

    assert {:join_failed, reason} =
             request_join_at([prober], System.monotonic_time(:millisecond) - 1, @control_deadline)

    assert reason =~ "budget expired",
           "the stage did not report budget expiry; it reported: #{reason}"

    send(collector, {:cleanup, self()})
    assert_receive {:cleanup_done, ^collector, :ok}, @cleanup_reply_deadline

    assert :erlang.trace_info({File, :exists?, 1}, :traced) == {:traced, false},
           "the expired stage left its global trace pattern armed"

    assert_mutations_disarmed("the expired stage")
  end

  test "a workload that never returns fails the join explicitly and still cleans up" do
    arm_spies()
    {stuck, armed} = spawn_owned(fn -> never_returns() end)
    assert armed == 1
    send(stuck, {:begin, self()})
    assert_receive {:begun, ^stuck}, 10_000

    # A convergence timeout must be REPORTED. An earlier form raised inside the collector,
    # which killed the collector with tracees still live and the global patterns still armed.
    assert {:join_failed, reason} = request_join([stuck], @control_deadline)
    assert reason =~ inspect(stuck), "the failure did not name the pid it waited for: #{reason}"

    collector = collector()
    send(collector, {:cleanup, self()})
    assert_receive {:cleanup_done, ^collector, :ok}, @cleanup_reply_deadline

    refute Process.alive?(stuck), "the failed join left its workload running"

    assert :erlang.trace_info({File, :exists?, 1}, :traced) == {:traced, false},
           "the failed join left its global trace pattern armed"

    assert_mutations_disarmed("the failed join")
  end

  # FINDING 2a. The child must be discovered DURING cleanup, not adopted beforehand by the
  # idle collector loop. The collector is held at its first cleanup round - where the owned
  # set is asserted to be exactly the parent - and only then is the parent told to spawn and
  # exit. `collector_loop/3` is not running at that point, so the spawn trace can only be
  # consumed inside the cleanup walk.
  test "cleanup discovers and kills a child first spawned after cleanup had already begun" do
    arm_spies(@spied_modules, self())
    {parent, armed} = spawn_owned(fn -> outliving_parent() end)
    assert armed == 1

    collector = collector()
    send(collector, {:cleanup, self()})

    {^parent, :live, owned_pids} =
      await_round(collector, fn {pid, state, _owned} -> pid == parent and state == :live end)

    assert owned_pids == [parent],
           "the collector already knew #{inspect(owned_pids -- [parent])} before cleanup began, " <>
             "so this control would not test discovery during cleanup"

    parent_ref = Process.monitor(parent)
    send(parent, {:spawn_lingering, self()})
    assert_receive {:lingering_child, child}, 10_000
    assert_receive {:DOWN, ^parent_ref, :process, ^parent, _reason}, 10_000

    assert Process.alive?(child),
           "the child did not outlive its parent, so this control tests nothing"

    # The seam proceeds unacknowledged if a round is not released in time. Had that happened
    # here, cleanup would already have killed the parent before it was told to spawn, and this
    # control would not have tested discovery during cleanup at all.
    refute_received {:round_unheld, ^collector, _event}

    send(collector, {:continue, self()})
    assert drain_rounds_until_cleanup(collector) == :ok

    refute Process.alive?(child),
           "cleanup left alive a child it could only have discovered during the cleanup walk"
  end

  # The window this control covers is the one in which a stand-in owner exists but nothing has
  # been registered to terminate it. An earlier form sent the collector identity as the owner's
  # first message and asserted it before registering anything, and a comment here claimed the
  # identity existed "before anything can go wrong" - which was false: that assertion itself
  # could fail, and then neither owner termination nor cleanup was ever joined.
  #
  # `standin_collector/1` closes the window structurally: both identities are obtained without
  # a handshake, and cleanup is registered before any assertion.
  #
  # WHAT THIS TEST IS, stated accurately: it exercises the CLEANUP HELPER with no handshake
  # ever performed, and asserts that termination and cleanup are both joined. It does NOT
  # prove that the `on_exit` callback is registered at the right moment - it calls the helper
  # directly, and nothing here injects a registration failure. The evidence for registration
  # ordering is the SOURCE ORDER in `standin_collector/1`, not this control.
  test "the stand-in cleanup helper joins owner death and cleanup with no handshake performed" do
    {owner, collector} = standin_collector(self(), [])

    assert Process.alive?(owner) and Process.alive?(collector),
           "the stand-in pair was not created, so this control tests nothing"

    # `:ok` here means BOTH the owner's exact `:DOWN` and the collector's acknowledgment were
    # received; a missing one is returned by name rather than passing silently.
    assert standin_cleanup(owner, collector) == :ok
    refute Process.alive?(owner), "the stand-in owner survived the cleanup that was registered"
  end

  # FINDING 2b and 3. Owner death must land DURING a partially evolved convergence, after the
  # tree has been adopted inside that same call.
  #
  # WHAT THIS ESTABLISHES, stated no more strongly than it holds: the join begins with an owned
  # snapshot of EXACTLY `[root]`, asserted at the first round; leaf is nevertheless present in
  # the owned set by the round that reaches it; therefore mid and leaf were adopted INSIDE the
  # join. It does NOT establish that each ancestor was discovered in its own separate round -
  # root and mid both run to completion before the first round is released, so one `await_down`
  # may consume both spawn events - and it does not establish any fencing order.
  test "owner death inside a partially evolved convergence still cleans the adopted tree" do
    reporter = self()
    {owner, collector} = standin_collector(reporter, @global_state_mutations)

    {root, armed} = spawn_owned_via(collector, fn -> exiting_root(reporter) end)
    assert armed == 1

    # The join starts BEFORE the chain produces any spawn, so every adoption below happens
    # inside the convergence call rather than in the idle collector loop.
    send(collector, {:join, self(), [root], System.monotonic_time(:millisecond) + 60_000})

    assert {^root, :live, [^root]} =
             await_round(collector, fn {pid, state, _owned} -> pid == root and state == :live end)

    # root and mid run to completion on their own; the collector is still held, so both pids
    # are known here before any round is released.
    send(root, :begin)
    assert_receive {:chain_mid, ^root, mid}, 10_000
    assert_receive {:chain_leaf, ^mid, leaf}, 10_000

    send(collector, {:continue, self()})

    assert {^leaf, :live, owned_pids} =
             await_round(collector, fn {pid, state, _owned} -> pid == leaf and state == :live end)

    assert root in owned_pids and mid in owned_pids,
           "the join had not adopted the chain, so killing the owner now proves nothing"

    assert Process.alive?(leaf), "the leaf was not alive at the transition under test"

    # The seam proceeds unacknowledged if a round is not released in time, so a control could
    # otherwise pass having never actually held the collector anywhere.
    refute_received {:round_unheld, ^collector, _event}

    # The armed precondition, at the transition under test: without it the disarmed check
    # below could not tell removal on this path from patterns that were never set.
    for mfa <- @global_state_mutations do
      assert :erlang.trace_info(mfa, :traced) == {:traced, :global},
             "#{inspect(mfa)} was not armed before owner death; its removal would prove nothing"
    end

    Process.exit(owner, :kill)
    send(collector, {:continue, self()})
    send(collector, {:cleanup, self()})

    assert drain_rounds_until_cleanup(collector) == :ok

    refute Process.alive?(leaf),
           "owner death discarded the state the join had evolved: the leaf adopted inside " <>
             "that call was left running"

    assert_mutations_disarmed("owner death inside convergence")
  end

  # MUTATION CONTROLS. These are the whole evidence that the global-state witness works: a
  # warm cache can hide a cold-cache write in the replay tests, so those prove nothing about
  # traceability. Every control uses a key it reserved as absent, registers that key's
  # cleanup BEFORE any pattern is armed, and asserts EFFECTS read outside the trace rather
  # than attempts. The key cleanup erases only after the collector has acknowledged a
  # complete tree cleanup; see `erase_key_after_cleanup/2`.

  # C1-C3: one positive control per witnessed mutation.
  for {function, arity} <- [put: 2, put_new: 2, erase: 1] do
    test "a traced #{function}/#{arity} is reported exactly and takes effect" do
      function = unquote(function)
      value = {:r11_value, make_ref()}
      key = reserve_key()
      collector = start_spies(@spied_modules, @global_state_mutations, nil)
      erase_key_after_cleanup(key, collector)

      {args, expected_result} = mutation_setup(function, key, value)
      arm_patterns(@spied_modules, @global_state_mutations)

      {prober, armed} = spawn_owned(fn -> mutate_on_command() end)
      assert armed == 1
      send(prober, {:mutate, self(), [{function, args}]})
      assert_receive {:mutated, ^prober, [result]}, 10_000

      assert mutations_reported(join_tracees([prober])) ==
               [{:trace, prober, :call, {:persistent_term, function, args}}],
             "#{function}/#{unquote(arity)} by a traced process was not reported exactly once"

      assert result == expected_result
      assert_mutation_effect(function, key, value)
    end
  end

  # C4: a replacement keeps the term count and changes only the value, which is exactly what
  # a count proxy over `info/0` would miss.
  test "replacing an existing value from a traced process is reported" do
    # One tuple shape with two fresh references, so the inequality below is a runtime fact rather
    # than one the type checker can decide statically.
    old = {:r11_value, make_ref()}
    new = {:r11_value, make_ref()}
    key = reserve_key()
    collector = start_spies(@spied_modules, @global_state_mutations, nil)
    erase_key_after_cleanup(key, collector)

    :ok = :persistent_term.put(key, old)
    assert :persistent_term.get(key) == old, "the key was not pre-created, so nothing is replaced"
    assert new != old
    arm_patterns(@spied_modules, @global_state_mutations)

    {prober, armed} = spawn_owned(fn -> mutate_on_command() end)
    assert armed == 1
    send(prober, {:mutate, self(), [{:put, [key, new]}]})
    assert_receive {:mutated, ^prober, [:ok]}, 10_000

    assert mutations_reported(join_tracees([prober])) ==
             [{:trace, prober, :call, {:persistent_term, :put, [key, new]}}]

    assert :persistent_term.get(key) == new
  end

  # C5: a put followed by an erase leaves the term count where it started. Both must be
  # reported, in the order they happened.
  test "a traced put followed by an erase of the same key reports both, in order" do
    value = {:r11_value, make_ref()}
    key = reserve_key()
    collector = start_spies(@spied_modules, @global_state_mutations, nil)
    erase_key_after_cleanup(key, collector)
    assert absent?(key)
    arm_patterns(@spied_modules, @global_state_mutations)

    {prober, armed} = spawn_owned(fn -> mutate_on_command() end)
    assert armed == 1
    send(prober, {:mutate, self(), [{:put, [key, value]}, {:erase, [key]}]})
    assert_receive {:mutated, ^prober, [:ok, true]}, 10_000

    assert mutations_reported(join_tracees([prober])) == [
             {:trace, prober, :call, {:persistent_term, :put, [key, value]}},
             {:trace, prober, :call, {:persistent_term, :erase, [key]}}
           ]

    assert absent?(key)
  end

  # C6: set_on_spawn carries the mutation witness to a descendant of the traced process.
  test "a mutation performed by a descendant of the traced process is reported" do
    value = {:r11_child, make_ref()}
    key = reserve_key()
    collector = start_spies(@spied_modules, @global_state_mutations, nil)
    erase_key_after_cleanup(key, collector)
    assert absent?(key)
    arm_patterns(@spied_modules, @global_state_mutations)

    {parent, armed} = spawn_owned(fn -> spawn_then_mutate() end)
    assert armed == 1
    send(parent, {:mutate_in_child, self(), key, value})
    assert_receive {:mutating_child, child}, 10_000
    assert_receive {:child_mutated, ^child, :ok}, 10_000

    assert mutations_reported(join_tracees([parent, child])) ==
             [{:trace, child, :call, {:persistent_term, :put, [key, value]}}],
           "the child's put was not reported, so descendants escape the mutation witness"

    assert :persistent_term.get(key) == value
  end

  # C7: reads are deliberately unarmed. They must not be reported even while the three
  # mutations are armed, including a read over every global key.
  test "persistent_term reads by a traced process are not reported" do
    value = {:r11_value, make_ref()}
    key = reserve_key()
    missing = reserve_key()
    collector = start_spies(@spied_modules, @global_state_mutations, nil)
    erase_key_after_cleanup(key, collector)

    :ok = :persistent_term.put(key, value)
    assert :persistent_term.get(key) == value, "the read key was not pre-created"
    arm_patterns(@spied_modules, @global_state_mutations)

    for read <- [
          {:persistent_term, :get, 0},
          {:persistent_term, :get, 1},
          {:persistent_term, :get, 2},
          {:persistent_term, :info, 0}
        ] do
      assert :erlang.trace_info(read, :traced) == {:traced, false},
             "#{inspect(read)} is traced, so this control would not show that reads stay unarmed"
    end

    {prober, armed} = spawn_owned(fn -> read_on_command() end)
    assert armed == 1
    send(prober, {:read, self(), key, missing})
    assert_receive {:read_done, ^prober, {own, default, global_has_own?, info}}, 10_000

    assert mutations_reported(join_tracees([prober])) == []
    assert own == value
    assert default == :r11_absent
    assert global_has_own?, "get/0 did not return this control's key, so it read nothing global"
    assert is_map(info)
  end

  # C8: the disarm witness. With every effect module armed but NOT the three mutations, a real
  # put is not reported, which is what makes the arming load-bearing.
  test "a put is not reported when the mutation patterns are left unarmed" do
    value = {:r11_value, make_ref()}
    key = reserve_key()
    collector = start_spies(@spied_modules, [], nil)
    erase_key_after_cleanup(key, collector)
    assert absent?(key)
    arm_patterns(@spied_modules, [])
    assert_mutations_disarmed("the disarm witness before its traced act")

    {prober, armed} = spawn_owned(fn -> mutate_on_command() end)
    assert armed == 1
    send(prober, {:mutate, self(), [{:put, [key, value]}]})
    assert_receive {:mutated, ^prober, [:ok]}, 10_000

    assert mutations_reported(join_tracees([prober])) == [],
           "a put was reported with its pattern unarmed, so the positive controls prove nothing"

    assert :persistent_term.get(key) == value,
           "the put did not happen, so its absence proves nothing"
  end

  test "the reader completes with every mutating filesystem operation planned to fail" do
    for dir <- journal_dirs() do
      fs = FaultFs.new()

      for op <- @mutating_ops do
        FaultFs.inject(fs, op, fn _args -> true end, {:error, :replay_is_read_only})
      end

      loaded = Reader.load(dir, fs: fs)

      # Success is ASSERTED, not discarded: a reader that errored would otherwise satisfy
      # this test while attempting nothing. The corpus contains rejection fixtures, so a
      # refusal is a legitimate outcome - an UNDECIDED one is not.
      assert match?({:ok, %{lines: _}}, loaded) or match?({:error, _}, loaded),
             "#{Path.basename(dir)}: undecided reader result #{inspect(loaded)}"

      attempted = fs |> FaultFs.trace() |> Enum.filter(&(elem(&1, 0) in @mutating_ops))

      assert attempted == [],
             "#{Path.basename(dir)}: the read path attempted #{inspect(attempted)}"
    end
  end

  test "the named golden reader load succeeds explicitly" do
    dir = Path.join(@fixture_root, "scenarios/gated_run_seed")
    fs = FaultFs.new()

    for op <- @mutating_ops do
      FaultFs.inject(fs, op, fn _args -> true end, {:error, :replay_is_read_only})
    end

    # The named effects are spied HERE too. Asserting only that the load succeeds and wrote
    # nothing leaves the row's actual question - does reading a journal reach dispatch, the
    # gate, the notifier or the pane registry - untested on the reader path. Real filesystem
    # reads are permitted deliberately: File, :file and :os are NOT armed, because reading the
    # journal is the operation. FileRegistry IS armed, because its writes go straight to File
    # and would never appear in the FaultFs trace below.
    arm_spies(@effect_modules)
    {reader, armed} = spawn_owned(fn -> reader_on_command() end)
    assert armed == 1
    send(reader, {:load, self(), dir, fs})

    assert_receive {:reader_loaded, loaded}, 60_000
    reached = join_tracees([reader])

    assert {:ok, %{lines: lines}} = loaded
    assert lines != []

    assert reached == [],
           "loading a journal reached a named effect: #{inspect(reached)}"

    attempted = fs |> FaultFs.trace() |> Enum.filter(&(elem(&1, 0) in @mutating_ops))
    assert attempted == []
  end

  test "the Journal boundary still depends on the clock and process identity alone" do
    source = File.read!(@journal_source)
    aliases = Regex.scan(~r/^\s*alias\s+([A-Za-z0-9_.]+)/m, source, capture: :all_but_first)

    assert aliases == [] or Enum.all?(aliases, fn [name] -> String.contains?(name, "Journal") end),
           "journal.ex aliases outside the Journal namespace: #{inspect(aliases)}"
  end

  test "the fold module aliases nothing but the event schema" do
    source = File.read!(@fold_source)
    aliases = Regex.scan(~r/^\s*alias\s+([A-Za-z0-9_.]+)/m, source, capture: :all_but_first)

    assert Enum.all?(aliases, fn [name] ->
             String.contains?(name, "Event") or String.contains?(name, "Fold")
           end),
           "fold.ex aliases more than the event schema: #{inspect(aliases)}"
  end

  defp fold_on_command do
    receive do
      {:fold, reply_to, corpus} ->
        outcomes =
          for {name, lines, events, views, counts} <- corpus do
            {name, counts,
             [
               {:lines, Fold.fold_lines(lines)},
               {:events, fold_if_applicable(events, &Fold.fold_events/1)},
               {:views, fold_if_applicable(views, &Fold.fold_views/1)}
             ]}
          end

        send(reply_to, {:folded, outcomes})
    end
  end

  # A refused input is NOT folded over its surviving subset. The entry point is reported
  # inapplicable and carries the refusal terms, so the caller attributes the outcome rather
  # than reading a partial fold as a whole-journal replay.
  defp fold_if_applicable({:applicable, input}, fun), do: fun.(input)
  defp fold_if_applicable({:inapplicable, refusals}, _fun), do: {:inapplicable, refusals}

  # The golden fold runs here, inside the traced process, so the summary oracle and the
  # effect spy observe the SAME fold rather than two different ones.
  defp golden_on_command do
    receive do
      {:golden, reply_to, lines, events, views} ->
        {:ok, from_lines} = Fold.fold_lines(lines)
        {:ok, from_events} = Fold.fold_events(events)
        {:ok, from_views} = Fold.fold_views(views)

        summaries = [
          {:lines, Fold.summary(from_lines)},
          {:events, Fold.summary(from_events)},
          {:views, Fold.summary(from_views)}
        ]

        send(reply_to, {:golden_folded, summaries})
    end
  end

  defp reject_on_command do
    receive do
      {:reject, reply_to, lines} -> send(reply_to, {:reject_folded, Fold.fold_lines(lines)})
    end
  end

  defp spawn_then_probe do
    receive do
      {:probe, reply_to} ->
        parent = self()

        child =
          spawn(fn ->
            _exists = File.exists?("/")
            send(parent, {:child_finished, self()})
          end)

        send(reply_to, {:child_started, child})

        receive do
          {:child_finished, ^child} -> send(reply_to, {:child_done, child})
        after
          10_000 -> send(reply_to, {:child_timeout, child})
        end
    end
  end

  defp control_on_command do
    receive do
      {:controls, reply_to, mfas} ->
        for {module, function, args} <- mfas, do: apply(module, function, args)
        send(reply_to, {:controlled, :ok})
    end
  end

  defp reader_on_command do
    receive do
      {:load, owner, dir, fs} -> send(owner, {:reader_loaded, Reader.load(dir, fs: fs)})
    end
  end

  # The traced act of the mutation controls. Each mutation is a DIRECT remote call, as replay
  # code would make it, and each result is returned so the control can assert it.
  defp mutate_on_command do
    receive do
      {:mutate, reply_to, calls} ->
        results = for {function, args} <- calls, do: mutate(function, args)
        send(reply_to, {:mutated, self(), results})
    end
  end

  defp mutate(:put, [key, value]), do: :persistent_term.put(key, value)
  defp mutate(:put_new, [key, value]), do: :persistent_term.put_new(key, value)
  defp mutate(:erase, [key]), do: :persistent_term.erase(key)

  defp spawn_then_mutate do
    receive do
      {:mutate_in_child, reply_to, key, value} ->
        parent = self()

        child =
          spawn(fn ->
            result = :persistent_term.put(key, value)
            send(parent, {:child_result, self(), result})
          end)

        send(reply_to, {:mutating_child, child})

        receive do
          {:child_result, ^child, result} -> send(reply_to, {:child_mutated, child, result})
        after
          10_000 -> send(reply_to, {:child_timeout, child})
        end
    end
  end

  defp read_on_command do
    receive do
      {:read, reply_to, key, missing} ->
        own = :persistent_term.get(key)
        default = :persistent_term.get(missing, :r11_absent)
        global_has_own? = Enum.any?(:persistent_term.get(), &match?({^key, _value}, &1))
        info = :persistent_term.info()
        send(reply_to, {:read_done, self(), {own, default, global_has_own?, info}})
    end
  end

  # The erase control needs a key to erase; the two writes need an absent one. Either way the
  # precondition is asserted before anything is armed.
  defp mutation_setup(:erase, key, value) do
    :ok = :persistent_term.put(key, value)
    assert :persistent_term.get(key) == value, "the key to erase was not pre-created"
    {[key], true}
  end

  defp mutation_setup(_write, key, value) do
    assert absent?(key), "the key to write already existed"
    {[key, value], :ok}
  end

  defp assert_mutation_effect(:erase, key, _value) do
    assert absent?(key), "the traced erase returned but the key is still present"
  end

  defp assert_mutation_effect(_write, key, value) do
    assert :persistent_term.get(key) == value,
           "the traced write returned but did not take effect"
  end

  # A key is this control's ONLY if it was absent when reserved. The name is unique per
  # reservation, and the check is a read made outside any trace.
  defp reserve_key do
    key = {__MODULE__, :r11, System.unique_integer([:positive])}
    assert absent?(key), "#{inspect(key)} already exists, so it is not this control's key"
    key
  end

  defp absent?(key) do
    sentinel = make_ref()
    :persistent_term.get(key, sentinel) == sentinel
  end

  # Registered after the collector exists and BEFORE any pattern is armed. ExUnit runs
  # `on_exit` callbacks last-registered-first, so this runs before the collector's own
  # callback; the ordering is therefore NOT left to registration order. This callback itself
  # requests cleanup and erases the key only on the collector's `:ok`. Any other status, or
  # no answer, raises and leaves the key in place: a surviving descendant could re-create it,
  # so its presence stays as evidence and nothing claims restoration.
  defp erase_key_after_cleanup(key, collector) do
    on_exit(fn ->
      send(collector, {:cleanup, self()})

      receive do
        {:cleanup_done, ^collector, :ok} ->
          :persistent_term.erase(key)
          if absent?(key), do: :ok, else: raise("control key #{inspect(key)} survived its erase")

        {:cleanup_done, ^collector, status} ->
          raise "traced tree cleanup incomplete (#{inspect(status)}); key #{inspect(key)} left as evidence"
      after
        @cleanup_reply_deadline ->
          raise "spy collector did not confirm cleanup in #{@cleanup_reply_deadline}ms; key #{inspect(key)} left"
      end
    end)
  end

  defp assert_mutations_disarmed(stage) do
    for mfa <- @global_state_mutations do
      assert :erlang.trace_info(mfa, :traced) == {:traced, false},
             "#{stage} left #{inspect(mfa)} armed"
    end
  end

  defp mutations_reported(calls) do
    Enum.filter(calls, &match?({:trace, _pid, :call, {:persistent_term, _function, _args}}, &1))
  end

  # Spawns a child ON COMMAND and then EXITS, leaving the child running. The command is what
  # lets a control decide exactly when - relative to a collector transition - the child comes
  # into existence.
  defp outliving_parent do
    receive do
      {:spawn_lingering, reply_to} ->
        child = spawn(fn -> idle() end)
        send(reply_to, {:lingering_child, child})
        :ok
    end
  end

  # A chain in which each link spawns the next and then EXITS, so the whole chain can only be
  # discovered through the trace rather than named by the caller. Only the leaf survives.
  # No claim is made about HOW MANY rounds that discovery takes: both links run to completion
  # immediately, so a single `await_down` may consume both spawn events.
  defp exiting_root(reporter) do
    receive do
      :begin ->
        mid = spawn(fn -> exiting_mid(reporter) end)
        send(reporter, {:chain_mid, self(), mid})
        :ok
    end
  end

  defp exiting_mid(reporter) do
    leaf = spawn(fn -> idle() end)
    send(reporter, {:chain_leaf, self(), leaf})
    :ok
  end

  defp never_returns do
    receive do
      {:begin, reply_to} ->
        send(reply_to, {:begun, self()})
        idle()
    end
  end

  # Waits for a message that is never sent. `:kill` is untrappable, so cleanup can still end
  # this process; nothing here depends on cooperation.
  defp idle do
    receive do
      :never_sent -> :ok
    end
  end

  # The module list is a parameter because the READER legitimately reads: arming File, :file
  # and :os there would fail on the operation the test exists to perform. The reader arms
  # @effect_modules only, and its mutation guarantee comes from the FaultFs seam plus the
  # FileRegistry spy, since FileRegistry writes bypass that seam entirely.
  #
  # `observer` is a TEST-ONLY seam. When set, the collector announces each convergence round
  # and waits - with a bound - for permission to take it, which is how a control acts at an
  # exact transition instead of inferring one afterwards.
  #
  # The three persistent_term mutations are armed on EVERY path through here, the reader's
  # included: none of them is an operation any tested path is allowed to perform.
  defp arm_spies(modules \\ @spied_modules, observer \\ nil) do
    start_spies(modules, @global_state_mutations, observer)
    arm_patterns(modules, @global_state_mutations)
  end

  # Split from arming so a control can register its own cleanup AFTER the collector exists and
  # BEFORE any pattern is armed.
  #
  # OWNERSHIP PRECHECK. Cleanup removes the exact mutation patterns, so each must be untraced
  # before we start: otherwise the removal would erase a pattern that is not ours. The check
  # runs before the collector exists, so a refusal arms nothing and removes nothing.
  defp start_spies(modules, mutations, observer) do
    for module <- modules, do: Code.ensure_loaded!(module)
    refuse_unowned_mutations(mutations)
    start_collector(modules, mutations, observer)
  end

  defp refuse_unowned_mutations(mutations) do
    for mfa <- mutations, :erlang.trace_info(mfa, :traced) != {:traced, false} do
      flunk("#{inspect(mfa)} is already traced by another owner; refusing to arm or remove it")
    end

    :ok
  end

  # Cleanup is already registered when this runs, so a failure part-way through arming still
  # removes every mutation pattern; removing one that was never armed is a no-op.
  defp arm_patterns(modules, mutations) do
    for module <- modules, do: :erlang.trace_pattern({module, :_, :_}, true, [:global])

    for mfa <- mutations do
      assert :erlang.trace_pattern(mfa, true, [:global]) == 1,
             "#{inspect(mfa)} matched no function, so it cannot be witnessed"

      assert :erlang.trace_info(mfa, :traced) == {:traced, :global},
             "#{inspect(mfa)} did not report a global call-trace pattern after arming"
    end

    :ok
  end

  # THE COLLECTOR IS THE TRACER. `{:tracer, Pid}` sends every trace message to the collector
  # instead of to the test process, which is what makes cleanup possible at all: when a test
  # fails, ITS MAILBOX IS GONE, so anything holding trace state there dies with it. The
  # collector survives, still holds the discovered pid set, the monitors and the events, and
  # can finish the owned-tree cleanup before removing patterns.
  #
  # It is UNLINKED on purpose. A linked collector would be killed by the very test failure
  # whose cleanup it exists to perform.
  #
  # It is started BEFORE any workload is dispatched, so no root can be armed and running
  # while the thing responsible for cleaning it up does not yet exist.
  defp start_collector(modules, mutations, observer) do
    owner = self()
    collector = spawn(fn -> collector_init(owner, modules, mutations, observer) end)
    Process.put(:spy_collector, collector)

    # SUPPLEMENTAL to the owner monitor, never the sole trigger. It may only REQUEST cleanup
    # and wait for the collector to report completion; it cannot reconstruct a vanished
    # mailbox and does not try. An INCOMPLETE cleanup is raised here rather than swallowed:
    # the collector reports what it could not finish and this is where that reaches ExUnit.
    on_exit(fn ->
      send(collector, {:cleanup, self()})

      receive do
        {:cleanup_done, ^collector, :ok} ->
          :ok

        {:cleanup_done, ^collector, status} ->
          raise "spy collector could not finish cleanup: #{inspect(status)}"
      after
        @cleanup_reply_deadline ->
          raise "spy collector did not confirm cleanup within #{@cleanup_reply_deadline}ms"
      end
    end)

    collector
  end

  defp collector do
    Process.get(:spy_collector) || flunk("spy collector was not started before dispatch")
  end

  # THE COLLECTOR CREATES, OWNS AND ARMS THE ROOT. The test never spawns a traced process
  # itself. Spawning in the test and registering afterwards leaves a window in which an
  # unlinked, running root is unknown to cleanup - if the test dies inside that window the
  # process leaks with the patterns still armed. Here the spawn, the adoption, the monitor
  # and the trace all happen inside one sequential step of the collector, so no window
  # exists, and the root stays PAUSED until arming has returned.
  defp spawn_owned(fun), do: spawn_owned_via(collector(), fun)

  defp spawn_owned_via(collector, fun) when is_function(fun, 0) do
    send(collector, {:spawn_owned, self(), fun})

    receive do
      {:spawned, ^collector, pid, armed} -> {pid, armed}
    after
      @cleanup_deadline ->
        flunk("spy collector did not create an owned root within #{@cleanup_deadline}ms")
    end
  end

  # A collector whose OWNER is a separate idle process, so a control can kill the owner while
  # the test process survives to make assertions.
  #
  # ORDER IS THE POINT. Both identities are obtained WITHOUT a handshake, and cleanup is
  # registered before any assertion - that ordering is what the previous form got wrong, and it
  # leaked the owner whenever its first assertion failed.
  #
  # Stated no more strongly than it holds: `spawn/1` is not infallible - the VM has a process
  # limit and can refuse - so this is registration before any ASSERTION, not before any
  # possible failure. In particular, a failure of the collector `spawn/1` itself is NOT handled
  # here; covering that would need an owner guard this helper does not have.
  #
  # No effect-module pattern is enabled: these controls are about process discovery, and arming
  # those would be global state the rest of the suite shares. `mutations` is explicit at every
  # call. A stand-in given `[]` arms nothing at all. One given the three mutation MFAs takes
  # them through the SAME ownership precheck as `start_spies/3`, registers cleanup, and only
  # then arms them, so the owner-death path is shown to remove patterns it really armed.
  defp standin_collector(observer, mutations) do
    refuse_unowned_mutations(mutations)
    owner = spawn(fn -> idle() end)
    collector = spawn(fn -> collector_init(owner, @effect_modules, mutations, observer) end)

    on_exit(fn ->
      case standin_cleanup(owner, collector) do
        :ok -> :ok
        other -> raise "stand-in collector could not finish cleanup: #{inspect(other)}"
      end
    end)

    arm_patterns([], mutations)
    {owner, collector}
  end

  # Unconditional and bounded, and it joins BOTH terminations under ONE overall deadline.
  #
  # `Process.exit/2` only SENDS a signal. An earlier form awaited the collector's cleanup
  # acknowledgment alone, which with an empty owned set can be produced and delivered before
  # that signal has killed the owner - so a following `Process.alive?/1` was a scheduling race
  # rather than an owner-death join. The monitor is therefore taken BEFORE the kill and its
  # exact `:DOWN` is awaited first. Either missing acknowledgment is surfaced by name.
  #
  # Safe to call twice, including for an already-dead owner: `Process.monitor/1` on a dead pid
  # delivers `:DOWN` with `:noproc` at once, `Process.exit/2` on one is a no-op, and a second
  # cleanup request is answered from `await_cleanup_request/2` with the RECORDED status, which
  # does NOT re-clear trace patterns. A control that has already acknowledged cleanup is not
  # undone by the `on_exit` copy.
  defp standin_cleanup(owner, collector) do
    deadline = System.monotonic_time(:millisecond) + @cleanup_reply_deadline
    ref = Process.monitor(owner)
    Process.exit(owner, :kill)
    send(collector, {:cleanup, self()})

    with :ok <- await_standin_down(owner, ref, deadline) do
      await_standin_cleanup(collector, deadline)
    end
  end

  defp await_standin_down(owner, ref, deadline) do
    receive do
      {:DOWN, ^ref, :process, ^owner, _reason} -> :ok
    after
      remaining(deadline) -> {:no_owner_death, owner}
    end
  end

  defp await_standin_cleanup(collector, deadline) do
    receive do
      {:cleanup_done, ^collector, status} -> status
    after
      remaining(deadline) -> :no_cleanup_report
    end
  end

  # The paused root does nothing at all until the collector releases it, which it does only
  # after `:erlang.trace/3` has returned.
  defp paused_root(collector, fun) do
    receive do
      {:run, ^collector} -> fun.()
    end
  end

  defp collector_init(owner, modules, mutations, observer) do
    ref = Process.monitor(owner)
    ctx = %{owner: owner, oref: ref, modules: modules, mutations: mutations, observer: observer}
    collector_loop(ctx, %{}, [])
  end

  # `owned` maps pid => {monitor_ref_or_nil, :live | :dead | :settled}.
  #
  #   :live    - monitored, its `:DOWN` has NOT been consumed
  #   :dead    - its `:DOWN` HAS been consumed; the reference is spent and can never fire again
  #   :settled - dead AND its post-exit trace delivery has been fenced
  #
  # Carrying this state explicitly is what lets a later stage - normally cleanup - pick up
  # where an earlier one stopped. An earlier form returned only the calls, so cleanup began
  # with an empty settled set and waited on monitor references a completed join had already
  # consumed, which could only ever expire.
  defp collector_loop(ctx, owned, calls) do
    %{owner: owner, oref: oref} = ctx

    receive do
      {:spawn_owned, from, fun} ->
        me = self()
        pid = spawn(fn -> paused_root(me, fun) end)
        owned = adopt(owned, [pid])
        armed = :erlang.trace(pid, true, [:call, :set_on_spawn, :procs, {:tracer, me}])
        send(pid, {:run, me})
        send(from, {:spawned, me, pid, armed})
        collector_loop(ctx, owned, calls)

      {:trace, _pid, :call, _mfa} = message ->
        collector_loop(ctx, owned, [message | calls])

      {:trace, _parent, :spawn, child, _mfa} when is_pid(child) ->
        collector_loop(ctx, adopt(owned, [child]), calls)

      {:join, from, roots, deadline} ->
        join(ctx, from, roots, deadline, owned, calls)

      {:cleanup, from} ->
        {status, _owned} = cleanup(ctx, owned)
        send(from, {:cleanup_done, self(), status})
        await_cleanup_request(self(), status)

      {:DOWN, ^oref, :process, ^owner, _reason} ->
        {status, _owned} = cleanup(ctx, owned)
        await_cleanup_request(self(), status)

      # `:procs` also announces exits, links and registrations. They carry nothing this
      # collector uses - exits are observed through monitors - but they must be consumed so
      # the mailbox stays bounded and the selective receives below stay cheap.
      {:trace, _pid, _event, _a} ->
        collector_loop(ctx, owned, calls)

      {:trace, _pid, _event, _a, _b} ->
        collector_loop(ctx, owned, calls)
    end
  end

  # A join NEVER kills: the workload is supposed to finish on its own and killing it would
  # destroy the evidence the test is about to read.
  defp join(ctx, from, roots, deadline, owned, calls) do
    case converge(ctx, adopt(owned, roots), calls, deadline, false) do
      {:ok, owned, calls} ->
        send(from, {:joined, self(), Enum.reverse(calls)})
        collector_loop(ctx, owned, [])

      # The convergence budget expired. Report it FIRST, inside the budget the caller gave us,
      # then clean up under a fresh one - an earlier form raised here instead, which killed the
      # collector while tracees were still live and left the global patterns armed.
      {{:timeout, reason}, owned, _calls} ->
        send(from, {:join_failed, self(), reason})
        {status, _owned} = cleanup(ctx, owned)
        await_cleanup_request(self(), status)

      # The test abandoned the join by dying. Nobody will read a result, so clean up the owned
      # tree - as evolved by this partial convergence - instead of replying into a vanished
      # mailbox.
      {:owner_down, owned, _calls} ->
        {status, _owned} = cleanup(ctx, owned)
        await_cleanup_request(self(), status)
    end
  end

  # Cleanup has already run by the time this is reached; `on_exit` still asks, and a control
  # may ask a second time. Answer every request with the SAME recorded status until the
  # mailbox goes quiet, so no asker blocks on a collector that has finished its work.
  defp await_cleanup_request(collector, status) do
    receive do
      {:cleanup, from} ->
        send(from, {:cleanup_done, collector, status})
        await_cleanup_request(collector, status)
    after
      @cleanup_deadline -> :ok
    end
  end

  # BOUNDED owned-tree cleanup, in this order and no other: kill, discover and fence to a
  # fixed point, and ONLY THEN remove the trace patterns. Disarming first would stop the spawn
  # events that discovery depends on, so the tree would be declared clean on the strength of
  # having stopped looking.
  #
  # Only OWNED pids are touched. There is no sweep over unrelated processes.
  #
  # The patterns are removed on EVERY path, including an incomplete one. A cleanup that could
  # not finish reports `{:incomplete, reason, live}` rather than leaving the suite armed.
  defp cleanup(ctx, owned) do
    deadline = System.monotonic_time(:millisecond) + @cleanup_deadline
    {status, owned} = cleanup_converge(ctx, owned, deadline)
    for module <- ctx.modules, do: :erlang.trace_pattern({module, :_, :_}, false, [:global])
    for mfa <- ctx.mutations, do: :erlang.trace_pattern(mfa, false, [:global])
    {status, owned}
  end

  defp cleanup_converge(ctx, owned, deadline) do
    case converge(ctx, owned, [], deadline, true) do
      {:ok, owned, _calls} ->
        {:ok, owned}

      # Owner death is CONSUMED AS STATE, never a successful early return. An earlier form
      # caught it and returned `:ok`, which removed the patterns and declared the tree clean
      # because it had stopped walking - the observer stopping is not the tree being empty.
      # The monitor fires once, so this recursion is bounded by that single delivery.
      {:owner_down, owned, _calls} ->
        cleanup_converge(ctx, owned, deadline)

      {{:timeout, reason}, owned, _calls} ->
        {{:incomplete, reason, live_pids(owned)}, owned}
    end
  end

  # DISCOVER, JOIN and FENCE to a FIXED POINT.
  #
  # WHY THIS TERMINATES AND IS COMPLETE. A DEAD PID CANNOT SPAWN, and `trace_delivered(Pid)`
  # delivers every trace message that pid had already generated. So once a pid is dead AND
  # fenced, every spawn event it ever produced is already in our hands. Consuming those events
  # yields its children; each child gets identical treatment. When every discovered pid is
  # dead, fenced, and the spawn events delivered by that fence have been consumed without
  # yielding an unknown pid, NO UNDISCOVERED PROCESS CAN EXIST. Each round settles one pid and
  # the set of processes actually spawned is finite, so the loop converges.
  #
  # NO FINAL `:all` SWEEP IS NEEDED, and none is performed.
  #
  # Discovery requires `:procs` on the trace flags. `:set_on_spawn` propagates flags to a
  # child but ANNOUNCES NOTHING; `{:trace, Parent, :spawn, Child, _}` only arrives with
  # `:procs`, and without it this loop would converge immediately on the roots and report
  # completeness it had not established.
  #
  # `kill?` is the cleanup mode. It kills EACH pid as the loop reaches it, including
  # descendants adopted in this very walk, so a child that outlasts its parent is killed in
  # the round that discovers it rather than waited on until the budget expires.
  #
  # THE BUDGET IS CHECKED HERE, BEFORE ANY ROUND, not only in the `after` clauses below. A
  # queue of matching messages would otherwise let a stage consume its way to a successful
  # result after its budget had already expired.
  #
  # Every return carries the EVOLVED `owned` map, on success and on both failures, so no
  # descendant adopted in an earlier round is ever lost by a later transition.
  defp converge(ctx, owned, calls, deadline, kill?) do
    if expired?(deadline) do
      {{:timeout, "convergence budget expired with #{length(unsettled(owned))} pid(s) unsettled"}, owned, calls}
    else
      converge_round(ctx, owned, calls, deadline, kill?)
    end
  end

  defp converge_round(ctx, owned, calls, deadline, kill?) do
    case Enum.find(owned, fn {_pid, {_ref, state}} -> state != :settled end) do
      nil ->
        {:ok, owned, calls}

      {pid, {ref, :live}} ->
        step(ctx.observer, {pid, :live, Map.keys(owned)}, deadline)
        if kill?, do: Process.exit(pid, :kill)

        case await_down(ctx, pid, ref, calls, [], deadline) do
          {:ok, calls, kids} ->
            owned = owned |> adopt(kids) |> Map.put(pid, {nil, :dead})
            converge(ctx, owned, calls, deadline, kill?)

          {status, calls, kids} ->
            {status, adopt(owned, kids), calls}
        end

      {pid, {_ref, :dead}} ->
        step(ctx.observer, {pid, :dead, Map.keys(owned)}, deadline)

        case fence(ctx, pid, calls, [], deadline) do
          {:ok, calls, kids} ->
            owned = owned |> adopt(kids) |> Map.put(pid, {nil, :settled})
            converge(ctx, owned, calls, deadline, kill?)

          {status, calls, kids} ->
            {status, adopt(owned, kids), calls}
        end
    end
  end

  # TEST-ONLY observation seam. It is BOUNDED on purpose: an observer that has died - which is
  # exactly what happens when the test whose cleanup this is fails - must never be able to
  # wedge that cleanup.
  #
  # It is NOT an unconditional hold and NOT a wall-clock ceiling on the stage. It waits at most
  # the smaller of its own bound and what REMAINS of the stage budget, so an observation can
  # never push a stage past its deadline; and on expiry it PROCEEDS WITHOUT ACKNOWLEDGMENT.
  # Because a control that was not actually held could otherwise still pass by coincidence, the
  # collector announces that it gave up, and a stepped control refutes having received such an
  # announcement at the moment it acts.
  defp step(nil, _event, _deadline), do: :ok

  defp step(observer, event, deadline) do
    send(observer, {:round, self(), event})

    receive do
      {:continue, ^observer} ->
        :ok
    after
      min(@control_deadline, remaining(deadline)) ->
        send(observer, {:round_unheld, self(), event})
        :ok
    end
  end

  # A child discovered here may already be dead; `Process.monitor/1` on a dead pid delivers
  # `:DOWN` with `:noproc` immediately, so it settles on its next round rather than hanging.
  defp adopt(owned, kids) do
    Enum.reduce(kids, owned, fn kid, acc ->
      if Map.has_key?(acc, kid), do: acc, else: Map.put(acc, kid, {Process.monitor(kid), :live})
    end)
  end

  defp live_pids(owned), do: for({pid, {_ref, :live}} <- owned, do: pid)

  defp unsettled(owned), do: for({pid, {_ref, state}} <- owned, state != :settled, do: pid)

  # The owner can die at any point while we wait. That is returned as a STATUS carrying the
  # calls and the kids found so far, never thrown: a throw unwinds past the caller that holds
  # the evolved `owned` map, so descendants adopted in earlier rounds would be lost and the
  # outer handler would rebuild from a stale set.
  #
  # The deadline is checked BEFORE the receive as well as inside it. `receive ... after 0`
  # consumes every matching message first, so a busy tracee could postpone the timeout
  # indefinitely and a finite queue could settle the pid after the budget had already gone.
  #
  # `:kill` during cleanup makes the reason `:killed` rather than `:normal`; the reason is
  # deliberately not inspected, because a dead pid satisfies the invariant however it died.
  defp await_down(ctx, pid, ref, calls, kids, deadline) do
    %{owner: owner, oref: oref} = ctx

    if expired?(deadline) do
      {down_timeout(pid), calls, kids}
    else
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} ->
          {:ok, calls, kids}

        {:DOWN, ^oref, :process, ^owner, _reason} ->
          {:owner_down, calls, kids}

        {:trace, _pid, :call, _mfa} = message ->
          await_down(ctx, pid, ref, [message | calls], kids, deadline)

        {:trace, _parent, :spawn, child, _mfa} when is_pid(child) ->
          await_down(ctx, pid, ref, calls, [child | kids], deadline)

        {:trace, _pid, _event, _a} ->
          await_down(ctx, pid, ref, calls, kids, deadline)

        {:trace, _pid, _event, _a, _b} ->
          await_down(ctx, pid, ref, calls, kids, deadline)
      after
        remaining(deadline) -> {down_timeout(pid), calls, kids}
      end
    end
  end

  defp fence(ctx, pid, calls, kids, deadline) do
    ref = :erlang.trace_delivered(pid)
    drain_to_fence(ctx, pid, ref, calls, kids, deadline)
  end

  defp drain_to_fence(ctx, pid, ref, calls, kids, deadline) do
    %{owner: owner, oref: oref} = ctx

    if expired?(deadline) do
      {fence_timeout(pid), calls, kids}
    else
      receive do
        {:trace_delivered, ^pid, ^ref} ->
          {:ok, calls, kids}

        {:DOWN, ^oref, :process, ^owner, _reason} ->
          {:owner_down, calls, kids}

        {:trace, _pid, :call, _mfa} = message ->
          drain_to_fence(ctx, pid, ref, [message | calls], kids, deadline)

        {:trace, _parent, :spawn, child, _mfa} when is_pid(child) ->
          drain_to_fence(ctx, pid, ref, calls, [child | kids], deadline)

        {:trace, _pid, _event, _a} ->
          drain_to_fence(ctx, pid, ref, calls, kids, deadline)

        {:trace, _pid, _event, _a, _b} ->
          drain_to_fence(ctx, pid, ref, calls, kids, deadline)
      after
        remaining(deadline) -> {fence_timeout(pid), calls, kids}
      end
    end
  end

  # The budget named in these messages is the CALLER's, not @cleanup_deadline: a lifecycle
  # control supplies a small one deliberately, and naming the module default would report a
  # number that was never actually waited.
  defp down_timeout(pid), do: {:timeout, "budget expired: owned tracee #{inspect(pid)} did not exit in time"}

  defp fence_timeout(pid), do: {:timeout, "budget expired: trace delivery fence for #{inspect(pid)} did not complete"}

  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  # Clamped at zero so an already-expired budget fails immediately and explicitly, rather
  # than passing a negative timeout to `receive`.
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  # The TEST side of a join is only a request. All trace, discovery, monitor and fence state
  # lives in the collector, because only the collector still exists when a test fails.
  defp join_tracees(roots) do
    case request_join(roots, @cleanup_deadline) do
      {:ok, calls} ->
        calls

      {:join_failed, reason} ->
        flunk("spy collector could not converge: #{reason}")

      {:no_answer, waited} ->
        flunk("spy collector did not answer the join within #{waited}ms")
    end
  end

  # Returns the failure instead of raising it, so a control can assert that a convergence
  # failure is REPORTED rather than silently absorbed.
  defp request_join(roots, budget) do
    request_join_at(roots, System.monotonic_time(:millisecond) + budget, budget)
  end

  # The deadline is supplied directly so a control can hand the collector one that has ALREADY
  # passed, which is the only way to test budget enforcement without a sleep or a timing
  # inference.
  defp request_join_at(roots, deadline, wait) do
    collector = collector()
    send(collector, {:join, self(), roots, deadline})

    receive do
      {:joined, ^collector, calls} -> {:ok, calls}
      {:join_failed, ^collector, reason} -> {:join_failed, reason}
    after
      wait + @reply_grace -> {:no_answer, wait + @reply_grace}
    end
  end

  # Waits until a matching `:DOWN` is PROVABLY queued at the collector, by reading the
  # collector's own mailbox rather than assuming delivery. This is an observation, not a
  # timing argument: the loop ends on the condition holding, and fails on the budget.
  defp await_queued_down(collector, pid) do
    deadline = System.monotonic_time(:millisecond) + @control_deadline
    await_queued_down(collector, pid, deadline)
  end

  defp await_queued_down(collector, pid, deadline) do
    queued =
      case Process.info(collector, :messages) do
        {:messages, messages} ->
          Enum.any?(messages, &match?({:DOWN, _ref, :process, ^pid, _reason}, &1))

        nil ->
          flunk("the spy collector is no longer alive")
      end

    cond do
      queued ->
        :ok

      expired?(deadline) ->
        flunk(
          "no #{inspect(pid)} DOWN was queued at the collector, so the expired-budget " <>
            "control would not have a matching message to consume"
        )

      true ->
        await_queued_down(collector, pid, deadline)
    end
  end

  # Holds the collector at the first round matching `match?`, WITHOUT releasing it, so the
  # caller can act at exactly that transition. Every earlier round is released as it arrives.
  defp await_round(collector, match_fun) do
    receive do
      {:round, ^collector, event} ->
        if match_fun.(event) do
          event
        else
          send(collector, {:continue, self()})
          await_round(collector, match_fun)
        end
    after
      @cleanup_reply_deadline ->
        flunk("the spy collector did not reach the awaited round")
    end
  end

  defp drain_rounds_until_cleanup(collector) do
    receive do
      {:round, ^collector, _event} ->
        send(collector, {:continue, self()})
        drain_rounds_until_cleanup(collector)

      {:cleanup_done, ^collector, status} ->
        status
    after
      @cleanup_reply_deadline ->
        flunk("the spy collector did not report cleanup within #{@cleanup_reply_deadline}ms")
    end
  end

  # Decoding and validating happen HERE, in the untraced test process: the spy is armed on
  # the fold itself, not on the reading of the fixtures.
  #
  # Parse and validation refusals are RETAINED, never filtered. An earlier form used
  # `for line <- lines, {:ok, e} <- [Jason.decode(line)], do: e`, which silently DROPS every
  # line that fails - so a journal with refusals was folded over the surviving subset and its
  # green result misdescribed a partial replay as a whole one. A later form kept counts, which
  # recorded the CARDINALITY of the loss but still folded the subset.
  #
  # Now an entry point whose input was refused at all is marked `{:inapplicable, refusals}`
  # and is not folded. 34 of the 49 corpus journals are rejection fixtures, so refusals are
  # expected; what is forbidden is calling the surviving subset a replay.
  defp corpus do
    for dir <- journal_dirs() do
      lines = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)

      decoded = Enum.map(lines, &Jason.decode/1)
      validated = Enum.map(lines, &Event.validate_line/1)

      decode_refusals = refusals(decoded)
      validate_refusals = refusals(validated)

      # Successes are COUNTED, never derived by subtracting refusals from the line count.
      # Deriving them would make the caller's `decoded + decode_refusals == lines`
      # reconciliation arithmetically true no matter what these lists contained - a check
      # that cannot fail. Counted independently, it fails the moment the result list stops
      # accounting for every line, which is exactly the regression the earlier filtering
      # form introduced.
      counts = %{
        lines: length(lines),
        decoded: Enum.count(decoded, &match?({:ok, _}, &1)),
        decode_refusals: length(decode_refusals),
        validated: Enum.count(validated, &match?({:ok, _}, &1)),
        validate_refusals: length(validate_refusals)
      }

      {Path.basename(dir), lines, entrypoint_input(decoded, decode_refusals),
       entrypoint_input(validated, validate_refusals), counts}
    end
  end

  # Anything that is not `{:ok, _}` is a refusal, carried with the index of the line that
  # produced it. Matching on `{:error, _}` alone would silently drop an unrecognised shape.
  defp refusals(results) do
    for {result, index} <- Enum.with_index(results), not match?({:ok, _}, result) do
      {index, result}
    end
  end

  defp entrypoint_input(results, []), do: {:applicable, for({:ok, value} <- results, do: value)}
  defp entrypoint_input(_results, refusals), do: {:inapplicable, refusals}

  defp journal_dirs do
    @fixture_root
    |> Path.join("**/events.jsonl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(&Path.dirname/1)
  end
end
