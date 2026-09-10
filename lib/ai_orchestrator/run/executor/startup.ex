defmodule AiOrchestrator.Run.Executor.Startup do
  @moduledoc """
  The bounded startup leg of ONE run subtree (docs/contracts/core-startup-bound.org), consumed identically by the
  foreground `AiOrchestrator.Run.Executor.Owner` and the mounted `AiOrchestrator.Host.RunOwner`.

  Three processes: the OWNER (the caller of `begin/2`) fixes one absolute monotonic deadline at its own birth
  (`budgets.birth + budgets.startup`) and stays responsive; the HELPER (spawned here, linked to and monitored by the
  owner) is the responsive reaper that never makes a synchronous call, acknowledges Writer births while the owner
  lives, and reaps every known identity on the owner's DOWN or an abort; the STARTER, which the helper ADOPTS by
  link and monitor as its first action, is the only process that blocks: it is the OTP parent of `Run.Supervisor`,
  retained for the whole run,
  performs the synchronous start (`Owner.start_subtree/1`, the `journal_exists` retry inside) and the discovery
  (`Owner.facts/1`), and does NO synchronous work before it holds a permit granted inside the deadline.

  Identity before work: the starter announces `{:startup_identity, ref, starter}`; the owner records it, then
  `permit/1` grants only while `now <= deadline` and denies (aborting through the helper) otherwise; the starter
  re-checks the same absolute clock when it consumes the permit. Every completion reaches the owner as
  `{:startup_started, ref, completion}` and is compared with the deadline at ACCEPTANCE (`accept/2`); a completion
  accepted late loses and its born tree is collected. No completion at all is the owner's own expiry
  (`abort/2` with `:deadline`). Timer messages are wake-ups, never authority: every guard re-reads the clock.

  Forced teardown (expiry, caller death, abort, stop) kills the Writer identities FIRST (acknowledged and
  discovered), then the supervisor, then born children, then the starter, every kill joined; a survivor is counted,
  never equated with success. The ownership diagnostic on every report is CONDITIONAL and never asserted: `:none`
  when no Writer was ever acknowledged, `:reclaimable` when the arbiter reports a `:down` registration (or a `:live`
  one naming a dead pid) after the kill, `:unknown` when the bounded observation is unavailable. No lock is deleted
  and nothing is recovered here: recovery is a fresh command's Writer reclaiming through the arbiter.

  The helper and the starter are `proc_lib` processes of this module so a census by `$initial_call` finds them.
  """

  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Executor.Owner

  @startup 45_000
  @ack 5_000
  @observe 500
  @join 5_000
  @stop 15_000
  @grace 500

  @typedoc """
  Startup budgets in milliseconds. The keys this seam reads: `:startup` (the absolute owner-birth bound), `:ack`
  (the Writer birth acknowledgment), `:birth` (the owner's own monotonic birth, default now), `:stop` and `:join`
  (the teardown bounds) and `:join_fn` (the join seam). An owner's other budget keys - its `:close`, for one - are
  carried through untouched, so an owner passes its whole budget map.
  """
  @type budgets :: %{optional(atom()) => term()}

  @typedoc "The startup handle the owner holds; `deadline` is absolute monotonic milliseconds."
  @type startup :: %{
          ref: reference(),
          helper: pid(),
          starter: pid(),
          deadline: integer(),
          budgets: map(),
          owner: pid(),
          helper_monitor: reference(),
          config: map()
        }

  @typedoc "Every abort/reap/teardown report."
  @type report :: %{
          required(:why) => term(),
          required(:phase) => atom(),
          required(:ownership) => :none | :reclaimable | :unknown,
          required(:joined) => non_neg_integer(),
          required(:survivors) => non_neg_integer(),
          optional(:starter_reaped) => true
        }

  @doc "The default startup budgets: 45 s owner-birth startup bound, 5 s Writer birth acknowledgment."
  @spec default_budgets() :: %{startup: pos_integer(), ack: pos_integer()}
  def default_budgets, do: %{startup: @startup, ack: @ack}

  @doc """
  Spawns both seam processes and RETURNS AT ONCE, waiting for nothing: no message from either process is awaited
  here, so the owner arms its own absolute expiry with no birth gap in front of it and is responsive from its first
  instant (review R3). The starter is spawned UNLINKED by this call, so the owner never holds a link to it, and the
  helper adopts it - link and monitor - as its own first action, which is what makes the reaper the starter's
  keeper and gives the chain owner -> helper -> starter -> `Run.Supervisor`. The starter does nothing at all until
  the owner permits its identity, so an adoption that has not happened yet can cost no work. `budgets.birth` is the
  owner's own monotonic birth (default now); the deadline is `birth + startup`.
  """
  @spec begin(map(), budgets()) :: {:ok, startup()}
  def begin(%{} = config, %{} = budgets) do
    budgets = Map.merge(default_budgets(), budgets)
    deadline = Map.get(budgets, :birth, now()) + budgets.startup
    ref = make_ref()
    owner = self()
    helper_config = Map.merge(config, %{writer_birth: {:helper, ref}, writer_ack: budgets.ack})
    starter = :proc_lib.spawn(__MODULE__, :starter_init, [%{owner: owner, ref: ref, deadline: deadline}])

    helper =
      :proc_lib.spawn_link(__MODULE__, :helper_init, [
        %{owner: owner, ref: ref, config: helper_config, starter: starter, deadline: deadline, budgets: budgets}
      ])

    {:ok,
     %{
       ref: ref,
       helper: helper,
       starter: starter,
       deadline: deadline,
       budgets: budgets,
       owner: owner,
       helper_monitor: Process.monitor(helper),
       config: config
     }}
  end

  @doc """
  Grants the starter its permit only while `now <= deadline` (the owner records the starter BEFORE calling this);
  otherwise denies it and aborts through the helper, whose `{:startup_aborted, ref, report}` follows.
  """
  @spec permit(startup()) :: :ok | {:error, :late_identity}
  def permit(%{ref: ref, starter: starter, helper: helper, deadline: deadline}) when is_pid(starter) do
    if now() <= deadline do
      send(starter, {:permit, ref})
      send(helper, {:startup_permitted, ref})
      :ok
    else
      send(starter, {:deny, ref})
      send(helper, {:abort, ref, :late_identity, :async})
      {:error, :late_identity}
    end
  end

  @doc """
  The acceptance-time comparison: a completion accepted after the deadline loses (`run_startup_timeout` with
  `:late_result_after_deadline` / `:late_error_after_deadline`, the late class recorded, the born tree collected);
  a timely completion answers itself.
  """
  @spec accept(startup(), {:startup_started, reference(), term()}) ::
          {:ok, pid(), map()} | {:error, map()}
  def accept(%{ref: ref, deadline: deadline} = startup, {:startup_started, ref, completion}) do
    late? = now() > deadline

    case completion do
      {:ok, _sup, _facts} when late? ->
        late(startup, :late_result_after_deadline, %{})

      {:error, rejection} when late? ->
        late(startup, :late_error_after_deadline, %{late_class: late_class(rejection)})

      {:ok, sup, facts} ->
        {:ok, sup, facts}

      {:error, rejection} ->
        {:error, rejection}
    end
  end

  defp late_class(%{clause: clause}), do: clause
  defp late_class(_rejection), do: nil

  defp late(startup, why, extra) do
    report = abort(startup, why)
    {:error, Map.merge(timeout_result(report), extra)}
  end

  @doc "The closed `run_startup_timeout` result map built from a report."
  @spec timeout_result(report()) :: map()
  def timeout_result(%{why: why, phase: phase, ownership: ownership, joined: joined} = report) do
    result = %{clause: "run_startup_timeout", why: why, phase: phase, ownership: ownership, joined: joined}
    if report.survivors > 0, do: Map.put(result, :survivors, report.survivors), else: result
  end

  @doc """
  Forced teardown through the helper (expiry `:deadline`, `:caller_gone`, a stop, a late completion): the helper
  reaps and joins, the caller receives the report, then joins the helper. A helper that is already gone (or dies
  during the abort) leaves the mirror duty to the owner, through the starter it holds.
  """
  @spec abort(startup(), term()) :: report()
  def abort(%{helper: helper, ref: ref} = startup, why) do
    tref = make_ref()
    monitor = Process.monitor(helper)
    send(helper, {:abort, ref, why, {:sync, self(), tref}})

    report =
      receive do
        {:aborted, ^tref, report} ->
          report

        {:DOWN, ^monitor, :process, ^helper, _reason} ->
          mirror(startup, why)
      after
        bound(startup.budgets) ->
          Process.exit(helper, :kill)
          mirror(startup, why)
      end

    Process.demonitor(monitor, [:flush])
    join_helper(startup)
    report
  end

  @doc """
  Waits for the helper's ASYNCHRONOUS abort report (a denied permit, a starter that refused an expired permit),
  then joins the helper. A helper that dies or stays silent leaves the mirror duty to the owner.
  """
  @spec await_report(startup()) :: report()
  def await_report(%{ref: ref, helper: helper} = startup) do
    monitor = Process.monitor(helper)

    report =
      receive do
        {:startup_aborted, ^ref, report} ->
          report

        {:DOWN, ^monitor, :process, ^helper, _reason} ->
          mirror(startup, :helper_down)
      after
        bound(startup.budgets) ->
          Process.exit(helper, :kill)
          mirror(startup, :helper_down)
      end

    Process.demonitor(monitor, [:flush])
    join_helper(startup)
    report
  end

  @doc "Joins the helper after a report the owner already received, so no link or DOWN survives into its terminal."
  @spec join(startup()) :: :ok
  def join(startup), do: join_helper(startup)

  @doc """
  The owner's mirror duty after the helper's DOWN: reap through the starter (its linked `Run.Supervisor`, that
  supervisor's Writer and born children), the starter last. Before the permit the responsive starter exits on its
  own (a short grace is given, `starter_reaped` is set only when a kill was needed).
  """
  @spec reap(startup()) :: report()
  def reap(%{helper: helper} = startup) do
    reason =
      receive do
        {:DOWN, _monitor, :process, ^helper, reason} -> reason
      after
        0 ->
          receive do
            {:EXIT, ^helper, reason} -> reason
          after
            0 -> if Process.alive?(helper), do: :alive, else: :noproc
          end
      end

    if reason == :alive, do: Process.exit(helper, :kill)
    report = mirror(startup, {:helper_down, reason})
    join_helper(startup)
    report
  end

  @doc """
  The normal end: the helper runs the shared orderly teardown (`Owner.teardown/3`) over every identity it holds
  (the owner's additions through `own/2` included), releases the starter, and both are joined. A helper that is
  already gone owns nothing; one that dies during the teardown leaves the mirror duty to the owner.
  """
  @spec teardown(startup(), map()) :: :ok | {:error, map()}
  def teardown(%{helper: helper, ref: ref} = startup, %{} = budgets) do
    budgets = Map.merge(%{stop: @stop, join: @join}, budgets)
    tref = make_ref()
    monitor = Process.monitor(helper)
    send(helper, {:teardown, ref, budgets, self(), tref})

    # A helper that is gone - including one that was already dead when this monitor was taken, which answers
    # :noproc - has established NOTHING about the subtree: the starter traps its exit and goes on holding a live
    # Run.Supervisor. Every path where the helper does not answer runs the owner's own mirror duty through the
    # starter and reports what it joined; a missing reaper is never cleanup success (review R2).
    outcome =
      receive do
        {:torn, ^tref, outcome} ->
          outcome

        {:DOWN, ^monitor, :process, ^helper, _reason} ->
          incomplete(mirror(%{startup | budgets: Map.merge(startup.budgets, budgets)}, :helper_down))
      after
        bound(budgets) ->
          Process.exit(helper, :kill)
          incomplete(mirror(%{startup | budgets: Map.merge(startup.budgets, budgets)}, :helper_down))
      end

    Process.demonitor(monitor, [:flush])
    join_helper(startup)
    outcome
  end

  @doc "Adds identities the owner learned after the start (the registered worker) to what the helper tears down."
  @spec own(startup(), map()) :: :ok
  def own(%{helper: helper, ref: ref}, %{} = identities) do
    send(helper, {:own, ref, identities})
    :ok
  end

  defp incomplete(%{survivors: 0}), do: :ok
  defp incomplete(%{survivors: n}), do: {:error, %{clause: "run_executor_teardown_incomplete", survivors: n}}

  # the wait for a helper's answer: several sequential joins, the observation and the grace, never less than the
  # orderly stop bound
  defp bound(budgets) do
    join = Map.get(budgets, :join, @join)
    stop = Map.get(budgets, :stop, @stop)
    max(join * 6 + @observe + @grace, stop + join * 2)
  end

  # the owner consumes its own monitor and link on the helper: no DOWN, EXIT or link survives into its terminal
  defp join_helper(%{helper: helper, helper_monitor: original, owner: owner, budgets: budgets}) do
    monitor = Process.monitor(helper)

    receive do
      {:DOWN, ^monitor, :process, ^helper, _reason} -> :ok
    after
      Map.get(budgets, :join, @join) ->
        Process.exit(helper, :kill)
        receive(do: ({:DOWN, ^monitor, :process, ^helper, _} -> :ok))
    end

    if self() == owner, do: Process.demonitor(original, [:flush])
    Process.unlink(helper)
    receive(do: ({:EXIT, ^helper, _} -> :ok), after: (0 -> :ok))
  end

  # ---- the HELPER: responsive reaper, no synchronous call ever ----

  @doc false
  def helper_init(%{owner: owner, ref: ref, config: config, starter: starter, deadline: deadline, budgets: budgets}) do
    Process.flag(:trap_exit, true)
    owner_monitor = Process.monitor(owner)
    # ADOPTION, before anything else: the reaper links and monitors the starter it was handed, and only then does
    # the starter hold anything of this reaper's. The Writer's birth reaper is this process.
    Process.link(starter)
    starter_monitor = Process.monitor(starter)
    config = %{config | writer_birth: {self(), ref}}
    send(starter, {:startup_config, ref, self(), config})
    _ = deadline

    helper_loop(%{
      owner: owner,
      owner_monitor: owner_monitor,
      ref: ref,
      config: config,
      budgets: budgets,
      starter: starter,
      starter_monitor: starter_monitor,
      phase: :handshake,
      writers: [],
      owned: %{}
    })
  end

  defp helper_loop(%{ref: ref, owner_monitor: owner_monitor, starter_monitor: starter_monitor} = s) do
    receive do
      # a Writer birth is acknowledged only while the owner lives and the startup is still in flight
      {:run_writer_born, ^ref, writer} ->
        if Process.alive?(s.owner) and s.phase in [:handshake, :starting] do
          send(writer, {:run_writer_ack, ref})
          helper_loop(%{s | writers: [writer | s.writers]})
        else
          send(writer, {:run_writer_abort, ref})
          helper_loop(s)
        end

      {:startup_permitted, ^ref} ->
        helper_loop(%{s | phase: :starting})

      {:starter_supervisor, ^ref, sup} ->
        helper_loop(%{s | owned: Map.put(s.owned, :supervisor, sup)})

      {:starter_completed, ^ref, {:ok, sup, facts}} ->
        send(s.owner, {:startup_started, ref, {:ok, sup, facts}})
        helper_loop(%{s | phase: :running, owned: Map.merge(s.owned, Map.put(facts, :supervisor, sup))})

      # a failed start owns nothing once a root born before a failed discovery is collected; the helper is done
      {:starter_completed, ^ref, {:error, _} = error} ->
        _ = if is_pid(s.owned[:supervisor]), do: reap_tree(%{s | phase: :failed}, :start_failed)
        send(s.owner, {:startup_started, ref, error})
        exit(:normal)

      # a timely permit consumed late: the starter refused it; the startup is over with nothing acquired
      {:starter_aborted, ^ref, why} ->
        report = reap_tree(s, why)
        send(s.owner, {:startup_aborted, ref, report})
        exit(:normal)

      {:DOWN, ^owner_monitor, :process, _owner, _reason} ->
        _ = reap_tree(s, :owner_down)
        exit(:normal)

      {:DOWN, ^starter_monitor, :process, _starter, _reason} ->
        helper_loop(%{s | starter: nil})

      {:abort, ^ref, why, reply} ->
        report = reap_tree(s, why)

        case reply do
          {:sync, from, tref} -> send(from, {:aborted, tref, report})
          :async -> send(s.owner, {:startup_aborted, ref, report})
        end

        exit(:normal)

      {:teardown, ^ref, budgets, from, tref} ->
        send(from, {:torn, tref, orderly(s, budgets)})
        exit(:normal)

      {:own, ^ref, identities} ->
        helper_loop(%{s | owned: Map.merge(s.owned, identities)})

      _other ->
        helper_loop(s)
    end
  end

  # the shared orderly teardown over every identity held, then the starter is released (joined, killed if it stays)
  defp orderly(s, budgets) do
    join = join_fn(Map.merge(s.budgets, budgets))
    owned = s.owned |> Map.put_new(:writer, List.first(s.writers)) |> Map.put(:writers, s.writers)
    outcome = Owner.teardown(owned, budgets, join)
    release_starter(s.starter, s.ref, Map.get(budgets, :join, @join))
    outcome
  end

  defp release_starter(nil, _ref, _join), do: :ok

  defp release_starter(starter, ref, join) do
    monitor = Process.monitor(starter)
    send(starter, {:release, ref})

    receive do
      {:DOWN, ^monitor, :process, ^starter, _} -> :ok
    after
      join ->
        Process.exit(starter, :kill)
        receive(do: ({:DOWN, ^monitor, :process, ^starter, _} -> :ok), after: (join -> :ok))
    end
  end

  # ---- the forced reap (helper on owner DOWN / abort; owner mirror through the starter) ----

  # Writer identities first (acknowledged and discovered), the bounded ownership observation and the trace, then
  # the supervisor and the born children, the starter last; every kill joined under the join seam
  defp reap_tree(s, why) do
    join = join_fn(s.budgets)
    join_ms = Map.get(s.budgets, :join, @join)
    walk = walk(s.starter)
    writers = pids([s.owned[:writer] | s.writers] ++ walk.writers)
    # The Writer identities die FIRST and their deaths are JOINED, so the diagnostic that follows rests on two
    # observations and never on an intention (review R4). The run supervisor is FROZEN for exactly that window:
    # its own reaction to losing a child would otherwise race this accounting and collapse the identities the
    # report is about, and it is killed under the freeze immediately afterwards. This is the same scheduler-level
    # primitive the Host.stop arbitration uses to make "read the evidence, then act" atomic.
    rest =
      pids(
        [s.owned[:supervisor], walk.supervisor] ++
          walk.children ++ Map.values(Map.take(s.owned, [:server, :work, :worker])) ++ List.wrap(s.owned[:late])
      ) -- writers

    # The freeze covers the WHOLE window it is claimed to cover: the Writer kills, the observation the diagnostic
    # rests on, the announcement, and the supervisor's own kill and join. `after` runs on every path - a raise, an
    # exit, a reaper killed mid-window - so no failure can leave a suspended process nobody owns.
    frozen = freeze(walk.supervisor || s.owned[:supervisor])

    {ownership, joined_writers, survived_writers, joined_rest, survived_rest} =
      try do
        {jw, sw} = kill_join(writers, join, join_ms)
        ownership = if writers == [], do: :none, else: diagnostic(observe(s.config), writers)
        trace(s.config, {:run_startup_aborted, s.owner, %{why: why, phase: s.phase, ownership: ownership}})
        {jr, sr} = kill_join(rest, join, join_ms)
        {ownership, jw, sw, jr, sr}
      after
        thaw(frozen)
      end

    {joined_starter, survived_starter, reaped?} = reap_starter(s.starter, s.ref, s.phase, join, join_ms)

    report = %{
      why: why,
      phase: s.phase,
      ownership: ownership,
      joined: joined_writers + joined_rest + joined_starter,
      survivors: survived_writers + survived_rest + survived_starter
    }

    if reaped?, do: Map.put(report, :starter_reaped, true), else: report
  end

  # before the permit the starter is responsive: released, it exits by itself within a short grace; after the permit
  # (or when the phase is unknown to a mirror) a starter that stays is killed and joined
  defp reap_starter(nil, _ref, _phase, _join, _join_ms), do: {0, 0, false}

  defp reap_starter(starter, ref, phase, join, join_ms) do
    monitor = Process.monitor(starter)
    if phase in [:handshake, :mirror], do: send(starter, {:release, ref})
    grace = if phase in [:handshake, :mirror], do: @grace, else: 0

    receive do
      {:DOWN, ^monitor, :process, ^starter, _} -> {1, 0, false}
    after
      grace ->
        Process.exit(starter, :kill)
        if join.(starter, monitor, join_ms), do: {1, 0, true}, else: {0, 1, true}
    end
  end

  defp mirror(startup, why) do
    reap_tree(
      %{
        owner: startup.owner,
        ref: startup.ref,
        config: startup.config,
        budgets: startup.budgets,
        starter: starter_of(startup),
        phase: :mirror,
        writers: [],
        owned: %{}
      },
      why
    )
  end

  # the owner may not have learned the starter yet (its identity is an ordinary event now), so a mirror reap finds
  # it through the helper's own links rather than losing the subtree it parents
  defp starter_of(%{starter: starter}) when is_pid(starter), do: starter

  defp starter_of(%{helper: helper}), do: Enum.find(links(helper), &starter?/1)

  # the tree reachable from the starter by links alone (never a call): its Run.Supervisor, that supervisor's
  # Writer(s) and other children, and the workers under the Work supervisor
  defp walk(starter) when is_pid(starter) do
    sup = Enum.find(links(starter), &run_supervisor?/1)
    children = if sup, do: links(sup) -- [starter], else: []
    writers = Enum.filter(children, &writer?/1)
    work = Enum.find(children, &work_supervisor?/1)
    workers = if work, do: links(work) -- [sup], else: []
    %{supervisor: sup, writers: writers, children: (children -- writers) ++ workers}
  end

  defp walk(_starter), do: %{supervisor: nil, writers: [], children: []}

  defp kill_join(pids, join, join_ms) do
    monitors = for pid <- pids, do: {pid, Process.monitor(pid)}
    for {pid, _} <- monitors, Process.alive?(pid), do: Process.exit(pid, :kill)

    Enum.reduce(monitors, {0, 0}, fn {pid, monitor}, {joined, survived} ->
      if join.(pid, monitor, join_ms), do: {joined + 1, survived}, else: {joined, survived + 1}
    end)
  end

  # the run supervisor is suspended only for the Writer window above; a dead or absent one is simply not frozen
  defp freeze(supervisor) when is_pid(supervisor) do
    if :erlang.suspend_process(supervisor, []), do: supervisor
  catch
    :error, _ -> nil
  end

  defp freeze(_supervisor), do: nil

  defp thaw(supervisor) when is_pid(supervisor) do
    :erlang.resume_process(supervisor)
  catch
    :error, _ -> false
  end

  defp thaw(_supervisor), do: false

  # the bounded observation itself: a throwaway process makes the arbiter call, so a blocked arbiter can never
  # block the reaper. It answers the arbiter's REGISTRATION, never a verdict; diagnostic/2 derives the verdict.
  defp observe(config) do
    run_dir = Map.get(config, :run_dir)
    opts = config |> Map.get(:opts, []) |> Keyword.get(:ownership, []) |> Keyword.put(:acquire_timeout, @observe)
    me = self()
    tag = make_ref()
    {worker, monitor} = spawn_monitor(fn -> send(me, {tag, Ownership.status(run_dir, opts)}) end)

    receive do
      {^tag, answer} ->
        Process.demonitor(monitor, [:flush])
        answer

      {:DOWN, ^monitor, :process, ^worker, _} ->
        :unavailable
    after
      @observe + 100 ->
        Process.demonitor(monitor, [:flush])
        Process.exit(worker, :kill)
        receive(do: ({^tag, _} -> :ok), after: (0 -> :ok))
        :unavailable
    end
  end

  # Contract section 5 exactly: `:none` when nothing was ever acquired; `:reclaimable` when the arbiter reports
  # `:down`, or reports `:live` naming a Writer THIS reap has already joined dead; `:unknown` when the observation
  # was unavailable, or when the registration names a writer this reap neither killed nor saw die - an identity it
  # cannot speak for.
  defp diagnostic(:none, _reaped), do: :none
  defp diagnostic({:ok, %{state: :down}}, _reaped), do: :reclaimable

  defp diagnostic({:ok, %{state: :live, writer: writer}}, reaped),
    do: if(writer in reaped and not Process.alive?(writer), do: :reclaimable, else: :unknown)

  defp diagnostic(_other, _reaped), do: :unknown

  defp join_fn(budgets) do
    case Map.get(budgets, :join_fn) do
      join when is_function(join, 3) -> join
      _ -> &Owner.joined?/3
    end
  end

  # ---- the STARTER: the only blocking process, the OTP parent of Run.Supervisor ----

  @doc false
  def starter_init(%{owner: owner, ref: ref, deadline: deadline}) do
    Process.flag(:trap_exit, true)
    # The owner is watched BEFORE the adoption wait. Between this process's unlinked spawn and the helper's spawn
    # no other process can reap it, so its own owner monitor is the only thing that can end it there; without that
    # an owner killed in that window would leave it waiting out the whole startup budget. Nothing is acquired on
    # any of these paths: an unadopted starter holds nothing and announces nothing.
    owner_monitor = Process.monitor(owner)

    {helper, config} =
      receive do
        {:startup_config, ^ref, helper, config} ->
          {helper, config}

        {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
          exit(:normal)

        {:release, ^ref} ->
          exit(:normal)
      after
        max(deadline - now(), 0) ->
          send(owner, {:startup_note, ref, :deadline_before_adoption})
          exit(:normal)
      end

    # adopted: the keeper is now the process this starter answers to, and the owner's own death reaches it there
    Process.demonitor(owner_monitor, [:flush])
    helper_monitor = Process.monitor(helper)
    send(owner, {:startup_identity, ref, self()})

    receive do
      {:permit, ^ref} ->
        # the permit is re-validated against the same absolute clock at consumption: no work on an expired one
        if now() > deadline do
          send(owner, {:startup_note, ref, :permit_expired})
          send(helper, {:starter_aborted, ref, :permit_expired})
          exit(:normal)
        else
          start(helper, owner, ref, config)
        end

      {:deny, ^ref} ->
        send(owner, {:startup_note, ref, :denied})
        exit(:normal)

      {:release, ^ref} ->
        exit(:normal)

      {:DOWN, ^helper_monitor, :process, ^helper, _reason} ->
        exit(:normal)

      {:EXIT, ^helper, _reason} ->
        exit(:normal)
    after
      max(deadline - now(), 0) ->
        send(owner, {:startup_note, ref, :deadline_before_permit})
        exit(:normal)
    end
  end

  defp start(helper, owner, ref, config) do
    case Owner.start_subtree(config) do
      {:ok, sup} ->
        send(helper, {:starter_supervisor, ref, sup})

        case Owner.facts(sup) do
          {:ok, facts} ->
            send(helper, {:starter_completed, ref, {:ok, sup, facts}})
            hold(owner, ref, sup)

          :error ->
            send(helper, {:starter_completed, ref, {:error, %{clause: "run_server_down"}}})
            exit(:normal)
        end

      {:error, rejection} ->
        send(helper, {:starter_completed, ref, {:error, rejection}})
        exit(:normal)
    end
  end

  # retained for the whole run: a returning parent would end the supervisor; released only by the teardown
  defp hold(owner, ref, sup) do
    receive do
      {:EXIT, ^sup, reason} ->
        send(owner, {:startup_subtree_exit, ref, sup, reason})
        hold(owner, ref, sup)

      {:release, ^ref} ->
        exit(:normal)

      _other ->
        hold(owner, ref, sup)
    end
  end

  # ---- shared helpers ----

  defp now, do: System.monotonic_time(:millisecond)

  defp pids(list), do: list |> List.flatten() |> Enum.filter(&is_pid/1) |> Enum.uniq()

  defp links(pid) do
    case Process.info(pid, :links) do
      {:links, links} -> Enum.filter(links, &is_pid/1)
      nil -> []
    end
  end

  defp initial_call(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} -> Keyword.get(dictionary, :"$initial_call")
      nil -> :dead
    end
  end

  defp run_supervisor?(pid), do: match?({:supervisor, Run.Supervisor, _}, initial_call(pid))
  defp starter?(pid), do: match?({__MODULE__, :starter_init, _}, initial_call(pid))
  defp work_supervisor?(pid), do: match?({:supervisor, Run.Work.Supervisor, _}, initial_call(pid))
  defp writer?(pid), do: match?({Writer, :init, _}, initial_call(pid))

  defp trace(%{trace: pid}, message) when is_pid(pid), do: send(pid, message)
  defp trace(_config, _message), do: :ok
end
