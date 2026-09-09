defmodule AiOrchestrator.Host.MountReviewControlsTest do
  @moduledoc """
  Permanent controls adopted from the 86711a8 review (logs/shared-host-mount-green-86711a8/codex/REVIEW.org):
  the seven reproduced interleavings plus the neighbouring cases the review names; and from the 40a8d546 review
  (logs/shared-host-mount-green-40a8d546/codex/REVIEW.org, successor_test.exs): the four measured S1-S4 rows
  verbatim plus the eligible-replacement and lifecycle-evidence rows it asked for. Each row pins an obligation of
  docs/contracts/host-mounted-runs.org against the real implementation; none relaxes an outcome. From the 4bc5928
  review (logs/shared-host-mount-green-4bc5928/codex/REVIEW.org): the late-acknowledgment row verbatim and the
  terminal-with-survivor witness the review asked for. From the 4bbf7f6
  review (logs/shared-host-mount-green-4bbf7f6/codex/REVIEW.org): the monotone-invalidation variant of the S3
  delayed row and the sampled-evidence stop race row, verbatim.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.RunOwner
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @deadline 15_000
  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @budgets %{close: 3_000, stop: 8_000, join: 1_000, handoff: 5_000, helper_join: 500}

  setup do
    dir = Path.join(System.tmp_dir!(), "mount_review_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  # ---- isolated host root (private arbiter, host supervisor, monitor LAST) ----
  defp start_host!(opts \\ []) do
    n = System.unique_integer([:positive])
    arb = :"review_arb_#{n}"
    hsup = :"review_hsup_#{n}"
    mon = :"review_mon_#{n}"

    children = [
      {Ownership, name: arb},
      {Host.Supervisor, name: hsup, child_shutdown_ms: Keyword.get(opts, :child_shutdown_ms, 20_000)},
      {Host.Monitor,
       name: mon, host_supervisor: hsup, ownership: arb, census_timeout: Keyword.get(opts, :census_timeout, 1_000)}
    ]

    root =
      start_supervised!(%{
        id: :"review_root_#{n}",
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    %{root: root, arb: arb, hsup: hsup, mon: mon, host: %{supervisor: hsup, monitor: mon, ownership: arb}}
  end

  defp command_ctx(dir, barrier, extra) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
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
        supervisor_instance: "sup_review_0001",
        trace: self(),
        barrier: barrier
      )
      |> Keyword.merge(extra)

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_review_0001",
        command_id: "cmd_review_000000001",
        now: @now
      )

    {command, ctx}
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  defp holding(test_pid, hold) do
    fn label, payload ->
      if label == hold do
        ref = make_ref()
        send(test_pid, {:held, label, ref, payload, self()})
        receive(do: ({:release, ^ref} -> :ok), after: (@deadline -> exit(:never_released)))
      else
        :ok
      end
    end
  end

  defp mount!(h, dir, barrier, extra \\ []) do
    H.reset_seams()
    {command, ctx} = command_ctx(dir, barrier, extra)
    {:ok, handle} = Host.mount(command, ctx, host: h.host, budgets: @budgets)
    handle
  end

  defp await_held!(hold) do
    assert_receive {:held, ^hold, ref, payload, helper}, @deadline
    {ref, payload, helper}
  end

  defp trusted_map!(supervisor) do
    children = Supervisor.which_children(supervisor)
    {_, server, _, _} = List.keyfind(children, Run.Server, 0)
    {_, work, :supervisor, _} = List.keyfind(children, Run.Work.Supervisor, 0)
    {_, writer, _, _} = Enum.find(children, &match?({{Writer, _}, pid, _, _} when is_pid(pid), &1))
    [{_, worker, _, _}] = Supervisor.which_children(work)
    %{supervisor: supervisor, server: server, writer: writer, work: work, worker: worker}
  end

  defp trusted_record(dir, payload, owner, generation) do
    payload.supervisor
    |> trusted_map!()
    |> Map.take([:supervisor, :server, :writer, :worker])
    |> Map.merge(%{run_dir: Path.expand(dir), run_id: "run_review_0001", owner: owner, generation: generation})
  end

  defp assert_released!(dir, arb) do
    assert :none == Ownership.status(dir, server: arb)
    assert :none == RunLock.owner(SystemFs.new(), dir)
  end

  defp wait_until(fun, tries \\ 750) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never held")
      true -> Process.sleep(20) && wait_until(fun, tries - 1)
    end
  end

  defp timed(fun) do
    started = System.monotonic_time(:millisecond)
    result = fun.()
    {result, System.monotonic_time(:millisecond) - started}
  end

  # ================================================================= R1: stop protocol
  test "R1 a stop deadline shorter than any acknowledgment still initiates the teardown obligation", %{dir: dir} do
    h = start_host!(child_shutdown_ms: 2_000)
    handle = mount!(h, dir, holding(self(), :subtree_started))
    await_held!(:subtree_started)
    # the obligation is the teardown, whatever the caller observed within 10 ms: a closed timeout or, when the
    # acknowledged teardown of a held tree completes first, the completed outcome
    assert Host.stop(handle, 10) in [{:error, %{clause: "run_host_stop_timeout"}}, {:ok, :stopped}]
    wait_until(fn -> not Process.alive?(handle.owner) end)
    assert_released!(dir, h.arb)
  end

  test "R1 a retained terminal owner survives a stop while its application mailbox is suspended", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, nil)
    assert {:ok, result} = Host.await(handle, @deadline)
    :ok = :sys.suspend(handle.owner)
    answer = Host.stop(handle, 1_000)
    alive = Process.alive?(handle.owner)
    if alive, do: :sys.resume(handle.owner)
    assert :ok == answer, "a wait cannot authorise the loss of a retained result"
    assert alive
    assert {:ok, ^result} = Host.await(handle, 1_000)
  end

  test "R1 a dead stopper leaves no record and the teardown still completes", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    await_held!(:subtree_started)
    stopper = spawn(fn -> Host.stop(handle, @deadline) end)
    Process.sleep(20)
    Process.exit(stopper, :kill)
    wait_until(fn -> not Process.alive?(handle.owner) end)
    assert_released!(dir, h.arb)
  end

  test "R1 a stop that crosses the run's own completion answers consistently", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {ref, _payload, helper} = await_held!(:subtree_started)
    send(helper, {:release, ref})
    answer = Host.stop(handle, @deadline)
    assert answer in [:ok, {:ok, :stopped}], inspect(answer)
    if answer == :ok, do: assert(match?({:ok, %{}}, Host.await(handle, @deadline)))
    assert_released!(dir, h.arb)
  end

  test "R1 supervisor loss during a stop still answers the stopper", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    await_held!(:subtree_started)
    task = Task.async(fn -> Host.stop(handle, @deadline) end)
    Process.sleep(20)
    :ok = Supervisor.terminate_child(h.root, Host.Supervisor)
    assert Task.await(task, @deadline) in [{:ok, :stopped}, {:error, %{clause: "run_host_owner_down"}}]
    assert_released!(dir, h.arb)
  end

  # ================================================================= R2: the sweep
  test "R2 a queued identity matching the handoff reference is swept at parent shutdown", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {_ref, payload, _helper} = await_held!(:subtree_started)
    %{handoff_ref: href} = RunOwner.inspect(handle.owner)
    stray = spawn(fn -> receive(do: (:never -> :ok)) end)
    on_exit(fn -> Process.exit(stray, :kill) end)
    smon = Process.monitor(stray)
    :ok = :sys.suspend(handle.owner)
    send(handle.owner, {:run_worker_registered, href, stray, payload.server})
    :ok = Supervisor.terminate_child(h.root, Host.Supervisor)
    assert_receive {:DOWN, ^smon, :process, ^stray, :killed}, @deadline
    refute Process.alive?(handle.owner)
  end

  test "R2 an identity arriving DURING teardown is swept, and one reaching a retained owner is collected", %{dir: dir} do
    h = start_host!()

    slow = fn pid, mon, timeout ->
      Process.sleep(150)
      receive(do: ({:DOWN, ^mon, :process, ^pid, _} -> true), after: (timeout -> false))
    end

    handle = mount!(h, dir, holding(self(), :subtree_started), join: slow)
    {_ref, payload, _helper} = await_held!(:subtree_started)
    %{handoff_ref: href} = RunOwner.inspect(handle.owner)
    stray = spawn(fn -> receive(do: (:never -> :ok)) end)
    on_exit(fn -> Process.exit(stray, :kill) end)
    smon = Process.monitor(stray)
    stopper = Task.async(fn -> Host.stop(handle, @deadline) end)
    Process.sleep(100)
    send(handle.owner, {:run_worker_registered, href, stray, payload.server})
    assert_receive {:DOWN, ^smon, :process, ^stray, :killed}, @deadline
    assert {:ok, :stopped} == Task.await(stopper, @deadline)
    # retained owner: a matched identity is collected at once
    d2 = dir <> "_retained"
    File.mkdir_p!(d2)
    on_exit(fn -> File.rm_rf!(d2) end)
    handle2 = mount!(h, d2, nil)
    assert {:ok, %{}} = Host.await(handle2, @deadline)
    %{handoff_ref: href2} = RunOwner.inspect(handle2.owner)
    stray2 = spawn(fn -> receive(do: (:never -> :ok)) end)
    on_exit(fn -> Process.exit(stray2, :kill) end)
    smon2 = Process.monitor(stray2)
    send(handle2.owner, {:run_worker_registered, href2, stray2, self()})
    assert_receive {:DOWN, ^smon2, :process, ^stray2, :killed}, @deadline
    assert Process.alive?(handle2.owner)
  end

  # ================================================================= R3: census confirmation off the Monitor
  test "R3 an eligible census confirmation blocked inside the owner never blocks Monitor lookups or its deadline", %{
    dir: dir
  } do
    h = start_host!(census_timeout: 150)
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {_ref, payload, _helper} = await_held!(:subtree_started)
    {:ok, %{generation: gen}} = Ownership.status(dir, server: h.arb)
    record = trusted_record(dir, payload, handle.owner, gen)
    :ok = :sys.suspend(handle.owner)
    :ok = Supervisor.terminate_child(h.root, Host.Monitor)
    {:ok, _} = Supervisor.restart_child(h.root, Host.Monitor)
    mon = Process.whereis(h.mon)
    wait_until(fn -> Enum.any?(elem(Process.info(handle.owner, :messages), 1), &match?({:census, _, ^mon}, &1)) end)
    testpid = self()
    tag = make_ref()

    :sys.replace_state(handle.owner, fn state ->
      receive(do: ({:census, ref, ^mon} -> send(testpid, {tag, ref})))
      state
    end)

    assert_receive {^tag, cref}, 1_000
    :ok = :sys.resume(handle.owner)

    blocker =
      Task.async(fn ->
        :sys.replace_state(handle.owner, fn state ->
          send(testpid, :owner_blocked)
          receive(do: (:release_owner -> state), after: (2_000 -> state))
        end)
      end)

    assert_receive :owner_blocked, 1_000
    send(mon, {:census_reply, cref, record, :barrier_subtree})
    answer = Host.lookup_run_id("run_review_0001", monitor: h.mon, timeout: 100)
    Process.sleep(200)
    queue = elem(Process.info(mon, :messages), 1)
    send(handle.owner, :release_owner)
    Task.await(blocker, 3_000)
    assert match?({:ok, _}, answer), "Monitor lookup blocked by owner confirmation"
    refute Enum.any?(queue, &match?({:census_deadline, _}, &1)), "census deadline could not be processed while confirming"
    assert {:ok, %{census: :complete}} = Host.census(monitor: h.mon)
  end

  # narrowed claim (40a8d546 review): the impostor reply below carries an UNKNOWN reference, so this row checks
  # eligibility only; replacement precedence under a genuinely eligible reply is the S3 replacement row
  test "R3 an ineligible (unknown-reference) census reply for a registered directory is ignored", %{dir: dir} do
    h = start_host!(census_timeout: 20_000)
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {_ref, payload, _helper} = await_held!(:subtree_started)
    wait_until(fn -> match?({:ok, [_]}, Host.lookup_run_id("run_review_0001", monitor: h.mon)) end)
    # a second live process claims the same directory through a genuinely pending census reply
    :ok = :sys.suspend(handle.owner)
    :ok = Supervisor.terminate_child(h.root, Host.Monitor)
    {:ok, _} = Supervisor.restart_child(h.root, Host.Monitor)
    mon = Process.whereis(h.mon)
    wait_until(fn -> Enum.any?(elem(Process.info(handle.owner, :messages), 1), &match?({:census, _, ^mon}, &1)) end)
    testpid = self()
    tag = make_ref()

    :sys.replace_state(handle.owner, fn state ->
      receive(do: ({:census, ref, ^mon} -> send(testpid, {tag, ref})))
      state
    end)

    assert_receive {^tag, cref}, 1_000
    :ok = :sys.resume(handle.owner)
    # the owner re-registers itself on its own path only when its barrier completes; deliver its own fact first
    {:ok, %{generation: gen}} = Ownership.status(dir, server: h.arb)
    send(mon, {:census_reply, cref, trusted_record(dir, payload, handle.owner, gen), :barrier_subtree})

    wait_until(fn ->
      match?({:ok, [%{owner: o}]} when o == handle.owner, Host.lookup_run_id("run_review_0001", monitor: h.mon))
    end)

    # a later reply for the SAME directory naming another live owner is not eligible (its ref is unknown)
    impostor = spawn(fn -> receive(do: (:never -> :ok)) end)
    on_exit(fn -> Process.exit(impostor, :kill) end)

    send(
      mon,
      {:census_reply, make_ref(), %{trusted_record(dir, payload, handle.owner, gen) | owner: impostor}, :awaiting}
    )

    Process.sleep(200)
    assert {:ok, [%{owner: o}]} = Host.lookup_run_id("run_review_0001", monitor: h.mon)
    assert o == handle.owner
  end

  # ================================================================= R4: mounted discovery boundary
  test "R4 an absent supervisor yields the closed unavailable answer without killing the caller" do
    dead = spawn(fn -> :ok end)
    dm = Process.monitor(dead)
    assert_receive {:DOWN, ^dm, :process, ^dead, _}, 1_000
    parent = self()

    {client, cm} =
      spawn_monitor(fn ->
        send(parent, {:mounted_answer, self(), Host.mounted(%{supervisor: dead}, 100)})
      end)

    assert_receive {:mounted_answer, ^client, {:error, %{clause: "host_supervisor_unavailable"}}}, 1_000
    assert_receive {:DOWN, ^cm, :process, ^client, :normal}, 1_000
  end

  test "R4 every owner query leg shares the remaining deadline and an unknown result keeps the owner" do
    # discovery-only stand-in speaking the real which_children protocol; its one child delays each leg 80 ms
    owner =
      spawn(fn ->
        receive do
          {:"$gen_call", from, :phase} ->
            Process.sleep(80)
            :gen_statem.reply(from, :awaiting)
        after
          150 -> :ok
        end

        receive do
          {:system, from, :get_state} ->
            Process.sleep(80)
            :gen.reply(from, {:awaiting, %{config: %{run_dir: "/tmp/review", command: %{run_id: "review"}}}})
        after
          150 -> :ok
        end

        receive(do: (:finish -> :ok))
      end)

    supervisor =
      spawn(fn ->
        receive(do: ({:"$gen_call", from, :which_children} -> GenServer.reply(from, [{:undefined, owner, :worker, []}])))
        receive(do: (:finish -> :ok))
      end)

    on_exit(fn ->
      Process.exit(owner, :kill)
      Process.exit(supervisor, :kill)
    end)

    {answer, elapsed} = timed(fn -> Host.mounted(%{supervisor: supervisor}, 100) end)
    assert {:ok, [view]} = answer
    assert view.owner == owner, "an unknown result must keep the owner it names"
    assert view.phase == :unknown
    assert elapsed < 140, "mounted exceeded its deadline plus a 40 ms allowance (#{elapsed} ms)"
  end

  test "R4 more than 64 owners are queried at once under one deadline (stand-in discovery)" do
    # GenServer stand-ins whose state is the owner data shape; :sys.get_state answers the map directly
    owners =
      for i <- 1..70 do
        {:ok, pid} =
          Agent.start(fn -> %{phase: :awaiting, config: %{run_dir: "/tmp/r#{i}", command: %{run_id: "r#{i}"}}} end)

        pid
      end

    supervisor =
      spawn(fn ->
        receive(
          do: ({:"$gen_call", from, :which_children} ->
                 GenServer.reply(from, for(o <- owners, do: {:undefined, o, :worker, []})))
        )
      end)

    on_exit(fn -> for o <- owners, do: Process.exit(o, :kill) end)
    {answer, elapsed} = timed(fn -> Host.mounted(%{supervisor: supervisor}, 500) end)
    assert {:ok, listed} = answer
    assert length(listed) == 70 and Enum.all?(listed, &(&1.phase == :awaiting))
    assert elapsed < 500
  end

  # ================================================================= R5: foreground executor + injected arbiter
  test "R5 the foreground Host.Executor registers with the injected arbiter", %{dir: dir} do
    h = start_host!()
    seams = [ownership: [server: h.arb], host_monitor: h.mon]
    {command, ctx} = command_ctx(dir, holding(self(), :subtree_started), seams)
    task = Task.async(fn -> Host.Executor.execute(command, ctx) end)
    {ref, payload, owner} = await_held!(:subtree_started)
    assert {:ok, %{writer: writer}} = Ownership.status(dir, server: h.arb)
    assert writer == payload.writer
    assert :none == Ownership.status(dir)

    wait_until(fn ->
      match?({:ok, %{registered: true, live: true}}, Host.status(dir, monitor: h.mon, ownership: h.arb))
    end)

    send(owner, {:release, ref})
    assert {:ok, _} = Task.await(task, @deadline)
  end

  test "S1 mounted timeout kills and joins its discovery worker" do
    parent = self()

    sup =
      spawn(fn ->
        receive do
          {:"$gen_call", {worker, _alias}, :which_children} -> send(parent, {:discovery_worker, worker})
        end

        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> Process.exit(sup, :kill) end)
    assert {:error, %{clause: "host_supervisor_unavailable"}} == Host.mounted(%{supervisor: sup}, 40)
    assert_receive {:discovery_worker, worker}, 1000
    on_exit(fn -> Process.exit(worker, :kill) end)
    refute Process.alive?(worker), "timed-out discovery worker is still blocked in infinite which_children"
  end

  test "S2 mounted listing does not consume unrelated caller DOWN messages" do
    {:ok, owner} =
      Agent.start(fn -> %{phase: :awaiting, config: %{run_dir: "/tmp/control", command: %{run_id: "control"}}} end)

    on_exit(fn -> Process.exit(owner, :kill) end)

    sup =
      spawn(fn ->
        receive do
          {:"$gen_call", from, :which_children} -> GenServer.reply(from, [{:undefined, owner, :worker, []}])
        end
      end)

    {dead, mon} = spawn_monitor(fn -> :ok end)
    Process.sleep(20)
    assert {:ok, [%{owner: ^owner}]} = Host.mounted(%{supervisor: sup}, 500)

    assert_receive {:DOWN, ^mon, :process, ^dead, :normal},
                   100,
                   "listing consumed the caller's unrelated monitor notification"
  end

  test "S3 delayed active confirmation cannot resurrect a completed retained owner", %{dir: dir} do
    h = start_host!(census_timeout: 20_000)
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {barrier_ref, payload, helper} = await_held!(:subtree_started)
    {:ok, %{generation: gen}} = Ownership.status(dir, server: h.arb)
    record = trusted_record(dir, payload, handle.owner, gen)
    :ok = :sys.suspend(handle.owner)
    :ok = Supervisor.terminate_child(h.root, Host.Monitor)
    {:ok, _} = Supervisor.restart_child(h.root, Host.Monitor)
    mon = Process.whereis(h.mon)
    wait_until(fn -> Enum.any?(elem(Process.info(handle.owner, :messages), 1), &match?({:census, _, ^mon}, &1)) end)
    observer = self()
    tag = make_ref()

    :sys.replace_state(handle.owner, fn state ->
      receive do
        {:census, ref, ^mon} -> send(observer, {tag, ref})
      end

      state
    end)

    assert_receive {^tag, cref}, 1000
    :ok = :sys.resume(handle.owner)

    blocker =
      Task.async(fn ->
        :sys.replace_state(handle.owner, fn state ->
          send(observer, :s3_owner_blocked)

          receive do
            :s3_release -> state
          after
            5000 -> state
          end
        end)
      end)

    assert_receive :s3_owner_blocked, 1000
    send(mon, {:census_reply, cref, record, :barrier_subtree})
    wait_until(fn -> Map.has_key?(:sys.get_state(mon).confirming, cref) end)
    task = :sys.get_state(mon).confirming[cref].task

    wait_until(fn ->
      Enum.any?(
        elem(Process.info(handle.owner, :messages), 1),
        &match?({:system, {^task, _}, :get_state}, &1)
      )
    end)

    :erlang.suspend_process(task)
    on_exit(fn -> if Process.alive?(task), do: Process.exit(task, :kill) end)
    # The owner answers the genuine inspection with its ACTIVE state, while the
    # confirmation worker is suspended before consuming that answer.
    send(handle.owner, :s3_release)
    Task.await(blocker, 6000)
    wait_until(fn -> elem(Process.info(task, :message_queue_len), 1) > 0 end)
    send(helper, {:release, barrier_ref})
    assert {:ok, _} = Host.await(handle, @deadline)
    assert RunOwner.inspect(handle.owner).phase == :terminal
    assert {:ok, []} = Host.lookup_run_id("run_review_0001", monitor: mon)
    # An unrelated lifecycle event must not revalidate this already-stale fact.
    Host.Monitor.unregister(mon, %{run_dir: dir <> "_unrelated", owner: self(), generation: 1})
    assert {:ok, []} = Host.lookup_run_id("run_review_0001", monitor: mon)
    :erlang.resume_process(task)
    wait_until(fn -> not Map.has_key?(:sys.get_state(mon).confirming, cref) end)

    assert {:ok, []} == Host.lookup_run_id("run_review_0001", monitor: mon),
           "a delayed pre-terminal confirmation restored the unregistered terminal owner"
  end

  test "S4 stop cannot destroy a terminal owner when state inspection is blocked", %{dir: dir} do
    h = start_host!(child_shutdown_ms: 500)
    handle = mount!(h, dir, nil)
    assert {:ok, result} = Host.await(handle, @deadline)
    observer = self()

    blocker =
      spawn(fn ->
        try do
          :sys.replace_state(handle.owner, fn state ->
            send(observer, :s4_blocked)

            receive do
              :s4_release -> state
            after
              3000 -> state
            end
          end)
        catch
          :exit, _ -> :ok
        end
      end)

    assert_receive :s4_blocked, 1000
    answer = Host.stop(handle, 100)
    Process.sleep(600)
    alive = Process.alive?(handle.owner)
    send(handle.owner, :s4_release)
    on_exit(fn -> Process.exit(blocker, :kill) end)
    assert alive, "unconfirmed state was treated as active and the retained result was destroyed"
    assert answer in [:ok, {:error, %{clause: "run_host_stop_timeout"}}]
    assert {:ok, ^result} = Host.await(handle, 1000)
  end

  test "S3 an ELIGIBLE confirmed reply naming a different live owner never displaces the registered one", %{dir: dir} do
    h = start_host!(census_timeout: 20_000)
    d2 = dir <> "_other"
    File.mkdir_p!(d2)
    on_exit(fn -> File.rm_rf!(d2) end)
    handle_a = mount!(h, dir, holding(self(), :subtree_started))
    {_ref_a, payload_a, _helper_a} = await_held!(:subtree_started)
    handle_b = mount!(h, d2, holding(self(), :subtree_started))
    {_ref_b, payload_b, _helper_b} = await_held!(:subtree_started)
    # a restarted Monitor learns A through A's own genuine census answer; B's request is captured instead
    :ok = :sys.suspend(handle_b.owner)
    :ok = Supervisor.terminate_child(h.root, Host.Monitor)
    {:ok, _} = Supervisor.restart_child(h.root, Host.Monitor)
    mon = Process.whereis(h.mon)

    wait_until(fn ->
      match?({:ok, [%{owner: o}]} when o == handle_a.owner, Host.lookup_run_id("run_review_0001", monitor: mon))
    end)

    wait_until(fn -> Enum.any?(elem(Process.info(handle_b.owner, :messages), 1), &match?({:census, _, ^mon}, &1)) end)
    testpid = self()
    tag = make_ref()

    :sys.replace_state(handle_b.owner, fn state ->
      receive(do: ({:census, ref, ^mon} -> send(testpid, {tag, ref})))
      state
    end)

    assert_receive {^tag, cref_b}, 1_000
    :ok = :sys.resume(handle_b.owner)
    {:ok, %{generation: gen_a}} = Ownership.status(dir, server: h.arb)
    # genuinely eligible (pending reference), genuinely confirmed (B is ACTIVE at its barrier), same generation:
    # the record claims A's directory for B and must still lose to the live registered owner
    forged = trusted_record(dir, payload_b, handle_b.owner, gen_a)
    send(mon, {:census_reply, cref_b, forged, :barrier_subtree})
    wait_until(fn -> not Map.has_key?(:sys.get_state(mon).pending, cref_b) end)
    wait_until(fn -> not Map.has_key?(:sys.get_state(mon).confirming, cref_b) end)
    assert {:ok, [%{owner: o}]} = Host.lookup_run_id("run_review_0001", monitor: mon)
    assert o == handle_a.owner, "an eligible confirmed reply displaced a live different owner"
    assert Process.alive?(handle_b.owner)
    _ = payload_a
  end

  test "S4 lifecycle evidence: an owner at its barrier is linked to a live run supervisor, a retained one is not",
       %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {ref, payload, helper} = await_held!(:subtree_started)
    {:links, links} = Process.info(handle.owner, :links)
    assert payload.supervisor in links, "the run supervisor is not linked to its owner"
    {:dictionary, dictionary} = Process.info(payload.supervisor, :dictionary)
    assert match?({:supervisor, Run.Supervisor, _}, Keyword.get(dictionary, :"$initial_call"))
    send(helper, {:release, ref})
    assert {:ok, _} = Host.await(handle, @deadline)
    {:links, links} = Process.info(handle.owner, :links)
    refute payload.supervisor in links
    refute Enum.any?(links, &(is_pid(&1) and &1 != Process.whereis(h.hsup)))
    assert :ok == Host.stop(handle, 1_000)
  end

  test "S4 sampled active evidence cannot kill a result retained before fallback termination", %{dir: dir} do
    h = start_host!()
    handle = mount!(h, dir, holding(self(), :subtree_started))
    {ref, payload, helper} = await_held!(:subtree_started)
    :ok = :sys.suspend(payload.server)
    send(helper, {:release, ref})
    assert {:ok, _} = Host.ready(handle, 1000)
    :ok = :sys.suspend(handle.owner)
    :ok = :sys.resume(payload.server)

    wait_until(fn ->
      Enum.any?(elem(Process.info(handle.owner, :messages), 1), &match?({_tag, {:ok, %{}}}, &1))
    end)

    # Delay only the supervisor termination request, not owner operation. Its
    # normal run result is already queued BEFORE the later stop request.
    observer = self()

    proxy =
      spawn(fn ->
        receive do
          {:"$gen_call", from, request} ->
            send(observer, {:fallback_pending, self()})

            receive do
              :finish_fallback -> :ok
            after
              5000 -> :ok
            end

            result = GenServer.call(h.hsup, request, 5000)
            GenServer.reply(from, result)
        end
      end)

    on_exit(fn -> Process.exit(proxy, :kill) end)
    stop = Task.async(fn -> Host.stop(%{handle | supervisor: proxy}, 2000) end)
    assert_receive {:fallback_pending, ^proxy}, 1500
    :ok = :sys.resume(handle.owner)
    assert :ok == Task.await(stop, 2500)
    assert {:ok, result} = Host.await(handle, 1000)
    assert RunOwner.inspect(handle.owner).phase == :terminal
    pm = Process.monitor(proxy)
    send(proxy, :finish_fallback)
    assert_receive {:DOWN, ^pm, :process, ^proxy, :normal}, 1000
    assert Process.alive?(handle.owner), "obsolete active fallback destroyed a result after stop reported retained"
    assert {:ok, ^result} = Host.await(handle, 1000)
  end

  test "R1 a real retained acknowledgment after the probe releases the stop agent", %{dir: dir} do
    h = start_host!(child_shutdown_ms: 2000)
    handle = mount!(h, dir, nil)
    assert {:ok, result} = Host.await(handle, @deadline)
    observer = self()

    blocker =
      spawn(fn ->
        :sys.replace_state(handle.owner, fn state ->
          send(observer, :owner_callback_held)

          receive do
            :release_callback -> state
          after
            5000 -> state
          end
        end)
      end)

    on_exit(fn ->
      send(handle.owner, :release_callback)
      Process.exit(blocker, :kill)
    end)

    assert_receive :owner_callback_held, 1000
    stop = Task.async(fn -> Host.stop(handle, 3000) end)

    wait_until(fn ->
      Enum.any?(
        elem(Process.info(handle.owner, :messages), 1),
        &match?({:"$gen_call", _, {:stop_request, _}}, &1)
      )
    end)

    {:"$gen_call", {agent, _tag}, {:stop_request, _}} =
      Enum.find(
        elem(Process.info(handle.owner, :messages), 1),
        &match?({:"$gen_call", _, {:stop_request, _}}, &1)
      )

    amon = Process.monitor(agent)
    # The real state query can only be enqueued AFTER the first ack probe expired.
    wait_until(fn ->
      Enum.any?(
        elem(Process.info(handle.owner, :messages), 1),
        &match?({:system, {^agent, _}, :get_state}, &1)
      )
    end)

    # Re-entry into the request wait proves the inspection leg has timed out.
    wait_until(fn ->
      Process.info(agent, :current_function) in [
        {:current_function, {:gen, :receive_response, 2}},
        {:current_function, {:gen, :wait_response, 2}}
      ]
    end)

    send(handle.owner, :release_callback)
    assert :ok == Task.await(stop, 1500)
    assert {:ok, ^result} = Host.await(handle, 1000)

    assert_receive {:DOWN, ^amon, :process, ^agent, :normal},
                   200,
                   "the actual retained reply reached the caller, but the stop agent discarded its acknowledgment and kept waiting"
  end

  # control of the construction behind the arbitration's link evidence: a survivor is a pid whose kill was sent but
  # whose DOWN the join did not observe (here every join is refused through the seam, so the teardown reports
  # survivors and retains run_executor_teardown_incomplete); a terminal owner must hold no link to any owned
  # identity, survivor or not, because it unlinks them all BEFORE it enters :terminal. Measured fact recorded with
  # this row (u1-review-red.log): a run supervisor suspended at scheduler level still dies from the teardown's kill,
  # so a "live survivor" is only ever a dying process, never a process that can be kept alive.
  test "R2 a terminal owner retaining an incomplete teardown holds no link to any owned identity", %{dir: dir} do
    h = start_host!(child_shutdown_ms: 500)
    refuse = fn _pid, _mon, _timeout -> false end
    handle = mount!(h, dir, holding(self(), :subtree_started), join: refuse)
    {ref, payload, helper} = await_held!(:subtree_started)
    owned = [payload.supervisor, payload.server, payload.writer, payload.worker, helper]
    send(helper, {:release, ref})
    assert {:error, %{clause: "run_executor_teardown_incomplete", survivors: n} = result} = Host.await(handle, @deadline)
    assert n >= 1
    assert RunOwner.inspect(handle.owner).phase == :terminal
    {:links, links} = Process.info(handle.owner, :links)
    refute Enum.any?(owned, &(&1 in links)), "a terminal owner must not stay linked to an owned identity"
    refute Enum.any?(links, &(is_pid(&1) and &1 != Process.whereis(h.hsup)))
    # the stop arbitration therefore leaves this retained owner untouched even when it is silent at the bound
    observer = self()

    blocker =
      spawn(fn ->
        :sys.replace_state(handle.owner, fn state ->
          send(observer, :survivor_owner_blocked)
          receive(do: (:survivor_release -> state), after: (3_000 -> state))
        end)
      end)

    on_exit(fn -> Process.exit(blocker, :kill) end)
    assert_receive :survivor_owner_blocked, 1_000
    answer = Host.stop(handle, 100)
    Process.sleep(600)
    alive = Process.alive?(handle.owner)
    send(handle.owner, :survivor_release)
    assert alive, "a silent terminal owner with reported survivors was classed active and killed"
    assert answer in [:ok, {:error, %{clause: "run_host_stop_timeout"}}]
    assert {:error, ^result} = Host.await(handle, 1_000)
  end
end
