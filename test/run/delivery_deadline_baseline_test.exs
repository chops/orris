defmodule AiOrchestrator.Run.DeliveryDeadlineBaselineTest do
  @moduledoc """
  MEASURED baseline controls on the unchanged product (exact 15afce9) for the delivery-deadline lane
  (docs/contracts/delivery-deadline.org, DC-1..DC-6): what deliver/reconcile failures, a blocked deliver and the
  direct Effects seam do TODAY. No RED rows here (they await interface review, m_1788756282768).
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [collector: 0, track!: 1, track_dir!: 1]

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Test.OwnedHarness
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.ScenarioHarness.FakePaneClient

  @instance "sup_delivery_deadline"
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @timeout_s 2

  # deliver answers a scripted result; every other method is the real LocalPane
  defmodule ScriptedDeliver do
    @moduledoc false
    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane
    def deliver(_command, opts), do: Keyword.fetch!(opts, :deliver_result)
  end

  # a NON-cooperative deliver: reports entry then blocks forever
  defmodule BlockingDeliver do
    @moduledoc false
    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate reconcile(command, opts), to: LocalPane

    def deliver(command, opts) do
      send(Keyword.fetch!(opts, :collector), {:deliver_entered, self(), command["assignment_id"]})

      receive do
        :never -> :ok
      end
    end
  end

  # a real deliver whose receipt says the send is QUEUED, then a scripted reconcile answer
  defmodule QueuedThenScriptedReconcile do
    @moduledoc false
    defdelegate observe(command, opts), to: LocalPane
    defdelegate snapshot(command, opts), to: LocalPane

    def deliver(command, opts) do
      {:ok, base} = LocalPane.deliver(command, Keyword.drop(opts, [:reconcile_result, :collector]))
      {:ok, Map.put(base, "send_status", "queued")}
    end

    def reconcile(_command, opts), do: Keyword.fetch!(opts, :reconcile_result)
  end

  # a witnessing pane client at the REAL IPC boundary of LocalPane: every reconcile/send is reported with its
  # message id (and the prompt's struct identity + digest, never its bytes); the send may block forever (loss window).
  # The daemon behind it is the scenario's FakePaneClient, which ALWAYS answers "absent" (disclosed in DC-7).
  defmodule WitnessPane do
    @moduledoc false
    alias FakePaneClient, as: Fake

    def capabilities(opts), do: Fake.capabilities(opts)
    def status(pane_ref, opts), do: Fake.status(pane_ref, opts)

    def reconcile(pane_ref, message_id, opts) do
      {:ok, answer} = Fake.reconcile(pane_ref, message_id, opts)
      send(Keyword.fetch!(opts, :collector), {:pane_call, :reconcile, self(), pane_ref, message_id, answer["outcome"]})
      {:ok, answer}
    end

    def send(pane_ref, prompt, opts) do
      send(
        Keyword.fetch!(opts, :collector),
        {:pane_call, :send, self(), pane_ref, opts[:message_id], is_struct(prompt, SensitiveBytes), inspect(prompt)}
      )

      if Keyword.get(opts, :block_send, false), do: block()
      Fake.send(pane_ref, prompt, opts)
    end

    defp block do
      receive do
        :never -> :ok
      end
    end
  end

  # DD-M6: a RECEIPT AUTHORITY that survives the run tree: a test-owned GenServer registering what the daemon side
  # has ACKed (delivered / queued) per message id with the payload hash it saw, answering reconcile from that
  # registry with the faithful protocol shape LocalPane parses (versioned, bound, outcome, positive attempt for
  # stored receipts), refusing a changed payload under the same key as conflict, and able to fault (no answer).
  # Explicit limits: no real pane, no bytes kept, one pane_ref, attempts count sends per key.
  defmodule ReceiptAuthority do
    @moduledoc false
    use GenServer

    def start_link(mode), do: GenServer.start_link(__MODULE__, mode)

    def register(pid, message_id, status, payload_hash),
      do: GenServer.call(pid, {:register, message_id, status, payload_hash}, :infinity)

    def release_register(pid, message_id), do: GenServer.call(pid, {:release_register, message_id})
    def set_mode(pid, mode), do: GenServer.call(pid, {:mode, mode})
    def lookup(pid, message_id), do: GenServer.call(pid, {:lookup, message_id})
    def answer(pid, message_id, payload_hash), do: GenServer.call(pid, {:answer, message_id, payload_hash})

    @impl true
    def init(mode), do: {:ok, %{mode: mode, registry: %{}, held: []}}

    @impl true
    # :hold_register parks the registration (and therefore the sender) until the test releases it: a FORCED delay
    # with no sleeps, proving the witness cannot be observed before the registration is committed
    def handle_call({:register, id, status, hash}, {caller, _} = from, %{mode: {:hold_register, notify}} = state) do
      # the parked registration is ACKNOWLEDGED to the test with key, hash and caller: the fact that proves the
      # call has parked, never a scheduler grace period
      send(notify, {:register_held, id, hash, caller})
      {:noreply, %{state | held: state.held ++ [{from, id, status, hash}]}}
    end

    def handle_call({:register, id, status, hash}, _from, state), do: {:reply, :ok, commit(state, id, status, hash)}

    # release EXACTLY the named registration; others stay parked
    def handle_call({:release_register, id}, _from, state) do
      {mine, rest} = Enum.split_with(state.held, fn {_from, held_id, _s, _h} -> held_id == id end)

      state =
        Enum.reduce(mine, state, fn {from, held_id, status, hash}, acc ->
          GenServer.reply(from, :ok)
          commit(acc, held_id, status, hash)
        end)

      {:reply, length(mine), %{state | held: rest}}
    end

    def handle_call({:mode, mode}, _from, state), do: {:reply, :ok, %{state | mode: mode}}
    def handle_call({:lookup, id}, _from, state), do: {:reply, Map.get(state.registry, id), state}

    def handle_call({:answer, _id, _hash}, _from, %{mode: :fault} = state),
      do: {:reply, {:error, %{"reason" => "receipt_authority_unavailable"}}, state}

    def handle_call({:answer, id, hash}, _from, state) do
      case Map.get(state.registry, id) do
        nil ->
          {:reply, {:ok, "absent", 0}, state}

        %{payload_hash: seen} when seen != hash ->
          {:reply, {:ok, "conflict", 0}, state}

        %{status: "queued", attempt: n} when state.mode == :queued_then_delivered ->
          # a faithful daemon drains its queue: the NEXT question sees the landed bytes
          {:reply, {:ok, "queued", n}, put_in(state, [:registry, id, :status], "delivered")}

        %{status: status, attempt: n} ->
          {:reply, {:ok, status, n}, state}
      end
    end

    defp commit(state, id, status, hash) do
      entry = Map.get(state.registry, id, %{attempt: 0})
      entry = Map.merge(%{entry | attempt: entry.attempt + 1}, %{status: status, payload_hash: hash})
      %{state | registry: Map.put(state.registry, id, entry)}
    end
  end

  # the pane client bound to the authority: send REGISTERS the ACK (status per test) BEFORE it blocks or answers,
  # so the receipt exists even if the Worker is lost afterwards; identity = SensitiveBytes.hash of the prompt
  defmodule AuthorityPane do
    @moduledoc false
    alias FakePaneClient, as: Fake

    def capabilities(opts), do: Fake.capabilities(opts)
    def status(pane_ref, opts), do: Fake.status(pane_ref, opts)

    def reconcile(pane_ref, message_id, opts) do
      collector = Keyword.fetch!(opts, :collector)
      hash = Keyword.get(opts, :payload_hash)

      answer =
        if Keyword.get(opts, :exhausted_absent, false),
          do: {:ok, "absent", 2},
          else: ReceiptAuthority.answer(Keyword.fetch!(opts, :authority), message_id, hash)

      case answer do
        {:ok, outcome, attempt} ->
          send(collector, {:pane_call, :reconcile, self(), pane_ref, message_id, outcome})

          answer = %{
            "ok" => true,
            "protocol_version" => 2,
            "outcome" => outcome,
            "msg_id" => message_id,
            "pane_id" => pane_ref
          }

          answer =
            if attempt > 0, do: Map.merge(answer, %{"delivery_attempt" => attempt, "status" => outcome}), else: answer

          {:ok, answer}

        {:error, _} = fault ->
          send(collector, {:pane_call, :reconcile, self(), pane_ref, message_id, :fault})
          fault
      end
    end

    def send(pane_ref, prompt, opts) do
      collector = Keyword.fetch!(opts, :collector)
      hash = SensitiveBytes.hash(prompt)
      # DD-M9: the registration is ACKNOWLEDGED (a synchronous call) BEFORE the witness is emitted, so a lookup that
      # follows the witness can never race the registration
      :ok =
        ReceiptAuthority.register(
          Keyword.fetch!(opts, :authority),
          opts[:message_id],
          Keyword.get(opts, :ack_as, "delivered"),
          hash
        )

      send(collector, {:pane_call, :send, self(), pane_ref, opts[:message_id], is_struct(prompt, SensitiveBytes), hash})
      if Keyword.get(opts, :block_send, false), do: block()
      Fake.send(pane_ref, prompt, opts)
    end

    defp block do
      receive do
        :never -> :ok
      end
    end
  end

  # the MUTATION DC-16 must catch: the witness is emitted BEFORE the registration (the rev-3 ordering)
  defmodule WitnessFirstPane do
    @moduledoc false
    alias FakePaneClient, as: Fake

    def capabilities(opts), do: Fake.capabilities(opts)
    def status(pane_ref, opts), do: Fake.status(pane_ref, opts)
    defdelegate reconcile(pane_ref, message_id, opts), to: AuthorityPane

    def send(pane_ref, prompt, opts) do
      hash = SensitiveBytes.hash(prompt)
      send(Keyword.fetch!(opts, :collector), {:pane_call, :send, self(), pane_ref, opts[:message_id], true, hash})
      :ok = ReceiptAuthority.register(Keyword.fetch!(opts, :authority), opts[:message_id], "delivered", hash)
      Fake.send(pane_ref, prompt, opts)
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "delivery-deadline-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  defp config(dir, dispatch, extra_dispatch_opts, extra_opts \\ [], mode \\ :run) do
    {_, :run, "gated_run_seed", [], make} = hd(H.cases())
    H.reset_seams()
    base = make.()

    dispatch_opts =
      base
      |> Keyword.get(:dispatch_opts, [])
      |> Keyword.put(:collector, collector())
      |> Keyword.merge(extra_dispatch_opts)

    opts =
      base
      |> Keyword.drop(@owned)
      |> Keyword.put(:supervisor_instance, @instance)
      |> Keyword.put(:dispatch, dispatch)
      |> Keyword.put(:dispatch_opts, dispatch_opts)
      |> Keyword.put(:default_assignment_timeout_s, @timeout_s)
      |> Keyword.merge(extra_opts)

    opts = if mode == :resume, do: Keyword.put(opts, :recovery_reason, "crash_recovery"), else: opts

    %{
      run_dir: dir,
      mode: mode,
      spec: H.spec("gated_run_seed"),
      plan: H.plan("gated_run_seed"),
      opts: opts,
      trace: collector()
    }
  end

  defp wait_for(fun, timeout_ms), do: wait(fun, System.monotonic_time(:millisecond) + timeout_ms)

  defp wait(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(20) && wait(fun, deadline)
    end
  end

  # consume every pane_call queued so far (the first attempt's calls), so a later listing is the resume's ONLY
  defp drain_pane_calls do
    receive do
      {:pane_call, _, _, _, _, _} -> drain_pane_calls()
      {:pane_call, _, _, _, _, _, _} -> drain_pane_calls()
    after
      0 -> :ok
    end
  end

  defp pane_calls(pid) do
    {:messages, messages} = Process.info(pid, :messages)

    for m <- messages, is_tuple(m) and elem(m, 0) == :pane_call do
      case m do
        {:pane_call, :reconcile, _, _, id, outcome} -> {:reconcile, id, outcome}
        {:pane_call, :send, _, _, id, sensitive?, digest} -> {:send, id, sensitive?, digest}
      end
    end
  end

  defp durable_deadline!(dir) do
    event = dir |> journal() |> Enum.find(&(&1["type"] == "assignment_requested"))
    assert event, "assignment_requested is durable"
    event["data"]["deadline_unix"]
  end

  # kill9 the whole tree and wait for the Writer ownership release (a resume before that is second_live_writer)
  defp kill9!(dir, root) do
    Process.exit(root, :kill)
    assert wait_for(fn -> not Process.alive?(root) end, 5_000)
    assert wait_for(fn -> Ownership.status(dir) == :none end, 5_000)
  end

  defp start!(config) do
    assert {:ok, root} = Run.Supervisor.start_link(config)
    track!(root)
    assert_receive {:run_child_started, ^root, :server, server}, 10_000
    assert_receive {:run_child_started, ^root, :work, work}, 10_000
    %{root: root, server: server, work: work}
  end

  defp stop!(root) do
    if Process.alive?(root), do: Supervisor.stop(root, :shutdown, 10_000)
    :ok
  end

  defp journal(dir),
    do: dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp tail(dir, n), do: dir |> journal() |> Enum.take(-n) |> Enum.map(&{&1["type"], &1["data"]})

  describe "measured baselines on the unchanged product (DC-1..DC-3)" do
    test "DC-1 deliver {:error, map} today: durable attention with detector dispatch_failed, run blocked", %{dir: dir} do
      facts = start!(config(dir, ScriptedDeliver, deliver_result: {:error, %{"reason" => "dd_probe_deliver_error"}}))
      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => [_]}}} = Server.await(facts.server, 30_000)
      assert Server.status(facts.server) == :finished

      # DD-M5: the FULL emitted payloads, nothing dropped: the wedge carries SYNTHETIC defaults (stale_for_ms 200000,
      # pending_count 0) that no measurement produced, and NEITHER event carries a deadline field
      assert [
               {"agent_wedge_detected",
                %{
                  "agent_ref" => "writer_agent",
                  "assignment_id" => "as_0001",
                  "detector" => "dispatch_failed",
                  "pane_ref" => "pane_writer",
                  "pane_state" => "unknown",
                  "pending_count" => 0,
                  "reason" => "dd_probe_deliver_error",
                  "stale_for_ms" => 200_000
                }},
               {"human_attention_required",
                %{
                  "attention_id" => "att_0001",
                  "blocking_entity" => "as_0001",
                  "reason" => "dd_probe_deliver_error",
                  "resume_command" => "run --resume RUN_DIR",
                  "summary_hash" => "sha256:" <> String.duplicate("0", 64),
                  "summary_path" => "attention/att_0001.org"
                }}
             ] == tail(dir, 2)

      stop!(facts.root)
    end

    test "DC-2 queued send then reconcile {:error, map} today: dispatch_reconcile_failed / unknown, run blocked", %{
      dir: dir
    } do
      # the adapter's cause carries a deadline_unix and a detailed reason on purpose: NEITHER survives this route
      facts =
        start!(
          config(dir, QueuedThenScriptedReconcile,
            reconcile_result: {:error, %{"reason" => "dd_probe_reconcile_error", "deadline_unix" => 123}}
          )
        )

      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts.server, 30_000)

      assert [{"assignment_dispatch_sent", %{"assignment_id" => "as_0001"}} | _] =
               dir |> tail(3) |> Enum.take(1) |> Enum.map(fn {t, d} -> {t, Map.take(d, ["assignment_id"])} end)

      # DD-M5: FULL payloads: the reconcile route builds a FIXED map (dispatch_reconcile_failed / dispatch_reconcile /
      # unknown), drops the adapter's reason and its deadline, and adds the same synthetic wedge defaults
      assert [
               {"agent_wedge_detected",
                %{
                  "agent_ref" => "writer_agent",
                  "assignment_id" => "as_0001",
                  "detector" => "dispatch_reconcile",
                  "pane_ref" => "pane_writer",
                  "pane_state" => "unknown",
                  "pending_count" => 0,
                  "reason" => "dispatch_reconcile_failed",
                  "stale_for_ms" => 200_000
                }},
               {"human_attention_required",
                %{
                  "attention_id" => "att_0001",
                  "blocking_entity" => "as_0001",
                  "reason" => "dispatch_reconcile_failed",
                  "resume_command" => "run --resume RUN_DIR",
                  "summary_hash" => "sha256:" <> String.duplicate("0", 64),
                  "summary_path" => "attention/att_0001.org"
                }}
             ] == tail(dir, 2)

      stop!(facts.root)
    end

    test "DC-3 (DD-M1) a blocked deliver runs IN the Worker and does NOT expire",
         %{dir: dir} do
      # the configured clock is the real SystemClock for BOTH the reducer (it journals the deadline) and this oracle
      facts = start!(config(dir, BlockingDeliver, [], clock: SystemClock))
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      assert_receive {:deliver_entered, entered_in, "as_0001"}, 30_000
      # GREEN transition (recorded table): the deliver adapter runs in the Worker's TASK, never in the Worker
      refute entered_in == worker, "GREEN: the deliver adapter runs in the Worker's task"
      deadline = durable_deadline!(dir)
      assert is_integer(deadline) and deadline > SystemClock.unix_now() - @timeout_s - 5, "a live journaled deadline"
      # positive due proof in the configured clock's own domain (never elapsed sleep or timeout_s arithmetic)
      assert wait_for(fn -> SystemClock.unix_now() > deadline end, (@timeout_s + 8) * 1_000)
      assert SystemClock.unix_now() > deadline
      # GREEN transition: at the journaled deadline the fence expires the blocked deliver: attention-only, blocked
      assert {:ok, %{summary: %{"status" => "blocked", "open_attention_ids" => ["att_0001"]}}} =
               Server.await(facts.server, 30_000)

      assert {"human_attention_required", %{"reason" => "dispatch_deadline_exceeded"}} = dir |> tail(1) |> hd()
      refute Enum.any?(journal(dir), &(&1["type"] == "agent_wedge_detected"))
      refute Process.alive?(entered_in), "the blocked task was killed and joined"
      assert Process.alive?(worker)
      # bounded teardown: the tree stops within its budget and the Worker is joined
      worker_mon = Process.monitor(worker)
      stop!(facts.root)
      assert_receive {:DOWN, ^worker_mon, :process, ^worker, _}, 10_000
    end

    test "DC-7 (DD-M2) loss window",
         %{dir: dir} do
      cfg = config(dir, LocalPane, [pane_client: WitnessPane, block_send: true], default_assignment_timeout_s: 600)
      prompt_root = Keyword.fetch!(cfg.opts, :prompt_root)
      facts = start!(cfg)
      assert_receive {:run_child_started, _, :worker, worker}, 10_000
      assert_receive {:pane_call, :send, sender, "pane_writer", mid, true, digest}, 30_000
      # GREEN transition (recorded table): the send runs in the Worker's TASK; identity and receipts are unchanged
      refute sender == worker, "GREEN: the send runs in the Worker's task"
      assert String.starts_with?(mid, "snd_")
      # the first attempt asked the daemon exactly once before sending, with the same message id
      assert pane_calls(self()) == [{:reconcile, mid, "absent"}]
      assert match?({"assignment_prompt_projected", _}, dir |> tail(1) |> hd()), "no receipt of any kind before the loss"
      kill9!(dir, facts.root)
      before = length(journal(dir))
      OwnedHarness.flush!()
      drain_pane_calls()

      facts2 =
        start!(
          config(
            dir,
            LocalPane,
            [pane_client: WitnessPane],
            [default_assignment_timeout_s: 600, prompt_root: prompt_root],
            :resume
          )
        )

      assert {:ok, %{summary: %{"status" => "completed"}}} = Server.await(facts2.server, 60_000)
      OwnedHarness.flush!()
      # MEASURED order on resume (first-attempt calls drained; an earlier draft counted one of them as a second
      # resume reconcile): the daemon is asked ONCE with the SAME message id, then ONE send with the SAME prompt
      # digest; the second assignment (a different id) follows. The absent answer is the FAKE daemon's, which cannot
      # know whether the interrupted bytes landed; DC-9/DC-10 witness the landed case with a surviving authority.
      resumed = pane_calls(self())
      assert [{:reconcile, ^mid, "absent"}, {:send, ^mid, true, ^digest} | rest] = resumed
      assert [{:reconcile, other, "absent"}, {:send, other, true, _}] = rest
      refute other == mid

      suffix = dir |> journal() |> Enum.drop(before)

      assert Enum.map(Enum.take(suffix, 7), & &1["type"]) ==
               ~w(run_resumed workspace_lease_release_requested workspace_lease_released workspace_lease_acquired pane_lease_release_requested pane_lease_released pane_lease_acquired)

      sent = Enum.filter(suffix, &(&1["type"] == "assignment_dispatch_sent"))
      assert [%{"data" => %{"send_message_id" => ^mid, "send_status" => "ok", "replayed" => false}}, _second] = sent
      stop!(facts2.root)
    end

    test "DC-7b (DD-M2) the same loss window resumed WITHOUT the original prompt root: prompt_fetch_failed attention, no send",
         %{dir: dir} do
      facts =
        start!(config(dir, LocalPane, [pane_client: WitnessPane, block_send: true], default_assignment_timeout_s: 600))

      assert_receive {:pane_call, :send, _, _, mid, true, _}, 30_000
      kill9!(dir, facts.root)
      before = length(journal(dir))
      OwnedHarness.flush!()
      drain_pane_calls()
      facts2 = start!(config(dir, LocalPane, [pane_client: WitnessPane], [default_assignment_timeout_s: 600], :resume))
      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts2.server, 60_000)
      OwnedHarness.flush!()
      # MEASURED: the prompt fetch fails BEFORE any dispatch, so the resume makes NO pane call at all (an earlier draft
      # counted the first attempt's reconcile); the message id of the lost attempt is never re-presented
      assert pane_calls(self()) == [], "no pane call: the fetch failed before any dispatch"
      assert String.starts_with?(mid, "snd_")
      assert {"human_attention_required", %{"reason" => "prompt_fetch_failed"}} = dir |> tail(1) |> hd()
      assert length(journal(dir)) == before + 8
      stop!(facts2.root)
    end

    test "DC-8 (DD-M2) resume at the COMMITTED blocked state (after DC-1)",
         %{dir: dir} do
      facts = start!(config(dir, ScriptedDeliver, deliver_result: {:error, %{"reason" => "dd_probe_deliver_error"}}))
      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts.server, 30_000)
      stop!(facts.root)
      before = journal(dir)
      OwnedHarness.flush!()
      cfg = config(dir, LocalPane, [pane_client: WitnessPane], [], :resume)

      # MEASURED: under this scenario config the tree starts and the Server answers the refusal (an earlier probe
      # with the kill9 case opts saw the same refusal from start_link itself); both are the same closed answer
      refused =
        case Run.Supervisor.start_link(cfg) do
          {:error, refusal} ->
            refusal

          {:ok, root} ->
            track!(root)
            assert_receive {:run_child_started, ^root, :server, server}, 10_000
            {:error, refusal} = Server.await(server, 30_000)
            stop!(root)
            refusal
        end

      assert refused == %{"reason" => "attention_required", "open_attention_ids" => ["att_0001"]}
      assert pane_calls(self()) == []
      assert journal(dir) == before
    end
  end

  describe "receipt authority (DD-M6): a landed/queued ACK that survives the Worker decides; no second send, no manufactured absence" do
    setup do
      {:ok, authority} = ReceiptAuthority.start_link(:faithful)
      track!(authority)
      {:ok, authority: authority}
    end

    defp authority_config(dir, authority, extra_dispatch, extra_opts \\ [], mode \\ :run) do
      config(
        dir,
        LocalPane,
        Keyword.merge([pane_client: AuthorityPane, authority: authority], extra_dispatch),
        [default_assignment_timeout_s: 600] ++ extra_opts,
        mode
      )
    end

    defp lost_after_ack!(dir, authority, ack_as) do
      cfg = authority_config(dir, authority, block_send: true, ack_as: ack_as)
      prompt_root = Keyword.fetch!(cfg.opts, :prompt_root)
      facts = start!(cfg)
      assert_receive {:pane_call, :send, _, "pane_writer", mid, true, hash}, 30_000
      # the ACK is registered (with the prompt's SensitiveBytes.hash) BEFORE the Worker is lost
      assert %{status: ^ack_as, payload_hash: ^hash, attempt: 1} = ReceiptAuthority.lookup(authority, mid)
      assert match?({"assignment_prompt_projected", _}, dir |> tail(1) |> hd()), "no receipt reached the journal"
      kill9!(dir, facts.root)
      OwnedHarness.flush!()
      drain_pane_calls()
      %{mid: mid, hash: hash, prompt_root: prompt_root, before: length(journal(dir))}
    end

    defp resumed_calls(dir, authority, %{prompt_root: prompt_root}, extra_dispatch \\ []) do
      facts = start!(authority_config(dir, authority, extra_dispatch, [prompt_root: prompt_root], :resume))
      result = Server.await(facts.server, 60_000)
      OwnedHarness.flush!()
      calls = pane_calls(self())
      stop!(facts.root)
      {result, calls}
    end

    test "DC-9 landed before the loss",
         %{dir: dir, authority: authority} do
      %{mid: mid, hash: hash, before: before} = lost = lost_after_ack!(dir, authority, "delivered")
      {result, calls} = resumed_calls(dir, authority, lost)
      assert {:ok, %{summary: %{"status" => "completed"}}} = result
      # the first assignment: one reconcile answered from the surviving receipt, NO send; then the next assignment
      assert [{:reconcile, ^mid, "delivered"} | rest] = calls
      refute Enum.any?(rest, &match?({:send, ^mid, _, _}, &1)), "ZERO second send for the landed message"
      assert [{:reconcile, other, "absent"}, {:send, other, true, _}] = rest
      refute other == mid
      # identity proof: the receipt's hash is the prompt's SensitiveBytes.hash AND the journaled payload_hash
      projected = dir |> journal() |> Enum.find(&(&1["type"] == "assignment_prompt_projected"))
      assert projected["data"]["prompt_hash"] == hash
      sent = dir |> journal() |> Enum.drop(before) |> Enum.find(&(&1["type"] == "assignment_dispatch_sent"))
      assert %{"send_message_id" => ^mid, "send_status" => "reconciled", "replayed" => true} = sent["data"]
      assert ReceiptAuthority.lookup(authority, mid).attempt == 1, "the authority saw exactly one send for this key"
    end

    test "DC-10 queued before the loss: resume reconciles QUEUED, converges on the daemon's drain, ZERO second send, completed",
         %{dir: dir, authority: authority} do
      %{mid: mid} = lost = lost_after_ack!(dir, authority, "queued")
      :ok = ReceiptAuthority.set_mode(authority, :queued_then_delivered)
      {result, calls} = resumed_calls(dir, authority, lost)
      assert {:ok, %{summary: %{"status" => "completed"}}} = result
      mine = Enum.filter(calls, fn c -> elem(c, 1) == mid end)
      assert [{:reconcile, ^mid, "queued"} | more] = mine
      refute Enum.any?(mine, &match?({:send, _, _, _}, &1)), "ZERO second send for the queued message"
      assert Enum.any?(more, &match?({:reconcile, ^mid, "delivered"}, &1)), "convergence saw the drain"
      assert ReceiptAuthority.lookup(authority, mid).attempt == 1
    end

    test "DC-11 the authority answers AMBIGUOUS: refused for attention, no send, no absence manufactured",
         %{dir: dir, authority: authority} do
      %{mid: mid} = lost = lost_after_ack!(dir, authority, "ambiguous")
      {result, calls} = resumed_calls(dir, authority, lost)
      assert {:ok, %{summary: %{"status" => "blocked"}}} = result
      assert [{:reconcile, ^mid, "ambiguous"}] = calls
      assert {"human_attention_required", %{"reason" => "dispatch_reconcile_ambiguous"}} = dir |> tail(1) |> hd()
      assert ReceiptAuthority.lookup(authority, mid).attempt == 1
    end

    test "DC-12 the authority FAULTS (no answer): the fresh send is refused for attention, no send, no absence manufactured",
         %{dir: dir, authority: authority} do
      :ok = ReceiptAuthority.set_mode(authority, :fault)
      facts = start!(authority_config(dir, authority, []))
      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts.server, 30_000)
      OwnedHarness.flush!()
      assert [{:reconcile, mid, :fault}] = pane_calls(self())
      assert String.starts_with?(mid, "snd_")
      assert {"human_attention_required", %{"reason" => "receipt_authority_unavailable"}} = dir |> tail(1) |> hd()
      assert ReceiptAuthority.lookup(authority, mid) == nil, "nothing was ever sent"
      stop!(facts.root)
    end

    test "DC-13 a CHANGED payload under the SAME key is a conflict: refused for attention, no send",
         %{dir: dir, authority: authority} do
      %{mid: mid} = lost = lost_after_ack!(dir, authority, "delivered")
      :ok = ReceiptAuthority.register(authority, mid, "delivered", "sha256:" <> String.duplicate("f", 64))
      {result, calls} = resumed_calls(dir, authority, lost)
      assert {:ok, %{summary: %{"status" => "blocked"}}} = result
      assert [{:reconcile, ^mid, "conflict"}] = calls
      assert {"human_attention_required", %{"reason" => "dispatch_reconcile_conflict"}} = dir |> tail(1) |> hd()
    end

    test "DC-15 (DD-M9/M14) forced held registration: the authority ACKs the parked call; no witness until release",
         %{dir: dir, authority: authority} do
      :ok = ReceiptAuthority.set_mode(authority, {:hold_register, self()})
      facts = start!(authority_config(dir, authority, block_send: true))
      assert_receive {:pane_call, :reconcile, _, _, mid, "absent"}, 30_000
      # the acknowledged held-registration FACT (key, hash, caller) is consumed first: the register call has parked
      assert_receive {:register_held, ^mid, held_hash, caller}, 5_000
      assert is_pid(caller) and String.starts_with?(held_hash, "sha256:")
      # only now: no witness yet, no registry entry (facts, not a grace period)
      refute_received {:pane_call, :send, _, _, _, _, _}
      assert ReceiptAuthority.lookup(authority, mid) == nil
      assert ReceiptAuthority.release_register(authority, mid) == 1, "exactly this registration released"
      assert_receive {:pane_call, :send, ^caller, _, ^mid, true, ^held_hash}, 5_000
      assert %{status: "delivered", payload_hash: ^held_hash, attempt: 1} = ReceiptAuthority.lookup(authority, mid)
      kill9!(dir, facts.root)
    end

    test "DC-16 (DD-M14) the witness-before-register mutation is caught for the intended reason",
         %{dir: dir, authority: authority} do
      :ok = ReceiptAuthority.set_mode(authority, {:hold_register, self()})
      facts = start!(authority_config(dir, authority, pane_client: WitnessFirstPane))
      assert_receive {:pane_call, :reconcile, _, _, mid, "absent"}, 30_000
      # the mutated client emits the witness BEFORE registering: the witness is observed while the registry is empty
      assert_receive {:pane_call, :send, _, _, ^mid, true, _}, 5_000
      assert ReceiptAuthority.lookup(authority, mid) == nil, "a lookup after the witness sees NO receipt: the race"
      assert_receive {:register_held, ^mid, _, _}, 5_000
      assert ReceiptAuthority.release_register(authority, mid) == 1
      kill9!(dir, facts.root)
    end

    test "DC-14 (DD-M7) the FRESH path's attempt bound is adapter-internal",
         %{dir: dir, authority: authority} do
      # the authority names the exhausted attempt for a key nothing registered: absent with the daemon's count
      facts = start!(authority_config(dir, authority, exhausted_absent: true))
      assert {:ok, %{summary: %{"status" => "blocked"}}} = Server.await(facts.server, 30_000)
      OwnedHarness.flush!()
      assert [{:reconcile, mid, "absent"}] = pane_calls(self())
      assert {"human_attention_required", %{"reason" => "dispatch_attempts_exhausted"}} = dir |> tail(1) |> hd()
      assert ReceiptAuthority.lookup(authority, mid) == nil, "no send happened"

      refute Enum.any?(journal(dir), &(&1["type"] == "assignment_dispatch_sent")),
             "no queued receipt: the reducer's retry authority was never entered"

      stop!(facts.root)
    end
  end

  describe "source facts pinned (DC-4..DC-6)" do
    test "DC-4 direct Effects (GREEN transition): a supplied runner IS invoked for both effects; :expired answers the stable reasons; no-runner path unchanged" do
      test = self()

      runner = fn _closure, %{deadline_unix: d} ->
        send(test, {:runner_invoked, d})
        :expired
      end

      deliver_opts = [dispatch: ScriptedDeliver, dispatch_opts: [deliver_result: {:error, %{"reason" => "x"}}]]

      dispatch = %Effect.Dispatch{
        assignment_id: "as_0001",
        command: %{"assignment_id" => "as_0001"},
        message_id: "m1",
        deadline_unix: 7
      }

      {%Observation.DispatchFailed{reason: reason_d}, _} =
        Effects.execute(dispatch, Runtime.new([]), opts: deliver_opts ++ [adapter_runner: runner])

      assert reason_d == %{"reason" => "dispatch_deadline_exceeded", "detector" => "dispatch_deadline"}
      assert_receive {:runner_invoked, 7}, 1_000

      reconcile_opts = [
        dispatch: QueuedThenScriptedReconcile,
        dispatch_opts: [reconcile_result: {:error, %{"reason" => "x"}}]
      ]

      reconcile = %Effect.ReconcileSend{
        assignment_id: "as_0001",
        command: %{"assignment_id" => "as_0001"},
        deadline_unix: 9
      }

      {%Observation.SendReconcileFailed{reason: reason_r}, _} =
        Effects.execute(reconcile, Runtime.new([]), opts: reconcile_opts ++ [adapter_runner: runner])

      assert reason_r == %{"reason" => "dispatch_reconcile_timeout", "detector" => "dispatch_reconcile"}
      assert_receive {:runner_invoked, 9}, 1_000
      # the NO-runner legacy path is unchanged: the adapter runs here and answers its own failure
      {%Observation.DispatchFailed{reason: %{"reason" => "x"}}, _} =
        Effects.execute(dispatch, Runtime.new([]), opts: deliver_opts)

      refute_receive {:runner_invoked, _}, 100
    end

    test "DC-5 (GREEN transition) the deliver/reconcile effect structs carry the propagated deadline field" do
      assert Enum.sort(Map.keys(%Effect.Dispatch{assignment_id: "a", command: %{}, message_id: "m"}) -- [:__struct__]) ==
               [:assignment_id, :command, :deadline_unix, :message_id]

      assert Enum.sort(Map.keys(%Effect.ReconcileSend{assignment_id: "a", command: %{}}) -- [:__struct__]) ==
               [:assignment_id, :command, :deadline_unix]
    end

    test "DC-6 reconcile outcome mapping at direct Effects: the five ratified outcomes and an invalid one" do
      reconcile = %Effect.ReconcileSend{assignment_id: "as_0001", command: %{"assignment_id" => "as_0001"}}

      for outcome <- ~w(delivered queued absent ambiguous conflict) do
        answer = {:ok, %{"outcome" => outcome, "delivery_attempt" => 1}}
        opts = [dispatch: QueuedThenScriptedReconcile, dispatch_opts: [reconcile_result: answer]]

        assert {%Observation.SendReconciled{outcome: ^outcome, delivery_attempt: 1}, _} =
                 Effects.execute(reconcile, Runtime.new([]), opts: opts)
      end

      opts = [
        dispatch: QueuedThenScriptedReconcile,
        dispatch_opts: [reconcile_result: {:ok, %{"outcome" => "elsewhere", "delivery_attempt" => 1}}]
      ]

      assert {%Observation.SendReconcileFailed{reason: %{"reason" => "dispatch_reconcile_invalid_return"}}, _} =
               Effects.execute(reconcile, Runtime.new([]), opts: opts)
    end
  end
end
