defmodule AiOrchestrator.Run.AdapterInterruptionSpikeTest do
  @moduledoc """
  U2a-M test-only mechanism spike, revision 2 (docs/contracts/adapter-interruption-spike.org, AM-S1..S6 and the
  review corrections AM-M1..M4): an owner shaped like the Worker keeps its Runtime, Port and memo while an
  ASSIGNMENT adapter runs as a monitored OTP Task under an owner-linked Task.Supervisor, inside a task-side
  runner boundary, and is interrupted by a due fence decided through the delivered Run.DeadlineFence. Evidence
  on unchanged 012086b; nothing here is product wiring.
  """
  use ExUnit.Case, async: false

  import AiOrchestrator.Test.OwnedHarness, only: [track!: 1, track_dir!: 1]
  import ExUnit.CaptureLog

  alias AiOrchestrator.Effects.Runtime
  # ---- the owner double -------------------------------------------------------------------------------------------
  alias AiOrchestrator.Run.DeadlineFence
  alias AiOrchestrator.Test.OwnedHarness

  @canary "ADAPTER-SPIKE-PRIVATE-CANARY-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
  @gate_key {"gr_spike", 1}

  defmodule SpikeOwner do
    @moduledoc false
    use GenServer

    alias AiOrchestrator.Contract.Diagnostic

    @reason_classes [:normal, :killed, :shutdown]

    def start(opts), do: GenServer.start(__MODULE__, opts)

    @impl true
    def init(opts) do
      port = Port.open({:spawn_executable, "/bin/cat"}, [:binary, :stream])
      {:ok, sup} = Task.Supervisor.start_link()
      Process.put({__MODULE__, :sentinel}, Keyword.fetch!(opts, :sentinel))

      runtime =
        []
        |> Runtime.new()
        |> Runtime.put(Keyword.fetch!(opts, :gate_key), :prepared, %{label: :fake_handle_ownership_witness, port: port})

      {:ok, %{port: port, sup: sup, runtime: runtime, op: nil, history: %{}, seq: 0, facts: [], kills: 0}}
    end

    # ---- admission: one in flight, monitor installed BEFORE the task may run the adapter ---------------------------

    @impl true
    def handle_call({:start_op, _identity, _adapter, _arm}, _from, %{op: %{closed: false}} = state),
      do: {:reply, {:error, :operation_outstanding}, fact(state, :start_refused_outstanding)}

    def handle_call({:start_op, identity, adapter, arm}, _from, state) do
      state = retire_closed(state)
      seq = state.seq + 1
      token = make_ref()
      task = Task.Supervisor.async_nolink(state.sup, runner(adapter, token))
      death = Process.monitor(task.pid)
      {:ok, outstanding} = DeadlineFence.outstanding(identity)

      op = %{
        seq: seq,
        identity: identity,
        task: task,
        death: death,
        outstanding: outstanding,
        fence: nil,
        delivery: nil,
        replied: false,
        closed: false
      }

      state = fact(%{state | op: op, seq: seq}, {:op_started, %{task_ref: task.ref, pid: task.pid, death_ref: death}})
      # GO only after registration and both monitors exist: the adapter never runs before its owner can see it
      send(task.pid, {:go, token})
      state = arm(state, arm)
      {:reply, {:ok, task.pid, task.ref, seq}, state}
    end

    def handle_call({:hand_fence, identity, fence}, _from, state) do
      state = store_fence(state, identity, fence)
      send(self(), {:deadline_fence, identity, :hand})
      {:reply, :ok, state}
    end

    def handle_call({:set_fence, identity, fence}, _from, state), do: {:reply, :ok, store_fence(state, identity, fence)}

    def handle_call({:observe_early, pid}, _from, state), do: {:reply, :ok, Map.put(state, :observer, pid)}

    def handle_call(:snapshot, _from, state) do
      {:reply,
       %{
         seq: state.seq,
         facts: Enum.reverse(state.facts),
         state: state.op && state.op.outstanding.state,
         delivery: state.op && state.op.delivery,
         closed: state.op && state.op.closed,
         kills: state.kills,
         runtime_entry: Runtime.handle(state.runtime, {"gr_spike", 1}),
         sentinel: Process.get({__MODULE__, :sentinel}),
         port: state.port,
         sup: state.sup,
         task_pid: state.op && state.op.task.pid,
         task_ref: state.op && state.op.task.ref,
         sup_children: Task.Supervisor.children(state.sup)
       }, state}
    end

    # a bounded Port round-trip proves the owner still drives its Port (fail-closed on timeout)
    def handle_call(:port_roundtrip, _from, %{port: port} = state) do
      payload = "ping-" <> Integer.to_string(System.unique_integer([:positive])) <> "\n"
      true = Port.command(port, payload)

      reply =
        receive do
          {^port, {:data, ^payload}} -> :ok
        after
          2_000 -> :timeout
        end

      {:reply, reply, state}
    end

    # ---- facts dequeued by the GenServer loop; only fence and result facts go through the fence -----------------

    @impl true
    def handle_info({ref, result}, %{op: %{task: %{ref: ref}, replied: false}} = state) do
      Process.demonitor(ref, [:flush])
      state = fact(%{state | op: %{state.op | replied: true}}, {:reply, result})
      {:noreply, observe(state, {:result, state.op.identity}, :reply)}
    end

    # a second message on the CURRENT task ref after its reply: a duplicate result, classified by the fence
    def handle_info({ref, result}, %{op: %{task: %{ref: ref}, replied: true}} = state),
      do: {:noreply, state |> fact({:duplicate_reply, result}) |> observe({:result, state.op.identity}, :reply)}

    def handle_info({:DOWN, ref, :process, pid, reason}, %{op: %{task: %{ref: ref, pid: pid}}} = state),
      do: {:noreply, fact(state, {:task_monitor_down_no_reply, death_fact(state.op, reason)})}

    def handle_info({:DOWN, ref, :process, pid, reason}, %{op: %{death: ref, task: %{pid: pid}}} = state) do
      state = %{state | op: %{state.op | closed: true}}
      {:noreply, fact(state, {:death_observed, death_fact(state.op, reason)})}
    end

    def handle_info(
          {:deadline_fence, identity, source},
          %{op: %{identity: identity, fence: fence, closed: false}} = state
        )
        when not is_nil(fence) do
      state = fact(state, {:fence_dequeued, source})

      case DeadlineFence.observe(state.op.outstanding, {:fence, fence, monotonic_now()}) do
        {:early, {:wait, ms}, _unchanged} ->
          arm_timer(identity, ms)
          # acknowledged observation AT handling time: state and kill count as they are right now
          notify_observer(state, {:early_observed, state.seq, state.op.outstanding.state, state.kills, ms})
          {:noreply, fact(state, {:early, ms})}

        {:ok, outstanding, :request_expiration} ->
          state = fact(%{state | op: %{state.op | outstanding: outstanding}}, {:kill_requested, state.op.task.ref})
          {:noreply, expire(state)}

        {:stale, reason, _unchanged} ->
          {:noreply, fact(state, {:stale_fence, reason})}
      end
    end

    def handle_info({:deadline_fence, identity, source}, state) do
      kind = if state.op && identity == state.op.identity, do: :closed_op, else: :foreign_or_absent
      {:noreply, fact(state, {:ignored_fence, kind, source})}
    end

    def handle_info({:settle, _cap, _gen, _ref}, state), do: {:noreply, fact(state, :settle_shaped_message_ignored)}

    def handle_info({ref, _result}, state) when is_reference(ref),
      do: {:noreply, fact(state, old_fact(state, :reply, ref))}

    def handle_info({:DOWN, ref, :process, _pid, _reason}, state),
      do: {:noreply, fact(state, old_fact(state, :down, ref))}

    def handle_info(_other, state), do: {:noreply, fact(state, :unknown_message_ignored)}

    # ---- the runner boundary: ANY adapter, raw failures closed inside the task with the existing vocabulary -----

    defp runner(adapter, token) do
      fn ->
        receive do
          {:go, ^token} -> :ok
        end

        try do
          {:ok, adapter.()}
        catch
          kind, reason -> {:closed_failure, kind, Diagnostic.describe(reason)}
        end
      end
    end

    # ---- the kill request: one shutdown, separate facts --------------------------------------------------------

    defp expire(%{op: op} = state) do
      state = %{state | kills: state.kills + 1}

      case Task.shutdown(op.task, :brutal_kill) do
        {:ok, result} ->
          state = %{state | op: %{state.op | replied: true}}

          state
          |> fact({:shutdown_return, :ok_reply})
          |> fact({:reply, result})
          |> observe({:result, op.identity}, :shutdown)

        {:exit, reason} ->
          state |> fact({:shutdown_return, {:exit, reason_class(reason)}}) |> unknown()

        nil ->
          state |> fact({:shutdown_return, nil}) |> unknown()
      end
    end

    defp unknown(state), do: fact(%{state | op: %{state.op | delivery: :unknown}}, {:delivery, :unknown})

    defp observe(state, event, via) do
      case DeadlineFence.observe(state.op.outstanding, event) do
        {:ok, outstanding, decision} ->
          fact(%{state | op: %{state.op | outstanding: outstanding}}, {:decision, decision, via})

        {:stale, reason, _} ->
          fact(state, {:stale_result, reason, via})
      end
    end

    defp retire_closed(%{op: nil} = state), do: state

    defp retire_closed(%{op: op} = state) do
      history = state.history |> Map.put(op.task.ref, op.seq) |> Map.put(op.death, op.seq)
      %{state | op: nil, history: history}
    end

    defp old_fact(state, kind, ref) do
      case Map.fetch(state.history, ref) do
        {:ok, seq} -> {:old_task_fact, kind, seq}
        :error -> {:unknown_ref_fact, kind}
      end
    end

    defp arm(state, nil), do: state

    defp arm(%{op: op} = state, {deadline_unix, unix_now, cap}) do
      {:ok, fence} = DeadlineFence.arm(op.identity, deadline_unix, unix_now, monotonic_now(), cap)

      case DeadlineFence.next(fence, monotonic_now()) do
        {:wait, ms} -> arm_timer(op.identity, ms)
        :due -> send(self(), {:deadline_fence, op.identity, :timer})
      end

      %{state | op: %{op | fence: fence}}
    end

    defp store_fence(%{op: %{identity: identity} = op} = state, identity, fence), do: %{state | op: %{op | fence: fence}}
    defp store_fence(state, _identity, _fence), do: state

    defp arm_timer(identity, ms), do: Process.send_after(self(), {:deadline_fence, identity, :timer}, ms)

    defp notify_observer(%{observer: pid}, message) when is_pid(pid), do: send(pid, message)
    defp notify_observer(_state, _message), do: :ok
    defp monotonic_now, do: System.monotonic_time(:millisecond)
    defp fact(state, fact), do: %{state | facts: [{state.seq, fact} | state.facts]}

    defp death_fact(op, reason),
      do: %{task_ref: op.task.ref, pid: op.task.pid, death_ref: op.death, reason: reason_class(reason)}

    # a death reason is recorded as a CLOSED class: three OTP atoms, everything else :other (no atom transfer)
    defp reason_class(reason) when reason in @reason_classes, do: reason
    defp reason_class(_reason), do: :other
  end

  defp blocking(test, tag) do
    fn ->
      send(test, {:adapter_started, tag, self()})

      receive do
        :never_sent -> :ok
      end
    end
  end

  defp gated_reply(test, tag) do
    fn ->
      send(test, {:adapter_started, tag, self()})

      receive do
        # ---- adapters: closures over their inputs and the ack channel only; failures are RAW (no self-catchi
        :reply_now -> {:ok, :delivered}
      end
    end
  end

  defp raw_failing(test, tag, kind, canary) do
    fn ->
      send(test, {:adapter_started, tag, self()})

      receive do
        :fail_now -> :ok
      end

      case kind do
        :error -> raise canary
        :throw -> throw(canary)
        :exit -> exit(canary)
      end
    end
  end

  defp raw_pre_ready_failure(canary), do: fn -> raise canary end

  defp marker_adapter(test, tag, path, :marker_before_block) do
    fn ->
      send(test, {:invoked, tag})
      File.write!(path, "marker\n")
      send(test, {:adapter_started, tag, self()})

      receive do
        :never_sent -> :ok
      end
    end
  end

  defp marker_adapter(test, tag, path, :block_before_marker) do
    fn ->
      send(test, {:invoked, tag})
      send(test, {:adapter_started, tag, self()})

      receive do
        :never_sent -> File.write!(path, "marker\n")
      end
    end
  end

  defp counted_normal(test, tag) do
    fn ->
      send(test, {:invoked, tag})
      {:ok, :done}
    end
  end

  # ---- OS children: an allocation AUTHORITY owns the Port and the record from BEFORE allocation (AM-M5) ----------

  # The authority is a test-tracked process started BEFORE any OS allocation. It owns the child's Port (so the
  # requesting task's death never loses it) and writes the record into :persistent_term at every transition:
  #   :not_started -> :allocating -> {:allocated_unidentified, reason} | {:identified, pid, start} -> :proven_absent
  # The cleanup oracle answers true ONLY for :not_started (nothing allocated) or :proven_absent; every other state
  # (including a missing record) is fail-closed false. Records are erased only after proven closure.
  defmodule OsAuthority do
    @moduledoc false
    use GenServer

    alias AiOrchestrator.Run.AdapterInterruptionSpikeTest, as: Spike

    def start(opts \\ []), do: GenServer.start(__MODULE__, opts)

    @impl true
    def init(_opts), do: {:ok, %{ports: %{}}}

    @impl true
    def handle_call({:register, key}, _from, state) do
      :persistent_term.put(key, :not_started)
      {:reply, :ok, state}
    end

    # seams: before_identity (fn -> :ok | :fail) runs after Port.open and before identity capture;
    # identity_failure forces the capture to fail (late identity failure)
    def handle_call({:allocate, key, seconds, opts}, _from, state) do
      :persistent_term.put(key, :allocating)
      args = ["-c", "echo $$; exec sleep #{seconds}"]
      port = Port.open({:spawn_executable, "/bin/sh"}, [:binary, :stream, :exit_status, args: args])
      state = %{state | ports: Map.put(state.ports, key, port)}
      # the seam receives the record state directly (it runs inside the authority: no self-call); a two-way
      # acknowledged barrier lives in the seam function itself (fail-closed, never timeout-released)
      Keyword.get(opts, :before_identity, fn _record -> :ok end).(:allocating)

      record =
        receive do
          {^port, {:data, line}} ->
            os_pid = String.trim(line)

            identify(os_pid, Keyword.get(opts, :identity_failure, false))
        after
          5_000 -> {:allocated_unidentified, :no_pid_line}
        end

      :persistent_term.put(key, record)
      {:reply, record, state}
    end

    # a task-owned allocation (the requesting task opened the Port itself) marks :allocating BEFORE its Port.open
    # and reports the captured identity afterwards; the authority holds the record, the task holds the Port
    def handle_call({:allocating, key}, _from, state) do
      :persistent_term.put(key, :allocating)
      {:reply, :ok, state}
    end

    def handle_call({:report, key, os_pid, start}, _from, state) do
      record =
        if is_binary(start) and start != "",
          do: {:identified, os_pid, start},
          else: {:allocated_unidentified, :identity_capture_failed}

      :persistent_term.put(key, record)
      {:reply, record, state}
    end

    def handle_call({:record, key}, _from, state), do: {:reply, :persistent_term.get(key, :missing), state}

    # the exit of an authority-owned Port's process is OBSERVED and stored: the only admissible proof of absence
    # without an identity-bound reap
    @impl true
    def handle_info({port, {:exit_status, status}}, state) do
      case Enum.find(state.ports, fn {_key, p} -> p == port end) do
        {key, _} ->
          record =
            case :persistent_term.get(key, :missing) do
              {:identified, os_pid, _start} -> {:exited, os_pid, status}
              {:allocated_unidentified, _} -> {:exited_unidentified, status}
              other -> other
            end

          :persistent_term.put(key, record)
          {:noreply, %{state | ports: Map.delete(state.ports, key)}}

        nil ->
          {:noreply, state}
      end
    end

    def handle_info(_other, state), do: {:noreply, state}

    defp identify(_os_pid, true), do: {:allocated_unidentified, :identity_capture_failed}

    defp identify(os_pid, false) do
      case Spike.os_cmd("ps", ["-o", "lstart=", "-p", os_pid]) do
        {:ok, start} when start != "" -> {:identified, os_pid, start}
        _ -> {:allocated_unidentified, :identity_capture_failed}
      end
    end
  end

  # ---- structured, bounded OS probes (AM-M6): outcomes are never collapsed ----------------------------------------

  @type cmd_outcome :: {:ok, String.t()} | {:exit, pos_integer(), String.t()} | :timeout | {:raised, atom()}

  # the runner seam lets a control inject command failures while a REAL tracked child is alive
  def os_cmd(cmd, args) do
    case Process.get({__MODULE__, :cmd_runner}) do
      nil -> real_os_cmd(cmd, args)
      runner -> runner.(cmd, args)
    end
  end

  # a raise inside the command is caught INSIDE the task (never kills the caller); a timeout is reported as such,
  # and killing the probe task after the bound is NOT a claim that the external command closed (S-4 limit)
  defp real_os_cmd(cmd, args) do
    task =
      Task.async(fn ->
        try do
          case System.cmd(cmd, args, stderr_to_stdout: true) do
            {out, 0} -> {:ok, String.trim(out)}
            {out, code} -> {:exit, code, String.trim(out)}
          end
        rescue
          e -> {:raised, e.__struct__}
        catch
          _kind, _reason -> {:raised, :caught}
        end
      end)

    case Task.yield(task, 2_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, outcome} -> outcome
      _ -> :timeout
    end
  end

  # kill -0 signal: :alive_signal | :esrch (the kernel positively says no such process) | :unknown (EPERM, usage, ...)
  defp signal_state(os_pid) do
    case os_cmd("kill", ["-0", os_pid]) do
      {:ok, _} -> :alive_signal
      {:exit, _code, out} -> if out =~ "No such process", do: :esrch, else: :unknown
      _ -> :unknown
    end
  end

  # ps identity: {:same, start} | :different | :empty | :unknown
  defp identity_state(os_pid, start) do
    case os_cmd("ps", ["-o", "lstart=", "-p", os_pid]) do
      {:ok, ""} -> :empty
      {:ok, ^start} -> :same
      {:ok, _other} -> :different
      _ -> :unknown
    end
  end

  # structured liveness: :alive (same pid AND start identity), :absent (positively established: ESRCH from the
  # kernel, or the pid now carries a different identity), :unknown (no identity, empty lookup, permission/usage
  # failure, timeout, raise). Command failure is NEVER absence.
  defp os_state(_os_pid, start) when not is_binary(start), do: :unknown

  defp os_state(os_pid, start) do
    case signal_state(os_pid) do
      :esrch ->
        :absent

      :unknown ->
        :unknown

      :alive_signal ->
        case identity_state(os_pid, start) do
          :same -> :alive
          :different -> :absent
          _ -> :unknown
        end
    end
  end

  # identity-bound cleanup with a bounded join; :unknown is retained, never claimed settled
  defp reap_os(os_pid, start) do
    case os_state(os_pid, start) do
      :absent ->
        :absent

      :alive ->
        _ = os_cmd("kill", ["-9", os_pid])
        joined = wait(fn -> os_state(os_pid, start) == :absent end)
        verdict_after_kill(joined)

      :unknown ->
        :unknown
    end
  end

  defp verdict_after_kill(true), do: :absent
  defp verdict_after_kill(false), do: :unknown

  # the oracle: true ONLY for explicit :not_started or :proven_absent; a missing record, :allocating, an
  # unidentified allocation or an unknown reap all stay false (fail-closed: the harness retains evidence)
  defp os_cleanup_oracle(key) do
    fn ->
      case :persistent_term.get(key, :missing) do
        :not_started ->
          true

        :proven_absent ->
          true

        # OBSERVED exit proof from an authority-owned Port (the child was exec'd into the Port's own process)
        {:exited, _os_pid, _status} ->
          :persistent_term.put(key, :proven_absent)
          true

        {:exited_unidentified, _status} ->
          :persistent_term.put(key, :proven_absent)
          true

        {:identified, os_pid, start} ->
          settle_identified(key, os_pid, start)

        _allocating_unidentified_or_missing ->
          false
      end
    end
  end

  defp settle_identified(key, os_pid, start) do
    case reap_os(os_pid, start) do
      :absent ->
        :persistent_term.put(key, :proven_absent)
        true

      :unknown ->
        false
    end
  end

  # registration BEFORE any allocation: the authority writes :not_started and the oracle enters the ordered harness
  defp register_os!(authority, tag) do
    key = {__MODULE__, :os_child, tag, make_ref()}
    :ok = GenServer.call(authority, {:register, key})
    OwnedHarness.os_oracle!(os_cleanup_oracle(key))
    key
  end

  defp authority! do
    {:ok, authority} = OsAuthority.start()
    track!(authority)
    authority
  end

  # TASK-OWNED Port (the original topology): the task marks :allocating with the authority BEFORE Port.open, opens
  # the child's Port itself, reports pid + start identity to the authority, tells the test, then blocks
  defp task_owned_child(test, tag, authority, key, seconds) do
    fn ->
      :ok = GenServer.call(authority, {:allocating, key})
      args = ["-c", "echo $$; exec sleep #{seconds}"]
      port = Port.open({:spawn_executable, "/bin/sh"}, [:binary, :stream, args: args])

      record =
        receive do
          {^port, {:data, line}} ->
            os_pid = String.trim(line)
            GenServer.call(authority, {:report, key, os_pid, start_identity(os_pid)})
        after
          5_000 -> GenServer.call(authority, {:report, key, :no_pid, nil})
        end

      send(test, {:external_child, tag, record, self()})
      send(test, {:adapter_started, tag, self()})

      receive do
        :never_sent -> :ok
      end
    end
  end

  defp start_identity(os_pid) do
    case os_cmd("ps", ["-o", "lstart=", "-p", os_pid]) do
      {:ok, start} when start != "" -> start
      _ -> nil
    end
  end

  # AUTHORITY-OWNED Port (a different topology, exit-proof capable): the adapter asks the authority to allocate,
  # reports, then blocks
  defp external_child(test, tag, authority, key, seconds, opts \\ []) do
    fn ->
      record = GenServer.call(authority, {:allocate, key, seconds, opts}, 15_000)
      send(test, {:external_child, tag, record, self()})
      send(test, {:adapter_started, tag, self()})

      receive do
        :never_sent -> :ok
      end
    end
  end

  # allocates through the authority, then fails RAW before reporting to anyone
  defp external_child_then_crash(authority, key, canary, seconds) do
    fn ->
      _record = GenServer.call(authority, {:allocate, key, seconds, []}, 15_000)
      raise canary
    end
  end

  # ---- harness ----------------------------------------------------------------------------------------------------

  setup do
    OwnedHarness.setup_owned()
    dir = Path.join(System.tmp_dir!(), "adapter-spike-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    track_dir!(dir)
    {:ok, dir: dir}
  end

  defp owner!(sentinel \\ :sentinel) do
    {:ok, owner} = SpikeOwner.start(sentinel: sentinel, gate_key: @gate_key)
    track!(owner)
    snap = GenServer.call(owner, :snapshot)
    track!(snap.sup)
    {owner, snap}
  end

  defp identity, do: %{cap: make_ref(), gen: 1, ref: make_ref()}

  defp start_op!(owner, id, adapter, arm \\ nil) do
    {:ok, pid, ref, seq} = GenServer.call(owner, {:start_op, id, adapter, arm})
    track!(pid)
    {pid, ref, seq}
  end

  defp started!(tag) do
    assert_receive {:adapter_started, ^tag, pid}, 5_000
    pid
  end

  defp snapshot(owner), do: GenServer.call(owner, :snapshot)

  # facts of the CURRENT operation only (seq-tagged)
  defp facts(owner, seq), do: for({^seq, fact} <- snapshot(owner).facts, do: fact)

  # wait_for/2 takes a RELATIVE timeout; wait/2 takes an ABSOLUTE monotonic deadline (never pass a duration to it)
  defp wait_for(fun, timeout_ms), do: wait(fun, System.monotonic_time(:millisecond) + timeout_ms)

  defp wait(fun), do: wait_for(fun, 5_000)

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

  defp wait_fact!(owner, seq, pred) do
    assert wait(fn -> Enum.any?(facts(owner, seq), pred) end), "fact not observed; facts: #{inspect(facts(owner, seq))}"
  end

  # waits until the CURRENT op is death-proven (independent monitor DOWN), so a next op may be admitted
  defp closed!(owner, seq), do: wait_fact!(owner, seq, &match?({:death_observed, _}, &1))

  defp mailbox(pid) do
    case Process.info(pid, :messages) do
      {:messages, messages} -> messages
      nil -> []
    end
  end

  defp now_unix, do: System.os_time(:second)

  # arming inputs captured from ONE unix_now read
  defp arm_in(ahead_s, cap) do
    now = now_unix()
    {now + ahead_s, now, cap}
  end

  # a fence that is due NOW in the owner's clock domain (same VM monotonic clock, deadline == unix_now)
  defp due_fence!(id) do
    now = now_unix()
    {:ok, fence} = DeadlineFence.arm(id, now, now, System.monotonic_time(:millisecond), 1_000)
    fence
  end

  defp death_of(owner, seq) do
    Enum.find_value(facts(owner, seq), fn
      {:death_observed, %{reason: reason}} -> reason
      _ -> nil
    end)
  end

  # ---- S-0 Task API semantics (AM-S2) --------------------------------------------------------------------------

  describe "H-0 harness wait wrappers are bounded" do
    test "wait_for/2 with a relative timeout and wait/2 with a past absolute deadline both return false promptly" do
      started = System.monotonic_time(:millisecond)
      refute wait_for(fn -> false end, 50)
      assert System.monotonic_time(:millisecond) - started < 1_000
      refute wait(fn -> false end, System.monotonic_time(:millisecond) - 1)
      assert System.monotonic_time(:millisecond) - started < 1_000
      assert wait_for(fn -> true end, 50)
    end
  end

  describe "S-0 baseline Task API semantics" do
    test "reply then DOWN :normal; brutal_kill on a blocked task returns nil and consumes its DOWN" do
      {:ok, sup} = Task.Supervisor.start_link()
      track!(sup)
      test = self()

      done = Task.Supervisor.async_nolink(sup, fn -> :finished end)
      assert_receive {ref, :finished} when ref == done.ref, 2_000
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 2_000

      blocked = Task.Supervisor.async_nolink(sup, blocking(test, :s0))
      _ = started!(:s0)
      death = Process.monitor(blocked.pid)
      assert Task.shutdown(blocked, :brutal_kill) == nil
      assert_receive {:DOWN, ^death, :process, _, :killed}, 2_000
      refute_receive {:DOWN, _, :process, _, _}, 200, "the Task monitor DOWN was consumed by shutdown"
    end

    test "brutal_kill returns {:ok, reply} for an arrived-but-undequeued reply and {:exit, reason} for a dead task" do
      {:ok, sup} = Task.Supervisor.start_link()
      track!(sup)
      test = self()

      replying = Task.Supervisor.async_nolink(sup, gated_reply(test, :s0b))
      adapter = started!(:s0b)
      send(adapter, :reply_now)
      assert wait(fn -> Enum.any?(mailbox(self()), &match?({_ref, {:ok, :delivered}}, &1)) end)
      assert Task.shutdown(replying, :brutal_kill) == {:ok, {:ok, :delivered}}
      refute_receive {_, {:ok, :delivered}}, 100, "the reply was consumed by shutdown"

      exiting = Task.Supervisor.async_nolink(sup, fn -> exit(:normal) end)
      assert wait(fn -> not Process.alive?(exiting.pid) end)
      assert match?({:exit, :normal}, Task.shutdown(exiting, :brutal_kill))
    end
  end

  # ---- S-1 ownership (AM-S1) ---------------------------------------------------------------------------------------

  describe "S-1 ownership across interruption" do
    test "nonempty runtime, owner Port and memo survive a real timer-armed interruption; I/O works before and after" do
      {owner, before} = owner!(:sentinel_one)
      assert %{label: :fake_handle_ownership_witness, port: port} = before.runtime_entry
      assert Port.info(port, :connected) == {:connected, owner}
      assert before.sentinel == :sentinel_one
      assert GenServer.call(owner, :port_roundtrip) == :ok

      id = identity()
      {task, _ref, seq} = start_op!(owner, id, blocking(self(), :s1), arm_in(1, 200))
      assert started!(:s1) == task
      wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
      closed!(owner, seq)
      assert death_of(owner, seq) == :killed

      after_snap = snapshot(owner)
      assert after_snap.state == :timeout_selected and after_snap.kills == 1 and after_snap.closed
      assert Enum.any?(facts(owner, seq), &match?({:early, _}, &1)), "real timer chunks before due"
      assert {:shutdown_return, nil} in facts(owner, seq)
      assert after_snap.runtime_entry == before.runtime_entry
      assert after_snap.sentinel == :sentinel_one
      assert Port.info(port, :connected) == {:connected, owner}
      assert GenServer.call(owner, :port_roundtrip) == :ok
      assert after_snap.sup_children == []
      refute Process.alive?(task)
    end
  end

  describe "S-2 dequeue order decides" do
    test "(a) result first: ordinary completion, no kill, DOWN :normal, later fence stale, duplicate result stale" do
      {owner, _} = owner!()
      id = identity()
      {_task, ref, seq} = start_op!(owner, id, gated_reply(self(), :s2a))
      adapter = started!(:s2a)
      send(adapter, :reply_now)
      wait_fact!(owner, seq, &match?({:decision, :retain_result, :reply}, &1))
      closed!(owner, seq)
      assert death_of(owner, seq) == :normal
      :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
      wait_fact!(owner, seq, &match?({:ignored_fence, :closed_op, :hand}, &1))

      # ---- S-2 ordering (AM-S3)

      # a duplicate result on the CURRENT task ref: recorded and classified stale, state unchanged
      send(owner, {ref, {:ok, :delivered_again}})
      wait_fact!(owner, seq, &match?({:stale_result, :duplicate_completion, :reply}, &1))
      snap = snapshot(owner)
      assert snap.state == :completed and snap.kills == 0 and snap.delivery == nil
    end

    test "(b) due fence first, genuinely no result: timeout selected, delivery unknown, never a late completion" do
      {owner, _} = owner!()
      id = identity()
      {_task, _ref, seq} = start_op!(owner, id, blocking(self(), :s2b))
      _ = started!(:s2b)
      :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
      wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
      closed!(owner, seq)
      assert death_of(owner, seq) == :killed
      snap = snapshot(owner)
      assert snap.state == :timeout_selected and snap.kills == 1
      refute Enum.any?(facts(owner, seq), &match?({:reply, _}, &1))
      refute Enum.any?(facts(owner, seq), &match?({:decision, :retain_late_result, _}, &1))
    end

    test "(c) due fence first, real raced reply queued behind it: shutdown returns the reply, late result retained" do
      {owner, _} = owner!()
      id = identity()
      {_task, ref, seq} = start_op!(owner, id, gated_reply(self(), :s2c))
      adapter = started!(:s2c)
      :ok = GenServer.call(owner, {:set_fence, id, due_fence!(id)})
      # freeze the owner, queue the fence wake FIRST, then let the adapter reply so the reply queues behind it
      :ok = :sys.suspend(owner)
      send(owner, {:deadline_fence, id, :hand})
      send(adapter, :reply_now)
      # the runner wraps the adapter's value: the task reply is {:ok, {:ok, :delivered}}
      assert wait(fn -> Enum.any?(mailbox(owner), &match?({^ref, {:ok, {:ok, :delivered}}}, &1)) end)
      [first | _] = Enum.filter(mailbox(owner), &(match?({:deadline_fence, _, _}, &1) or match?({^ref, _}, &1)))
      assert match?({:deadline_fence, _, :hand}, first), "the fence is ahead of the reply in the mailbox"
      :ok = :sys.resume(owner)
      wait_fact!(owner, seq, &match?({:decision, :retain_late_result, :shutdown}, &1))
      closed!(owner, seq)
      snap = snapshot(owner)
      assert snap.state == :completed_after_timeout and snap.kills == 1
      assert {:shutdown_return, :ok_reply} in facts(owner, seq)
      assert Enum.any?(facts(owner, seq), &match?({:kill_requested, ^ref}, &1))
      assert death_of(owner, seq) == :normal, "the task had replied and exited normally"
    end

    test "(d) early wakes observed AT handling: kills 0 and :running captured then; delayed observer sees the same" do
      {owner, _} = owner!()
      :ok = GenServer.call(owner, {:observe_early, self()})
      id = identity()
      {_task, _ref, seq} = start_op!(owner, id, blocking(self(), :s2d), arm_in(1, 100))
      _ = started!(:s2d)
      # the observation is captured by the owner AT early-event handling, not by a later snapshot
      assert_receive {:early_observed, ^seq, :running, 0, _ms}, 5_000
      assert_receive {:early_observed, ^seq, :running, 0, _ms}, 5_000
      wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
      closed!(owner, seq)
      assert snapshot(owner).kills == 1
      # deliberately delayed observer: read everything only after the due wake killed; every early observation
      # still carries the state and kill count captured at its own handling time
      Process.sleep(300)
      earlies = for {:early_observed, ^seq, state, kills, _} <- mailbox(self()), do: {state, kills}
      assert earlies != [] and Enum.all?(earlies, &(&1 == {:running, 0}))
      # (e) duplicate on the closed op and a foreign fence: no second kill
      :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
      wait_fact!(owner, seq, &match?({:ignored_fence, :closed_op, :hand}, &1))
      foreign_id = %{id | ref: make_ref()}
      :ok = GenServer.call(owner, {:hand_fence, foreign_id, due_fence!(foreign_id)})
      wait_fact!(owner, seq, &match?({:ignored_fence, :foreign_or_absent, :hand}, &1))
      assert snapshot(owner).kills == 1
    end

    test "(e') duplicate fence while the op is still open after selection is stale, no second kill" do
      {owner, _} = owner!()
      id = identity()
      {_task, _ref, seq} = start_op!(owner, id, gated_reply(self(), :s2e))
      adapter = started!(:s2e)
      :ok = GenServer.call(owner, {:set_fence, id, due_fence!(id)})
      :ok = :sys.suspend(owner)
      send(owner, {:deadline_fence, id, :hand})
      send(owner, {:deadline_fence, id, :hand})
      send(adapter, :reply_now)
      :ok = :sys.resume(owner)
      wait_fact!(owner, seq, &match?({:stale_fence, :duplicate_timeout}, &1))
      closed!(owner, seq)
      assert snapshot(owner).kills == 1
    end
  end

  describe "S-2x one in flight and old facts" do
    test "a second start is refused while the first is outstanding; admitted after death proof; old facts stay old" do
      {owner, _} = owner!()
      id1 = identity()
      {task1, ref1, seq1} = start_op!(owner, id1, gated_reply(self(), :x1))
      adapter1 = started!(:x1)

      assert GenServer.call(owner, {:start_op, identity(), blocking(self(), :x_refused), nil}) ==
               {:error, :operation_outstanding}

      assert length(snapshot(owner).sup_children) == 1
      refute_received {:adapter_started, :x_refused, _}

      death1 =
        Enum.find_value(facts(owner, seq1), fn
          {:op_started, %{death_ref: d}} -> d
          _ -> nil
        end)

      send(adapter1, :reply_now)
      wait_fact!(owner, seq1, &match?({:decision, :retain_result, :reply}, &1))
      closed!(owner, seq1)
      refute Process.alive?(task1)

      id2 = identity()
      {_task2, _ref2, seq2} = start_op!(owner, id2, blocking(self(), :x2))
      _ = started!(:x2)
      assert seq2 == seq1 + 1
      # INJECTED REPLAYS after the second admitted op (real retired refs, synthetic messages): old reply, old DOWN,
      # old fence; the current op is unchanged (ref rejection, not genuine delayed OTP arrivals)
      send(owner, {ref1, {:ok, :late_from_first}})
      send(owner, {:DOWN, death1, :process, task1, :normal})
      :ok = GenServer.call(owner, {:hand_fence, id1, due_fence!(id1)})
      wait_fact!(owner, seq2, &(&1 == {:old_task_fact, :reply, seq1}))
      wait_fact!(owner, seq2, &(&1 == {:old_task_fact, :down, seq1}))
      wait_fact!(owner, seq2, &match?({:ignored_fence, :foreign_or_absent, :hand}, &1))
      snap = snapshot(owner)
      assert snap.state == :running and snap.kills == 0 and snap.closed == false
      refute Enum.any?(facts(owner, seq2), &match?({:reply, _}, &1))
    end

    test "op_started and death facts carry the native Task.ref, task pid and death ref (linked to the logical identity by seq)" do
      # ---- S-2x admission and current-operation correlation (AM-M2) ------------------------------------------------
      {owner, _} = owner!()
      {task, ref, seq} = start_op!(owner, identity(), counted_normal(self(), :refs))
      assert_receive {:invoked, :refs}, 5_000
      closed!(owner, seq)
      assert Enum.any?(facts(owner, seq), &match?({:op_started, %{task_ref: ^ref, pid: ^task, death_ref: _}}, &1))

      assert Enum.any?(
               facts(owner, seq),
               &match?({:death_observed, %{task_ref: ^ref, pid: ^task, death_ref: _, reason: :normal}}, &1)
             )
    end
  end

  describe "S-3 lifetime and failure boundaries" do
    test "orderly owner stop while the task is blocked: owner, task supervisor, task and Port all close" do
      {owner, snap} = owner!()
      {task, _ref, _seq} = start_op!(owner, identity(), blocking(self(), :s3a))
      _ = started!(:s3a)
      monitors = for pid <- [owner, snap.sup, task], do: {pid, Process.monitor(pid)}
      :ok = GenServer.stop(owner, :shutdown, 5_000)
      for {pid, mon} <- monitors, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, 5_000)
      assert Port.info(snap.port) == nil
    end

    test "hard owner kill while the task is blocked: everything linked goes; task-supervisor death takes the owner" do
      {owner, snap} = owner!()
      {task, _ref, _seq} = start_op!(owner, identity(), blocking(self(), :s3b))
      _ = started!(:s3b)
      monitors = for pid <- [owner, snap.sup, task], do: {pid, Process.monitor(pid)}
      Process.exit(owner, :kill)
      for {pid, mon} <- monitors, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, 5_000)
      assert Port.info(snap.port) == nil

      {owner2, snap2} = owner!()
      {task2, _, _} = start_op!(owner2, identity(), blocking(self(), :s3c))
      _ = started!(:s3c)
      monitors2 = for pid <- [owner2, snap2.sup, task2], do: {pid, Process.monitor(pid)}
      Process.exit(snap2.sup, :kill)
      for {pid, mon} <- monitors2, do: assert_receive({:DOWN, ^mon, :process, ^pid, _}, 5_000)
    end

    test "RAW adapter raise/throw/exit are closed by the runner: closed reply, death :normal, no canary anywhere" do
      {owner, _} = owner!()

      log =
        capture_log(fn ->
          for kind <- [:error, :throw, :exit] do
            {task, _ref, seq} = start_op!(owner, identity(), raw_failing(self(), {:raw, kind}, kind, @canary))
            adapter = started!({:raw, kind})
            assert adapter == task
            death = Process.monitor(task)
            send(adapter, :fail_now)
            wait_fact!(owner, seq, &match?({:reply, {:closed_failure, ^kind, %{"digest" => _}}}, &1))
            assert_receive {:DOWN, ^death, :process, ^task, :normal}, 5_000
            closed!(owner, seq)
            assert death_of(owner, seq) == :normal
          end

          # pre-readiness failure: the runner closes it before any ack
          {_task, _ref, seq} = start_op!(owner, identity(), raw_pre_ready_failure(@canary))
          wait_fact!(owner, seq, &match?({:reply, {:closed_failure, :error, %{"digest" => _}}}, &1))
          closed!(owner, seq)
          refute_received {:adapter_started, _, _}
        end)

      snap = snapshot(owner)
      refute inspect(snap.facts, limit: :infinity, printable_limit: :infinity) =~ @canary
      refute inspect(:sys.get_status(owner), limit: :infinity, printable_limit: :infinity) =~ @canary
      refute log =~ @canary
    end

    # imported from Codex's review probe (/tmp/adapter_spike_review_test.exs), meaning preserved
    test "review probe: the runner closes a raw adapter throw before Task crash logging" do
      canary = "SPIKE-REVIEW-RAW-THROW-CANARY"
      {owner, _} = owner!()

      log =
        capture_log(fn ->
          parent = self()

          adapter = fn ->
            send(parent, {:entered, self()})

            receive do
              :go -> throw(canary)
            end
          end

          {task, _ref, seq} = start_op!(owner, identity(), adapter)
          assert_receive {:entered, ^task}, 5_000
          monitor = Process.monitor(task)
          send(task, :go)
          assert_receive {:DOWN, ^monitor, :process, ^task, :normal}, 5_000
          closed!(owner, seq)
        end)

      refute log =~ canary
    end

    # ---- S-3 lifetime and failure boundaries (AM-S4, AM-M1)

    # imported from Codex's review probe: one in-flight operation cannot orphan a running predecessor
    test "review probe: a second start cannot orphan a still-running predecessor" do
      {owner, _} = owner!()
      parent = self()

      adapter = fn ->
        send(parent, {:entered, self()})

        receive do
          :finish -> :done
        end
      end

      {first, _ref, _seq} = start_op!(owner, identity(), adapter)
      assert_receive {:entered, ^first}, 5_000
      second_reply = GenServer.call(owner, {:start_op, identity(), adapter, nil})
      snap = snapshot(owner)

      assert length(snap.sup_children) == 1,
             "second start #{inspect(second_reply)} left #{length(snap.sup_children)} tasks"

      assert second_reply == {:error, :operation_outstanding}
      send(first, :finish)
    end
  end

  describe "S-4 external descendant across task interruption" do
    test "liveness controls: positive live, proven dead, ESRCH absence, unknown without identity" do
      authority = authority!()
      key = register_os!(authority, :oracle_control)
      assert GenServer.call(authority, {:record, key}) == :not_started
      assert {:identified, os_pid, start} = GenServer.call(authority, {:allocate, key, 30, []})
      assert os_state(os_pid, start) == :alive
      assert os_state("999999999", start) == :absent, "ESRCH from the kernel is positively established absence"
      assert os_state(os_pid, nil) == :unknown, "no identity is never a verdict"
      assert reap_os(os_pid, nil) == :unknown, "cleanup refuses without identity"
      assert reap_os(os_pid, start) == :absent
      assert os_state(os_pid, start) == :absent
      assert os_cleanup_oracle(key).() == true
      assert GenServer.call(authority, {:record, key}) == :proven_absent
    end

    test "command failures while a REAL child is alive are :unknown, never absence (permission, usage, timeout, raise)" do
      authority = authority!()
      key = register_os!(authority, :cmd_failures)
      assert {:identified, os_pid, start} = GenServer.call(authority, {:allocate, key, 30, []})
      assert os_state(os_pid, start) == :alive

      failures = [
        fn _cmd, _args -> {:exit, 1, "kill: #{os_pid}: Operation not permitted"} end,
        fn _cmd, _args -> {:exit, 64, "usage: kill ..."} end,
        fn _cmd, _args -> :timeout end,
        fn _cmd, _args -> {:raised, ArgumentError} end,
        fn cmd, _args -> if cmd == "kill", do: {:ok, ""}, else: {:exit, 1, "ps: permission denied"} end,
        fn cmd, _args -> if cmd == "kill", do: {:ok, ""}, else: {:ok, ""} end
      ]

      for runner <- failures do
        Process.put({__MODULE__, :cmd_runner}, runner)
        assert os_state(os_pid, start) == :unknown, "runner #{inspect(runner)} must not establish absence"
        assert reap_os(os_pid, start) == :unknown
        assert os_cleanup_oracle(key).() == false, "the oracle stays false on unknown; evidence retained"
        Process.delete({__MODULE__, :cmd_runner})
      end

      # the imported decision probe: two command failures do not establish OS absence
      Process.put({__MODULE__, :cmd_runner}, fn _cmd, _args -> {:exit, 1, "nonzero"} end)
      assert os_state(os_pid, start) == :unknown
      Process.delete({__MODULE__, :cmd_runner})

      assert os_state(os_pid, start) == :alive, "the real child was untouched by the injected failures"
      assert reap_os(os_pid, start) == :absent
      assert os_cleanup_oracle(key).() == true
    end

    test "record lifecycle: missing/allocating/unidentified are fail-closed; absence only by reap or observed exit" do
      authority = authority!()
      # imported decision probe: an unreported allocation (missing record) cannot default to successful cleanup
      refute os_cleanup_oracle({__MODULE__, :never_registered, make_ref()}).()
      key = register_os!(authority, :lifecycle)
      assert os_cleanup_oracle(key).() == true, ":not_started is the only pre-allocation state that settles"

      # requester-kill seam with a TWO-WAY acknowledged barrier: the authority reports the :allocating record and
      # waits (no timeout) for the test's proceed token; the requester is tracked and joined
      parent = self()
      token = make_ref()

      seam = fn record ->
        send(parent, {:at_before_identity, token, record})

        receive do
          {:proceed, ^token} -> :ok
        end
      end

      requester = spawn(fn -> GenServer.call(authority, {:allocate, key, 20, [before_identity: seam]}, 15_000) end)
      track!(requester)
      requester_mon = Process.monitor(requester)
      assert_receive {:at_before_identity, ^token, :allocating}, 5_000
      Process.exit(requester, :kill)
      assert_receive {:DOWN, ^requester_mon, :process, ^requester, :killed}, 5_000
      send(authority, {:proceed, token})
      assert wait(fn -> match?({:identified, _, _}, GenServer.call(authority, {:record, key})) end)
      assert {:identified, os_pid, start} = GenServer.call(authority, {:record, key})
      assert os_state(os_pid, start) == :alive
      assert os_cleanup_oracle(key).() == true
      assert os_state(os_pid, start) == :absent

      # late identity failure on an authority-owned Port: unidentified, the oracle REFUSES while the child lives;
      # absence is admitted only from the authority's OBSERVED exit proof (the bounded helper exits on its own)
      key2 = register_os!(authority, :unidentified)

      assert {:allocated_unidentified, :identity_capture_failed} =
               GenServer.call(authority, {:allocate, key2, 3, [identity_failure: true]}, 15_000)

      refute os_cleanup_oracle(key2).(), "an unidentified allocation is never claimed settled while unproven"
      assert wait_for(fn -> match?({:exited_unidentified, _}, GenServer.call(authority, {:record, key2})) end, 10_000)
      assert {:exited_unidentified, status} = GenServer.call(authority, {:record, key2})
      assert is_integer(status)
      assert os_cleanup_oracle(key2).() == true, "observed exit proof admits absence"
    end

    test "TASK-OWNED Port (original topology): the descendant's survival after task kill is MEASURED; cleanup by identity" do
      authority = authority!()
      key = register_os!(authority, :s4_task_owned)
      {owner, _} = owner!()
      id = identity()
      {task, _ref, seq} = start_op!(owner, id, task_owned_child(self(), :s4t, authority, key, 300))
      assert_receive {:external_child, :s4t, {:identified, os_pid, start}, ^task}, 20_000
      assert GenServer.call(authority, {:record, key}) == {:identified, os_pid, start}
      _ = started!(:s4t)
      assert os_state(os_pid, start) == :alive
      :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
      wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
      closed!(owner, seq)
      assert death_of(owner, seq) == :killed
      refute Process.alive?(task)
      Process.sleep(200)
      measured = os_state(os_pid, start)
      IO.puts("\n[S-4 measured, task-owned Port] external descendant after task brutal_kill: #{measured}")
      assert measured in [:alive, :absent], "a verdict, never unknown; recorded as measured"
      assert reap_os(os_pid, start) == :absent
      assert os_cleanup_oracle(key).() == true
    end

    test "AUTHORITY-OWNED Port (exit-proof topology): survival after task kill MEASURED; cleanup by identity" do
      authority = authority!()
      key = register_os!(authority, :s4)
      {owner, _} = owner!()
      id = identity()
      {_task, _ref, seq} = start_op!(owner, id, external_child(self(), :s4, authority, key, 300))
      assert_receive {:external_child, :s4, {:identified, os_pid, start}, _adapter}, 20_000
      assert GenServer.call(authority, {:record, key}) == {:identified, os_pid, start}
      _ = started!(:s4)
      assert os_state(os_pid, start) == :alive
      :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
      wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
      closed!(owner, seq)
      assert death_of(owner, seq) == :killed
      Process.sleep(200)
      measured = os_state(os_pid, start)
      IO.puts("\n[S-4 measured, authority-owned Port] external descendant after task brutal_kill: #{measured}")
      assert measured in [:alive, :absent], "a verdict, never unknown; recorded as measured"
      assert reap_os(os_pid, start) == :absent
      assert os_cleanup_oracle(key).() == true
    end

    test "failure before report: an adapter that allocates then fails raw leaves a recorded child the oracle settles" do
      authority = authority!()
      key = register_os!(authority, :s4_crash)
      {owner, _} = owner!()

      log =
        capture_log(fn ->
          {_task, _ref, seq} = start_op!(owner, identity(), external_child_then_crash(authority, key, @canary, 30))
          wait_fact!(owner, seq, &match?({:reply, {:closed_failure, :error, %{"digest" => _}}}, &1))
          closed!(owner, seq)
        end)

      refute log =~ @canary
      assert {:identified, os_pid, start} = GenServer.call(authority, {:record, key})
      assert os_state(os_pid, start) in [:alive, :absent]
      assert os_cleanup_oracle(key).() == true
      assert os_state(os_pid, start) == :absent
    end
  end

  # ---- S-5 uncertainty (AM-S5)

  describe "S-5 delivery stays unknown, exactly one invocation" do
    test "marker-before-block and block-before-marker end unknown with one invocation; positive counter control",
         %{dir: dir} do
      {owner, _} = owner!()

      for mode <- [:marker_before_block, :block_before_marker] do
        path = Path.join(dir, "#{mode}.marker")
        id = identity()
        {_task, _ref, seq} = start_op!(owner, id, marker_adapter(self(), mode, path, mode))
        assert_receive {:invoked, ^mode}, 5_000
        _ = started!(mode)
        :ok = GenServer.call(owner, {:hand_fence, id, due_fence!(id)})
        wait_fact!(owner, seq, &match?({:delivery, :unknown}, &1))
        closed!(owner, seq)
        assert death_of(owner, seq) == :killed
        refute_receive {:invoked, ^mode}, 300, "no resend / second invocation"
        assert File.exists?(path) == (mode == :marker_before_block), "the side effect may or may not have happened"
        refute Enum.any?(facts(owner, seq), &match?({:reply, _}, &1))
      end

      {_t, _, seq1} = start_op!(owner, identity(), counted_normal(self(), :count1))
      assert_receive {:invoked, :count1}, 5_000
      closed!(owner, seq1)
      {_t2, _, seq2} = start_op!(owner, identity(), counted_normal(self(), :count2))
      assert_receive {:invoked, :count2}, 5_000
      closed!(owner, seq2)
      assert seq2 == seq1 + 1
    end
  end

  # ---- S-6 the M-1 distinction (AM-S6)

  describe "S-6 dequeuing a settle-shaped message is not interruption" do
    test "the owner dequeues settle while its TASK stays blocked; the adapter is untouched" do
      {owner, _} = owner!()
      {task, _ref, seq} = start_op!(owner, identity(), blocking(self(), :s6))
      adapter = started!(:s6)
      send(owner, {:settle, make_ref(), 1, make_ref()})
      wait_fact!(owner, seq, &(&1 == :settle_shaped_message_ignored))
      assert Process.alive?(adapter) and adapter == task
      assert snapshot(owner).kills == 0 and snapshot(owner).state == :running
      # the real Worker blocked in Effects.execute cannot dequeue at all: cited U2a-0 control C-1
    end
  end
end
