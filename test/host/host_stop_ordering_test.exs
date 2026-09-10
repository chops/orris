defmodule AiOrchestrator.Host.StopOrderingTest do
  @moduledoc """
  NS-04.C.901 / REG-HOST-STOP-01: actual Host.stop with controlled protocol owners.
  These rows prove caller ordering and arbitration, not RunOwner's real teardown.
  An owner DOWN does not upgrade incomplete descendant teardown evidence. G3 in
  core_startup_bound_green_test.exs remains separate real integration evidence.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.Supervisor, as: HostSupervisor

  @stopped {:ok, :stopped}
  @incomplete {:error, %{clause: "run_executor_teardown_incomplete", survivors: 3}}
  @unproven {:error, %{clause: "run_host_stop_unproven"}}
  @timed_out {:error, %{clause: "run_host_stop_timeout"}}
  @owner_down {:error, %{clause: "run_host_owner_down"}}

  for branch <- [:agent_alive, :agent_dead], outcome <- [@stopped, @incomplete] do
    @branch branch
    @outcome outcome
    test "HS1-HS3 #{@branch} joins owner before returning #{inspect(@outcome)}" do
      fixture([branch: @branch], fn f ->
        emit(f, @outcome)
        refute_return(f)
        assert Process.alive?(f.owner)
        release(f)
        assert returned(f) == @outcome
        refute Process.alive?(f.owner)
        assert Host.stop(f.handle, 100) == @owner_down
      end)
    end

    test "HS4 #{@branch} killed owner overrides #{inspect(@outcome)}" do
      fixture([branch: @branch], fn f ->
        emit(f, @outcome)
        refute_return(f)
        Process.exit(f.owner, :kill)
        assert returned(f) == @unproven
      end)
    end
  end

  for branch <- [:agent_alive, :agent_dead] do
    @branch branch
    test "HS5 #{@branch} retained returns immediately and preserves owner" do
      fixture([branch: @branch, ack: :retained], fn f ->
        emit(f, :retained)
        assert returned(f) == :ok
        assert Process.alive?(f.owner)
      end)
    end

    test "HS7 #{@branch} delayed outcome does not renew original deadline" do
      fixture([branch: @branch, timeout: 1_000], fn f ->
        wait_until(fn -> now() - f.started >= 700 end)
        emitted = emit(f, @stopped)
        {answer, finished} = result(f)
        assert answer == @timed_out
        assert emitted - f.started >= 700
        assert finished - f.started >= 950
        assert finished - f.started < 1_450
        assert finished - emitted < 650
        assert Process.alive?(f.owner)
      end)
    end

    test "HS9 #{@branch} infinity still joins owner" do
      fixture([branch: @branch, timeout: :infinity], fn f ->
        emit(f, @stopped)
        refute_return(f)
        release(f)
        assert returned(f) == @stopped
        refute Process.alive?(f.owner)
      end)
    end
  end

  test "HS6 silent owner's real arbitration remains unproven with live owner" do
    fixture([ack: :silent, shutdown: 160, timeout: 1_000], fn f ->
      assert returned(f) == @unproven
      assert Process.alive?(f.owner)
    end)
  end

  test "HS8 caller timeout leaves detached agent's acknowledged obligation running" do
    fixture([timeout: 80], fn f ->
      assert returned(f) == @timed_out
      assert Process.alive?(f.owner)
      assert Process.alive?(f.agent)
      agent_mon = Process.monitor(f.agent)
      release(f)
      assert_receive {:DOWN, ^agent_mon, :process, agent, :normal}, 1_000
      assert agent == f.agent
    end)
  end

  for reason <- [:normal, :killed], outcome <- [:none, @stopped, @incomplete, :retained] do
    @reason reason
    @outcome outcome
    test "HS10-HS11 owner DOWN first #{@reason}, queued #{inspect(@outcome)}" do
      fixture([], fn f ->
        :erlang.suspend_process(f.caller)
        owner_mon = Process.monitor(f.owner)
        if @reason == :killed, do: Process.exit(f.owner, :kill), else: send(f.owner, :release)
        assert_receive {:DOWN, ^owner_mon, :process, owner, @reason}, 1_000
        assert owner == f.owner
        wait_until(fn -> queued_owner_down?(f) end)
        if @outcome != :none, do: send(f.caller, {:stop_outcome, f.ref, @outcome})
        :erlang.resume_process(f.caller)

        expected =
          case {@reason, @outcome} do
            {:killed, _} -> @unproven
            {_, :none} -> @owner_down
            {_, :retained} -> :ok
            {_, outcome} -> outcome
          end

        assert returned(f) == expected
      end)
    end
  end

  test "HS12 unrelated real DOWN and wrong reference cannot satisfy owner join" do
    fixture([], fn f ->
      # The caller makes this unrelated monitor itself before it enters Host.stop.
      emit(f, @stopped)
      send(f.caller, {:stop_outcome, make_ref(), @stopped})
      send(f.noise, :release)
      refute_return(f)
      release(f)
      assert returned(f) == @stopped
      {:messages, messages} = Process.info(f.caller, :messages)
      assert Enum.any?(messages, &match?({:DOWN, _, :process, pid, :normal} when pid == f.noise, &1))
      assert Enum.any?(messages, &match?({:stop_outcome, ref, @stopped} when ref != f.ref, &1))
    end)
  end

  test "HS13 stop agent death after active outcome cannot release owner join" do
    fixture([], fn f ->
      emit(f, @stopped)
      wait_until(fn -> waiting_in?(f.caller, :stop_join, 4) end)
      kill_agent(f)
      refute_return(f)
      release(f)
      assert returned(f) == @stopped
    end)
  end

  test "fixture joins all private processes even when assertion fails before outcome" do
    proof = make_ref()

    assert_raise ExUnit.AssertionError, fn ->
      fixture([cleanup_proof: proof], fn _f -> flunk("controlled fixture assertion failure") end)
    end

    assert_receive {:cleanup_proof, ^proof, pids}
    assert length(pids) == 5
    refute Enum.any?(pids, &Process.alive?/1)
  end

  defp fixture(opts, fun) do
    observer = self()
    tracking = make_ref()
    {:ok, supervisor} = HostSupervisor.start_link(name: nil, child_shutdown_ms: Keyword.get(opts, :shutdown, 5_000))
    owner = spawn(fn -> owner_loop(observer, Keyword.get(opts, :ack, :stopping)) end)
    noise = spawn(fn -> receive do: (:release -> :ok) end)
    handle = %{owner: owner, supervisor: supervisor}
    timeout = Keyword.get(opts, :timeout, 2_000)

    caller =
      spawn(fn ->
        Process.monitor(noise)
        send(observer, {:call_started, self(), now()})
        answer = Host.stop(handle, timeout)
        send(observer, {:returned, self(), answer, now()})
        receive do: (:finish -> :ok)
      end)

    # This after covers even failure to receive the request identity. The owner's
    # dictionary retains the actual detached agent before any acknowledgment/fault.
    try do
      assert_receive {:call_started, ^caller, started}, 1_000
      assert_receive {:request, ^owner, agent, ^caller, ref}, 1_000
      Process.put(tracking, agent)
      f = %{owner: owner, caller: caller, agent: agent, ref: ref, started: started, handle: handle, noise: noise}

      if Keyword.get(opts, :branch) == :agent_dead do
        kill_agent(f)
        wait_until(fn -> waiting_in?(caller, :stop_wait_owner_only, 4) end)
      end

      fun.(f)
    after
      agent = Process.delete(tracking) || owner_agent(owner) || caller_agent(caller, owner, noise)
      pids = [owner, caller, agent, noise, supervisor] |> Enum.filter(&is_pid/1) |> Enum.uniq()
      cleanup(pids, owner, caller, noise, supervisor)
      if proof = opts[:cleanup_proof], do: send(observer, {:cleanup_proof, proof, pids})
    end
  end

  defp owner_loop(observer, ack) do
    receive do
      {:"$gen_call", from, {:stop_request, {caller, ref, _timeout}}} ->
        agent = elem(from, 0)
        Process.put(:stop_agent, agent)
        send(observer, {:request, self(), agent, caller, ref})
        if ack != :silent, do: :gen_statem.reply(from, {:ack, ack})
        owner_hold(observer, caller, ref)

      :release ->
        :ok
    end
  end

  defp owner_hold(observer, caller, ref) do
    receive do
      {:emit, outcome} ->
        emitted = now()
        send(caller, {:stop_outcome, ref, outcome})
        send(observer, {:emitted, self(), emitted})
        owner_hold(observer, caller, ref)

      :release ->
        :ok
    end
  end

  defp emit(f, outcome) do
    send(f.owner, {:emit, outcome})
    owner = f.owner
    assert_receive {:emitted, ^owner, emitted}, 1_000
    emitted
  end

  defp release(f), do: send(f.owner, :release)

  defp refute_return(f) do
    caller = f.caller
    refute_receive {:returned, ^caller, _, _}, 40
  end

  defp result(f) do
    caller = f.caller
    assert_receive {:returned, ^caller, answer, finished}, 2_000
    {answer, finished}
  end

  defp returned(f), do: elem(result(f), 0)

  defp kill_agent(f) do
    mon = Process.monitor(f.agent)
    Process.exit(f.agent, :kill)
    assert_receive {:DOWN, ^mon, :process, agent, reason}, 1_000
    assert agent == f.agent
    assert reason in [:killed, :noproc, :normal]
  end

  defp queued_owner_down?(f) do
    case Process.info(f.caller, :messages) do
      {:messages, messages} -> Enum.any?(messages, &match?({:DOWN, _, :process, pid, _} when pid == f.owner, &1))
      nil -> false
    end
  end

  defp waiting_in?(pid, function, arity) do
    Process.info(pid, :current_function) == {:current_function, {Host, function, arity}} and
      Process.info(pid, :status) == {:status, :waiting}
  end

  defp wait_until(predicate), do: wait_until(predicate, now() + 1_500)

  defp wait_until(predicate, deadline) do
    if predicate.() do
      :ok
    else
      assert now() < deadline, "fixture interleaving was not reached"
      Process.sleep(2)
      wait_until(predicate, deadline)
    end
  end

  defp owner_agent(owner) do
    case Process.info(owner, :dictionary) do
      {:dictionary, dictionary} -> Keyword.get(dictionary, :stop_agent)
      nil -> nil
    end
  end

  defp caller_agent(caller, owner, noise) do
    case Process.info(caller, :monitors) do
      {:monitors, monitors} ->
        Enum.find_value(monitors, fn
          {:process, pid} when pid != owner and pid != noise and is_pid(pid) -> pid
          _ -> nil
        end)

      nil ->
        nil
    end
  end

  defp cleanup(pids, owner, caller, noise, supervisor) do
    monitors = Enum.map(pids, &{&1, Process.monitor(&1)})
    resume(caller)
    send(owner, :release)
    send(caller, :finish)
    send(noise, :release)
    if Process.alive?(supervisor), do: Supervisor.stop(supervisor)

    Enum.each(monitors, fn {pid, mon} ->
      receive do
        {:DOWN, ^mon, :process, ^pid, _} -> :ok
      after
        1_000 ->
          Process.exit(pid, :kill)
          assert_receive {:DOWN, ^mon, :process, ^pid, _}, 1_000
      end

      refute Process.alive?(pid), "private fixture survivor #{inspect(pid)}"
    end)
  end

  defp resume(pid) do
    :erlang.resume_process(pid)
  catch
    :error, :badarg -> :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
end
