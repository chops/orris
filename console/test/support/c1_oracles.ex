defmodule C1.Oracles do
  @moduledoc """
  TEST SUPPORT ONLY: the oracles shared by the harness controls (run against C1.Doubles) and the RED rows (run
  against the product modules). Each raises an ExUnit assertion whose message starts with `step:<name>` so the
  controls can prove which step a mutant fails at (`outcome/1`). `outcome/1` also kills every process the oracle
  tracked (`track/1`), on pass and on fail, and proves them gone.
  """
  import ExUnit.Assertions

  @doc "Runs an oracle closure; :ok or {:failed, step}; tracked processes are killed and proven dead afterwards."
  def outcome(fun) do
    Process.put(:c1_tracked, [])

    try do
      fun.()
      :ok
    rescue
      e in ExUnit.AssertionError ->
        case Regex.run(~r/step:([a-z_]+)/, e.message) do
          [_, step] -> {:failed, String.to_atom(step)}
          nil -> {:failed, :unnamed}
        end
    after
      for pid <- Process.get(:c1_tracked, []), Process.alive?(pid) do
        Process.exit(pid, :kill)
        down(pid, :tracked_process_survived)
      end

      Process.delete(:c1_tracked)
    end
  end

  @doc "Registers a test-owned process for cleanup by outcome/1."
  def track(pid) when is_pid(pid) do
    Process.put(:c1_tracked, [pid | Process.get(:c1_tracked, [])])
    pid
  end

  defp step!(cond, step, detail \\ ""), do: assert(cond, "step:#{step} #{detail}")

  # ---- credential permission-first (C1-02e) ----
  @doc """
  `setup.(path)` runs in a SEPARATE traced process with this process as tracer (a tracer never sees its own calls);
  a trace-delivered barrier precedes the reading; tracing is reset on every path. Required, bound to ONE identity:
  an exclusive open of `path` returning descriptor D; then a change_mode of `path` to exactly 0600; then a write of
  32 bytes to D (the nonempty witness). Between the exclusive open and that write the path must not be deleted,
  renamed or given any other mode, and the secret must go to D, not to another descriptor or another path.
  """
  def permission_first(setup, path) do
    me = self()
    :erlang.trace_pattern({:file, :open, 2}, [{:_, [], [{:return_trace}]}], [:global])
    :erlang.trace_pattern({:file, :write, 2}, true, [:global])
    :erlang.trace_pattern({:file, :write_file, :_}, true, [:global])
    :erlang.trace_pattern({:file, :pwrite, :_}, true, [:global])
    :erlang.trace_pattern({:file, :change_mode, 2}, true, [:global])
    :erlang.trace_pattern({:file, :delete, :_}, true, [:global])
    :erlang.trace_pattern({:file, :rename, :_}, true, [:global])
    pid = spawn(fn -> receive do: (:go -> send(me, {:setup_result, setup.(path)})) end)
    :erlang.trace(pid, true, [:call, {:tracer, me}])
    send(pid, :go)

    try do
      result =
        receive do
          {:setup_result, r} -> r
        after
          5_000 -> flunk("step:setup_hung")
        end

      ref = :erlang.trace_delivered(pid)

      receive do
        {:trace_delivered, ^pid, ^ref} -> :ok
      end

      calls = collect(pid, [])
      step!(result == :ok, :setup_failed, inspect(result))
      judge(calls, path)
    after
      if Process.alive?(pid), do: :erlang.trace(pid, false, [:call])
      :erlang.trace_pattern({:file, :_, :_}, false, [:global])
      flush_traces(pid)
    end
  end

  defp judge(calls, path) do
    chars = String.to_charlist(path)
    same? = fn p -> p == path or p == chars end

    open_at =
      Enum.find_index(calls, fn c ->
        match?({:call, {:file, :open, [_, _]}}, c) and same?.(elem(elem(c, 1), 2) |> hd()) and
          :exclusive in List.wrap(elem(elem(c, 1), 2) |> List.last())
      end)

    step!(open_at != nil, :no_exclusive_open, inspect(calls))
    {device, after_open} = descriptor_after(calls, open_at)
    step!(device != nil, :no_exclusive_open, "the exclusive open returned no descriptor")

    perm_at =
      Enum.find_index(after_open, &match?({:call, {:file, :change_mode, [p, 0o600]}} when p == path or p == chars, &1))

    step!(perm_at != nil, :no_permission_change, inspect(after_open))

    write_at =
      Enum.find_index(
        after_open,
        &match?(
          {:call, {:file, :write, [d, bytes]}} when d == device and is_binary(bytes) and byte_size(bytes) == 32,
          &1
        )
      )

    window = if write_at, do: Enum.slice(after_open, 0, write_at), else: after_open

    replaced =
      Enum.any?(window, fn
        {:call, {:file, f, [p | _]}} when f in [:delete, :rename] -> same?.(p)
        {:call, {:file, :change_mode, [p, mode]}} -> same?.(p) and mode != 0o600
        {:call, {:file, :open, [p, modes]}} -> same?.(p) and :exclusive not in List.wrap(modes)
        {:call, {:file, :write_file, [p | _]}} -> same?.(p)
        _ -> false
      end)

    step!(not replaced, :target_replaced, inspect(window))
    step!(write_at != nil, :no_secret_write, "no 32-byte write to the exclusive descriptor: #{inspect(after_open)}")
    step!(perm_at < write_at, :write_before_permission, inspect(Enum.slice(after_open, 0, write_at + 1)))
    :ok
  end

  defp descriptor_after(calls, open_at) do
    rest = Enum.drop(calls, open_at + 1)

    case Enum.find_index(rest, &match?({:return, {:file, :open, 2}, {:ok, _}}, &1)) do
      nil -> {nil, rest}
      i -> {elem(elem(Enum.at(rest, i), 2), 1), Enum.drop(rest, i + 1)}
    end
  end

  defp collect(pid, acc) do
    receive do
      {:trace, ^pid, :call, mfa} -> collect(pid, [{:call, mfa} | acc])
      {:trace, ^pid, :return_from, mfa, value} -> collect(pid, [{:return, mfa, value} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp flush_traces(pid) do
    receive do
      {:trace, ^pid, _, _} -> flush_traces(pid)
      {:trace, ^pid, _, _, _} -> flush_traces(pid)
    after
      0 -> :ok
    end
  end

  # ---- worker lifetime (C1-11c ordering, C1-11d ack/link) ----
  @doc "A held read: announces {:reading, worker} to the test and waits for :finish."
  def held(test) do
    fn ->
      send(test, {:reading, self()})
      receive do: (:finish -> {:ok, :value})
    end
  end

  def down(pid, step) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      1_500 -> step!(false, step, "surviving #{inspect(pid)}")
    end
  end

  defp dummy_view do
    track(spawn(fn -> receive do: (:stop -> :ok) end))
  end

  @doc """
  The C1-11c cut point: worker DEAD and its Registry key gone while the controller has NOT yet observed the DOWN
  (the controller is suspended). A replacement read for the same view must be refused (:view_busy) there: an admitted
  replacement whose worker starts reading is the named violation. After the controller observes DOWN and delivers, a
  replacement is admitted. `adapter` = %{start: fn(view, fun, opts), registry: name}.
  """
  def ordering(adapter) do
    test = self()
    {:ok, j1} = adapter.start.(test, held(test), correlation: :first)
    track(j1)
    w1 = receive do: ({:reading, w} -> track(w))
    :ok = :sys.suspend(j1)
    send(w1, :finish)
    down(w1, :worker_survived)
    # the Registry clears a dead worker's key asynchronously: bounded settle, then the key must be gone
    step!(settle_key(adapter.registry, {:worker, test}, 20) == [], :registry_not_cleaned)
    refute_received {:query_result, ^j1, _, _}

    case adapter.start.(test, held(test), correlation: :second) do
      {:error, :view_busy} ->
        :ok

      {:ok, j2} ->
        track(j2)
        w2 = receive do: ({:reading, w} -> track(w)), after: (1_000 -> nil)

        step!(
          false,
          :premature_admission,
          "replacement #{inspect(j2)} admitted before the controller observed DOWN; reading worker #{inspect(w2)}"
        )

      other ->
        step!(false, :refusal_vocabulary, "unexpected admission answer #{inspect(other)}")
    end

    :ok = :sys.resume(j1)

    step!(
      match?(
        {:query_result, ^j1, :first, {:ok, :value}},
        receive(do: (m = {:query_result, _, _, _} -> m), after: (1_500 -> :none))
      ),
      :no_delivery
    )

    down(j1, :controller_survived)
    # the dead controller's key clears asynchronously; the replacement is admitted once both keys are gone
    step!(settle_key(adapter.registry, {:controller, test}, 20) == [], :controller_key_not_cleaned)
    {:ok, j2} = adapter.start.(test, held(test), correlation: :second)
    track(j2)
    w2 = receive do: ({:reading, w} -> track(w))
    send(w2, :finish)

    step!(
      match?(
        {:query_result, ^j2, :second, {:ok, :value}},
        receive(do: (m = {:query_result, _, _, _} -> m), after: (1_500 -> :none))
      ),
      :no_replacement_delivery
    )

    :ok
  end

  @doc """
  C1-11d, two witnesses. (1) Worker acknowledgement before read entry: a tracer process records every
  `:proc_lib.init_ack/2` call of NEW processes with monotonic timestamps; the read records its own entry time; the
  worker's ack must precede it (a read before the ack is the violation). (2) A controller killed before the read
  proceeds admits no detached read (the linked worker dies; a no-link mutant lets the read finish); also a controller
  killed immediately after start (before or around the worker's ack/link) leaves no worker and no read, repeatedly.
  """
  def link_gate(adapter) do
    test = self()
    tracer = spawn_link(fn -> ack_tracer([]) end)
    :erlang.trace_pattern({:proc_lib, :init_ack, 2}, true, [:global])
    :erlang.trace(:new_processes, true, [:call, :monotonic_timestamp, {:tracer, tracer}])

    gated = fn ->
      entered = :erlang.monotonic_time()
      send(test, {:entered, self(), entered})
      receive do: (:proceed -> :ok)
      send(test, {:read_done, self()})
      {:ok, :value}
    end

    try do
      started = System.monotonic_time(:millisecond)
      {:ok, job} = adapter.start.(dummy_view(), gated, deadline_ms: 5_000)
      track(job)
      step!(System.monotonic_time(:millisecond) - started < 500, :ack_awaits_read)

      {worker, entered} =
        receive do: ({:entered, w, t} -> {track(w), t}), after: (1_500 -> step!(false, :read_never_entered))

      ref = :erlang.trace_delivered(worker)
      receive do: ({:trace_delivered, ^worker, ^ref} -> :ok)
      send(tracer, {:acks, test})
      acks = receive do: ({:acks, a} -> a)
      ack_ts = for {^worker, ts} <- acks, do: ts

      step!(
        ack_ts != [],
        :read_before_worker_ack,
        "the read entered before any init_ack of the worker #{inspect(worker)}: #{inspect(acks)}"
      )

      step!(
        Enum.min(ack_ts) < entered,
        :read_before_worker_ack,
        "read entered at #{entered}, worker ack at #{inspect(ack_ts)}"
      )

      Process.exit(job, :kill)
      down(job, :controller_survived)
      send(worker, :proceed)

      receive do
        {:read_done, ^worker} -> step!(false, :detached_read, "the read finished after its controller died")
      after
        300 -> :ok
      end

      down(worker, :worker_survived)

      # early controller death, ten times: the cut point is the controller DOWN; afterwards NO worker may stay
      # registered under THIS view's key, any worker that entered must be DOWN (proceed is offered so a blocked
      # read is not mistaken for a dead one), and no read may complete
      for _ <- 1..10 do
        view = dummy_view()
        {:ok, j} = adapter.start.(view, gated, deadline_ms: 5_000)
        track(j)
        Process.exit(j, :kill)
        down(j, :controller_survived)
        entered = drain_entered([])
        # ownership first: with the controller dead, THIS view's key must clear before any read is offered to proceed
        registered = settle_key(adapter.registry, {:worker, view}, 20)

        step!(
          registered == [],
          :orphan_after_early_kill,
          "worker(s) #{inspect(registered)} stay registered under #{inspect(view)} after its controller died"
        )

        for w <- entered, do: send(w, :proceed)

        receive do
          {:read_done, w} ->
            step!(false, :detached_read_after_early_kill, "read of #{inspect(w)} completed after its controller died")
        after
          300 -> :ok
        end

        for w <- entered, do: down(w, :detached_worker_survived)
      end

      :ok
    after
      :erlang.trace(:new_processes, false, [:call])
      :erlang.trace_pattern({:proc_lib, :init_ack, 2}, false, [:global])
      Process.unlink(tracer)
      Process.exit(tracer, :kill)
    end
  end

  defp drain_entered(acc) do
    receive do
      {:entered, w, _} -> drain_entered([track(w) | acc])
    after
      100 -> acc
    end
  end

  # waits (bounded) for the key to clear; answers the registered workers that remain
  defp settle_key(registry, key, tries) do
    case Registry.lookup(registry, key) do
      [] ->
        []

      _pids when tries > 0 ->
        Process.sleep(50)
        settle_key(registry, key, tries - 1)

      pids ->
        Enum.map(pids, &elem(&1, 0))
    end
  end

  defp ack_tracer(acc) do
    receive do
      {:trace_ts, pid, :call, {:proc_lib, :init_ack, _}, ts} -> ack_tracer([{pid, ts} | acc])
      {:acks, to} -> send(to, {:acks, acc})
      _ -> ack_tracer(acc)
    end
  end

  # ---- delivery revalidation and identity (C1-09d/e) ----
  @doc """
  While the session is valid: the current {job, correlation} is applied; a mismatched job or a mismatched
  correlation is dropped; the current one afterwards is applied. After revocation the current result is dropped.
  """
  def delivery(deliver) do
    valid = :ets.new(:c1_valid, [:public])
    :ets.insert(valid, {:valid, true})
    state = %{valid?: fn -> :ets.lookup_element(valid, :valid, 2) end, current: {:job, :c1}, data: nil, dropped: 0}
    applied = deliver.(state, {:query_result, :job, :c1, {:ok, :first}})
    step!(applied.data == {:ok, :first}, :accepted_control_dropped)
    state = %{applied | current: {:job, :c2}}
    forged_job = deliver.(state, {:query_result, :other, :c2, {:ok, :forged_job}})
    step!(forged_job.data == {:ok, :first} and forged_job.dropped == 1, :forged_job, inspect(forged_job))
    forged_corr = deliver.(forged_job, {:query_result, :job, :c3, {:ok, :forged_correlation}})
    step!(forged_corr.data == {:ok, :first} and forged_corr.dropped == 2, :forged_correlation, inspect(forged_corr))
    current = deliver.(forged_corr, {:query_result, :job, :c2, {:ok, :second}})
    step!(current.data == {:ok, :second} and current.current == nil, :current_after_forgeries_dropped, inspect(current))
    state = %{current | current: {:job, :c4}}
    :ets.insert(valid, {:valid, false})
    stale = deliver.(state, {:query_result, :job, :c4, {:ok, :stale}})
    step!(stale.data == {:ok, :second} and stale.dropped == 3, :stale_delivery, inspect(stale))
    :ok
  end
end
