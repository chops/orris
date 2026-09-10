defmodule AiOrchestrator.Lifecycle.HostStagedRedTest do
  @moduledoc """
  RED/interface for the staged Host record (EO-M1, contract docs/contracts/effect-owner-lifecycle.org, revision 2):
  `Host.commit_step/1`, `Host.resume_step/2`, `Host.close_step/2`, `Host.reject_step/2` and the `%Host.Stage{}`
  record. Every RED row is paired with a CONTROL that reaches the same oracle today through the synchronous
  `Host.advance/1` (kept for parity). Pure: no processes beyond the recording agents.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.OwnerDoubles.AbandonGate
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @sentinel "STAGED-OBSERVER-SENTINEL-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

  # late-bound: the staged functions do not exist yet; a dynamic call keeps --warnings-as-errors honest
  defp host, do: Module.concat(["AiOrchestrator", "Lifecycle", "Host"])

  defp require_staged! do
    Code.ensure_loaded(Host)

    for {fun, arity} <- [commit_step: 1, resume_step: 2, close_step: 2, reject_step: 2] do
      assert function_exported?(Host, fun, arity), "Host.#{fun}/#{arity} does not exist"
    end
  end

  # ---- doubles ----
  defmodule CountingDispatch do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def deliver(command, opts) do
      Agent.update(opts[:counter], &Map.update(&1, :deliver, 1, fn n -> n + 1 end))
      opts[:inner].deliver(command, opts[:inner_opts])
    end

    def snapshot(command, opts), do: opts[:inner].snapshot(command, opts[:inner_opts])
    def observe(command, opts), do: opts[:inner].observe(command, opts[:inner_opts])
    def reconcile(command, opts), do: opts[:inner].reconcile(command, opts[:inner_opts])
  end

  defmodule CountingGate do
    @moduledoc false
    # counts release calls; everything else is the C4 double
    def counter, do: Process.get(:counting_gate_counter)
    def prepare(fs, request, opts), do: GateDouble.prepare(fs, request, opts)
    def started_data(prepared), do: GateDouble.started_data(prepared)
    def identity(prepared), do: GateDouble.identity(prepared)
    def ack(prepared, persisted), do: GateDouble.ack(prepared, persisted)

    def release(prepared, ack, opts \\ []) do
      Agent.update(counter(), &Map.update(&1, :release, 1, fn n -> n + 1 end))
      GateDouble.release(prepared, ack, opts)
    end

    def abandon(handle), do: GateDouble.abandon(handle)
    def expire(handle), do: GateDouble.expire(handle)
    def await(running, opts \\ []), do: GateDouble.await(running, opts)
    def evidence(run_dir, id, attempt), do: GateDouble.evidence(run_dir, id, attempt)
    def reconcile(fs, run_dir, expected, opts \\ []), do: GateDouble.reconcile(fs, run_dir, expected, opts)
    def pass?(data), do: GateDouble.pass?(data)
  end

  # ---- helpers ----
  defp scenario_case(index) do
    {_name, :run, scenario, [], make} = Enum.at(H.cases(), index)
    H.reset_seams()
    {scenario, make.()}
  end

  defp gated_index, do: Enum.find_index(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

  # a recording sink: every persisted event, in order, exactly as the Host received it back
  defp recording(opts) do
    {:ok, log} = Agent.start_link(fn -> [] end)
    stamping = GateDouble.receipt(fn _event -> :ok end)

    # records what the sink HANDS BACK (the stamped receipt), which is what the Host commits
    sink = fn event ->
      case stamping.(event) do
        {:ok, persisted} = answer ->
          Agent.update(log, &(&1 ++ [persisted]))
          answer

        other ->
          other
      end
    end

    {Keyword.put(opts, :event_sink, sink), log}
  end

  defp counting_dispatch(opts) do
    {:ok, counter} = Agent.start_link(fn -> %{} end)

    opts =
      Keyword.merge(opts,
        dispatch: CountingDispatch,
        dispatch_opts: [inner: opts[:dispatch], inner_opts: Keyword.get(opts, :dispatch_opts, []), counter: counter]
      )

    {opts, counter}
  end

  defp persisted(log), do: Agent.get(log, & &1)
  defp since(log, n), do: log |> persisted() |> Enum.drop(n)

  defp open!(scenario, opts) do
    {:ok, loop} = Host.open(:run, %{spec: H.spec(scenario), plan: H.plan(scenario)}, opts)
    loop
  end

  # advance (control path) until the step satisfies `pred`; a halt before that is a harness failure
  defp drive_until(loop, pred) do
    if pred.(loop.step) do
      loop
    else
      case Host.advance(loop) do
        {:continue, next} -> drive_until(next, pred)
        {:halt, result} -> flunk("halted before the wanted step: #{inspect(elem(result, 0))}")
      end
    end
  end

  defp release_step?({:effect, %Effect.ReleaseGate{}, _state, _appended}), do: true
  defp release_step?(_), do: false
  defp done_step?({:done, _state, _appended}), do: true
  defp done_step?(_), do: false

  # the step whose OWN suffix carries the gate terminal: the running handle is live in the runtime until this
  # suffix commits as a whole (release_terminal drops it); a refusal here must abandon it instead
  defp terminal_suffix_step?({_kind, _a, _b, appended}) when is_list(appended), do: gate_terminal?(appended)
  defp terminal_suffix_step?({:done, _state, appended}), do: gate_terminal?(appended)
  defp terminal_suffix_step?(_), do: false
  defp gate_terminal?(appended), do: Enum.any?(appended, &(&1["type"] in ["gate_passed", "gate_failed"]))

  defp abandoning(opts) do
    AbandonGate.control(self(), :ok)
    Keyword.put(opts, :gate_executor, AbandonGate)
  end

  defp abandons do
    receive do
      {:abandoned, _pid, _mode} -> 1 + abandons()
    after
      0 -> 0
    end
  end

  defp refusing_terminal(opts) do
    {:ok, log} = Agent.start_link(fn -> [] end)

    sink =
      GateDouble.receipt(fn event ->
        if event["type"] in ["gate_passed", "gate_failed"] do
          {:error, %{"reason" => "sink_refused_terminal"}}
        else
          Agent.update(log, &(&1 ++ [event]))
          {:ok, event}
        end
      end)

    {Keyword.put(opts, :event_sink, sink), log}
  end

  defp failing_sink(opts, kind, secret) do
    sink =
      GateDouble.receipt(fn event ->
        if event["type"] in ["gate_passed", "gate_failed"], do: fail_with(kind, :sink, secret), else: {:ok, event}
      end)

    Keyword.put(opts, :event_sink, sink)
  end

  defp fail_with(:error, _origin, secret), do: raise(secret)
  defp fail_with(:throw, origin, secret), do: throw({origin, secret})
  defp fail_with(:exit, origin, secret), do: exit({origin, secret})

  defp failing_observer(opts, kind, secret),
    do: Keyword.put(opts, :effect_observer, fn _effect, _observation -> fail_with(kind, :observer, secret) end)

  # the ORIGINAL value at the origin, not only its kind: the exact message for a raise, the exact term otherwise
  defp trap(:error, _origin, secret, fun), do: assert_raise(RuntimeError, secret, fun)
  defp trap(:throw, origin, secret, fun), do: assert(catch_throw(fun.()) == {origin, secret})
  defp trap(:exit, origin, secret, fun), do: assert(catch_exit(fun.()) == {origin, secret})

  # the staged driver: the runtime is LOCAL to the driver (the worker's role); the loop never carries one
  defp drive_staged(loop, runtime, opts, mutate) do
    loop = mutate.(loop)

    case host().commit_step(loop) do
      {:effect, stage} ->
        assert stage.loop.runtime == nil, "the loop handed back by commit_step carries no runtime"
        runtime = Effects.release_terminal(runtime, stage.suffix)
        # the loop's opts are the RESOLVED ones (identity added at open): the owner executes with those
        {observation, runtime} = Effects.execute(stage.intent, runtime, opts: stage.loop.opts, receipt: stage.receipt)
        {:continue, next} = host().resume_step(stage, observation)
        drive_staged(next, runtime, opts, mutate)

      {:done, stage, result} ->
        runtime = Effects.release_terminal(runtime, stage.suffix)
        {cleanup, _empty} = Effects.settle(runtime)
        host().close_step(result, cleanup)

      {:rejected, rejection} ->
        {cleanup, _empty} = Effects.settle(runtime)
        host().reject_step(rejection, cleanup)
    end
  end

  defp drive_advance({:ok, loop}, mutate) do
    case Host.advance(mutate.(loop)) do
      {:continue, next} -> drive_advance({:ok, next}, mutate)
      {:halt, result} -> result
    end
  end

  defp identity, do: & &1

  # =================================================================================================
  describe "S-1 effect stage" do
    test "control: advance persists exactly the suffix the stage would carry, and continues" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, log} = recording(opts)
      loop = open!(scenario, opts)
      n = length(persisted(log))
      assert {:continue, next} = Host.advance(loop)
      suffix = since(log, n)
      assert suffix != [] and next.committed == loop.committed ++ suffix
      assert Enum.map(suffix, & &1["seq"]) == Enum.to_list((n + 1)..(n + length(suffix)))
    end

    test "S-1 commit_step returns the immutable persisted suffix, the intent, no receipt for a non-gate effect and a loop without a runtime" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, log} = recording(opts)
      loop = open!(scenario, opts)
      {:effect, intent, _state, _appended} = loop.step
      n = length(persisted(log))
      assert {:effect, stage} = host().commit_step(loop)
      suffix = since(log, n)
      assert stage.kind == :effect and stage.intent == intent and is_reference(stage.ref)
      assert stage.suffix == suffix and suffix != []
      assert stage.loop.committed == loop.committed ++ suffix
      assert stage.loop.runtime == nil
      assert stage.receipt == nil
    end

    test "S-1b the ReleaseGate receipt is the persisted gate_started at started_seq, byte-identical to the sink's" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, log} = recording(opts)
      loop = scenario |> open!(opts) |> drive_until(&release_step?/1)
      {:effect, %Effect.ReleaseGate{started_seq: seq}, _state, _appended} = loop.step
      assert {:effect, stage} = host().commit_step(loop)
      assert %{"seq" => ^seq, "type" => "gate_started"} = stage.receipt
      assert stage.receipt == Enum.find(persisted(log), &(&1["seq"] == seq))
    end
  end

  describe "S-2 done stage" do
    test "control: advance's done leg halts with the summary; cleanup is merged only when non-empty" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(opts)
      loop = scenario |> open!(opts) |> drive_until(&done_step?/1)
      assert {:halt, {:ok, %{summary: %{"status" => "completed"}} = result}} = Host.advance(loop)
      refute Map.has_key?(result, :gate_cleanup), "no retained handle at the terminal: no cleanup key"
    end

    test "S-2 commit_step returns the terminal stage and a result without cleanup; close_step merges cleanup only when non-empty" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, log} = recording(opts)
      loop = scenario |> open!(opts) |> drive_until(&done_step?/1)
      {:done, _state, appended} = loop.step
      assert {:halt, {:ok, control}} = Host.advance(loop)
      n = length(persisted(log))
      assert {:done, stage, result} = host().commit_step(loop)
      assert stage.kind == :done and stage.intent == nil and stage.loop.runtime == nil
      assert stage.suffix == since(log, n) and length(stage.suffix) == length(appended)
      assert result.summary == control.summary
      refute Map.has_key?(result, :gate_cleanup)
      assert host().close_step(result, []) == {:ok, result}
      cleanup = [%{"gate_run_id" => "g", "attempt" => 1, "settle" => %{"clause" => "settle_unproven"}}]
      assert host().close_step(result, cleanup) == {:ok, Map.put(result, :gate_cleanup, cleanup)}
    end
  end

  describe "S-3 sequence gap" do
    defp gapped(loop), do: %{loop | committed: Enum.drop(loop.committed, -1)}

    test "control: advance halts with event_sequence_gap on a committed prefix that lost its tail" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(opts)
      {:continue, loop} = Host.advance(open!(scenario, opts))
      assert {:halt, {:error, %{"reason" => "event_sequence_gap"}}} = Host.advance(gapped(loop))
    end

    test "S-3 commit_step rejects with the same rejection and produces no stage; reject_step merges cleanup only when non-empty" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, log} = recording(opts)
      {:continue, loop} = Host.advance(open!(scenario, opts))
      assert {:halt, {:error, rejection}} = Host.advance(gapped(loop))
      n = length(persisted(log))
      assert {:rejected, ^rejection} = host().commit_step(gapped(loop))
      assert since(log, n) == [], "a rejected commit persists nothing"
      assert host().reject_step(rejection, []) == {:error, rejection}
      cleanup = [%{"gate_run_id" => "g", "attempt" => 1, "settle" => %{"clause" => "settle_unproven"}}]
      assert host().reject_step(rejection, cleanup) == {:error, Map.put(rejection, "gate_cleanup", cleanup)}
    end
  end

  describe "S-4 sink refusal inside a suffix" do
    defp refusing(opts, refused_type) do
      {:ok, log} = Agent.start_link(fn -> [] end)

      sink =
        GateDouble.receipt(fn event ->
          if event["type"] == refused_type do
            {:error, %{"reason" => "sink_refused", "type" => refused_type}}
          else
            Agent.update(log, &(&1 ++ [event]))
            {:ok, event}
          end
        end)

      {Keyword.put(opts, :event_sink, sink), log}
    end

    test "control: advance halts with the sink's rejection, the partial suffix stays partial and no effect executes" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, counter} = counting_dispatch(opts)
      {opts, log} = refusing(opts, "plan_recorded")
      loop = open!(scenario, opts)
      assert {:halt, {:error, rejection}} = Host.advance(loop)
      assert is_map(rejection)
      assert Enum.map(persisted(log), & &1["type"]) == ["run_created", "run_spec_loaded"], "the refusal stops the suffix"
      assert Agent.get(counter, & &1) == %{}, "no effect executes after a refused commit"
    end

    test "S-4 commit_step rejects with the identical rejection, no stage, no execute" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, counter} = counting_dispatch(opts)
      {opts, log} = refusing(opts, "plan_recorded")
      loop = open!(scenario, opts)
      assert {:halt, {:error, rejection}} = Host.advance(loop)
      persisted_by_control = persisted(log)
      assert {:rejected, ^rejection} = host().commit_step(loop)
      # the fixed clock ticks per read, so the two commits differ ONLY by the measured "ts" (the recording sink
      # stamps no chain hash): every other field is compared in full
      without_ts = &Enum.map(&1, fn e -> Map.delete(e, "ts") end)

      assert without_ts.(persisted(log)) == without_ts.(persisted_by_control ++ persisted_by_control),
             "the same partial suffix"

      # the comparison is not a projection: a one-field payload mutation is detected
      [first | rest] = persisted(log)
      mutated = [put_in(first, ["data", "mutated"], true) | rest]
      refute without_ts.(mutated) == without_ts.(persisted_by_control ++ persisted_by_control)
      assert Agent.get(counter, & &1) == %{}
    end
  end

  describe "S-2b terminal suffix with a RETAINED handle: release, never abandon" do
    test "control: the running handle is live before the terminal suffix commits and dropped by release (zero abandons)" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(abandoning(opts))
      loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
      assert map_size(loop.runtime.gates) == 1, "the gate handle is retained until its terminal commits"
      result = Host.advance(loop)

      assert match?({:continue, %{runtime: %{gates: gates}}} when map_size(gates) == 0, result) or
               match?({:halt, {:ok, _}}, result)

      assert abandons() == 0, "a committed terminal releases the handle; nothing is abandoned"
    end

    test "S-2b commit_step's suffix carries the gate terminal; release_terminal on that suffix drops the handle; settle then finds nothing" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(abandoning(opts))
      loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
      runtime = loop.runtime
      assert map_size(runtime.gates) == 1

      stage =
        case host().commit_step(%{loop | runtime: nil}) do
          {:effect, stage} -> stage
          {:done, stage, _result} -> stage
        end

      assert gate_terminal?(stage.suffix)
      released = Effects.release_terminal(runtime, stage.suffix)
      assert map_size(released.gates) == 0
      assert Effects.settle(released) == {[], released}
      assert abandons() == 0
    end
  end

  describe "S-4b FINAL refusal with a retained handle: abandon, cleanup reported, primary rejection kept" do
    test "control: advance refused at the terminal suffix settles the live handle (one abandon) and reports gate_cleanup" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = refusing_terminal(abandoning(opts))
      loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
      # the sink's own rejection is closed by the Host into journal_append_failed; the settle carries the
      # executor's proof
      assert {:halt, {:error, %{"reason" => "journal_append_failed", "gate_cleanup" => [entry]}}} = Host.advance(loop)
      assert %{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true}} = entry
      assert abandons() == 1
    end

    test "S-4b commit_step rejects with the primary rejection and NO cleanup; the owner's settle abandons once; reject_step reports it" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = refusing_terminal(abandoning(opts))
      loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
      runtime = loop.runtime
      assert {:rejected, %{"reason" => "journal_append_failed"} = rejection} = host().commit_step(%{loop | runtime: nil})
      refute Map.has_key?(rejection, "gate_cleanup")
      assert abandons() == 0, "the Host settles nothing: it holds no runtime"
      {cleanup, _} = Effects.settle(runtime)
      assert abandons() == 1

      assert rejection["reason"] == "journal_append_failed"
      assert host().reject_step(rejection, cleanup) == {:error, Map.put(rejection, "gate_cleanup", cleanup)}
    end
  end

  for kind <- [:error, :throw, :exit] do
    describe "S-8 sink #{kind} with a retained handle" do
      test "control: advance settles the live handle in the same invocation, then re-raises the original #{kind}" do
        {scenario, opts} = scenario_case(gated_index())
        opts = failing_sink(abandoning(opts), unquote(kind), @sentinel)
        loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
        trap(unquote(kind), :sink, @sentinel, fn -> Host.advance(loop) end)
        assert abandons() == 1
      end

      test "S-8 commit_step propagates the original #{kind} unchanged and settles nothing (the owner settles)" do
        require_staged!()
        {scenario, opts} = scenario_case(gated_index())
        opts = failing_sink(abandoning(opts), unquote(kind), @sentinel)
        loop = scenario |> open!(opts) |> drive_until(&terminal_suffix_step?/1)
        trap(unquote(kind), :sink, @sentinel, fn -> host().commit_step(%{loop | runtime: nil}) end)
        assert abandons() == 0
      end
    end

    describe "S-9 observer #{kind}" do
      test "control: advance re-raises the observer's original #{kind}" do
        {scenario, opts} = scenario_case(gated_index())
        {opts, _log} = recording(failing_observer(opts, unquote(kind), @sentinel))
        loop = open!(scenario, opts)
        trap(unquote(kind), :observer, @sentinel, fn -> Host.advance(loop) end)
      end

      test "S-9 resume_step re-raises the observer's original #{kind} unchanged" do
        require_staged!()
        {scenario, opts} = scenario_case(gated_index())
        {opts, _log} = recording(failing_observer(opts, unquote(kind), @sentinel))
        loop = open!(scenario, opts)
        assert {:effect, stage} = host().commit_step(loop)
        {observation, _} = Effects.execute(stage.intent, Runtime.new(opts), opts: stage.loop.opts, receipt: stage.receipt)
        trap(unquote(kind), :observer, @sentinel, fn -> host().resume_step(stage, observation) end)
      end
    end
  end

  # Reducer-origin rejection: no fixture scenario reaches a `{:error, rejection}` reducer step through a legitimate
  # observation (every adapter/executor answer is mapped to a closed observation first). S-3/S-4/S-4b are HOST
  # commit rejections; a reducer-origin witness is surfaced as unreachable rather than faked.

  describe "S-5 receipt missing for a ReleaseGate (ruled: the nil receipt reaches the executor; ack refuses; behavior preserved)" do
    # the ReleaseGate names a started_seq no persisted event carries: receipt selection finds nothing
    defp orphan(loop) do
      case loop.step do
        {:effect, %Effect.ReleaseGate{started_seq: seq} = intent, state, appended} ->
          %{loop | step: {:effect, %{intent | started_seq: seq + 100_000}, state, appended}}

        _other ->
          loop
      end
    end

    defp with_counting_gate(opts) do
      {:ok, counter} = Agent.start_link(fn -> %{} end)
      Process.put(:counting_gate_counter, counter)
      {Keyword.put(opts, :gate_executor, CountingGate), counter}
    end

    test "control: advance with a missing receipt never reaches release (ack refuses first) and the run still closes" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, counter} = with_counting_gate(opts)
      result = drive_advance(Host.open(:run, %{spec: H.spec(scenario), plan: H.plan(scenario)}, opts), &orphan/1)
      assert match?({:ok, %{summary: %{"status" => _}}}, result) or match?({:error, %{}}, result)
      assert Map.get(Agent.get(counter, & &1), :release, 0) == 0, "no release without the persisted receipt"
    end

    test "S-5 the staged path hands the worker the same nil receipt: no release, identical closed outcome to advance" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, counter} = with_counting_gate(opts)
      control = drive_advance(Host.open(:run, %{spec: H.spec(scenario), plan: H.plan(scenario)}, opts), &orphan/1)
      control_releases = Map.get(Agent.get(counter, & &1), :release, 0)
      H.reset_seams()
      {scenario, opts} = scenario_case(gated_index())
      {opts, counter} = with_counting_gate(opts)
      loop = open!(scenario, opts)
      staged = drive_staged(loop, Runtime.new(opts), opts, &orphan/1)
      assert staged == control
      assert Map.get(Agent.get(counter, & &1), :release, 0) == control_releases and control_releases == 0
    end
  end

  describe "S-6 observer failure" do
    defp raising_observer(opts), do: Keyword.put(opts, :effect_observer, fn _effect, _observation -> raise(@sentinel) end)

    test "control: advance re-raises the observer's original error unchanged" do
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(raising_observer(opts))
      loop = open!(scenario, opts)
      assert_raise RuntimeError, @sentinel, fn -> Host.advance(loop) end
    end

    test "S-6 resume_step raises the observer's original error unchanged and settles nothing (the Host holds no runtime)" do
      require_staged!()
      {scenario, opts} = scenario_case(gated_index())
      {opts, _log} = recording(raising_observer(opts))
      loop = open!(scenario, opts)
      assert {:effect, stage} = host().commit_step(loop)

      {observation, _runtime} =
        Effects.execute(stage.intent, Runtime.new(opts), opts: stage.loop.opts, receipt: stage.receipt)

      assert_raise RuntimeError, @sentinel, fn -> host().resume_step(stage, observation) end
    end
  end

  describe "S-7b parity for resume and cancel over the fixture cases" do
    defp reference(:resume, scenario, prior, opts), do: Host.resume(H.spec(scenario), H.plan(scenario), prior, opts)
    defp reference(:cancel, _scenario, prior, opts), do: Host.cancel(prior, opts)
    defp inputs(:resume, scenario, prior), do: %{spec: H.spec(scenario), plan: H.plan(scenario), prior_lines: prior}
    defp inputs(:cancel, _scenario, prior), do: %{prior_lines: prior}

    test "control: Host.resume / Host.cancel are deterministic per case" do
      for {name, kind, scenario, prior, make} <- H.cases(), kind in [:resume, :cancel] do
        H.reset_seams()
        first = reference(kind, scenario, prior, make.())
        H.reset_seams()
        assert first == reference(kind, scenario, prior, make.()), name
      end
    end

    test "S-7b the staged driver over open(:resume | :cancel) equals Host.resume / Host.cancel (:continue is exercised only through the Server's admission path)" do
      require_staged!()

      for {name, kind, scenario, prior, make} <- H.cases(), kind in [:resume, :cancel] do
        H.reset_seams()
        expected = reference(kind, scenario, prior, make.())
        H.reset_seams()
        opts = make.()

        staged =
          case Host.open(kind, inputs(kind, scenario, prior), opts) do
            {:ok, loop} -> drive_staged(loop, Runtime.new(opts), opts, identity())
            {:error, _} = error -> error
          end

        assert staged == expected, name
      end
    end
  end

  describe "S-7 parity with Host.run over every fixture run scenario" do
    test "control: Host.run is deterministic per scenario (the parity oracle is meaningful)" do
      for {name, :run, scenario, [], make} <- H.cases() do
        H.reset_seams()
        first = Host.run(H.spec(scenario), H.plan(scenario), make.())
        H.reset_seams()
        second = Host.run(H.spec(scenario), H.plan(scenario), make.())
        assert first == second, name
      end
    end

    test "S-7 the staged driver with a local runtime yields the same events, appended events and summary as Host.run" do
      require_staged!()

      for {name, :run, scenario, [], make} <- H.cases() do
        H.reset_seams()
        expected = Host.run(H.spec(scenario), H.plan(scenario), make.())
        H.reset_seams()
        opts = make.()
        {:ok, loop} = Host.open(:run, %{spec: H.spec(scenario), plan: H.plan(scenario)}, opts)
        assert drive_staged(loop, Runtime.new(opts), opts, identity()) == expected, name
      end
    end
  end
end
