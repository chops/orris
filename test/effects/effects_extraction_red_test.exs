defmodule AiOrchestrator.Effects.ExtractionRedTest do
  @moduledoc """
  RED/interface for Effects extraction (docs/contracts/effects-extraction.org). The boundary does
  not exist yet: every call into it goes through a runtime-computed module receiver (`effects/0`,
  `runtime/0`), so the tree compiles warning-free and each test names the exact missing capability.
  Execution is measured, never read from a table.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Lifecycle.Core.Diagnostic, as: CoreDiagnostic
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @host_src Path.expand("../../lib/ai_orchestrator/lifecycle/host.ex", __DIR__)
  @effects_src Path.expand("../../lib/ai_orchestrator/effects.ex", __DIR__)
  @lifecycle_src Path.expand("../../lib/ai_orchestrator/lifecycle.ex", __DIR__)
  @core_diag_src Path.expand("../../lib/ai_orchestrator/lifecycle/core/diagnostic.ex", __DIR__)

  # runtime-computed receivers: no compile-time reference, no apply/3
  defp effects, do: Module.concat([AiOrchestrator, Effects])
  defp runtime, do: Module.concat([AiOrchestrator, Effects, Runtime])
  defp interrupted, do: Module.concat([AiOrchestrator, Effects, Interrupted])
  defp unreachable, do: Module.concat([AiOrchestrator, Effects, Unreachable])
  defp contract_diagnostic, do: Module.concat([AiOrchestrator, Contract, Diagnostic])
  defp loaded?(module), do: Code.ensure_loaded?(module)

  defp require_boundary!,
    do: assert(loaded?(effects()) and loaded?(runtime()), "AiOrchestrator.Effects / Effects.Runtime do not exist")

  # ---- inventory: read from the compiled application, never a hand list ----

  defp effect_modules do
    {:ok, modules} = :application.get_key(:ai_orchestrator, :modules)
    prefix = Atom.to_string(Effect) <> "."

    modules
    |> Enum.filter(fn module ->
      String.starts_with?(Atom.to_string(module), prefix) and Code.ensure_loaded?(module) and
        function_exported?(module, :__struct__, 0)
    end)
    |> Enum.sort()
  end

  # the reducer's constructors, from its AST (Codex's audit probe), not from string presence
  defp reducer_constructors do
    source = File.read!(Path.expand("../../lib/ai_orchestrator/lifecycle/core/reducer.ex", __DIR__))

    {_, names} =
      Macro.prewalk(Code.string_to_quoted!(source), [], fn
        {:%, _, [{:__aliases__, _, [:Effect, name]}, _]} = node, names -> {node, [name | names]}
        node, names -> {node, names}
      end)

    names |> Enum.uniq() |> Enum.map(&Module.concat(Effect, &1)) |> Enum.sort()
  end

  defmodule QueuedAdapter do
    @moduledoc false
    alias AiOrchestrator.Dispatch.LocalPane

    defdelegate snapshot(command, opts), to: LocalPane
    defdelegate observe(command, opts), to: LocalPane

    def deliver(command, opts) do
      {:ok, result} = LocalPane.deliver(command, opts)
      {:ok, Map.put(result, "send_status", "queued")}
    end

    def reconcile(_command, _opts) do
      outcome = if Process.get(:red_polled), do: "delivered", else: "queued"
      Process.put(:red_polled, true)
      {:ok, %{"outcome" => outcome, "delivery_attempt" => 1}}
    end
  end

  @emitted_and_executable ~w(Clock Dispatch SnapshotArtifact ReconcileSend Observe ReadReview RetainPrompt FetchPrompt Timer PrepareGate ReleaseGate AwaitGate ReconcileGate)
  @executable_not_emitted ~w(RunGate)
  @unreachable ~w(Notify)

  describe "boundary and Host guard" do
    test "AiOrchestrator.Effects is a root Boundary that does not depend on Lifecycle, and Lifecycle depends on it" do
      assert File.exists?(@effects_src), "lib/ai_orchestrator/effects.ex must declare the boundary"
      effects = File.read!(@effects_src)
      assert effects =~ ~r/use Boundary,/
      refute effects =~ ~r/AiOrchestrator\.Lifecycle\b/, "Effects must not depend on Lifecycle"
      assert File.read!(@lifecycle_src) =~ ~r/AiOrchestrator\.Effects\b/, "Lifecycle must depend on Effects"
    end

    test "the Host keeps the loop, the whole-suffix commit and the receipt selection, and nothing of execution" do
      host = File.read!(@host_src)
      assert host =~ "defp stamp_and_sink("
      assert host =~ ~s|Enum.find(committed, &(&1["seq"] == seq))|, "receipt selection stays the Host's"
      refute host =~ "Process.put(:host_gate", "no process-dictionary gate state in the Host"
      refute host =~ "defp execute(%Effect.", "no effect execution in the Host"
      refute host =~ "defp observe(%Effect.", "no adapter-result mapping in the Host"
      refute host =~ "defp gate_reason(", "the closed diagnostics mapper moves with the executors"
    end

    # SHOULD-1: a FIXED corpus captured from Core.Diagnostic at 61a16ac (pre-move), not two calls to
    # one future implementation. Core must keep producing it; Contract.Diagnostic must produce it too.
    @diagnostic_corpus [
      {%Effect.Clock{read_index: 3, purpose: "probe"},
       %{
         "correlation" => "3",
         "digest" => :formula,
         "kind" => "clock"
       }, :same},
      {%Observation.Deadline{purpose: "p", deadline_unix: 1, now: nil},
       %{
         "correlation" => nil,
         "digest" => :formula,
         "kind" => "deadline"
       }, :same},
      {%Effect.Dispatch{assignment_id: "as_0001", command: %{}, message_id: "m"},
       %{
         "correlation" => "as_0001",
         "digest" => :formula,
         "kind" => "dispatch"
       }, :same},
      {%Effect.Dispatch{assignment_id: "bad id with spaces", command: %{}, message_id: "m"},
       %{
         "correlation" => nil,
         "digest" => :formula,
         "kind" => "dispatch"
       }, :same},
      {{:error, {:SECRET_ATOM_TAG, "private"}},
       %{
         "digest" => "sha256:eba2246ccb470cf4375ca5cda737b97d0b61de306e1152c2528a1ad5fccbd52a",
         "result_class" => "tuple"
       }, :same},
      {"bytes",
       %{
         "digest" => "sha256:5a800dcb1a29ea6be82c2d609c95dbead1ba9818ceb8e9c021705d92fe1059ca",
         "result_class" => "binary"
       }, :same},
      {42,
       %{
         "digest" => "sha256:3a72816924ea23201f92c5eba28ff78b11b6da4c4294e18e2878565eee75ceeb",
         "result_class" => "integer"
       }, :same},
      {nil,
       %{"digest" => "sha256:fc95eee81c37e4805997ab9ca4c94a50329fdd9d94b8216369d103347753e9fb", "result_class" => "atom"},
       :same},
      {%{"a" => 1}, %{"digest" => :formula, "result_class" => "map"}, :same},
      {[1, 2],
       %{"digest" => "sha256:ae02806c09624bed3db185ffb29d8aeefdc7fbfbf49968c80a5532fc8d492435", "result_class" => "list"},
       :same},
      {{:prompt_unreadable, :enoent},
       %{
         "digest" => "sha256:ba57fd03714be7c5094a2392a36a74e99d914936065b000b51caa7fd7f9e2b51",
         "result_class" => "tuple"
       }, :same},
      {{:prompt_assignment_id_invalid, :byte},
       %{
         "digest" => "sha256:3d3494dfbd982b727c433ad3374ae588929dc3eaa4d7ad10cd3f92afc57957a0",
         "result_class" => "tuple"
       }, %{"class" => "byte", "reason" => "prompt_assignment_id_invalid"}},
      {{:prompt_assignment_id_invalid, :separator},
       %{
         "digest" => "sha256:4cce31ec841854f0b4e521e89b0d8d79a619d8c11b43716731a555382fe1a24c",
         "result_class" => "tuple"
       }, %{"class" => "separator", "reason" => "prompt_assignment_id_invalid"}},
      {{:prompt_assignment_id_invalid, :not_a_class},
       %{
         "digest" => "sha256:254c9dee29bd7f5a1ce7d4db7fbc7e9f47c44f5f77f070bd1c373e09a0c8cb4f",
         "result_class" => "tuple"
       }, %{"class" => nil, "reason" => "prompt_assignment_id_invalid"}},
      {{:not_a_reason, :absolute},
       %{
         "digest" => "sha256:3680cb005b5a75ed8148e2bfcee00dce9871b4f8c7a857cb9d8de767b29cecff",
         "result_class" => "tuple"
       }, :same}
    ]

    defp rejected(described, :same), do: described
    defp rejected(_described, rejected), do: rejected

    # The digest is the documented formula: sha256 over term_to_binary. For atom-keyed maps (every
    # struct) that binary depends on the VM's atom creation order (flatmap keys serialize in atom-table
    # order), so those digests are NOT stable across runs; the corpus pins them by the formula computed
    # in this VM and pins non-map terms as literals. Finding disclosed in the RED doc.
    defp expected(%{"digest" => :formula} = described, term) do
      Map.put(
        described,
        "digest",
        "sha256:" <> Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(term)), case: :lower)
      )
    end

    defp expected(described, _term), do: described

    test "Core.Diagnostic still produces the pinned pre-move corpus (baseline preservation)" do
      for {term, described, rej} <- @diagnostic_corpus do
        described = expected(described, term)
        actual = CoreDiagnostic.describe(term)
        assert actual == described, "describe #{inspect(term)}: got #{inspect(actual)}"
        actual_rej = CoreDiagnostic.describe_rejection(term)
        assert actual_rej == rejected(described, rej), "describe_rejection #{inspect(term)}: got #{inspect(actual_rej)}"
      end
    end

    test "Contract.Diagnostic produces the same pinned corpus and Core.Diagnostic delegates to it (no Effects -> Lifecycle edge)" do
      assert loaded?(contract_diagnostic()), "AiOrchestrator.Contract.Diagnostic does not exist"
      core = File.read!(@core_diag_src)

      assert core =~ ~r/defdelegate describe\(/ and core =~ ~r/defdelegate describe_rejection\(/,
             "Core.Diagnostic must delegate"

      for {term, described, rej} <- @diagnostic_corpus do
        described = expected(described, term)
        assert contract_diagnostic().describe(term) == described, inspect(term)
        assert contract_diagnostic().describe_rejection(term) == rejected(described, rej), inspect(term)
      end
    end
  end

  describe "inventory and classification (execution measured, not tabled)" do
    test "exactly 15 Contract.Effect structs exist; the reducer constructs Timer but never RunGate or Notify" do
      modules = effect_modules()
      assert length(modules) == 15, inspect(modules)
      constructed = reducer_constructors()
      assert Enum.sort(Enum.map(@emitted_and_executable, &Module.concat(Effect, &1))) == constructed
      refute Effect.RunGate in constructed
      refute Effect.Notify in constructed
    end

    test "classify/1 agrees with the reducer AST and with runtime execution for every struct" do
      require_boundary!()

      for module <- effect_modules() do
        short = module |> Module.split() |> List.last()
        classification = effects().classify(module)
        assert match?(%{executable: _, emitted: _}, classification), inspect({module, classification})

        assert classification.emitted == module in reducer_constructors(),
               "#{short}: emitted disagrees with the reducer AST"

        cond do
          short in @emitted_and_executable -> assert classification.executable and classification.emitted, short
          short in @executable_not_emitted -> assert classification.executable and not classification.emitted, short
          short in @unreachable -> assert not classification.executable and not classification.emitted, short
        end
      end
    end

    test "Notify refused by name; Timer answers Deadline against the injected clock; RunGate runs the legacy runner" do
      require_boundary!()
      rt = runtime().new([])
      notify = %Effect.Notify{notification_id: "n_0001", hook_argv: ["true"], payload: %{}}
      assert_raise unreachable(), fn -> effects().execute(notify, rt, opts: []) end

      past = FixedClock.unix_now() - 1
      timer = %Effect.Timer{purpose: "probe", deadline_unix: past}

      assert {%Observation.Deadline{purpose: "probe", deadline_unix: ^past}, ^rt} =
               effects().execute(timer, rt, opts: [clock: FixedClock])

      pass = %{
        "exit_status" => 0,
        "duration_ms" => 1,
        "stdout_hash" => GateDouble.helper() && zero_hash(),
        "stderr_hash" => zero_hash()
      }

      run_gate = %Effect.RunGate{
        gate_run_id: "gr_legacy",
        requested: %{"gate_run_id" => "gr_legacy"},
        repo_root: "/tmp",
        run_dir: "/tmp"
      }

      assert {%Observation.GateFinished{gate_run_id: "gr_legacy"}, ^rt} =
               effects().execute(run_gate, rt, opts: [clock: FixedClock, gate_runner: fn _gate -> {:ok, pass} end])
    end

    # runtime coverage: every executable, emitted effect is EXECUTED through the Host by a real
    # reducer witness, and every observation is admissible for and correlated with its effect
    test "every emitted effect executes through a real reducer witness with an admissible, correlated observation" do
      {:ok, seen} = Agent.start_link(fn -> [] end)
      observer = fn effect, observation -> Agent.update(seen, &[{effect, observation} | &1]) end

      # the scenario corpus: every witness reproduces its parity oracle summary, so a blocked or
      # erroring fixture cannot hide behind another case that collects the same structs
      for {{name, kind, scenario, prior, opts_fun}, index} <- Enum.with_index(H.cases()) do
        H.reset_seams()
        opts = Keyword.put(opts_fun.(), :effect_observer, observer)

        result =
          case kind do
            :run -> Host.run(H.spec(scenario), H.plan(scenario), opts)
            :resume -> Host.resume(H.spec(scenario), H.plan(scenario), prior, opts)
            :cancel -> Host.cancel(prior, opts)
          end

        assert {:ok, %{summary: summary}} = result
        assert summary == oracle_summary(index, name), name
      end

      {_, :run, "gated_run_seed", [], make_opts} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))

      # Timer: a queued delivery polls through Effect.Timer before convergence (Codex audit probe)
      H.reset_seams()

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               Host.run(
                 H.spec("gated_run_seed"),
                 H.plan("gated_run_seed"),
                 Keyword.merge(make_opts.(), dispatch: QueuedAdapter, effect_observer: observer)
               )

      # ReconcileGate: a v2 start with no terminal reconciles dead and reruns at attempt 2
      H.reset_seams()

      assert {:ok, %{summary: %{"status" => "completed"}}} =
               Host.resume(
                 H.spec("gated_run_seed"),
                 H.plan("gated_run_seed"),
                 recovery_prior(),
                 Keyword.put(make_opts.(), :effect_observer, observer)
               )

      # FetchPrompt: a run journaled through its prompt projection, cut before dispatch, resumed with the
      # retained object present (precedent: run_fsm_prompt_retention_test "resume fetches through the store")
      assert {:ok, %{summary: %{"status" => "completed"}}} = resume_through_projection(observer)

      pairs = Agent.get(seen, & &1)
      executed = pairs |> Enum.map(fn {e, _} -> e.__struct__ end) |> Enum.uniq() |> Enum.sort()
      expected = Enum.sort(Enum.map(@emitted_and_executable, &Module.concat(Effect, &1)))

      assert executed == expected,
             "executed #{inspect(executed -- expected)} unexpected; missing #{inspect(expected -- executed)}"

      for {effect, observation} <- pairs do
        assert observation.__struct__ in Effect.admissible_observations(effect),
               inspect({effect.__struct__, observation.__struct__})

        assert correlation(effect) == correlation(observation), inspect(effect.__struct__)
      end
    end
  end

  describe "runtime value and staged boundaries" do
    defmodule SentinelFrames do
      @moduledoc false
      # a frame whose function name is a sentinel: any rendering of the stacktrace exposes it. The call
      # is NOT in tail position (its result is wrapped), so the frame is retained when `fun` raises.
      def red_stack_frame_private_stack_mark(fun) do
        result = fun.()
        {:kept, result}
      end
    end

    defmodule RaisingClock do
      @moduledoc false
      defdelegate wall_ts(), to: FixedClock

      def unix_now do
        if Process.delete(:red_clock_raise),
          do: SentinelFrames.red_stack_frame_private_stack_mark(fn -> raise("clock probe") end)

        FixedClock.unix_now()
      end
    end

    defmodule StagedExecutor do
      @moduledoc false
      # records the exact handle it transfers, so a test can prove Interrupted carries THAT one
      def prepare(fs, request, opts) do
        if Process.get(:red_raise_in) == :prepare_before_transfer, do: raise("prepare probe")
        {:ok, handle} = GateDouble.prepare(fs, request, opts)
        transferred(handle, :prepare)
        {:ok, handle}
      end

      def release(prepared, ack, opts) do
        if Process.get(:red_raise_in) == :release, do: raise("release probe")
        {:ok, handle} = GateDouble.release(prepared, ack, opts)
        transferred(handle, :release)
        {:ok, handle}
      end

      def await(handle, opts) do
        if Process.get(:red_raise_in) == :await, do: raise("await probe")
        GateDouble.await(handle, opts)
      end

      # every attempt is traced in order; the FIRST attempt fails with the configured kind (no
      # assumption about the production iteration order)
      def abandon(handle) do
        attempts = Process.get(:red_attempts, [])
        Process.put(:red_attempts, attempts ++ [handle])

        case {attempts, Process.get(:red_first_abandon_fails)} do
          {[], :error} ->
            raise("abandon probe")

          {[], :throw} ->
            throw(:abandon_probe)

          {[], :exit} ->
            exit(:abandon_probe)

          _ ->
            Process.put(:red_abandoned, Process.get(:red_abandoned, []) ++ [handle])
            :ok
        end
      end

      defp transferred(handle, stage) do
        Process.put(:red_latest, handle)
        if Process.get(:red_clock_raise_after) == stage, do: Process.put(:red_clock_raise, true)
      end

      defdelegate started_data(handle), to: GateDouble
      defdelegate ack(handle, event), to: GateDouble
      defdelegate pass?(outcome), to: GateDouble
      defdelegate evidence(dir, id, attempt), to: GateDouble
      defdelegate reconcile(fs, dir, expected, opts), to: GateDouble
    end

    defp opts,
      do: [
        gate_executor: StagedExecutor,
        gate_helper: GateDouble.helper(),
        run_id: "run_fixture_0001",
        supervisor_instance: "sup_0001",
        clock: RaisingClock
      ]

    defp prepare_effect do
      %Effect.PrepareGate{
        gate_run_id: "gr_0001",
        attempt: 1,
        requested: %{"gate_run_id" => "gr_0001", "command_argv" => ["mix", "test"], "gate_id" => "tests"},
        deadline_unix: 4_102_444_800,
        repo_root: "/tmp/example-repo",
        run_dir: "/tmp/example-run"
      }
    end

    defp release_effect, do: %Effect.ReleaseGate{gate_run_id: "gr_0001", attempt: 1, started_seq: 27}
    defp await_effect, do: %Effect.AwaitGate{gate_run_id: "gr_0001", attempt: 1, deadline_unix: 4_102_444_800}

    defp receipt(started) do
      %{
        "schema_version" => 2,
        "prev_line_sha256" => "sha256:" <> String.duplicate("0", 64),
        "seq" => 27,
        "type" => "gate_started",
        "event_version" => 2,
        "run_id" => "run_fixture_0001",
        "data" => started
      }
    end

    defp interrupted!(fun) do
      fun.()
      flunk("execute must raise")
    rescue
      e -> e
    end

    setup do
      for key <- [
            :red_raise_in,
            :red_clock_raise,
            :red_clock_raise_after,
            :red_abandoned,
            :red_latest,
            :red_attempts,
            :red_first_abandon_fails
          ],
          do: Process.delete(key)

      :ok
    end

    test "native prepare failing before any handle transfer: Interrupted holds the input runtime, nothing to clean" do
      require_boundary!()
      Process.put(:red_raise_in, :prepare_before_transfer)
      rt = runtime().new(opts())
      e = interrupted!(fn -> effects().execute(prepare_effect(), rt, opts: opts()) end)
      assert e.__struct__ == interrupted()
      assert e.runtime == rt
      assert {[], _} = effects().settle(e.runtime)
      assert Process.get(:red_abandoned, []) == []
    end

    test "raise after prepare transferred the handle, before the observation returned: Interrupted holds the NEW prepared handle" do
      require_boundary!()
      Process.put(:red_clock_raise_after, :prepare)
      rt = runtime().new(opts())
      e = interrupted!(fn -> effects().execute(prepare_effect(), rt, opts: opts()) end)
      assert e.__struct__ == interrupted()
      assert %{kind: :error, reason: %RuntimeError{message: "clock probe"}, stacktrace: [_ | _]} = e
      transferred = Process.get(:red_latest)
      assert %{gates: %{{"gr_0001", 1} => %{phase: :prepared, handle: ^transferred}}} = e.runtime
      {cleanup, empty} = effects().settle(e.runtime)

      assert [%{"gate_run_id" => "gr_0001", "attempt" => 1, "settle" => %{"settled" => true, "proof" => "gone"}}] =
               cleanup

      assert empty.gates == %{}
      assert Process.get(:red_abandoned) == [transferred]
    end

    test "a raise inside release (before a new handle exists) leaves the PREPARED handle in Interrupted" do
      require_boundary!()
      rt = runtime().new(opts())
      {%Observation.GatePrepared{started: started}, rt} = effects().execute(prepare_effect(), rt, opts: opts())
      prepared = Process.get(:red_latest)
      Process.put(:red_raise_in, :release)
      e = interrupted!(fn -> effects().execute(release_effect(), rt, opts: opts(), receipt: receipt(started)) end)
      assert %{kind: :error, reason: %RuntimeError{message: "release probe"}} = e
      assert %{gates: %{{"gr_0001", 1} => %{phase: :prepared, handle: ^prepared}}} = e.runtime
      effects().settle(e.runtime)
      assert Process.get(:red_abandoned) == [prepared]
    end

    test "raise after release transferred the running handle, before GateReleased returned: Interrupted holds the RUNNING handle" do
      require_boundary!()
      rt = runtime().new(opts())
      {%Observation.GatePrepared{started: started}, rt} = effects().execute(prepare_effect(), rt, opts: opts())
      Process.put(:red_clock_raise_after, :release)
      e = interrupted!(fn -> effects().execute(release_effect(), rt, opts: opts(), receipt: receipt(started)) end)
      running = Process.get(:red_latest)
      assert running.released == true
      assert %{gates: %{{"gr_0001", 1} => %{phase: :running, handle: ^running}}} = e.runtime
      effects().settle(e.runtime)

      assert Process.get(:red_abandoned) == [running],
             "cleanup sees the latest (running) handle, not the prepared snapshot"
    end

    test "a raise inside await leaves the RUNNING handle in Interrupted" do
      require_boundary!()
      rt = runtime().new(opts())
      {%Observation.GatePrepared{started: started}, rt} = effects().execute(prepare_effect(), rt, opts: opts())

      {%Observation.GateReleased{}, rt} =
        effects().execute(release_effect(), rt, opts: opts(), receipt: receipt(started))

      running = Process.get(:red_latest)
      Process.put(:red_raise_in, :await)
      e = interrupted!(fn -> effects().execute(await_effect(), rt, opts: opts()) end)
      assert %{gates: %{{"gr_0001", 1} => %{phase: :running, handle: ^running}}} = e.runtime
      effects().settle(e.runtime)
      assert Process.get(:red_abandoned) == [running]
    end

    # helper controls (GREEN at baseline, no Effects needed): the sentinel frame is retained in a raised
    # stack, and the clock fault armed by a transfer fires exactly once
    test "control: the sentinel frame is retained in the stack of a raise made through it" do
      stack =
        try do
          SentinelFrames.red_stack_frame_private_stack_mark(fn -> raise("control") end)
        rescue
          RuntimeError -> __STACKTRACE__
        end

      assert Enum.any?(stack, fn {m, f, _, _} -> m == SentinelFrames and f == :red_stack_frame_private_stack_mark end)
    end

    test "control: a clock fault armed by the prepare transfer fires once and is then consumed" do
      Process.put(:red_clock_raise_after, :prepare)

      request = %{
        run_id: "run_fixture_0001",
        gate_run_id: "gr_0001",
        attempt: 1,
        command_argv: ["mix", "test"],
        repo_root: "/tmp",
        run_dir: "/tmp",
        deadline_unix: 4_102_444_800,
        supervisor_instance: "sup_0001"
      }

      {:ok, _handle} = StagedExecutor.prepare(nil, request, [])
      assert Process.get(:red_clock_raise) == true, "armed by the transfer"
      assert_raise RuntimeError, "clock probe", fn -> RaisingClock.unix_now() end
      assert Process.get(:red_clock_raise) == nil, "consumed by the first read"
      assert RaisingClock.unix_now() == FixedClock.unix_now(), "the second read does not raise"
    end

    test "Interrupted and Runtime render payload-free: no handle, reason or stack bytes" do
      require_boundary!()
      Process.put(:red_clock_raise_after, :prepare)
      rt = runtime().new(opts())
      e = interrupted!(fn -> effects().execute(prepare_effect(), rt, opts: opts()) end)
      assert e.__struct__ == interrupted()
      # a second entry under a MALFORMED correlation key (fails the identifier grammar), built directly
      # from the wrapper's runtime: rendering must not reflect the key; no malformed PrepareGate is needed
      [{_key, entry}] = Map.to_list(e.runtime.gates)
      rt2 = %{e.runtime | gates: Map.put(e.runtime.gates, {"gr private_corr_key with spaces", 1}, entry)}
      rendered = inspect(e, limit: :infinity) <> Exception.message(e) <> inspect(rt2, limit: :infinity)
      refute rendered =~ "clock probe", "the original reason's text must not be rendered"
      refute rendered =~ "private_stack_mark", "no stacktrace frame (module/function/file) may be rendered"
      refute rendered =~ "private_corr_key", "a key that fails the identifier grammar must not be reflected"
      refute rendered =~ "1756728000.123456", "handle identity bytes must not be rendered"
      refute rendered =~ "gates/gr_0001", "handle paths must not be rendered"
      refute rendered =~ "started_data", "the handle map must not be rendered"
      assert rendered =~ "gr_0001", "the gate id (an identifier) may be named"

      assert Enum.any?(e.stacktrace, fn {m, f, _, _} ->
               m == SentinelFrames and f == :red_stack_frame_private_stack_mark
             end),
             "the wrapper still CARRIES the original stack"
    end

    for kind <- [:error, :throw, :exit] do
      test "settle continues past an abandon that fails by #{kind}: failing attempt reported unproven, a later handle still attempted" do
        require_boundary!()
        rt = runtime().new(opts())
        {_, rt} = effects().execute(prepare_effect(), rt, opts: opts())

        second_effect = %{
          prepare_effect()
          | gate_run_id: "gr_0002",
            requested: %{"gate_run_id" => "gr_0002", "command_argv" => ["mix", "test"], "gate_id" => "tests"}
        }

        {_, rt} = effects().execute(second_effect, rt, opts: opts())
        Process.put(:red_first_abandon_fails, unquote(kind))

        {cleanup, empty} = effects().settle(rt)
        assert empty.gates == %{}, "tracking relinquished for every handle (not a claim of proven closure)"
        [first_attempt, second_attempt] = Process.get(:red_attempts)
        assert Process.get(:red_abandoned) == [second_attempt], "the attempt after the failed one still ran"
        by_id = Map.new(cleanup, &{&1["gate_run_id"], &1["settle"]})

        assert by_id[first_attempt.request.gate_run_id]["clause"] == "settle_unproven",
               "the failing attempt is reported unproven"

        assert by_id[second_attempt.request.gate_run_id] == %{"settled" => true, "proof" => "gone"}
      end
    end

    test "release_terminal drops a gate's entries only when the WHOLE committed suffix holds that gate's gate_passed/gate_failed" do
      require_boundary!()
      rt = runtime().new(opts())
      {_, rt} = effects().execute(prepare_effect(), rt, opts: opts())
      kept = effects().release_terminal(rt, [%{"type" => "gate_passed", "data" => %{"gate_run_id" => "gr_other"}}])
      assert map_size(kept.gates) == 1
      dropped = effects().release_terminal(rt, [%{"type" => "gate_failed", "data" => %{"gate_run_id" => "gr_0001"}}])
      assert dropped.gates == %{}
    end
  end

  describe "same-invocation close of every trappable exit (R-1 revised)" do
    # the injecting callback records the EXACT kind, reason and stack it escapes with; the test compares
    # what propagates out of the Host to that triple, so no replacement reason or stack can pass
    defmodule Escape do
      @moduledoc false
      def now!(kind, reason) do
        case kind do
          :error -> raise(reason)
          :throw -> throw(reason)
          :exit -> exit(reason)
        end
      catch
        k, r ->
          Process.put(:red_origin, {k, r, __STACKTRACE__})
          :erlang.raise(k, r, __STACKTRACE__)
      end

      def caught(fun) do
        fun.()
        :no_escape
      catch
        k, r -> {k, r, __STACKTRACE__}
      end
    end

    defmodule MatrixExecutor do
      @moduledoc false
      defdelegate prepare(fs, request, opts), to: GateDouble
      defdelegate started_data(handle), to: GateDouble
      defdelegate ack(handle, event), to: GateDouble
      defdelegate release(handle, ack, opts), to: GateDouble
      defdelegate evidence(dir, id, attempt), to: GateDouble
      defdelegate pass?(outcome), to: GateDouble
      defdelegate reconcile(fs, dir, expected, opts), to: GateDouble

      # executor origin: escapes from await, AFTER the running handle is owned
      def await(handle, opts) do
        case Process.get(:red_matrix_origin) do
          {:executor, kind, reason} -> Escape.now!(kind, reason)
          _ -> GateDouble.await(handle, opts)
        end
      end

      def abandon(handle) do
        Process.put(:red_host_attempts, Process.get(:red_host_attempts, []) ++ [handle])

        case Process.get(:red_cleanup_fails) do
          nil -> :ok
          :error -> raise("cleanup probe")
          :throw -> throw(:cleanup_probe)
          :exit -> exit(:cleanup_probe)
        end
      end
    end

    defp host_run(extra) do
      {_, :run, "gated_run_seed", [], make_opts} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
      H.reset_seams()

      Host.run(
        H.spec("gated_run_seed"),
        H.plan("gated_run_seed"),
        make_opts.() |> Keyword.put(:gate_executor, MatrixExecutor) |> Keyword.merge(extra)
      )
    end

    defp matrix_opts({:observer, kind, reason}) do
      [
        effect_observer: fn
          %Effect.ReleaseGate{}, _ -> Escape.now!(kind, reason)
          _, _ -> :ok
        end
      ]
    end

    defp matrix_opts({:executor, kind, reason}) do
      Process.put(:red_matrix_origin, {:executor, kind, reason})
      []
    end

    defp matrix_opts({:sink, kind, reason}) do
      [
        event_sink:
          GateDouble.receipt(fn event ->
            if event["type"] == "gate_passed", do: Escape.now!(kind, reason), else: :ok
          end)
      ]
    end

    setup do
      for key <- [:red_origin, :red_host_attempts, :red_matrix_origin, :red_cleanup_fails], do: Process.delete(key)
      :ok
    end

    # BASELINE at 61a16ac (Codex audit probe): the Host loop rescues :error only; a caught :throw or
    # :exit leaves the running handle in the process dictionary for the NEXT invocation to report.
    # PINNED CHANGE: every trappable exit, from every origin after handle ownership, settles the latest
    # runtime once in the same invocation and propagates the ORIGINAL kind, reason and stacktrace.
    for origin <- [:observer, :executor, :sink],
        {kind, reason} <- [{:error, "red interruption"}, {:throw, :red_interruption}, {:exit, :red_interruption}] do
      test "#{origin} #{kind} after handle ownership: exact origin propagates, running handle abandoned once now, nothing inherited" do
        origin = unquote(origin)
        kind = unquote(kind)
        reason = unquote(Macro.escape(reason))
        extra = matrix_opts({origin, kind, reason})

        propagated = Escape.caught(fn -> host_run(extra) end)
        origin_triple = Process.get(:red_origin) || flunk("the escape never fired")

        assert propagated == origin_triple,
               "exact kind/reason/stack must propagate; got kind #{inspect(elem(propagated, 0))}"

        assert elem(origin_triple, 0) == kind
        attempts = Process.get(:red_host_attempts, [])

        assert match?([%{released: true}], attempts),
               "abandoned exactly once, in this invocation, the RUNNING handle (attempts: #{length(attempts)})"

        Process.delete(:red_host_attempts)
        Process.delete(:red_matrix_origin)
        assert {:ok, result} = host_run([])
        refute Map.has_key?(result, :gate_cleanup), "nothing inherited by the next invocation"
        assert Process.get(:red_host_attempts, []) == [], "no straggler to abandon"
      end
    end

    # cleanup never replaces the primary failure: when the abandon itself fails (any kind) during the
    # close of an observer throw, the observer's ORIGINAL throw is what propagates
    for cleanup_kind <- [:error, :throw, :exit] do
      test "a cleanup failure by #{cleanup_kind} during the close never replaces the original escape; nothing inherited" do
        Process.put(:red_cleanup_fails, unquote(cleanup_kind))
        extra = matrix_opts({:observer, :throw, :red_interruption})
        propagated = Escape.caught(fn -> host_run(extra) end)
        assert propagated == Process.get(:red_origin), "the primary failure propagates, not the cleanup's"
        assert length(Process.get(:red_host_attempts, [])) == 1, "the abandon was attempted once"

        Process.delete(:red_cleanup_fails)
        Process.delete(:red_host_attempts)
        assert {:ok, result} = host_run([])
        refute Map.has_key?(result, :gate_cleanup)
      end
    end
  end

  # ---- helpers ----

  # a v2 start with no terminal: resume reconciles (ReconcileGate) and, dead at attempt 1 before the
  # deadline, reruns (PrepareGate attempt 2)
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
        "execution" => %{"pid" => 4242, "pgid" => 4242, "start" => "1756728000.123456", "claim_hash" => zero_hash()}
      }
    }

    Enum.map(prefix ++ [start], &Jason.encode!/1)
  end

  defp zero_hash, do: "sha256:" <> String.duplicate("ab", 32)

  defp oracle_summary(index, name) do
    base = String.pad_leading(Integer.to_string(index + 1), 2, "0") <> "_" <> String.replace(name, " ", "_")
    "../fixtures/contracts/parity/#{base}.summary.json" |> Path.expand(__DIR__) |> File.read!() |> Jason.decode!()
  end

  # the kill9 scenario run through its writer prompt projection with a real prompt root; the journal is
  # cut before assignment_dispatch_sent (the crash point) and the objects of assignments the cut
  # journal never requested are removed; the resume must FETCH the retained object
  defp resume_through_projection(observer) do
    root = Path.join(System.tmp_dir!(), "effects-red-prompts-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {_, :resume, "kill9_resume", _, make_opts} = Enum.find(H.cases(), &match?({_, :resume, "kill9_resume", _, _}, &1))
    opts = Keyword.put(make_opts.(), :prompt_root, root)

    H.reset_seams()
    assert {:ok, %{events: events}} = Host.run(H.spec("kill9_resume"), H.plan("kill9_resume"), opts)
    prefix = Enum.take_while(events, &(&1["type"] != "assignment_dispatch_sent"))
    assert Enum.any?(prefix, &(&1["type"] == "assignment_prompt_projected")), "the cut journal names a retained object"
    requested = for %{"type" => "assignment_requested", "data" => %{"assignment_id" => id}} <- prefix, do: id

    for path <- Path.wildcard(Path.join([root, "prompts", "*.org"])),
        not Enum.any?(requested, &String.starts_with?(Path.basename(path), &1 <> "-")),
        do: File.rm!(path)

    H.reset_seams()
    lines = Enum.map(prefix, &Jason.encode!/1)
    Host.resume(H.spec("kill9_resume"), H.plan("kill9_resume"), lines, Keyword.put(opts, :effect_observer, observer))
  end

  defp correlation(%{read_index: index}), do: {:read_index, index}
  defp correlation(%{gate_run_id: id}), do: {:gate_run_id, id}
  defp correlation(%{assignment_id: id}), do: {:assignment_id, id}
  defp correlation(%{object: %{assignment_id: id}}), do: {:assignment_id, id}
  defp correlation(%{purpose: purpose, deadline_unix: deadline}), do: {:timer, purpose, deadline}
  defp correlation(_), do: :none
end
