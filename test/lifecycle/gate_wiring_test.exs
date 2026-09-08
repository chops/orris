defmodule AiOrchestrator.Lifecycle.GateWiringTest do
  @moduledoc """
  C4 RED/interface rev 2 (ruling A m_1788587850000; corrections C4-M1..M6 in
  m_1788608760001): the orchestrated gate route runs through `Gate.Execution` with the reviewed
  ordering (prepare + durable claim -> complete gate_started v2 -> real Writer append -> Ack for
  THAT persisted event -> release once -> evidence -> truthful terminal or attention), the Host
  owns the runtime objects, the live deadline and the cleanup, the reducer stays pure, recovery
  is read-only and bounded, unproven settlement of ANY outcome is attention, and legacy v1
  starts are attention. Contract: docs/contracts/gate-execution-wiring.org.

  The pure route is driven through the real Host with a scripted, call-recording
  `gate_executor` double (the C4 seam) whose sink validates every event against the journal
  schema, and a shared ordered trace of sink / effect / ack / release / abandon entries. The
  real route uses the Writer and the native guardian.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation

  # ---- the C4 seam: a scripted executor with Execution's shapes, recording every call ----

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @moduletag :native

  @source Path.expand("../../native/gate_guardian/gate_guardian.c", __DIR__)
  @gate_id "gr_0001"
  @hash "sha256:" <> String.duplicate("ab", 32)
  @far 4_102_444_800
  # the ts every v2_start/2 fixture start carries (2026-09-01T12:00:00Z, the FixedClock base)
  @v2_start_unix 1_788_264_000

  defmodule DoubleExecutor do
    @moduledoc false
    alias AiOrchestrator.Gate.Execution.Ack

    def script(map), do: Process.put(:gate_double_script, map)
    defp get(key, default), do: :gate_double_script |> Process.get(%{}) |> Map.get(key, default)
    def record(entry), do: Process.put(:gate_double_calls, Process.get(:gate_double_calls, []) ++ [entry])
    def calls, do: Process.get(:gate_double_calls, [])
    defp trace(entry), do: if(agent = Process.get(:gate_trace), do: Agent.update(agent, &(&1 ++ [entry])))

    def prepare(_fs, request, _opts) do
      record({:prepare, request.gate_run_id, request.attempt})

      case get(:prepare, :ok) do
        :ok ->
          started = %{
            "gate_run_id" => request.gate_run_id,
            "command_argv" => request.command_argv,
            "stdout_path" => "gates/#{request.gate_run_id}.#{request.attempt}.out",
            "stderr_path" => "gates/#{request.gate_run_id}.#{request.attempt}.err",
            "attempt" => request.attempt,
            "deadline_unix" => request.deadline_unix,
            "execution" => %{
              "pid" => 4242,
              "pgid" => 4242,
              "start" => "1756728000.123456",
              "claim_hash" => "sha256:" <> String.duplicate("ab", 32)
            }
          }

          {:ok,
           %{
             started_data: started,
             identity: %{guardian: 4241, worker: 4242, pgid: 4242, start: "1756728000.123456"},
             request: request
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end

    def started_data(%{started_data: data}), do: data
    def identity(%{identity: identity}), do: identity

    # the REAL checked constructor decides and its Ack is returned unchanged; the double only records
    def ack(prepared, persisted) do
      result = Execution.ack(prepared, persisted)
      with {:ok, %Ack{seq: seq}} <- result, do: trace({:ack, seq})
      result
    end

    def release(prepared, %Ack{}, _opts \\ []) do
      record({:release, prepared.request.gate_run_id, prepared.request.attempt})
      trace({:release, prepared.request.gate_run_id})
      get(:release, {:ok, Map.put(prepared, :released, true)})
    end

    def abandon(handle) do
      record({:abandon, handle.request.gate_run_id, handle.request.attempt})
      trace({:abandon, handle.request.gate_run_id})
      get(:abandon, :ok)
    end

    def expire(handle) do
      record({:expire, handle.request.gate_run_id})
      {:timeout, %{kind: "timeout", settled: true, leftovers: "0", proof: "gone", duration_ms: 1}}
    end

    def await(_running, _opts \\ []), do: get(:await, {:exit, pass_outcome()})

    def evidence(_run_dir, _gate_run_id, _attempt),
      do:
        get(
          :evidence,
          {:ok,
           %{
             "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
             "stderr_hash" => "sha256:" <> String.duplicate("ab", 32)
           }}
        )

    def reconcile(_fs, _run_dir, _expected, _opts \\ []),
      do: get(:reconcile, {:dead, %{"leader" => "gone", "group" => "gone", "members" => 0}})

    def pass?(%{"exit_status" => 0, "settled" => true, "proof" => "gone"}), do: true
    def pass?(_), do: false

    def pass_outcome do
      %{
        "kind" => "exited",
        "exit_status" => 0,
        "settled" => true,
        "leftovers" => "0",
        "proof" => "gone",
        "escaped" => "unknown",
        "stdout_hash" => "sha256:" <> String.duplicate("ab", 32),
        "stderr_hash" => "sha256:" <> String.duplicate("ab", 32),
        "stderr_merged" => false,
        "duration_ms" => 12
      }
    end
  end

  # the owner's clock: the fixed harness clock unless a test moves it (the owner-deadline case)
  defmodule WiringClock do
    @moduledoc false
    def unix_now, do: Process.get(:wiring_now) || FixedClock.unix_now()
    def wall_ts, do: unix_now() |> DateTime.from_unix!() |> DateTime.to_iso8601()
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "gate-wiring-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")

    {"", 0} =
      System.cmd("cc", ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-o", bin, @source], stderr_to_stdout: true)

    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin}
  end

  setup do
    H.reset_seams()
    Process.delete(:gate_double_script)
    Process.delete(:gate_double_calls)
    Process.delete(:wiring_now)
    :ok
  end

  defp scenario_opts do
    {_name, :run, "gated_run_seed", [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    opts_fun.()
  end

  # a sink that VALIDATES each event against the journal schema before stamping it like the writer
  defp validating_sink(trace) do
    fn event ->
      case Event.validate_append(event) do
        {:ok, _} ->
          stamped =
            Map.merge(event, %{"schema_version" => 2, "prev_line_sha256" => "sha256:" <> String.duplicate("0", 64)})

          Agent.update(trace, &(&1 ++ [{:sink, event["type"], event["seq"]}]))
          {:ok, stamped}

        {:error, rejection} ->
          {:error, rejection}
      end
    end
  end

  # runs the scenario through the real Host with the double; returns {result, ordered trace, journaled events}
  # the seed scenario carries artifacts for its two assignments; a gate failure re-dispatches the work
  # item, so later assignment ids are served from the same fixture artifacts (harness support only)
  defp rework_artifact_reader do
    fixture = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    artifacts =
      for %{"type" => "artifact_observed", "data" => data} <- fixture, into: %{}, do: {data["assignment_id"], data}

    [first | _] = Enum.sort(Map.keys(artifacts))

    fn %{"assignment_id" => id} ->
      case artifacts do
        %{^id => data} -> {:ok, data}
        _ -> {:ok, %{artifacts[first] | "assignment_id" => id, "artifact_id" => "art_" <> id}}
      end
    end
  end

  defp drive(kind, opts_extra, prior \\ []) do
    {:ok, trace} = Agent.start_link(fn -> [] end)
    {:ok, journal} = Agent.start_link(fn -> [] end)
    Process.put(:gate_trace, trace)
    sink = Keyword.get_lazy(opts_extra, :event_sink, fn -> validating_sink(trace) end)

    journaling_sink = journaling(sink, journal)

    opts =
      scenario_opts()
      |> Keyword.put(:gate_executor, DoubleExecutor)
      |> Keyword.put(:gate_helper, System.find_executable("true"))
      |> Keyword.put(:gate_runner, fn _gate, _opts ->
        raise "the legacy GateRunner must never run on the orchestrated route"
      end)
      |> Keyword.put(:effect_observer, fn
        # clock reads are traced with their answer: the journal-derived durations are checked against them
        %Effect.Clock{purpose: purpose} = effect, %Observation.Clock{now: %{unix: unix}} ->
          Agent.update(trace, &(&1 ++ [{:effect, effect.__struct__, effect}, {:clock, purpose, unix}]))

        effect, _observation ->
          Agent.update(trace, &(&1 ++ [{:effect, effect.__struct__, effect}]))
      end)
      |> Keyword.merge(Keyword.delete(opts_extra, :event_sink))
      |> Keyword.put(:event_sink, journaling_sink)
      |> Keyword.update!(:dispatch_opts, &Keyword.put(&1, :artifact_reader, rework_artifact_reader()))

    result =
      case kind do
        :run -> Host.run(H.spec("gated_run_seed"), H.plan("gated_run_seed"), opts)
        :resume -> Host.resume(H.spec("gated_run_seed"), H.plan("gated_run_seed"), prior, opts)
      end

    {result, Agent.get(trace, & &1), Agent.get(journal, & &1)}
  end

  # records what the sink persisted (or, for an :ok sink, what it was handed) for the assertions
  defp journaling(nil, _journal), do: nil

  defp journaling(fun, journal) when is_function(fun, 1) do
    fn event ->
      result = fun.(event)
      with {:ok, persisted} <- result, do: Agent.update(journal, &(&1 ++ [persisted]))
      if result == :ok, do: Agent.update(journal, &(&1 ++ [event]))
      result
    end
  end

  defp mutate(map, {:put, path, value}), do: put_in(map, path, value)
  defp mutate(map, {:delete, key}), do: Map.delete(map, key)

  defp types(events), do: Enum.map(events, & &1["type"])
  defp attention(events), do: for(%{"type" => "human_attention_required", "data" => data} <- events, do: data)
  defp attention_reasons(events), do: Enum.map(attention(events), & &1["reason"])
  defp effects(trace, struct), do: for({:effect, ^struct, effect} <- trace, do: effect)
  defp index_of(trace, fun), do: Enum.find_index(trace, fun)

  describe "the orchestrated route" do
    test "prepare -> gate_started v2 verbatim -> a persisted receipt precedes the Ack, which precedes the one release -> await -> gate_passed v1; never RunGate" do
      {result, trace, events} = drive(:run, [])
      assert {:ok, %{summary: %{"status" => "completed"}}} = result
      assert effects(trace, Effect.RunGate) == []

      assert [%Effect.PrepareGate{gate_run_id: @gate_id, attempt: 1, deadline_unix: deadline}] =
               effects(trace, Effect.PrepareGate)

      assert [%Effect.ReleaseGate{gate_run_id: @gate_id, attempt: 1, started_seq: seq}] =
               effects(trace, Effect.ReleaseGate)

      assert [%Effect.AwaitGate{gate_run_id: @gate_id, attempt: 1, deadline_unix: ^deadline}] =
               effects(trace, Effect.AwaitGate)

      assert [
               %{
                 "event_version" => 2,
                 "seq" => ^seq,
                 "prev_line_sha256" => _,
                 "data" => %{"attempt" => 1, "deadline_unix" => ^deadline, "execution" => %{"pid" => 4242}}
               }
             ] =
               Enum.filter(events, &(&1["type"] == "gate_started"))

      # the shared trace proves the order: receipt of THAT seq, then the Ack for that seq, then the
      # single physical release; the observer seam fires AFTER execution, so its ReleaseGate entry
      # follows the release (Host.drive: execute/observe, then notify_observer)
      committed = index_of(trace, &match?({:sink, "gate_started", ^seq}, &1))
      acked = index_of(trace, &match?({:ack, ^seq}, &1))
      released = index_of(trace, &match?({:release, @gate_id}, &1))
      observed = index_of(trace, &match?({:effect, Effect.ReleaseGate, _}, &1))
      assert Enum.all?([committed, acked, released, observed], &is_integer/1), "every ordered entry exists"
      assert committed < acked and acked < released and released < observed
      assert Enum.count(DoubleExecutor.calls(), &match?({:release, _, _}, &1)) == 1, "released exactly once"
      # gate_passed stays version 1 with exactly one stderr evidence field
      assert [%{"event_version" => 1, "data" => %{"exit_status" => 0, "stderr_hash" => @hash} = passed}] =
               Enum.filter(events, &(&1["type"] == "gate_passed"))

      refute Map.has_key?(passed, "stderr_merged")
    end

    for {label, mutate} <- [
          {"seq missing", {:delete, "seq"}},
          {"seq wrong", {:put, ["seq"], 99}},
          {"type wrong", {:put, ["type"], "gate_requested"}},
          {"event_version wrong", {:put, ["event_version"], 1}},
          {"stamp missing", {:delete, "prev_line_sha256"}},
          {"stamp not a string", {:put, ["prev_line_sha256"], 42}},
          {"schema_version wrong", {:put, ["schema_version"], 1}},
          {"data altered", {:put, ["data", "attempt"], 2}},
          {"run_id wrong", {:put, ["run_id"], "run_other"}}
        ] do
      test "a persisted map with #{label} never releases: zero release calls, the handle abandoned, attention" do
        {:ok, trace} = Agent.start_link(fn -> [] end)
        base = validating_sink(trace)

        altered = fn event ->
          with {:ok, persisted} <- base.(event) do
            if event["type"] == "gate_started",
              do: {:ok, mutate(persisted, unquote(Macro.escape(mutate)))},
              else: {:ok, persisted}
          end
        end

        {_result, _trace, events} = drive(:run, event_sink: altered)
        refute Enum.any?(DoubleExecutor.calls(), &match?({:release, _, _}, &1)), unquote(label)
        assert Enum.any?(DoubleExecutor.calls(), &match?({:abandon, _, _}, &1)), unquote(label)
        assert "gate_release_failed" in attention_reasons(events), unquote(label)
      end
    end

    test "the double's ack IS the real constructor: every negative in the table is refused by both, the valid map by neither" do
      prepared = %{
        started_data: %{
          "gate_run_id" => @gate_id,
          "command_argv" => ["mix", "test"],
          "stdout_path" => "gates/#{@gate_id}.1.out",
          "stderr_path" => "gates/#{@gate_id}.1.err",
          "attempt" => 1,
          "deadline_unix" => @far,
          "execution" => %{"pid" => 4242, "pgid" => 4242, "start" => "1756728000.123456", "claim_hash" => @hash}
        },
        identity: %{guardian: 4241, worker: 4242, pgid: 4242, start: "1756728000.123456"},
        request: %{run_id: "run_fixture_0001", gate_run_id: @gate_id, attempt: 1},
        binding: "x"
      }

      valid = %{
        "schema_version" => 2,
        "prev_line_sha256" => "sha256:" <> String.duplicate("0", 64),
        "seq" => 27,
        "type" => "gate_started",
        "event_version" => 2,
        "run_id" => "run_fixture_0001",
        "data" => prepared.started_data
      }

      assert match?({:ok, _}, Execution.ack(prepared, valid))
      assert match?({:ok, _}, DoubleExecutor.ack(prepared, valid))
      # a different POSITIVE seq is a constructor positive: selecting the persisted event for
      # ReleaseGate.started_seq is the Host's responsibility, pinned in the Host-level table
      assert match?({:ok, %{seq: 99}}, Execution.ack(prepared, mutate(valid, {:put, ["seq"], 99})))
      assert match?({:ok, %{seq: 99}}, DoubleExecutor.ack(prepared, mutate(valid, {:put, ["seq"], 99})))

      for mutation <- [
            {:delete, "seq"},
            {:put, ["seq"], 0},
            {:put, ["seq"], -1},
            {:put, ["type"], "gate_requested"},
            {:put, ["event_version"], 1},
            {:delete, "prev_line_sha256"},
            {:put, ["prev_line_sha256"], 42},
            {:put, ["schema_version"], 1},
            {:put, ["data", "attempt"], 2},
            {:put, ["run_id"], "run_other"}
          ] do
        bad = mutate(valid, mutation)
        assert match?({:error, %{clause: "ack_mismatch"}}, Execution.ack(prepared, bad)), inspect(mutation)
        assert match?({:error, %{clause: "ack_mismatch"}}, DoubleExecutor.ack(prepared, bad)), inspect(mutation)
      end
    end

    test "a nil sink yields no persisted receipt: no Ack, no release, attention gate_release_failed" do
      {_result, _trace, _events} = drive(:run, event_sink: nil)

      refute Enum.any?(DoubleExecutor.calls(), &match?({:release, _, _}, &1)),
             "a synthetic non-persisted event never releases a worker"

      assert Enum.any?(DoubleExecutor.calls(), &match?({:abandon, _, _}, &1)), "the prepared handle is abandoned"
    end

    test "an :ok sink (no stamped map) yields no Ack, no release" do
      {_result, _trace, _events} = drive(:run, event_sink: fn _event -> :ok end)
      refute Enum.any?(DoubleExecutor.calls(), &match?({:release, _, _}, &1))
      assert Enum.any?(DoubleExecutor.calls(), &match?({:abandon, _, _}, &1))
    end

    test "a missing helper is attention gate_helper_missing and never a RunGate fallback" do
      DoubleExecutor.script(%{prepare: {:error, %{clause: "helper_missing"}}})
      {_result, trace, events} = drive(:run, [])
      assert "gate_helper_missing" in attention_reasons(events)
      assert effects(trace, Effect.RunGate) == []
      refute "gate_started" in types(events)
    end

    test "a timeout termination journals gate_failed v2 (null exit, closed termination, journal-owned evidence) and the retry policy proceeds" do
      DoubleExecutor.script(%{
        await: {:timeout, %{kind: "timeout", settled: true, leftovers: "0", proof: "gone", duration_ms: 600_000}}
      })

      {_result, _trace, events} = drive(:run, [])
      termination = %{"kind" => "timeout", "settled" => true, "leftovers" => "0", "proof" => "gone"}

      assert [
               %{
                 "event_version" => 2,
                 "data" => %{
                   "exit_status" => nil,
                   "termination" => ^termination,
                   "duration_ms" => 600_000,
                   "stdout_hash" => @hash,
                   "failure_summary" => _
                 }
               }
               | _
             ] =
               Enum.filter(events, &(&1["type"] == "gate_failed"))
    end

    for {label, script} <- [
          {"exit 0",
           %{
             await:
               {:exit,
                Map.merge(DoubleExecutor.pass_outcome(), %{
                  "settled" => false,
                  "leftovers" => "unknown",
                  "proof" => "unknown"
                })}
           }},
          {"non-zero exit",
           %{
             await:
               {:exit,
                Map.merge(DoubleExecutor.pass_outcome(), %{
                  "exit_status" => 3,
                  "settled" => false,
                  "leftovers" => "2",
                  "proof" => "alive"
                })}
           }},
          {"signal",
           %{
             await:
               {:exit,
                DoubleExecutor.pass_outcome()
                |> Map.delete("exit_status")
                |> Map.merge(%{"kind" => "signaled", "signal" => 9, "settled" => false, "proof" => "unknown"})}
           }},
          {"timeout",
           %{
             await: {:timeout, %{kind: "timeout", settled: false, leftovers: "unknown", proof: "unknown", duration_ms: 5}}
           }}
        ] do
      test "an unsettled #{label} is attention gate_settlement_unknown: no pass, no terminal, no retry, no fabricated evidence" do
        DoubleExecutor.script(unquote(Macro.escape(script)))
        {_result, trace, events} = drive(:run, [])
        assert [%{"reason" => "gate_settlement_unknown", "detail" => detail}] = attention(events)
        assert detail["settled"] == false and detail["proof"] in ["alive", "unknown"]
        refute Enum.any?(types(events), &(&1 in ["gate_passed", "gate_failed"]))
        assert length(effects(trace, Effect.PrepareGate)) == 1, "no retry while the group may still run"
      end
    end

    test "a sink rejection after release abandons the running handle through its retained channel and preserves a failed settlement" do
      DoubleExecutor.script(%{abandon: {:error, %{clause: "settle_unproven", record: "malformed DEAD"}}})
      {:ok, trace} = Agent.start_link(fn -> [] end)
      base = validating_sink(trace)

      failing = fn event ->
        if event["type"] == "gate_passed", do: {:error, %{clause: "append_failed", stage: "write"}}, else: base.(event)
      end

      {result, _trace, _events} = drive(:run, event_sink: failing)

      assert {:error,
              %{
                "reason" => "journal_append_failed",
                "gate_cleanup" => [
                  %{"gate_run_id" => @gate_id, "attempt" => 1, "settle" => %{"clause" => "settle_unproven"}}
                ]
              }} = result

      assert Enum.any?(DoubleExecutor.calls(), &match?({:abandon, @gate_id, 1}, &1))
    end

    for {label, abandon, expected} <- [
          {"ok", :ok, %{"settled" => true, "proof" => "gone"}},
          {"guardian_gone", {:error, %{clause: "guardian_gone"}}, %{"clause" => "guardian_gone"}},
          {"settle_unproven", {:error, %{clause: "settle_unproven", record: "malformed DEAD"}},
           %{"clause" => "settle_unproven"}},
          {"abandon_unsettled (the real producer's top-level facts)",
           {:error,
            %{clause: "abandon_unsettled", settled: false, leftovers: "unknown", proof: "unknown", reason: "command"}},
           %{
             "clause" => "abandon_unsettled",
             "settled" => false,
             "leftovers" => "unknown",
             "proof" => "unknown",
             "reason" => "command"
           }},
          {"an unexpected term", {:error, "not a map"}, %{"clause" => "settle_unproven"}},
          {"an unlisted clause", {:error, %{clause: "PRIVATE", settled: false}},
           %{"clause" => "settle_unproven", "settled" => false}}
        ] do
      test "cleanup shape #{label}: admitted on the release-attention path and returned on the sink-error path" do
        # release-attention path: the release is refused, the prepared handle is abandoned, the
        # abandonment's settlement is the attention detail's settle (journaled, so schema-admitted)
        DoubleExecutor.script(%{release: {:error, %{clause: "ack_mismatch"}}, abandon: unquote(Macro.escape(abandon))})
        {result, _trace, events} = drive(:run, [])
        assert {:ok, %{summary: %{"status" => "blocked"}}} = result
        assert [%{"reason" => "gate_release_failed", "detail" => %{"settle" => settle}}] = attention(events)
        assert settle == unquote(Macro.escape(expected))

        # sink-error path: the running handle is abandoned at the error exit and reported in gate_cleanup
        H.reset_seams()
        Process.delete(:gate_double_calls)
        DoubleExecutor.script(%{abandon: unquote(Macro.escape(abandon))})
        {:ok, trace} = Agent.start_link(fn -> [] end)
        base = validating_sink(trace)

        failing = fn e ->
          if e["type"] == "gate_passed", do: {:error, %{clause: "append_failed", stage: "write"}}, else: base.(e)
        end

        {result, _trace, _events} = drive(:run, event_sink: failing)
        assert {:error, %{"gate_cleanup" => [%{"gate_run_id" => @gate_id, "attempt" => 1, "settle" => cleanup}]}} = result
        assert cleanup == unquote(Macro.escape(expected))
      end
    end

    test "a sink that raises after release still abandons every registered handle, then re-raises" do
      {:ok, trace} = Agent.start_link(fn -> [] end)
      base = validating_sink(trace)
      raising = fn event -> if event["type"] == "gate_passed", do: raise("sink exploded"), else: base.(event) end
      assert_raise RuntimeError, "sink exploded", fn -> drive(:run, event_sink: raising) end
      assert Enum.any?(DoubleExecutor.calls(), &match?({:abandon, @gate_id, 1}, &1))
    end

    test "diagnostics are a value domain: off-domain values are refused or normalized, durability facts retained, a non-map return is invalid_return" do
      raw = %{
        clause: "claim_unpublished\nraw bytes",
        stage: "write",
        class: "eio garbage",
        cleanup: "removed_unsynced",
        temp: "removed_unsynced",
        final: "removed",
        residue:
          [
            "gates/gr_0001.claim.1.tok.tmp",
            "/etc/passwd",
            "gates/../../x",
            "gates/.",
            "gates/..",
            "gates/ok.tmp\n",
            String.duplicate("gates/a", 100),
            42
          ] ++
            Enum.map(1..40, &"gates/flood_#{&1}.tmp"),
        settle: %{settled: true, leftovers: "0", proof: "gone", reason: "command", extra: {:raw, :term}},
        outputs: "removed",
        field: <<255>>
      }

      DoubleExecutor.script(%{prepare: {:error, raw}})
      {_result, _trace, events} = drive(:run, [])
      assert [%{"reason" => "gate_prepare_failed", "detail" => detail}] = attention(events)

      assert detail == %{
               "clause" => "unknown",
               "stage" => "write",
               "class" => "other",
               "cleanup" => "removed_unsynced",
               "temp" => "removed_unsynced",
               "final" => "removed",
               # dot names, a trailing newline, absolute and traversal entries are dropped; the list is bounded to 8
               "residue" => ["gates/gr_0001.claim.1.tok.tmp"] ++ Enum.map(1..7, &"gates/flood_#{&1}.tmp"),
               "settle" => %{"settled" => true, "leftovers" => "0", "proof" => "gone", "reason" => "command"},
               "outputs" => "removed"
             }

      H.reset_seams()
      Process.delete(:gate_double_calls)
      DoubleExecutor.script(%{prepare: {:error, "not a map"}})
      {_result, _trace, events} = drive(:run, [])
      assert [%{"reason" => "gate_prepare_failed", "detail" => %{"clause" => "invalid_return"}}] = attention(events)
    end

    for {label, raw, expected} <- [
          {"prior temp residue with the final absent",
           %{
             clause: "claim_unpublished",
             stage: "rm_temp",
             class: "eacces",
             cleanup: "cleanup_required",
             residue: ["gates/gr_0001.claim.1.tok.tmp"],
             final: "absent",
             settle: %{settled: true, leftovers: "0", proof: "gone", reason: "command"},
             outputs: "removed"
           },
           %{
             "clause" => "claim_unpublished",
             "stage" => "rm_temp",
             "class" => "eacces",
             "cleanup" => "cleanup_required",
             "residue" => ["gates/gr_0001.claim.1.tok.tmp"],
             "final" => "absent",
             "settle" => %{"settled" => true, "leftovers" => "0", "proof" => "gone", "reason" => "command"},
             "outputs" => "removed"
           }},
          {"a foreign final with our temp absent_unsynced",
           %{
             clause: "claim_conflict",
             stage: "link",
             class: "eexist",
             cleanup: "foreign_final_untouched",
             temp: "absent_unsynced",
             settle: %{settled: true, leftovers: "0", proof: "gone", reason: "command"},
             outputs: "removed"
           },
           %{
             "clause" => "claim_conflict",
             "stage" => "link",
             "class" => "eexist",
             "cleanup" => "foreign_final_untouched",
             "temp" => "absent_unsynced",
             "settle" => %{"settled" => true, "leftovers" => "0", "proof" => "gone", "reason" => "command"},
             "outputs" => "removed"
           }},
          {"a temp already absent but its fsync failed",
           %{
             clause: "claim_unpublished",
             stage: "chmod",
             class: "eperm",
             cleanup: "absent_unsynced",
             settle: %{settled: true, leftovers: "0", proof: "gone", reason: "command"},
             outputs: "removed"
           },
           %{
             "clause" => "claim_unpublished",
             "stage" => "chmod",
             "class" => "other",
             "cleanup" => "absent_unsynced",
             "settle" => %{"settled" => true, "leftovers" => "0", "proof" => "gone", "reason" => "command"},
             "outputs" => "removed"
           }}
        ] do
      test "the C3 shape #{label} maps through the Host with every absence/durability fact retained" do
        DoubleExecutor.script(%{prepare: {:error, unquote(Macro.escape(raw))}})
        {_result, _trace, events} = drive(:run, [])
        assert [%{"reason" => "gate_prepare_failed", "detail" => detail}] = attention(events)
        assert detail == unquote(Macro.escape(expected))
      end
    end

    test "diagnostics carry only the closed mapping: raw terms, records and off-run paths never reach the journal" do
      raw = %{
        clause: "claim_unpublished",
        stage: "write",
        class: "eio",
        cleanup: "cleanup_required",
        residue: ["gates/gr_0001.claim.1.tok.tmp"],
        settle: %{settled: true, leftovers: "0", proof: "gone", reason: "command"},
        outputs: "removed",
        detail: {:raw, :term},
        record: "verbatim bytes",
        path: "/etc/passwd"
      }

      DoubleExecutor.script(%{prepare: {:error, raw}})
      {_result, _trace, events} = drive(:run, [])
      assert [%{"reason" => "gate_prepare_failed", "detail" => detail}] = attention(events)

      assert detail == %{
               "clause" => "claim_unpublished",
               "stage" => "write",
               "class" => "eio",
               "cleanup" => "cleanup_required",
               "residue" => ["gates/gr_0001.claim.1.tok.tmp"],
               "settle" => %{"settled" => true, "leftovers" => "0", "proof" => "gone", "reason" => "command"},
               "outputs" => "removed"
             }
    end
  end

  # ---- resume and recovery, pure ----

  # the archived prefix (maps) up to the gate start, then the events under test re-sequenced and
  # validated in read mode; archived bytes are never rewritten
  defp prior_with(extra_events) do
    prior = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    {prefix, _} = Enum.split_while(prior, &(&1["type"] != "gate_started"))

    extras =
      extra_events
      |> Enum.with_index(length(prefix) + 1)
      |> Enum.map(fn {e, seq} ->
        Map.merge(e, %{"seq" => seq, "event_id" => "ev_#{String.pad_leading(Integer.to_string(seq), 4, "0")}"})
      end)

    events = prefix ++ extras

    for e <- events,
        do:
          assert(match?({:ok, _}, Event.validate_read(e)), "fixture event #{e["seq"]} #{e["type"]} must be a valid line")

    Enum.map(events, &Jason.encode!/1)
  end

  defp v2_start(attempt, deadline) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 2,
      "type" => "gate_started",
      "ts" => "2026-09-01T12:00:00Z",
      "run_id" => "run_scenario_0001",
      "actor" => "run_supervisor",
      "data" => %{
        "gate_run_id" => @gate_id,
        "command_argv" => ["mix", "test"],
        "stdout_path" => "gates/#{@gate_id}.#{attempt}.out",
        "stderr_path" => "gates/#{@gate_id}.#{attempt}.err",
        "attempt" => attempt,
        "deadline_unix" => deadline,
        "execution" => %{"pid" => 4242, "pgid" => 4242, "start" => "1756728000.123456", "claim_hash" => @hash}
      }
    }
  end

  describe "resume and recovery" do
    test "a v2 start with no terminal reconciles with the complete journaled start; :dead before the deadline reruns at attempt 2 with the same gate and deadline" do
      {result, trace, events} = drive(:resume, [], prior_with([v2_start(1, @far)]))
      assert {:ok, _} = result

      assert [%Effect.ReconcileGate{gate_run_id: @gate_id, attempt: 1, expected: expected}] =
               effects(trace, Effect.ReconcileGate)

      assert %{
               "run_id" => "run_scenario_0001",
               "journaled" => true,
               "deadline_unix" => @far,
               "command_argv" => ["mix", "test"],
               "execution" => %{"pid" => 4242, "claim_hash" => @hash}
             } = expected

      assert [%Effect.PrepareGate{attempt: 2, deadline_unix: @far, gate_run_id: @gate_id}] =
               effects(trace, Effect.PrepareGate)

      assert [%{"data" => %{"attempt" => 2, "deadline_unix" => @far}}] =
               Enum.filter(events, &(&1["type"] == "gate_started"))
    end

    test ":dead at attempt 2 is the lawful recovery terminal with journal-owned evidence and a journal-derived duration; no rerun" do
      {_result, trace, events} = drive(:resume, [], prior_with([v2_start(2, @far)]))

      assert [
               %{
                 "event_version" => 2,
                 "data" => %{
                   "exit_status" => nil,
                   "termination" => %{"kind" => "recovery", "proof" => "gone"},
                   "stdout_hash" => @hash,
                   "duration_ms" => duration
                 }
               }
             ] = Enum.filter(events, &(&1["type"] == "gate_failed"))

      # journal-derived: exactly the gate_recovery clock read minus the journaled gate_started ts, in ms
      assert [{:clock, "gate_recovery", read}] = for({:clock, "gate_recovery", _} = entry <- trace, do: entry)
      assert duration == (read - @v2_start_unix) * 1000
      refute Enum.any?(effects(trace, Effect.PrepareGate), &(&1.gate_run_id == @gate_id)), "no rerun of this gate"
    end

    test ":dead after the original deadline at attempt 1 is the recovery terminal, not a rerun" do
      {_result, trace, events} = drive(:resume, [], prior_with([v2_start(1, FixedClock.unix_now() - 1)]))

      assert [%{"data" => %{"termination" => %{"kind" => "recovery"}}}] =
               Enum.filter(events, &(&1["type"] == "gate_failed"))

      refute Enum.any?(effects(trace, Effect.PrepareGate), &(&1.gate_run_id == @gate_id)), "no rerun of this gate"
    end

    test ":dead whose reconcile clock read precedes the journaled start is attention gate_recovery_clock_skew with closed facts; no terminal, no invented duration" do
      # the fixture start is stamped 12:00:00; a start after the resumed clock is skew across owners
      late = 2 |> v2_start(@far) |> Map.put("ts", "2026-09-01T12:30:00Z")
      {_result, _trace, events} = drive(:resume, [], prior_with([late]))

      assert [%{"reason" => "gate_recovery_clock_skew", "detail" => detail}] = attention(events)
      assert detail["clause"] == "clock_skew"
      assert detail["recorded_start_unix"] == @v2_start_unix + 1800
      assert is_integer(detail["observed_now_unix"]) and detail["observed_now_unix"] < detail["recorded_start_unix"]
      refute "gate_failed" in types(events)
    end

    test ":dead with a non-hex hash is attention gate_evidence_unreadable at the Host boundary; no terminal" do
      DoubleExecutor.script(%{
        evidence: {:ok, %{"stdout_hash" => "sha256:" <> String.duplicate("z", 64), "stderr_hash" => @hash}}
      })

      {_result, trace, events} = drive(:resume, [], prior_with([v2_start(2, @far)]))

      assert [
               %{
                 "reason" => "gate_evidence_unreadable",
                 "detail" => %{"clause" => "evidence_incomplete", "field" => "stdout_hash"}
               }
             ] =
               attention(events)

      refute "gate_failed" in types(events)
      assert effects(trace, Effect.PrepareGate) == []
    end

    test "an unknown verdict keeps the genuine members=unknown fact in the closed detail" do
      DoubleExecutor.script(%{
        reconcile: {:unknown, %{"leader" => "alive", "group" => "unknown", "members" => "unknown"}}
      })

      {_result, _trace, events} = drive(:resume, [], prior_with([v2_start(1, @far)]))
      assert [%{"reason" => "gate_recovery_unknown", "detail" => detail}] = attention(events)
      assert detail["members"] == "unknown" and detail["leader"] == "alive" and detail["group"] == "unknown"
    end

    test ":dead with incomplete evidence (one hash) is attention gate_evidence_unreadable, never a terminal with a zero hash" do
      DoubleExecutor.script(%{evidence: {:ok, %{"stdout_hash" => @hash}}})
      {_result, _trace, events} = drive(:resume, [], prior_with([v2_start(2, @far)]))

      assert [%{"reason" => "gate_evidence_unreadable", "detail" => %{"clause" => "evidence_incomplete"}}] =
               attention(events)

      refute "gate_failed" in types(events)
    end

    test ":dead with unreadable evidence is attention gate_evidence_unreadable, never a terminal with invented hashes" do
      DoubleExecutor.script(%{evidence: {:error, %{clause: "output_unreadable", which: "out", class: "enoent"}}})
      {_result, _trace, events} = drive(:resume, [], prior_with([v2_start(2, @far)]))
      assert "gate_evidence_unreadable" in attention_reasons(events)
      refute "gate_failed" in types(events)
    end

    for {label, verdict, reason} <- [
          {"unknown", {:unknown, %{"leader" => "alive", "group" => "alive", "members" => 1}}, "gate_recovery_unknown"},
          {"no_claim", :no_claim, "gate_start_unresolved"},
          {"orphan_claim", {:orphan_claim, %{"residue" => ["gates/gr_0001.claim.1.tok.tmp"]}}, "gate_claim_orphaned"}
        ] do
      test "a #{label} verdict is attention #{reason}, never a rerun" do
        DoubleExecutor.script(%{reconcile: unquote(Macro.escape(verdict))})
        {_result, trace, events} = drive(:resume, [], prior_with([v2_start(1, @far)]))
        assert unquote(reason) in attention_reasons(events)
        assert effects(trace, Effect.PrepareGate) == [] and effects(trace, Effect.RunGate) == []
      end
    end

    test "a legacy v1 gate_started with no terminal is attention gate_start_unresolved: no RunGate, no PrepareGate, no ReconcileGate" do
      prior = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
      {prefix, [v1_start | _]} = Enum.split_while(prior, &(&1["type"] != "gate_started"))
      assert v1_start["event_version"] == 1
      {_result, trace, events} = drive(:resume, [], Enum.map(prefix ++ [v1_start], &Jason.encode!/1))
      assert "gate_start_unresolved" in attention_reasons(events)
      assert Enum.all?([Effect.RunGate, Effect.PrepareGate, Effect.ReconcileGate], &(effects(trace, &1) == []))
    end
  end

  # the native guardian enters the spec's repo_root before exec, so a real-Host spec must own a real one
  defp real_gates(spec, argv, run_dir),
    do:
      spec
      |> Map.update!("gates", fn gates -> Map.new(gates, fn {id, _} -> {id, argv} end) end)
      |> Map.put("repo_root", run_dir)

  defp host_opts(run_dir, helper, extra) do
    scenario_opts()
    |> Keyword.put(:run_dir, run_dir)
    |> Keyword.put(:repo_root, run_dir)
    |> Keyword.put(:gate_executor, Execution)
    |> Keyword.put(:gate_helper, helper)
    |> Keyword.put(:clock, WiringClock)
    |> Keyword.put(:gate_runner, fn _gate, _opts ->
      raise "the legacy GateRunner must never run on the orchestrated route"
    end)
    |> Keyword.update!(:dispatch_opts, &Keyword.put(&1, :artifact_reader, rework_artifact_reader()))
    |> Keyword.merge(extra)
  end

  # the test-owned Writer is ALWAYS closed, whatever the body does (a failing assertion or a
  # raising sink must not strand its lock)
  defp with_writer(run_dir, overrides, fun) do
    lock = [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
    {:ok, writer, _} = Writer.open(run_dir, Keyword.merge([create: true, clock: FixedClock, lock: lock], overrides))

    try do
      fun.(&Writer.append(writer, &1))
    after
      _ = Writer.close(writer)
    end
  end

  # bounded native cleanup registered BEFORE assertions: absence within a bound, no signal
  defp track(identity) do
    on_exit(fn ->
      wait_until(fn -> dead?(identity) end, 15_000) || raise("owned group #{identity.pgid} still present at teardown")
    end)

    :ok
  end

  defp helper_of(:missing, run_dir), do: Path.join(run_dir, "no-such-helper")
  defp helper_of(nil, _run_dir), do: nil

  defp helper_of(:not_executable, run_dir) do
    path = Path.join(run_dir, "not-executable")
    File.write!(path, "#!/bin/sh\nexit 0\n")
    File.chmod!(path, 0o644)
    path
  end

  defp journal(run_dir),
    do:
      run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp tmp_run_dir do
    dir = Path.join(System.tmp_dir!(), "gate-wiring-run-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end

  # through the CLI the gate executes inside the owned Run.Server, not the test process: the READY identity is
  # forwarded to the test, which registers the bounded cleanup itself once the invocation returned (on_exit is
  # only callable from the test process)
  defp forwarding_barrier(test) do
    fn
      :after_ready, identity ->
        send(test, {:track_identity, identity})
        :ok

      _name, _info ->
        :ok
    end
  end

  # every real-owner case registers bounded native cleanup the moment READY identity arrives, in
  # the test process, before any later assertion; composed with a test's own barrier
  defp tracking_barrier(inner \\ fn _, _ -> :ok end) do
    fn
      :after_ready, identity ->
        :ok = track(identity)
        inner.(:after_ready, identity)

      name, info ->
        inner.(name, info)
    end
  end

  # ---- the Host transaction, real: Writer + native guardian ----

  # OS oracles (as in the execution suite): leader absence by identity AND an empty group
  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} -> String.split(out, "\n", trim: true)
      {"", 1} -> []
      {out, status} -> flunk("ps proved nothing (#{status}): #{inspect(out)}")
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do:
      signal_zero(Integer.to_string(pid)) == :gone and signal_zero("-" <> Integer.to_string(pgid)) == :gone and
        members(pgid) == []

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    fun.()
  end

  describe "host transaction" do
    test "a real run: prepare, commit gate_started v2 through the Writer, Ack, release, evidence, gate_passed v1; the legacy runner never runs",
         %{helper: helper} do
      run_dir = tmp_run_dir()
      spec = real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "echo gate-out; exit 0"], run_dir)

      result =
        with_writer(run_dir, [], fn sink ->
          Host.run(
            spec,
            H.plan("gated_run_seed"),
            host_opts(run_dir, helper, event_sink: sink, gate_opts: [barrier: tracking_barrier()])
          )
        end)

      assert {:ok, %{summary: %{"status" => "completed"}}} = result
      events = journal(run_dir)

      assert [
               %{
                 "event_version" => 2,
                 "data" => %{"attempt" => 1, "execution" => %{"pid" => pid, "claim_hash" => "sha256:" <> _}}
               }
             ] = Enum.filter(events, &(&1["type"] == "gate_started"))

      assert is_integer(pid)

      assert [%{"event_version" => 1, "data" => %{"exit_status" => 0, "stderr_hash" => "sha256:" <> _} = passed}] =
               Enum.filter(events, &(&1["type"] == "gate_passed"))

      refute Map.has_key?(passed, "stderr_merged")
      assert File.exists?(Path.join(run_dir, "gates/#{@gate_id}.claim.1"))
      assert File.read!(Path.join(run_dir, "gates/#{@gate_id}.1.out")) == "gate-out\n"
    end

    test "a Writer receipt failure on the gate_started append: the fault provably fires, the exact receipt rejection surfaces, no GO, the identified worker is settled",
         %{helper: helper} do
      run_dir = tmp_run_dir()

      spec =
        real_gates(
          H.spec("gated_run_seed"),
          ["/bin/sh", "-c", "echo ran > #{Path.join(run_dir, "ran")}; exit 0"],
          run_dir
        )

      fault_fs = FaultFs.new()
      armed = :counters.new(1, [])
      fired = :counters.new(1, [])

      FaultFs.inject(
        fault_fs,
        :rename,
        fn [_from, to] ->
          hit = to == "events.head" and :counters.get(armed, 1) == 1
          if hit, do: :counters.add(fired, 1, 1)
          hit
        end,
        {:error, :eio}
      )

      parent = self()

      barrier =
        tracking_barrier(fn
          :after_ready, identity -> send(parent, {:identity, identity})
          _, _ -> :ok
        end)

      result =
        with_writer(run_dir, [fs: fault_fs], fn sink ->
          arming_sink = fn event ->
            if event["type"] == "gate_started", do: :counters.put(armed, 1, 1)
            sink.(event)
          end

          Host.run(
            spec,
            H.plan("gated_run_seed"),
            host_opts(run_dir, helper, event_sink: arming_sink, gate_opts: [barrier: barrier])
          )
        end)

      assert_received {:identity, identity}

      assert {:error,
              %{
                "reason" => "journal_append_failed",
                "stage" => "receipt",
                "gate_cleanup" => [%{"gate_run_id" => @gate_id, "settle" => %{"settled" => true, "proof" => "gone"}}]
              }} = result

      assert :counters.get(fired, 1) >= 1,
             "the ARMED receipt fault fired at the gate_started append, not at an earlier receipt"

      refute File.exists?(Path.join(run_dir, "ran")), "never released"
      assert wait_until(fn -> dead?(identity) end, 5_000), "the identified worker is settled"
      refute Enum.any?(journal(run_dir), &(&1["type"] in ["gate_passed", "gate_failed"]))
    end

    for {label, kind} <- [
          {"a missing helper binary", :missing},
          {"an existing but non-executable helper", :not_executable},
          {"a nil helper", nil}
        ] do
      test "#{label} is journaled attention gate_helper_missing; the legacy runner never runs" do
        run_dir = tmp_run_dir()
        spec = real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "exit 0"], run_dir)
        helper = helper_of(unquote(kind), run_dir)

        result =
          with_writer(run_dir, [], fn sink ->
            Host.run(
              spec,
              H.plan("gated_run_seed"),
              host_opts(run_dir, helper, event_sink: sink, gate_opts: [barrier: tracking_barrier()])
            )
          end)

        assert {:ok, %{summary: %{"status" => status}}} = result
        assert status != "completed"
        assert "gate_helper_missing" in attention_reasons(journal(run_dir))
      end
    end

    test "the CLI routes the resolved helper (environment) to the Host: a run with AI_ORCHESTRATOR_GATE_GUARDIAN set journals gate_started v2",
         %{helper: helper} do
      run_dir = tmp_run_dir()
      spec = real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "exit 0"], run_dir)
      File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(spec))
      File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(H.plan("gated_run_seed")))
      env = %{"AI_ORCHESTRATOR_GATE_GUARDIAN" => helper}
      # the real executor, and no helper in the options: the environment is the only helper source
      cli_opts =
        scenario_opts()
        |> Keyword.drop([:gate_helper, :gate_executor])
        |> Keyword.put(:env, env)
        |> Keyword.put(:gate_opts, barrier: forwarding_barrier(self()))

      result = AiOrchestrator.CLI.run(["run", run_dir], cli_opts)
      assert_receive {:track_identity, identity}, 5_000
      :ok = track(identity)
      events = journal(run_dir)
      assert Enum.any?(events, &(&1["type"] == "gate_started" and &1["event_version"] == 2)), inspect(result)
      refute "gate_helper_missing" in attention_reasons(events)
    end

    test "the CLI accepts the literal --gate-guardian flag: the flag names the helper and outranks the environment",
         %{helper: helper} do
      run_dir = tmp_run_dir()
      spec = real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "exit 0"], run_dir)
      File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(spec))
      File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(H.plan("gated_run_seed")))
      # the environment names a helper that is NOT executable; only the flag names the real one
      env = %{"AI_ORCHESTRATOR_GATE_GUARDIAN" => helper_of(:not_executable, run_dir)}

      cli_opts =
        scenario_opts()
        |> Keyword.drop([:gate_helper, :gate_executor])
        |> Keyword.put(:env, env)
        |> Keyword.put(:gate_opts, barrier: forwarding_barrier(self()))

      result = AiOrchestrator.CLI.run(["run", "--gate-guardian", helper, run_dir], cli_opts)
      assert_receive {:track_identity, identity}, 5_000
      :ok = track(identity)
      events = journal(run_dir)
      assert %{status: 0} = result
      assert Enum.any?(events, &(&1["type"] == "gate_started" and &1["event_version"] == 2)), inspect(result)
      refute "gate_helper_missing" in attention_reasons(events)

      # the flag is a flag: a bare word there is a usage error, not a run directory
      assert %{status: 64} = AiOrchestrator.CLI.run(["run", "--gate-guardian"], cli_opts)
    end

    test "the owner's deadline expires the running gate through the original channel before the guardian backstop", %{
      helper: helper
    } do
      run_dir = tmp_run_dir()
      # the first gate blocks as a group (a background and a foreground sleep). The lawful rework
      # gate the retry policy opens afterwards exits at once: it sees the first gate's claim file
      # next to its own (claims are published before GO, so the count is decided, never raced)
      spec =
        real_gates(
          H.spec("gated_run_seed"),
          ["/bin/sh", "-c", ~s{[ "$(ls gates/*.claim.* | wc -l)" -gt 1 ] && exit 0; sleep 30 & sleep 30}],
          run_dir
        )

      # once the first gate is released, the owner's clock jumps past its deadline: the Host's await
      # must expire it via TERM on the retained channel (the backstop would only fire 602 s later);
      # the jump is to a fixed instant, so the rework gate's later deadline is not expired by it
      parent = self()

      observer = fn
        %Effect.ReleaseGate{}, _ -> Process.put(:wiring_now, FixedClock.unix_now() + 10_000)
        _, _ -> :ok
      end

      # the Host reports the termination it obtained from Execution through this seam
      barrier =
        tracking_barrier(fn
          :after_ready, identity -> send(parent, {:identity, identity})
          :terminated, termination -> send(parent, {:termination, termination})
          _, _ -> :ok
        end)

      t0 = System.monotonic_time(:millisecond)

      result =
        with_writer(run_dir, [], fn sink ->
          Host.run(
            spec,
            H.plan("gated_run_seed"),
            host_opts(run_dir, helper, event_sink: sink, effect_observer: observer, gate_opts: [barrier: barrier])
          )
        end)

      elapsed = System.monotonic_time(:millisecond) - t0
      assert {:ok, _} = result
      assert elapsed < 20_000, "expired by the owner, not by a 600 s backstop"

      assert [
               %{
                 "event_version" => 2,
                 "data" => %{
                   "exit_status" => nil,
                   "termination" => %{"kind" => "timeout", "proof" => "gone", "settled" => true},
                   "duration_ms" => d
                 }
               }
               | _
             ] = Enum.filter(journal(run_dir), &(&1["type"] == "gate_failed"))

      assert is_integer(d) and d < 20_000
      # the original channel expired it: the guardian's record was DEAD reason=command, never the
      # backstop, and the identified group is gone
      assert_received {:identity, identity}
      assert dead?(identity)
      assert_received {:termination, %{kind: "timeout"} = termination}

      refute Map.get(termination, :backstop, false),
             "expired by the owner through the original channel, not the native backstop"
    end

    test "cold crash window after the committed start, before GO: the barrier is recorded, the owner dies, settlement is observed, then resume reconciles dead and completes at attempt 2 under the original deadline",
         %{helper: helper} do
      run_dir = tmp_run_dir()
      spec = real_gates(H.spec("gated_run_seed"), ["/bin/sh", "-c", "exit 0"], run_dir)
      parent = self()

      barrier = fn
        :after_ready, identity ->
          send(parent, {:identity, identity})

        :after_ack, _ ->
          send(parent, :barrier_reached)
          Process.exit(self(), :kill)

        _, _ ->
          :ok
      end

      # the harness registers on_exit hooks, so the options are built here, in the test process
      owner_opts = host_opts(run_dir, helper, gate_opts: [barrier: barrier])

      {owner, ref} =
        spawn_monitor(fn ->
          with_writer(run_dir, [], fn sink ->
            Host.run(spec, H.plan("gated_run_seed"), Keyword.put(owner_opts, :event_sink, sink))
          end)
        end)

      # at each checkpoint an unexpected DOWN is consumed immediately, never after a 15 s wait
      identity =
        receive do
          {:identity, identity} -> identity
          {:DOWN, ^ref, :process, ^owner, reason} -> flunk("the owner died before READY: #{inspect(reason)}")
        after
          15_000 -> flunk("no READY identity")
        end

      :ok = track(identity)

      receive do
        :barrier_reached -> :ok
        {:DOWN, ^ref, :process, ^owner, reason} -> flunk("the owner died before the barrier: #{inspect(reason)}")
      after
        15_000 -> flunk("the intended crash window was not reached")
      end

      receive do
        {:DOWN, ^ref, :process, ^owner, :killed} -> :ok
        {:DOWN, ^ref, :process, ^owner, other} -> flunk("the owner died for another reason: #{inspect(other)}")
      after
        15_000 -> flunk("the owner did not die")
      end

      assert wait_until(fn -> dead?(identity) end, 10_000),
             "the guardian settled the blocked worker on control EOF (untrappable kill: EOF, not an after clause)"

      assert [%{"event_version" => 2, "data" => %{"attempt" => 1, "deadline_unix" => deadline}}] =
               Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))

      lines = run_dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)

      resumed =
        with_writer(run_dir, [create: false], fn sink ->
          Host.resume(
            spec,
            H.plan("gated_run_seed"),
            lines,
            host_opts(run_dir, helper, event_sink: sink, gate_opts: [barrier: tracking_barrier()])
          )
        end)

      assert {:ok, %{summary: %{"status" => "completed"}}} = resumed

      assert [%{"data" => %{"attempt" => 1}}, %{"data" => %{"attempt" => 2, "deadline_unix" => ^deadline}}] =
               Enum.filter(journal(run_dir), &(&1["type"] == "gate_started"))
    end
  end
end
