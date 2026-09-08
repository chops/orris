defmodule AiOrchestrator.Gate.GateExecutionTest do
  @moduledoc """
  RED rev 3 (rulings m_1788583146000, corrections MUST-6..12 in m_1788584540000): prepare /
  durable claim / checked ack / release once / owner-harness time fence / await evidence /
  publication truth / crash windows / read-only cold reconcile, on the real OS through the
  native guardian, with FaultFs at the claim seam and payload-carrying barriers.
  Contract: docs/contracts/gate-execution-claim.org.

  Identities come from the READY barrier or the handle, never from pgrep. Every owned
  group is registered for bounded cleanup BEFORE any assertion. Owners that may die run
  monitored; results travel as messages.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Gate.Execution
  alias AiOrchestrator.Gate.Execution.Ack
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @moduletag :native

  @source Path.expand("../../native/gate_guardian/gate_guardian.c", __DIR__)
  @run_id "run_fixture_0001"
  @deadline 1_700_000_600
  @now 1_700_000_000

  defmodule Clock do
    @moduledoc false
    def unix_now, do: Process.get(:gate_exec_now, 1_700_000_000)
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  defmodule BrokenClock do
    @moduledoc false
    def unix_now, do: raise("clock unavailable")
    def monotonic_ms, do: System.monotonic_time(:millisecond)
  end

  setup_all do
    dir = Path.join(System.tmp_dir!(), "gate-exec-build-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")
    seam = Path.join(dir, "gate_guardian_seam")
    flags = ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror"]
    {"", 0} = System.cmd("cc", flags ++ ["-o", bin, @source], stderr_to_stdout: true)
    {"", 0} = System.cmd("cc", flags ++ ["-DGATE_GUARDIAN_TESTING", "-o", seam, @source], stderr_to_stdout: true)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, helper: bin, seam: seam}
  end

  setup %{helper: helper} do
    run_dir = Path.join(System.tmp_dir!(), "gate-exec-run-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf(run_dir) end)
    Process.put(:gate_exec_now, @now)
    {:ok, run_dir: run_dir, helper: helper, opts: [helper: helper, settle_ms: 200, rounds: 2, clock: Clock]}
  end

  defp request(run_dir, argv, attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        gate_run_id: "gr_0001",
        attempt: 1,
        command_argv: argv,
        repo_root: run_dir,
        run_dir: run_dir,
        deadline_unix: @deadline,
        supervisor_instance: "sup_0001"
      },
      attrs
    )
  end

  defp fs, do: {SystemFs, nil}

  # ---- OS oracles: leader absence by identity AND an empty group, never kill -0 PID alone ----

  defp signal_zero(target) do
    case System.cmd("kill", ["-0", target], stderr_to_stdout: true) do
      {_, 0} -> :alive
      {out, _} -> if out =~ "No such process", do: :gone, else: :unknown
    end
  end

  defp group_gone?(pgid), do: signal_zero("-" <> Integer.to_string(pgid)) == :gone

  defp members(pgid) do
    case System.cmd("ps", ["-o", "pid=", "-g", Integer.to_string(pgid)], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n", trim: true) |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
      {"", 1} -> []
      {out, status} -> flunk("ps proved nothing (#{status}): #{inspect(out)}")
    end
  end

  defp dead?(%{worker: pid, pgid: pgid}),
    do: signal_zero(Integer.to_string(pid)) == :gone and group_gone?(pgid) and members(pgid) == []

  defp dead?(%{"pid" => pid, "pgid" => pgid}), do: dead?(%{worker: pid, pgid: pgid})

  # bounded cleanup registered BEFORE assertions, with NO signal on any historical number: when
  # the test process exits its Port closes and the ORIGINAL guardian settles the group on control
  # EOF; deliberately guardian-less fixtures are finite by construction. Teardown only proves
  # absence within a bound and fails the test loudly otherwise.
  defp track(%{worker: _, pgid: _, guardian: _} = identity) do
    on_exit(fn -> prove_absent!(identity) end)
    :ok
  end

  defp prove_absent!(identity) do
    wait_until(fn -> dead?(identity) end, 15_000) ||
      raise("owned group #{identity.pgid} (worker #{identity.worker}) still present at teardown")
  end

  # the ONE deliberate destruction of a guardian we currently own (its Port is open in this
  # process): proven alive immediately before, exact captured pid, never a historical number
  defp kill_guardian(%{guardian: guardian}) do
    assert signal_zero(Integer.to_string(guardian)) == :alive, "the owned guardian must be alive to be destroyed"
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(guardian)], stderr_to_stdout: true)
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    _ =
      Enum.find(Stream.repeatedly(fn -> fun.() end), fn
        true -> true
        false -> System.monotonic_time(:millisecond) > deadline or (Process.sleep(20) && false)
      end)

    fun.()
  end

  defp wait_for_file(path), do: wait_until(fn -> File.exists?(path) end, 5_000)

  # a monitored owner: runs `fun`, reports its result, then parks until killed
  defp owner(fun) do
    parent = self()

    spawn_monitor(fn ->
      result =
        try do
          {:finished, fun.()}
        rescue
          e -> {:crashed, e}
        end

      send(parent, {:owner_event, :result, result})
      Process.sleep(:infinity)
    end)
  end

  # the next owner event of this tag, or an immediate flunk when the owner crashed first
  defp expect(tag, timeout_ms \\ 10_000) do
    receive do
      {:owner_event, ^tag, payload} -> payload
      {:owner_event, :result, {:crashed, e}} -> flunk("owner crashed before #{tag}: #{Exception.message(e)}")
    after
      timeout_ms -> flunk("no #{tag} within #{timeout_ms} ms")
    end
  end

  defp lock,
    do: [supervisor_instance: "sup_0001", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]

  defp open_writer(run_dir, overrides \\ []) do
    {:ok, w, _} = Writer.open(run_dir, Keyword.merge([create: true, clock: FixedClock, lock: lock()], overrides))
    w
  end

  defp journaled(prepared, run_dir, seq) do
    w = open_writer(run_dir)
    result = Writer.append(w, started_event(prepared, seq))
    :ok = Writer.close(w)
    result
  end

  defp started_event(prepared, seq) do
    %{
      "schema" => "ai-orchestrator/journal-event",
      "schema_version" => 1,
      "event_version" => 2,
      "seq" => seq,
      "event_id" => "ev_#{String.pad_leading(Integer.to_string(seq), 4, "0")}",
      "type" => "gate_started",
      "ts" => "2026-01-01T00:00:01Z",
      "run_id" => @run_id,
      "actor" => "run_supervisor",
      "data" => Execution.started_data(prepared)
    }
  end

  defp mutate(event, {:put, path, value}), do: put_in(event, path, value)
  defp mutate(event, {:delete, key}), do: Map.delete(event, key)

  # a prepared handle plus a real ack, tracked
  defp acked(run_dir, argv, opts, attrs \\ %{}) do
    assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, argv, attrs), opts)
    :ok = track(Execution.identity(prepared))
    assert {:ok, persisted} = journaled(prepared, run_dir, 1)
    assert {:ok, ack} = Execution.ack(prepared, persisted)
    {prepared, ack}
  end

  defp gates_names(run_dir) do
    case File.ls(Path.join(run_dir, "gates")) do
      {:ok, names} -> Enum.sort(names)
      {:error, :enoent} -> []
    end
  end

  # ---- claim document and hash: ordered bytes, one field at a time ----

  describe "claim document and hash" do
    test "the canonical document is exactly these ordered bytes", %{run_dir: run_dir} do
      assert {:ok, bytes} = Execution.claim_document(request(run_dir, ["/bin/sleep", "30"]))

      assert bytes ==
               ~s({"attempt":1,"command_argv":["/bin/sleep","30"],"cwd":) <>
                 Jason.encode!(run_dir) <>
                 ~s(,"deadline_unix":#{@deadline},"gate_run_id":"gr_0001","run_id":"#{@run_id}",) <>
                 ~s("schema":"ai-orchestrator/gate-claim","schema_version":1})

      assert Execution.claim_hash(bytes) == "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    end

    for {field, value} <- [
          {:run_id, "run_fixture_0002"},
          {:gate_run_id, "gr_0002"},
          {:attempt, 2},
          {:command_argv, ["/bin/sleep", "31"]},
          {:command_argv, ["/bin/sleep", "30", ""]},
          {:repo_root, "/private/tmp/elsewhere"},
          {:deadline_unix, @deadline + 1}
        ] do
      test "changing only #{field} to #{inspect(value)} changes the hash", %{run_dir: run_dir} do
        base = request(run_dir, ["/bin/sleep", "30"])
        {:ok, b0} = Execution.claim_document(base)
        {:ok, b1} = Execution.claim_document(Map.put(base, unquote(field), unquote(Macro.escape(value))))
        assert Execution.claim_hash(b0) != Execution.claim_hash(b1)
      end
    end

    test "a request outside the domains is refused before anything exists", %{run_dir: run_dir, opts: opts} do
      for bad <- [
            %{attempt: 0},
            %{attempt: 3},
            %{gate_run_id: "../gr"},
            %{gate_run_id: "gr/0001"},
            %{gate_run_id: "gr_0001\n"},
            %{gate_run_id: "gr" <> <<0>>},
            %{gate_run_id: ""},
            %{gate_run_id: String.duplicate("a", 65)},
            %{command_argv: []},
            %{command_argv: ["/bin/sleep", 30]},
            %{run_id: ""},
            %{repo_root: "relative/dir"}
          ] do
        req = Map.merge(request(run_dir, ["/bin/sleep", "30"]), bad)
        assert match?({:error, %{clause: "invalid_request"}}, Execution.claim_document(req)), inspect(bad)
        assert match?({:error, %{clause: "invalid_request"}}, Execution.prepare(fs(), req, opts)), inspect(bad)
      end

      assert File.ls!(run_dir) == [], "nothing was created"
    end
  end

  # ---- prepare ----

  describe "prepare" do
    test "publishes the claim durably: identity, 0700 dir, 0600 regular claim with exact keys, started_data v2, worker blocked",
         %{run_dir: run_dir, opts: opts} do
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      assert %{guardian: g, worker: w, pgid: p, start: start} = identity
      assert is_integer(g) and is_integer(w) and is_integer(p) and g != w and is_binary(start) and start != ""
      data = Execution.started_data(prepared)
      assert %{"gate_run_id" => "gr_0001", "attempt" => 1, "deadline_unix" => @deadline} = data

      assert data["execution"] == %{
               "pid" => w,
               "pgid" => p,
               "start" => start,
               "claim_hash" => data["execution"]["claim_hash"]
             }

      {:ok, doc} = Execution.claim_document(req)
      assert data["execution"]["claim_hash"] == Execution.claim_hash(doc)
      assert data["stdout_path"] == "gates/gr_0001.1.out" and data["stderr_path"] == "gates/gr_0001.1.err"

      assert data |> Map.keys() |> Enum.sort() ==
               ~w(attempt command_argv deadline_unix execution gate_run_id stderr_path stdout_path)

      gates = Path.join(run_dir, "gates")
      assert Bitwise.band(File.stat!(gates).mode, 0o777) == 0o700
      claim_path = Path.join(gates, "gr_0001.claim.1")
      assert %File.Stat{type: :regular, mode: mode} = File.lstat!(claim_path)
      assert Bitwise.band(mode, 0o777) == 0o600
      {:ok, claim} = claim_path |> File.read!() |> Jason.decode()

      assert claim |> Map.keys() |> Enum.sort() ==
               ~w(attempt claim_hash claimed_at command_argv cwd deadline_unix gate_run_id identity run_id schema schema_version stderr_path stdout_path supervisor_instance token)

      assert claim["identity"] == %{"guardian_pid" => g, "worker_pid" => w, "pgid" => p, "start" => start}
      assert claim["claim_hash"] == data["execution"]["claim_hash"] and claim["cwd"] == run_dir
      assert is_binary(claim["token"]) and byte_size(claim["token"]) >= 16
      refute File.exists?(Path.join(run_dir, "ran")), "not released"
      assert :ok == Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
      assert "gr_0001.claim.1" in gates_names(run_dir), "abandon retains the claim for recovery"
    end

    test "an expired deadline is refused before anything is spawned or claimed", %{run_dir: run_dir, opts: opts} do
      Process.put(:gate_exec_now, @deadline)
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert match?({:error, %{clause: "deadline_expired"}}, Execution.prepare(fs(), req, opts))
      assert File.ls!(run_dir) == []
    end

    test "a broken clock refuses to prepare", %{run_dir: run_dir, opts: opts} do
      req = request(run_dir, ["/bin/sleep", "30"])

      assert match?(
               {:error, %{clause: "clock_unavailable"}},
               Execution.prepare(fs(), req, Keyword.put(opts, :clock, BrokenClock))
             )

      assert File.ls!(run_dir) == []
    end

    test "a claim for the same attempt already present is a conflict: fresh guardian settled, foreign file untouched, outputs removed",
         %{run_dir: run_dir, opts: opts} do
      gates = Path.join(run_dir, "gates")
      File.mkdir_p!(gates)
      claim_path = Path.join(gates, "gr_0001.claim.1")
      File.write!(claim_path, ~s({"schema":"ai-orchestrator/gate-claim","schema_version":1,"foreign":true}\n))
      before = File.read!(claim_path)
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      result = Execution.prepare(fs(), req, opts)

      assert match?(
               {:error,
                %{
                  clause: "claim_conflict",
                  stage: "link",
                  class: "eexist",
                  cleanup: "foreign_final_untouched",
                  outputs: "removed",
                  settle: %{settled: true, proof: "gone"}
                }},
               result
             )

      assert File.read!(claim_path) == before
      assert gates_names(run_dir) == ["gr_0001.claim.1"], "no temp residue, no output objects left"
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "a guardian setup failure is a closed prepare_failed class, nothing claimed", %{run_dir: run_dir, opts: opts} do
      missing_cwd = request(run_dir, ["/bin/sleep", "30"], %{repo_root: Path.join(run_dir, "missing")})
      assert {:error, rejection} = Execution.prepare(fs(), missing_cwd, opts)
      assert rejection.clause == "prepare_failed" and rejection.class == "chdir"
      assert rejection |> Map.keys() |> Enum.sort() == [:class, :clause, :cleanup, :settle]
      assert gates_names(run_dir) == [], "the guardian removed its output objects; nothing claimed"
    end

    for {label, script} <- [
          {"malformed READY", "printf 'READY guardian=abc worker=1 pgid=1 start=\\n'; sleep 30"},
          {"oversize first line", "head -c 5000 /dev/zero | tr '\\\\0' A; printf '\\n'; sleep 30"},
          {"foreign first record", "printf 'RELEASED\\n'; sleep 30"},
          {"READY coalesced with a second record",
           "printf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\nRELEASED\\n'; sleep 30"},
          {"helper exits without a record", "exit 7"}
        ] do
      test "a helper that answers with #{label} is prepare_failed class protocol with no claim", %{
        run_dir: run_dir,
        opts: opts
      } do
        fake = Path.join(run_dir, "fake_helper")
        File.write!(fake, "#!/bin/sh\n" <> unquote(script) <> "\n")
        File.chmod!(fake, 0o700)
        req = request(run_dir, ["/bin/sleep", "30"])
        result = Execution.prepare(fs(), req, [{:helper, fake}, {:ready_ms, 1000} | Keyword.delete(opts, :helper)])
        assert match?({:error, %{clause: "prepare_failed", class: "protocol"}}, result), unquote(label)
        {:error, rejection} = result
        refute Map.has_key?(rejection, :record) and is_map(rejection[:record]), "no arbitrary parsed map escapes"
        assert gates_names(run_dir) == []
      end
    end
  end

  # ---- ack and release fencing (MUST-1, MUST-10) ----

  describe "ack and release" do
    test "one owner-harness transaction with a real Writer: prepare, append, ack, release, await, then close",
         %{run_dir: run_dir, opts: opts} do
      w = open_writer(run_dir)
      req = request(run_dir, ["/bin/sh", "-c", "echo out-line; echo err-line >&2; exit 3"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      :ok = track(Execution.identity(prepared))
      assert {:ok, persisted} = Writer.append(w, started_event(prepared, 1))
      assert %{"seq" => 1, "prev_line_sha256" => _, "schema_version" => 2} = persisted
      assert {:ok, %Ack{seq: 1, binding: binding}} = Execution.ack(prepared, persisted)
      assert is_binary(binding)
      assert {:ok, running} = Execution.release(prepared, %Ack{seq: 1, binding: binding})
      assert match?({:error, %{clause: "already_released"}}, Execution.release(prepared, %Ack{seq: 1, binding: binding}))
      assert {:exit, outcome} = Execution.await(running, opts)
      assert %{"exit_status" => 3, "kind" => "exited", "settled" => true, "proof" => "gone"} = outcome
      assert outcome["stderr_merged"] == false
      assert File.read!(Path.join(run_dir, "gates/gr_0001.1.out")) == "out-line\n"
      assert File.read!(Path.join(run_dir, "gates/gr_0001.1.err")) == "err-line\n"
      assert outcome["stdout_hash"] == "sha256:" <> Base.encode16(:crypto.hash(:sha256, "out-line\n"), case: :lower)
      assert outcome["stderr_hash"] == "sha256:" <> Base.encode16(:crypto.hash(:sha256, "err-line\n"), case: :lower)
      assert %{"headline" => "out-line"} = outcome["failure_summary"]
      refute Map.has_key?(outcome, "signal")
      refute Execution.pass?(outcome)
      assert Writer.last_seq(w) == 1
      assert :ok == Writer.close(w)
    end

    test "a Writer append that fails before writing (seq_mismatch) yields no Ack; the handle is abandoned with proof",
         %{run_dir: run_dir, opts: opts} do
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      assert {:error, %{clause: "seq_mismatch"}} = journaled(prepared, run_dir, 2)
      assert match?({:error, %{clause: "ack_mismatch"}}, Execution.ack(prepared, %{clause: "seq_mismatch"}))
      assert :ok == Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "a Writer append that fails at the receipt stage (durability) yields no persisted map, no GO, settlement proven",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      armed = :counters.new(1, [])
      # only the receipt publish AFTER the writer is open is faulted: rename onto events.head
      FaultFs.inject(
        fault_fs,
        :rename,
        fn [_from, to] -> to == "events.head" and :counters.get(armed, 1) == 1 end,
        {:error, :eio}
      )

      w = open_writer(run_dir, fs: fault_fs)
      :counters.put(armed, 1, 1)
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      assert {:error, rejection} = Writer.append(w, started_event(prepared, 1))
      assert %{clause: "append_failed", stage: "receipt"} = rejection
      assert match?({:error, %{clause: "ack_mismatch"}}, Execution.ack(prepared, rejection))
      assert :ok == Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran")), "no GO was ever sent"
      _ = Writer.close(w)
    end

    for {label, mutation} <- [
          {"wrong type", {:put, ["type"], "gate_requested"}},
          {"wrong run", {:put, ["run_id"], "run_fixture_0002"}},
          {"wrong event_version", {:put, ["event_version"], 1}},
          {"wrong gate_run_id", {:put, ["data", "gate_run_id"], "gr_0002"}},
          {"wrong attempt", {:put, ["data", "attempt"], 2}},
          {"wrong deadline", {:put, ["data", "deadline_unix"], 1_700_000_601}},
          {"wrong command_argv", {:put, ["data", "command_argv"], ["/bin/sh", "-c", "true"]}},
          {"wrong stdout_path", {:put, ["data", "stdout_path"], "gates/gr_0001.2.out"}},
          {"wrong stderr_path", {:put, ["data", "stderr_path"], "gates/gr_0001.2.err"}},
          {"wrong claim_hash", {:put, ["data", "execution", "claim_hash"], "sha256:" <> String.duplicate("0", 64)}},
          {"wrong pid", {:put, ["data", "execution", "pid"], 1}},
          {"wrong pgid", {:put, ["data", "execution", "pgid"], 1}},
          {"wrong start", {:put, ["data", "execution", "start"], "0.000000"}},
          {"extra data key", {:put, ["data", "extra"], true}},
          {"missing seq", {:delete, "seq"}},
          {"invalid seq", {:put, ["seq"], 0}},
          {"unstamped (no writer)", {:delete, "prev_line_sha256"}}
        ] do
      test "an ack with #{label} is refused and cannot release", %{run_dir: run_dir, opts: opts} do
        req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
        assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
        :ok = track(Execution.identity(prepared))
        assert {:ok, persisted} = journaled(prepared, run_dir, 1)
        mutated = mutate(persisted, unquote(Macro.escape(mutation)))
        assert match?({:error, %{clause: "ack_mismatch", field: _}}, Execution.ack(prepared, mutated)), unquote(label)
        assert :ok == Execution.abandon(prepared)
        refute File.exists?(Path.join(run_dir, "ran"))
      end
    end

    test "an Ack built for another prepared handle (stale binding) cannot release this one",
         %{run_dir: run_dir, opts: opts} do
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert {:ok, first} = Execution.prepare(fs(), req, opts)
      :ok = track(Execution.identity(first))
      assert {:ok, persisted} = journaled(first, run_dir, 1)
      assert {:ok, ack} = Execution.ack(first, persisted)
      assert :ok == Execution.abandon(first)
      assert {:ok, second} = Execution.prepare(fs(), %{req | attempt: 2}, opts)
      :ok = track(Execution.identity(second))
      assert match?({:error, %{clause: "ack_mismatch"}}, Execution.release(second, ack))
      assert :ok == Execution.abandon(second)
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "an in-memory event never yields an Ack: only the writer's stamped result does",
         %{run_dir: run_dir, opts: opts} do
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      :ok = track(Execution.identity(prepared))
      in_memory = started_event(prepared, 1)
      assert match?({:error, %{clause: "ack_mismatch", field: "prev_line_sha256"}}, Execution.ack(prepared, in_memory))
      assert :ok == Execution.abandon(prepared)
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "expiry between ack and release refuses the go-token and abandons with proof", %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"], opts)
      Process.put(:gate_exec_now, @deadline)
      result = Execution.release(prepared, ack)
      assert match?({:error, %{clause: "deadline_expired", settle: %{settled: true, proof: "gone"}}}, result)
      assert wait_until(fn -> dead?(Execution.identity(prepared)) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "a broken clock at release means no go-token", %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"], opts)
      result = Execution.release(prepared, ack, clock: BrokenClock)
      assert match?({:error, %{clause: "clock_unavailable", settle: %{settled: true}}}, result)
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "a helper whose DEAD record is malformed makes abandon settle_unproven, never a silent :ok", %{
      run_dir: run_dir,
      opts: opts
    } do
      fake = Path.join(run_dir, "fake_helper")

      File.write!(
        fake,
        "#!/bin/sh\nprintf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\n'\nread cmd\nprintf 'DEAD reason=command settled=maybe\\n'\n"
      )

      File.chmod!(fake, 0o700)
      req = request(run_dir, ["/bin/sleep", "30"])
      assert {:ok, prepared} = Execution.prepare(fs(), req, [{:helper, fake} | Keyword.delete(opts, :helper)])
      assert match?({:error, %{clause: "settle_unproven"}}, Execution.abandon(prepared))
    end
  end

  # ---- await evidence and the owner-owned deadline (D3, MUST-4) ----

  describe "await" do
    test "a signaled worker reports the signal and no invented exit status", %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "kill -9 $$"], opts)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert {:exit, outcome} = Execution.await(running, opts)
      assert %{"kind" => "signaled", "signal" => 9, "settled" => true} = outcome
      refute Map.has_key?(outcome, "exit_status")
      assert %{"headline" => "gate killed by signal 9"} = outcome["failure_summary"]
      refute Execution.pass?(outcome)
    end

    test "exit 0 with an unknown group is never a pass", %{run_dir: run_dir, seam: seam} do
      opts = [helper: seam, settle_ms: 200, rounds: 2, clock: Clock, env: [{"GATE_GUARDIAN_FAULT", "members_fail"}]]
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "exit 0"], opts)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert {:exit, outcome} = Execution.await(running, opts)
      assert %{"exit_status" => 0, "settled" => false, "leftovers" => "unknown"} = outcome
      refute Execution.pass?(outcome), "exit 0 with an unsettled/unknown group is attention, never a pass"
    end

    test "the owner harness timer expires the running handle independent of any consumer await", %{
      run_dir: run_dir,
      opts: opts
    } do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "sleep 30 & sleep 30"], opts)
      identity = Execution.identity(prepared)
      assert {:ok, running} = Execution.release(prepared, ack)
      harness = self()
      Process.send_after(harness, :deadline, 100)
      assert_receive :deadline, 1_000
      Process.put(:gate_exec_now, @deadline)
      assert {:timeout, termination} = Execution.expire(running)
      assert %{kind: "timeout", settled: true, leftovers: "0", proof: "gone"} = termination
      refute Map.has_key?(termination, :exit_status) or Map.get(termination, :backstop, false)
      assert dead?(identity)
      assert Execution.await(running, opts) == {:timeout, termination}, "a later await agrees, never a fresh run"
    end

    test "the guardian backstop settles a running group when the owner is alive but stuck, reported as backstop",
         %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "sleep 30 & sleep 30"], opts, %{deadline_unix: @now + 1})
      identity = Execution.identity(prepared)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert wait_until(fn -> dead?(identity) end, 8_000), "the guardian settled the group with nobody asking"
      Process.put(:gate_exec_now, @now + 3)
      assert {:timeout, termination} = Execution.await(running, opts)
      assert %{kind: "timeout", backstop: true, settled: true, proof: "gone"} = termination
    end

    test "await after expiry with a TERM-ignoring descendant terminates the group with proof", %{
      run_dir: run_dir,
      opts: opts
    } do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "trap '' TERM; sleep 30 & wait"], opts)
      identity = Execution.identity(prepared)
      assert {:ok, running} = Execution.release(prepared, ack)
      Process.put(:gate_exec_now, @deadline)
      assert {:timeout, termination} = Execution.await(running, opts)
      assert %{kind: "timeout", settled: true, proof: "gone"} = termination
      assert dead?(identity), "a TERM-ignoring descendant is escalated to KILL"
    end

    test "unreadable output is a closed output_unreadable, never an invented hash", %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "exit 0"], opts)
      assert {:ok, running} = Execution.release(prepared, ack)
      out = Path.join(run_dir, "gates/gr_0001.1.out")
      assert wait_for_file(out)
      File.rm!(out)

      assert match?(
               {:error, %{clause: "output_unreadable", which: "out", class: "enoent"}},
               Execution.await(running, opts)
             )
    end
  end

  # ---- publication truth (MUST-3, MUST-8) ----

  describe "publication faults" do
    for {op, stage} <- [{:open, "open"}, {:write, "write"}, {:sync, "sync"}, {:close, "close"}, {:link, "link"}] do
      test "#{op} fault: claim_unpublished at stage #{stage}, cleanup removed, worker settled, outputs removed, no residue",
           %{run_dir: run_dir, opts: opts} do
        fault_fs = FaultFs.new()
        FaultFs.inject(fault_fs, unquote(op), 1, {:error, :eio})
        req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
        result = Execution.prepare(fault_fs, req, opts)

        assert match?(
                 {:error,
                  %{
                    clause: "claim_unpublished",
                    stage: unquote(stage),
                    class: "eio",
                    cleanup: cleanup,
                    outputs: "removed",
                    settle: %{settled: true, proof: "gone"}
                  }}
                 when cleanup in ["none", "removed"],
                 result
               )

        {:error, rejection} = result
        refute Map.has_key?(rejection, :detail), "no raw errno term escapes"
        assert gates_names(run_dir) == []
        refute File.exists?(Path.join(run_dir, "ran"))
      end
    end

    test "dir_sync fault after link: final retracted by path, then a SUCCESSFUL dir_sync; cleanup removed",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :dir_sync, 1, {:error, :eio})
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      result = Execution.prepare(fault_fs, req, opts)

      assert match?(
               {:error,
                %{
                  clause: "claim_unpublished",
                  stage: "dir_sync",
                  class: "eio",
                  cleanup: "removed",
                  settle: %{settled: true}
                }},
               result
             )

      assert gates_names(run_dir) == []
      trace = FaultFs.trace(fault_fs)
      final_rm = Enum.find_index(trace, &(&1 == {:rm, "gr_0001.claim.1"}))
      assert final_rm, "the FINAL was removed by its own path"
      after_rm = Enum.drop(trace, final_rm + 1)
      assert Enum.any?(after_rm, &match?({:dir_sync, "gates"}, &1)), "a dir_sync AFTER the final's removal"
      assert length(Enum.filter(trace, &match?({:dir_sync, _}, &1))) == 2, "one failed, one successful"
    end

    test "dir_sync fault after link and the retraction's own dir_sync fails too: removed_unsynced",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :dir_sync, 1, {:error, :eio})
      FaultFs.inject(fault_fs, :dir_sync, 2, {:error, :eio})
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      result = Execution.prepare(fault_fs, req, opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "dir_sync", cleanup: "removed_unsynced"}}, result)
      refute File.exists?(Path.join(run_dir, "gates/gr_0001.claim.1"))
    end

    test "dir_sync fault after link and the final cannot be removed: cleanup_required naming the final",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :dir_sync, 1, {:error, :eio})
      FaultFs.inject(fault_fs, :rm, fn [name] -> name == "gr_0001.claim.1" end, {:error, :eacces})
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      result = Execution.prepare(fault_fs, req, opts)

      assert match?(
               {:error,
                %{
                  clause: "claim_unpublished",
                  stage: "dir_sync",
                  cleanup: "cleanup_required",
                  residue: ["gates/gr_0001.claim.1"]
                }},
               result
             )

      assert File.exists?(Path.join(run_dir, "gates/gr_0001.claim.1"))
    end

    test "temp removal failure after a successful link: cleanup_required naming only the temp, final retracted",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :rm, fn [name] -> String.ends_with?(name, ".tmp") end, {:error, :eacces})
      req = request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      result = Execution.prepare(fault_fs, req, opts)

      assert {:error, %{clause: "claim_unpublished", stage: "rm_temp", cleanup: "cleanup_required", residue: [residue]}} =
               result

      assert residue =~ ~r/\Agates\/gr_0001\.claim\.1\.[A-Za-z0-9_-]+\.tmp\z/
      refute File.exists?(Path.join(run_dir, "gates/gr_0001.claim.1")), "the final was retracted, not left half-published"
    end

    test "a settle failure composes with the filesystem error instead of disappearing", %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      parent = self()
      # the exact guardian is killed at the :after_ready barrier, so its TERM can never be answered;
      # the claim then fails at sync: both truths must be reported
      barrier = fn
        :after_ready, identity ->
          send(parent, {:owner_event, :identity, identity})
          kill_guardian(identity)

        _, _ ->
          :ok
      end

      FaultFs.inject(fault_fs, :sync, 1, {:error, :eio})
      req = request(run_dir, ["/bin/sh", "-c", "sleep 30"])
      result = Execution.prepare(fault_fs, req, [{:barrier, barrier} | opts])
      identity = expect(:identity)
      :ok = track(identity)
      assert {:error, rejection} = result
      assert rejection.clause == "claim_unpublished" and rejection.class == "eio"

      assert match?(%{clause: "guardian_gone"}, rejection.settle) or
               match?(%{clause: "settle_unproven"}, rejection.settle)
    end

    test "a foreign final replaced between our link and our retraction is never removed: foreign_final_untouched",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      final = Path.join(run_dir, "gates/gr_0001.claim.1")
      # the window: after OUR successful link, at the dir_sync, a foreign process replaces the final
      once = :counters.new(1, [])

      FaultFs.inject(
        fault_fs,
        :dir_sync,
        fn ["gates"] ->
          # exactly one adversarial replacement, at the publication sync; never at a later cleanup sync
          if :counters.get(once, 1) == 0 do
            :counters.add(once, 1, 1)
            File.rm!(final)
            File.write!(final, ~s({"token":"foreign"}\n))
            true
          else
            false
          end
        end,
        {:error, :eio}
      )

      req = request(run_dir, ["/bin/sh", "-c", "sleep 30"])
      result = Execution.prepare(fault_fs, req, opts)

      assert match?(
               {:error, %{clause: "claim_unpublished", stage: "dir_sync", cleanup: "foreign_final_untouched"}},
               result
             )

      assert File.read!(final) == ~s({"token":"foreign"}\n)
    end
  end

  # ---- crash windows (MUST-5, MUST-6) ----

  describe "crash windows" do
    test ":before_spawn: owner dies before READY; nothing spawned, no claim, no outputs", %{run_dir: run_dir, opts: opts} do
      barrier = fn
        :before_spawn, _ -> Process.exit(self(), :kill)
        _, _ -> :ok
      end

      {pid, ref} =
        owner(fn -> Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), [{:barrier, barrier} | opts]) end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert gates_names(run_dir) == []
    end

    test ":after_ready: owner dies post-READY / pre-claim; the guardian settles the worker on port close; no temp, no final",
         %{run_dir: run_dir, opts: opts} do
      parent = self()

      barrier = fn
        :after_ready, identity ->
          parent |> send({:owner_event, :identity, identity}) |> then(fn _ -> Process.exit(self(), :kill) end)

        _, _ ->
          :ok
      end

      {pid, ref} =
        owner(fn ->
          Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"]), [
            {:barrier, barrier} | opts
          ])
        end)

      identity = expect(:identity)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert wait_until(fn -> dead?(identity) end, 5_000)
      assert Enum.reject(gates_names(run_dir), &String.ends_with?(&1, [".out", ".err"])) == []
      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "post-link / pre-dir-sync (rm hook): final present, temp is residue recovery names, worker settled",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      parent = self()

      barrier = fn
        :after_ready, identity -> send(parent, {:owner_event, :identity, identity})
        _, _ -> :ok
      end

      FaultFs.inject(
        fault_fs,
        :rm,
        fn [name] -> String.ends_with?(name, ".tmp") end,
        {:hook, fn -> Process.exit(self(), :kill) end}
      )

      {pid, ref} =
        owner(fn ->
          Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"]), [
            {:barrier, barrier} | opts
          ])
        end)

      identity = expect(:identity)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      names = gates_names(run_dir)
      assert "gr_0001.claim.1" in names
      assert Enum.any?(names, &String.ends_with?(&1, ".tmp"))
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
      {:ok, claim} = run_dir |> Path.join("gates/gr_0001.claim.1") |> File.read!() |> Jason.decode()
      expected = claim |> claim_expected() |> Map.put("journaled", false)
      assert {:orphan_claim, %{"residue" => [tmp]}} = Execution.reconcile(fs(), run_dir, expected, opts)
      assert String.ends_with?(tmp, ".tmp")
    end

    test ":after_claim: owner dies before the ack; claim outlives the owner; reconcile with journaled=false is orphan",
         %{run_dir: run_dir, opts: opts} do
      parent = self()

      barrier = fn
        :after_ready, identity -> send(parent, {:owner_event, :identity, identity})
        :after_claim, _ -> Process.exit(self(), :kill)
        _, _ -> :ok
      end

      {pid, ref} =
        owner(fn ->
          Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"]), [
            {:barrier, barrier} | opts
          ])
        end)

      identity = expect(:identity)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      claim_path = Path.join(run_dir, "gates/gr_0001.claim.1")
      assert File.exists?(claim_path)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
      {:ok, claim} = claim_path |> File.read!() |> Jason.decode()
      expected = claim |> claim_expected() |> Map.put("journaled", false)
      assert {:orphan_claim, _} = Execution.reconcile(fs(), run_dir, expected, opts)
    end

    test ":after_ack: owner dies after the durable ack, before GO; never released; reconcile => dead", %{
      run_dir: run_dir,
      opts: opts
    } do
      parent = self()

      barrier = fn
        :after_ack, _ -> Process.exit(self(), :kill)
        _, _ -> :ok
      end

      {pid, ref} =
        owner(fn ->
          assert {:ok, prepared} =
                   Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"]), opts)

          assert {:ok, persisted} = journaled(prepared, run_dir, 1)
          assert {:ok, ack} = Execution.ack(prepared, persisted)
          send(parent, {:owner_event, :handle, {Execution.identity(prepared), Execution.started_data(prepared)}})
          Execution.release(prepared, ack, barrier: barrier)
        end)

      {identity, data} = expect(:handle)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert wait_until(fn -> dead?(identity) end, 5_000)
      refute File.exists?(Path.join(run_dir, "ran"))
      assert {:dead, _facts} = Execution.reconcile(fs(), run_dir, expected_from(data), opts)
    end

    test ":after_go: owner dies after GO before RELEASED; the guardian settles the released group; reconcile => dead",
         %{run_dir: run_dir, opts: opts} do
      parent = self()

      barrier = fn
        :after_go, _ -> Process.exit(self(), :kill)
        _, _ -> :ok
      end

      {pid, ref} =
        owner(fn ->
          {:ok, prepared} =
            Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo started > marker; sleep 30"]), opts)

          assert {:ok, persisted} = journaled(prepared, run_dir, 1)
          assert {:ok, ack} = Execution.ack(prepared, persisted)
          send(parent, {:owner_event, :handle, {Execution.identity(prepared), Execution.started_data(prepared)}})
          Execution.release(prepared, ack, barrier: barrier)
        end)

      {identity, data} = expect(:handle)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert wait_until(fn -> dead?(identity) end, 5_000)
      assert {:dead, _facts} = Execution.reconcile(fs(), run_dir, expected_from(data), opts)
    end

    test "running owner crash: a command-written marker and a real group exist, then the guardian settles it",
         %{run_dir: run_dir, opts: opts} do
      parent = self()
      marker = Path.join(run_dir, "marker")

      {pid, ref} =
        owner(fn ->
          {:ok, prepared} =
            Execution.prepare(
              fs(),
              request(run_dir, ["/bin/sh", "-c", "echo started > marker; sleep 30 & sleep 30"]),
              opts
            )

          assert {:ok, persisted} = journaled(prepared, run_dir, 1)
          assert {:ok, ack} = Execution.ack(prepared, persisted)
          assert {:ok, running} = Execution.release(prepared, ack)
          send(parent, {:owner_event, :handle, {Execution.identity(prepared), Execution.started_data(prepared)}})
          Execution.await(running, opts)
        end)

      {identity, data} = expect(:handle)
      :ok = track(identity)
      assert wait_for_file(marker) and File.read!(marker) == "started\n"
      assert length(members(identity.pgid)) >= 2, "a real group: sh plus its sleep"
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert wait_until(fn -> dead?(identity) end, 5_000)
      assert {:dead, _facts} = Execution.reconcile(fs(), run_dir, expected_from(data), opts)
      assert Bitwise.band(File.stat!(Path.join(run_dir, "gates/gr_0001.1.out")).mode, 0o077) == 0
    end

    test ":after_exit: completion before the terminal is journaled; outputs intact; reconcile => dead", %{
      run_dir: run_dir,
      opts: opts
    } do
      parent = self()

      barrier = fn
        :after_exit, _ -> Process.exit(self(), :kill)
        _, _ -> :ok
      end

      {pid, ref} =
        owner(fn ->
          assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo done; exit 0"]), opts)
          assert {:ok, persisted} = journaled(prepared, run_dir, 1)
          assert {:ok, ack} = Execution.ack(prepared, persisted)
          assert {:ok, running} = Execution.release(prepared, ack)
          send(parent, {:owner_event, :handle, {Execution.identity(prepared), Execution.started_data(prepared)}})
          Execution.await(running, [{:barrier, barrier} | opts])
        end)

      {identity, data} = expect(:handle)
      :ok = track(identity)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 10_000
      assert File.read!(Path.join(run_dir, "gates/gr_0001.1.out")) == "done\n"
      assert {:dead, _facts} = Execution.reconcile(fs(), run_dir, expected_from(data), opts)
    end
  end

  @orphan_program """
  #!/bin/sh
  trap '' TERM
  echo $$ > "$1"
  i=0
  while [ "$i" -lt 30 ]; do i=$((i + 1)); sleep 0.1; done
  """

  defp orphan_script!(run_dir) do
    path = Path.join(run_dir, "orphan.sh")
    File.write!(path, @orphan_program)
    File.chmod!(path, 0o700)
    path
  end

  # argv only: $0 is the script path, $1 the ready path; the leader launches the child, waits
  # (bounded) for readiness written AFTER the child's trap, then exits 0
  defp orphan_launcher(script, ready) do
    [
      "/bin/sh",
      "-c",
      ~S|"$0" "$1" & i=0; while [ ! -s "$1" ] && [ "$i" -lt 50 ]; do i=$((i + 1)); sleep 0.1; done; exit 0|,
      script,
      ready
    ]
  end

  defp expected_from(data), do: Map.put(data, "run_id", @run_id)

  defp claim_expected(claim) do
    %{
      "run_id" => claim["run_id"],
      "gate_run_id" => claim["gate_run_id"],
      "attempt" => claim["attempt"],
      "command_argv" => claim["command_argv"],
      "stdout_path" => claim["stdout_path"],
      "stderr_path" => claim["stderr_path"],
      "deadline_unix" => claim["deadline_unix"],
      "execution" => %{
        "pid" => claim["identity"]["worker_pid"],
        "pgid" => claim["identity"]["pgid"],
        "start" => claim["identity"]["start"],
        "claim_hash" => claim["claim_hash"]
      }
    }
  end

  # ---- cold reconcile is read-only (D2, MUST-11) ----

  describe "reconcile" do
    test "no claim for the expected attempt is :no_claim; a listing error, a malformed family name, a symlink or a directory fail closed",
         %{run_dir: run_dir, opts: opts} do
      expected = %{"run_id" => @run_id, "gate_run_id" => "gr_0001", "attempt" => 1}
      assert :no_claim == Execution.reconcile(fs(), run_dir, expected, opts)
      gates = Path.join(run_dir, "gates")
      File.mkdir_p!(gates)
      claim = Path.join(gates, "gr_0001.claim.1")

      cases = [
        {"malformed family name", fn -> File.write!(Path.join(gates, "gr_0001.claim.x"), "") end,
         fn -> File.rm!(Path.join(gates, "gr_0001.claim.x")) end},
        {"symlink", fn -> File.ln_s!("/etc/hosts", claim) end, fn -> File.rm!(claim) end},
        {"directory", fn -> File.mkdir!(claim) end, fn -> File.rmdir!(claim) end}
      ]

      for {label, arrange, cleanup} <- cases do
        arrange.()
        assert match?({:error, %{clause: "claim_unreadable"}}, Execution.reconcile(fs(), run_dir, expected, opts)), label
        cleanup.()
      end

      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :list_dir, 1, {:error, :eio})

      assert match?(
               {:error, %{clause: "claim_unreadable", class: "eio"}},
               Execution.reconcile(fault_fs, run_dir, expected, opts)
             )
    end

    test "one accepted claim, one property mutated each: wrong mode, one extra key, oversize (size checked before any read)",
         %{run_dir: run_dir, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      expected = expected_from(data)
      path = Path.join(run_dir, "gates/gr_0001.claim.1")
      accepted = File.read!(path)
      # positive control: the untouched accepted claim reconciles to a verdict, not a rejection
      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected, opts))

      # wrong mode only
      File.chmod!(path, 0o644)

      assert match?(
               {:error, %{clause: "claim_unreadable", field: "mode"}},
               Execution.reconcile(fs(), run_dir, expected, opts)
             )

      File.chmod!(path, 0o600)
      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected, opts)), "restored"

      # one extra key only, everything else the accepted content
      with_extra = accepted |> Jason.decode!() |> Map.put("extra", 1) |> Jason.encode!()
      File.write!(path, with_extra <> "\n")
      File.chmod!(path, 0o600)

      assert match?(
               {:error, %{clause: "claim_unreadable", field: "extra"}},
               Execution.reconcile(fs(), run_dir, expected, opts)
             )

      # oversize only: the accepted document padded with trailing whitespace past 8192 bytes is still
      # valid JSON, so only a size check BEFORE reading can reject it; the trace proves no read happened
      File.write!(path, String.trim_trailing(accepted) <> String.duplicate(" ", 9_000) <> "\n")
      File.chmod!(path, 0o600)
      fault_fs = FaultFs.new()

      assert match?(
               {:error, %{clause: "claim_unreadable", field: "size"}},
               Execution.reconcile(fault_fs, run_dir, expected, opts)
             )

      trace = FaultFs.trace(fault_fs)
      assert Enum.any?(trace, &match?({:lstat, "gr_0001.claim.1"}, &1)), "size comes from lstat"
      refute Enum.any?(trace, &match?({:read, "gr_0001.claim.1"}, &1)), "an oversize claim is never read"

      File.write!(path, accepted)
      File.chmod!(path, 0o600)
      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected, opts)), "restored again"
    end

    test "a decoded claim whose recomputed hash disagrees with its claim_hash field is claim_unreadable", %{
      run_dir: run_dir,
      opts: opts
    } do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)
      :ok = track(Execution.identity(prepared))
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      path = Path.join(run_dir, "gates/gr_0001.claim.1")
      tampered = path |> File.read!() |> Jason.decode!() |> Map.put("cwd", "/private/tmp/elsewhere")
      File.write!(path, Jason.encode!(tampered) <> "\n")

      assert match?(
               {:error, %{clause: "claim_unreadable"}},
               Execution.reconcile(fs(), run_dir, expected_from(data), opts)
             )
    end

    test "a claim that disagrees with the journaled start is claim_mismatch naming the field; a higher attempt is claim_unexpected",
         %{run_dir: run_dir, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)
      :ok = track(Execution.identity(prepared))
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      expected = expected_from(data)

      for {path, value} <- [
            {["run_id"], "run_fixture_0002"},
            {["deadline_unix"], @deadline + 1},
            {["command_argv"], ["/bin/sleep", "31"]},
            {["stdout_path"], "gates/gr_0001.2.out"},
            {["execution", "claim_hash"], "sha256:" <> String.duplicate("1", 64)},
            {["execution", "start"], "0.000000"},
            {["execution", "pid"], 1},
            {["execution", "pgid"], 1}
          ] do
        wrong = put_in(expected, path, value)

        assert match?({:error, %{clause: "claim_mismatch", field: _}}, Execution.reconcile(fs(), run_dir, wrong, opts)),
               inspect(path)
      end

      File.cp!(Path.join(run_dir, "gates/gr_0001.claim.1"), Path.join(run_dir, "gates/gr_0001.claim.2"))
      assert match?({:error, %{clause: "claim_unexpected"}}, Execution.reconcile(fs(), run_dir, expected, opts))
    end

    test "a live same-start leader held by this owner is unknown and never signalled; after abandon it is dead",
         %{run_dir: run_dir, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      expected = expected_from(Execution.started_data(prepared))
      assert {:unknown, facts} = Execution.reconcile(fs(), run_dir, expected, opts)
      assert facts["leader"] == "alive" and facts["group"] == "alive" and facts["members"] >= 1
      refute dead?(identity), "never signalled"
      :ok = Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      assert {:dead, facts} = Execution.reconcile(fs(), run_dir, expected, opts)
      assert facts["leader"] == "gone" and facts["group"] == "gone" and facts["members"] == 0
    end

    test "a pid reused by a stranger (different start) is never signalled: dead only if its group is gone, else unknown",
         %{run_dir: run_dir, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      # a finite stranger (3 s): owned by this test process's Port, reaped by the BEAM when it exits
      port = Port.open({:spawn_executable, "/bin/sleep"}, [:binary, :exit_status, {:args, ["3"]}])
      {:os_pid, stranger} = Port.info(port, :os_pid)
      # rewrite BOTH the claim's identity and the journaled expectation to name the stranger's pid with OUR start
      path = Path.join(run_dir, "gates/gr_0001.claim.1")
      claim = path |> File.read!() |> Jason.decode!() |> put_in(["identity", "worker_pid"], stranger)
      File.write!(path, Jason.encode!(claim) <> "\n")
      expected = data |> expected_from() |> put_in(["execution", "pid"], stranger)
      verdict = Execution.reconcile(fs(), run_dir, expected, opts)
      assert match?({:dead, %{"leader" => "gone"}}, verdict) or match?({:unknown, %{"leader" => "alive"}}, verdict)
      assert signal_zero(Integer.to_string(stranger)) == :alive, "the stranger is never signalled"
      assert_receive {^port, {:exit_status, 0}}, 5_000
    end

    test "leader gone but the owned group non-empty (guardian destroyed mid-settle) is unknown, never a kill",
         %{run_dir: run_dir, opts: opts} do
      # settle window widened so the guardian is provably mid-settle (TERM ignored) when we destroy it
      opts = Keyword.merge(opts, settle_ms: 5_000, rounds: 2)
      ready = Path.join(run_dir, "ready")
      # the orphan is a FRESH interpreter running its own script file, with the ready path as ITS
      # argument: no `$` ever crosses an outer shell. It installs the TERM ignore first, then writes
      # its OWN pid as readiness, then survives ~3 s in a loop (each sleep may die to TERM; the
      # shell itself ignores it) and exits by itself: finite even without any guardian.
      # The leader waits (bounded, 5 s) for that post-trap readiness before exiting, so the
      # guardian can never TERM the child before its trap exists.
      orphan = orphan_script!(run_dir)
      {prepared, ack} = acked(run_dir, orphan_launcher(orphan, ready), opts)
      identity = Execution.identity(prepared)
      data = Execution.started_data(prepared)
      assert {:ok, _running} = Execution.release(prepared, ack)
      assert wait_for_file(ready)
      orphan = ready |> File.read!() |> String.trim() |> String.to_integer()
      assert orphan != identity.worker, "the orphan is a distinct child, not the leader"

      on_exit(fn ->
        wait_until(fn -> signal_zero(Integer.to_string(orphan)) == :gone end, 15_000) ||
          raise("orphan #{orphan} outlived its bound")
      end)

      # the leader exits at once; the guardian (mid-settle, 5 s) is destroyed while we still own it
      assert wait_until(fn -> orphan in members(identity.pgid) end, 5_000)
      kill_guardian(identity)
      assert wait_until(fn -> signal_zero(Integer.to_string(identity.guardian)) == :gone end, 5_000)

      assert wait_until(fn -> signal_zero(Integer.to_string(identity.worker)) == :gone end, 5_000),
             "the leader is gone by identity"

      assert orphan in members(identity.pgid), "the group is non-empty: the distinct orphan"
      assert {:unknown, facts} = Execution.reconcile(fs(), run_dir, expected_from(data), opts)
      assert facts["leader"] == "gone" and facts["members"] >= 1
      assert signal_zero(Integer.to_string(orphan)) == :alive, "never a kill"
    end

    test "unknown facts propagate: members unknown, group EPERM or leader identity unknown is never dead, even with the leader gone",
         %{run_dir: run_dir, seam: seam, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
      identity = Execution.identity(prepared)
      :ok = track(identity)
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      assert wait_until(fn -> dead?(identity) end, 5_000)
      seam_opts = Keyword.put(opts, :helper, seam)

      # an absent leader with an unreadable group or an EPERM group is unknown, never dead
      for fault <- ["members_fail", "probe_group_eperm"] do
        verdict =
          Execution.reconcile(
            fs(),
            run_dir,
            expected_from(data),
            Keyword.put(seam_opts, :env, [{"GATE_GUARDIAN_FAULT", fault}])
          )

        assert match?({:unknown, _}, verdict), "#{fault}: an unknown fact can never become dead"
      end

      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected_from(data), opts)),
             "positive control without a fault"

      # identity_short on a LIVE subject: kill(pid, 0) says alive, the identity cannot be read, so the
      # leader is unknown (an already-absent leader proven by ESRCH is not made unknown by this seam)
      assert {:ok, live} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"], %{gate_run_id: "gr_0002"}), opts)
      :ok = track(Execution.identity(live))
      live_expected = expected_from(Execution.started_data(live))

      verdict =
        Execution.reconcile(
          fs(),
          run_dir,
          live_expected,
          Keyword.put(seam_opts, :env, [{"GATE_GUARDIAN_FAULT", "identity_short"}])
        )

      assert match?({:unknown, %{"leader" => "unknown"}}, verdict)
      assert :ok == Execution.abandon(live)
    end

    for {label, script} <- [
          {"malformed PROBE", "printf 'PROBE leader=gone start=- members=0\\n'"},
          {"truncated PROBE", "printf 'PROBE leader=go'"},
          {"oversize PROBE", "printf 'PROBE '; head -c 5000 /dev/zero | tr '\\\\0' A; printf '\\n'"},
          {"foreign record", "printf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\n'"},
          {"two records",
           "printf 'PROBE leader=gone start=- group=gone members=0\\nPROBE leader=gone start=- group=gone members=0\\n'"},
          {"exit without record", "exit 3"}
        ] do
      test "a probe helper answering with #{label} is probe_invalid, never a verdict", %{run_dir: run_dir, opts: opts} do
        assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
        identity = Execution.identity(prepared)
        :ok = track(identity)
        data = Execution.started_data(prepared)
        :ok = Execution.abandon(prepared)
        fake = Path.join(run_dir, "fake_probe")
        File.write!(fake, "#!/bin/sh\n" <> unquote(script) <> "\n")
        File.chmod!(fake, 0o700)
        verdict = Execution.reconcile(fs(), run_dir, expected_from(data), Keyword.put(opts, :helper, fake))
        assert match?({:error, %{clause: "probe_invalid"}}, verdict), unquote(label)
      end
    end
  end

  # ---- C3 successor: strict grammar, mandatory binding, gates boundary, claim bound, cleanup truth ----

  describe "strict record grammar (C3-M1)" do
    for {label, script} <- [
          {"PROBE with a live leader and a garbage start",
           "printf 'PROBE leader=alive start=garbage group=gone members=0\\n'"},
          {"PROBE with duplicate leader keys", "printf 'PROBE leader=alive leader=gone start=- group=gone members=0\\n'"},
          {"PROBE with an extra key", "printf 'PROBE leader=gone start=- group=gone members=0 extra=1\\n'"},
          {"PROBE with a signed members count", "printf 'PROBE leader=gone start=- group=gone members=+0\\n'"}
        ] do
      test "#{label} is probe_invalid, never a verdict", %{run_dir: run_dir, opts: opts} do
        assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
        :ok = track(Execution.identity(prepared))
        data = Execution.started_data(prepared)
        :ok = Execution.abandon(prepared)
        fake = Path.join(run_dir, "fake_probe")
        File.write!(fake, "#!/bin/sh\n" <> unquote(script) <> "\n")
        File.chmod!(fake, 0o700)
        verdict = Execution.reconcile(fs(), run_dir, expected_from(data), Keyword.put(opts, :helper, fake))
        assert match?({:error, %{clause: "probe_invalid"}}, verdict), unquote(label)
      end
    end

    for {label, script} <- [
          {"a blank startup record", "printf '\\n'; sleep 1"},
          {"a malformed startup record after the helper closes its command pipe",
           "exec 0<&-; printf 'HELLO world=1\\n'; sleep 1"},
          {"SETUP_FAILED with a synthetic class and cleanup",
           "printf 'SETUP_FAILED class=SYNTHETIC_PAYLOAD cleanup=SYNTHETIC_PAYLOAD\\n'; exit 1"},
          {"READY with duplicate pgid keys",
           "printf 'READY guardian=1 worker=2 pgid=2 pgid=3 start=1.000000\\n'; sleep 1"},
          {"READY with an unbounded start",
           "printf 'READY guardian=1 worker=2 pgid=2 start=1.0000000000000000000000000000000000000001\\n'; sleep 1"},
          {"an unknown record head", "printf 'HELLO world=1\\n'; sleep 1"}
        ] do
      test "#{label} is prepare_failed class protocol; nothing verbatim escapes, nothing raises", %{
        run_dir: run_dir,
        opts: opts
      } do
        fake = Path.join(run_dir, "fake_helper")
        pid_path = Path.join(run_dir, "fake_helper.pid")
        File.write!(fake, ~s(#!/bin/sh\nprintf '%s\\n' "$$" > "$HELPER_PID"\n) <> unquote(script) <> "\n")
        File.chmod!(fake, 0o700)
        parent = self()
        tag = make_ref()

        {owner, monitor} =
          spawn_monitor(fn ->
            {:trap_exit, false} = Process.info(self(), :trap_exit)

            result =
              Execution.prepare(
                fs(),
                request(run_dir, ["/bin/sleep", "30"]),
                Keyword.merge(opts, helper: fake, ready_ms: 1000, env: [{"HELPER_PID", pid_path}])
              )

            send(parent, {tag, result})
          end)

        try do
          assert_receive {^tag, {:error, rejection}}, 5000
          assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, 1000
          assert rejection.clause == "prepare_failed" and rejection.class == "protocol"
          refute inspect(rejection) =~ "SYNTHETIC", "no verbatim payload in a diagnostic"
          refute inspect(rejection) =~ "HELLO"
          pid = pid_path |> File.read!() |> String.trim() |> String.to_integer()
          assert wait_until(fn -> signal_zero(Integer.to_string(pid)) == :gone end, 5000), "the finite helper must exit"
        after
          if Process.alive?(owner) do
            Process.exit(owner, :kill)
            assert_receive {:DOWN, ^monitor, :process, ^owner, _}, 5000
          end

          Process.demonitor(monitor, [:flush])
        end
      end
    end

    test "malformed startup closes the native control channel without claiming settlement", %{
      run_dir: run_dir,
      helper: helper,
      opts: opts
    } do
      wrapper = Path.join(run_dir, "startup_wrapper")
      ready_path = Path.join(run_dir, "native_ready")

      File.write!(wrapper, """
      #!/bin/sh
      "$REAL_GUARDIAN" "$@" | {
        IFS= read -r ready
        printf '%s\\n' "$ready" > "$READY_RECORD"
        printf 'HELLO\\n'
        cat >/dev/null
      }
      """)

      File.chmod!(wrapper, 0o700)

      result =
        Execution.prepare(
          fs(),
          request(run_dir, ["/bin/sleep", "30"]),
          Keyword.merge(opts, helper: wrapper, env: [{"REAL_GUARDIAN", helper}, {"READY_RECORD", ready_path}])
        )

      [_, guardian, worker, pgid] =
        Regex.run(~r/\AREADY guardian=([0-9]+) worker=([0-9]+) pgid=([0-9]+) start=/, File.read!(ready_path))

      identity = %{
        guardian: String.to_integer(guardian),
        worker: String.to_integer(worker),
        pgid: String.to_integer(pgid)
      }

      :ok = track(identity)

      assert {:error, %{class: "protocol", settle: %{clause: "settle_unproven"}}} = result
      assert wait_until(fn -> dead?(identity) end, 5000), "the original native group must settle on control EOF"
    end

    test "a malformed EXIT from the helper is a closed await failure, never a proof", %{run_dir: run_dir, opts: opts} do
      fake = Path.join(run_dir, "fake_helper")

      File.write!(
        fake,
        "#!/bin/sh\nprintf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\n'\nread cmd\nprintf 'RELEASED\\n'\n" <>
          "printf 'EXIT kind=exited status=999 settled=1 leftovers=0 proof=gone escaped=unknown\\n'\nread cmd\n" <>
          "printf 'DEAD reason=command kind=unknown settled=1 leftovers=lots proof=gone escaped=unknown\\n'\n"
      )

      File.chmod!(fake, 0o700)

      assert {:ok, prepared} =
               Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), [
                 {:helper, fake} | Keyword.delete(opts, :helper)
               ])

      assert {:ok, persisted} = journaled(prepared, run_dir, 1)
      assert {:ok, ack} = Execution.ack(prepared, persisted)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert match?({:error, %{clause: "await_failed", record: "malformed EXIT"}}, Execution.await(running, opts))
    end
  end

  describe "mandatory binding (C3-M2)" do
    test "a partial expected never authorizes a verdict: each missing field is named and the probe is never reached",
         %{run_dir: run_dir, opts: opts} do
      assert {:ok, prepared} = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
      :ok = track(Execution.identity(prepared))
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      marker = Path.join(run_dir, "probe-ran")
      fake = Path.join(run_dir, "fake_probe")
      File.write!(fake, "#!/bin/sh\ntouch #{marker}\nprintf 'PROBE leader=gone start=- group=gone members=0\\n'\n")
      File.chmod!(fake, 0o700)
      probing = Keyword.put(opts, :helper, fake)
      expected = expected_from(data)
      partial = %{"gate_run_id" => "gr_0001", "attempt" => 1}

      assert match?(
               {:error, %{clause: "claim_mismatch", field: "run_id", missing: true}},
               Execution.reconcile(fs(), run_dir, partial, probing)
             )

      for path <- [
            ["run_id"],
            ["deadline_unix"],
            ["command_argv"],
            ["stdout_path"],
            ["stderr_path"],
            ["execution", "pid"],
            ["execution", "pgid"],
            ["execution", "start"],
            ["execution", "claim_hash"]
          ] do
        {_, dropped} = pop_in(expected, path)

        assert match?(
                 {:error, %{clause: "claim_mismatch", missing: true}},
                 Execution.reconcile(fs(), run_dir, dropped, probing)
               ),
               inspect(path)

        assert match?(
                 {:error, %{clause: "claim_mismatch", missing: true}},
                 Execution.reconcile(fs(), run_dir, put_in(expected, path, nil), probing)
               ),
               inspect(path)
      end

      refute File.exists?(marker), "the fact probe never ran on a partial expectation"
      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected, probing)), "the complete binding does"
    end
  end

  describe "gates directory boundary (C3-M3)" do
    test "a symlink at run_dir/gates is refused before chmod, spawn or publication; the target is untouched", %{
      run_dir: run_dir,
      opts: opts
    } do
      target = Path.join(run_dir, "elsewhere")
      File.mkdir_p!(target)
      File.chmod!(target, 0o755)
      File.ln_s!(target, Path.join(run_dir, "gates"))
      result = Execution.prepare(fs(), request(run_dir, ["/bin/sh", "-c", "echo ran > ran; sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "gates_dir", field: "type", cleanup: "none"}}, result)
      assert Bitwise.band(File.stat!(target).mode, 0o777) == 0o755, "the target's mode is untouched"
      assert File.ls!(target) == [], "nothing was written through the link"
      refute File.exists?(Path.join(run_dir, "ran"))
      expected = %{"run_id" => @run_id, "gate_run_id" => "gr_0001", "attempt" => 1}

      assert match?(
               {:error, %{clause: "claim_unreadable", field: "gates"}},
               Execution.reconcile(fs(), run_dir, expected, opts)
             ),
             "cold traversal never follows it"
    end

    test "a regular file at run_dir/gates is refused the same way", %{run_dir: run_dir, opts: opts} do
      File.write!(Path.join(run_dir, "gates"), "")

      assert match?(
               {:error, %{stage: "gates_dir", field: "type"}},
               Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "3"]), opts)
             )
    end
  end

  describe "claim bound before spawn (C3-M4)" do
    test "a request whose widest possible claim would exceed the reader's bound is refused before anything exists", %{
      run_dir: run_dir,
      opts: opts
    } do
      # 8100 bytes is a lawful single argument, but the widest claim it yields exceeds the reader bound
      req = request(run_dir, ["/bin/sh", "-c", String.duplicate("x", 8_100)])
      assert match?({:error, %{clause: "invalid_request", field: "size"}}, Execution.claim_document(req))
      assert match?({:error, %{clause: "invalid_request", field: "size"}}, Execution.prepare(fs(), req, opts))
      assert File.ls!(run_dir) == []
    end

    test "every accepted request publishes a claim the reader accepts, at the boundary too", %{
      run_dir: run_dir,
      opts: opts
    } do
      accepted =
        Enum.find(Enum.reverse(Enum.to_list(7_000..8_100//25)), fn n ->
          match?({:ok, _}, Execution.claim_document(request(run_dir, ["/bin/sh", "-c", String.duplicate("y", n)])))
        end)

      assert is_integer(accepted)
      req = request(run_dir, ["/bin/sh", "-c", String.duplicate("y", accepted)])
      assert {:ok, prepared} = Execution.prepare(fs(), req, opts)
      :ok = track(Execution.identity(prepared))
      data = Execution.started_data(prepared)
      :ok = Execution.abandon(prepared)
      assert File.stat!(Path.join(run_dir, "gates/gr_0001.claim.1")).size <= 8192
      assert match?({:dead, _}, Execution.reconcile(fs(), run_dir, expected_from(data), opts))
    end
  end

  describe "cleanup truth (C3-M5)" do
    test "a chmod fault on the temp closes the descriptor before removing it; cleanup removed", %{
      run_dir: run_dir,
      opts: opts
    } do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :chmod, fn [name, _] -> String.ends_with?(name, ".tmp") end, {:error, :eperm})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "chmod", cleanup: "removed"}}, result)
      trace = FaultFs.trace(fault_fs)
      open_index = Enum.find_index(trace, &match?({:open, _, _}, &1))
      assert Enum.any?(Enum.drop(trace, open_index), &match?({:close}, &1)), "the opened descriptor is closed"
      refute Enum.any?(gates_names(run_dir), &String.ends_with?(&1, ".tmp"))
    end

    test "a write fault whose temp cannot be removed names the temp as residue", %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :write, 1, {:error, :eio})
      FaultFs.inject(fault_fs, :rm, fn [name] -> String.ends_with?(name, ".tmp") end, {:error, :eacces})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)

      assert {:error, %{clause: "claim_unpublished", stage: "write", cleanup: "cleanup_required", residue: [residue]}} =
               result

      assert residue =~ ~r/\Agates\/gr_0001\.claim\.1\.[A-Za-z0-9_-]+\.tmp\z/
      assert Enum.any?(gates_names(run_dir), &String.ends_with?(&1, ".tmp")), "the temp really remains"
    end

    test "a temp removal failure after a won link retracts the final AND fsyncs the directory after the retraction", %{
      run_dir: run_dir,
      opts: opts
    } do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :rm, fn [name] -> String.ends_with?(name, ".tmp") end, {:error, :eacces})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)

      assert {:error,
              %{
                clause: "claim_unpublished",
                stage: "rm_temp",
                cleanup: "cleanup_required",
                residue: [_tmp],
                final: "removed"
              }} = result

      trace = FaultFs.trace(fault_fs)
      final_rm = Enum.find_index(trace, &(&1 == {:rm, "gr_0001.claim.1"}))
      assert final_rm, "the final was retracted"

      assert Enum.any?(Enum.drop(trace, final_rm + 1), &match?({:dir_sync, "gates"}, &1)),
             "a dir_sync AFTER the final's removal"

      refute File.exists?(Path.join(run_dir, "gates/gr_0001.claim.1"))
    end
  end

  # ---- C3 successor 2: consistency, real setup records, temp sync truth, error arms, supplemental API ----

  describe "settlement consistency (C3-S1)" do
    test "a proven pass can never carry leftovers: settled=1 with leftovers=2 is a malformed EXIT, not a pass", %{
      run_dir: run_dir,
      opts: opts
    } do
      fake = Path.join(run_dir, "fake_helper")

      File.write!(
        fake,
        "#!/bin/sh\nprintf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\n'\nread cmd\nprintf 'RELEASED\\n'\n" <>
          "printf 'EXIT kind=exited status=0 escaped=unknown settled=1 leftovers=2 proof=gone\\n'\nread cmd\n" <>
          "printf 'DEAD reason=command kind=unknown settled=1 leftovers=0 proof=gone escaped=unknown\\n'\n"
      )

      File.chmod!(fake, 0o700)

      assert {:ok, prepared} =
               Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), [
                 {:helper, fake} | Keyword.delete(opts, :helper)
               ])

      assert {:ok, persisted} = journaled(prepared, run_dir, 1)
      assert {:ok, ack} = Execution.ack(prepared, persisted)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert match?({:error, %{clause: "await_failed", record: "malformed EXIT"}}, Execution.await(running, opts))
    end

    for {label, trailer} <- [
          {"settled=1 proof=alive", "settled=1 leftovers=0 proof=alive"},
          {"settled=1 leftovers=unknown", "settled=1 leftovers=unknown proof=gone"},
          {"settled=0 leftovers=0 proof=gone", "settled=0 leftovers=0 proof=gone"}
        ] do
      test "a contradictory DEAD trailer (#{label}) is settle_unproven, never a settlement", %{
        run_dir: run_dir,
        opts: opts
      } do
        fake = Path.join(run_dir, "fake_helper")

        File.write!(
          fake,
          "#!/bin/sh\nprintf 'READY guardian=1 worker=2 pgid=2 start=1.000000\\n'\nread cmd\nprintf 'DEAD reason=command kind=unknown #{unquote(trailer)} escaped=unknown\\n'\n"
        )

        File.chmod!(fake, 0o700)

        assert {:ok, prepared} =
                 Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), [
                   {:helper, fake} | Keyword.delete(opts, :helper)
                 ])

        assert match?({:error, %{clause: "settle_unproven"}}, Execution.abandon(prepared))
      end
    end

    test "the REAL pre-worker setup record (a pre-existing stdout object) is prepare_failed with its closed class and cleanup, no worker to settle",
         %{run_dir: run_dir, opts: opts} do
      gates = Path.join(run_dir, "gates")
      File.mkdir_p!(gates)
      File.chmod!(gates, 0o700)
      File.write!(Path.join(gates, "gr_0001.1.out"), "")
      result = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)

      assert match?(
               {:error, %{clause: "prepare_failed", class: "open_stdout", cleanup: "none", settle: %{worker: false}}},
               result
             )

      assert "gr_0001.1.out" in gates_names(run_dir), "the pre-existing object is foreign to this attempt: never removed"
      File.rm!(Path.join(gates, "gr_0001.1.out"))
      File.write!(Path.join(gates, "gr_0001.1.err"), "")
      result = Execution.prepare(fs(), request(run_dir, ["/bin/sleep", "30"]), opts)

      assert match?(
               {:error, %{clause: "prepare_failed", class: "open_stderr", cleanup: "removed", settle: %{worker: false}}},
               result
             )

      assert gates_names(run_dir) == ["gr_0001.1.err"],
             "our created stdout object was removed by the guardian; the foreign stderr object stays"
    end
  end

  describe "temp removal durability (C3-S2)" do
    test "a chmod fault whose cleanup fsync fails is removed_unsynced, with close, rm and dir_sync in the trace", %{
      run_dir: run_dir,
      opts: opts
    } do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :chmod, fn [name, _] -> String.ends_with?(name, ".tmp") end, {:error, :eperm})
      FaultFs.inject(fault_fs, :dir_sync, fn _ -> true end, {:error, :eio})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "chmod", cleanup: "removed_unsynced"}}, result)
      trace = FaultFs.trace(fault_fs)
      rm_index = Enum.find_index(trace, &match?({:rm, _}, &1))
      assert rm_index, "the temp was removed"
      assert Enum.any?(Enum.take(trace, rm_index), &match?({:close}, &1)), "the descriptor was closed before the removal"

      assert Enum.any?(Enum.drop(trace, rm_index + 1), &match?({:dir_sync, "gates"}, &1)),
             "a dir_sync was attempted AFTER the removal"
    end

    test "a claim conflict whose temp cleanup fsync fails keeps the foreign final untouched AND states the temp as removed_unsynced",
         %{run_dir: run_dir, opts: opts} do
      gates = Path.join(run_dir, "gates")
      File.mkdir_p!(gates)
      File.chmod!(gates, 0o700)
      claim_path = Path.join(gates, "gr_0001.claim.1")
      File.write!(claim_path, ~s({"schema":"ai-orchestrator/gate-claim","schema_version":1,"foreign":true}\n))
      before = File.read!(claim_path)
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :dir_sync, fn _ -> true end, {:error, :eio})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)

      assert match?(
               {:error, %{clause: "claim_conflict", cleanup: "foreign_final_untouched", temp: "removed_unsynced"}},
               result
             )

      assert File.read!(claim_path) == before
      refute Enum.any?(gates_names(run_dir), &String.ends_with?(&1, ".tmp"))
    end

    test "the write-fault temp cleanup is durable: rm then a successful dir_sync, labelled removed", %{
      run_dir: run_dir,
      opts: opts
    } do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :write, 1, {:error, :eio})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "write", cleanup: "removed"}}, result)
      trace = FaultFs.trace(fault_fs)
      rm_index = Enum.find_index(trace, &match?({:rm, _}, &1))
      assert Enum.any?(Enum.drop(trace, rm_index + 1), &match?({:dir_sync, "gates"}, &1))
    end
  end

  describe "validation and filesystem error arms (C3-S3)" do
    test "a filesystem error while checking or creating the gates directory is a named rejection, never a crash", %{
      run_dir: run_dir,
      opts: opts
    } do
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :lstat, 2, {:error, :eio})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "gates_dir", class: "eio", cleanup: "none"}}, result)
      fault_fs = FaultFs.new()
      FaultFs.inject(fault_fs, :mkdir, 1, {:error, :eacces})
      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)
      assert match?({:error, %{clause: "claim_unpublished", stage: "mkdir", class: "eacces", cleanup: "none"}}, result)
    end

    for {label, bad} <- [
          {"invalid UTF-8 in argv", %{command_argv: ["/usr/bin/true", <<255>>]}},
          {"NUL in argv", %{command_argv: ["/usr/bin/true", "a" <> <<0>> <> "b"]}},
          {"NUL in run_id", %{run_id: "run" <> <<0>>}},
          {"invalid UTF-8 in repo_root", %{repo_root: "/tmp/" <> <<255>>}}
        ] do
      test "#{label} is invalid_request before any encoding, spawn or file", %{run_dir: run_dir, opts: opts} do
        req = Map.merge(request(run_dir, ["/bin/sleep", "30"]), unquote(Macro.escape(bad)))
        assert match?({:error, %{clause: "invalid_request"}}, Execution.claim_document(req))
        assert {:error, rejection} = Execution.prepare(fs(), req, opts)
        assert rejection.clause == "invalid_request"
        refute inspect(rejection) =~ "255", "no encoded term leaks"
        assert File.ls!(run_dir) == []
      end
    end
  end

  describe "supplemental Execution API (evidence/3, durations)" do
    test "evidence/3 hashes exactly the attempt's private files under run_dir/gates and refuses anything else", %{
      run_dir: run_dir,
      opts: opts
    } do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "echo out-line; echo err-line >&2; exit 0"], opts)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert {:exit, outcome} = Execution.await(running, opts)
      assert {:ok, %{"stdout_hash" => out, "stderr_hash" => err}} = Execution.evidence(run_dir, "gr_0001", 1)
      assert out == outcome["stdout_hash"] and err == outcome["stderr_hash"]
      File.rm!(Path.join(run_dir, "gates/gr_0001.1.err"))

      assert match?(
               {:error, %{clause: "output_unreadable", which: "err", class: "enoent"}},
               Execution.evidence(run_dir, "gr_0001", 1)
             )

      for bad <- [
            {run_dir, "../etc", 1},
            {run_dir, "gr_0001/../x", 1},
            {run_dir, "", 1},
            {"relative", "gr_0001", 1},
            {run_dir, "gr_0001", 3},
            {run_dir, :gr, 1},
            {nil, "gr_0001", 1}
          ] do
        {d, g, a} = bad
        assert match?({:error, %{clause: "invalid_request"}}, Execution.evidence(d, g, a)), inspect(bad)
      end
    end

    test "durations are measured from the recorded release: exit and owner expire carry them, a never-released handle carries none",
         %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "sleep 0.3; exit 0"], opts)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert {:exit, %{"duration_ms" => d}} = Execution.await(running, opts)
      assert d >= 250

      # each handle gets its own run directory: one journal, one seq 1 each
      run_dir2 = Path.join(run_dir, "r2")
      File.mkdir_p!(run_dir2)
      {prepared, ack} = acked(run_dir2, ["/bin/sh", "-c", "sleep 30"], opts, %{gate_run_id: "gr_0002"})
      assert {:ok, running} = Execution.release(prepared, ack)
      Process.sleep(300)
      assert {:timeout, %{kind: "timeout", duration_ms: expire_d} = termination} = Execution.expire(running)
      assert expire_d >= 250 and not Map.has_key?(termination, :backstop)

      run_dir3 = Path.join(run_dir, "r3")
      File.mkdir_p!(run_dir3)
      {prepared, ack} = acked(run_dir3, ["/bin/sh", "-c", "echo ran > ran; sleep 30"], opts, %{gate_run_id: "gr_0003"})
      Process.put(:gate_exec_now, @deadline)
      assert {:error, rejection} = Execution.release(prepared, ack)

      assert rejection.clause == "deadline_expired" and not Map.has_key?(rejection, :duration_ms),
             "never released: no duration is invented"

      refute File.exists?(Path.join(run_dir, "ran"))
    end

    test "the native backstop termination carries a measured duration from the release", %{run_dir: run_dir, opts: opts} do
      {prepared, ack} = acked(run_dir, ["/bin/sh", "-c", "sleep 30 & sleep 30"], opts, %{deadline_unix: @now + 1})
      identity = Execution.identity(prepared)
      assert {:ok, running} = Execution.release(prepared, ack)
      assert wait_until(fn -> dead?(identity) end, 8_000)
      Process.put(:gate_exec_now, @now + 3)
      assert {:timeout, %{backstop: true, duration_ms: d}} = Execution.await(running, opts)
      assert d >= 900
    end
  end

  # ---- C3-A1: already-absent removals are sync-or-unsynced, never phantom residue ----

  describe "absent removals (C3-A1)" do
    test "a temp already gone (real enoent) at the chmod-fault cleanup: the absence is synced and labelled absent; with a failing sync, absent_unsynced",
         %{run_dir: run_dir, opts: opts} do
      for {sync_fault, label} <- [{false, "absent"}, {true, "absent_unsynced"}] do
        dir = Path.join(run_dir, "r_#{label}")
        File.mkdir_p!(dir)
        fault_fs = FaultFs.new()
        FaultFs.inject(fault_fs, :chmod, fn [name, _] -> String.ends_with?(name, ".tmp") end, {:error, :eperm})
        # the hook REALLY removes the temp immediately before SystemFs.rm: a real enoent
        FaultFs.inject(
          fault_fs,
          :rm,
          fn [name] -> String.ends_with?(name, ".tmp") end,
          {:hook, fn -> remove_temps!(dir) end}
        )

        if sync_fault, do: FaultFs.inject(fault_fs, :dir_sync, fn _ -> true end, {:error, :eio})
        result = Execution.prepare(fault_fs, request(dir, ["/bin/sh", "-c", "sleep 30"]), opts)
        assert match?({:error, %{clause: "claim_unpublished", stage: "chmod", cleanup: ^label}}, result), label
        trace = FaultFs.trace(fault_fs)
        rm_index = Enum.find_index(trace, &match?({:rm, _}, &1))

        assert Enum.any?(Enum.drop(trace, rm_index + 1), &match?({:dir_sync, "gates"}, &1)),
               "a dir_sync was attempted after the (absent) removal"

        assert Enum.reject(gates_names(dir), &String.ends_with?(&1, [".out", ".err"])) == [],
               "gates holds no claim objects"
      end
    end

    test "a final already gone (real enoent) between the token read and the retraction: reported absent (synced or unsynced), never residue, never a phantom foreign final",
         %{run_dir: run_dir, opts: opts} do
      for {sync_fault, label} <- [{false, "absent"}, {true, "absent_unsynced"}] do
        dir = Path.join(run_dir, "f_#{label}")
        File.mkdir_p!(dir)
        fault_fs = FaultFs.new()
        final = Path.join(dir, "gates/gr_0001.claim.1")
        # the publication fsync fails; then the hook REALLY removes the final before SystemFs.rm
        FaultFs.inject(fault_fs, :dir_sync, 1, {:error, :eio})

        FaultFs.inject(
          fault_fs,
          :rm,
          fn [name] -> name == "gr_0001.claim.1" end,
          {:hook, fn -> remove_final!(final) end}
        )

        if sync_fault, do: FaultFs.inject(fault_fs, :dir_sync, fn _ -> true end, {:error, :eio})
        result = Execution.prepare(fault_fs, request(dir, ["/bin/sh", "-c", "sleep 30"]), opts)
        assert match?({:error, %{clause: "claim_unpublished", stage: "dir_sync", cleanup: ^label}}, result), label
        {:error, rejection} = result
        refute Map.has_key?(rejection, :residue), "an absent object is never residue"
        refute File.exists?(final)
        assert Enum.reject(gates_names(dir), &String.ends_with?(&1, [".out", ".err"])) == []
        trace = FaultFs.trace(fault_fs)
        rm_index = Enum.find_index(trace, &(&1 == {:rm, "gr_0001.claim.1"}))

        assert Enum.any?(Enum.drop(trace, rm_index + 1), &match?({:dir_sync, "gates"}, &1)),
               "a dir_sync was attempted after the absent removal"
      end
    end

    test "a final absent before the token could be read is reported absent with its sync, with prior temp residue carried independently",
         %{run_dir: run_dir, opts: opts} do
      fault_fs = FaultFs.new()
      final = Path.join(run_dir, "gates/gr_0001.claim.1")
      FaultFs.inject(fault_fs, :rm, fn [name] -> String.ends_with?(name, ".tmp") end, {:error, :eacces})
      # after the won link the temp cannot be removed; the final vanishes before the retraction reads it
      FaultFs.inject(
        fault_fs,
        :read,
        fn [name] -> name == "gr_0001.claim.1" end,
        {:hook, fn -> remove_final!(final) end}
      )

      result = Execution.prepare(fault_fs, request(run_dir, ["/bin/sh", "-c", "sleep 30"]), opts)

      assert {:error,
              %{
                clause: "claim_unpublished",
                stage: "rm_temp",
                cleanup: "cleanup_required",
                residue: [tmp],
                final: "absent"
              }} = result

      assert String.ends_with?(tmp, ".tmp")
      refute File.exists?(final)
    end
  end

  # the hook REALLY removes the final and proves it (a real enoent follows for SystemFs.rm/read)
  defp remove_final!(final) do
    :ok = File.rm(final)
    true
  end

  defp remove_temps!(dir) do
    for name <- File.ls!(Path.join(dir, "gates")),
        String.ends_with?(name, ".tmp"),
        do: File.rm!(Path.join([dir, "gates", name]))

    true
  end
end
