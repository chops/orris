defmodule AiOrchestrator.Run.RecoveryAcquisitionRedTest do
  @moduledoc """
  U1a-A RED/interface, revision 4 (docs/contracts/recovery-acquisition.org): `AiOrchestrator.Run.Recovery` as a
  consumer over the existing Journal APIs - cold evidence, acquisition with a closed refusal domain that admits
  uncertainty, explicit release, and an explicit trapping-caller precondition. Baseline controls prove on the
  EXISTING Writer/RunLock/Reader that every contention, fault and lifetime mechanic the rows rely on is real
  and reachable before any missing-API row is judged. Every owned subject carries a role and is reaped in
  dependency order: callers, Writers, OS holders (through a live owner process or an identity-bound wait), Fs
  agents; the directory last. No historical-pid signals anywhere.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.ProcessIdentity
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @canary "RECOVERY-ACQUISITION-PRIVATE-CANARY"
  @helper Path.expand("../support/run_lock_process.exs", __DIR__)
  @fixture Path.expand("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")
  @corpus @fixture |> File.read!() |> String.split("\n", trim: true) |> Enum.take(6)
  @next @fixture
        |> File.read!()
        |> String.split("\n", trim: true)
        |> Enum.at(6)
        |> Jason.decode!()
        |> Map.drop(["schema_version", "prev_line_sha256"])
  @moduletag timeout: 90_000
  @mutating [:write, :sync, :rename, :dir_sync, :rm, :mkdir, :mkdir_p, :chmod, :link, :rmdir]
  @none %{lock: :none, registration: :none, descriptor: :none}
  @released %{lock: :released, registration: :released, descriptor: :none}
  @closed %{lock: :released, registration: :released, descriptor: :closed}
  @unknown %{lock: :unknown, registration: :unknown, descriptor: :unknown}
  @roles [:caller, :writer, :holder, :fs]

  setup do
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "recovery_acq_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(@corpus, "\n") <> "\n")
    {:ok, tracked} = Agent.start(fn -> [] end)
    FixedClock.reset()
    on_exit(fn -> teardown!(tracked, dir) end)
    %{dir: dir, tracked: tracked}
  end

  # ---- the reaper (RA-M8): explicit roles, dependency order, bounded, survivors raised ----

  # the registrations are RE-READ per role: stopping the callers first lets a hook registration made by a
  # still-running caller land before the Writers are reaped. Survivors keep their resources (no directory
  # removal) and are raised.
  defp teardown!(tracked, dir) do
    survivors =
      Enum.reduce(@roles, [], fn role, acc ->
        subjects = Agent.get(tracked, &Enum.reverse/1)

        Enum.reduce(subjects, acc, fn
          {^role, subject}, acc -> reap(role, subject, acc)
          _other, acc -> acc
        end)
      end)

    Agent.stop(tracked)

    if survivors == [] do
      File.rm_rf!(dir)
    else
      raise "tracked subjects survived the reaper (resources retained): #{inspect(survivors)}"
    end
  end

  # a holder is stopped through its owner (Port close -> helper stdin EOF -> proven exit); an owner already gone
  # means the Port closed with it: the helper's death is proven identity-bound or reported as unproven
  defp reap(:holder, %{owner: owner} = holder, acc) do
    case stop_holder(owner) do
      {:ok, _proof} -> acc
      {:error, :owner_already_dead} -> reap_orphaned_holder(holder, acc)
      {:error, why} -> [{:holder, why} | acc]
    end
  end

  defp reap(role, pid, acc) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> acc
      after
        5_000 -> [{role, pid} | acc]
      end
    else
      acc
    end
  end

  defp reap_orphaned_holder(%{identity: identity}, acc) do
    if is_map(identity) and holder_status(identity) == :dead,
      do: acc,
      else: [{:holder, {:unproven, identity}} | acc]
  end

  defp track!(tracked, role, subject) when role in @roles, do: Agent.update(tracked, &[{role, subject} | &1])
  defp recovery, do: Module.concat(["AiOrchestrator", "Run", "Recovery"])
  defp lock_opts(overrides \\ []), do: Keyword.merge([supervisor_instance: "sup_recovery"], overrides)

  # real overriding keyword semantics: one owner_status, the last word wins
  defp live_opts(overrides \\ []),
    do: Keyword.merge(lock_opts(pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end), overrides)

  defp journal(dir), do: File.read!(Path.join(dir, "events.jsonl"))
  defp lock_files(dir), do: dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "run.lock.")) |> Enum.sort()
  defp ops(fs, since), do: fs |> FaultFs.trace() |> Enum.drop(since) |> Enum.map(&elem(&1, 0))
  defp mutations(fs, since), do: fs |> ops(since) |> Enum.filter(&(&1 in @mutating))
  defp trace_len(fs), do: length(FaultFs.trace(fs))
  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))
  defp final_lock_name?(name), do: Regex.match?(~r/^run\.lock\.[0-9]+$/, name)

  defp fault_fs!(tracked) do
    fs = FaultFs.new()
    {_, agent} = fs
    Process.unlink(agent)
    track!(tracked, :fs, agent)
    fs
  end

  defp open!(dir, tracked, opts) do
    {:ok, w, opened} = Writer.open(dir, opts)
    track!(tracked, :writer, w)
    {w, opened}
  end

  defp corrupt_middle!(dir) do
    [l1, l2 | rest] = @corpus

    File.write!(
      Path.join(dir, "events.jsonl"),
      Enum.join([l1, String.replace(l2, "run_spec_loaded", @canary) | rest], "\n") <> "\n"
    )
  end

  defp torn_tail!(dir), do: File.write!(Path.join(dir, "events.jsonl"), "{\"torn", [:append])
  defp inject_read_fault!(fs, name), do: FaultFs.inject(fs, :read, fn [n] -> n == name end, {:error, {:seam, @canary}})

  # the supported PRE-operation hook on the descriptor open ([:append]): after the initial read and repair, before
  # the open, the open's real return preserved. It runs INSIDE the allocated Writer, which registers ITSELF for
  # the reaper and sends its pid to the test: the identity-bound DOWN witness of every refusal path (RA-M8).
  defp arm_before_descriptor_open!(fs, parent, tracked, side_effect) do
    FaultFs.inject(
      fs,
      :open,
      fn [name, modes] -> name == "events.jsonl" and :append in modes end,
      {:hook,
       fn ->
         track!(tracked, :writer, self())
         send(parent, {:hook_invoked, self()})
         side_effect.()
         true
       end}
    )
  end

  # the descriptor close is the FIRST close of the teardown; the lock leg is the final-name rm
  defp inject_close_legs!(fs, faults), do: Enum.each(faults, &inject_close_leg!(fs, &1))

  defp inject_close_leg!(fs, :rm),
    do: FaultFs.inject(fs, :rm, fn [name] -> final_lock_name?(name) end, {:error, {:seam, @canary}})

  defp inject_close_leg!(fs, :close), do: FaultFs.inject(fs, :close, count(fs, :close) + 1, {:error, {:seam, @canary}})

  # ---- the OS holder harness (RA-M8/RA-M10): a live, unlinked owner process owns the Port ----
  #
  # The owner opens the Port and serves requests from the start (readiness is not a precondition for stop):
  # it collects the helper's status lines, learns the helper's OS identity (pid + ps start time) from the first
  # line through an injectable, test-gateable lookup, answers `{:ready, from}` once the first line is in and
  # `{:exit_status, from}` once the Port's exit_status has been observed, and on `{:stop, from}` CLOSES the Port -
  # the helper sees stdin EOF and exits silently - then proves the exit: an exit_status already observed, or an
  # identity-bound death (bounded). No numeric signal is ever sent; nil Port.info is never taken as proof; an
  # unproven exit is reported as such and the holder stays tracked. Every reply is correlated by the owner pid.

  @readiness_ms 20_000
  @stop_ms 20_000

  defp start_holder!(dir, tracked, hold_ms \\ 30_000, opts \\ []) do
    owner = spawn_holder_owner(dir, hold_ms, opts)
    track!(tracked, :holder, %{owner: owner, identity: nil})
    send(owner, {:ready, self()})

    receive do
      {:ready, ^owner, first, identity} ->
        Agent.update(tracked, fn subjects ->
          Enum.map(subjects, fn
            {:holder, %{owner: ^owner}} -> {:holder, %{owner: owner, identity: identity}}
            other -> other
          end)
        end)

        {owner, first, identity}
    after
      @readiness_ms -> flunk("holder owner did not report readiness")
    end
  end

  # `identity_lookup:` is the test seam for the identity capture (default: the real lookup); a control gates it
  # to make "the helper exited BEFORE its identity was captured" deterministic
  defp spawn_holder_owner(dir, hold_ms, opts) do
    lookup = Keyword.get(opts, :identity_lookup, &holder_identity/1)

    spawn(fn ->
      Process.flag(:trap_exit, true)
      port = open_holder_port(dir, hold_ms)

      holder_owner_loop(%{
        port: port,
        buffer: "",
        first: nil,
        identity: :pending,
        lookup: lookup,
        exit_status: nil,
        waiting: [],
        exit_waiting: [],
        identity_waiting: []
      })
    end)
  end

  defp open_holder_port(dir, hold_ms) do
    Port.open(
      {:spawn_executable, System.find_executable("elixir")},
      [:binary, :exit_status, :stderr_to_stdout, args: code_path_args() ++ [@helper, dir, Integer.to_string(hold_ms)]]
    )
  end

  defp holder_owner_loop(%{port: port} = st) do
    receive do
      {^port, {:data, chunk}} ->
        st |> ingest(chunk) |> holder_owner_loop()

      {^port, {:exit_status, status}} ->
        st |> Map.put(:exit_status, status) |> publish_ready() |> publish_exit() |> holder_owner_loop()

      {:ready, from} ->
        st |> Map.update!(:waiting, &[from | &1]) |> publish_ready() |> holder_owner_loop()

      {:exit_status, from} ->
        st |> Map.update!(:exit_waiting, &[from | &1]) |> publish_exit() |> holder_owner_loop()

      {:identity_captured, identity} ->
        st |> Map.put(:identity, identity) |> publish_ready() |> publish_identity() |> holder_owner_loop()

      {:await_identity, from} ->
        st |> Map.update!(:identity_waiting, &[from | &1]) |> publish_identity() |> holder_owner_loop()

      {:identity, from} ->
        send(from, {:identity, self(), known_identity(st)})
        holder_owner_loop(st)

      {:stop, from} ->
        send(from, {:stopped, self(), stop_port(st)})
        # ends the linked lookup task too, so nothing outlives the owner
        exit(:shutdown)

      {:EXIT, _pid, _reason} ->
        holder_owner_loop(st)
    end
  end

  defp known_identity(%{identity: :pending}), do: nil
  defp known_identity(%{identity: identity}), do: identity

  defp ingest(%{first: nil} = st, chunk) do
    case String.split(st.buffer <> chunk, "\n", parts: 2) do
      [line, _rest] ->
        first = Jason.decode!(line)
        owner = self()
        lookup = st.lookup
        # the lookup never blocks the owner: it runs linked, reporting when it has an answer
        spawn_link(fn -> send(owner, {:identity_captured, lookup.(first)}) end)
        publish_ready(%{st | buffer: "", first: first})

      [partial] ->
        %{st | buffer: partial}
    end
  end

  defp ingest(st, _chunk), do: st

  # readiness = the first line WITH its identity captured, or the first line once the helper has exited (its
  # identity may then be uncapturable: reported as nil, never invented), or an exit before any line
  defp publish_ready(%{waiting: []} = st), do: st
  defp publish_ready(%{first: nil, exit_status: nil} = st), do: st
  defp publish_ready(%{identity: :pending, exit_status: nil} = st), do: st

  defp publish_ready(st) do
    first = st.first || %{"status" => "exited", "exit_status" => st.exit_status}
    Enum.each(st.waiting, &send(&1, {:ready, self(), first, known_identity(st)}))
    %{st | waiting: []}
  end

  # the COMPLETED lookup result, answered only once the lookup has reported (held while :pending)
  defp publish_identity(%{identity_waiting: []} = st), do: st
  defp publish_identity(%{identity: :pending} = st), do: st

  defp publish_identity(st) do
    Enum.each(st.identity_waiting, &send(&1, {:identity_completed, self(), st.identity}))
    %{st | identity_waiting: []}
  end

  # the observed exit_status, answered only once it exists
  defp publish_exit(%{exit_waiting: []} = st), do: st
  defp publish_exit(%{exit_status: nil} = st), do: st

  defp publish_exit(st) do
    Enum.each(st.exit_waiting, &send(&1, {:exit_status, self(), st.exit_status}))
    %{st | exit_waiting: []}
  end

  # close the owned Port (helper: stdin EOF -> silent exit); proof = observed exit_status or identity-bound death
  defp stop_port(%{port: port, exit_status: nil} = st) do
    # an exit already in the mailbox is the proof; otherwise closing the Port (stdin EOF) ends the helper and
    # its death is proven identity-bound (a closed Port delivers no exit_status)
    receive do
      {^port, {:exit_status, status}} -> {:exited, status}
    after
      0 ->
        if Port.info(port) != nil, do: Port.close(port)
        stop_proof(st)
    end
  end

  defp stop_port(%{exit_status: status}), do: {:exited, status}

  defp stop_proof(%{identity: %{"pid_start" => start} = identity}) when is_binary(start) do
    case holder_status(identity) do
      :dead -> {:dead, identity}
      other -> {:unproven, other, identity}
    end
  end

  # nil / pending identity AND no exit evidence: unproven, never coerced
  defp stop_proof(st), do: {:unproven, :no_identity, known_identity(st)}

  defp holder_identity(%{"pid" => pid}) do
    case ProcessIdentity.current(pid) do
      {:ok, start} -> %{"pid" => pid, "pid_start" => start}
      _ -> %{"pid" => pid, "pid_start" => nil}
    end
  end

  defp holder_identity(_other), do: nil

  # {:ok, proof} ONLY on a proven exit ({:exited, status} | {:dead, identity}); an unproven exit keeps the holder
  # tracked
  defp stop_holder(owner) do
    if Process.alive?(owner) do
      ref = Process.monitor(owner)
      send(owner, {:stop, self()})

      receive do
        {:stopped, ^owner, {:exited, _status} = proof} -> proven(await_down(owner, ref), proof)
        {:stopped, ^owner, {:dead, _identity} = proof} -> proven(await_down(owner, ref), proof)
        {:stopped, ^owner, {:unproven, why, identity}} -> {:error, {:unproven, why, identity}}
        {:DOWN, ^ref, :process, ^owner, _} -> {:error, :owner_died_while_stopping}
      after
        @stop_ms -> {:error, :stop_timeout}
      end
    else
      {:error, :owner_already_dead}
    end
  end

  defp proven(:ok, proof), do: {:ok, proof}
  defp proven({:error, why}, _proof), do: {:error, why}

  # a bounded, correlated wait for the owner's OBSERVED exit_status (the helper already exited)
  defp await_exit_status!(owner) do
    send(owner, {:exit_status, self()})

    receive do
      {:exit_status, ^owner, status} -> status
    after
      @stop_ms -> flunk("no exit_status observed by the holder owner")
    end
  end

  # bounded, correlated wait for the owner's CONSUMED lookup result (never answered while pending)
  defp await_identity_completed!(owner) do
    send(owner, {:await_identity, self()})

    receive do
      {:identity_completed, ^owner, identity} -> identity
    after
      @stop_ms -> flunk("the identity lookup never completed at the owner")
    end
  end

  defp await_down!(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> flunk("#{inspect(pid)} did not exit")
    end
  end

  defp holder_identity_of(owner) do
    send(owner, {:identity, self()})

    receive do
      {:identity, ^owner, identity} -> identity
    after
      5_000 -> nil
    end
  end

  defp await_down(pid, ref) do
    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> {:error, :did_not_exit}
    end
  end

  # identity-bound liveness of the helper (pid + ps start time), bounded (10 s)
  defp holder_status(%{"pid_start" => nil}), do: :unknown

  defp holder_status(%{"pid_start" => start} = identity) when is_binary(start) do
    Enum.reduce_while(1..100, :live, fn _, _ ->
      case ProcessIdentity.owner_status(identity) do
        :dead -> {:halt, :dead}
        _ -> Process.sleep(100) && {:cont, :live}
      end
    end)
  end

  # untracks a holder after a PROVEN stop
  defp untrack_holder!(tracked, owner),
    do: Agent.update(tracked, &Enum.reject(&1, fn subject -> match?({:holder, %{owner: ^owner}}, subject) end))

  # runs `fun.(holder_pid)` under a real second-OS-process holder; the readiness assertion sits INSIDE the try, so
  # a readiness failure still reaches the stop
  defp with_os_holder!(dir, tracked, fun) do
    {owner, first, _identity} = start_holder!(dir, tracked)

    try do
      assert %{"status" => "acquired", "pid" => holder_pid} = first
      fun.(holder_pid)
    after
      stop_holder!(tracked, owner)
    end
  end

  # an orderly stop through the live owner; a PROVEN stop leaves the reaper's list and its PROOF is returned; an
  # unproven one raises a RuntimeError (never an assertion, so a row's assert_raise cannot swallow it) and stays
  # tracked ({:exited, status} | {:dead, identity})
  defp stop_holder!(tracked, owner) do
    case stop_holder(owner) do
      {:ok, proof} ->
        untrack_holder!(tracked, owner)
        proof

      {:error, why} ->
        raise "holder stop unproven: #{inspect(why)}"
    end
  end

  defp code_path_args do
    :code.get_path()
    |> Enum.map(&List.to_string/1)
    |> Enum.filter(&String.contains?(&1, "_build"))
    |> Enum.flat_map(&["-pa", &1])
  end

  defp assert_no_canary(term),
    do: refute(String.contains?(inspect(term, limit: :infinity, printable_limit: :infinity), @canary))

  defp assert_down!(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  # a NON-trapping process that runs `fun` and reports; tracked as a caller
  defp spawn_caller!(tracked, fun) do
    parent = self()

    caller =
      spawn(fn ->
        send(parent, {:caller_result, self(), fun.()})

        receive do
          :stop -> :ok
        after
          10_000 -> :ok
        end
      end)

    track!(tracked, :caller, caller)
    caller
  end

  describe "baseline controls (existing API; green before any RED row is judged)" do
    test "C-1 a Writer in a real second OS process refuses Writer.open with run_locked naming its pid", %{
      dir: dir,
      tracked: tracked
    } do
      with_os_holder!(dir, tracked, fn holder_pid ->
        assert {:error, %{clause: "run_locked", owner: %{"pid" => ^holder_pid}}} = Writer.open(dir, lock: lock_opts())
      end)
    end

    test "C-2 a live in-BEAM Writer refuses a second Writer.open with second_live_writer", %{dir: dir, tracked: tracked} do
      {w, _} = open!(dir, tracked, lock: live_opts())

      assert {:error, %{clause: "second_live_writer", generation: 1}} =
               Writer.open(dir, lock: live_opts(supervisor_instance: "sup_2"))

      :ok = Writer.close(w)
    end

    test "C-3 a corrupt middle line: Writer.open refuses with EXACTLY unknown_event_type at_seq 2 echoing the bytes; no lock",
         %{dir: dir} do
      corrupt_middle!(dir)
      bytes = journal(dir)

      assert {:error, %{clause: "unknown_event_type", at_seq: 2, event_type: @canary}} =
               Writer.open(dir, lock: live_opts())

      assert :none = RunLock.owner(SystemFs.new(), dir)
      assert journal(dir) == bytes
    end

    test "C-4 read fault + release-leg fault: writer_open_cleanup_failed with a STRUCTURED nested release; the file remains",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      inject_read_fault!(fs, "events.jsonl")
      inject_close_legs!(fs, [:rm])

      assert {:error,
              %{
                clause: "writer_open_cleanup_failed",
                cause: %{clause: "journal_unreadable"},
                release: %{clause: "release_incomplete"},
                lock_path: "run.lock.1"
              }} = Writer.open(dir, fs: fs, lock: live_opts())

      assert "run.lock.1" in lock_files(dir)
    end

    test "C-5 Reader.load carries seam bytes on an unreadable journal and journal bytes on a corrupt line", %{
      dir: dir,
      tracked: tracked
    } do
      fs = fault_fs!(tracked)
      inject_read_fault!(fs, "events.jsonl")
      assert {:error, %{clause: "journal_unreadable", detail: detail}} = Reader.load(dir, fs: fs)
      assert String.contains?(detail, @canary)
      corrupt_middle!(dir)
      assert {:error, %{clause: "unknown_event_type", at_seq: 2, event_type: @canary}} = Reader.load(dir)
    end

    for {label, mode} <- [{"read fault", :read_fault}, {"torn tail", :torn_tail}] do
      test "C-6 positive hook witness (#{label}): the pre-open hook fires inside the Writer before a descriptor open that succeeds",
           %{dir: dir, tracked: tracked} do
        fs = fault_fs!(tracked)
        parent = self()

        arm_before_descriptor_open!(fs, parent, tracked, fn ->
          case unquote(mode) do
            :read_fault -> inject_read_fault!(fs, "events.jsonl")
            :torn_tail -> torn_tail!(dir)
          end
        end)

        {w, %{repair: nil}} = open!(dir, tracked, fs: fs, clock: FixedClock, lock: live_opts())
        assert_receive {:hook_invoked, ^w}, 1_000
        assert {:open, "events.jsonl", [:append]} in FaultFs.trace(fs)

        case unquote(mode) do
          :read_fault ->
            assert {:error, %{clause: "snapshot_unreadable", source: "journal"}} = Writer.verified(w)

          :torn_tail ->
            assert {:ok, %{token: nil, repair: %{action: :truncate_tail}}} = Writer.verified(w)
            assert String.ends_with?(journal(dir), "{\"torn")
        end

        assert :ok = Writer.close(w)
      end
    end

    for {label, legs, faults} <- [
          {"lock leg", ["lock"], [:rm]},
          {"descriptor leg", ["descriptor"], [:close]},
          {"both legs", ["descriptor", "lock"], [:close, :rm]}
        ] do
      test "C-7 Writer.close #{label} failure is close_failed with STRUCTURED leg names only (no nested clause)", %{
        dir: dir,
        tracked: tracked
      } do
        fs = fault_fs!(tracked)
        {w, _} = open!(dir, tracked, fs: fs, lock: live_opts())
        inject_close_legs!(fs, unquote(faults))

        assert {:error, %{clause: "close_failed", failures: failures} = result} = Writer.close(w)
        assert Enum.sort(Map.keys(result)) == [:clause, :failures]
        assert Enum.map(failures, & &1.leg) == unquote(legs)
        assert Enum.all?(failures, &(Enum.sort(Map.keys(&1)) == [:detail, :leg]))
        refute Process.alive?(w)
        assert :none = RunLock.owner(fs, dir)

        if "lock" in unquote(legs),
          do: assert("run.lock.1" in lock_files(dir)),
          else: refute("run.lock.1" in lock_files(dir))
      end
    end

    test "C-7b RA-M7b: a held file whose token was replaced -> Writer.close reports only close_failed/lock/detail; the FOREIGN",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      {w, _} = open!(dir, tracked, fs: fs, lock: live_opts())
      path = Path.join(dir, "run.lock.1")
      foreign = path |> File.read!() |> Jason.decode!() |> Map.put("token", @canary) |> Jason.encode!()
      File.write!(path, foreign)

      assert {:error, %{clause: "close_failed", failures: [%{leg: "lock", detail: detail}]}} = Writer.close(w)
      assert is_binary(detail)
      assert File.read!(path) == foreign
    end

    test "C-8 RunLock.acquire under a dir_sync fault: lock_unavailable rollback removed_unsynced, file absent", %{
      dir: dir,
      tracked: tracked
    } do
      fs = fault_fs!(tracked)
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:error, {:seam, @canary}})
      assert {:error, %{clause: "lock_unavailable", rollback: "removed_unsynced"}} = RunLock.acquire(fs, dir, live_opts())
      assert lock_files(dir) == []
    end

    test "C-8b RA-M7c: rollback not_ours carries NO path/owner in RunLock's error", %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:error, {:seam, @canary}})

      FaultFs.inject(
        fs,
        :read,
        fn [name] -> name == "run.lock.1" end,
        {:return, {:ok, Jason.encode!(%{"token" => @canary})}}
      )

      assert {:error, %{clause: "lock_unavailable", rollback: "not_ours"} = result} =
               RunLock.acquire(fs, dir, live_opts())

      refute Map.has_key?(result, :path)
      refute Map.has_key?(result, :owner)
    end

    test "C-9 RA-M7a: a preexisting foreign candidate -> candidate_conflict naming a TOKEN-BEARING path, byte-identical, no rm",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      candidate = Path.join(dir, "run.lock.1.#{@canary}.tmp")
      File.write!(candidate, "FOREIGN-CANDIDATE")

      assert {:error, %{clause: "candidate_conflict", path: ^candidate}} =
               RunLock.acquire(fs, dir, live_opts(token: @canary))

      assert String.contains?(Path.basename(candidate), @canary)
      assert File.read!(candidate) == "FOREIGN-CANDIDATE"
      refute Enum.any?(FaultFs.trace(fs), &match?({:rm, _}, &1))
    end

    test "C-10 a Writer killed inside verified/1's re-read: the call EXITs for a TRAPPING caller, DOWN observed, the lock file",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      {w, _} = open!(dir, tracked, fs: fs, lock: live_opts())
      ref = Process.monitor(w)
      FaultFs.inject(fs, :read, fn [name] -> name == "events.jsonl" end, {:hook, fn -> Process.exit(self(), :kill) end})
      assert catch_exit(Writer.verified(w))
      assert_receive {:DOWN, ^ref, :process, ^w, :killed}, 5_000
      assert lock_files(dir) == ["run.lock.1"]
    end

    test "C-11 RA-M6: a NON-trapping linked caller cannot contain the Writer's death: it goes DOWN :killed, the held lock remains",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      parent = self()

      caller =
        spawn_caller!(tracked, fn ->
          {:ok, w, _} = Writer.open(dir, fs: fs, clock: FixedClock, lock: live_opts())
          send(parent, {:allocated_writer, w})

          FaultFs.inject(
            fs,
            :read,
            fn [name] -> name == "events.jsonl" end,
            {:hook, fn -> Process.exit(self(), :kill) end}
          )

          catch_exit(Writer.verified(w))
        end)

      ref = Process.monitor(caller)
      assert_receive {:allocated_writer, w}, 5_000
      track!(tracked, :writer, w)
      assert_receive {:DOWN, ^ref, :process, ^caller, :killed}, 5_000
      refute_received {:caller_result, ^caller, _}
      refute Process.alive?(w)
      assert lock_files(dir) == ["run.lock.1"]
    end

    test "C-12 RA-M8/M10 harness: a readiness failure still reaches the stop; an already-exited holder stops on its OBSERVED exit",
         %{dir: dir, tracked: tracked} do
      # a REAL-identity in-BEAM holder: the helper's RunLock sees this OS process live and is refused
      {w, _} = open!(dir, tracked, lock: lock_opts())

      assert_raise ExUnit.AssertionError, fn ->
        with_os_holder!(dir, tracked, fn _ -> flunk("must not run: the holder was refused") end)
      end

      :ok = Writer.close(w)

      # an already-exited holder (zero hold): the exit_status is OBSERVED through the owner BEFORE stop is asked,
      # and stop's proof is that observed exit - never dead-by-identity, whether or not the identity was captured
      {owner, first, _identity} = start_holder!(dir, tracked, 0)
      assert %{"status" => "acquired"} = first
      status = await_exit_status!(owner)
      assert is_integer(status)
      assert {:exited, ^status} = stop_holder!(tracked, owner)
      assert :none = RunLock.owner(SystemFs.new(), dir)
    end

    test "C-16 RA-M10 deterministic: the helper exits BEFORE its identity is captured (gated lookup) -> the COMPLETED lookup is a dead pid, stop proof = the observed exit",
         %{dir: dir, tracked: tracked} do
      parent = self()

      # the gate is released ONLY by the test's :capture - no timeout ever stands in for the handshake; the lookup
      # task is linked to the owner, which ends after its stop reply
      lookup = fn first ->
        send(parent, {:lookup_entered, self(), first})

        receive do
          :capture -> holder_identity(first)
        end
      end

      owner = spawn_holder_owner(dir, 0, identity_lookup: lookup)
      track!(tracked, :holder, %{owner: owner, identity: nil})
      assert_receive {:lookup_entered, lookup_task, %{"status" => "acquired", "pid" => helper_pid}}, 20_000
      # the helper has exited (observed through the owner) while the lookup is still gated
      status = await_exit_status!(owner)
      # release the gate: the REAL lookup now runs against an exited pid, and the owner must CONSUME its result
      send(lookup_task, :capture)
      assert %{"pid" => ^helper_pid, "pid_start" => nil} = await_identity_completed!(owner)
      assert {:exited, ^status} = stop_holder!(tracked, owner)
      await_down!(owner)
      await_down!(lookup_task)
    end

    test "C-17 RA-M10 negative: nil identity AND no exit evidence is UNPROVEN, never coerced; the test proves the cleanup by its own capture",
         %{dir: dir, tracked: tracked} do
      parent = self()

      # the owner NEVER learns the identity: the gate has no release at all (the linked task ends with the owner)
      lookup = fn first ->
        send(parent, {:first_line, self(), first})

        receive do
          :never -> holder_identity(first)
        end
      end

      owner = spawn_holder_owner(dir, 30_000, identity_lookup: lookup)
      track!(tracked, :holder, %{owner: owner, identity: nil})
      assert_receive {:first_line, lookup_task, %{"status" => "acquired"} = first}, 20_000
      # the TEST captures the identity while the helper is alive and hands it to the reaper BEFORE anything fallible
      identity = holder_identity(first)
      assert %{"pid_start" => start} = identity
      assert is_binary(start)

      Agent.update(tracked, fn subjects ->
        Enum.map(subjects, fn
          {:holder, %{owner: ^owner}} -> {:holder, %{owner: owner, identity: identity}}
          other -> other
        end)
      end)

      assert :live = holder_status_once(identity)
      assert {:error, {:unproven, :no_identity, nil}} = stop_holder(owner)
      # the closed Port ended the helper (stdin EOF): proven by the test's own identity capture; both BEAM subjects
      # are joined
      assert :dead = holder_status(identity)
      await_down!(owner)
      await_down!(lookup_task)
      untrack_holder!(tracked, owner)
    end

    test "C-13 RA-M8/M10 harness: the holder owner dies -> the Port closes, the helper exits on stdin EOF, proven identity-bound",
         %{dir: dir, tracked: tracked} do
      {owner, %{"status" => "acquired"}, identity} = start_holder!(dir, tracked)
      assert :live = holder_status_once(identity)
      ref = Process.monitor(owner)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^ref, :process, ^owner, :killed}, 5_000
      assert :dead = holder_status(identity)
      assert :none = RunLock.owner(SystemFs.new(), dir)
    end

    test "C-14 RA-M10 harness: the REQUESTER dies before readiness is published; the owner still serves stop and proves the exit",
         %{dir: dir, tracked: tracked} do
      parent = self()

      requester =
        spawn(fn ->
          owner = spawn_holder_owner(dir, 30_000, [])
          track!(tracked, :holder, %{owner: owner, identity: nil})
          send(parent, {:owner, owner})
          exit(:requester_died)
        end)

      req_ref = Process.monitor(requester)
      assert_receive {:owner, owner}, 5_000
      assert_receive {:DOWN, ^req_ref, :process, ^requester, :requester_died}, 5_000
      # readiness was never requested by the dead requester; the owner learns the identity on its own
      identity = Enum.find_value(1..100, fn _ -> holder_identity_of(owner) || (Process.sleep(100) && nil) end)
      assert %{"pid_start" => start} = identity
      assert is_binary(start)
      assert :live = holder_status_once(identity)
      stop_holder!(tracked, owner)
      assert :dead = holder_status(identity)
    end

    test "C-15 RA-M9 measured: not_ours leaves a real run.lock.1; a following candidate leg in the SAME directory is run_locked, the candidate never opened",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:error, {:seam, @canary}})

      FaultFs.inject(
        fs,
        :read,
        fn [name] -> name == "run.lock.1" end,
        {:return, {:ok, Jason.encode!(%{"token" => @canary})}}
      )

      assert {:error, %{clause: "lock_unavailable", rollback: "not_ours"}} = Writer.open(dir, fs: fs, lock: live_opts())
      assert "run.lock.1" in lock_files(dir)

      candidate = Path.join(dir, "run.lock.1.#{@canary}.tmp")
      File.write!(candidate, "FOREIGN-CANDIDATE")
      fs2 = fault_fs!(tracked)
      assert {:error, %{clause: "run_locked"}} = Writer.open(dir, fs: fs2, lock: live_opts(token: @canary))
      refute Enum.any?(FaultFs.trace(fs2), &match?({:open, "run.lock.1." <> _, _}, &1))
      assert File.read!(candidate) == "FOREIGN-CANDIDATE"
    end
  end

  defp holder_status_once(identity), do: ProcessIdentity.owner_status(identity)

  describe "A-0 interface" do
    test "Recovery.evidence/2, acquire/2 and release/1 exist" do
      Code.ensure_loaded(recovery())
      assert function_exported?(recovery(), :evidence, 2), "Run.Recovery.evidence/2 does not exist"
      assert function_exported?(recovery(), :acquire, 2), "Run.Recovery.acquire/2 does not exist"
      assert function_exported?(recovery(), :release, 1), "Run.Recovery.release/1 does not exist"
    end
  end

  describe "evidence: cold, never authority (no caller precondition)" do
    test "E-1 the verified evidence shape WITHOUT generation/token; zero mutating ops; no lock file; arbiter unchanged",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      since = trace_len(fs)
      status_before = Ownership.status(dir)
      assert {:ok, evidence} = recovery().evidence(dir, fs: fs)

      assert Enum.sort(Map.keys(evidence)) ==
               [:envelope_version, :last_line_sha256, :lines, :receipt, :repair, :seq, :version_2_from]

      assert %{seq: 6, lines: @corpus, repair: nil} = evidence
      assert mutations(fs, since) == []
      assert lock_files(dir) == []
      assert Ownership.status(dir) == status_before
    end

    test "E-1b evidence needs no trapping caller: a non-trapping caller reads it", %{dir: dir, tracked: tracked} do
      caller = spawn_caller!(tracked, fn -> recovery().evidence(dir, []) end)
      assert_receive {:caller_result, ^caller, {:ok, %{seq: 6}}}, 5_000
      send(caller, :stop)
    end

    for {label, name, source} <- [{"journal", "events.jsonl", "journal"}, {"receipt", "events.head", "receipt"}] do
      test "E-2 an unreadable #{label} is normalized to journal_unavailable stage read source #{source}; canary absent",
           %{dir: dir, tracked: tracked} do
        if unquote(name) == "events.head", do: File.write!(Path.join(dir, "events.head"), "{}\n")
        fs = fault_fs!(tracked)
        inject_read_fault!(fs, unquote(name))
        log = capture_log(fn -> send(self(), {:actual, recovery().evidence(dir, fs: fs)}) end)
        assert_receive {:actual, actual}

        assert actual ==
                 {:error, %{clause: "journal_unavailable", stage: "read", source: unquote(source), cleanup: @none}}

        refute String.contains?(log, @canary)
      end
    end

    test "E-3 a corrupt line is journal_corrupt reason EXACTLY unknown_event_type at_seq 2, no event_type key, canary absent",
         %{dir: dir, tracked: tracked} do
      corrupt_middle!(dir)
      fs = fault_fs!(tracked)
      since = trace_len(fs)
      bytes = journal(dir)
      log = capture_log(fn -> send(self(), {:actual, recovery().evidence(dir, fs: fs)}) end)
      assert_receive {:actual, actual}

      assert actual ==
               {:error,
                %{clause: "journal_corrupt", stage: "read", reason: "unknown_event_type", at_seq: 2, cleanup: @none}}

      refute String.contains?(log, @canary)
      assert mutations(fs, since) == []
      assert journal(dir) == bytes
    end

    test "E-4 options are closed before any IO: unknown key, create:, duplicate key, bare element", %{
      dir: dir,
      tracked: tracked
    } do
      fs = fault_fs!(tracked)
      since = trace_len(fs)

      refusal = fn field ->
        {:error, %{clause: "recovery_option_invalid", field: field, stage: "options", cleanup: @none}}
      end

      assert recovery().evidence(dir, fs: fs, create: true) == refusal.("create")
      assert recovery().evidence(dir, fs: fs, lock: lock_opts()) == refusal.("options")
      assert recovery().evidence(dir, fs: fs, fs: fs) == refusal.("options")
      assert recovery().evidence(dir, [@canary]) == refusal.("options")
      assert ops(fs, since) == []
    end

    test "E-5 evidence reads under a live OS holder and under a live in-BEAM Writer (A-4 cold half)", %{
      dir: dir,
      tracked: tracked
    } do
      with_os_holder!(dir, tracked, fn _holder_pid -> assert {:ok, %{seq: 6}} = recovery().evidence(dir, []) end)
      {w, _} = open!(dir, tracked, lock: live_opts())
      {:ok, _} = Writer.append(w, @next)
      assert {:ok, %{seq: 7}} = recovery().evidence(dir, [])
      assert dir |> lock_files() |> length() == 1
      :ok = Writer.close(w)
    end
  end

  describe "acquire/release: the trapping-caller precondition (RA-M6)" do
    test "P-1 a NON-trapping caller is refused before any IO with recovery_caller_invalid, precedence over option checks",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      since = trace_len(fs)
      refusal = {:error, %{clause: "recovery_caller_invalid", field: "trap_exit", stage: "caller", cleanup: @none}}

      caller =
        spawn_caller!(tracked, fn ->
          {Process.info(self(), :trap_exit), recovery().acquire(dir, fs: fs, lock: live_opts()),
           recovery().acquire(dir, fs: fs, create: true, lock: [@canary]), recovery().release(%{})}
        end)

      assert_receive {:caller_result, ^caller, {{:trap_exit, false}, ^refusal, ^refusal, ^refusal}}, 5_000
      assert Process.alive?(caller)
      assert ops(fs, since) == []
      assert lock_files(dir) == []
      send(caller, :stop)
    end

    test "P-2 the precondition is never satisfied by mutating the caller: trap_exit stays false after the refusal", %{
      dir: dir,
      tracked: tracked
    } do
      caller =
        spawn_caller!(tracked, fn ->
          _ = recovery().acquire(dir, lock: live_opts())
          Process.info(self(), :trap_exit)
        end)

      assert_receive {:caller_result, ^caller, {:trap_exit, false}}, 5_000
      send(caller, :stop)
    end

    test "P-3 a TRAPPING caller in another process acquires, survives a Writer death during verification with the closed refusal",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      parent = self()

      arm_before_descriptor_open!(fs, parent, tracked, fn ->
        FaultFs.inject(fs, :read, fn [name] -> name == "events.jsonl" end, {:hook, fn -> Process.exit(self(), :kill) end})
      end)

      caller =
        spawn_caller!(tracked, fn ->
          Process.flag(:trap_exit, true)
          recovery().acquire(dir, fs: fs, lock: live_opts())
        end)

      assert_receive {:hook_invoked, w}, 5_000
      ref = Process.monitor(w)

      assert_receive {:caller_result, ^caller,
                      {:error,
                       %{
                         clause: "journal_unavailable",
                         stage: "verify",
                         reason: "writer_exit",
                         cleanup: %{lock: :unknown, registration: :retained, descriptor: :unknown}
                       }}},
                     5_000

      assert_receive {:DOWN, ^ref, :process, ^w, :killed}, 5_000
      assert Process.alive?(caller)
      send(caller, :stop)
    end
  end

  describe "acquire: refusals with cleanup truth" do
    test "Q-1 a real OS holder: journal_unavailable stage lock, cleanup none, nothing of ours on disk or in the arbiter",
         %{
           dir: dir,
           tracked: tracked
         } do
      with_os_holder!(dir, tracked, fn _holder_pid ->
        files = lock_files(dir)
        status = Ownership.status(dir)

        assert recovery().acquire(dir, lock: lock_opts()) ==
                 {:error, %{clause: "journal_unavailable", stage: "lock", cleanup: @none}}

        assert lock_files(dir) == files
        assert Ownership.status(dir) == status
      end)
    end

    test "Q-2 a live in-BEAM Writer: journal_unavailable stage ownership, cleanup none", %{dir: dir, tracked: tracked} do
      {w, _} = open!(dir, tracked, lock: live_opts())
      files = lock_files(dir)

      assert recovery().acquire(dir, lock: live_opts(supervisor_instance: "sup_2")) ==
               {:error, %{clause: "journal_unavailable", stage: "ownership", cleanup: @none}}

      assert lock_files(dir) == files
      :ok = Writer.close(w)
    end

    test "Q-3 a corrupt line: journal_corrupt stage read reason unknown_event_type, cleanup released, bytes unchanged, owner none",
         %{dir: dir, tracked: tracked} do
      corrupt_middle!(dir)
      fs = fault_fs!(tracked)
      bytes = journal(dir)
      log = capture_log(fn -> send(self(), {:actual, recovery().acquire(dir, fs: fs, lock: live_opts())}) end)
      assert_receive {:actual, actual}

      assert actual ==
               {:error,
                %{clause: "journal_corrupt", stage: "read", reason: "unknown_event_type", at_seq: 2, cleanup: @released}}

      refute String.contains?(log, @canary)
      assert journal(dir) == bytes
      assert :none = RunLock.owner(fs, dir)
      assert :none = Ownership.status(dir)
    end

    test "Q-4 an unreadable journal under the lock: journal_unavailable stage read, cleanup released, canary absent", %{
      dir: dir,
      tracked: tracked
    } do
      fs = fault_fs!(tracked)
      inject_read_fault!(fs, "events.jsonl")
      log = capture_log(fn -> send(self(), {:actual, recovery().acquire(dir, fs: fs, lock: live_opts())}) end)
      assert_receive {:actual, actual}
      assert actual == {:error, %{clause: "journal_unavailable", stage: "read", source: "journal", cleanup: @released}}
      refute String.contains?(log, @canary)
      assert :none = RunLock.owner(fs, dir)
    end

    test "Q-5 read fault + release-leg fault: the cause's clause with the STRUCTURED nested release, cleanup unproven/retained",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      inject_read_fault!(fs, "events.jsonl")
      inject_close_legs!(fs, [:rm])
      log = capture_log(fn -> send(self(), {:actual, recovery().acquire(dir, fs: fs, lock: live_opts())}) end)
      assert_receive {:actual, actual}

      assert actual ==
               {:error,
                %{
                  clause: "journal_unavailable",
                  stage: "read",
                  source: "journal",
                  release: "release_incomplete",
                  cleanup: %{lock: :unproven, registration: :retained, descriptor: :none},
                  residue: %{kind: :generation, generation: 1, ours: true}
                }}

      refute String.contains?(log, @canary)
      assert "run.lock.1" in lock_files(dir)
    end

    test "Q-14a rollback arm: a dir_sync fault at acquisition -> unavailable/lock, cleanup lock :removed_unsynced, no residue",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:error, {:seam, @canary}})

      assert recovery().acquire(dir, fs: fs, lock: live_opts()) ==
               {:error,
                %{
                  clause: "journal_unavailable",
                  stage: "lock",
                  cleanup: %{lock: :removed_unsynced, registration: :none, descriptor: :none}
                }}

      assert lock_files(dir) == []
    end

    test "Q-14b rollback arm: not_ours -> unavailable/lock, cleanup none, NO residue, canary absent; the real file stays",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      FaultFs.inject(fs, :dir_sync, fn _ -> true end, {:error, {:seam, @canary}})

      FaultFs.inject(
        fs,
        :read,
        fn [name] -> name == "run.lock.1" end,
        {:return, {:ok, Jason.encode!(%{"token" => @canary})}}
      )

      actual = recovery().acquire(dir, fs: fs, lock: live_opts())
      assert actual == {:error, %{clause: "journal_unavailable", stage: "lock", cleanup: @none}}
      assert_no_canary(actual)
      assert "run.lock.1" in lock_files(dir)
    end

    test "Q-14c a foreign candidate with a PRIVATE token (fresh directory) -> unavailable/lock, cleanup none, residue {candidate, 1, ours false}, no token bytes",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      candidate = Path.join(dir, "run.lock.1.#{@canary}.tmp")
      File.write!(candidate, "FOREIGN-CANDIDATE")
      actual = recovery().acquire(dir, fs: fs, lock: live_opts(token: @canary))

      assert actual ==
               {:error,
                %{
                  clause: "journal_unavailable",
                  stage: "lock",
                  cleanup: @none,
                  residue: %{kind: :candidate, generation: 1, ours: false}
                }}

      assert_no_canary(actual)
      assert File.read!(candidate) == "FOREIGN-CANDIDATE"
      refute Enum.any?(FaultFs.trace(fs), &match?({:rm, _}, &1))
    end
  end

  describe "acquire: success handle, release, lifetimes" do
    test "Q-6 success: a live Writer linked to the caller, generation = the held lock's, token, repaired nil, fenced append",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      bytes = journal(dir)

      assert {:ok, %{writer: w, generation: generation, snapshot: snapshot, repaired: nil} = handle} =
               recovery().acquire(dir, fs: fs, lock: live_opts())

      track!(tracked, :writer, w)
      assert Enum.sort(Map.keys(handle)) == [:generation, :repaired, :snapshot, :writer]
      {:links, links} = Process.info(self(), :links)
      assert Process.alive?(w) and w in links
      assert {:ok, %{generation: ^generation}} = RunLock.holder(fs, dir)
      assert %{seq: 6, token: %{generation: ^generation}, repair: nil} = snapshot
      assert journal(dir) == bytes
      assert {:ok, %{"seq" => 7}} = Writer.append(w, @next, fence: snapshot.token)
      assert :ok = recovery().release(handle)
      assert :none = RunLock.owner(fs, dir)
    end

    test "Q-7 release :ok stops the Writer, releases lock and registration; re-acquire works", %{
      dir: dir,
      tracked: tracked
    } do
      assert {:ok, %{writer: w} = handle} = recovery().acquire(dir, lock: live_opts())
      track!(tracked, :writer, w)
      assert :ok = recovery().release(handle)
      assert_down!(w)
      assert :none = RunLock.owner(SystemFs.new(), dir)
      assert :none = Ownership.status(dir)
      assert {:ok, %{writer: w2} = handle2} = recovery().acquire(dir, lock: live_opts())
      track!(tracked, :writer, w2)
      assert :ok = recovery().release(handle2)
    end

    for {label, legs, faults, cleanup, residue?} <- [
          {"Q-8 lock leg", ["lock"], [:rm], %{lock: :unproven, registration: :retained, descriptor: :closed}, true},
          {"Q-8b descriptor leg", ["descriptor"], [:close],
           %{lock: :released, registration: :released, descriptor: :unproven}, false},
          {"Q-8c both legs", ["descriptor", "lock"], [:close, :rm],
           %{lock: :unproven, registration: :retained, descriptor: :unproven}, true}
        ] do
      test "#{label} close failure on release: recovery_release_failed with structured legs, cleanup truth, ownership UNKNOWN on",
           %{dir: dir, tracked: tracked} do
        fs = fault_fs!(tracked)
        assert {:ok, %{writer: w, generation: generation} = handle} = recovery().acquire(dir, fs: fs, lock: live_opts())
        track!(tracked, :writer, w)
        inject_close_legs!(fs, unquote(faults))
        actual = recovery().release(handle)
        assert_down!(w)

        expected = %{
          clause: "recovery_release_failed",
          stage: "release",
          legs: unquote(legs),
          cleanup: unquote(Macro.escape(cleanup))
        }

        expected =
          if unquote(residue?),
            do: Map.put(expected, :residue, %{kind: :generation, generation: generation, ours: :unknown}),
            else: expected

        assert actual == {:error, expected}
        assert_no_canary(actual)
        if unquote(residue?), do: assert("run.lock.1" in lock_files(dir)), else: refute("run.lock.1" in lock_files(dir))
      end
    end

    test "Q-8d release on a handle whose Writer is gone -> recovery_release_failed reason writer_exit, cleanup unknown; invalid",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      assert {:ok, %{writer: w} = handle} = recovery().acquire(dir, fs: fs, lock: live_opts())
      track!(tracked, :writer, w)
      Process.exit(w, :kill)
      assert_down!(w)

      assert recovery().release(handle) ==
               {:error,
                %{clause: "recovery_release_failed", stage: "release", reason: "writer_exit", legs: [], cleanup: @unknown}}

      since = trace_len(fs)
      invalid = {:error, %{clause: "recovery_option_invalid", field: "handle", stage: "options", cleanup: @none}}
      assert recovery().release(%{}) == invalid
      assert recovery().release(Map.put(handle, :writer, @canary)) == invalid
      assert recovery().release(Map.put(handle, :extra, 1)) == invalid
      assert recovery().release(@canary) == invalid
      assert ops(fs, since) == []
    end

    test "Q-9 post-open read fault (hook on the descriptor open): unavailable/verify, cleanup released/closed, THAT Writer's DOWN",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      parent = self()
      arm_before_descriptor_open!(fs, parent, tracked, fn -> inject_read_fault!(fs, "events.jsonl") end)
      actual = recovery().acquire(dir, fs: fs, lock: live_opts())
      assert_receive {:hook_invoked, w}, 1_000
      assert actual == {:error, %{clause: "journal_unavailable", stage: "verify", source: "journal", cleanup: @closed}}
      assert_down!(w)
      assert :none = RunLock.owner(fs, dir)
    end

    test "Q-10 torn tail appended by the hook: corrupt/verify reason pending_repair, no token ever returned, THAT Writer's DOWN",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      parent = self()
      arm_before_descriptor_open!(fs, parent, tracked, fn -> torn_tail!(dir) end)
      actual = recovery().acquire(dir, fs: fs, lock: live_opts())
      assert_receive {:hook_invoked, w}, 1_000
      assert actual == {:error, %{clause: "journal_corrupt", stage: "verify", reason: "pending_repair", cleanup: @closed}}
      assert_down!(w)
      assert :none = RunLock.owner(fs, dir)
      assert String.ends_with?(journal(dir), "{\"torn")
    end

    test "Q-10b the Writer dies during verification (trapping caller): unavailable/verify reason writer_exit, cleanup",
         %{dir: dir, tracked: tracked} do
      fs = fault_fs!(tracked)
      parent = self()

      arm_before_descriptor_open!(fs, parent, tracked, fn ->
        FaultFs.inject(fs, :read, fn [name] -> name == "events.jsonl" end, {:hook, fn -> Process.exit(self(), :kill) end})
      end)

      actual = recovery().acquire(dir, fs: fs, lock: live_opts())
      assert_receive {:hook_invoked, w}, 1_000

      assert actual ==
               {:error,
                %{
                  clause: "journal_unavailable",
                  stage: "verify",
                  reason: "writer_exit",
                  cleanup: %{lock: :unknown, registration: :retained, descriptor: :unknown}
                }}

      assert_down!(w)
      assert lock_files(dir) == ["run.lock.1"]
      assert {:ok, %{state: :down}} = Ownership.status(dir)
    end

    test "Q-11 caller death (tracked TRAPPING caller, bounded handshake, monitors on both) takes the Writer down and releases the",
         %{dir: dir, tracked: tracked} do
      parent = self()

      caller =
        spawn(fn ->
          Process.flag(:trap_exit, true)
          send(parent, {:acquired, self(), recovery().acquire(dir, lock: live_opts())})

          receive do
            :die -> exit(:caller_died)
          after
            5_000 -> exit(:handshake_timeout)
          end
        end)

      track!(tracked, :caller, caller)
      caller_ref = Process.monitor(caller)
      assert_receive {:acquired, ^caller, {:ok, %{writer: w}}}, 5_000
      track!(tracked, :writer, w)
      ref = Process.monitor(w)
      send(caller, :die)
      assert_receive {:DOWN, ^caller_ref, :process, ^caller, :caller_died}, 5_000
      assert_receive {:DOWN, ^ref, :process, ^w, _}, 5_000
      assert :none = RunLock.owner(SystemFs.new(), dir)
    end

    test "Q-12 option matrix before any IO; without lock seams the lock names the real OS pid", %{
      dir: dir,
      tracked: tracked
    } do
      fs = fault_fs!(tracked)
      since = trace_len(fs)

      refusal = fn field ->
        {:error, %{clause: "recovery_option_invalid", field: field, stage: "options", cleanup: @none}}
      end

      for {opts, field} <- [
            {[fs: fs, create: true, lock: live_opts()], "create"},
            {[fs: fs, lock: []], "supervisor_instance"},
            {[fs: fs], "supervisor_instance"},
            {[fs: fs, lock: [supervisor_instance: ""]], "supervisor_instance"},
            {[fs: fs, lock: [supervisor_instance: 123]], "supervisor_instance"},
            {[fs: fs, lock: [supervisor_instance: "a", supervisor_instance: "b"]], "lock"},
            {[fs: fs, lock: @canary], "lock"},
            {[fs: fs, lock: [@canary]], "lock"},
            {[fs: fs, lock: [{:supervisor_instance, "a"} | @canary]], "lock"},
            {[fs: fs, lock: live_opts(), bogus: @canary], "options"},
            {[fs: fs, lock: live_opts(), lock: live_opts()], "options"},
            {[@canary], "options"}
          ] do
        assert recovery().acquire(dir, opts) == refusal.(field), "opts #{inspect(opts)}"
      end

      assert ops(fs, since) == []
      assert {:ok, %{writer: w} = handle} = recovery().acquire(dir, lock: lock_opts())
      track!(tracked, :writer, w)
      assert {:ok, %{"pid" => pid}} = RunLock.owner(SystemFs.new(), dir)
      assert pid == System.pid()
      assert :ok = recovery().release(handle)
    end

    test "Q-13 a killed generation + torn tail: acquire succeeds at a PROVEN newer generation, repaired = the executed truncate",
         %{dir: dir, tracked: tracked} do
      {w, _} = open!(dir, tracked, lock: live_opts())
      {:ok, _} = Writer.append(w, @next)
      {:ok, %{generation: killed_generation}} = RunLock.holder(SystemFs.new(), dir)
      torn_tail!(dir)
      Process.exit(w, :kill)
      assert_receive {:EXIT, ^w, :killed}, 5_000
      opts = live_opts(owner_status: fn _ -> :dead end)

      assert {:ok, %{writer: w2, generation: generation, repaired: repaired, snapshot: snapshot} = handle} =
               recovery().acquire(dir, lock: opts)

      track!(tracked, :writer, w2)
      assert generation > killed_generation
      assert {:ok, %{generation: ^generation}} = RunLock.holder(SystemFs.new(), dir)
      assert %{action: :truncate_tail} = repaired
      assert %{repair: nil, seq: 7} = snapshot
      refute String.ends_with?(journal(dir), "{\"torn")
      assert :ok = recovery().release(handle)
    end
  end
end
