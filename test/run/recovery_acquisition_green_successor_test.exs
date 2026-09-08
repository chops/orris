defmodule AiOrchestrator.Run.RecoveryAcquisitionGreenSuccessorTest do
  @moduledoc """
  Per-field companions to Codex's green-review probes (RG-M1..M3, docs/contracts/recovery-acquisition.org rev 6):
  a call timeout is not death (T-1/T-2), the nested lock set is closed with every admitted seam passing through
  (L-1), and handle VALUES are validated before any call reaches the Writer (H-1).
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Run.Recovery
  alias AiOrchestrator.Test.FaultFs

  @moduletag timeout: 60_000
  @canary "RECOVERY-GREEN-SUCCESSOR-PRIVATE-CANARY"
  @fixture Path.expand("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")
  @corpus @fixture |> File.read!() |> String.split("\n", trim: true) |> Enum.take(6)
  @unknown %{lock: :unknown, registration: :retained, descriptor: :unknown}
  @none %{lock: :none, registration: :none, descriptor: :none}

  setup do
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "recovery_green_succ_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(@corpus, "\n") <> "\n")
    fs = FaultFs.new()
    {_, fs_agent} = fs
    Process.unlink(fs_agent)
    {:ok, tracked} = Agent.start(fn -> [] end)

    on_exit(fn ->
      for role <- [:caller, :writer], {^role, pid} <- Agent.get(tracked, & &1) do
        ref = Process.monitor(pid)
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
      end

      Agent.stop(tracked)
      Agent.stop(fs_agent)
      File.rm_rf!(dir)
    end)

    %{dir: dir, fs: fs, tracked: tracked}
  end

  defp live_opts(overrides \\ []),
    do:
      Keyword.merge(
        [supervisor_instance: "sup_succ", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end],
        overrides
      )

  defp track(ctx, role, pid), do: Agent.update(ctx.tracked, &[{role, pid} | &1])
  defp lock_files(dir), do: dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "run.lock.")) |> Enum.sort()

  defp acquire!(ctx, overrides \\ []) do
    {:ok, handle} = Recovery.acquire(ctx.dir, fs: ctx.fs, lock: live_opts(overrides))
    track(ctx, :writer, handle.writer)
    handle
  end

  defp assert_down!(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  describe "RG-M1 a call timeout is not death" do
    test "T-1 verification timeout: writer_timeout, THAT Writer's DOWN observed, cleanup unknown/retained/unknown, held file remains",
         ctx do
      parent = self()

      FaultFs.inject(
        ctx.fs,
        :open,
        fn [name, modes] -> name == "events.jsonl" and :append in modes end,
        {:hook,
         fn ->
           track(ctx, :writer, self())
           send(parent, {:allocated, self()})

           FaultFs.inject(
             ctx.fs,
             :read,
             fn [name] -> name == "events.jsonl" end,
             {:hook,
              fn ->
                receive do
                  :never -> true
                after
                  60_000 -> true
                end
              end}
           )

           true
         end}
      )

      actual = Recovery.acquire(ctx.dir, fs: ctx.fs, lock: live_opts())
      assert_receive {:allocated, writer}, 5_000

      assert actual ==
               {:error, %{clause: "journal_unavailable", stage: "verify", reason: "writer_timeout", cleanup: @unknown}}

      refute Process.alive?(writer)
      assert_down!(writer)
      assert lock_files(ctx.dir) == ["run.lock.1"]
      assert {:ok, %{state: :down}} = Ownership.status(ctx.dir)
    end

    test "T-2 release timeout (suspended Writer): writer_timeout, legs [], cleanup unknown/retained/unknown, DOWN observed, held file remains",
         ctx do
      handle = acquire!(ctx)
      :ok = :sys.suspend(handle.writer)

      assert Recovery.release(handle) ==
               {:error,
                %{
                  clause: "recovery_release_failed",
                  stage: "release",
                  reason: "writer_timeout",
                  legs: [],
                  cleanup: @unknown
                }}

      refute Process.alive?(handle.writer)
      assert_down!(handle.writer)
      assert lock_files(ctx.dir) == ["run.lock.1"]
    end

    test "T-3 a genuine death is still writer_exit (dead Writer on release)", ctx do
      handle = acquire!(ctx)
      Process.exit(handle.writer, :kill)
      assert_down!(handle.writer)

      assert Recovery.release(handle) ==
               {:error,
                %{
                  clause: "recovery_release_failed",
                  stage: "release",
                  reason: "writer_exit",
                  legs: [],
                  cleanup: %{lock: :unknown, registration: :unknown, descriptor: :unknown}
                }}
    end
  end

  describe "RG-M2 the nested lock set is closed; every admitted seam passes through" do
    test "L-1 pid / pid_start / token pass through to the held record; an unknown nested key is refused before IO without echo",
         ctx do
      handle = acquire!(ctx, pid: "41007", pid_start: "start_41007", token: "seam_token")

      assert {:ok, %{"pid" => "41007", "pid_start" => "start_41007", "token" => "seam_token"}} =
               RunLock.owner(ctx.fs, ctx.dir)

      assert :ok = Recovery.release(handle)

      since = length(FaultFs.trace(ctx.fs))

      for extra <- [[private_unknown: @canary], [create: true], [fs: ctx.fs], [ownership: []]] do
        actual = Recovery.acquire(ctx.dir, fs: ctx.fs, lock: live_opts(extra))

        assert actual ==
                 {:error, %{clause: "recovery_option_invalid", field: "lock", stage: "options", cleanup: @none}},
               inspect(extra)
      end

      assert length(FaultFs.trace(ctx.fs)) == since
    end

    test "L-1b owner_status IS consulted: a genuine contention witness (a stranded prior holder's record reaches the seam)",
         ctx do
      parent = self()
      # a stranded lock the arbiter never recorded: RunLock must classify its holder through owner_status
      {:ok, %{generation: prior}} =
        RunLock.acquire(ctx.fs, ctx.dir,
          supervisor_instance: "sup_prior",
          pid: "41001",
          pid_start: "start_41001",
          owner_status: fn _ -> :live end
        )

      handle =
        acquire!(ctx,
          pid: "41008",
          pid_start: "start_41008",
          owner_status: fn metadata ->
            send(parent, {:consulted, metadata})
            :dead
          end
        )

      assert_receive {:consulted, %{"pid" => "41001", "pid_start" => "start_41001"}}, 1_000
      assert handle.generation > prior
      assert :ok = Recovery.release(handle)
    end
  end

  describe "RG-M3 handle values are validated before any call reaches the Writer" do
    test "H-1 one-field negatives leave the Writer alive with zero IO; the genuine handle releases :ok", ctx do
      handle = acquire!(ctx)
      since = length(FaultFs.trace(ctx.fs))
      invalid = {:error, %{clause: "recovery_option_invalid", field: "handle", stage: "options", cleanup: @none}}

      negatives = [
        %{handle | generation: 0},
        %{handle | generation: -1},
        %{handle | generation: "1"},
        %{handle | snapshot: @canary},
        %{handle | snapshot: nil},
        %{handle | repaired: :invalid},
        %{handle | repaired: @canary},
        %{handle | writer: @canary},
        Map.put(handle, :extra, 1),
        Map.delete(handle, :snapshot)
      ]

      for negative <- negatives do
        assert Recovery.release(negative) == invalid, inspect(Map.keys(negative))
        assert Process.alive?(handle.writer)
      end

      assert length(FaultFs.trace(ctx.fs)) == since
      assert :ok = Recovery.release(handle)
    end
  end
end
