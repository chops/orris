defmodule AiOrchestrator.Journal.WriterSnapshotGreenSuccessorTest do
  @moduledoc """
  Stage/shape companions to Codex's green-review probes (SG-M1 / SG-M2): the option domain is closed by
  STRUCTURE (an improper list, a bare element after the fence, a non-atom key cannot kill the Writer or echo
  their bytes), and the fenced append's persist error is closed from its FIRST failure at EVERY stage
  (write / journal sync / receipt sync / receipt rename / post-rename dir_sync) while the legacy `append/2`
  reply at the same stage keeps its seam detail unchanged.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @canary "SNAPSHOT-GREEN-SUCCESSOR-PRIVATE-CANARY"
  @fixture Path.expand("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")
  @events @fixture
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.take(2)
          |> Enum.map(&(&1 |> Jason.decode!() |> Map.drop(["schema_version", "prev_line_sha256"])))

  setup do
    dir = Path.join(System.tmp_dir!(), "snapshot_green_successor_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "")
    fs = FaultFs.new()
    {_, fs_agent} = fs
    Process.unlink(fs_agent)
    FixedClock.reset()

    {:ok, writer, _} =
      Writer.open(dir,
        fs: fs,
        clock: FixedClock,
        lock: [supervisor_instance: "sup_succ", pid: "41002", pid_start: "start_41002", owner_status: fn _ -> :live end]
      )

    Process.unlink(writer)
    # the sync counters of this test's fault matchers: alive until the Writer close attempt has ended (the
    # matcher may still consult them during close), then stopped and joined, bounded, even after a failure
    {:ok, counters} = Agent.start(fn -> [] end)

    on_exit(fn ->
      try do
        if Process.alive?(writer), do: Writer.close(writer)
      after
        for counter <- Agent.get(counters, & &1), do: stop_and_join!(counter)
        stop_and_join!(counters)
        Agent.stop(fs_agent)
        File.rm_rf!(dir)
      end
    end)

    {:ok, snapshot} = Writer.verified(writer)
    %{writer: writer, token: snapshot.token, fs: fs, dir: dir, counters: counters}
  end

  defp stop_and_join!(pid) do
    ref = Process.monitor(pid)
    if Process.alive?(pid), do: Agent.stop(pid, :normal, 5_000)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> raise "counter Agent survived teardown"
    end
  end

  defp event(n), do: Enum.at(@events, n - 1)
  defp count(fs, op), do: fs |> FaultFs.trace() |> Enum.count(&(elem(&1, 0) == op))

  defp result(fun) do
    fun.()
  catch
    :exit, reason -> {:call_exited, reason}
  end

  # the Nth sync of the NEXT append: 1 = the journal fsync, 2 = the receipt temp fsync (matcher-based);
  # the counter is registered for the bounded teardown in setup
  defp inject_nth_sync!(ctx, nth, fault) do
    {:ok, counter} = Agent.start(fn -> 0 end)
    Agent.update(ctx.counters, &[counter | &1])
    FaultFs.inject(ctx.fs, :sync, fn _args -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) == nth end, fault)
  end

  defp inject_stage!(ctx, {:sync, nth}, fault), do: inject_nth_sync!(ctx, nth, fault)
  defp inject_stage!(ctx, {op, nth}, fault), do: FaultFs.inject(ctx.fs, op, count(ctx.fs, op) + nth, fault)

  describe "SG-M1 structure: the option list is judged by shape, never by Keyword functions" do
    for {label, shape} <- [
          {"improper list", :improper},
          {"bare element after the fence", :trailing_bare},
          {"non-atom key after the fence", :trailing_non_atom},
          {"map key element", :map_element}
        ] do
      test "#{label} is fence_invalid options: Writer alive, bytes unchanged, no canary logged, fence still admits",
           ctx do
        opts =
          case unquote(shape) do
            :improper -> [{:fence, ctx.token} | @canary]
            :trailing_bare -> [{:fence, ctx.token}, @canary]
            :trailing_non_atom -> [{:fence, ctx.token}, {@canary, ctx.token}]
            :map_element -> [%{fence: ctx.token}]
          end

        before_bytes = File.read!(Path.join(ctx.dir, "events.jsonl"))
        mutations_before = Enum.map([:write, :sync, :rename, :dir_sync], &count(ctx.fs, &1))

        log =
          capture_log(fn ->
            actual = result(fn -> Writer.append(ctx.writer, event(1), opts) end)
            send(self(), {:probe_actual, actual})
          end)

        assert_receive {:probe_actual, actual}
        assert actual == {:error, %{clause: "fence_invalid", field: "options"}}
        assert Process.alive?(ctx.writer)
        assert File.read!(Path.join(ctx.dir, "events.jsonl")) == before_bytes
        assert Enum.map([:write, :sync, :rename, :dir_sync], &count(ctx.fs, &1)) == mutations_before
        refute String.contains?(log, @canary)
        # the retained token was neither consumed nor cleared by the refusal
        assert {:ok, _} = Writer.append(ctx.writer, event(1), fence: ctx.token)
      end
    end

    test "a malformed list on a warm-failed Writer answers writer_failed (failure precedes option judgement)", ctx do
      FaultFs.inject(ctx.fs, :write, fn _ -> true end, {:error, {:private, @canary}})
      assert {:error, %{clause: "append_failed", stage: "write"}} = Writer.append(ctx.writer, event(1), fence: ctx.token)

      actual = result(fn -> Writer.append(ctx.writer, event(1), [@canary]) end)
      assert actual == {:error, %{clause: "writer_failed", cause: %{clause: "append_failed", stage: "write"}}}
      assert Process.alive?(ctx.writer)
    end
  end

  describe "SG-M2 shape: the FIRST fenced persist failure is closed at every stage; the legacy reply is unchanged" do
    for {label, stage, target} <- [
          {"journal sync", "sync", {:sync, 1}},
          {"receipt sync", "receipt", {:sync, 2}},
          {"receipt rename", "receipt", {:rename, 1}},
          {"post-rename dir_sync", "receipt", {:dir_sync, 1}}
        ] do
      test "#{label}: fenced append_failed stage #{stage} carries no detail; verified answers the closed cause", ctx do
        inject_stage!(ctx, unquote(Macro.escape(target)), {:error, {:private, @canary}})

        log =
          capture_log(fn ->
            actual = Writer.append(ctx.writer, event(1), fence: ctx.token)
            send(self(), {:probe_actual, actual})
          end)

        assert_receive {:probe_actual, actual}
        assert actual == {:error, %{clause: "append_failed", stage: unquote(stage)}}
        refute String.contains?(inspect(actual), @canary)
        refute String.contains?(log, @canary)

        assert Writer.verified(ctx.writer) ==
                 {:error, %{clause: "writer_failed", cause: %{clause: "append_failed", stage: unquote(stage)}}}

        assert Writer.append(ctx.writer, event(2), fence: ctx.token) ==
                 {:error, %{clause: "writer_failed", cause: %{clause: "append_failed", stage: unquote(stage)}}}
      end

      test "#{label}: the legacy append/2 reply at stage #{stage} keeps its seam detail (bytes unchanged)", ctx do
        inject_stage!(ctx, unquote(Macro.escape(target)), {:error, {:private, @canary}})

        assert {:error, %{clause: "append_failed", stage: unquote(stage), detail: detail}} =
                 Writer.append(ctx.writer, event(1))

        assert String.contains?(detail, @canary)
        # and the legacy warm-failed reply still carries the full stored failure, as before this unit
        assert {:error, %{clause: "writer_failed", cause: %{detail: ^detail}}} = Writer.append(ctx.writer, event(2))
      end
    end
  end
end
