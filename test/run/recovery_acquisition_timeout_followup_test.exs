defmodule AiOrchestrator.Run.RecoveryTimeoutFollowupReviewTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Run.Recovery
  alias AiOrchestrator.Test.FaultFs

  @moduletag timeout: 30_000
  @lock [supervisor_instance: "sup_followup", pid: "41001", pid_start: "start_41001", owner_status: &__MODULE__.live/1]
  def live(_), do: :live

  setup do
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "recovery_timeout_followup_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "")
    fs = FaultFs.new()
    {_, agent} = fs
    Process.unlink(agent)
    {:ok, tracked} = Agent.start(fn -> [] end)

    on_exit(fn ->
      for role <- [:caller, :writer], {^role, pid} <- Agent.get(tracked, & &1) do
        ref = Process.monitor(pid)
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
      end

      Agent.stop(tracked)
      Agent.stop(agent)
      File.rm_rf!(dir)
    end)

    %{dir: dir, fs: fs, tracked: tracked}
  end

  defp track(ctx, role, pid), do: Agent.update(ctx.tracked, &[{role, pid} | &1])

  defp start_acquire(ctx) do
    parent = self()

    caller =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, {:result, self(), Recovery.acquire(ctx.dir, fs: ctx.fs, lock: @lock)})

        receive do
          :done -> :ok
        after
          20_000 -> :ok
        end
      end)

    track(ctx, :caller, caller)
    caller
  end

  defp arm(ctx, hook) do
    FaultFs.inject(
      ctx.fs,
      :open,
      fn [name, modes] -> name == "events.jsonl" and :append in modes end,
      {:hook,
       fn ->
         track(ctx, :writer, self())
         hook.()
         true
       end}
    )
  end

  defp await_close(writer, deadline) do
    {:messages, messages} = Process.info(writer, :messages)

    if Enum.any?(messages, &match?({:"$gen_call", _, :close}, &1)) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline, "orderly close never reached the Writer"
      Process.sleep(10)
      await_close(writer, deadline)
    end
  end

  test "verification timeout followed by orderly close reports proved cleanup", ctx do
    parent = self()

    arm(ctx, fn ->
      FaultFs.inject(
        ctx.fs,
        :read,
        fn [name] -> name == "events.jsonl" end,
        {:hook,
         fn ->
           send(parent, {:read_entered, self()})

           receive do
             :finish_read -> true
           after
             20_000 -> true
           end
         end}
      )
    end)

    caller = start_acquire(ctx)
    assert_receive {:read_entered, writer}, 5_000
    ref = Process.monitor(writer)
    await_close(writer, System.monotonic_time(:millisecond) + 8_000)
    send(writer, :finish_read)
    assert_receive {:result, ^caller, {:error, %{reason: "writer_timeout", cleanup: cleanup}}}, 5_000
    assert cleanup == %{lock: :released, registration: :released, descriptor: :closed}
    assert_receive {:DOWN, ^ref, :process, ^writer, :normal}, 5_000
    assert :none = RunLock.owner(ctx.fs, ctx.dir)
    assert :none = Ownership.status(ctx.dir)
  end

  test "verification refusal plus close timeout preserves reason and proves killed Writer", ctx do
    parent = self()

    arm(ctx, fn ->
      FaultFs.inject(ctx.fs, :read, fn [name] -> name == "events.jsonl" end, {:error, :eio})

      FaultFs.inject(
        ctx.fs,
        :close,
        fn _ -> true end,
        {:hook,
         fn ->
           send(parent, {:close_entered, self()})

           receive do
             :never -> true
           after
             20_000 -> true
           end
         end}
      )
    end)

    caller = start_acquire(ctx)
    assert_receive {:close_entered, writer}, 5_000
    ref = Process.monitor(writer)
    assert_receive {:result, ^caller, {:error, result}}, 8_000

    assert result == %{
             clause: "journal_unavailable",
             stage: "verify",
             source: "journal",
             close: "writer_timeout",
             cleanup: %{lock: :unknown, registration: :retained, descriptor: :unknown}
           }

    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}, 5_000
    assert {:ok, %{state: :down}} = Ownership.status(ctx.dir)
    assert File.exists?(Path.join(ctx.dir, "run.lock.1"))
  end
end
