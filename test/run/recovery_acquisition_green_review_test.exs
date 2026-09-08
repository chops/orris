defmodule AiOrchestrator.Run.RecoveryGreenReviewTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Run.Recovery
  alias AiOrchestrator.Test.FaultFs

  @moduletag timeout: 40_000
  @lock [supervisor_instance: "sup_review", pid: "41001", pid_start: "start_41001", owner_status: &__MODULE__.live/1]
  def live(_), do: :live

  setup do
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "recovery_green_review_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), "")
    fs = FaultFs.new()
    {_, fs_agent} = fs
    Process.unlink(fs_agent)
    {:ok, tracked} = Agent.start(fn -> [] end)

    on_exit(fn ->
      for role <- [:caller, :writer] do
        for {^role, pid} <- Agent.get(tracked, & &1) do
          ref = Process.monitor(pid)
          if Process.alive?(pid), do: Process.exit(pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
        end
      end

      Agent.stop(tracked)
      Agent.stop(fs_agent)
      File.rm_rf!(dir)
    end)

    %{dir: dir, fs: fs, tracked: tracked}
  end

  defp track(ctx, role, pid), do: Agent.update(ctx.tracked, &[{role, pid} | &1])

  defp acquire!(ctx) do
    {:ok, handle} = Recovery.acquire(ctx.dir, fs: ctx.fs, lock: @lock)
    track(ctx, :writer, handle.writer)
    handle
  end

  test "RG-1 unknown nested lock option is refused before IO", ctx do
    actual = Recovery.acquire(ctx.dir, fs: ctx.fs, lock: @lock ++ [private_unknown: "PRIVATE"])

    case actual do
      {:ok, handle} -> track(ctx, :writer, handle.writer)
      _ -> :ok
    end

    assert {:error, %{clause: "recovery_option_invalid"}} = actual
    assert FaultFs.trace(ctx.fs) == []
  end

  test "RG-2 malformed handle values are refused before closing a valid Writer", ctx do
    handle = acquire!(ctx)
    before = FaultFs.trace(ctx.fs)

    assert {:error, %{clause: "recovery_option_invalid", field: "handle"}} =
             Recovery.release(%{handle | generation: -1, snapshot: "PRIVATE", repaired: :invalid})

    assert Process.alive?(handle.writer)
    assert FaultFs.trace(ctx.fs) == before
    assert :ok = Recovery.release(handle)
  end

  test "RG-3 a verification call timeout does not discard a still-live Writer", ctx do
    parent = self()

    FaultFs.inject(
      ctx.fs,
      :open,
      fn [name, modes] -> name == "events.jsonl" and :append in modes end,
      {:hook,
       fn ->
         track(ctx, :writer, self())

         FaultFs.inject(
           ctx.fs,
           :read,
           fn [name] -> name == "events.jsonl" end,
           {:hook,
            fn ->
              send(parent, {:verification_entered, self()})

              receive do
                :resume_read -> true
              after
                30_000 -> true
              end
            end}
         )

         true
       end}
    )

    caller =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, {:acquire_result, self(), Recovery.acquire(ctx.dir, fs: ctx.fs, lock: @lock)})

        receive do
          :done -> :ok
        after
          30_000 -> :ok
        end
      end)

    track(ctx, :caller, caller)
    assert_receive {:verification_entered, writer}, 5_000
    assert_receive {:acquire_result, ^caller, result}, 15_000
    assert {:error, %{stage: "verify"}} = result

    refute Process.alive?(writer),
           "acquire returned #{inspect(result)} but its Writer is alive with an open descriptor and held lock"
  end

  test "RG-4 release timeout is not observed Writer death", ctx do
    handle = acquire!(ctx)
    :ok = :sys.suspend(handle.writer)

    try do
      actual = Recovery.release(handle)

      refute match?({:error, %{reason: "writer_exit"}}, actual) and Process.alive?(handle.writer),
             "release reported writer_exit while the monitored Writer is still alive"
    after
      if Process.alive?(handle.writer), do: :sys.resume(handle.writer)
    end
  end
end
