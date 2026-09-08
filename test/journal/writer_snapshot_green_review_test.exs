defmodule AiOrchestrator.Journal.WriterSnapshotGreenReviewTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.FixedClock

  @canary "SNAPSHOT-GREEN-REVIEW-PRIVATE-CANARY"
  @fixture Path.expand("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")
  @event @fixture
         |> File.read!()
         |> String.split("\n", trim: true)
         |> hd()
         |> Jason.decode!()
         |> Map.drop(["schema_version", "prev_line_sha256"])

  setup do
    dir = Path.join(System.tmp_dir!(), "snapshot_green_probe_#{System.unique_integer([:positive])}")
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
        lock: [supervisor_instance: "sup_probe", pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end]
      )

    Process.unlink(writer)

    on_exit(fn ->
      try do
        if Process.alive?(writer), do: Writer.close(writer)
      after
        Agent.stop(fs_agent)
        File.rm_rf!(dir)
      end
    end)

    {:ok, snapshot} = Writer.verified(writer)
    %{writer: writer, token: snapshot.token, fs: fs, dir: dir}
  end

  defp result(fun) do
    fun.()
  catch
    :exit, reason -> {:call_exited, reason}
  end

  for shape <- [:bare_element, :non_atom_key] do
    test "malformed option list #{shape} refuses closed without killing Writer", ctx do
      opts =
        case unquote(shape) do
          :bare_element -> [@canary]
          :non_atom_key -> [{@canary, ctx.token}]
        end

      before_bytes = File.read!(Path.join(ctx.dir, "events.jsonl"))

      log =
        capture_log(fn ->
          actual = result(fn -> Writer.append(ctx.writer, @event, opts) end)
          send(self(), {:probe_actual, actual})
        end)

      assert_receive {:probe_actual, actual}
      assert actual == {:error, %{clause: "fence_invalid", field: "options"}}
      assert Process.alive?(ctx.writer)
      assert File.read!(Path.join(ctx.dir, "events.jsonl")) == before_bytes
      refute String.contains?(log, @canary)
      assert {:ok, _} = Writer.append(ctx.writer, @event, fence: ctx.token)
    end
  end

  test "first fenced persistence failure omits private seam detail", ctx do
    FaultFs.inject(ctx.fs, :write, fn _ -> true end, {:error, {:private, @canary}})
    actual = Writer.append(ctx.writer, @event, fence: ctx.token)
    assert {:error, %{clause: "append_failed", stage: "write"}} = actual
    refute String.contains?(inspect(actual), @canary)
    assert actual == {:error, %{clause: "append_failed", stage: "write"}}

    assert {:error, %{clause: "writer_failed", cause: %{clause: "append_failed", stage: "write"}}} =
             Writer.verified(ctx.writer)
  end

  test "legacy unfenced persistence failure keeps its existing detail", ctx do
    FaultFs.inject(ctx.fs, :write, fn _ -> true end, {:error, {:private, @canary}})
    assert {:error, %{clause: "append_failed", detail: detail}} = Writer.append(ctx.writer, @event)
    assert String.contains?(detail, @canary)
  end

  test "duplicate and unknown option keys are already closed refusals", ctx do
    for opts <- [[fence: ctx.token, fence: ctx.token], [fence: ctx.token, extra: @canary]] do
      assert Writer.append(ctx.writer, @event, opts) == {:error, %{clause: "fence_invalid", field: "options"}}
    end

    assert {:ok, _} = Writer.append(ctx.writer, @event, fence: ctx.token)
  end
end
