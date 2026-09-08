defmodule AiOrchestrator.Contracts.FaultFsWriteHookTest do
  @moduledoc """
  The shared `{:hook, fun}` fault on `FaultFs.write/3` (added for the gate execution REDs): the
  hook runs, the bytes are then really written, and the existing torn/halt/error semantics are
  unchanged.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Test.FaultFs

  setup do
    dir = Path.join(System.tmp_dir!(), "fault-fs-write-hook-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, path: Path.join(dir, "f")}
  end

  test "the write hook runs before the real write and the bytes land", %{path: path} do
    fs = FaultFs.new()
    parent = self()
    FaultFs.inject(fs, :write, 1, {:hook, fn -> send(parent, :hook_ran) end})
    {:ok, fd} = Fs.open(fs, path, [:write])
    assert :ok == Fs.write(fs, fd, "hello")
    assert_received :hook_ran
    assert :ok == Fs.close(fs, fd)
    assert File.read!(path) == "hello"
    assert Enum.count(FaultFs.trace(fs), &match?({:write, 5}, &1)) == 1
  end

  test "a hook fires only on its planned call; later writes are ordinary", %{path: path} do
    fs = FaultFs.new()
    parent = self()
    FaultFs.inject(fs, :write, 2, {:hook, fn -> send(parent, :second) end})
    {:ok, fd} = Fs.open(fs, path, [:write])
    assert :ok == Fs.write(fs, fd, "a")
    refute_received :second
    assert :ok == Fs.write(fs, fd, "b")
    assert_received :second
    assert :ok == Fs.write(fs, fd, "c")
    assert :ok == Fs.close(fs, fd)
    assert File.read!(path) == "abc"
  end

  test "torn, halt and error faults are unchanged", %{path: path} do
    fs = FaultFs.new()
    FaultFs.inject(fs, :write, 1, {:torn, 2})
    {:ok, fd} = Fs.open(fs, path, [:write])
    assert {:error, :halted} = Fs.write(fs, fd, "hello")
    assert FaultFs.halted?(fs)
    assert File.read!(path) == "he"
    assert {:error, :halted} = Fs.write(fs, fd, "more")

    fs2 = FaultFs.new()
    FaultFs.inject(fs2, :write, 1, {:error, :eio})
    {:ok, fd2} = Fs.open(fs2, path <> "2", [:write])
    assert {:error, :eio} = Fs.write(fs2, fd2, "x")
    refute FaultFs.halted?(fs2)

    fs3 = FaultFs.new()
    FaultFs.inject(fs3, :write, 1, :halt)
    {:ok, fd3} = Fs.open(fs3, path <> "3", [:write])
    assert {:error, :halted} = Fs.write(fs3, fd3, "x")
    assert FaultFs.halted?(fs3)
  end
end
