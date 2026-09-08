defmodule AiOrchestrator.Journal.FsTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Test.FaultFs

  setup do
    dir = Path.join(System.tmp_dir!(), "fs_seam_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  describe "SystemFs" do
    test "mkdir_p, exclusive create, and a second exclusive open refuses", %{dir: dir} do
      fs = SystemFs.new()
      path = Path.join(dir, "events.jsonl")
      refute Fs.exists?(fs, dir)
      assert :ok = Fs.mkdir_p(fs, dir)
      assert Fs.exists?(fs, dir)
      assert {:ok, fd} = Fs.open(fs, path, [:exclusive])
      assert :ok = Fs.close(fs, fd)
      assert Fs.exists?(fs, path)
      assert {:error, :eexist} = Fs.open(fs, path, [:exclusive])
    end

    test "write, sync, close, read round-trips exact bytes and append appends", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "events.jsonl")
      {:ok, fd} = Fs.open(fs, path, [:exclusive])
      assert :ok = Fs.write(fs, fd, ["{\"seq\":1}", "\n"])
      assert :ok = Fs.sync(fs, fd)
      assert :ok = Fs.close(fs, fd)
      assert {:ok, "{\"seq\":1}\n"} = Fs.read(fs, path)
      {:ok, fd} = Fs.open(fs, path, [:append])
      :ok = Fs.write(fs, fd, "{\"seq\":2}\n")
      :ok = Fs.sync(fs, fd)
      :ok = Fs.close(fs, fd)
      assert {:ok, ~s({"seq":1}\n{"seq":2}\n)} = Fs.read(fs, path)
    end

    test "rename publishes atomically and dir_sync succeeds on a directory", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      tmp = Path.join(dir, "events.head.tmp")
      final = Path.join(dir, "events.head")
      {:ok, fd} = Fs.open(fs, tmp, [:write])
      :ok = Fs.write(fs, fd, "{\"seq\":1}\n")
      :ok = Fs.sync(fs, fd)
      :ok = Fs.close(fs, fd)
      assert :ok = Fs.rename(fs, tmp, final)
      refute Fs.exists?(fs, tmp)
      assert {:ok, "{\"seq\":1}\n"} = Fs.read(fs, final)
      assert :ok = Fs.dir_sync(fs, dir)
    end

    test "mkdir is an atomic create, rm and rmdir remove exactly one entry", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      mutex = Path.join(dir, "run.lock.reclaim")
      assert :ok = Fs.mkdir(fs, mutex)
      assert {:error, :eexist} = Fs.mkdir(fs, mutex)
      token = Path.join(mutex, "token")
      {:ok, fd} = Fs.open(fs, token, [:exclusive])
      :ok = Fs.close(fs, fd)
      assert {:error, :eexist} = Fs.rmdir(fs, mutex)
      assert :ok = Fs.rm(fs, token)
      assert {:error, :enoent} = Fs.rm(fs, token)
      assert :ok = Fs.rmdir(fs, mutex)
      refute Fs.exists?(fs, mutex)
    end

    test "link publishes a complete file under a new name and never replaces", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      tmp = Path.join(dir, "run.lock.tok.tmp")
      final = Path.join(dir, "run.lock")
      {:ok, fd} = Fs.open(fs, tmp, [:exclusive])
      :ok = Fs.write(fs, fd, ~s({"token":"tok"}\n))
      :ok = Fs.sync(fs, fd)
      :ok = Fs.close(fs, fd)
      assert :ok = Fs.link(fs, tmp, final)
      assert {:ok, ~s({"token":"tok"}\n)} = Fs.read(fs, final)
      assert {:error, :eexist} = Fs.link(fs, tmp, final)
      assert :ok = Fs.rm(fs, tmp)
      assert {:ok, ~s({"token":"tok"}\n)} = Fs.read(fs, final)
      assert {:error, :enoent} = Fs.link(fs, Path.join(dir, "missing"), Path.join(dir, "other"))
    end

    test "dir_sync leaves no descriptor open on the directory", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)

      open_on_dir = fn ->
        "lsof"
        |> System.cmd(["-p", System.pid()])
        |> elem(0)
        |> String.split("\n")
        |> Enum.count(&String.contains?(&1, dir))
      end

      before = open_on_dir.()
      for _ <- 1..25, do: :ok = Fs.dir_sync(fs, dir)
      assert open_on_dir.() == before
      assert {:error, :enoent} = Fs.dir_sync(fs, Path.join(dir, "missing"))
    end

    test "lstat reports type, mode and link count without following a symlink", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "object")
      File.write!(path, "bytes")
      File.chmod!(path, 0o600)

      assert {:ok, %{type: :regular, mode: 0o600, links: 1}} = Fs.lstat(fs, path)

      other = Path.join(dir, "outside")
      assert :ok = Fs.link(fs, path, other)

      # A name is not an inode. An entry reachable under a second name the run does
      # not control is one whose mode and bytes another writer can change after the
      # check, so a caller that reads only type and mode reads a complete-looking
      # answer to a question it did not ask. The count belongs in the shape for the
      # same reason the type does.
      assert {:ok, %{links: 2}} = Fs.lstat(fs, path)
      assert {:ok, %{links: 2}} = Fs.lstat(fs, other)

      link = Path.join(dir, "link")
      File.ln_s!(path, link)
      assert {:ok, %{type: :symlink}} = Fs.lstat(fs, link)
    end

    test "chmod sets the bits lstat reads back and surfaces one it cannot apply", %{dir: dir} do
      fs = SystemFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "object")
      File.write!(path, "bytes")
      File.chmod!(path, 0o644)

      assert :ok = Fs.chmod(fs, path, 0o600)
      assert {:ok, %{mode: 0o600}} = Fs.lstat(fs, path)
      assert :ok = Fs.chmod(fs, dir, 0o700)
      assert {:ok, %{type: :directory, mode: 0o700}} = Fs.lstat(fs, dir)

      # The mode is something publication has to succeed at, so a mode it could not
      # apply has to arrive as a failure. A seam that answered :ok here would let a
      # caller record evidence as 0600 that is still readable by everyone.
      assert {:error, :enoent} = Fs.chmod(fs, Path.join(dir, "missing"), 0o600)
    end

    test "errors are surfaced, never raised", %{dir: dir} do
      fs = SystemFs.new()
      assert {:error, :enoent} = Fs.open(fs, Path.join(dir, "missing/events.jsonl"), [:append])
      assert {:error, :enoent} = Fs.read(fs, Path.join(dir, "events.jsonl"))
      assert {:error, :enoent} = Fs.dir_sync(fs, Path.join(dir, "missing"))
      assert {:error, :enoent} = Fs.rename(fs, Path.join(dir, "a"), Path.join(dir, "b"))
    end
  end

  describe "FaultFs" do
    test "delegates and records the operation trace in order", %{dir: dir} do
      fs = FaultFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "events.jsonl")
      {:ok, fd} = Fs.open(fs, path, [:exclusive])
      :ok = Fs.write(fs, fd, "line\n")
      :ok = Fs.sync(fs, fd)
      :ok = Fs.close(fs, fd)
      :ok = Fs.rename(fs, path, Path.join(dir, "renamed"))
      :ok = Fs.dir_sync(fs, dir)
      :ok = Fs.mkdir(fs, Path.join(dir, "m"))
      :ok = Fs.rmdir(fs, Path.join(dir, "m"))
      :ok = Fs.link(fs, Path.join(dir, "renamed"), Path.join(dir, "linked"))
      :ok = Fs.chmod(fs, Path.join(dir, "linked"), 0o600)
      {:ok, %{links: 2}} = Fs.lstat(fs, Path.join(dir, "linked"))
      :ok = Fs.rm(fs, Path.join(dir, "renamed"))

      assert [
               {:mkdir_p, ^dir},
               {:open, "events.jsonl", [:exclusive]},
               {:write, 5},
               {:sync},
               {:close},
               {:rename, "events.jsonl", "renamed"},
               {:dir_sync, _},
               {:mkdir, "m"},
               {:rmdir, "m"},
               {:link, "renamed", "linked"},
               {:chmod, "linked", 0o600},
               {:lstat, "linked"},
               {:rm, "renamed"}
             ] = FaultFs.trace(fs)

      refute FaultFs.halted?(fs)
    end

    test "an injected error is returned without touching the disk", %{dir: dir} do
      fs = FaultFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "events.jsonl")
      FaultFs.inject(fs, :write, 2, {:error, :enospc})
      {:ok, fd} = Fs.open(fs, path, [:exclusive])
      :ok = Fs.write(fs, fd, "one\n")
      assert {:error, :enospc} = Fs.write(fs, fd, "two\n")
      :ok = Fs.sync(fs, fd)
      :ok = Fs.close(fs, fd)
      assert {:ok, "one\n"} = Fs.read(SystemFs.new(), path)
      refute FaultFs.halted?(fs)
    end

    test "a torn write keeps the prefix and halts every later operation", %{dir: dir} do
      fs = FaultFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      path = Path.join(dir, "events.jsonl")
      FaultFs.inject(fs, :write, 2, {:torn, 3})
      {:ok, fd} = Fs.open(fs, path, [:exclusive])
      :ok = Fs.write(fs, fd, "one\n")
      assert {:error, :halted} = Fs.write(fs, fd, "two\n")
      assert FaultFs.halted?(fs)
      assert {:error, :halted} = Fs.sync(fs, fd)
      assert {:error, :halted} = Fs.rename(fs, path, Path.join(dir, "other"))
      assert {:ok, "one\ntwo"} = Fs.read(SystemFs.new(), path)
      refute Fs.exists?(fs, Path.join(dir, "other"))
    end

    test "a halt at sync leaves the rename unperformed", %{dir: dir} do
      fs = FaultFs.new()
      :ok = Fs.mkdir_p(fs, dir)
      tmp = Path.join(dir, "events.head.tmp")
      FaultFs.inject(fs, :sync, 1, :halt)
      {:ok, fd} = Fs.open(fs, tmp, [:write])
      :ok = Fs.write(fs, fd, "{\"seq\":1}\n")
      assert {:error, :halted} = Fs.sync(fs, fd)
      assert {:error, :halted} = Fs.rename(fs, tmp, Path.join(dir, "events.head"))
      refute Fs.exists?(fs, Path.join(dir, "events.head"))
      assert {:error, :halted} = Fs.read(fs, tmp)
      assert {:ok, "{\"seq\":1}\n"} = Fs.read(SystemFs.new(), tmp)
    end
  end
end
