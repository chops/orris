defmodule AiOrchestrator.Gate.GateGuardianUnit2Test do
  @moduledoc """
  RED (MUST-12, m_1788584540000): the two unit-2 native additions have their own protocol
  contract. `--probe PID PGID` is read-only and reports leader, start, group and members as
  independent facts with unknown propagation; `--timeout-ms` is a bounded cleanup backstop
  measured from the guardian's own start, never refreshed on GO. Pinned in
  docs/contracts/gate-guardian-protocol.org.
  """
  use ExUnit.Case, async: false

  @moduletag :native

  @source Path.expand("../../native/gate_guardian/gate_guardian.c", __DIR__)

  setup_all do
    dir = Path.join(System.tmp_dir!(), "gate-guardian-u2-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    bin = Path.join(dir, "gate_guardian")
    seam = Path.join(dir, "gate_guardian_seam")
    flags = ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror"]
    {"", 0} = System.cmd("cc", flags ++ ["-o", bin, @source], stderr_to_stdout: true)
    {"", 0} = System.cmd("cc", flags ++ ["-DGATE_GUARDIAN_TESTING", "-o", seam, @source], stderr_to_stdout: true)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, bin: bin, seam: seam}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "gate-guardian-u2-run-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, dir: dir}
  end

  defp probe(bin, args, env \\ []), do: System.cmd(bin, ["--probe" | args], stderr_to_stdout: true, env: env)

  # a finite child owned by this test's Port; the BEAM reaps it on exit
  defp child(seconds) do
    port = Port.open({:spawn_executable, "/bin/sleep"}, [:binary, :exit_status, {:args, [seconds]}])
    {:os_pid, pid} = Port.info(port, :os_pid)
    {ps, 0} = System.cmd("ps", ["-o", "pgid=", "-p", Integer.to_string(pid)])
    {port, pid, ps |> String.trim() |> String.to_integer()}
  end

  defp fields(line) do
    [head | pairs] = line |> String.trim() |> String.split(" ")
    {head, Map.new(pairs, fn pair -> pair |> String.split("=", parts: 2) |> List.to_tuple() end)}
  end

  defp line(port, timeout_ms \\ 5_000) do
    receive do
      {^port, {:data, {:eol, text}}} -> text
      {^port, {:exit_status, status}} -> "guardian exited #{status}"
    after
      timeout_ms -> flunk("no control line within #{timeout_ms} ms")
    end
  end

  defp start(bin, dir, extra, argv) do
    args =
      [
        "--stdout",
        Path.join(dir, "out"),
        "--stderr",
        Path.join(dir, "err"),
        "--cwd",
        dir,
        "--settle-ms",
        "200",
        "--rounds",
        "2"
      ] ++ extra ++ ["--" | argv]

    Port.open({:spawn_executable, bin}, [:binary, :exit_status, :use_stdio, {:args, args}, {:line, 1024}])
  end

  describe "--probe (read-only)" do
    test "a live process we own: leader alive with its kernel start, group alive, members counted; nothing is signalled",
         %{bin: bin} do
      {port, pid, pgid} = child("3")
      assert {out, 0} = probe(bin, [Integer.to_string(pid), Integer.to_string(pgid)])
      assert {"PROBE", %{"leader" => "alive", "start" => start, "group" => "alive", "members" => members}} = fields(out)
      assert start =~ ~r/\A[0-9]+\.[0-9]{6}\z/ or start =~ ~r/\Aticks:[0-9]+\z/
      assert String.to_integer(members) >= 1

      assert match?({_, 0}, System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true)),
             "still alive: never signalled"

      assert_receive {^port, {:exit_status, 0}}, 5_000
    end

    test "a process whose exit we observed: leader gone, start -, group gone, members 0", %{bin: bin} do
      {port, pid, pgid} = child("0.2")
      assert_receive {^port, {:exit_status, 0}}, 5_000
      assert {out, 0} = probe(bin, [Integer.to_string(pid), Integer.to_string(pgid)])
      assert {"PROBE", %{"leader" => "gone", "start" => "-", "group" => "gone", "members" => "0"}} = fields(out)
    end

    test "unknown propagates through the seams: members unknown, group EPERM unknown, leader identity unknown",
         %{seam: seam} do
      {port, pid, pgid} = child("3")
      args = [Integer.to_string(pid), Integer.to_string(pgid)]
      assert {out, 0} = probe(seam, args, [{"GATE_GUARDIAN_FAULT", "members_fail"}])
      assert {"PROBE", %{"leader" => "alive", "group" => "alive", "members" => "unknown"}} = fields(out)
      assert {out, 0} = probe(seam, args, [{"GATE_GUARDIAN_FAULT", "probe_group_eperm"}])
      assert {"PROBE", %{"group" => "unknown"}} = fields(out)
      assert {out, 0} = probe(seam, args, [{"GATE_GUARDIAN_FAULT", "identity_short"}])
      assert {"PROBE", %{"leader" => "unknown", "start" => "-"}} = fields(out)
      assert_receive {^port, {:exit_status, 0}}, 5_000
    end

    test "malformed probe arguments are refused as usage before any fact is read, in both positions", %{bin: bin} do
      bad = [" 1", " +1", "+1", "-1", "1 ", "\t1", " \t 1", "", "1x", "0x1", "1.0", "99999999999", "0", "abc"]

      for value <- bad, args <- [[value, "1"], ["1", value]] do
        assert probe(bin, args) == {"SETUP_FAILED class=usage\n", 2}, inspect(args)
      end

      for args <- [[], ["1"], ["1", "1", "extra"]] do
        assert probe(bin, args) == {"SETUP_FAILED class=usage\n", 2}, inspect(args)
      end
    end

    test "the probe prints exactly one bounded record and exits 0", %{bin: bin} do
      {port, pid, pgid} = child("0.2")
      assert_receive {^port, {:exit_status, 0}}, 5_000
      assert {out, 0} = probe(bin, [Integer.to_string(pid), Integer.to_string(pgid)])
      assert length(String.split(out, "\n", trim: true)) == 1 and byte_size(out) < 200
    end
  end

  describe "--timeout-ms (backstop from the guardian's own start)" do
    test "with a delayed GO, the deadline still elapses from start: DEAD reason=deadline arrives early after release",
         %{bin: bin, dir: dir} do
      port = start(bin, dir, ["--timeout-ms", "1500"], ["/bin/sh", "-c", "sleep 30"])
      assert "READY " <> _ = line(port)
      Process.sleep(1_000)
      true = Port.command(port, "GO\n")
      assert line(port) == "RELEASED"
      t0 = System.monotonic_time(:millisecond)
      assert "DEAD reason=deadline" <> rest = line(port, 3_000)
      elapsed = System.monotonic_time(:millisecond) - t0
      assert elapsed < 1_200, "the deadline was not refreshed on GO (elapsed #{elapsed} ms after release)"
      assert rest =~ "settled=1" and rest =~ "proof=gone"
      assert_receive {^port, {:exit_status, 0}}, 3_000
    end

    test "a deadline that elapses before GO settles the still-blocked worker and reports deadline, not parent_gone",
         %{bin: bin, dir: dir} do
      port = start(bin, dir, ["--timeout-ms", "300"], ["/bin/sh", "-c", "echo ran > ran; sleep 30"])
      assert "READY " <> _ = line(port)
      assert "DEAD reason=deadline" <> _ = line(port, 3_000)
      refute File.exists?(Path.join(dir, "ran")), "never released"
      assert_receive {^port, {:exit_status, 0}}, 3_000
    end

    test "a GO that arrives after the backstop elapsed is never honoured: no RELEASED, no side effect, deadline settlement",
         %{seam: seam, dir: dir} do
      # deterministic barrier: the seam makes the expiry check at the GO-token boundary read as elapsed,
      # exactly the window a late GO lands in (the timing reproduction is 512 ms GO on a 500 ms backstop)
      args = [
        "--stdout",
        Path.join(dir, "out"),
        "--stderr",
        Path.join(dir, "err"),
        "--cwd",
        dir,
        "--settle-ms",
        "200",
        "--rounds",
        "2"
      ]

      args = args ++ ["--timeout-ms", "600000", "--", "/bin/sh", "-c", "echo ran > ran; sleep 30"]
      env = [{~c"GATE_GUARDIAN_FAULT", ~c"go_after_deadline"}]

      port =
        Port.open({:spawn_executable, seam}, [
          :binary,
          :exit_status,
          :use_stdio,
          {:args, args},
          {:env, env},
          {:line, 1024}
        ])

      assert "READY " <> _ = line(port)
      true = Port.command(port, "GO\n")
      assert "DEAD reason=deadline" <> rest = line(port, 3_000)
      assert rest =~ "settled=1" and rest =~ "proof=gone"
      refute rest =~ "RELEASED"
      assert_receive {^port, {:exit_status, 0}}, 3_000
      refute File.exists?(Path.join(dir, "ran")), "the worker was settled without ever being released"
    end

    test "--timeout-ms is validated like every other option, with its own 24 h range", %{bin: bin, dir: dir} do
      for value <- ["0", "-1", "abc", "99", "86400001", "99999999999"] do
        port = start(bin, dir, ["--timeout-ms", value], ["/bin/sleep", "30"])
        assert line(port) == "SETUP_FAILED class=usage", value
        assert_receive {^port, {:exit_status, 2}}, 3_000
      end
    end
  end
end
