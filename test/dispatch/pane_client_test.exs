defmodule AiOrchestrator.Dispatch.PaneClientTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.PaneClient

  test "status decodes the last JSON object emitted by ap" do
    parent = self()

    runner = fn ap_path, args, opts ->
      Process.send(parent, {:ap_call, ap_path, args, opts}, [])
      {~s(log line\n{"state":"idle","pane_ref":"pane_writer","pending_count":0}\n), 0}
    end

    assert {:ok, %{"state" => "idle", "pane_ref" => "pane_writer", "pending_count" => 0}} =
             PaneClient.status("pane_writer", ap_path: "/tmp/ap", runner: runner)

    assert_receive {:ap_call, "/tmp/ap", ["pane_status", "pane_writer"],
                    [stderr_to_stdout: true, env: [{"ERL_CRASH_DUMP_SECONDS", "0"}]]}
  end

  test "send passes the prompt through stdin and returns decoded status" do
    parent = self()

    input_runner = fn ap_path, args, input, opts ->
      Process.send(parent, {:ap_call, ap_path, args, input, opts}, [])
      {~s({"sent":true,"queued":false}\n), 0}
    end

    assert {:ok, %{"sent" => true, "queued" => false}} =
             PaneClient.send("pane_writer", "Do the work", ap_path: "/tmp/ap", input_runner: input_runner)

    assert_receive {:ap_call, "/tmp/ap", ["send", "pane_writer", "--stdin"], "Do the work", opts}
    assert opts[:stderr_to_stdout]
    assert opts[:env] == [{"ERL_CRASH_DUMP_SECONDS", "0"}]
  end

  test "default input runner delivers prompt bytes to a real executable stdin" do
    temp_dir = Path.join(System.tmp_dir!(), "pane_client_stdin_#{System.unique_integer([:positive])}")
    fake_ap = Path.join(temp_dir, "fake-ap")
    File.mkdir_p!(temp_dir)

    File.write!(fake_ap, """
    #!/bin/sh
    body=$(cat)
    if [ "$body" = "Do the work" ]; then
      printf '%s\\n' '{"sent":true,"stdin_verified":true}'
      exit 0
    fi
    printf '%s\\n' 'wrong stdin' >&2
    exit 1
    """)

    File.chmod!(fake_ap, 0o700)
    on_exit(fn -> File.rm_rf!(temp_dir) end)

    assert {:ok, %{"sent" => true, "stdin_verified" => true}} =
             PaneClient.send("pane_writer", "Do the work", ap_path: fake_ap)
  end

  test "nonzero ap exits return structured errors that do not repeat the daemon's text" do
    runner = fn _ap_path, _args, _opts -> {"pane not found", 1} end

    assert {:error, %{"reason" => "ap_failed", "exit_status" => 1, "output_digest" => "sha256:" <> digest} = error} =
             PaneClient.status("pane_writer", runner: runner)

    assert byte_size(digest) == 64

    refute inspect(error) =~ "pane not found",
           "the daemon's stdout is the daemon's text; a diagnostic correlates it, never quotes it"
  end

  test "a nonzero ap exit that decoded to an unknown-command reply is named, and the reply is not kept" do
    runner = fn _ap_path, _args, _opts ->
      {~s({"ok":false,"error":"unknown command: pane_status","detail":"CANARY_7c1f"}\n), 1}
    end

    assert {:error, %{"reason" => "reconcile_unsupported"} = error} = PaneClient.status("pane_writer", runner: runner)
    refute inspect(error) =~ "CANARY_7c1f", "the daemon's reply is recognized and then dropped, never carried"
  end

  test "any other decoded failure reply is reduced to a digest" do
    runner = fn _ap_path, _args, _opts -> {~s({"ok":false,"error":"internal","detail":"CANARY_7c1f"}\n), 1} end

    assert {:error, %{"reason" => "ap_failed", "exit_status" => 1, "output_digest" => "sha256:" <> _} = error} =
             PaneClient.status("pane_writer", runner: runner)

    refute inspect(error) =~ "CANARY_7c1f"
    refute Map.has_key?(error, "reply")
  end

  # DD-9 incident: transient IPC children must never inherit crash-dump routing —
  # an orphaned child writing a dump can clobber the product VM's own dump path.
  test "ap children run with crash dumps disabled via scrubbed env" do
    runner = fn _cmd, _args, cmd_opts ->
      send(self(), {:cmd_opts, cmd_opts})
      {~s({"ok":true}), 0}
    end

    assert {:ok, _response} = PaneClient.status("pane_a", runner: runner, env: %{})

    assert_receive {:cmd_opts, cmd_opts}
    env = Keyword.get(cmd_opts, :env, [])
    assert {"ERL_CRASH_DUMP_SECONDS", "0"} in env
  end
end
