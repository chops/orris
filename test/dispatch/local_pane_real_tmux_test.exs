defmodule AiOrchestrator.Dispatch.LocalPaneRealTmuxTest do
  @moduledoc """
  One dispatch row through the DELIVERED adapter and client (`LocalPane.deliver/2` ->
  `PaneClient` -> an `ap` OS process) whose `ap` really pastes into a REAL tmux pane, on a
  tmux server this row owns and the operator's default server cannot see.

  Until this file, every Orris dispatch test answered `ap` from a fake and no tmux ran
  (R13/R14 audit, NS-31 element "real isolated tmux tests under tmux -L": ABSENT). The
  Orrisd suite reached the operator's default server 18 times before its route guard
  existed, and its `-L`-only servers still leave their sockets in the operator's socket
  directory (measured 2026-09-19: 70+ `ai_pair_test_*` sockets beside `default` in
  `/private/tmp/tmux-<uid>/`). Isolation here is therefore by CONSTRUCTION, and every
  leg of it is asserted:

    * a private server (`-L <unique>`), started with `-f /dev/null` so no operator
      tmux.conf is read;
    * `TMUX` and `TMUX_PANE` UNSET for the server, for the client calls, and inside the
      fake `ap`, so no inherited client identity can address the operator's server;
    * `TMUX_TMPDIR` pointed at a directory this row created, so the socket cannot be
      created in the operator's socket directory at all;
    * the control: tmux's own `\#{socket_path}` must lie under the owned directory, the
      socket must NOT exist under any default location, and every tmux argv the fake `ap`
      executed must name the private socket. Removing the `TMUX_TMPDIR` export makes the
      first two fail (mutation-checked in the lane record); a fake `ap` that dropped `-L`
      makes the third fail.

  The fake `ap` is the daemon stand-in the delivered client already talks to in every
  other row; what is real here is the client, the OS process, the paste and the pane.
  The daemon itself is the other repository and is not exercised.

  Needs a `tmux` on PATH: the devShell provides one (flake.nix), so `bin/verify` and CI
  have it. Outside the devShell the row fails loudly rather than skipping.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Dispatch.LocalPane

  @prompt "Implement item A in lib/item_a.ex -- real tmux dispatch row 2026-09-19"

  # The bash body of the fake `ap`, with no Elixir interpolation: placeholders are
  # replaced verbatim. It answers the three v2 verbs the delivered client issues on a
  # fresh dispatch (ping, reconcile, send), pastes the send's stdin into the pane through
  # the PRIVATE server only, and logs every tmux argv it executes so the row can prove no
  # call went anywhere else.
  @fake_ap_body ~S"""
  set -euo pipefail
  unset TMUX TMUX_PANE
  export TMUX_TMPDIR="@@SOCKET_DIR@@"
  tmux="@@TMUX@@"
  sock="@@SOCKET@@"
  log="@@LOG@@"
  verb="${1:-}"
  case "$verb" in
    ping)
      printf '%s\n' '{"ok":true,"protocol_version":2,"pong":"fake","capabilities":["delivery_reconcile"]}'
      ;;
    reconcile)
      pane="$2"
      test "$3" = "--msg-id"
      msg="$4"
      printf '{"ok":true,"protocol_version":2,"outcome":"absent","delivery_attempt":0,"msg_id":"%s","pane_id":"%s"}\n' "$msg" "$pane"
      ;;
    send)
      pane="$2"
      test "$3" = "--stdin"
      test "$4" = "--msg-id"
      msg="$5"
      printf '%s\n' "load-buffer -L $sock" >> "$log"
      "$tmux" -L "$sock" load-buffer -b orris_prompt -
      printf '%s\n' "paste-buffer -L $sock" >> "$log"
      "$tmux" -L "$sock" paste-buffer -d -b orris_prompt -t "$pane"
      printf '{"ok":true,"protocol_version":2,"status":"sent","msg_id":"%s","pane_id":"%s"}\n' "$msg" "$pane"
      ;;
    *)
      printf '%s\n' '{"ok":false,"error":"unknown command"}'
      exit 2
      ;;
  esac
  """

  setup do
    tmux = System.find_executable("tmux") || flunk("tmux is required for this row: run through nix develop")
    bash = System.find_executable("bash") || flunk("bash is required")

    # A leaked socket lands under a DEFAULT directory, so the leak control below computes
    # that path exactly as tmux computes it: <dir>/tmux-<uid>/<socket>.
    {uid, 0} = System.cmd("id", ["-u"])
    uid = String.trim(uid)

    unique = System.unique_integer([:positive])
    socket = "orris_d#{unique}"
    root = socket_root("orris-tmux-#{unique}", uid, socket)
    File.mkdir_p!(root)
    File.chmod!(root, 0o700)
    # the owned directory IS the socket directory: TMUX_TMPDIR points at it
    socket_dir = root
    env = [{"TMUX", nil}, {"TMUX_PANE", nil}, {"TMUX_TMPDIR", socket_dir}]

    {out, 0} =
      System.cmd(
        tmux,
        ["-L", socket, "-f", "/dev/null", "new-session", "-d", "-s", "dispatch", "-x", "80", "-y", "24", "sleep", "3600"],
        env: env,
        stderr_to_stdout: true
      )

    assert out == "", "starting the private server must be silent, got: #{inspect(out)}"

    on_exit(fn ->
      System.cmd(tmux, ["-L", socket, "kill-server"], env: env, stderr_to_stdout: true)
      File.rm_rf!(root)
    end)

    log = Path.join(root, "tmux-argv.log")
    ap = Path.join(root, "fake-ap")

    body =
      @fake_ap_body
      |> String.replace("@@SOCKET_DIR@@", socket_dir)
      |> String.replace("@@TMUX@@", tmux)
      |> String.replace("@@SOCKET@@", socket)
      |> String.replace("@@LOG@@", log)

    File.write!(ap, "#!" <> bash <> " -p\n" <> body)
    File.chmod!(ap, 0o700)

    {:ok, tmux: tmux, env: env, root: root, socket_dir: socket_dir, socket: socket, uid: uid, ap: ap, log: log}
  end

  defp tmux!(ctx, args) do
    case System.cmd(ctx.tmux, ["-L", ctx.socket | args], env: ctx.env, stderr_to_stdout: true) do
      {out, 0} -> String.trim_trailing(out, "\n")
      {out, status} -> flunk("tmux -L #{ctx.socket} #{Enum.join(args, " ")} failed (#{status}): #{inspect(out)}")
    end
  end

  # tmux resolves the socket directory as TMUX_TMPDIR, else /tmp (NOT TMPDIR: measured
  # 2026-09-19 by removing the export -- the socket appeared at /private/tmp/tmux-<uid>/,
  # the OPERATOR's socket directory, with the gate's TMPDIR elsewhere), then
  # tmux-<uid>/<name>. The row asserts the socket is under the owned directory and NOT at
  # the fallback it would use if the export were lost.
  defp default_socket_paths(ctx), do: [Path.join(["/tmp", "tmux-#{ctx.uid}", ctx.socket])]

  defp capture(ctx), do: tmux!(ctx, ["capture-pane", "-p", "-t", "dispatch"])

  defp wait_until(fun, timeout_ms), do: wait_until_by(fun, System.monotonic_time(:millisecond) + timeout_ms)

  defp wait_until_by(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) > deadline -> fun.()
      true -> Process.sleep(50) && wait_until_by(fun, deadline)
    end
  end

  defp sha256(bytes), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  # A UNIX socket path is bounded (sun_path: 104 bytes on macOS, 108 on Linux) and the gate's
  # TMPDIR can be long (measured 2026-09-19: the nix-shell TMPDIR made the first draft's socket
  # path fail with "File name too long"). The owned root goes under System.tmp_dir!/0 when the
  # socket path fits, else under /tmp; either way it is a fresh 0700 directory this row creates
  # and removes, never a directory anything else uses.
  @sun_path_budget 100
  defp socket_root(name, uid, socket) do
    candidate = Path.join(System.tmp_dir!(), name)

    if byte_size(Path.join([candidate, "tmux-#{uid}", socket])) <= @sun_path_budget,
      do: candidate,
      else: Path.join("/tmp", name)
  end

  test "the private server lives only under the owned socket directory, never at a default location", ctx do
    socket_path = tmux!(ctx, ["display-message", "-p", "\#{socket_path}"])

    assert String.starts_with?(socket_path, ctx.socket_dir <> "/"),
           "the server's own socket path must be under the owned TMUX_TMPDIR #{ctx.socket_dir}, got #{socket_path}"

    assert socket_path == Path.join([ctx.socket_dir, "tmux-#{ctx.uid}", ctx.socket])
    assert File.exists?(socket_path), "tmux named a socket that does not exist: #{socket_path}"

    for leaked <- default_socket_paths(ctx) do
      refute File.exists?(leaked), "the private socket leaked to a default tmux socket location: #{leaked}"
    end

    # ANTI-VACUITY for the default-location control: the fallback path has exactly the
    # shape tmux gave the owned socket (tmux-<uid>/<name>) and is a different path, so the
    # refutation above is about the place a lost export would really put the socket.
    [fallback] = default_socket_paths(ctx)
    assert String.ends_with?(fallback, "/tmux-#{ctx.uid}/#{ctx.socket}")
    refute String.starts_with?(fallback, ctx.socket_dir <> "/")
  end

  test "deliver pastes the projected bytes into the real pane through the private socket only", ctx do
    pane_ref = tmux!(ctx, ["list-panes", "-t", "dispatch", "-F", "\#{pane_id}"])
    assert pane_ref =~ ~r/\A%\d+\z/, "expected exactly one pane id, got #{inspect(pane_ref)}"

    # ANTI-VACUITY: the pane does not carry the prompt before the dispatch, and no tmux
    # argv has been executed by the fake ap yet.
    refute capture(ctx) =~ @prompt
    refute File.exists?(ctx.log)

    command = %{
      "assignment_id" => "as_0001",
      "artifact_id" => "art_as_0001",
      "artifact_baseline" => %{"exists" => false},
      "expected_artifact" => "lib/item_a.ex",
      "pane_ref" => pane_ref,
      "payload_hash" => sha256(@prompt),
      "prompt" => @prompt,
      "repo_root" => ctx.root,
      "send_message_id" => "send_as_0001"
    }

    # The delivered adapter over the delivered client: preflight (ping), reconcile, then the
    # send with the prompt on stdin, all as real `ap` OS processes.
    assert {:ok, data} = LocalPane.deliver(command, ap_path: ctx.ap)

    assert %{
             "assignment_id" => "as_0001",
             "backend" => "local_pane",
             "pane_ref" => ^pane_ref,
             "prompt_hash" => prompt_hash,
             "replayed" => false,
             "send_message_id" => "send_as_0001",
             "send_status" => "ok"
           } = data

    assert prompt_hash == sha256(@prompt)

    assert wait_until(fn -> capture(ctx) =~ @prompt end, 5_000),
           "the prompt bytes never appeared in the real pane; last capture: #{inspect(capture(ctx))}"

    # Every tmux invocation the fake ap made named the private socket: two, load then paste.
    assert File.read!(ctx.log) == "load-buffer -L #{ctx.socket}\npaste-buffer -L #{ctx.socket}\n"

    # The socket is still where it was created and nowhere else after the dispatch.
    assert tmux!(ctx, ["display-message", "-p", "\#{socket_path}"]) ==
             Path.join([ctx.socket_dir, "tmux-#{ctx.uid}", ctx.socket])

    for leaked <- default_socket_paths(ctx) do
      refute File.exists?(leaked), "the private socket leaked to a default tmux socket location: #{leaked}"
    end
  end
end
