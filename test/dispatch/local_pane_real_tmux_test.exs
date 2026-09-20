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

  Two properties of the fixture itself are asserted by their own controls (successor
  2026-09-20, after independent review):

    * the owned directory is created EXCLUSIVELY (`File.mkdir/1`, never `mkdir_p`, which
      adopts whatever already exists) under a name unique across OS processes (OS pid,
      VM unique integer, random bytes); a colliding path is retried with a fresh name a
      bounded number of times and is never chmodded, entered or removed; cleanup is
      registered the moment the directory exists, before any later setup step can fail;
    * every path the fake `ap` needs reaches it as DATA in the environment the `ap`
      process is started with, never as text inside the script, so a valid path byte
      that is shell syntax in source (dollar, backtick, double quote) is never shell
      source. A row runs the whole dispatch with such a path and proves nothing ran.

  The fake `ap` is the daemon stand-in the delivered client already talks to in every
  other row; what is real here is the client, the OS process, the paste and the pane.
  The daemon itself is the other repository and is not exercised.

  Needs a `tmux` on PATH: the devShell provides one (flake.nix), so `bin/verify` and CI
  have it. Outside the devShell the row fails loudly rather than skipping.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Dispatch.LocalPane

  @prompt "Implement item A in lib/item_a.ex -- real tmux dispatch row 2026-09-19"

  # The four values the fake `ap` needs, passed as environment DATA. `PaneClient` starts
  # `ap` with this VM's environment (plus ERL_CRASH_DUMP_SECONDS), so the row exports them
  # for its duration and the template below reads them as quoted expansions. The template
  # text therefore never contains a path.
  @env_socket_dir "ORRIS_ROW_SOCKET_DIR"
  @env_tmux "ORRIS_ROW_TMUX"
  @env_socket "ORRIS_ROW_SOCKET"
  @env_log "ORRIS_ROW_LOG"

  # The bash body of the fake `ap`, with no Elixir interpolation and no substitution of any
  # kind: it is written verbatim. It answers the three v2 verbs the delivered client issues
  # on a fresh dispatch (ping, reconcile, send), pastes the send's stdin into the pane
  # through the PRIVATE server only, and logs the socket directory it sees plus every tmux
  # argv it executes so the row can prove no call went anywhere else.
  @fake_ap_body ~S"""
  set -euo pipefail
  unset TMUX TMUX_PANE
  export TMUX_TMPDIR="${ORRIS_ROW_SOCKET_DIR:?}"
  tmux="${ORRIS_ROW_TMUX:?}"
  sock="${ORRIS_ROW_SOCKET:?}"
  log="${ORRIS_ROW_LOG:?}"
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
      printf 'tmpdir %s\n' "$TMUX_TMPDIR" >> "$log"
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

  # A short directory name whose bytes are shell syntax when they appear in shell SOURCE: a
  # dollar command substitution, a backtick pair and a double-quote pair. Each substitution,
  # if it ever ran, would create a marker beside the script (`$0` is the fake ap path):
  # `<ap>.m` from the dollar form, `<ap>.b` from the backtick form. The quotes are a PAIR so
  # that, spliced into a double-quoted assignment, the text before them still parses and the
  # substitutions run (a lone quote leaves the script unterminated, which also fails the row
  # but never reaches the markers; measured 2026-09-20). As data the name is 23 path bytes.
  @shell_dir ~S|d$(>"$0.m")`>"$0.b"`"q"|

  setup ctx do
    tmux = System.find_executable("tmux") || flunk("tmux is required for this row: run through nix develop")
    bash = System.find_executable("bash") || flunk("bash is required")

    # A leaked socket lands under a DEFAULT directory, so the leak control below computes
    # that path exactly as tmux computes it: <dir>/tmux-<uid>/<socket>.
    {uid, 0} = System.cmd("id", ["-u"])
    uid = String.trim(uid)

    {root, socket} = claim_root!(uid)

    # The owned root IS the socket directory (TMUX_TMPDIR points at it), unless the row asks
    # for the shell-syntax subdirectory, which is created inside the owned root.
    socket_dir =
      case ctx[:socket_subdir] do
        nil -> root
        sub -> Path.join(root, sub)
      end

    env = [{"TMUX", nil}, {"TMUX_PANE", nil}, {"TMUX_TMPDIR", socket_dir}]
    log = Path.join(root, "tmux-argv.log")
    ap = Path.join(root, "fake-ap")
    row_env = [{@env_socket_dir, socket_dir}, {@env_tmux, tmux}, {@env_socket, socket}, {@env_log, log}]

    # Ownership is established: register the whole cleanup NOW, before any later step can
    # fail. Killing a server that was never started is harmless ("no server running").
    on_exit(fn ->
      System.cmd(tmux, ["-L", socket, "kill-server"], env: env, stderr_to_stdout: true)
      File.rm_rf!(root)
      for {name, _} <- row_env, do: System.delete_env(name)
    end)

    if socket_dir != root do
      File.mkdir!(socket_dir)
      File.chmod!(socket_dir, 0o700)
    end

    for {name, value} <- row_env, do: System.put_env(name, value)

    {out, 0} =
      System.cmd(
        tmux,
        ["-L", socket, "-f", "/dev/null", "new-session", "-d", "-s", "dispatch", "-x", "80", "-y", "24", "sleep", "3600"],
        env: env,
        stderr_to_stdout: true
      )

    assert out == "", "starting the private server must be silent, got: #{inspect(out)}"

    File.write!(ap, "#!" <> bash <> " -p\n" <> @fake_ap_body)
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
  # LONGEST socket path any row builds under it (the shell-syntax subdirectory row) fits, else
  # under /tmp; either way it is a fresh 0700 directory this row creates and removes, never a
  # directory anything else uses. tmux reports the socket path through realpath(3), so the
  # fallback is /tmp with one symlink level resolved (/private/tmp on macOS, /tmp on Linux;
  # measured 2026-09-20: the first successor draft compared /tmp/... against /private/tmp/...).
  @sun_path_budget 100
  defp socket_root(name, uid, socket) do
    candidate = Path.join(System.tmp_dir!(), name)

    if byte_size(Path.join([candidate, @shell_dir, "tmux-#{uid}", socket])) <= @sun_path_budget,
      do: candidate,
      else: Path.join(fallback_tmp(), name)
  end

  defp fallback_tmp do
    case File.read_link("/tmp") do
      {:ok, target} -> Path.expand(target, "/")
      {:error, _} -> "/tmp"
    end
  end

  # A name no other OS process can be building at the same time: the OS pid, this VM's unique
  # integer and three random bytes. The VM integer alone is unique only inside one VM.
  defp unique_token do
    "#{System.pid()}-#{System.unique_integer([:positive])}-#{Base.encode16(:crypto.strong_rand_bytes(3), case: :lower)}"
  end

  @claim_attempts 8

  defp claim_root!(uid) do
    case claim_root(uid, &unique_token/0, @claim_attempts) do
      {:ok, root, socket} -> {root, socket}
      {:error, reason} -> flunk("no fresh socket root claimed in #{@claim_attempts} attempts: #{inspect(reason)}")
    end
  end

  # Exclusive ownership. `File.mkdir/1` fails with :eexist on a path that already exists and
  # then NOTHING is done to that path: no chmod, no read, no removal; the next token is tried.
  # Only a directory this call created is chmodded and returned. The bound turns a namespace
  # that never yields a fresh name into a refusal instead of an adoption.
  defp claim_root(_uid, _token_fun, 0), do: {:error, :exhausted}

  defp claim_root(uid, token_fun, attempts) do
    token = token_fun.()
    socket = "od#{token}"
    root = socket_root("orris-tmux-#{token}", uid, socket)

    case File.mkdir(root) do
      :ok ->
        File.chmod!(root, 0o700)
        {:ok, root, socket}

      {:error, :eexist} ->
        claim_root(uid, token_fun, attempts - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The token sequence a control test feeds to claim_root/3, popped from the test process.
  defp scripted_tokens(tokens) do
    Process.put(:scripted_tokens, tokens)

    fn ->
      [token | rest] = Process.get(:scripted_tokens)
      Process.put(:scripted_tokens, rest)
      token
    end
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
    dispatch_row(ctx)
  end

  @tag socket_subdir: @shell_dir
  test "a socket directory whose bytes are shell syntax is carried as data: nothing is substituted or run", ctx do
    assert String.ends_with?(ctx.socket_dir, "/" <> @shell_dir)
    assert ctx.socket_dir =~ "$(" and ctx.socket_dir =~ "`" and ctx.socket_dir =~ "\""

    # tmux itself received the same bytes through the environment: its socket is under them.
    assert tmux!(ctx, ["display-message", "-p", "\#{socket_path}"]) ==
             Path.join([ctx.socket_dir, "tmux-#{ctx.uid}", ctx.socket])

    dollar_marker = ctx.ap <> ".m"
    backtick_marker = ctx.ap <> ".b"
    refute File.exists?(dollar_marker)
    refute File.exists?(backtick_marker)

    result = LocalPane.deliver(dispatch_command(ctx), ap_path: ctx.ap)

    refute File.exists?(dollar_marker),
           "the dollar substitution in the socket directory path ran as shell source: #{dollar_marker} exists"

    refute File.exists?(backtick_marker),
           "the backtick substitution in the socket directory path ran as shell source: #{backtick_marker} exists"

    assert {:ok, %{"send_status" => "ok"}} = result
    assert_row_log(ctx)
  end

  test "a pre-existing directory at a colliding path is never adopted, chmodded or removed", ctx do
    # Plant what another process could have left: a directory at exactly the path the first
    # token names, mode 0755, with a file inside that is not ours.
    planted_token = unique_token()
    planted = socket_root("orris-tmux-#{planted_token}", ctx.uid, "od#{planted_token}")
    File.mkdir!(planted)
    File.chmod!(planted, 0o755)
    sentinel = Path.join(planted, "sentinel")
    File.write!(sentinel, "not yours\n")
    on_exit(fn -> File.rm_rf!(planted) end)

    fresh_token = unique_token()
    assert {:ok, root, socket} = claim_root(ctx.uid, scripted_tokens([planted_token, fresh_token]), 2)
    on_exit(fn -> File.rm_rf!(root) end)

    refute root == planted, "the fixture adopted a directory it did not create"
    assert root == socket_root("orris-tmux-#{fresh_token}", ctx.uid, "od#{fresh_token}")
    assert socket == "od#{fresh_token}"
    assert Bitwise.band(File.stat!(root).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(planted).mode, 0o777) == 0o755, "the planted directory was chmodded"
    assert File.read!(sentinel) == "not yours\n"
    assert File.ls!(planted) == ["sentinel"]

    # A namespace that never yields a fresh name is refused after the bound, and the planted
    # directory is still untouched.
    assert claim_root(ctx.uid, fn -> planted_token end, 3) == {:error, :exhausted}
    assert Bitwise.band(File.stat!(planted).mode, 0o777) == 0o755
    assert File.read!(sentinel) == "not yours\n"
    assert File.ls!(planted) == ["sentinel"]
  end

  defp dispatch_command(ctx) do
    %{
      "assignment_id" => "as_0001",
      "artifact_id" => "art_as_0001",
      "artifact_baseline" => %{"exists" => false},
      "expected_artifact" => "lib/item_a.ex",
      "pane_ref" => pane_ref!(ctx),
      "payload_hash" => sha256(@prompt),
      "prompt" => @prompt,
      "repo_root" => ctx.root,
      "send_message_id" => "send_as_0001"
    }
  end

  defp pane_ref!(ctx) do
    pane_ref = tmux!(ctx, ["list-panes", "-t", "dispatch", "-F", "\#{pane_id}"])
    assert pane_ref =~ ~r/\A%\d+\z/, "expected exactly one pane id, got #{inspect(pane_ref)}"
    pane_ref
  end

  # The fake ap's log: the socket directory it observed, byte for byte, then every tmux argv it
  # made, each naming the private socket (two: load then paste).
  defp assert_row_log(ctx) do
    assert File.read!(ctx.log) ==
             "tmpdir #{ctx.socket_dir}\nload-buffer -L #{ctx.socket}\npaste-buffer -L #{ctx.socket}\n"
  end

  defp dispatch_row(ctx) do
    command = dispatch_command(ctx)
    pane_ref = command["pane_ref"]

    # ANTI-VACUITY: the pane does not carry the prompt before the dispatch, and no tmux
    # argv has been executed by the fake ap yet.
    refute capture(ctx) =~ @prompt
    refute File.exists?(ctx.log)

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

    assert_row_log(ctx)

    # The socket is still where it was created and nowhere else after the dispatch.
    assert tmux!(ctx, ["display-message", "-p", "\#{socket_path}"]) ==
             Path.join([ctx.socket_dir, "tmux-#{ctx.uid}", ctx.socket])

    for leaked <- default_socket_paths(ctx) do
      refute File.exists?(leaked), "the private socket leaked to a default tmux socket location: #{leaked}"
    end
  end
end
