defmodule AiOrchestrator.PaneRegistry.RootLock do
  @moduledoc """
  The one claims-root lock (NS-15.G.005, B3a G2): a kernel flock on `<root>/.root-lock`, held by the native
  `root_lock` helper (native/root_lock) for the calling process over an Erlang Port.

  The helper never blocks inside flock: it retries non-blocking attempts until its own deadline and ends at its
  first stdin check after stdin delivers anything or closes, so an abandoned waiter that acquires in the gap before
  that check holds only until the check and runs no caller code. A helper killed from outside while the caller's
  function runs releases the lock through the kernel; that is answered as a lost lock, never as exclusive success.
  This side never signals the
  helper's operating-system pid: every helper exits by itself (busy, released, or its port closed) and the runtime
  reaps it and reports the exit status. When that report does not arrive within the grace the call answers an
  error, closes the port and logs the pid for an operator; it never retries.

  A legacy `.reclaim-lock` (the removed mkdir mutex of older releases) under the root means an older writer may be
  active: the lock answers `lock_unidentified` and never removes it.
  """

  require Logger

  @default_wait_ms 1_000
  @default_grace_ms 1_000
  @legacy_lock ".reclaim-lock"

  @type reason :: String.t()

  @spec run(String.t(), String.t(), keyword(), (map() -> result)) ::
          {:ok, result} | {:error, reason() | map()}
        when result: term()
  def run(root, owner_token, opts, fun) when is_binary(root) and is_binary(owner_token) and is_function(fun, 1) do
    root = Path.expand(root)

    with :ok <- no_legacy_lock(root),
         {:ok, helper} <- helper(opts),
         {:ok, port} <- open(helper, root, wait_ms(opts)) do
      observe(opts, {:spawned, os_pid(port)})
      deadline = System.monotonic_time(:millisecond) + wait_ms(opts) + grace_ms(opts)

      # Any exception on the way out (a raising on_contend or fun) closes the port first, so the helper sees its
      # stdin close and ends instead of waiting on, or holding for, a caller that has moved on.
      try do
        await_lock(port, deadline, false, opts, fun)
      catch
        kind, reason ->
          close(port)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  defp no_legacy_lock(root) do
    case File.lstat(Path.join(root, @legacy_lock)) do
      {:ok, _any_type} -> {:error, "lock_unidentified"}
      {:error, :enoent} -> :ok
      {:error, _unreadable} -> {:error, "lock_unidentified"}
    end
  end

  defp helper(opts) do
    case Keyword.get(opts, :root_lock_helper) || Application.get_env(:ai_orchestrator, :root_lock_helper) ||
           System.get_env("AI_ORCHESTRATOR_ROOT_LOCK_HELPER") do
      path when is_binary(path) and path != "" -> {:ok, path}
      _unconfigured -> {:error, "lock_unavailable"}
    end
  end

  defp open(helper, root, wait_ms) do
    port =
      Port.open({:spawn_executable, helper}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:args, [root, Integer.to_string(wait_ms)]},
        {:line, 256}
      ])

    {:ok, port}
  rescue
    ErlangError -> {:error, "lock_unavailable"}
  end

  defp await_lock(port, deadline, contended?, opts, fun) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        observe(opts, {:line, line})
        lock_line(line, port, deadline, contended?, opts, fun)

      {^port, {:data, {:noeol, _partial}}} ->
        exited(port, opts, "lock_unavailable")

      {^port, {:exit_status, status}} ->
        observe(opts, {:exit_status, status})
        {:error, "lock_unavailable"}
    after
      remaining(deadline) -> unconfirmed(port, "lock_unavailable")
    end
  end

  defp lock_line("contended " <> _pid, port, deadline, contended?, opts, fun) do
    # on_contend follows the helper's report of an actual EWOULDBLOCK, and only the first one
    if not contended?, do: on_contend(opts)
    await_lock(port, deadline, true, opts, fun)
  end

  defp lock_line("acquired " <> pid, port, _deadline, _contended?, opts, fun) do
    case Integer.parse(pid) do
      {os_pid, ""} -> locked(port, os_pid, opts, fun)
      _malformed -> exited(port, opts, "lock_unavailable")
    end
  end

  defp lock_line("busy", port, _deadline, _contended?, opts, _fun), do: exited(port, opts, "lock_busy")
  defp lock_line(_error_or_other, port, _deadline, _contended?, opts, _fun), do: exited(port, opts, "lock_unavailable")

  defp locked(port, os_pid, opts, fun) do
    outcome =
      try do
        {:returned, fun.(%{"helper_os_pid" => os_pid})}
      catch
        kind, reason -> {:raised, kind, reason, __STACKTRACE__}
      end

    lock = if helper_exited?(port, opts), do: :lost, else: release(port, opts)

    case outcome do
      {:returned, result} when lock == :released -> {:ok, result}
      {:returned, result} when lock == :lost -> {:error, %{"lock" => "lost", "result" => result}}
      {:returned, result} -> {:error, %{"lock" => "release_unconfirmed", "result" => result}}
      {:raised, kind, reason, stacktrace} -> :erlang.raise(kind, reason, stacktrace)
    end
  end

  # Bounded failure (scope r4 C): a helper killed from outside while fun ran has already let the kernel release the
  # lock, so fun may have overlapped another holder. That is detected, never answered as exclusive success.
  defp helper_exited?(port, opts) do
    receive do
      {^port, {:exit_status, status}} ->
        observe(opts, {:exit_status, status})
        true
    after
      0 -> false
    end
  end

  # The helper holds until its stdin delivers anything; the release is confirmed only by exit status 0 as the
  # runtime reports it. Any other status means the helper ended some other way while holding: the lock was lost.
  defp release(port, opts) do
    send_release(port)
    deadline = System.monotonic_time(:millisecond) + grace_ms(opts)

    case await_exit(port, deadline, opts) do
      {:exited, 0} ->
        :released

      {:exited, _status} ->
        :lost

      :unconfirmed ->
        unconfirmed(port, nil)
        :unconfirmed
    end
  end

  defp send_release(port) do
    Port.command(port, "release\n")
  rescue
    ArgumentError -> :closed
  end

  # The helper has said its last line (busy, error, or a malformed line): wait for the runtime's exit report.
  defp exited(port, opts, reason) do
    deadline = System.monotonic_time(:millisecond) + grace_ms(opts)

    case await_exit(port, deadline, opts) do
      {:exited, _status} -> {:error, reason}
      :unconfirmed -> unconfirmed(port, reason)
    end
  end

  defp await_exit(port, deadline, opts) do
    receive do
      {^port, {:data, {_eol, line}}} ->
        observe(opts, {:line, line})
        await_exit(port, deadline, opts)

      {^port, {:exit_status, status}} ->
        observe(opts, {:exit_status, status})
        {:exited, status}
    after
      remaining(deadline) -> :unconfirmed
    end
  end

  # Bounded failure: the exit report did not arrive. Closing the port closes the helper's stdin, which ends it within
  # one poll slice; nothing signals the pid, which may no longer name the helper.
  defp unconfirmed(port, reason) do
    os_pid = os_pid(port)
    close(port)
    Logger.warning("pane claims-root lock helper exit not confirmed; helper os_pid=#{inspect(os_pid)}")
    if reason, do: {:error, "lock_unavailable"}, else: :unconfirmed
  end

  defp close(port) do
    Port.close(port)
  rescue
    ArgumentError -> :closed
  end

  defp os_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} -> os_pid
      nil -> nil
    end
  end

  defp on_contend(opts) do
    case Keyword.get(opts, :on_contend) do
      fun when is_function(fun, 0) -> fun.()
      nil -> :ok
    end
  end

  # Test-only observation seam: copies of what this process already received, sent to an observer pid when one is
  # given. Production passes none, and nothing here depends on it.
  defp observe(opts, event) do
    case Keyword.get(opts, :helper_observer) do
      observer when is_pid(observer) -> send(observer, {:root_lock_helper, self(), event})
      nil -> :ok
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
  defp wait_ms(opts), do: Keyword.get(opts, :wait_ms, @default_wait_ms)
  defp grace_ms(opts), do: Keyword.get(opts, :grace_ms, @default_grace_ms)
end
