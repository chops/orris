# Opens the journal writer for a run directory from a separate OS process and
# holds it for hold_ms (or until killed, or until its stdin reaches EOF: the
# owning Port went away, so the holder releases and leaves SILENTLY - a write to
# the dead stdio would otherwise crash the runtime and dump). Used by the
# cross-process lock tests.
alias AiOrchestrator.Journal.Ownership
alias AiOrchestrator.Journal.Writer

[run_dir, hold_ms] = System.argv()

# This script runs as a bare `elixir` process with no application started, so
# the ownership arbiter every writer acquires through has to be started here.
# It arbitrates only inside this BEAM; exclusion against the test's BEAM is
# still `RunLock`'s, which is exactly what this helper exists to exercise.
{:ok, _arbiter} = Ownership.start_link()

say = fn payload ->
  try do
    IO.puts(Jason.encode!(payload))
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end

case Writer.open(run_dir, lock: [supervisor_instance: "sup_os_" <> System.pid()]) do
  {:ok, writer, opened} ->
    say.(%{"status" => "acquired", "pid" => System.pid(), "last_seq" => opened.last_seq})
    main = self()

    spawn(fn ->
      IO.read(:stdio, :eof) && :ok
      send(main, :stdin_eof)
    end)

    receive do
      :stdin_eof ->
        _ = Writer.close(writer)
        System.halt(0)
    after
      String.to_integer(hold_ms) ->
        _ = Writer.close(writer)
        say.(%{"status" => "released", "pid" => System.pid()})
    end

  {:error, rejection} ->
    say.(%{"status" => "rejected", "pid" => System.pid(), "error" => rejection})
end
