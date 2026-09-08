defmodule AiOrchestrator.Journal.Ownership do
  @moduledoc """
  Arbiter of run-directory ownership inside one BEAM host.

  `AiOrchestrator.Journal.RunLock` decides ownership *between* OS processes
  from the holder's pid and pid start time. Inside a single BEAM that test
  cannot separate a dead writer from a live one: a brutally killed writer
  strands a lock file naming this OS process, which is still alive, so its
  replacement reads its own predecessor's lock as held and can never start.

  This arbiter closes that ambiguity and only that one. It serialises the
  whole local acquisition transaction for a run directory: it inspects the
  registration it holds for that directory, releases a stranded lock only
  when the on-disk holder is exactly the record it registered, calls
  `RunLock.acquire/3`, and records the held lock and a monitor on the writer
  *before* it replies. Writers never call `RunLock.acquire/3` themselves, so
  there is no window in which a writer holds a lock the arbiter has not
  recorded, and none in which a writer dies between acquiring and
  registering.

  What it never does: infer ownership from the shared OS pid, release a lock
  it did not register, or widen `RunLock`'s cross-process exclusion. It
  releases only on an exact-record match — the generation it published and
  holder metadata equal in full to the owner map written under it. When the
  registered writer is dead but the disk holds anything else, in any field,
  the registration is stale evidence and not authority: the arbiter drops it
  untouched and lets `RunLock` adjudicate, which fails closed on a live
  holder.

  The arbiter's own rejections are exactly three: `second_live_writer`,
  `reclaim_failed`, and `ownership_unavailable`. Everything else a caller sees
  came from `RunLock` on a fresh claim and keeps that module's contract.

  An arbiter that has restarted holds no registrations and therefore reclaims
  nothing; a stranded lock then fails closed as `run_locked` until the run
  tree it belongs to restarts and re-registers, or an operator adjudicates.
  Because a lost arbiter must not leave run trees holding unrecorded locks,
  it is mounted first under a `rest_for_one` host root
  (`AiOrchestrator.Application`), so its loss terminates the trees beneath it
  and each writer releases its own token-safe lock on the way down.

  Nothing this module names carries a token, in any form, or an absolute
  path. `status/2` answers with a writer pid, a generation and a state.
  `reclaim_failed` is rebuilt rather than forwarded, from the generation, the
  cause clause, the resulting lock state, and the run-relative basename of the
  lock file — because the `RunLock` release rejection it is built from does
  carry the decoded owner, the raw token and the file's absolute path.
  Rejections `RunLock` raises on a fresh claim are passed through exactly as
  that module produced them, under that module's documented contract.
  """

  use GenServer

  alias AiOrchestrator.Journal.RunLock

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type registration :: %{writer: pid(), generation: pos_integer(), state: :live | :down}

  # A writer blocks in `init/1` for this long; a hung arbiter must surface as a
  # named rejection rather than an unbounded wait.
  @acquire_timeout 30_000

  @unavailable %{clause: "ownership_unavailable"}

  @doc """
  Starts the arbiter, registered as `__MODULE__` unless `:name` says otherwise.

  `name: nil` starts an unregistered arbiter, which is how a test or a second
  host root gets one that arbitrates only for the callers that address it by
  pid; every other caller keeps reaching the registered one.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, registration(name))
  end

  defp registration(nil), do: []
  defp registration(name), do: [name: name]

  @doc """
  Acquires the run lock for `run_dir` on behalf of `writer_pid`.

  The whole transaction — stale-registration check, authorised release of a
  stranded lock, `RunLock.acquire/3`, and recording the held lock with a
  monitor — happens inside this call, so a caller that receives `{:ok, held}`
  is already registered.

  Options: `:fs` (required, the `AiOrchestrator.Journal.Fs` seam), `:lock`
  (options forwarded to `RunLock.acquire/3`), `:server`, `:acquire_timeout`.
  """
  @spec acquire(Path.t(), pid(), keyword()) :: {:ok, RunLock.held()} | {:error, rejection()}
  def acquire(run_dir, writer_pid, opts \\ []) when is_pid(writer_pid) do
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)
    {timeout, opts} = Keyword.pop(opts, :acquire_timeout, @acquire_timeout)
    call(server, {:acquire, run_dir, writer_pid, opts}, timeout, {:error, @unavailable})
  end

  @doc """
  Drops the registration `writer_pid` holds for `run_dir`.

  The disk lock is the writer's own to release during its shutdown; this only
  retires the arbiter's record of it, so a lock released gracefully is never
  offered to the reclaim path. An arbiter that is already gone has no record
  to retire, which is why this cannot fail.
  """
  @spec release(Path.t(), pid(), keyword()) :: :ok
  def release(run_dir, writer_pid, opts \\ []) when is_pid(writer_pid) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :acquire_timeout, @acquire_timeout)
    call(server, {:release, run_dir, writer_pid}, timeout, :ok)
  end

  @doc """
  Reports the registration held for `run_dir`, without token or path bytes.

  `:none` is an authoritative answer from a live arbiter: there is no
  registration for that directory. An arbiter that could not answer at all is
  `{:error, %{clause: "ownership_unavailable"}}`, never `:none`, because the
  two mean opposite things to a caller deciding whether a lock may be touched.
  """
  @spec status(Path.t(), keyword()) :: {:ok, registration()} | :none | {:error, rejection()}
  def status(run_dir, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    call(server, {:status, run_dir}, Keyword.get(opts, :acquire_timeout, @acquire_timeout), {:error, @unavailable})
  end

  # `GenServer.call/3` is the only thing in this body that can exit, so every
  # exit is the arbiter failing to answer: absent, hung, shutting down, killed,
  # or crashed with the call in flight. None of the reason is carried out —
  # it quotes the call arguments, which include the run directory and the lock
  # options — so all of them collapse to the one named outcome.
  defp call(server, message, timeout, unavailable) do
    GenServer.call(server, message, timeout)
  catch
    :exit, _reason -> unavailable
  end

  @impl true
  def init(_opts), do: {:ok, %{registry: %{}, monitors: %{}}}

  @impl true
  def handle_call({:acquire, run_dir, writer_pid, opts}, _from, state) do
    key = key(run_dir)

    case authorise(Map.get(state.registry, key), opts[:fs], run_dir) do
      {:ok, state_after} ->
        state = forget(state, key)
        claim(state, key, run_dir, writer_pid, opts, state_after)

      {:error, rejection} ->
        {:reply, {:error, rejection}, state}
    end
  end

  def handle_call({:release, run_dir, writer_pid}, _from, state) do
    key = key(run_dir)

    case Map.get(state.registry, key) do
      %{writer: ^writer_pid} -> {:reply, :ok, forget(state, key)}
      _other -> {:reply, :ok, state}
    end
  end

  def handle_call({:status, run_dir}, _from, state) do
    case Map.get(state.registry, key(run_dir)) do
      nil -> {:reply, :none, state}
      registration -> {:reply, {:ok, Map.take(registration, [:writer, :generation, :state])}, state}
    end
  end

  # A DOWN retires nothing: the lock the dead writer held may still be on disk,
  # and the record of it is the only authority that can release it safely. The
  # next acquisition for that directory reclaims it under `authorise/3`.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.monitors, ref) do
      {:ok, key} ->
        registry = Map.update!(state.registry, key, &%{&1 | state: :down, ref: nil})
        {:noreply, %{state | registry: registry, monitors: Map.delete(state.monitors, ref)}}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # No registration: plain `RunLock` semantics, including failing closed on a
  # stranded lock this arbiter never recorded (an arbiter that restarted).
  defp authorise(nil, _fs, _run_dir), do: {:ok, :fresh}

  # A registration whose writer has died but whose DOWN has not been processed
  # yet is reclaimed on the same terms as one already marked down; liveness is
  # the monitor and the BEAM pid together, never the shared OS pid.
  defp authorise(%{state: :live, writer: writer} = registration, fs, run_dir) do
    if Process.alive?(writer) do
      {:error, %{clause: "second_live_writer", generation: registration.generation}}
    else
      reclaim(registration, fs, run_dir)
    end
  end

  defp authorise(%{state: :down} = registration, fs, run_dir), do: reclaim(registration, fs, run_dir)

  # `:absent` and `:released` are kept apart so that a rejection is only ever
  # annotated as reclaimed when this arbiter actually released something.
  defp reclaim(registration, fs, run_dir) do
    case RunLock.holder(fs, run_dir) do
      :none -> {:ok, :absent}
      {:ok, holder} -> release_if_ours(registration, fs, holder)
      {:error, rejection} -> {:error, rejection}
    end
  end

  # Only the exact record this arbiter registered is released: the same
  # generation, and holder metadata equal in full to the owner map that was
  # written when this arbiter registered it. The token alone is not the record
  # — pid, pid_start, supervisor_instance or acquired_at can differ while a
  # token is retained — and this is the only code authorised to remove a
  # live-looking lock file, so it compares the whole closed shape.
  #
  # Anything else means the registration is stale evidence, not authority: it
  # is dropped untouched and `RunLock` decides. The match is total over
  # `RunLock.holder/0`, so no holder shape can raise here; that matters
  # because this process is the root of a `rest_for_one` host tree.
  defp release_if_ours(registration, fs, %{generation: generation, metadata: metadata}) do
    held = registration.held

    if generation == held.generation and metadata == held.owner do
      case RunLock.release(fs, held) do
        :ok -> {:ok, :released}
        {:error, rejection} -> {:error, reclaim_failed(rejection, held)}
      end
    else
      {:ok, :superseded}
    end
  end

  # MUST-4: a `RunLock` release rejection names the file, the decoded owner and
  # the raw token. None of that may cross the arbiter's own error boundary, so
  # a failed reclaim is rebuilt from bounded facts: which generation, which
  # cause, what the lock file is now, and the run-relative basename an operator
  # needs to find it.
  #
  # `RunLock.rejection/0` requires a binary `:clause`, and Dialyzer proves in
  # this gate that nothing else can arrive here, so a defensive fallback would
  # be unreachable code rather than protection. An unrecognised clause *value*
  # is still handled, by `lock_state/1`.
  defp reclaim_failed(%{clause: cause}, held) do
    %{
      clause: "reclaim_failed",
      generation: held.generation,
      lock_basename: Path.basename(held.path),
      cause_clause: cause,
      lock_state: lock_state(cause)
    }
  end

  # Classed from the cause `RunLock` names structurally, never inferred from
  # wording: the tombstone either stands (the lock is released and files remain
  # to be cleaned) or it never became visible (the lock is still held).
  defp lock_state("release_incomplete"), do: :released_residue
  defp lock_state("release_removed_unsynced"), do: :released_unsynced
  defp lock_state("release_failed"), do: :still_held
  defp lock_state("not_owner"), do: :not_ours
  defp lock_state(_other), do: :unknown

  # The monitor and the held record are in place before the reply, so a writer
  # that dies the instant it is told it owns the lock is still reclaimable.
  defp claim(state, key, run_dir, writer_pid, opts, reclaimed) do
    case RunLock.acquire(opts[:fs], run_dir, Keyword.get(opts, :lock, [])) do
      {:ok, held} ->
        ref = Process.monitor(writer_pid)
        registration = %{writer: writer_pid, ref: ref, held: held, generation: held.generation, state: :live}

        state = %{
          state
          | registry: Map.put(state.registry, key, registration),
            monitors: Map.put(state.monitors, ref, key)
        }

        {:reply, {:ok, held}, state}

      {:error, rejection} ->
        {:reply, {:error, annotate(rejection, reclaimed)}, state}
    end
  end

  # A failure that follows an authorised release says so, so an operator can
  # tell "never reclaimed" from "reclaimed and still refused".
  defp annotate(rejection, :released), do: Map.put(rejection, :reclaimed, true)
  defp annotate(rejection, _other), do: rejection

  defp forget(state, key) do
    case Map.pop(state.registry, key) do
      {nil, _registry} ->
        state

      {%{ref: ref}, registry} ->
        if is_reference(ref), do: Process.demonitor(ref, [:flush])
        %{state | registry: registry, monitors: Map.delete(state.monitors, ref)}
    end
  end

  defp key(run_dir), do: Path.expand(run_dir)
end
