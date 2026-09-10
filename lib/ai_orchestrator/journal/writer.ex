defmodule AiOrchestrator.Journal.Writer do
  @moduledoc """
  The sole writer of a run's journal (Gate B).

  One process per open run directory. `open/2` acquires the run lock through
  `AiOrchestrator.Journal.Ownership`, which serialises the whole local
  acquisition transaction and records the held lock before this process is
  told it owns it; the writer never claims a lock itself. It then verifies
  the journal bytes and the head receipt (`AiOrchestrator.Journal.Chain`), performs the one bounded repair
  the write protocol allows (truncate a single torn tail, advance a receipt
  by one line), and keeps a raw append descriptor. Every rejection is named;
  nothing is guessed.

  `append/2` takes the envelope the run supervisor built, stamps
  `schema_version` 2 and `prev_line_sha256`, validates it with
  `AiOrchestrator.Journal.Event.validate_append/1` before any byte is written,
  and then persists in this order: line write, file fsync, receipt written to
  `events.head.tmp` and fsynced, atomic rename to `events.head`, directory
  fsync (OPEN-19(a), OPEN-01(a)). A failure at any stage names its stage and
  fails the writer closed; the on-disk state is always one `open/2` can
  reconcile. The returned map is the persisted event, so memory equals disk.
  """

  use GenServer

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.RunLock

  @journal "events.jsonl"
  @repair "events.jsonl.repair"
  @head "events.head"
  @head_tmp "events.head.tmp"
  # Long enough for the close legs (descriptor close, lock tombstone, fsyncs).
  @shutdown 15_000
  # The birth acknowledgment bound (docs/contracts/core-startup-bound.org, section 3).
  @ack 5_000

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type opened :: %{
          lock_path: String.t(),
          last_seq: non_neg_integer(),
          lines: [binary()],
          envelope_version: 0 | 1 | 2,
          version_2_from: pos_integer() | nil,
          receipt_seq: non_neg_integer(),
          repair: Chain.plan() | nil
        }

  @doc """
  Starts a writer under a supervisor.

  Unlike `open/2` this links before `init/1` runs, so a rejection reaches the
  caller as a linked exit as well as a return value; only a process that traps
  exits (a supervisor) should call it. The opened state is read afterwards
  with `opened/1`, because a supervisor keeps only the pid.
  """
  @spec start_link(Path.t(), keyword()) :: {:ok, pid()} | {:error, rejection()}
  def start_link(run_dir, opts \\ []) do
    case GenServer.start_link(__MODULE__, {run_dir, opts}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:shutdown, {:rejected, rejection}}} -> {:error, rejection}
      {:error, reason} -> {:error, %{clause: "writer_start_failed", detail: inspect(reason)}}
    end
  end

  @doc "Child spec for `{run_dir, opts}`, keyed by the run directory it writes."
  def child_spec({run_dir, opts}) do
    %{
      id: {__MODULE__, Path.expand(run_dir)},
      start: {__MODULE__, :start_link, [run_dir, opts]},
      type: :worker,
      restart: :permanent,
      shutdown: @shutdown
    }
  end

  @doc """
  The `run_resumed.data.tail_repair` / `run_cancel_requested.data.tail_repair` record for an
  opened writer's repair plan (nil when the journal needed no repair).
  """
  @spec tail_repair_data(Chain.plan() | nil) :: map() | nil
  def tail_repair_data(nil), do: nil

  def tail_repair_data(%{action: action} = plan) do
    %{
      "action" => Atom.to_string(action),
      "truncated_bytes" => plan.truncate_bytes,
      "receipt_seq_before" => plan.receipt_seq_before,
      "receipt_seq_after" => plan.receipt_seq_after
    }
  end

  @doc "Returns what `open/2` reports, for a writer started with `start_link/2`."
  @spec opened(pid()) :: opened()
  def opened(writer), do: GenServer.call(writer, :opened)

  @spec open(Path.t(), keyword()) :: {:ok, pid(), opened()} | {:error, rejection()}
  def open(run_dir, opts \\ []) do
    # Started unlinked so a named rejection returns instead of exiting the caller;
    # linked on success so the writer lives and dies with its supervisor process.
    case GenServer.start(__MODULE__, {run_dir, opts}) do
      {:ok, pid} ->
        Process.link(pid)
        {:ok, pid, GenServer.call(pid, :opened)}

      {:error, {:shutdown, {:rejected, rejection}}} ->
        {:error, rejection}

      {:error, reason} ->
        {:error, %{clause: "writer_start_failed", detail: inspect(reason)}}
    end
  end

  @spec append(pid(), map()) :: {:ok, map()} | {:error, rejection()}
  def append(pid, event) when is_map(event), do: GenServer.call(pid, {:append, event})

  @typedoc """
  The fence a live snapshot mints: the head it was taken at and this writer's own reference. It is
  authenticated as a WHOLE against the single retained entry (contract: docs/contracts/writer-snapshot.org).
  """
  @type token :: %{seq: non_neg_integer(), last_line_sha256: String.t(), generation: pos_integer(), ref: reference()}

  @typedoc "A live, re-read and hash-verified snapshot; `token` is nil while a repair is pending."
  @type snapshot :: %{
          lines: [binary()],
          seq: non_neg_integer(),
          last_line_sha256: String.t(),
          envelope_version: 0 | 1 | 2,
          version_2_from: pos_integer() | nil,
          receipt: %{seq: pos_integer(), line_sha256: String.t()} | nil,
          repair: Chain.plan() | nil,
          generation: pos_integer(),
          token: token() | nil
        }

  @doc """
  A LIVE verified snapshot: the journal bytes and the head receipt are RE-READ through this writer's Fs
  seam under its held lock, chain-verified and reconciled with the Reader's rules, and then required to
  equal this process's own chain head. The call is serialized with appends, so a durable-but-unreplied
  append is included. Nothing is repaired: a pending plan is reported and mints no token. Refusals are
  closed: `writer_failed` (cause = clause and stage only), `snapshot_unreadable` (source, stage),
  `snapshot_corrupt` (reason = the Chain/Reader clause), `snapshot_divergent` (disk_seq, memory_seq).
  """
  @spec verified(pid()) :: {:ok, snapshot()} | {:error, rejection()}
  def verified(pid), do: GenServer.call(pid, :verified)

  @doc """
  A conditional append: `fence: token` admits the event only if the token is exactly the one this writer
  retained AND the head is unchanged since it was minted; otherwise nothing is written (`fence_invalid`,
  `snapshot_foreign`, `snapshot_stale`). Options are closed: only `fence:` is accepted, and `fence: nil` is
  invalid rather than an unfenced append.
  """
  @spec append(pid(), map(), keyword()) :: {:ok, map()} | {:error, rejection()}
  def append(pid, event, opts) when is_map(event) and is_list(opts), do: GenServer.call(pid, {:append, event, opts})

  @spec last_seq(pid()) :: non_neg_integer()
  def last_seq(pid), do: GenServer.call(pid, :last_seq)

  @doc """
  Closes the descriptor and releases the lock, always attempting both legs.
  Returns `{:error, %{clause: "close_failed", failures: [...]}}` naming each
  failed leg; the process stops either way. A failed lock release may leave
  `run.lock` behind, which the next open reclaims once this OS process is gone.
  """
  @spec close(pid()) :: :ok | {:error, rejection()}
  def close(pid), do: GenServer.call(pid, :close)

  @impl true
  def init({run_dir, opts}) do
    # Trapping exits is what makes the release leg of `terminate/2` run when a
    # supervisor shuts this writer down, so a lock is never stranded by an
    # orderly shutdown.
    Process.flag(:trap_exit, true)
    fs = Keyword.get(opts, :fs, SystemFs.new())
    clock = Keyword.get(opts, :clock, SystemClock)
    ownership = Keyword.get(opts, :ownership, [])

    with :ok <- birth(opts),
         {:ok, lock} <- acquire_lock(run_dir, fs, opts, ownership),
         {:ok, state} <- locked_open(fs, clock, run_dir, lock, ownership, Keyword.get(opts, :create, false)) do
      {:ok, Map.put(state, :ownership, ownership)}
    else
      {:error, rejection} -> {:stop, {:shutdown, {:rejected, rejection}}}
    end
  end

  # The acknowledged birth (docs/contracts/core-startup-bound.org, section 3): a writer started with
  # `birth: {reaper, ref}` announces `{:run_writer_born, ref, self()}` to its reaper and acquires NOTHING until
  # `{:run_writer_ack, ref}` arrives. `{:run_writer_abort, ref}`, the reaper's death or silence at the `ack`
  # bound stops it with a named rejection: no registration, no lock, no descriptor.
  defp birth(opts) do
    case Keyword.get(opts, :birth) do
      {reaper, ref} when is_pid(reaper) and is_reference(ref) ->
        monitor = Process.monitor(reaper)
        send(reaper, {:run_writer_born, ref, self()})

        receive do
          {:run_writer_ack, ^ref} ->
            Process.demonitor(monitor, [:flush])
            :ok

          {:run_writer_abort, ^ref} ->
            Process.demonitor(monitor, [:flush])
            {:error, %{clause: "writer_birth_aborted"}}

          {:DOWN, ^monitor, :process, ^reaper, _reason} ->
            {:error, %{clause: "writer_birth_aborted"}}
        after
          Keyword.get(opts, :ack, @ack) ->
            Process.demonitor(monitor, [:flush])
            {:error, %{clause: "writer_birth_unacknowledged"}}
        end

      _absent ->
        :ok
    end
  end

  defp acquire_lock(run_dir, fs, opts, ownership) do
    Ownership.acquire(run_dir, self(), Keyword.merge([fs: fs, lock: Keyword.get(opts, :lock, [])], ownership))
  end

  @impl true
  def handle_call(:opened, _from, state), do: {:reply, state.opened, state}
  def handle_call(:last_seq, _from, state), do: {:reply, state.last_seq, state}

  def handle_call({:append, _event}, _from, %{failed: rejection} = state) when is_map(rejection) do
    {:reply, {:error, %{clause: "writer_failed", cause: rejection}}, state}
  end

  def handle_call({:append, event}, _from, state), do: append_event(event, state, :legacy)

  # the fence is judged BEFORE the event (options -> token -> head -> event); a refusal writes nothing
  def handle_call({:append, _event, _opts}, _from, %{failed: rejection} = state) when is_map(rejection) do
    {:reply, {:error, %{clause: "writer_failed", cause: closed_cause(rejection)}}, state}
  end

  def handle_call({:append, event, opts}, _from, state) do
    with {:ok, token} <- fence_option(opts),
         :ok <- authenticate(token, state),
         :ok <- current(token, state) do
      append_event(event, state, :closed)
    else
      {:error, rejection} -> {:reply, {:error, rejection}, state}
    end
  end

  def handle_call(:verified, _from, %{failed: rejection} = state) when is_map(rejection) do
    {:reply, {:error, %{clause: "writer_failed", cause: closed_cause(rejection)}}, %{state | token: nil}}
  end

  def handle_call(:verified, _from, state) do
    case snapshot(state) do
      {:ok, snapshot, token} -> {:reply, {:ok, snapshot}, %{state | token: token}}
      {:error, rejection} -> {:reply, {:error, rejection}, %{state | token: nil}}
    end
  end

  def handle_call(:close, _from, state) do
    {result, state} = teardown(state)
    {:stop, :normal, result, state}
  end

  # The parent exit is handled by `:gen_server` itself; this covers the links
  # `open/2` adds, so an opener that dies takes its writer with it rather than
  # leaving one holding a lock nobody is watching.
  @impl true
  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    {_result, _state} = teardown(state)
    :ok
  end

  defp locked_open(fs, clock, run_dir, lock, ownership, create?) do
    with :ok <- maybe_create(fs, run_dir, create?),
         {:ok, verified, repair} <- verified_journal(fs, clock, run_dir) do
      open_descriptor(fs, clock, run_dir, lock, ownership, verified, repair)
    else
      {:error, rejection} -> release_and(fs, run_dir, lock, ownership, {:error, rejection})
    end
  end

  # Exclusive creation of the journal under the lock, made durable with a directory fsync.
  defp maybe_create(_fs, _run_dir, false), do: :ok

  defp maybe_create(fs, run_dir, true) do
    with :ok <- create_failure(Fs.mkdir_p(fs, run_dir)),
         {:ok, fd} <- create_failure(Fs.open(fs, Path.join(run_dir, @journal), [:exclusive])),
         :ok <- create_failure(Fs.close(fs, fd)) do
      create_failure(Fs.dir_sync(fs, run_dir))
    end
  end

  defp create_failure(:ok), do: :ok
  defp create_failure({:ok, fd}), do: {:ok, fd}
  defp create_failure({:error, :eexist}), do: {:error, %{clause: "journal_exists"}}
  defp create_failure({:error, reason}), do: {:error, %{clause: "journal_create_failed", detail: inspect(reason)}}

  defp verified_journal(fs, clock, run_dir) do
    with {:ok, loaded} <- Reader.load(run_dir, fs: fs),
         :ok <- execute(loaded.pending_repair, loaded.verified, fs, clock, run_dir),
         :ok <- clear_head_temp(fs, run_dir) do
      {:ok, loaded.verified, loaded.pending_repair}
    end
  end

  # A receipt temp left by a crashed append is removed by the next open, durably: absent is fine,
  # removal needs a directory fsync, and any other outcome is a named rejection (the lock release
  # that follows is reported by release_and).
  defp clear_head_temp(fs, run_dir) do
    case Fs.rm(fs, Path.join(run_dir, @head_tmp)) do
      {:error, :enoent} ->
        :ok

      :ok ->
        case Fs.dir_sync(fs, run_dir) do
          :ok -> :ok
          {:error, reason} -> {:error, %{clause: "head_temp_removed_unsynced", path: @head_tmp, detail: inspect(reason)}}
        end

      {:error, reason} ->
        {:error, %{clause: "head_temp_cleanup_required", path: @head_tmp, detail: inspect(reason)}}
    end
  end

  defp open_descriptor(fs, clock, run_dir, lock, ownership, verified, repair) do
    case Fs.open(fs, Path.join(run_dir, @journal), [:append]) do
      {:ok, fd} ->
        {:ok,
         %{
           fs: fs,
           clock: clock,
           run_dir: run_dir,
           lock: lock,
           fd: fd,
           last_seq: verified.count,
           last_hash: verified.last_line_sha256,
           failed: nil,
           closed: false,
           # the ONE retained fence token (the most recently minted); nil while none, failed or pending
           token: nil,
           opened: %{
             lock_path: Path.basename(lock.path),
             last_seq: verified.count,
             lines: verified.lines,
             envelope_version: verified.envelope_version,
             version_2_from: verified.version_2_from,
             receipt_seq: if(repair, do: repair.receipt_seq_after, else: verified.count),
             repair: repair
           }
         }}

      {:error, reason} ->
        release_and(fs, run_dir, lock, ownership, {:error, %{clause: "journal_open_failed", detail: inspect(reason)}})
    end
  end

  defp execute(nil, _verified, _fs, _clock, _run_dir), do: :ok

  defp execute(%{action: action} = plan, verified, fs, clock, run_dir) do
    with :ok <- maybe_truncate(action, verified, fs, run_dir) do
      maybe_advance(action, plan, verified, fs, clock, run_dir)
    end
  end

  defp maybe_truncate(action, verified, fs, run_dir) when action in [:truncate_tail, :advance_and_truncate] do
    bytes = Enum.map(verified.lines, &(&1 <> "\n"))

    case publish(fs, run_dir, @repair, @journal, bytes) do
      :ok -> :ok
      {:error, reason} -> {:error, %{clause: "repair_failed", stage: "truncate", detail: inspect(reason)}}
    end
  end

  defp maybe_truncate(_action, _verified, _fs, _run_dir), do: :ok

  defp maybe_advance(action, plan, verified, fs, clock, run_dir)
       when action in [:advance_receipt, :advance_and_truncate] do
    case write_receipt(fs, clock, run_dir, plan.receipt_seq_after, verified.last_line_sha256) do
      :ok -> :ok
      {:error, reason} -> {:error, %{clause: "repair_failed", stage: "receipt", detail: inspect(reason)}}
    end
  end

  defp maybe_advance(_action, _plan, _verified, _fs, _clock, _run_dir), do: :ok

  # `shape` names the reply the persist stages may take: the legacy append/2 error (with its seam
  # detail) or the fenced append/3 error, which is closed (clause and stage only) from its FIRST failure.
  defp append_event(event, state, shape) do
    with :ok <- check_seq(event, state.last_seq),
         stamped = stamp(event, state.last_hash),
         {:ok, _parsed} <- Event.validate_append(stamped) do
      persist(stamped, state, shape)
    else
      {:error, rejection} -> {:reply, {:error, rejection}, state}
    end
  end

  # ---- the live verified snapshot (docs/contracts/writer-snapshot.org) ----

  defp snapshot(state) do
    case Reader.load(state.run_dir, fs: state.fs) do
      {:ok, loaded} -> linearize(loaded, state)
      {:error, %{clause: "journal_unreadable"}} -> {:error, unreadable("journal")}
      {:error, %{clause: "journal_missing"}} -> {:error, unreadable("journal")}
      {:error, %{clause: "receipt_unreadable"}} -> {:error, unreadable("receipt")}
      {:error, %{clause: clause}} -> {:error, %{clause: "snapshot_corrupt", reason: clause}}
    end
  end

  defp unreadable(source), do: %{clause: "snapshot_unreadable", source: source, stage: "read"}

  # the re-read must equal this process's own head; a pending repair is evidence and mints no token
  defp linearize(%{verified: verified} = loaded, state) do
    cond do
      verified.count != state.last_seq or verified.last_line_sha256 != state.last_hash ->
        {:error, %{clause: "snapshot_divergent", disk_seq: verified.count, memory_seq: state.last_seq}}

      loaded.pending_repair != nil ->
        {:ok, snapshot_map(loaded, state, nil), nil}

      true ->
        token = retained_or_minted(state)
        {:ok, snapshot_map(loaded, state, token), token}
    end
  end

  defp snapshot_map(loaded, state, token) do
    %{
      lines: loaded.lines,
      seq: loaded.last_seq,
      last_line_sha256: loaded.verified.last_line_sha256,
      envelope_version: loaded.envelope_version,
      version_2_from: loaded.version_2_from,
      receipt: receipt_evidence(loaded.receipt),
      repair: loaded.pending_repair,
      generation: generation(state),
      token: token
    }
  end

  defp receipt_evidence(nil), do: nil
  defp receipt_evidence(%{seq: seq, line_sha256: hash}), do: %{seq: seq, line_sha256: hash}

  # one retained entry: the same token while the head is unchanged, a fresh one otherwise
  defp retained_or_minted(%{token: %{seq: seq, last_line_sha256: hash} = token} = state)
       when seq == state.last_seq and hash == state.last_hash, do: token

  defp retained_or_minted(state),
    do: %{seq: state.last_seq, last_line_sha256: state.last_hash, generation: generation(state), ref: make_ref()}

  defp generation(%{lock: %{generation: generation}}) when is_integer(generation), do: generation

  # ---- the fence: closed option domain, whole-token authentication, head check ----

  @token_keys [:generation, :last_line_sha256, :ref, :seq]

  # The option domain is closed by structure, never by Keyword functions: exactly `[fence: value]` is the
  # only admitted list; the empty list is a missing fence; anything else (an unknown or duplicated key, a
  # bare element, a non-atom key, an improper list) is refused before any element is inspected, so a
  # malformed list can neither kill the Writer nor echo its bytes.
  defp fence_option([{:fence, value}]), do: fence_token(value)
  defp fence_option([]), do: {:error, %{clause: "fence_invalid", field: "fence"}}
  defp fence_option(_other), do: {:error, %{clause: "fence_invalid", field: "options"}}

  defp fence_token(%{} = token) do
    cond do
      Enum.sort(Map.keys(token)) != @token_keys ->
        {:error, %{clause: "fence_invalid", field: "keys"}}

      not (is_integer(token.seq) and token.seq >= 0) ->
        {:error, %{clause: "fence_invalid", field: "seq"}}

      not is_binary(token.last_line_sha256) ->
        {:error, %{clause: "fence_invalid", field: "last_line_sha256"}}

      not (is_integer(token.generation) and token.generation > 0) ->
        {:error, %{clause: "fence_invalid", field: "generation"}}

      not is_reference(token.ref) ->
        {:error, %{clause: "fence_invalid", field: "ref"}}

      true ->
        {:ok, token}
    end
  end

  defp fence_token(_other), do: {:error, %{clause: "fence_invalid", field: "fence"}}

  defp authenticate(token, %{token: token}) when is_map(token), do: :ok
  defp authenticate(_token, _state), do: {:error, %{clause: "snapshot_foreign"}}

  defp current(%{seq: seq, last_line_sha256: hash}, %{last_seq: seq, last_hash: hash}), do: :ok

  defp current(%{seq: seq}, state),
    do: {:error, %{clause: "snapshot_stale", expected_seq: seq, current_seq: state.last_seq}}

  # the new API never carries the legacy append error's seam detail: clause and stage only
  defp closed_cause(%{clause: clause, stage: stage}), do: %{clause: clause, stage: stage}
  defp closed_cause(%{clause: clause}), do: %{clause: clause}

  defp check_seq(%{"seq" => seq}, last_seq) when seq == last_seq + 1, do: :ok

  defp check_seq(event, last_seq) do
    {:error, %{clause: "seq_mismatch", expected: last_seq + 1, got: Map.get(event, "seq")}}
  end

  defp stamp(event, last_hash) do
    event
    |> Map.put("schema_version", 2)
    |> Map.put("prev_line_sha256", last_hash)
  end

  defp persist(stamped, state, shape) do
    line = Jason.encode!(stamped) <> "\n"

    with :ok <- stage(Fs.write(state.fs, state.fd, line), "write"),
         :ok <- stage(Fs.sync(state.fs, state.fd), "sync"),
         hash = Chain.line_sha256(line),
         :ok <- stage(write_receipt(state.fs, state.clock, state.run_dir, stamped["seq"], hash), "receipt") do
      next = %{state | last_seq: stamped["seq"], last_hash: hash}
      {:reply, {:ok, stamped}, next}
    else
      # the Writer remembers the full failure; only the legacy reply carries its seam detail
      {:error, rejection} -> {:reply, {:error, reply_shape(rejection, shape)}, %{state | failed: rejection}}
    end
  end

  defp reply_shape(rejection, :legacy), do: rejection
  defp reply_shape(rejection, :closed), do: closed_cause(rejection)

  defp stage(:ok, _stage), do: :ok
  defp stage({:error, reason}, stage), do: {:error, %{clause: "append_failed", stage: stage, detail: inspect(reason)}}

  defp write_receipt(fs, clock, run_dir, seq, hash) do
    receipt = Chain.encode_receipt(%{seq: seq, line_sha256: hash, updated_at: clock.wall_ts()})
    publish(fs, run_dir, @head_tmp, @head, [receipt])
  end

  # Durable replace: write the temp file, fsync, close, rename over the target, fsync the directory.
  defp publish(fs, run_dir, tmp_name, final_name, iodata_list) do
    tmp = Path.join(run_dir, tmp_name)

    with {:ok, fd} <- Fs.open(fs, tmp, [:write]),
         :ok <- write_all(fs, fd, iodata_list),
         :ok <- Fs.rename(fs, tmp, Path.join(run_dir, final_name)) do
      Fs.dir_sync(fs, run_dir)
    end
  end

  defp write_all(fs, fd, iodata_list) do
    with :ok <- Fs.write(fs, fd, iodata_list),
         :ok <- Fs.sync(fs, fd) do
      Fs.close(fs, fd)
    else
      {:error, reason} ->
        _ = Fs.close(fs, fd)
        {:error, reason}
    end
  end

  # Rolling back an open that failed after the lock was acquired must not hide a failed release:
  # the caller gets both the cause and the release outcome, and the lock path that may be stranded.
  #
  # The registration is retired on exactly the same condition `teardown/1` uses. A rollback that
  # released the disk lock leaves nothing for the arbiter to reclaim, and this process is about to
  # stop, so leaving the record behind would strand a `:down` registration naming a lock that is
  # already gone. A rollback that could NOT release keeps the record, because it is the only
  # authority that can release that file safely later.
  defp release_and(fs, run_dir, lock, ownership, {:error, cause}) do
    case RunLock.release(fs, lock) do
      :ok ->
        :ok = Ownership.release(run_dir, self(), ownership)
        {:error, cause}

      {:error, release} ->
        {:error,
         %{clause: "writer_open_cleanup_failed", cause: cause, release: release, lock_path: Path.basename(lock.path)}}
    end
  end

  defp teardown(%{closed: true} = state), do: {:ok, state}

  defp teardown(state) do
    descriptor = Fs.close(state.fs, state.fd)
    lock = RunLock.release(state.fs, state.lock)

    # The registration is retired only when the lock it names is really gone.
    # A release that failed leaves a lock on disk which that registration is
    # the only authority to release safely, so the arbiter keeps it and
    # reclaims it when this directory is next acquired.
    if lock == :ok, do: :ok = Ownership.release(state.run_dir, self(), state.ownership)

    failures =
      Enum.flat_map([{"descriptor", descriptor}, {"lock", lock}], fn
        {_leg, :ok} -> []
        {leg, {:error, reason}} -> [%{leg: leg, detail: inspect(reason)}]
      end)

    result = if failures == [], do: :ok, else: {:error, %{clause: "close_failed", failures: failures}}
    {result, %{state | closed: true}}
  end
end
