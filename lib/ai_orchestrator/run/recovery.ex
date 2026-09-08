defmodule AiOrchestrator.Run.Recovery do
  @moduledoc """
  Recovery acquisition (U1a-A, docs/contracts/recovery-acquisition.org): a consumer over the existing Journal
  APIs, nothing more.

    * `evidence/2` - COLD evidence: the Reader's verified view without a lock, structurally not a capability
      (no generation, no token), Reader errors normalized into the closed refusal domain.
    * `acquire/2` - APPENDING AUTHORITY: `Writer.open` (Ownership + RunLock + the Writer's own bounded repair)
      followed by `Writer.verified/1`; success only with a token. Every refusal carries `cleanup`, the truth the
      seams' returns prove about what this attempt left behind - never more.
    * `release/1` - explicit release through `Writer.close`, its structured leg names reported as they are.

  `acquire/2` and `release/1` require a TRAPPING caller: `Writer.open` links the Writer to the caller, and a
  Writer that dies during verification would otherwise take a non-trapping caller with it. The flag is never
  mutated here. This module writes no event, admits no effect, retries nothing, removes no lock, and never
  parses a seam's `detail`.
  """

  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Journal.Writer

  @type cleanup :: %{lock: atom(), registration: atom(), descriptor: atom()}
  @type refusal :: %{
          required(:clause) => String.t(),
          required(:stage) => String.t(),
          required(:cleanup) => cleanup(),
          optional(atom()) => term()
        }
  @type handle :: %{writer: pid(), generation: pos_integer(), snapshot: map(), repaired: map() | nil}

  @none %{lock: :none, registration: :none, descriptor: :none}
  @released %{lock: :released, registration: :released, descriptor: :none}
  @unknown %{lock: :unknown, registration: :unknown, descriptor: :unknown}

  @evidence_keys [:fs]
  @acquire_keys [:fs, :clock, :ownership, :lock]
  @handle_keys [:generation, :repaired, :snapshot, :writer]
  @lock_keys [:owner_status, :pid, :pid_start, :supervisor_instance, :token]
  @down_ms 5_000
  @timeout_cleanup %{lock: :unknown, registration: :retained, descriptor: :unknown}
  @generation_name ~r/\Arun\.lock\.([1-9][0-9]{0,11})\z/
  @candidate_name ~r/\Arun\.lock\.([1-9][0-9]{0,11})\.[A-Za-z0-9_-]{1,64}\.tmp\z/

  # ---- evidence ----

  @spec evidence(Path.t(), keyword()) :: {:ok, map()} | {:error, refusal()}
  def evidence(run_dir, opts \\ []) do
    with {:ok, opts} <- options(opts, @evidence_keys) do
      case Reader.load(run_dir, opts) do
        {:ok, loaded} -> {:ok, evidence_map(loaded)}
        {:error, rejection} -> {:error, Map.put(read_refusal(rejection), :cleanup, @none)}
      end
    end
  end

  defp evidence_map(loaded) do
    %{
      lines: loaded.lines,
      seq: loaded.last_seq,
      last_line_sha256: loaded.verified.last_line_sha256,
      envelope_version: loaded.envelope_version,
      version_2_from: loaded.version_2_from,
      receipt: receipt_evidence(loaded.receipt),
      repair: loaded.pending_repair
    }
  end

  defp receipt_evidence(nil), do: nil
  defp receipt_evidence(%{seq: seq, line_sha256: hash}), do: %{seq: seq, line_sha256: hash}

  # ---- acquire ----

  @spec acquire(Path.t(), keyword()) :: {:ok, handle()} | {:error, refusal()}
  def acquire(run_dir, opts) do
    with :ok <- trapping_caller(),
         {:ok, opts} <- options(opts, @acquire_keys),
         :ok <- lock_option(opts) do
      case Writer.open(run_dir, opts) do
        {:ok, writer, opened} -> verify(writer, opened)
        {:error, rejection} -> {:error, open_refusal(rejection)}
      end
    end
  end

  # the caller is linked to the Writer (Writer.open); the monitor is the identity-bound witness of its death.
  # A call that TIMES OUT is not death: the Writer is still this attempt's responsibility (retire_live/3).
  defp verify(writer, opened) do
    ref = Process.monitor(writer)

    case call(fn -> Writer.verified(writer) end) do
      {:ok, {:ok, %{token: token} = snapshot}} when is_map(token) ->
        Process.demonitor(ref, [:flush])
        {:ok, %{writer: writer, generation: snapshot.generation, snapshot: snapshot, repaired: opened.repair}}

      {:ok, result} ->
        {:error, close_after(writer, ref, opened, verify_refusal(result))}

      :timeout ->
        {:error, retire_live(writer, ref, verify_unavailable("writer_timeout"))}

      :exit ->
        {:error, Map.put(verify_unavailable("writer_exit"), :cleanup, exit_cleanup(down!(writer, ref)))}
    end
  end

  defp verify_unavailable(reason), do: %{clause: "journal_unavailable", stage: "verify", reason: reason}

  # a GenServer call's exit, classified: the call's own timeout, or the Writer's exit
  defp call(fun) do
    {:ok, fun.()}
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, _reason -> :exit
  end

  # the Writer is alive but unresponsive after a timed-out call: a bounded orderly close, then (only if that
  # times out too) termination of this exact owned BEAM process, and its observed DOWN. Its lost reply is never
  # invented: cleanup is what the close return proves, else unknown.
  defp retire_live(writer, ref, refusal) do
    case close(writer) do
      {:legs, legs} -> Map.put(refusal, :cleanup, exit_cleanup(down!(writer, ref), leg_cleanup(legs)))
      :timeout -> Map.put(refusal, :cleanup, kill_cleanup(writer, ref))
      :exit -> Map.put(refusal, :cleanup, exit_cleanup(down!(writer, ref)))
    end
  end

  defp kill_cleanup(writer, ref) do
    Process.exit(writer, :kill)
    exit_cleanup(down!(writer, ref), @timeout_cleanup)
  end

  # a DOWN that was not observed within the bound leaves every fact unknown
  defp exit_cleanup(down_outcome, proven \\ @timeout_cleanup)
  defp exit_cleanup(:down, proven), do: proven
  defp exit_cleanup(:unobserved, _proven), do: @unknown

  defp verify_refusal({:ok, %{token: nil}}), do: %{clause: "journal_corrupt", stage: "verify", reason: "pending_repair"}

  defp verify_refusal({:error, %{clause: "snapshot_unreadable", source: source}}),
    do: %{clause: "journal_unavailable", stage: "verify", source: source}

  defp verify_refusal({:error, %{clause: "snapshot_corrupt", reason: reason}}),
    do: %{clause: "journal_corrupt", stage: "verify", reason: reason}

  defp verify_refusal({:error, %{clause: clause}}), do: %{clause: "journal_corrupt", stage: "verify", reason: clause}

  # the acquired Writer is closed truthfully; the close's structured return is the cleanup evidence
  defp close_after(writer, ref, opened, refusal) do
    case close(writer) do
      {:legs, []} ->
        Map.put(refusal, :cleanup, exit_cleanup(down!(writer, ref), leg_cleanup([])))

      {:legs, legs} ->
        cleanup = exit_cleanup(down!(writer, ref), leg_cleanup(legs))
        refusal |> Map.put(:cleanup, cleanup) |> put_leg_residue(legs, opened_generation(opened))

      :timeout ->
        refusal |> Map.put(:close, "writer_timeout") |> Map.put(:cleanup, kill_cleanup(writer, ref))

      :exit ->
        Map.put(refusal, :cleanup, exit_cleanup(down!(writer, ref)))
    end
  end

  # ---- release ----

  @spec release(handle()) :: :ok | {:error, refusal()}
  def release(handle) do
    with :ok <- trapping_caller(),
         {:ok, writer} <- handle_writer(handle) do
      ref = Process.monitor(writer)
      release_outcome(close(writer), writer, ref, handle.generation)
    end
  end

  defp release_outcome({:legs, []}, writer, ref, _generation) do
    case down!(writer, ref) do
      :down -> :ok
      :unobserved -> {:error, release_refusal(%{reason: "writer_timeout", legs: [], cleanup: @unknown})}
    end
  end

  defp release_outcome({:legs, legs}, writer, ref, generation) do
    cleanup = exit_cleanup(down!(writer, ref), leg_cleanup(legs))
    {:error, %{legs: legs, cleanup: cleanup} |> release_refusal() |> put_leg_residue(legs, generation)}
  end

  # the Writer is alive but unresponsive: this exact owned process is terminated and its DOWN observed
  defp release_outcome(:timeout, writer, ref, _generation),
    do: {:error, release_refusal(%{reason: "writer_timeout", legs: [], cleanup: kill_cleanup(writer, ref)})}

  # a Writer already gone before this release: nothing about its lock, registration or descriptor is proven here
  defp release_outcome(:exit, writer, ref, _generation) do
    _ = down!(writer, ref)
    {:error, release_refusal(%{reason: "writer_exit", legs: [], cleanup: @unknown})}
  end

  defp release_refusal(fields), do: Map.merge(%{clause: "recovery_release_failed", stage: "release"}, fields)

  # the declared handle shape, keys AND values, before any call reaches the Writer
  defp handle_writer(%{writer: writer, generation: generation, snapshot: snapshot, repaired: repaired} = handle)
       when is_pid(writer) and is_integer(generation) and generation > 0 and is_map(snapshot) and
              (is_nil(repaired) or is_map(repaired)) do
    if Enum.sort(Map.keys(handle)) == @handle_keys,
      do: {:ok, writer},
      else: {:error, option_refusal("handle")}
  end

  defp handle_writer(_other), do: {:error, option_refusal("handle")}

  # the close's structured legs, the call's own timeout, or the Writer's exit
  defp close(writer) do
    case call(fn -> Writer.close(writer) end) do
      {:ok, :ok} -> {:legs, []}
      {:ok, {:error, %{clause: "close_failed", failures: failures}}} -> {:legs, Enum.map(failures, & &1.leg)}
      other -> other
    end
  end

  defp leg_cleanup(legs) do
    lock? = "lock" in legs

    %{
      lock: if(lock?, do: :unproven, else: :released),
      registration: if(lock?, do: :retained, else: :released),
      descriptor: if("descriptor" in legs, do: :unproven, else: :closed)
    }
  end

  # the opaque close leg cannot prove ownership: the cached generation locates evidence, nothing more
  defp put_leg_residue(refusal, legs, generation) do
    if "lock" in legs and is_integer(generation),
      do: Map.put(refusal, :residue, %{kind: :generation, generation: generation, ours: :unknown}),
      else: refusal
  end

  # the opened view always names the held lock (Writer.opened/0); its basename yields the generation
  defp opened_generation(%{lock_path: lock_path}), do: lock_path |> residue(:unknown) |> Map.get(:generation)

  # waits (bounded) for the monitored Writer to be gone - an explicit :down | :unobserved outcome, never assumed -
  # and retires the linked exit it sent this (trapping) caller
  defp down!(writer, ref) do
    outcome =
      receive do
        {:DOWN, ^ref, :process, ^writer, _} -> :down
      after
        @down_ms ->
          Process.demonitor(ref, [:flush])
          :unobserved
      end

    receive do
      {:EXIT, ^writer, _} -> outcome
    after
      0 -> outcome
    end
  end

  # ---- refusal mapping (docs/contracts/recovery-acquisition.org, source-clause table) ----

  defp open_refusal(%{clause: "writer_open_cleanup_failed", cause: cause, release: release, lock_path: lock_path}) do
    cause
    |> open_refusal()
    |> Map.put(:release, release.clause)
    |> Map.put(:cleanup, release_cleanup(release.clause))
    |> Map.put(:residue, residue(lock_path, if(release.clause == "not_owner", do: :unknown, else: true)))
  end

  defp open_refusal(%{clause: clause}) when clause in ["ownership_unavailable", "second_live_writer"],
    do: unavailable("ownership", @none)

  defp open_refusal(%{clause: "reclaim_failed", lock_basename: name}),
    do: "ownership" |> unavailable(@none) |> Map.put(:residue, residue(name, false))

  defp open_refusal(%{clause: "run_locked"}), do: unavailable("lock", @none)

  defp open_refusal(%{clause: clause}) when clause in ["lock_unreadable", "malformed_lock_family"],
    do: unavailable("lock", @none)

  defp open_refusal(%{clause: "lock_unavailable", rollback: "removed_unsynced"}),
    do: unavailable("lock", %{lock: :removed_unsynced, registration: :none, descriptor: :none})

  defp open_refusal(%{clause: "lock_unavailable"}), do: unavailable("lock", @none)

  defp open_refusal(%{clause: "cleanup_required", path: path}),
    do:
      "lock"
      |> unavailable(%{lock: :unproven, registration: :none, descriptor: :none})
      |> Map.put(:residue, residue(path, :unknown))

  defp open_refusal(%{clause: "candidate_conflict", path: path}),
    do: "lock" |> unavailable(@none) |> Map.put(:residue, residue(path, false))

  defp open_refusal(%{clause: "ownership_lost", path: path}),
    do: "lock" |> unavailable(@none) |> Map.put(:residue, residue(path, :unknown))

  defp open_refusal(%{clause: "writer_start_failed"}), do: unavailable("start", @unknown)

  defp open_refusal(%{clause: "repair_failed", stage: stage}),
    do: "repair" |> unavailable(@released) |> Map.put(:reason, stage)

  defp open_refusal(%{clause: clause}) when clause in ["head_temp_cleanup_required", "head_temp_removed_unsynced"],
    do: "repair" |> unavailable(@released) |> Map.put(:reason, clause)

  defp open_refusal(%{clause: "journal_open_failed"}), do: unavailable("descriptor", @released)
  defp open_refusal(rejection), do: rejection |> read_refusal() |> Map.put(:cleanup, @released)

  # the Reader's clauses: unreadable sources are unavailability; everything else is corruption named by clause
  defp read_refusal(%{clause: clause}) when clause in ["journal_missing", "journal_unreadable"],
    do: %{clause: "journal_unavailable", stage: "read", source: "journal"}

  defp read_refusal(%{clause: "receipt_unreadable"}),
    do: %{clause: "journal_unavailable", stage: "read", source: "receipt"}

  defp read_refusal(%{clause: clause} = rejection) do
    base = %{clause: "journal_corrupt", stage: "read", reason: clause}

    case rejection do
      %{at_seq: at_seq} when is_integer(at_seq) -> Map.put(base, :at_seq, at_seq)
      _ -> base
    end
  end

  defp unavailable(stage, cleanup), do: %{clause: "journal_unavailable", stage: stage, cleanup: cleanup}

  # the STRUCTURED nested release of a rolled-back open: RunLock's own clause names what remains
  defp release_cleanup("release_removed_unsynced"),
    do: %{lock: :removed_unsynced, registration: :retained, descriptor: :none}

  defp release_cleanup("not_owner"), do: %{lock: :unknown, registration: :retained, descriptor: :none}
  defp release_cleanup(_clause), do: %{lock: :unproven, registration: :retained, descriptor: :none}

  # a token-free description of a possibly remaining file, from the basename of a structured source path
  defp residue(path, ours) do
    name = Path.basename(path)

    cond do
      captures = Regex.run(@generation_name, name) -> %{kind: :generation, generation: generation(captures), ours: ours}
      captures = Regex.run(@candidate_name, name) -> %{kind: :candidate, generation: generation(captures), ours: ours}
      true -> %{kind: :unknown, generation: nil, ours: ours}
    end
  end

  defp generation([_, digits]), do: String.to_integer(digits)

  # ---- preconditions and options (before any IO) ----

  defp trapping_caller do
    case Process.info(self(), :trap_exit) do
      {:trap_exit, true} -> :ok
      _ -> {:error, %{clause: "recovery_caller_invalid", field: "trap_exit", stage: "caller", cleanup: @none}}
    end
  end

  defp options(opts, allowed) do
    with :ok <- keyword_shape(opts, "options"),
         :ok <- no_create(opts),
         :ok <- known_keys(opts, allowed) do
      {:ok, opts}
    end
  end

  defp no_create(opts) do
    if Keyword.has_key?(opts, :create), do: {:error, option_refusal("create")}, else: :ok
  end

  defp known_keys(opts, allowed) do
    if Enum.all?(opts, fn {key, _} -> key in allowed end), do: :ok, else: {:error, option_refusal("options")}
  end

  # a proper keyword list with unique keys; judged by structure, never by Keyword traversal of a malformed list
  defp keyword_shape(opts, field) do
    if proper_keyword?(opts, []), do: :ok, else: {:error, option_refusal(field)}
  end

  defp proper_keyword?([], _seen), do: true

  defp proper_keyword?([{key, _value} | rest], seen) when is_atom(key),
    do: key not in seen and proper_keyword?(rest, [key | seen])

  defp proper_keyword?(_other, _seen), do: false

  # the ratified nested set only: the seams pass through unchanged, anything else is refused without echo
  defp lock_option(opts) do
    lock = Keyword.get(opts, :lock, [])

    with :ok <- keyword_shape(lock, "lock"),
         :ok <- lock_keys(lock) do
      case Keyword.get(lock, :supervisor_instance) do
        name when is_binary(name) and byte_size(name) > 0 -> :ok
        _ -> {:error, option_refusal("supervisor_instance")}
      end
    end
  end

  defp lock_keys(lock) do
    if Enum.all?(lock, fn {key, _} -> key in @lock_keys end), do: :ok, else: {:error, option_refusal("lock")}
  end

  defp option_refusal(field), do: %{clause: "recovery_option_invalid", field: field, stage: "options", cleanup: @none}
end
