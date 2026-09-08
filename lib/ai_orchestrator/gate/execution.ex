defmodule AiOrchestrator.Gate.Execution do
  @moduledoc """
  Prepare / record / release execution of a gate under the native guardian, with a durable
  execution claim, a checked journal ack, one owner-owned time fence, and read-only cold
  reconcile. Contract: `docs/contracts/gate-execution-claim.org`.

  Ordering, which nothing here may shortcut: validate and time-check the request; spawn the
  guardian (a Port, no shell string) and read the worker's kernel identity; publish the claim
  RunLock-style through the `Journal.Fs` seam; only a `%Ack{}` built by `ack/2` from the
  Writer's own persisted `gate_started` v2 event releases the retained handle, once, and only
  before the original deadline; `expire/1` is the owner harness's timer path; `await/2` turns
  the guardian's records into evidence without inventing an exit status; `reconcile/4` reads
  claims and kernel facts and never signals anything.

  Runtime objects (the Port, the identity, the claim) live in the handle the owner holds and
  in this process's dictionary keyed by the Port (release-once, termination memo); nothing of
  them is journal data. Barriers (`opts[:barrier]`, test-only, default no-op) are called as
  `barrier.(name, info)` at `:before_spawn`, `:after_ready`, `:after_claim`, `:after_ack`,
  `:after_go` and `:after_exit`.
  """

  alias AiOrchestrator.Clock.SystemClock
  alias AiOrchestrator.Gate.Runner
  alias AiOrchestrator.Journal.Fs

  defmodule Ack do
    @moduledoc """
    The checked proof that the journal writer persisted THIS start's `gate_started` v2 event.
    Built only by `Execution.ack/2` from the writer's own append result; carries the binding
    the release compares against the prepared handle. An internal consistency contract, not an
    unforgeable capability.
    """
    @enforce_keys [:seq, :binding]
    defstruct [:seq, :binding]
    @type t :: %__MODULE__{seq: pos_integer(), binding: String.t()}
  end

  @schema "ai-orchestrator/gate-claim"
  @schema_version 1
  @id_grammar ~r/\A[A-Za-z0-9_-]{1,64}\z/
  @claim_name ~r/\A([A-Za-z0-9_-]{1,64})\.claim\.([12])\z/
  @candidate_name ~r/\A([A-Za-z0-9_-]{1,64})\.claim\.([12])\.[A-Za-z0-9_-]{1,64}\.tmp\z/
  @claim_keys ~w(attempt claim_hash claimed_at command_argv cwd deadline_unix gate_run_id identity run_id schema schema_version stderr_path stdout_path supervisor_instance token)
  @identity_keys ~w(guardian_pid pgid start worker_pid)
  @claim_max_bytes 8192
  @backstop_grace_ms 2_000
  @backstop_max_ms 86_400_000
  @default_ready_ms 5_000
  @default_settle_ms 2_000
  @default_rounds 2
  @wait_margin_ms 2_000
  @receive_cap_ms 4_000_000_000
  @errno_classes ~w(eio enospc enoent eacces eexist erofs)
  @started_keys ~w(attempt command_argv deadline_unix execution gate_run_id stderr_path stdout_path)

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type request :: %{
          run_id: String.t(),
          gate_run_id: String.t(),
          attempt: 1..2,
          command_argv: [String.t(), ...],
          repo_root: Path.t(),
          run_dir: Path.t(),
          deadline_unix: integer(),
          supervisor_instance: String.t()
        }
  @opaque prepared :: map()
  @opaque running :: map()
  @type facts :: map()
  @type verdict ::
          :no_claim | {:orphan_claim, map()} | {:dead, facts()} | {:unknown, facts()} | {:error, rejection()}

  # ---- request, document, hash ----

  @doc "The canonical version-1 claim document for a request, as ORDERED bytes."
  @spec claim_document(request()) :: {:ok, binary()} | {:error, rejection()}
  def claim_document(request) do
    with {:ok, req} <- validate_request(request), do: {:ok, document(req)}
  end

  @doc "sha256 over the canonical document bytes, as `sha256:<hex>`."
  @spec claim_hash(binary()) :: String.t()
  def claim_hash(bytes) when is_binary(bytes), do: sha256(bytes)

  defp document(req) do
    ~s({"attempt":#{req.attempt},"command_argv":#{Jason.encode!(req.command_argv)},"cwd":#{Jason.encode!(req.repo_root)},) <>
      ~s("deadline_unix":#{req.deadline_unix},"gate_run_id":#{Jason.encode!(req.gate_run_id)},"run_id":#{Jason.encode!(req.run_id)},) <>
      ~s("schema":#{Jason.encode!(@schema)},"schema_version":#{@schema_version}})
  end

  @request_keys [
    :run_id,
    :gate_run_id,
    :attempt,
    :command_argv,
    :repo_root,
    :run_dir,
    :deadline_unix,
    :supervisor_instance
  ]

  defp validate_request(%{command_argv: [exe | rest] = argv} = req) when is_binary(exe) and is_list(rest) do
    checks = [
      Enum.all?(argv, &os_argument?/1),
      safe_id?(req[:gate_run_id]),
      req[:attempt] in [1, 2],
      nonempty?(req[:run_id]) and os_argument?(req[:run_id]),
      absolute?(req[:repo_root]) and os_argument?(req[:repo_root]),
      absolute?(req[:run_dir]) and os_argument?(req[:run_dir]),
      is_integer(req[:deadline_unix]),
      nonempty?(req[:supervisor_instance]) and os_argument?(req[:supervisor_instance])
    ]

    cond do
      not Enum.all?(checks) ->
        {:error, %{clause: "invalid_request"}}

      claim_bound(Map.take(req, @request_keys)) > @claim_max_bytes ->
        {:error, %{clause: "invalid_request", field: "size"}}

      true ->
        {:ok, Map.take(req, @request_keys)}
    end
  end

  defp validate_request(_request), do: {:error, %{clause: "invalid_request"}}

  # the largest claim this request could publish (widest identity, token and timestamp) must fit
  # the reader's bound, so no accepted request ever publishes an unrecoverable claim
  defp claim_bound(req) do
    widest = %{
      guardian: 2_147_483_647,
      worker: 2_147_483_647,
      pgid: 2_147_483_647,
      start: "ticks:" <> String.duplicate("9", 20)
    }

    byte_size(Jason.encode!(claim_record(req, widest, 253_402_300_799)) <> "\n")
  end

  defp safe_id?(id), do: is_binary(id) and Regex.match?(@id_grammar, id)
  # an OS argument or path: valid UTF-8 with no NUL, bounded; nothing else is ever encoded or execed
  defp os_argument?(text),
    do: is_binary(text) and byte_size(text) <= 8192 and String.valid?(text) and not String.contains?(text, <<0>>)

  defp nonempty?(value), do: is_binary(value) and value != ""
  defp absolute?(path), do: is_binary(path) and path != "" and Path.type(path) == :absolute

  # ---- prepare ----

  @spec prepare(Fs.t(), request(), keyword()) :: {:ok, prepared()} | {:error, rejection()}
  def prepare(fs, request, opts \\ []) do
    with {:ok, req} <- validate_request(request),
         {:ok, helper} <- fetch_helper(opts),
         {:ok, now} <- now_unix(opts),
         {:ok, backstop_ms} <- backstop(req, now),
         :ok <- barrier(opts, :before_spawn, nil),
         :ok <- gates_dir(fs, req),
         {:ok, port, identity} <- spawn_guardian(helper, req, backstop_ms, opts),
         :ok <- barrier(opts, :after_ready, identity) do
      handle = %{fs: fs, port: port, identity: identity, request: req, opts: opts}
      claim = claim_record(req, identity, now)

      case publish_claim(fs, req, claim) do
        {:ok, claim_hash} ->
          handle =
            Map.merge(handle, %{
              claim: claim,
              started_data: started(req, identity, claim_hash),
              binding: binding(req, identity, claim_hash)
            })

          :ok = barrier(opts, :after_claim, handle.started_data)
          {:ok, handle}

        {:error, rejection} ->
          {:error, reject_after_ready(handle, rejection)}
      end
    end
  end

  @spec started_data(prepared()) :: map()
  def started_data(%{started_data: data}), do: data

  @doc "The operational identity the owner holds (never journaled as such)."
  @spec identity(prepared() | running()) :: %{
          guardian: pos_integer(),
          worker: pos_integer(),
          pgid: pos_integer(),
          start: String.t()
        }
  def identity(%{identity: identity}), do: identity

  defp backstop(req, now) do
    remaining_ms = (req.deadline_unix - now) * 1000

    cond do
      remaining_ms <= 0 ->
        {:error, %{clause: "deadline_expired"}}

      remaining_ms + @backstop_grace_ms > @backstop_max_ms ->
        {:error, %{clause: "deadline_unsupported", max_ms: @backstop_max_ms}}

      true ->
        {:ok, max(remaining_ms + @backstop_grace_ms, 100)}
    end
  end

  # the gates namespace must be a REAL directory: an existing symlink or file at that path is
  # refused before any chmod, spawn or publication (lstat first, never follow)
  defp gates_dir(fs, req) do
    dir = Path.join(req.run_dir, "gates")

    with :ok <- real_directory_or_absent(fs, dir),
         :ok <- mkdir_if_absent(fs, dir),
         {:ok, %{type: :directory}} <- Fs.lstat(fs, dir),
         :ok <- Fs.chmod(fs, dir, 0o700) do
      :ok
    else
      {:error, %{clause: _} = rejection} ->
        {:error, rejection}

      {:ok, %{type: _}} ->
        {:error, gates_boundary()}

      {:error, reason} ->
        {:error, %{clause: "claim_unpublished", stage: "mkdir", class: errno_class(reason), cleanup: "none"}}
    end
  end

  defp gates_boundary,
    do: %{clause: "claim_unpublished", stage: "gates_dir", class: "other", cleanup: "none", field: "type"}

  defp real_directory_or_absent(fs, dir) do
    case Fs.lstat(fs, dir) do
      {:ok, %{type: :directory}} ->
        :ok

      {:ok, %{type: _}} ->
        {:error, gates_boundary()}

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, %{clause: "claim_unpublished", stage: "gates_dir", class: errno_class(reason), cleanup: "none"}}
    end
  end

  defp mkdir_if_absent(fs, dir) do
    case Fs.lstat(fs, dir) do
      {:ok, _} ->
        :ok

      {:error, :enoent} ->
        Fs.mkdir(fs, dir)

      {:error, reason} ->
        {:error, %{clause: "claim_unpublished", stage: "gates_dir", class: errno_class(reason), cleanup: "none"}}
    end
  end

  # After READY every rejection settles the prepared worker, removes the never-released output
  # objects, and composes both truths with the filesystem outcome; nothing is ever lost.
  defp reject_after_ready(%{port: port, identity: identity, opts: opts, fs: fs, request: req}, rejection) do
    settle = terminate(port, opts)
    outputs = remove_outputs(fs, req)

    rejection
    |> Map.merge(%{settle: settle_summary(settle), outputs: outputs, worker_pid: identity.worker})
    |> Map.delete(:worker_pid)
  end

  defp settle_summary({:ok, fields}), do: fields
  defp settle_summary({:error, rejection}), do: rejection

  defp remove_outputs(fs, req) do
    results = for ext <- ["out", "err"], do: Fs.rm(fs, Path.join(req.run_dir, output_path(req, ext)))
    if Enum.all?(results, &(&1 in [:ok, {:error, :enoent}])), do: "removed", else: "left"
  end

  # ---- ack ----

  @doc "Checked constructor: the writer's persisted `gate_started` v2 event for this prepared handle."
  @spec ack(prepared(), term()) :: {:ok, Ack.t()} | {:error, rejection()}
  def ack(%{started_data: data, request: req} = _prepared, persisted) when is_map(persisted) do
    checks = [
      {"prev_line_sha256", fn p -> is_binary(p["prev_line_sha256"]) end},
      {"schema_version", fn p -> p["schema_version"] == 2 end},
      {"seq", fn p -> is_integer(p["seq"]) and p["seq"] > 0 end},
      {"type", fn p -> p["type"] == "gate_started" end},
      {"event_version", fn p -> p["event_version"] == 2 end},
      {"run_id", fn p -> p["run_id"] == req.run_id end},
      {"data", fn p -> is_map(p["data"]) end},
      {"extra", fn p -> p["data"] |> Map.keys() |> Enum.sort() == @started_keys end}
      | Enum.map(@started_keys, fn key -> {key, fn p -> p["data"][key] == data[key] end} end)
    ]

    case Enum.find(checks, fn {_field, check} -> not check.(persisted) end) do
      nil -> {:ok, %Ack{seq: persisted["seq"], binding: binding_of(persisted)}}
      {field, _} -> {:error, %{clause: "ack_mismatch", field: field}}
    end
  end

  def ack(_prepared, _other), do: {:error, %{clause: "ack_mismatch", field: "persisted"}}

  defp binding(req, identity, claim_hash),
    do: binding_of(%{"run_id" => req.run_id, "data" => started(req, identity, claim_hash)})

  defp binding_of(%{"run_id" => run_id, "data" => data}) do
    canonical = Jason.encode!(%{"run_id" => run_id, "data" => Map.take(data, @started_keys)})
    sha256(canonical)
  end

  # ---- release / abandon / expire ----

  @doc "The go-token, only against a matching Ack, only once, only before the deadline."
  @spec release(prepared(), Ack.t(), keyword()) :: {:ok, running()} | {:error, rejection()}
  def release(%{port: port, binding: binding} = prepared, %Ack{binding: ack_binding}, opts \\ []) do
    opts = Keyword.merge(prepared.opts, opts)

    cond do
      ack_binding != binding -> {:error, %{clause: "ack_mismatch", field: "binding"}}
      Process.get({__MODULE__, port, :released}) -> {:error, %{clause: "already_released"}}
      true -> release_checked(prepared, opts)
    end
  end

  defp release_checked(%{port: port, request: req, identity: identity} = prepared, opts) do
    with {:ok, now} <- now_unix(opts),
         :ok <- not_expired(now, req),
         :ok <- barrier(opts, :after_ack, prepared.started_data),
         true <- safe_command(port, "GO\n"),
         :ok <- barrier(opts, :after_go, prepared.started_data),
         {"RELEASED", _} <- next_record(port, wait_ms(opts)) do
      released_at = monotonic_ms(opts)
      Process.put({__MODULE__, port, :released}, true)
      Process.put({__MODULE__, port, :released_at_ms}, released_at)
      Process.put({__MODULE__, port, :opts}, opts)
      {:ok, prepared |> Map.delete(:started_data) |> Map.put(:released_at_ms, released_at)}
    else
      {:error, %{clause: clause} = rejection} when clause in ["clock_unavailable", "deadline_expired"] ->
        {:error, Map.put(rejection, :settle, settle_summary(terminate(port, opts)))}

      false ->
        {:error, %{clause: "release_failed", settle: %{clause: "guardian_gone"}, worker_pid: identity.worker}}

      other ->
        {:error,
         %{clause: "release_failed", settle: settle_summary(terminate(port, opts)), record: describe_record(other)}}
    end
  end

  defp not_expired(now, req), do: if(now >= req.deadline_unix, do: {:error, %{clause: "deadline_expired"}}, else: :ok)

  @spec abandon(prepared()) :: :ok | {:error, rejection()}
  def abandon(%{port: port, opts: opts}) do
    case terminate(port, opts) do
      {:ok, %{settled: true}} -> :ok
      {:ok, fields} -> {:error, Map.put(fields, :clause, "abandon_unsettled")}
      {:error, rejection} -> {:error, rejection}
    end
  end

  @doc "Owner-harness deadline control, independent of any consumer await: TERM through the original channel."
  @spec expire(running()) :: {:timeout, map()} | {:error, rejection()}
  def expire(%{port: port, opts: opts}) do
    case Process.get({__MODULE__, port, :termination}) do
      nil -> terminate_and_memo(port, opts)
      termination -> {:timeout, termination}
    end
  end

  defp terminate_and_memo(port, opts) do
    case terminate(port, opts) do
      {:ok, fields} ->
        termination = fields |> termination() |> put_duration(duration_since_release(port, opts))
        Process.put({__MODULE__, port, :termination}, termination)
        {:timeout, termination}

      {:error, rejection} ->
        {:error, rejection}
    end
  end

  # duration is measured from the release the owner recorded; a handle that was never released
  # (a timeout before GO) has no duration to report and carries none
  defp duration_since_release(port, opts) do
    case Process.get({__MODULE__, port, :released_at_ms}) do
      nil -> nil
      released_at -> max(monotonic_ms(opts) - released_at, 0)
    end
  end

  defp put_duration(termination, nil), do: termination
  defp put_duration(termination, ms), do: Map.put(termination, :duration_ms, ms)

  defp termination(fields) do
    base = %{kind: "timeout", settled: fields.settled, leftovers: fields.leftovers, proof: fields.proof}
    if fields[:reason] == "deadline", do: Map.put(base, :backstop, true), else: base
  end

  # ---- retention (docs/contracts/gate-record-retention-proposal.org) ----

  @typedoc "Closed staging dispositions; `:stage_failed` is the OWNER's verdict for an escaped executor, never ours."
  @type disposition :: :staged | :foreign | :finalized | :overflow | :not_owner | :unsupported
  @staged_capacity 4
  @overflow_sentinel {:malformed, "staged_overflow"}

  @doc """
  Owner-only, pure staging of ONE Port message the owner already dequeued (an idle Worker's loop, or a loop return
  between operations), so the next reader answers it in arrival order instead of losing it to a catch-all.
  Binding is the handle's own Port; ownership is judged exactly as `begin_await/2` judges it (connected to the
  caller, or a closed Port with retained evidence). A finalized handle (termination memo, or a finalized
  descriptor) stores nothing. The payload is classified by the SAME pure decoders `next_record/2` applies and
  appended to a bounded owner-local queue (capacity #{@staged_capacity} live slots) whose overflow is ONE tail
  sentinel; nothing is interpreted, no clock is read, nothing is received, no Port is closed. Every check precedes
  the single storage write.
  """
  @spec stage(map(), term()) :: disposition()
  def stage(%{port: port}, {port, payload}) when is_port(port) do
    cond do
      not owner?(port) -> :not_owner
      finalized?(port) -> :finalized
      true -> stage_classified(port, classify(payload))
    end
  end

  def stage(%{port: port}, {other, _payload}) when is_port(port) and is_port(other), do: :foreign
  def stage(%{port: port}, _message) when is_port(port), do: :unsupported

  defp classify({:data, {:eol, line}}) when is_binary(line), do: {:record, parse_record(line)}
  defp classify({:data, {:noeol, _}}), do: {:record, {:malformed, "overlong"}}
  defp classify({:exit_status, status}) when is_integer(status), do: {:record, {:exited, status}}
  defp classify(_payload), do: :unsupported

  defp stage_classified(_port, :unsupported), do: :unsupported

  defp stage_classified(port, {:record, record}) do
    slots = staged(port)

    cond do
      List.last(slots) == @overflow_sentinel ->
        :overflow

      length(slots) >= @staged_capacity ->
        put_staged(port, slots ++ [@overflow_sentinel])
        :overflow

      true ->
        put_staged(port, slots ++ [record])
        :staged
    end
  end

  defp owner?(port) do
    case Port.info(port, :connected) do
      {:connected, pid} -> pid == self()
      nil -> retained?(port)
    end
  end

  defp finalized?(port) do
    Process.get({__MODULE__, port, :termination}) != nil or Process.get({__MODULE__, port, :descriptor}) == :finalized
  end

  defp staged(port), do: Process.get({__MODULE__, port, :staged}, [])

  defp put_staged(port, slots), do: Process.put({__MODULE__, port, :staged}, slots)

  # ---- await ----

  @doc """
  Owner-resident, resumable await (docs/contracts/gate-ownership.org, revision 4): the EXACT prefix of `await/2`
  without its blocking wait. Reads the Port-keyed termination memo first; then consumes an already-queued terminal
  record with a zero-timeout scan (queued proof precedes any clock decision); a closed Port with neither answers
  the existing `guardian_gone` clause; otherwise `{:pending, waiting}` with NO clock read and NO further receive.
  Only the Port owner may call it (a Port connected to another process raises `ArgumentError`). A duplicate begin
  while pending returns the SAME active descriptor; a begin that consumes a queued terminal finalizes it.
  """
  @spec begin_await(running(), keyword()) :: {:done, answer()} | {:pending, waiting()}
  def begin_await(%{port: port} = running, opts \\ []) do
    owner!(port)
    opts = Keyword.merge(running.opts, opts)

    case Process.get({__MODULE__, port, :termination}) do
      nil -> begin_scan(running, port, opts)
      termination -> {:done, {:timeout, termination}}
    end
  end

  @doc """
  Hands ONE message the owner already dequeued (`{port, {:data, {:eol | :noeol, _}}}` or
  `{port, {:exit_status, _}}`) to the existing private decoder. Validates the exact Port and the retained active
  descriptor BEFORE any memo lookup (a foreign Port, a non-owner, or a finalized descriptor raises `ArgumentError`);
  an active descriptor whose handle was expired externally answers the memo on its first valid resume and is then
  finalized. Every resume answers `{:done, _}`: a record that is not EXIT/DEAD is the closed `await_failed`, exactly
  as `await/2` treats it. Terminal finalization is the legacy one (`exit_outcome/3`, `memo_dead/2`).
  """
  @spec resume_await(waiting(), tuple()) :: {:done, answer()}
  def resume_await(%{port: port, running: running, opts: opts} = waiting, {port, payload}) do
    active!(port, waiting)

    case Process.get({__MODULE__, port, :termination}) do
      nil -> resume_decode(running, port, opts, payload)
      termination -> finalized(port, {:timeout, termination})
    end
  end

  def resume_await(%{port: _}, {other, _}) when is_port(other),
    do: raise(ArgumentError, "resume_await: message for a different Port")

  def resume_await(_waiting, _message), do: raise(ArgumentError, "resume_await: not a Port message")

  @doc """
  Owner-only settlement of the ACTIVE descriptor on the deadline wake path (docs/contracts/gate-async-await-proposal.org,
  AW-M4, D-1). Validates the exact retained active descriptor, then the owner evidence, BEFORE any memo lookup or
  mutation (a finalized descriptor, an unknown activation, a non-owner: `ArgumentError`, nothing mutated). Answers,
  in this order: the termination memo; the FIRST record (`next_record/2` with a zero timeout: the staged head, then
  the mailbox) decoded exactly as `resume_await/2` decodes it, never searching past it; a closed Port with an empty
  scan as the existing `guardian_gone` clause (no memo); otherwise the owner-channel expiry of `expire/1` (TERM
  through the original channel, memo written, never `:backstop`; a refusal is the closed error and invents no memo).
  Every ANSWER finalizes the descriptor; a raising callback (barrier, output evidence) propagates before finalization,
  so the descriptor and the native evidence stay for the boundary's closed settlement. Never `{:pending, _}`.
  """
  @spec settle_await(waiting()) :: {:done, answer()}
  def settle_await(%{port: port, running: running, opts: opts} = waiting) when is_port(port) do
    active!(port, waiting)
    owner!(port)

    case Process.get({__MODULE__, port, :termination}) do
      nil -> settle_scan(running, port, opts)
      termination -> finalized(port, {:timeout, termination})
    end
  end

  def settle_await(_waiting), do: raise(ArgumentError, "settle_await: not an active descriptor")

  # the zero-timeout scan consumes ONE record (staged first, FIFO): a queued terminal wins over the wake, a failure
  # record answers as it would in await/2, an empty scan expires through the owner channel
  defp settle_scan(running, port, opts) do
    case next_record(port, 0) do
      {"EXIT", fields} -> finalized(port, exit_outcome(running, fields, opts))
      {"DEAD", fields} -> finalized(port, memo_dead(port, fields))
      :timeout -> finalized(port, settle_expire(port, opts))
      other -> finalized(port, {:error, %{clause: "await_failed", record: describe_record(other)}})
    end
  end

  defp settle_expire(port, opts) do
    if Port.info(port) == nil, do: {:error, %{clause: "guardian_gone"}}, else: terminate_and_memo(port, opts)
  end

  @typedoc "The existing `await/2` answers."
  @type answer :: {:exit, map()} | {:timeout, map()} | {:error, rejection()}
  @typedoc "Opaque active descriptor; valid only in the owner process until its terminal answer."
  @type waiting :: %{port: port(), running: running(), opts: keyword(), ref: reference()}

  defp begin_scan(running, port, opts) do
    case next_record(port, 0) do
      {"EXIT", fields} ->
        finalized(port, exit_outcome(running, fields, opts))

      {"DEAD", fields} ->
        finalized(port, memo_dead(port, fields))

      :timeout ->
        if Port.info(port) == nil,
          do: finalized(port, {:error, %{clause: "guardian_gone"}}),
          else: descriptor(port, running, opts)

      other ->
        finalized(port, {:error, %{clause: "await_failed", record: describe_record(other)}})
    end
  end

  defp resume_decode(running, port, opts, {:data, {:eol, line}}) do
    case parse_record(line) do
      {"EXIT", fields} -> finalized(port, exit_outcome(running, fields, opts))
      {"DEAD", fields} -> finalized(port, memo_dead(port, fields))
      other -> finalized(port, {:error, %{clause: "await_failed", record: describe_record(other)}})
    end
  end

  defp resume_decode(_running, port, _opts, {:data, {:noeol, _}}),
    do: finalized(port, {:error, %{clause: "await_failed", record: describe_record({:malformed, "overlong"})}})

  defp resume_decode(_running, port, _opts, {:exit_status, status}),
    do: finalized(port, {:error, %{clause: "await_failed", record: describe_record({:exited, status})}})

  defp resume_decode(_running, port, _opts, _payload),
    do: finalized(port, {:error, %{clause: "await_failed", record: describe_record(:unknown)}})

  # the active descriptor is owner-local bookkeeping keyed by the Port: reused by a duplicate begin, replaced by
  # :finalized on any terminal answer (from resume, or from a begin that consumed a queued terminal)
  defp descriptor(port, running, opts) do
    case Process.get({__MODULE__, port, :descriptor}) do
      %{port: ^port} = active ->
        {:pending, active}

      _ ->
        # a fresh descriptor never equals a consumed one: the token makes every activation distinct
        waiting = %{port: port, running: running, opts: opts, ref: make_ref()}
        Process.put({__MODULE__, port, :descriptor}, waiting)
        {:pending, waiting}
    end
  end

  defp finalized(port, answer) do
    Process.put({__MODULE__, port, :descriptor}, :finalized)
    {:done, answer}
  end

  # the caller must be the Port's owner while the Port is alive; a closed Port is judged ONLY by this process's
  # retained evidence (the release memo written at release, a termination memo, or a descriptor), never by the
  # absence of a live Port
  defp owner!(port) do
    case Port.info(port, :connected) do
      {:connected, pid} when pid != self() -> raise(ArgumentError, "begin_await: caller is not the Port owner")
      {:connected, _self} -> :ok
      nil -> if retained?(port), do: :ok, else: raise(ArgumentError, "begin_await: no retained ownership evidence")
    end
  end

  defp retained?(port) do
    Enum.any?([:released, :termination, :descriptor], &(Process.get({__MODULE__, port, &1}) != nil))
  end

  # the retained descriptor must be THIS owner's active one: a non-owner has no such entry, a finalized one raises
  defp active!(port, waiting) do
    case Process.get({__MODULE__, port, :descriptor}) do
      ^waiting -> :ok
      _ -> raise(ArgumentError, "resume_await: descriptor is not active in this owner")
    end
  end

  @spec await(running(), keyword()) :: {:exit, map()} | {:timeout, map()} | {:error, rejection()}
  def await(%{port: port} = running, opts \\ []) do
    opts = Keyword.merge(running.opts, opts)

    case Process.get({__MODULE__, port, :termination}) do
      nil -> await_records(running, opts)
      termination -> {:timeout, termination}
    end
  end

  defp await_records(%{port: port, request: req} = running, opts) do
    # anything the guardian already said (its backstop, the exit) is the truth before any clock
    case next_record(port, 0) do
      {"EXIT", fields} -> exit_outcome(running, fields, opts)
      {"DEAD", fields} -> memo_dead(port, fields)
      :timeout -> await_until_deadline(running, req, opts)
      other -> {:error, %{clause: "await_failed", record: describe_record(other)}}
    end
  end

  defp await_until_deadline(%{port: port} = running, req, opts) do
    with {:ok, now} <- now_unix(opts) do
      remaining_ms = (req.deadline_unix - now) * 1000

      if remaining_ms <= 0, do: expire_or_backstop(port, opts), else: await_record(running, remaining_ms, opts)
    end
  end

  defp await_record(%{port: port} = running, remaining_ms, opts) do
    case next_record(port, min(remaining_ms, @receive_cap_ms)) do
      {"EXIT", fields} -> exit_outcome(running, fields, opts)
      {"DEAD", fields} -> memo_dead(port, fields)
      :timeout -> expire_or_backstop(port, opts)
      other -> {:error, %{clause: "await_failed", record: describe_record(other)}}
    end
  end

  defp expire_or_backstop(port, opts), do: terminate_and_memo(port, opts)

  defp memo_dead(port, fields) do
    termination =
      fields
      |> proof()
      |> Map.put(:reason, fields["reason"])
      |> termination()
      |> put_duration(duration_since_release(port, Process.get({__MODULE__, port, :opts}, [])))

    Process.put({__MODULE__, port, :termination}, termination)
    drain_exit(port)
    {:timeout, termination}
  end

  defp exit_outcome(%{port: port, request: req, released_at_ms: released_at}, fields, opts) do
    :ok = barrier(opts, :after_exit, nil)
    drain_exit(port)
    ending = ending(fields)

    with {:ok, out_hash, out_head} <- output_evidence(req, "out"),
         {:ok, err_hash, err_head} <- output_evidence(req, "err") do
      outcome =
        fields
        |> proof()
        |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)
        |> Map.merge(%{
          "kind" => fields["kind"] || "unknown",
          "escaped" => fields["escaped"] || "unknown",
          "stdout_hash" => out_hash,
          "stderr_hash" => err_hash,
          "stderr_merged" => false,
          "duration_ms" => max(monotonic_ms(opts) - released_at, 0)
        })
        |> put_ending(ending)
        |> put_failure_summary(ending, out_head, err_head)

      {:exit, outcome}
    end
  end

  @doc """
  The bounded output evidence of an attempt, from its private files: streamed sha256 of both
  files. The ONE owner of gate output hashing: `await/2` uses it for exits, and the Host uses it
  for timeouts, signals and cold recovery. An unreadable file is `output_unreadable` with a
  closed class; nothing is invented.
  """
  @spec evidence(Path.t(), String.t(), 1..2) :: {:ok, map()} | {:error, rejection()}
  def evidence(run_dir, gate_run_id, attempt) do
    if absolute?(run_dir) and os_argument?(run_dir) and safe_id?(gate_run_id) and attempt in [1, 2] do
      req = %{run_dir: run_dir, gate_run_id: gate_run_id, attempt: attempt}

      with {:ok, out_hash, _} <- output_evidence(req, "out"),
           {:ok, err_hash, _} <- output_evidence(req, "err") do
        {:ok, %{"stdout_hash" => out_hash, "stderr_hash" => err_hash}}
      end
    else
      {:error, %{clause: "invalid_request"}}
    end
  end

  @doc "Exit 0 AND settled AND proof gone; anything else is never a pass."
  @spec pass?(map()) :: boolean()
  def pass?(%{"exit_status" => 0, "settled" => true, "proof" => "gone"}), do: true
  def pass?(_outcome), do: false

  defp ending(%{"kind" => "exited", "status" => status}), do: with({n, ""} <- Integer.parse(status), do: {:exited, n})
  defp ending(%{"kind" => "signaled", "signal" => signal}), do: with({n, ""} <- Integer.parse(signal), do: {:signaled, n})
  defp ending(_fields), do: :unknown

  defp put_ending(outcome, {:exited, status}), do: Map.put(outcome, "exit_status", status)
  defp put_ending(outcome, {:signaled, signal}), do: Map.put(outcome, "signal", signal)
  defp put_ending(outcome, _unknown), do: outcome

  defp put_failure_summary(outcome, {:exited, 0}, _out, _err), do: outcome

  defp put_failure_summary(outcome, ending, out_head, err_head) do
    evidence = if out_head == "", do: err_head, else: out_head

    marker =
      case ending do
        {:signaled, n} -> {:signal, n}
        {:exited, n} -> n
        :unknown -> 0
      end

    Map.put(outcome, "failure_summary", Runner.failure_summary(evidence, marker))
  end

  # The private output files are hashed by streaming; only a bounded head is retained for the
  # existing summariser. An unreadable file is a closed class, never an invented hash.
  defp output_evidence(req, ext) do
    path = Path.join(req.run_dir, output_path(req, ext))

    case File.stat(path) do
      {:ok, _} ->
        {hash_state, head} =
          path
          |> File.stream!(65_536)
          |> Enum.reduce({:crypto.hash_init(:sha256), <<>>}, fn chunk, {state, head} ->
            {:crypto.hash_update(state, chunk), if(byte_size(head) < 65_536, do: head <> chunk, else: head)}
          end)

        {:ok, "sha256:" <> Base.encode16(:crypto.hash_final(hash_state), case: :lower),
         binary_part(head, 0, min(byte_size(head), 65_536))}

      {:error, reason} ->
        {:error, %{clause: "output_unreadable", which: ext, class: errno_class(reason)}}
    end
  rescue
    _ -> {:error, %{clause: "output_unreadable", which: ext, class: "other"}}
  end

  # ---- cold reconcile: read-only ----

  @doc "Cold, read-only reconcile of the journaled start `expected` against the claim and kernel facts."
  @spec reconcile(Fs.t(), Path.t(), map(), keyword()) :: verdict()
  def reconcile(fs, run_dir, expected, opts \\ []) do
    with {:ok, id, attempt} <- expected_identity(expected),
         {:ok, names} <- list_gates(fs, Path.join(run_dir, "gates")),
         {:ok, claim_name, residue} <- locate_claim(names, id, attempt),
         {:ok, claim} <- read_claim(fs, Path.join(run_dir, "gates"), claim_name),
         :ok <- agree(claim, expected),
         :ok <- journaled?(expected, claim, residue),
         {:ok, facts} <- probe(claim, opts) do
      verdict(claim, facts)
    end
  end

  defp expected_identity(%{"gate_run_id" => id, "attempt" => attempt}) when attempt in [1, 2] do
    if safe_id?(id), do: {:ok, id, attempt}, else: {:error, %{clause: "claim_unreadable", field: "gate_run_id"}}
  end

  defp expected_identity(_expected), do: {:error, %{clause: "claim_unreadable", field: "expected"}}

  # cold traversal: the gates path must be a real directory (never followed) or absent
  defp list_gates(fs, dir) do
    case Fs.lstat(fs, dir) do
      {:ok, %{type: :directory}} -> list_names(fs, dir)
      {:ok, %{type: _}} -> {:error, %{clause: "claim_unreadable", field: "gates"}}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, %{clause: "claim_unreadable", class: errno_class(reason), stage: "lstat"}}
    end
  end

  defp list_names(fs, dir) do
    case Fs.list_dir(fs, dir) do
      {:ok, names} -> {:ok, names}
      {:error, reason} -> {:error, %{clause: "claim_unreadable", class: errno_class(reason), stage: "list_dir"}}
    end
  end

  # Every family name is classified: a claim, a private candidate (residue), or malformed
  # (fail closed). A higher attempt than expected is unexpected; the expected one may be absent.
  defp locate_claim(names, id, attempt) do
    classified = names |> Enum.filter(&String.starts_with?(&1, id <> ".claim.")) |> Enum.map(&classify_name/1)

    cond do
      Enum.any?(classified, &match?({:malformed, _}, &1)) ->
        {:error, %{clause: "claim_unreadable", field: "name"}}

      Enum.any?(classified, &higher_claim?(&1, attempt)) ->
        {:error, %{clause: "claim_unexpected", expected_attempt: attempt}}

      true ->
        expected_claim(classified, attempt)
    end
  end

  defp classify_name(name) do
    cond do
      m = Regex.run(@claim_name, name) -> {:claim, String.to_integer(Enum.at(m, 2)), name}
      m = Regex.run(@candidate_name, name) -> {:candidate, String.to_integer(Enum.at(m, 2)), name}
      true -> {:malformed, name}
    end
  end

  defp higher_claim?({:claim, n, _}, attempt), do: n > attempt
  defp higher_claim?(_other, _attempt), do: false

  defp expected_claim(classified, attempt) do
    case Enum.find(classified, &match?({:claim, ^attempt, _}, &1)) do
      nil -> :no_claim
      {:claim, _, name} -> {:ok, name, for({:candidate, ^attempt, c} <- classified, do: "gates/" <> c)}
    end
  end

  # lstat before read: regular file, private mode, bounded size; then exact keys, types, and the
  # recomputed document hash must reproduce the stored claim_hash.
  defp read_claim(fs, dir, name) do
    path = Path.join(dir, name)

    with {:ok, stat} <- stat_claim(fs, path),
         :ok <- claim_stat_ok(stat),
         {:ok, bytes} <- read_bytes(fs, path) do
      decode_claim(bytes)
    end
  end

  defp stat_claim(fs, path) do
    case Fs.lstat(fs, path) do
      {:ok, %{type: :regular} = stat} -> {:ok, stat}
      {:ok, %{type: _other}} -> {:error, %{clause: "claim_unreadable", field: "type"}}
      {:error, reason} -> {:error, %{clause: "claim_unreadable", class: errno_class(reason), stage: "lstat"}}
    end
  end

  defp claim_stat_ok(%{mode: mode, size: size}) do
    cond do
      Bitwise.band(mode, 0o777) != 0o600 -> {:error, %{clause: "claim_unreadable", field: "mode"}}
      size > @claim_max_bytes -> {:error, %{clause: "claim_unreadable", field: "size"}}
      true -> :ok
    end
  end

  defp read_bytes(fs, path) do
    case Fs.read(fs, path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, %{clause: "claim_unreadable", class: errno_class(reason), stage: "read"}}
    end
  end

  defp decode_claim(bytes) do
    with {:ok, claim} when is_map(claim) <- Jason.decode(bytes),
         :ok <- exact_keys(claim, @claim_keys, "extra"),
         :ok <- exact_keys(claim["identity"], @identity_keys, "identity"),
         :ok <- claim_types(claim),
         :ok <- claim_hash_agrees(claim) do
      {:ok, claim}
    else
      {:error, %{clause: _} = rejection} -> {:error, rejection}
      _ -> {:error, %{clause: "claim_unreadable", field: "json"}}
    end
  end

  defp exact_keys(map, keys, field) when is_map(map) do
    if map |> Map.keys() |> Enum.sort() == keys, do: :ok, else: {:error, %{clause: "claim_unreadable", field: field}}
  end

  defp exact_keys(_map, _keys, field), do: {:error, %{clause: "claim_unreadable", field: field}}

  defp claim_types(claim) do
    identity = claim["identity"]

    checks = [
      claim["schema"] == @schema,
      claim["schema_version"] == @schema_version,
      safe_id?(claim["gate_run_id"]),
      claim["attempt"] in [1, 2],
      nonempty?(claim["run_id"]),
      is_list(claim["command_argv"]) and Enum.all?(claim["command_argv"], &is_binary/1),
      absolute?(claim["cwd"]),
      is_integer(claim["deadline_unix"]),
      is_binary(claim["claim_hash"]),
      Enum.all?(~w(stdout_path stderr_path supervisor_instance token claimed_at), &nonempty?(claim[&1])),
      Enum.all?(~w(guardian_pid worker_pid pgid), &(is_integer(identity[&1]) and identity[&1] > 0)),
      nonempty?(identity["start"])
    ]

    if Enum.all?(checks), do: :ok, else: {:error, %{clause: "claim_unreadable", field: "types"}}
  end

  defp claim_hash_agrees(claim) do
    recomputed =
      document(%{
        attempt: claim["attempt"],
        command_argv: claim["command_argv"],
        repo_root: claim["cwd"],
        deadline_unix: claim["deadline_unix"],
        gate_run_id: claim["gate_run_id"],
        run_id: claim["run_id"]
      })

    if sha256(recomputed) == claim["claim_hash"],
      do: :ok,
      else: {:error, %{clause: "claim_unreadable", field: "claim_hash"}}
  end

  # every fact the journaled start carries must agree with the claim; the first disagreement names the field
  defp agree(claim, expected) do
    pairs =
      [
        {"run_id", expected["run_id"], claim["run_id"]},
        {"gate_run_id", expected["gate_run_id"], claim["gate_run_id"]},
        {"attempt", expected["attempt"], claim["attempt"]},
        {"deadline_unix", expected["deadline_unix"], claim["deadline_unix"]},
        {"command_argv", expected["command_argv"], claim["command_argv"]},
        {"stdout_path", expected["stdout_path"], claim["stdout_path"]},
        {"stderr_path", expected["stderr_path"], claim["stderr_path"]},
        {"execution.pid", get_in(expected, ["execution", "pid"]), claim["identity"]["worker_pid"]},
        {"execution.pgid", get_in(expected, ["execution", "pgid"]), claim["identity"]["pgid"]},
        {"execution.start", get_in(expected, ["execution", "start"]), claim["identity"]["start"]},
        {"execution.claim_hash", get_in(expected, ["execution", "claim_hash"]), claim["claim_hash"]}
      ]

    # every binding is mandatory: a missing or nil expectation is a mismatch, never a wildcard,
    # and the fact probe is never reached without the complete journaled start
    case Enum.find(pairs, fn {_field, want, have} -> want == nil or want != have end) do
      nil -> :ok
      {field, nil, _} -> {:error, %{clause: "claim_mismatch", field: field, missing: true}}
      {field, _, _} -> {:error, %{clause: "claim_mismatch", field: field}}
    end
  end

  # a claim the caller states was never journaled is a pre-ack orphan: its own verdict, never no_claim
  defp journaled?(%{"journaled" => false}, claim, residue), do: {:orphan_claim, %{"residue" => residue, "claim" => claim}}

  defp journaled?(_expected, _claim, _residue), do: :ok

  defp probe(claim, opts) do
    with {:ok, helper} <- fetch_helper(opts) do
      args = ["--probe", Integer.to_string(claim["identity"]["worker_pid"]), Integer.to_string(claim["identity"]["pgid"])]

      case run_helper(helper, args, opts) do
        {out, 0} -> parse_probe(out)
        {_out, status} -> {:error, %{clause: "probe_invalid", exit: status}}
      end
    end
  end

  defp run_helper(helper, args, opts) do
    System.cmd(helper, args, stderr_to_stdout: true, env: Keyword.get(opts, :env, []))
  rescue
    _ -> {"", 255}
  end

  # exactly one bounded record with all four closed facts; anything else is invalid, never a verdict
  defp parse_probe(out) do
    with true <- byte_size(out) <= 256,
         [line] <- String.split(out, "\n", trim: true),
         {"PROBE", %{"leader" => leader, "start" => start, "group" => group, "members" => members}} <- parse_record(line),
         {:ok, count} <- members(members) do
      {:ok, %{"leader" => leader, "start" => start, "group" => group, "members" => count}}
    else
      _ -> {:error, %{clause: "probe_invalid"}}
    end
  end

  defp members("unknown"), do: {:ok, "unknown"}

  defp members(text) do
    case Integer.parse(text) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> :error
    end
  end

  # the leader fact is an IDENTITY fact: a live pid with another start is the original leader gone
  # (pid reused by a stranger, reported as such, never signalled); dead needs all three facts
  defp verdict(claim, %{"leader" => "alive", "start" => start} = facts) when start != "-" do
    if start == claim["identity"]["start"],
      do: {:unknown, facts},
      else: verdict(claim, Map.merge(facts, %{"leader" => "gone", "leader_pid" => "reused"}))
  end

  defp verdict(_claim, %{"leader" => "gone", "group" => "gone", "members" => 0} = facts), do: {:dead, facts}
  defp verdict(_claim, facts), do: {:unknown, facts}

  # ---- guardian port ----

  defp spawn_guardian(helper, req, backstop_ms, opts) do
    args =
      [
        "--stdout",
        Path.join(req.run_dir, output_path(req, "out")),
        "--stderr",
        Path.join(req.run_dir, output_path(req, "err"))
      ] ++
        ["--cwd", req.repo_root, "--ready-ms", Integer.to_string(Keyword.get(opts, :ready_ms, @default_ready_ms))] ++
        ["--settle-ms", Integer.to_string(settle_ms(opts)), "--rounds", Integer.to_string(rounds(opts))] ++
        ["--timeout-ms", Integer.to_string(backstop_ms), "--" | req.command_argv]

    env = for {k, v} <- Keyword.get(opts, :env, []), do: {String.to_charlist(k), String.to_charlist(v)}

    port =
      Port.open({:spawn_executable, helper}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:args, args},
        {:env, env},
        {:line, 1024}
      ])

    case next_record(port, Keyword.get(opts, :ready_ms, @default_ready_ms) + @wait_margin_ms) do
      {"READY", fields} -> ready(port, fields)
      {"SETUP_FAILED", fields} -> setup_failed(port, fields)
      other -> protocol_failure(port, other)
    end
  end

  defp ready(port, fields) do
    with {:ok, identity} <- parse_identity(fields),
         :timeout <- next_record(port, 0) do
      {:ok, port, identity}
    else
      _ -> protocol_failure(port, "READY")
    end
  end

  defp parse_identity(%{"guardian" => g, "worker" => w, "pgid" => p, "start" => start})
       when is_binary(start) and start != "" do
    with {gi, ""} when gi > 0 <- Integer.parse(g),
         {wi, ""} when wi > 0 <- Integer.parse(w),
         {pi, ""} when pi > 0 <- Integer.parse(p) do
      {:ok, %{guardian: gi, worker: wi, pgid: pi, start: start}}
    else
      _ -> :error
    end
  end

  defp parse_identity(_fields), do: :error

  defp setup_failed(port, %{"settled" => _} = fields) do
    drain_exit(port)

    {:error,
     %{
       clause: "prepare_failed",
       class: fields["class"] || "unknown",
       cleanup: fields["cleanup"] || "none",
       settle: proof(fields)
     }}
  end

  # usage or pre-worker: no worker ever existed, so there is no settlement to report
  defp setup_failed(port, fields) do
    drain_exit(port)

    {:error,
     %{clause: "prepare_failed", class: fields["class"], cleanup: fields["cleanup"] || "none", settle: %{worker: false}}}
  end

  # Startup rejection has no trusted command channel. Writing TERM can raise an asynchronous
  # linked :epipe after the helper closes stdin. EOF triggers native cleanup, but is not its proof.
  defp protocol_failure(port, _record) do
    safe_close(port)
    {:error, %{clause: "prepare_failed", class: "protocol", cleanup: "none", settle: %{clause: "settle_unproven"}}}
  end

  # TERM, then the DEAD record; the port is closed either way.
  defp terminate(port, opts) do
    result =
      with true <- safe_command(port, "TERM\n"),
           {"DEAD", fields} <- next_record(port, wait_ms(opts)),
           {:ok, proof} <- dead_proof(fields) do
        {:ok, Map.put(proof, :reason, fields["reason"])}
      else
        false -> {:error, %{clause: "guardian_gone"}}
        {:error, :malformed} -> {:error, %{clause: "settle_unproven", record: "DEAD"}}
        other -> {:error, %{clause: "settle_unproven", record: describe_record(other)}}
      end

    drain_exit(port)
    result
  end

  defp dead_proof(%{"settled" => settled, "leftovers" => leftovers, "proof" => proof} = fields)
       when settled in ["0", "1"] and is_binary(leftovers) and proof in ~w(gone alive unknown), do: {:ok, proof(fields)}

  defp dead_proof(_fields), do: {:error, :malformed}

  defp safe_command(port, bytes) do
    Port.command(port, bytes)
  rescue
    ArgumentError -> false
  end

  # a staged record (stage/2) precedes the mailbox: the owner dequeued it earlier, so the mailbox holds only later
  # messages; FIFO keeps the first record (a failure included) ahead of anything after it
  defp next_record(port, timeout_ms) do
    case staged(port) do
      [record | rest] ->
        put_staged(port, rest)
        record

      [] ->
        receive do
          {^port, {:data, {:eol, line}}} -> parse_record(line)
          {^port, {:data, {:noeol, _}}} -> {:malformed, "overlong"}
          {^port, {:exit_status, status}} -> {:exited, status}
        after
          max(timeout_ms, 0) -> :timeout
        end
    end
  end

  # selective, like its receive: the FIRST staged exit_status anywhere in the queue is consumed and every other
  # slot keeps its place; only when none is staged does the mailbox wait (then close) run
  defp drain_exit(port) do
    case Enum.split_while(staged(port), &(not match?({:exited, _}, &1))) do
      {before, [{:exited, _} | after_]} ->
        put_staged(port, before ++ after_)
        :ok

      {_all, []} ->
        receive do
          {^port, {:exit_status, _}} -> :ok
        after
          1000 -> safe_close(port)
        end
    end
  end

  defp safe_close(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  # Every guardian record is decoded against an exact per-head grammar BEFORE any fact is used:
  # one head from the closed set, exactly the allowed keys, no duplicates, closed enums, native
  # decimal and identity domains, internally consistent. Anything else is {:malformed, head}
  # with a closed head class; the bytes never reach a verdict, a proof, or a diagnostic.
  @heads ~w(READY RELEASED EXIT DEAD SETUP_FAILED PROTOCOL_ERROR PROBE)
  @setup_classes ~w(open_stdout open_stderr pipe fork setsid chdir redirect ready_timeout ready identity usage)
  @dead_reasons ~w(command parent_gone guardian_signaled release_failed deadline)
  @kernel_start ~r/\A(?:[0-9]{1,20}\.[0-9]{6}|ticks:[0-9]{1,20})\z/
  @count ~r/\A(?:0|[1-9][0-9]{0,9})\z/

  defp parse_record(line) when byte_size(line) > 512, do: {:malformed, "overlong"}

  defp parse_record(line) do
    case String.split(line, " ", trim: true) do
      [head | pairs] when head in @heads -> decode_pairs(head, pairs)
      _ -> {:malformed, "unknown_head"}
    end
  end

  defp decode_pairs(head, pairs) do
    parsed = Enum.map(pairs, fn pair -> pair |> String.split("=", parts: 2) |> List.to_tuple() end)
    keys = Enum.map(parsed, &elem(&1, 0))

    with true <- Enum.all?(parsed, &match?({_, _}, &1)),
         true <- length(keys) == length(Enum.uniq(keys)),
         fields = Map.new(parsed),
         :ok <- record_grammar(head, fields) do
      {head, fields}
    else
      _ -> {:malformed, head}
    end
  end

  # one dispatch to a boolean predicate per head; the predicate is the whole grammar of that head
  defp record_grammar(head, f), do: if(head_grammar?(head, f), do: :ok, else: :error)

  defp head_grammar?("READY", f), do: ready_grammar?(f)
  defp head_grammar?("RELEASED", f), do: exact(f, [])
  defp head_grammar?("PROTOCOL_ERROR", f), do: exact(f, [])
  defp head_grammar?("EXIT", f), do: settlement_grammar(f)
  defp head_grammar?("DEAD", f), do: dead_grammar?(f)
  defp head_grammar?("SETUP_FAILED", f), do: setup_grammar?(f)
  defp head_grammar?("PROBE", f), do: probe_grammar?(f)

  # the producer's three forms, keyed on the exact key set: usage (class only, before any side
  # effect), pre-worker (class + cleanup, output objects handled but no worker existed),
  # post-worker (class + cleanup + the complete settlement of the prepared child)
  defp setup_grammar?(f) do
    case Enum.sort(Map.keys(f)) do
      ["class"] -> setup_class?(f)
      ["class", "cleanup"] -> setup_class?(f) and setup_cleanup?(f)
      ["class", "cleanup", "leftovers", "proof", "settled"] -> setup_class?(f) and setup_cleanup?(f) and proof_grammar(f)
      _ -> false
    end
  end

  defp ready_grammar?(f) do
    exact(f, ~w(guardian pgid start worker)) and pid_token?(f["guardian"]) and pid_token?(f["worker"]) and
      pid_token?(f["pgid"]) and Regex.match?(@kernel_start, f["start"])
  end

  defp probe_grammar?(f) do
    exact(f, ~w(group leader members start)) and f["leader"] in ~w(gone alive unknown) and
      f["group"] in ~w(gone alive unknown) and
      count_or_unknown?(f["members"]) and probe_start_consistent?(f)
  end

  defp dead_grammar?(f), do: f["reason"] in @dead_reasons and settlement_grammar(Map.delete(f, "reason"))
  defp setup_class?(f), do: f["class"] in @setup_classes
  defp setup_cleanup?(f), do: f["cleanup"] in ~w(removed left none)

  # kind exited needs status 0..255 and no signal; kind signaled needs signal 1..64 and no status;
  # kind unknown needs neither; the settlement trailer is always complete
  @settlement_base ~w(escaped kind leftovers proof settled)

  defp settlement_grammar(%{"kind" => "exited", "status" => status} = f),
    do: exact(f, ["status" | @settlement_base]) and bounded_int?(status, 0, 255) and trailer?(f)

  defp settlement_grammar(%{"kind" => "signaled", "signal" => signal} = f),
    do: exact(f, ["signal" | @settlement_base]) and bounded_int?(signal, 1, 64) and trailer?(f)

  defp settlement_grammar(%{"kind" => "unknown"} = f), do: exact(f, @settlement_base) and trailer?(f)
  defp settlement_grammar(_f), do: false

  defp trailer?(f), do: proof_grammar(f) and f["escaped"] == "unknown"

  # individually valid fields are not a proof: settled means the group is gone with nothing left,
  # and an unsettled group can never claim that; contradictions are refused, never interpreted
  defp proof_grammar(f) do
    f["settled"] in ["0", "1"] and count_or_unknown?(f["leftovers"]) and f["proof"] in ~w(gone alive unknown) and
      consistent_settlement?(f["settled"], f["leftovers"], f["proof"])
  end

  defp consistent_settlement?("1", "0", "gone"), do: true
  defp consistent_settlement?("1", _leftovers, _proof), do: false
  defp consistent_settlement?("0", "0", "gone"), do: false
  defp consistent_settlement?("0", _leftovers, _proof), do: true

  defp probe_start_consistent?(%{"leader" => "alive", "start" => start}), do: Regex.match?(@kernel_start, start)
  defp probe_start_consistent?(%{"start" => "-"}), do: true
  defp probe_start_consistent?(_), do: false
  defp exact(fields, keys), do: fields |> Map.keys() |> Enum.sort() == Enum.sort(keys)
  defp count_or_unknown?("unknown"), do: true
  defp count_or_unknown?(text) when is_binary(text), do: Regex.match?(@count, text)
  defp count_or_unknown?(_), do: false

  defp pid_token?(text) when is_binary(text) and byte_size(text) in 1..10 do
    Regex.match?(@count, text) and String.to_integer(text) in 1..2_147_483_647
  end

  defp pid_token?(_), do: false

  defp bounded_int?(text, lo, hi) when is_binary(text) and byte_size(text) in 1..3 do
    Regex.match?(~r/\A(?:0|[1-9][0-9]{0,2})\z/, text) and String.to_integer(text) in lo..hi
  end

  defp bounded_int?(_, _, _), do: false

  defp proof(fields) do
    %{settled: fields["settled"] == "1", leftovers: fields["leftovers"] || "unknown", proof: fields["proof"] || "unknown"}
  end

  # closed descriptions only: a head from the closed set, a closed malformed class, or a fixed word
  defp describe_record(:timeout), do: "timeout"
  defp describe_record({:exited, status}) when is_integer(status), do: "exited #{status}"
  defp describe_record({:malformed, head}) when head in @heads, do: "malformed " <> head
  defp describe_record({:malformed, _class}), do: "malformed"
  defp describe_record({head, _fields}) when head in @heads, do: head
  defp describe_record(_other), do: "unknown"

  # ---- claim record and publication ----

  # claimed_at derives from the one injected clock (unix seconds), never a second time source
  defp claim_record(req, identity, now) do
    %{
      "schema" => @schema,
      "schema_version" => @schema_version,
      "run_id" => req.run_id,
      "gate_run_id" => req.gate_run_id,
      "attempt" => req.attempt,
      "command_argv" => req.command_argv,
      "cwd" => req.repo_root,
      "deadline_unix" => req.deadline_unix,
      "claim_hash" => sha256(document(req)),
      "identity" => %{
        "guardian_pid" => identity.guardian,
        "worker_pid" => identity.worker,
        "pgid" => identity.pgid,
        "start" => identity.start
      },
      "stdout_path" => output_path(req, "out"),
      "stderr_path" => output_path(req, "err"),
      "supervisor_instance" => req.supervisor_instance,
      "token" => random_token(),
      "claimed_at" => now |> DateTime.from_unix!() |> DateTime.to_iso8601()
    }
  end

  defp started(req, identity, claim_hash) do
    %{
      "gate_run_id" => req.gate_run_id,
      "command_argv" => req.command_argv,
      "stdout_path" => output_path(req, "out"),
      "stderr_path" => output_path(req, "err"),
      "attempt" => req.attempt,
      "deadline_unix" => req.deadline_unix,
      "execution" => %{
        "pid" => identity.worker,
        "pgid" => identity.pgid,
        "start" => identity.start,
        "claim_hash" => claim_hash
      }
    }
  end

  # Exclusive private temp (0600), write, fsync, close, hard link (never replaces), remove temp,
  # directory fsync. Every failure states the original stage/class and the cleanup outcome.
  defp publish_claim(fs, req, claim) do
    dir = Path.join(req.run_dir, "gates")
    final = Path.join(dir, "#{req.gate_run_id}.claim.#{req.attempt}")
    tmp = final <> "." <> claim["token"] <> ".tmp"

    pub = %{
      fs: fs,
      dir: dir,
      final: final,
      tmp: tmp,
      token: claim["token"],
      rel_final: "gates/" <> Path.basename(final),
      rel_tmp: "gates/" <> Path.basename(tmp)
    }

    encoded = Jason.encode!(claim) <> "\n"

    with :ok <- bounded_claim(encoded),
         {:ok, fd} <- opened(Fs.open(fs, tmp, [:exclusive])),
         :ok <- chmod_temp(pub, fd),
         :ok <- written(pub, fd, encoded),
         :ok <- linked(pub),
         :ok <- temp_removed(pub),
         :ok <- synced(pub) do
      {:ok, claim["claim_hash"]}
    end
  end

  # nothing was created yet when the exclusive open fails, so there is no cleanup to report
  defp opened({:ok, fd}), do: {:ok, fd}
  defp opened({:error, reason}), do: {:error, unpublished("open", reason, "none")}

  # a defensive re-check at publication of the bound validated before spawn
  defp bounded_claim(encoded) when byte_size(encoded) <= @claim_max_bytes, do: :ok

  defp bounded_claim(_encoded),
    do: {:error, %{clause: "claim_unpublished", stage: "encode", class: "other", cleanup: "none"}}

  # the opened descriptor is closed on every later error before the temp is removed
  defp chmod_temp(%{fs: fs, tmp: tmp} = pub, fd) do
    case Fs.chmod(fs, tmp, 0o600) do
      :ok ->
        :ok

      {:error, reason} ->
        _ = Fs.close(fs, fd)
        {:error, with_cleanup(unpublished("chmod", reason, "none"), cleanup_temp(pub))}
    end
  end

  defp with_cleanup(rejection, {"cleanup_required", residue}),
    do: Map.merge(rejection, %{cleanup: "cleanup_required", residue: residue})

  defp with_cleanup(rejection, {label, temp}) when is_binary(label),
    do: Map.merge(rejection, %{cleanup: label, temp: temp})

  defp with_cleanup(rejection, cleanup) when is_binary(cleanup), do: Map.put(rejection, :cleanup, cleanup)

  defp unpublished(stage, reason, cleanup, extra \\ %{}) do
    Map.merge(%{clause: "claim_unpublished", stage: stage, class: errno_class(reason), cleanup: cleanup}, extra)
  end

  defp written(%{fs: fs} = pub, fd, bytes) do
    with :ok <- tagged(Fs.write(fs, fd, bytes), "write"),
         :ok <- tagged(Fs.sync(fs, fd), "sync"),
         :ok <- tagged(Fs.close(fs, fd), "close") do
      :ok
    else
      {:error, stage, reason} ->
        if stage != "close", do: _ = Fs.close(fs, fd)
        {:error, with_cleanup(unpublished(stage, reason, "none"), cleanup_temp(pub))}
    end
  end

  defp tagged(:ok, _stage), do: :ok
  defp tagged({:error, reason}, stage), do: {:error, stage, reason}

  defp linked(%{fs: fs, tmp: tmp, final: final} = pub) do
    case Fs.link(fs, tmp, final) do
      :ok ->
        :ok

      {:error, :eexist} ->
        {:error,
         with_cleanup(
           %{clause: "claim_conflict", stage: "link", class: "eexist"},
           cleanup_temp_or(pub, "foreign_final_untouched")
         )}

      {:error, reason} ->
        {:error, with_cleanup(unpublished("link", reason, "none"), cleanup_temp(pub))}
    end
  end

  # the conflict keeps the foreign final untouched AND states our own temp cleanup truth
  defp cleanup_temp_or(pub, label) do
    case cleanup_temp(pub) do
      "removed" -> label
      "absent" -> label
      unsynced when unsynced in ["removed_unsynced", "absent_unsynced"] -> {label, unsynced}
      {"cleanup_required", _residue} = required -> required
    end
  end

  defp temp_removed(%{fs: fs, tmp: tmp} = pub) do
    case Fs.rm(fs, tmp) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, temp_residue(pub, reason)}
    end
  end

  # the temp could not be removed after a successful link: the final is retracted (our token) so
  # nothing half-published remains, and the residue names exactly what is left
  defp temp_residue(pub, reason) do
    case retract_final(pub, [pub.rel_tmp]) do
      {"cleanup_required", residue} ->
        unpublished("rm_temp", reason, "cleanup_required", %{residue: residue})

      {"cleanup_required", residue, final} ->
        unpublished("rm_temp", reason, "cleanup_required", %{residue: residue, final: final})

      "foreign_final_untouched" ->
        unpublished("rm_temp", reason, "cleanup_required", %{residue: [pub.rel_tmp]})

      other ->
        unpublished("rm_temp", reason, other, %{residue: [pub.rel_tmp]})
    end
  end

  defp synced(%{fs: fs, dir: dir} = pub) do
    case Fs.dir_sync(fs, dir) do
      :ok -> :ok
      {:error, reason} -> {:error, dir_sync_failed(pub, reason)}
    end
  end

  # After a failed publication fsync the visible final is retracted (token-verified) and the
  # retraction fsynced; each outcome is reported for what it is.
  defp dir_sync_failed(pub, reason) do
    case retract_final(pub, []) do
      label when is_binary(label) ->
        unpublished("dir_sync", reason, label)

      {"cleanup_required", residue} ->
        unpublished("dir_sync", reason, "cleanup_required", %{residue: residue})

      {"cleanup_required", residue, final} ->
        unpublished("dir_sync", reason, "cleanup_required", %{residue: residue, final: final})
    end
  end

  # retracts OUR final only: the token in the file must be ours; then fsyncs the directory
  defp retract_final(%{fs: fs, final: final, dir: dir, token: token, rel_final: rel_final}, prior_residue) do
    case Fs.read(fs, final) do
      {:ok, bytes} ->
        if ours?(bytes, token),
          do: remove_final(fs, final, dir, rel_final, prior_residue),
          else: "foreign_final_untouched"

      # absent before the token could be read: report the absence and its durability, never a
      # phantom foreign final and never phantom residue
      {:error, :enoent} ->
        absent_final(fs, dir, prior_residue)

      {:error, _} ->
        {"cleanup_required", prior_residue ++ [rel_final]}
    end
  end

  defp ours?(bytes, token), do: match?({:ok, %{"token" => ^token}}, Jason.decode(bytes))

  # a removed final is ALWAYS followed by a directory fsync; a remaining temp does not skip it,
  # it only turns the label into cleanup_required with the retraction's own durability stated
  # ONE remove-and-sync primitive for our objects: a successful removal OR an already-absent
  # object both proceed to the directory fsync, whose outcome is the durability truth
  defp remove_and_sync(fs, dir, path, present_label, absent_label) do
    case Fs.rm(fs, path) do
      :ok -> {:ok, present_label, Fs.dir_sync(fs, dir) == :ok}
      {:error, :enoent} -> {:ok, absent_label, Fs.dir_sync(fs, dir) == :ok}
      {:error, _} -> :error
    end
  end

  defp durability(label, true), do: label
  defp durability(label, false), do: label <> "_unsynced"

  defp remove_final(fs, final, dir, rel_final, prior_residue) do
    case remove_and_sync(fs, dir, final, "removed", "absent") do
      {:ok, label, synced} -> with_prior_residue(durability(label, synced), prior_residue)
      :error -> {"cleanup_required", prior_residue ++ [rel_final]}
    end
  end

  defp absent_final(fs, dir, prior_residue) do
    with_prior_residue(durability("absent", Fs.dir_sync(fs, dir) == :ok), prior_residue)
  end

  # prior temp residue is carried independently of the final's own outcome
  defp with_prior_residue(retraction, []), do: retraction
  defp with_prior_residue(retraction, prior_residue), do: {"cleanup_required", prior_residue, retraction}

  # our temp: removed or already absent, then the directory fsync decides durability; a temp that
  # could not be removed is named by its safe relative path
  defp cleanup_temp(%{fs: fs, tmp: tmp, rel_tmp: rel_tmp, dir: dir}) do
    case remove_and_sync(fs, dir, tmp, "removed", "absent") do
      {:ok, label, synced} -> durability(label, synced)
      :error -> {"cleanup_required", [rel_tmp]}
    end
  end

  # ---- helpers ----

  defp errno_class(reason) when is_atom(reason) do
    name = Atom.to_string(reason)
    if name in @errno_classes, do: name, else: "other"
  end

  defp errno_class(_reason), do: "other"

  defp fetch_helper(opts) do
    case Keyword.get(opts, :helper) do
      path when is_binary(path) and path != "" -> {:ok, path}
      _ -> {:error, %{clause: "helper_missing"}}
    end
  end

  defp now_unix(opts) do
    clock = Keyword.get(opts, :clock, SystemClock)
    {:ok, clock.unix_now()}
  rescue
    _ -> {:error, %{clause: "clock_unavailable"}}
  end

  defp barrier(opts, name, info) do
    case Keyword.get(opts, :barrier) do
      fun when is_function(fun, 2) -> fun.(name, info) && :ok
      _ -> :ok
    end
  end

  defp output_path(req, ext), do: "gates/#{req.gate_run_id}.#{req.attempt}.#{ext}"
  defp settle_ms(opts), do: Keyword.get(opts, :settle_ms, @default_settle_ms)
  defp rounds(opts), do: Keyword.get(opts, :rounds, @default_rounds)
  defp wait_ms(opts), do: settle_ms(opts) * rounds(opts) + @wait_margin_ms

  defp monotonic_ms(opts) do
    clock = Keyword.get(opts, :clock, SystemClock)
    clock.monotonic_ms()
  end

  defp random_token, do: 16 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  defp sha256(bytes), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
