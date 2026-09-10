defmodule AiOrchestrator.Run.Supervisor do
  @moduledoc """
  One run's subtree, `rest_for_one` with `max_restarts: 0`:

      Journal.Writer  ->  Run.Server  ->  Work.Supervisor

  The order is ownership: the Writer holds the run directory's lock and is the only journal
  authority; the Server steps the Host loop against it; the (empty) Work supervisor comes last.
  With intensity 0 a qualifying child exit exhausts the subtree at once: every remaining child
  (the Writer included) is shut down and the supervisor exits `:shutdown`. Nothing is restarted
  and no side effect is replayed; recovery is a NEW subtree in `:resume` mode reading the new
  Writer's verified journal.

  `config` is internal executor context:

      %{run_dir: Path.t(), mode: :run | :resume | :cancel, spec: map() | nil, plan: map() | nil,
        opts: keyword(), trace: pid() | nil}

  `:run` creates the journal; `:resume`/`:cancel` open an existing one, and their prior events come
  only from the Writer's verified, repaired view. A config carrying raw prior lines is refused
  before any child starts. `opts` is the Host option keyword minus `:event_sink`, which the Server
  binds to the Writer sibling.

  Each successful child start is recorded, in start order, by THIS process (the one that starts
  the children) as `{:run_child_started, supervisor_pid, label, pid}` to `config.trace`
  (nil in production). Original start returns, child ids and types are preserved. The wrapped
  start call is carried as a closure, not as argument terms: a supervisor's child-termination
  report prints the child's start MFA, and the Server's config (spec, plan, options, directory) is
  not report material.
  """

  use Supervisor

  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Id.SystemId
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Server
  alias AiOrchestrator.Run.Work

  @type mode :: :run | :resume | :cancel
  @type config :: %{
          required(:run_dir) => Path.t(),
          required(:mode) => mode(),
          required(:opts) => keyword(),
          optional(:spec) => map() | nil,
          optional(:plan) => map() | nil,
          optional(:trace) => pid() | nil,
          optional(:open) => :create | :existing,
          optional(:admission) => :fresh | :retry_only | :restart_empty,
          optional(:command) => Command.t() | nil,
          optional(:owner_handoff) => {pid(), reference()},
          optional(:writer_birth) => {pid(), reference()},
          optional(:writer_ack) => pos_integer()
        }

  # `owner_handoff` (executor path only) names the reaper the Server hands the worker's identity to before
  # admitting any effect; absent in the standalone path. `writer_birth` / `writer_ack` (executor path only,
  # docs/contracts/core-startup-bound.org) name the reaper the Writer announces its birth to before it acquires
  # anything, and the acknowledgment bound; absent in the standalone path.
  # `open` decides whether the Writer creates the journal (`:create`, the default for `:run`) or opens an
  # existing one; `admission` is the explicit provenance of a second attempt (`:retry_only`: only an
  # already-accepted matching command may be admitted, never a fresh start); `command` is the authorized
  # command whose admission the Server decides under the Writer lock (nil: the plain foundation path).

  @spec start_link(config()) :: {:ok, pid()} | {:error, map()}
  def start_link(%{} = config) do
    with {:ok, config} <- validate(config) do
      case Supervisor.start_link(__MODULE__, opaque(config)) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:shutdown, {:failed_to_start_child, _id, %{} = rejection}}} -> {:error, rejection}
        {:error, _reason} -> {:error, %{clause: "run_supervisor_start_failed"}}
      end
    end
  end

  # The supervisor keeps its init argument in its state for the lifetime of the run, and that state is printed by
  # every OTP diagnostic surface of the root (`:sys.get_status`, crash and termination reports). The config carries
  # adapter options and directories, which are not report material (F-1). The init argument is therefore an
  # OPAQUE zero-arity closure - consistent with the closure-wrapped child starts below - and the config is read
  # from it inside `init/1` only. Same-BEAM introspection of a closure's environment is not claimed to be hidden;
  # what this changes is the printable surfaces, which the effect-owner suites measure.
  defp opaque(config), do: fn -> config end

  @impl true
  def init(thunk) when is_function(thunk, 0), do: init_children(thunk.())

  defp init_children(config) do
    config = resolve_identity(config)
    run_dir = config.run_dir

    writer_opts =
      config.opts
      |> Keyword.take([:fs, :clock, :ownership])
      |> Keyword.put(:create, Map.get(config, :open, default_open(config.mode)) == :create)
      |> Keyword.put(:lock, supervisor_instance: Keyword.fetch!(config.opts, :supervisor_instance))
      |> birth_opts(config)

    children = [
      traced(:writer, Writer.child_spec({run_dir, writer_opts}), config),
      traced(:server, Server.child_spec(config), config),
      traced(:work, Work.Supervisor.child_spec([]), config)
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 0)
  end

  @doc false
  # Runs in the supervisor process (child starts are synchronous MFAs): records the successful
  # start to the trace, then returns the original result untouched.
  @spec start_child(atom(), (-> term()), pid() | nil) :: term()
  def start_child(label, start, trace) when is_function(start, 0) do
    result = start.()

    case result do
      {:ok, pid} when is_pid(pid) -> record(trace, label, pid)
      {:ok, pid, _info} when is_pid(pid) -> record(trace, label, pid)
      _other -> :ok
    end

    result
  end

  @doc false
  # a child born OUTSIDE this process's init (the Server's worker under Work) is recorded by its birth
  # authority with the same closed trace shape, naming the supervisor it lives under
  @spec record_child(map(), pid(), atom(), pid()) :: :ok
  def record_child(config, under, label, pid) when is_pid(under) and is_pid(pid) do
    case Map.get(config, :trace) do
      trace when is_pid(trace) -> send(trace, {:run_child_started, under, label, pid})
      _ -> :ok
    end

    :ok
  end

  defp record(nil, _label, _pid), do: :ok
  defp record(trace, label, pid) when is_pid(trace), do: send(trace, {:run_child_started, self(), label, pid})

  defp traced(label, %{start: {module, function, args}} = spec, config) do
    start = fn -> apply(module, function, args) end
    %{spec | start: {__MODULE__, :start_child, [label, start, Map.get(config, :trace)]}}
  end

  # the Writer's lock needs the supervisor instance before the Host resolves identity; the same id
  # seam and the same key the Host uses, so the Host finds it already resolved
  defp resolve_identity(%{opts: opts} = config) do
    id = Keyword.get(opts, :id, SystemId)
    %{config | opts: Keyword.put_new_lazy(opts, :supervisor_instance, &id.supervisor_instance/0)}
  end

  # the executor's reaper acknowledges the Writer's birth before any acquire; the standalone path has no reaper
  defp birth_opts(opts, %{writer_birth: {reaper, ref}} = config) when is_pid(reaper) and is_reference(ref),
    do: opts |> Keyword.put(:birth, {reaper, ref}) |> Keyword.put(:ack, Map.get(config, :writer_ack, 5_000))

  defp birth_opts(opts, _config), do: opts

  defp default_open(:run), do: :create
  defp default_open(_mode), do: :existing

  defp validate(%{prior_lines: _}), do: {:error, %{clause: "raw_lines_not_accepted"}}

  defp validate(%{run_dir: run_dir, mode: mode, opts: opts} = config)
       when is_binary(run_dir) and mode in [:run, :resume, :cancel] and is_list(opts) do
    with :ok <- validate_admission(config) do
      case {mode, Map.get(config, :spec), Map.get(config, :plan)} do
        {:cancel, _, _} -> {:ok, config}
        {_, %{}, %{}} -> {:ok, config}
        _ -> {:error, %{clause: "run_inputs_missing"}}
      end
    end
  end

  defp validate(_config), do: {:error, %{clause: "run_config_invalid"}}

  defp validate_admission(config) do
    open = Map.get(config, :open, default_open(config.mode))
    admission = Map.get(config, :admission, :fresh)
    command = Map.get(config, :command)

    with :ok <- valid_open(open),
         :ok <- valid_admission(admission, open, command, config.mode) do
      valid_command(command)
    end
  end

  defp valid_open(open) when open in [:create, :existing], do: :ok
  defp valid_open(_open), do: {:error, %{clause: "run_config_invalid"}}

  # :retry_only and :restart_empty are explicit provenances of a command over an EXISTING journal
  defp valid_admission(:fresh, _open, _command, _mode), do: :ok
  defp valid_admission(:retry_only, :existing, %Command{}, _mode), do: :ok
  defp valid_admission(:restart_empty, :existing, %Command{}, :run), do: :ok
  defp valid_admission(_admission, _open, _command, _mode), do: {:error, %{clause: "run_config_invalid"}}

  defp valid_command(nil), do: :ok
  defp valid_command(%Command{}), do: :ok
  defp valid_command(_command), do: {:error, %{clause: "run_config_invalid"}}
end
