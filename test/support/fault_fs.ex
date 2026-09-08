defmodule AiOrchestrator.Test.FaultFs do
  @moduledoc """
  Fault-injecting filesystem seam for durability tests.

  Delegates every operation to `AiOrchestrator.Journal.Fs.SystemFs`, records
  the operation trace in order, and applies a plan of injected faults keyed by
  `{op, nth_call}`:

    * `{:error, reason}` — return the error without touching the disk
    * `{:return, value}` — return `value` without touching the disk (foreign content, etc.)
    * `{:hook, fun}` — run `fun` (which must return a truthy value) and then perform the operation
      normally, so a competing actor's effect (a path vanishing) can precede it deterministically
    * `{:after, fun}` — (every operation dispatched through `run/4`: not `write`, which has its
      own dispatcher) perform the operation normally and, ONLY if it returned `:ok`, call
      `fun.(trace)` in the calling process (the writer's) with the trace so far, newest first, the
      current operation at its head; the operation's result is returned unchanged. This is the
      "durable but not yet replied" seam: a fun that ends the calling process models a crash after
      a successful fsync and before the append reply. A failed operation never reaches the fun.
    * `{:torn, keep_bytes}` — (write only) persist the first `keep_bytes` bytes,
      then halt
    * `:halt` — perform nothing and halt

  Once halted, every later operation returns `{:error, :halted}` and leaves the
  disk untouched, which models the process dying at that exact boundary. The
  state lives in an Agent so a writer process and its test share one plan.
  """

  @behaviour AiOrchestrator.Journal.Fs

  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs

  @spec new() :: Fs.t()
  def new do
    {:ok, agent} = Agent.start_link(fn -> %{plan: %{}, matchers: [], counts: %{}, trace: [], halted: false} end)
    {__MODULE__, agent}
  end

  @doc """
  Plans a fault for the nth call of `op`, or for every call whose trace args
  satisfy `matcher` (a one-arity function over the args list).
  """
  @spec inject(Fs.t(), atom(), pos_integer() | (list() -> boolean()), term()) :: :ok
  def inject({__MODULE__, agent}, op, nth, fault) when is_atom(op) and is_integer(nth) and nth >= 1 do
    Agent.update(agent, &put_in(&1, [:plan, {op, nth}], fault))
  end

  def inject({__MODULE__, agent}, op, matcher, fault) when is_atom(op) and is_function(matcher, 1) do
    Agent.update(agent, &Map.update!(&1, :matchers, fn list -> list ++ [{op, matcher, fault}] end))
  end

  @spec trace(Fs.t()) :: [tuple()]
  def trace({__MODULE__, agent}), do: agent |> Agent.get(& &1.trace) |> Enum.reverse()

  @spec halted?(Fs.t()) :: boolean()
  def halted?({__MODULE__, agent}), do: Agent.get(agent, & &1.halted)

  @impl true
  def mkdir_p(agent, dir), do: run(agent, :mkdir_p, [dir], fn -> SystemFs.mkdir_p(nil, dir) end)

  @impl true
  def mkdir(agent, dir), do: run(agent, :mkdir, [Path.basename(dir)], fn -> SystemFs.mkdir(nil, dir) end)

  @impl true
  def rm(agent, path), do: run(agent, :rm, [Path.basename(path)], fn -> SystemFs.rm(nil, path) end)

  @impl true
  def rmdir(agent, dir), do: run(agent, :rmdir, [Path.basename(dir)], fn -> SystemFs.rmdir(nil, dir) end)

  @impl true
  def list_dir(agent, dir), do: run(agent, :list_dir, [Path.basename(dir)], fn -> SystemFs.list_dir(nil, dir) end)

  @impl true
  def lstat(agent, path), do: run(agent, :lstat, [Path.basename(path)], fn -> SystemFs.lstat(nil, path) end)

  @impl true
  def chmod(agent, path, mode),
    do: run(agent, :chmod, [Path.basename(path), mode], fn -> SystemFs.chmod(nil, path, mode) end)

  @impl true
  def link(agent, existing, new),
    do: run(agent, :link, [Path.basename(existing), Path.basename(new)], fn -> SystemFs.link(nil, existing, new) end)

  @impl true
  def open(agent, path, modes),
    do: run(agent, :open, [Path.basename(path), modes], fn -> SystemFs.open(nil, path, modes) end)

  @impl true
  def write(agent, fd, iodata) do
    bytes = IO.iodata_to_binary(iodata)

    case decide(agent, :write, [byte_size(bytes)]) do
      :perform -> SystemFs.write(nil, fd, bytes)
      :halted -> {:error, :halted}
      {:fault, fault} -> write_fault(agent, fd, bytes, fault)
    end
  end

  defp write_fault(agent, fd, bytes, {:torn, keep}) when is_integer(keep) and keep >= 0 and keep < byte_size(bytes) do
    :ok = SystemFs.write(nil, fd, binary_part(bytes, 0, keep))
    :ok = SystemFs.sync(nil, fd)
    halt(agent)
  end

  defp write_fault(agent, _fd, _bytes, :halt), do: halt(agent)
  defp write_fault(_agent, _fd, _bytes, {:error, reason}), do: {:error, reason}
  # the same hook every other operation admits: side effect first, then the real write
  defp write_fault(_agent, fd, bytes, {:hook, fun}) when is_function(fun, 0), do: fun.() && SystemFs.write(nil, fd, bytes)

  @impl true
  def sync(agent, fd), do: run(agent, :sync, [], fn -> SystemFs.sync(nil, fd) end)

  @impl true
  def close(agent, fd), do: run(agent, :close, [], fn -> SystemFs.close(nil, fd) end)

  @impl true
  def rename(agent, from, to),
    do: run(agent, :rename, [Path.basename(from), Path.basename(to)], fn -> SystemFs.rename(nil, from, to) end)

  @impl true
  def dir_sync(agent, dir), do: run(agent, :dir_sync, [Path.basename(dir)], fn -> SystemFs.dir_sync(nil, dir) end)

  @impl true
  def read(agent, path), do: run(agent, :read, [Path.basename(path)], fn -> SystemFs.read(nil, path) end)

  @impl true
  def exists?(_agent, path), do: SystemFs.exists?(nil, path)

  defp run(agent, op, args, perform) do
    case decide(agent, op, args) do
      :perform -> perform.()
      :halted -> {:error, :halted}
      {:fault, fault} -> apply_fault(agent, fault, perform)
    end
  end

  defp apply_fault(agent, :halt, _perform), do: halt(agent)
  defp apply_fault(_agent, {:error, reason}, _perform), do: {:error, reason}
  defp apply_fault(_agent, {:return, value}, _perform), do: value
  defp apply_fault(_agent, {:hook, fun}, perform) when is_function(fun, 0), do: fun.() && perform.()
  defp apply_fault(agent, {:after, fun}, perform) when is_function(fun, 1), do: after_success(agent, perform.(), fun)
  defp apply_fault(_agent, other, _perform), do: raise(ArgumentError, "fault #{inspect(other)} is only valid for :write")

  # the operation already happened and succeeded; the fun sees the trace (this op at its head)
  defp after_success(agent, :ok, fun) do
    _ = fun.(Agent.get(agent, & &1.trace))
    :ok
  end

  defp after_success(_agent, result, _fun), do: result

  defp decide(agent, op, args) do
    Agent.get_and_update(agent, fn state ->
      n = Map.get(state.counts, op, 0) + 1
      state = %{state | counts: Map.put(state.counts, op, n), trace: [List.to_tuple([op | args]) | state.trace]}

      matched = Enum.find(state.matchers, fn {m_op, matcher, _fault} -> m_op == op and matcher.(args) end)

      cond do
        state.halted -> {:halted, state}
        Map.has_key?(state.plan, {op, n}) -> {{:fault, Map.fetch!(state.plan, {op, n})}, state}
        matched != nil -> {{:fault, elem(matched, 2)}, state}
        true -> {:perform, state}
      end
    end)
  end

  defp halt(agent) do
    Agent.update(agent, &%{&1 | halted: true})
    {:error, :halted}
  end
end
