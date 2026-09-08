defmodule AiOrchestrator.Test.GateDeadlineAuthority do
  @moduledoc """
  TEST-ONLY Server-side timer authority double (docs/contracts/gate-ownership.org, prototype GP rows; GO-M1).

  Arms exactly one `Run.DeadlineFence` per operation identity from ONE wall and ONE monotonic read of the injected
  clock, sleeps in bounded timer chunks (`cap_ms`, proposed default 60 s, not runtime policy), and treats every
  dequeued wake as a CLAIM to be validated against the fence's canonical monotonic due time: a wake before the due
  time is `:early` (re-chunked, never fired), a wake at or after it fires `{:gate_deadline, op}` to the owner ONCE.
  Duplicate arms, completions of fired/cancelled identities and unknown identities are classified, never actuated.
  A timer identity is not due-time proof; the fence is. Not product proof.
  """
  use GenServer

  alias AiOrchestrator.Run.DeadlineFence

  @default_cap_ms 60_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Arm the identity at `deadline_unix`; `{:armed, due_ms}` | `{:refused, :duplicate}` | `{:refused, map}`."
  def arm(pid, op, deadline_unix, owner), do: GenServer.call(pid, {:arm, op, deadline_unix, owner})

  @doc "The effect answered: `:cancelled` | `{:stale, :fired | :cancelled | :foreign}`."
  def complete(pid, op), do: GenServer.call(pid, {:complete, op})

  @doc "`:armed` | `:fired` | `:cancelled` | `:foreign`."
  def classify(pid, op), do: GenServer.call(pid, {:classify, op})

  @doc "Classification of the last dequeued wake for the identity: `:early` | `:due` | `:stale` | nil."
  def last_wake(pid, op), do: GenServer.call(pid, {:last_wake, op})

  @doc "Remaining ms of the identity's CURRENT timer chunk (GP-2c witness), or nil when none is scheduled."
  def timer_remaining_ms(pid, op), do: GenServer.call(pid, {:timer_remaining_ms, op})

  @impl true
  def init(opts) do
    {:ok, %{clock: Keyword.fetch!(opts, :clock), cap_ms: Keyword.get(opts, :cap_ms, @default_cap_ms), entries: %{}}}
  end

  @impl true
  def handle_call({:arm, op, deadline_unix, owner}, _from, %{entries: entries} = state) do
    if Map.has_key?(entries, op) do
      {:reply, {:refused, :duplicate}, state}
    else
      unix = state.clock.unix_now()
      mono = state.clock.monotonic_ms()

      case DeadlineFence.arm(op, deadline_unix, unix, mono, state.cap_ms) do
        {:ok, fence} ->
          entry = schedule(%{fence: fence, status: :armed, owner: owner, timer: nil, last_wake: nil}, op, mono)
          {:reply, {:armed, fence.due_ms}, %{state | entries: Map.put(entries, op, entry)}}

        {:error, refusal} ->
          {:reply, {:refused, refusal}, state}
      end
    end
  end

  def handle_call({:complete, op}, _from, %{entries: entries} = state) do
    case Map.fetch(entries, op) do
      {:ok, %{status: :armed} = entry} ->
        cancel(entry)
        {:reply, :cancelled, %{state | entries: Map.put(entries, op, %{entry | status: :cancelled, timer: nil})}}

      {:ok, %{status: status}} ->
        {:reply, {:stale, status}, state}

      :error ->
        {:reply, {:stale, :foreign}, state}
    end
  end

  def handle_call({:classify, op}, _from, state), do: {:reply, get_in(state.entries, [op, :status]) || :foreign, state}
  def handle_call({:last_wake, op}, _from, state), do: {:reply, get_in(state.entries, [op, :last_wake]), state}

  def handle_call({:timer_remaining_ms, op}, _from, state) do
    reply =
      case get_in(state.entries, [op, :timer]) do
        nil -> nil
        timer -> Process.read_timer(timer)
      end

    {:reply, reply, state}
  end

  # a wake is a claim: the fence's monotonic due time decides, never the timer's identity
  @impl true
  def handle_info({:fire, op}, %{entries: entries} = state) do
    case Map.fetch(entries, op) do
      {:ok, %{status: :armed, fence: fence, owner: owner} = entry} ->
        mono = state.clock.monotonic_ms()

        entry =
          case DeadlineFence.next(fence, mono) do
            :due ->
              cancel(entry)
              send(owner, {:gate_deadline, op})
              %{entry | status: :fired, timer: nil, last_wake: :due}

            {:wait, _ms} ->
              schedule(%{entry | last_wake: :early}, op, mono)
          end

        {:noreply, %{state | entries: Map.put(entries, op, entry)}}

      {:ok, entry} ->
        {:noreply, %{state | entries: Map.put(entries, op, %{entry | last_wake: :stale})}}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, %{entries: entries}), do: Enum.each(entries, fn {_op, entry} -> cancel(entry) end)

  defp schedule(entry, op, mono) do
    cancel(entry)

    case DeadlineFence.next(entry.fence, mono) do
      :due -> %{entry | timer: Process.send_after(self(), {:fire, op}, 0)}
      {:wait, ms} -> %{entry | timer: Process.send_after(self(), {:fire, op}, ms)}
    end
  end

  defp cancel(%{timer: nil}), do: :ok
  defp cancel(%{timer: timer}), do: Process.cancel_timer(timer)
end
