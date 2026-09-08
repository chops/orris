defmodule AiOrchestrator.Run.DeadlineFence do
  @moduledoc """
  The pure deadline fence (docs/contracts/deadline-fence.org, U2a-0): a monotonic due instant armed ONCE from
  a journaled wall deadline, bounded waits that never shorten it, and one winner-preserving transition over facts
  the consuming process has already dequeued.

  Nothing here reads a clock, starts a timer, owns a process or touches the journal. `arm/5` and `next/2` are
  arithmetic over SUPPLIED instants; `observe/2` classifies a dequeued fence wake or result against the
  caller-threaded outstanding state and answers with advice (`:request_expiration`, `:retain_result`,
  `:retain_late_result`). An expiration decision is neither a stop nor settlement, delivery or journal evidence.
  Passing a fence map into `observe/2` authenticates nothing about its due time: the later runtime reads its own
  clock, retains its canonical armed fence, correlates its own dequeue and threads the returned state itself.
  This module enforces no deadline (R-D / NS-18 stay open).
  """

  @max_wait_cap_ms 4_294_967_295
  @states [:running, :timeout_selected, :completed, :completed_after_timeout]

  @type identity :: %{cap: reference(), gen: pos_integer(), ref: reference()}
  @type fence :: %{due_ms: integer(), identity: identity(), wait_cap_ms: pos_integer()}
  @type state :: :running | :timeout_selected | :completed | :completed_after_timeout
  @type outstanding :: %{identity: identity(), state: state()}
  @type decision :: :request_expiration | :retain_result | :retain_late_result
  @type stale ::
          :foreign_cap
          | :foreign_generation
          | :foreign_ref
          | :completed
          | :duplicate_timeout
          | :duplicate_completion
  @type refusal :: {:error, %{clause: String.t()}}
  @type event :: {:fence, fence(), integer()} | {:result, identity()}

  @doc "Arm once: the monotonic due instant from the journaled wall deadline, `unix_now` and `monotonic_now_ms`."
  @spec arm(term(), term(), term(), term(), term()) :: {:ok, fence()} | refusal()
  def arm(identity, deadline_unix, unix_now, monotonic_now_ms, wait_cap_ms) do
    with :ok <- check_identity(identity),
         :ok <- check_instants([deadline_unix, unix_now, monotonic_now_ms]),
         :ok <- check_cap(wait_cap_ms) do
      due_ms = monotonic_now_ms + max(deadline_unix - unix_now, 0) * 1_000
      {:ok, %{due_ms: due_ms, identity: identity, wait_cap_ms: wait_cap_ms}}
    end
  end

  @doc "The bounded next wait at a supplied monotonic instant, or `:due`; a chunk end before due is another wait."
  @spec next(term(), term()) :: {:wait, pos_integer()} | :due | refusal()
  def next(fence, monotonic_now_ms) do
    with :ok <- check_fence(fence),
         :ok <- check_instant(monotonic_now_ms) do
      wait_or_due(fence, monotonic_now_ms)
    end
  end

  @doc "The initial outstanding state for an operation identity."
  @spec outstanding(term()) :: {:ok, outstanding()} | refusal()
  def outstanding(identity) do
    with :ok <- check_identity(identity), do: {:ok, %{identity: identity, state: :running}}
  end

  @doc "One pure transition over a DEQUEUED fact; the winner is preserved in the returned state."
  @spec observe(term(), term()) ::
          {:ok, outstanding(), decision()}
          | {:early, {:wait, pos_integer()}, outstanding()}
          | {:stale, stale(), outstanding()}
          | refusal()
  def observe(outstanding, event) do
    with :ok <- check_outstanding(outstanding),
         {:ok, kind, identity, extra} <- check_event(event),
         :ok <- correlate(outstanding.identity, identity) do
      transition(kind, outstanding, extra)
    else
      {:stale, reason} -> {:stale, reason, outstanding}
      {:error, _} = refusal -> refusal
    end
  end

  # ---- the state table (docs/contracts/deadline-fence.org, "State machine") ----

  defp transition(:fence, %{state: :running} = outstanding, {fence, now_ms}) do
    case wait_or_due(fence, now_ms) do
      :due -> {:ok, %{outstanding | state: :timeout_selected}, :request_expiration}
      {:wait, _ms} = wait -> {:early, wait, outstanding}
    end
  end

  defp transition(:fence, %{state: :timeout_selected} = outstanding, _extra),
    do: {:stale, :duplicate_timeout, outstanding}

  defp transition(:fence, %{state: :completed} = outstanding, _extra), do: {:stale, :completed, outstanding}

  defp transition(:fence, %{state: :completed_after_timeout} = outstanding, _extra),
    do: {:stale, :duplicate_timeout, outstanding}

  defp transition(:result, %{state: :running} = outstanding, nil),
    do: {:ok, %{outstanding | state: :completed}, :retain_result}

  defp transition(:result, %{state: :timeout_selected} = outstanding, nil),
    do: {:ok, %{outstanding | state: :completed_after_timeout}, :retain_late_result}

  defp transition(:result, outstanding, nil), do: {:stale, :duplicate_completion, outstanding}

  defp wait_or_due(%{due_ms: due_ms, wait_cap_ms: cap}, now_ms) do
    case max(due_ms - now_ms, 0) do
      0 -> :due
      remaining -> {:wait, min(remaining, cap)}
    end
  end

  # foreign identity, checked cap then gen then ref (no input is copied into the reason)
  defp correlate(%{cap: cap, gen: gen, ref: ref}, %{cap: cap, gen: gen, ref: ref}), do: :ok
  defp correlate(%{cap: cap}, %{cap: other}) when cap != other, do: {:stale, :foreign_cap}
  defp correlate(%{gen: gen}, %{gen: other}) when gen != other, do: {:stale, :foreign_generation}
  defp correlate(_ours, _theirs), do: {:stale, :foreign_ref}

  # ---- closed input domains and precedence (docs/contracts/deadline-fence.org, "Closed input domains") ----

  defp check_event({:fence, fence, now_ms}) do
    with :ok <- check_fence(fence),
         :ok <- check_instant(now_ms) do
      {:ok, :fence, fence.identity, {fence, now_ms}}
    end
  end

  defp check_event({:result, identity}) do
    with :ok <- check_identity(identity), do: {:ok, :result, identity, nil}
  end

  defp check_event(_other), do: refuse("event_invalid")

  defp check_outstanding(%{identity: identity, state: state} = outstanding) when is_map(outstanding) do
    if plain_map?(outstanding) and map_size(outstanding) == 2 and state in @states and identity_ok?(identity),
      do: :ok,
      else: refuse("outstanding_invalid")
  end

  defp check_outstanding(_other), do: refuse("outstanding_invalid")

  defp check_fence(%{due_ms: due_ms, identity: identity, wait_cap_ms: cap} = fence) when is_map(fence) do
    if plain_map?(fence) and map_size(fence) == 3 and is_integer(due_ms) and identity_ok?(identity) and cap_ok?(cap),
      do: :ok,
      else: refuse("fence_invalid")
  end

  defp check_fence(_other), do: refuse("fence_invalid")

  defp check_identity(identity), do: if(identity_ok?(identity), do: :ok, else: refuse("fence_identity_invalid"))

  defp identity_ok?(%{cap: cap, gen: gen, ref: ref} = identity) when is_map(identity),
    do: plain_map?(identity) and map_size(identity) == 3 and is_reference(cap) and is_reference(ref) and pos_int?(gen)

  defp identity_ok?(_other), do: false

  defp check_instants(instants), do: if(Enum.all?(instants, &is_integer/1), do: :ok, else: refuse("deadline_invalid"))
  defp check_instant(instant), do: if(is_integer(instant), do: :ok, else: refuse("instant_invalid"))
  defp check_cap(cap), do: if(cap_ok?(cap), do: :ok, else: refuse("wait_cap_invalid"))

  defp cap_ok?(cap), do: pos_int?(cap) and cap <= @max_wait_cap_ms
  defp pos_int?(value), do: is_integer(value) and value >= 1
  # every caller has already matched a map; a struct is a map carrying :__struct__
  defp plain_map?(map) when is_map(map), do: not Map.has_key?(map, :__struct__)
  defp refuse(clause), do: {:error, %{clause: clause}}
end
