defmodule OrrisConsole.SessionStore do
  @moduledoc """
  The console's in-memory session authority (C1-05/08/09): loads the credential digest at start (fails closed),
  serializes login (bounded token bucket, secure digest comparison, capacity 128, fresh random ids), validation
  (:observe never renews idle, :action does; expiry is exact against the configured clock), view registration
  (at most 8 per session, monitored) and revocation (views notified BEFORE revoke returns). Only digests of ids are
  stored. The formatted status and crash reports never carry digests or ids (format_status).
  """
  use GenServer
  alias OrrisConsole.{Config, Credential, Session}

  @secret_bytes 32

  def start_link(%Config{} = config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, restart: :permanent, shutdown: 5_000}

  @spec login(GenServer.server(), term()) ::
          {:ok, binary()} | {:error, :invalid | :rate_limited | :capacity | :unavailable}
  def login(store, submitted) when is_binary(submitted) and byte_size(submitted) == @secret_bytes,
    do: call(store, {:login, submitted})

  # a malformed submission still consumes a login token (the limiter counts attempts) and allocates nothing
  def login(store, _other), do: call(store, {:login, :invalid})

  @spec validate(GenServer.server(), term(), :observe | :action) ::
          {:ok, Session.t()} | {:error, :invalid | :expired | :unavailable}
  def validate(store, id, mode) when is_binary(id) and byte_size(id) in 16..128 and mode in [:observe, :action],
    do: call(store, {:validate, id, mode})

  def validate(_store, _id, _mode), do: {:error, :invalid}

  @spec register_view(GenServer.server(), term(), pid()) ::
          :ok | {:error, :invalid | :expired | :view_capacity | :unavailable}
  def register_view(store, id, pid) when is_binary(id) and is_pid(pid), do: call(store, {:register_view, id, pid})
  def register_view(_store, _id, _pid), do: {:error, :invalid}

  @spec revoke(GenServer.server(), term()) :: :ok
  def revoke(store, id) when is_binary(id), do: call(store, {:revoke, id}) |> then(fn _ -> :ok end)
  def revoke(_store, _id), do: :ok

  @spec revoke_all(GenServer.server()) :: :ok
  def revoke_all(store), do: call(store, :revoke_all) |> then(fn _ -> :ok end)

  @spec counts(GenServer.server()) :: %{sessions: non_neg_integer(), views: non_neg_integer()}
  def counts(store), do: GenServer.call(store, :counts)

  defp call(store, request) do
    GenServer.call(store, request)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  # ---- server ----
  @impl true
  def init(%Config{} = config) do
    case Credential.load(config.credential_path) do
      {:ok, digest} ->
        Process.send_after(self(), :sweep, config.sweep_ms)

        {:ok,
         %{
           config: config,
           digest: digest,
           clock: config.clock || fn -> System.monotonic_time(:millisecond) end,
           sessions: %{},
           views: %{},
           bucket: %{tokens: config.login_capacity, last_ms: nil}
         }}

      {:error, reason} ->
        {:stop, {:credential, reason}}
    end
  end

  @impl true
  def handle_call({:login, submitted}, _from, state) do
    now = state.clock.()
    {allowed?, bucket} = take_token(state.bucket, state.config, now)

    cond do
      not allowed? ->
        {:reply, {:error, :rate_limited}, %{state | bucket: bucket}}

      submitted == :invalid or not Plug.Crypto.secure_compare(Credential.digest(submitted), state.digest) ->
        {:reply, {:error, :invalid}, %{state | bucket: bucket}}

      map_size(state.sessions) >= state.config.session_capacity ->
        {:reply, {:error, :capacity}, %{state | bucket: bucket}}

      true ->
        id = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

        session = %Session{
          actor_id: state.config.operator.id,
          root_ids: state.config.operator.root_ids,
          issued_ms: now,
          idle_deadline_ms: now + state.config.idle_ms,
          absolute_deadline_ms: now + state.config.absolute_ms
        }

        {:reply, {:ok, id}, %{state | bucket: bucket, sessions: Map.put(state.sessions, key(id), session)}}
    end
  end

  def handle_call({:validate, id, mode}, _from, state) do
    case lookup(state, id) do
      {:ok, k, session} ->
        session =
          if mode == :action, do: %{session | idle_deadline_ms: state.clock.() + state.config.idle_ms}, else: session

        {:reply, {:ok, session}, %{state | sessions: Map.put(state.sessions, k, session)}}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:register_view, id, pid}, _from, state) do
    case lookup(state, id) do
      {:ok, k, _session} ->
        if Enum.count(state.views, fn {_ref, {vk, _pid}} -> vk == k end) >= state.config.views_per_session do
          {:reply, {:error, :view_capacity}, state}
        else
          ref = Process.monitor(pid)
          {:reply, :ok, %{state | views: Map.put(state.views, ref, {k, pid})}}
        end

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:revoke, id}, _from, state), do: {:reply, :ok, drop(state, key(id), id)}

  def handle_call(:revoke_all, _from, state) do
    for {ref, {_k, pid}} <- state.views do
      Process.demonitor(ref, [:flush])
      send(pid, {:session_revoked, :all})
    end

    {:reply, :ok, %{state | sessions: %{}, views: %{}}}
  end

  def handle_call(:counts, _from, state),
    do: {:reply, %{sessions: map_size(state.sessions), views: map_size(state.views)}, state}

  @impl true
  def handle_info(:sweep, state) do
    now = state.clock.()

    expired = for {k, s} <- state.sessions, now >= s.idle_deadline_ms or now >= s.absolute_deadline_ms, do: k
    state = Enum.reduce(expired, state, fn k, acc -> drop(acc, k, nil) end)
    Process.send_after(self(), :sweep, state.config.sweep_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state),
    do: {:noreply, %{state | views: Map.delete(state.views, ref)}}

  def handle_info(_other, state), do: {:noreply, state}

  # the formatted status (:sys.get_status, crash reports) carries counts only: no digests, ids, config secrets
  @impl true
  def format_status(status) do
    summary = fn
      %{sessions: s, views: v} -> %{sessions: map_size(s), views: map_size(v)}
      other -> other
    end

    status |> Map.update(:state, nil, summary) |> Map.put(:message, :redacted) |> Map.put(:queue, :redacted)
  end

  # ---- helpers ----
  defp key(id), do: :crypto.hash(:sha256, id)

  defp lookup(state, id) do
    k = key(id)

    case Map.fetch(state.sessions, k) do
      :error ->
        {:error, :invalid, state}

      {:ok, session} ->
        now = state.clock.()

        if now >= session.idle_deadline_ms or now >= session.absolute_deadline_ms,
          do: {:error, :expired, drop(state, k, nil)},
          else: {:ok, k, session}
    end
  end

  # removes a session and its views; each view receives the revocation notice BEFORE the caller is answered
  defp drop(state, k, id) do
    {gone, kept} = Enum.split_with(state.views, fn {_ref, {vk, _pid}} -> vk == k end)

    for {ref, {_k, pid}} <- gone do
      Process.demonitor(ref, [:flush])
      send(pid, {:session_revoked, id || :expired})
    end

    %{state | sessions: Map.delete(state.sessions, k), views: Map.new(kept)}
  end

  defp take_token(%{tokens: tokens, last_ms: last}, config, now) do
    refilled = if last, do: min(config.login_capacity, tokens + div(now - last, config.login_refill_ms)), else: tokens
    last = if last && refilled > tokens, do: now, else: last || now

    if refilled > 0, do: {true, %{tokens: refilled - 1, last_ms: last}}, else: {false, %{tokens: 0, last_ms: last}}
  end
end
