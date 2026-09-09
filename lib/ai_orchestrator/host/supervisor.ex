defmodule AiOrchestrator.Host.Supervisor do
  @moduledoc """
  The host's run-tree supervisor (docs/contracts/host-mounted-runs.org): a DynamicSupervisor whose children
  are `AiOrchestrator.Host.RunOwner` processes, one per mounted run, `restart: :temporary`. Children are
  terminated concurrently on shutdown, each bounded by the configured `child_shutdown_ms` (default 60_000),
  after which the supervisor kills. Nothing is restarted: recovery is a new command on the journal.
  """

  use DynamicSupervisor

  @default_child_shutdown_ms 60_000

  @doc "Starts the host run supervisor, registered as `__MODULE__` unless `:name` says otherwise (`nil` = unregistered)."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, registration(name))
  end

  defp registration(nil), do: []
  defp registration(name), do: [name: name]

  @doc "The child shutdown budget this supervisor was started with (read by the host when it mounts)."
  @spec child_shutdown_ms(pid() | atom()) :: pos_integer()
  def child_shutdown_ms(server) do
    pid = if is_pid(server), do: server, else: Process.whereis(server)
    :persistent_term.get({__MODULE__, pid}, @default_child_shutdown_ms)
  end

  @impl true
  def init(opts) do
    :persistent_term.put({__MODULE__, self()}, Keyword.get(opts, :child_shutdown_ms, @default_child_shutdown_ms))
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
