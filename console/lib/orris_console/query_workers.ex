defmodule OrrisConsole.QueryWorkers do
  @moduledoc "The actual read workers: a DynamicSupervisor whose max_children is the worker capacity; a slot is held until the worker's real DOWN."
  use DynamicSupervisor

  def start_link(%OrrisConsole.Config{} = config),
    do: DynamicSupervisor.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor}

  @impl true
  def init(config), do: DynamicSupervisor.init(strategy: :one_for_one, max_children: config.worker_capacity)
end
