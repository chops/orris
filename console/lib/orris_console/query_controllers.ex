defmodule OrrisConsole.QueryControllers do
  @moduledoc "The responsive QueryJob controllers: a DynamicSupervisor capped separately (controller capacity); a caller never queues for a slot."
  use DynamicSupervisor

  def start_link(%OrrisConsole.Config{} = config),
    do: DynamicSupervisor.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor}

  @impl true
  def init(config), do: DynamicSupervisor.init(strategy: :one_for_one, max_children: config.controller_capacity)
end
