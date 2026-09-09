defmodule OrrisConsole.MutationWorkers do
  @moduledoc "The accepted operations: a DynamicSupervisor capped at mutation_capacity whose parent budget is :infinity (each operation drains under its own mutation_shutdown_ms)."
  use DynamicSupervisor

  def start_link(%OrrisConsole.Config{} = config),
    do: DynamicSupervisor.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor, shutdown: :infinity}

  @impl true
  def init(config), do: DynamicSupervisor.init(strategy: :one_for_one, max_children: config.mutation_capacity)
end
