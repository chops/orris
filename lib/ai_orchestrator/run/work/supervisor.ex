defmodule AiOrchestrator.Run.Work.Supervisor do
  @moduledoc """
  The run's work supervisor: present in the ratified topology, deliberately empty in this
  foundation (no workers are started under it). Its readiness is traced like every sibling.
  """

  use DynamicSupervisor

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(_arg), do: DynamicSupervisor.start_link(__MODULE__, [])

  @impl true
  def init(_arg), do: DynamicSupervisor.init(strategy: :one_for_one)
end
