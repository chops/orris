defmodule OrrisConsole.Mutations do
  @moduledoc """
  The mutation subtree (docs/contracts/console-mutations.org): MutationRegistry (start wrapper) → MutationWorkers →
  SessionStore under rest_for_one, so a Registry loss drains the workers and restarts the authority after them, and
  a workers loss restarts the authority (whose census then sees any init-held orphan). Default restart intensity:
  a crash storm ends this subtree and, one level up, the console (fail closed).
  """
  use Supervisor
  alias OrrisConsole.{Config, MutationRegistry, MutationWorkers, SessionStore}

  def start_link(%Config{} = config), do: Supervisor.start_link(__MODULE__, config, name: __MODULE__)

  def child_spec(config), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :supervisor}

  @impl true
  def init(config) do
    children = [MutationRegistry, {MutationWorkers, config}, {SessionStore, config}]
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
