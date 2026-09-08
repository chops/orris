defmodule AiOrchestrator.Gate do
  @moduledoc false

  use Boundary, deps: [AiOrchestrator.Clock, AiOrchestrator.Journal], exports: [Execution, Execution.Ack, Runner]
end
