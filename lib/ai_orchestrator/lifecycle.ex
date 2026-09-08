defmodule AiOrchestrator.Lifecycle do
  @moduledoc false

  use Boundary,
    deps: [
      AiOrchestrator.Clock,
      AiOrchestrator.Contract,
      AiOrchestrator.Id,
      AiOrchestrator.Effects,
      AiOrchestrator.Journal,
      AiOrchestrator.Projection,
      AiOrchestrator.Spec
    ],
    exports: [Host, RunFSM]
end
