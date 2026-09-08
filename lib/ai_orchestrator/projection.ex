defmodule AiOrchestrator.Projection do
  @moduledoc false

  use Boundary, deps: [AiOrchestrator.Journal], exports: [RunContext, RunSummary]
end
