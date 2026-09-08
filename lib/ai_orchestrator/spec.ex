defmodule AiOrchestrator.Spec do
  @moduledoc false

  use Boundary, deps: [], exports: [Budgets, RunSpec, Plan]
end
