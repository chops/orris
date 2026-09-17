defmodule AiOrchestrator.Spec do
  @moduledoc false

  use Boundary, deps: [], exports: [Budgets, PathBoundary, RunSpec, Plan]
end
