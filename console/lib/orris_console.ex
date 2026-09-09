defmodule OrrisConsole do
  @moduledoc """
  The console boundary: it depends on the core ONLY through the top-level exports (`AiOrchestrator.Query` for this
  read-only slice). A reference to a private core module is a forbidden reference under the Boundary compiler and
  fails `--warnings-as-errors` (row C1-01c, bin/c1-packaging-controls). No application child is defined yet (RED).
  """
  use Boundary, deps: [AiOrchestrator], exports: []
end
