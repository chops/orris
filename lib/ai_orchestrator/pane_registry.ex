defmodule AiOrchestrator.PaneRegistry do
  @moduledoc false

  use Boundary, deps: [AiOrchestrator.ProcessIdentity], exports: [FileRegistry]
end
