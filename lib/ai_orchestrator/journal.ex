defmodule AiOrchestrator.Journal do
  @moduledoc false

  use Boundary,
    deps: [AiOrchestrator.Clock, AiOrchestrator.ProcessIdentity],
    exports: [
      Event,
      Fold,
      Fold.Context,
      Fold.State,
      Fs,
      Fs.SystemFs,
      Ownership,
      Reader,
      Schemas.RequestedBy,
      Vocabulary,
      Writer
    ]
end
