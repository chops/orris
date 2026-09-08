defmodule AiOrchestrator.Run do
  @moduledoc """
  The foreground run subtree (NS-05): one `AiOrchestrator.Run.Supervisor` per run hosting the
  journal writer, a `AiOrchestrator.Run.Server` that steps the Host loop, and an empty
  `AiOrchestrator.Run.Work.Supervisor`. Internal executor context, not a command surface: the
  Host and RunFSM signatures are unchanged and nothing here restarts, migrates or times work.
  """

  use Boundary,
    deps: [
      AiOrchestrator.Clock,
      AiOrchestrator.Commands,
      AiOrchestrator.Contract,
      AiOrchestrator.Effects,
      AiOrchestrator.Id,
      AiOrchestrator.Journal,
      AiOrchestrator.Lifecycle,
      AiOrchestrator.Spec
    ],
    exports: [DeadlineFence, Executor, Recovery, Recovery.Exhaustion, Server, Supervisor, Work.Supervisor, Worker]
end
