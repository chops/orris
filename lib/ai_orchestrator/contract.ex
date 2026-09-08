defmodule AiOrchestrator.Contract do
  @moduledoc """
  Dependency-free command, effect, and observation values shared by the
  functional lifecycle core and its hosts.

  These structs describe data only. Authorization belongs to `Commands`, state
  admissibility belongs to the lifecycle reducer, and execution belongs to a
  host or worker.
  """

  use Boundary,
    deps: [],
    exports: [
      Command,
      ArtifactBaseline,
      Diagnostic,
      Effect,
      FileError,
      Moment,
      PromptObject,
      PromptRejection,
      SensitiveBytes,
      Effect.Clock,
      Effect.Dispatch,
      Effect.FetchPrompt,
      Effect.Notify,
      Effect.Observe,
      Effect.ReadReview,
      Effect.ReconcileSend,
      Effect.RetainPrompt,
      Effect.RunGate,
      Effect.PrepareGate,
      Effect.ReleaseGate,
      Effect.AwaitGate,
      Effect.ReconcileGate,
      Effect.SnapshotArtifact,
      Effect.Timer,
      Observation.ArtifactObserved,
      Observation.ArtifactSnapshot,
      Observation.ArtifactSnapshotFailed,
      Observation.Blocked,
      Observation.Clock,
      Observation.Deadline,
      Observation.Dispatched,
      Observation.DispatchFailed,
      Observation.GatePrepared,
      Observation.GatePrepareFailed,
      Observation.GateReleased,
      Observation.GateReleaseFailed,
      Observation.GateUnsettled,
      Observation.GateReconciled,
      Observation.GateReconcileFailed,
      Observation.GateFailed,
      Observation.GateError,
      Observation.GateFinished,
      Observation.Notified,
      Observation.NotifyFailed,
      Observation.ObserveFailed,
      Observation,
      Observation.Pending,
      Observation.PromptFetched,
      Observation.PromptFetchFailed,
      Observation.SendReconciled,
      Observation.SendReconcileFailed,
      Observation.PromptRetained,
      Observation.PromptRetentionFailed,
      Observation.ReviewRead,
      Observation.ReviewUnreadable,
      Observation.TimedOut,
      SendId
    ]
end
