defmodule AiOrchestrator.Contract.Effect do
  @moduledoc "Typed effect descriptions emitted by the lifecycle reducer."

  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes

  defmodule Clock do
    @moduledoc "Read wall-clock facts after preceding intent events are durable."
    @enforce_keys [:read_index, :purpose]
    defstruct [:read_index, :purpose]

    @type t :: %__MODULE__{read_index: non_neg_integer(), purpose: String.t()}
  end

  defmodule Dispatch do
    @moduledoc "Deliver one assignment message through a dispatch adapter."
    @enforce_keys [:assignment_id, :command, :message_id]
    defstruct [:assignment_id, :command, :message_id, :deadline_unix]

    @typedoc "The durable assignment deadline the reducer propagates from `assignment_requested` (U2b delivery deadline)."
    @type t :: %__MODULE__{
            assignment_id: String.t(),
            command: map(),
            message_id: String.t(),
            deadline_unix: integer() | nil
          }
  end

  defmodule SnapshotArtifact do
    @moduledoc """
    Take the expected artifact's baseline at the adapter boundary, before the projection
    is committed and before anything is pasted (MUST-7). Emitted with no events: the
    projection that records the answer is journaled only once the host has it.
    """
    @enforce_keys [:assignment_id, :command]
    defstruct [:assignment_id, :command]

    @type t :: %__MODULE__{assignment_id: String.t(), command: map()}
  end

  defmodule ReconcileSend do
    @moduledoc """
    Ask the dispatch adapter what became of a send the journal recorded as `queued`.

    The command is the same one the send was made with, so the adapter asks under the
    recorded id and the journaled payload hash; the answer is one of the five ratified
    reconcile outcomes, never a paste.
    """
    @enforce_keys [:assignment_id, :command]
    defstruct [:assignment_id, :command, :deadline_unix]

    @type t :: %__MODULE__{assignment_id: String.t(), command: map(), deadline_unix: integer() | nil}
  end

  defmodule Observe do
    @moduledoc "Observe assignment progress without deciding lifecycle state."
    @enforce_keys [:assignment_id, :command, :deadline_unix]
    defstruct [:assignment_id, :command, :deadline_unix]

    @type t :: %__MODULE__{
            assignment_id: String.t(),
            command: map(),
            deadline_unix: integer()
          }
  end

  defmodule ReadReview do
    @moduledoc "Read a reviewer artifact at an operator-authored path."
    @enforce_keys [:assignment_id, :path]
    defstruct [:assignment_id, :path]

    @type t :: %__MODULE__{assignment_id: String.t(), path: String.t()}
  end

  defmodule RunGate do
    @moduledoc "Run one argv-native gate with an enforced deadline."
    @enforce_keys [:gate_run_id, :requested, :repo_root, :run_dir]
    defstruct [:gate_run_id, :requested, :repo_root, :run_dir]

    @type t :: %__MODULE__{
            gate_run_id: String.t(),
            requested: map(),
            repo_root: String.t(),
            run_dir: String.t()
          }
  end

  defmodule PrepareGate do
    @moduledoc "Prepare one gate under the native guardian and publish its durable claim (attempt 1 or 2)."
    @enforce_keys [:gate_run_id, :attempt, :requested, :deadline_unix, :repo_root, :run_dir]
    defstruct [:gate_run_id, :attempt, :requested, :deadline_unix, :repo_root, :run_dir]

    @type t :: %__MODULE__{
            gate_run_id: String.t(),
            attempt: 1..2,
            requested: map(),
            deadline_unix: integer(),
            repo_root: String.t(),
            run_dir: String.t()
          }
  end

  defmodule ReleaseGate do
    @moduledoc "Release the prepared gate once, against the Ack of the committed gate_started event at `started_seq`."
    @enforce_keys [:gate_run_id, :attempt, :started_seq]
    defstruct [:gate_run_id, :attempt, :started_seq]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, started_seq: pos_integer()}
  end

  defmodule AwaitGate do
    @moduledoc "Await the released gate's evidence, bounded by the original absolute deadline."
    @enforce_keys [:gate_run_id, :attempt, :deadline_unix]
    defstruct [:gate_run_id, :attempt, :deadline_unix]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, deadline_unix: integer()}
  end

  defmodule ReconcileGate do
    @moduledoc "Cold, read-only reconcile of a journaled gate start against its claim and kernel facts."
    @enforce_keys [:gate_run_id, :attempt, :expected]
    defstruct [:gate_run_id, :attempt, :expected]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, expected: map()}
  end

  defmodule Notify do
    @moduledoc "Invoke one bounded best-effort notification hook."
    @enforce_keys [:notification_id, :hook_argv, :payload]
    defstruct [:notification_id, :hook_argv, :payload]

    @type t :: %__MODULE__{
            notification_id: String.t(),
            hook_argv: [String.t()],
            payload: map()
          }
  end

  defmodule Timer do
    @moduledoc "Ask the run host to own a durable lifecycle deadline."
    @enforce_keys [:purpose, :deadline_unix]
    defstruct [:purpose, :deadline_unix]

    @type t :: %__MODULE__{purpose: String.t(), deadline_unix: integer()}
  end

  defmodule RetainPrompt do
    @moduledoc """
    Retain rendered prompt bytes as a durable object before any event names them.

    `scheme` is the naming scheme the object is published under, with exactly
    `PromptObject.version/0`'s meaning: `2` is content-addressed and is what
    retention produces; `1` is the bare legacy name, used only to make a legacy
    projection true in place, exactly once. The effect still carries no object --
    the store names the object, and the observation returns it.
    """
    @enforce_keys [:assignment_id, :bytes]
    defstruct [:assignment_id, :bytes, scheme: 2]

    @type t :: %__MODULE__{assignment_id: String.t(), bytes: SensitiveBytes.t(), scheme: PromptObject.version()}
  end

  defmodule FetchPrompt do
    @moduledoc """
    Read back the retained prompt object a projection already names.

    The object carries the assignment. A separate `assignment_id` beside it
    would be a second answer to the same question, and the two are only ever
    checked against each other by whoever remembers to.
    """
    @enforce_keys [:object]
    defstruct [:object]

    @type t :: %__MODULE__{object: PromptObject.t()}
  end

  @type t ::
          Clock.t()
          | Dispatch.t()
          | ReconcileSend.t()
          | SnapshotArtifact.t()
          | Observe.t()
          | ReadReview.t()
          | RunGate.t()
          | PrepareGate.t()
          | ReleaseGate.t()
          | AwaitGate.t()
          | ReconcileGate.t()
          | Notify.t()
          | Timer.t()
          | RetainPrompt.t()
          | FetchPrompt.t()

  @doc "Lists the only observation variants that may complete an effect."
  @spec admissible_observations(t()) :: [module()]
  def admissible_observations(%Clock{}), do: [Observation.Clock]
  def admissible_observations(%Dispatch{}), do: [Observation.Dispatched, Observation.DispatchFailed]
  def admissible_observations(%ReconcileSend{}), do: [Observation.SendReconciled, Observation.SendReconcileFailed]
  def admissible_observations(%SnapshotArtifact{}), do: [Observation.ArtifactSnapshot, Observation.ArtifactSnapshotFailed]

  def admissible_observations(%Observe{}) do
    [
      Observation.ArtifactObserved,
      Observation.Blocked,
      Observation.Pending,
      Observation.ObserveFailed,
      Observation.TimedOut
    ]
  end

  def admissible_observations(%ReadReview{}), do: [Observation.ReviewRead, Observation.ReviewUnreadable]

  def admissible_observations(%RunGate{}) do
    [Observation.GateFinished, Observation.GateFailed, Observation.GateError]
  end

  def admissible_observations(%PrepareGate{}), do: [Observation.GatePrepared, Observation.GatePrepareFailed]
  def admissible_observations(%ReleaseGate{}), do: [Observation.GateReleased, Observation.GateReleaseFailed]

  def admissible_observations(%AwaitGate{}) do
    [Observation.GateFinished, Observation.GateFailed, Observation.GateUnsettled, Observation.GateError]
  end

  def admissible_observations(%ReconcileGate{}), do: [Observation.GateReconciled, Observation.GateReconcileFailed]
  def admissible_observations(%Notify{}), do: [Observation.Notified, Observation.NotifyFailed]
  def admissible_observations(%Timer{}), do: [Observation.Deadline]

  def admissible_observations(%RetainPrompt{}) do
    [Observation.PromptRetained, Observation.PromptRetentionFailed]
  end

  def admissible_observations(%FetchPrompt{}) do
    [Observation.PromptFetched, Observation.PromptFetchFailed]
  end
end
