defmodule AiOrchestrator.Contract.Observation do
  @moduledoc "Typed results returned by effect executors to the lifecycle reducer."

  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes

  defmodule Clock do
    @moduledoc false
    @enforce_keys [:read_index, :now]
    defstruct [:read_index, :now]
    @type t :: %__MODULE__{read_index: non_neg_integer(), now: Moment.t()}
  end

  defmodule Dispatched do
    @moduledoc false
    @enforce_keys [:assignment_id, :result, :now]
    defstruct [:assignment_id, :result, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), result: map(), now: Moment.t()}
  end

  defmodule DispatchFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule ArtifactSnapshot do
    @moduledoc false
    @enforce_keys [:assignment_id, :baseline, :now]
    defstruct [:assignment_id, :baseline, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), baseline: map(), now: Moment.t()}
  end

  defmodule ArtifactSnapshotFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule SendReconciled do
    @moduledoc """
    The daemon's answer about a recorded send: one of the five ratified outcomes, and the
    receipt's `delivery_attempt` -- how many physical attempts the daemon has admitted under
    this id. An `absent` with no receipt at all carries 0. The reducer's retry bound reads
    this number, never only its own journal, because the journal can be a crash behind.
    """
    @enforce_keys [:assignment_id, :outcome, :delivery_attempt, :now]
    defstruct [:assignment_id, :outcome, :delivery_attempt, :now]

    @type t :: %__MODULE__{
            assignment_id: String.t(),
            outcome: String.t(),
            delivery_attempt: non_neg_integer(),
            now: Moment.t()
          }
  end

  defmodule SendReconcileFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule ArtifactObserved do
    @moduledoc false
    @enforce_keys [:assignment_id, :artifact, :now]
    defstruct [:assignment_id, :artifact, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), artifact: map(), now: Moment.t()}
  end

  defmodule Blocked do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule Pending do
    @moduledoc false
    @enforce_keys [:assignment_id, :details, :now]
    defstruct [:assignment_id, :details, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), details: map(), now: Moment.t()}
  end

  defmodule ObserveFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule TimedOut do
    @moduledoc false
    @enforce_keys [:assignment_id, :deadline_unix, :now]
    defstruct [:assignment_id, :deadline_unix, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), deadline_unix: integer(), now: Moment.t()}
  end

  defmodule ReviewRead do
    @moduledoc false
    @enforce_keys [:assignment_id, :contents, :now]
    defstruct [:assignment_id, :contents, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), contents: String.t(), now: Moment.t()}
  end

  defmodule ReviewUnreadable do
    @moduledoc false
    @enforce_keys [:assignment_id, :path, :reason, :now]
    defstruct [:assignment_id, :path, :reason, :now]

    @type t :: %__MODULE__{
            assignment_id: String.t(),
            path: String.t(),
            reason: map(),
            now: Moment.t()
          }
  end

  defmodule GateFinished do
    @moduledoc false
    @enforce_keys [:gate_run_id, :result, :now]
    defstruct [:gate_run_id, :result, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), result: map(), now: Moment.t()}
  end

  defmodule GateFailed do
    @moduledoc false
    @enforce_keys [:gate_run_id, :result, :now]
    defstruct [:gate_run_id, :result, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), result: map(), now: Moment.t()}
  end

  defmodule GatePrepared do
    @moduledoc "The complete gate_started v2 payload for a prepared, claimed, still-blocked gate."
    @enforce_keys [:gate_run_id, :attempt, :started, :now]
    defstruct [:gate_run_id, :attempt, :started, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, started: map(), now: Moment.t()}
  end

  defmodule GatePrepareFailed do
    @moduledoc false
    @enforce_keys [:gate_run_id, :attempt, :reason, :now]
    defstruct [:gate_run_id, :attempt, :reason, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, reason: map(), now: Moment.t()}
  end

  defmodule GateReleased do
    @moduledoc false
    @enforce_keys [:gate_run_id, :attempt, :now]
    defstruct [:gate_run_id, :attempt, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, now: Moment.t()}
  end

  defmodule GateReleaseFailed do
    @moduledoc false
    @enforce_keys [:gate_run_id, :attempt, :reason, :now]
    defstruct [:gate_run_id, :attempt, :reason, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, reason: map(), now: Moment.t()}
  end

  defmodule GateUnsettled do
    @moduledoc "Any outcome (exit 0, non-zero, signal, timeout) whose settlement is unproven: attention, never a pass, never a retry while the group may still run."
    @enforce_keys [:gate_run_id, :attempt, :result, :now]
    defstruct [:gate_run_id, :attempt, :result, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, result: map(), now: Moment.t()}
  end

  defmodule GateReconciled do
    @moduledoc "A read-only verdict about a journaled start: dead, unknown, no_claim or orphan_claim."
    @enforce_keys [:gate_run_id, :attempt, :verdict, :facts, :now]
    defstruct [:gate_run_id, :attempt, :verdict, :facts, :now]

    @type t :: %__MODULE__{
            gate_run_id: String.t(),
            attempt: 1..2,
            verdict: :dead | :unknown | :no_claim | :orphan_claim,
            facts: map(),
            now: Moment.t()
          }
  end

  defmodule GateReconcileFailed do
    @moduledoc false
    @enforce_keys [:gate_run_id, :attempt, :reason, :now]
    defstruct [:gate_run_id, :attempt, :reason, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), attempt: 1..2, reason: map(), now: Moment.t()}
  end

  defmodule GateError do
    @moduledoc false
    @enforce_keys [:gate_run_id, :reason, :now]
    defstruct [:gate_run_id, :reason, :now]
    @type t :: %__MODULE__{gate_run_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule Notified do
    @moduledoc false
    @enforce_keys [:notification_id, :result, :now]
    defstruct [:notification_id, :result, :now]
    @type t :: %__MODULE__{notification_id: String.t(), result: map(), now: Moment.t()}
  end

  defmodule NotifyFailed do
    @moduledoc false
    @enforce_keys [:notification_id, :reason, :now]
    defstruct [:notification_id, :reason, :now]
    @type t :: %__MODULE__{notification_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule Deadline do
    @moduledoc false
    @enforce_keys [:purpose, :deadline_unix, :now]
    defstruct [:purpose, :deadline_unix, :now]
    @type t :: %__MODULE__{purpose: String.t(), deadline_unix: integer(), now: Moment.t()}
  end

  defmodule PromptRetained do
    @moduledoc "The object the store published. It names its own assignment."
    @enforce_keys [:object, :now]
    defstruct [:object, :now]
    @type t :: %__MODULE__{object: PromptObject.t(), now: Moment.t()}
  end

  defmodule PromptRetentionFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  defmodule PromptFetched do
    @moduledoc """
    The bytes a fetch returned, together with the object they came from.

    Carrying the object rather than a bare id keeps the correlation exact: the
    reducer learns which object satisfied the fetch, not merely which
    assignment asked, so a substituted object cannot be observed as a
    successful fetch for the assignment it displaced.
    """
    @enforce_keys [:object, :bytes, :now]
    defstruct [:object, :bytes, :now]
    @type t :: %__MODULE__{object: PromptObject.t(), bytes: SensitiveBytes.t(), now: Moment.t()}
  end

  defmodule PromptFetchFailed do
    @moduledoc false
    @enforce_keys [:assignment_id, :reason, :now]
    defstruct [:assignment_id, :reason, :now]
    @type t :: %__MODULE__{assignment_id: String.t(), reason: map(), now: Moment.t()}
  end

  @type t ::
          Clock.t()
          | Dispatched.t()
          | DispatchFailed.t()
          | ArtifactObserved.t()
          | Blocked.t()
          | Pending.t()
          | ObserveFailed.t()
          | TimedOut.t()
          | ReviewRead.t()
          | ReviewUnreadable.t()
          | GateFinished.t()
          | GateFailed.t()
          | GateError.t()
          | GatePrepared.t()
          | GatePrepareFailed.t()
          | GateReleased.t()
          | GateReleaseFailed.t()
          | GateUnsettled.t()
          | GateReconciled.t()
          | GateReconcileFailed.t()
          | Notified.t()
          | NotifyFailed.t()
          | Deadline.t()
          | PromptRetained.t()
          | PromptRetentionFailed.t()
          | PromptFetched.t()
          | PromptFetchFailed.t()
end
