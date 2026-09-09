defmodule AiOrchestrator.Prepare.Prepared do
  @moduledoc """
  An admitted, not yet executed command: the verb, its PUBLIC argument document, the run identity, the resolved
  directory, the executor context (server seams and input bindings) and the pane claims to take around the
  invocation. Opaque to consumers: they never build or expand it (docs/contracts/public-console-seam.org).
  """

  @opaque t :: %__MODULE__{
            verb: String.t(),
            args: map(),
            run_id: String.t(),
            run_dir: Path.t(),
            context: keyword(),
            claims: :none | {:panes, map()},
            inputs: %{spec_hash: String.t(), plan_hash: String.t()} | nil
          }

  @enforce_keys [:verb, :args, :run_id, :run_dir, :context, :claims]
  defstruct verb: nil, args: %{}, run_id: nil, run_dir: nil, context: [], claims: :none, inputs: nil
end
