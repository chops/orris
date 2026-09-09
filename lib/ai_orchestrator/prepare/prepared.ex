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

  # ---- the trusted tier's builder and readers: the ONLY code that expands the opaque term ----

  @doc false
  @spec new(map()) :: t()
  def new(%{} = fields), do: struct!(__MODULE__, fields)

  @spec prepared?(term()) :: boolean()
  def prepared?(%__MODULE__{}), do: true
  def prepared?(_other), do: false

  @spec verb(t()) :: String.t()
  def verb(%__MODULE__{verb: verb}), do: verb

  @spec args(t()) :: map()
  def args(%__MODULE__{args: args}), do: args

  @spec run_id(t()) :: String.t()
  def run_id(%__MODULE__{run_id: run_id}), do: run_id

  @spec run_dir(t()) :: Path.t()
  def run_dir(%__MODULE__{run_dir: run_dir}), do: run_dir

  @spec context(t()) :: keyword()
  def context(%__MODULE__{context: context}), do: context

  @spec claims(t()) :: :none | {:panes, map()}
  def claims(%__MODULE__{claims: claims}), do: claims
end
