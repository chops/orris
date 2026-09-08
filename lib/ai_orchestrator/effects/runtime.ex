defmodule AiOrchestrator.Effects.Runtime do
  @moduledoc """
  The gate handles one Host invocation owns, as an explicit value threaded through the loop.
  Keyed by `{gate_run_id, attempt}`; each entry names its phase (`:prepared` | `:running`) and
  holds the executor's opaque handle (which holds the Port; the Port stays owned by the process
  running the loop). Renders payload-free: phases and identifier-shaped keys only.
  """

  alias AiOrchestrator.Gate.Execution

  @enforce_keys [:executor]
  defstruct executor: Execution, gates: %{}

  @type key :: {String.t(), pos_integer()}
  @typedoc "A retained handle, or (docs/contracts/gate-async-await-proposal.org) the owner-resident await carrying its descriptor."
  @type entry ::
          %{phase: :prepared | :running, handle: term()}
          | %{phase: :awaiting, handle: term(), waiting: term(), effect: struct()}
  @type t :: %__MODULE__{executor: module(), gates: %{optional(key()) => entry()}}

  @identifier ~r/\A[A-Za-z0-9_.-]{1,64}\z/

  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts), do: %__MODULE__{executor: Keyword.get(opts, :gate_executor, Execution), gates: %{}}

  @spec put(t(), key(), :prepared | :running, term()) :: t()
  def put(%__MODULE__{gates: gates} = runtime, key, phase, handle) when phase in [:prepared, :running],
    do: %{runtime | gates: Map.put(gates, key, %{phase: phase, handle: handle})}

  @spec handle(t(), key()) :: term() | nil
  def handle(%__MODULE__{gates: gates}, key), do: get_in(gates, [key, :handle])

  @spec drop(t(), key()) :: t()
  def drop(%__MODULE__{gates: gates} = runtime, key), do: %{runtime | gates: Map.delete(gates, key)}

  @doc "Pure: the retained `:running` entry enters the owner-resident await with its opaque descriptor and effect."
  @spec awaiting(t(), key(), term(), struct()) :: t()
  def awaiting(%__MODULE__{gates: gates} = runtime, key, waiting, effect) when is_struct(effect) do
    %{phase: :running, handle: handle} = Map.fetch!(gates, key)
    %{runtime | gates: Map.put(gates, key, %{phase: :awaiting, handle: handle, waiting: waiting, effect: effect})}
  end

  @doc "Pure: the awaiting entry returns to EXACTLY its retained `:running` handle; the carriers are removed."
  @spec resumed(t(), key()) :: t()
  def resumed(%__MODULE__{gates: gates} = runtime, key) do
    %{phase: :awaiting, handle: handle} = Map.fetch!(gates, key)
    %{runtime | gates: Map.put(gates, key, %{phase: :running, handle: handle})}
  end

  @doc false
  def render_keys(%__MODULE__{gates: gates}) do
    for {{id, attempt}, %{phase: phase}} <- gates do
      shown = if is_binary(id) and Regex.match?(@identifier, id), do: id, else: "<non-identifier>"
      {shown, attempt, phase}
    end
  end

  defimpl Inspect do
    alias AiOrchestrator.Effects.Runtime

    def inspect(runtime, _opts),
      do: "#AiOrchestrator.Effects.Runtime<gates: " <> Kernel.inspect(Runtime.render_keys(runtime)) <> ">"
  end
end
