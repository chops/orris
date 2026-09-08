defmodule AiOrchestrator.Effects.Interrupted do
  @moduledoc """
  The internal carrier for a trappable failure (`:error`, `:throw` or `:exit`) inside effect
  execution: the original kind, reason and stacktrace, and the LATEST runtime at the failing stage.
  The Host settles that runtime and re-raises the original; this exception never leaves the Host.
  It renders payload-free (kind and the runtime's phases only): no reason, stack or handle bytes.
  """

  defexception [:kind, :reason, :stacktrace, :runtime]

  @type t :: %__MODULE__{
          kind: :error | :throw | :exit,
          reason: term(),
          stacktrace: Exception.stacktrace(),
          runtime: term()
        }

  @impl true
  def message(%__MODULE__{kind: kind}),
    do: "effect execution interrupted by #{kind}; the latest runtime is carried for cleanup"

  defimpl Inspect do
    def inspect(%{kind: kind, runtime: runtime}, _opts),
      do:
        "#AiOrchestrator.Effects.Interrupted<kind: " <>
          Kernel.inspect(kind) <> ", runtime: " <> Kernel.inspect(runtime) <> ">"
  end
end
