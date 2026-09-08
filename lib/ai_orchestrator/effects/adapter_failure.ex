defmodule AiOrchestrator.Effects.AdapterFailure do
  @moduledoc """
  The carrier for a CLOSED adapter failure reported by an adapter runner (`{:failed, diagnostic}`): the
  diagnostic already holds only kind, result class, digest and stack depth. It is raised under the Host's
  guarded boundary so the owner settles its latest runtime exactly as for a raw failure, then answers the
  diagnostic unchanged (plus the cleanup summary). It never carries a reason, a stack or adapter output.
  """

  defexception [:diagnostic]

  @type t :: %__MODULE__{diagnostic: AiOrchestrator.Effects.AdapterRunner.diagnostic()}

  @impl true
  def message(%__MODULE__{diagnostic: %{kind: kind}}),
    do: "adapter failed under the runner (#{kind}); closed diagnostic carried"

  def message(%__MODULE__{}), do: "adapter failed under the runner; closed diagnostic carried"
end
