defmodule AiOrchestrator.Dispatch do
  @moduledoc false

  use Boundary,
    deps: [AiOrchestrator.Config, AiOrchestrator.Contract, AiOrchestrator.Journal],
    exports: [LocalPane, PaneClient, PromptStore]

  # MUST-7: the artifact baseline is taken before the projection is committed, never by
  # deliver on its own account for a projected assignment. Required, not optional: an
  # adapter that cannot snapshot cannot be trusted with a baseline it did not take.
  @callback snapshot(map(), keyword()) :: {:ok, map()} | {:error, map()}
  @callback deliver(map(), keyword()) :: {:ok, map()} | {:error, map()}
  @callback observe(map(), keyword()) :: {:ok, map()} | {:blocked, map()} | {:pending, map()} | {:error, map()}

  # R1: delivery idempotency is part of dispatch semantics, not an optional adapter
  # feature. An adapter that cannot ask what became of a send cannot be resumed against,
  # because every resume would have to assume the prompt was never sent.
  @callback reconcile(map(), keyword()) :: {:ok, map()} | {:error, map()}
  # Optional at the behaviour so a test double or a read-only adapter still compiles;
  # preflight/1 is where an adapter that cannot reconcile is refused, by name.
  @optional_callbacks reconcile: 2

  @doc """
  D3: capability discovery is a named step with a named refusal, run before an adapter is
  asked to do anything, so the reducer never discovers mid-conversation that it is holding
  a half-finished dispatch it has no rule for.
  """
  @spec preflight(module()) :: :ok | {:error, map()}
  def preflight(adapter) when is_atom(adapter) do
    if Code.ensure_loaded?(adapter) and function_exported?(adapter, :reconcile, 2) do
      :ok
    else
      {:error, %{"reason" => "dispatch_adapter_cannot_reconcile"}}
    end
  end
end
