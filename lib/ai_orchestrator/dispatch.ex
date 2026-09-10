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

  @callback reconcile(map(), keyword()) :: {:ok, map()} | {:error, map()}
  @optional_callbacks reconcile: 2

  @doc "The adapter's declared capabilities; LocalPane obtains its live daemon proof here."
  @callback capabilities(keyword()) :: {:ok, [String.t()]} | {:error, map()}

  @doc "Validate declared reconciliation capability before a durable adapter invocation."
  @spec preflight(module(), keyword()) :: :ok | {:error, map()}
  def preflight(adapter, opts \\ []) do
    case adapter.capabilities(opts) do
      {:ok, tokens} when is_list(tokens) ->
        cond do
          not valid_capabilities?(tokens) -> refusal("dispatch_capabilities_invalid")
          "delivery_reconcile" in tokens -> :ok
          true -> unsupported()
        end

      {:error, _reason} ->
        refusal("dispatch_capabilities_failed")

      _other ->
        refusal("dispatch_capabilities_invalid")
    end
  rescue
    error in UndefinedFunctionError ->
      if error.module == adapter and error.function == :capabilities and error.arity == 1,
        do: refusal("dispatch_adapter_cannot_reconcile"),
        else: refusal("dispatch_capabilities_failed")

    _error ->
      refusal("dispatch_capabilities_failed")
  catch
    _kind, _reason -> refusal("dispatch_capabilities_failed")
  end

  @doc false
  @spec valid_capabilities?(term()) :: boolean()
  def valid_capabilities?(tokens) when is_list(tokens), do: Enum.all?(tokens, &capability_token?/1)
  def valid_capabilities?(_tokens), do: false

  # The wire does not pin a naming style. Reject malformed tokens, not future
  # punctuation, case or Unicode spellings the adapter does not yet understand.
  defp capability_token?(token) when is_binary(token),
    do: token != "" and String.valid?(token) and not Regex.match?(~r/[\s\p{Cc}]/u, token)

  defp capability_token?(_token), do: false

  defp refusal(reason), do: {:error, %{"reason" => reason, "detector" => "dispatch_preflight"}}

  defp unsupported do
    {:error,
     %{
       "reason" => "dispatch_preflight_unsupported",
       "detector" => "dispatch_preflight",
       "capability" => "delivery_reconcile",
       "missing_capabilities" => ["delivery_reconcile"]
     }}
  end
end
