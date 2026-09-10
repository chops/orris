defmodule AiOrchestrator.Test.ScriptedDispatchReceipt do
  @moduledoc """
  Receipt history for synthetic, direct-call dispatch fixtures. It records the
  fixture's actual return rather than inventing an absent answer after delivery.
  History is process-local: another callback process, or a new process after a
  restart, answers ambiguous. This helper never claims cross-process durability
  and never answers absent. Transport/restart tests use their own receipt models.
  """

  defmacro __using__(_opts) do
    quote do
      if AiOrchestrator.Dispatch in Module.get_attribute(__MODULE__, :behaviour) do
        @impl true
      end

      def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

      if AiOrchestrator.Dispatch in Module.get_attribute(__MODULE__, :behaviour) do
        @impl true
      end

      def reconcile(command, _opts), do: AiOrchestrator.Test.ScriptedDispatchReceipt.reconcile(__MODULE__, command)
      @before_compile AiOrchestrator.Test.ScriptedDispatchReceipt
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      defoverridable deliver: 2

      if AiOrchestrator.Dispatch in Module.get_attribute(__MODULE__, :behaviour) do
        @impl true
      end

      def deliver(command, opts) do
        AiOrchestrator.Test.ScriptedDispatchReceipt.run(__MODULE__, command, fn -> super(command, opts) end)
      end
    end
  end

  def run(module, command, invoke) do
    # Until the synthetic call returns successfully it may have acted. Keeping
    # invocation here also supports intentionally non-returning error fixtures.
    record(module, command, :ambiguous)
    result = invoke.()
    record(module, command, result)
    result
  end

  def record(module, command, result) do
    status =
      case result do
        {:ok, %{"send_status" => "queued"}} -> "queued"
        {:ok, %{"send_status" => status}} when status in ["ok", "reconciled"] -> "delivered"
        _ -> "ambiguous"
      end

    key = {__MODULE__, module, command["send_message_id"]}
    Process.put(key, {command["pane_ref"], command["payload_hash"], status})
    :ok
  end

  def reconcile(module, command) do
    pane = command["pane_ref"]
    hash = command["payload_hash"]

    outcome =
      case Process.get({__MODULE__, module, command["send_message_id"]}) do
        {^pane, ^hash, status} -> status
        nil -> "ambiguous"
        _mismatch -> "conflict"
      end

    {:ok, %{"outcome" => outcome, "delivery_attempt" => 1}}
  end
end
