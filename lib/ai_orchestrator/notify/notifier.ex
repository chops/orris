defmodule AiOrchestrator.Notify.Notifier do
  @moduledoc """
  Best-effort operator notification (EJ-11 bracket).

  Invariants enforced here and property-tested:
  - Delivery NEVER gates or alters domain state: both outcomes are plain
    journalable result data, no exception escapes, no retry loop.
  - The payload is bounded before it reaches any hook.
  - The hook is operator-authored argv from the run spec — never model text.
  """

  @payload_byte_cap 2048

  @type request :: %{required(String.t()) => term()}

  @doc "Builds notification_requested event data for a trigger (attention or terminal) event id."
  @spec request_data(String.t(), String.t(), binary()) :: request()
  def request_data(notification_id, trigger_event_id, payload)
      when is_binary(notification_id) and is_binary(trigger_event_id) and is_binary(payload) do
    %{
      "notification_id" => notification_id,
      "trigger_event_id" => trigger_event_id,
      "channel" => "shell_hook",
      "payload_hash" => sha256(bound_payload(payload))
    }
  end

  @doc """
  Executes the operator hook. Returns `{:sent, data}` or `{:failed, data}` —
  both are results to journal, never errors to propagate. Exactly one attempt.
  """
  @spec deliver([String.t()], String.t(), binary(), keyword()) :: {:sent, map()} | {:failed, map()}
  def deliver(hook_argv, notification_id, payload, opts \\ [])

  def deliver([command | args], notification_id, payload, opts) when is_binary(command) do
    runner = Keyword.get(opts, :runner, &System.cmd/3)
    bounded = bound_payload(payload)

    try do
      case runner.(command, args, input: bounded, stderr_to_stdout: true) do
        {_output, 0} -> {:sent, sent_data(notification_id)}
        {output, exit_status} -> {:failed, failed_data(notification_id, "hook_exit_#{exit_status}", output)}
      end
    rescue
      exception -> {:failed, failed_data(notification_id, "hook_raised", Exception.message(exception))}
    end
  end

  def deliver(_hook_argv, notification_id, _payload, _opts) do
    {:failed, failed_data(notification_id, "hook_not_argv", "")}
  end

  @doc "Bounds a payload to the notification byte cap without splitting UTF-8."
  @spec bound_payload(binary()) :: binary()
  def bound_payload(payload) when byte_size(payload) <= @payload_byte_cap, do: payload

  def bound_payload(payload) do
    payload
    |> binary_part(0, @payload_byte_cap)
    |> trim_partial_utf8()
  end

  defp trim_partial_utf8(binary) do
    if String.valid?(binary) do
      binary
    else
      binary |> binary_part(0, byte_size(binary) - 1) |> trim_partial_utf8()
    end
  end

  defp sent_data(notification_id), do: %{"notification_id" => notification_id, "channel" => "shell_hook"}

  defp failed_data(notification_id, reason, detail) do
    %{
      "notification_id" => notification_id,
      "channel" => "shell_hook",
      "reason" => reason,
      "detail" => detail |> bound_payload() |> String.slice(0, 200)
    }
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end
end
