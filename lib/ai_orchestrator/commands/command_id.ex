defmodule AiOrchestrator.Commands.CommandId do
  @moduledoc "CSPRNG-backed opaque command identifiers."

  @pattern ~r/^[A-Za-z0-9_-]{16,64}$/

  @spec generate() :: String.t()
  def generate do
    "cmd_" <> (16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower))
  end

  @spec validate(term()) :: {:ok, String.t()} | {:error, %{clause: String.t()}}
  def validate(value) when is_binary(value) do
    if Regex.match?(@pattern, value) do
      {:ok, value}
    else
      {:error, %{clause: "invalid_command_id"}}
    end
  end

  def validate(_value), do: {:error, %{clause: "invalid_command_id"}}
end
