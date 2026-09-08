defmodule AiOrchestrator.Contract.ArtifactBaseline do
  @moduledoc """
  The closed grammar of an artifact baseline (MUST-7), owned once and used wherever a
  baseline crosses a boundary: the host admitting an adapter's snapshot, the pane adapter
  deciding whether a command carries a durable baseline, and the journal schema (which
  is cross-checked against this module by a contract test, so the two cannot drift).

  Recorded shapes -- the only shapes a producer may append:

      %{"exists" => false}
      %{"exists" => true, "bytes" => n, "mtime_unix" => n, "sha256" => "sha256:" <> 64 hex}

  with no other keys. The read-side view of a version-1 projection carries
  `%{"status" => "unrecorded"}`; that is a claim the journal does not know, never a
  recorded baseline, and it is never appended.
  """

  @hash ~r/\Asha256:[0-9a-f]{64}\z/

  @doc "true when `value` is a recorded baseline in the closed grammar."
  @spec recorded?(term()) :: boolean()
  def recorded?(value), do: validate(value) == :ok

  @doc "true for the explicit read-side claim that no baseline was recorded."
  @spec unrecorded?(term()) :: boolean()
  def unrecorded?(%{"status" => "unrecorded"} = map) when map_size(map) == 1, do: true
  def unrecorded?(_value), do: false

  @doc "Validates a recorded baseline; the error names the class, never the value."
  @spec validate(term()) :: :ok | {:error, %{String.t() => String.t()}}
  def validate(%{"exists" => false} = map) when map_size(map) == 1, do: :ok

  def validate(%{"exists" => true, "bytes" => bytes, "mtime_unix" => mtime, "sha256" => sha} = map)
      when map_size(map) == 4 do
    if count?(bytes) and count?(mtime) and is_binary(sha) and Regex.match?(@hash, sha),
      do: :ok,
      else: {:error, %{"reason" => "artifact_baseline_invalid"}}
  end

  def validate(_value), do: {:error, %{"reason" => "artifact_baseline_invalid"}}

  defp count?(value), do: is_integer(value) and value >= 0
end
