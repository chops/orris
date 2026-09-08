defmodule AiOrchestrator.Contract.SendId do
  @moduledoc """
  The message identity a run presents to the daemon's receipt store (NS-42 rule 4).

  The store is global but assignment ids are only run-scoped, so an id derived
  from the assignment alone makes every run holding `as_0001` ask about the same
  receipt. The second run is then answered from the first run's record: a prompt
  that was never sent reads as delivered and the work is silently dropped, or,
  where the payloads differ, an unrelated run reads as a conflict and blocks for
  attention it did not earn. Binding the run into the identity removes the shared
  key rather than arbitrating the collision.

  The id is a pure function of run and assignment because resume has to mint the
  id the pre-crash run used. Anything drawn from a clock or a random source would
  make the receipt unfindable at exactly the moment it decides whether a prompt
  was already delivered.

  The preimage is domain-separated by an ASCII tag and each field carries a
  big-endian `u32` byte length, which makes the encoding injective: no shift of
  the run/assignment boundary can produce a second pair with the same preimage,
  the way a bare concatenation lets `{"run_a", "as_0001"}` and `{"run_", "aas_0001"}`
  agree. The full 64-hex digest is kept rather than truncated, so collision
  resistance stays at least 128 bits.
  """

  @tag "SEND-ID-1\n"

  @doc """
  Mints the send id for one assignment within one run: `snd_` followed by the
  lowercase SHA-256 of the pinned preimage.

  The grammar is a cross-repository contract with the daemon, so it is written
  here once and pinned by `test/contracts/send_id_test.exs` against an
  independent computation.
  """
  @spec mint(String.t(), String.t()) :: String.t()
  def mint(run_id, assignment_id) when is_binary(run_id) and is_binary(assignment_id) do
    digest =
      :sha256
      |> :crypto.hash(preimage(run_id, assignment_id))
      |> Base.encode16(case: :lower)

    "snd_" <> digest
  end

  defp preimage(run_id, assignment_id) do
    @tag <> length_prefixed(run_id) <> length_prefixed(assignment_id)
  end

  defp length_prefixed(field), do: <<byte_size(field)::unsigned-big-32>> <> field
end
