defmodule AiOrchestrator.Test.DerivedLive do
  @moduledoc """
  Derived-live journal inputs (rulings m_1788751885000 and m_1788752018000): a historical kill9 prefix whose ONLY
  change is that every assignment deadline field (`assignment_requested` and `assignment_observation_started`
  `data.deadline_unix`) is moved to one verified future value, so a resumed run's fresh owner arms with the
  retained deadline still ahead under `FixedClock`. Every other line stays byte-identical: ids, phases, leases,
  timestamps and history are untouched, and the exact source -> derived delta is verified and returned. The
  historical fixture bytes are never edited; positive recovery rows call this explicitly and are labelled
  derived-live; the original expired inputs stay in their own explicit D1 witnesses.
  """

  alias AiOrchestrator.Test.FixedClock

  @deadline_types ~w(assignment_requested assignment_observation_started)
  @default_ahead_s 3_600

  @type delta :: [%{seq: pos_integer(), type: String.t(), from: integer(), to: integer()}]

  @doc "The live deadline: the FixedClock base plus a margin, ahead at any fresh owner's arm under FixedClock."
  @spec deadline(pos_integer()) :: integer()
  def deadline(ahead_s \\ @default_ahead_s), do: FixedClock.base_unix() + ahead_s

  @doc "Derive the live prefix: ONLY the deadline fields change; returns the lines and the exact verified delta."
  @spec shift_deadlines([String.t()], integer()) :: {[String.t()], delta()}
  def shift_deadlines(lines, deadline_unix \\ deadline()) when is_list(lines) and is_integer(deadline_unix) do
    {derived, delta} =
      Enum.map_reduce(lines, [], fn raw, acc ->
        case Jason.decode!(raw) do
          %{"type" => type, "seq" => seq, "data" => %{"deadline_unix" => from} = data} = event
          when type in @deadline_types ->
            line = event |> Map.put("data", Map.put(data, "deadline_unix", deadline_unix)) |> Jason.encode!()
            {line, acc ++ [%{seq: seq, type: type, from: from, to: deadline_unix}]}

          _untouched ->
            {raw, acc}
        end
      end)

    verify_delta!(lines, derived, delta, deadline_unix)
    {derived, delta}
  end

  # the exact delta: same length, untouched lines byte-identical, changed lines equal to the source with only
  # the deadline replaced, and at least one deadline actually moved
  defp verify_delta!(source, derived, delta, to) do
    if length(source) != length(derived), do: raise("derived prefix changed its length")
    if delta == [], do: raise("no deadline field found in the source prefix")

    source
    |> Enum.zip(derived)
    |> Enum.each(fn {a, b} ->
      event = Jason.decode!(a)

      cond do
        event["type"] in @deadline_types and Map.has_key?(event["data"], "deadline_unix") ->
          expected = put_in(event, ["data", "deadline_unix"], to)

          expected == Jason.decode!(b) ||
            raise("a deadline-bearing line changed more than its deadline (seq #{event["seq"]})")

        a != b ->
          raise("an untouched line changed (seq #{event["seq"]})")

        true ->
          :ok
      end
    end)

    if Enum.any?(delta, &(&1.to != to)), do: raise("delta target mismatch")
    :ok
  end
end
