defmodule AiOrchestrator.Journal.Chain do
  @moduledoc """
  Pure hash-chain and head-receipt rules for the journal (OPEN-01(a)).

  Chain rule: every envelope version 2 line carries `prev_line_sha256`, the
  SHA-256 of the exact bytes of the previous persisted line including its
  newline; the first line carries `anchor/0`, the hash of the empty string.
  Envelope version 1 journals are legacy: accepted without links, never
  rewritten. A legacy journal may upgrade once, one way, at a resume point:
  the first version 2 line links to the exact bytes of the last version 1
  line, and every later line must be version 2. A version 1 line after a
  version 2 line fails closed.

  Head receipt: after each accepted append the writer publishes `events.head`,
  one JSON line naming the last committed `seq` and that line's hash. The
  write protocol makes exactly these states reachable after a crash:
  complete lines equal `receipt.seq` or `receipt.seq + 1`, plus at most one
  incomplete tail (bytes after the final newline). `reconcile/2` accepts only
  those states and fails closed on every other one.

  No IO here: `verify/1` takes the raw journal bytes, `reconcile/2` takes the
  verified journal and the decoded receipt (or nil), and returns the repair
  plan the writer executes and records in `run_resumed.data.tail_repair`.
  """

  @receipt_schema "ai-orchestrator/journal-head"
  @hash_pattern ~r/^sha256:[0-9a-f]{64}$/

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type receipt :: %{seq: pos_integer(), line_sha256: String.t(), updated_at: String.t()}
  @type verified :: %{
          lines: [binary()],
          count: non_neg_integer(),
          envelope_version: 0 | 1 | 2,
          version_2_from: pos_integer() | nil,
          version_2_count: non_neg_integer(),
          last_line_sha256: String.t(),
          incomplete_tail: binary() | nil
        }
  @type action :: :none | :advance_receipt | :truncate_tail | :advance_and_truncate
  @type plan :: %{
          action: action(),
          truncate_bytes: non_neg_integer(),
          receipt_seq_before: non_neg_integer(),
          receipt_seq_after: non_neg_integer()
        }

  @spec anchor() :: String.t()
  def anchor, do: line_sha256("")

  @spec line_sha256(binary()) :: String.t()
  def line_sha256(bytes) when is_binary(bytes) do
    "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  end

  @spec verify(binary()) :: {:ok, verified()} | {:error, rejection()}
  def verify(raw) when is_binary(raw) do
    {complete, tail} = split_lines(raw)

    with {:ok, verified} <- walk(complete, 1, anchor(), 0, []) do
      {:ok, Map.put(verified, :incomplete_tail, tail)}
    end
  end

  @spec encode_receipt(receipt()) :: binary()
  def encode_receipt(%{seq: seq, line_sha256: hash, updated_at: ts})
      when is_integer(seq) and seq >= 1 and is_binary(hash) and is_binary(ts) do
    Jason.encode!(%{"schema" => @receipt_schema, "seq" => seq, "line_sha256" => hash, "updated_at" => ts}) <>
      "\n"
  end

  @spec decode_receipt(binary()) :: {:ok, receipt()} | {:error, rejection()}
  def decode_receipt(bytes) when is_binary(bytes) do
    case Jason.decode(bytes) do
      {:ok, %{"schema" => @receipt_schema, "seq" => seq, "line_sha256" => hash, "updated_at" => ts} = map}
      when map_size(map) == 4 and is_integer(seq) and seq >= 1 and is_binary(hash) and is_binary(ts) ->
        if Regex.match?(@hash_pattern, hash) do
          {:ok, %{seq: seq, line_sha256: hash, updated_at: ts}}
        else
          {:error, %{clause: "invalid_receipt"}}
        end

      _ ->
        {:error, %{clause: "invalid_receipt"}}
    end
  end

  @spec reconcile(verified(), receipt() | nil) :: {:ok, plan()} | {:error, rejection()}
  def reconcile(%{envelope_version: 1} = verified, nil), do: {:ok, plan(verified, 0, 0)}
  def reconcile(%{envelope_version: 1}, _receipt), do: {:error, %{clause: "receipt_on_legacy_journal"}}

  def reconcile(%{count: count, version_2_count: chained} = verified, nil) do
    if chained >= 2 do
      {:error, %{clause: "receipt_missing", count: count}}
    else
      {:ok, plan(verified, 0, count)}
    end
  end

  def reconcile(%{count: count} = verified, %{seq: seq, line_sha256: hash}) do
    cond do
      seq > count -> {:error, %{clause: "receipt_beyond_tail", receipt_seq: seq, count: count}}
      seq < count - 1 -> {:error, %{clause: "receipt_stale", receipt_seq: seq, count: count}}
      hash_at(verified, seq) != hash -> {:error, %{clause: "receipt_hash_mismatch", receipt_seq: seq}}
      true -> {:ok, plan(verified, seq, count)}
    end
  end

  defp split_lines(raw) do
    parts = String.split(raw, "\n")
    {complete, [last]} = Enum.split(parts, length(parts) - 1)
    {complete, if(last == "", do: nil, else: last)}
  end

  defp walk(lines, index, prev_hash, version, acc) do
    walk(lines, %{index: index, prev_hash: prev_hash, version: version, acc: acc, from: nil})
  end

  defp walk([], %{acc: acc, prev_hash: prev_hash, version: version, from: from}) do
    {:ok,
     %{
       lines: Enum.reverse(acc),
       count: length(acc),
       envelope_version: version,
       version_2_from: from,
       version_2_count: if(from, do: length(acc) - from + 1, else: 0),
       last_line_sha256: prev_hash
     }}
  end

  defp walk([line | rest], state) do
    case Jason.decode(line) do
      {:ok, %{"schema_version" => v} = event} when v in [1, 2] ->
        case link(event, v, line, state) do
          {:ok, next} -> walk(rest, next)
          {:error, _} = error -> error
        end

      {:ok, %{} = event} ->
        {:error, %{clause: "invalid_envelope_version", at_seq: seq_of(event, state.index)}}

      _ ->
        {:error, %{clause: "undecodable_line", at_seq: state.index}}
    end
  end

  defp link(event, v, line, %{index: index, prev_hash: prev_hash, version: version} = state) do
    at_seq = seq_of(event, index)

    cond do
      version == 2 and v == 1 ->
        {:error, %{clause: "mixed_envelope_versions", at_seq: at_seq}}

      v == 2 and Map.get(event, "prev_line_sha256") != prev_hash ->
        {:error, %{clause: "chain_mismatch", at_seq: at_seq}}

      true ->
        {:ok,
         %{
           state
           | index: index + 1,
             prev_hash: line_sha256(line <> "\n"),
             version: v,
             acc: [line | state.acc],
             from: state.from || first_chained(v, index)
         }}
    end
  end

  defp first_chained(2, index), do: index
  defp first_chained(_version, _index), do: nil

  defp seq_of(%{"seq" => seq}, _index) when is_integer(seq), do: seq
  defp seq_of(_event, index), do: index

  defp hash_at(_verified, 0), do: anchor()
  defp hash_at(%{count: count, last_line_sha256: last}, seq) when seq == count, do: last
  defp hash_at(%{lines: lines}, seq), do: line_sha256(Enum.at(lines, seq - 1) <> "\n")

  defp plan(%{incomplete_tail: tail}, before, after_seq) do
    truncate = if tail, do: byte_size(tail), else: 0

    action =
      case {after_seq > before, truncate > 0} do
        {false, false} -> :none
        {true, false} -> :advance_receipt
        {false, true} -> :truncate_tail
        {true, true} -> :advance_and_truncate
      end

    %{action: action, truncate_bytes: truncate, receipt_seq_before: before, receipt_seq_after: after_seq}
  end
end
