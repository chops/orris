defmodule AiOrchestrator.Journal.Reader do
  @moduledoc """
  Read-only, chain-verified view of a run journal.

  Every reader of `events.jsonl` (status, list, the writer on open) goes
  through `load/2`, which verifies the hash chain and reconciles the head
  receipt with `AiOrchestrator.Journal.Chain` and never writes. A journal in
  a state the write protocol cannot produce fails closed with the named
  clause; a torn tail or a receipt one line behind is reported as
  `pending_repair` for the writer to execute. Every complete line is
  validated through `AiOrchestrator.Journal.Event.validate_line/1` before a
  repair is even planned, so no truncate or receipt advance ever runs on a
  journal the fold would reject.
  """

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Fs.SystemFs

  @journal "events.jsonl"
  @head "events.head"

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}
  @type loaded :: %{
          lines: [binary()],
          last_seq: non_neg_integer(),
          envelope_version: 0 | 1 | 2,
          version_2_from: pos_integer() | nil,
          verified: Chain.verified(),
          receipt: Chain.receipt() | nil,
          pending_repair: Chain.plan() | nil
        }

  @spec load(Path.t(), keyword()) :: {:ok, loaded()} | {:error, rejection()}
  def load(run_dir, opts \\ []) do
    fs = Keyword.get(opts, :fs, SystemFs.new())

    with {:ok, bytes} <- read_journal(fs, run_dir),
         {:ok, verified} <- Chain.verify(bytes),
         :ok <- validate_lines(verified.lines),
         {:ok, receipt} <- read_receipt(fs, run_dir),
         {:ok, plan} <- Chain.reconcile(verified, receipt) do
      {:ok,
       %{
         lines: verified.lines,
         last_seq: verified.count,
         envelope_version: verified.envelope_version,
         version_2_from: verified.version_2_from,
         verified: verified,
         receipt: receipt,
         pending_repair: if(plan.action == :none, do: nil, else: plan)
       }}
    end
  end

  defp validate_lines(lines) do
    lines
    |> Enum.with_index(1)
    |> Enum.reduce_while(:ok, fn {line, seq}, :ok ->
      case Event.validate_line(line) do
        {:ok, _event} -> {:cont, :ok}
        {:error, rejection} -> {:halt, {:error, Map.put_new(rejection, :at_seq, seq)}}
      end
    end)
  end

  defp read_journal(fs, run_dir) do
    case Fs.read(fs, Path.join(run_dir, @journal)) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, :enoent} -> {:error, %{clause: "journal_missing"}}
      {:error, reason} -> {:error, %{clause: "journal_unreadable", detail: inspect(reason)}}
    end
  end

  defp read_receipt(fs, run_dir) do
    case Fs.read(fs, Path.join(run_dir, @head)) do
      {:ok, bytes} -> Chain.decode_receipt(bytes)
      {:error, :enoent} -> {:ok, nil}
      {:error, reason} -> {:error, %{clause: "receipt_unreadable", detail: inspect(reason)}}
    end
  end
end
