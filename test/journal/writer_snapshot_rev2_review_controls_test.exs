defmodule WriterSnapshotRev2ReviewControlsTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Journal.Fs
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Test.FaultFs

  test "journal fault remains active after receipt fault is installed" do
    fs = FaultFs.new()
    FaultFs.inject(fs, :read, fn [name] -> name == "events.jsonl" end, {:error, :journal_fault})
    assert {:error, %{clause: "journal_unreadable"}} = Reader.load("/unused-review-dir", fs: fs)
    FaultFs.inject(fs, :read, fn [name] -> name == "events.head" end, {:error, :receipt_fault})
    assert {:error, %{clause: "journal_unreadable"}} = Reader.load("/unused-review-dir", fs: fs)
    assert {:error, :receipt_fault} = Fs.read(fs, "/unused-review-dir/events.head")
    assert Enum.take(FaultFs.trace(fs), 2) == [{:read, "events.jsonl"}, {:read, "events.jsonl"}]
  end

  test "an unresolved one-line receipt lag becomes unrecoverable after another line" do
    {lines, _} =
      Enum.map_reduce(1..4, Chain.anchor(), fn seq, prior ->
        line = Jason.encode!(%{"seq" => seq, "schema_version" => 2, "prev_line_sha256" => prior}) <> "\n"
        {line, Chain.line_sha256(line)}
      end)

    {:ok, three} = lines |> Enum.take(3) |> Enum.join() |> Chain.verify()
    {:ok, four} = Chain.verify(Enum.join(lines))
    receipt = %{seq: 2, line_sha256: Chain.line_sha256(Enum.at(lines, 1))}
    assert {:ok, %{action: :advance_receipt}} = Chain.reconcile(three, receipt)
    assert {:error, %{clause: "receipt_stale", receipt_seq: 2, count: 4}} = Chain.reconcile(four, receipt)
  end
end
