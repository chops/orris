defmodule AiOrchestrator.Journal.ReplayPurityTest do
  @moduledoc """
  NS-02.B.002 control: replay has no side effects, with effect spies.

  The row's acceptance is "replay a golden journal with effect spies" and its failure
  control is "any dispatch/gate/notification during replay fails". At the audited head the
  verdict rested on a whole-file read of `Fold` plus two compile-time boundary facts. This
  installs the spies the row names: every fold entry point runs inside a traced process
  with global call tracing armed on the effect modules and on both filesystem modules, and
  ONE trace message is a failure.

  The reader half is structural rather than observational: `Reader.load/2` runs through a
  `FaultFs` whose every mutating operation is planned to fail, so a read path that wrote
  anything could not succeed.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.Test.FaultFs

  @fixture_root Path.expand("../fixtures/contracts", __DIR__)
  @journal_source Path.expand("../../lib/ai_orchestrator/journal.ex", __DIR__)
  @fold_source Path.expand("../../lib/ai_orchestrator/journal/fold.ex", __DIR__)

  # Every effect the row names, plus both filesystem modules. `Fold` reaching any of these
  # is the failure; they are traced globally so only external calls register.
  @spied_modules [
    AiOrchestrator.Dispatch,
    AiOrchestrator.Gate.Runner,
    AiOrchestrator.Notify.Notifier,
    AiOrchestrator.PaneRegistry,
    File,
    :file,
    :os
  ]

  @mutating_ops ~w(mkdir_p mkdir rm rmdir link chmod open write sync close rename dir_sync)a

  test "folding every fixture journal calls no dispatch, gate, notifier, registry or filesystem function" do
    corpus = corpus()
    assert corpus != [], "the fixture corpus is empty, so this spy would report green over nothing"

    arm_spies()
    folder = spawn(fn -> fold_on_command() end)
    assert :erlang.trace(folder, true, [:call]) == 1
    send(folder, {:fold, self(), corpus})

    assert_receive {:folded, folded}, 60_000
    assert folded == length(corpus)
    # The folding process has already exited; tracing ends with it, and the patterns the
    # spy armed are cleared by `arm_spies/0`'s on_exit. Signals from one process keep their
    # order, so every trace message it caused is already in this mailbox.
    reached = traces()

    assert reached == [],
           """
           NS-02.B.002: replay reached an effect. A fold that dispatches, runs a gate,
           notifies, touches the pane registry or opens a file is not a replay.

           #{Enum.map_join(reached, "\n", &"  #{inspect(&1)}")}
           """
  end

  test "the spy still reports a call made from inside the traced process" do
    # Without this the test above reports the same green against tracing that has stopped
    # working: an armed spy must be shown catching the call it exists to catch.
    arm_spies()
    prober = spawn(fn -> probe_on_command() end)
    assert :erlang.trace(prober, true, [:call]) == 1
    send(prober, {:probe, self()})

    assert_receive {:probed, :ok}, 10_000

    assert Enum.any?(traces(), &match?({:trace, _pid, :call, {File, :exists?, _args}}, &1)),
           "the spy did not report a File call made inside the traced process"
  end

  test "the reader completes with every mutating filesystem operation planned to fail" do
    for dir <- journal_dirs() do
      fs = FaultFs.new()

      for op <- @mutating_ops do
        FaultFs.inject(fs, op, fn _args -> true end, {:error, :replay_is_read_only})
      end

      _loaded = Reader.load(dir, fs: fs)

      attempted = fs |> FaultFs.trace() |> Enum.filter(&(elem(&1, 0) in @mutating_ops))

      assert attempted == [],
             "#{Path.basename(dir)}: the read path attempted #{inspect(attempted)}"
    end
  end

  test "the Journal boundary still depends on the clock and process identity alone" do
    source = File.read!(@journal_source)

    assert source =~
             ~r/use Boundary,\s*deps: \[AiOrchestrator\.Clock, AiOrchestrator\.ProcessIdentity\],/,
           "journal.ex no longer declares deps: [Clock, ProcessIdentity]; a widened boundary is a widened replay"
  end

  test "the fold module aliases nothing but the event schema" do
    aliases =
      @fold_source
      |> File.read!()
      |> String.split("\n")
      |> Enum.filter(&String.starts_with?(String.trim(&1), "alias "))
      |> Enum.map(&String.trim/1)

    assert aliases == ["alias AiOrchestrator.Journal.Event"],
           "fold.ex aliases more than the event schema: #{inspect(aliases)}"
  end

  defp fold_on_command do
    receive do
      {:fold, reply_to, corpus} ->
        folded =
          for {_name, lines, events, views} <- corpus do
            _lines = Fold.fold_lines(lines)
            _events = Fold.fold_events(events)
            _views = Fold.fold_views(views)
            :folded
          end

        send(reply_to, {:folded, length(folded)})
    end
  end

  defp probe_on_command do
    receive do
      {:probe, reply_to} ->
        _exists = File.exists?("/")
        send(reply_to, {:probed, :ok})
    end
  end

  defp arm_spies do
    for module <- @spied_modules, do: Code.ensure_loaded!(module)
    on_exit(fn -> for module <- @spied_modules, do: :erlang.trace_pattern({module, :_, :_}, false, [:global]) end)
    for module <- @spied_modules, do: :erlang.trace_pattern({module, :_, :_}, true, [:global])
    :ok
  end

  defp traces, do: collect_traces([])

  defp collect_traces(acc) do
    receive do
      {:trace, _pid, :call, _mfa} = message -> collect_traces([message | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Decoding and validating happen HERE, in the untraced test process: the spy is armed on
  # the fold itself, not on the reading of the fixtures.
  defp corpus do
    for dir <- journal_dirs() do
      lines = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)
      events = for line <- lines, {:ok, event} <- [Jason.decode(line)], do: event
      views = for line <- lines, {:ok, view} <- [Event.validate_line(line)], do: view
      {Path.basename(dir), lines, events, views}
    end
  end

  defp journal_dirs do
    @fixture_root
    |> Path.join("**/events.jsonl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(&Path.dirname/1)
  end
end
