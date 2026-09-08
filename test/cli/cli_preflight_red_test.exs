defmodule AiOrchestrator.CLIPreflightRedTest do
  @moduledoc """
  RED/interface for the bounded CLI fresh-start preflight removal (ruling m_1788675821000, NS-43 item 4):
  the admission decision of `run <dir>` is the Writer's exclusive create under the lock through the INJECTED
  filesystem; a `File.exists?/1` preflight outside the Fs seam and the lock may not make it. Rows: P-0 control,
  P-1/P-2 existing empty/nonempty journal refused BY THE WRITER (the Fs trace proves the lock and the create
  attempt happened), P-3/P-4 injected-filesystem discrepancy (the clause is the Writer's verbatim), P-5 the
  diagnostic precedence that the removal implies (inputs are read before the Writer decides), disclosed for a ruling.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  @journal "events.jsonl"

  defp fresh_dir(name) do
    dir = Path.join(System.tmp_dir!(), "cli-preflight-#{name}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp with_inputs(dir) do
    File.write!(Path.join(dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    dir
  end

  # the CLI test's own option shape (LocalPane with a fake pane client, GateDouble, fixture-fed readers)
  defp opts(fs, registry \\ __MODULE__.CountingRegistry) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")
    registry_root = Path.join(System.tmp_dir!(), "cli_preflight_registry_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(registry_root) end)

    [
      fs: fs,
      pane_registry: registry,
      pane_registry_root: registry_root,
      dispatch: LocalPane,
      dispatch_opts: [
        artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
        pane_client: __MODULE__.FakePaneClient,
        test_pid: self()
      ],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  defp fixture_data(events, type), do: Enum.find(events, &(&1["type"] == type))["data"]

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(&{&1["data"]["assignment_id"], &1["data"]})
  end

  # a cross-process witness: every delivery/reconcile and every registry claim/release is counted here, whichever
  # process performs it (the Server, the executor owner, the CLI process), so "journal unchanged" is never the only
  # evidence of "nothing was sent" or "claims balanced"
  defmodule Witness do
    @moduledoc false
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
    def bump(key), do: Agent.update(__MODULE__, &Map.update(&1, key, 1, fn n -> n + 1 end))
    def count(key), do: Agent.get(__MODULE__, &Map.get(&1, key, 0))
  end

  defmodule FakePaneClient do
    @moduledoc false
    def reconcile(pane_ref, message_id, _opts) do
      Witness.bump(:reconcile)

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}
    end

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts) do
      Witness.bump(:send)

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  # the real registry, counted (claims and releases go through FileRegistry itself)
  defmodule CountingRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry

    def claim(pane_refs, owner, opts) do
      case FileRegistry.claim(pane_refs, owner, opts) do
        {:ok, claim} ->
          Witness.bump(:claim)
          {:ok, claim}

        other ->
          Witness.bump(:claim_refused)
          other
      end
    end

    def release(claim) do
      Witness.bump(:release)
      FileRegistry.release(claim)
    end
  end

  # a registry that refuses every claim: the precedence witness (claim refusal happens BEFORE Writer admission)
  defmodule RefusingRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry

    def claim(_pane_refs, _owner, _opts) do
      Witness.bump(:claim_refused)
      {:error, %{"reason" => "pane_claim_refused", "pane_ref" => "pane_writer"}}
    end

    def release(_claim), do: :ok
  end

  setup do
    case Witness.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> Witness.reset()
    end

    :ok
  end

  # the Writer's own evidence, ORDERED: the lock link happens strictly before the exclusive create is attempted,
  # both through the injected Fs
  defp writer_decided?(fs) do
    trace = FaultFs.trace(fs)
    lock_at = Enum.find_index(trace, &match?({:link, _tmp, "run.lock." <> _}, &1))

    create_at =
      Enum.find_index(trace, fn
        {:open, @journal, modes} -> :exclusive in modes
        _ -> false
      end)

    is_integer(lock_at) and is_integer(create_at) and lock_at < create_at
  end

  defp no_delivery!, do: assert({Witness.count(:send), Witness.count(:reconcile)} == {0, 0}, "no send/reconcile activity")
  defp claims_balanced!, do: assert(Witness.count(:claim) == Witness.count(:release), "every acquired claim released")

  defp reason(stderr), do: stderr |> String.trim() |> Jason.decode!() |> Map.get("reason")

  test "P-0 control: a fresh start succeeds and the journal is created by the Writer's exclusive create" do
    dir = "fresh" |> fresh_dir() |> with_inputs()
    fs = FaultFs.new()
    assert %{status: 0, stdout: stdout, stderr: ""} = CLI.run(["run", dir], opts(fs))
    assert stdout =~ "* Status: completed"
    assert writer_decided?(fs)
    assert File.exists?(Path.join(dir, @journal))
    assert Witness.count(:send) >= 1 and Witness.count(:reconcile) >= 1, "the delivery witness observes the real run"
    assert Witness.count(:claim) == 1 and Witness.count(:release) == 1, "the counted registry observes the claim"
  end

  for {label, bytes} <- [{"EMPTY", ""}, {"NONEMPTY", nil}] do
    test "P-#{if bytes == "", do: 1, else: 2} an existing #{label} journal is refused BY THE WRITER under the lock; bytes unchanged; no summary" do
      dir = "existing" |> fresh_dir() |> with_inputs()

      bytes = unquote(bytes) || File.read!("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")

      File.write!(Path.join(dir, @journal), bytes)
      fs = FaultFs.new()
      assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", dir], opts(fs))
      assert reason(stderr) == "journal_exists"
      assert File.read!(Path.join(dir, @journal)) == bytes, "nothing appended"
      refute File.exists?(Path.join(dir, "run-summary.org"))
      assert writer_decided?(fs), "the refusal must be the Writer's exclusive create under the lock, not File.exists?"
      no_delivery!()
      claims_balanced!()
    end
  end

  # CONTROL (green today, measured): File.exists? says absent so the preflight passes; the injected filesystem refuses
  # the exclusive create (eexist), the Writer's single retry opens the existing journal through the SAME injected
  # filesystem and finds none: the CLI reports that Writer verdict verbatim (journal_missing). The injected
  # filesystem decided both attempts; File.exists? decided nothing.
  test "P-3 discrepancy: the real file is ABSENT but the injected filesystem refuses the exclusive create -> the Writer's retry verdict, nothing created" do
    dir = "absent-but-refused" |> fresh_dir() |> with_inputs()
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :open,
      fn
        [@journal, modes] -> :exclusive in modes
        _ -> false
      end,
      {:error, :eexist}
    )

    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", dir], opts(fs))
    assert reason(stderr) == "journal_missing", "the injected filesystem's answers decide (create refused, then absent)"
    refute File.exists?(Path.join(dir, @journal)), "no journal is created behind the injected filesystem's back"
    refute File.exists?(Path.join(dir, "run-summary.org"))
    no_delivery!()
    claims_balanced!()
  end

  test "P-4 discrepancy: the real file is PRESENT but the injected filesystem answers eacces -> the Writer's own clause, verbatim" do
    dir = "present-but-eacces" |> fresh_dir() |> with_inputs()
    File.write!(Path.join(dir, @journal), "")
    fs = FaultFs.new()

    FaultFs.inject(
      fs,
      :open,
      fn
        [@journal, modes] -> :exclusive in modes
        _ -> false
      end,
      {:error, :eacces}
    )

    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", dir], opts(fs))

    assert reason(stderr) == "journal_create_failed",
           "File.exists? may not answer journal_exists before the Writer speaks"

    assert File.read!(Path.join(dir, @journal)) == ""
    assert writer_decided?(fs)
    no_delivery!()
    claims_balanced!()
  end

  # DISCLOSED precedence change for a ruling: with the preflight gone, the inputs are read (and hashed) before the
  # Writer decides, so a present journal next to unreadable inputs reports the input failure, not journal_exists.
  test "P-5 precedence (disclosed): a present journal next to a missing spec.json reports the input failure first" do
    dir = fresh_dir("precedence")
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    File.write!(Path.join(dir, @journal), "")
    fs = FaultFs.new()
    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", dir], opts(fs))
    assert reason(stderr) == "file_not_found"
    assert FaultFs.trace(fs) == [], "no Writer activity before the inputs are valid"
    assert File.read!(Path.join(dir, @journal)) == ""
    no_delivery!()
    assert Witness.count(:claim) == 0, "no pane claim before the inputs are valid"
  end

  # RULED (m_1788676500000): the pane-registry claim precedes Writer admission on this path, so an existing journal
  # plus a claim refusal reports the claim refusal, with no Writer activity and nothing delivered - the second
  # diagnostic consequence of the removal, recorded and pinned rather than hidden.
  test "P-6 precedence: a refused pane claim next to an existing journal reports the claim refusal before the Writer" do
    dir = "claim-refused" |> fresh_dir() |> with_inputs()
    File.write!(Path.join(dir, @journal), "")
    fs = FaultFs.new()
    assert %{status: 70, stdout: "", stderr: stderr} = CLI.run(["run", dir], opts(fs, __MODULE__.RefusingRegistry))
    assert reason(stderr) == "pane_claim_refused"
    assert Witness.count(:claim_refused) == 1
    assert FaultFs.trace(fs) == [], "no Writer activity after a refused claim"
    assert File.read!(Path.join(dir, @journal)) == ""
    no_delivery!()
  end
end
