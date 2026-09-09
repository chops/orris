defmodule AiOrchestrator.Contracts.PublicConsoleSeamControlsTest do
  @moduledoc """
  docs/contracts/public-console-seam.org, CONTROLS C-0..C-7: they hold at 2c33d78 and must keep holding. They validate the
  disposable consumer harness, the private-reference negatives, Policy, Reader semantics, CLI outputs and the claim lifetime.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Reader
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.ConsoleConsumerHarness, as: Harness
  alias AiOrchestrator.Test.GateDouble

  @moduletag :public_console_seam
  @moduletag timeout: 600_000

  setup_all do
    dir = Harness.build!()
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, consumer: dir}
  end

  test "C-0 the consumer harness is valid: a public-only reference compiles under WAE", %{consumer: dir} do
    {exit, out} = Harness.compile(dir, "defmodule ConsoleConsumer.Probe do\n  def v, do: AiOrchestrator.version()\nend\n")
    assert exit == 0, out
  end

  for {row, module, call} <- [
        {"C-1", "AiOrchestrator.Journal.Writer", "&AiOrchestrator.Journal.Writer.start_link/1"},
        {"C-2", "AiOrchestrator.Run.Executor", "&AiOrchestrator.Run.Executor.prepare/2"},
        {"C-3", "AiOrchestrator.Host", "&AiOrchestrator.Host.stop/2"}
      ] do
    test "#{row} a private reference to #{module} is rejected under WAE with the forbidden-reference diagnostic", %{
      consumer: dir
    } do
      {exit, out} = Harness.compile(dir, "defmodule ConsoleConsumer.Probe do\n  def p, do: #{unquote(call)}\nend\n")
      assert exit == 1, out
      assert out =~ "forbidden reference to #{unquote(module)}", out
    end
  end

  test "C-4 the console actor class is admitted for the three verbs; an agent actor is refused" do
    console = %{"class" => "console", "id" => "session_abc"}
    for verb <- ~w(start resume cancel), do: assert({:ok, _} = Policy.authorize(console, verb))
    agent = %{"class" => "agent", "id" => "agent_1", "run_id" => "run_x", "assignment_id" => "as_1"}
    for verb <- ~w(start resume cancel), do: assert({:error, %{clause: _}} = Policy.authorize(agent, verb))
  end

  test "C-5 Reader: a torn tail loads with pending_repair and unchanged bytes; a hard-invalid journal fails closed" do
    dir = tmp("reader")
    lines = F.lines("scenarios", "gated_run_seed")
    torn = Enum.join(lines, "\n") <> "\n" <> String.slice(List.last(lines), 0, 20)
    File.write!(Path.join(dir, "events.jsonl"), torn)
    before = :crypto.hash(:sha256, File.read!(Path.join(dir, "events.jsonl")))
    assert {:ok, %{pending_repair: plan, lines: verified}} = Reader.load(dir)
    assert plan != nil and length(verified) == length(lines)
    assert before == :crypto.hash(:sha256, File.read!(Path.join(dir, "events.jsonl")))
    bad = tmp("reader_bad")
    File.write!(Path.join(bad, "events.jsonl"), "not-json\n")
    assert {:error, %{clause: _}} = Reader.load(bad)
  end

  test "C-6 CLI preservation: absolute and relative run directories give the exact outputs" do
    dir = tmp("cli")
    File.write!(Path.join(dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    assert CLI.run(["validate", dir]) == %{status: 0, stdout: "valid\n", stderr: ""}
    relative = Path.relative_to(dir, File.cwd!())
    assert relative != dir
    assert CLI.run(["validate", relative]) == %{status: 0, stdout: "valid\n", stderr: ""}
    completed = tmp("cli_status")
    File.write!(Path.join(completed, "events.jsonl"), Enum.join(F.lines("scenarios", "gated_run_seed"), "\n") <> "\n")
    assert %{status: 0, stdout: json, stderr: ""} = CLI.run(["status", "--json", completed])
    assert Jason.decode!(json) == F.json("scenarios", "gated_run_seed", "expected.json")

    assert %{status: 0, stdout: ^json, stderr: ""} =
             CLI.run(["status", "--json", Path.relative_to(completed, File.cwd!())])

    cancel = tmp("cli_cancel")

    File.write!(
      Path.join(cancel, "events.jsonl"),
      kill9("events_pre_dispatch.jsonl")
    )

    assert %{status: 0, stdout: out, stderr: ""} = CLI.run(["cancel", Path.relative_to(cancel, File.cwd!())])
    assert out =~ "* Status: cancelled" and File.exists?(Path.join(cancel, "run-summary.org"))
    assert %{status: 66, stdout: "", stderr: _} = CLI.run(["status", "--json", tmp("cli_missing")])
  end

  defmodule ObservingRegistry do
    @moduledoc false
    def pane_refs(spec), do: FileRegistry.pane_refs(spec)

    def claim(pane_refs, _owner, opts) do
      {:ok,
       %{
         root: Keyword.fetch!(opts, :root),
         token: "observing",
         pane_refs: pane_refs,
         test: Keyword.fetch!(opts, :test_pid),
         run_dir: Keyword.fetch!(opts, :run_dir)
       }}
    end

    # release observes whether the outcome (projection files) already exists, then fails as the legacy double does
    def release(%{test: test, run_dir: run_dir}) do
      send(test, {:released, File.exists?(Path.join(run_dir, "run-summary.org"))})
      {:error, %{"reason" => "pane_claim_release_failed"}}
    end
  end

  defmodule FakePaneClient do
    @moduledoc false
    def reconcile(pane_ref, message_id, _opts),
      do:
        {:ok,
         %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts),
      do:
        {:ok,
         %{
           "ok" => true,
           "protocol_version" => 2,
           "status" => "sent",
           "msg_id" => opts[:message_id],
           "pane_id" => pane_ref
         }}

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  test "C-7 claim lifetime: the outcome precedes release; a release failure keeps the legacy exit 70 with projections written" do
    dir = tmp("claim")
    File.write!(Path.join(dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    by_assignment =
      events |> Enum.filter(&(&1["type"] == "artifact_observed")) |> Map.new(&{&1["data"]["assignment_id"], &1["data"]})

    gate_pass = events |> Enum.find(&(&1["type"] == "gate_passed")) |> Map.fetch!("data") |> Map.delete("gate_run_id")

    result =
      CLI.run(["run", dir],
        pane_registry: ObservingRegistry,
        pane_registry_opts: [test_pid: self(), run_dir: dir],
        pane_registry_root: tmp("registry"),
        dispatch: AiOrchestrator.Dispatch.LocalPane,
        dispatch_opts: [
          artifact_reader: fn command -> {:ok, Map.fetch!(by_assignment, command["assignment_id"])} end,
          pane_client: FakePaneClient,
          test_pid: self()
        ],
        gate_executor: GateDouble,
        gate_helper: GateDouble.helper(),
        gate_opts: [runner: fn _gate, _opts -> {:ok, gate_pass} end],
        review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
      )

    assert_receive {:released, projections_existed_at_release}, 60_000
    assert projections_existed_at_release, "release ran before the outcome wrote the projections"
    refute_receive {:released, _}, 200
    assert %{status: 70, stderr: stderr} = result
    assert stderr =~ "pane_claim_release_failed"
    assert File.exists?(Path.join(dir, "run-summary.org"))
  end

  defp kill9(file) do
    [File.cwd!(), "test", "fixtures", "contracts", "scenarios", "kill9_resume", file] |> Path.join() |> File.read!()
  end

  defp tmp(name) do
    dir = Path.join(Mix.Project.build_path(), "console_seam_#{name}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end
end
