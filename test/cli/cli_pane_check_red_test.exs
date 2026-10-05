defmodule AiOrchestrator.CLIPaneCheckRedTest do
  @moduledoc """
  B3a RED (design r7, D/B3A-RED-DESIGN-r7.org; scope r4): the claim-time daemon check and its operator surface.
  After the pane claim succeeds and before the command runs, one v1 status read per claimed pane goes through the
  configured pane client; here `status/2` delegates to the REAL `PaneClient.status/2` with a `:runner` that serves the
  v1 fixture bytes, so the production decode and typed mapping are exercised. send/reconcile/capabilities are fakes.

  A refusal exits 70 with `{"reason": "pane_claim_refused", "diagnosis": {...}}` on stderr, writes the diagnosis file
  under <pane_registry_root>/diagnoses, makes no Writer activity, delivers nothing and leaves claims balanced.

  Rows: R3.1 dead, R3.2 unregistered, R3.5 daemon unavailable, two binding rows (another pane's echo, and no echo ->
  daemon_unavailable, never dead), R3.4 live holder (persisted, no Writer, no delivery, claims balanced), R3.8a a
  repeated dead refusal stays open with seen_count 2, R3.8b a later healthy check resolves the dead diagnosis, and six
  R3.11 persistence failures, one per :diagnosis_fs primitive at the state where the protocol reaches it (ensure_dir and
  publish_new at a first refusal, list and read at a repeat, replace at a resolving check, remove at a create that
  must evict under :diagnosis_resolved_bound 1; each exit 74 with its typed reason, files preserved). Control R3.7: a
  healthy pane runs normally and writes no diagnosis (green today). R3.6 BASELINE is the existing claim-refusal
  invariant in cli_preflight_red_test.exs P-6 and is not repeated here.
  Expected at this head: exactly fourteen failures (R3.1, R3.2, R3.5, two binding rows, R3.4, R3.8a, R3.8b and the
  six R3.11 rows); the control passes.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  @journal "events.jsonl"
  @v1 Path.expand("../fixtures/contracts/ipc/v1", __DIR__)
  @keys ~w(diagnosis_id holder last_seen_at next_action observed_daemon_state opened_at pane_ref daemon_pane_id
           seen_count status trigger)

  defmodule Witness do
    @moduledoc false
    def start, do: Agent.start(fn -> %{} end, name: __MODULE__)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
    def bump(key), do: Agent.update(__MODULE__, &Map.update(&1, key, 1, fn n -> n + 1 end))
    def count(key), do: Agent.get(__MODULE__, &Map.get(&1, key, 0))
    def put_reply(reply), do: Agent.update(__MODULE__, &Map.put(&1, :reply, reply))
    def reply, do: Agent.get(__MODULE__, &Map.get(&1, :reply))
  end

  # status/2 is the real PaneClient.status/2 behind a runner that serves the current scripted v1 reply
  defmodule CheckingPaneClient do
    @moduledoc false
    alias AiOrchestrator.CLIPaneCheckRedTest.Witness

    def status(pane_ref, opts) do
      Witness.bump(:status)
      {bytes, exit_status} = Witness.reply().(pane_ref)
      PaneClient.status(pane_ref, Keyword.put(opts, :runner, fn _ap, _args, _cmd_opts -> {bytes, exit_status} end))
    end

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def reconcile(pane_ref, message_id, _opts) do
      Witness.bump(:reconcile)

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}
    end

    def send(pane_ref, _prompt, opts) do
      Witness.bump(:send)

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end
  end

  defmodule CountingRegistry do
    @moduledoc false
    alias AiOrchestrator.CLIPaneCheckRedTest.Witness

    defdelegate pane_refs(spec), to: FileRegistry

    def claim(pane_refs, owner, opts) do
      case FileRegistry.claim(pane_refs, owner, opts) do
        {:ok, claim} ->
          Witness.bump(:claim)
          {:ok, claim}

        other ->
          other
      end
    end

    def release(claim) do
      Witness.bump(:release)
      FileRegistry.release(claim)
    end
  end

  # a registry whose claim is held by another live run (the existing rejection shape, file_registry.ex:291-297)
  defmodule LiveHolderRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry

    def claim(_pane_refs, _owner, _opts) do
      owner = %{"run_id" => "run_other", "run_dir" => "/tmp/run_other", "supervisor_instance" => "sup_other"}
      {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_writer", "owner" => owner}}
    end

    def release(_claim), do: :ok
  end

  # a :diagnosis_fs double: the configured primitive fails with its typed reason; every other primitive is the real
  # one, so a fault is reached only at the state where the protocol really uses that primitive
  defmodule FailingFs do
    @moduledoc false
    def with(primitive, reason) do
      :persistent_term.put({__MODULE__, :failing}, {primitive, reason})
      __MODULE__
    end

    defp fail_or(primitive, fun) do
      case :persistent_term.get({__MODULE__, :failing}, nil) do
        {^primitive, reason} -> {:error, reason}
        _other -> fun.()
      end
    end

    def ensure_dir(dir), do: fail_or(:ensure_dir, fn -> local().ensure_dir(dir) end)
    def list(dir), do: fail_or(:list, fn -> local().list(dir) end)
    def read(dir, name), do: fail_or(:read, fn -> local().read(dir, name) end)
    def publish_new(dir, name, bytes), do: fail_or(:publish_new, fn -> local().publish_new(dir, name, bytes) end)
    def replace(dir, name, bytes), do: fail_or(:replace, fn -> local().replace(dir, name, bytes) end)
    def remove(dir, name), do: fail_or(:remove, fn -> local().remove(dir, name) end)

    defp local, do: Module.concat([AiOrchestrator, PaneRegistry, Diagnosis, LocalFs])
  end

  setup do
    case Witness.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> Witness.reset()
    end

    :ok
  end

  defp fresh_dir(name) do
    dir = Path.join(System.tmp_dir!(), "cli-pane-check-#{name}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    File.write!(Path.join(dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    dir
  end

  defp opts(fs, registry_root, extra \\ []) do
    events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifacts = by_assignment(events, "artifact_observed")
    gate_pass = Map.delete(Enum.find(events, &(&1["type"] == "gate_passed"))["data"], "gate_run_id")

    Keyword.merge(
      [
        fs: fs,
        pane_registry: CountingRegistry,
        pane_registry_root: registry_root,
        dispatch: LocalPane,
        dispatch_opts: [
          artifact_reader: fn command -> {:ok, Map.fetch!(artifacts, command["assignment_id"])} end,
          pane_client: CheckingPaneClient,
          test_pid: self()
        ],
        gate_executor: GateDouble,
        gate_helper: GateDouble.helper(),
        gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
        review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
      ],
      extra
    )
  end

  defp by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(&{&1["data"]["assignment_id"], &1["data"]})
  end

  defp registry_root do
    root = Path.join(System.tmp_dir!(), "cli_pane_check_registry_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    root
  end

  defp v1(name, echo), do: @v1 |> Path.join(name) |> File.read!() |> fill(echo)

  defp fill(bytes, echo) do
    bytes
    |> String.replace("<pane_id>", echo)
    |> String.replace("<agent>", "claude")
    |> String.replace("<classifier>", "fingerprint")
    |> String.replace("<state>", "idle")
  end

  defp script(name, exit_status), do: Witness.put_reply(fn pane -> {v1(name, pane), exit_status} end)

  # never raises: a non-JSON stderr (today's behaviour on some paths) becomes an empty map, so rows fail on their own
  # named assertions rather than on a decode crash
  defp stderr_object(stderr) do
    case stderr |> String.trim() |> Jason.decode() do
      {:ok, object} when is_map(object) -> object
      _other -> %{}
    end
  end

  defp diagnosis_files(root) do
    dir = Path.join(root, "diagnoses")
    if File.dir?(dir), do: dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".json")), else: []
  end

  defp refused!(result, fs, root, trigger) do
    assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))
    object = stderr_object(result.stderr)
    assert object["reason"] == "pane_claim_refused"
    diagnosis = object["diagnosis"]
    assert is_map(diagnosis), "the refusal carries a diagnosis object"
    assert diagnosis["trigger"] == trigger
    assert Enum.sort(Map.keys(diagnosis)) == Enum.sort(@keys)
    assert [_file] = diagnosis_files(root)
    assert FaultFs.trace(fs) == [], "no Writer activity for this trigger"
    assert {Witness.count(:send), Witness.count(:reconcile)} == {0, 0}, "nothing delivered"
    assert Witness.count(:claim) == Witness.count(:release), "claims balanced"
    diagnosis
  end

  test "R3.7 control: a live, correctly echoed pane runs normally and writes no diagnosis" do
    root = registry_root()
    script("pane_status.ok.json", 0)
    fs = FaultFs.new()

    result = CLI.run(["run", fresh_dir("healthy")], opts(fs, root))

    assert match?(%{status: 0, stderr: ""}, result), inspect(Map.take(result, [:status, :stderr]))
    assert diagnosis_files(root) == []
  end

  test "R3.1 RED a dead pane is refused at claim time with a dead diagnosis" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    fs = FaultFs.new()

    diagnosis = refused!(CLI.run(["run", fresh_dir("dead")], opts(fs, root)), fs, root, "dead")
    assert diagnosis["next_action"]["code"] == "reattach_pane"
  end

  test "R3.2 RED an unregistered pane is refused with an unregistered diagnosis" do
    root = registry_root()
    script("pane_status.error.pane_not_found.json", 1)
    fs = FaultFs.new()

    refused!(CLI.run(["run", fresh_dir("unregistered")], opts(fs, root)), fs, root, "unregistered")
  end

  test "R3.5 RED an unreadable daemon is refused fail-closed as daemon_unavailable" do
    root = registry_root()
    Witness.put_reply(fn _pane -> {"connection refused\n", 2} end)
    fs = FaultFs.new()

    diagnosis = refused!(CLI.run(["run", fresh_dir("unavailable")], opts(fs, root)), fs, root, "daemon_unavailable")
    assert diagnosis["observed_daemon_state"]["source"] == "unavailable"
    refute Jason.encode!(diagnosis) =~ "connection refused", "no daemon text in the diagnosis"
  end

  test "R3 binding RED a dead reply echoing another pane is daemon_unavailable, never dead" do
    root = registry_root()
    Witness.put_reply(fn _pane -> {v1("pane_status.error.pane_dead.json", "pane_other"), 1} end)
    fs = FaultFs.new()

    refused!(CLI.run(["run", fresh_dir("misbound")], opts(fs, root)), fs, root, "daemon_unavailable")
  end

  test "R3 binding RED a dead reply without a pane echo is daemon_unavailable, never dead" do
    root = registry_root()

    Witness.put_reply(fn pane ->
      decoded = "pane_status.error.pane_dead.json" |> v1(pane) |> Jason.decode!() |> Map.delete("pane_id")
      {Jason.encode!(decoded) <> "\n", 1}
    end)

    fs = FaultFs.new()

    refused!(CLI.run(["run", fresh_dir("unbound")], opts(fs, root)), fs, root, "daemon_unavailable")
  end

  test "R3.4 RED a live-holder refusal opens a persisted live_holder diagnosis" do
    root = registry_root()
    script("pane_status.ok.json", 0)
    fs = FaultFs.new()

    result = CLI.run(["run", fresh_dir("live-holder")], opts(fs, root, pane_registry: LiveHolderRegistry))

    diagnosis = refused!(result, fs, root, "live_holder")
    assert diagnosis["next_action"]["code"] == "wait_for_holder"
    assert diagnosis["holder"]["run_id"] == "run_other"
  end

  test "R3.8a RED a successful claim alone does not resolve a dead diagnosis while status stays dead" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("dead-once")], opts(FaultFs.new(), root))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))

    fs = FaultFs.new()
    second = refused!(CLI.run(["run", fresh_dir("dead-twice")], opts(fs, root)), fs, root, "dead")

    assert second["status"] == "open"
    assert second["seen_count"] == 2
  end

  test "R3.8b RED a later healthy status read resolves the dead diagnosis" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("dead-then-healthy")], opts(FaultFs.new(), root))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))

    script("pane_status.ok.json", 0)
    second = CLI.run(["run", fresh_dir("healthy-after")], opts(FaultFs.new(), root))
    assert match?(%{status: 0}, second), inspect(Map.take(second, [:status]))

    assert [name] = diagnosis_files(root)
    resolved = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert resolved["status"] == "resolved"
    assert resolved["resolved_by"]["check"] == "pane_status_v1"
  end

  # exit 74 with a typed persistence reason, no Writer activity, nothing delivered, claims balanced
  defp unpersisted!(result, fs, reason) do
    assert match?(%{status: 74}, result), inspect(Map.take(result, [:status]))
    assert stderr_object(result.stderr)["persistence"] == %{"ok" => false, "error" => reason}
    assert FaultFs.trace(fs) == [], "no Writer activity"
    assert {Witness.count(:send), Witness.count(:reconcile)} == {0, 0}, "nothing delivered"
    assert Witness.count(:claim) == Witness.count(:release), "claims balanced"
  end

  test "R3.11 RED a first diagnosis whose create fails refuses with exit 74" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("create-fails")],
        opts(fs, root, diagnosis_fs: FailingFs.with(:publish_new, "create_failed"))
      )

    unpersisted!(result, fs, "create_failed")
    assert diagnosis_files(root) == []
  end

  test "R3.11 RED a repeat whose read fails refuses with exit 74 and leaves the diagnosis unchanged" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("repeat-first")], opts(FaultFs.new(), root))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))
    assert [name] = diagnosis_files(root)
    before = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!()

    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("repeat-read-fails")],
        opts(fs, root, diagnosis_fs: FailingFs.with(:read, "file_unreadable"))
      )

    unpersisted!(result, fs, "file_unreadable")
    assert root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() == before
  end

  test "R3.11 RED a resolving update that fails refuses with exit 74 and the diagnosis stays open" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("resolve-first")], opts(FaultFs.new(), root))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))
    assert [name] = diagnosis_files(root)

    script("pane_status.ok.json", 0)
    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("resolve-fails")],
        opts(fs, root, diagnosis_fs: FailingFs.with(:replace, "update_failed"))
      )

    unpersisted!(result, fs, "update_failed")
    decoded = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert decoded["status"] == "open"
  end

  test "R3.11 RED a first diagnosis whose directory cannot be prepared refuses with exit 74" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("ensure-dir-fails")],
        opts(fs, root, diagnosis_fs: FailingFs.with(:ensure_dir, "dir_unavailable"))
      )

    unpersisted!(result, fs, "dir_unavailable")
    assert diagnosis_files(root) == []
  end

  test "R3.11 RED a repeat whose listing fails refuses with exit 74 and leaves the diagnosis unchanged" do
    root = registry_root()
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("list-first")], opts(FaultFs.new(), root))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))
    assert [name] = diagnosis_files(root)
    before = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!()

    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("list-fails")],
        opts(fs, root, diagnosis_fs: FailingFs.with(:list, "list_failed"))
      )

    unpersisted!(result, fs, "list_failed")
    assert diagnosis_files(root) == [name]
    assert root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() == before
  end

  # bound 1: a dead diagnosis is opened, then resolved by a healthy run; the next refusal (another trigger) must evict
  # it before its create, and that removal fails
  test "R3.11 RED a create whose retention removal fails refuses with exit 74 and keeps the resolved file" do
    root = registry_root()
    bound = [diagnosis_resolved_bound: 1]
    script("pane_status.error.pane_dead.json", 1)
    first = CLI.run(["run", fresh_dir("evict-first")], opts(FaultFs.new(), root, bound))
    assert match?(%{status: 70}, first), inspect(Map.take(first, [:status]))
    assert [name] = diagnosis_files(root)

    script("pane_status.ok.json", 0)
    healthy = CLI.run(["run", fresh_dir("evict-resolve")], opts(FaultFs.new(), root, bound))
    assert match?(%{status: 0}, healthy), inspect(Map.take(healthy, [:status]))
    before = root |> Path.join("diagnoses") |> Path.join(name) |> File.read!()

    # the healthy run above delivered, as a healthy run must: the refused run's "nothing delivered" and "claims
    # balanced" witnesses count from here (B3a G4 fix3; hosted run 37272773545 failed only on this carried count)
    Witness.reset()
    script("pane_status.error.pane_not_found.json", 1)
    fs = FaultFs.new()

    result =
      CLI.run(
        ["run", fresh_dir("evict-fails")],
        opts(fs, root, bound ++ [diagnosis_fs: FailingFs.with(:remove, "remove_failed")])
      )

    unpersisted!(result, fs, "remove_failed")
    assert diagnosis_files(root) == [name]
    assert root |> Path.join("diagnoses") |> Path.join(name) |> File.read!() == before
  end
end
