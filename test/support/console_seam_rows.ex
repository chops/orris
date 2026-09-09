defmodule AiOrchestrator.Test.ConsoleSeamRows do
  @moduledoc """
  Shared test helpers for docs/contracts/public-console-seam.org (test support only): owned fresh fixtures with
  registered cleanup, the kill9 fixture writer with provenance re-anchoring (as test/cli/cli_test.exs), Monitor
  records with live owner processes, and the COUNTING ROWS used both by the feature row F-8 (real `AiOrchestrator.Query`)
  and by the controls C-8 that prove disposable wrong implementations fail them.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Journal.Fold

  @doc "A fresh directory under the project build directory: exclusive creation, random name, cleanup registered."
  @spec fresh(String.t()) :: Path.t()
  def fresh(label) do
    name = "console_seam_#{label}_#{Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)}"
    dir = Path.join(Mix.Project.build_path(), name)
    :ok = File.mkdir(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  @doc "The kill9_resume fixture events file as a string."
  def kill9(file) do
    [File.cwd!(), "test", "fixtures", "contracts", "scenarios", "kill9_resume", file] |> Path.join() |> File.read!()
  end

  @doc "Writes spec.json/plan.json from a scenario into `dir`; returns the independently hashed bytes."
  def write_inputs(dir, scenario) do
    spec = Jason.encode!(F.json("scenarios", scenario, "spec.json"))
    plan = Jason.encode!(F.json("scenarios", scenario, "plan.json"))
    File.write!(Path.join(dir, "spec.json"), spec)
    File.write!(Path.join(dir, "plan.json"), plan)
    %{spec_hash: sha(spec), plan_hash: sha(plan)}
  end

  def sha(bytes), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  @doc "Re-anchors the fixture journal's spec/plan provenance to the inputs present in `dir` (as the CLI test does)."
  def reanchored(lines, dir) do
    spec_hash = sha(File.read!(Path.join(dir, "spec.json")))
    plan_hash = sha(File.read!(Path.join(dir, "plan.json")))

    Enum.map(lines, fn line ->
      event = Jason.decode!(line)

      updated =
        case event do
          %{"type" => type, "data" => data} when type in ["run_created", "run_spec_loaded"] ->
            Map.put(event, "data", Map.put(data, "spec_hash", spec_hash))

          %{"type" => "plan_recorded", "data" => data} ->
            Map.put(event, "data", Map.put(data, "plan_hash", plan_hash))

          other ->
            other
        end

      Jason.encode!(updated)
    end)
  end

  def write_journal(dir, lines), do: File.write!(Path.join(dir, "events.jsonl"), Enum.join(lines, "\n") <> "\n")

  def event_kinds(dir) do
    dir
    |> Path.join("events.jsonl")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!(&1)["type"])
  end

  def state(dir) do
    {:ok, state} = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Fold.fold_lines()
    state
  end

  def run_id(dir), do: Fold.summary(state(dir))["run_id"]

  @doc "A complete Monitor record whose pids are live sleeper processes (cleaned up on exit); `run_dir` is expanded."
  def record(run_dir, run_id, generation \\ 1) do
    pids = for _ <- 1..5, do: spawn(fn -> receive(do: (:never -> :ok)) end)
    on_exit(fn -> Enum.each(pids, &Process.exit(&1, :kill)) end)
    [owner, supervisor, server, writer, worker] = pids

    %{
      run_dir: Path.expand(run_dir),
      run_id: run_id,
      owner: owner,
      supervisor: supervisor,
      server: server,
      writer: writer,
      worker: worker,
      generation: generation
    }
  end

  def registered!(monitor, record) do
    :ok = Monitor.register(monitor, record)
    :sys.get_state(monitor)
    :ok
  end

  @doc "A private Monitor owned by the test through start_supervised! (unique id); stopped with the test."
  def monitor!(label) do
    ExUnit.Callbacks.start_supervised!(%{
      id: {:console_seam_monitor, label, System.unique_integer([:positive])},
      start: {Monitor, :start_link, [[name: nil]]}
    })
  end

  @doc "A Monitor that is already dead and REAPED (its DOWN observed) before use, for the unavailable-lookup case."
  def dead_monitor! do
    {:ok, pid} = Monitor.start_link(name: nil)
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> pid
    after
      5_000 -> flunk("the killed monitor was not reaped")
    end
  end

  @doc """
  THE COUNTING ROWS (contract F-8): `impl.host_view(run_ref, opts)` against private Monitors holding real records.
  Each step is a named assertion; `C-8` asserts which STEP a disposable witness fails.
  1. own entry + one same-root peer + one outside-root entry (path canary) -> 1                 [step :own_present]
  2. own entry ABSENT + two same-root peers (both directories exist) -> 2                        [step :own_absent]
  3. Monitor dead and reaped -> :unknown with a :lookup error                                    [step :unavailable]
  Privacy: the outward view holds no pid, reference or the canary path                           [step :privacy]
  """
  def counting_rows(impl, root, ref, run_id) do
    own = Path.join(root, ref)
    peer = Path.join(root, "peer_#{System.unique_integer([:positive])}")
    peer2 = Path.join(root, "peer2_#{System.unique_integer([:positive])}")
    outside = Path.join(fresh("outside_SECRET_PATH_CANARY"), "run")
    for dir <- [peer, peer2, outside], do: File.mkdir_p!(dir)

    mon = monitor!(:own_present)
    for dir <- [own, peer, outside], do: registered!(mon, record(dir, run_id))
    base = [root: root, budget_ms: 500]
    assert {:ok, %{other_registered_directories: count} = view} = impl.host_view(ref, Keyword.put(base, :monitor, mon))
    assert count == 1, "step :own_present expected 1 (peer only), got #{inspect(count)}"
    rendered = inspect(view, limit: :infinity)
    refute rendered =~ "#PID", "step :privacy pid leaked: #{rendered}"
    refute rendered =~ "#Reference", "step :privacy reference leaked: #{rendered}"
    refute rendered =~ "SECRET_PATH_CANARY", "step :privacy path leaked: #{rendered}"

    mon2 = monitor!(:own_absent)
    for dir <- [peer, peer2], do: registered!(mon2, record(dir, run_id))
    assert {:ok, %{other_registered_directories: count2}} = impl.host_view(ref, Keyword.put(base, :monitor, mon2))
    assert count2 == 2, "step :own_absent expected 2 (nothing subtracted), got #{inspect(count2)}"

    dead = dead_monitor!()

    assert {:ok, %{other_registered_directories: count3, errors: errors}} =
             impl.host_view(ref, Keyword.put(base, :monitor, dead))

    assert count3 == :unknown, "step :unavailable expected :unknown, got #{inspect(count3)}"
    assert Enum.any?(errors, &(&1.leg == :lookup)), "step :unavailable expected a :lookup error, got #{inspect(errors)}"
    :ok
  end

  @doc "Runs the counting rows and returns :ok or {:failed, step_atom} extracted from the assertion message."
  def counting_outcome(impl, root, ref, run_id) do
    counting_rows(impl, root, ref, run_id)
    :ok
  rescue
    e in ExUnit.AssertionError ->
      case Regex.run(~r/step :([a-z_]+)/, Exception.message(e)) do
        [_, step] -> {:failed, String.to_atom(step)}
        nil -> {:failed, :unnamed}
      end
  end
end
