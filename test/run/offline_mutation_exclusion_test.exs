defmodule AiOrchestrator.Run.OfflineMutationExclusionTest do
  @moduledoc """
  NS-02.B.001's failure control is "Second writer/offline mutation while owner live refused". The
  acquisition half is delivered and covered; this file closes the two residues a read of
  `lib/ai_orchestrator/run/recovery.ex` exposes.

  1. APPENDS NOTHING. The delivered refusal rows assert the clause, the lock files and the arbiter
     state (test/run/recovery_acquisition_red_test.exs Q-1, Q-2), and the CLI rows assert the bytes
     for the three CLI verbs (test/cli/cli_test.exs, "a held run lock refuses run, resume, and
     cancel by name and changes nothing"). No row asserts that the offline ACQUISITION path leaves
     the run directory byte-identical. A refusal that had already appended, truncated or published a
     receipt would satisfy every delivered row.

  2. THE SAME PRIMITIVE. `Recovery.acquire/2` is refused under a live owner because it goes through
     `Writer.open` (recovery.ex:78) and therefore through `Ownership` and `RunLock`. Nothing enforces
     that. A future edit could take a lock, read a lock file, or consult the arbiter on its own
     account and every behavioural row above would still pass, because the refusal would still
     arrive. The structural row pins the module's whole reach instead: exactly two `AiOrchestrator`
     modules, both in `Journal`, and no direct Erlang filesystem call.

  Recorded scope: the refusals are `journal_unavailable` with stage `ownership` or `lock`, not the
  bare `second_live_writer` / `run_locked` the audit named; `Recovery` maps the seam clauses into its
  own closed domain (recovery.ex:269-283) and never echoes the seam's.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Journal.RunLock
  alias AiOrchestrator.Journal.Writer
  alias AiOrchestrator.Run.Recovery

  @fixture Path.expand("test/fixtures/contracts/scenarios/kill9_resume/events_pre_dispatch.jsonl")
  @corpus @fixture |> File.read!() |> String.split("\n", trim: true) |> Enum.take(6)
  @none %{lock: :none, registration: :none, descriptor: :none}
  @source "lib/ai_orchestrator/run/recovery.ex"
  @effectful [File, IO, Port, System]
  @reachable [AiOrchestrator.Journal.Reader, Writer]

  setup do
    Process.flag(:trap_exit, true)
    dir = Path.join(System.tmp_dir!(), "offline_exclusion_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "events.jsonl"), Enum.join(@corpus, "\n") <> "\n")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp lock_opts(overrides \\ []), do: Keyword.merge([supervisor_instance: "sup_offline"], overrides)

  defp live_opts(overrides \\ []),
    do: Keyword.merge(lock_opts(pid: "41001", pid_start: "start_41001", owner_status: fn _ -> :live end), overrides)

  # every byte and every name the run directory carries, so an append, a truncation, a receipt or a
  # lock file created by a refused attempt is visible
  defp directory_state(dir) do
    dir
    |> File.ls!()
    |> Enum.sort()
    |> Map.new(fn name ->
      path = Path.join(dir, name)
      {name, if(File.regular?(path), do: :crypto.hash(:sha256, File.read!(path)), else: :directory)}
    end)
  end

  test "an offline acquisition under a live in-BEAM writer is refused at ownership and appends nothing", %{dir: dir} do
    {:ok, writer, _opened} = Writer.open(dir, lock: live_opts())
    before = directory_state(dir)

    assert Recovery.acquire(dir, lock: live_opts(supervisor_instance: "sup_offline_2")) ==
             {:error, %{clause: "journal_unavailable", stage: "ownership", cleanup: @none}}

    assert directory_state(dir) == before, "a refused offline acquisition changed the run directory"
    assert :ok = Writer.close(writer)
  end

  test "an offline acquisition under a live cross-process lock holder is refused at lock and appends nothing", %{
    dir: dir
  } do
    fs = SystemFs.new()
    # the lock names THIS live OS process, which is what a foreign live holder looks like to the
    # acquiring side (test/cli/cli_test.exs holds a run lock the same way)
    {:ok, held} = RunLock.acquire(fs, dir, supervisor_instance: "sup_holder")
    before = directory_state(dir)

    assert Recovery.acquire(dir, lock: lock_opts()) ==
             {:error, %{clause: "journal_unavailable", stage: "lock", cleanup: @none}}

    assert directory_state(dir) == before, "a refused offline acquisition changed the run directory"
    assert :ok = RunLock.release(fs, held)
  end

  test "the offline acquisition path reaches no lock, registration or filesystem primitive of its own" do
    assert {:ok, {:defmodule, _meta, [{:__aliases__, _name_meta, segments}, [do: body]]}} =
             @source |> File.read!() |> Code.string_to_quoted()

    assert Module.concat(segments) == Recovery, "#{@source} no longer defines the offline acquisition module"

    {_body, %{modules: modules, effects: effects, erlang: erlang}} =
      Macro.prewalk(body, %{modules: MapSet.new(), effects: MapSet.new(), erlang: MapSet.new()}, fn node, acc ->
        {node, record(node, acc)}
      end)

    assert Enum.sort(effects) == [],
           "#{@source} reaches #{inspect(Enum.sort(effects))} directly; the offline path may touch the run directory " <>
             "only through the Writer and Reader, which hold the Fs seam"

    assert Enum.sort(modules) == @reachable,
           "#{@source} reaches #{inspect(Enum.sort(modules))}; the offline path may only reach #{inspect(@reachable)}, " <>
             "so that Ownership and RunLock stay the single acquisition primitive"

    assert Enum.sort(erlang) == [],
           "#{@source} calls the Erlang modules #{inspect(Enum.sort(erlang))} directly instead of going through the Fs seam"
  end

  defp record({:__aliases__, _meta, [:AiOrchestrator | _] = segments}, acc),
    do: %{acc | modules: MapSet.put(acc.modules, Module.concat(segments))}

  defp record({:__aliases__, _meta, segments}, acc) when is_list(segments) do
    module = Module.concat(segments)
    if module in @effectful, do: %{acc | effects: MapSet.put(acc.effects, module)}, else: acc
  end

  defp record({{:., _meta, [module, _function]}, _call_meta, _args}, acc) when is_atom(module),
    do: %{acc | erlang: MapSet.put(acc.erlang, module)}

  defp record(_node, acc), do: acc
end
