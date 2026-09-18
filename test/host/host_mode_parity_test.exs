defmodule AiOrchestrator.Host.ModeParityTest do
  @moduledoc """
  Foreground/mounted command parity: register row NS-04.C.001, "Compare same command outcomes in both host
  modes" (R07 mounted-keeper audit, slice S2). Test only; nothing under `lib/` changes.

  No row drove one command through BOTH host modes before this file. `test/host/host_mount_red_test.exs`
  exercises the mounted route alone; the CLI suite exercises the foreground route alone;
  `test/host/host_registry_red_test.exs` compares `Run.Executor` against `Host.Executor`, which is two
  FOREGROUND routes under different monitors. The register row that asks for the comparison therefore had
  no acceptance at all.

  Method, following the console/CLI parity discipline of docs/contracts/console-mutations.org row M-06: the
  SAME authorized command runs on byte-identical fixture trees, one route at a time, with the fixed id and
  clock seams reset before each side and the SAME private arbiter injected into both, so the routes differ
  only in the mode under test. The rows then assert the equal event type sequence, the equal line count, the
  equal `Fold` status and `last_seq`, the equal result shape and the equal released-ownership observation,
  and confine every remaining difference to a NAMED allowlist the row enforces by equality (a new difference
  fails the row rather than widening silently).

  What these rows do NOT claim. NS-04.C.001's failure control reads "Separate lifecycle path or unsupervised
  owner detected". The lifecycle path is one and HP-1/HP-2 measure that. The owner shapes differ by design -
  the foreground owner is an unlinked `spawn` the caller monitors, the mounted owner a supervised
  `:temporary` child (docs/contracts/host-mounted-runs.org, Scope) - and HP-3 MEASURES that difference
  exactly instead of ruling on it. Whether the caller-monitored spawn satisfies "supervised owner" for this
  row is the ledger decision the R07 audit records as D2, and no row here decides it.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Host
  alias AiOrchestrator.Host.Monitor
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Journal.Ownership
  alias AiOrchestrator.Run.Executor, as: RunExecutor
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @operator %{"class" => "operator", "id" => "local_operator"}
  @now %Moment{wall_ts: "2026-09-09T06:00:00Z", unix: 1_788_933_600}
  @owned [:event_sink, :run_dir, :run_lock_path, :tail_repair, :requested_by, :cancel_reason, :recovery_reason]
  @deadline 30_000
  @budgets %{close: 3_000, stop: 8_000, join: 1_000, handoff: 5_000, helper_join: 500}

  # Every journal field whose value may differ between the two routes, named the way
  # docs/contracts/console-mutations.org row M-06 names its own. The list is asserted by EQUALITY, so a
  # difference this file has not named fails the row.
  #
  #   data.run_dir      - the two routes run on two DIRECTORIES; the path is the input, never a mode
  #                       difference (M-06 names this class "path-bearing fields").
  #   prev_line_sha256  - the journal chain hash, derived from the bytes above and from nothing else
  #                       (M-06: "the chain hash prev_line_sha256 derived from them").
  #
  # Everything else is IDENTICAL across the two modes under the reset seams: the type sequence, every
  # event_id and ts, supervisor_instance, the requested_by stamp and every other data field. That is the
  # measured content of "one lifecycle path".
  @named_event_differences ["data.run_dir", "prev_line_sha256"]

  # the SAME two fields reached through the two event lists the command result carries, and nothing else:
  # the result is otherwise equal value for value, not merely equal in its key set
  @named_result_differences [
    "appended_events[].data.run_dir",
    "appended_events[].prev_line_sha256",
    "events[].data.run_dir",
    "events[].prev_line_sha256"
  ]

  setup do
    base = Path.join(System.tmp_dir!(), "mode_parity_#{System.unique_integer([:positive])}")
    fg = base <> "_foreground"
    mt = base <> "_mounted"
    Enum.each([fg, mt], &File.mkdir_p!/1)
    on_exit(fn -> Enum.each([fg, mt], &File.rm_rf!/1) end)
    %{fg: fg, mt: mt}
  end

  describe "one command, both host modes (NS-04.C.001)" do
    test "HP-1 an authorized start: equal type sequence, line count, Fold status/last_seq, result shape and release",
         %{fg: fg, mt: mt} do
      h = isolated_host()

      foreground = foreground!(fg, h.arb, nil)
      mounted = mounted!(mt, h, nil)

      assert {:ok, fg_result} = foreground
      assert {:ok, mt_result} = mounted

      fg_events = events!(fg)
      mt_events = events!(mt)

      assert fg_events != [], "the foreground route journalled nothing: the comparison would be vacuous"
      assert length(fg_events) == length(mt_events), "line count differs"
      assert Enum.map(fg_events, & &1["type"]) == Enum.map(mt_events, & &1["type"]), "event type sequence differs"

      fg_summary = summary!(fg)
      mt_summary = summary!(mt)
      assert fg_summary["status"] == mt_summary["status"], "Fold status differs"
      assert fg_summary["last_seq"] == mt_summary["last_seq"], "Fold last_seq differs"
      assert fg_summary["run_id"] == mt_summary["run_id"], "Fold run_id differs"

      assert differing_fields(fg_events, mt_events) == @named_event_differences,
             "an unnamed field differs between the host modes: #{inspect(differing_fields(fg_events, mt_events))}"

      assert Enum.sort(Map.keys(fg_result)) == Enum.sort(Map.keys(mt_result)), "result key set differs"
      result_differences = differing_fields([stringify(fg_result)], [stringify(mt_result)])

      assert result_differences == @named_result_differences,
             "an unnamed result value differs between the host modes: #{inspect(result_differences)}"

      # cleanup parity: both routes retire the arbiter record for their own directory
      assert :none == Ownership.status(fg, server: h.arb), "foreground route left an arbiter record"
      assert :none == Ownership.status(mt, server: h.arb), "mounted route left an arbiter record"
    end

    test "HP-2 a refused command: both modes answer the identical closed refusal and journal nothing",
         %{fg: fg, mt: mt} do
      h = isolated_host()

      # the refusals `prepare/2` decides before any Writer, file or effect activity; the mounted route shares
      # that validation instead of duplicating it, which is exactly what this row measures
      refusals = [
        {"missing spec", fn ctx -> Keyword.delete(ctx, :spec) end},
        {"missing run_dir", fn ctx -> Keyword.delete(ctx, :run_dir) end},
        {"declared input hashes the command does not carry", fn ctx -> Keyword.put(ctx, :spec_hash, "sha256:00") end},
        {"a reserved server-owned binding", fn ctx -> Keyword.put(ctx, :await_timeout, 1_000) end}
      ]

      for {label, mutate} <- refusals do
        H.reset_seams()
        {command, fg_ctx} = command_ctx(fg, h.arb, nil)
        from_run = RunExecutor.execute(command, mutate.(fg_ctx))

        H.reset_seams()
        {command, mt_ctx} = command_ctx(mt, h.arb, nil)
        from_host = Host.mount(command, mutate.(mt_ctx), host: h.host, budgets: @budgets)

        assert match?({:error, %{clause: _}}, from_run), "#{label}: the foreground route must refuse"
        assert from_run == from_host, "#{label}: the mounted route diverged from the foreground refusal"
      end

      for dir <- [fg, mt] do
        refute File.exists?(Path.join(dir, "events.jsonl")), "a refused command wrote a journal in #{dir}"
        assert :none == Ownership.status(dir, server: h.arb), "a refused command took ownership in #{dir}"
      end
    end

    test "HP-3 the one lifecycle difference the contract preserves, measured here and decided nowhere",
         %{fg: fg, mt: mt} do
      h = isolated_host()
      test_pid = self()

      H.reset_seams()
      {fg_command, fg_ctx} = command_ctx(fg, h.arb, holding(test_pid, :foreground))
      task = Task.async(fn -> RunExecutor.execute(fg_command, fg_ctx) end)
      {fg_ref, fg_payload, fg_barrier_pid} = await_held!(:foreground)

      H.reset_seams()
      {mt_command, mt_ctx} = command_ctx(mt, h.arb, holding(test_pid, :mounted))
      {:ok, handle} = Host.mount(mt_command, mt_ctx, host: h.host, budgets: @budgets)
      {mt_ref, mt_payload, mt_barrier_pid} = await_held!(:mounted)

      hosted = for {_, pid, _, _} <- DynamicSupervisor.which_children(h.hsup), is_pid(pid), do: pid

      # THE FOREGROUND OWNER: an unlinked spawn. Not a child of the host supervisor, and not linked to the
      # caller that monitors it (run/executor/owner.ex). The barrier is owner-resident (H-6a), so the process
      # that ran it IS the owner.
      assert fg_payload.owner == fg_barrier_pid, "the foreground barrier must run in the owner"
      refute fg_payload.owner in hosted, "the foreground owner is a child of the host supervisor"
      refute task.pid in links(fg_payload.owner), "the foreground owner is linked to the caller that invoked it"

      # THE MOUNTED OWNER: a supervised :temporary child, linked to the host supervisor. Its barrier runs in a
      # linked helper, which is the mounted-route semantic the contract states, not a lifecycle divergence.
      assert handle.owner == mt_payload.owner
      assert handle.owner in hosted, "the mounted owner is not a child of the host supervisor"
      assert Process.whereis(h.hsup) in links(handle.owner), "the mounted owner is not linked to its supervisor"
      refute mt_barrier_pid == handle.owner, "the mounted barrier must run in a helper, not the owner"
      assert handle.owner in links(mt_barrier_pid), "the mounted barrier helper is not linked to the owner"

      send(fg_barrier_pid, {:release, fg_ref})
      send(mt_barrier_pid, {:release, mt_ref})
      assert {:ok, _} = Task.await(task, @deadline)
      assert {:ok, _} = Host.await(handle, @deadline)
    end
  end

  # ---- routes ----

  defp foreground!(dir, arb, barrier) do
    H.reset_seams()
    {command, ctx} = command_ctx(dir, arb, barrier)
    RunExecutor.execute(command, ctx)
  end

  defp mounted!(dir, h, barrier) do
    H.reset_seams()
    {command, ctx} = command_ctx(dir, h.arb, barrier)
    {:ok, handle} = Host.mount(command, ctx, host: h.host, budgets: @budgets)
    assert {:ok, _ready} = Host.ready(handle, @deadline)
    Host.await(handle, @deadline)
  end

  # ---- the isolated root both routes share (one arbiter, so ownership is the same authority on both sides) ----

  defp isolated_host do
    n = System.unique_integer([:positive])
    arb = :"parity_arb_#{n}"
    hsup = :"parity_hsup_#{n}"
    mon = :"parity_mon_#{n}"

    children = [
      {Ownership, name: arb},
      {Host.Supervisor, name: hsup, child_shutdown_ms: 20_000},
      {Monitor, name: mon, host_supervisor: hsup, ownership: arb, census_timeout: 2_000}
    ]

    start_supervised!(%{
      id: :"parity_root_#{n}",
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
      type: :supervisor
    })

    %{arb: arb, hsup: hsup, mon: mon, host: %{supervisor: hsup, monitor: mon, ownership: arb}}
  end

  # ---- one command, built identically for both routes; `ownership:` names the private arbiter on BOTH
  # (Host.mount only puts it when the caller did not, so the two configurations are identical) ----

  defp command_ctx(dir, arb, barrier) do
    {_, _, scenario, [], opts_fun} = Enum.find(H.cases(), &match?({_, :run, "gated_run_seed", [], _}, &1))
    spec = H.spec(scenario)
    plan = H.plan(scenario)

    ctx =
      opts_fun.()
      |> Keyword.drop(@owned)
      |> Keyword.merge(
        run_dir: dir,
        spec: spec,
        plan: plan,
        spec_hash: hash(spec),
        plan_hash: hash(plan),
        supervisor_instance: "sup_parity_0001",
        ownership: [server: arb],
        barrier: barrier
      )

    {:ok, command} =
      Commands.build(@operator, "start", %{"spec_hash" => ctx[:spec_hash], "plan_hash" => ctx[:plan_hash]},
        run_id: "run_parity_0001",
        command_id: "cmd_parity_000000001",
        now: @now
      )

    {command, ctx}
  end

  defp hash(term), do: "sha256:" <> (:sha256 |> :crypto.hash(Jason.encode!(term)) |> Base.encode16(case: :lower))

  # ---- a barrier that holds the run at :subtree_started until the test releases it ----

  defp holding(test_pid, tag) do
    fn label, payload ->
      if label == :subtree_started do
        ref = make_ref()
        send(test_pid, {:held, tag, ref, payload, self()})

        receive do
          {:release, ^ref} -> :ok
        after
          @deadline -> exit({:hold_never_released, tag})
        end
      else
        :ok
      end
    end
  end

  defp await_held!(tag) do
    assert_receive {:held, ^tag, ref, payload, pid}, @deadline
    {ref, payload, pid}
  end

  defp links(pid) do
    case Process.info(pid, :links) do
      {:links, links} -> Enum.filter(links, &is_pid/1)
      nil -> []
    end
  end

  # ---- journal reading and the named-difference computation ----

  defp journal_lines!(dir), do: dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true)

  defp events!(dir), do: Enum.map(journal_lines!(dir), &Jason.decode!/1)

  defp summary!(dir) do
    assert {:ok, state} = Fold.fold_lines(journal_lines!(dir))
    Fold.summary(state)
  end

  defp stringify(map), do: Map.new(map, fn {k, v} -> {to_string(k), v} end)

  # the sorted set of field paths whose values differ across the aligned pairs, descending into nested maps
  # so a difference is named at the leaf that carries it rather than at the object above it
  defp differing_fields(left, right) do
    left
    |> Enum.zip(right)
    |> Enum.flat_map(fn {a, b} -> field_diff(a, b, "") end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp field_diff(a, b, prefix) when is_map(a) and is_map(b) do
    a |> Map.keys() |> Enum.concat(Map.keys(b)) |> Enum.uniq() |> Enum.flat_map(&key_diff(a, b, prefix, &1))
  end

  defp key_diff(a, b, prefix, key), do: value_diff(Map.get(a, key), Map.get(b, key), prefix <> to_string(key))

  defp value_diff(left, right, path) do
    cond do
      left == right -> []
      is_map(left) and is_map(right) -> field_diff(left, right, path <> ".")
      is_list(left) and is_list(right) and length(left) == length(right) -> list_diff(left, right, path)
      true -> [path]
    end
  end

  defp list_diff(left, right, path) do
    left |> Enum.zip(right) |> Enum.flat_map(fn {l, r} -> value_diff(l, r, path <> "[]") end)
  end
end
