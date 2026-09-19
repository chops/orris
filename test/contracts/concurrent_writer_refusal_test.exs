defmodule AiOrchestrator.Contracts.ConcurrentWriterRefusalTest do
  @moduledoc """
  NS-23.I.000 / NS-23.I.001, the CONTROL half only, pinned exactly as the source delivers it.

  `Spec.Plan.validate_stretch_overlap/2` refuses a plan whose writer items are DAG-independent and
  whose `allowed_paths` overlap by prefix, with clause `stretch_paths_overlap` (plan.ex:306-315,
  361-403). It is OPT-IN: it runs only when the spec carries `stretch_worktrees: true` and otherwise
  answers `:ok` unconditionally (plan.ex:306, 317; run_spec.ex:89, 110). These rows state that
  exactly, including the opt-in, so the row cannot later be read as more than it is.

  The last row is the one that matters. The ACCEPTANCE half of NS-23.I.001 -- "Run independent
  disjoint writers in dedicated journaled-base worktrees" -- does not exist: `work_items_loop/2`
  consumes one item at a time (reducer.ex:515-526) and there is no `git` reference anywhere in
  `lib/`. So a refusal about concurrent writers is delivered over an executor that has no
  concurrency, and T6-5 measures the sequential execution directly so that the refusal can never be
  mistaken for evidence of the acceptance half.

  This file deliberately does NOT propose making the check unconditional. Turning it on by default
  would change admission for every existing spec, and that belongs with the NS-23 guarantee ruling
  (row PROPOSED; both rule rows DECISION_PENDING), not with a control.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Spec.Plan
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule CapturingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "backend" => "local_pane",
         "pane_ref" => command["pane_ref"],
         "send_status" => "ok",
         "send_message_id" => command["send_message_id"],
         "replayed" => false
       }}
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(command, _opts) do
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => LocalPane.zero_hash(),
         "stable_for_ms" => 5_000,
         "modified_after_dispatch" => true
       }}
    end
  end

  @fixture "invalid_stretch_overlap"

  setup do
    {:ok, plan: F.json("plans", @fixture, "plan.json"), spec: F.json("plans", @fixture, "spec.json")}
  end

  test "T6-1 the shipped fixture really opts in, and a DAG-independent overlapping writer pair is refused",
       %{plan: plan, spec: spec} do
    # the row is not vacuous: the refusal it asserts exists only because the spec asked for it
    assert spec["stretch_worktrees"] == true
    assert Enum.map(plan["work_items"], & &1["deps"]) == [[], []]
    assert Enum.map(plan["work_items"], & &1["allowed_paths"]) == [["lib"], ["lib"]]

    assert Plan.validate(plan, spec) == {:error, %{clause: "stretch_paths_overlap"}}
  end

  test "T6-2 the same overlapping paths are admitted when one writer is DAG-reachable from the other",
       %{plan: plan, spec: spec} do
    reachable = update_in(plan, ["work_items", Access.at(1), "deps"], fn [] -> ["item_a"] end)

    assert Enum.map(reachable["work_items"], & &1["allowed_paths"]) == [["lib"], ["lib"]]
    assert {:ok, _validated} = Plan.validate(reachable, spec)
  end

  test "T6-3 the check is opt-in: the same refused plan is admitted with the flag absent or false",
       %{plan: plan, spec: spec} do
    assert Plan.validate(plan, spec) == {:error, %{clause: "stretch_paths_overlap"}}

    assert {:ok, _absent} = Plan.validate(plan, Map.delete(spec, "stretch_worktrees"))
    assert {:ok, _false} = Plan.validate(plan, Map.put(spec, "stretch_worktrees", false))
  end

  test "T6-4 under the same opt-in, a DAG-independent pair with disjoint roots is admitted",
       %{spec: spec} do
    assert {:ok, _validated} = Plan.validate(disjoint_plan(), spec)

    # and the overlap is judged by prefix, not by string equality: a subpath still overlaps
    nested = update_in(disjoint_plan(), ["work_items", Access.at(1), "allowed_paths"], fn _ -> ["lib/nested"] end)
    assert Plan.validate(nested, spec) == {:error, %{clause: "stretch_paths_overlap"}}
  end

  # Regression (2026-09-19): the overlap is judged through the containment rule's own expansion, so a
  # spelling `validate_allowed_paths` admits as the same directory (`./lib`) cannot slip past the
  # refusal as a different string. The spelling table is test/spec/path_spellings_overlap_test.exs.
  test "T6-6 the same root under a dotted spelling is still the same root to the refusal",
       %{plan: plan, spec: spec} do
    dotted = update_in(plan, ["work_items", Access.at(1), "allowed_paths"], fn ["lib"] -> ["./lib"] end)

    assert Enum.map(dotted["work_items"], & &1["allowed_paths"]) == [["lib"], ["./lib"]]
    assert Plan.validate(dotted, spec) == {:error, %{clause: "stretch_paths_overlap"}}
  end

  # The control the audit asks for: the refusal above must never be read as evidence that concurrent
  # writers exist. They do not. This runs the admitted disjoint pair -- the very shape the refusal is
  # designed to let through -- and measures that the delivered executor still runs it one item at a
  # time. A future concurrent executor makes this row fail, which is the point: it is the row that
  # must be revisited when NS-23's acceptance half lands, not silently inherited.
  test "T6-5 an admitted DAG-independent disjoint writer pair still executes strictly sequentially",
       %{spec: spec} do
    plan = disjoint_plan()
    assert {:ok, _validated} = Plan.validate(plan, spec)

    assert {:ok, %{events: events, summary: summary}} = RunFSM.run(spec, plan, fsm_opts())
    assert summary["status"] == "completed"
    assert summary["completed_work_item_ids"] == ["item_a", "item_b"]

    # every event carrying a work item, in journal order
    ordered =
      events
      |> Enum.map(& &1["data"]["work_item_id"])
      |> Enum.filter(&is_binary/1)

    assert ordered != []

    # sequential means: the journal never returns to an item after leaving it
    assert Enum.dedup(ordered) == ["item_a", "item_b"],
           "work items interleave in the journal, so execution is no longer sequential: #{inspect(Enum.dedup(ordered))}"

    # and stated again as the ordering the register would have to see for concurrency: item_a is
    # COMPLETED before item_b is even requested
    assert seq(events, "work_item_completed", "item_a") < seq(events, "assignment_requested", "item_b")
  end

  # ---- helpers ----

  # two writers that depend on nothing and write under different allowed roots: concurrent by the
  # DAG, disjoint by path, so the opt-in refusal must let them through
  defp disjoint_plan do
    plan = F.json("plans", @fixture, "plan.json")

    update_in(plan, ["work_items", Access.at(1)], fn item ->
      item
      |> Map.put("allowed_paths", ["test"])
      |> Map.put("expected_artifacts", ["test/item_b.ex"])
    end)
  end

  defp seq(events, type, work_item_id) do
    events
    |> Enum.find(&(&1["type"] == type and &1["data"]["work_item_id"] == work_item_id))
    |> Map.fetch!("seq")
  end

  defp fsm_opts do
    [
      dispatch: CapturingDispatch,
      prompt_root: ScenarioHarness.prompt_root(),
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [
        runner: fn _gate ->
          {:ok,
           %{
             "exit_status" => 0,
             "duration_ms" => 10,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash()
           }}
        end
      ],
      event_sink: GateDouble.receipt_sink(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end
end
