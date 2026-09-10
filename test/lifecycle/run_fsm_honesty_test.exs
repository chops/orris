defmodule AiOrchestrator.Lifecycle.RunFSMHonestyTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness

  defmodule PromptCapturingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use AiOrchestrator.Test.ScriptedDispatchReceipt

    @impl true
    def deliver(command, opts) do
      if pid = Keyword.get(opts, :test_pid) do
        # The double stands where the pane adapter stands, so it is the one place in this
        # file entitled to reveal; the assertions below are about the prompt's content.
        send(pid, {:prompt, command["assignment_id"], SensitiveBytes.reveal(command["prompt"])})
      end

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
    def observe(command, opts) do
      sha =
        case Keyword.get(opts, :artifact_sha) do
          nil -> LocalPane.zero_hash()
          sha -> sha
        end

      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => sha,
         "stable_for_ms" => 5000,
         "modified_after_dispatch" => true
       }}
    end
  end

  defp seed_inputs do
    {F.json("scenarios", "gated_run_seed", "spec.json"), F.json("scenarios", "gated_run_seed", "plan.json")}
  end

  defp pass_gate do
    {:ok,
     %{
       "exit_status" => 0,
       "duration_ms" => 100,
       "stdout_hash" => LocalPane.zero_hash(),
       "stderr_hash" => LocalPane.zero_hash()
     }}
  end

  defp base_opts(extra) do
    Keyword.merge(
      [
        dispatch: PromptCapturingDispatch,
        prompt_root: ScenarioHarness.prompt_root(),
        dispatch_opts: [test_pid: self()],
        review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end,
        gate_executor: GateDouble,
        gate_helper: GateDouble.helper(),
        gate_opts: [runner: fn _gate -> pass_gate() end],
        event_sink: GateDouble.receipt_sink()
      ],
      extra
    )
  end

  # Fix 1: the retry prompt must carry the bounded failure summary of the
  # latest failed gate for the work item; the first attempt must not.
  test "retry prompt contains the latest gate failure summary; first attempt does not" do
    {spec, plan} = seed_inputs()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    flaky_gate = fn _gate ->
      case Agent.get_and_update(counter, &{&1, &1 + 1}) do
        0 ->
          {:failed,
           %{
             "exit_status" => 1,
             "duration_ms" => 90,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash(),
             "failure_summary" => %{
               "headline" => "1 test failed in greeting_test.exs",
               "failures" => [%{"line" => "assertion failed at line 12"}],
               "suggestion" => "make the greeting lowercase"
             }
           }}

        _later ->
          pass_gate()
      end
    end

    assert {:ok, %{summary: %{"status" => "completed"}}} =
             RunFSM.run(
               spec,
               plan,
               base_opts(gate_executor: GateDouble, gate_helper: GateDouble.helper(), gate_opts: [runner: flaky_gate])
             )

    prompts = collect_prompts([])

    writer_prompts =
      for {"as_" <> _rest = id, prompt} <- prompts,
          String.ends_with?(id, "1") or String.ends_with?(id, "3"),
          do: {id, prompt}

    [first_prompt | _] = for {_id, p} <- prompts, do: p
    retry_prompt = prompts |> Enum.filter(fn {_id, p} -> p =~ "Previous attempt" end) |> Enum.map(&elem(&1, 1))

    refute first_prompt =~ "1 test failed in greeting_test.exs"
    assert [prompt] = retry_prompt
    assert prompt =~ "1 test failed in greeting_test.exs"
    assert prompt =~ "assertion failed at line 12"
    assert prompt =~ "make the greeting lowercase"
    assert writer_prompts != []
  end

  # Fix 2: review facts are parsed from the reviewer artifact, never fabricated.
  test "parsed clean verdict journals real hash and accepted_clean disposition" do
    {spec, plan} = seed_inputs()

    assert {:ok, %{events: events, summary: %{"status" => "completed"}}} =
             RunFSM.run(spec, plan, base_opts([]))

    received = Enum.find(events, &(&1["type"] == "review_received"))
    assert received["data"]["verdict"] == "clean"
    assert received["data"]["finding_count"] == 0
    refute received["data"]["review_hash"] == LocalPane.zero_hash()

    disposition = Enum.find(events, &(&1["type"] == "review_disposition_recorded"))
    assert disposition["data"]["disposition"] == "accepted_clean"
  end

  test "findings verdict journals changes_requested and the gate still decides completion" do
    {spec, plan} = seed_inputs()

    reader = fn _path -> {:ok, "- Verdict :: findings\n- Findings :: 2\n- greeting could be friendlier\n"} end

    assert {:ok, %{events: events, summary: %{"status" => "completed"}}} =
             RunFSM.run(spec, plan, base_opts(review_reader: reader))

    received = Enum.find(events, &(&1["type"] == "review_received"))
    assert received["data"]["verdict"] == "findings"
    assert received["data"]["finding_count"] == 2

    disposition = Enum.find(events, &(&1["type"] == "review_disposition_recorded"))
    assert disposition["data"]["disposition"] == "changes_requested"

    assert Enum.any?(events, &(&1["type"] == "work_item_completed"))
  end

  test "unparseable review artifact escalates instead of fabricating a verdict" do
    {spec, plan} = seed_inputs()

    reader = fn _path -> {:ok, "Looks good to me!\n"} end

    assert {:ok, %{events: events, summary: summary}} =
             RunFSM.run(spec, plan, base_opts(review_reader: reader))

    received = Enum.find(events, &(&1["type"] == "review_received"))
    assert received["data"]["verdict"] == "invalid"

    assert Enum.any?(events, &(&1["type"] == "human_attention_required"))
    assert summary["status"] == "blocked"
    refute Enum.any?(events, &(&1["type"] == "work_item_completed"))
  end

  test "reviewer prompt declares the verdict convention" do
    {spec, plan} = seed_inputs()

    assert {:ok, _result} = RunFSM.run(spec, plan, base_opts([]))

    prompts = collect_prompts([])
    reviewer_prompt = prompts |> Enum.reverse() |> Enum.find_value(fn {_id, p} -> if p =~ "Review subject", do: p end)

    assert reviewer_prompt
    assert reviewer_prompt =~ "- Verdict :: clean"
    assert reviewer_prompt =~ "- Verdict :: findings"
    assert reviewer_prompt =~ "- Findings :: "
  end

  # MUST-1: crash windows around invalid reviews must stay blocked on resume.
  describe "resume crash windows around escalated reviews" do
    test "crash after invalid review_received: resume appends attention, blocks, never gates" do
      {prior, spec, plan} = prior_until_invalid_received()

      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts(review_reader: fn _p -> {:ok, "nope"} end))

      assert Enum.count(events, &(&1["type"] == "human_attention_required")) == 1
      assert summary["status"] == "blocked"
      refute Enum.any?(events, &(&1["type"] == "gate_requested"))
      refute Enum.any?(events, &(&1["type"] == "work_item_completed"))
      assert_no_new_dispatch(events, prior_events)
      assert Enum.count(events, &(&1["type"] == "review_received")) == 1

      assert [%{"data" => %{"disposition" => "escalated"}}] =
               Enum.filter(events, &(&1["type"] == "review_disposition_recorded"))
    end

    test "crash after escalated disposition without attention: resume appends attention and blocks" do
      {prior, spec, plan} = prior_until_escalated_disposition()

      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts([]))

      assert Enum.count(events, &(&1["type"] == "human_attention_required")) == 1
      assert summary["status"] == "blocked"
      refute Enum.any?(events, &(&1["type"] == "gate_requested"))
      assert_no_new_dispatch(events, prior_events)
      assert Enum.count(events, &(&1["type"] == "review_received")) == 1

      assert [%{"data" => %{"disposition" => "escalated"}}] =
               Enum.filter(events, &(&1["type"] == "review_disposition_recorded"))

      refute Enum.any?(events, &(&1["type"] == "work_item_completed"))
    end
  end

  defp assert_no_new_dispatch(events, prior_events) do
    count = fn evs, type -> Enum.count(evs, &(&1["type"] == type)) end
    assert count.(events, "assignment_requested") == count.(prior_events, "assignment_requested")
    assert count.(events, "assignment_dispatch_sent") == count.(prior_events, "assignment_dispatch_sent")
  end

  # MUST-2: the journaled review hash must be the hash of the bytes parsed.
  test "artifact bytes drifting between observation and parse escalate as drift" do
    {spec, plan} = seed_inputs()

    reader = fn _path -> {:ok, "- Verdict :: clean\n"} end
    mismatching = "sha256:" <> String.duplicate("ab", 32)

    assert {:ok, %{events: events, summary: summary}} =
             RunFSM.run(
               spec,
               plan,
               base_opts(review_reader: reader, dispatch_opts: [test_pid: self(), artifact_sha: mismatching])
             )

    received = Enum.find(events, &(&1["type"] == "review_received"))
    assert received["data"]["verdict"] == "invalid"
    assert summary["status"] == "blocked"
  end

  # MUST-3: parser totality and exactly-one canonical verdict.
  for {label, artifact, expected} <- [
        {"uppercase CLEAN does not crash and is invalid", "- Verdict :: CLEAN\n", "invalid"},
        {"duplicate verdict lines are invalid", "- Verdict :: clean\n- Verdict :: findings\n", "invalid"},
        {"findings without a positive count are invalid", "- Verdict :: findings\n", "invalid"},
        {"clean claiming positive findings is invalid", "- Verdict :: clean\n- Findings :: 3\n", "invalid"}
      ] do
    test label do
      {spec, plan} = seed_inputs()
      artifact = unquote(artifact)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.run(spec, plan, base_opts(review_reader: fn _p -> {:ok, artifact} end))

      received = Enum.find(events, &(&1["type"] == "review_received"))
      assert received["data"]["verdict"] == unquote(expected)
      assert summary["status"] == "blocked"
    end
  end

  # Valid-review crash windows: durable verdicts recover in place — no redispatch.
  describe "resume recovery of valid reviews" do
    test "clean verdict without disposition: disposition appended, gate runs, no redispatch" do
      {prior, spec, plan} = prior_valid_cut("- Verdict :: clean\n", "review_disposition_recorded")
      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts([]))

      assert summary["status"] == "completed"

      assert [%{"data" => %{"disposition" => "accepted_clean"}}] =
               Enum.filter(events, &(&1["type"] == "review_disposition_recorded"))

      assert Enum.count(events, &(&1["type"] == "review_received")) == 1
      assert Enum.any?(events, &(&1["type"] == "gate_passed"))
      assert Enum.any?(events, &(&1["type"] == "work_item_completed"))
      assert_no_new_dispatch(events, prior_events)
    end

    test "findings verdict without disposition: changes_requested appended, gate still decides" do
      {prior, spec, plan} =
        prior_valid_cut("- Verdict :: findings\n- Findings :: 1\n", "review_disposition_recorded")

      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts([]))

      assert summary["status"] == "completed"

      assert [%{"data" => %{"disposition" => "changes_requested"}}] =
               Enum.filter(events, &(&1["type"] == "review_disposition_recorded"))

      assert Enum.count(events, &(&1["type"] == "review_received")) == 1
      assert Enum.any?(events, &(&1["type"] == "work_item_completed"))
      assert_no_new_dispatch(events, prior_events)
    end

    test "crash after disposition before gate: gate runs on the original artifact, no redispatch" do
      {prior, spec, plan} = prior_valid_cut("- Verdict :: clean\n", "gate_requested")
      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts([]))

      assert summary["status"] == "completed"
      assert Enum.count(events, &(&1["type"] == "review_disposition_recorded")) == 1
      assert Enum.any?(events, &(&1["type"] == "gate_passed"))
      assert Enum.any?(events, &(&1["type"] == "work_item_completed"))
      assert_no_new_dispatch(events, prior_events)
    end
  end

  # Retry-attempt MUSTs: recovery must preserve the attempt budget and must key
  # gate presence to the reviewed writer assignment, not the work item.
  describe "resume recovery across retry attempts" do
    test "crash before attempt-2 gate: recovery gates attempt-2 artifact and a failure exhausts the budget" do
      {prior, spec, plan} = prior_retry_cut_before_second_gate()
      prior_events = Enum.map(prior, &Jason.decode!/1)

      failing_gate = fn _gate ->
        {:failed,
         %{
           "exit_status" => 1,
           "duration_ms" => 50,
           "stdout_hash" => LocalPane.zero_hash(),
           "stderr_hash" => LocalPane.zero_hash(),
           "failure_summary" => %{"headline" => "still failing", "failures" => [], "suggestion" => "n/a"}
         }}
      end

      assert {:error, %{"reason" => "gate_failed"}} =
               RunFSM.resume(
                 spec,
                 plan,
                 prior,
                 base_opts(
                   gate_executor: GateDouble,
                   gate_helper: GateDouble.helper(),
                   gate_opts: [runner: failing_gate],
                   event_sink: GateDouble.receipt(sink(self()))
                 )
               )

      appended = collect_sunk(self())

      gate_requests = Enum.filter(appended, &(&1["type"] == "gate_requested"))
      assert [%{"data" => %{"assignment_id" => "as_0003"}}] = gate_requests

      refute Enum.any?(appended, &(&1["type"] == "assignment_requested"))
      refute Enum.any?(appended, &(&1["type"] == "review_requested"))
      refute Enum.any?(appended, &(&1["type"] == "work_item_retry_scheduled"))
      assert prior_events != []
    end

    test "crash before attempt-2 disposition: recovery derives the attempt and completes on a pass" do
      {prior, spec, plan} = prior_retry_cut_before_second_disposition()
      prior_events = Enum.map(prior, &Jason.decode!/1)

      assert {:ok, %{events: events, summary: summary}} =
               RunFSM.resume(spec, plan, prior, base_opts([]))

      assert summary["status"] == "completed"
      assert Enum.count(events, &(&1["type"] == "review_disposition_recorded")) == 2
      assert_no_new_dispatch(events, prior_events)

      new_gates =
        events
        |> Enum.drop(length(prior_events))
        |> Enum.filter(&(&1["type"] == "gate_requested"))

      assert [%{"data" => %{"assignment_id" => "as_0003"}}] = new_gates
    end
  end

  defp sink(pid) do
    fn event ->
      send(pid, {:sunk, event})
      :ok
    end
  end

  defp collect_sunk(_pid) do
    receive do
      {:sunk, event} -> [event | collect_sunk(nil)]
    after
      0 -> []
    end
  end

  defp retry_plan do
    {spec, plan} = seed_inputs()
    plan = update_in(plan, ["work_items", Access.at(0)], &Map.put(&1, "max_attempts", 2))
    {spec, plan}
  end

  defp retry_prior_events do
    {spec, plan} = retry_plan()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    flaky = fn _gate ->
      case Agent.get_and_update(counter, &{&1, &1 + 1}) do
        0 ->
          {:failed,
           %{
             "exit_status" => 1,
             "duration_ms" => 50,
             "stdout_hash" => LocalPane.zero_hash(),
             "stderr_hash" => LocalPane.zero_hash(),
             "failure_summary" => %{"headline" => "first failure", "failures" => [], "suggestion" => "retry"}
           }}

        _later ->
          pass_gate()
      end
    end

    {:ok, %{events: events, summary: %{"status" => "completed"}}} =
      RunFSM.run(
        spec,
        plan,
        base_opts(gate_executor: GateDouble, gate_helper: GateDouble.helper(), gate_opts: [runner: flaky])
      )

    {events, spec, plan}
  end

  defp prior_retry_cut_before_second_gate do
    {events, spec, plan} = retry_prior_events()
    cut = cut_before_nth(events, "gate_requested", 2)
    {Enum.map(cut, &Jason.encode!/1), spec, plan}
  end

  defp prior_retry_cut_before_second_disposition do
    {events, spec, plan} = retry_prior_events()
    cut = cut_before_nth(events, "review_disposition_recorded", 2)
    {Enum.map(cut, &Jason.encode!/1), spec, plan}
  end

  defp cut_before_nth(events, type, n) do
    {kept, _count} = Enum.reduce(events, {[], 0}, &keep_before_nth(&1, &2, type, n))
    Enum.reverse(kept)
  end

  defp keep_before_nth(event, {acc, count}, type, n) do
    count = if event["type"] == type, do: count + 1, else: count

    if count >= n do
      {acc, count}
    else
      {[event | acc], count}
    end
  end

  defp prior_valid_cut(artifact, cut_before_type) do
    {spec, plan} = seed_inputs()

    {:ok, %{events: events}} =
      RunFSM.run(spec, plan, base_opts(review_reader: fn _p -> {:ok, artifact} end))

    cut = Enum.take_while(events, &(&1["type"] != cut_before_type))
    {Enum.map(cut, &Jason.encode!/1), spec, plan}
  end

  defp prior_until_invalid_received do
    {spec, plan} = seed_inputs()

    {:ok, %{events: events}} =
      RunFSM.run(spec, plan, base_opts(review_reader: fn _p -> {:ok, "no verdict here"} end))

    cut = Enum.take_while(events, &(&1["type"] != "review_disposition_recorded"))
    {Enum.map(cut, &Jason.encode!/1), spec, plan}
  end

  defp prior_until_escalated_disposition do
    {spec, plan} = seed_inputs()

    {:ok, %{events: events}} =
      RunFSM.run(spec, plan, base_opts(review_reader: fn _p -> {:ok, "no verdict here"} end))

    cut = Enum.take_while(events, &(&1["type"] != "human_attention_required"))
    {Enum.map(cut, &Jason.encode!/1), spec, plan}
  end

  defp collect_prompts(acc) do
    receive do
      {:prompt, id, prompt} -> collect_prompts(acc ++ [{id, prompt}])
    after
      0 -> acc
    end
  end
end
