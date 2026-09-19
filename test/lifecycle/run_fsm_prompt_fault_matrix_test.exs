defmodule AiOrchestrator.Lifecycle.RunFSMPromptFaultMatrixTest do
  @moduledoc """
  NS-42 rule 5, the residue the register names: "durable retention of hash/size-verified
  prompt bytes, resumed verification before paste and the put-phase failure control ...
  remain UNPROVEN".

  Two files already stand either side of this one. `test/dispatch/prompt_store_test.exs`
  proves what the store does at its own seam, phase by phase. `run_fsm_prompt_retention_test.exs`
  proves the ORDER -- put before projection, fetch before reconcile -- and takes two phases
  (`write`, `link`) through a whole run. Neither one proves the control the rule actually
  states, which is quantified over phases and lives at the join between them: *no* failure of
  *any* required durable-put phase may end with a committed projection or a paste.

  A control quantified over phases is only a control if every phase is instantiated. Two of
  eleven is a sample, and the two that were sampled are the two where a reader would most
  expect the store to fail closed. The phases that are easy to get wrong are the ones after
  the bytes are already safe -- the temporary that will not unlink, the directory entry that
  will not fsync -- because at that point the object really is on disk and "succeed anyway"
  is the tempting answer.

  So this file is the matrix:

    * Every one of the eleven durable-put phases, injected one at a time through a whole
      run, each ending in `prompt_retention_failed` naming that phase, with no projection
      and no paste. The last two are the ones that matter most: the object IS linked into
      place and the run still refuses to claim it, because a store that reports success for
      a publication it could not make durable is a store whose journal outlives its bytes.
    * The two fetch classes the run level has not yet seen. `prompt_hash_mismatch`,
      `prompt_object_missing` and `prompt_object_unreadable` are already pinned there; the
      size clause and the containment clause are not, and they are the two the rule names
      that the digest cannot stand in for.

  Each row names the phase, so a store that starts reporting one phase's failure under
  another phase's name fails here rather than in a log an operator reads six weeks later.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  @run_id "run_scenario_0001"
  @assignment "as_0001"

  # The publication chain, in the order `PromptStore.put/5` runs it, with the class each
  # phase answers and whether the object is already linked when that phase fires.
  #
  # The selector is an argument matcher wherever two calls of one operation are different
  # phases, and a call ordinal where the store is the only caller of that operation. The
  # two `chmod` calls are the directory at 0700 and the temporary at 0600; the two
  # `dir_sync` calls are the run root (the new directory's own entry) and `prompts/` (the
  # object's entry). Counting calls there would bind this table to an order it is supposed
  # to be independent of.
  # The selector is data rather than a closure because these rows are escaped into generated
  # tests, and a closure cannot be. `selector/1` below is the one place it becomes a
  # function, so a row and the predicate it means cannot drift apart in eleven places.
  @phases [
    {"the prompts directory cannot be created", :mkdir, {:args, ["prompts"]}, {:error, :eacces},
     "prompt_dir_create_failed", false},
    {"the prompts directory cannot be narrowed", :chmod, {:args, ["prompts", 0o700]}, {:error, :eperm},
     "prompt_dir_chmod_failed", false},
    {"the new directory entry cannot be made durable", :dir_sync, {:args_not, ["prompts"]}, {:error, :eio},
     "prompt_dir_sync_failed", false},
    {"the temporary cannot be opened", :open, 1, {:error, :emfile}, "prompt_open_failed", false},
    {"the temporary cannot be narrowed to 0600", :chmod, {:mode, 0o600}, {:error, :eperm}, "prompt_chmod_failed", false},
    {"the bytes cannot be written", :write, 1, {:error, :enospc}, "prompt_write_failed", false},
    {"the bytes cannot be fsynced", :sync, 1, {:error, :eio}, "prompt_sync_failed", false},
    {"the temporary cannot be closed", :close, 1, {:error, :eio}, "prompt_close_failed", false},
    {"the object cannot be linked into place", :link, 1, {:error, :eacces}, "prompt_link_failed", false},
    {"the temporary cannot be removed", :rm, 1, {:error, :eacces}, "prompt_temp_cleanup_failed", true},
    {"the publication cannot be made durable", :dir_sync, {:args, ["prompts"]}, {:error, :eio},
     "prompt_publication_sync_failed", true}
  ]

  @doc false
  @spec selector(pos_integer() | tuple()) :: pos_integer() | (list() -> boolean())
  def selector(nth) when is_integer(nth), do: nth
  def selector({:args, args}), do: &(&1 == args)
  def selector({:args_not, args}), do: &(&1 != args)
  def selector({:mode, mode}), do: fn args -> match?([_name, ^mode], args) end

  defmodule PaneClient do
    @moduledoc false

    # Reports the SHAPE it was handed rather than the prompt: a stub that forwards raw
    # bytes puts them in a mailbox ExUnit dumps verbatim on a timeout.
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, prompt, opts) do
      size = if is_struct(prompt, SensitiveBytes), do: SensitiveBytes.byte_size(prompt), else: byte_size(prompt)
      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, size}, [])

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def reconcile(pane_ref, message_id, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:reconcile_called, pane_ref}, [])

      {:ok,
       %{
         "ok" => true,
         "protocol_version" => 2,
         "outcome" => "absent",
         "msg_id" => message_id,
         "pane_id" => pane_ref
       }}
    end

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  setup do
    root = Path.join(System.tmp_dir!(), "prompt-fault-#{System.unique_integer([:positive])}")
    # The run directory is the caller's and exists before anything runs in it, exactly as
    # the CLI leaves it. A root that does not exist makes every put fail as
    # `prompt_dir_create_failed`, which would make ten of the eleven rows below pass for a
    # reason that has nothing to do with the phase they name.
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root}
  end

  describe "no durable-put phase may fail into a committed projection" do
    for {label, op, selector, fault, expected, published?} <- @phases do
      test "when #{label}, the run blocks naming #{expected} and pastes nothing", ctx do
        fs = FaultFs.new()
        FaultFs.inject(fs, unquote(op), selector(unquote(Macro.escape(selector))), unquote(Macro.escape(fault)))

        assert {:ok, result} = fresh_run(ctx, fs)

        types = Enum.map(result.appended_events, & &1["type"])

        refute "assignment_prompt_projected" in types,
               "no event may name an object the store refused to accept"

        refute "assignment_dispatch_sent" in types
        assert "human_attention_required" in types
        assert result.summary["status"] == "blocked"
        assert sent() == [], "nothing was retained, so nothing may be pasted"

        detail = attention(result)["detail"]

        assert attention(result)["reason"] == "prompt_retention_failed"
        assert detail["stage"] == "retain"

        assert detail["error"] == unquote(expected), """
        The phase that failed is #{unquote(label)}, and the class an operator routes on has
        to say so. A store that answers a neighbouring phase's class sends them to the wrong
        half of the chain -- the disk rather than the permissions, or the reverse.
        """

        if !unquote(published?) do
          assert objects(ctx.root) == [],
                 "this phase fails before the link, so no object may be reachable under prompts/"
        end
      end
    end

    test "the eleven phases are eleven distinct classes, so the matrix is a matrix" do
      classes = for {_label, _op, _selector, _fault, expected, _published} <- @phases, do: expected

      assert Enum.uniq(classes) == classes
      assert length(classes) == 11
    end

    test "the two post-link phases really do leave the object on disk, and still refuse", ctx do
      # This is the claim the last two rows above rest on. If the object were absent, those
      # rows would be ordinary pre-link failures wearing a post-link label, and the
      # interesting part of the control -- refusing to claim a publication whose durability
      # is unproven -- would never be exercised.
      for {_label, op, selector, fault, expected, true} <-
            Enum.filter(@phases, fn {_l, _o, _s, _f, _e, published?} -> published? end) do
        fs = FaultFs.new()
        FaultFs.inject(fs, op, selector(selector), fault)

        assert {:ok, result} = fresh_run(ctx, fs)

        assert attention(result)["detail"]["error"] == expected
        assert objects(ctx.root) != [], expected

        File.rm_rf!(Path.join(ctx.root, "prompts"))
        drain()
      end
    end

    test "with no fault injected the same harness retains, projects and sends", ctx do
      assert {:ok, result} = fresh_run(ctx, FaultFs.new())

      types = Enum.map(result.appended_events, & &1["type"])

      assert "assignment_prompt_projected" in types
      assert "assignment_dispatch_sent" in types
      assert sent() != []
      assert objects(ctx.root) != []
    end
  end

  describe "a resumed send verifies both facts the journal holds about the object" do
    test "a size that disagrees while the digest agrees stops the send", ctx do
      {lines, projection} = journal_through_projection(ctx)

      lines = with_projection(lines, &Map.put(&1, "prompt_bytes", &1["prompt_bytes"] + 1))

      assert {:ok, result} = resume(ctx, FaultFs.new(), lines)

      assert attention(result)["detail"]["error"] == "prompt_size_mismatch", """
      The digest matches, so a store that checked only the digest would hand these bytes
      over and the run would paste a prompt whose journaled length is a lie. The size is
      the journal's own second claim about one object, and two claims that disagree are
      not evidence whichever of them the bytes happen to satisfy.
      """

      assert sent() == []
      assert result.summary["status"] == "blocked"

      assert File.exists?(Path.join(ctx.root, projection["prompt_path"])),
             "the object is left for an operator to look at, not quietly removed"
    end

    test "a journaled path outside the prompts directory is refused before the disk is touched", ctx do
      {lines, projection} = journal_through_projection(ctx)

      for escape <- ["../#{@assignment}.org", "prompts/../../#{@assignment}.org", "/etc/#{@assignment}.org"] do
        mutated = with_projection(lines, &Map.put(&1, "prompt_path", escape))
        fs = FaultFs.new()

        assert {:ok, result} = resume(ctx, fs, mutated)

        assert attention(result)["reason"] == "prompt_fetch_failed", escape

        assert attention(result)["detail"]["error"] == "prompt_object_name_mismatch", """
        A projection is a decoded journal fragment, so the object is built through the
        checked constructor, and `path` there is DERIVED from the assignment and the digest
        rather than trusted. A traversing name therefore cannot reach the store's own
        containment check at all: it stops one layer earlier, as a path that is not the path
        this object would have. Recorded as measured, because a reader looking for
        `prompt_path_escapes_root` at this boundary will not find it and should know why
        rather than conclude containment is missing. The store's clause is still reachable
        and is pinned at its own seam in prompt_store_test.exs.
        """

        assert sent() == [], escape

        refute Enum.any?(FaultFs.trace(fs), &match?({:read, _}, &1)), """
        A rejection that arrives after a read has already told a caller whether the
        substituted path exists. The name is judged first, so nothing on the escaping path
        is opened, stat-ed or read.
        """

        drain()
      end

      assert projection["prompt_path"] =~ ~r{\Aprompts/}
    end

    test "the unmutated journal resumes and sends, so the two refusals are about the mutation", ctx do
      {lines, _projection} = journal_through_projection(ctx)

      assert {:ok, _result} = resume(ctx, FaultFs.new(), lines)
      assert sent() != []
    end
  end

  defp fresh_run(ctx, fs) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.run(spec, plan, [run_id: @run_id] ++ fsm_opts(ctx, fs))
  end

  defp resume(ctx, fs, lines) do
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.resume(spec, plan, lines, fsm_opts(ctx, fs))
  end

  # A fresh run stopped at the moment the projection became durable. Built from a real run
  # rather than a checked-in fixture so the hash, the byte count and the retained object
  # agree with each other by construction, which is the property the mutations below break
  # one at a time.
  defp journal_through_projection(ctx) do
    assert {:ok, result} = fresh_run(ctx, FaultFs.new())
    drain()

    events = Enum.take_while(result.events, &(&1["type"] != "assignment_dispatch_sent"))
    projection = events |> Enum.find(&(&1["type"] == "assignment_prompt_projected")) |> Map.fetch!("data")

    # The journal is truncated to the crash point, so the filesystem has to be too: the run
    # went on to retain a reviewer's prompt that, at the moment this journal ends, had not
    # happened yet.
    # ---- arrangement ----

    requested = for %{"type" => "assignment_requested", "data" => %{"assignment_id" => id}} <- events, do: id

    for path <- objects(ctx.root),
        not Enum.any?(requested, &String.starts_with?(Path.basename(path), &1 <> "-")),
        do: File.rm!(path)

    {Enum.map(events, &Jason.encode!/1), projection}
  end

  # Rewriting a line changes its bytes, and every following `prev_line_sha256` hashes those
  # bytes, so the chain is rebuilt. A mutated journal that the reader would refuse is not a
  # fixture about the fetch boundary, it is a fixture about the reader.
  defp with_projection(lines, fun) do
    lines
    |> Enum.map(fn line ->
      event = Jason.decode!(line)

      if event["type"] == "assignment_prompt_projected",
        do: Jason.encode!(Map.update!(event, "data", fun)),
        else: line
    end)
    |> rechain()
  end

  defp rechain(lines) do
    {rechained, _prev} =
      Enum.map_reduce(lines, Chain.anchor(), fn line, prev ->
        rechained =
          line
          |> Jason.decode!()
          |> Map.put("schema_version", 2)
          |> Map.put("prev_line_sha256", prev)
          |> Jason.encode!()

        {rechained, Chain.line_sha256(rechained <> "\n")}
      end)

    rechained
  end

  defp fsm_opts(ctx, fs) do
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    artifact_by_assignment =
      fixture_events
      |> Enum.filter(&(&1["type"] == "artifact_observed"))
      |> Map.new(fn event -> {Map.fetch!(event["data"], "assignment_id"), event["data"]} end)

    gate_pass =
      fixture_events
      |> Enum.find(&(&1["type"] == "gate_passed"))
      |> Map.fetch!("data")
      |> Map.delete("gate_run_id")

    [
      fs: fs,
      prompt_root: ctx.root,
      dispatch: LocalPane,
      dispatch_opts: [
        artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
        pane_client: PaneClient,
        test_pid: self()
      ],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end,
      event_sink: GateDouble.receipt_sink()
    ]
  end

  defp sent do
    accumulated = Process.get(:sent, []) ++ for({:send_called, pane, _size} <- drain_messages(), do: pane)
    Process.put(:sent, accumulated)
    accumulated
  end

  defp drain do
    drain_messages()
    Process.put(:sent, [])
    :ok
  end

  defp drain_messages do
    receive do
      message -> [message | drain_messages()]
    after
      0 -> []
    end
  end

  defp attention(result) do
    case Enum.find(result.events, &(&1["type"] == "human_attention_required")) do
      nil -> flunk("the run appended no human_attention_required event")
      event -> event["data"]
    end

    # ---- reading what happened ----
  end

  defp objects(root), do: [root, "prompts", "*.org"] |> Path.join() |> Path.wildcard() |> Enum.sort()

  # Referenced so the alias is load-bearing rather than decorative: the prompt reaches the
  # pane stub wrapped, and the stub asks it for its size rather than revealing it.
  @doc false
  def wrapped?(term), do: is_struct(term, SensitiveBytes)
end
