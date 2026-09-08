defmodule AiOrchestrator.Lifecycle.RunFSMPromptRetentionTest do
  @moduledoc """
  RED for the ordering half of the PromptStore matrix, against Codex ruling
  `m_1788513216000000000_a93fb78e`. The store's own contract is in
  `test/dispatch/prompt_store_test.exs`; this file is about *when* the store is used
  relative to the journal, and about what must not happen when it cannot be.

  ## Why retention is an effect and not a step inside `commit/3`

  The ruling fixes the order -- render, hash, durable put, then append
  `assignment_prompt_projected` -- and fixes the reducer as pure. `Host.drive/3` already
  commits an emitted suffix before it executes the following effect, so a put interposed
  in `commit/3` would be a second, differently-shaped effect path hidden inside the
  journal writer, and the one place in the host that is deliberately free of adapters
  would start deciding things.

  The existing effect protocol already produces exactly the required order without that.
  The reducer emits a retention intent carrying the rendered bytes and *no* event; the
  host puts them through `Journal.Fs`; the reducer receives the object descriptor as an
  observation and only then emits the projection naming it. Blob strictly precedes event
  because the reducer cannot name the object until the put has answered. `commit/3` keeps
  its single job, and the reducer still touches no filesystem.

  This costs zero `Host.drive`/`commit` control-flow changes -- not zero host changes.
  `drive/3` already returns `{:ok, committed}` untouched for an empty suffix, so an effect
  emitted with no events is served against a journal that has not moved, and that is the
  whole of the ordering guarantee. What the host does gain is two adapter clauses:
  `execute/observe` must serve `Effect.RetainPrompt` and `Effect.FetchPrompt`, exactly as
  they serve every other effect. Those two clauses are pinned in the exhaustive
  effect-to-observation table in `test/contracts/lifecycle_contract_test.exs`, so the pair
  cannot be added to the reducer and quietly left unserved by the host.

  It also gives the two crash windows their honest asymmetry:

    * put succeeded, append never landed -> a blob no event points at. Inert: nothing
      reads the store except by a path a journaled event supplied. It is garbage, not
      corruption, and it is not repaired here.
    * append landed, blob absent -> an event pointing at nothing. Under v2 that is a
      durability fault and stops the run, because re-rendering it would reintroduce the
      exact bug this slice exists to remove.

  ## Why the name can be computed by a pure reducer

  `prompt_metadata/5` already hashes the rendered bytes. A content-addressed path is a
  pure function of that hash, so naming the object costs no I/O and the reducer stays
  pure. What the reducer cannot do is make the bytes exist; that is the whole of what the
  effect adds.

  ## The bytes in an effect are not evidence, but they are still not safe to print

  `Effect.RetainPrompt` carries the prompt and `Observation.PromptFetched` returns it.
  Both are in-memory arguments to an adapter call. The no-payload rule governs journals,
  protocol replies, diagnostics, telemetry and logs; an effect is how a pure reducer asks
  the host to retain bytes, so an opaque handle cannot precede the retention that mints
  it. The journal still keeps a hash, a size and a path, exactly as it does today.

  Crossing that boundary is nonetheless the largest new leak surface in the slice, so the
  bytes travel wrapped in `AiOrchestrator.Contract.SensitiveBytes`: a custom `Inspect` that
  prints class, hash and byte count and nothing else, and deliberately *no* `Jason.Encoder`,
  which turns an accidental journal write into an encode failure rather than a disclosure.
  `Effect.Dispatch` carries its prompt the same way, because a crashed `Run.Server` prints
  its state and `Diagnostic.describe/1` being payload-free does not help if the raw struct
  is what lands in the crash report. Unwrapping happens in exactly two places:
  `PromptStore`, which writes the bytes, and the final `PaneClient` adapter, which sends
  them.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal
  alias AiOrchestrator.Journal.Chain
  alias AiOrchestrator.Lifecycle.Core.Diagnostic
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  @run_id "run_scenario_0001"
  @assignment "as_0001"

  defmodule PaneClient do
    @moduledoc false

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    # This stub stands where the concrete adapter stands, so it is the one place entitled
    # to reveal. It reports the *shape* it was handed rather than the prompt: a stub that
    # forwards raw bytes puts them in a test process mailbox, and ExUnit dumps that mailbox
    # verbatim whenever an `assert_receive` in this file times out.
    def send(pane_ref, prompt, opts) do
      shape =
        if is_struct(prompt, SensitiveBytes) do
          {:wrapped, SensitiveBytes.hash(prompt), SensitiveBytes.byte_size(prompt)}
        else
          {:bare, byte_size(prompt)}
        end

      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, shape, opts[:message_id]}, [])

      # M2 (S1 review): a v2 send reply echoes the pane as well as the message.
      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def reconcile(pane_ref, message_id, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:reconcile_called, pane_ref, message_id, opts[:payload_hash]}, [])

      outcome = Keyword.get(opts, :reconcile_outcome, "absent")

      answer = %{
        "ok" => true,
        "protocol_version" => 2,
        "outcome" => outcome,
        "msg_id" => message_id,
        "pane_id" => pane_ref
      }

      {:ok, if(outcome in ~w(delivered queued ambiguous), do: Map.put(answer, "delivery_attempt", 1), else: answer)}
    end

    def status(pane_ref, opts),
      do: {:ok, Keyword.get(opts, :pane_status, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0})}
  end

  setup do
    root = Path.join(System.tmp_dir!(), "prompt-retention-#{System.unique_integer([:positive])}")
    # The run directory is the caller's, and it exists before anything runs in it: the CLI
    # reads the spec and the plan out of it, and `PromptStore` syncs *its* entry when it
    # creates `prompts/` beneath it, which is a durability claim about a directory that is
    # already there. A root that does not exist makes every put fail as
    # `prompt_dir_create_failed`, which is a fact about this fixture and not about the claim
    # any test below makes -- exactly the RED-that-proves-nothing this file warns about.
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root, fs: FaultFs.new()}
  end

  describe "the object's name is a fact about its bytes" do
    test "the projected path carries the digest the same event carries", ctx do
      assert {:ok, result} = fresh_run(ctx)

      projection = event_data(result, "assignment_prompt_projected")
      "sha256:" <> hex = projection["prompt_hash"]

      assert projection["prompt_path"] == "prompts/#{@assignment}-#{hex}.org",
             """
             `prompts/as_0001.org` is one name for every render an assignment ever
             produces, so a retry overwrites the bytes a journaled hash still refers to.
             Naming the object after its contents makes that unrepresentable rather than
             merely discouraged.
             """

      assert byte_size(hex) == 64
    end

    test "the object on disk is the object the event names", ctx do
      assert {:ok, result} = fresh_run(ctx)
      projection = event_data(result, "assignment_prompt_projected")

      bytes = File.read!(object!(ctx, projection))

      assert "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) ==
               projection["prompt_hash"]

      assert byte_size(bytes) == projection["prompt_bytes"],
             "the journaled size is a fact about the retained object, not about a render nobody kept"
    end
  end

  describe "the bytes are durable before the event that names them" do
    test "retention is observed before the projection is committed", ctx do
      assert {:ok, _result} = fresh_run(ctx)

      assert before?({:retained, @assignment}, {:committed, "assignment_prompt_projected"}),
             """
             The ordering is the guarantee. An event that names an object the store has
             not yet accepted is a promise the journal cannot keep, and replay believes
             the journal.
             """
    end

    test "a put that fails leaves no projection and pastes nothing", ctx do
      # `inject/4` plans a fault on the seam it is given and returns `:ok`; the seam is the
      # handle from `setup` and does not change. Binding its return and passing that as `fs:`
      # hands the host `:ok` where a filesystem goes, which at GREEN is a crash in the seam
      # rather than the fault this test is about -- a RED test that fails for a reason it
      # does not name is a RED test that proves nothing when it goes green.
      FaultFs.inject(ctx.fs, :write, 1, {:error, :enospc})

      assert {:ok, result} = fresh_run(ctx)

      types = Enum.map(result.appended_events, & &1["type"])

      refute "assignment_prompt_projected" in types,
             "no event may name an object the store refused to accept"

      assert "human_attention_required" in types
      assert attention_reason(result) == "prompt_retention_failed"
      assert sent_panes() == [], "nothing was retained, so nothing may be pasted"
      assert result.summary["status"] == "blocked"
    end

    test "a fault in the publish step is reported as itself", ctx do
      FaultFs.inject(ctx.fs, :link, 1, {:error, :eacces})

      assert {:ok, result} = fresh_run(ctx)

      assert attention_reason(result) == "prompt_retention_failed"

      assert attention(result)["detail"]["error"] == "prompt_link_failed",
             """
             "retention failed" is the class a human needs to route on; the named step is
             what tells them whether to look at the disk, the permissions or the code.
             """

      assert sent_panes() == []
    end

    test "an append that fails after a successful put leaves an inert orphan", ctx do
      sink = failing_sink_on("assignment_prompt_projected")

      assert {:error, rejection} = fresh_run(ctx, event_sink: sink)
      assert rejection["reason"] == "journal_append_failed"

      assert length(objects(ctx.root)) == 1,
             """
             The blob outlives the failed append, and that is the correct direction to
             fail in: an unreferenced object is garbage that nothing can read, while an
             event pointing at absent bytes is a lie replay would act on.
             """

      assert sent_panes() == []
    end
  end

  describe "a resumed send reads the committed bytes or stops" do
    test "resume fetches through the store before it reconciles", ctx do
      {lines, _projection} = journal_through_projection(ctx)

      assert {:ok, _result} = resume(ctx, lines)

      assert before?(&match?({:fetched, _}, &1), &match?({:reconcile_called, _, _, _}, &1)),
             """
             Reconciliation binds a payload by hash. Asking the daemon about bytes the
             product has not yet proven it can produce inverts the dependency: a fetch
             failure would surface as a payload conflict.
             """
    end

    test "the reconciled hash is the journaled one, not a fresh render", ctx do
      {lines, projection} = journal_through_projection(ctx)

      assert {:ok, _result} = resume(ctx, lines)

      assert {:reconcile_called, _pane, _msg, payload_hash} =
               Enum.find(timeline(), &match?({:reconcile_called, _, _, _}, &1))

      assert payload_hash == projection["prompt_hash"]
    end

    test "an absent v2 object is a durability fault, never a re-render", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))

      assert {:ok, result} = resume(ctx, lines)

      assert attention(result)["detail"]["error"] == "prompt_object_missing"
      assert sent_panes() == []

      assert objects(ctx.root) == [],
             """
             Re-rendering here would be the original bug wearing a repair's clothes: the
             renderer is a function of the whole event prefix, so the bytes it produced on
             resume are not the bytes the journal committed. Missing evidence is missing.
             """
    end

    test "an object whose bytes drifted stops the send", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.write!(object!(ctx, projection), "not the prompt\n")

      assert {:ok, result} = resume(ctx, lines)

      assert attention(result)["detail"]["error"] == "prompt_hash_mismatch"
      assert sent_panes() == []

      assert File.read!(object!(ctx, projection)) == "not the prompt\n",
             "the damaged object is left for an operator to look at, not quietly rewritten"
    end

    test "an unreadable object is refused rather than worked around", ctx do
      {lines, projection} = journal_through_projection(ctx)

      # A fresh seam for the resume. `inject/4` counts calls per seam, and the run that
      # produced the journal above advanced that count, so an nth-call fault planned on the
      # setup's seam names a call the resume will never make. The plan lives in the seam and
      # the bytes live on disk, so a new seam over the same root is the same filesystem with
      # the count where the phase under test begins.
      fs = FaultFs.new()
      FaultFs.inject(fs, :read, 1, {:error, :eacces})

      assert {:ok, result} = resume(%{ctx | fs: fs}, lines)

      assert attention(result)["detail"]["error"] == "prompt_object_unreadable"
      assert sent_panes() == []
      assert File.exists?(Path.join(ctx.root, projection["prompt_path"]))
    end
  end

  describe "a v1 journal may be migrated exactly once" do
    @legacy_path "prompts/#{@assignment}.org"

    test "an absent v1 object is restored at the path the journal already names", ctx do
      {lines, projection} = journal_through_projection(ctx)
      bytes = File.read!(object!(ctx, projection))
      File.rm!(object!(ctx, projection))

      lines = legacy_journal(ctx, lines)

      assert {:ok, result} = resume(ctx, lines)

      assert sent_panes() != [], "a restored v1 prompt is a normal dispatch"
      assert File.read!(Path.join(ctx.root, @legacy_path)) == bytes

      # The resumed run goes on to request and project the reviewer's assignment; that is a
      # different assignment and a different claim. This one is that the repaired
      # assignment is never projected again.
      assert Enum.all?(result.appended_events, &(not projection_for?(&1, @assignment))), """
      Restoration is a repair of the object the journal already names, not a new
      projection. Publishing the bytes under the v2 content-addressed name instead
      would leave the only journaled reference -- `#{@legacy_path}` -- pointing at
      nothing, so the next cold resume would find the object absent and migrate
      again. "Exactly once" has to be a property of the durable record, not of a
      single process's memory.

      New projections still get content-addressed names; that is pinned by the
      fresh-run tests. This is only about making an old event true.
      """
    end

    test "a second cold resume of a migrated journal re-renders nothing", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))
      lines = legacy_journal(ctx, lines)

      assert {:ok, _first} = resume(ctx, lines)
      reset_timeline()

      assert {:ok, _second} = resume(ctx, lines)

      assert Enum.filter(timeline(), &(&1 == {:retained, @assignment})) == [], """
      The second resume reads the same journal from disk with no memory of the
      first. If it re-renders, the migration was never durable and every restart
      pays for it again -- and on a journal whose render no longer reproduces, every
      restart would block the run instead of one.
      """

      assert {:fetched, @legacy_path} in timeline(), "after the repair it is an ordinary fetch of a present object"
      assert sent_panes() != []
    end

    test "a downgraded journal is still a journal the reader will accept", ctx do
      {lines, _projection} = journal_through_projection(ctx)
      lines = legacy_journal(ctx, lines)

      assert {:ok, loaded} = load_through_reader(ctx, lines)

      assert loaded.lines == lines, """
      `RunFSM.resume` only `Jason.decode!`s the lines it is handed, so a mutation
      that breaks the envelope -- a stale `prev_line_sha256`, a field the schema
      rejects -- passes here and fails in the CLI, which loads through
      `Journal.Reader`. Routing the fixture through the reader first makes this
      file's evidence about a journal the product would actually resume from.
      """

      assert loaded.envelope_version == 2, """
      Envelope version and event version are different things, and neither of them is
      what makes this journal v1. `Writer` has stamped `schema_version: 2` on every
      line it appends since Wave 2, and `Journal.Event` requires `event_version` on
      every line, so the pre-slice journal is a perfectly valid chained v2 envelope
      full of valid v1 events. Its only v1 property is the name in `prompt_path`.
      That, not an unchained or under-specified file, is the migration case.
      """

      # The downgrade is of the object path only: the projection stays at its current version
      # with its recorded baseline (a version-1 event is a different legacy, one MUST-7 blocks
      # before any send), so the reader accepts it and the migration reads as "repair the
      # object the valid journal names", never "repair the invalid journal".
      assert event_version(loaded.lines, "assignment_prompt_projected") == 2
    end

    test "the same journal without its receipt is refused", ctx do
      {lines, _projection} = journal_through_projection(ctx)
      lines = legacy_journal(ctx, lines)

      assert {:error, %{clause: "receipt_missing"}} = load_through_reader(ctx, lines, receipt: :none)

      assert match?({:ok, _}, load_through_reader(ctx, lines)), """
      Without this pair, `load_through_reader/2` could be rejecting every fixture for a
      reason that has nothing to do with the downgrade under test, and the positive
      assertion above would be asserting that the helper is broken in a stable way.
      """
    end

    test "a v1 object that no longer reproduces is not restored", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))

      lines =
        legacy_journal(ctx, lines, fn data ->
          Map.put(data, "prompt_hash", "sha256:" <> String.duplicate("11", 32))
        end)

      assert {:ok, result} = resume(ctx, lines)

      assert attention(result)["detail"]["error"] == "prompt_v1_render_divergent"
      assert sent_panes() == []
      assert objects(ctx.root) == [], "a render that does not match the journal is not the journal's prompt"
    end

    test "a v1 restoration must match the byte count as well as the digest", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))

      lines =
        legacy_journal(ctx, lines, fn data ->
          Map.put(data, "prompt_bytes", projection["prompt_bytes"] + 1)
        end)

      assert {:ok, result} = resume(ctx, lines)

      assert attention(result)["detail"]["error"] == "prompt_v1_render_divergent",
             """
             The ruling requires both. A journal whose two facts about one object disagree
             is not evidence a restoration can rest on, whichever of them the render
             happens to match.
             """

      assert sent_panes() == []
    end

    test "the v1 re-render is attempted once", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))
      lines = legacy_journal(ctx, lines)

      assert {:ok, _result} = resume(ctx, lines)

      assert Enum.count(timeline(), &(&1 == {:retained, @assignment})) == 1,
             "migration is a repair, and a repair that loops is an outage"
    end

    test "restoration is create-only, so a present v1 object is never rewritten", ctx do
      {lines, projection} = journal_through_projection(ctx)
      bytes = File.read!(object!(ctx, projection))
      File.rm!(object!(ctx, projection))
      # A v1 object that is present is one a migration restored, and the store publishes at
      # its own object mode. A file left wider than that is refused as a reused object
      # nobody verified -- a different claim, pinned in the store's own contract -- so the
      # fixture writes the object the way the migration would have.
      File.write!(Path.join(ctx.root, @legacy_path), bytes)
      File.chmod!(Path.join(ctx.root, @legacy_path), 0o600)
      lines = legacy_journal(ctx, lines)

      # Fresh for the same reason as above: publication ran during the projection, so
      # `{:link, 1}` on the setup's seam is a call that already happened.
      fs = FaultFs.new()
      FaultFs.inject(fs, :link, 1, {:error, :eexist})

      assert {:ok, _result} = resume(%{ctx | fs: fs}, lines)

      assert Enum.filter(timeline(), &(&1 == {:retained, @assignment})) == [], """
      A v1 object that is already there is fetched and verified like any other. The
      injected `:eexist` is here to prove the restoration path was not entered at
      all, rather than entered and quietly tolerated: a `link` that could return
      `:eexist` on a legacy name is a race with a concurrent resume, and treating it
      as success would mean publishing over bytes nobody verified.
      """

      assert File.read!(Path.join(ctx.root, @legacy_path)) == bytes
      assert sent_panes() != []
    end
  end

  describe "the crash windows" do
    test "a crash after the blob and before the event re-renders onto the same object", ctx do
      {lines, projection} = journal_through_projection(ctx)
      object = object!(ctx, projection)
      lines = Enum.take_while(lines, &(Jason.decode!(&1)["type"] != "assignment_prompt_projected"))

      assert {:ok, result} = resume(ctx, lines)

      assert event_data(result, "assignment_prompt_projected")["prompt_path"] ==
               projection["prompt_path"],
             """
             The journal never learned about this blob, so replay renders again -- and
             because the object is named by its bytes, the second put lands on the first
             one. Content addressing turns the orphan back into the object rather than
             leaving a second copy beside it.
             """

      # The resumed run goes on to request the reviewer's assignment and retain its prompt;
      # that is a different object, and the claim here is that *this* assignment has exactly
      # one, the one that was already there.
      assert objects(ctx.root, @assignment) == [object]
      assert sent_panes() != []
    end

    test "a crash after the event and before the send re-reads rather than re-renders", ctx do
      {lines, projection} = journal_through_projection(ctx)

      assert {:ok, _result} = resume(ctx, lines)

      refute Enum.any?(timeline(), &(&1 == {:retained, @assignment})),
             """
             This is the window `events_post_prompt.jsonl` describes. The bytes are already
             durable; producing them again would produce different bytes.
             """

      assert {:fetched, projection["prompt_path"]} in timeline(),
             "the fetch names the journaled object, so a drifted path cannot pass unnoticed"
    end

    test "a crash after the send is answered from the receipt with the committed bytes", ctx do
      {lines, projection} = journal_through_projection(ctx)

      assert {:ok, result} = resume(ctx, lines, reconcile_outcome: "delivered")

      assert sent_panes() == [], "a delivered send is reconstructed from the receipt, never repeated"

      assert {:reconcile_called, _pane, _msg, payload_hash} =
               Enum.find(timeline(), &match?({:reconcile_called, _, _, _}, &1))

      assert payload_hash == projection["prompt_hash"]

      assert event_data(result, "assignment_dispatch_sent")["prompt_hash"] == projection["prompt_hash"]
    end

    test "an ambiguous answer blocks even though the bytes verified", ctx do
      {lines, _projection} = journal_through_projection(ctx)

      assert {:ok, result} = resume(ctx, lines, reconcile_outcome: "ambiguous")

      assert sent_panes() == []
      assert "human_attention_required" in Enum.map(result.appended_events, & &1["type"])

      assert result.summary["status"] == "blocked",
             """
             Verifying the payload answers "which bytes"; it does not answer "did they
             land". The store removes one unknown and leaves the other exactly as it was.
             """
    end
  end

  describe "the prompt travels wrapped, so printing a struct does not print a prompt" do
    @sentinel "UNIQUE-PROMPT-SENTINEL-6f3a"

    test "no effect or observation prints the prompt, its path, or its bytes", ctx do
      test_pid = self()

      observer = fn effect, observation ->
        Process.send(
          test_pid,
          {:printed, inspect(effect, limit: :infinity, printable_limit: :infinity),
           inspect(observation, limit: :infinity, printable_limit: :infinity)},
          []
        )
      end

      assert {:ok, result} = fresh_run(ctx, goal: @sentinel, effect_observer: observer)
      projection = event_data(result, "assignment_prompt_projected")
      bytes = File.read!(object!(ctx, projection))
      printed = for {:printed, effect, observation} <- timeline(), text <- [effect, observation], do: text

      assert printed != [], "the run has to have emitted something for this to be evidence of anything"

      for text <- printed do
        refute text =~ @sentinel, """
        `Diagnostic.describe/1` being payload-free does not help here, because a
        crash report, a `Logger.error` interpolating an effect, and an `IO.inspect`
        left in an unexercised branch all print the struct directly. The redaction
        has to live on the value.
        """

        refute text =~ ctx.root, "an absolute path is a payload too: it names where the bytes are"
        refute String.contains?(text, String.slice(bytes, 0, 40))
      end
    end

    test "raising the inspect limits does not undo the redaction", ctx do
      test_pid = self()
      observer = fn effect, _observation -> Process.send(test_pid, {:effect, effect}, []) end

      assert {:ok, _result} = fresh_run(ctx, goal: @sentinel, effect_observer: observer)

      assert [%Effect.RetainPrompt{bytes: sensitive} | _] =
               for({:effect, %Effect.RetainPrompt{} = effect} <- timeline(), do: effect)

      assert SensitiveBytes.reveal(sensitive) =~ @sentinel,
             "the wrapper is carrying the real prompt; the point is that only an explicit reveal gets it"

      refute inspect(sensitive, limit: :infinity, printable_limit: :infinity) =~ @sentinel, """
      Raising the limits is what a person does when the redacted form is not telling
      them enough, and it is what `Logger`'s own truncation handling does on the way
      to a crash report. Neither may become the way the prompt gets printed.
      """

      # Dynamic: a literal wrong-type call is a compile-time type warning bin/verify counts.
      chars = String.to_existing_atom("Elixir.String.Chars")
      assert_raise Protocol.UndefinedError, fn -> chars.to_string(sensitive) end
      assert_raise Protocol.UndefinedError, fn -> Jason.encode!(sensitive) end

      assert SensitiveBytes.hash(sensitive) =~ ~r/^sha256:[0-9a-f]{64}$/,
             "the wrapper answers the questions a log line actually needs, so nothing has to reach past it"

      # Deliberately not asserted: `inspect(.., structs: false)`, `:sys.get_state/1`, a
      # debugger, and `:erlang.process_info(pid, :binary)` all read the value's storage
      # directly. A struct holding bytes by value cannot honestly promise otherwise. What
      # is promised here is narrower and is the thing that leaks in practice: every
      # ordinary printing, stringifying and encoding path prints facts, and getting the
      # bytes requires writing `reveal/1` where a reviewer can see it.
    end

    # This one is a static guard, not a behavioural RED: at RED there is no `reveal/1`
    # caller in `lib/` at all, so it passes vacuously. It is here to fail later, on the
    # commit that adds an unwrap somewhere convenient, and it is excluded from this
    # slice's RED accounting for exactly that reason.
    test "reveal/1 is called only where the bytes leave the system" do
      allowed =
        MapSet.new([
          "lib/ai_orchestrator/dispatch/prompt_store.ex",
          "lib/ai_orchestrator/dispatch/pane_client.ex"
        ])

      callers =
        "lib/**/*.ex"
        |> Path.wildcard()
        |> Enum.filter(&(File.read!(&1) =~ "SensitiveBytes.reveal("))
        |> MapSet.new()

      assert MapSet.subset?(callers, allowed), """
      Revealing is the whole risk surface, so it has to stay countable by reading two
      files. Every other module either carries the wrapper or asks it a question.
      Unexpected callers: #{inspect(MapSet.to_list(MapSet.difference(callers, allowed)))}
      """
    end

    test "the prompt reaches the pane still wrapped", ctx do
      assert {:ok, _result} = fresh_run(ctx, goal: @sentinel)

      shapes = for {:send_called, _pane, shape, _msg} <- timeline(), do: shape

      assert shapes != [], "the run has to have sent something for this to be evidence of anything"

      for shape <- shapes do
        assert match?({:wrapped, _hash, _size}, shape), """
        Unwrapping in the host, or in the store's return, hands a bare prompt to every
        layer between there and the adapter. `reveal/1` belongs at the call that writes
        the bytes out, which is the last place they can still be redacted.
        """
      end
    end

    test "the dispatch command carries the wrapper, so encoding it raises", ctx do
      test_pid = self()
      observer = fn effect, _observation -> Process.send(test_pid, {:effect, effect}, []) end

      assert {:ok, _result} = fresh_run(ctx, goal: @sentinel, effect_observer: observer)

      assert [%Effect.Dispatch{command: command} | _] =
               for({:effect, %Effect.Dispatch{} = effect} <- timeline(), do: effect)

      assert is_struct(command["prompt"], SensitiveBytes), """
      The prompt reaches the pane inside the dispatch command, which is the longest
      lived of the three carriers: it is held in reducer state across the whole
      dispatch step. `Run.Server` printing its state must not print it.
      """

      assert_raise Protocol.UndefinedError, fn -> Jason.encode!(command) end

      refute inspect(command, limit: :infinity, printable_limit: :infinity) =~ @sentinel
    end

    test "what the journal keeps is still the hash and the size, not the bytes", ctx do
      assert {:ok, result} = fresh_run(ctx, goal: @sentinel)

      for event <- result.appended_events do
        refute Jason.encode!(event) =~ @sentinel,
               "the boundary ruling widened where bytes may travel, not what may be committed"
      end

      projection = event_data(result, "assignment_prompt_projected")
      bytes = File.read!(object!(ctx, projection))

      assert projection["prompt_hash"] == "sha256:" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
      assert projection["prompt_bytes"] == byte_size(bytes)
    end
  end

  describe "a failure is evidence an operator can act on, and nothing more" do
    test "a retention failure names the class, the step and the render it was about", ctx do
      FaultFs.inject(ctx.fs, :link, 1, {:error, :eacces})

      assert {:ok, result} = fresh_run(ctx, goal: @sentinel)

      detail = attention(result)["detail"]

      assert attention_reason(result) == "prompt_retention_failed"
      assert detail["error"] == "prompt_link_failed"
      assert detail["stage"] == "retain"
      assert detail["prompt_hash"] =~ ~r/^sha256:[0-9a-f]{64}$/
      assert is_integer(detail["prompt_bytes"])

      assert detail |> Map.keys() |> Enum.sort() == ["error", "prompt_bytes", "prompt_hash", "stage"], """
      The keys are the contract. `error` routes, `stage` says which half of the store
      was running, and the hash and size identify the render without reproducing it.
      A detail map that grows a key per call site is a detail map an alert rule cannot
      be written against.

      There is no `prompt_path` here on purpose: publication is what failed, so no
      object has a name yet, and inventing one would point an operator at a file that
      does not exist.
      """
    end

    test "a fetch failure points at the object by its relative name and its digest", ctx do
      {lines, projection} = journal_through_projection(ctx)
      File.rm!(object!(ctx, projection))

      assert {:ok, result} = resume(ctx, lines)

      detail = attention(result)["detail"]

      assert detail["error"] == "prompt_object_missing"
      assert detail["stage"] == "fetch"
      assert detail["prompt_path"] == projection["prompt_path"]
      assert detail["prompt_hash"] == projection["prompt_hash"]

      refute String.starts_with?(detail["prompt_path"], "/"), """
      The reference is relative for the same reason the projection's is: an operator
      may have moved the run directory, and an absolute path recorded at write time
      is then a path to somewhere else. It also keeps the machine's layout out of an
      artifact that gets pasted into tickets.
      """
    end

    test "no attention event carries the prompt, a path off the run, or an inspected term", ctx do
      failures = [
        # One seam each: these run in sequence against one root, and a shared seam would
        # carry the first failure's call count into the second, where `{:write, 1}` names a
        # write that has already happened and no fault fires at all.
        fn -> fresh_run(%{ctx | fs: seam(:link, 1, {:error, :eacces})}, goal: @sentinel) end,
        fn -> fresh_run(%{ctx | fs: seam(:write, 1, {:error, :enospc})}, goal: @sentinel) end,
        fn ->
          {lines, projection} = journal_through_projection(ctx, goal: @sentinel)
          File.write!(object!(ctx, projection), "not the prompt\n")
          resume(ctx, lines)
        end
      ]

      for failure <- failures do
        assert {:ok, result} = failure.()

        assert data = attention(result),
               "the run reported no human_attention_required event, so there is no record to inspect"

        json = Jason.encode!(data)

        refute json =~ @sentinel, "the prompt is the payload; a failure about it is not a place to quote it"
        refute json =~ ctx.root, "an absolute path is a payload too"
        refute json =~ "SensitiveBytes"

        refute json =~ ~r/[%{]\{|#PID|#Reference|#Function/, """
        `inspect(reason)` is how a typed failure becomes an untyped string: it renders
        whatever the seam happened to return, including terms that carry pids, refs and
        closures, and it changes shape between OTP releases. Every value here is a
        string or a number a reader chose.
        """

        detail = data["detail"]

        assert is_map(detail),
               "the attention event carries no detail map, so there is nothing to constrain"

        assert Enum.all?(Map.values(detail), &(is_binary(&1) or is_integer(&1))),
               "a nested term in the detail is an escape hatch the next failure will use"

        assert detail["error"] =~ ~r/^[a-z][a-z0-9_]*$/
        assert detail["stage"] in ["retain", "fetch"]
      end
    end

    test "the failed observation is described by its kind, not by its contents", ctx do
      FaultFs.inject(ctx.fs, :link, 1, {:error, :eacces})

      assert {:ok, _result} =
               fresh_run(ctx,
                 goal: @sentinel,
                 effect_observer: fn _effect, observation ->
                   send(self(), {:described, observation.__struct__, Diagnostic.describe(observation)})
                   :ok
                 end
               )

      described =
        for {:described, module, described} <- drain(),
            module == Observation.PromptRetentionFailed,
            do: described

      assert [%{"kind" => "prompt_retention_failed"} = only] = described

      refute only |> Jason.encode!() |> String.contains?(@sentinel), """
      `Diagnostic.describe/1` is the boundary every log line and telemetry event goes
      through, so a struct that leaks there leaks everywhere at once. Its closed
      known-set is what keeps that from being a per-call-site decision.
      """
    end

    # Everything above is about the record a failure leaves behind. The four tests below are
    # about the value the host hands the reducer on the way there, and about where it acquires
    # its shape.
    #
    # `PromptStore` reports a rejection as a pair -- `{:prompt_link_failed, :eacces}` -- and
    # `Observation.PromptRetentionFailed` types its `reason` as a `map()`. Something performs
    # that translation. It is the host, at the effect where it ran the store, before the
    # observation is handed to the reducer.
    #
    # Anywhere later is worse, for a reason that is not stylistic. The reducer is pure and
    # total over the observations the contract admits, so giving it the raw pair means either
    # teaching it the rejection vocabulary -- a second copy of the table, free to drift from
    # the first -- or carrying the pair onward, in which case the store's own atoms reach the
    # journal writer with whatever the store chose still attached to them.
    #
    # And the class does not survive that far: the journal's detail keys above are fixed and
    # `class` is not among them, deliberately, because `error` is what an alert routes on. So
    # the observation is the only place the normalized pair is ever visible, and these are the
    # only tests that can look at it. The last of the four asserts both halves at once.
    test "the retain boundary hands on an errno as two names, not as the store's pair", ctx do
      FaultFs.inject(ctx.fs, :link, 1, {:error, :eacces})

      assert {:ok, _result} = fresh_run(ctx, goal: @sentinel, effect_observer: capture_observations())

      assert only_rejection(Effect.RetainPrompt, Observation.PromptRetentionFailed) ==
               %{"reason" => "prompt_link_failed", "class" => "eacces"},
             """
             The store refused the publish with `{:prompt_link_failed, :eacces}`. A pair
             arriving here is a pair the reducer has to destructure, and destructuring it is
             how the vocabulary acquires its second home.
             """
    end

    test "the retain boundary carries a class the store chose, not only ones the kernel did", ctx do
      FaultFs.inject(ctx.fs, :write, 1, {:torn, 7})

      assert {:ok, _result} = fresh_run(ctx, goal: @sentinel, effect_observer: capture_observations())

      assert only_rejection(Effect.RetainPrompt, Observation.PromptRetentionFailed) ==
               %{"reason" => "prompt_write_failed", "class" => "torn_write"},
             """
             Every other class under a store reason is an errno the kernel returned.
             `:torn_write` is the store's own word for a write that reported success and moved
             fewer bytes than it was given, which no errno describes. Normalizing by naming
             the atom rather than by looking it up in an errno list is what keeps it reachable.
             """
    end

    test "the fetch boundary keeps the class the journal is about to drop", ctx do
      {lines, _projection} = journal_through_projection(ctx, goal: @sentinel)

      assert {:ok, result} =
               resume(%{ctx | fs: seam(:read, 1, {:error, :eacces})}, lines, effect_observer: capture_observations())

      assert only_rejection(Effect.FetchPrompt, Observation.PromptFetchFailed) ==
               %{"reason" => "prompt_object_unreadable", "class" => "eacces"}

      detail = attention(result)["detail"]

      assert detail["error"] == "prompt_object_unreadable"

      refute Map.has_key?(detail, "class"), """
      One failure, two boundaries, and the class is present at one and absent at the other on
      purpose. `:eacces` is what an operator reads to know the object is there and unreadable
      rather than gone; the journal is not where they read it, because a detail map that grows
      a routing dimension is one an alert rule cannot be written against. Dropping it is a
      decision, so it is asserted next to the place it was still there.
      """
    end

    test "a rejection carrying a path arrives as its reason and no class, without the path", ctx do
      {lines, projection} = journal_through_projection(ctx, goal: @sentinel)

      # `object_path/2` rather than `object!/2`, and this is the one test in the file that
      # wants the difference. The other callers read, overwrite or assert restoration, so an
      # object that never landed makes their arrangement a lie and `object!` says so. Here
      # absence is the arrangement. Requiring presence first would make this test fail on a
      # precondition instead of on its claim, which is exactly the RED that proves nothing --
      # and it would assert a fact this test does not own. That the run publishes an object at
      # all is pinned by its neighbours, once, where it is the claim.
      File.rm_rf!(object_path(ctx, projection))

      assert {:ok, _result} = resume(ctx, lines, effect_observer: capture_observations())

      reason = only_rejection(Effect.FetchPrompt, Observation.PromptFetchFailed)

      assert reason == %{"reason" => "prompt_object_missing", "class" => nil}, """
      `prompt_object_missing` reports the path it could not find, because the path is what its
      caller needs in order to go look. The observation is not its caller: it is the value the
      reducer steps on and the writer journals from, and a path that reaches it has left the
      run. `nil` rather than an absent key so that every rejection is one map shape, and a
      consumer reading `class` gets an answer instead of a `KeyError` for two reasons out of
      thirty.
      """

      encoded = Jason.encode!(reason)

      refute encoded =~ projection["prompt_path"]
      refute encoded =~ ctx.root
    end
  end

  # Records every effect the host performed together with the observation it completed it
  # with. The pair is the point: the claim under test is about a particular boundary, and a
  # probe that kept only observations could not tell a retention failure raised where the
  # store runs from the same struct arriving from somewhere else.
  defp capture_observations do
    test_pid = self()

    fn effect, observation -> Process.send(test_pid, {:observed, effect, observation}, []) end
  end

  # The single rejection a boundary produced, or a failure naming what the host did instead.
  # In RED that failure is the expected one and it is not subtle: the reducer emits neither
  # prompt effect yet, so there is no boundary to normalize at and nothing is captured.
  defp only_rejection(effect_module, observation_module) do
    performed =
      for {:observed, effect, observation} <- drain(),
          do: {effect.__struct__, observation.__struct__, observation}

    matched = for {^effect_module, ^observation_module, observation} <- performed, do: observation.reason
    boundaries = for {effect, observation, _reason} <- performed, do: {effect, observation}

    case matched do
      [reason] ->
        reason

      other ->
        flunk("""
        Expected one #{inspect(observation_module)} completing #{inspect(effect_module)} and got
        #{inspect(other)}. The boundaries the host actually reached, as effect and the
        observation it was completed with, were #{inspect(boundaries)}.
        """)
    end
  end

  # A seam carrying exactly one planned fault, for tests that need more than one of them.
  defp seam(op, injection, fault) do
    fs = FaultFs.new()
    FaultFs.inject(fs, op, injection, fault)
    fs
  end

  defp fresh_run(ctx, extra_opts \\ []) do
    {goal, extra_opts} = Keyword.pop(extra_opts, :goal)
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    spec = if goal, do: Map.put(spec, "goal", goal), else: spec
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.run(spec, plan, [run_id: @run_id] ++ fsm_opts(ctx, extra_opts))
  end

  # The third argument is dispatch options, as every existing caller passes it, except for
  # `:effect_observer`, which is a host seam and is lifted out. It is only forwarded when
  # given: passing `effect_observer: nil` would not fall back to the default probe, it would
  # silence it, and the ordering tests read that probe.
  defp resume(ctx, lines, extra_opts \\ []) do
    {observer, dispatch_extra} = Keyword.pop(extra_opts, :effect_observer)
    host_extra = if observer, do: [effect_observer: observer], else: []
    spec = F.json("scenarios", "kill9_resume", "spec.json")
    plan = F.json("scenarios", "kill9_resume", "plan.json")

    RunFSM.resume(spec, plan, lines, fsm_opts(ctx, [dispatch_extra: dispatch_extra] ++ host_extra))
  end

  # A fresh run, stopped at the moment the projection became durable. Building the resume
  # journal from a real run rather than a checked-in fixture keeps the hash, the byte count
  # and the retained object consistent with each other by construction, which is the whole
  # property these tests are about.
  defp journal_through_projection(ctx, opts \\ []) do
    assert {:ok, result} = fresh_run(ctx, opts)
    reset_timeline()

    events = Enum.take_while(result.events, &(&1["type"] != "assignment_dispatch_sent"))
    projection = events |> Enum.find(&(&1["type"] == "assignment_prompt_projected")) |> Map.fetch!("data")

    # The journal is truncated to the crash point; the filesystem has to be too. The run that
    # produced these lines went on to request and retain a reviewer's prompt that, at the
    # moment this journal ends, had not happened yet. An object for an assignment the journal
    # never requested is not an orphan the crash left behind -- it is the future leaking into
    # the fixture, and every assertion about "what is on disk" below would be about it.
    requested = for %{"type" => "assignment_requested", "data" => %{"assignment_id" => id}} <- events, do: id

    for path <- objects(ctx.root),
        not Enum.any?(requested, &String.starts_with?(Path.basename(path), &1 <> "-")),
        do: File.rm!(path)

    {Enum.map(events, &Jason.encode!/1), projection}
  end

  # A journal as it would have been written before this slice: a chained v2 envelope whose
  # `assignment_prompt_projected` names the prompt by the un-digested `prompts/<id>.org`
  # path. That name is the whole v1 signal; the event itself is an ordinary, valid
  # `event_version: 1` event, because `Journal.Event` has required `event_version` on every
  # line since it existed and a journal without it is one this product never wrote.
  # `after_downgrade` is the per-test damage, applied to the projection's data before the
  # chain is rebuilt.
  defp legacy_journal(ctx, lines, after_downgrade \\ & &1) do
    # A legacy projection here is a legacy OBJECT PATH (the version-1 naming scheme) on an
    # otherwise current projection: the event version and its recorded baseline are kept,
    # because a version-1 event (no baseline) is a different legacy -- one that blocks for
    # attention before any send (MUST-7) and is pinned in the baseline and version tests.
    lines =
      lines
      |> map_event("assignment_prompt_projected", fn event ->
        Map.update!(event, "data", fn data ->
          data
          |> Map.put("prompt_path", @legacy_path)
          |> then(after_downgrade)
        end)
      end)
      |> rechain()

    assert {:ok, %{lines: ^lines}} = load_through_reader(ctx, lines)
    lines
  end

  # Rewriting a line changes its exact bytes, and every following `prev_line_sha256` is a
  # hash of those bytes. Mutating in place without rebuilding the chain produces a file the
  # reducer will happily decode and `Journal.Reader` will refuse -- which is the opposite of
  # what a compatibility fixture should be.
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

  # A chained v2 journal with no `events.head` is not a valid journal: `Chain.reconcile`
  # rejects it as `receipt_missing`. Writing the lines and nothing else would have made
  # this helper answer `{:error, ..}` for every fixture and prove nothing about the
  # fixture's *content*, so the receipt is part of the fixture.
  defp load_through_reader(ctx, lines, opts \\ []) do
    run_dir = Path.join(ctx.root, "reader-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    File.write!(Path.join(run_dir, "events.jsonl"), Enum.map(lines, &(&1 <> "\n")))

    case Keyword.get(opts, :receipt, :tail) do
      :tail -> File.write!(Path.join(run_dir, "events.head"), tail_receipt(lines))
      :none -> :ok
    end

    Journal.Reader.load(run_dir)
  end

  defp tail_receipt(lines) do
    Chain.encode_receipt(%{
      seq: length(lines),
      line_sha256: Chain.line_sha256(List.last(lines) <> "\n"),
      updated_at: DateTime.to_iso8601(DateTime.utc_now())
    })
  end

  defp event_version(lines, type) do
    lines
    |> Enum.map(&Jason.decode!/1)
    |> Enum.find(&(&1["type"] == type))
    |> Map.fetch!("event_version")
  end

  defp map_event(lines, type, fun) do
    Enum.map(lines, fn line ->
      event = Jason.decode!(line)
      if event["type"] == type, do: Jason.encode!(fun.(event)), else: line
    end)
  end

  defp fsm_opts(ctx, extra_opts) do
    {dispatch_extra, extra_opts} = Keyword.pop(extra_opts, :dispatch_extra, [])
    fixture_events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)
    artifact_by_assignment = fixture_data_by_assignment(fixture_events, "artifact_observed")
    gate_pass = fixture_events |> fixture_data("gate_passed") |> Map.delete("gate_run_id")
    test_pid = self()

    [
      fs: ctx.fs,
      prompt_root: ctx.root,
      dispatch: LocalPane,
      dispatch_opts:
        [
          artifact_reader: fn command -> {:ok, Map.fetch!(artifact_by_assignment, command["assignment_id"])} end,
          pane_client: PaneClient,
          test_pid: test_pid
        ] ++ dispatch_extra,
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _gate_opts -> {:ok, gate_pass} end],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end,
      effect_observer: Keyword.get(extra_opts, :effect_observer, &observe_effect(test_pid, &1, &2)),
      event_sink: GateDouble.receipt(Keyword.get(extra_opts, :event_sink, &commit_probe(test_pid, &1)))
    ]
  end

  defp observe_effect(test_pid, %Effect.RetainPrompt{}, %Observation.PromptRetained{object: object}),
    do: Process.send(test_pid, {:retained, object.assignment_id}, [])

  defp observe_effect(test_pid, %Effect.FetchPrompt{object: %PromptObject{path: path}}, %Observation.PromptFetched{}),
    do: Process.send(test_pid, {:fetched, path}, [])

  defp observe_effect(_test_pid, _effect, _observation), do: :ok

  defp commit_probe(test_pid, event) do
    Process.send(test_pid, {:committed, event["type"]}, [])
    {:ok, event}
  end

  defp failing_sink_on(type) do
    test_pid = self()

    fn event ->
      if event["type"] == type do
        {:error, %{"reason" => "disk_full"}}
      else
        commit_probe(test_pid, event)
      end
    end
  end

  # ---- reading what happened ----

  # Messages can only be received once, so the timeline is accumulated rather than
  # re-read: an assertion about ordering and a later assertion about what was sent are
  # both asking about the same single history.
  defp timeline do
    accumulated = Process.get(:timeline, []) ++ drain()
    Process.put(:timeline, accumulated)
    accumulated
  end

  defp reset_timeline do
    drain()
    Process.put(:timeline, [])
  end

  defp drain do
    receive do
      message -> [message | drain()]
    after
      0 -> []
    end
  end

  defp before?(earlier, later) when is_function(earlier, 1) and is_function(later, 1) do
    history = timeline()

    with earlier_at when is_integer(earlier_at) <- Enum.find_index(history, earlier),
         later_at when is_integer(later_at) <- Enum.find_index(history, later) do
      earlier_at < later_at
    else
      nil -> flunk("one of the two events never happened: #{inspect(labels(history))}")
    end
  end

  defp before?(earlier, later), do: before?(&(&1 == earlier), &(&1 == later))

  # A timeline is the one place a whole prompt would otherwise reach a CI log: the pane
  # stub records the bytes it was handed. Ordering is a fact about labels, so the failure
  # message carries labels.
  defp labels(history) do
    Enum.map(history, fn
      {:send_called, pane, _prompt, msg_id} -> {:send_called, pane, msg_id}
      {:committed, type} -> {:committed, type}
      other -> other
    end)
  end

  defp sent_panes, do: for({:send_called, pane, _prompt, _msg} <- timeline(), do: pane)

  defp event_data(result, type) do
    case Enum.find(result.events, &(&1["type"] == type)) do
      nil -> nil
      event -> event["data"]
    end
  end

  defp attention(result), do: event_data(result, "human_attention_required")

  defp projection_for?(%{"type" => "assignment_prompt_projected", "data" => %{"assignment_id" => id}}, id), do: true
  defp projection_for?(_event, _assignment_id), do: false
  defp attention_reason(result), do: attention(result)["reason"]

  # Every arrangement below reads, removes or overwrites the retained object. If
  # retention has not landed, `File.read!/1` reports "no such file or directory",
  # which describes the arrangement rather than the claim, and a reader of the RED
  # cannot tell a missing implementation from a broken fixture. Naming the absence
  # here keeps the failure about the object the event promised.
  defp object!(ctx, projection) do
    path = Path.join(ctx.root, projection["prompt_path"])

    assert File.exists?(path),
           "the run journaled #{projection["prompt_path"]} but wrote no object there"

    path
  end

  # The same path with no claim about what is at it, for the one arrangement that is about the
  # absence rather than about the bytes.
  defp object_path(ctx, projection), do: Path.join(ctx.root, projection["prompt_path"])

  defp objects(root), do: [root, "prompts", "*.org"] |> Path.join() |> Path.wildcard() |> Enum.sort()

  defp objects(root, assignment_id),
    do: Enum.filter(objects(root), &String.starts_with?(Path.basename(&1), assignment_id <> "-"))

  defp fixture_data(events, type), do: events |> Enum.find(&(&1["type"] == type)) |> Map.fetch!("data")

  defp fixture_data_by_assignment(events, type) do
    events
    |> Enum.filter(&(&1["type"] == type))
    |> Map.new(fn event -> {Map.fetch!(event["data"], "assignment_id"), event["data"]} end)
  end
end
