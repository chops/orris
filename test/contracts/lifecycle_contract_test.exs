defmodule AiOrchestrator.Contracts.LifecycleContractTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Lifecycle.Core.Diagnostic

  @now %Moment{wall_ts: "2026-09-03T20:00:00Z", unix: 1_788_400_000}

  test "command binds the authorized stamp, arguments, run, and time fact" do
    stamp = %{
      "class" => "operator",
      "id" => "local_operator",
      "command_id" => "cmd_01J9X3T2QF5G7H8K1N3P",
      "verb" => "start",
      "args_hash" => "sha256:" <> String.duplicate("0", 64)
    }

    command = %Command{
      requested_by: stamp,
      run_id: "run_contract_0001",
      args: %{"plan_hash" => "sha256:plan", "spec_hash" => "sha256:spec"},
      now: @now
    }

    assert command.requested_by == stamp
    assert command.now == @now
  end

  test "effect variants are data-only tagged values" do
    effects = [
      %Effect.Clock{read_index: 0, purpose: "assignment_deadline"},
      %Effect.Dispatch{
        assignment_id: "as_0001",
        command: %{"pane_ref" => "writer"},
        message_id: "msg_0001"
      },
      %Effect.Observe{
        assignment_id: "as_0001",
        command: %{
          "artifact_baseline" => %{},
          "artifact_id" => "art_0001",
          "assignment_id" => "as_0001",
          "expected_artifact" => "lib/example.ex",
          "pane_ref" => "writer",
          "repo_root" => "/repo",
          "stable_for_ms" => 100
        },
        deadline_unix: @now.unix + 60
      },
      %Effect.ReadReview{assignment_id: "as_0001", path: "review/item.org"},
      %Effect.RunGate{
        gate_run_id: "gr_0001",
        requested: %{
          "artifact_ids" => ["art_0001"],
          "assignment_id" => "as_0001",
          "command_argv" => ["mix", "test"],
          "gate_id" => "test",
          "gate_run_id" => "gr_0001",
          "timeout_s" => 60,
          "work_item_id" => "item_0001"
        },
        repo_root: "/repo",
        run_dir: "/run"
      },
      %Effect.Notify{notification_id: "not_0001", hook_argv: ["notify"], payload: %{}},
      %Effect.Timer{purpose: "assignment_deadline", deadline_unix: @now.unix + 60}
    ]

    assert Enum.map(effects, & &1.__struct__) == [
             Effect.Clock,
             Effect.Dispatch,
             Effect.Observe,
             Effect.ReadReview,
             Effect.RunGate,
             Effect.Notify,
             Effect.Timer
           ]
  end

  test "observation variants carry the effect result and observation time" do
    observations = [
      %Observation.Clock{read_index: 0, now: @now},
      %Observation.Dispatched{assignment_id: "as_0001", result: %{}, now: @now},
      %Observation.DispatchFailed{assignment_id: "as_0001", reason: %{}, now: @now},
      %Observation.SendReconciled{assignment_id: "as_0001", outcome: "delivered", delivery_attempt: 1, now: @now},
      %Observation.SendReconcileFailed{assignment_id: "as_0001", reason: %{}, now: @now},
      %Observation.ArtifactObserved{assignment_id: "as_0001", artifact: %{}, now: @now},
      %Observation.ArtifactSnapshot{assignment_id: "as_0001", baseline: %{"exists" => false}, now: @now},
      %Observation.ArtifactSnapshotFailed{assignment_id: "as_0001", reason: %{}, now: @now},
      %Observation.Blocked{assignment_id: "as_0001", reason: %{}, now: @now},
      %Observation.Pending{assignment_id: "as_0001", details: %{}, now: @now},
      %Observation.ObserveFailed{assignment_id: "as_0001", reason: %{}, now: @now},
      %Observation.TimedOut{assignment_id: "as_0001", deadline_unix: @now.unix, now: @now},
      %Observation.ReviewRead{assignment_id: "as_0001", contents: "CLEAN", now: @now},
      %Observation.ReviewUnreadable{
        assignment_id: "as_0001",
        path: "review.org",
        reason: %{"reason" => "review_unreadable", "class" => "enoent"},
        now: @now
      },
      %Observation.GateFinished{gate_run_id: "gr_0001", result: %{}, now: @now},
      %Observation.GateFailed{gate_run_id: "gr_0001", result: %{}, now: @now},
      %Observation.GateError{
        gate_run_id: "gr_0001",
        reason: %{
          "reason" => "gate_runner_crashed",
          "result_class" => "map",
          "digest" => "sha256:" <> String.duplicate("0", 64)
        },
        now: @now
      },
      %Observation.Notified{notification_id: "not_0001", result: %{}, now: @now},
      %Observation.NotifyFailed{notification_id: "not_0001", reason: %{}, now: @now},
      %Observation.Deadline{purpose: "assignment_deadline", deadline_unix: @now.unix, now: @now}
    ]

    assert Enum.all?(observations, &(&1.now == @now))
  end

  test "required fields fail at construction" do
    assert_raise ArgumentError, fn -> struct!(Command, requested_by: %{}) end
    assert_raise ArgumentError, fn -> struct!(Effect.Timer, purpose: "deadline") end
    assert_raise ArgumentError, fn -> struct!(Observation.Deadline, purpose: "deadline") end
    assert_raise ArgumentError, fn -> struct!(Effect.RetainPrompt, assignment_id: "as") end
    assert_raise ArgumentError, fn -> struct!(Effect.FetchPrompt, []) end
    assert_raise ArgumentError, fn -> struct!(Observation.PromptRetained, now: @now) end
    assert_raise ArgumentError, fn -> struct!(Observation.PromptFetched, now: @now) end
    assert_raise ArgumentError, fn -> struct!(PromptObject, path: "prompts/as.org") end

    assert_raise KeyError, fn -> struct!(Effect.FetchPrompt, assignment_id: "as", object: object()) end

    assert_raise KeyError, fn ->
      struct!(Observation.PromptRetained, assignment_id: "as", object: object(), now: @now)
    end
  end

  test "each effect has an exhaustive, typed observation family" do
    effects = [
      {%Effect.Clock{read_index: 0, purpose: "deadline"}, [Observation.Clock]},
      {%Effect.Dispatch{assignment_id: "as", command: %{}, message_id: "send"},
       [Observation.Dispatched, Observation.DispatchFailed]},
      {%Effect.Observe{assignment_id: "as", command: %{}, deadline_unix: 1},
       [
         Observation.ArtifactObserved,
         Observation.Blocked,
         Observation.Pending,
         Observation.ObserveFailed,
         Observation.TimedOut
       ]},
      {%Effect.ReadReview{assignment_id: "as", path: "review.org"},
       [Observation.ReviewRead, Observation.ReviewUnreadable]},
      {%Effect.RunGate{gate_run_id: "gr", requested: %{}, repo_root: "/repo", run_dir: "/run"},
       [Observation.GateFinished, Observation.GateFailed, Observation.GateError]},
      {%Effect.Notify{notification_id: "not", hook_argv: ["notify"], payload: %{}},
       [Observation.Notified, Observation.NotifyFailed]},
      {%Effect.Timer{purpose: "deadline", deadline_unix: 1}, [Observation.Deadline]},
      {%Effect.RetainPrompt{assignment_id: "as", bytes: SensitiveBytes.new("prompt", :prompt)},
       [Observation.PromptRetained, Observation.PromptRetentionFailed]},
      {%Effect.FetchPrompt{object: object()}, [Observation.PromptFetched, Observation.PromptFetchFailed]},
      {%Effect.ReconcileSend{assignment_id: "as", command: %{}},
       [Observation.SendReconciled, Observation.SendReconcileFailed]},
      {%Effect.SnapshotArtifact{assignment_id: "as", command: %{}},
       [Observation.ArtifactSnapshot, Observation.ArtifactSnapshotFailed]},
      # unit-2 C4: the orchestrated gate route (docs/contracts/gate-execution-wiring.org)
      {%Effect.PrepareGate{
         gate_run_id: "gr_0001",
         attempt: 1,
         requested: %{},
         deadline_unix: 1,
         repo_root: "/r",
         run_dir: "/d"
       }, [Observation.GatePrepared, Observation.GatePrepareFailed]},
      {%Effect.ReleaseGate{gate_run_id: "gr_0001", attempt: 1, started_seq: 1},
       [Observation.GateReleased, Observation.GateReleaseFailed]},
      {%Effect.AwaitGate{gate_run_id: "gr_0001", attempt: 1, deadline_unix: 1},
       [Observation.GateFinished, Observation.GateFailed, Observation.GateUnsettled, Observation.GateError]},
      {%Effect.ReconcileGate{gate_run_id: "gr_0001", attempt: 1, expected: %{}},
       [Observation.GateReconciled, Observation.GateReconcileFailed]}
    ]

    assert Enum.all?(effects, fn {effect, observations} ->
             Effect.admissible_observations(effect) == observations
           end)
  end

  test "every effect the contract defines is in that table" do
    tabled =
      MapSet.new([
        Effect.Clock,
        Effect.Dispatch,
        Effect.Observe,
        Effect.ReadReview,
        Effect.RunGate,
        Effect.Notify,
        Effect.Timer,
        Effect.RetainPrompt,
        Effect.FetchPrompt,
        Effect.ReconcileSend,
        Effect.SnapshotArtifact,
        Effect.PrepareGate,
        Effect.ReleaseGate,
        Effect.AwaitGate,
        Effect.ReconcileGate
      ])

    assert defined_structs_under(Effect) == tabled, """
    An exhaustive table is only exhaustive against a list someone remembered to
    extend. Enumerating the loaded modules instead makes an effect that is added to
    the reducer without an observation family -- and therefore without a host clause
    to serve it -- fail here rather than at the first run that emits it.

    `Effect.RetainPrompt` and `Effect.FetchPrompt` are the two this slice adds, and
    they are the two `Host.execute/observe` gain clauses for. `Host.drive/3` and
    `commit/3` are untouched: an effect emitted with no events is already served
    against an unmoved journal.
    """
  end

  defp defined_structs_under(namespace) do
    prefix = Atom.to_string(namespace) <> "."

    :ai_orchestrator
    |> :application.get_key(:modules)
    |> elem(1)
    |> Enum.filter(fn module ->
      String.starts_with?(Atom.to_string(module), prefix) and Code.ensure_loaded?(module) and
        function_exported?(module, :__struct__, 0)
    end)
    |> MapSet.new()
  end

  test "effect and observation families expose the same typed correlation field" do
    families = [
      {%Effect.Clock{read_index: 0, purpose: "deadline"}, [%Observation.Clock{read_index: 0, now: @now}], :read_index},
      {%Effect.Dispatch{assignment_id: "as", command: %{}, message_id: "send"},
       [
         %Observation.Dispatched{assignment_id: "as", result: %{}, now: @now},
         %Observation.DispatchFailed{assignment_id: "as", reason: %{}, now: @now}
       ], :assignment_id},
      {%Effect.Observe{assignment_id: "as", command: %{}, deadline_unix: 1},
       [
         %Observation.ArtifactObserved{assignment_id: "as", artifact: %{}, now: @now},
         %Observation.Blocked{assignment_id: "as", reason: %{}, now: @now},
         %Observation.Pending{assignment_id: "as", details: %{}, now: @now},
         %Observation.ObserveFailed{assignment_id: "as", reason: %{}, now: @now},
         %Observation.TimedOut{assignment_id: "as", deadline_unix: 1, now: @now}
       ], :assignment_id},
      {%Effect.ReadReview{assignment_id: "as", path: "review.org"},
       [
         %Observation.ReviewRead{assignment_id: "as", contents: "CLEAN", now: @now},
         %Observation.ReviewUnreadable{
           assignment_id: "as",
           path: "review.org",
           reason: %{"reason" => "review_unreadable", "class" => "enoent"},
           now: @now
         }
       ], :assignment_id},
      {%Effect.RunGate{gate_run_id: "gr", requested: %{}, repo_root: "/repo", run_dir: "/run"},
       [
         %Observation.GateFinished{gate_run_id: "gr", result: %{}, now: @now},
         %Observation.GateFailed{gate_run_id: "gr", result: %{}, now: @now},
         %Observation.GateError{gate_run_id: "gr", reason: %{"reason" => "gate_runner_failed"}, now: @now}
       ], :gate_run_id},
      {%Effect.Notify{notification_id: "not", hook_argv: ["notify"], payload: %{}},
       [
         %Observation.Notified{notification_id: "not", result: %{}, now: @now},
         %Observation.NotifyFailed{notification_id: "not", reason: %{}, now: @now}
       ], :notification_id},
      {%Effect.Timer{purpose: "deadline", deadline_unix: 1},
       [%Observation.Deadline{purpose: "deadline", deadline_unix: 1, now: @now}], :purpose},
      {%Effect.RetainPrompt{assignment_id: "as", bytes: SensitiveBytes.new("prompt", :prompt)},
       [
         %Observation.PromptRetained{object: object(), now: @now},
         %Observation.PromptRetentionFailed{assignment_id: "as", reason: %{}, now: @now}
       ], :assignment_id},
      {%Effect.FetchPrompt{object: object()},
       [
         %Observation.PromptFetched{
           object: object(),
           bytes: SensitiveBytes.new("prompt", :prompt),
           now: @now
         },
         %Observation.PromptFetchFailed{assignment_id: "as", reason: %{}, now: @now}
       ], :assignment_id},
      {%Effect.ReconcileSend{assignment_id: "as", command: %{}},
       [
         %Observation.SendReconciled{assignment_id: "as", outcome: "queued", delivery_attempt: 1, now: @now},
         %Observation.SendReconcileFailed{assignment_id: "as", reason: %{}, now: @now}
       ], :assignment_id},
      {%Effect.SnapshotArtifact{assignment_id: "as", command: %{}},
       [
         %Observation.ArtifactSnapshot{assignment_id: "as", baseline: %{"exists" => false}, now: @now},
         %Observation.ArtifactSnapshotFailed{assignment_id: "as", reason: %{}, now: @now}
       ], :assignment_id}
    ]

    assert Enum.all?(families, fn {effect, observations, key} ->
             Enum.all?(observations, &(correlation(&1, key) == correlation(effect, key)))
           end)
  end

  test "no effect or observation pairs an assignment id with a prompt object" do
    paired =
      for namespace <- [Effect, Observation],
          module <- defined_structs_under(namespace),
          :object in Map.keys(struct(module)),
          :assignment_id in Map.keys(struct(module)),
          do: module

    assert paired == [], """
    A struct that carries both an object and an assignment id beside it has two
    answers to "whose prompt is this?", and nothing in the type makes them agree.
    `as_0001` paired with an object naming `prompts/as_0002-<digest>.org` passes a
    store that checks only containment and bytes: the path is inside the run's
    prompt directory and the digest does match the bytes it names, so the fetch
    succeeds and returns the other assignment's prompt.

    The object names its own assignment, so there is nothing to pair. Failure
    observations may still carry a bare id -- they carry no object, so there is
    nothing for it to drift against.

    Paired: #{inspect(paired)}
    """
  end

  describe "a prompt object is constructed checked or not at all" do
    @hex String.duplicate("a", 64)
    @sha "sha256:" <> @hex

    test "the checked constructor accepts a name it can derive" do
      assert {:ok, object} = PromptObject.new(consistent())

      assert object == %PromptObject{
               assignment_id: "as_0001",
               path: "prompts/as_0001-#{@hex}.org",
               hash: @sha,
               byte_size: 6,
               version: 2
             }

      assert PromptObject.verify(object) == :ok
    end

    test "a legacy name is derivable too, and only at version 1" do
      legacy = %{consistent() | path: "prompts/as_0001.org", version: 1}

      assert {:ok, object} = PromptObject.new(legacy)
      assert object.version == 1
      assert PromptObject.verify(object) == :ok

      assert PromptObject.new(%{legacy | version: 2}) ==
               {:error, {:prompt_object_name_mismatch, :scheme}},
             """
             The two schemes are told apart by the name, not by trust in the field
             beside it. `prompts/as_0001.org` under version 2 is a legacy name wearing
             an immutable object's version, and reading it is reading whatever the last
             writer of that one mutable name left behind.
             """
    end

    test "string keys from a decoded journal are checked in the same shape" do
      decoded = Map.new(consistent(), fn {key, value} -> {Atom.to_string(key), value} end)

      assert {:ok, object} = PromptObject.new(decoded)
      assert object.assignment_id == "as_0001"

      assert PromptObject.new(Map.put(decoded, "assignment_id", "as_0002")) ==
               {:error, {:prompt_object_name_mismatch, :assignment}},
             """
             A decoded journal fragment is the case the constructor exists for. If the
             string-keyed path skipped the derivation, every check below would be one a
             caller reaches only when it already holds a struct someone else built.
             """
    end

    # Each inconsistency is paired with the class the constructor may report, in the same
    # discipline the store applies to assignment ids: pinning the mapping, not membership,
    # is what stops a reducer's branches from collapsing into one uninformative atom.
    @inconsistencies [
      {"a path naming another assignment", %{assignment_id: "as_0002"}, {:prompt_object_name_mismatch, :assignment}},
      {"a digest that is not the one the hash names", %{path: "prompts/as_0001-#{String.duplicate("b", 64)}.org"},
       {:prompt_object_name_mismatch, :digest}},
      {"a truncated digest", %{path: "prompts/as_0001-#{String.duplicate("a", 63)}.org"},
       {:prompt_object_name_mismatch, :digest}},
      {"a legacy name at the content-addressed version", %{path: "prompts/as_0001.org"},
       {:prompt_object_name_mismatch, :scheme}},
      {"a name outside the prompt directory", %{path: "as_0001-#{String.duplicate("a", 64)}.org"},
       {:prompt_object_name_mismatch, :scheme}},
      {"a hash with no algorithm", %{hash: String.duplicate("a", 64)}, {:prompt_object_hash_malformed, :algorithm}},
      {"another algorithm", %{hash: "sha512:" <> String.duplicate("a", 64)}, {:prompt_object_hash_malformed, :algorithm}},
      {"a hash that is not hex", %{hash: "sha256:" <> String.duplicate("z", 64)},
       {:prompt_object_hash_malformed, :digest}},
      {"a hash in upper case", %{hash: "sha256:" <> String.duplicate("A", 64)}, {:prompt_object_hash_malformed, :digest}},
      {"a hash of the wrong length", %{hash: "sha256:" <> String.duplicate("a", 63)},
       {:prompt_object_hash_malformed, :digest}},
      {"a naming scheme that does not exist", %{version: 3}, {:prompt_object_version_unsupported, :unknown}},
      {"a version that is not a number", %{version: "2"}, {:prompt_object_version_unsupported, :unknown}},
      {"an assignment id with a separator in it", %{assignment_id: "as/0001"},
       {:prompt_object_assignment_id_invalid, :separator}},
      {"an empty assignment id", %{assignment_id: ""}, {:prompt_object_assignment_id_invalid, :empty}},
      {"an assignment id that is a directory name", %{assignment_id: ".."},
       {:prompt_object_assignment_id_invalid, :dot_name}},
      {"a negative size", %{byte_size: -1}, {:prompt_object_byte_size_invalid, :negative}},
      {"a size that is not a number", %{byte_size: "6"}, {:prompt_object_byte_size_invalid, :not_an_integer}}
    ]

    for {name, override, expected} <- @inconsistencies do
      test "the checked constructor refuses #{name}" do
        override = unquote(Macro.escape(override))
        expected = unquote(Macro.escape(expected))

        assert {:error, reason} = PromptObject.new(Map.merge(consistent(), override))

        assert reason == expected, """
        #{unquote(name)} was refused as `#{inspect(reason)}` rather than
        `#{inspect(expected)}`. A rejection is a name a reducer branches on, and two
        inconsistencies arriving under one name cannot be told apart by the code that
        has to decide whether the run is resumable or the object must be re-rendered.
        """

        assert Diagnostic.describe_rejection(reason) ==
                 %{"reason" => Atom.to_string(elem(reason, 0)), "class" => Atom.to_string(elem(reason, 1))},
               """
               `#{inspect(reason)}` is the in-process shape and it is not the shape that leaves
               the process. `Jason.Encoder` is not implemented for tuples, so the pair reaches a
               journal event or a protocol reply only through `Diagnostic.describe_rejection/1`,
               which reflects it because both names are in this release's closed set. A reason
               this module raises and that set does not list is not an encoder crash and not a
               leak -- it is worse than either, because it degrades silently to a result class
               and the vocabulary an operator needs is gone with no sign that it was ever there.
               """

        assert {:ok, _encoded} = Jason.encode(Diagnostic.describe_rejection(reason))
      end
    end

    test "an inconsistency reaches verify/1 too, so a struct built elsewhere is still checked" do
      forged = %PromptObject{
        assignment_id: "as_0001",
        path: "prompts/as_0002-#{@hex}.org",
        hash: @sha,
        byte_size: 6,
        version: 2
      }

      assert PromptObject.verify(forged) == {:error, {:prompt_object_name_mismatch, :assignment}}, """
      `new/1` guards the boundary the contract controls. `verify/1` is what a consumer
      calls on an object it was handed, because a bare struct literal stays
      constructible -- deliberately, so a test can forge one -- and the effect that
      carries it crosses a process boundary where nothing re-runs the constructor.
      """
    end

    # The five fields are written out one at a time rather than derived from the struct,
    # because a table generated from `@enforce_keys` agrees with whatever the module says
    # today and so can never catch the day the module says something else.
    for field <- [:assignment_id, :path, :hash, :byte_size, :version] do
      test "a missing #{field} is refused rather than defaulted" do
        field = unquote(field)

        assert PromptObject.new(Map.delete(consistent(), field)) ==
                 {:error, {:prompt_object_incomplete, field}},
               """
               The class names the missing field, and it names it from this module's own closed
               set of five. That is what makes the pair branchable: a class typed `atom()` is not
               a closed set, and a consumer matching on one has nothing to be exhaustive against.
               """
      end
    end

    test "a field that is present and nil is missing, because a forged struct has every key" do
      for field <- [:assignment_id, :path, :hash, :byte_size, :version] do
        forged = struct!(PromptObject, Map.put(consistent(), field, nil))

        assert PromptObject.verify(forged) == {:error, {:prompt_object_incomplete, field}}, """
        `Map.has_key?/2` answers `true` for every field of a struct that filled none of
        them, so presence is not the question `verify/1` is being asked. A struct arrives
        over a process boundary that ran no constructor; `nil` is what an incomplete one
        looks like when it gets there, and `#{field}` is the field this case emptied.
        """
      end
    end

    test "an unknown field is refused rather than dropped, and neither its name nor its value comes back" do
      assert {:error, {:prompt_object_unknown_field, :unknown} = reason} =
               PromptObject.new(Map.put(consistent(), :bytes, "PROMPT"))

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ "PROMPT", """
      The refused field may hold the prompt itself. Echoing its value is how the bytes
      reach a journal.
      """

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ "bytes", """
      `{:prompt_object_incomplete, :hash}` names its field and this one does not, and the
      difference is not inconsistency. A missing field's name is drawn from this module's
      own closed set; an unknown field's name is drawn from whoever wrote the map. A rule
      that decided case by case which caller-supplied string were safe to repeat would
      eventually decide wrong, and there is nothing here for it to buy: the operator
      already has the map.
      """
    end

    test "an unknown string key is refused without ever becoming an atom" do
      key = "prompt_object_probe_#{System.unique_integer([:positive])}"

      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end

      decoded = Map.new(consistent(), fn {field, value} -> {Atom.to_string(field), value} end)

      assert {:error, {:prompt_object_unknown_field, :unknown} = reason} =
               PromptObject.new(Map.put(decoded, key, "PROMPT"))

      refute inspect(reason, limit: :infinity, printable_limit: :infinity) =~ key

      # The atom table is a fixed BEAM resource and nothing reclaims it, so a constructor
      # that converts a decoded key before it checks it turns a malformed journal into a
      # node that stops scheduling. That is why the unknown-key answer is a refusal rather
      # than a conversion that happens to fail: `String.to_existing_atom/1` raises here
      # too, so a constructor built on it would read as correct while every key that does
      # name a real atom somewhere in the release still gets converted on the way past.
      # The key is still absent from the table after the call, which is the only evidence
      # that distinguishes the two implementations from outside.
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end

    test "a field named twice under two key shapes is refused rather than resolved" do
      doubled =
        consistent()
        |> Map.new(fn {field, value} -> {Atom.to_string(field), value} end)
        |> Map.put(:assignment_id, "as_0002")

      assert PromptObject.new(doubled) == {:error, {:prompt_object_duplicate_field, :alias}}, """
      There is no correct winner to pick. The two values disagree about a field the whole
      contract exists to make authoritative, and keeping one silently makes which `hash` a
      store verifies against a question about map iteration order.
      """

      agreeing =
        consistent()
        |> Map.new(fn {field, value} -> {Atom.to_string(field), value} end)
        |> Map.put(:assignment_id, "as_0001")

      assert PromptObject.new(agreeing) == {:error, {:prompt_object_duplicate_field, :alias}}, """
      Refused even when the two agree. Accepting the agreeing case is accepting the shape,
      and the next map in that shape is the disagreeing one.
      """
    end

    test "an assignment id past the filename bound is refused even though the path derives from it" do
      long = String.duplicate("a", 65)
      over = %{consistent() | assignment_id: long, path: "prompts/#{long}-#{@hex}.org"}

      assert PromptObject.new(over) == {:error, {:prompt_object_assignment_id_invalid, :too_long}}, """
      This object is internally perfect: the path derives from the id and the digest
      exactly. Derivation is not a bound -- an id of any length derives a path that agrees
      with it -- so the bound has to be applied to the id before the name is built from
      it, or it is not applied at all.
      """

      at_bound = String.duplicate("a", 64)
      ok = %{consistent() | assignment_id: at_bound, path: "prompts/#{at_bound}-#{@hex}.org"}

      assert match?({:ok, _}, PromptObject.new(ok)), "sixty-four bytes is inside the bound, not outside it"
    end

    test "an assignment id outside the grammar is refused by the byte that is not in it" do
      for id <- ["as 0001", "as\t0001", "as_0001\n", "asé0001", "as*0001", "as\0"] do
        candidate = %{consistent() | assignment_id: id, path: "prompts/#{id}-#{@hex}.org"}

        assert PromptObject.new(candidate) == {:error, {:prompt_object_assignment_id_invalid, :byte}}, """
        `#{inspect(id)}` was not refused as an out-of-grammar byte. The id becomes a
        filename component, and `..` and `/` are only the two cases with names: a space,
        a newline, a NUL or a multi-byte character are each a name the run may or may not
        be able to write, on a filesystem whose answer is not this contract's to guess.
        The grammar is an allow list for that reason -- `[A-Za-z0-9_.-]` -- so a byte
        nobody thought about is refused rather than tried.
        """
      end
    end
  end

  defp consistent do
    %{
      assignment_id: "as_0001",
      path: "prompts/as_0001-#{@hex}.org",
      hash: @sha,
      byte_size: 6,
      version: 2
    }
  end

  # The correlation value is the same in every member of a family; where it lives is
  # not. A prompt object carries its own assignment, so a struct holding an object
  # answers `:assignment_id` from inside it rather than from a field beside it --
  # which is the point: there is no second field to disagree with.
  defp correlation(struct, :assignment_id) when is_map_key(struct, :object), do: struct.object.assignment_id

  defp correlation(struct, key), do: Map.fetch!(struct, key)

  defp object do
    %PromptObject{
      assignment_id: "as",
      path: "prompts/as-#{String.duplicate("0", 64)}.org",
      hash: "sha256:#{String.duplicate("0", 64)}",
      byte_size: 6,
      version: 2
    }
  end
end
