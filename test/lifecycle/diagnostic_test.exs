defmodule AiOrchestrator.Lifecycle.Core.DiagnosticTest do
  @moduledoc """
  Diagnostic.describe/1 normalizes untrusted adapter values: it never crashes,
  never reflects adapter-controlled bytes or atom names, describes only the
  known contract structs by kind and typed id, and always carries a full digest.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptObject
  alias AiOrchestrator.Contract.PromptRejection
  alias AiOrchestrator.Contract.SensitiveBytes
  alias AiOrchestrator.Lifecycle.Core.Diagnostic

  defmodule Foreign do
    @moduledoc false
    defstruct assignment_id: "SENSITIVE_CORRELATION", payload: "SENSITIVE_PAYLOAD"
  end

  @now %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

  defp digest_ok?(%{"digest" => "sha256:" <> hex}), do: byte_size(hex) == 64 and hex == String.downcase(hex)
  defp digest_ok?(_other), do: false

  test "a map with a fake __struct__ key never crashes and is a plain map" do
    for fake <- ["SENSITIVE_KIND", 42, nil, :"Elixir.NotAModule"] do
      value = %{__struct__: fake, assignment_id: "SENSITIVE_CORRELATION"}
      described = Diagnostic.describe(value)
      assert %{"result_class" => "map"} = described
      assert digest_ok?(described)
      refute inspect(described) =~ "SENSITIVE"
      refute Map.has_key?(described, "kind") or Map.has_key?(described, "correlation")
    end
  end

  test "an arbitrary struct with an id-looking field is a plain map, not a correlated kind" do
    described = Diagnostic.describe(%Foreign{})
    assert %{"result_class" => "map"} = described
    assert digest_ok?(described)
    refute inspect(described) =~ "SENSITIVE"
  end

  test "tuples and atoms are classed without reflecting adapter-controlled names" do
    described = Diagnostic.describe({:SECRET_ATOM_TAG, "SENSITIVE_PAYLOAD"})
    assert %{"result_class" => "tuple"} = described
    assert digest_ok?(described)
    refute inspect(described) =~ "SECRET_ATOM_TAG"
    refute inspect(described) =~ "SENSITIVE"

    assert %{"result_class" => "atom"} = Diagnostic.describe(:SECRET_ATOM)
    refute inspect(Diagnostic.describe(:SECRET_ATOM)) =~ "SECRET"
    assert %{"result_class" => "binary"} = Diagnostic.describe("SENSITIVE")
    assert %{"result_class" => "list"} = Diagnostic.describe(["SENSITIVE"])
    assert %{"result_class" => "integer"} = Diagnostic.describe(7)
    assert %{"result_class" => "other"} = Diagnostic.describe(self())
  end

  test "a known struct whose id carries adapter bytes is described without a correlation" do
    long = String.duplicate("a", 65)

    cases = [
      %Effect.Dispatch{assignment_id: "SENSITIVE PAYLOAD/with bytes", command: %{}, message_id: "m"},
      %Effect.Dispatch{assignment_id: long, command: %{}, message_id: "m"},
      %Effect.Dispatch{assignment_id: :SENSITIVE_ATOM, command: %{}, message_id: "m"},
      %Effect.RunGate{gate_run_id: "SENSITIVE\nbytes", requested: %{}, repo_root: "/r", run_dir: "/d"},
      %Effect.Notify{notification_id: %{"secret" => "SENSITIVE"}, hook_argv: [], payload: %{}},
      %Observation.Clock{read_index: "SENSITIVE_INDEX", now: @now},
      %Observation.Clock{read_index: -1, now: @now}
    ]

    for value <- cases do
      described = Diagnostic.describe(value)
      assert match?(%{"correlation" => nil}, described), inspect(value.__struct__)
      assert digest_ok?(described)
      refute inspect(described) =~ "SENSITIVE"
      refute inspect(described) =~ long
    end
  end

  test "a trailing newline is not an identifier: the grammar is anchored absolutely" do
    cases = [
      %Effect.Dispatch{assignment_id: "as_1\n", command: %{}, message_id: "m"},
      %Effect.Dispatch{assignment_id: "as_1\n\n", command: %{}, message_id: "m"},
      %Effect.Observe{assignment_id: "as_1\n", command: %{}, deadline_unix: 1},
      %Effect.ReadReview{assignment_id: "as_1\n", path: "/r"},
      %Effect.RunGate{gate_run_id: "gr_1\n", requested: %{}, repo_root: "/r", run_dir: "/d"},
      %Effect.Notify{notification_id: "n_1\n", hook_argv: [], payload: %{}},
      %Observation.Dispatched{assignment_id: "as_1\n", result: %{}, now: @now},
      %Observation.GateError{gate_run_id: "gr_1\n", reason: "r", now: @now},
      %Observation.NotifyFailed{notification_id: "n_1\n", reason: %{}, now: @now}
    ]

    for value <- cases do
      described = Diagnostic.describe(value)
      assert match?(%{"correlation" => nil}, described), inspect(value.__struct__)
      assert digest_ok?(described)
      refute inspect(described) =~ "\\n"
    end
  end

  test "known contract structs are described by kind and their typed id only" do
    cases = [
      {%Effect.Clock{read_index: 3, purpose: "SENSITIVE_PURPOSE"}, "clock", "3"},
      {%Effect.Dispatch{assignment_id: "as_1", command: %{"p" => "SENSITIVE"}, message_id: "m"}, "dispatch", "as_1"},
      {%Effect.Observe{assignment_id: "as_1", command: %{}, deadline_unix: 1}, "observe", "as_1"},
      {%Effect.ReadReview{assignment_id: "as_1", path: "/SENSITIVE/path"}, "read_review", "as_1"},
      {%Effect.RunGate{gate_run_id: "gr_1", requested: %{}, repo_root: "/SENSITIVE", run_dir: "/r"}, "run_gate", "gr_1"},
      {%Effect.Notify{notification_id: "n_1", hook_argv: ["SENSITIVE"], payload: %{}}, "notify", "n_1"},
      {%Effect.Timer{purpose: "SENSITIVE_PURPOSE", deadline_unix: 1}, "timer", nil},
      {%Observation.Clock{read_index: 3, now: @now}, "clock", "3"},
      {%Observation.Dispatched{assignment_id: "as_1", result: %{"out" => "SENSITIVE"}, now: @now}, "dispatched", "as_1"},
      {%Observation.GateError{gate_run_id: "gr_1", reason: "SENSITIVE", now: @now}, "gate_error", "gr_1"},
      {%Observation.NotifyFailed{notification_id: "n_1", reason: %{}, now: @now}, "notify_failed", "n_1"},
      {%Observation.Deadline{purpose: "SENSITIVE_PURPOSE", deadline_unix: 1, now: @now}, "deadline", nil},
      {%Effect.RetainPrompt{assignment_id: "as_1", bytes: SensitiveBytes.new("SENSITIVE", :prompt)}, "retain_prompt",
       "as_1"},
      {%Effect.FetchPrompt{object: sensitive_object()}, "fetch_prompt", "as_1"},
      {%Observation.PromptRetained{object: sensitive_object(), now: @now}, "prompt_retained", "as_1"},
      {%Observation.PromptRetentionFailed{assignment_id: "as_1", reason: %{"detail" => "SENSITIVE"}, now: @now},
       "prompt_retention_failed", "as_1"},
      {%Observation.PromptFetched{
         object: sensitive_object(),
         bytes: SensitiveBytes.new("SENSITIVE", :prompt),
         now: @now
       }, "prompt_fetched", "as_1"},
      {%Observation.PromptFetchFailed{assignment_id: "as_1", reason: %{"detail" => "SENSITIVE"}, now: @now},
       "prompt_fetch_failed", "as_1"}
    ]

    for {value, kind, correlation} <- cases do
      described = Diagnostic.describe(value)
      assert match?(%{"kind" => ^kind, "correlation" => ^correlation}, described), inspect(value.__struct__)
      assert digest_ok?(described)
      refute inspect(described) =~ "SENSITIVE"
    end
  end

  test "every contract struct is a known kind, so none can fall through to a bare class" do
    for module <- contract_structs() do
      described = module |> struct() |> Diagnostic.describe()

      assert Map.has_key?(described, "kind"), """
      #{inspect(module)} is described only by `result_class`, which is the clause
      for adapter-controlled values. A contract struct reaching it means the closed
      known-set was not extended when the struct was added, and the run's own
      vocabulary is being reported as if it came from outside.
      """
    end
  end

  # The rejection vocabulary is `PromptRejection`, and this file reads it rather than
  # restating it. It used to carry two lists -- one of reasons, one of classes -- and
  # asserted their product. That is not a closed set but a rectangle, and it was wrong in
  # both directions at once: it named a reason the store no longer emits alone while
  # omitting `prompt_publication_sync_failed`, it left out nine classes the store does
  # emit, and its product asserted more than a thousand pairs of which only about a
  # tenth exist. A consumer branching on a rectangle has nothing to be exhaustive
  # against. `PromptRejection` is pinned in turn by a hand-written literal in
  # `AiOrchestrator.Contracts.PromptRejectionTest`, so reading it here is not reading a
  # table that could have validated itself.

  # Pairs whose two halves are both names this release owns, but which no producer can
  # make: five name a class belonging to a different reason, and two offer a class to a
  # reason whose second element is a contained path. Each was in a hand-written table
  # once, which is the argument for not keeping hand-written tables.
  @impossible_pairs [
    {:prompt_dir_not_directory, :enotdir},
    {:prompt_object_not_regular, :scheme},
    {:prompt_object_mode_unexpected, :unknown},
    {:prompt_path_escapes_root, :separator},
    {:prompt_object_conflict, :digest},
    {:prompt_object_missing, :enoent},
    {:prompt_hash_mismatch, :byte_size}
  ]

  describe "a closed rejection is reflected; everything else is only described" do
    test "every declared pair normalizes to exactly its two names and nothing more" do
      for {reason, class} <- PromptRejection.pairs() do
        assert Diagnostic.describe_rejection({reason, class}) ==
                 %{"reason" => Atom.to_string(reason), "class" => Atom.to_string(class)},
               """
               `#{inspect({reason, class})}` did not normalize to its two names. Both halves are
               drawn from a set this release owns, so both are reflected in full: the reason is
               what an alert rule matches and the class is what tells one occurrence of that
               reason from another.
               """
      end
    end

    test "a normalized rejection is a shape the journal and a protocol reply can hold" do
      assert match?({:error, %Protocol.UndefinedError{}}, Jason.encode({:prompt_open_failed, :emfile})), """
      The premise. `Jason.Encoder` is not implemented for tuples, so the pair itself cannot
      cross the wire, and a normalizer that returned it unchanged would turn a refusal into
      an encoder crash at the moment something has already gone wrong.
      """

      for {reason, class} <- PromptRejection.pairs() do
        normalized = Diagnostic.describe_rejection({reason, class})

        json = Jason.encode(normalized)

        assert match?({:ok, _}, json), """
        `#{inspect({reason, class})}` normalized to `#{inspect(normalized)}`, which JSON cannot
        hold: `#{inspect(json)}`. The journal, a protocol reply and a diagnostic are all JSON;
        a normalization that does not survive them has normalized nothing.
        """

        {:ok, encoded} = json

        assert Jason.decode(encoded) == {:ok, normalized}, """
        `#{inspect({reason, class})}` did not survive the round trip. A reason that reads
        differently on the far side of the journal is a reason two operators disagree about.
        """
      end
    end

    test "a pair of two known names that no producer makes is described, not reflected" do
      admitted = MapSet.new(PromptRejection.pairs())

      crossed =
        for reason <- PromptRejection.reasons(),
            class <- PromptRejection.classes(),
            not MapSet.member?(admitted, {reason, class}),
            do: {reason, class}

      for pair <- @impossible_pairs do
        assert pair in crossed, """
        `#{inspect(pair)}` is admitted by the table, so this file is asserting the wrong
        thing about it. Either the table gained a pair nobody reviewed or this probe is
        stale; both are a read of `PromptRejection`, not a change here.
        """
      end

      for {reason, class} = pair <- crossed do
        assert Diagnostic.describe_rejection(pair) == Diagnostic.describe(pair), """
        `#{inspect(pair)}` was reflected. Both halves are names this release owns, which is
        exactly what makes the pair plausible enough to be written by hand and impossible
        to produce -- `#{inspect(reason)}` reports #{inspect(PromptRejection.classes(reason))},
        and `#{inspect(class)}` is not among them. Gating the two halves separately admits
        every such pair, and a consumer that branches on the pair would be told a fact
        about this run that did not happen.
        """
      end
    end

    test "a path-carrying reason reports its reason and leaves the path behind" do
      paths = [
        "prompts/as_0001-" <> String.duplicate("a", 64) <> ".org",
        "prompts/SECRET_ASSIGNMENT.org"
      ]

      for reason <- PromptRejection.path_carrying(), path <- paths do
        assert Diagnostic.describe_rejection({reason, path}) ==
                 %{"reason" => Atom.to_string(reason), "class" => nil},
               """
               `#{inspect(reason)}` names an object that is on disk and wrong, and its second
               element is the contained relative path the journal already committed rather
               than a class. The reason is what an alert matches, so it reflects; the path is
               a value the store was handed, so a diagnostic does not carry it -- the journal
               already holds it, and this is the wire.
               """

        refute inspect(Diagnostic.describe_rejection({reason, path})) =~ "SECRET"
        refute inspect(Diagnostic.describe_rejection({reason, path})) =~ "prompts/"
      end
    end

    test "a rejection carries no digest, because both halves are already printed" do
      normalized = Diagnostic.describe_rejection({:prompt_hash_mismatch, :digest})

      assert normalized |> Map.keys() |> Enum.sort() == ["class", "reason"], """
      `describe/1` digests because it must not print what it was handed. A rejection is
      printed in full, so a digest beside it would hash a value the same map already
      shows, and an operator would be invited to compare the two.
      """
    end

    test "a reason outside the closed set is described rather than reflected" do
      for forged <- [{:SECRET_ATOM_TAG, :eacces}, {:prompt_object_missing_x, :path}, {:error, :enoent}] do
        assert Diagnostic.describe_rejection(forged) == Diagnostic.describe(forged), """
        `#{inspect(forged)}` was reflected. An adapter returns `{:error, reason}` and the host
        normalizes whatever is inside it, so a pair is a shape an adapter can mint. Reflecting
        an unlisted reason would make `describe_rejection/1` the hole `describe/1` exists to
        close: a way to get an adapter-chosen atom name into a log by wrapping it in a tuple.
        """

        refute inspect(Diagnostic.describe_rejection(forged)) =~ "SECRET"
      end
    end

    test "a class outside the closed set drops to nil while the reason still reports" do
      for forged_class <- [:SECRET_PAYLOAD_BYTES_9f3c, :enosuchthing] do
        assert Diagnostic.describe_rejection({:prompt_open_failed, forged_class}) ==
                 %{"reason" => "prompt_open_failed", "class" => nil},
               """
               The pair is gated as a pair, and this one fails on its class: `prompt_open_failed`
               reports an `errno`, and no release chose this atom. Dropping the whole pair would
               cost an operator the reason as well, which is the half an alert rule matches, so
               the reason survives and the class does not.
               """

        refute inspect(Diagnostic.describe_rejection({:prompt_open_failed, forged_class})) =~ "SECRET"
        refute inspect(Diagnostic.describe_rejection({:prompt_open_failed, forged_class})) =~ "enosuchthing"
      end
    end

    test "a class that is not an atom is dropped, so no path and no prompt travels as one" do
      for class <- [
            "/srv/runs/run_0001/prompts/as_0001.org",
            "* Assignment\n\nSECRET_PROMPT_BYTES\n",
            %{"path" => "/abs/SECRET"},
            {:nested, "SECRET"},
            123
          ] do
        normalized = Diagnostic.describe_rejection({:prompt_object_unreadable, class})

        assert normalized == %{"reason" => "prompt_object_unreadable", "class" => nil}, """
        A rejection whose second element is the offending value is how a path or a prompt
        reaches a log by way of an error. The reason survives; the value does not.
        """

        refute inspect(normalized, limit: :infinity, printable_limit: :infinity) =~ "SECRET"
        refute inspect(normalized, limit: :infinity, printable_limit: :infinity) =~ "/srv/"
      end
    end

    test "anything that is not a pair is described exactly as an adapter value is described" do
      for term <- [
            :prompt_object_missing,
            "SENSITIVE_PAYLOAD",
            %{"reason" => "prompt_open_failed", "class" => "emfile"},
            {:prompt_open_failed, :emfile, :extra},
            {:prompt_open_failed},
            [prompt_open_failed: :emfile],
            nil,
            42
          ] do
        assert Diagnostic.describe_rejection(term) == Diagnostic.describe(term), """
        There is one fallback and it is the existing one. A second, kinder description for
        terms that nearly look like rejections is a second place for an adapter to aim at.
        """

        assert digest_ok?(Diagnostic.describe_rejection(term))
        refute inspect(Diagnostic.describe_rejection(term)) =~ "SENSITIVE"
      end
    end

    test "an already-normalized map is not re-reflected, so normalization is not a loop" do
      normalized = %{"reason" => "prompt_open_failed", "class" => "emfile"}

      assert Diagnostic.describe_rejection(normalized) == Diagnostic.describe(normalized), """
      `describe_rejection/1` takes the in-process pair. A map that already went through it
      is not a rejection any more, and a clause that recognized its own output would make
      a caller's second, defensive call silently different from its first.
      """
    end
  end

  defp contract_structs do
    :ai_orchestrator
    |> :application.get_key(:modules)
    |> elem(1)
    |> Enum.filter(fn module ->
      name = Atom.to_string(module)

      (String.starts_with?(name, "Elixir.AiOrchestrator.Contract.Effect.") or
         String.starts_with?(name, "Elixir.AiOrchestrator.Contract.Observation.")) and
        Code.ensure_loaded?(module) and function_exported?(module, :__struct__, 0)
    end)
  end

  # Deliberately inconsistent: the derivation rule gives `as_1` the path
  # `prompts/as_1.org`, so this object could never come from the checked constructor.
  # That is the point. `Diagnostic.describe/1` runs on whatever a decoded journal
  # produced, before anything has re-derived it, and a describe clause that dumped the
  # struct would publish this path. Correlation still comes from inside the object,
  # which is where the assignment now lives.
  defp sensitive_object do
    %PromptObject{
      assignment_id: "as_1",
      path: "prompts/SENSITIVE.org",
      hash: "sha256:#{String.duplicate("b", 64)}",
      byte_size: 9,
      version: 1
    }
  end
end
