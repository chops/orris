defmodule AiOrchestrator.Lifecycle.HostDiagnosticsTest do
  @moduledoc """
  Invalid return diagnostics never carry payload bytes. Prompts, commands,
  artifact contents, provider output, and paths stay inside the process; an
  invalid-return error names the effect or observation kind, its correlation
  id, the result class, and a digest. Valid result arms retain the adapter data
  their observation contracts require.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.Contract.Effect
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Contract.Observation
  alias AiOrchestrator.Contract.PromptRejection
  alias AiOrchestrator.Lifecycle.Core.Reducer
  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.GateDouble
  alias AiOrchestrator.Test.ScenarioHarness, as: H
  alias AiOrchestrator.Test.ScenarioHarness.OkDispatch
  alias AiOrchestrator.Test.ScriptedDispatchReceipt

  @secret "SECRET_PAYLOAD_BYTES_9f3c"
  @now %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

  defmodule WeirdDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: {:weird, "SECRET_PAYLOAD_BYTES_9f3c"}
    @impl true
    def observe(_command, _opts), do: {:weird, "SECRET_PAYLOAD_BYTES_9f3c"}
  end

  defmodule FakeStructDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: %{__struct__: "SENSITIVE_KIND", assignment_id: "SENSITIVE_CORRELATION"}

    @impl true
    def observe(_command, _opts), do: %{__struct__: "SENSITIVE_KIND", assignment_id: "SENSITIVE_CORRELATION"}
  end

  defmodule IntegerStructDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: %{__struct__: 42, assignment_id: "SENSITIVE_CORRELATION"}
    @impl true
    def observe(_command, _opts), do: %{__struct__: 42, assignment_id: "SENSITIVE_CORRELATION"}
  end

  defmodule SecretAtomDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: {:SECRET_ATOM_TAG, "SECRET_PAYLOAD_BYTES_9f3c"}
    @impl true
    def observe(_command, _opts), do: {:SECRET_ATOM_TAG, "SECRET_PAYLOAD_BYTES_9f3c"}
  end

  defmodule TrailingNewlineDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @stamp %Moment{unix: 1_788_264_900, wall_ts: "2026-08-31T12:15:00Z"}

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: %Observation.Dispatched{assignment_id: "as_1\n", result: %{}, now: @stamp}

    @impl true
    def observe(_command, _opts), do: %Observation.Dispatched{assignment_id: "as_1\n", result: %{}, now: @stamp}
  end

  defmodule InvalidErrorDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: {:error, {:SECRET_ATOM_TAG, "SECRET_PAYLOAD_BYTES_9f3c"}}
    @impl true
    def observe(_command, _opts), do: {:error, {:SECRET_ATOM_TAG, "SECRET_PAYLOAD_BYTES_9f3c"}}
  end

  defmodule InvalidErrorObserve do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(command, opts), do: OkDispatch.deliver(command, opts)
    @impl true
    def observe(_command, _opts), do: {:error, {:SECRET_ATOM_TAG, "SECRET_PAYLOAD_BYTES_9f3c"}}
  end

  # The reason is parametrized rather than baked in because the invariant is about every
  # rejection family, not about one of them, and an adapter module cannot take an
  # argument. `:persistent_term` rather than the process dictionary: the guarantee under
  # test is that the host normalizes wherever it performs the effect, and a double that
  # only works while the host stays in the calling process would quietly stop proving
  # that the day it does not.
  defmodule RejectingDispatch do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    def put(reason), do: :persistent_term.put({__MODULE__, :reason}, reason)
    def erase, do: :persistent_term.erase({__MODULE__, :reason})
    defp reason, do: :persistent_term.get({__MODULE__, :reason})

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(_command, _opts), do: {:error, reason()}
    @impl true
    def observe(_command, _opts), do: {:error, reason()}
  end

  defmodule RejectingObserve do
    @moduledoc false
    @behaviour AiOrchestrator.Dispatch

    use ScriptedDispatchReceipt

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def deliver(command, opts), do: OkDispatch.deliver(command, opts)
    @impl true
    def observe(_command, _opts), do: {:error, :persistent_term.get({RejectingDispatch, :reason})}
  end

  defp fresh_opts do
    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    {scenario, opts_fun.()}
  end

  test "an inadmissible observation names kinds and correlation ids, never effect payloads" do
    {scenario, opts} = fresh_opts()
    opts = Keyword.merge(opts, run_id: "run_diag_0001", supervisor_instance: "sup_diag_0001")
    assert {:effect, %Effect.Clock{}, state, _events} = Reducer.init(H.spec(scenario), H.plan(scenario), opts)

    observation = %Observation.Dispatched{assignment_id: "as_diag", result: %{"out" => @secret}, now: @now}

    assert {:error, %{"reason" => "observation_mismatch", "expected" => expected, "observed" => actual} = error} =
             Reducer.step(state, observation)

    assert %{"kind" => "clock", "correlation" => "1"} = expected
    assert %{"kind" => "dispatched", "correlation" => "as_diag", "digest" => "sha256:" <> digest} = actual
    assert byte_size(digest) == 64
    refute inspect(error) =~ @secret
  end

  test "an adapter result outside its behaviour is reported by class and digest, not by value" do
    {scenario, opts} = fresh_opts()
    opts = Keyword.put(opts, :dispatch, WeirdDispatch)

    # A first-send delivery outside the behaviour stops the run with the typed reason.
    assert {:error, %{"reason" => "dispatch_invalid_return"} = error} =
             Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert %{"result_class" => "tuple", "digest" => "sha256:" <> digest} = error
    assert byte_size(digest) == 64
    refute inspect(error) =~ @secret
  end

  test "sink failures and invalid sink results carry no payload bytes" do
    {scenario, opts} = fresh_opts()

    weird_sink = Keyword.put(opts, :event_sink, fn _event -> {:weird, @secret} end)

    assert {:error, %{"reason" => "journal_append_failed"} = error} =
             Host.run(H.spec(scenario), H.plan(scenario), weird_sink)

    assert error["clause"] == "invalid_sink_result"
    assert %{"result_class" => "tuple", "digest" => "sha256:" <> digest} = error
    assert byte_size(digest) == 64
    refute inspect(error) =~ @secret

    {scenario, opts} = fresh_opts()
    string_sink = Keyword.put(opts, :event_sink, fn _event -> {:error, @secret} end)

    assert {:error, %{"reason" => "journal_append_failed"} = error} =
             Host.run(H.spec(scenario), H.plan(scenario), string_sink)

    refute inspect(error) =~ @secret
    assert %{"result_class" => "binary", "digest" => "sha256:" <> digest} = error
    assert byte_size(digest) == 64
  end

  test "an invalid adapter returning a fake __struct__ map is classed as a map without crashing" do
    for adapter <- [FakeStructDispatch, IntegerStructDispatch] do
      {scenario, opts} = fresh_opts()
      opts = Keyword.put(opts, :dispatch, adapter)

      assert {:error, %{"reason" => "dispatch_invalid_return"} = error} =
               Host.run(H.spec(scenario), H.plan(scenario), opts)

      assert %{"result_class" => "map", "digest" => "sha256:" <> digest} = error
      assert byte_size(digest) == 64
      refute Map.has_key?(error, "kind")
      refute inspect(error) =~ "SENSITIVE"
    end
  end

  test "an invalid adapter returning a tagged tuple never reflects the tag or the payload" do
    {scenario, opts} = fresh_opts()
    opts = Keyword.put(opts, :dispatch, SecretAtomDispatch)

    assert {:error, %{"reason" => "dispatch_invalid_return"} = error} =
             Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert %{"result_class" => "tuple", "digest" => "sha256:" <> digest} = error
    assert byte_size(digest) == 64
    refute inspect(error) =~ "SECRET_ATOM_TAG"
    refute inspect(error) =~ @secret
  end

  test "an invalid adapter returning a known struct with a trailing-newline id drops the correlation" do
    {scenario, opts} = fresh_opts()
    opts = Keyword.put(opts, :dispatch, TrailingNewlineDispatch)

    assert {:error, %{"reason" => "dispatch_invalid_return"} = error} =
             Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert %{"kind" => "dispatched", "correlation" => nil, "digest" => "sha256:" <> digest} = error
    assert byte_size(digest) == 64
    refute inspect(error) =~ "\\n"
  end

  test "dispatch and observation error payloads must satisfy the declared map contract" do
    for {adapter, reason} <- [
          {InvalidErrorDispatch, "dispatch_invalid_return"},
          {InvalidErrorObserve, "observe_invalid_return"}
        ] do
      {scenario, opts} = fresh_opts()
      opts = Keyword.put(opts, :dispatch, adapter)

      assert {:error, %{"reason" => ^reason, "result_class" => "tuple"} = error} =
               Host.run(H.spec(scenario), H.plan(scenario), opts)

      refute inspect(error) =~ "SECRET_ATOM_TAG"
      refute inspect(error) =~ @secret
    end
  end

  test "review-reader error payloads are normalized at their own boundary" do
    {scenario, opts} = fresh_opts()
    owner = self()

    opts =
      opts
      |> Keyword.put(:effect_observer, fn effect, observation ->
        send(owner, {:effect_observed, effect, observation})
      end)
      |> Keyword.put(:review_reader, fn _path -> {:error, {:SECRET_ATOM_TAG, @secret}} end)

    _result = Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert_receive {:effect_observed, %Effect.ReadReview{},
                    %{reason: %{"reason" => "review_reader_invalid_return", "result_class" => "tuple"} = error}}

    refute inspect(error) =~ "SECRET_ATOM_TAG"
    refute inspect(error) =~ @secret
  end

  test "a gate executor's raw error term is normalized at the await boundary: invalid_return, never its contents" do
    {scenario, opts} = fresh_opts()
    owner = self()

    opts =
      opts
      |> Keyword.put(:effect_observer, fn effect, observation ->
        send(owner, {:effect_observed, effect, observation})
      end)
      |> Keyword.put(:gate_executor, GateDouble)
      |> Keyword.put(:gate_helper, GateDouble.helper())
      |> Keyword.put(:gate_opts, runner: fn _gate -> {:error, {:SECRET_ATOM_TAG, @secret}} end)

    _result = Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert_receive {:effect_observed, %Effect.AwaitGate{},
                    %Observation.GateError{reason: %{"clause" => "invalid_return"} = error}}

    refute inspect(error) =~ "SECRET_ATOM_TAG"
    refute inspect(error) =~ @secret
  end

  test "a declared file error is retained as bounded review metadata" do
    {scenario, opts} = fresh_opts()
    owner = self()

    opts =
      Keyword.merge(opts,
        review_reader: fn _path -> {:error, :enoent} end,
        effect_observer: fn effect, observation -> send(owner, {:effect_observed, effect, observation}) end
      )

    assert {:ok, _result} = Host.run(H.spec(scenario), H.plan(scenario), opts)

    assert_receive {:effect_observed, %Effect.ReadReview{},
                    %Observation.ReviewUnreadable{
                      reason: %{"reason" => "review_unreadable", "class" => "enoent"}
                    }}
  end

  describe "a store rejection is not a shape a Dispatch adapter can return" do
    setup do
      on_exit(&RejectingDispatch.erase/0)
      :ok
    end

    # `PromptStore` really does produce these pairs, and the host really does normalize them
    # into the two names an observation carries -- at `Effect.RetainPrompt` and
    # `Effect.FetchPrompt`, where it runs the store, and nowhere else. That proof lives in
    # `test/lifecycle/run_fsm_prompt_retention_test.exs`, which is the file with a filesystem
    # seam in it; it asserts the normalized map on the failed observation, before the reducer
    # is stepped with it.
    #
    # These two are the converse, and they belong here rather than there. `AiOrchestrator.Dispatch`
    # declares `deliver/2` and `observe/2` as `{:ok, map()} | {:error, map()}`, so a tuple is
    # outside the behaviour no matter what is spelled inside it, and `invalid_return/2` is the
    # entire answer.
    #
    # Without them the two halves are indistinguishable from outside the host, and the
    # difference is not cosmetic. Recognizing store pairs at the Dispatch seam would make
    # `{:error, reason}` the one door through which an adapter -- including one a pane, a
    # provider or a plugin supplied -- puts a name of its own choosing from the prompt
    # vocabulary in front of an operator, by wrapping it in a tuple. Every other shape an
    # adapter can return is already refused that, one line away, by the same function.
    test "a tuple spelling a real store pair is still an invalid return" do
      pair = {:prompt_open_failed, :eacces}

      assert pair in PromptRejection.pairs(), """
      These tests are about a pair the store can genuinely produce; an invented one would
      prove nothing an arbitrary tuple does not already prove. `#{inspect(pair)}` is not in
      the table, so either the table moved or this premise did.
      """

      for {adapter, expected} <- [
            {RejectingDispatch, "dispatch_invalid_return"},
            {RejectingObserve, "observe_invalid_return"}
          ] do
        RejectingDispatch.put(pair)
        {scenario, opts} = fresh_opts()
        opts = Keyword.put(opts, :dispatch, adapter)

        assert {:error, error} = Host.run(H.spec(scenario), H.plan(scenario), opts)

        assert %{"reason" => ^expected, "result_class" => "tuple", "digest" => "sha256:" <> digest} = error
        assert byte_size(digest) == 64

        refute inspect(error) =~ "prompt_open_failed", """
        `#{inspect(adapter)}` returned `{:error, #{inspect(pair)}}` and the run reported
        `#{inspect(error)}`. Echoing the reason would not merely be untidy: the store's
        vocabulary is closed because it is the set of names an operator's routing is written
        against, and a seam that reflects whatever atom it was handed is a seam through which
        that set is open.
        """
      end
    end

    test "a tuple carrying a path is reported by class and digest, and the path does not survive" do
      path = "prompts/as_0001-" <> String.duplicate("b", 64) <> ".org"
      RejectingDispatch.put({:prompt_object_missing, path})
      {scenario, opts} = fresh_opts()
      opts = Keyword.put(opts, :dispatch, RejectingDispatch)

      assert {:error, error} = Host.run(H.spec(scenario), H.plan(scenario), opts)

      assert %{"reason" => "dispatch_invalid_return", "result_class" => "tuple"} = error

      refute inspect(error, limit: :infinity, printable_limit: :infinity) =~ "as_0001", """
      The two path-carrying reasons are the ones where reflecting the second element would
      copy a payload rather than a name. `describe/1` reduces the whole term to a class and a
      digest, which is the property that does not depend on anyone checking the arm first.
      """
    end
  end
end
