defmodule AiOrchestrator.Test.DispatchAdapterConformance do
  @moduledoc """
  The rows every `AiOrchestrator.Dispatch` adapter must satisfy, stated once and
  instantiated per adapter.

  NS-16.L names "common conformance" as REQUIRED foundational adapter coverage, and the
  R06 audit records the problem with it: LocalPane is the only implemented adapter, so a
  suite written against LocalPane proves nothing about what is common. The suite therefore
  has to exist BEFORE the second adapter does, and it has to be written so that the second
  adapter inherits it rather than being tested separately -- otherwise "common conformance"
  is discovered at the moment it is most expensive to fix.

  So these rows are about the behaviour, never about how an adapter reaches its daemon. A
  harness module supplies the adapter and, for each named scenario, the command and the
  options that put THAT adapter in that scenario; nothing here knows about `ap`, sockets,
  panes or fixtures. An adapter that claims the behaviour satisfies the same rows or it
  does not claim the behaviour.

  The scenarios a harness must be able to produce:

    * `:admitting` -- the daemon answers the capability query and declares
      `delivery_reconcile`.
    * `:capability_absent` -- it answers, and declares something else.
    * `:capability_malformed` -- it answers with a token outside the token grammar.
    * `:capability_query_failed` -- the capability query itself does not answer.
    * `{:reconcile, outcome}` -- it answers the reconcile with that legal outcome.
    * `:reconcile_invalid` -- it answers with a word outside the closed union.
    * `:observe` -- an environment in which `observe/2` returns without waiting.

  Every scenario is a fact about the DAEMON, not about the adapter, which is what makes
  the same row meaningful for an adapter that speaks a different transport.

  A harness guarantees one more thing: any pane effect reaches the test process as
  `{:conformance_paste, term()}`, so a row can assert that a refusal happened BEFORE the
  effect rather than merely instead of its result.
  """

  alias AiOrchestrator.Dispatch

  @doc "The closed reconcile union, as ipc-v2.org fixes it."
  @spec outcomes() :: [String.t()]
  def outcomes, do: ~w(delivered queued absent ambiguous conflict)

  @doc "The behaviour's required callbacks; `reconcile/2` is optional and is checked separately."
  @spec required_callbacks() :: [{atom(), arity()}]
  def required_callbacks, do: [snapshot: 2, deliver: 2, observe: 2, capabilities: 1]

  @doc """
  The behaviours an adapter module declares.

  Read from the compiled module rather than from a source grep: a `@behaviour` that was
  deleted while the functions stayed would still pass a grep and would still lose every
  `@impl` warning that keeps an adapter in step with the contract.
  """
  @spec declared_behaviours(module()) :: [module()]
  def declared_behaviours(adapter) do
    Code.ensure_loaded!(adapter)

    :attributes
    |> adapter.module_info()
    |> Keyword.get_values(:behaviour)
    |> List.flatten()
  end

  @doc """
  Whether a result is one of the four shapes the behaviour's callbacks may answer.

  Both elements matter: the tag says what the caller must do, and the map is where a
  reason or a datum lives. A bare atom or a string answer is outside the contract however
  well it reads.
  """
  @spec shaped?(term(), [atom()]) :: boolean()
  def shaped?({tag, value}, tags), do: tag in tags and is_map(value)
  def shaped?(_result, _tags), do: false

  @doc """
  Whether an error is a refusal rather than a disguised answer about delivery.

  A refusal names a reason and says nothing about a send: an error map carrying
  `send_status` or a reconcile `outcome` would let a caller read a failure as a delivery.
  """
  @spec refusal?(term()) :: boolean()
  def refusal?(%{"reason" => reason} = error) when is_binary(reason), do: not Map.has_key?(error, "send_status")
  def refusal?(_error), do: false

  @doc false
  @spec preflight(module(), keyword()) :: :ok | {:error, map()}
  def preflight(adapter, opts), do: Dispatch.preflight(adapter, opts)

  @doc """
  The harness an instantiation supplies.

  `opts/1` returns the adapter options that put the adapter in the named scenario; it is
  called fresh for every row, so a harness may build per-row doubles in it.
  """
  @callback command() :: map()
  @callback opts(scenario :: term()) :: keyword()

  # The rows are grouped so each generated block stays readable on its own; an instantiation
  # gets all four, because a partial conformance is not conformance.
  defmacro __using__(options) do
    quote do
      alias AiOrchestrator.Test.DispatchAdapterConformance, as: Conformance

      @conformance_adapter Keyword.fetch!(unquote(options), :adapter)
      @conformance_harness Keyword.fetch!(unquote(options), :harness)

      defp conformance_opts(scenario), do: @conformance_harness.opts(scenario)
      defp conformance_command, do: @conformance_harness.command()

      defp conformance_pasted? do
        receive do
          {:conformance_paste, _detail} -> true
        after
          0 -> false
        end
      end

      unquote(behaviour_surface_rows())
      unquote(capability_rows())
      unquote(preflight_rows())
      unquote(reconcile_rows())
      unquote(gate_l_dimension_rows())
    end
  end

  defp behaviour_surface_rows do
    quote do
      describe "#{inspect(@conformance_adapter)} conformance: the behaviour surface" do
        test "declares AiOrchestrator.Dispatch and exports every required callback" do
          assert AiOrchestrator.Dispatch in Conformance.declared_behaviours(@conformance_adapter),
                 "an adapter that does not declare the behaviour loses every @impl check that keeps it in step"

          for {function, arity} <- Conformance.required_callbacks() do
            assert function_exported?(@conformance_adapter, function, arity),
                   "#{function}/#{arity} is a required callback, not an optional one"
          end
        end

        test "snapshot/2 is required rather than optional, and answers the two-shape grammar" do
          result = @conformance_adapter.snapshot(conformance_command(), conformance_opts(:admitting))

          assert Conformance.shaped?(result, [:ok, :error]),
                 "MUST-7: an adapter that cannot take a baseline cannot be trusted with one it did not take; got #{inspect(result)}"
        end

        test "observe/2 answers only the four shapes the behaviour names" do
          result = @conformance_adapter.observe(conformance_command(), conformance_opts(:observe))

          assert Conformance.shaped?(result, [:ok, :blocked, :pending, :error]),
                 "observe/2 is a four-valued question; got #{inspect(result)}"
        end
      end
    end
  end

  defp capability_rows do
    quote do
      describe "#{inspect(@conformance_adapter)} conformance: the declared capability" do
        test "capabilities/1 answers a closed set of well-formed tokens, and answers it the same way twice" do
          opts = conformance_opts(:admitting)
          assert {:ok, tokens} = @conformance_adapter.capabilities(opts)
          assert is_list(tokens)

          assert AiOrchestrator.Dispatch.valid_capabilities?(tokens),
                 "every declared token must pass the wire token grammar; got #{inspect(tokens)}"

          assert "delivery_reconcile" in tokens,
                 "the admitting scenario is the one where the daemon declares the capability that authorizes a send"

          assert {:ok, ^tokens} = @conformance_adapter.capabilities(conformance_opts(:admitting)),
                 "the declared set is read from the daemon on this invocation, not accumulated across calls"
        end

        test "a malformed token is refused rather than filtered out of an otherwise usable set" do
          assert {:error, error} = @conformance_adapter.capabilities(conformance_opts(:capability_malformed))
          assert Conformance.refusal?(error)
          assert error["reason"] == "dispatch_capabilities_invalid"
        end
      end
    end
  end

  defp preflight_rows do
    quote do
      describe "#{inspect(@conformance_adapter)} conformance: preflight decides before any effect" do
        test "the declared capability admits, and its absence is the typed refusal that names it" do
          assert :ok = Conformance.preflight(@conformance_adapter, conformance_opts(:admitting))

          assert {:error, error} = Conformance.preflight(@conformance_adapter, conformance_opts(:capability_absent))
          assert error["reason"] == "dispatch_preflight_unsupported"
          assert error["detector"] == "dispatch_preflight"

          assert "delivery_reconcile" in error["missing_capabilities"],
                 "the operator's next action is to upgrade a daemon, so the refusal names what was missing"
        end

        # MEASURED, not assumed. The adapter distinguishes a malformed declaration from an
        # unanswerable query -- row "a malformed token is refused" above asserts it at
        # `capabilities/1` -- but `Dispatch.preflight/2` maps every `{:error, _}` the
        # adapter returns onto one word (`dispatch_capabilities_failed`, dispatch.ex:45), so
        # the distinction does not survive the step that journals the refusal. Both harnesses
        # showed this, which is what makes it a fact about the behaviour rather than about
        # LocalPane. What preflight DOES guarantee is pinned here; narrowing the word is a
        # refusal-vocabulary change and is left to its own reviewed slice.
        test "a malformed declaration and an unanswerable query both refuse, and neither is read as an old daemon" do
          for scenario <- [:capability_malformed, :capability_query_failed] do
            assert {:error, error} = Conformance.preflight(@conformance_adapter, conformance_opts(scenario)),
                   "#{inspect(scenario)} must never admit a send"

            assert error["detector"] == "dispatch_preflight", inspect(scenario)
            assert Conformance.refusal?(error), inspect(scenario)

            assert error["reason"] == "dispatch_capabilities_failed",
                   "preflight collapses both onto one word today; #{inspect(scenario)} gave #{inspect(error["reason"])}"

            refute error["reason"] == "dispatch_preflight_unsupported",
                   "an unanswerable query is not proof of an old daemon, and must not send an operator to upgrade one"
          end
        end

        test "deliver/2 refuses BEFORE any pane effect when the capability is absent" do
          result = @conformance_adapter.deliver(conformance_command(), conformance_opts(:capability_absent))

          assert {:error, error} = result
          assert error["reason"] == "dispatch_preflight_unsupported"

          refute conformance_pasted?(),
                 "R4: a daemon with no receipt cannot say a prompt is absent, so nothing may reach the pane"
        end

        test "deliver/2 refuses before any pane effect when the capability query cannot be answered" do
          assert {:error, error} =
                   @conformance_adapter.deliver(conformance_command(), conformance_opts(:capability_query_failed))

          assert error["reason"] == "dispatch_capabilities_failed"
          refute conformance_pasted?()
        end
      end
    end
  end

  defp reconcile_rows do
    quote do
      describe "#{inspect(@conformance_adapter)} conformance: the reconcile union" do
        test "reconcile/2 is exported by an adapter whose capability set declares delivery_reconcile" do
          assert function_exported?(@conformance_adapter, :reconcile, 2),
                 "declaring delivery_reconcile without implementing reconcile/2 is a capability the adapter cannot honour"
        end

        test "each of the five legal outcomes is accepted and read as that outcome and nothing else" do
          for outcome <- Conformance.outcomes() do
            assert {:ok, answer} =
                     @conformance_adapter.reconcile(conformance_command(), conformance_opts({:reconcile, outcome})),
                   outcome

            assert answer["outcome"] == outcome, outcome
            assert answer["outcome"] in Conformance.outcomes(), outcome
          end
        end

        test "a word outside the union is refused, and the refusal is not an answer about delivery" do
          result = @conformance_adapter.reconcile(conformance_command(), conformance_opts(:reconcile_invalid))

          assert {:error, error} = result, "a sixth outcome must not be answered; got #{inspect(result)}"
          assert Conformance.refusal?(error)
          refute Map.has_key?(error, "outcome")
        end

        test "every refusal this adapter can produce is a reason and never a delivery claim" do
          results = [
            {:capability_absent,
             @conformance_adapter.deliver(conformance_command(), conformance_opts(:capability_absent))},
            {:capability_query_failed,
             @conformance_adapter.deliver(conformance_command(), conformance_opts(:capability_query_failed))},
            {:reconcile_invalid,
             @conformance_adapter.deliver(conformance_command(), conformance_opts(:reconcile_invalid))},
            {:reconcile_invalid,
             @conformance_adapter.reconcile(conformance_command(), conformance_opts(:reconcile_invalid))}
          ]

          for {scenario, result} <- results do
            assert {:error, error} = result, inspect(scenario)
            assert Conformance.refusal?(error), "#{inspect(scenario)}: #{inspect(error)}"
          end

          refute conformance_pasted?(), "a refused delivery leaves no pane effect behind"
        end
      end
    end
  end

  # NS-29.L.000 / NS-29.L.001. Gate L names six dimensions (architecture:716): delivery, observation,
  # cancellation, identity, BACKPRESSURE and AMBIGUOUS FAILURE. The rows above cover delivery and
  # observation. `queued` and `ambiguous` are already members of the reconcile union the suite
  # exercises, but only as WORDS a `reconcile/2` answer may carry -- nothing here says what an adapter
  # must DO with them, which is what a Gate L dimension is about. These two rows say it.
  #
  # Cancellation and identity are deliberately NOT here: `AiOrchestrator.Dispatch` declares no `cancel`
  # and no `identity` callback (dispatch.ex:11-19) though architecture:387 names both, so those two
  # dimensions cannot be written without a behaviour change, which is its own reviewed contract change.
  defp gate_l_dimension_rows do
    quote do
      describe "#{inspect(@conformance_adapter)} conformance: the Gate L dimensions the union expresses" do
        # BACKPRESSURE. A queued receipt says the daemon accepted the bytes and has not pasted them.
        # The one thing an adapter may not do with it is paste again, and the second thing it may not
        # do is call it a send this process performed: EJ-7 fixes `send_status` as `ok | queued |
        # reconciled`, where `ok` alone names a fresh paste. Both delivered adapters answer a value
        # here, and they answer DIFFERENT values (`queued` for LocalPane, `reconciled` for the scripted
        # adapter), so the common row states the two things the behaviour actually fixes and not a
        # word one adapter happens to use.
        test "backpressure: a queued reconcile is not resent, and is never reported as a fresh send" do
          result = @conformance_adapter.deliver(conformance_command(), conformance_opts({:reconcile, "queued"}))

          assert {:ok, data} = result, "a queued receipt is an answer, not a refusal; got #{inspect(result)}"

          refute conformance_pasted?(),
                 "the daemon already holds these bytes: a second paste is a duplicate prompt"

          refute data["send_status"] == "ok",
                 "`ok` names a paste this process performed, and this process pasted nothing"

          assert data["send_status"] in ["queued", "reconciled"],
                 "a queued receipt must be carried under a status that says it was not freshly sent; " <>
                   "got #{inspect(data["send_status"])}"
        end

        # The discriminator that keeps the row above from passing for an adapter that never pastes at
        # all. `absent` is the ONE outcome that admits a paste, and under it the same command must
        # reach the pane.
        test "backpressure control: the one outcome that admits a paste does paste, so the row above is not vacuous" do
          assert {:ok, _data} =
                   @conformance_adapter.deliver(conformance_command(), conformance_opts({:reconcile, "absent"}))

          assert conformance_pasted?(),
                 "an absent receipt is the only outcome that may send, so this adapter never pastes at all " <>
                   "and the queued row above proves nothing"
        end

        # AMBIGUOUS FAILURE. The daemon cannot prove the bytes did not land. A guess either duplicates
        # work or drops it, so the only admissible answer is a refusal that names the ambiguity -- and
        # a refusal must not be readable as an answer about delivery.
        test "ambiguous failure: an unproven receipt refuses, names the ambiguity, and leaves the pane alone" do
          result = @conformance_adapter.deliver(conformance_command(), conformance_opts({:reconcile, "ambiguous"}))

          assert {:error, error} = result,
                 "an ambiguous receipt must not be answered as a delivery; got #{inspect(result)}"

          assert Conformance.refusal?(error), inspect(error)
          assert error["reason"] == "dispatch_reconcile_ambiguous", inspect(error)
          assert error["detector"] == "dispatch_reconcile", inspect(error)

          refute conformance_pasted?(),
                 "the bytes may already have landed: an ambiguous receipt may not be resolved by pasting"
        end

        # MEASURED, not assumed, and recorded here rather than fixed. `refusal?/1`'s own docstring says
        # "an error map carrying `send_status` or a reconcile `outcome` would let a caller read a
        # failure as a delivery", but the predicate only checks `send_status`. The two delivered
        # adapters diverge on exactly that: LocalPane's ambiguous refusal carries
        # `"outcome" => "ambiguous"` (local_pane.ex:113-114) and the scripted adapter's does not. So
        # this row pins the union of what both guarantee -- no `send_status` -- and names the
        # divergence instead of hiding it behind an assertion only one adapter can pass. Narrowing
        # `refusal?/1` to match its own docstring would change a delivered refusal shape and is a
        # refusal-vocabulary change, left to its own reviewed slice exactly as the preflight-word
        # collapse above is.
        test "ambiguous failure: whatever else a refusal carries, it never carries a send status" do
          for scenario <- [{:reconcile, "ambiguous"}, {:reconcile, "conflict"}] do
            assert {:error, error} = @conformance_adapter.deliver(conformance_command(), conformance_opts(scenario)),
                   inspect(scenario)

            refute Map.has_key?(error, "send_status"), "#{inspect(scenario)}: #{inspect(error)}"
            refute conformance_pasted?(), inspect(scenario)
          end
        end
      end
    end
  end
end
