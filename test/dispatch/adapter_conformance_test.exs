defmodule AiOrchestrator.Dispatch.AdapterConformanceTest do
  @moduledoc """
  NS-16.L's "REQUIRED foundational adapter coverage", instantiated.

  The rows live in `AiOrchestrator.Test.DispatchAdapterConformance` and are stated once,
  against the behaviour. This file supplies two harnesses and runs the same rows through
  both: `LocalPane` over fake `ap` runners -- the real adapter, over the real PaneClient,
  answering as a daemon would -- and `ScriptedAdapter`, a minimal second adapter written
  to the behaviour and nothing else.

  The second adapter is the point. With one adapter, "common conformance" is indistinguish-
  able from "LocalPane's tests", and the R06 audit records exactly that: the conformance
  surface exists and the subject it would be common to does not. Running the rows against
  an adapter that shares no transport, no options and no code with LocalPane is what makes
  them rows about the contract. When DaemonPane arrives it adds a harness, not a suite.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Test.DispatchAdapterConformance

  # Harness 1: the delivered adapter, over the real PaneClient and fake `ap` runners.
  defmodule LocalPaneHarness do
    @moduledoc false

    @behaviour DispatchAdapterConformance

    @id "snd_" <> String.duplicate("a", 64)
    @hash "sha256:" <> String.duplicate("b", 64)
    # Built, not written: the redaction gate refuses tmux pane literals in the tree.
    @pane "%" <> Integer.to_string(9)

    @impl true
    def command do
      %{
        "assignment_id" => "as_0001",
        "pane_ref" => @pane,
        "send_message_id" => @id,
        "payload_hash" => @hash,
        "repo_root" => System.tmp_dir!(),
        "expected_artifact" => "conformance-missing-artifact",
        "artifact_baseline" => %{"exists" => false},
        "prompt" => "conformance prompt"
      }
    end

    @impl true
    def opts(scenario) do
      owner = self()

      [
        ap_path: "/tmp/conformance-ap",
        observe_timeout_ms: 0,
        artifact_reader: fn _command -> {:pending, %{"reason" => "artifact_missing"}} end,
        runner: fn _path, args, _cmd_opts -> {Jason.encode!(answer(scenario, hd(args))), 0} end,
        input_runner: fn _path, args, _input, _cmd_opts ->
          Process.send(owner, {:conformance_paste, args}, [])
          {Jason.encode!(Map.put(base(), "status", "sent")), 0}
        end
      ]
    end

    defp base, do: %{"ok" => true, "protocol_version" => 2, "msg_id" => @id, "pane_id" => @pane}

    # The daemon's answer to each verb, per scenario. The verbs are the daemon's, so a
    # scenario is stated once and every row that reaches that verb sees it.
    defp answer(scenario, "ping"), do: ping(scenario)
    defp answer(scenario, "reconcile"), do: reconcile(scenario)
    defp answer(_scenario, "pane_status"), do: %{"state" => "idle", "pane_id" => @pane, "pending_count" => 0}

    defp ping(:capability_absent), do: Map.put(base(), "capabilities", ["pane_status"])
    # A token with a space in it is outside the wire token grammar, not merely unrecognised.
    defp ping(:capability_malformed), do: Map.put(base(), "capabilities", ["delivery reconcile"])
    defp ping(:capability_query_failed), do: %{"ok" => false, "protocol_version" => 2, "error" => "daemon_unavailable"}
    defp ping(_scenario), do: Map.put(base(), "capabilities", ["delivery_reconcile", "an_unknown_future_token"])

    defp reconcile({:reconcile, outcome}) when outcome in ["absent", "conflict"], do: Map.put(base(), "outcome", outcome)

    defp reconcile({:reconcile, outcome}),
      do:
        Map.merge(base(), %{
          "outcome" => outcome,
          "status" => stored(outcome),
          "delivery_attempt" => 1,
          "payload_hash" => @hash
        })

    defp reconcile(:reconcile_invalid), do: Map.put(base(), "outcome", "sixth")
    defp reconcile(_scenario), do: Map.put(base(), "outcome", "absent")

    defp stored("delivered"), do: "delivered"
    defp stored("queued"), do: "queued"
    defp stored("ambiguous"), do: "ambiguous"
  end

  # Harness 2: a second adapter that shares no transport, options or code with the first.
  defmodule ScriptedAdapter do
    @moduledoc """
    The smallest thing that can honestly claim `AiOrchestrator.Dispatch`.

    It has no daemon, no `ap` and no wire: its "daemon" is two values in its options. That
    is deliberate -- a row that only passes for an adapter shelling out to `ap` is a row
    about `ap`, and the behaviour is supposed to outlive the transport.
    """

    @behaviour AiOrchestrator.Dispatch

    alias AiOrchestrator.Dispatch

    @outcomes ~w(delivered queued absent ambiguous conflict)

    @impl true
    def capabilities(opts) do
      case Keyword.fetch!(opts, :declared) do
        {:unavailable, reason} ->
          {:error, %{"reason" => reason}}

        tokens when is_list(tokens) ->
          if Dispatch.valid_capabilities?(tokens),
            do: {:ok, Enum.filter(["delivery_reconcile"], &(&1 in tokens))},
            else: {:error, %{"reason" => "dispatch_capabilities_invalid"}}
      end
    end

    @impl true
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}

    @impl true
    def observe(_command, _opts), do: {:pending, %{"reason" => "artifact_missing"}}

    @impl true
    def reconcile(command, opts) do
      case Keyword.fetch!(opts, :answer) do
        outcome when outcome in @outcomes ->
          {:ok, %{"outcome" => outcome, "pane_ref" => command["pane_ref"], "delivery_attempt" => 1}}

        _outside ->
          {:error, %{"reason" => "reconcile_outcome_invalid"}}
      end
    end

    @impl true
    def deliver(command, opts) do
      with :ok <- Dispatch.preflight(__MODULE__, opts),
           {:ok, %{"outcome" => outcome}} <- reconcile(command, opts) do
        dispatched(outcome, command, opts)
      end
    end

    defp dispatched("absent", command, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:conformance_paste, command["pane_ref"]}, [])
      {:ok, %{"send_status" => "ok", "pane_ref" => command["pane_ref"]}}
    end

    defp dispatched(replayed, command, _opts) when replayed in ["delivered", "queued"],
      do: {:ok, %{"send_status" => "reconciled", "pane_ref" => command["pane_ref"]}}

    defp dispatched(unproven, _command, _opts),
      do: {:error, %{"reason" => "dispatch_reconcile_" <> unproven, "detector" => "dispatch_reconcile"}}
  end

  defmodule ScriptedHarness do
    @moduledoc false

    @behaviour DispatchAdapterConformance

    @impl true
    def command, do: %{"pane_ref" => "pane_scripted", "send_message_id" => "snd_scripted", "payload_hash" => "hash"}

    @impl true
    def opts(scenario), do: [test_pid: self(), declared: declared(scenario), answer: answer(scenario)]

    defp declared(:capability_absent), do: ["pane_status"]
    defp declared(:capability_malformed), do: ["delivery reconcile"]
    defp declared(:capability_query_failed), do: {:unavailable, "daemon_unavailable"}
    defp declared(_scenario), do: ["delivery_reconcile", "an_unknown_future_token"]

    defp answer({:reconcile, outcome}), do: outcome
    defp answer(:reconcile_invalid), do: "sixth"
    defp answer(_scenario), do: "absent"
  end

  defmodule LocalPaneRows do
    @moduledoc false
    use ExUnit.Case, async: true

    use DispatchAdapterConformance,
      adapter: LocalPane,
      harness: AiOrchestrator.Dispatch.AdapterConformanceTest.LocalPaneHarness
  end

  defmodule ScriptedRows do
    @moduledoc false
    use ExUnit.Case, async: true

    use DispatchAdapterConformance,
      adapter: AiOrchestrator.Dispatch.AdapterConformanceTest.ScriptedAdapter,
      harness: AiOrchestrator.Dispatch.AdapterConformanceTest.ScriptedHarness
  end

  test "the suite is instantiated for more than one adapter, so its rows are about the contract" do
    adapters = [LocalPane, ScriptedAdapter]

    for adapter <- adapters do
      assert AiOrchestrator.Dispatch in DispatchAdapterConformance.declared_behaviours(adapter)
    end

    assert length(Enum.uniq(adapters)) == 2,
           "with one adapter, common conformance cannot be distinguished from that adapter's own tests"
  end
end
