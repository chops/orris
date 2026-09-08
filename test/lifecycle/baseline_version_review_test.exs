defmodule AiOrchestrator.Lifecycle.BaselineVersionReviewTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane
  alias AiOrchestrator.Journal.Event
  alias AiOrchestrator.Lifecycle.Host

  @root Path.expand("../fixtures/contracts/journal/versions", __DIR__)

  test "raw journal input cannot claim the synthetic unrecorded view" do
    line = File.read!(Path.join(@root, "assignment_prompt_projected.v2.unrecorded.malformed.json"))
    assert {:error, _} = Event.validate_line(line)
  end

  test "a malformed historical projection fails closed before the upcaster" do
    line = Jason.encode!(%{"type" => "assignment_prompt_projected", "event_version" => 1})
    assert {:error, _} = Host.cancel([line])
  end

  defmodule Peer do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def reconcile(pane, id, _opts),
      do: {:ok, %{"ok" => true, "protocol_version" => 2, "pane_id" => pane, "msg_id" => id, "outcome" => "absent"}}

    def send(pane, _text, opts) do
      send(Keyword.fetch!(opts, :test_pid), :paste_called)

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "pane_id" => pane, "msg_id" => opts[:message_id], "status" => "sent"}}
    end
  end

  defmodule BadSnapshot do
    @moduledoc false
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false, "unexpected" => "private baseline payload"}}
    def deliver(command, opts), do: opts[:inner].deliver(command, opts[:inner_opts])
    def observe(command, opts), do: opts[:inner].observe(command, opts[:inner_opts])
  end

  test "snapshot boundary rejects maps outside the closed baseline grammar" do
    alias AiOrchestrator.Contract.Effect
    alias AiOrchestrator.Contract.Observation
    alias AiOrchestrator.Test.ScenarioHarness, as: H

    {_name, :run, scenario, [], opts_fun} = Enum.at(H.cases(), 0)
    H.reset_seams()
    opts = opts_fun.()
    owner = self()

    _result =
      Host.run(
        H.spec(scenario),
        H.plan(scenario),
        Keyword.merge(opts,
          dispatch: BadSnapshot,
          dispatch_opts: [inner: opts[:dispatch], inner_opts: Keyword.get(opts, :dispatch_opts, [])],
          effect_observer: fn
            %Effect.SnapshotArtifact{}, observation -> send(owner, {:snapshot_observation, observation})
            _, _ -> :ok
          end
        )
      )

    assert_received {:snapshot_observation,
                     %Observation.ArtifactSnapshotFailed{reason: %{"reason" => "dispatch_snapshot_invalid_return"}}}
  end

  test "a projected legacy assignment cannot take an unjournaled replacement snapshot before send" do
    root = Path.join(System.tmp_dir!(), "baseline-review-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    File.write!(Path.join(root, "artifact"), "already existing artifact")

    command = %{
      "assignment_id" => "as_0001",
      "pane_ref" => "%" <> "test",
      "repo_root" => root,
      "expected_artifact" => "artifact",
      "prompt" => "review input",
      "send_message_id" => "snd_" <> String.duplicate("a", 64),
      "payload_hash" => "sha256:" <> String.duplicate("b", 64),
      "artifact_baseline" => %{"status" => "unrecorded"}
    }

    result = LocalPane.deliver(command, pane_client: Peer, test_pid: self())
    refute_received :paste_called
    assert {:error, _} = result
  end
end
