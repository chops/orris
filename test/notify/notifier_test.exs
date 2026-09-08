defmodule AiOrchestrator.Notify.NotifierTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.Notify.Notifier

  test "request_data hashes the bounded notification payload" do
    assert %{
             "notification_id" => "n_0001",
             "trigger_event_id" => "ev_0001",
             "channel" => "shell_hook",
             "payload_hash" => payload_hash
           } = Notifier.request_data("n_0001", "ev_0001", "payload")

    assert payload_hash == sha256("payload")
  end

  describe "deliver/4" do
    test "hook success returns sent data" do
      runner = fn _cmd, _args, _opts -> {"", 0} end

      assert {:sent, %{"notification_id" => "n_0001", "channel" => "shell_hook"}} =
               Notifier.deliver(["notify-hook"], "n_0001", "payload", runner: runner)
    end

    test "hook failure returns failed data, never an error tuple" do
      runner = fn _cmd, _args, _opts -> {"boom", 7} end
      assert {:failed, data} = Notifier.deliver(["notify-hook"], "n_0001", "payload", runner: runner)
      assert data["reason"] == "hook_exit_7"
    end

    test "raising hook is contained, exactly one attempt, no retry" do
      {:ok, agent} = Agent.start_link(fn -> 0 end)

      runner = fn _cmd, _args, _opts ->
        Agent.update(agent, &(&1 + 1))
        raise "hook exploded"
      end

      assert {:failed, data} = Notifier.deliver(["notify-hook"], "n_0001", "payload", runner: runner)
      assert data["reason"] == "hook_raised"
      assert Agent.get(agent, & &1) == 1
    end

    test "non-argv hook is rejected as failed data" do
      assert {:failed, %{"reason" => "hook_not_argv"}} = Notifier.deliver("notify-hook", "n_0001", "payload")
    end
  end

  describe "payload bounding" do
    property "bounded payload never exceeds the cap and stays valid UTF-8" do
      check all(payload <- StreamData.string(:printable, max_length: 5000)) do
        bounded = Notifier.bound_payload(payload)
        assert byte_size(bounded) <= 2048
        assert String.valid?(bounded)
      end
    end
  end

  describe "notification events never alter domain state (deferred EJ-13 coverage)" do
    property "appending a notification cycle to a terminal journal leaves the fold summary unchanged" do
      base_lines = F.lines("scenarios", "gated_run_seed")
      {:ok, base_state} = Fold.fold_lines(base_lines)
      base_summary = Fold.summary(base_state)
      last_seq = base_state.last_seq

      check all(
              status <- StreamData.member_of(["notification_sent", "notification_failed"]),
              payload <- StreamData.string(:printable, max_length: 100),
              reason <- StreamData.string(:printable, min_length: 1, max_length: 100)
            ) do
        requested =
          notification_event(last_seq + 1, "notification_requested", %{
            "notification_id" => "n_prop",
            "trigger_event_id" => "ev_#{last_seq}",
            "channel" => "shell_hook",
            "payload_hash" => sha256(payload)
          })

        result_data =
          case status do
            "notification_sent" -> %{"notification_id" => "n_prop", "channel" => "shell_hook"}
            "notification_failed" -> %{"notification_id" => "n_prop", "channel" => "shell_hook", "reason" => reason}
          end

        result = notification_event(last_seq + 2, status, result_data)
        {:ok, state} = Fold.fold_lines(base_lines ++ [Jason.encode!(requested), Jason.encode!(result)])

        assert Fold.summary(state) == Map.put(base_summary, "last_seq", last_seq + 2)
      end
    end

    defp notification_event(seq, type, data) do
      %{
        "schema" => "ai-orchestrator/journal-event",
        "schema_version" => 1,
        "event_version" => 1,
        "seq" => seq,
        "event_id" => "ev_#{String.pad_leading(Integer.to_string(seq), 4, "0")}",
        "type" => type,
        "ts" => "2026-01-01T01:00:00Z",
        "run_id" => "run_scenario_0001",
        "actor" => "run_supervisor",
        "data" => data
      }
    end
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end
end
