defmodule AiOrchestrator.Dispatch.LocalPaneTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.LocalPane

  defmodule FakePaneClient do
    @moduledoc false
    # Every stand-in answers a reconcile: the adapter asks before every send.
    def reconcile(pane_ref, message_id, _opts),
      do:
        {:ok,
         %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, prompt, opts) do
      Process.send(Keyword.fetch!(opts, :test_pid), {:send_called, pane_ref, prompt}, [])

      {:ok,
       %{"ok" => true, "protocol_version" => 2, "status" => "sent", "msg_id" => opts[:message_id], "pane_id" => pane_ref}}
    end

    def status(pane_ref, opts) do
      case Keyword.get(opts, :pane_status_reader) do
        nil -> {:ok, Keyword.get(opts, :pane_status, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0})}
        reader -> reader.(pane_ref)
      end
    end
  end

  test "deliver sends the prompt and returns dispatch event data" do
    command =
      base_command()
      |> Map.put("prompt", "Implement item A")
      |> Map.put("replayed", true)

    assert {:ok,
            %{
              "assignment_id" => "as_0001",
              "backend" => "local_pane",
              "pane_ref" => "pane_writer",
              "send_status" => "ok",
              "send_message_id" => "send_as_0001",
              "replayed" => true
            }} = LocalPane.deliver(command, pane_client: FakePaneClient, test_pid: self())

    assert_receive {:send_called, "pane_writer", "Implement item A"}
  end

  test "observe returns artifact data once the pane is idle" do
    artifact_reader = fn command ->
      {:ok,
       %{
         "assignment_id" => command["assignment_id"],
         "artifact_id" => command["artifact_id"],
         "path" => command["expected_artifact"],
         "match_kind" => "exact",
         "bytes" => 128,
         "sha256" => LocalPane.zero_hash(),
         "stable_for_ms" => 5000,
         "modified_after_dispatch" => true
       }}
    end

    assert {:ok,
            %{
              "assignment_id" => "as_0001",
              "artifact_id" => "art_as_0001",
              "path" => "lib/item_a.ex",
              "sha256" => "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            }} =
             LocalPane.observe(base_command(),
               artifact_reader: artifact_reader,
               pane_client: FakePaneClient
             )
  end

  test "observe hashes file artifacts when using the default reader" do
    root = tmp_dir()
    File.mkdir_p!(Path.join(root, "lib"))
    File.write!(Path.join(root, "lib/item_a.ex"), "defmodule ItemA, do: :ok\n")

    command =
      base_command()
      |> Map.put("artifact_baseline", %{"exists" => false})
      |> Map.put("repo_root", root)
      |> Map.put("stable_for_ms", 0)

    expected_hash = sha256("defmodule ItemA, do: :ok\n")

    assert {:ok,
            %{
              "path" => "lib/item_a.ex",
              "bytes" => 25,
              "sha256" => ^expected_hash
            }} = LocalPane.observe(command, pane_client: FakePaneClient)
  end

  test "observe refuses an unchanged artifact captured before dispatch" do
    root = tmp_dir()
    File.mkdir_p!(root)
    File.write!(Path.join(root, "item.txt"), "stale\n")

    command =
      base_command()
      |> Map.put("expected_artifact", "item.txt")
      |> Map.put("repo_root", root)

    assert {:ok, dispatch_data} = LocalPane.deliver(command, pane_client: FakePaneClient, test_pid: self())
    command = Map.put(command, "artifact_baseline", dispatch_data["artifact_baseline"])

    assert {:error,
            %{
              "reason" => "observation_timeout",
              "last_observation" => %{"reason" => "artifact_not_modified", "path" => "item.txt"}
            }} =
             LocalPane.observe(command,
               monotonic_ms: fn -> 0 end,
               observe_timeout_ms: 0,
               pane_client: FakePaneClient
             )
  end

  test "observe measures an unchanged fingerprint across the stability window" do
    root = tmp_dir()
    File.mkdir_p!(root)
    File.write!(Path.join(root, "item.txt"), "fresh\n")
    {:ok, times} = Agent.start_link(fn -> [0, 0, 0, 10] end)

    clock = fn ->
      Agent.get_and_update(times, fn
        [time | rest] -> {time, rest}
        [] -> {10, []}
      end)
    end

    command =
      base_command()
      |> Map.put("artifact_baseline", %{"exists" => false})
      |> Map.put("expected_artifact", "item.txt")
      |> Map.put("repo_root", root)
      |> Map.put("stable_for_ms", 10)

    assert {:ok, %{"modified_after_dispatch" => true, "stable_for_ms" => 10}} =
             LocalPane.observe(command,
               monotonic_ms: clock,
               observe_timeout_ms: 100,
               pane_client: FakePaneClient,
               poll_interval_ms: 0,
               sleeper: fn _milliseconds -> :ok end
             )
  end

  test "observe polls while the pane is busy and until the artifact exists" do
    {:ok, calls} = Agent.start_link(fn -> 0 end)

    status_reader = fn pane_ref ->
      call = Agent.get_and_update(calls, fn count -> {count, count + 1} end)
      state = if call == 0, do: "busy", else: "idle"
      {:ok, %{"state" => state, "pane_ref" => pane_ref, "pending_count" => 0}}
    end

    artifact_reader = fn command ->
      call = Agent.get_and_update(calls, fn count -> {count, count + 1} end)

      if call < 3 do
        {:pending, %{"reason" => "artifact_missing"}}
      else
        {:ok,
         %{
           "assignment_id" => command["assignment_id"],
           "artifact_id" => command["artifact_id"],
           "path" => command["expected_artifact"],
           "bytes" => 5,
           "sha256" => LocalPane.zero_hash()
         }}
      end
    end

    assert {:ok, %{"artifact_id" => "art_as_0001"}} =
             LocalPane.observe(base_command(),
               artifact_reader: artifact_reader,
               monotonic_ms: fn -> 0 end,
               observe_timeout_ms: 100,
               pane_client: FakePaneClient,
               pane_status_reader: status_reader,
               poll_interval_ms: 0,
               sleeper: fn _milliseconds -> :ok end
             )

    assert Agent.get(calls, & &1) == 5
  end

  test "observe reports an auth-blocked pane without reading artifacts" do
    artifact_reader = fn _command -> flunk("artifact should not be read while pane is blocked") end

    assert {:blocked,
            %{
              "reason" => "agent_auth_blocked",
              "pane_ref" => "pane_writer",
              "pane_state" => "blocked",
              "pending_count" => 1
            }} =
             LocalPane.observe(base_command(),
               artifact_reader: artifact_reader,
               pane_client: FakePaneClient,
               pane_status: %{"state" => "blocked", "pane_ref" => "pane_writer", "pending_count" => 1}
             )
  end

  defp base_command do
    %{
      "assignment_id" => "as_0001",
      "artifact_id" => "art_as_0001",
      "expected_artifact" => "lib/item_a.ex",
      "pane_ref" => "pane_writer",
      "prompt" => "",
      "repo_root" => "/tmp/example-repo",
      "send_message_id" => "send_as_0001",
      "payload_hash" => "sha256:" <> String.duplicate("ab", 32)
    }
  end

  defp tmp_dir do
    path = Path.join(System.tmp_dir!(), "ai_orchestrator_local_pane_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp sha256(contents) do
    digest = :sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower)
    "sha256:" <> digest
  end

  describe "the send reply shape" do
    defmodule ShapedPaneClient do
      @moduledoc false
      def reconcile(pane_ref, message_id, _opts),
        do:
          {:ok,
           %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

      def send(_pane_ref, _prompt, opts), do: {:ok, Keyword.fetch!(opts, :send_reply)}
      def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
      def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}
    end

    test "a map with no status is not a version two send reply, whatever it echoes" do
      reply = %{"ok" => true, "protocol_version" => 2, "msg_id" => "send_as_0001", "pane_id" => "pane_writer"}

      assert {:error, %{"reason" => "send_status_unmapped"}} =
               LocalPane.deliver(shaped_command(), pane_client: ShapedPaneClient, send_reply: reply)
    end

    test "a reply that is not a map is refused, never read as sent" do
      for reply <- [:sent, "sent", 1, [status: "sent"]] do
        result = LocalPane.deliver(shaped_command(), pane_client: ShapedPaneClient, send_reply: reply)

        assert match?({:error, %{"reason" => "send_reply_invalid"}}, result),
               "#{inspect(reply)} was accepted as a send reply"
      end
    end

    defp shaped_command do
      %{
        "assignment_id" => "as_0001",
        "artifact_id" => "art_as_0001",
        "expected_artifact" => "lib/item.ex",
        "pane_ref" => "pane_writer",
        "prompt" => "hello",
        "repo_root" => System.tmp_dir!(),
        "send_message_id" => "send_as_0001",
        "payload_hash" => "sha256:" <> String.duplicate("ab", 32),
        "stable_for_ms" => 0
      }
    end
  end
end
