defmodule AiOrchestrator.Dispatch.PaneClientContractTest do
  @moduledoc """
  Wave 1 consumer half of the IPC v1 contract (NS-39): every reply shape ai-pair freezes in
  test/fixtures/contracts/ipc/v1 is decoded exactly as the daemon emits it, even when `ap`
  prints log lines before the JSON. `pane_status` and `send` fixtures go through the real
  PaneClient operations with a fake runner; `ping` has no PaneClient operation, so its
  fixture is decode-only coverage through `PaneClient.decode_reply/1`. The typed-outcome
  table is the contract Wave 6 (Dispatch.DaemonPane) implements; here it only pins which
  fixtures are successes, typed refusals the orchestrator must treat as observations, and
  request errors.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.PaneClient

  @fixture_dir Path.expand("../fixtures/contracts/ipc/v1", __DIR__)

  # fixture => {command, expected decoded outcome class}
  @outcomes %{
    "ping.ok.json" => {:decode_only, :ok},
    "pane_status.ok.json" => {:pane_status, :ok},
    "pane_status.error.missing_pane_id.json" => {:pane_status, :request_error},
    "pane_status.error.pane_not_found.json" => {:pane_status, :typed_refusal},
    "pane_status.error.pane_dead.json" => {:pane_status, :typed_refusal},
    "send.sent.json" => {:send, :ok},
    "send.queued.json" => {:send, :ok},
    "send.error.missing_pane_id.json" => {:send, :request_error},
    "send.error.missing_text.json" => {:send, :request_error},
    "send.error.pane_not_found.json" => {:send, :typed_refusal},
    "send.error.pane_dead.json" => {:send, :typed_refusal},
    "send.error.queue_full.json" => {:send, :typed_refusal},
    "send.error.oversize.json" => {:send, :request_error},
    "send.error.send_timeout.json" => {:send, :typed_refusal},
    "send.error.paste_failed.json" => {:send, :typed_refusal}
  }

  @states ~w(idle busy dialog dead unknown)
  @queue_reasons ~w(debounce busy dialog unknown)

  test "the outcome table names every fixture exactly once" do
    files = @fixture_dir |> Path.join("*.json") |> Path.wildcard() |> Enum.map(&Path.basename/1) |> Enum.sort()
    assert files == Enum.sort(Map.keys(@outcomes))
  end

  for {file, {command, outcome}} <- @outcomes do
    test "#{file} decodes as a #{outcome} #{command} reply" do
      assert_fixture_decodes(unquote(file), unquote(command), unquote(outcome))
    end
  end

  # Runtime helpers keep every branch reachable from the compiler's point of view; the
  # generated tests above only pass values in.
  defp assert_fixture_decodes(file, command, outcome) do
    raw = File.read!(Path.join(@fixture_dir, file))
    expected = Jason.decode!(raw)
    output = "12:00:00.000 [info] OTLP exporter successfully initialized\n" <> raw <> "\n"

    assert {:ok, decoded} = decode_via(command, output)
    assert decoded == expected
    assert_outcome_shape(outcome, decoded)
  end

  defp decode_via(:send, output) do
    PaneClient.send("<pane_id>", "hello", ap_path: "/tmp/ap", input_runner: fn _, _, _, _ -> {output, 0} end)
  end

  defp decode_via(:pane_status, output) do
    PaneClient.status("<pane_id>", ap_path: "/tmp/ap", runner: fn _, _, _ -> {output, 0} end)
  end

  defp decode_via(:decode_only, output), do: PaneClient.decode_reply(output)

  defp assert_outcome_shape(:ok, decoded), do: assert(decoded["ok"] == true)

  defp assert_outcome_shape(:typed_refusal, decoded) do
    assert decoded["ok"] == false
    assert is_binary(decoded["pane_id"])
    assert is_binary(decoded["error"])
  end

  defp assert_outcome_shape(:request_error, decoded) do
    assert decoded["ok"] == false
    assert is_binary(decoded["error"])
  end

  test "pane_status.ok carries the documented dynamic fields and state placeholder" do
    fixture = Jason.decode!(File.read!(Path.join(@fixture_dir, "pane_status.ok.json")))

    assert fixture |> Map.keys() |> Enum.sort() == ~w(agent classifier ok pane_id pending_count state)
    assert fixture["state"] == "<state>" and fixture["classifier"] == "<classifier>" and fixture["agent"] == "<agent>"
    assert is_integer(fixture["pending_count"]) and fixture["pending_count"] >= 0
  end

  test "send.queued carries a queue_reason from the documented closed set" do
    fixture = Jason.decode!(File.read!(Path.join(@fixture_dir, "send.queued.json")))
    assert fixture["status"] == "queued" and fixture["queue_reason"] in @queue_reasons
  end

  test "the state and queue_reason vocabularies are documented in the vendored contract" do
    doc = File.read!(Path.expand("../../docs/contracts/ipc-v1.md", __DIR__))
    for state <- @states, do: assert(String.contains?(doc, "`#{state}`"), "#{state} missing from ipc-v1.md")
    for reason <- @queue_reasons, do: assert(String.contains?(doc, "`#{reason}`"), "#{reason} missing from ipc-v1.md")
  end
end
