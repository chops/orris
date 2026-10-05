defmodule AiOrchestrator.Dispatch.PaneClientTypedStatusRedTest do
  @moduledoc """
  B3a RED (design r7, D/B3A-RED-DESIGN-r7.org): the typed v1 pane_status contract on the production transport.
  Driven only through the existing `:runner` seam, which replaces the `ap` process and nothing else.

  - A failed status reply whose decoded last JSON object is `"ok": false` with `"error"` exactly `pane_dead` or
    `pane_not_found`, and whose `pane_id` echoes the requested pane, answers `{:error, %{"reason" => code}}` and nothing
    from the reply; at exit 0 and at exit 1.
  - A typed refusal or an `"ok": true` reply whose echo names another pane answers `reply_pane_mismatch`; one with no
    echo answers `reply_pane_missing` (the reasons the v2 reconcile path already uses). `"ok": true` is bound at
    exit 0 only.
  - Controls: an `"ok": true` reply at a nonzero exit, and any other failure, stay `ap_failed` with a digest; a
    correctly echoed success stays `{:ok, map}`.
  - Expected at this head: exactly fifteen failures (the twelve generated typed/echo rows, the CANARY row and the two
    ok:true echo rows, every one named RED); the three controls pass.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.PaneClient

  @pane "pane_writer"
  @fixtures Path.expand("../fixtures/contracts/ipc/v1", __DIR__)

  defp fixture(name, pane_id) do
    @fixtures
    |> Path.join(name)
    |> File.read!()
    |> String.replace("<pane_id>", pane_id)
    |> String.replace("<agent>", "claude")
    |> String.replace("<classifier>", "fingerprint")
    |> String.replace("<state>", "idle")
  end

  defp without_echo(name) do
    decoded = name |> fixture(@pane) |> Jason.decode!() |> Map.delete("pane_id")
    Jason.encode!(decoded) <> "\n"
  end

  defp status(bytes, exit_status) do
    PaneClient.status(@pane, runner: fn _ap_path, _args, _opts -> {bytes, exit_status} end)
  end

  for code <- ["pane_dead", "pane_not_found"], exit_status <- [0, 1] do
    @code code
    @exit_status exit_status

    test "RED #{code} at exit #{exit_status} is the typed refusal and nothing else" do
      assert status(fixture("pane_status.error.#{@code}.json", @pane), @exit_status) ==
               {:error, %{"reason" => @code}}
    end

    test "RED #{code} at exit #{exit_status} echoing another pane is reply_pane_mismatch" do
      assert status(fixture("pane_status.error.#{@code}.json", "pane_reviewer"), @exit_status) ==
               {:error, %{"reason" => "reply_pane_mismatch"}}
    end

    test "RED #{code} at exit #{exit_status} without an echo is reply_pane_missing" do
      assert status(without_echo("pane_status.error.#{@code}.json"), @exit_status) ==
               {:error, %{"reason" => "reply_pane_missing"}}
    end
  end

  test "RED a typed refusal never carries other reply fields" do
    bytes = ~s({"error":"pane_dead","ok":false,"pane_id":"#{@pane}","detail":"CANARY_b3a"}\n)
    result = status(bytes, 1)

    assert result == {:error, %{"reason" => "pane_dead"}}
    refute inspect(result) =~ "CANARY_b3a"
  end

  test "RED ok:true at exit 0 echoing another pane is reply_pane_mismatch" do
    assert status(fixture("pane_status.ok.json", "pane_reviewer"), 0) ==
             {:error, %{"reason" => "reply_pane_mismatch"}}
  end

  test "RED ok:true at exit 0 without an echo is reply_pane_missing" do
    assert status(without_echo("pane_status.ok.json"), 0) == {:error, %{"reason" => "reply_pane_missing"}}
  end

  test "control: ok:true at a nonzero exit is not a successful status read" do
    result = status(fixture("pane_status.ok.json", @pane), 1)

    assert match?({:error, %{"reason" => "ap_failed", "exit_status" => 1, "output_digest" => "sha256:" <> _}}, result),
           inspect(result)
  end

  test "control: any other failure stays ap_failed with a digest" do
    result = status(~s({"ok":false,"error":"internal","pane_id":"#{@pane}"}\n), 1)

    assert match?({:error, %{"reason" => "ap_failed", "exit_status" => 1}}, result), inspect(result)
  end

  test "control: a correctly echoed success stays a status map" do
    result = status(fixture("pane_status.ok.json", @pane), 0)

    assert match?({:ok, %{"ok" => true, "pane_id" => @pane, "state" => "idle"}}, result), inspect(result)
  end
end
