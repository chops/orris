defmodule AiOrchestrator.Test.ScriptedDispatchReceiptTest do
  use ExUnit.Case, async: true

  defmodule Fixture do
    @moduledoc false
    use AiOrchestrator.Test.ScriptedDispatchReceipt

    def deliver(_command, opts), do: Keyword.fetch!(opts, :result)
  end

  @command %{"send_message_id" => "id", "pane_ref" => "%1", "payload_hash" => "hash"}

  test "receipt observes the actual synthetic result and binds pane plus payload" do
    assert {:ok, %{"outcome" => "ambiguous"}} = Fixture.reconcile(@command, [])
    sent = {:ok, %{"send_status" => "ok"}}
    assert Fixture.deliver(@command, result: sent) == sent
    assert {:ok, %{"outcome" => "delivered"}} = Fixture.reconcile(@command, [])

    for field <- ["pane_ref", "payload_hash"] do
      assert {:ok, %{"outcome" => "conflict"}} = Fixture.reconcile(Map.put(@command, field, "other"), [])
    end
  end

  test "an unknown callback process cannot report absence after synthetic delivery" do
    Fixture.deliver(@command, result: {:ok, %{"send_status" => "ok"}})
    task = Task.async(fn -> Fixture.reconcile(@command, []) end)
    assert {:ok, %{"outcome" => "ambiguous"}} = Task.await(task)
  end

  test "queued stays queued and invalid fixture results are not rewritten into success" do
    Fixture.deliver(@command, result: {:ok, %{"send_status" => "queued"}})
    assert {:ok, %{"outcome" => "queued"}} = Fixture.reconcile(@command, [])
    result = {:error, %{"reason" => "scripted_failure"}}
    assert Fixture.deliver(@command, result: result) == result
    assert {:ok, %{"outcome" => "ambiguous"}} = Fixture.reconcile(@command, [])
  end
end
