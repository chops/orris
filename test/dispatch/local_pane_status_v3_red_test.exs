defmodule AiOrchestrator.Dispatch.LocalPaneStatusV3RedTest do
  @moduledoc """
  NS-15.G.002 B1c (scope r3): the product version 3 read, over the real PaneClient and fake `ap` runners.

  - `PaneClient.status_v3/2` runs `ap status <pane> --protocol-version 3` and answers the reply bytes, a typed refusal
    at any exit included; output holding no JSON object is the ap failure digest, never the daemon's text.
  - `PaneClient.identity_capability/1` answers `:capable`, `:not_capable` (PROVEN: a typed
    unsupported_protocol_version, or a valid version 3 ping lacking pane_identity) or `{:indeterminate, reason}`.
  - `LocalPane.status_v3/2` asks the capability first and reads nothing from a daemon that does not prove it.

  GREEN-only functions are called through apply/3: they do not exist at the RED base.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Dispatch.{LocalPane, PaneClient}

  @pane "pane_writer"
  @identity %{"pane_id" => @pane, "registration_id" => "reg_" <> String.duplicate("a", 32), "generation" => "7"}

  defp opts(answer) do
    owner = self()

    [
      ap_path: "/tmp/b1c-ap",
      runner: fn _path, args, _cmd_opts ->
        send(owner, {:ap, args})
        answer.(args)
      end
    ]
  end

  defp json(map, exit \\ 0), do: {"logger noise\n" <> Jason.encode!(map), exit}

  defp status_v3(answer), do: apply(PaneClient, :status_v3, [@pane, opts(answer)])
  defp capability(answer), do: apply(PaneClient, :identity_capability, [opts(answer)])
  defp local(answer), do: apply(LocalPane, :status_v3, [@pane, opts(answer)])

  defp ping(fields), do: Map.merge(%{"ok" => true, "protocol_version" => 3, "pong" => "0.1.0"}, fields)

  defp ok_status do
    %{
      "ok" => true,
      "protocol_version" => 3,
      "pane_id" => @pane,
      "state" => "idle",
      "quarantined" => false,
      "queue_depth" => 0,
      "pane_pid" => 4242,
      "pane_identity" => @identity
    }
  end

  test "status_v3 sends exactly the version 3 status argv and answers the reply bytes" do
    assert {:ok, bytes} = status_v3(fn _ -> json(ok_status()) end)
    assert Jason.decode!(bytes) == ok_status()
    assert_received {:ap, ["status", @pane, "--protocol-version", "3"]}
  end

  test "status_v3 answers a typed refusal's bytes even at a nonzero exit" do
    refusal = %{"ok" => false, "error" => "pane_identity_unavailable", "protocol_version" => 3, "pane_id" => @pane}
    assert {:ok, bytes} = status_v3(fn _ -> json(refusal, 1) end)
    assert Jason.decode!(bytes) == refusal
  end

  test "status_v3 output with no JSON object is the ap failure digest, never the daemon text" do
    assert {:error, error} = status_v3(fn _ -> {"usage: ai-pair ... secret-ish text", 2} end)
    assert error["reason"] == "ap_failed"
    refute inspect(error) =~ "secret-ish"
  end

  test "identity_capability: a valid version 3 ping naming pane_identity is capable" do
    assert capability(fn _ -> json(ping(%{"capabilities" => ["delivery_reconcile", "pane_identity"]})) end) ==
             :capable

    assert_received {:ap, ["ping", "--protocol-version", "3"]}
  end

  test "identity_capability: only the two proven forms are not capable" do
    assert capability(fn _ -> json(ping(%{"capabilities" => ["delivery_reconcile", "sessions_read"]})) end) ==
             :not_capable

    refused = %{"ok" => false, "error" => "unsupported_protocol_version", "protocol_version" => 3}
    assert capability(fn _ -> json(refused, 1) end) == :not_capable
  end

  test "identity_capability: everything else is indeterminate, never a downgrade" do
    cases = [
      fn _ -> {"ai-pair: invalid versioned delivery arguments", 2} end,
      fn _ -> raise "no ap" end,
      fn _ -> exit(:no_ap) end,
      fn _ -> json(ping(%{})) end,
      fn _ -> json(ping(%{"capabilities" => ["Bad Token!"]})) end,
      fn _ -> json(ping(%{"capabilities" => "pane_identity"})) end,
      fn _ -> json(%{"ok" => true, "protocol_version" => 2, "capabilities" => ["pane_identity"]}) end,
      fn _ -> json(%{"ok" => false, "error" => "receipt_store_unavailable", "protocol_version" => 3}, 1) end
    ]

    for answer <- cases do
      assert {:indeterminate, reason} = capability(answer)
      assert is_binary(reason) and reason != ""
    end
  end

  test "an ok answer at a nonzero exit authorizes nothing: status fails, capability is indeterminate" do
    assert {:error, %{"reason" => "ap_failed"}} = status_v3(fn _ -> json(ok_status(), 1) end)

    assert capability(fn _ -> json(ping(%{"capabilities" => ["pane_identity"]}), 1) end) ==
             {:indeterminate, "exit_status"}
  end

  test "status_v3 at a nonzero exit answers only a framed typed refusal" do
    assert {:error, %{"reason" => "ap_failed"}} = status_v3(fn _ -> json(%{"ok" => false}, 1) end)
    assert {:error, %{"reason" => "ap_failed"}} = status_v3(fn _ -> json(%{"protocol_version" => 3}, 1) end)
  end

  test "the proven unsupported_protocol_version refusal must name version 3; otherwise it is indeterminate" do
    for version <- [nil, 2, "3"] do
      refused = %{"ok" => false, "error" => "unsupported_protocol_version", "protocol_version" => version}
      assert {:indeterminate, _} = capability(fn _ -> json(refused, 1) end)
    end
  end

  test "LocalPane.status_v3 reads nothing from a daemon that does not prove the capability" do
    no_token = fn ["ping" | _] -> json(ping(%{"capabilities" => ["delivery_reconcile"]})) end
    assert local(no_token) == {:error, %{"reason" => "status_v3_unavailable"}}
    refute_received {:ap, ["status" | _]}

    broken = fn ["ping" | _] -> {"garbage", 1} end
    assert {:error, %{"reason" => "identity_capability_" <> _}} = local(broken)
    refute_received {:ap, ["status" | _]}
  end

  test "LocalPane.status_v3 passes a capable daemon's status bytes through" do
    answer = fn
      ["ping" | _] -> json(ping(%{"capabilities" => ["pane_identity"]}))
      ["status" | _] -> json(ok_status())
    end

    assert {:ok, bytes} = local(answer)
    assert Jason.decode!(bytes)["pane_identity"] == @identity
  end
end
