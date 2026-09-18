defmodule AiOrchestrator.Contracts.IpcBoundaryStrictnessTest do
  @moduledoc """
  NS-07.A.001's amended control is a PAIR, and the two halves contradict each other unless the
  boundary between them is pinned: unknown-field rejection applies per NAMED CLOSED schema, while
  NS-42 requires a client to ignore additional well-formed capability tokens. A single blanket
  assertion in either direction would break the other.

  The pin, measured over the delivered source at this head:

    * `Protocol.Envelope`'s top-level map and its `from`/`to` peer objects are named CLOSED schemas
      (`unrecognized_keys: :error`, envelope.ex:40, 51): an unknown key is refused.
    * `trace` and `context` are named OPEN schemas (`unrecognized_keys: :preserve`, envelope.ex:58,
      78): an unknown key is admitted AND preserved. The delivered `valid_full_context` fixture
      already carries `topic_slug` there, so the openness is load-bearing today.
    * The `ap` IPC v1/v2 reply wire has NO schema module at all. `PaneClient` decodes a reply to a
      bare map and reads named keys out of it (pane_client.ex:116-135); an unknown top-level key is
      carried through untouched. There is therefore no named closed IPC schema that admits unknown
      fields, which is the condition the R12 audit's C4 was made conditional on.

  The capability half of the pair is delivered at
  test/effects/dispatch_capability_admission_test.exs:138-141 (a novel token survives PaneClient);
  the row here is its `Dispatch.preflight/2` partner, so the two halves stand in one file.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Dispatch
  alias AiOrchestrator.Dispatch.PaneClient
  alias AiOrchestrator.Protocol.Envelope

  @unknown_key "unreviewed_extension"
  @pane "pane_fixture"
  @message_id "m_0000000042_beef0042"
  @payload_hash "sha256:" <> String.duplicate("0", 64)

  defmodule NovelCapabilities do
    @moduledoc false
    def capabilities(opts), do: {:ok, Keyword.fetch!(opts, :tokens)}
  end

  defp envelope, do: F.json("envelopes", "valid_full_context", "envelope.json")

  defp reply(extra) do
    Map.merge(
      %{"ok" => true, "protocol_version" => 2, "msg_id" => @message_id, "pane_id" => @pane, "status" => "sent"},
      extra
    )
  end

  defp runner(reply), do: fn _executable, _args, _opts -> {Jason.encode!(reply), 0} end

  defp input_runner(reply), do: fn _executable, _args, _input, _opts -> {Jason.encode!(reply), 0} end

  # ---- closed half: every named closed schema refuses an unknown key ----

  test "the envelope's own closed schema refuses an unknown top-level key" do
    assert {:ok, _} = Envelope.validate(envelope())

    assert {:error, %{clause: "invalid_envelope_shape"}} =
             envelope() |> Map.put(@unknown_key, "value") |> Envelope.validate()
  end

  for field <- ["from", "to"] do
    test "the closed peer schema refuses an unknown key in #{field}" do
      field = unquote(field)
      result = envelope() |> update_in([field], &Map.put(&1, @unknown_key, "value")) |> Envelope.validate()

      assert match?({:error, %{clause: "invalid_envelope_shape"}}, result),
             "the #{field} peer object admitted an unreviewed key: #{inspect(result)}"
    end
  end

  # ---- open half: the declared open sub-schemas admit AND preserve an unknown key ----

  for field <- ["trace", "context"] do
    test "the open #{field} schema admits and preserves an unknown key" do
      field = unquote(field)
      result = envelope() |> update_in([field], &Map.put(&1, @unknown_key, "value")) |> Envelope.validate()

      assert match?({:ok, _}, result),
             "the #{field} object refused a well-formed extension NS-42 requires a client to ignore: #{inspect(result)}"

      {:ok, parsed} = result

      assert get_in(parsed, [field, @unknown_key]) == "value",
             "the #{field} extension was admitted but dropped: #{inspect(parsed[field])}"
    end
  end

  test "the IPC reply wire is not a closed schema: an unknown top-level key is still an answer" do
    extended = reply(%{@unknown_key => %{"nested" => [1, 2, 3]}})

    assert {:ok, sent} =
             PaneClient.send(@pane, "prompt",
               ap_path: "/unused",
               message_id: @message_id,
               input_runner: input_runner(extended)
             )

    assert sent[@unknown_key] == %{"nested" => [1, 2, 3]},
           "a v2 send reply dropped an extension the client must ignore rather than refuse"

    reconciled =
      reply(%{@unknown_key => "future", "status" => "delivered", "delivery_attempt" => 1})

    assert {:ok, answer} =
             PaneClient.reconcile(@pane, @message_id,
               ap_path: "/unused",
               payload_hash: @payload_hash,
               runner: runner(reconciled)
             )

    assert answer[@unknown_key] == "future"

    ping = %{"ok" => true, "protocol_version" => 2, "capabilities" => ["delivery_reconcile"], @unknown_key => "future"}
    assert {:ok, ["delivery_reconcile"]} = PaneClient.capabilities(ap_path: "/unused", runner: runner(ping))
  end

  # ---- the pair partner: a novel well-formed capability token is admitted, a malformed one is not ----

  test "preflight admits a novel well-formed capability token and refuses a malformed one" do
    assert :ok = Dispatch.preflight(NovelCapabilities, tokens: ["delivery_reconcile", "Vendor:future-v3/2", "未来"])

    assert {:error, %{"reason" => "dispatch_capabilities_invalid"}} =
             Dispatch.preflight(NovelCapabilities, tokens: ["delivery_reconcile", "bad token"])

    assert {:error, %{"reason" => "dispatch_preflight_unsupported"}} =
             Dispatch.preflight(NovelCapabilities, tokens: ["Vendor:future-v3/2"])
  end
end
