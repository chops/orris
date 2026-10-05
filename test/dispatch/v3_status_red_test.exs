defmodule AiOrchestrator.Dispatch.V3StatusRedTest do
  @moduledoc """
  NS-15.G.005 B3b (scope r2, D/B3B-SCOPE-r2.org): RED rows R4.1-R4.3 for the version 3 status decoder and the pane
  identity comparison, written against docs/contracts/ipc-v3.org and the v3 example fixtures (placeholders
  substituted with grammar-valid values). Consumer side only: no producer emits version 3 yet.

  Names this RED requires (absent at this head, so every row fails on the export assertion):
  - `AiOrchestrator.Dispatch.V3Status.decode(reply_bytes, pane_ref)` answers
    `{:ok, %{"state", "quarantined", "queue_depth", "pane_pid", "pane_identity" => %{"pane_id", "registration_id",
    "generation"}}}` for an ok status, `{:refused, reason}` for a typed refusal (ok:false, no identity required), and
    `{:error, :reply_identity, detail}` for a missing or malformed identity, a pane_id echo mismatch or a version
    other than 3 (ipc-v3.org L84-89, L95-104);
  - `AiOrchestrator.PaneRegistry.PaneIdentity.compare(claim_identity, reply_identity)` answers `:match` or
    `{:mismatch, fields}` naming which of pane_id / registration_id / generation differ, compared byte for byte
    (generation is a decimal string, never a number: L78-83).

  The modules are reached through a runtime module name so this file compiles before they exist; GREEN replaces the
  `v3()` / `identity()` indirection with direct calls.
  """

  use ExUnit.Case, async: true

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)
  @pane "pane_writer"
  @reg "reg_" <> String.duplicate("0123456789abcdef", 2)
  @gen "123456789012345678901234567890123456789"

  defp v3, do: Module.concat(["AiOrchestrator", "Dispatch", "V3Status"])
  defp identity, do: Module.concat(["AiOrchestrator", "PaneRegistry", "PaneIdentity"])

  defp assert_exported!(module, fun, arity) do
    Code.ensure_loaded(module)

    assert function_exported?(module, fun, arity),
           "#{inspect(module)}.#{fun}/#{arity} (B3b scope r2) does not exist"
  end

  defp decode(bytes, pane_ref) do
    assert_exported!(v3(), :decode, 2)
    v3().decode(bytes, pane_ref)
  end

  defp compare(claim_identity, reply_identity) do
    assert_exported!(identity(), :compare, 2)
    identity().compare(claim_identity, reply_identity)
  end

  defp fixture(name, substitutions \\ %{}) do
    defaults = %{"<pane_id>" => @pane, "<registration_id>" => @reg, "<generation>" => @gen}

    defaults
    |> Map.merge(substitutions)
    |> Enum.reduce(File.read!(Path.join(@fixtures, name)), fn {placeholder, value}, bytes ->
      String.replace(bytes, placeholder, value)
    end)
  end

  defp edit(bytes, fun), do: bytes |> Jason.decode!() |> fun.() |> Jason.encode!()

  defp ident(overrides \\ %{}),
    do: Map.merge(%{"pane_id" => @pane, "registration_id" => @reg, "generation" => @gen}, overrides)

  test "R4.1 RED an ok version 3 status decodes with its identity, generation kept as a string" do
    assert {:ok, status} = decode(fixture("status.ok.json"), @pane)
    assert status["pane_identity"] == ident()
    assert is_binary(status["pane_identity"]["generation"])
    assert status["state"] == "idle"
    assert status["quarantined"] == false
    assert status["queue_depth"] == 0
    assert status["pane_pid"] == 4242
  end

  test "R4.1 RED a quarantined version 3 status decodes with quarantined true" do
    assert {:ok, %{"quarantined" => true, "pane_identity" => identity}} =
             decode(fixture("status.quarantined.json"), @pane)

    assert identity == ident()
  end

  test "R4.2 RED an ok status without pane_identity is a reply-identity error" do
    bytes = edit(fixture("status.ok.json"), &Map.delete(&1, "pane_identity"))
    assert {:error, :reply_identity, _detail} = decode(bytes, @pane)
  end

  test "R4.2 RED a malformed registration_id is a reply-identity error, never an identity" do
    for bad <- [
          "rg_" <> String.duplicate("a", 32),
          "reg_" <> String.duplicate("a", 31),
          "reg_" <> String.duplicate("a", 33),
          "reg_" <> String.duplicate("A", 32)
        ] do
      bytes = fixture("status.ok.json", %{"<registration_id>" => bad})
      assert {:error, :reply_identity, _detail} = decode(bytes, @pane), bad
    end
  end

  test "R4.2 RED a generation that is a JSON number, empty or not all digits is a reply-identity error" do
    number = edit(fixture("status.ok.json"), &put_in(&1, ["pane_identity", "generation"], 12))
    assert {:error, :reply_identity, _detail} = decode(number, @pane)

    for bad <- ["", "12a", "-1", "1.5"] do
      bytes = fixture("status.ok.json", %{"<generation>" => bad})
      assert {:error, :reply_identity, _detail} = decode(bytes, @pane), inspect(bad)
    end
  end

  test "R4.2 RED a pane_id echo or identity pane_id that does not name the pane is a reply-identity error" do
    assert {:error, :reply_identity, _detail} = decode(fixture("status.ok.json"), "pane_reviewer")

    other = edit(fixture("status.ok.json"), &put_in(&1, ["pane_identity", "pane_id"], "pane_reviewer"))
    assert {:error, :reply_identity, _detail} = decode(other, @pane)
  end

  test "R4.2 RED a reply that is not protocol version 3 is a reply-identity error" do
    v2 = edit(fixture("status.ok.json"), &Map.put(&1, "protocol_version", 2))
    assert {:error, :reply_identity, _detail} = decode(v2, @pane)
  end

  test "R4.2 RED control: a typed pane_not_found refusal needs no identity" do
    assert decode(fixture("status.error.pane_not_found.json"), @pane) == {:refused, "pane_not_found"}
  end

  test "R4.3 RED equal identities match" do
    assert compare(ident(), ident()) == :match
  end

  test "R4.3 RED a re-attached pane (new registration_id) or a new generation is a mismatch naming the field" do
    reattached = ident(%{"registration_id" => "reg_" <> String.duplicate("f", 32)})
    assert compare(ident(), reattached) == {:mismatch, ["registration_id"]}

    regenerated = ident(%{"generation" => "7"})
    assert compare(ident(), regenerated) == {:mismatch, ["generation"]}
  end

  test "R4.3 RED a 39-digit generation compares as text, without integer conversion" do
    near = ident(%{"generation" => String.slice(@gen, 0, 38) <> "0"})
    assert compare(ident(), near) == {:mismatch, ["generation"]}
    # equal value, different text (a leading zero) is a mismatch: the comparison is byte for byte
    assert compare(ident(%{"generation" => "7"}), ident(%{"generation" => "07"})) == {:mismatch, ["generation"]}
  end
end
