defmodule AiOrchestrator.PaneRegistry.PaneIdentityTest do
  @moduledoc """
  NS-15.G.005 B3b GREEN G1: PaneIdentity.compare/2 matches only two complete identities; an identity missing a key,
  or carrying a non-string value, never matches, even when both sides lack the same key.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.PaneRegistry.PaneIdentity

  @identity %{"pane_id" => "pane_writer", "registration_id" => "reg_" <> String.duplicate("a", 32), "generation" => "7"}

  test "an identity missing a key on either side, or on both, is incomplete, never a match" do
    for key <- Map.keys(@identity) do
      partial = Map.delete(@identity, key)
      assert PaneIdentity.compare(partial, @identity) == {:error, :incomplete_identity}, key
      assert PaneIdentity.compare(@identity, partial) == {:error, :incomplete_identity}, key
      assert PaneIdentity.compare(partial, partial) == {:error, :incomplete_identity}, key
    end
  end

  test "non-string values and non-maps are incomplete, never a match" do
    numeric = Map.put(@identity, "generation", 7)
    assert PaneIdentity.compare(numeric, numeric) == {:error, :incomplete_identity}
    assert PaneIdentity.compare(nil, @identity) == {:error, :incomplete_identity}
    assert PaneIdentity.compare(@identity, nil) == {:error, :incomplete_identity}
  end
end
