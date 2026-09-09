defmodule C1.ConfigTest do
  @moduledoc "R6: every named configuration rejection clause is exercised; the trusted config never takes browser values."
  use ExUnit.Case, async: false
  alias C1.Harness

  @mods [OrrisConsole.Config]

  defp rejected(overrides) do
    Harness.red!(@mods)
    {:error, %{clause: clause}} = OrrisConsole.Config.load(Harness.config(overrides))
    clause
  end

  test "C1-00a a non-loopback bind is refused (config_bind_not_loopback)" do
    assert rejected(bind: {0, 0, 0, 0}) == "config_bind_not_loopback"
    assert rejected(bind: {10, 0, 0, 1}) == "config_bind_not_loopback"
  end

  test "C1-00b the configured hostname is normalized to lowercase (the raw Host header stays case-sensitive, C1-03a); wildcard, empty, spaced or port-bearing hosts, port 0 and https are refused" do
    Harness.red!(@mods)

    assert {:ok, %{host: "localhost", authority: authority}} =
             OrrisConsole.Config.load(Harness.config(host: "LocalHost", port: 4321))

    assert authority == "localhost:4321"
    for host <- ["*", "", "local host", "localhost:1"], do: assert(rejected(host: host) == "config_authority_invalid")
    assert rejected(port: 0) == "config_authority_invalid"
    assert rejected(scheme: :https) == "config_authority_invalid"
  end

  test "C1-00c proxy or forwarded-header trust is refused (config_proxy_refused)" do
    assert rejected(trust_forwarded: true) == "config_proxy_refused"
    assert rejected(proxy: %{}) == "config_proxy_refused"
  end

  test "C1-00d roots must map ids to absolute directories; the operator's root ids must be configured (config_roots_invalid / config_operator_invalid)" do
    assert rejected(roots: %{"alpha" => "relative/dir"}) == "config_roots_invalid"
    assert rejected(roots: [{"alpha", "/tmp"}]) == "config_roots_invalid"
    assert rejected(roots: %{"al pha" => "/tmp"}) == "config_roots_invalid"

    assert rejected(roots: %{"alpha" => "/tmp"}, operator: %{id: "operator", root_ids: ["beta"]}) ==
             "config_operator_invalid"

    assert rejected(operator: %{id: "", root_ids: []}) == "config_operator_invalid"
  end

  test "C1-00e a missing credential path and an expiry above the defaults are refused" do
    assert rejected(credential_path: nil) == "config_credential_path_missing"
    assert rejected(credential_path: "relative/credential") == "config_credential_path_missing"
    assert rejected(idle_ms: 1_800_001) == "config_expiry_exceeds_default"
    assert rejected(absolute_ms: 43_200_001) == "config_expiry_exceeds_default"
    assert rejected(idle_ms: 0) == "config_expiry_exceeds_default"
  end

  test "C1-00f the loaded configuration derives the exact authority and origin and carries no browser-settable field" do
    Harness.red!(@mods)
    root = Harness.fresh("cfg_root")
    assert {:ok, config} = OrrisConsole.Config.load(Harness.config(roots: %{"alpha" => root}, port: 4321))
    assert config.authority == "localhost:4321" and config.origin == "http://localhost:4321"
    assert config.roots == %{"alpha" => root} and config.operator.root_ids == ["alpha"]
    refute Map.has_key?(config, :actor) or Map.has_key?(config, :executor)
  end
end
