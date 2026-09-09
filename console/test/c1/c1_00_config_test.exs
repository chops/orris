defmodule C1.ConfigTest do
  @moduledoc "R6: every named configuration rejection clause is exercised; the trusted config never takes browser values."
  use ExUnit.Case, async: false
  alias C1.Harness

  @mods [OrrisConsole.Config]

  # the rejection clause, or :accepted when the configuration loads (an attributed assertion, never a MatchError)
  defp rejected(overrides) do
    Harness.red!(@mods)

    case OrrisConsole.Config.load(Harness.config(overrides)) do
      {:error, %{clause: clause}} -> clause
      {:ok, _config} -> :accepted
    end
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

  test "C1-00e a missing credential path, an expiry above the defaults, an operator id outside the Commands actor grammar and a mutation limit outside its bound are refused (U1 amendment)" do
    assert rejected(credential_path: nil) == "config_credential_path_missing"
    assert rejected(credential_path: "relative/credential") == "config_credential_path_missing"
    assert rejected(idle_ms: 1_800_001) == "config_expiry_exceeds_default"
    assert rejected(absolute_ms: 43_200_001) == "config_expiry_exceeds_default"
    assert rejected(idle_ms: 0) == "config_expiry_exceeds_default"
    # U1 amendment (docs/contracts/console-mutations.org, M-16): fail closed on the actor grammar and the new bounds
    assert rejected(operator: %{id: "op erator", root_ids: []}) == "config_operator_invalid",
           "RED (U1 amendment C1-00e): operator id grammar not enforced"

    assert rejected(mutation_capacity: 0) == "config_mutation_limits_invalid"
    assert rejected(intent_ttl_ms: 600_001) == "config_mutation_limits_invalid"
  end

  test "C1-00f the loaded configuration derives the exact authority and origin and carries no browser-settable field" do
    Harness.red!(@mods)
    root = Harness.fresh("cfg_root")
    assert {:ok, config} = OrrisConsole.Config.load(Harness.config(roots: %{"alpha" => root}, port: 4321))
    assert config.authority == "localhost:4321" and config.origin == "http://localhost:4321"
    assert config.roots == %{"alpha" => root} and config.operator.root_ids == ["alpha"]
    refute Map.has_key?(config, :actor) or Map.has_key?(config, :executor)
    # U1 amendment: the seven closed mutation limits are present with their defaults; the trusted seams default off
    # (read through the plain field map: the struct gains these fields only at GREEN)
    fields = Map.from_struct(config)

    assert fields[:mutation_capacity] == 4 and fields[:intent_ttl_ms] == 60_000,
           "RED (U1 amendment C1-00f): mutation limits absent from the loaded configuration"

    assert fields[:mutation_wait_ms] == 5_000 and fields[:mutation_retention_ms] == 300_000
    assert fields[:mutation_shutdown_ms] == 60_000 and fields[:mutation_start_ms] == 1_000
    assert fields[:mutation_read_ms] == 1_000

    for seam <- [:mutation_witness, :operation_gate, :operation_finish_gate, :starter_gate, :mutation_invoke],
        do: assert(fields[seam] == nil)

    assert fields[:mutation_opts] == []
  end
end
