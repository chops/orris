defmodule AiOrchestrator.Contracts.DependencyInventoryTest do
  @moduledoc """
  NS-34.M.001, the provenance half that is not a clean-room install: `DEPENDENCIES.org` is the
  repository's dependency inventory and its license record, and until now it was prose. Nothing
  compared it with the dependency set it claims to describe, so a package added, removed or
  upgraded left the inventory saying something that had quietly stopped being true.

  Two measurements hold it. DI-1 compares the inventory with `mix.lock`, which is the exact set
  that is fetched and the only place the versions are authoritative. DI-2 compares it with Mix's
  own production dependency graph -- read through the CLI, as `production_escript_test.exs` reads
  it -- so a package that actually SHIPS cannot be recorded as development-only, which is the
  error this document exists to prevent.

  Deliberately not asserted here: the declared license column. Nothing in this repository measures
  an upstream package's license, and a row comparing the document with itself would be a control
  that cannot fail. It stays a reviewed human statement and is named as one.
  """

  use ExUnit.Case, async: true

  alias Mix.Dep.Lock

  @inventory Path.expand("../../DEPENDENCIES.org", __DIR__)

  # `| [[https://hex.pm/packages/<package>/<version>][<label>]] | <version> | <role> | <license> |`
  @row ~r/^\|\s*\[\[https:\/\/hex\.pm\/packages\/([a-z0-9_]+)\/([^\]]+)\]\[([a-z0-9_]+)\]\]\s*\|\s*([^|]*?)\s*\|\s*(.*?)\s*\|\s*([^|]*?)\s*\|\s*$/m

  # a role that classifies a package as build-time only; a packaged application may not carry one
  @development_roles ["Direct development/test", "Direct test", "Direct development"]

  test "DI-1 the inventory names exactly the locked packages, at the locked versions" do
    rows = inventory_rows()
    locked = locked_versions()

    # witnesses first: an unreadable document or an empty lock must not satisfy the equalities below
    assert map_size(locked) > 1, "mix.lock produced no locked packages, so this row would be vacuous"
    assert Map.has_key?(rows, "jason"), "the inventory does not name jason, so it was not read as expected"

    documented = Map.new(rows, fn {package, row} -> {package, row.version} end)
    missing = Map.drop(locked, Map.keys(documented))
    extra = Map.drop(documented, Map.keys(locked))

    assert missing == %{}, "locked packages absent from DEPENDENCIES.org: #{inspect(missing)}"
    assert extra == %{}, "DEPENDENCIES.org names packages that are not locked: #{inspect(extra)}"
    assert documented == locked, "DEPENDENCIES.org disagrees with mix.lock on versions"
  end

  test "DI-1a every inventory row agrees with itself: the link, its label and the version column" do
    inventory = File.read!(@inventory)
    scanned = Regex.scan(@row, inventory)

    assert scanned != [], "no inventory rows parsed from #{@inventory}"

    for [_line, link_package, link_version, label, version, _role, _license] <- scanned do
      assert label == link_package, "row label #{label} does not match its link #{link_package}"
      assert version == link_version, "row version #{version} does not match its link #{link_version}"
    end
  end

  # The shipped closure is the part of this document that carries redistribution consequences. A
  # package that is started in production but recorded as development-only is a provenance error
  # the license text above the table is written against.
  test "DI-2 every package in the production dependency graph is documented as something that ships" do
    rows = inventory_rows()
    locked = locked_entries()
    graph = production_graph()

    assert :jason in graph, "the production dependency graph is empty or unreadable: #{inspect(graph)}"

    for application <- graph do
      package = package_for(application, locked)
      role = rows |> Map.fetch!(package) |> Map.fetch!(:role)

      refute role in @development_roles,
             "#{package} is started in production but DEPENDENCIES.org records it as #{role}"
    end
  end

  # ---- helpers ----

  defp inventory_rows do
    @inventory
    |> File.read!()
    |> then(&Regex.scan(@row, &1))
    |> Map.new(fn [_line, package, _link_version, _label, version, role, license] ->
      {package, %{version: version, role: role, license: license}}
    end)
  end

  # mix.lock is the measured set: every entry must be a Hex entry, so a git or path dependency
  # cannot slip past an inventory that only knows how to describe Hex packages. The lock keys by
  # OTP application and names the Hex package beside it, which is the only mapping between the two.
  defp locked_entries do
    Map.new(Lock.read(), fn
      {application, entry} when elem(entry, 0) == :hex ->
        {to_string(application), %{package: to_string(elem(entry, 1)), version: elem(entry, 2)}}

      {application, other} ->
        flunk("#{application} is locked as something other than a Hex package: #{inspect(other)}")
    end)
  end

  defp locked_versions do
    Map.new(locked_entries(), fn {_application, entry} -> {entry.package, entry.version} end)
  end

  # Mix's own production dependency graph, read through the CLI, independent of this document
  defp production_graph do
    {out, 0} = System.cmd("mix", ["deps.tree", "--only", "prod", "--format", "plain"], stderr_to_stdout: true)

    for line <- String.split(out, "\n"),
        [_, application] <- [Regex.run(~r/^[\s|`-]*-- ([a-z][a-z0-9_]*) /, line)],
        uniq: true,
        do: String.to_atom(application)
  end

  # the graph names OTP applications and the inventory names Hex packages; for chatterbox and hpack
  # those differ (ts_chatterbox, hpack_erl), so the lock is what maps one onto the other
  defp package_for(application, locked) do
    case Map.fetch(locked, to_string(application)) do
      {:ok, entry} -> entry.package
      :error -> flunk("#{application} is in the production graph but not locked; the mapping is unknown")
    end
  end
end
