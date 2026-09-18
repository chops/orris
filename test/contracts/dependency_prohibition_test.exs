defmodule AiOrchestrator.Contracts.DependencyProhibitionTest do
  @moduledoc """
  NS-13.B.001 control: SQL, a job queue and a resource framework own no part of
  execution or retry, and the prohibition is FALSIFIABLE rather than true by absence.

  The row's own acceptance ("remove the analytical projector and recover the run") is
  vacuous at this head because no analytical projector exists. What can be asserted, and
  what the row's failure control actually forbids, is that no such package is reachable
  in ANY environment and that no delivered module names one of their namespaces. A
  dependency added under `only: :dev` would change no delivered behaviour and, without
  this control, nothing would notice.

  The scanner half is paired with a synthetic table: the tree scan alone reports the same
  green against a scanner that has stopped detecting anything.
  """

  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  # Package names whose presence in any environment would put a database, a job queue or a
  # resource framework where execution and retry live (ledger NS-13; architecture 268-271).
  @prohibited_packages ~w(
    ash
    ash_events
    ash_phoenix
    ash_postgres
    ash_sql
    ecto
    ecto_sql
    ecto_sqlite3
    myxql
    oban
    oban_pro
    postgrex
  )

  # The namespaces those packages introduce. A vendored or renamed copy would carry no
  # package name at all, so the source scan is the half that does not depend on mix.
  @prohibited_namespaces ~w(Ash AshEvents AshPostgres Ecto Myxql Oban Postgrex)

  @source_roots ~w(lib console/lib)

  test "no prohibited package is declared by the core project in any environment" do
    declared = MapSet.new(Mix.Project.config()[:deps], &dep_name/1)
    offenders = Enum.filter(@prohibited_packages, &MapSet.member?(declared, &1))

    assert offenders == [],
           "mix.exs declares prohibited execution-owning packages: #{inspect(offenders)}"
  end

  test "no prohibited package is resolved by either lock file" do
    for lock <- ~w(mix.lock console/mix.lock) do
      packages = lock_packages(lock)

      assert length(packages) > 10,
             "#{lock}: the lock reader found #{length(packages)} packages, so this row would pass vacuously"

      resolved = MapSet.new(packages)
      offenders = Enum.filter(@prohibited_packages, &MapSet.member?(resolved, &1))

      assert offenders == [], "#{lock} resolves prohibited execution-owning packages: #{inspect(offenders)}"
    end
  end

  test "no prohibited package is declared by the console project" do
    # The console is a separate Mix project inside the repository, so its dependency list
    # cannot be read through `Mix.Project.config/0` from here; its mix.exs text is the fact.
    source = @root |> Path.join("console/mix.exs") |> File.read!()
    offenders = Enum.filter(@prohibited_packages, &Regex.match?(dep_pattern(&1), source))

    assert offenders == [],
           "console/mix.exs declares prohibited execution-owning packages: #{inspect(offenders)}"
  end

  test "no delivered module names a prohibited namespace" do
    offenders =
      for path <- delivered_sources(),
          {line, number} <- path |> File.read!() |> String.split("\n") |> Enum.with_index(1),
          namespace <- @prohibited_namespaces,
          Regex.match?(namespace_pattern(namespace), line),
          do: {Path.relative_to(path, @root), number, namespace}

    assert offenders == [],
           """
           Delivered source names a namespace NS-13.B.001 forbids from owning execution or retry:

           #{Enum.map_join(offenders, "\n", fn {file, line, namespace} -> "  #{file}:#{line} #{namespace}" end)}
           """
  end

  test "the namespace scanner still finds the reference it was written for" do
    # Proves the scan above is not a no-op: the same matcher, over sources that do name them.
    rejected = [
      {"a schema module", "defmodule Repo, do: use(Ecto.Repo)", "Ecto"},
      {"a worker module", "defmodule Job, do: use(Oban.Worker)", "Oban"},
      {"a resource module", "defmodule Run, do: use(Ash.Resource)", "Ash"},
      {"a direct driver call", "Postgrex.query!(conn, sql, [])", "Postgrex"}
    ]

    for {label, source, namespace} <- rejected do
      assert Regex.match?(namespace_pattern(namespace), source), label
    end

    accepted = [
      {"a longer name that merely starts the same way", "AshleyModule.call()", "Ash"},
      {"the namespace without a call", "the word Ecto in prose", "Ecto"},
      {"an unrelated module", "AiOrchestrator.Journal.Fold.fold_lines(lines)", "Oban"}
    ]

    for {label, source, namespace} <- accepted do
      refute Regex.match?(namespace_pattern(namespace), source), label
    end
  end

  test "the package matcher distinguishes a prohibited name from a name that contains it" do
    assert Regex.match?(dep_pattern("ecto"), "{:ecto, \"~> 3.13\"}")
    assert Regex.match?(dep_pattern("ecto"), "{:ecto, \"~> 3.13\", only: :dev, runtime: false}")
    refute Regex.match?(dep_pattern("ecto"), "{:ecto_sql, \"~> 3.13\"}")
    refute Regex.match?(dep_pattern("ash"), "{:lazy_html, \"~> 0.1\", only: :test}")
  end

  defp dep_name(dep) when is_atom(dep), do: Atom.to_string(dep)
  defp dep_name(dep) when is_tuple(dep), do: dep |> elem(0) |> Atom.to_string()

  defp dep_pattern(package), do: Regex.compile!("\\{\\s*:#{Regex.escape(package)}\\s*[,}]")

  defp namespace_pattern(namespace), do: Regex.compile!("\\b#{Regex.escape(namespace)}\\.[A-Za-z_]")

  # The lock is read as TEXT, never evaluated: one quoted package name per line is the whole
  # format, and evaluating a lock to inspect it would be a side effect this control does not need.
  defp lock_packages(relative_path) do
    @root
    |> Path.join(relative_path)
    |> File.read!()
    |> then(&Regex.scan(~r/^\s*"([A-Za-z0-9_]+)":/m, &1))
    |> Enum.map(fn [_line, package] -> package end)
  end

  defp delivered_sources do
    for root <- @source_roots,
        path <- @root |> Path.join("#{root}/**/*.ex") |> Path.wildcard() do
      path
    end
  end
end
