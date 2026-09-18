defmodule C1.NS11ClosureTest do
  @moduledoc """
  NS-11.K.000 / NS-11.K.001, the console half of "SQL or Ash state required for execution fails".

  `test/contracts/ns11_projection_independence_test.exs` in the core binds that row to two MEASURED
  oracles -- the runtime application closure the compiled application DECLARES, and Mix's own
  production dependency graph -- rather than to the absence of a `deps` line. It measures
  `:ai_orchestrator` only (its line 40). The console is a separate Mix project with its own lock, its
  own deps and its own `.app`, so until this file existed the console's closure was asserted by
  NOTHING: the strongest statement available was that `console/mix.exs` DECLARES no prohibited
  package, which is a claim about six direct entries and says nothing about what they drag in.

  These rows apply the same oracles to `:orris_console`, with the same segment predicate, the same
  witness application, and the same control row proving the predicate can fire. The middle row goes
  one step further than the core's and walks the closure TRANSITIVELY, because "a console-only
  transitive dependency introduces a stored-state application" is exactly the question the direct
  `applications` key cannot answer.

  Declaring nothing and starting nothing are separate claims; this file makes the second one for the
  console. It makes no claim about Ash, which does not exist in either project.
  """

  use ExUnit.Case, async: false

  # a stored-state technology is named by one of these SEGMENTS of an application name; the segment
  # rule keeps an unrelated application whose name merely contains the letters (`ssl_verify_fun`) out
  @stored_state_segments ~w(ash ecto oban sql sqlite postgrex myxql tds mnesia)
  # one application that must be in every measured closure, so an empty or unparsed closure cannot pass
  @witness :jason

  test "NS-11.K the console's declared runtime application closure carries no SQL, Ash or Oban application" do
    applications = declared(:orris_console)

    assert @witness in applications, "the measured console closure is empty or unreadable: #{inspect(applications)}"
    assert :ai_orchestrator in applications, "the console no longer declares the core it reads through"
    assert stored_state(applications) == []
  end

  test "NS-11.K the console's TRANSITIVE runtime closure carries no SQL, Ash or Oban application" do
    closure = transitive_closure(:orris_console)

    assert @witness in closure, "the measured transitive closure is empty or unreadable"

    # the walk really walked: a transitive closure that equalled the direct one would mean nothing was
    # followed, and this row would then be the previous row under another name
    direct = MapSet.new(declared(:orris_console))

    assert MapSet.size(MapSet.difference(closure, direct)) > 0,
           "the transitive closure adds nothing to the declared one, so nothing was followed"

    assert stored_state(closure) == []
  end

  test "NS-11.K Mix's own production dependency graph for the console carries no SQL, Ash or Oban application" do
    graph = production_graph()

    assert @witness in graph, "the measured production graph is empty or unparsed: #{inspect(graph)}"
    assert :ai_orchestrator in graph, "the console production graph no longer resolves the core"
    assert stored_state(graph) == []
  end

  test "NS-11.K control: the same predicate names a stored-state application in a closure that carries one" do
    assert stored_state([:jason, :phoenix, :bandit]) == []
    assert stored_state([:jason, :ash_postgres, :ecto_sql, :oban]) == [:ash_postgres, :ecto_sql, :oban]
    # the segment rule is precise: an application whose name merely contains the letters is not named
    assert stored_state([:ssl_verify_fun, :tls_certificate_check, :chatterbox, :hpack]) == []
  end

  # ---- helpers ----

  defp stored_state(applications), do: applications |> Enum.filter(&stored_state?/1) |> Enum.sort()

  defp stored_state?(app) do
    app |> Atom.to_string() |> String.split("_") |> Enum.any?(&(&1 in @stored_state_segments))
  end

  # the `applications` key out of the compiled `.app`, exactly as the core row reads it. The suite runs
  # under `test --no-start` (console/mix.exs), so the application is loaded here rather than assumed.
  defp declared(app) do
    case Application.load(app) do
      :ok -> :ok
      {:error, {:already_loaded, ^app}} -> :ok
      {:error, _reason} -> :unloadable
    end

    Application.spec(app, :applications) || []
  end

  # The names reachable from the console by following each application's own `applications` key. An
  # application whose `.app` cannot be loaded still contributes its NAME -- the predicate is about
  # names -- it just contributes no children, so an unloadable entry can never hide one.
  defp transitive_closure(root), do: walk([root], MapSet.new())

  defp walk([], seen), do: seen

  defp walk([app | rest], seen) do
    if MapSet.member?(seen, app) do
      walk(rest, seen)
    else
      walk(declared(app) ++ rest, MapSet.put(seen, app))
    end
  end

  # Mix's own production dependency graph for THIS project, read exactly as the core row reads the
  # core's. The test process already runs in `console/`, so no directory is named here.
  defp production_graph do
    {out, 0} = System.cmd("mix", ["deps.tree", "--only", "prod", "--format", "plain"], stderr_to_stdout: true)

    for line <- String.split(out, "\n"),
        [_, app] <- [Regex.run(~r/^[\s|`-]*-- ([a-z][a-z0-9_]*) /, line)],
        uniq: true,
        do: String.to_atom(app)
  end
end
