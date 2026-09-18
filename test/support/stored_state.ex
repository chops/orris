defmodule AiOrchestrator.Test.StoredState do
  @moduledoc """
  TEST SUPPORT ONLY. The NS-11 stored-state predicate, in one place because two rows apply it to two different
  measurements of the same product: `test/contracts/ns11_projection_independence_test.exs` applies it to the
  declared runtime application closure and to Mix's production dependency graph, and
  `test/contracts/production_escript_test.exs` applies it to the applications actually packaged into the built
  production archive. Two copies of the list would let one row be widened while the other kept passing.

  The rule is by SEGMENT of an application name, not by substring, so an unrelated application whose name merely
  contains the letters (`ssl_verify_fun`, `tls_certificate_check`) is not named. `named/1` answers the sorted
  stored-state applications in the given closure, `[]` when there are none.
  """

  @segments ~w(ash ecto oban sql sqlite postgrex myxql tds mnesia)

  @doc "The sorted stored-state applications in `applications` (a list or a MapSet of atoms)."
  @spec named(Enumerable.t()) :: [atom()]
  def named(applications), do: applications |> Enum.filter(&named?/1) |> Enum.sort()

  @doc "Whether one application name is a stored-state technology by the segment rule."
  @spec named?(atom()) :: boolean()
  def named?(app), do: app |> Atom.to_string() |> String.split("_") |> Enum.any?(&(&1 in @segments))
end
