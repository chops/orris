defmodule AiOrchestrator.Contracts.TestAssertionShapeTest do
  use ExUnit.Case, async: true

  # ExUnit special-cases match translation for `assert/1` only. At arity two the match is an
  # ordinary expression evaluated before `assert` runs, so a failing match raises MatchError
  # and the custom message -- the only thing the second argument was written for -- never prints.
  #
  # The defect hides during RED: while the right-hand side is still undefined the call raises
  # UndefinedFunctionError first, so the site looks fine and only goes silent at GREEN, which is
  # exactly when the message was supposed to explain the failure. Editing the sites without this
  # guard leaves the class open, because the next author writes the same shape for the same reason.
  #
  # A bare variable on the left is correct and stays legal: `assert event = Enum.find(...), msg`
  # binds and then checks truthiness, and its message prints. The defect is a *pattern that can
  # fail to match*. Repairs are `assert EXPR == LITERAL, MSG` where the pattern binds nothing, or
  # `assert match?(PATTERN, EXPR), MSG` where it binds only underscores or matches a map subset.
  #
  # The two halves below need each other. The scan over `test/**/*.exs` proves the tree is clean
  # today, but it reports exactly the same green against a scanner that has stopped detecting
  # anything, so alone it decays into a no-op the first time the matcher is edited. The synthetic
  # table proves the scanner still finds the defect it was written for. A manual mutation proves
  # that once and then leaves the repository; these probes stay.

  @root Path.expand("../..", __DIR__)

  @rejected [
    {"an unqualified assert/2 whose pattern can fail to match",
     """
     test "example" do
       assert {:ok, _} = call(), "message"
     end
     """, [2]},
    {"an unqualified refute/2 whose pattern can fail to match",
     """
     test "example" do
       refute {:ok, _} = call(), "message"
     end
     """, [2]},
    {"a fully qualified ExUnit.Assertions.assert/2",
     """
     test "example" do
       ExUnit.Assertions.assert({:ok, _} = call(), "message")
     end
     """, [2]},
    {"a remote assert/2 reached through an alias",
     """
     test "example" do
       Assertions.refute({:ok, _} = call(), "message")
     end
     """, [2]},
    {"a pinned left-hand side, which is a match and not a binding",
     """
     test "example" do
       assert ^expected = call(), "message"
     end
     """, [2]},
    {"each offending site at its own line, and nothing in between",
     """
     test "example" do
       assert {:ok, _} = call(), "first"
       assert value == 1, "an equality, not a match"
       refute %{a: _} = call(), "second"
     end
     """, [2, 4]}
  ]

  @accepted [
    {"a bare variable binding, which cannot fail and does print its message",
     """
     test "example" do
       assert event = Enum.find(list, fun), "message"
     end
     """},
    {"an underscore-prefixed binding, which also cannot fail",
     """
     test "example" do
       assert _event = call(), "message"
     end
     """},
    {"a fallible match at arity one, which ExUnit translates itself",
     """
     test "example" do
       assert {:ok, _} = call()
     end
     """},
    {"the match?/2 repair",
     """
     test "example" do
       assert match?({:ok, _}, call()), "message"
     end
     """},
    {"the equality repair",
     """
     test "example" do
       assert call() == {:ok, 1}, "message"
     end
     """}
  ]

  for {label, source, expected} <- @rejected do
    test "the scanner reports #{label}" do
      assert sites(unquote(source)) == unquote(expected)
    end
  end

  for {label, source} <- @accepted do
    test "the scanner allows #{label}" do
      assert sites(unquote(source)) == []
    end
  end

  test "no assert/2 or refute/2 in the suite hides its message behind a fallible match" do
    {offenders, unparsed} = scan()

    assert unparsed == [],
           """
           These test files could not be parsed, so this guard could not inspect them.
           A file the guard cannot read is a file the defect can hide in.

           #{Enum.map_join(unparsed, "\n", fn {file, reason} -> "  #{file}: #{reason}" end)}
           """

    assert offenders == [],
           """
           `assert`/`refute` at arity two evaluates its first argument as an ordinary expression.
           A pattern that fails to match raises MatchError before the assertion runs, and the
           message below each of these sites will never be printed:

           #{Enum.map_join(offenders, "\n", fn {file, line} -> "  #{file}:#{line}" end)}

           Rewrite as `assert EXPR == LITERAL, MSG` when the pattern binds nothing, or as
           `assert match?(PATTERN, EXPR), MSG` when it binds only underscores or matches a subset
           of a map. Bind first into a variable when a binding is used by later lines.
           """
  end

  defp scan do
    @root
    |> Path.join("test/**/*.exs")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.reduce({[], []}, fn path, {offenders, unparsed} ->
      relative = Path.relative_to(path, @root)

      case Code.string_to_quoted(File.read!(path)) do
        {:ok, ast} -> {offenders ++ Enum.map(fallible_match_sites(ast), &{relative, &1}), unparsed}
        {:error, {_meta, message, _token}} -> {offenders, unparsed ++ [{relative, inspect(message)}]}
      end
    end)
  end

  defp sites(source), do: source |> Code.string_to_quoted!() |> fallible_match_sites()

  defp fallible_match_sites(ast) do
    for {call, meta, [{:=, _, [left, _right]} | rest]} <- Macro.prewalker(ast),
        called_function(call) in [:assert, :refute],
        rest != [],
        not bare_variable?(left),
        do: meta[:line]
  end

  # The call node is a bare atom for `assert ...` and a dot node for `ExUnit.Assertions.assert
  # ...`, so the function name has to be extracted rather than matched. Any remote target is
  # accepted, not only ExUnit's: at arity two the first argument is evaluated before the callee
  # ever sees it whenever the callee is a function, and ExUnit's macro -- the one case where it is
  # not -- is precisely the one that declines to translate the match. A callee that would genuinely
  # be safe in this shape is exotic enough to be worth rewriting rather than worth widening a hole
  # for, so the guard errs toward naming the site.
  defp called_function({:., _meta, [_target, name]}) when is_atom(name), do: name
  defp called_function(name) when is_atom(name), do: name
  defp called_function(_other), do: nil

  # `{name, meta, context}` with an atom context is a variable; a list in that position is a call.
  defp bare_variable?({name, _meta, context}) when is_atom(name) and is_atom(context), do: true
  defp bare_variable?(_other), do: false
end
