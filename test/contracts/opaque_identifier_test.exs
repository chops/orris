defmodule AiOrchestrator.Contracts.OpaqueIdentifierTest do
  @moduledoc """
  NS-37.A.001 control: run identities stay opaque to their consumers.

  The row's failure control is "consumer parsing time/project/actor from ID fails". At the
  audited head that control was proven only by a grep nothing enforced, while the id itself
  DOES embed a parseable UTC stamp — so the whole guarantee is "no consumer parses it".
  This makes that a control: an AST gate over every delivered module, of the same shape
  `bin/verify` already runs for reducer purity, that names any site decomposing a value
  bound from `run_id` or `supervisor_instance`.

  SCOPE, stated so the gate is not read as wider than it is: `run_ref` is deliberately NOT
  an identity subject here. It is a public directory HANDLE, and `Prepare.Scope` must test
  it for separators, NUL bytes and printability — containment, not identity parsing.

  The tree scan and the synthetic table need each other. The scan proves the tree is clean
  today but reports the same green against a matcher that has stopped detecting anything;
  the table proves the matcher still finds the decomposition it was written for.

  The matcher also reports three forms that take an identity apart without a `String` or
  `Regex` call: a binary pattern match (`"run_" <> rest = run_id`, a `<<...>>` pattern, or a
  `case` clause of either shape), any Erlang `:binary` call on the identity, and an identity
  renamed first (`id = run_id`, `%{"run_id" => id}`, `Keyword.fetch!(opts, :run_id)`) and
  decomposed under the new name. Renames are tracked within one function clause.
  `test/fixtures/contracts/opaque_identifier/run_id_decompositions.ex.txt` holds one function
  per form; it is parsed, never compiled (and, not being `.ex`, outside Mix's test load
  filters), and every function in it must be reported.
  """

  use ExUnit.Case, async: true

  alias AiOrchestrator.Id.SystemId

  @root Path.expand("../..", __DIR__)
  @source_roots ~w(lib console/lib)

  # The opaque identities NS-37 names. `run_ref` is excluded on purpose (see @moduledoc).
  @identity_names ~w(run_id supervisor_instance)

  @string_ops ~w(split slice starts_with? ends_with? contains? at first last to_integer)a
  @regex_ops ~w(run match? scan named_captures replace)a
  @map_readers ~w(get fetch fetch!)a
  @def_kinds ~w(def defp defmacro defmacrop)a

  @fixture Path.expand("../fixtures/contracts/opaque_identifier/run_id_decompositions.ex.txt", __DIR__)

  # Every function the fixture must hold, so a form deleted from it is noticed.
  @fixture_functions ~w(
    prefix_match prefix_match_in_head bitstring_match case_prefix binary_split binary_part_call
    binary_match_on_field renamed_then_split renamed_twice_then_binary renamed_by_map_pattern
    renamed_by_keyword_read_then_matched
  )a

  @rejected [
    {"String.split applied to a run id variable", "def a(run_id), do: String.split(run_id, \"_\")"},
    {"a piped decomposition, which hides the subject from the argument list",
     "def b(run_id), do: run_id |> String.slice(4, 8)"},
    {"binary_part on a run id", "def c(run_id), do: binary_part(run_id, 4, 8)"},
    {"a regex match against a run id", "def d(run_id), do: Regex.match?(~r/x/, run_id)"},
    {"a decomposition of a map field holding the identity", ~s{def e(event), do: String.split(event["run_id"], "_")}},
    {"a decomposition of a struct field holding the identity",
     "def f(state), do: String.starts_with?(state.run_id, \"run_\")"},
    {"a decomposition of a Map.fetch! of the identity",
     "def g(event), do: String.slice(Map.fetch!(event, \"supervisor_instance\"), 0, 4)"}
  ]

  @accepted [
    {"comparing a run id whole", "def a(run_id, other), do: run_id == other"},
    {"carrying a run id into a map", "def b(run_id), do: %{\"run_id\" => run_id}"},
    {"decomposing a handle, which is a path segment and not an identity",
     "def c(run_ref), do: String.contains?(run_ref, [\"/\", <<0>>])"},
    {"decomposing an unrelated value", "def d(reason), do: String.split(reason, \":\")"},
    {"a regex whose subject is a path", "def e(path), do: Regex.match?(~r/x/, path)"},
    {"renaming a run id and comparing it whole", "def f(run_id) do\n  id = run_id\n  id == \"x\"\nend"},
    {"a prefix match on an unrelated value", "def g(reason) do\n  \"run_\" <> rest = reason\n  rest\nend"},
    {"a :binary call on an unrelated value", "def h(path), do: :binary.split(path, \"/\")"},
    {"an id-named variable that is not bound from an identity", "def i(id), do: String.split(id, \"_\")"},
    {"a rename in one clause does not leak into another",
     "def j(run_id) do\n  id = run_id\n  id\nend\ndef k(id), do: String.split(id, \"_\")"}
  ]

  for {label, source} <- @rejected do
    test "the gate reports #{label}" do
      refute sites(unquote(source)) == [], unquote(label)
    end
  end

  for {label, source} <- @accepted do
    test "the gate allows #{label}" do
      assert sites(unquote(source)) == [], unquote(label)
    end
  end

  describe "the test-local decomposition fixture" do
    test "holds every form, parsed and never compiled" do
      assert fixture_functions() |> Map.keys() |> Enum.sort() == Enum.sort(@fixture_functions)
      refute Code.ensure_loaded?(AiOrchestrator.Test.Fixtures.RunIdDecompositions), "the fixture must stay inert"
    end

    for name <- @fixture_functions do
      test "the gate reports the fixture's #{name}" do
        clause = Map.fetch!(fixture_functions(), unquote(name))
        refute decomposition_sites(clause) == [], "#{unquote(name)} takes a run id apart and was not reported"
      end
    end

    test "the unextended matcher misses every form the fixture holds" do
      # the control that makes the extension necessary: the String/Regex/binary_part matcher
      # alone, with no pattern, :binary or rename rules, reports none of these functions
      for {name, clause} <- fixture_functions() do
        assert clause |> expand_pipes() |> Macro.prewalker() |> Enum.flat_map(&call_site(&1, %{})) == [],
               "#{name} was already caught by the String/Regex/binary_part rules"
      end
    end
  end

  test "no delivered module decomposes a run or supervisor identity" do
    {offenders, unparsed} = scan()

    assert unparsed == [],
           """
           These delivered sources could not be parsed, so this gate could not inspect them.
           A file the gate cannot read is a file the defect can hide in.

           #{Enum.map_join(unparsed, "\n", fn {file, reason} -> "  #{file}: #{reason}" end)}
           """

    assert offenders == [],
           """
           NS-37.A.001: a consumer decomposes an opaque identity. Run ids carry a UTC stamp,
           so any consumer that splits, slices or matches one turns a journaled fact into a
           parsed field. Address a run by its handle and read `run_id` out of the fold.

           #{Enum.map_join(offenders, "\n", fn {file, line, call} -> "  #{file}:#{line} #{call}" end)}
           """
  end

  test "generated identities are path-safe and unique" do
    run_ids = for _index <- 1..16, do: SystemId.run_id()
    supervisor_ids = for _index <- 1..16, do: SystemId.supervisor_instance()

    assert length(Enum.uniq(run_ids)) == 16
    assert length(Enum.uniq(supervisor_ids)) == 16

    for id <- run_ids ++ supervisor_ids do
      assert String.printable?(id), id
      refute String.contains?(id, ["/", "\\", <<0>>, ".", " "]), id
      assert byte_size(id) <= 255, id
    end

    for id <- run_ids, do: assert(Regex.match?(~r/^run_\d{8}T\d{6}Z_[0-9a-f]{24}$/, id), id)
    for id <- supervisor_ids, do: assert(Regex.match?(~r/^sup_[0-9a-f]{24}$/, id), id)
  end

  defp scan do
    for root <- @source_roots,
        path <- @root |> Path.join("#{root}/**/*.ex") |> Path.wildcard() |> Enum.sort(),
        reduce: {[], []} do
      {offenders, unparsed} ->
        relative = Path.relative_to(path, @root)

        case Code.string_to_quoted(File.read!(path)) do
          {:ok, ast} ->
            {offenders ++ Enum.map(decomposition_sites(ast), fn {line, call} -> {relative, line, call} end), unparsed}

          {:error, {_meta, message, _token}} ->
            {offenders, unparsed ++ [{relative, inspect(message)}]}
        end
    end
  end

  defp sites(source), do: source |> Code.string_to_quoted!() |> decomposition_sites()

  # The fixture's function clauses by name, each still a `def` node for the matcher.
  defp fixture_functions do
    {:ok, ast} = @fixture |> File.read!() |> Code.string_to_quoted()

    ast
    |> Macro.prewalker()
    |> Enum.flat_map(fn
      {:def, _meta, [head | _body]} = clause -> [{head |> strip_guard() |> elem(0), clause}]
      _ast_node -> []
    end)
    |> Map.new()
  end

  # Every call site over the whole tree with no renames known, then every function clause
  # again with the renames bound inside it; a site both passes see is reported once.
  defp decomposition_sites(ast) do
    ast = expand_pipes(ast)
    whole = ast |> Macro.prewalker() |> Enum.flat_map(&site(&1, %{}))

    per_clause =
      for {kind, _meta, [_head | _body]} = clause <- Macro.prewalker(ast),
          kind in @def_kinds,
          renames = renames(clause),
          ast_node <- Macro.prewalker(clause),
          found <- site(ast_node, renames),
          do: found

    Enum.uniq(whole ++ per_clause)
  end

  # A pipe hides the subject from the callee's argument list, so the whole tree is
  # un-piped before anything is inspected: `run_id |> String.split("_")` and
  # `String.split(run_id, "_")` must reach the matcher as the same call.
  defp expand_pipes(ast) do
    Macro.prewalk(ast, fn
      {:|>, _meta, [left, {_target, _right_meta, args} = right]} when is_list(args) -> Macro.pipe(left, right, 0)
      ast_node -> ast_node
    end)
  end

  # Taking an identity apart without a String/Regex call: a binary pattern matched against
  # it (in a body, a head, or a case clause), or any Erlang `:binary` call on it.
  defp site({:=, meta, [pattern, subject]}, renames) do
    if binary_pattern?(pattern), do: reported(meta, "binary pattern match", subject, renames), else: []
  end

  defp site({:case, meta, [subject, [do: clauses]]}, renames) when is_list(clauses) do
    if Enum.any?(clauses, &clause_binary?/1),
      do: reported(meta, "case binary pattern", subject, renames),
      else: []
  end

  defp site({{:., _, [:binary, function]}, meta, args}, renames) when is_list(args) do
    args |> Enum.flat_map(&reported(meta, ":binary.#{function}", &1, renames)) |> Enum.take(1)
  end

  defp site(ast_node, renames), do: call_site(ast_node, renames)

  defp call_site({{:., _, [{:__aliases__, _, [:String]}, function]}, meta, [subject | _rest]}, renames)
       when function in @string_ops, do: reported(meta, "String.#{function}", subject, renames)

  defp call_site({{:., _, [{:__aliases__, _, [:Regex]}, function]}, meta, [_pattern, subject | _rest]}, renames)
       when function in @regex_ops, do: reported(meta, "Regex.#{function}", subject, renames)

  defp call_site({:binary_part, meta, [subject | _rest]}, renames), do: reported(meta, "binary_part", subject, renames)
  defp call_site(_ast_node, _renames), do: []

  defp clause_binary?({:->, _, [[pattern], _body]}), do: pattern |> strip_guard() |> binary_pattern?()
  defp clause_binary?(_clause), do: false

  defp binary_pattern?({:<>, _, _}), do: true
  defp binary_pattern?({:<<>>, _, _}), do: true
  defp binary_pattern?({:=, _, [left, right]}), do: binary_pattern?(left) or binary_pattern?(right)
  defp binary_pattern?(_other), do: false

  defp strip_guard({:when, _, [pattern | _guards]}), do: pattern
  defp strip_guard(pattern), do: pattern

  defp reported(meta, call, subject, renames) do
    case identity_name(subject, renames) do
      nil -> []
      name -> [{meta[:line], "#{call} on #{name}"}]
    end
  end

  # Variables that hold an identity under another name inside one function clause: bound by
  # a match against an identity (`id = run_id`, `id = state.run_id`, `id = Keyword.fetch!(opts,
  # :run_id)`, and onward through `other = id`), or by an identity key in a map pattern
  # (`%{"run_id" => id}`, `%{run_id: id}`). Grown to a fixed point, so chains are followed.
  defp renames(clause, known \\ %{}) do
    grown =
      clause
      |> Macro.prewalker()
      |> Enum.reduce(known, fn ast_node, acc -> Map.merge(acc, rename_from(ast_node, acc)) end)

    if map_size(grown) == map_size(known), do: grown, else: renames(clause, grown)
  end

  defp rename_from({:=, _, [left, right]}, known) do
    case {identity_name(right, known), identity_name(left, known)} do
      {nil, nil} -> %{}
      {name, nil} -> variable(left, name)
      {nil, name} -> variable(right, name)
      {_both, _named} -> %{}
    end
  end

  defp rename_from({:%{}, _, pairs}, _known) when is_list(pairs) do
    for {key, value} <- pairs, is_binary(key) or is_atom(key), name = named(to_string(key)), name != nil, reduce: %{} do
      acc -> Map.merge(acc, variable(value, name))
    end
  end

  defp rename_from(_ast_node, _known), do: %{}

  defp variable({var, _meta, context}, name) when is_atom(var) and is_atom(context) do
    if named(Atom.to_string(var)) || String.starts_with?(Atom.to_string(var), "_"),
      do: %{},
      else: %{var => root_name(name)}
  end

  defp variable(_pattern, _name), do: %{}

  defp root_name(name), do: name |> String.split(" ") |> hd()

  # An identity subject: the bare variable, a variable renamed from one, a keyed access, a
  # struct or map field, or a `Map`/`Keyword` read of the same key.
  defp identity_name({var, _meta, context}, renames) when is_atom(var) and is_atom(context) do
    case renames do
      %{^var => name} -> "#{name} (renamed #{var})"
      _none -> named(Atom.to_string(var))
    end
  end

  defp identity_name({{:., _, [Access, :get]}, _, [_subject, key]}, _renames) when is_binary(key) or is_atom(key),
    do: named(to_string(key))

  defp identity_name({{:., _, [{:__aliases__, _, [module]}, reader]}, _, [_subject, key]}, _renames)
       when module in [:Map, :Keyword] and reader in @map_readers and (is_binary(key) or is_atom(key)),
       do: named(to_string(key))

  defp identity_name({{:., _, [_subject, field]}, _, []}, _renames) when is_atom(field), do: named(Atom.to_string(field))

  defp identity_name(_other, _renames), do: nil

  defp named(name) when is_binary(name), do: if(name in @identity_names, do: name)
end
