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
    {"a regex whose subject is a path", "def e(path), do: Regex.match?(~r/x/, path)"}
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

  defp decomposition_sites(ast) do
    ast
    |> expand_pipes()
    |> Macro.prewalker()
    |> Enum.flat_map(&site/1)
  end

  # A pipe hides the subject from the callee's argument list, so the whole tree is
  # un-piped before anything is inspected: `run_id |> String.split("_")` and
  # `String.split(run_id, "_")` must reach the matcher as the same call.
  defp expand_pipes(ast) do
    Macro.prewalk(ast, fn
      {:|>, _meta, [left, {_target, _right_meta, args} = right]} when is_list(args) -> Macro.pipe(left, right, 0)
      node -> node
    end)
  end

  defp site({{:., _, [{:__aliases__, _, [:String]}, function]}, meta, [subject | _rest]}) when function in @string_ops,
    do: reported(meta, "String.#{function}", subject)

  defp site({{:., _, [{:__aliases__, _, [:Regex]}, function]}, meta, [_pattern, subject | _rest]})
       when function in @regex_ops, do: reported(meta, "Regex.#{function}", subject)

  defp site({:binary_part, meta, [subject | _rest]}), do: reported(meta, "binary_part", subject)
  defp site(_node), do: []

  defp reported(meta, call, subject) do
    case identity_name(subject) do
      nil -> []
      name -> [{meta[:line], "#{call} on #{name}"}]
    end
  end

  # An identity subject: the bare variable, a string-keyed access, a struct or map field,
  # or a `Map` read of the same key. Anything else is not this row's business.
  defp identity_name({name, _meta, context}) when is_atom(name) and is_atom(context) do
    named(Atom.to_string(name))
  end

  defp identity_name({{:., _, [Access, :get]}, _, [_subject, key]}) when is_binary(key), do: named(key)

  defp identity_name({{:., _, [{:__aliases__, _, [:Map]}, reader]}, _, [_subject, key]})
       when reader in @map_readers and is_binary(key), do: named(key)

  defp identity_name({{:., _, [_subject, field]}, _, []}) when is_atom(field), do: named(Atom.to_string(field))
  defp identity_name(_other), do: nil

  defp named(name) when is_binary(name), do: if(name in @identity_names, do: name)
end
