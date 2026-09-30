defmodule C1.NS38K002CoreWebExclusionTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)
  @forbidden_packages ~w(phoenix phoenix_live_view phoenix_html phoenix_pubsub phoenix_template phoenix_live_dashboard plug plug_cowboy plug_crypto bandit cowboy websock websock_adapter)
  @forbidden_modules ~w(Phoenix Plug Bandit Cowboy WebSock)
  @legitimate_packages ~w(mime mint finch req hpax ts_chatterbox hpack_erl grpcbox)

  test "root app declares no web dependency in any environment" do
    ast = read_ast!(Path.join(@root, "mix.exs"))
    project = unique_body!(ast, :project)
    deps = unique_body!(ast, :deps)

    assert is_list(project) and Keyword.keyword?(project) and
             Keyword.get(project, :app) == :ai_orchestrator,
           "the scanned root must declare app :ai_orchestrator"

    names = declared_names!(deps)
    assert names != [], "root dependency declaration was empty"
    assert :jason in names, "root dependency witness :jason was absent"
    assert_excluded!("root mix.exs", package_findings(names))
  end

  test "root lock contains no resolved web package under its key or hex name" do
    entries = @root |> Path.join("mix.lock") |> read_ast!() |> lock_entries!()
    assert entries != [], "root lock was empty"

    names = lock_names!(entries)
    assert Enum.all?(@legitimate_packages, &(&1 in names)),
           "legitimate HTTP/OTel lock witness was absent"

    assert_excluded!("root mix.lock", package_findings(names))
  end

  test "root lib source has no web module references" do
    paths = @root |> Path.join("lib/**/*.ex") |> Path.wildcard() |> Enum.sort()
    assert paths != [], "root lib source scan was empty"

    findings =
      Enum.flat_map(paths, fn path ->
        path |> read_ast!() |> module_findings() |> Enum.map(&{Path.relative_to(path, @root), &1})
      end)

    assert_excluded!("root lib source", findings)
  end

  test "INERT CONTROL: added phoenix and same-count jason-to-plug substitution fail by name" do
    names = declared_names!(unique_body!(read_ast!(Path.join(@root, "mix.exs")), :deps))
    assert package_findings(names ++ [:phoenix]) == ["phoenix"]

    replaced = Enum.map(names, fn name -> if name == :jason, do: :plug, else: name end)
    assert length(replaced) == length(names)
    assert package_findings(replaced) == ["plug"], "same-count control did not name plug"
  end

  test "INERT CONTROL: lock key and hex name are independently checked" do
    hex_name = ~S'%{"renamed" => {:hex, :phoenix, "1.0"}}'
    lock_key = ~S'%{"plug" => {:hex, :renamed, "1.0"}}'
    assert package_findings(lock_names!(lock_entries!(Code.string_to_quoted!(hex_name)))) == ["phoenix"]
    assert package_findings(lock_names!(lock_entries!(Code.string_to_quoted!(lock_key)))) == ["plug"]
    assert package_findings(@legitimate_packages) == []
  end

  test "INERT CONTROL: aliases, quoted atoms, and Erlang module atoms are found" do
    source = ~S'''
    defmodule Synthetic do
      alias Phoenix.LiveView, as: View
      def a, do: View.render()
      def b, do: :"Elixir.Plug.Conn"
      def c, do: :cowboy_router
    end
    '''

    assert source |> Code.string_to_quoted!() |> module_findings() ==
             ["Cowboy", "Phoenix", "Plug"]
  end

  test "INERT CONTROL: comments, docs, and strings do not create references" do
    source = ~S'''
    defmodule Synthetic do
      @moduledoc "Phoenix.LiveView and :cowboy_router"
      # Plug.Conn.call()
      def a, do: "Bandit and WebSock"
    end
    '''

    assert source |> Code.string_to_quoted!() |> module_findings() == []
  end

  defp read_ast!(path) do
    path |> File.read!() |> Code.string_to_quoted!(file: path, emit_warnings: false)
  end

  defp unique_body!(ast, function) do
    {_ast, bodies} =
      Macro.prewalk(ast, [], fn
        {kind, _, [{^function, _, args}, [do: body]]} = node, bodies
        when kind in [:def, :defp] and args in [[], nil] ->
          {node, [body | bodies]}

        node, bodies ->
          {node, bodies}
      end)

    case bodies do
      [body] -> body
      _ -> flunk("expected exactly one literal #{function}/0 body in root mix.exs")
    end
  end

  defp declared_names!(deps) when is_list(deps) do
    Enum.map(deps, fn
      {name, requirement} when is_atom(name) and is_binary(requirement) ->
        name

      {:{}, _, [name, requirement]} when is_atom(name) and is_binary(requirement) ->
        name

      {:{}, _, [name, requirement, options]}
      when is_atom(name) and is_binary(requirement) and is_list(options) ->
        assert Keyword.keyword?(options), "unsupported root dependency options: #{inspect(options)}"
        assert valid_environment?(Keyword.get(options, :only, :all)),
               "unsupported root dependency environment: #{inspect(options)}"

        name

      other ->
        flunk("unsupported root dependency declaration: #{inspect(other)}")
    end)
  end

  defp declared_names!(other), do: flunk("root deps/0 is not a literal list: #{inspect(other)}")

  defp valid_environment?(:all), do: true
  defp valid_environment?(environment) when is_atom(environment), do: true
  defp valid_environment?(environments) when is_list(environments), do: Enum.all?(environments, &is_atom/1)
  defp valid_environment?(_), do: false

  defp lock_entries!({:%{}, _, entries}) when is_list(entries) do
    Enum.map(entries, fn
      {key, {:{}, _, [:hex, hex_name | _]}}
      when (is_binary(key) or is_atom(key)) and is_atom(hex_name) ->
        {to_string(key), Atom.to_string(hex_name)}

      other ->
        flunk("unsupported root lock entry: #{inspect(other)}")
    end)
  end

  defp lock_entries!(other), do: flunk("root mix.lock is not a literal map: #{inspect(other)}")

  defp lock_names!(entries), do: Enum.flat_map(entries, fn {key, hex_name} -> [key, hex_name] end)

  defp package_findings(names) do
    names
    |> Enum.map(&to_string/1)
    |> Enum.filter(&(&1 in @forbidden_packages))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp module_findings(ast) do
    {_ast, found} =
      Macro.prewalk(ast, [], fn
        {:__aliases__, _, parts} = node, found when is_list(parts) ->
          assert Enum.all?(parts, &is_atom/1), "unsupported dynamic root source alias: #{inspect(parts)}"
          name = Enum.map_join(parts, ".", &Atom.to_string/1)
          {node, module_match(name, found)}

        atom, found when is_atom(atom) ->
          {atom, module_match(Atom.to_string(atom), found)}

        node, found ->
          {node, found}
      end)

    found |> Enum.uniq() |> Enum.sort()
  end

  defp module_match(name, found) do
    Enum.reduce(@forbidden_modules, found, fn namespace, acc ->
      erlang = String.downcase(namespace)

      if name == namespace or String.starts_with?(name, namespace <> ".") or
           name == "Elixir." <> namespace or String.starts_with?(name, "Elixir." <> namespace <> ".") or
           name == erlang or String.starts_with?(name, erlang <> "_"),
         do: [namespace | acc],
         else: acc
    end)
  end

  defp assert_excluded!(location, findings) do
    assert findings == [], "#{location} has forbidden web extra: #{inspect(findings)}"
  end
end
