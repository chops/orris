defmodule C1.NS38CoreSurfaceTest do
  @moduledoc """
  NS-38.K.001: the console reaches the core only through `AiOrchestrator.Prepare` and `AiOrchestrator.Query`.

  At the audited head only a grep showed this. These rows measure it twice, structurally, over every console
  source under `console/lib`:

    * SOURCE: each file is parsed and every module reference is resolved through the file's `alias`
      directives (plain, `as:`, and `{}` multi-alias) plus `:"Elixir.AiOrchestrator..."` atom literals. Comments
      are not in the AST and strings and docs are binaries, so neither is ever read as a reference. The bare
      `AiOrchestrator` root is admitted only inside `use Boundary` (the boundary declaration names it).
    * COMPILED: every loaded console module whose source is under `console/lib` is read from its bytecode --
      the remote calls it imports and the module atoms it carries.

  Either measure must name `Prepare` and `Query` (so an empty scan cannot pass) and nothing else in the core.
  The inert control `console/test/fixtures/ns38/forbidden_core_reference.ex.txt` is outside the compile paths (and,
  not being `.ex`, outside Mix's test load filters); both measures run over it and must report exactly its
  forbidden references, and none of its comment, docstring or string decoys.
  """

  use ExUnit.Case, async: true

  @console Path.expand("../..", __DIR__)
  @allowed [AiOrchestrator.Prepare, AiOrchestrator.Query]
  @fixture Path.expand("../fixtures/ns38/forbidden_core_reference.ex.txt", __DIR__)
  @fixture_module C1.Fixtures.ForbiddenCoreReference

  describe "source references" do
    test "console/lib references the core only through Prepare and Query" do
      references =
        for path <- @console |> Path.join("lib/**/*.ex") |> Path.wildcard() |> Enum.sort(),
            module <- source_references(path),
            do: {Path.relative_to(path, @console), module}

      core = references |> Enum.map(&elem(&1, 1)) |> MapSet.new()
      assert MapSet.subset?(MapSet.new(@allowed), core), "the scan found no Prepare/Query use: #{inspect(core)}"

      forbidden = Enum.reject(references, fn {_file, module} -> module in @allowed end)
      assert forbidden == [], "console source reaches the core outside Prepare/Query: #{inspect(forbidden)}"
    end

    test "INERT CONTROL: the fixture's forbidden references are reported, its decoys are not" do
      assert @fixture |> source_references() |> Enum.reject(&(&1 in @allowed)) |> Enum.sort() ==
               Enum.sort([
                 AiOrchestrator,
                 AiOrchestrator.Gate.Execution,
                 AiOrchestrator.Id.SystemId,
                 AiOrchestrator.Journal.Reader,
                 AiOrchestrator.Lifecycle.RunFSM,
                 AiOrchestrator.Run.Executor
               ])
    end

    test "INERT CONTROL: a mention only in comments, docs and strings is no reference" do
      source = """
      defmodule C1.Fixtures.Mentions do
        @moduledoc "reads AiOrchestrator.Lifecycle.RunFSM state"
        # AiOrchestrator.Journal.Writer.append(w, event)
        def a, do: "AiOrchestrator.Host.Monitor"
        def b, do: ~s(AiOrchestrator.Run.Executor)
      end
      """

      assert source |> Code.string_to_quoted!() |> references() == []
    end

    test "the Boundary declaration is the only place the bare core root is admitted" do
      boundary = "defmodule C1.Fixtures.B do\n  use Boundary, deps: [AiOrchestrator], exports: []\nend\n"
      outside = "defmodule C1.Fixtures.O do\n  def a, do: AiOrchestrator\nend\n"

      assert boundary |> Code.string_to_quoted!() |> references() == []
      assert outside |> Code.string_to_quoted!() |> references() == [AiOrchestrator]
    end
  end

  describe "compiled references" do
    test "compiled console/lib modules call and name only Prepare and Query in the core" do
      lib = Path.join(@console, "lib")

      modules =
        for module <- Application.spec(:orris_console, :modules),
            source = module.module_info(:compile)[:source],
            String.starts_with?(List.to_string(source), lib),
            do: module

      assert OrrisConsole.ReadModel in modules and OrrisConsole.MutationOperation in modules,
             "the compiled console modules could not be found: #{inspect(modules)}"

      references =
        for module <- modules,
            {_module, binary, _file} = :code.get_object_code(module),
            core <- compiled_references(binary),
            do: {module, core}

      assert MapSet.subset?(MapSet.new(@allowed), MapSet.new(references, &elem(&1, 1)))

      forbidden = Enum.reject(references, fn {_module, core} -> core in @allowed end)
      assert forbidden == [], "compiled console code reaches the core outside Prepare/Query: #{inspect(forbidden)}"
    end

    test "INERT CONTROL: the fixture compiled in memory is reported by its bytecode" do
      on_exit(fn ->
        :code.purge(@fixture_module)
        :code.delete(@fixture_module)
      end)

      {[{@fixture_module, binary}], _diagnostics} = Code.with_diagnostics(fn -> Code.compile_file(@fixture) end)

      assert binary |> compiled_references() |> Enum.reject(&(&1 in @allowed)) |> Enum.sort() ==
               Enum.sort([
                 AiOrchestrator.Gate.Execution,
                 AiOrchestrator.Id.SystemId,
                 AiOrchestrator.Journal.Reader,
                 AiOrchestrator.Lifecycle.RunFSM,
                 AiOrchestrator.Run.Executor
               ])
    end
  end

  # ---- source measure ----

  defp source_references(path), do: path |> File.read!() |> Code.string_to_quoted!() |> references()

  # every core module the AST names, resolved through the file's aliases; `use Boundary` and the alias
  # directives themselves are consumed whole so their parts are not re-read as bare references
  defp references(ast) do
    aliases = aliases(ast)

    {_ast, found} =
      Macro.prewalk(ast, [], fn
        {:use, _, [{:__aliases__, _, [:Boundary]} | _opts]}, acc ->
          {:ok, acc}

        {:alias, _, _args} = directive, acc ->
          {:ok, Enum.map(alias_targets(directive), &Module.concat/1) ++ acc}

        {:__aliases__, _, parts} = node, acc when is_list(parts) ->
          {node, if(Enum.all?(parts, &is_atom/1), do: [resolve(parts, aliases) | acc], else: acc)}

        atom, acc when is_atom(atom) ->
          {atom, [atom | acc]}

        node, acc ->
          {node, acc}
      end)

    found |> Enum.filter(&core?/1) |> Enum.uniq()
  end

  defp aliases(ast) do
    {_ast, map} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, _args} = directive, acc -> {:ok, Map.merge(acc, alias_map(directive))}
        node, acc -> {node, acc}
      end)

    map
  end

  defp alias_map({:alias, _, [{:__aliases__, _, parts}, [as: {:__aliases__, _, [short]}]]}), do: %{short => parts}

  defp alias_map(directive) do
    Map.new(alias_targets(directive), fn parts -> {List.last(parts), parts} end)
  end

  defp alias_targets({:alias, _, [{:__aliases__, _, parts} | _opts]}), do: [parts]

  defp alias_targets({:alias, _, [{{:., _, [{:__aliases__, _, base}, :{}]}, _, children} | _opts]}),
    do: for({:__aliases__, _, sub} <- children, do: base ++ sub)

  defp alias_targets(_directive), do: []

  defp resolve([head | rest] = parts, aliases) do
    case aliases do
      %{^head => full} -> Module.concat(full ++ rest)
      _none -> Module.concat(parts)
    end
  end

  # ---- compiled measure ----

  defp compiled_references(binary) do
    {:ok, {_module, [imports: imports, atoms: atoms]}} = :beam_lib.chunks(binary, [:imports, :atoms])

    (Enum.map(imports, fn {module, _function, _arity} -> module end) ++ Enum.map(atoms, &elem(&1, 1)))
    |> Enum.filter(&core?/1)
    |> Enum.uniq()
  end

  defp core?(module) when is_atom(module) do
    name = Atom.to_string(module)
    name == "Elixir.AiOrchestrator" or String.starts_with?(name, "Elixir.AiOrchestrator.")
  end
end
