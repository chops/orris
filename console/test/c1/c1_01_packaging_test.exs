defmodule C1.PackagingTest do
  use ExUnit.Case, async: false
  alias C1.Harness

  test "C1-01a the console application is the project's mod and supervises Mutations (Registry, Workers, Store under rest_for_one), QueryRegistry, Controllers, Workers, Endpoint in that order (U1 amendment)" do
    Harness.red!([
      OrrisConsole.Application,
      OrrisConsole.SessionStore,
      OrrisConsole.QueryControllers,
      OrrisConsole.QueryWorkers,
      OrrisConsole.Endpoint
    ])

    C1.Mutations.red!()

    assert Application.spec(:orris_console, :mod) == {OrrisConsole.Application, []}
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(roots: %{"alpha" => root})
    Harness.credential!(config)
    Harness.start_app!(config)
    ids = for {id, _pid, _type, _mods} <- Supervisor.which_children(OrrisConsole.Supervisor), do: id

    assert Enum.reverse(ids) == [
             OrrisConsole.Mutations,
             OrrisConsole.QueryRegistry,
             OrrisConsole.QueryControllers,
             OrrisConsole.QueryWorkers,
             OrrisConsole.Endpoint
           ],
           "RED (U1 amendment C1-01a): children #{inspect(Enum.reverse(ids))}"

    assert %{max_children: 128} = :sys.get_state(OrrisConsole.QueryWorkers) |> Map.take([:max_children])
    assert %{max_children: 128} = :sys.get_state(OrrisConsole.QueryControllers) |> Map.take([:max_children])
    # the Mutations subtree: Registry (start wrapper) -> Workers -> Store, rest_for_one; Workers capped and draining
    mutations = Process.whereis(OrrisConsole.Mutations)
    sub_ids = for {id, _pid, _type, _mods} <- Supervisor.which_children(mutations), do: id

    assert Enum.reverse(sub_ids) == [
             OrrisConsole.MutationRegistry,
             OrrisConsole.MutationWorkers,
             OrrisConsole.SessionStore
           ]

    assert :rest_for_one in Tuple.to_list(:sys.get_state(mutations))
    assert %{max_children: 4} = :sys.get_state(OrrisConsole.MutationWorkers) |> Map.take([:max_children])
    assert {:ok, %{shutdown: :infinity}} = :supervisor.get_childspec(mutations, OrrisConsole.MutationWorkers)
  end

  test "C1-01b startup fails closed without an initialized credential (no implicit generation); STRUCTURAL U1 amendment: the SessionStore failure tuple nests under the Mutations supervisor" do
    Harness.red!([OrrisConsole.Application])
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(roots: %{"alpha" => root})
    Application.put_env(:orris_console, :config, config)
    result = Application.ensure_all_started(:orris_console)
    # refusal preserved: whatever the nesting, the credential clause is the cause and nothing was generated
    assert {:error, {:orris_console, {{:shutdown, {:failed_to_start_child, _child, _reason}}, _}}} = result

    assert Harness.term_contains?(result, "credential_missing"),
           "the refusal lost its credential clause: #{inspect(result)}"

    refute File.exists?(config[:credential_path])
    # the named structural amendment (docs/contracts/console-mutations.org, M-14): nested under Mutations
    assert match?(
             {:error,
              {:orris_console,
               {{:shutdown,
                 {:failed_to_start_child, OrrisConsole.Mutations,
                  {:shutdown,
                   {:failed_to_start_child, OrrisConsole.SessionStore, {:credential, %{clause: "credential_missing"}}}}}},
                _}}},
             result
           ),
           "RED (U1 amendment C1-01b): the SessionStore startup failure is not nested under Mutations: #{inspect(result)}"
  end

  test "C1-01c the release reads the closed configuration FILE named by ORRIS_CONSOLE_CONFIG_FILE and nothing from the environment string" do
    Harness.red!([OrrisConsole.Config])
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(roots: %{"alpha" => root})
    path = Harness.config_file!(config)
    assert {:ok, loaded} = OrrisConsole.Config.load_file(path)
    assert loaded.authority == Harness.authority(config) and loaded.roots == %{"alpha" => root}
    File.write!(path, ~s({"host":"localhost","port":1,"eval":"System.cmd"}))
    assert {:error, %{clause: "config_file_invalid"}} = OrrisConsole.Config.load_file(path)
    assert {:error, %{clause: "config_file_missing"}} = OrrisConsole.Config.load_file(path <> ".absent")
  end
end
