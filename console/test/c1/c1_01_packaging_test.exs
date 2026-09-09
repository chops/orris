defmodule C1.PackagingTest do
  use ExUnit.Case, async: false
  alias C1.Harness

  test "C1-01a the console application is the project's mod and supervises Store, Registry, Controllers, Workers, Endpoint in that order" do
    Harness.red!([
      OrrisConsole.Application,
      OrrisConsole.SessionStore,
      OrrisConsole.QueryControllers,
      OrrisConsole.QueryWorkers,
      OrrisConsole.Endpoint
    ])

    assert Application.spec(:orris_console, :mod) == {OrrisConsole.Application, []}
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(roots: %{"alpha" => root})
    Harness.credential!(config)
    Harness.start_app!(config)
    ids = for {id, _pid, _type, _mods} <- Supervisor.which_children(OrrisConsole.Supervisor), do: id

    assert Enum.reverse(ids) == [
             OrrisConsole.SessionStore,
             OrrisConsole.QueryRegistry,
             OrrisConsole.QueryControllers,
             OrrisConsole.QueryWorkers,
             OrrisConsole.Endpoint
           ]

    assert %{max_children: 128} = :sys.get_state(OrrisConsole.QueryWorkers) |> Map.take([:max_children])
    assert %{max_children: 128} = :sys.get_state(OrrisConsole.QueryControllers) |> Map.take([:max_children])
  end

  test "C1-01b startup fails closed without an initialized credential (no implicit generation)" do
    Harness.red!([OrrisConsole.Application])
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(roots: %{"alpha" => root})
    Application.put_env(:orris_console, :config, config)

    assert {:error,
            {:orris_console,
             {{:shutdown,
               {:failed_to_start_child, OrrisConsole.SessionStore, {:credential, %{clause: "credential_missing"}}}}, _}}} =
             Application.ensure_all_started(:orris_console)

    refute File.exists?(config[:credential_path])
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
