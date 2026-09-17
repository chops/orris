defmodule AiOrchestrator.Dispatch.PaneClientShellTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Dispatch.PaneClient

  test "stdin wrapper ignores ambient startup code but retains the target environment" do
    root = Path.join(System.tmp_dir!(), "pane_client_shell_#{System.unique_integer([:positive])}")
    File.mkdir!(root)
    keys = ["BASH_ENV", "ORRIS_TEST_SHELL_SENTINEL"]
    previous = Map.new(keys, &{&1, System.get_env(&1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)

      File.rm_rf!(root)
    end)

    bash = System.find_executable("bash") || flunk("Bash is required")
    startup = Path.join(root, "startup.bash")
    sentinel = Path.join(root, "startup-ran")
    executable = Path.join(root, "fake-ap")

    File.write!(startup, "printf '%s\\n' startup >> \"$ORRIS_TEST_SHELL_SENTINEL\"\n")

    File.write!(executable, """
    #!#{bash} -p
    test -n "$BASH_ENV" || exit 1
    printf '%s\\n' '{"sent":true,"target_environment_retained":true}'
    """)

    File.chmod!(executable, 0o700)
    System.put_env("BASH_ENV", startup)
    System.put_env("ORRIS_TEST_SHELL_SENTINEL", sentinel)

    assert {:ok, %{"sent" => true, "target_environment_retained" => true}} =
             PaneClient.send("pane_writer", "Do the work", ap_path: executable)

    refute File.exists?(sentinel)
    assert System.get_env("BASH_ENV") == startup
  end
end
