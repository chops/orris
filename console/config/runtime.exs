import Config

# The release reads ONLY the closed JSON configuration file named by ORRIS_CONSOLE_CONFIG_FILE (Config.load_file/1);
# nothing is taken from an environment string. In dev/test the keyword under :config is used instead.
if config_env() == :prod do
  case System.get_env("ORRIS_CONSOLE_CONFIG_FILE") do
    nil -> :ok
    path -> config :orris_console, :config_file, path
  end
end
