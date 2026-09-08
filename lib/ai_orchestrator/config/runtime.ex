defmodule AiOrchestrator.Config.Runtime do
  @moduledoc """
  Validated runtime configuration for local operator integrations.

  Precedence is explicit flags, then environment, then an optional local config
  file, then public defaults.
  """

  @default_config_file ".ai-orchestrator/config.local.json"
  @defaults %{"ap_path" => "ap", "poll_interval_ms" => 250, "default_assignment_timeout_s" => 900}

  @type t :: %{
          required(:ap_path) => String.t(),
          required(:poll_interval_ms) => pos_integer(),
          required(:default_assignment_timeout_s) => pos_integer(),
          required(:pane_registry_root) => String.t(),
          required(:gate_guardian) => String.t() | nil
        }

  @spec resolve(keyword()) :: {:ok, t()} | {:error, map()}
  def resolve(opts \\ []) when is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())

    with {:ok, file_config} <- file_config(opts, env),
         {:ok, env_overrides} <- env_config(env) do
      env
      |> defaults()
      |> Map.merge(file_config)
      |> Map.merge(env_overrides)
      |> Map.merge(flag_config(opts))
      |> validate()
    end
  end

  defp file_config(opts, env) do
    file = Keyword.get(opts, :config_file) || Map.get(env, "AI_ORCHESTRATOR_CONFIG") || @default_config_file
    reader = Keyword.get(opts, :file_reader, &File.read/1)

    case reader.(file) do
      {:ok, contents} -> parse_config_file(contents, file)
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, %{"reason" => "config_file_read_failed", "path" => file, "detail" => inspect(reason)}}
    end
  end

  defp parse_config_file(contents, file) do
    with {:ok, decoded} <- Jason.decode(contents),
         {:ok, parsed} <- Zoi.parse(file_schema(), decoded) do
      {:ok, parsed}
    else
      {:error, _reason} -> {:error, %{"reason" => "invalid_config_file", "path" => file}}
    end
  end

  defp file_schema do
    Zoi.map(
      %{
        "ap_path" => Zoi.optional(Zoi.string()),
        "poll_interval_ms" => Zoi.optional(Zoi.integer()),
        "default_assignment_timeout_s" => Zoi.optional(Zoi.integer()),
        "pane_registry_root" => Zoi.optional(Zoi.string()),
        "gate_guardian" => Zoi.optional(Zoi.string())
      },
      unrecognized_keys: :error
    )
  end

  defp env_config(env) do
    Enum.reduce_while(
      [
        {"AI_ORCHESTRATOR_AP_PATH", "ap_path", &{:ok, &1}},
        {"AI_ORCHESTRATOR_POLL_INTERVAL_MS", "poll_interval_ms", &parse_int/1},
        {"AI_ORCHESTRATOR_ASSIGNMENT_TIMEOUT_S", "default_assignment_timeout_s", &parse_int/1},
        {"AI_ORCHESTRATOR_PANE_REGISTRY_ROOT", "pane_registry_root", &{:ok, &1}},
        {"AI_ORCHESTRATOR_GATE_GUARDIAN", "gate_guardian", &{:ok, &1}}
      ],
      {:ok, %{}},
      fn {var, key, parser}, {:ok, acc} ->
        case declared_env_value(env, var, key, parser) do
          :absent -> {:cont, {:ok, acc}}
          {:ok, parsed} -> {:cont, {:ok, Map.put(acc, key, parsed)}}
          {:error, rejection} -> {:halt, {:error, rejection}}
        end
      end
    )
  end

  defp declared_env_value(env, var, key, parser) do
    case Map.get(env, var) do
      nil -> :absent
      value -> parse_declared_value(value, key, var, parser)
    end
  end

  defp parse_declared_value(value, key, var, parser) do
    case parser.(value) do
      {:ok, parsed} -> {:ok, parsed}
      :error -> {:error, %{"reason" => "invalid_config", "field" => key, "env" => var}}
    end
  end

  defp parse_int(value) do
    case Integer.parse(value) do
      {parsed, ""} -> {:ok, parsed}
      _other -> :error
    end
  end

  defp flag_config(opts) do
    Enum.reduce(
      [
        {:ap_path, "ap_path"},
        {:poll_interval_ms, "poll_interval_ms"},
        {:default_assignment_timeout_s, "default_assignment_timeout_s"},
        {:pane_registry_root, "pane_registry_root"},
        {:gate_guardian, "gate_guardian"}
      ],
      %{},
      fn {flag, key}, acc ->
        case Keyword.fetch(opts, flag) do
          {:ok, value} -> Map.put(acc, key, value)
          :error -> acc
        end
      end
    )
  end

  defp validate(%{"ap_path" => ap_path} = config) when is_binary(ap_path) do
    with :ok <- validate_non_empty(ap_path),
         :ok <- validate_non_empty(config["pane_registry_root"], "pane_registry_root"),
         :ok <- validate_positive(config, "poll_interval_ms"),
         :ok <- validate_positive(config, "default_assignment_timeout_s"),
         :ok <- validate_gate_guardian(config["gate_guardian"]) do
      {:ok,
       %{
         ap_path: ap_path,
         poll_interval_ms: config["poll_interval_ms"],
         default_assignment_timeout_s: config["default_assignment_timeout_s"],
         pane_registry_root: Path.expand(config["pane_registry_root"]),
         gate_guardian: gate_guardian(config["gate_guardian"])
       }}
    end
  end

  defp validate(_config), do: {:error, %{"reason" => "invalid_config", "field" => "ap_path"}}

  # the native guardian path: absent -> nil (the Host refuses at PrepareGate); a present value
  # must be an absolute, non-empty, NUL-free path (executability is checked at use)
  defp validate_gate_guardian(nil), do: :ok

  defp validate_gate_guardian(path) when is_binary(path),
    do: if(absolute_arg?(path), do: :ok, else: {:error, %{"reason" => "invalid_config", "field" => "gate_guardian"}})

  defp validate_gate_guardian(_other), do: {:error, %{"reason" => "invalid_config", "field" => "gate_guardian"}}

  defp absolute_arg?(path), do: path != "" and Path.type(path) == :absolute and not String.contains?(path, <<0>>)

  defp gate_guardian(nil), do: nil
  defp gate_guardian(path), do: path

  defp validate_non_empty(value, field \\ "ap_path")

  defp validate_non_empty(value, field) when is_binary(value) do
    case String.trim(value) do
      "" -> {:error, %{"reason" => "invalid_config", "field" => field}}
      _non_empty -> :ok
    end
  end

  defp validate_non_empty(_value, field), do: {:error, %{"reason" => "invalid_config", "field" => field}}

  defp validate_positive(config, key) do
    case config[key] do
      value when is_integer(value) and value > 0 -> :ok
      _other -> {:error, %{"reason" => "invalid_config", "field" => key}}
    end
  end

  defp defaults(env) do
    state_home =
      Map.get(env, "XDG_STATE_HOME") ||
        Path.join(Map.get(env, "HOME") || System.user_home!(), ".local/state")

    Map.put(@defaults, "pane_registry_root", Path.join([state_home, "ai-orchestrator", "pane-claims"]))
  end
end
