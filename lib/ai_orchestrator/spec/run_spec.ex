defmodule AiOrchestrator.Spec.RunSpec do
  @moduledoc false

  alias AiOrchestrator.Spec.Budgets

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  # Version 1 is immutable: `budgets` REQUIRED and untyped. Version 2 (docs/contracts/typed-budgets-v2.org) is a
  # separately supported version: the v1 shape with `budgets` OPTIONAL and, when present, strict (`Budgets`).
  @supported_versions [1, 2]

  @agent_name_regex ~r/^[a-z][a-z0-9_-]{0,31}$/
  @reserved_agent_names MapSet.new(["user", "orchestrator"])
  @effort_hints ["low", "medium", "high", "xhigh"]

  @doc "JSON encoding of one gate definition as embedded in assignment prompts; `[]` when the gate is unknown."
  @spec gate_json(map(), String.t()) :: String.t()
  def gate_json(spec, gate_id) when is_map(spec) and is_binary(gate_id) do
    Jason.encode!(get_in(spec, ["gates", gate_id]) || [])
  end

  @spec validate(map()) :: {:ok, map()} | {:error, rejection()}
  def validate(spec) when is_map(spec) do
    with :ok <- validate_schema_version(spec),
         :ok <- validate_gates(spec["gates"]),
         :ok <- validate_agents(spec["agents"]),
         :ok <- validate_allowed_roots(spec),
         :ok <- validate_budgets(spec),
         {:ok, parsed} <- Zoi.parse(schema(spec["schema_version"]), spec) do
      {:ok, parsed}
    else
      {:error, %{} = rejection} -> {:error, rejection}
      {:error, _errors} -> {:error, %{clause: "invalid_run_spec_shape"}}
    end
  end

  def validate(_spec), do: {:error, %{clause: "invalid_run_spec_shape"}}

  @doc """
  The budgets of a spec returned by `validate/1`: `{:typed, map}` for version 2 (`{:typed, %{}}` when the section
  is absent), `{:legacy, map}` for version 1 - a v1 map is never called typed, even when its contents would
  satisfy `Budgets` (no silent upcast). Unsupported or missing versions and non-maps ->
  `unsupported_schema_version`. An unvalidated map carrying version 2 has its section judged (a malformed section
  is `budget_invalid`, never typed); an unvalidated map carrying version 1 whose budgets is not a map is
  `invalid_run_spec_shape`, never legacy. This authenticates nothing about a run directory.
  """
  @spec budgets(term()) :: {:typed, Budgets.typed()} | {:legacy, map()} | {:error, rejection()}
  def budgets(%{"schema_version" => 2} = spec) do
    case Budgets.validate(Map.get(spec, "budgets", %{})) do
      {:ok, typed} -> {:typed, typed}
      {:error, rejection} -> {:error, rejection}
    end
  end

  def budgets(%{"schema_version" => 1, "budgets" => legacy}) when is_map(legacy), do: {:legacy, legacy}
  def budgets(%{"schema_version" => 1}), do: {:error, %{clause: "invalid_run_spec_shape"}}
  def budgets(_other), do: {:error, %{clause: "unsupported_schema_version"}}

  # version 2 only: a PRESENT section is judged by the standalone schema before the shape parse
  defp validate_budgets(%{"schema_version" => 2} = spec) do
    case Map.fetch(spec, "budgets") do
      :error ->
        :ok

      {:ok, section} ->
        case Budgets.validate(section) do
          {:ok, _typed} -> :ok
          {:error, rejection} -> {:error, rejection}
        end
    end
  end

  defp validate_budgets(_spec), do: :ok

  defp schema(2) do
    Zoi.map(
      %{
        "schema" => Zoi.literal("ai-orchestrator/run-spec"),
        "schema_version" => Zoi.literal(2),
        "goal" => Zoi.string(),
        "repo_root" => Zoi.string(),
        "allowed_roots" => Zoi.array(Zoi.string()),
        "agents" => Zoi.array(agent_schema()),
        "gates" => Zoi.map(Zoi.string(), Zoi.array(Zoi.string())),
        "budgets" => Zoi.optional(Zoi.map()),
        "stop_policy" => Zoi.map(),
        "notification" => Zoi.optional(Zoi.array(Zoi.string())),
        "fingerprint_policy" => Zoi.optional(Zoi.enum(["warn", "escalate"])),
        "stretch_worktrees" => Zoi.optional(Zoi.boolean())
      },
      unrecognized_keys: :error
    )
  end

  # version 1: byte-for-byte the historical schema
  defp schema(_version) do
    Zoi.map(
      %{
        "schema" => Zoi.literal("ai-orchestrator/run-spec"),
        "schema_version" => Zoi.literal(1),
        "goal" => Zoi.string(),
        "repo_root" => Zoi.string(),
        "allowed_roots" => Zoi.array(Zoi.string()),
        "agents" => Zoi.array(agent_schema()),
        "gates" => Zoi.map(Zoi.string(), Zoi.array(Zoi.string())),
        "budgets" => Zoi.map(),
        "stop_policy" => Zoi.map(),
        "notification" => Zoi.optional(Zoi.array(Zoi.string())),
        "fingerprint_policy" => Zoi.optional(Zoi.enum(["warn", "escalate"])),
        "stretch_worktrees" => Zoi.optional(Zoi.boolean())
      },
      unrecognized_keys: :error
    )
  end

  defp agent_schema do
    Zoi.map(
      %{
        "name" => Zoi.string(),
        "role" => Zoi.string(),
        "agent_id" => Zoi.optional(Zoi.string()),
        "pane_hint" => Zoi.optional(pane_hint_schema()),
        "concurrency_cap" => Zoi.optional(Zoi.integer()),
        "effort_hint" => Zoi.optional(Zoi.enum(@effort_hints))
      },
      unrecognized_keys: :error
    )
  end

  defp pane_hint_schema do
    Zoi.map(
      %{
        "pane_ref" => Zoi.optional(Zoi.string()),
        "session_name" => Zoi.optional(Zoi.string())
      },
      unrecognized_keys: :error
    )
  end

  defp validate_schema_version(%{"schema_version" => version}) when version in @supported_versions, do: :ok
  defp validate_schema_version(%{"schema_version" => _version}), do: {:error, %{clause: "unsupported_schema_version"}}
  defp validate_schema_version(_spec), do: {:error, %{clause: "invalid_run_spec_shape"}}

  defp validate_gates(gates) when gates == %{}, do: {:error, %{clause: "empty_gates"}}

  defp validate_gates(gates) when is_map(gates) do
    case Enum.find(gates, fn {_name, command} -> not argv?(command) end) do
      nil -> :ok
      {name, _command} -> {:error, %{clause: "gate_not_argv", field: name}}
    end
  end

  defp validate_gates(_gates), do: {:error, %{clause: "invalid_run_spec_shape"}}

  defp validate_agents(agents) when is_list(agents) do
    Enum.reduce_while(agents, :ok, fn agent, :ok ->
      case validate_agent(agent) do
        :ok -> {:cont, :ok}
        {:error, rejection} -> {:halt, {:error, rejection}}
      end
    end)
  end

  defp validate_agents(_agents), do: {:error, %{clause: "invalid_run_spec_shape"}}

  defp validate_allowed_roots(%{"repo_root" => repo_root, "allowed_roots" => roots})
       when is_binary(repo_root) and is_list(roots) do
    case Enum.find(roots, &(not path_under_base?(repo_root, &1))) do
      nil -> :ok
      root -> {:error, %{clause: "allowed_root_outside_repo", field: root}}
    end
  end

  defp validate_allowed_roots(_spec), do: {:error, %{clause: "invalid_run_spec_shape"}}

  defp validate_agent(%{"name" => name} = agent) when is_binary(name) do
    cond do
      not Regex.match?(@agent_name_regex, name) -> {:error, %{clause: "agent_name_grammar", field: "name"}}
      MapSet.member?(@reserved_agent_names, name) -> {:error, %{clause: "reserved_agent_name", field: "name"}}
      true -> validate_pane_hint(agent["pane_hint"])
    end
  end

  defp validate_agent(_agent), do: {:error, %{clause: "invalid_run_spec_shape"}}

  defp validate_pane_hint(nil), do: :ok

  defp validate_pane_hint(%{"pane_ref" => pane_ref}) when is_binary(pane_ref) do
    if String.trim(pane_ref) == "" do
      {:error, %{clause: "pane_ref_blank", field: "pane_hint.pane_ref"}}
    else
      :ok
    end
  end

  defp validate_pane_hint(%{}), do: {:error, %{clause: "invalid_pane_hint", field: "pane_ref"}}
  defp validate_pane_hint(_pane_hint), do: {:error, %{clause: "invalid_pane_hint", field: "pane_ref"}}

  defp argv?([head | _rest] = argv), do: is_binary(head) and Enum.all?(argv, &is_binary/1)
  defp argv?(_argv), do: false

  defp path_under_base?(base, path) when is_binary(path) do
    expanded_base = Path.expand(base)
    expanded_path = Path.expand(path, expanded_base)

    expanded_path == expanded_base or String.starts_with?(expanded_path, expanded_base <> "/")
  end

  defp path_under_base?(_base, _path), do: false
end
