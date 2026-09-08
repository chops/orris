defmodule AiOrchestrator.Spec.Plan do
  @moduledoc false

  alias AiOrchestrator.Spec.RunSpec

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  @effort_hints ["low", "medium", "high", "xhigh"]
  @writer_kinds MapSet.new(["implement", "integration"])

  @doc """
  Hash of the plan's initial context as recorded at plan time: sha256 over the
  JSON encoding of `context_initial`, or over the empty string when absent. The
  encoding is a wire fact; a canonical JSON scheme would be a new hash rule.
  """
  @spec context_initial_hash(map()) :: String.t()
  def context_initial_hash(plan) when is_map(plan) do
    case plan["context_initial"] do
      nil -> sha256("")
      context -> sha256(Jason.encode!(context))
    end
  end

  defp sha256(contents), do: "sha256:" <> (:sha256 |> :crypto.hash(contents) |> Base.encode16(case: :lower))

  @spec validate(map(), map()) :: {:ok, map()} | {:error, rejection()}
  def validate(plan, spec) when is_map(plan) and is_map(spec) do
    with {:ok, validated_spec} <- RunSpec.validate(spec),
         :ok <- validate_schema_version(plan),
         :ok <- validate_effort_hints(plan),
         :ok <- validate_timeouts(plan),
         :ok <- validate_duplicate_ids(plan),
         :ok <- validate_missing_deps(plan),
         :ok <- validate_agent_roles(plan, validated_spec),
         :ok <- validate_allowed_paths(plan, validated_spec),
         :ok <- validate_dag(plan),
         :ok <- validate_stretch_overlap(plan, validated_spec),
         {:ok, parsed} <- Zoi.parse(schema(), plan) do
      {:ok, parsed}
    else
      {:error, %{} = rejection} -> {:error, rejection}
      {:error, _errors} -> {:error, %{clause: "invalid_run_plan_shape"}}
    end
  end

  def validate(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp schema do
    Zoi.map(
      %{
        "schema" => Zoi.literal("ai-orchestrator/run-plan"),
        "schema_version" => Zoi.literal(1),
        "plan_id" => Zoi.string(),
        "work_items" => Zoi.array(work_item_schema()),
        "context_initial" => Zoi.optional(Zoi.map())
      },
      unrecognized_keys: :error
    )
  end

  defp work_item_schema do
    Zoi.map(
      %{
        "id" => Zoi.string(),
        "title" => Zoi.string(),
        "role" => Zoi.string(),
        "deps" => Zoi.array(Zoi.string()),
        "allowed_paths" => Zoi.array(Zoi.string()),
        "acceptance" => Zoi.array(Zoi.string()),
        "expected_artifacts" => Zoi.array(Zoi.string()),
        "max_attempts" => Zoi.optional(Zoi.integer()),
        "timeout_s" => Zoi.optional(Zoi.integer()),
        "effort_hint" => Zoi.optional(Zoi.enum(@effort_hints)),
        "kind" => Zoi.enum(["implement", "review", "integration"])
      },
      unrecognized_keys: :error
    )
  end

  defp validate_schema_version(%{"schema_version" => 1}), do: :ok
  defp validate_schema_version(%{"schema_version" => _version}), do: {:error, %{clause: "unsupported_schema_version"}}
  defp validate_schema_version(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_effort_hints(%{"work_items" => work_items}) when is_list(work_items) do
    case Enum.find_value(work_items, &invalid_effort_hint/1) do
      nil -> :ok
      value -> {:error, %{clause: "invalid_effort_hint", field: value}}
    end
  end

  defp validate_effort_hints(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp invalid_effort_hint(%{"effort_hint" => value}) when value not in @effort_hints, do: value
  defp invalid_effort_hint(_work_item), do: nil

  defp validate_timeouts(%{"work_items" => work_items}) when is_list(work_items) do
    case Enum.find(work_items, &nonpositive_timeout?/1) do
      nil -> :ok
      %{"id" => id} -> {:error, %{clause: "nonpositive_timeout", field: id}}
    end
  end

  defp validate_timeouts(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp nonpositive_timeout?(%{"timeout_s" => timeout_s}), do: not (is_integer(timeout_s) and timeout_s > 0)
  defp nonpositive_timeout?(_work_item), do: false

  defp validate_duplicate_ids(%{"work_items" => work_items}) when is_list(work_items) do
    work_items
    |> Enum.map(& &1["id"])
    |> duplicate()
    |> case do
      nil -> :ok
      id -> {:error, %{clause: "duplicate_work_item_id", field: id}}
    end
  end

  defp validate_duplicate_ids(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_missing_deps(%{"work_items" => work_items}) when is_list(work_items) do
    ids = MapSet.new(work_items, & &1["id"])

    case Enum.find_value(work_items, &missing_dep(&1, ids)) do
      nil -> :ok
      dep -> {:error, %{clause: "missing_dependency", field: dep}}
    end
  end

  defp validate_missing_deps(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_agent_roles(%{"work_items" => work_items}, %{"agents" => agents}) do
    roles = MapSet.new(agents, & &1["role"])

    case Enum.find(work_items, fn item -> not MapSet.member?(roles, item["role"]) end) do
      nil -> :ok
      %{"role" => role} -> {:error, %{clause: "undeclared_agent_role", field: role}}
    end
  end

  defp validate_agent_roles(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_allowed_paths(%{"work_items" => work_items}, %{"allowed_roots" => allowed_roots}) do
    case Enum.find_value(work_items, &invalid_mutating_path(&1, allowed_roots)) do
      nil -> :ok
      path -> {:error, %{clause: "work_item_paths_outside_roots", field: path}}
    end
  end

  defp validate_allowed_paths(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_dag(%{"work_items" => work_items}) do
    graph = Map.new(work_items, fn item -> {item["id"], item["deps"] || []} end)

    if cycle?(graph) do
      {:error, %{clause: "dag_cycle"}}
    else
      :ok
    end
  end

  defp validate_dag(_plan), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp validate_stretch_overlap(%{"work_items" => work_items}, %{"stretch_worktrees" => true}) do
    graph = Map.new(work_items, fn item -> {item["id"], item["deps"] || []} end)
    writers = Enum.filter(work_items, &writer?/1)

    if overlapping_concurrent_writers?(writers, graph) do
      {:error, %{clause: "stretch_paths_overlap"}}
    else
      :ok
    end
  end

  defp validate_stretch_overlap(_plan, _spec), do: :ok

  defp duplicate(values) do
    values
    |> Enum.reduce_while(MapSet.new(), fn value, seen ->
      if MapSet.member?(seen, value) do
        {:halt, value}
      else
        {:cont, MapSet.put(seen, value)}
      end
    end)
    |> case do
      %MapSet{} -> nil
      duplicate -> duplicate
    end
  end

  defp missing_dep(%{"deps" => deps}, ids) when is_list(deps) do
    Enum.find(deps, &(not MapSet.member?(ids, &1)))
  end

  defp missing_dep(_work_item, _ids), do: nil

  defp invalid_mutating_path(%{"kind" => kind, "allowed_paths" => paths}, allowed_roots)
       when kind in ["implement", "integration"] do
    Enum.find(paths, &(not path_under_any_root?(&1, allowed_roots)))
  end

  defp invalid_mutating_path(_work_item, _allowed_roots), do: nil

  @type graph :: %{String.t() => [String.t()]}

  @spec cycle?(graph()) :: boolean()
  defp cycle?(graph) do
    graph
    |> Map.keys()
    |> Enum.any?(&cycle_from?(&1, graph, %{}))
  end

  @spec cycle_from?(String.t(), graph(), %{String.t() => true}) :: boolean()
  defp cycle_from?(id, graph, path) do
    if Map.has_key?(path, id) do
      true
    else
      Enum.any?(Map.get(graph, id, []), &cycle_from?(&1, graph, Map.put(path, id, true)))
    end
  end

  defp writer?(%{"kind" => kind}), do: MapSet.member?(@writer_kinds, kind)

  defp overlapping_concurrent_writers?(writers, graph) do
    writers
    |> pairs()
    |> Enum.any?(fn {left, right} ->
      concurrent?(left, right, graph) and paths_overlap?(left["allowed_paths"] || [], right["allowed_paths"] || [])
    end)
  end

  defp pairs(items) do
    for {left, left_idx} <- Enum.with_index(items),
        {right, right_idx} <- Enum.with_index(items),
        left_idx < right_idx,
        do: {left, right}
  end

  @spec concurrent?(map(), map(), graph()) :: boolean()
  defp concurrent?(left, right, graph) do
    not reachable?(left["id"], right["id"], graph) and not reachable?(right["id"], left["id"], graph)
  end

  @spec reachable?(String.t(), String.t(), graph()) :: boolean()
  defp reachable?(from, to, graph), do: reachable?(from, to, graph, %{})

  @spec reachable?(String.t(), String.t(), graph(), %{String.t() => true}) :: boolean()
  defp reachable?(from, to, graph, visited) do
    deps = Map.get(graph, from, [])

    cond do
      to in deps -> true
      Map.has_key?(visited, from) -> false
      true -> Enum.any?(deps, &reachable?(&1, to, graph, Map.put(visited, from, true)))
    end
  end

  defp paths_overlap?(left_paths, right_paths) do
    Enum.any?(left_paths, fn left ->
      Enum.any?(right_paths, &path_overlap?(left, &1))
    end)
  end

  defp path_overlap?(left, right) do
    left = normalize_path(left)
    right = normalize_path(right)

    left == right or String.starts_with?(left, right <> "/") or String.starts_with?(right, left <> "/")
  end

  defp normalize_path(path), do: path |> String.trim() |> String.trim_trailing("/")

  defp path_under_any_root?(path, roots) do
    Enum.any?(roots, &path_under_root?(path, &1))
  end

  defp path_under_root?(path, root) do
    path = normalize_path(path)
    root = normalize_path(root)

    path == root or String.starts_with?(path, root <> "/")
  end
end
