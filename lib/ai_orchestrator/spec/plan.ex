defmodule AiOrchestrator.Spec.Plan do
  @moduledoc false

  alias AiOrchestrator.Spec.PathBoundary
  alias AiOrchestrator.Spec.RunSpec

  @type rejection :: %{required(:clause) => String.t(), optional(atom()) => term()}

  @effort_hints ["low", "medium", "high", "xhigh"]
  @writer_kinds MapSet.new(["implement", "integration"])
  # the repository itself, as the one root a path is judged against when only its FORM is at issue
  @repository_root ["."]

  @doc """
  The allowed paths of the writer kinds (implement, integration): the paths the containment rule
  judges (NS-20.D.001). Non-list and non-string entries are left to the shape check.
  """
  @spec writer_allowed_paths(map()) :: [String.t()]
  def writer_allowed_paths(%{"work_items" => work_items}) when is_list(work_items) do
    for %{"kind" => kind, "allowed_paths" => paths} <- work_items,
        MapSet.member?(@writer_kinds, kind),
        is_list(paths),
        path <- paths,
        is_binary(path),
        do: path
  end

  def writer_allowed_paths(_plan), do: []

  @doc "A `PathBoundary` rejection in the plan's rejection vocabulary; the field is the offending path."
  @spec path_rejection(PathBoundary.rejection()) :: rejection()
  def path_rejection(%{clause: "path_outside_roots", path: path}),
    do: %{clause: "work_item_paths_outside_roots", field: path}

  def path_rejection(%{clause: clause, path: path}), do: %{clause: "work_item_" <> clause, field: path}

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
         :ok <- validate_expected_artifacts(plan),
         :ok <- validate_expected_artifact_paths(plan, validated_spec),
         :ok <- validate_acceptance_gates(plan, validated_spec),
         :ok <- validate_agent_roles(plan, validated_spec),
         :ok <- validate_reviewer_independence(plan, validated_spec),
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

  defp validate_expected_artifacts(%{"work_items" => work_items}) do
    case Enum.find(work_items, &invalid_expected_artifacts?/1) do
      nil -> :ok
      %{"id" => id} -> {:error, %{clause: "expected_artifact_cardinality", field: id}}
    end
  end

  defp invalid_expected_artifacts?(%{"expected_artifacts" => [artifact]}) when is_binary(artifact) and artifact != "",
    do: false

  # The reducer observes one artifact. Leave malformed field types to the existing schema check.
  defp invalid_expected_artifacts?(%{"id" => id, "expected_artifacts" => artifacts})
       when is_binary(id) and is_list(artifacts), do: Enum.all?(artifacts, &is_binary/1)

  defp invalid_expected_artifacts?(_work_item), do: false

  # NS-20.D.001, the declared-artifact half: the artifact the host joins to `repo_root` and reads as
  # the assignment's evidence is judged by the same pure layer as `allowed_paths`. EVERY kind is
  # judged by FORM against the repository itself -- an absolute artifact, or one carrying `..`, is
  # refused whoever declares it. A WRITER kind is additionally required to place its artifact under
  # one of the spec's allowed roots, because a writer's evidence is a file it is allowed to write.
  # A declared review item names its own review document, which need not lie under a writer's root,
  # so the roots rule does not apply to it. Malformed entries are left to the existing shape check.
  defp validate_expected_artifact_paths(%{"work_items" => work_items} = plan, %{"allowed_roots" => allowed_roots})
       when is_list(work_items) and is_list(allowed_roots) do
    with :ok <- judged(@repository_root, expected_artifact_paths(plan, fn _kind -> true end)) do
      judged(allowed_roots, expected_artifact_paths(plan, &MapSet.member?(@writer_kinds, &1)))
    end
  end

  defp validate_expected_artifact_paths(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  defp judged(roots, paths) do
    case PathBoundary.lexical(roots, paths) do
      :ok -> :ok
      {:error, rejection} -> {:error, path_rejection(rejection)}
    end
  end

  defp expected_artifact_paths(%{"work_items" => work_items}, kind?) do
    for %{"kind" => kind, "expected_artifacts" => artifacts} <- work_items,
        kind?.(kind),
        is_list(artifacts),
        artifact <- artifacts,
        is_binary(artifact),
        do: artifact
  end

  defp validate_acceptance_gates(%{"work_items" => work_items}, %{"gates" => gates}) when is_map(gates) do
    case Enum.find_value(work_items, &invalid_acceptance(&1, gates)) do
      nil -> :ok
      rejection -> {:error, rejection}
    end
  end

  defp validate_acceptance_gates(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  # The reducer runs exactly one gate, the first acceptance entry looked up in the spec's gates; any
  # further entry would be accepted but inert. Leave malformed field types to the existing schema check.
  defp invalid_acceptance(%{"id" => id, "acceptance" => [gate_id]}, gates) when is_binary(id) and is_binary(gate_id) do
    if Map.has_key?(gates, gate_id), do: nil, else: %{clause: "unknown_acceptance_gate", field: gate_id}
  end

  defp invalid_acceptance(%{"id" => id, "acceptance" => entries}, _gates) when is_binary(id) and is_list(entries) do
    if Enum.all?(entries, &is_binary/1), do: %{clause: "acceptance_gate_cardinality", field: id}
  end

  defp invalid_acceptance(_work_item, _gates), do: nil

  defp validate_agent_roles(%{"work_items" => work_items}, %{"agents" => agents}) do
    roles = MapSet.new(agents, & &1["role"])

    case Enum.find(work_items, fn item -> not MapSet.member?(roles, item["role"]) end) do
      nil -> :ok
      %{"role" => role} -> {:error, %{clause: "undeclared_agent_role", field: role}}
    end
  end

  defp validate_agent_roles(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  # NS-25.D.001, the provable half. The register's control is "review by the same agent
  # implementation AND pinned model as the writer without a recorded operator exception fails".
  # The spec records neither an implementation nor a pinned model (`run_environment` has no
  # producer), so the ONE identity fact a plan and a spec can decide at admission is the DECLARED
  # agent: a writer work item whose role resolves to the same agent as the `reviewer` role would
  # have its work reviewed by itself. Two declarations are the same agent when they carry the same
  # `name`, or the same non-nil `agent_id` under two names. A spec with no reviewer role is
  # unaffected -- no review is requested for it at all.
  #
  # This refuses a PROVABLE violation. It never establishes independence: two differently named
  # agents may still be one CLI process running one pinned model, which nothing in the spec or the
  # journal records today. The positive half is a separate slice.
  defp validate_reviewer_independence(%{"work_items" => work_items}, %{"agents" => agents})
       when is_list(work_items) and is_list(agents) do
    case agent_for_role(agents, "reviewer") do
      nil -> :ok
      reviewer -> dependent_writer_agent(work_items, agents, reviewer)
    end
  end

  defp validate_reviewer_independence(_plan, _spec), do: {:error, %{clause: "invalid_run_plan_shape"}}

  # the field is the writer item's own agent name: the agent that would review its own work
  defp dependent_writer_agent(work_items, agents, reviewer) do
    work_items
    |> Enum.filter(&writer?/1)
    |> Enum.find_value(&same_agent(agent_for_role(agents, &1["role"]), reviewer))
    |> case do
      nil -> :ok
      name -> {:error, %{clause: "reviewer_agent_not_independent", field: name}}
    end
  end

  defp agent_for_role(agents, role), do: Enum.find(agents, &(&1["role"] == role))

  defp same_agent(%{"name" => name}, %{"name" => name}), do: name

  defp same_agent(%{"name" => name, "agent_id" => agent_id}, %{"agent_id" => agent_id}) when is_binary(agent_id), do: name

  defp same_agent(_writer, _reviewer), do: nil

  # the pure layer of the containment rule: absolute and `..` paths are refused by form, the rest by
  # expanded containment under the spec's allowed roots (the physical layer runs at trusted admission)
  defp validate_allowed_paths(%{"work_items" => work_items} = plan, %{"allowed_roots" => allowed_roots})
       when is_list(work_items) do
    case PathBoundary.lexical(allowed_roots, writer_allowed_paths(plan)) do
      :ok -> :ok
      {:error, rejection} -> {:error, path_rejection(rejection)}
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
  defp writer?(_work_item), do: false

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

  # judged through the containment rule's own expansion, so the spellings `validate_allowed_paths`
  # admits as one directory (`./lib`, `lib/`, `lib//x/./y`) are one directory here too
  defp path_overlap?(left, right), do: PathBoundary.overlapping?(String.trim(left), String.trim(right))
end
