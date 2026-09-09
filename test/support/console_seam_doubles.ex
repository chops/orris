defmodule AiOrchestrator.Test.ConsoleSeamDoubles do
  @moduledoc """
  TEST DOUBLES for docs/contracts/public-console-seam.org (test support only, never product code).

  `FakePaneClient` and the seam options mirror test/cli/cli_test.exs so the public seam is measured against the same
  doubles as the CLI. The `Disposable*` modules are DISPOSABLE WITNESSES: deliberately wrong implementations of
  `Query.host_view/2` (fixed zero; unconditional subtract-one; unfiltered by root) used ONLY to prove that the
  counting rows reject them. They are not stubs of the product and must never be promoted.
  """

  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.Host
  alias AiOrchestrator.Journal.Fold
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.ConsoleSeamDoubles
  alias AiOrchestrator.Test.FixedClock
  alias AiOrchestrator.Test.FixedId
  alias AiOrchestrator.Test.GateDouble

  defmodule FakePaneClient do
    @moduledoc false
    def reconcile(pane_ref, message_id, _opts),
      do:
        {:ok,
         %{"ok" => true, "protocol_version" => 2, "outcome" => "absent", "msg_id" => message_id, "pane_id" => pane_ref}}

    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}

    def send(pane_ref, _prompt, opts),
      do:
        {:ok,
         %{
           "ok" => true,
           "protocol_version" => 2,
           "status" => "sent",
           "msg_id" => opts[:message_id],
           "pane_id" => pane_ref
         }}

    def status(pane_ref, _opts), do: {:ok, %{"state" => "idle", "pane_ref" => pane_ref, "pending_count" => 0}}
  end

  @doc "The CLI test's seam options with deterministic identity and clock; `registry_root` is the pane registry root."
  @spec seams(Path.t(), pid()) :: keyword()
  def seams(registry_root, test_pid) do
    events = Enum.map(F.lines("scenarios", "gated_run_seed"), &Jason.decode!/1)

    by_assignment =
      events |> Enum.filter(&(&1["type"] == "artifact_observed")) |> Map.new(&{&1["data"]["assignment_id"], &1["data"]})

    gate_pass = events |> Enum.find(&(&1["type"] == "gate_passed")) |> Map.fetch!("data") |> Map.delete("gate_run_id")

    [
      id: FixedId,
      clock: FixedClock,
      pane_registry_root: registry_root,
      dispatch: AiOrchestrator.Dispatch.LocalPane,
      dispatch_opts: [
        artifact_reader: fn command -> {:ok, Map.fetch!(by_assignment, command["assignment_id"])} end,
        pane_client: FakePaneClient,
        test_pid: test_pid
      ],
      gate_executor: GateDouble,
      # ---- disposable witnesses for the counting rows (F-8 / C-8) and the budget row (F-7 / C-9) ----
      # Each derives the run id from the resolved directory's journal (as the product must); none takes a witness
      # option.
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _opts -> {:ok, gate_pass} end],
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]
  end

  # a refusing pane registry: `claim` returns the given reason; `release` is never reached
  defmodule RefusingRegistry do
    @moduledoc false
    def pane_refs(spec), do: FileRegistry.pane_refs(spec)
    def claim(_pane_refs, _owner, _opts), do: {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_writer"}}
    def release(_claim), do: :ok
  end

  defp raw(run_id, opts) do
    case Host.lookup_run_id(run_id, Keyword.take(opts, [:monitor, :timeout])) do
      {:ok, entries} -> entries
      _ -> :unknown
    end
  end

  defp own_and_root(run_ref, opts) do
    root = Path.expand(Keyword.fetch!(opts, :root))
    {Path.join(root, run_ref), root}
  end

  defp journal_run_id(dir) do
    {:ok, state} = dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Fold.fold_lines()
    Fold.summary(state)["run_id"]
  end

  defp under_root?(dir, root), do: String.starts_with?(Path.expand(dir) <> "/", root <> "/")

  defp unknown_view,
    do: {:ok, %{other_registered_directories: :unknown, errors: [%{leg: :lookup, clause: "lookup_unavailable"}]}}

  defmodule DisposableFaithful do
    @moduledoc "DISPOSABLE WITNESS: the correct count (root-filtered, own directory excluded explicitly). Must pass every counting step."
    def host_view(run_ref, opts), do: ConsoleSeamDoubles.__count__(run_ref, opts, :faithful)
  end

  defmodule DisposableFixedZero do
    @moduledoc "DISPOSABLE WITNESS: always zero, never unknown."
    def host_view(_run_ref, _opts), do: {:ok, %{other_registered_directories: 0, errors: []}}
  end

  defmodule DisposableSubtractOnly do
    @moduledoc "DISPOSABLE WITNESS: root-filtered correctly, then subtracts one UNCONDITIONALLY (wrong when the own entry is absent)."
    def host_view(run_ref, opts), do: ConsoleSeamDoubles.__count__(run_ref, opts, :subtract_only)
  end

  defmodule DisposableUnfiltered do
    @moduledoc "DISPOSABLE WITNESS: excludes the own directory but does NOT filter by root."
    def host_view(run_ref, opts), do: ConsoleSeamDoubles.__count__(run_ref, opts, :unfiltered)
  end

  defmodule DisposablePerLegReset do
    @moduledoc "DISPOSABLE WITNESS for the budget row: gives EVERY host leg the full budget instead of the remaining time."
    def host_view(run_ref, opts) do
      budget = Keyword.get(opts, :budget_ms, 1_000)
      mon = Keyword.get(opts, :monitor)
      {own, _root} = ConsoleSeamDoubles.__own_and_root__(run_ref, opts)
      run_id = ConsoleSeamDoubles.__journal_run_id__(own)

      legs = [
        {:status, fn -> Host.status(own, monitor: mon, timeout: budget) end},
        {:lookup, fn -> Host.lookup_run_id(run_id, monitor: mon, timeout: budget) end}
      ]

      errors = for {leg, call} <- legs, match?({:error, _}, call.()), do: %{leg: leg, clause: "leg_timeout"}
      {:ok, %{run_id: run_id, registered: :unknown, other_registered_directories: :unknown, errors: errors}}
    end
  end

  @doc false
  def __own_and_root__(run_ref, opts), do: own_and_root(run_ref, opts)
  @doc false
  def __journal_run_id__(dir), do: journal_run_id(dir)

  @doc false
  def __count__(run_ref, opts, mode) do
    {own, root} = own_and_root(run_ref, opts)
    run_id = journal_run_id(own)

    case raw(run_id, opts) do
      :unknown ->
        unknown_view()

      entries ->
        count =
          case mode do
            :faithful -> Enum.count(entries, &(under_root?(&1.run_dir, root) and Path.expand(&1.run_dir) != own))
            :subtract_only -> max(Enum.count(entries, &under_root?(&1.run_dir, root)) - 1, 0)
            :unfiltered -> Enum.count(entries, &(Path.expand(&1.run_dir) != own))
          end

        {:ok, %{other_registered_directories: count, errors: []}}
    end
  end
end
