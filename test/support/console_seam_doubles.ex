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
      gate_helper: GateDouble.helper(),
      gate_opts: [runner: fn _gate, _opts -> {:ok, gate_pass} end],
      # ---- disposable witnesses for the counting rows (F-8 / C-8): wrong on purpose ----
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

  defmodule DisposableFixedZero do
    @moduledoc false
    def host_view(_run_ref, _opts), do: {:ok, %{other_registered_directories: 0, errors: []}}
  end

  defmodule DisposableUnconditionalSubtract do
    @moduledoc false
    def host_view(run_ref, opts) do
      run_id = Keyword.fetch!(opts, :witness_run_id)
      _ = run_ref

      case ConsoleSeamDoubles.__raw__(run_id, opts) do
        :unknown ->
          {:ok, %{other_registered_directories: :unknown, errors: [%{leg: :lookup, clause: "lookup_unavailable"}]}}

        entries ->
          {:ok, %{other_registered_directories: max(length(entries) - 1, 0), errors: []}}
      end
    end
  end

  defmodule DisposableUnfiltered do
    @moduledoc false
    def host_view(run_ref, opts) do
      run_id = Keyword.fetch!(opts, :witness_run_id)
      own = Path.expand(Path.join(Keyword.fetch!(opts, :root), run_ref))

      case ConsoleSeamDoubles.__raw__(run_id, opts) do
        :unknown ->
          {:ok, %{other_registered_directories: :unknown, errors: [%{leg: :lookup, clause: "lookup_unavailable"}]}}

        entries ->
          {:ok, %{other_registered_directories: Enum.count(entries, &(&1.run_dir != own)), errors: []}}
      end
    end
  end

  @doc false
  def __raw__(run_id, opts), do: raw(run_id, opts)
end
