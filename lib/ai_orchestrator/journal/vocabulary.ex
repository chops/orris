defmodule AiOrchestrator.Journal.Vocabulary do
  @moduledoc """
  Inventory of the journal event vocabulary at the Wave 1 baseline (ledger NS-40).

  Every type declared in `AiOrchestrator.Journal.Event` is either `:produced` by a
  runtime emit site or `:reserved` with the wave that gives it a producer. Reserved
  means not appendable by current production code; historical fixtures that contain a
  reserved type stay fold-legal. `test/contracts/event_vocabulary_test.exs` checks this
  table against `lib/` sources and fixture journals, so it cannot drift silently.

  Producers are named as strings, never as module references, so this module stays
  inside the `Journal` boundary without depending on `Lifecycle`.
  """

  @type status :: :produced | :reserved
  @type fold :: :clause | :generic
  @type target_wave :: :w4 | :w6 | :w7a | :w9
  @type entry :: %{
          status: status(),
          producer: String.t() | nil,
          source: String.t() | nil,
          fold: fold(),
          target_wave: target_wave() | nil,
          family: String.t()
        }

  @run_fsm "AiOrchestrator.Lifecycle.RunFSM"
  @run_fsm_source "ai_orchestrator/lifecycle/core/reducer.ex"

  @produced ~w(
    agent_wedge_detected
    artifact_observed
    assignment_completed
    assignment_dispatch_sent
    assignment_observation_started
    assignment_prompt_projected
    assignment_requested
    gate_failed
    gate_passed
    gate_requested
    gate_started
    human_attention_required
    pane_lease_acquired
    pane_lease_release_requested
    pane_lease_released
    pane_lease_requested
    plan_recorded
    review_disposition_recorded
    review_received
    review_requested
    run_cancel_requested
    run_cancelled
    run_completed
    run_created
    run_resumed
    run_spec_loaded
    run_started
    work_item_completed
    work_item_retry_scheduled
    workspace_lease_acquired
    workspace_lease_release_requested
    workspace_lease_released
    workspace_lease_requested
  )

  # type => closed north-star wave (see wave_label/1) that lands its producer, typed payload,
  # fold rule, and fixtures.
  @reserved %{
    "assignment_failed" => :w4,
    "context_conflict_detected" => :w9,
    "context_patch_accepted" => :w9,
    "context_patch_proposed" => :w9,
    "context_patch_rejected" => :w9,
    "contract_change_proposed" => :w9,
    "contract_change_ratified" => :w9,
    "contract_change_rejected" => :w9,
    "notification_failed" => :w7a,
    "notification_requested" => :w7a,
    "notification_sent" => :w7a,
    "pane_lease_failed" => :w4,
    "run_budget_exhausted" => :w4,
    "run_failed" => :w4,
    "run_pause_requested" => :w4,
    "run_paused" => :w4,
    "run_recovery_reserved" => :w4,
    "scheduler_stalled" => :w6,
    "stop_policy_evaluated" => :w4,
    "work_item_failed" => :w4,
    "workspace_lease_failed" => :w4
  }

  # Types the fold handles through generic clauses rather than a named literal
  # (checked against fold.ex by the contract test).
  @generic_fold ~w(
    agent_wedge_detected
    context_conflict_detected
    context_patch_proposed
    context_patch_rejected
    contract_change_rejected
    pane_lease_failed
    run_cancel_requested
    run_pause_requested
    run_paused
    scheduler_stalled
    stop_policy_evaluated
    work_item_retry_scheduled
    workspace_lease_failed
  )

  @families %{
    "agent_wedge_detected" => "escalation",
    "artifact_observed" => "assignment",
    "assignment_completed" => "assignment",
    "assignment_dispatch_sent" => "assignment",
    "assignment_failed" => "assignment",
    "assignment_observation_started" => "assignment",
    "assignment_prompt_projected" => "assignment",
    "assignment_requested" => "assignment",
    "context_conflict_detected" => "context",
    "context_patch_accepted" => "context",
    "context_patch_proposed" => "context",
    "context_patch_rejected" => "context",
    "contract_change_proposed" => "contract",
    "contract_change_ratified" => "contract",
    "contract_change_rejected" => "contract",
    "gate_failed" => "gate",
    "gate_passed" => "gate",
    "gate_requested" => "gate",
    "gate_started" => "gate",
    "human_attention_required" => "escalation",
    "notification_failed" => "notification",
    "notification_requested" => "notification",
    "notification_sent" => "notification",
    "pane_lease_acquired" => "lease",
    "pane_lease_failed" => "lease",
    "pane_lease_release_requested" => "lease",
    "pane_lease_released" => "lease",
    "pane_lease_requested" => "lease",
    "plan_recorded" => "run",
    "review_disposition_recorded" => "review",
    "review_received" => "review",
    "review_requested" => "review",
    "run_budget_exhausted" => "run",
    "run_cancel_requested" => "run",
    "run_cancelled" => "run",
    "run_completed" => "run",
    "run_created" => "run",
    "run_failed" => "run",
    "run_pause_requested" => "run",
    "run_paused" => "run",
    "run_recovery_reserved" => "run",
    "run_resumed" => "run",
    "run_spec_loaded" => "run",
    "run_started" => "run",
    "scheduler_stalled" => "scheduling",
    "stop_policy_evaluated" => "escalation",
    "work_item_completed" => "work_item",
    "work_item_failed" => "work_item",
    "work_item_retry_scheduled" => "work_item",
    "workspace_lease_acquired" => "lease",
    "workspace_lease_failed" => "lease",
    "workspace_lease_release_requested" => "lease",
    "workspace_lease_released" => "lease",
    "workspace_lease_requested" => "lease"
  }

  @entries Map.new(
             Enum.map(@produced, fn type ->
               {type,
                %{
                  status: :produced,
                  producer: @run_fsm,
                  source: @run_fsm_source,
                  fold: if(type in @generic_fold, do: :generic, else: :clause),
                  target_wave: nil,
                  family: Map.fetch!(@families, type)
                }}
             end) ++
               Enum.map(@reserved, fn {type, wave} ->
                 {type,
                  %{
                    status: :reserved,
                    producer: nil,
                    source: nil,
                    fold: if(type in @generic_fold, do: :generic, else: :clause),
                    target_wave: wave,
                    family: Map.fetch!(@families, type)
                  }}
               end)
           )

  @spec entries() :: %{String.t() => entry()}
  def entries, do: @entries

  @doc "Human label for a closed wave atom, as the north star writes it."
  @spec wave_label(target_wave()) :: String.t()
  def wave_label(:w4), do: "4"
  def wave_label(:w6), do: "6"
  def wave_label(:w7a), do: "7a"
  def wave_label(:w9), do: "9"

  @spec produced_types() :: MapSet.t(String.t())
  def produced_types, do: MapSet.new(@produced)

  @spec reserved_types() :: MapSet.t(String.t())
  def reserved_types, do: @reserved |> Map.keys() |> MapSet.new()
end
