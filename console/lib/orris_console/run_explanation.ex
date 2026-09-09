defmodule OrrisConsole.RunExplanation do
  @moduledoc "Presentation guidance over the existing public projections; no execution or additional journal reads."
  use Phoenix.Component

  @steps [
    %{
      ref: "before-dispatch",
      seq: 9,
      title: "1. Ready to send work",
      recorded:
        "An assignment and its pane and workspace leases have been recorded. Dispatch has not yet been recorded in this sample.",
      next: "Look for dispatch and observation records next.",
      why: "This checkpoint helps distinguish prepared work from work that has actually been sent."
    },
    %{
      ref: "awaiting-artifact",
      seq: 12,
      title: "2. Waiting for an artifact",
      recorded:
        "The prompt was prepared, dispatch was recorded, and observation began. This sample has no artifact observation yet.",
      next: "An artifact is the agent's recorded output. Its observation lets the workflow move on to review.",
      why: "This checkpoint helps explain why sending an assignment is not enough to mark the work complete."
    },
    %{
      ref: "before-gate",
      seq: 27,
      title: "3. Waiting for a gate result",
      recorded:
        "An artifact and review disposition have been recorded. The gate has already started in this sample, despite the directory's name.",
      next: "Look for the gate's result before treating the work as accepted.",
      why:
        "A gate is an acceptance check. An agent's output and a review do not by themselves establish that the check passed."
    }
  ]

  def ordered(runs) do
    if demo_runs?(runs),
      do: Enum.sort_by(runs, & &1.last_seq),
      else: Enum.sort_by(runs, &{priority(&1), &1.run_ref})
  end

  def demo_runs?(runs) do
    length(runs) == 3 and
      Enum.all?(@steps, fn step ->
        Enum.any?(
          runs,
          &(&1.run_ref == step.ref and &1.run_id == "run_scenario_0001" and &1.last_seq == step.seq and is_nil(&1.error))
        )
      end)
  end

  defp priority(%{error: error}) when not is_nil(error), do: 0
  defp priority(%{status: status}) when status in ["blocked", "failed", "budget_exhausted"], do: 0
  defp priority(%{status: status}) when status in ["completed", "cancelled"], do: 2
  defp priority(_), do: 1

  attr :summary, :map, required: true
  attr :root_id, :string, required: true
  attr :run_ref, :string, required: true

  def overview(assigns) do
    step = Enum.find(@steps, &(&1.ref == assigns.run_ref and &1.seq == assigns.summary.last_seq))
    step = if assigns.summary.run_id == "run_scenario_0001", do: step
    assigns = assign(assigns, step: step, guidance: guidance(assigns.summary))

    ~H"""
    <section class="run-overview" aria-labelledby="run-overview-title">
      <p class="eyebrow">Reading this run</p>
      <h2 id="run-overview-title">{@guidance.title}</h2>
      <p>{@guidance.meaning}</p>
      <p><strong>What to inspect next:</strong> {@guidance.next}</p>
      <p class="status">This page reads saved journal records and checks the local host separately. It does not start or resume work.</p>
    </section>
    <section :if={@step} class="demo-guide" aria-labelledby="demo-guide-title">
      <p class="eyebrow">Supplied demo · checkpoint reference</p>
      <h2 id="demo-guide-title">{@step.title}</h2>
      <p>These examples are separate saved checkpoints of the same run. The explanation below describes the supplied fixture; current observations appear in the recorded facts and projections.</p>
      <p><strong>What this example contains:</strong> {@step.recorded}</p>
      <p><strong>How it is used:</strong> {@step.next}</p>
      <p><strong>Why it matters:</strong> {@step.why}</p>
      <ol class="checkpoint-nav">
        <li :for={step <- steps()}>
          <strong :if={step.ref == @run_ref} aria-current="step">{step.title}</strong>
          <a :if={step.ref != @run_ref} href={"/runs/#{@root_id}/#{step.ref}"}>{step.title}</a>
          <span class="status"> — {step.ref}, event {step.seq}</span>
        </li>
      </ol>
      <p class="status">The sequence above is the demo's learning order. Directory names are labels; journal records determine the recorded state.</p>
    </section>
    """
  end

  def steps, do: @steps

  def guidance(%{pending_repair: repair}) when not is_nil(repair),
    do: %{
      title: "The journal needs attention",
      meaning: "The page shows the verified part of the journal. An incomplete tail remains beyond it.",
      next:
        "Read the repair notice before relying on the final state. Viewing this page does not repair or change the journal."
    }

  def guidance(%{status: "blocked"}),
    do: %{
      title: "A recorded issue needs attention",
      meaning: "The journal records that this run is blocked.",
      next: "Use the attention identifiers in the summary and context to locate the unresolved decision or problem."
    }

  def guidance(%{status: "in_flight"}),
    do: %{
      title: "Work is recorded as in progress",
      meaning:
        "The journal has not recorded a terminal outcome. This status alone does not tell you whether an agent is currently running.",
      next:
        "Check open assignments, remaining work items and any attention entries below. Then compare them with the local host observation."
    }

  def guidance(%{status: "completed"}),
    do: %{
      title: "Completion has been recorded",
      meaning: "The journal records a completed run.",
      next:
        "Use the work-item summary to check what completed, and the context to understand the recorded state supplied to assignments."
    }

  def guidance(%{status: "cancelled"}),
    do: %{
      title: "Cancellation has been recorded",
      meaning: "The journal records a cancelled run. Earlier recorded work remains part of its history.",
      next: "Inspect the summary and context to see what was complete or still open when the run ended."
    }

  def guidance(%{status: status}) when status in ["failed", "budget_exhausted"],
    do: %{
      title: "The run ended without completion",
      meaning: "The journal records a failure or an exhausted budget.",
      next:
        "Read the summary for the recorded reason and the context for unfinished work before deciding how to recover."
    }

  def guidance(_),
    do: %{
      title: "Inspect the recorded state",
      meaning: "The status below is the journal's recorded state; this view does not infer a later outcome.",
      next: "Read the summary for work progress and the context for assignments and attention entries."
    }
end
