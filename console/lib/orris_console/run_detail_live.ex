defmodule OrrisConsole.RunDetailLive do
  @moduledoc """
  One run: summary, host facts (false/unknown preserved), pending repair and escaped context text. U1: the "Cancel
  run" form on an allowed NON-TERMINAL run (a POST to the intent route, CSRF by form) and the session's retained
  cancel outcome sentence for this run on every refresh (never an internal identity or a detail map).
  """
  use Phoenix.LiveView
  alias OrrisConsole.{Config, ReadModel, Reader, RunExplanation, SessionStore}

  @terminal ~w(cancelled completed failed budget_exhausted)

  @impl true
  def mount(%{"root_id" => root_id, "run_ref" => run_ref}, _session, socket) do
    socket = assign(socket, root_id: root_id, run_ref: run_ref, state: Reader.initial(socket.assigns.session_id))
    if connected?(socket), do: send(self(), :refresh)
    {:ok, socket}
  end

  defdelegate deliver(state, message), to: Reader

  @impl true
  def handle_info(:refresh, socket) do
    if socket.assigns.state.valid?.() do
      %{console_session: session, root_id: root_id, run_ref: run_ref} = socket.assigns

      read = fn ->
        config = Config.current()

        with {:ok, summary} <- ReadModel.summary(config, session, root_id, run_ref),
             {:ok, context} <- ReadModel.context(config, session, root_id, run_ref) do
          {:ok, %{summary: summary, context: context}}
        end
      end

      {:noreply, assign(socket, state: Reader.admit(socket.assigns.state, read))}
    else
      {:noreply, redirect(socket, to: "/login")}
    end
  end

  def handle_info({:query_result, _, _, _} = message, socket) do
    {state, _outcome} = Reader.receive_result(socket.assigns.state, message)

    if state.valid?.(),
      do: {:noreply, assign(socket, state: state)},
      else: {:noreply, redirect(socket, to: "/login")}
  end

  # the current controller died (its exact monitor): the read is lost, the view marks stale and retries; stale DOWNs are ignored
  def handle_info({:DOWN, ref, :process, _pid, _reason}, socket) do
    {state, _outcome} = Reader.controller_down(socket.assigns.state, ref)
    {:noreply, assign(socket, state: state)}
  end

  def handle_info(_other, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    assigns =
      assigns
      |> assign(detail: Reader.value(assigns.state))
      |> assign(outcome: outcome_line(assigns.session_id, assigns.root_id, assigns.run_ref))

    ~H"""
    <main data-c1-read={if @detail, do: "complete", else: "pending"} class="detail">
      <a href="/" class="back-link">← All runs</a>
      <h1>Run {@run_ref}</h1>
      <p :if={@state.delayed?} class="status">
        {if @state.stale?, do: "stale (refresh delayed)", else: "unavailable (refresh delayed)"}
        <span :if={@state.last_success_ms}> last successful read: {@state.last_success_ms}</span>
      </p>
      <p :if={@detail == nil and not @state.delayed?} class="status">loading</p>
      <p :if={@outcome} class="outcome">{@outcome}</p>
      <section :if={@detail}>
        <RunExplanation.overview summary={@detail.summary} root_id={@root_id} run_ref={@run_ref} />
        <h2>Recorded facts</h2>
        <dl class="run-facts">
          <div><dt>Run ID</dt><dd>{@detail.summary.run_id}</dd><dd class="fact-help">Use this identifier to correlate records for the same run. Copied checkpoints can share an ID.</dd></div>
          <div><dt>Recorded status</dt><dd>{@detail.summary.status}</dd><dd class="fact-help">The state reconstructed from the journal. Completion must be recorded before work counts as finished.</dd></div>
          <div><dt>Last event sequence</dt><dd>{@detail.summary.last_seq}</dd><dd class="fact-help">The latest verified event in this journal. Compare positions within the same run to see how its recorded history advanced.</dd></div>
          <div><dt>Local host observation</dt><dd>{host_label(@detail.summary.host)}</dd><dd class="fact-help">Registration describes this local host's ownership record. “Not registered” does not rule out execution elsewhere; “unknown” means the observation was unavailable.</dd></div>
          <div :if={@detail.summary.pending_repair}><dt>Journal repair</dt><dd>pending repair ({@detail.summary.pending_repair.action})</dd><dd class="fact-help">Only the verified prefix is shown. An incomplete tail needs separate repair; browsing does not alter it.</dd></div>
        </dl>
        <form :if={cancellable?(@detail)} method="post" action={"/runs/#{@root_id}/#{@run_ref}/cancel"} class="cancel">
          <input type="hidden" name="_csrf_token" value={Plug.CSRFProtection.get_csrf_token()} />
          <button type="submit">Cancel run</button>
        </form>
        <section class="projection-guide">
          <h2>Work summary</h2>
          <p>Use this to see which planned work items are complete and which remain pending. A work item is a unit of the plan; it can involve an agent assignment, review and acceptance checks.</p>
          <p><strong>Attention</strong> identifies unresolved issues when present. <strong>Journal position</strong> anchors this summary to the recorded history.</p>
          <details open><summary>Show the recorded summary</summary><pre>{@detail.summary.rendered}</pre></details>
        </section>
        <section class="projection-guide">
          <h2>Shared context</h2>
          <p>This is the current shared state reconstructed from the journal, using the same context projection the execution core uses when preparing assignment prompts. It helps explain what an assignment could be told about the run.</p>
          <dl class="context-key">
            <dt>Context revision</dt><dd>The recorded version of shared context. It is separate from the journal's event sequence.</dd>
            <dt>Work item status</dt><dd>OPEN means the item still has work remaining; DONE means completion was recorded.</dd>
            <dt>Open assignments</dt><dd>Assignments with no recorded completion yet. This is a record of outstanding work, not an agent liveness check.</dd>
            <dt>Open attention</dt><dd>Recorded issues that still need resolution. “None” means no open attention is recorded in this projection.</dd>
          </dl>
          <details open><summary>Show the recorded context</summary><pre>{@detail.context.rendered}</pre></details>
        </section>
      </section>
    </main>
    """
  end

  defp cancellable?(%{summary: %{status: status}}) when is_binary(status), do: status not in @terminal
  defp cancellable?(_), do: false

  # the session's ONE retained outcome, shown only for this run; internal identities never rendered
  defp outcome_line(session_id, root_id, run_ref) do
    case SessionStore.outcome(SessionStore, session_id) do
      {:ok, %{root_id: ^root_id, run_ref: ^run_ref, state: state, outcome: outcome}} ->
        cond do
          state in [:starting, :running] -> "Cancel pending"
          is_map(outcome) and is_binary(outcome[:message]) -> outcome.message
          state == :failed_to_start -> "Cancel could not start"
          state == :refused_at_grant -> "Cancel refused: session no longer valid"
          true -> nil
        end

      _ ->
        nil
    end
  end

  defp host_label(%{registered: :unknown}), do: "host: unknown"
  defp host_label(%{registered: true, live: live}), do: "host: registered" <> if(live == true, do: ", live", else: "")
  defp host_label(%{registered: false}), do: "host: not registered"
  defp host_label(_), do: "host: unknown"
end
