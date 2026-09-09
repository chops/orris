defmodule OrrisConsole.RunIndexLive do
  @moduledoc "The run index of one allowed root: real listing through ReadModel, stale/unavailable states, no controls but logout."
  use Phoenix.LiveView
  alias OrrisConsole.{Config, ReadModel, Reader, RunExplanation}

  @impl true
  def mount(_params, _session, socket) do
    session = socket.assigns.console_session
    root_id = List.first(session.root_ids)
    socket = assign(socket, root_id: root_id, notice: nil, state: Reader.initial(socket.assigns.session_id))
    if connected?(socket), do: send(self(), :refresh)
    {:ok, socket}
  end

  defdelegate deliver(state, message), to: Reader

  @impl true
  def handle_event("select_root", %{"root_id" => root_id}, socket) do
    session = socket.assigns.console_session

    if root_id in session.root_ids and Map.has_key?(Config.current().roots, root_id) do
      send(self(), :refresh)
      {:noreply, assign(socket, root_id: root_id, notice: nil, state: Reader.initial(socket.assigns.session_id))}
    else
      {:noreply, assign(socket, notice: "That root is not available.")}
    end
  end

  def handle_event(_other, _params, socket), do: {:noreply, socket}

  @impl true
  def handle_info(:refresh, socket) do
    if socket.assigns.state.valid?.() do
      %{console_session: session, root_id: root_id} = socket.assigns
      read = fn -> ReadModel.list(Config.current(), session, root_id) end
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
    listing = Reader.value(assigns.state)
    demo? = listing != nil and RunExplanation.demo_runs?(listing.runs)
    listing = if listing, do: %{listing | runs: RunExplanation.ordered(listing.runs)}
    assigns = assign(assigns, listing: listing, demo?: demo?)

    ~H"""
    <main data-c1-read={if @listing, do: "complete", else: "pending"} class="index">
      <h1>Runs in root {@root_id}</h1>
      <p :if={@notice} class="notice">{@notice}</p>
      <p :if={@state.delayed?} class="status">
        {if @state.stale?, do: "stale (refresh delayed)", else: "unavailable (refresh delayed)"}
        <span :if={@state.last_success_ms}> last successful read: {@state.last_success_ms}</span>
      </p>
      <p :if={@listing == nil and not @state.delayed?} class="status">loading</p>
      <p :if={@listing} id="run-table-help">
        <span :if={@demo?}>Supplied demo checkpoints, ordered by journal position: prepare work → observe output → check acceptance. Each row is a saved checkpoint of the same example run.</span>
        <span :if={not @demo?}>Needs attention first, finished runs last; alphabetical by directory within each group.</span>
        Last event sequence identifies the latest verified journal event.
        Recorded status comes from the journal; it does not establish whether an agent is currently running.
      </p>
      <table :if={@listing} aria-describedby="run-table-help">
        <thead>
          <tr>
            <th scope="col">Run directory</th>
            <th scope="col">Run ID</th>
            <th scope="col">Recorded status</th>
            <th scope="col">Last event sequence</th>
            <th scope="col">Read status</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={run <- @listing.runs}>
            <td><a href={"/runs/#{@root_id}/#{run.run_ref}"}>{run.run_ref}</a></td>
            <td>{run.run_id}</td>
            <td>{run.status}</td>
            <td>{run.last_seq}</td>
            <td>{if run.error, do: "unavailable", else: ""}</td>
          </tr>
        </tbody>
      </table>
      <p :if={@listing && @listing.skipped > 0} class="status">{@listing.skipped} entries outside the root were skipped</p>
      <ul :if={length(@console_session.root_ids) > 1} class="roots">
        <li :for={id <- @console_session.root_ids}><a phx-click="select_root" phx-value-root_id={id}>{id}</a></li>
      </ul>
    </main>
    """
  end
end
