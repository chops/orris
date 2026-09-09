defmodule OrrisConsole.RunDetailLive do
  @moduledoc "One run: summary, host facts (false/unknown preserved), pending repair and escaped context text."
  use Phoenix.LiveView
  alias OrrisConsole.{Config, ReadModel, Reader}

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
    assigns = assign(assigns, detail: Reader.value(assigns.state))

    ~H"""
    <main data-c1-read={if @detail, do: "complete", else: "pending"} class="detail">
      <h1>Run {@run_ref}</h1>
      <p :if={@state.delayed?} class="status">
        {if @state.stale?, do: "stale (refresh delayed)", else: "unavailable (refresh delayed)"}
        <span :if={@state.last_success_ms}> last successful read: {@state.last_success_ms}</span>
      </p>
      <p :if={@detail == nil and not @state.delayed?} class="status">loading</p>
      <section :if={@detail}>
        <dl>
          <dt>run id</dt><dd>{@detail.summary.run_id}</dd>
          <dt>status</dt><dd>{@detail.summary.status}</dd>
          <dt>last seq</dt><dd>{@detail.summary.last_seq}</dd>
          <dt>host</dt><dd>{host_label(@detail.summary.host)}</dd>
          <dt :if={@detail.summary.pending_repair}>repair</dt>
          <dd :if={@detail.summary.pending_repair}>pending repair ({@detail.summary.pending_repair.action})</dd>
        </dl>
        <h2>Summary</h2>
        <pre>{@detail.summary.rendered}</pre>
        <h2>Context</h2>
        <pre>{@detail.context.rendered}</pre>
      </section>
    </main>
    """
  end

  defp host_label(%{registered: :unknown}), do: "host: unknown"
  defp host_label(%{registered: true, live: live}), do: "host: registered" <> if(live == true, do: ", live", else: "")
  defp host_label(%{registered: false}), do: "host: not registered"
  defp host_label(_), do: "host: unknown"
end
