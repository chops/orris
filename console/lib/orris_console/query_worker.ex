defmodule OrrisConsole.QueryWorker do
  @moduledoc """
  A temporary read worker: links to its controller BEFORE anything else, registers the view's worker key, acknowledges
  its start BEFORE any read, then runs the read closure once told to. A failing read is a closed {:error, :failed};
  nothing is logged. The process exits normally when done; the Registry key and the supervisor slot last until then.
  """

  def child_spec(args),
    do: %{id: make_ref(), start: {__MODULE__, :start_link, [args]}, restart: :temporary, shutdown: :brutal_kill}

  def start_link(args), do: :proc_lib.start_link(__MODULE__, :init, [self(), args])

  def init(parent, {owner, view, fun}) do
    Process.link(owner)

    case Registry.register(OrrisConsole.QueryRegistry, {:worker, view}, :worker) do
      {:ok, _} ->
        :proc_lib.init_ack(parent, {:ok, self()})

        receive do
          {:run, ^owner} -> send(owner, {:worker_value, self(), run(fun)})
        end

      {:error, _} ->
        :proc_lib.init_ack(parent, {:error, :view_busy})
        exit(:normal)
    end
  end

  defp run(fun) do
    case fun.() do
      {:ok, _} = ok -> ok
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _ -> {:error, :failed}
    end
  rescue
    _ -> {:error, :failed}
  catch
    _, _ -> {:error, :failed}
  end
end
