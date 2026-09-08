defmodule AiOrchestrator.ProcessIdentity do
  @moduledoc """
  OS process identity seam: a process is identified by its pid together with
  its `ps` start time (`lstart`), so a reused pid with a different start time
  reads as dead rather than live. Shared by pane claims and the run lock.
  """

  use Boundary, deps: [], exports: []

  @spec current(String.t(), keyword()) :: {:ok, String.t()} | :dead | {:error, map()}
  def current(pid, opts \\ []) when is_binary(pid) do
    ps_path = Keyword.get(opts, :ps_path, "ps")

    case System.cmd(ps_path, ["-p", pid, "-o", "lstart="], stderr_to_stdout: true) do
      {output, 0} ->
        case String.trim(output) do
          "" -> :dead
          started_at -> {:ok, started_at}
        end

      {_output, 1} ->
        :dead

      {output, status} ->
        {:error,
         %{
           "reason" => "pane_registry_unavailable",
           "detail" => "ps exited #{status}: #{String.trim(output)}"
         }}
    end
  rescue
    error in ErlangError ->
      {:error, %{"reason" => "pane_registry_unavailable", "detail" => Exception.message(error)}}
  end

  @spec owner_status(map(), keyword()) :: :live | :dead | {:error, map()}
  def owner_status(%{"pid" => pid, "pid_start" => expected}, opts \\ []) do
    case current(pid, opts) do
      {:ok, ^expected} -> :live
      {:ok, _different_start} -> :dead
      :dead -> :dead
      {:error, reason} -> {:error, reason}
    end
  end
end
