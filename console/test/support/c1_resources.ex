defmodule C1.Resources do
  @moduledoc """
  TEST SUPPORT ONLY: a resource owner for rows that acquire several disposable resources (release process, listener,
  browser, credential). Cleanup is installed at acquisition, BEFORE any later fallible startup, and runs in reverse
  order on every path; a cleanup that raises does not prevent the remaining cleanups, and every cleanup failure is
  reported together at the end.
  """
  import ExUnit.Assertions

  @doc "Runs `body.(owner)`; `owner` is a pid-free handle (this process's stack). Cleanups run in the after block."
  def run(body) do
    Process.put(:c1_resources, [])

    try do
      body.()
    after
      failures =
        Process.get(:c1_resources, [])
        |> Enum.reduce([], fn {name, cleanup}, acc ->
          try do
            cleanup.()
            acc
          rescue
            e -> [{name, Exception.message(e)} | acc]
          catch
            kind, reason -> [{name, inspect({kind, reason})} | acc]
          end
        end)

      Process.delete(:c1_resources)

      if failures != [],
        do: flunk("resource cleanup failures (all cleanups were attempted): #{inspect(Enum.reverse(failures))}")
    end
  end

  @doc "Acquires a resource with its cleanup installed immediately; `acquire.()` returns the resource."
  def acquire(name, acquire, cleanup) do
    resource = acquire.()
    Process.put(:c1_resources, [{name, fn -> cleanup.(resource) end} | Process.get(:c1_resources, [])])
    resource
  end

  @doc "Names of the resources whose cleanup is currently installed (newest first)."
  def installed, do: Enum.map(Process.get(:c1_resources, []), &elem(&1, 0))
end
