defmodule C1.Fixtures.ForbiddenCoreReference do
  @moduledoc """
  Inert control for console/test/c1/c1_ns38_core_surface_test.exs (NS-38.K.001). It is NOT in the
  console's compile paths: the test parses it, and compiles it in memory only to read its bytecode.
  Nothing calls it. Each function reaches the core through a module other than Prepare or Query,
  in a different spelling. This docstring naming AiOrchestrator.Journal.Writer is a decoy the
  source check must ignore.
  """

  # decoy in a comment: AiOrchestrator.Host.Monitor.start_link(opts)
  alias AiOrchestrator.{Journal.Reader, Query}
  alias AiOrchestrator.Lifecycle.RunFSM
  alias AiOrchestrator, as: Core

  def through_a_lifecycle_alias(lines), do: RunFSM.cancel(lines, [])

  def through_a_multi_alias(dir), do: {Reader.load(dir), Query}

  def through_a_renamed_root, do: Core.Id.SystemId.run_id()

  def fully_qualified(dir), do: AiOrchestrator.Run.Executor.__info__(:module) && dir

  def through_an_erlang_atom, do: :"Elixir.AiOrchestrator.Gate.Execution".__info__(:module)

  def decoy_in_a_string, do: "AiOrchestrator.Notify.Hook.fire/1"
end
