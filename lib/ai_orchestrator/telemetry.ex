defmodule AiOrchestrator.Telemetry do
  @moduledoc """
  Domain-neutral lifecycle telemetry: the emit vocabulary every instrumented boundary uses
  (`AiOrchestrator.Telemetry.Events`) and the application-boundary span handler that turns those
  events into local OpenTelemetry spans (`AiOrchestrator.Telemetry.Handler`).

  The contract is `docs/contracts/lifecycle-telemetry.org` (NS-26.F.000 emit sites, NS-26.F.002
  span correlation). The command boundary keeps its own producer,
  `AiOrchestrator.Commands.Telemetry`, unchanged: this boundary consumes its events and never
  changes them.

  Its `deps` are exactly `Contract`, for the closed `Diagnostic.result_class/1` vocabulary. It
  depends on no domain boundary, so instrumenting a domain can never reverse a dependency.
  """

  use Boundary, deps: [AiOrchestrator.Contract], exports: [Events, Handler]
end
