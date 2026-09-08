defmodule AiOrchestrator.Lifecycle.RunFSM do
  @moduledoc """
  Compatibility façade over the one execution path. NOT a supported public lifecycle surface: the
  public entry point is `AiOrchestrator.Commands.invoke/4` with `AiOrchestrator.Run.Executor` (NS-43);
  this module keeps its signatures as an internal compatibility/test seam and has no production caller.

  `run/3`, `resume/4`, and `cancel/2` keep the signatures and result shapes the
  CLI and the lifecycle suites were written against, and delegate to
  `AiOrchestrator.Lifecycle.Host`, which drives the pure `Lifecycle.Core.Reducer`
  and executes its effects through the injected adapters. There is no second
  decision implementation behind this module.
  """

  alias AiOrchestrator.Lifecycle.Host

  @spec run(map(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def run(spec, plan, opts \\ []) when is_map(spec) and is_map(plan), do: Host.run(spec, plan, opts)

  @spec resume(map(), map(), [String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def resume(spec, plan, prior_lines, opts \\ []) when is_map(spec) and is_map(plan) and is_list(prior_lines),
    do: Host.resume(spec, plan, prior_lines, opts)

  @spec cancel([String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def cancel(prior_lines, opts \\ []) when is_list(prior_lines), do: Host.cancel(prior_lines, opts)
end
