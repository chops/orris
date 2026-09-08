defmodule AiOrchestrator.Contracts.ReducerPurityTest do
  use ExUnit.Case, async: false

  alias AiOrchestrator.Lifecycle.Host
  alias AiOrchestrator.Test.ScenarioHarness, as: H

  @reducer Path.expand("../../lib/ai_orchestrator/lifecycle/core/reducer.ex", __DIR__)
  @core Path.expand("../../lib/ai_orchestrator/lifecycle/core.ex", __DIR__)
  @forbidden ~r/AiOrchestrator\.Dispatch\b|\bDispatch\.[a-z]|\bGate\.|\bGateRunner\b|\bPaneRegistry\b|\bNotify\b|\bFile\.|\bSystem\.|Process\.sleep|:timer|\bclock\.|\bSystemClock\b|\bSystemId\b|\bAgent\.|\bGenServer\b|\bsend\(|\breceive\b|\bevent_sink\b/

  test "the reducer source references no effect adapter, clock, id seam, process, or file" do
    assert File.exists?(@reducer), "lib/ai_orchestrator/lifecycle/core/reducer.ex must exist"
    source = File.read!(@reducer)
    offenders = source |> String.split("\n") |> Enum.with_index(1) |> Enum.filter(fn {line, _} -> line =~ @forbidden end)
    assert offenders == [], "impure references in reducer.ex: #{inspect(offenders)}"
  end

  test "the reducer lives in a Boundary that depends on Journal and Spec only" do
    assert File.exists?(@core), "lib/ai_orchestrator/lifecycle/core.ex must declare the boundary"
    core = File.read!(@core)

    assert core =~
             ~r/use Boundary,\s*type: :strict,\s*deps: \[\s*AiOrchestrator\.Contract,\s*AiOrchestrator\.Journal,\s*AiOrchestrator\.Spec\s*\]/
  end

  test "the host is deterministic: equal inputs with reset seams give equal bytes" do
    for {name, kind, scenario, prior, opts_fun} <- H.cases() do
      first = drive(kind, scenario, prior, opts_fun)
      second = drive(kind, scenario, prior, opts_fun)
      assert first == second, name
    end
  end

  defp drive(kind, scenario, prior, opts_fun) do
    H.reset_seams()
    opts = opts_fun.()

    result =
      case kind do
        :run -> Host.run(H.spec(scenario), H.plan(scenario), opts)
        :resume -> Host.resume(H.spec(scenario), H.plan(scenario), prior, opts)
        :cancel -> Host.cancel(prior, opts)
      end

    case result do
      {:ok, %{events: events}} -> Enum.map(events, &Jason.encode!/1)
      other -> other
    end
  end
end
