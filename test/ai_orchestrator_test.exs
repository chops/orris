defmodule AiOrchestratorTest do
  use ExUnit.Case, async: true

  test "exposes the application version" do
    assert AiOrchestrator.version() == "0.1.0-dev"
  end
end
