defmodule AiOrchestrator.ApplicationTest do
  use ExUnit.Case, async: true

  describe "the generated application spec" do
    # This is a startup-order pin, not a style rule. `:opentelemetry` is
    # started before `:opentelemetry_exporter`, so the SDK can reach the
    # configured OTLP exporter before the exporter application has started
    # `:inets`. Losing that race leaves the whole OS process with no telemetry
    # and only a warning to say so, which is why the escript emitted it
    # sometimes and not others. Naming `:inets` in `extra_applications` puts it
    # ahead of everything OTel; this test is what would notice it going away.
    #
    # The order is read back from the generated `.app`, so the assertion is
    # deterministic where the symptom it protects against is not.
    test "starts :inets before anything that can initialise the OTLP exporter" do
      applications = Application.spec(:ai_orchestrator, :applications)

      assert :inets in applications,
             ":inets must be an application dependency, not an accident of the runtime closure"

      inets = Enum.find_index(applications, &(&1 == :inets))

      for otel <- [:opentelemetry, :opentelemetry_exporter] do
        position = Enum.find_index(applications, &(&1 == otel))

        assert is_integer(position), "#{otel} is missing from the application dependency list"

        assert inets < position,
               "#{otel} would start before :inets, which is the exporter initialisation race"
      end
    end
  end
end
