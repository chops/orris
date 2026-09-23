defmodule AiOrchestrator.Journal.Schemas.EventDataSchemas do
  @moduledoc false

  alias AiOrchestrator.Journal.Schemas.EventData
  alias AiOrchestrator.Journal.Schemas.LiteralSchema

  # The compile-time call tracks the builder dependency; parsing only fetches.
  for_result =
    for mode <- [:read, :append, :view],
        type <- Enum.sort(EventData.typed_types()),
        version <- EventData.admitted_versions(type, mode),
        into: %{} do
      {{type, version, mode}, EventData.build_schema(type, version, mode)}
    end

  @schemas LiteralSchema.check!(for_result)

  def fetch!(type, version, mode), do: Map.fetch!(@schemas, {type, version, mode})
end
