defmodule OrrisConsole.Session do
  @moduledoc "A server-only session record: identity and scope come from trusted configuration, never from the browser."
  defstruct actor_id: nil, root_ids: [], issued_ms: 0, idle_deadline_ms: 0, absolute_deadline_ms: 0
  @type t :: %__MODULE__{}
end
