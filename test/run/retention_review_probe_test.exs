defmodule AiOrchestrator.Run.RetentionReviewProbeTest do
  @moduledoc """
  Reviewer probe imported verbatim from review m_1788826770000 (RG-M1), plus two capability controls: an admitted
  Worker with NO owned entries must ignore a foreign Port message before any executor configuration is resolved,
  and an executor selection that is not a loadable module, or has no stage/2, must never crash the owner.
  """
  use ExUnit.Case, async: false

  alias AiOrchestrator.Run.Worker

  test "an empty admitted Worker ignores a foreign Port before resolving unused executor configuration" do
    pid = start_supervised!({Worker, self()})
    cap = make_ref()
    send(pid, {:admit, cap, 1, [gate_executor: %{}]})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    port = Port.open({:spawn_executable, "/bin/cat"}, [:binary])

    try do
      send(pid, {port, {:data, {:eol, "foreign"}}})
      assert %{runtime: %{gates: gates}} = :sys.get_state(pid, 2_000)
      assert gates == %{}
    after
      Port.close(port)
    end
  end

  # control (RG-M1): an unloadable module atom as the executor selection, no owned entries: ignored, owner serves
  test "an empty admitted Worker ignores a foreign Port when the executor module does not exist" do
    pid = start_supervised!({Worker, self()})
    cap = make_ref()
    send(pid, {:admit, cap, 1, [gate_executor: :"Elixir.AiOrchestrator.NoSuchExecutor"]})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    port = Port.open({:spawn_executable, "/bin/cat"}, [:binary])

    try do
      send(pid, {port, {:data, {:eol, "foreign"}}})
      assert %{runtime: %{gates: gates}} = :sys.get_state(pid, 2_000)
      assert gates == %{}
    after
      Port.close(port)
    end
  end

  # control (RG-M1): a real module without stage/2, no owned entries: ignored, owner serves
  test "an empty admitted Worker ignores a foreign Port when the executor has no stage/2" do
    pid = start_supervised!({Worker, self()})
    cap = make_ref()
    send(pid, {:admit, cap, 1, [gate_executor: Enum]})
    assert_receive {:admitted, ^cap, 1, ^pid}, 2_000
    port = Port.open({:spawn_executable, "/bin/cat"}, [:binary])

    try do
      send(pid, {port, {:data, {:eol, "foreign"}}})
      assert %{runtime: %{gates: gates}} = :sys.get_state(pid, 2_000)
      assert gates == %{}
    after
      Port.close(port)
    end
  end
end
