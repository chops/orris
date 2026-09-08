defmodule AiOrchestrator.Test.FixedClockDecouplingTest do
  @moduledoc """
  Regression controls for the FixedClock monotonic/wall decoupling (m_1788751885000): monotonic reads do not
  alter unix/wall progression, wall reads and advance do not alter the monotonic stream, reset clears both, and
  separate processes keep independent streams. Base and wall behaviour are preserved.
  """
  use ExUnit.Case, async: true

  alias AiOrchestrator.Test.FixedClock

  setup do
    FixedClock.reset()
    :ok
  end

  test "base preserved and unix_now peeks while wall_ts consumes one wall tick per read" do
    assert FixedClock.base_unix() == DateTime.to_unix(~U[2026-09-01 12:00:00Z])
    assert FixedClock.unix_now() == FixedClock.base_unix()
    assert FixedClock.unix_now() == FixedClock.base_unix(), "unix_now never consumes a tick"
    assert FixedClock.wall_ts() == "2026-09-01T12:00:00Z"
    assert FixedClock.unix_now() == FixedClock.base_unix() + 1, "wall_ts consumed one tick"
  end

  test "monotonic reads advance their own stream by 10 ms and never move the wall clock" do
    assert FixedClock.monotonic_ms() == 0
    assert FixedClock.monotonic_ms() == 10
    assert FixedClock.monotonic_ms() == 20
    assert FixedClock.unix_now() == FixedClock.base_unix(), "three monotonic reads moved no wall tick"
    assert FixedClock.wall_ts() == "2026-09-01T12:00:00Z"
  end

  test "wall reads and advance never move the monotonic stream" do
    assert FixedClock.monotonic_ms() == 0
    _ = FixedClock.wall_ts()
    _ = FixedClock.wall_ts()
    FixedClock.advance(30)
    assert FixedClock.unix_now() == FixedClock.base_unix() + 32
    assert FixedClock.monotonic_ms() == 10, "two wall reads and an advance moved no monotonic read"
  end

  test "reset clears both streams" do
    _ = FixedClock.wall_ts()
    _ = FixedClock.monotonic_ms()
    FixedClock.reset()
    assert FixedClock.unix_now() == FixedClock.base_unix()
    assert FixedClock.monotonic_ms() == 0
  end

  test "separate processes keep independent wall and monotonic streams" do
    _ = FixedClock.wall_ts()
    _ = FixedClock.monotonic_ms()
    parent = self()

    spawn_link(fn ->
      send(parent, {:other, FixedClock.unix_now(), FixedClock.monotonic_ms()})
    end)

    assert_receive {:other, unix, mono}, 1_000
    assert unix == FixedClock.base_unix() and mono == 0
    assert FixedClock.unix_now() == FixedClock.base_unix() + 1 and FixedClock.monotonic_ms() == 10
  end
end
