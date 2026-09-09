defmodule C1.FakeOS do
  @moduledoc "TEST SUPPORT ONLY: scripted OS answers for the teardown ownership controls; signals are recorded, never sent."
  defstruct pgrep: [], lines: %{}, children: %{}

  def absent, do: %C1.FakeOS{}

  def present(_profile, lines, children),
    do: %C1.FakeOS{
      pgrep: lines |> Map.keys() |> Enum.filter(&String.contains?(lines[&1], "user-data-dir=")),
      lines: lines,
      children: children
    }

  def pgrep_profile(%C1.FakeOS{pgrep: p}, _profile), do: p
  def children(%C1.FakeOS{children: c}, pid), do: Map.get(c, pid, [])
  def command_line(%C1.FakeOS{lines: l}, pid), do: Map.get(l, pid, "")

  def kill9(%C1.FakeOS{}, pid) do
    send(self(), {:os_signal, pid})
    {"", 0}
  end

  def alive?(%C1.FakeOS{}, _pid), do: false
end
