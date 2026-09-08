defmodule AiOrchestrator.Contracts.FileErrorTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Contract.FileError

  test "the closed vocabulary equals :file.posix() in the pinned toolchain" do
    {:ok, types} = Code.Typespec.fetch_types(:file)

    {:type, _, :union, members} =
      Enum.find_value(types, fn
        {:type, {:posix, form, []}} -> form
        _other -> nil
      end)

    toolchain_errnos = for {:atom, _, errno} <- members, do: errno

    assert Enum.sort(FileError.errnos()) == Enum.sort(toolchain_errnos)
  end
end
