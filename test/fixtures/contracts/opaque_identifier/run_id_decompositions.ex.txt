defmodule AiOrchestrator.Test.Fixtures.RunIdDecompositions do
  @moduledoc """
  Test-local fixture for test/contracts/opaque_identifier_test.exs (NS-37.A.001). It is
  never compiled or loaded: the test parses it and requires the opacity gate to report
  EVERY function below. Each one takes an opaque run identity apart in a form the gate
  used to miss: a binary pattern match, an Erlang `:binary` call, or a rename first.
  """

  def prefix_match(run_id) do
    "run_" <> rest = run_id
    rest
  end

  def prefix_match_in_head("run_" <> rest = run_id), do: {rest, run_id}

  def bitstring_match(run_id) do
    <<"run_", stamp::binary-size(16), _rest::binary>> = run_id
    stamp
  end

  def case_prefix(run_id) do
    case run_id do
      "run_" <> rest -> rest
      _other -> nil
    end
  end

  def binary_split(run_id), do: :binary.split(run_id, "_", [:global])

  def binary_part_call(run_id), do: :binary.part(run_id, 4, 16)

  def binary_match_on_field(event), do: :binary.match(event["run_id"], "T")

  def renamed_then_split(run_id) do
    id = run_id
    String.split(id, "_")
  end

  def renamed_twice_then_binary(state) do
    id = state.run_id
    other = id
    :binary.split(other, "_")
  end

  def renamed_by_map_pattern(%{"run_id" => id}), do: String.slice(id, 4, 16)

  def renamed_by_keyword_read_then_matched(opts) do
    id = Keyword.fetch!(opts, :run_id)
    "run_" <> stamp = id
    stamp
  end
end
