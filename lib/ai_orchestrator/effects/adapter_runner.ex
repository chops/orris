defmodule AiOrchestrator.Effects.AdapterRunner do
  @moduledoc """
  The shared small runner boundary (docs/contracts/observe-deadline.org, U2a-1O): runs one zero-arity adapter
  closure wherever the owner placed it (a Worker-internal task today) and answers a disjoint grammar:
  `{:ok, raw}` with the adapter's raw return, or `{:failed, diagnostic}` where the diagnostic is CLOSED at the
  catch point (kind, result class, digest, real stack depth). The raw reason never leaves the catch.
  """

  alias AiOrchestrator.Contract.Diagnostic

  @kinds [:error, :throw, :exit]

  @type diagnostic :: %{kind: :error | :throw | :exit, class: String.t(), digest: String.t(), frames: non_neg_integer()}

  @spec run((-> term())) :: {:ok, term()} | {:failed, diagnostic()}
  def run(closure) when is_function(closure, 0) do
    {:ok, closure.()}
  catch
    kind, reason -> {:failed, closed(kind, reason, length(__STACKTRACE__))}
  end

  @doc "A closed diagnostic for a failure the caller already dequeued (a task death without reply, a refused hold)."
  @spec closed(:error | :throw | :exit, term(), non_neg_integer()) :: diagnostic()
  def closed(kind, reason, frames) when kind in @kinds and is_integer(frames) and frames >= 0 do
    %{kind: kind, class: Diagnostic.result_class(reason), digest: Diagnostic.describe(reason)["digest"], frames: frames}
  end

  @doc """
  Whether a runner's `{:failed, value}` carries exactly a closed diagnostic: the kind vocabulary, a result class
  from `Diagnostic.result_classes/0`, a lowercase `sha256:<64 hex>` digest and a non-negative depth. Anything
  else (an arbitrary string in class or digest included) fails closed as an invalid return.
  """
  @spec diagnostic?(term()) :: boolean()
  def diagnostic?(%{kind: kind, class: class, digest: digest, frames: frames} = map)
      when map_size(map) == 4 and kind in @kinds and is_binary(class) and is_binary(digest) and is_integer(frames) and
             frames >= 0, do: class in Diagnostic.result_classes() and digest?(digest)

  def diagnostic?(_other), do: false

  defp digest?("sha256:" <> hex) when byte_size(hex) == 64,
    do: hex == String.downcase(hex) and Base.decode16(hex, case: :lower) != :error

  defp digest?(_other), do: false
end
