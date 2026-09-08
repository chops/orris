defmodule AiOrchestrator.Test.OwnerOracle do
  @moduledoc """
  TEST-ONLY reference driver mirroring the ratified ownership split: the loop (open, commit, receipt selection,
  observer, reducer) runs in the calling process while EVERY effect stage (release_terminal, execute, settle)
  runs in a separate owner process that owns the `Effects.Runtime` - exactly the Server/Worker distribution. Under
  the per-process `FixedClock` this reproduces the Server's clock-read distribution, so a parity oracle for the
  two-process design compares like with like. Same signatures as `Host.run/3`, `Host.resume/4`, `Host.cancel/2`.
  """

  alias AiOrchestrator.Effects
  alias AiOrchestrator.Effects.Runtime
  alias AiOrchestrator.Lifecycle.Host

  @spec run(map(), map(), keyword()) :: {:ok, map()} | {:error, map()}
  def run(spec, plan, opts), do: drive(:run, %{spec: spec, plan: plan, prior_lines: []}, opts)

  @spec resume(map(), map(), [String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def resume(spec, plan, prior_lines, opts), do: drive(:resume, %{spec: spec, plan: plan, prior_lines: prior_lines}, opts)

  @spec cancel([String.t()], keyword()) :: {:ok, map()} | {:error, map()}
  def cancel(prior_lines, opts), do: drive(:cancel, %{prior_lines: prior_lines}, opts)

  defp drive(mode, inputs, opts) do
    case Host.open(mode, inputs, opts) do
      {:ok, loop} ->
        # LINKED: a hard death of the caller (kill, ExUnit timeout) terminates the runtime owner even while it is
        # blocked inside an effect; the monitor is the bounded join for the orderly stop
        owner = spawn_link(fn -> owner_loop(Runtime.new(loop.opts), loop.opts) end)
        monitor = Process.monitor(owner)

        try do
          step(loop, owner)
        after
          send(owner, :stop)
          join!(owner, monitor)
        end

      {:error, _} = error ->
        error
    end
  end

  defp step(loop, owner) do
    case Host.commit_step(loop) do
      {:effect, stage} ->
        :ok = ask(owner, {:release_terminal, stage.suffix})
        observation = ask(owner, {:execute, stage.intent, stage.receipt})
        {:continue, next} = Host.resume_step(stage, observation)
        step(next, owner)

      {:done, stage, result} ->
        :ok = ask(owner, {:release_terminal, stage.suffix})
        cleanup = ask(owner, :settle)
        Host.close_step(result, cleanup)

      {:rejected, rejection} ->
        cleanup = ask(owner, :settle)
        Host.reject_step(rejection, cleanup)
    end
  end

  # orderly stop first (the owner settles what it holds), then a bounded forced kill; an unjoined owner is an
  # oracle failure, never a leak
  defp join!(owner, monitor) do
    receive do
      {:DOWN, ^monitor, :process, ^owner, _} -> :ok
    after
      5_000 ->
        Process.exit(owner, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^owner, _} -> :ok
        after
          5_000 -> exit({:owner_oracle_owner_unjoined, owner})
        end
    end
  end

  defp request_name(request) when is_tuple(request), do: elem(request, 0)
  defp request_name(request) when is_atom(request), do: request

  defp ask(owner, request) do
    ref = make_ref()
    send(owner, {ref, self(), request})

    receive do
      {^ref, {:ok, answer}} -> answer
      {^ref, {:raise, kind, reason, stacktrace}} -> :erlang.raise(kind, reason, stacktrace)
    after
      60_000 -> exit({:owner_oracle_unanswered, request_name(request)})
    end
  end

  # the owner: one runtime, every effect stage, trappable failures carried back with the runtime settled
  defp owner_loop(runtime, opts) do
    receive do
      :stop ->
        _ = Effects.settle(runtime)
        :ok

      {ref, from, {:release_terminal, suffix}} ->
        runtime = Effects.release_terminal(runtime, suffix)
        send(from, {ref, {:ok, :ok}})
        owner_loop(runtime, opts)

      {ref, from, {:execute, intent, receipt}} ->
        try do
          {observation, runtime} = Effects.execute(intent, runtime, opts: opts, receipt: receipt)
          send(from, {ref, {:ok, observation}})
          owner_loop(runtime, opts)
        catch
          :error, %Effects.Interrupted{} = interrupted ->
            _ = Effects.settle(interrupted.runtime)
            send(from, {ref, {:raise, interrupted.kind, interrupted.reason, interrupted.stacktrace}})
            owner_loop(Runtime.new(opts), opts)

          kind, reason ->
            _ = Effects.settle(runtime)
            send(from, {ref, {:raise, kind, reason, __STACKTRACE__}})
            owner_loop(Runtime.new(opts), opts)
        end

      {ref, from, :settle} ->
        {cleanup, runtime} = Effects.settle(runtime)
        send(from, {ref, {:ok, cleanup}})
        owner_loop(runtime, opts)
    end
  end
end
