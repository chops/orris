defmodule C1.Doubles do
  @moduledoc """
  TEST SUPPORT ONLY: disposable reference adapters (faithful) and named mutants for the C1 oracles. They prove the
  oracles discriminate (harness controls) before the product exists; the RED rows run the same oracles against the
  product modules. Nothing here is product code or a claim about it.
  """

  defmodule Credential do
    @moduledoc "Permission-first credential setup (faithful) and its mutants, each violating one named step."
    def faithful(path), do: setup(path, :permission_first)
    def write_first(path), do: setup(path, :write_first)

    # never opens exclusively: create, chmod, then write through write_file (the reviewer's first counterexample)
    def no_exclusive_open(path) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "")
      File.chmod!(path, 0o600)
      File.write!(path, :crypto.strong_rand_bytes(32))
      :ok
    end

    # exclusive open + 0600, then the path is replaced by a world-readable file that receives the secret
    def chmod_then_replace(path) do
      File.mkdir_p!(Path.dirname(path))
      {:ok, device} = :file.open(String.to_charlist(path), [:write, :exclusive, :binary, :raw])
      :ok = File.chmod(path, 0o600)
      :ok = :file.close(device)
      File.rm!(path)
      File.write!(path, "")
      File.chmod!(path, 0o644)
      File.write!(path, :crypto.strong_rand_bytes(32))
      :ok
    end

    # exclusive open + 0600 on the target, but the secret goes to another path; the target stays empty
    def secret_elsewhere(path) do
      File.mkdir_p!(Path.dirname(path))
      {:ok, device} = :file.open(String.to_charlist(path), [:write, :exclusive, :binary, :raw])
      :ok = File.chmod(path, 0o600)
      File.write!(path <> ".elsewhere", :crypto.strong_rand_bytes(32))
      :ok = :file.close(device)
      :ok
    end

    defp setup(path, order) do
      dir = Path.dirname(path)
      File.mkdir_p!(dir)
      File.chmod!(dir, 0o700)
      {:ok, device} = :file.open(String.to_charlist(path), [:write, :exclusive, :binary, :raw])
      secret = :crypto.strong_rand_bytes(32)

      case order do
        :permission_first ->
          :ok = File.chmod(path, 0o600)
          :ok = :file.write(device, secret)

        :write_first ->
          :ok = :file.write(device, secret)
          :ok = File.chmod(path, 0o600)
      end

      :ok = :file.sync(device)
      :ok = :file.close(device)
      :ok
    end
  end

  defmodule Job do
    @moduledoc """
    Reference QueryJob adapter with the SAME shape as the pinned product API: `start(view, read_fun, opts)` answering
    {:ok, controller} | {:error, :capacity | :view_busy}; result {:query_result, controller, correlation, result} to
    the view after the worker's actual DOWN. Variants: :faithful; :premature_admission (no controller key at all and
    admission checks only the worker key, so a replacement really starts at the cut point); :no_link (worker never
    links); :read_before_ack (worker runs the read before its init_ack); :early_detached (worker never links,
    monitors its controller and starts the read only after the controller died: the reviewer's injection).
    """
    use GenServer

    def fresh(variant, limit \\ 2) do
      registry = :"c1_double_registry_#{System.unique_integer([:positive])}"
      counter = :counters.new(1, [:atomics])
      {:ok, registry_pid} = Registry.start_link(keys: :unique, name: registry)
      Process.unlink(registry_pid)
      {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one, max_children: limit)
      Process.unlink(sup)

      %{
        variant: variant,
        registry: registry,
        registry_pid: registry_pid,
        workers: sup,
        # the reviewer's injection shape: the FIRST start is faithful (the linked-read witness passes), later starts
        # carry the variant (early_detached injects an unlinked worker that reads after its controller died)
        start: fn view, fun, opts ->
          n = :counters.get(counter, 1)
          :counters.add(counter, 1, 1)
          effective = if variant == :early_detached and n == 0, do: :faithful, else: variant
          start(effective, registry, sup, view, fun, opts)
        end
      }
    end

    def cleanup(%{registry_pid: registry, workers: sup}) do
      if Process.alive?(sup), do: Supervisor.stop(sup)
      if Process.alive?(registry), do: GenServer.stop(registry)
    end

    def start(variant, registry, sup, view, fun, opts) do
      busy? =
        Registry.lookup(registry, {:worker, view}) != [] or
          (variant != :premature_admission and Registry.lookup(registry, {:controller, view}) != [])

      if busy?, do: {:error, :view_busy}, else: GenServer.start(__MODULE__, {variant, registry, sup, view, fun, opts})
    end

    @impl true
    def init({variant, registry, sup, view, fun, opts}) do
      Process.flag(:trap_exit, true)

      registered =
        if variant == :premature_admission,
          do: {:ok, :none},
          else: Registry.register(registry, {:controller, view}, :controller)

      case registered do
        {:ok, _} ->
          vref = Process.monitor(view)
          send(self(), :start)

          {:ok,
           %{
             variant: variant,
             registry: registry,
             sup: sup,
             view: view,
             vref: vref,
             fun: fun,
             opts: opts,
             worker: nil,
             wref: nil,
             result: nil,
             timer: nil
           }}

        {:error, _} ->
          {:stop, :normal}
      end
    end

    @impl true
    def handle_info(:start, s) do
      spec = %{
        id: make_ref(),
        start: {C1.Doubles.Worker, :start_link, [{self(), s.view, s.registry, s.fun, s.variant}]},
        restart: :temporary,
        shutdown: 500
      }

      case DynamicSupervisor.start_child(s.sup, spec) do
        {:ok, worker} ->
          ref = Process.monitor(worker)
          timer = Process.send_after(self(), :deadline, Keyword.get(s.opts, :deadline_ms, 2_000))
          send(worker, {:run, self()})
          {:noreply, %{s | worker: worker, wref: ref, timer: timer}}

        {:error, :max_children} ->
          send(s.view, {:query_refused, self(), Keyword.get(s.opts, :correlation), :capacity})
          {:stop, :normal, s}

        {:error, _} ->
          send(s.view, {:query_refused, self(), Keyword.get(s.opts, :correlation), :view_busy})
          {:stop, :normal, s}
      end
    end

    def handle_info({:worker_value, worker, value}, %{worker: worker} = s), do: {:noreply, %{s | result: value}}

    def handle_info(:deadline, s) do
      if s.worker, do: Process.exit(s.worker, :kill)
      {:noreply, %{s | result: {:error, :deadline}}}
    end

    def handle_info({:DOWN, ref, :process, _, _}, %{vref: ref} = s) do
      if s.worker, do: Process.exit(s.worker, :kill)
      {:noreply, %{s | view: nil}}
    end

    def handle_info({:DOWN, ref, :process, _, _}, %{wref: ref} = s) when not is_nil(ref) do
      if s.view,
        do: send(s.view, {:query_result, self(), Keyword.get(s.opts, :correlation), s.result || {:error, :down}})

      {:stop, :normal, s}
    end

    def handle_info(_, s), do: {:noreply, s}

    @impl true
    def terminate(_, s) do
      if s.timer, do: Process.cancel_timer(s.timer)
      if s.worker, do: Process.exit(s.worker, :kill)
    end
  end

  defmodule Worker do
    @moduledoc false
    def start_link(args), do: :proc_lib.start_link(__MODULE__, :init, [self(), args])

    def init(parent, {owner, view, registry, fun, variant}) do
      if variant not in [:no_link, :early_detached], do: Process.link(owner)

      case Registry.register(registry, {:worker, view}, :worker) do
        {:ok, _} when variant == :early_detached ->
          ref = Process.monitor(owner)
          :proc_lib.init_ack(parent, {:ok, self()})

          receive do
            {:DOWN, ^ref, :process, ^owner, _} -> fun.()
            {:run, ^owner} -> send(owner, {:worker_value, self(), fun.()})
          end

        {:ok, _} ->
          if variant == :read_before_ack do
            value = fun.()
            :proc_lib.init_ack(parent, {:ok, self()})
            receive do: ({:run, ^owner} -> send(owner, {:worker_value, self(), value}))
          else
            :proc_lib.init_ack(parent, {:ok, self()})
            receive do: ({:run, ^owner} -> send(owner, {:worker_value, self(), fun.()}))
          end

        {:error, _} ->
          :proc_lib.init_ack(parent, {:error, :view_busy})
          exit(:normal)
      end
    end
  end

  defmodule Delivery do
    @moduledoc "View-side result delivery: faithful (session valid AND current identity) and two mutants."
    def faithful(state, {:query_result, job, correlation, result}) do
      if state.valid?.() and state.current == {job, correlation},
        do: %{state | data: result, current: nil},
        else: %{state | dropped: state.dropped + 1}
    end

    def no_revalidation(state, {:query_result, job, correlation, result}) do
      if state.current == {job, correlation},
        do: %{state | data: result, current: nil},
        else: %{state | dropped: state.dropped + 1}
    end

    def ignores_identity(state, {:query_result, _job, _correlation, result}) do
      if state.valid?.(), do: %{state | data: result, current: nil}, else: %{state | dropped: state.dropped + 1}
    end
  end
end
