defmodule C1.Mutations do
  @moduledoc """
  TEST SUPPORT ONLY (never product code): witnesses for the U1 mutation rows (docs/contracts/console-mutations.org).
  The console's mutation modules are reached DYNAMICALLY (this support compiles warning-free while the product is
  RED); nothing here models the product's own mechanism. Real core through its public seam and the supported `fs`
  seam (GateFs holds a real Writer transaction, exactly as the MAP spikes did; PartialAppendFs fails the lease
  release append after durable rows: review R1); the CLI's own cancel path for parity; Prepare-call tracing;
  intent/confirm HTTP helpers; witness receipt; an explicit owned-pid tracker whose teardown outlives the test
  process (review R2) and records leaks BEFORE emergency cleanup (review R6).
  """
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias AiOrchestrator.Journal.Fs.SystemFs
  alias AiOrchestrator.Prepare
  alias AiOrchestrator.Prepare.Trusted
  alias C1.Harness

  @store OrrisConsole.SessionStore
  @mods [
    OrrisConsole.Mutations,
    OrrisConsole.MutationRegistry,
    OrrisConsole.MutationWorkers,
    OrrisConsole.MutationOperation,
    OrrisConsole.MutationController
  ]
  @store_api [
    {@store, :issue_intent, 4},
    {@store, :accept, 5},
    {@store, :await, 3},
    {@store, :outcome, 2},
    {@store, :mutation_status, 1}
  ]

  def mods, do: @mods
  def store, do: @store

  @doc "RED attribution for the whole unit: missing modules first, then the missing extended Store API."
  def red!(extra_mods \\ []) do
    Harness.red!(@mods ++ extra_mods)
    red_api!(@store_api)
  end

  @doc "Flunks naming exactly the missing functions (an existing module whose new API is absent is RED, not a crash)."
  def red_api!(fun_arities) do
    missing =
      Enum.reject(fun_arities, fn {mod, fun, arity} ->
        Code.ensure_loaded?(mod) and function_exported?(mod, fun, arity)
      end)

    if missing != [] do
      flunk(
        "RED (missing production API): " <>
          Enum.map_join(missing, ", ", fn {m, f, a} -> "#{inspect(m)}.#{f}/#{a}" end)
      )
    end

    :ok
  end

  # ---- the extended Store API, reached dynamically ----
  def issue_intent(id, root_id, run_ref), do: apply(@store, :issue_intent, [@store, id, root_id, run_ref])
  def accept(id, intent, root_id, run_ref), do: apply(@store, :accept, [@store, id, intent, root_id, run_ref])
  def await(op_ref, ms), do: apply(@store, :await, [@store, op_ref, ms])
  def outcome(id), do: apply(@store, :outcome, [@store, id])
  def status, do: apply(@store, :mutation_status, [@store])
  def login(secret), do: apply(@store, :login, [@store, secret])
  def revoke(id), do: apply(@store, :revoke, [@store, id])
  def revoke_all, do: apply(@store, :revoke_all, [@store])
  def workers, do: Process.whereis(OrrisConsole.MutationWorkers)
  def registry, do: OrrisConsole.MutationRegistry
  def operation_pid(op_ref), do: registry() |> Registry.lookup({:operation, op_ref}) |> Enum.map(&elem(&1, 0))

  @doc "A fresh intent for the session (flunks with the closed refusal; a retained/fenced refusal is a named result)."
  def intent!(id, root_id, run_ref) do
    case issue_intent(id, root_id, run_ref) do
      {:ok, intent} -> intent
      other -> flunk("issue_intent answered #{inspect(other)} for #{root_id}/#{run_ref}")
    end
  end

  @doc """
  Intent then accept for a logged-in session. Answers {:accepted, op_ref, intent}, {:error, reason, intent} (the
  intent is retained so a refused accept can be RETRIED on the SAME intent: a new issue_intent answers :in_progress)
  or {:error, {:intent, reason}} when no intent was issued.
  """
  def accept_now(id, root_id, run_ref) do
    case issue_intent(id, root_id, run_ref) do
      {:ok, intent} ->
        case accept(id, intent, root_id, run_ref) do
          {:accepted, op} -> {:accepted, op, intent}
          {:error, reason} -> {:error, reason, intent}
        end

      {:error, reason} ->
        {:error, {:intent, reason}}
    end
  end

  @doc "accept_now/3 that must be accepted; {op_ref, intent}."
  def accept_now!(id, root_id, run_ref) do
    case accept_now(id, root_id, run_ref) do
      {:accepted, op, intent} -> {op, intent}
      other -> flunk("expected acceptance for #{root_id}/#{run_ref}, got #{inspect(other)}")
    end
  end

  @doc "Whether the (suspended) DynamicSupervisor holds a queued start_child request in its mailbox."
  def queued_start_child?(sup) do
    case Process.info(sup, :messages) do
      {:messages, messages} -> Enum.any?(messages, &match?({:"$gen_call", _from, {:start_child, _spec}}, &1))
      nil -> false
    end
  end

  # ---- pinned configuration for the mutation rows ----
  @doc "Harness.config plus the U1 keys (defaults unless overridden); seams stay nil unless the row sets them."
  def config(overrides \\ []) do
    Harness.config(
      Harness.merged(
        [
          mutation_capacity: 4,
          intent_ttl_ms: 60_000,
          mutation_wait_ms: 5_000,
          mutation_retention_ms: 300_000,
          mutation_shutdown_ms: 60_000,
          mutation_start_ms: 1_000,
          mutation_read_ms: 1_000,
          # server-only trusted seams (in-VM keyword form only): fs/clock/id for Prepare, witness pid, gates,
          # invoke stub
          mutation_opts: [],
          mutation_witness: nil,
          operation_gate: nil,
          operation_finish_gate: nil,
          starter_gate: nil,
          mutation_invoke: nil
        ],
        overrides
      )
    )
  end

  # ---- fixtures: real journals, byte-identical pairs, terminal copies ----
  @pre_dispatch "events_pre_dispatch.jsonl"

  def in_flight!(dir), do: Harness.write_run(dir, @pre_dispatch)

  @doc "A COMPLETED run (the gated_run_seed journal ends with run_completed)."
  def completed!(dir) do
    File.mkdir_p!(dir)
    src = [Harness.core_path(), "test", "fixtures", "contracts", "scenarios", "gated_run_seed", "events.jsonl"]
    File.write!(Path.join(dir, "events.jsonl"), File.read!(Path.join(src)))
    dir
  end

  @doc "Two byte-identical in-flight copies under `root`: {console_dir, cli_dir}."
  def pair!(root) do
    a = in_flight!(Path.join(root, "console"))
    b = in_flight!(Path.join(root, "cli"))
    assert Harness.journal_sha(a) == Harness.journal_sha(b)
    {a, b}
  end

  def events(dir),
    do: dir |> Path.join("events.jsonl") |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  def types(dir), do: Enum.map(events(dir), & &1["type"])
  def lines(dir), do: length(events(dir))

  def cancel_command_ids(dir),
    do:
      dir
      |> events()
      |> Enum.filter(&(&1["type"] == "run_cancel_requested"))
      |> Enum.map(&get_in(&1, ["data", "requested_by", "command_id"]))

  def cancel_requests(dir), do: Enum.filter(events(dir), &(&1["type"] == "run_cancel_requested"))

  @doc "The CLI's own cancel (cli.ex invoke_prepared verbatim): Trusted.cancel + Trusted.invoke with actor class operator."
  def cli_cancel!(run_dir, operator \\ "operator") do
    {:ok, prepared} = Trusted.cancel(run_dir, operator: operator)
    # Trusted.invoke answers {:ok, outcome.(commands_result)}; with the identity outcome that is {:ok, {:ok, result}}
    {:ok, {:ok, result}} =
      Trusted.invoke(%{"class" => "operator", "id" => operator}, prepared, AiOrchestrator.Run.Executor, & &1)

    result
  end

  @doc "A direct console-actor cancel through the PUBLIC seam (harness controls only; the product must not be modelled)."
  def seam_cancel(run_ref, server_opts, actor_id \\ "operator") do
    with {:ok, prepared} <- Prepare.cancel(run_ref, server_opts) do
      Prepare.invoke(%{"class" => "console", "id" => actor_id}, prepared, server_opts)
    end
  end

  # ---- GateFs: the supported :fs seam, holding a REAL Writer transaction (from the MAP spikes) ----
  defmodule GateFs do
    @moduledoc "TEST SEAM: gates the Writer's write (mode :write), close (:close) or open (:open); {:gated, pid} then :release."
    @behaviour AiOrchestrator.Journal.Fs
    def new(gate, mode \\ :write), do: {__MODULE__, %{gate: gate, mode: mode}}
    def mkdir_p(_, d), do: SystemFs.mkdir_p(nil, d)
    def mkdir(_, d), do: SystemFs.mkdir(nil, d)
    def rm(_, p), do: SystemFs.rm(nil, p)
    def rmdir(_, d), do: SystemFs.rmdir(nil, d)
    def link(_, a, b), do: SystemFs.link(nil, a, b)
    def chmod(_, p, m), do: SystemFs.chmod(nil, p, m)
    def lstat(_, p), do: SystemFs.lstat(nil, p)
    def list_dir(_, d), do: SystemFs.list_dir(nil, d)
    def sync(_, fd), do: SystemFs.sync(nil, fd)
    def rename(_, a, b), do: SystemFs.rename(nil, a, b)
    def dir_sync(_, d), do: SystemFs.dir_sync(nil, d)
    def read(_, p), do: SystemFs.read(nil, p)
    def exists?(_, p), do: SystemFs.exists?(nil, p)

    # only the JOURNAL descriptor is ever gated: the Writer opens events.jsonl for append in its own process, so the
    # descriptor is remembered there and the :close gate fires for that descriptor alone (the core's Ownership
    # arbiter closes lock descriptors through the same seam and must never be held: harness precision found at
    # GREEN, where an ungated arbiter blocked every later cancel)
    def open(%{gate: g, mode: mode}, p, m) do
      journal? = String.ends_with?(p, "events.jsonl")
      if journal? and mode == :open, do: hold(g)
      result = SystemFs.open(nil, p, m)
      with true <- journal?, {:ok, fd} <- result, do: Process.put({__MODULE__, :journal_fd}, fd)
      result
    end

    def write(%{gate: g, mode: mode}, fd, iodata) do
      bytes = IO.iodata_to_binary(iodata)
      if mode == :write and bytes =~ "run_cancel_requested", do: hold(g)
      SystemFs.write(nil, fd, bytes)
    end

    def close(%{gate: g, mode: :close}, fd) do
      if Process.get({__MODULE__, :journal_fd}) == fd, do: hold(g)
      SystemFs.close(nil, fd)
    end

    def close(_, fd), do: SystemFs.close(nil, fd)

    defp hold(g) when is_pid(g) do
      send(g, {:gated, self()})
      receive do: (:release -> :ok)
    end

    defp hold(_), do: :ok
  end

  # ---- PartialAppendFs: a REAL partial append (review R1): the lease-release append fails AFTER durable rows ----
  defmodule PartialAppendFs do
    @moduledoc "TEST SEAM: every write carrying `lease_released` fails with :eio; earlier rows of the same command stay durable."
    @behaviour AiOrchestrator.Journal.Fs
    def new, do: {__MODULE__, nil}
    def mkdir_p(_, d), do: SystemFs.mkdir_p(nil, d)
    def mkdir(_, d), do: SystemFs.mkdir(nil, d)
    def rm(_, p), do: SystemFs.rm(nil, p)
    def rmdir(_, d), do: SystemFs.rmdir(nil, d)
    def link(_, a, b), do: SystemFs.link(nil, a, b)
    def chmod(_, p, m), do: SystemFs.chmod(nil, p, m)
    def lstat(_, p), do: SystemFs.lstat(nil, p)
    def list_dir(_, d), do: SystemFs.list_dir(nil, d)
    def open(_, p, m), do: SystemFs.open(nil, p, m)
    def sync(_, fd), do: SystemFs.sync(nil, fd)
    def close(_, fd), do: SystemFs.close(nil, fd)
    def rename(_, a, b), do: SystemFs.rename(nil, a, b)
    def dir_sync(_, d), do: SystemFs.dir_sync(nil, d)
    def read(_, p), do: SystemFs.read(nil, p)
    def exists?(_, p), do: SystemFs.exists?(nil, p)

    def write(_, fd, iodata) do
      bytes = IO.iodata_to_binary(iodata)
      if bytes =~ "lease_released", do: {:error, :eio}, else: SystemFs.write(nil, fd, bytes)
    end
  end

  @doc "The Writer pid announced by the gate, within `ms`."
  def gated!(ms \\ 5_000) do
    receive do
      {:gated, writer} -> writer
    after
      ms -> flunk("no gated Writer within #{ms} ms")
    end
  end

  def release(writer), do: send(writer, :release)

  @doc """
  The Writer's subtree identities (ancestors and their children) captured WHILE it is alive: {pids, monitors}.
  The list must be kept and joined by these monitors; a recapture after death answers only the dead Writer (R6).
  """
  def capture_subtree!(writer) do
    assert Process.alive?(writer), "capture the subtree while the Writer is alive"
    # the gated process must be a REAL run Writer, never a named core singleton (the Ownership arbiter)
    {:dictionary, dict} = Process.info(writer, :dictionary)

    assert Keyword.get(dict, :"$initial_call") == {AiOrchestrator.Journal.Writer, :init, 1},
           "the gate did not hold a Writer"

    pids = subtree(writer)
    named = Enum.filter(pids, &match?({:registered_name, n} when n != [], Process.info(&1, :registered_name)))
    assert named == [], "a captured subtree must hold no registered core process: #{inspect(named)}"
    {pids, Map.new(pids, &{&1, Process.monitor(&1)})}
  end

  @doc "Joins every captured identity through its own monitor within `ms`; answers the survivors (must be [])."
  def join(monitors, ms) do
    deadline = System.monotonic_time(:millisecond) + ms

    monitors
    |> Enum.reject(fn {pid, ref} ->
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> true
      after
        remaining -> false
      end
    end)
    |> Enum.map(&elem(&1, 0))
  end

  def subtree(writer) do
    ancestors =
      case Process.info(writer, :dictionary) do
        {:dictionary, d} -> Enum.filter(d[:"$ancestors"] || [], &is_pid/1)
        _ -> []
      end

    # BOUNDED: Supervisor.which_children/1 calls with :infinity and blocks while the supervisor is terminating a
    # child (a Writer gated inside its close); a supervisor that cannot answer within the bound contributes no
    # children (harness precision found at GREEN: the close leg of M-11 hung here)
    children =
      case ancestors do
        [sup | _] ->
          try do
            sup |> GenServer.call(:which_children, 1_000) |> Enum.map(&elem(&1, 1)) |> Enum.filter(&is_pid/1)
          rescue
            _ -> []
          catch
            :exit, _ -> []
          end

        _ ->
          []
      end

    Enum.uniq([writer | ancestors] ++ children)
  end

  # ---- Prepare-call tracing (the actor-binding witness; same shape as Harness.query_calls) ----
  @doc "Every call into AiOrchestrator.Prepare by ANY process while `fun` runs: {result, [{fun, args}]}."
  def prepare_calls(fun) do
    # a global call pattern binds only to LOADED code: load the module first (a row may be the first caller)
    {:module, Prepare} = Code.ensure_loaded(Prepare)
    tracer = spawn_link(fn -> tracer_loop([]) end)
    :erlang.trace_pattern({Prepare, :_, :_}, true, [:global])
    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    try do
      result = fun.()
      ref = :erlang.trace_delivered(:all)

      receive do
        {:trace_delivered, :all, ^ref} -> :ok
      end

      send(tracer, {:calls, self()})

      receive do
        {:calls, calls} -> {result, calls}
      after
        2_000 -> flunk("Prepare tracer did not answer")
      end
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Prepare, :_, :_}, false, [:global])
      Process.unlink(tracer)
      Process.exit(tracer, :kill)
    end
  end

  defp tracer_loop(acc) do
    receive do
      {:trace, _pid, :call, {Prepare, f, args}} -> tracer_loop([{f, args} | acc])
      {:calls, to} -> send(to, {:calls, Enum.reverse(acc)})
      _other -> tracer_loop(acc)
    end
  end

  def invokes(calls), do: for({:invoke, args} <- calls, do: args)
  def cancels(calls), do: for({:cancel, args} <- calls, do: args)

  # ---- HTTP: intent and confirm forms through the real endpoint ----
  def form(config, cookie, path, fields) do
    Harness.conn(
      config,
      :post,
      path,
      [{"origin", Harness.origin(config)}, {"cookie", cookie}, {"content-type", "application/x-www-form-urlencoded"}],
      URI.encode_query(fields)
    )
  end

  def cancel_path(root_id, run_ref), do: "/runs/#{root_id}/#{run_ref}/cancel"
  def confirm_path(root_id, run_ref), do: cancel_path(root_id, run_ref) <> "/confirm"

  @doc "GET the detail page (its CSRF token) then POST the intent; returns the intent conn."
  def intent(config, cookie, root_id, run_ref, extra \\ %{}) do
    page = Harness.conn(config, :get, "/runs/#{root_id}/#{run_ref}", [{"cookie", cookie}])
    assert page.status == 200, "detail page #{page.status}"
    token = Harness.csrf_token(page.resp_body)
    form(config, cookie, cancel_path(root_id, run_ref), Map.merge(%{"_csrf_token" => token}, extra))
  end

  @doc "The hidden intent token of a confirmation page."
  def intent_token(html) do
    case Regex.run(~r/name="intent"[^>]*value="([^"]+)"/, html) ||
           Regex.run(~r/value="([^"]+)"[^>]*name="intent"/, html) do
      [_, token] -> token
      nil -> flunk("no intent field in the confirmation page: #{String.slice(html, 0, 300)}")
    end
  end

  @doc "POST the confirm form with the page's CSRF token and the intent (plus mutant fields); returns the conn."
  def confirm(config, cookie, root_id, run_ref, confirmation_html, extra \\ %{}) do
    fields = %{"_csrf_token" => Harness.csrf_token(confirmation_html), "intent" => intent_token(confirmation_html)}
    form(config, cookie, confirm_path(root_id, run_ref), Map.merge(fields, extra))
  end

  @doc """
  The confirm POST runs in an OWNED task (the controller blocks in await ≤ mutation_wait_ms): the row can release
  a held operation gate while the request is in flight (review R4). Answers the task; Task.await it for the conn.
  """
  def confirm_async(config, cookie, root_id, run_ref, confirmation_html, extra \\ %{}) do
    Task.async(fn -> confirm(config, cookie, root_id, run_ref, confirmation_html, extra) end)
  end

  @doc "Full browser path: intent, then confirm; {intent_conn, confirm_conn}."
  def cancel!(config, cookie, root_id, run_ref, extra \\ %{}) do
    i = intent(config, cookie, root_id, run_ref)
    assert i.status == 200, "intent answered #{i.status}: #{String.slice(i.resp_body, 0, 200)}"
    {i, confirm(config, cookie, root_id, run_ref, i.resp_body, extra)}
  end

  def forms(html), do: Regex.scan(~r/<form[^>]*action="([^"]+)"/, html) |> Enum.map(&List.last/1)
  def buttons(html), do: Regex.scan(~r/<button[^>]*>([^<]*)</, html) |> Enum.map(&List.last/1)

  @doc "The outcome line rendered on the connected detail view (class outcome), or nil."
  def outcome_line(html) do
    case Regex.run(~r/class="outcome"[^>]*>([^<]*)</, html) do
      [_, text] -> String.trim(text)
      nil -> nil
    end
  end

  # ---- witness receipt (mutation_witness: self()) ----
  @doc "The next {:mutation, kind, op_ref, ...} witness of `kind` (any op_ref when nil); {op_ref, payload}."
  def witness!(kind, op_ref \\ nil, ms \\ 2_000) do
    receive do
      {:mutation, ^kind, ref, payload} when is_nil(op_ref) or ref == op_ref -> {ref, payload}
      {:mutation, ^kind, ref} when is_nil(op_ref) or ref == op_ref -> {ref, nil}
    after
      ms -> flunk("no #{inspect(kind)} witness for #{inspect(op_ref)} within #{ms} ms")
    end
  end

  def refute_witness!(kind, op_ref, ms \\ 300) do
    receive do
      {:mutation, ^kind, ^op_ref, payload} -> flunk("unexpected #{inspect(kind)} witness: #{inspect(payload)}")
      {:mutation, ^kind, ^op_ref} -> flunk("unexpected #{inspect(kind)} witness")
    after
      ms -> :ok
    end
  end

  @doc "The starter helper pid of an accepted operation (witness {:mutation, :starter, op_ref, pid})."
  def starter!(op_ref), do: witness!(:starter, op_ref) |> elem(1)

  # ---- owned-pid tracker (M-17, H-16): unlinked, explicit ownership, leaks recorded before emergency cleanup ----
  defmodule Owned do
    @moduledoc """
    TEST SUPPORT: every pid a row creates or captures is registered with a label. The tracker is NOT linked to the
    test process (ExUnit's on_exit runs after that process has exited: review R2); `teardown/2` resumes any process
    the row suspended, records the still-alive owned pids as LEAKS first, then kills them and joins their monitored
    DOWN (survivors are a second, separate failure), then stops the tracker.
    """
    def start, do: Agent.start(fn -> %{pids: %{}, suspended: MapSet.new()} end)

    def add(agent, pid, label) when is_pid(pid) do
      Agent.update(agent, &put_in(&1, [:pids, pid], label))
      pid
    end

    def add_all(agent, pids, label), do: Enum.each(pids, &add(agent, &1, label))

    @doc "Records that the row suspended `pid` (teardown resumes it before anything blocks on it)."
    def suspended(agent, pid), do: Agent.update(agent, &update_in(&1.suspended, fn s -> MapSet.put(s, pid) end))
    def resumed(agent, pid), do: Agent.update(agent, &update_in(&1.suspended, fn s -> MapSet.delete(s, pid) end))

    def alive(agent), do: agent |> Agent.get(& &1.pids) |> Enum.filter(fn {p, _} -> Process.alive?(p) end)

    @doc "Resume suspended, record leaks, kill + join, stop: %{leaks: [{label, pid}], survivors: [{label, pid}]}."
    def teardown(agent, ms \\ 5_000) do
      state = Agent.get(agent, & &1)

      for pid <- state.suspended, Process.alive?(pid) do
        try do
          :sys.resume(pid, 1_000)
        catch
          _, _ -> :ok
        end
      end

      leaks = for {pid, label} <- state.pids, Process.alive?(pid), do: {label, pid}

      survivors =
        for {label, pid} <- leaks, reduce: [] do
          acc ->
            ref = Process.monitor(pid)
            Process.exit(pid, :kill)

            receive do
              {:DOWN, ^ref, :process, ^pid, _} -> acc
            after
              ms -> [{label, pid} | acc]
            end
        end

      Agent.stop(agent)
      %{leaks: leaks, survivors: Enum.reverse(survivors)}
    end
  end

  @doc """
  An owned tracker for the row whose teardown runs in ExUnit's on_exit AFTER the test process is gone (register it
  LAST so it runs BEFORE the application stop and directory removal registered earlier: on_exit is LIFO). A leak
  fails the row by name; `expect_leaks: true` turns a deliberate leak into a witness sent to the caller instead.
  """
  def owned!(opts \\ []) do
    {:ok, agent} = Owned.start()
    expect_leaks = Keyword.get(opts, :expect_leaks, false)
    witness = Keyword.get(opts, :witness)

    on_exit(fn ->
      result = Owned.teardown(agent)
      if is_pid(witness), do: send(witness, {:owned_teardown, result})

      cond do
        result.survivors != [] -> flunk("owned processes survived emergency cleanup: #{inspect(result.survivors)}")
        result.leaks != [] and not expect_leaks -> flunk("owned processes leaked at teardown: #{inspect(result.leaks)}")
        true -> :ok
      end
    end)

    agent
  end

  # ---- a disposable consumer BEAM holding the run's lock (M-10b; the core's own cross-process helper) ----
  @doc "Starts another OS process holding the Writer lock on `run_dir`; {port, holder_os_pid}; stopped and gone on exit."
  def hold_lock_elsewhere!(run_dir) do
    helper = Path.join([Harness.core_path(), "test", "support", "run_lock_process.exs"])
    paths = [Mix.Project.compile_path() | Path.wildcard(Path.join([Mix.Project.build_path(), "lib", "*", "ebin"]))]
    args = Enum.flat_map(paths, &["-pa", &1]) ++ [helper, run_dir, "60000"]

    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [:binary, :exit_status, :stderr_to_stdout, args: args]
      )

    %{"status" => "acquired", "pid" => holder} = read_json_line(port)
    {:os_pid, os_pid} = Port.info(port, :os_pid)

    on_exit(fn ->
      # closing the Port reaches the holder's stdin EOF: it releases and exits; escalate only if it lingers
      try do
        Port.close(port)
      rescue
        _ -> :ok
      end

      wait_gone(os_pid, 100)
    end)

    {port, holder}
  end

  def release_lock_elsewhere!(port) do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    Port.close(port)
    wait_gone(os_pid, 100)
  end

  defp wait_gone(os_pid, tries) do
    case System.cmd("kill", ["-0", Integer.to_string(os_pid)], stderr_to_stdout: true) do
      {_, 0} when tries > 0 ->
        Process.sleep(50)
        wait_gone(os_pid, tries - 1)

      {_, 0} ->
        System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
        flunk("lock holder #{os_pid} did not exit on stdin EOF; killed")

      _ ->
        :ok
    end
  end

  defp read_json_line(port, buffer \\ "") do
    receive do
      {^port, {:data, chunk}} ->
        case String.split(buffer <> chunk, "\n", parts: 2) do
          [line, _rest] -> Jason.decode!(line)
          [partial] -> read_json_line(port, partial)
        end

      {^port, {:exit_status, status}} ->
        flunk("lock holder exited early with status #{status}: #{inspect(buffer)}")
    after
      20_000 -> flunk("lock holder produced no output: #{inspect(buffer)}")
    end
  end

  # ---- app bootstrap shared by the rows ----
  @doc """
  RED attribution, one in-flight run `a` under root alpha, credential, app started, then the owned tracker (created
  LAST so its teardown runs first); %{config, secret, root, dir, owned}.
  """
  def app!(overrides \\ []) do
    red!()
    root = Harness.fresh("mroot")
    dir = in_flight!(Path.join(root, "a"))
    config = config(Harness.merged([roots: %{"alpha" => root}], overrides))
    secret = Harness.credential!(config)
    Harness.start_app!(config)
    owned = owned!()
    %{config: config, secret: secret, root: root, dir: dir, owned: owned}
  end

  @doc "Login through the endpoint (cookie) AND the raw Store id of that session (for Store-level rows)."
  def session!(config, secret) do
    cookie = Harness.login!(config, secret)
    {id, _} = Harness.raw_session_id(cookie)
    {cookie, id}
  end
end
