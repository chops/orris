defmodule C1.LifetimeTest do
  @moduledoc "C1-11a..d: the measured worker adapter bound to the product QueryJob; the ordering and link oracles are the ones H-11/H-12 prove discriminating."
  use ExUnit.Case, async: false
  import Phoenix.ConnTest, only: [get: 2]
  import Phoenix.LiveViewTest

  @endpoint OrrisConsole.Endpoint
  alias C1.{Harness, Oracles}
  alias OrrisConsole.QueryJob

  @mods [
    OrrisConsole.QueryJob,
    OrrisConsole.QueryWorkers,
    OrrisConsole.QueryControllers,
    OrrisConsole.QueryRegistry,
    OrrisConsole.Endpoint,
    OrrisConsole.RunIndexLive
  ]

  defp app!(overrides \\ []) do
    Harness.red!(@mods)
    {root, _} = Harness.fixture_root(["a"])

    config =
      Harness.config(
        Harness.merged(
          [roots: %{"alpha" => root}, worker_capacity: 2, controller_capacity: 2, read_deadline_ms: 300, retry_ms: 200],
          overrides
        )
      )

    secret = Harness.credential!(config)
    Harness.start_app!(config)
    %{config: config, secret: secret}
  end

  defp adapter,
    do: %{start: &QueryJob.start/3, registry: OrrisConsole.QueryRegistry, workers: OrrisConsole.QueryWorkers}

  defp view do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp active, do: DynamicSupervisor.count_children(OrrisConsole.QueryWorkers).active

  test "C1-11a results are delivered only after actual worker DOWN; owner kill, view kill, normal stop, suspended worker and supervisor loss leak no slot" do
    app!()
    test = self()
    assert {:ok, job} = QueryJob.start(test, Oracles.held(test), correlation: :c1)
    assert_receive {:reading, worker}
    send(worker, :finish)
    assert_receive {:query_result, ^job, :c1, {:ok, :value}}
    refute Process.alive?(worker)
    Oracles.down(job, :controller_survived)

    for {label, action} <- [
          {:owner_kill, fn j, _w, _v -> Process.exit(j, :kill) end},
          {:view_kill, fn _j, _w, v -> Process.exit(v, :kill) end},
          {:owner_normal_stop, fn j, _w, _v -> GenServer.stop(j) end},
          {:suspended_worker, fn _j, w, _v -> :erlang.suspend_process(w) end}
        ] do
      v = view()
      assert {:ok, j} = QueryJob.start(v, Oracles.held(test), deadline_ms: 150)
      assert_receive {:reading, w}
      action.(j, w, v)
      Oracles.down(w, :worker_survived)
      Oracles.down(j, :controller_survived)
      Process.exit(v, :kill)
      assert Harness.eventually(fn -> active() == 0 end), "#{label}: worker slot leaked"

      assert Harness.eventually(fn -> Registry.count(OrrisConsole.QueryRegistry) == 0 end),
             "#{label}: registry key leaked"
    end

    v = view()
    assert {:ok, j} = QueryJob.start(v, Oracles.held(test))
    assert_receive {:reading, w}
    sup = Process.whereis(OrrisConsole.QueryWorkers)
    Process.exit(sup, :kill)
    Oracles.down(w, :worker_survived)
    Oracles.down(j, :controller_survived)
    Process.sleep(50)
    assert Process.whereis(OrrisConsole.QueryWorkers) not in [nil, sup]
    assert {:ok, j2} = QueryJob.start(v, Oracles.held(test))
    assert_receive {:reading, w2}
    send(w2, :finish)
    Oracles.down(j2, :controller_survived)
  end

  test "C1-11a2 a controller death releases neither the global nor the per-view slot while the worker still lives; controllers are capped separately and a controller key lives until its delivery" do
    app!(worker_capacity: 2, controller_capacity: 1)
    test = self()

    delayed = fn ->
      Process.flag(:trap_exit, true)
      send(test, {:reading, self()})

      receive do
        {:EXIT, _owner, _} ->
          send(test, {:exit_observed, self()})
          receive do: (:finish -> {:ok, :value})

        :finish ->
          {:ok, :value}
      end
    end

    assert {:ok, j} = QueryJob.start(test, delayed)
    assert_receive {:reading, w}
    assert Registry.lookup(OrrisConsole.QueryRegistry, {:controller, test}) == [{j, :controller}]
    # the controller cap (1) refuses another view although a worker slot (2) is free
    assert {:error, :capacity} = QueryJob.start(view(), Oracles.held(test)),
           "the controller cap admitted a second controller"

    assert active() == 1
    Process.exit(j, :kill)
    Oracles.down(j, :controller_survived)
    assert_receive {:exit_observed, ^w}
    assert Process.alive?(w)
    assert active() == 1
    assert Registry.lookup(OrrisConsole.QueryRegistry, {:worker, test}) == [{w, :worker}]
    assert {:error, :view_busy} = QueryJob.start(test, Oracles.held(test))
    send(w, :finish)
    Oracles.down(w, :worker_survived)
    v2 = view()
    assert {:ok, j2} = QueryJob.start(v2, Oracles.held(test))
    assert_receive {:reading, w2}
    assert Registry.lookup(OrrisConsole.QueryRegistry, {:controller, v2}) == [{j2, :controller}]
    send(w2, :finish)
    Oracles.down(j2, :controller_survived)

    assert Harness.eventually(fn -> Registry.lookup(OrrisConsole.QueryRegistry, {:controller, v2}) == [] end),
           "the controller key outlived its delivery"
  end

  test "C1-11b a capacity refusal leaves the view STALE (or unavailable without prior success) while an explicitly held blocker outlives the observation; no admission before release" do
    %{config: c, secret: s} = app!(worker_capacity: 1, retry_ms: 300, read_witness: self())
    cookie = Harness.login!(c, s)
    {:ok, view, _shell} = live(Harness.conn(c, :get, "/", [{"cookie", cookie}]))
    refute Harness.await_read(view) =~ "stale"
    test = self()
    blocker_view = view()
    assert {:ok, blocker} = QueryJob.start(blocker_view, Oracles.held(test), deadline_ms: 60_000)
    assert_receive {:reading, w}

    {_, refused_window} =
      Harness.query_calls(fn ->
        send(view.pid, :refresh)
        Process.sleep(100)
      end)

    assert refused_window == [], "a refused refresh still reached Query"
    assert Process.alive?(w) and Process.alive?(blocker), "the blocker did not outlive the refusal"
    stale = render(view)
    assert stale =~ "stale" and stale =~ "refresh delayed"
    refute stale =~ "error"
    token = Harness.csrf_token(stale)

    out =
      Harness.conn(
        c,
        :post,
        "/logout",
        [{"origin", Harness.origin(c)}, {"cookie", cookie}, {"content-type", "application/x-www-form-urlencoded"}],
        URI.encode_query(%{"_csrf_token" => token})
      )

    assert out.status == 302, "logout blocked behind a held read"
    # a fresh view without prior success while capacity is held: unavailable, and no admission before release
    cookie = Harness.login!(c, s)

    {{:ok, view, _}, before_release} =
      Harness.query_calls(fn ->
        live(Harness.conn(c, :get, "/", [{"cookie", cookie}])) |> tap(fn _ -> Process.sleep(400) end)
      end)

    assert before_release == [], "a read was admitted while the sole slot was held: #{inspect(before_release)}"
    assert Process.alive?(w), "the blocker expired during the observation"
    unavailable = render(view)
    assert unavailable =~ "unavailable" and unavailable =~ "refresh delayed"
    refute unavailable =~ ~s(data-c1-read="complete")
    # explicit release: the window runs until the view's server-side applied-read witness; exactly one retry
    # admission and nothing else
    view_pid = view.pid

    {_, at_retry} =
      Harness.query_calls(fn ->
        send(w, :finish)
        Oracles.down(blocker, :controller_survived)
        # the witness of THIS view (the first view's earlier witnesses may still sit in the mailbox)
        assert_receive {:read_applied, ^view_pid, _corr}, 2_000
      end)

    assert length(Enum.filter(at_retry, &match?({:list_runs, _}, &1))) == 1,
           "expected exactly one retry admission: #{inspect(at_retry)}"

    html = Harness.await_read(view)
    refute html =~ "stale" or html =~ "unavailable"
    assert html =~ ~s(href="/runs/alpha/a")
    assert Harness.eventually(fn -> active() == 0 end), "a worker outlived the periodic refresh"
  end

  test "C1-11c a replacement read is refused at the dead-worker / clean-Registry / controller-DOWN-not-observed cut point and admitted after the controller delivered (H-11 oracle)" do
    app!()
    assert :ok = Oracles.outcome(fn -> Oracles.ordering(adapter()) end)
  end

  test "C1-11d the start acknowledgement precedes the read, and a controller killed before the read proceeds admits no detached read (H-12 oracle); a held read blocks neither another start nor a refusal" do
    app!(worker_capacity: 2, read_deadline_ms: 5_000)
    assert :ok = Oracles.outcome(fn -> Oracles.link_gate(adapter()) end)
    test = self()
    assert {:ok, j1} = QueryJob.start(view(), Oracles.held(test))
    assert_receive {:reading, w1}
    t = System.monotonic_time(:millisecond)
    assert {:ok, j2} = QueryJob.start(view(), Oracles.held(test))
    assert_receive {:reading, w2}
    assert {:error, :capacity} = QueryJob.start(view(), Oracles.held(test))
    assert System.monotonic_time(:millisecond) - t < 200
    for w <- [w1, w2], do: send(w, :finish)
    for j <- [j1, j2], do: Oracles.down(j, :controller_survived)
  end
end
