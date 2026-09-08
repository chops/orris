defmodule AiOrchestrator.PaneRegistry.FileRegistryTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.PaneRegistry.FileRegistry

  setup do
    root = Path.join(System.tmp_dir!(), "ai_orchestrator_pane_registry_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "publishes complete claims and releases only with the owning token", %{root: root} do
    assert {:ok, claim} = FileRegistry.claim(["pane_writer"], owner("run_a"), claim_opts(root, "token_a"))

    path = FileRegistry.claim_path(root, "pane_writer")
    metadata = path |> File.read!() |> Jason.decode!()

    assert metadata["token"] == "token_a"
    assert metadata["pane_ref"] == "pane_writer"
    assert metadata["run_id"] == "run_a"
    assert metadata["pid"] == "41001"
    assert metadata["pid_start"] == "start_41001"

    assert {:error, %{"reason" => "pane_claim_not_owned"}} =
             FileRegistry.release(%{claim | token: "not_the_owner"})

    assert File.exists?(path)
    assert :ok = FileRegistry.release(claim)
    refute File.exists?(path)
  end

  test "rejects a live owner and reports actionable ownership", %{root: root} do
    assert {:ok, existing} = FileRegistry.claim(["pane_shared"], owner("run_a"), claim_opts(root, "token_a"))

    opts = claim_opts(root, "token_b", owner_status: fn _metadata -> :live end)

    assert {:error,
            %{
              "reason" => "pane_claim_rejected",
              "pane_ref" => "pane_shared",
              "owner" => %{"run_id" => "run_a", "pid" => "41001"}
            }} = FileRegistry.claim(["pane_shared"], owner("run_b"), opts)

    assert :ok = FileRegistry.release(existing)
  end

  test "reclaims a dead owner and a reused pid with a different start identity", %{root: root} do
    assert {:ok, _orphan} = FileRegistry.claim(["pane_shared"], owner("run_a"), claim_opts(root, "token_a"))

    status = fn metadata ->
      if metadata["pid_start"] == "start_41001", do: :dead, else: :live
    end

    assert {:ok, replacement} =
             FileRegistry.claim(
               ["pane_shared"],
               owner("run_b"),
               claim_opts(root, "token_b", pid: "41001", pid_start: "start_reused", owner_status: status)
             )

    metadata = root |> FileRegistry.claim_path("pane_shared") |> File.read!() |> Jason.decode!()
    assert metadata["token"] == "token_b"
    assert metadata["pid_start"] == "start_reused"
    assert :ok = FileRegistry.release(replacement)
  end

  test "rechecks ownership under the reclaim mutex and never unlinks a fresh owner", %{root: root} do
    assert {:ok, existing} = FileRegistry.claim(["pane_shared"], owner("run_a"), claim_opts(root, "token_a"))
    {:ok, calls} = Agent.start_link(fn -> 0 end)

    status = fn _metadata ->
      Agent.get_and_update(calls, fn count -> {if(count == 0, do: :dead, else: :live), count + 1} end)
    end

    assert {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_shared"}} =
             FileRegistry.claim(
               ["pane_shared"],
               owner("run_b"),
               claim_opts(root, "token_b", owner_status: status)
             )

    assert root |> FileRegistry.claim_path("pane_shared") |> File.read!() |> Jason.decode!() |> Access.get("token") ==
             "token_a"

    assert :ok = FileRegistry.release(existing)
  end

  test "overlapping rosters are all-or-none and roll back partial claims", %{root: root} do
    assert {:ok, existing} = FileRegistry.claim(["pane_b"], owner("run_a"), claim_opts(root, "token_a"))

    assert {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => "pane_b"}} =
             FileRegistry.claim(
               ["pane_c", "pane_a", "pane_b"],
               owner("run_b"),
               claim_opts(root, "token_b", owner_status: fn _metadata -> :live end)
             )

    refute File.exists?(FileRegistry.claim_path(root, "pane_a"))
    assert File.exists?(FileRegistry.claim_path(root, "pane_b"))
    refute File.exists?(FileRegistry.claim_path(root, "pane_c"))
    assert :ok = FileRegistry.release(existing)
  end

  test "release continues cleaning later panes after a token mismatch", %{root: root} do
    assert {:ok, claim} = FileRegistry.claim(["pane_a", "pane_b"], owner("run_a"), claim_opts(root, "token_a"))
    pane_b_path = FileRegistry.claim_path(root, "pane_b")
    pane_b = pane_b_path |> File.read!() |> Jason.decode!() |> Map.put("token", "foreign_token")
    File.write!(pane_b_path, Jason.encode!(pane_b))

    assert {:error, %{"reason" => "pane_claim_not_owned", "pane_ref" => "pane_b"}} =
             FileRegistry.release(claim)

    refute File.exists?(FileRegistry.claim_path(root, "pane_a"))
    assert File.exists?(pane_b_path)
  end

  test "stale reclaim mutex is broken and a fresh mutex fails within the wait bound", %{root: root} do
    assert {:ok, _existing} = FileRegistry.claim(["pane_shared"], owner("run_a"), claim_opts(root, "token_a"))
    mutex = Path.join(root, ".reclaim-lock")
    File.mkdir!(mutex)
    File.write!(Path.join(mutex, "token"), "dead_mutex")
    future = System.os_time(:second) + 10

    assert {:ok, replacement} =
             FileRegistry.claim(
               ["pane_shared"],
               owner("run_b"),
               claim_opts(root, "token_b",
                 owner_status: fn _metadata -> :dead end,
                 now_unix: fn -> future end,
                 mutex_ttl_s: 0
               )
             )

    assert :ok = FileRegistry.release(replacement)

    assert {:ok, existing} = FileRegistry.claim(["pane_shared"], owner("run_c"), claim_opts(root, "token_c"))
    File.mkdir!(mutex)
    File.write!(Path.join(mutex, "token"), "live_mutex")
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    monotonic = fn -> Agent.get_and_update(clock, &{&1, &1 + 10}) end

    assert {:error, %{"reason" => "pane_registry_unavailable", "detail" => "reclaim mutex is busy"}} =
             FileRegistry.claim(
               ["pane_shared"],
               owner("run_d"),
               claim_opts(root, "token_d",
                 owner_status: fn _metadata -> :dead end,
                 now_unix: fn -> System.os_time(:second) end,
                 monotonic_ms: monotonic,
                 mutex_wait_ms: 15,
                 mutex_ttl_s: 60
               )
             )

    File.rm_rf!(mutex)
    assert :ok = FileRegistry.release(existing)
  end

  test "malformed claims fail closed and remain untouched", %{root: root} do
    path = FileRegistry.claim_path(root, "pane_shared")
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "not-json")

    assert {:error, %{"reason" => "pane_claim_malformed", "path" => ^path}} =
             FileRegistry.claim(["pane_shared"], owner("run_b"), claim_opts(root, "token_b"))

    assert File.read!(path) == "not-json"
  end

  test "claim paths hash pane refs instead of embedding operator input", %{root: root} do
    first_ref = "../../" <> "%" <> "42 pane"
    second_ref = "../../" <> "%" <> "43 pane"
    first = FileRegistry.claim_path(root, first_ref)
    second = FileRegistry.claim_path(root, second_ref)

    assert Path.dirname(first) == Path.expand(root)
    assert Path.basename(first) =~ ~r/^pane-[0-9a-f]{64}\.json$/
    refute first == second
    refute first =~ "%" <> "42"
  end

  test "two concurrent claimants produce exactly one winner", %{root: root} do
    parent = self()

    tasks =
      for {run_id, token} <- [{"run_a", "token_a"}, {"run_b", "token_b"}] do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> FileRegistry.claim(["pane_shared"], owner(run_id), claim_opts(root, token))
          end
        end)
      end

    pids =
      for _index <- 1..2 do
        assert_receive {:ready, pid}
        pid
      end

    Enum.each(pids, &send(&1, :go))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert [winner] = for({:ok, claim} <- results, do: claim)
    assert [_loser] = for({:error, %{"reason" => "pane_claim_rejected"} = error} <- results, do: error)
    assert :ok = FileRegistry.release(winner)
  end

  defp owner(run_id) do
    %{
      "run_id" => run_id,
      "run_dir" => "/tmp/#{run_id}",
      "supervisor_instance" => "sup_#{run_id}"
    }
  end

  defp claim_opts(root, token, overrides \\ []) do
    Keyword.merge(
      [
        root: root,
        token_fun: fn -> token end,
        pid: "41001",
        pid_start: "start_41001",
        owner_status: fn metadata ->
          if metadata["pid"] == "41001" and metadata["pid_start"] == "start_41001", do: :live, else: :dead
        end,
        now_unix: fn -> 1_788_220_000 end,
        sleep: fn _milliseconds -> :ok end
      ],
      overrides
    )
  end
end
