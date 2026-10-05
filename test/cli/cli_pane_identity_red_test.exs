defmodule AiOrchestrator.CLIPaneIdentityRedTest do
  @moduledoc """
  NS-15.G.005 B3b (scope r2 D2, RED r2): R4.9, the claim-time provenance of a claim's daemon_identity through the
  real run path (`CLI.run` -> Prepare.Trusted). When the run's dispatch module offers `status_v3/2`, each claimed
  pane's version 3 status is read and decoded BEFORE the claim, and the claim receives exactly those decoded
  identities as `daemon_identities: %{pane_ref => %{"pane_id", "registration_id", "generation"}}`; nothing else
  supplies them. A dispatch module without `status_v3/2` passes no identities (legacy claims). The row reads the
  mailbox in arrival order (every claimed pane's read strictly before the claim, none after) and runs twice with
  different identities per pane (salted registration_id and generation), so only identities decoded from the replies
  can match.

  The registry double records the claim call and then rejects it as held, so the run ends at the claim (exit 70, a
  live_holder refusal) and nothing is delivered.

  R4.10 (scope r4 addendum D5): when the claim-time version 3 read yields no valid identity, the run refuses BEFORE
  any claim, and the diagnosis holder comes from the configured registry's read-only `holder/2` snapshot of the
  pane's existing claim file (null when no file; the file's owner fields when valid; {"claim_file": "malformed"}
  when malformed). The double's `holder/2` records each call and delegates to `FileRegistry.holder/2`, so every
  holder case is witnessed through the registry the run is configured with.
  """

  use ExUnit.Case, async: false

  alias AiOrchestrator.CLI
  alias AiOrchestrator.Contracts.FixtureHelper, as: F
  alias AiOrchestrator.PaneRegistry.FileRegistry
  alias AiOrchestrator.Test.FaultFs
  alias AiOrchestrator.Test.GateDouble

  @fixtures Path.expand("../fixtures/contracts/ipc/v3", __DIR__)

  # records the claim call (pane_refs and options), then rejects it as held so the run stops at the claim
  defmodule CapturingRegistry do
    @moduledoc false
    defdelegate pane_refs(spec), to: FileRegistry

    def claim(pane_refs, _owner, opts) do
      send(:persistent_term.get({__MODULE__, :test_pid}), {:claim, pane_refs, opts})
      {:error, %{"reason" => "pane_claim_rejected", "pane_ref" => hd(pane_refs)}}
    end

    def release(_claim), do: :ok

    # the read-only holder snapshot (D5): recorded, then delegated to FileRegistry.holder/2 (reached through a runtime
    # module name so this file compiles before GREEN adds it; GREEN replaces it with a defdelegate)
    def holder(root, pane_ref) do
      send(:persistent_term.get({__MODULE__, :test_pid}), {:holder, pane_ref})
      Module.concat(["AiOrchestrator", "PaneRegistry", "FileRegistry"]).holder(root, pane_ref)
    end
  end

  # version 3 capable: status_v3/2 answers status.ok.json for the asked pane; delivery callbacks report themselves
  defmodule V3Dispatch do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}
    def deliver(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :delivered)
    def observe(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :observed)
    def reconcile(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :reconciled)
    def pane_status(_pane_ref, _opts), do: {:ok, %{"state" => "idle"}}

    def status_v3(pane_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:status_v3, pane_ref})

      case Keyword.fetch!(opts, :reply).(pane_ref) do
        :raise -> raise "no ap"
        result -> result
      end
    end
  end

  # version 1 only
  defmodule V1Dispatch do
    @moduledoc false
    def capabilities(_opts), do: {:ok, ["delivery_reconcile"]}
    def snapshot(_command, _opts), do: {:ok, %{"exists" => false}}
    def deliver(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :delivered)
    def observe(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :observed)
    def reconcile(_command, opts), do: send(Keyword.fetch!(opts, :test_pid), :reconciled)
    def pane_status(_pane_ref, _opts), do: {:ok, %{"state" => "idle"}}
  end

  # a distinct, grammar-valid identity per (salt, pane_ref): a hard-coded identities map cannot match two salts
  defp identity(salt, pane_ref) do
    hex = :sha256 |> :crypto.hash(salt <> ":" <> pane_ref) |> Base.encode16(case: :lower)
    %{"pane_id" => pane_ref, "registration_id" => "reg_" <> binary_part(hex, 0, 32), "generation" => digits(hex)}
  end

  defp digits(hex), do: hex |> binary_part(32, 16) |> String.to_integer(16) |> Integer.to_string()

  defp reply(salt), do: fn pane_ref -> {:ok, status_bytes("status.ok.json", identity(salt, pane_ref))} end

  defp status_bytes(name, ident) do
    Enum.reduce(
      %{
        "<pane_id>" => ident["pane_id"],
        "<registration_id>" => ident["registration_id"],
        "<generation>" => ident["generation"]
      },
      File.read!(Path.join(@fixtures, name)),
      fn {placeholder, value}, bytes -> String.replace(bytes, placeholder, value) end
    )
  end

  defp edit(bytes, fun), do: bytes |> Jason.decode!() |> fun.() |> Jason.encode!()

  defp new_root do
    root = Path.join(System.tmp_dir!(), "cli-pane-identity-registry-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    root
  end

  defp spec_pane_refs, do: FileRegistry.pane_refs(F.json("scenarios", "gated_run_seed", "spec.json"))

  # every message in the mailbox, in arrival order (the run sends from its own process, in call order)
  defp drain(acc \\ []) do
    receive do
      message -> drain([message | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp run(dispatch, reply, root \\ new_root()) do
    :persistent_term.put({CapturingRegistry, :test_pid}, self())
    on_exit(fn -> :persistent_term.erase({CapturingRegistry, :test_pid}) end)
    run_dir = Path.join(System.tmp_dir!(), "cli-pane-identity-#{System.unique_integer([:positive])}")
    File.mkdir_p!(run_dir)
    on_exit(fn -> File.rm_rf!(run_dir) end)
    File.write!(Path.join(run_dir, "spec.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "spec.json")))
    File.write!(Path.join(run_dir, "plan.json"), Jason.encode!(F.json("scenarios", "gated_run_seed", "plan.json")))
    fs = FaultFs.new()

    opts = [
      fs: fs,
      pane_registry: CapturingRegistry,
      pane_registry_root: root,
      dispatch: dispatch,
      dispatch_opts: [test_pid: self(), reply: reply],
      gate_executor: GateDouble,
      gate_helper: GateDouble.helper(),
      review_reader: fn _path -> {:ok, "- Verdict :: clean\n"} end
    ]

    {CLI.run(["run", run_dir], opts), fs}
  end

  test "R4.9 RED a claim made through the run path receives exactly the decoded claim-time version 3 identities" do
    for salt <- ["s1", "s2"] do
      {result, fs} = run(V3Dispatch, reply(salt))
      assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))

      events = Enum.filter(drain(), &(match?({:status_v3, _ref}, &1) or match?({:claim, _refs, _opts}, &1)))
      claim_at = Enum.find_index(events, &match?({:claim, _refs, _opts}, &1))
      assert is_integer(claim_at), "no claim call recorded: #{inspect(events)}"
      {:claim, pane_refs, claim_opts} = Enum.at(events, claim_at)

      # every claimed pane's version 3 read happened BEFORE the claim, once each, and no read followed it
      assert Enum.sort(Enum.take(events, claim_at)) == Enum.sort(Enum.map(pane_refs, &{:status_v3, &1})),
             "the version 3 reads did not precede the claim, one per claimed pane: #{inspect(events)}"

      assert Enum.drop(events, claim_at + 1) == []

      expected = Map.new(pane_refs, &{&1, identity(salt, &1)})

      assert Keyword.get(claim_opts, :daemon_identities) == expected,
             "the claim did not receive the decoded claim-time version 3 identities (salt #{salt})"

      assert FaultFs.trace(fs) == [], "no Writer activity"
      refute_received :delivered
    end

    refute identity("s1", "pane_writer") == identity("s2", "pane_writer")
  end

  test "R4.9 control: a dispatch module without status_v3/2 passes no identities to the claim" do
    {result, fs} = run(V1Dispatch, reply("s1"))

    assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))
    assert_received {:claim, _pane_refs, claim_opts}
    refute Keyword.has_key?(claim_opts, :daemon_identities)
    assert FaultFs.trace(fs) == [], "no Writer activity"
    refute_received :delivered
  end

  # R4.10 (scope r4 addendum D5, GO m_20261005T202029Z): a claim-time version 3 read that yields no valid identity
  # refuses BEFORE any claim; the holder comes from one read-only snapshot of the pane's existing claim file.

  defp failing_reply(:transport), do: fn _pane_ref -> {:error, %{"reason" => "ap_timeout"}} end
  defp failing_reply(:raise), do: fn _pane_ref -> :raise end

  defp failing_reply(:missing_identity),
    do: fn ref -> {:ok, edit(status_bytes("status.ok.json", identity("s1", ref)), &Map.delete(&1, "pane_identity"))} end

  defp failing_reply(:malformed_registration),
    do: fn ref ->
      {:ok, status_bytes("status.ok.json", Map.put(identity("s1", ref), "registration_id", "reg_short"))}
    end

  defp failing_reply(:pane_not_found),
    do: fn ref -> {:ok, status_bytes("status.error.pane_not_found.json", identity("s1", ref))} end

  defp expected(:transport), do: {"daemon_unavailable", &(&1 == %{"source" => "unavailable", "error" => "ap_timeout"})}
  defp expected(:raise), do: {"daemon_unavailable", &(&1 == %{"source" => "unavailable", "error" => "ap_unavailable"})}

  defp expected(kind) when kind in [:missing_identity, :malformed_registration],
    do: {"daemon_unavailable", &match?(%{"source" => "unavailable", "error" => "reply_identity:" <> _detail}, &1)}

  defp expected(:pane_not_found), do: {"unregistered", &match?(%{"source" => "status_v3"}, &1)}

  # exit 70 before any claim call, nothing written by the Writer, nothing delivered; answers the diagnosis
  defp preclaim_refusal!(result, fs) do
    assert match?(%{status: 70, stdout: ""}, result), inspect(Map.take(result, [:status, :stdout]))
    events = drain()
    claims = Enum.filter(events, &match?({:claim, _refs, _opts}, &1))
    assert claims == [], "the run called claim/3 although the claim-time version 3 read failed"
    assert FaultFs.trace(fs) == [], "no Writer activity"
    refute_received :delivered
    object = Jason.decode!(result.stderr)
    assert object["reason"] == "pane_claim_refused"
    diagnosis = object["diagnosis"]

    # exactly ONE bounded holder read, of the diagnosed pane, through the configured registry (scope r4 D5)
    assert Enum.filter(events, &match?({:holder, _pane_ref}, &1)) == [{:holder, diagnosis["pane_ref"]}],
           "expected one holder/2 read of the diagnosed pane through the configured registry: #{inspect(events)}"

    diagnosis
  end

  for kind <- [:transport, :raise, :missing_identity, :malformed_registration, :pane_not_found] do
    test "R4.10 RED a claim-time #{kind} refuses before any claim, holder null when no claim file exists" do
      {result, fs} = run(V3Dispatch, failing_reply(unquote(kind)))
      diagnosis = preclaim_refusal!(result, fs)
      {trigger, observed?} = expected(unquote(kind))

      assert diagnosis["trigger"] == trigger
      assert observed?.(diagnosis["observed_daemon_state"]), inspect(diagnosis["observed_daemon_state"])
      assert diagnosis["pane_ref"] in spec_pane_refs()
      assert Map.has_key?(diagnosis, "holder") and diagnosis["holder"] == nil
      assert diagnosis["daemon_pane_id"] == nil
    end
  end

  test "R4.10 RED a pre-claim refusal names an existing live claimant as holder and leaves its file unchanged" do
    root = new_root()
    refs = spec_pane_refs()
    other = %{"run_id" => "run_other", "run_dir" => "/tmp/run_other", "supervisor_instance" => "sup_other"}
    assert {:ok, held} = FileRegistry.claim(refs, other, root: root)
    on_exit(fn -> FileRegistry.release(held) end)
    before = Map.new(refs, &{&1, File.read!(FileRegistry.claim_path(root, &1))})

    {result, fs} = run(V3Dispatch, failing_reply(:transport), root)
    diagnosis = preclaim_refusal!(result, fs)

    file = before |> Map.fetch!(diagnosis["pane_ref"]) |> Jason.decode!()
    assert diagnosis["holder"] == Map.take(file, ~w(run_id run_dir pid pid_start acquired_at_unix))
    assert Map.new(refs, &{&1, File.read!(FileRegistry.claim_path(root, &1))}) == before
  end

  test "R4.10 RED a pre-claim refusal reports a malformed claim file as such and leaves it unchanged" do
    root = new_root()
    refs = spec_pane_refs()

    for ref <- refs do
      path = FileRegistry.claim_path(root, ref)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "not a claim")
    end

    {result, fs} = run(V3Dispatch, failing_reply(:transport), root)
    diagnosis = preclaim_refusal!(result, fs)

    assert diagnosis["holder"] == %{"claim_file" => "malformed"}
    assert Enum.all?(refs, &(File.read!(FileRegistry.claim_path(root, &1)) == "not a claim"))
  end
end
