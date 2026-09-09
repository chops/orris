defmodule C1.MutationParityTest do
  @moduledoc "M-06 journal parity with the CLI's own cancel path, M-07 idempotency; H-17 proves the CLI witness on real fixtures at every head."
  use ExUnit.Case, async: false
  alias AiOrchestrator.Query
  alias C1.{Harness, Mutations}
  alias C1.Mutations, as: Mut

  @moduletag timeout: 60_000
  @path_fields ~w(run_dir path run_lock_path lock_path)

  # requested_by class/id, command_id, the per-invocation identities (event_id, ts, supervisor_instance), the chain
  # hash derived from them (prev_line_sha256) and path-bearing fields are the ONLY allowed differences (supervisor_instance named at GREEN: a fresh identity per
  # invocation exactly like command_id; harness precision, not a product change)
  defp strip(event) do
    event
    |> Map.drop(["event_id", "ts", "prev_line_sha256"])
    |> Map.update("data", %{}, fn data ->
      data
      |> Map.drop(@path_fields ++ ["supervisor_instance"])
      |> Map.update("requested_by", nil, fn
        %{} = by -> Map.drop(by, ["class", "id", "command_id"])
        other -> other
      end)
    end)
  end

  # H-17 (control) the CLI's own cancel on a real in-flight fixture appends run_cancel_requested with reason operator_cancel and the lease releases before run_cancelled; the run is observed cancelled; a completed fixture is unchanged

  test "H-17 (control) the CLI's own cancel on a real in-flight fixture: run_cancel_requested with reason operator_cancel, lease releases before run_cancelled, observed cancelled; a completed fixture is unchanged" do
    root = Harness.fresh("h17")
    dir = Mut.in_flight!(Path.join(root, "run"))
    before = Mut.types(dir)
    assert %{events: _} = Mut.cli_cancel!(dir, "cli-op")
    types = Mut.types(dir)
    assert Enum.take(types, length(before)) == before
    appended = Enum.drop(types, length(before))
    assert hd(appended) == "run_cancel_requested" and List.last(appended) == "run_cancelled"

    assert Enum.any?(appended, &String.ends_with?(&1, "lease_released")),
           "no lease release between request and cancelled: #{inspect(appended)}"

    [request] = Mut.cancel_requests(dir)
    assert request["data"]["reason"] == "operator_cancel"
    assert request["data"]["requested_by"]["class"] == "operator" and request["data"]["requested_by"]["id"] == "cli-op"
    assert {:ok, %{status: "cancelled", last_seq: seq}} = Query.run_summary("run", root: root)
    assert seq == length(types)
    done = Mut.completed!(Path.join(root, "done"))
    sha = Harness.journal_sha(done)
    assert %{events: _} = Mut.cli_cancel!(done, "cli-op")
    assert Harness.journal_sha(done) == sha
    assert {:ok, %{status: "completed"}} = Query.run_summary("done", root: root)
  end

  # M-06 a console cancel and the CLI's cancel on byte-identical copies append the same type sequence (lease releases included) with reason operator_cancel and equal Fold status/last_seq; differences limited to requested_by class/id, command_id, event_id, ts and path fields; run_cancelled unstamped; terminal fixtures observed as such with bytes unchanged

  test "M-06 console vs CLI cancel on byte-identical copies: same type sequence and reason, equal status/last_seq; differences limited to requested_by/command_id/event_id/ts/paths; run_cancelled unstamped; terminal unchanged" do
    %{config: c, secret: s, root: root} = Mut.app!()
    {console_dir, cli_dir} = Mut.pair!(root)
    Mut.completed!(Path.join(root, "done"))
    cookie = Harness.login!(c, s)
    {_, confirm} = Mut.cancel!(c, cookie, "alpha", "console")
    assert confirm.status == 302, "RED (U1 M-06): confirm answered #{confirm.status}"
    assert %{events: _} = Mut.cli_cancel!(cli_dir, "cli-op")
    console_events = Mut.events(console_dir)
    cli_events = Mut.events(cli_dir)
    assert Enum.map(console_events, & &1["type"]) == Enum.map(cli_events, & &1["type"])
    assert Enum.map(console_events, &strip/1) == Enum.map(cli_events, &strip/1)
    [console_request] = Mut.cancel_requests(console_dir)
    [cli_request] = Mut.cancel_requests(cli_dir)
    assert console_request["data"]["reason"] == "operator_cancel"
    assert console_request["data"]["requested_by"]["class"] == "console"
    assert console_request["data"]["requested_by"]["id"] == c[:operator].id
    assert cli_request["data"]["requested_by"]["class"] == "operator"
    assert console_request["data"]["requested_by"]["command_id"] != cli_request["data"]["requested_by"]["command_id"]
    cancelled = Enum.find(console_events, &(&1["type"] == "run_cancelled"))
    refute Map.has_key?(cancelled["data"] || %{}, "requested_by"), "run_cancelled is stamped"
    {:ok, a} = Query.run_summary("console", root: root)
    {:ok, b} = Query.run_summary("cli", root: root)
    assert a.status == "cancelled" and a.status == b.status and a.last_seq == b.last_seq
    # terminal fixtures: observed as such, bytes unchanged (the cancelled copy and the completed run)
    for {ref, status} <- [{"console", "cancelled"}, {"done", "completed"}] do
      dir = Path.join(root, ref)
      sha = Harness.journal_sha(dir)
      {_, confirm} = Mut.cancel!(c, cookie, "alpha", ref)
      assert confirm.status == 302
      assert Harness.journal_sha(dir) == sha, "#{ref}: bytes changed by a cancel on a terminal run"
      assert {:ok, %{status: ^status}} = Query.run_summary(ref, root: root)
    end
  end

  test "M-07 a replayed confirm (the same intent) is refused without a second invoke; a fresh second cancel on the cancelled copy observes cancelled at the same seq with bytes unchanged" do
    %{config: c, secret: s, root: root} = Mut.app!()
    cookie = Harness.login!(c, s)
    intent_page = Mut.intent(c, cookie, "alpha", "a")
    assert intent_page.status == 200, "RED (U1 M-07): intent answered #{intent_page.status}"
    {first, calls} = Mut.prepare_calls(fn -> Mut.confirm(c, cookie, "alpha", "a", intent_page.resp_body) end)
    assert first.status == 302 and length(Mut.invokes(calls)) == 1
    {:ok, %{status: "cancelled", last_seq: seq}} = Query.run_summary("a", root: root)
    sha = Harness.journal_sha(root <> "/a")
    {replay, calls} = Mut.prepare_calls(fn -> Mut.confirm(c, cookie, "alpha", "a", intent_page.resp_body) end)
    assert replay.status in [302, 409, 422] and calls == [], "the replayed confirm invoked Prepare again"
    assert Harness.journal_sha(root <> "/a") == sha
    {{_, second}, calls} = Mut.prepare_calls(fn -> Mut.cancel!(c, cookie, "alpha", "a") end)
    assert second.status == 302 and length(Mut.invokes(calls)) == 1
    assert Harness.journal_sha(root <> "/a") == sha
    assert {:ok, %{status: "cancelled", last_seq: ^seq}} = Query.run_summary("a", root: root)
    {id, _} = Harness.raw_session_id(cookie)
    assert {:ok, %{state: :finished, outcome: %{observed: %{status: "cancelled", last_seq: ^seq}}}} = Mut.outcome(id)
    refute inspect(Mutations.status()) =~ root
  end
end
