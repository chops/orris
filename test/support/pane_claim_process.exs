alias AiOrchestrator.PaneRegistry.FileRegistry

[root, pane_ref, run_id, hold_ms] = System.argv()

owner = %{
  "run_id" => run_id,
  "run_dir" => Path.join(root, run_id),
  "supervisor_instance" => "sup_#{run_id}"
}

case FileRegistry.claim([pane_ref], owner, root: root) do
  {:ok, claim} ->
    IO.puts(Jason.encode!(%{"status" => "acquired", "pid" => System.pid(), "token" => claim.token}))
    Process.sleep(String.to_integer(hold_ms))
    FileRegistry.release(claim)

  {:error, reason} ->
    IO.puts(Jason.encode!(%{"status" => "rejected", "error" => reason}))
end
