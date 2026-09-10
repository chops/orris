defmodule AiOrchestrator.Dispatch.PaneClient do
  @moduledoc false

  alias AiOrchestrator.Config.Runtime
  alias AiOrchestrator.Contract.SensitiveBytes

  @doc """
  Sends a prompt to a pane over `ap`.

  When `:message_id` is given it is transmitted as `--msg-id`, making the ai-pair
  daemon — the only party that observes whether bytes reached the pane — the
  idempotency authority for this send (ruling R3). The id is carried as its own
  argument rather than injected into the prompt, so the bytes pasted stay
  byte-identical to the projected and hashed prompt.
  """
  @spec send(String.t(), SensitiveBytes.t() | String.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def send(pane_ref, prompt, opts \\ [])

  # This is where the bytes leave the system, so this is where the wrapper is opened: the
  # reveal is the last operation before the bytes are staged for `ap`, and nothing above
  # this call ever holds them bare.
  def send(pane_ref, %SensitiveBytes{} = prompt, opts) when is_binary(pane_ref) do
    ["send", pane_ref, "--stdin"]
    |> Kernel.++(message_id_args(opts))
    |> Kernel.++(version_args(opts))
    |> run_ap(Keyword.put(opts, :input, SensitiveBytes.reveal(prompt)))
    |> answered(pane_ref, opts[:message_id])
  end

  def send(pane_ref, prompt, opts) when is_binary(pane_ref) and is_binary(prompt) do
    ["send", pane_ref, "--stdin"]
    |> Kernel.++(message_id_args(opts))
    |> Kernel.++(version_args(opts))
    |> run_ap(Keyword.put(opts, :input, prompt))
    |> answered(pane_ref, opts[:message_id])
  end

  @doc """
  Asks the daemon what became of one previously sent message id.

  Carries no prompt bytes in either direction: the question is a message id and
  the answer is a delivery outcome.
  """
  @spec reconcile(String.t(), String.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def reconcile(pane_ref, message_id, opts \\ []) when is_binary(pane_ref) and is_binary(message_id) do
    # MUST-2: the question is bound to the payload by its digest, never by its bytes. A
    # reconcile that carried only a pane and a message id could answer `delivered` about
    # a prompt the daemon never saw; one that carried the prompt would put it on the wire.
    with {:ok, payload_hash} <- payload_hash(opts) do
      ["reconcile", pane_ref, "--msg-id", message_id, "--payload-hash", payload_hash, "--protocol-version", "2"]
      |> run_ap(opts |> Keyword.delete(:input) |> Keyword.delete(:prompt))
      |> reconcile_reply(pane_ref, message_id)
    end
  end

  @doc """
  Asks the daemon what it can do. R4: a daemon that cannot reconcile has no receipt, so it
  cannot say a prompt is absent; the capability is proven before any side effect.
  """
  @spec capabilities(keyword()) :: {:ok, [String.t()]} | {:error, map()}
  def capabilities(opts \\ []) do
    # M5: merely sending the version flag is not proof the daemon honours it. A ping reply
    # that is not ok, or does not name version 2, authorizes nothing -- least of all a paste.
    case run_ap(["ping", "--protocol-version", "2"], Keyword.delete(opts, :input)) do
      {:ok, %{"ok" => true, "protocol_version" => 2, "capabilities" => tokens}} when is_list(tokens) ->
        if AiOrchestrator.Dispatch.valid_capabilities?(tokens),
          do: {:ok, tokens},
          else: {:error, %{"reason" => "dispatch_capabilities_invalid"}}

      {:ok, %{"ok" => true, "protocol_version" => 2}} ->
        {:ok, []}

      {:ok, %{"ok" => true}} ->
        {:error, %{"reason" => "protocol_version_unsupported"}}

      {:ok, _not_ok} ->
        {:error, %{"reason" => "reply_not_ok"}}

      {:error, _reason} = error ->
        error
    end
  end

  # \A and \z, not ^ and $: `$` matches before a final newline, so "sha256:<64 hex>\n" would
  # pass the grammar and reach the daemon as an argument with a newline in it.
  @hash_pattern ~r/\Asha256:[0-9a-f]{64}\z/

  defp payload_hash(opts) do
    case Keyword.get(opts, :payload_hash) do
      nil ->
        {:error, %{"reason" => "reconcile_payload_hash_missing"}}

      hash when is_binary(hash) ->
        if hash =~ @hash_pattern, do: {:ok, hash}, else: {:error, %{"reason" => "reconcile_payload_hash_invalid"}}

      _other ->
        {:error, %{"reason" => "reconcile_payload_hash_invalid"}}
    end
  end

  # NS-42 rule 1 / MUST-8: a reply with no explicit protocol_version is a v1 reply, and a v1
  # daemon has no durable receipt; an answer about another message, or another pane, says
  # nothing about this one; a reply that is not ok is not an answer either. None may be
  # read as an answer, and the absence of an answer is never `absent`.
  defp reconcile_reply({:ok, reply}, pane_ref, message_id), do: v2_answer(reply, pane_ref, message_id)
  defp reconcile_reply({:error, _reason} = error, _pane_ref, _message_id), do: error

  # A send with no message id in play is the pre-receipt, decode-only shape and passes
  # through as it always did. A send that named a message is a v2 operation and is held to
  # every v2 check: ok, version, and both echoes.
  defp answered(result, _pane_ref, nil), do: result
  defp answered({:ok, reply}, pane_ref, message_id), do: v2_answer(reply, pane_ref, message_id)
  defp answered({:error, _reason} = error, _pane_ref, _message_id), do: error

  defp v2_answer(%{"ok" => true, "protocol_version" => 2} = reply, pane_ref, message_id) do
    case reply do
      %{"msg_id" => ^message_id, "pane_id" => ^pane_ref} -> {:ok, reply}
      %{"msg_id" => ^message_id, "pane_id" => _other} -> {:error, %{"reason" => "reply_pane_mismatch"}}
      %{"msg_id" => ^message_id} -> {:error, %{"reason" => "reply_pane_missing"}}
      %{"msg_id" => _other} -> {:error, %{"reason" => "reply_identity_mismatch"}}
      _no_msg_id -> {:error, %{"reason" => "reply_identity_missing"}}
    end
  end

  defp v2_answer(%{"ok" => true}, _pane_ref, _message_id), do: {:error, %{"reason" => "protocol_version_unsupported"}}
  defp v2_answer(_not_ok, _pane_ref, _message_id), do: {:error, %{"reason" => "reply_not_ok"}}

  # D2: the version is stated on every v2 call; a pre-receipt send with no message id keeps
  # the argument vector it always had.
  defp version_args(opts) do
    case Keyword.get(opts, :message_id) do
      nil -> []
      _message_id -> ["--protocol-version", "2"]
    end
  end

  defp message_id_args(opts) do
    case Keyword.get(opts, :message_id) do
      nil -> []
      message_id when is_binary(message_id) -> ["--msg-id", message_id]
    end
  end

  @spec status(String.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def status(pane_ref, opts \\ []) when is_binary(pane_ref) do
    run_ap(["pane_status", pane_ref], opts)
  end

  defp run_ap(args, opts) do
    with {:ok, config} <- Runtime.resolve(opts) do
      # Transient IPC children never write crash dumps: an orphaned child that
      # inherited ERL_CRASH_DUMP could clobber the product VM's own dump
      # (DD-9 capture incident, 2026-09-01).
      cmd_opts = [stderr_to_stdout: true, env: [{"ERL_CRASH_DUMP_SECONDS", "0"}]]

      case execute(config.ap_path, args, opts, cmd_opts) do
        {output, 0} ->
          decode_last_json(output)

        {output, exit_status} ->
          ap_failure(output, exit_status)
      end
    end
  end

  # The daemon's stdout is the daemon's text: it is not repeated into a diagnostic, decoded
  # or not. The one refusal a reader must be able to name -- a daemon that does not
  # implement the verb -- is recognized from the decoded reply and then the reply is
  # dropped; everything else is reduced to a digest a reader can correlate without
  # reproducing it.
  defp ap_failure(output, exit_status) do
    case decode_last_json(output) do
      {:ok, %{"error" => "unknown command" <> _}} ->
        {:error, %{"reason" => "reconcile_unsupported"}}

      _other ->
        digest = "sha256:" <> Base.encode16(:crypto.hash(:sha256, output), case: :lower)
        {:error, %{"reason" => "ap_failed", "exit_status" => exit_status, "output_digest" => digest}}
    end
  end

  defp execute(ap_path, args, opts, cmd_opts) do
    case Keyword.fetch(opts, :input) do
      {:ok, input} ->
        input_runner = Keyword.get(opts, :input_runner, &run_with_input/4)
        input_runner.(ap_path, args, input, cmd_opts)

      :error ->
        runner = Keyword.get(opts, :runner, &System.cmd/3)
        runner.(ap_path, args, cmd_opts)
    end
  end

  defp run_with_input(executable, args, input, cmd_opts) do
    temp_dir =
      Path.join(
        System.tmp_dir!(),
        "ai-orchestrator-stdin-#{System.pid()}-#{System.unique_integer([:positive, :monotonic])}"
      )

    case stage_input(temp_dir, input) do
      {:ok, input_path} ->
        try do
          System.cmd(
            "/bin/sh",
            [
              "-c",
              ~s(executable=$1; shift; exec "$executable" "$@" < "$0"),
              input_path,
              executable | args
            ],
            cmd_opts
          )
        after
          File.rm_rf(temp_dir)
        end

      {:error, reason} ->
        {"failed to stage stdin: #{inspect(reason)}", 74}
    end
  end

  defp stage_input(temp_dir, input) do
    with :ok <- File.mkdir(temp_dir),
         :ok <- File.chmod(temp_dir, 0o700),
         input_path = Path.join(temp_dir, "prompt"),
         :ok <- File.write(input_path, input) do
      {:ok, input_path}
    end
  end

  @doc """
  Decodes one `ap` reply from its stdout, taking the last JSON object because `ap`
  may print logger and exporter lines first. Exposed so contract tests can prove
  reply shapes that have no PaneClient operation of their own (for example `ping`)
  without pretending such an operation exists.
  """
  @spec decode_reply(String.t()) :: {:ok, map()} | {:error, map()}
  def decode_reply(output) when is_binary(output), do: decode_last_json(output)

  defp decode_last_json(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find_value(&decode_json_line/1)
    |> case do
      nil -> {:error, %{"reason" => "ap_json_missing"}}
      decoded -> {:ok, decoded}
    end
  end

  defp decode_json_line(line) do
    case Jason.decode(line) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _other -> nil
    end
  end
end
