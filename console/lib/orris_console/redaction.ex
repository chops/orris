defmodule OrrisConsole.Redaction do
  @moduledoc """
  Console-owned log redaction (C1-13): a primary :logger filter that renders every event to text and scrubs the
  configured roots and credential path, 32-byte binary renderings (inspect form, 64 hex, base64) and bearer-shaped
  tokens. A report that cannot be rendered is dropped rather than leaked.
  """

  @doc false
  def install(config) do
    keep_supervisor_reports()
    # replace, never merely add: a filter left by a previous instance would carry that instance's configuration
    :logger.remove_primary_filter(:orris_console_redaction)
    :ok = :logger.add_primary_filter(:orris_console_redaction, {&__MODULE__.filter/2, sensitive(config)})
  end

  # supervisor (SASL) reports are the operator's diagnostic for a crashed child; Elixir's translator drops them unless
  # the Logger application started with handle_sasl_reports (a test runner starts Logger earlier), so the console
  # keeps them explicitly here; every report then passes the scrubbing filter below
  defp keep_supervisor_reports do
    case :logger.get_primary_config().filters[:logger_translator] do
      {fun, %{sasl: false} = cfg} ->
        :logger.remove_primary_filter(:logger_translator)
        :logger.add_primary_filter(:logger_translator, {fun, %{cfg | sasl: true}})

      _ ->
        :ok
    end
  end

  @doc false
  def uninstall, do: :logger.remove_primary_filter(:orris_console_redaction)

  def sensitive(config), do: Enum.uniq([config.credential_path | Map.values(config.roots)])

  def filter(%{msg: msg, meta: meta} = event, sensitive) do
    text = render(msg, meta)
    %{event | msg: {:string, scrub(text, sensitive)}}
  rescue
    _ -> :stop
  catch
    _, _ -> :stop
  end

  defp render({:string, chardata}, _meta), do: IO.chardata_to_string(chardata)

  defp render({:report, report}, meta) do
    case Map.get(meta, :report_cb) do
      cb when is_function(cb, 1) ->
        {format, args} = cb.(report)
        format |> :io_lib.format(args) |> IO.chardata_to_string()

      cb when is_function(cb, 2) ->
        cb.(report, %{depth: :unlimited, chars_limit: :unlimited, single_line: false}) |> IO.chardata_to_string()

      _ ->
        inspect(report, limit: :infinity, printable_limit: :infinity)
    end
  end

  defp render({format, args}, _meta), do: format |> :io_lib.format(args) |> IO.chardata_to_string()

  @doc "Scrubs sensitive literals and secret-shaped tokens from text."
  def scrub(text, sensitive) do
    text
    |> then(fn t -> Enum.reduce(sensitive, t, fn s, acc -> String.replace(acc, s, "[redacted]") end) end)
    |> String.replace(~r/<<(?:\d{1,3}, ){31}\d{1,3}>>/, "[redacted-bytes]")
    |> String.replace(~r/\b[0-9a-fA-F]{64}\b/, "[redacted-hex]")
    |> String.replace(~r/(?<![A-Za-z0-9+\/_-])[A-Za-z0-9+\/_-]{43}={0,1}(?![A-Za-z0-9+\/_-])/, "[redacted-token]")
  end
end
