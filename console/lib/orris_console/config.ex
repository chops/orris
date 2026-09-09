defmodule OrrisConsole.Config do
  @moduledoc """
  Trusted server configuration (docs/contracts/console-readonly.org, C1-00). Loaded once at application start from
  the in-VM keyword (dev/test) or the closed JSON file (release); never from a request. `load/1` validates every
  clause and derives the exact lowercase authority and origin; `current/0` answers the running configuration.
  """

  @max_idle_ms 1_800_000
  @max_absolute_ms 43_200_000
  @file_keys ~w(bind port host scheme socket_mounts credential_path operator roots limits)
  @limit_keys ~w(idle_ms absolute_ms login_capacity login_refill_ms session_capacity views_per_session read_deadline_ms retry_ms worker_capacity controller_capacity max_login_body)

  defstruct bind: {127, 0, 0, 1},
            port: nil,
            host: nil,
            scheme: :http,
            authority: nil,
            origin: nil,
            socket_mounts: ["/live"],
            credential_path: nil,
            operator: nil,
            roots: %{},
            idle_ms: @max_idle_ms,
            absolute_ms: @max_absolute_ms,
            login_capacity: 5,
            login_refill_ms: 6_000,
            session_capacity: 128,
            views_per_session: 8,
            read_deadline_ms: 2_000,
            retry_ms: 1_000,
            worker_capacity: 128,
            controller_capacity: 128,
            max_login_body: 4_096,
            sweep_ms: 1_000,
            server: false,
            query_opts: [],
            read_gate: nil,
            read_witness: nil,
            clock: nil

  @type t :: %__MODULE__{}
  @type rejection :: %{clause: String.t(), detail: map() | nil}

  @spec current() :: t()
  def current, do: :persistent_term.get({__MODULE__, :current})

  @doc false
  def install(%__MODULE__{} = config), do: :persistent_term.put({__MODULE__, :current}, config)

  @doc "Validates a keyword (dev/test) into the configuration struct or a closed rejection."
  @spec load(keyword()) :: {:ok, t()} | {:error, rejection()}
  def load(input) when is_list(input) do
    with :ok <- refuse_proxy(input),
         {:ok, bind} <- bind(Keyword.get(input, :bind, {127, 0, 0, 1})),
         {:ok, host, port} <-
           authority(Keyword.get(input, :host), Keyword.get(input, :port), Keyword.get(input, :scheme, :http)),
         {:ok, roots} <- roots(Keyword.get(input, :roots, %{})),
         {:ok, operator} <- operator(Keyword.get(input, :operator), roots),
         {:ok, credential_path} <- credential_path(Keyword.get(input, :credential_path)),
         {:ok, idle, absolute} <-
           expiry(Keyword.get(input, :idle_ms, @max_idle_ms), Keyword.get(input, :absolute_ms, @max_absolute_ms)),
         {:ok, mounts} <- mounts(Keyword.get(input, :socket_mounts, ["/live"])) do
      {:ok,
       %__MODULE__{
         bind: bind,
         port: port,
         host: host,
         scheme: :http,
         authority: "#{host}:#{port}",
         origin: "http://#{host}:#{port}",
         socket_mounts: mounts,
         credential_path: credential_path,
         operator: operator,
         roots: roots,
         idle_ms: idle,
         absolute_ms: absolute,
         login_capacity: positive(input, :login_capacity, 5),
         login_refill_ms: positive(input, :login_refill_ms, 6_000),
         session_capacity: positive(input, :session_capacity, 128),
         views_per_session: positive(input, :views_per_session, 8),
         read_deadline_ms: positive(input, :read_deadline_ms, 2_000),
         retry_ms: positive(input, :retry_ms, 1_000),
         worker_capacity: positive(input, :worker_capacity, 128),
         controller_capacity: positive(input, :controller_capacity, 128),
         max_login_body: positive(input, :max_login_body, 4_096),
         sweep_ms: positive(input, :sweep_ms, 1_000),
         server: Keyword.get(input, :server, false) == true,
         query_opts:
           Keyword.get(input, :query_opts, []) |> List.wrap() |> Keyword.take([:monitor, :budget_ms, :ownership]),
         read_gate: seam(Keyword.get(input, :read_gate)),
         read_witness: seam(Keyword.get(input, :read_witness)),
         clock: Keyword.get(input, :clock)
       }}
    end
  end

  def load(_other), do: {:error, %{clause: "config_invalid", detail: nil}}

  @doc "Loads the closed JSON configuration file (release); unknown keys or malformed data are refused."
  @spec load_file(Path.t()) :: {:ok, t()} | {:error, rejection()}
  def load_file(path) do
    with {:ok, bytes} <- read_file(path),
         {:ok, data} <- decode(bytes),
         {:ok, input} <- from_file(data) do
      load(input)
    end
  end

  defp read_file(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, :enoent} -> {:error, %{clause: "config_file_missing", detail: nil}}
      {:error, reason} -> {:error, %{clause: "config_file_invalid", detail: %{reason: inspect(reason)}}}
    end
  end

  defp decode(bytes) do
    case Jason.decode(bytes) do
      {:ok, %{} = data} -> {:ok, data}
      _ -> {:error, %{clause: "config_file_invalid", detail: nil}}
    end
  end

  defp from_file(data) do
    limits = Map.get(data, "limits", %{})

    cond do
      not Enum.all?(Map.keys(data), &(&1 in @file_keys)) ->
        {:error, %{clause: "config_file_invalid", detail: %{unknown_keys: Map.keys(data) -- @file_keys}}}

      not is_map(limits) or not Enum.all?(Map.keys(limits), &(&1 in @limit_keys)) ->
        {:error, %{clause: "config_file_invalid", detail: %{limits: :invalid}}}

      true ->
        with {:ok, bind} <- parse_bind(Map.get(data, "bind", "127.0.0.1")),
             {:ok, operator} <- parse_operator(Map.get(data, "operator")) do
          base = [
            bind: bind,
            port: Map.get(data, "port"),
            host: Map.get(data, "host"),
            scheme: if(Map.get(data, "scheme", "http") == "http", do: :http, else: :invalid),
            socket_mounts: Map.get(data, "socket_mounts", ["/live"]),
            credential_path: Map.get(data, "credential_path"),
            operator: operator,
            roots: Map.get(data, "roots", %{}),
            # the release exists to serve: the closed file carries no server flag
            server: true
          ]

          {:ok, base ++ Enum.map(limits, fn {k, v} -> {String.to_existing_atom(k), v} end)}
        end
    end
  end

  defp parse_bind(text) when is_binary(text) do
    case :inet.parse_address(String.to_charlist(text)) do
      {:ok, ip} -> {:ok, ip}
      _ -> {:error, %{clause: "config_bind_not_loopback", detail: nil}}
    end
  end

  defp parse_bind(_), do: {:error, %{clause: "config_bind_not_loopback", detail: nil}}

  defp parse_operator(%{"id" => id, "root_ids" => ids}) when is_binary(id) and is_list(ids),
    do: {:ok, %{id: id, root_ids: ids}}

  defp parse_operator(_), do: {:error, %{clause: "config_operator_invalid", detail: nil}}

  # ---- clauses ----
  defp refuse_proxy(input) do
    if Keyword.has_key?(input, :trust_forwarded) or Keyword.has_key?(input, :proxy),
      do: {:error, %{clause: "config_proxy_refused", detail: nil}},
      else: :ok
  end

  defp bind({127, _, _, _} = ip), do: {:ok, ip}
  defp bind({0, 0, 0, 0, 0, 0, 0, 1} = ip), do: {:ok, ip}
  defp bind(_), do: {:error, %{clause: "config_bind_not_loopback", detail: nil}}

  defp authority(host, port, scheme) do
    with true <- is_binary(host) and host != "" and scheme == :http and is_integer(port) and port > 0 and port < 65_536,
         lower = String.downcase(host),
         true <- Regex.match?(~r/^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/, lower) do
      {:ok, lower, port}
    else
      _ -> {:error, %{clause: "config_authority_invalid", detail: nil}}
    end
  end

  defp roots(roots) when is_map(roots) do
    valid? =
      Enum.all?(roots, fn
        {id, dir} when is_binary(id) and is_binary(dir) ->
          Regex.match?(~r/^[A-Za-z0-9_-]{1,64}$/, id) and String.starts_with?(dir, "/")

        _ ->
          false
      end)

    if valid?, do: {:ok, roots}, else: {:error, %{clause: "config_roots_invalid", detail: nil}}
  end

  defp roots(_), do: {:error, %{clause: "config_roots_invalid", detail: nil}}

  # every operator root id must be a configured root; with no roots configured the operator simply has none
  defp operator(%{id: id, root_ids: ids}, roots) when is_binary(id) and id != "" and is_list(ids) do
    cond do
      roots == %{} -> {:ok, %{id: id, root_ids: []}}
      ids != [] and Enum.all?(ids, &(is_binary(&1) and Map.has_key?(roots, &1))) -> {:ok, %{id: id, root_ids: ids}}
      true -> {:error, %{clause: "config_operator_invalid", detail: nil}}
    end
  end

  defp operator(_, _), do: {:error, %{clause: "config_operator_invalid", detail: nil}}

  defp credential_path(path) when is_binary(path) do
    if String.starts_with?(path, "/"),
      do: {:ok, path},
      else: {:error, %{clause: "config_credential_path_missing", detail: nil}}
  end

  defp credential_path(_), do: {:error, %{clause: "config_credential_path_missing", detail: nil}}

  defp expiry(idle, absolute)
       when is_integer(idle) and idle > 0 and idle <= @max_idle_ms and is_integer(absolute) and absolute > 0 and
              absolute <= @max_absolute_ms,
       do: {:ok, idle, absolute}

  defp expiry(_, _), do: {:error, %{clause: "config_expiry_exceeds_default", detail: nil}}

  defp mounts(mounts) when is_list(mounts) and mounts != [] do
    if Enum.all?(mounts, &(is_binary(&1) and Regex.match?(~r|^/[A-Za-z0-9_-]+$|, &1))),
      do: {:ok, mounts},
      else: {:error, %{clause: "config_socket_mounts_invalid", detail: nil}}
  end

  defp mounts(_), do: {:error, %{clause: "config_socket_mounts_invalid", detail: nil}}

  defp positive(input, key, default) do
    case Keyword.get(input, key, default) do
      n when is_integer(n) and n > 0 -> n
      _ -> default
    end
  end

  defp seam(pid) when is_pid(pid), do: pid
  defp seam(_), do: nil
end
