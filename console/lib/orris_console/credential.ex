defmodule OrrisConsole.Credential do
  @moduledoc """
  The console-local operator credential (C1-02): 32 random bytes in an exclusively created regular file, directory
  0700 before the file, file 0600 BEFORE any secret byte, owner checked against a VM-created probe file (never the
  environment). Load validates type, mode, owner and length and answers the SHA-256 digest only.
  """

  @type rejection :: %{clause: String.t(), detail: map() | nil}
  @secret_bytes 32

  @spec setup(Path.t(), keyword()) :: :ok | {:error, rejection()}
  def setup(path, opts \\ []) do
    with {:ok, uid} <- probe_uid(opts),
         :ok <- parent(Path.dirname(path), uid),
         :ok <- absent(path),
         {:ok, device} <- open_exclusive(path),
         :ok <- owned(path, uid, device),
         :ok <- File.chmod(path, 0o600) |> closed("credential_write_failed"),
         :ok <- write(device, :crypto.strong_rand_bytes(@secret_bytes), path) do
      :ok
    end
  end

  @spec load(Path.t(), keyword()) :: {:ok, binary()} | {:error, rejection()}
  def load(path, opts \\ []) do
    with {:ok, uid} <- probe_uid(opts),
         {:ok, stat} <- lstat(path),
         :ok <- regular(stat),
         :ok <- mode(stat),
         :ok <- owner(stat, uid),
         {:ok, bytes} <- bytes(path) do
      {:ok, digest(bytes)}
    end
  end

  @spec digest(binary()) :: binary()
  def digest(bytes) when is_binary(bytes), do: :crypto.hash(:sha256, bytes)

  # ---- ownership witness: the VM creates a fresh regular file inside its own private directory and reads its uid ----
  defp probe_uid(opts) do
    case Keyword.get(opts, :probe_uid) do
      probe when is_function(probe, 0) -> normalize_probe(probe.())
      nil -> normalize_probe(vm_probe())
    end
  end

  defp normalize_probe({:ok, uid}) when is_integer(uid), do: {:ok, uid}
  defp normalize_probe(other), do: {:error, %{clause: "credential_uid_probe_failed", detail: %{reason: inspect(other)}}}

  defp vm_probe do
    dir =
      Path.join(
        System.tmp_dir!(),
        "orris-console-probe-#{System.unique_integer([:positive])}-#{:erlang.phash2(self())}"
      )

    with :ok <- File.mkdir(dir),
         :ok <- File.chmod(dir, 0o700),
         file = Path.join(dir, "probe"),
         {:ok, device} <- :file.open(String.to_charlist(file), [:write, :exclusive, :binary, :raw]),
         :ok <- :file.close(device),
         {:ok, %File.Stat{uid: uid}} <- File.lstat(file) do
      File.rm_rf(dir)
      {:ok, uid}
    else
      other ->
        File.rm_rf(dir)
        other
    end
  end

  defp parent(dir, uid) do
    case File.lstat(dir) do
      # the parent must be private (0700); ownership is judged on the credential file itself against the probe
      {:ok, %File.Stat{type: :directory, mode: mode}} ->
        if Bitwise.band(mode, 0o077) == 0, do: :ok, else: {:error, %{clause: "credential_parent_unsafe", detail: nil}}

      {:ok, _not_a_directory} ->
        {:error, %{clause: "credential_parent_unsafe", detail: nil}}

      {:error, :enoent} ->
        with :ok <- File.mkdir_p(dir) |> closed("credential_parent_unsafe"),
             :ok <- File.chmod(dir, 0o700) |> closed("credential_parent_unsafe") do
          parent(dir, uid)
        end

      {:error, _} ->
        {:error, %{clause: "credential_parent_unsafe", detail: nil}}
    end
  end

  defp absent(path) do
    case File.lstat(path) do
      {:error, :enoent} -> :ok
      {:ok, %File.Stat{type: :symlink}} -> {:error, %{clause: "credential_symlink", detail: nil}}
      {:ok, _} -> {:error, %{clause: "credential_exists", detail: nil}}
      {:error, _} -> {:error, %{clause: "credential_exists", detail: nil}}
    end
  end

  defp open_exclusive(path) do
    case :file.open(String.to_charlist(path), [:write, :exclusive, :binary, :raw]) do
      {:ok, device} -> {:ok, device}
      {:error, :eexist} -> {:error, %{clause: "credential_exists", detail: nil}}
      {:error, reason} -> {:error, %{clause: "credential_write_failed", detail: %{reason: inspect(reason)}}}
    end
  end

  defp owned(path, uid, device) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular, uid: ^uid}} ->
        :ok

      _ ->
        :file.close(device)
        File.rm(path)
        {:error, %{clause: "credential_owner_mismatch", detail: nil}}
    end
  end

  defp write(device, secret, path) do
    with :ok <- :file.write(device, secret),
         :ok <- :file.sync(device),
         :ok <- :file.close(device) do
      :ok
    else
      {:error, reason} ->
        :file.close(device)
        File.rm(path)
        {:error, %{clause: "credential_write_failed", detail: %{reason: inspect(reason)}}}
    end
  end

  defp lstat(path) do
    case File.lstat(path) do
      {:ok, stat} -> {:ok, stat}
      {:error, :enoent} -> {:error, %{clause: "credential_missing", detail: nil}}
      {:error, reason} -> {:error, %{clause: "credential_missing", detail: %{reason: inspect(reason)}}}
    end
  end

  defp regular(%File.Stat{type: :regular}), do: :ok
  defp regular(%File.Stat{type: :symlink}), do: {:error, %{clause: "credential_symlink", detail: nil}}
  defp regular(_), do: {:error, %{clause: "credential_not_regular", detail: nil}}

  defp mode(%File.Stat{mode: mode}) do
    if Bitwise.band(mode, 0o077) == 0, do: :ok, else: {:error, %{clause: "credential_mode_unsafe", detail: nil}}
  end

  defp owner(%File.Stat{uid: uid}, uid), do: :ok
  defp owner(_, _), do: {:error, %{clause: "credential_owner_mismatch", detail: nil}}

  defp bytes(path) do
    case File.read(path) do
      {:ok, bytes} when byte_size(bytes) == @secret_bytes -> {:ok, bytes}
      {:ok, _} -> {:error, %{clause: "credential_invalid_length", detail: nil}}
      {:error, reason} -> {:error, %{clause: "credential_missing", detail: %{reason: inspect(reason)}}}
    end
  end

  defp closed(:ok, _clause), do: :ok
  defp closed({:error, reason}, clause), do: {:error, %{clause: clause, detail: %{reason: inspect(reason)}}}
end
