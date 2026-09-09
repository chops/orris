defmodule C1.BootstrapTest do
  use ExUnit.Case, async: false
  alias C1.{Harness, Oracles}
  alias OrrisConsole.Credential

  @mods [OrrisConsole.Credential]

  defp path, do: Path.join(Harness.fresh("cred"), "console/credential")
  defp mode(p), do: Bitwise.band(File.stat!(p).mode, 0o777)

  test "C1-02a setup creates directory 0700 and a regular 0600 file of 32 random bytes; a second setup is refused" do
    Harness.red!(@mods)
    p = path()
    assert :ok = Credential.setup(p)
    assert mode(Path.dirname(p)) == 0o700
    assert File.lstat!(p).type == :regular and mode(p) == 0o600 and byte_size(File.read!(p)) == 32
    assert {:error, %{clause: "credential_exists"}} = Credential.setup(p)
    assert {:ok, digest} = Credential.load(p)
    assert digest == :crypto.hash(:sha256, File.read!(p))
  end

  test "C1-02b an unsafe (group/world accessible) parent and a symlinked path are refused before any byte is written" do
    Harness.red!(@mods)
    dir = Harness.fresh("unsafe")
    File.chmod!(dir, 0o755)
    assert {:error, %{clause: "credential_parent_unsafe"}} = Credential.setup(Path.join(dir, "credential"))
    refute File.exists?(Path.join(dir, "credential"))
    link_dir = Harness.fresh("link")
    File.chmod!(link_dir, 0o700)
    File.ln_s!(Path.join(link_dir, "elsewhere"), Path.join(link_dir, "credential"))
    assert {:error, %{clause: "credential_symlink"}} = Credential.setup(Path.join(link_dir, "credential"))
    refute File.exists?(Path.join(link_dir, "elsewhere"))
  end

  test "C1-02c load refuses missing, non-regular, group/world-readable and wrong-length credentials" do
    Harness.red!(@mods)
    p = path()
    assert {:error, %{clause: "credential_missing"}} = Credential.load(p)
    :ok = Credential.setup(p)
    File.chmod!(p, 0o640)
    assert {:error, %{clause: "credential_mode_unsafe"}} = Credential.load(p)
    File.chmod!(p, 0o600)
    File.write!(p, "short")
    assert {:error, %{clause: "credential_invalid_length"}} = Credential.load(p)
    File.rm!(p)
    File.mkdir!(p)
    assert {:error, %{clause: "credential_not_regular"}} = Credential.load(p)
  end

  test "C1-02d UID and PATH poisoning have no effect: ownership comes from a VM-created probe file" do
    Harness.red!(@mods)
    p = path()
    old = {System.get_env("UID"), System.get_env("PATH")}
    System.put_env("UID", "0")
    System.put_env("PATH", "/nonexistent")

    try do
      assert :ok = Credential.setup(p)
    after
      {uid, path_env} = old
      if uid, do: System.put_env("UID", uid), else: System.delete_env("UID")
      System.put_env("PATH", path_env)
    end

    probe = Path.join(Harness.fresh("uidprobe"), "f")
    File.write!(probe, "")
    assert File.stat!(p).uid == File.stat!(probe).uid
  end

  test "C1-02e the credential file has mode 0600 before any secret byte is written (traced separate process, delivered-trace barrier)" do
    Harness.red!(@mods)
    assert :ok = Oracles.outcome(fn -> Oracles.permission_first(&Credential.setup/1, path()) end)
  end

  test "C1-02f owner mismatch and a failing UID probe are refused (explicit test seam: the OS cannot safely provide a foreign owner)" do
    Harness.red!(@mods)
    p = path()
    me = File.stat!(Harness.fresh("me")).uid
    assert {:error, %{clause: "credential_owner_mismatch"}} = Credential.setup(p, probe_uid: fn -> {:ok, me + 1} end)
    refute File.exists?(p)

    assert {:error, %{clause: "credential_uid_probe_failed"}} =
             Credential.setup(p, probe_uid: fn -> {:error, :eacces} end)

    refute File.exists?(p)
    assert :ok = Credential.setup(p, probe_uid: fn -> {:ok, me} end)
    assert {:error, %{clause: "credential_owner_mismatch"}} = Credential.load(p, probe_uid: fn -> {:ok, me + 1} end)
  end
end
