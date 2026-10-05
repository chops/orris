# B3a G2: the claims-root lock helper (native/root_lock) is built once per test run into a private directory and
# named through application config, so every FileRegistry reclaim and lock row reaches the real kernel lock.
root_lock_dir = Path.join(System.tmp_dir!(), "root-lock-build-#{System.unique_integer([:positive])}")
File.mkdir_p!(root_lock_dir)
root_lock = Path.join(root_lock_dir, "root_lock")

{root_lock_output, 0} =
  System.cmd(Path.expand("../bin/build-root-lock", __DIR__), [root_lock], stderr_to_stdout: true)

if root_lock_output =~ "warning:", do: raise("the root lock helper must build without warnings: #{root_lock_output}")
Application.put_env(:ai_orchestrator, :root_lock_helper, root_lock)
System.at_exit(fn _status -> File.rm_rf(root_lock_dir) end)

# Spikes fork real OS processes: run them on purpose with `mix test --include spike`.
ExUnit.start(exclude: [:spike])
