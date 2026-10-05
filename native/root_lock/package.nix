{ lib, rustPlatform }:
rustPlatform.buildRustPackage {
  pname = "ai-orchestrator-root-lock";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [ ./Cargo.toml ./Cargo.lock ./src ];
  };
  cargoLock.lockFile = ./Cargo.lock;
  env.RUSTFLAGS = "-D warnings";
  doInstallCheck = true;
  installCheckPhase = ''
    if output="$("$out/bin/root_lock")"; then
      echo "root lock helper accepted an invocation without required arguments" >&2
      exit 1
    else
      status=$?
    fi
    test "$status" -eq 2
    test "$output" = "error usage"
  '';
  meta = {
    license = lib.licenses.asl20;
    description = "Kernel flock helper for the ai-orchestrator pane claims root";
    mainProgram = "root_lock";
    platforms = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
  };
}
