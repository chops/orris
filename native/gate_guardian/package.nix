{ lib, rustPlatform }:

rustPlatform.buildRustPackage {
  pname = "ai-orchestrator-gate-guardian";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [ ./Cargo.toml ./Cargo.lock ./src ./tests ];
  };
  cargoLock.lockFile = ./Cargo.lock;
  # Fault injection is compiled only in the separately built testing feature.
  env.RUSTFLAGS = "-D warnings";
  doCheck = true;
  doInstallCheck = true;
  installCheckPhase = ''
    if output="$("$out/bin/gate_guardian")"; then
      echo "guardian accepted an invocation without required arguments" >&2
      exit 1
    else
      status=$?
    fi
    test "$status" -eq 2
    test "$output" = "SETUP_FAILED class=usage"
  '';
  meta = {
    license = lib.licenses.asl20;
    description = "Rust process guardian for ai-orchestrator gates";
    mainProgram = "gate_guardian";
    platforms = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
  };
}
