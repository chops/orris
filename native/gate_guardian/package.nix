{ lib, stdenv }:

stdenv.mkDerivation {
  pname = "ai-orchestrator-gate-guardian";
  version = "0.1.0-dev";

  src = lib.fileset.toSource {
    root = ./.;
    fileset = ./gate_guardian.c;
  };

  strictDeps = true;
  dontConfigure = true;

  buildPhase = ''
    runHook preBuild
    $CC -std=c11 -O2 -Wall -Wextra -Werror gate_guardian.c -o gate_guardian
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    if output="$(./gate_guardian)"; then
      echo "guardian accepted an invocation without required arguments" >&2
      exit 1
    else
      status=$?
    fi
    test "$status" -eq 2
    test "$output" = "SETUP_FAILED class=usage"
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    install -Dm755 gate_guardian "$out/bin/gate_guardian"
    runHook postInstall
  '';

  meta = {
    license = lib.licenses.asl20;
    description = "Native process guardian for ai-orchestrator gates";
    mainProgram = "gate_guardian";
    platforms = [
      "aarch64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ];
  };
}
