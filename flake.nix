{
  description = "ai-orchestrator development and verification toolchain";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/e8be7818e19ada32105a8af937a6a473b38167ca";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forEachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          gate-guardian = pkgs.callPackage ./native/gate_guardian/package.nix { };
        });

      devShells = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          beamPackages = pkgs.beam.packages.erlang_29;
          erlang = beamPackages.erlang;
          elixir = beamPackages.elixir_1_20;
          toolchainTag = "elixir-${elixir.version}-otp-${erlang.version}";
        in
        {
          default = pkgs.mkShell {
            packages = [
              erlang
              elixir
              beamPackages.hex
              beamPackages.rebar3
              pkgs.bash
              pkgs.coreutils
              pkgs.git
              pkgs.gnugrep
              pkgs.ripgrep
            ];

            shellHook = ''
              projectRoot="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
              projectRoot="$(cd "$projectRoot" && pwd -P)"
              export MIX_HOME="''${XDG_CACHE_HOME:-$HOME/.cache}/ai-orchestrator/mix/${toolchainTag}"
              export MIX_BUILD_ROOT="$projectRoot/_build/${toolchainTag}"
              export MIX_DEPS_PATH="$projectRoot/deps/${toolchainTag}"
              export HEX_HOME="''${XDG_CACHE_HOME:-$HOME/.cache}/ai-orchestrator/hex"
              mkdir -p "$MIX_HOME" "$MIX_BUILD_ROOT" "$MIX_DEPS_PATH" "$HEX_HOME"
            '';
          };
        });
    };
}
