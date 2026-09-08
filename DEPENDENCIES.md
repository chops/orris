# Dependency licenses

Orris project source is licensed under Apache-2.0. Separately maintained
packages retain their own licenses. This source repository records locked
dependencies but does not vendor their implementations or distribute their
compiled artifacts.

The table records the package-declared license from each exact locked Hex
archive. It is a dependency inventory, not a replacement for upstream license
texts. Development fetches retrieve the packages separately. Anyone packaging
or redistributing dependencies must preserve the applicable upstream copyright,
license and notice material with that distribution.

“Transitive” means the package is in the lockfile but is not a direct project
dependency; it can serve runtime or development dependencies. Optional upstream
packages absent from the lockfile are not included in this snapshot.

| Package | Version | Dependency role | Declared license |
|---------|---------|-----------------|------------------|
| [acceptor_pool](https://hex.pm/packages/acceptor_pool/1.0.1) | 1.0.1 | Transitive | Apache-2.0 |
| [boundary](https://hex.pm/packages/boundary/0.10.4) | 0.10.4 | Direct development/test | MIT |
| [bunt](https://hex.pm/packages/bunt/1.0.0) | 1.0.0 | Transitive | MIT |
| [ts_chatterbox](https://hex.pm/packages/ts_chatterbox/0.16.0) | 0.16.0 | Transitive | MIT |
| [credo](https://hex.pm/packages/credo/1.7.19) | 1.7.19 | Direct development/test | MIT |
| [ctx](https://hex.pm/packages/ctx/0.6.0) | 0.6.0 | Transitive | Apache-2.0 |
| [dialyxir](https://hex.pm/packages/dialyxir/1.4.7) | 1.4.7 | Direct development/test | Apache-2.0 |
| [erlex](https://hex.pm/packages/erlex/0.2.9) | 0.2.9 | Transitive | Apache-2.0 |
| [ex_ast](https://hex.pm/packages/ex_ast/0.13.1) | 0.13.1 | Transitive | MIT |
| [file_system](https://hex.pm/packages/file_system/1.1.1) | 1.1.1 | Transitive | Apache-2.0 |
| [finch](https://hex.pm/packages/finch/0.23.0) | 0.23.0 | Transitive | MIT |
| [glob_ex](https://hex.pm/packages/glob_ex/0.1.12) | 0.1.12 | Transitive | MIT |
| [gproc](https://hex.pm/packages/gproc/1.2.0) | 1.2.0 | Transitive | Apache-2.0 |
| [grpcbox](https://hex.pm/packages/grpcbox/0.18.0) | 0.18.0 | Transitive | Apache-2.0 |
| [hpack_erl](https://hex.pm/packages/hpack_erl/0.3.0) | 0.3.0 | Transitive | MIT |
| [hpax](https://hex.pm/packages/hpax/1.0.4) | 1.0.4 | Transitive | Apache-2.0 |
| [igniter](https://hex.pm/packages/igniter/0.8.4) | 0.8.4 | Transitive | MIT |
| [jason](https://hex.pm/packages/jason/1.4.5) | 1.4.5 | Direct runtime | Apache-2.0 |
| [mime](https://hex.pm/packages/mime/2.0.7) | 2.0.7 | Transitive | Apache-2.0 |
| [mint](https://hex.pm/packages/mint/1.10.0) | 1.10.0 | Transitive | Apache-2.0 |
| [mix_audit](https://hex.pm/packages/mix_audit/2.1.5) | 2.1.5 | Direct development/test | BSD-3-Clause |
| [nimble_options](https://hex.pm/packages/nimble_options/1.1.1) | 1.1.1 | Transitive | Apache-2.0 |
| [nimble_pool](https://hex.pm/packages/nimble_pool/1.1.0) | 1.1.0 | Transitive | Apache-2.0 |
| [opentelemetry](https://hex.pm/packages/opentelemetry/1.7.0) | 1.7.0 | Direct runtime | Apache-2.0 |
| [opentelemetry_api](https://hex.pm/packages/opentelemetry_api/1.5.0) | 1.5.0 | Direct runtime | Apache-2.0 |
| [opentelemetry_exporter](https://hex.pm/packages/opentelemetry_exporter/1.10.0) | 1.10.0 | Direct runtime | Apache-2.0 |
| [owl](https://hex.pm/packages/owl/0.13.1) | 0.13.1 | Transitive | Apache-2.0 |
| [req](https://hex.pm/packages/req/0.7.4) | 0.7.4 | Transitive | Apache-2.0 |
| [rewrite](https://hex.pm/packages/rewrite/1.3.0) | 1.3.0 | Transitive | MIT |
| [sourceror](https://hex.pm/packages/sourceror/1.12.2) | 1.12.2 | Transitive | Apache-2.0 |
| [spitfire](https://hex.pm/packages/spitfire/0.4.0) | 0.4.0 | Transitive | MIT |
| [ssl_verify_fun](https://hex.pm/packages/ssl_verify_fun/1.1.7) | 1.1.7 | Transitive | MIT |
| [stream_data](https://hex.pm/packages/stream_data/1.4.0) | 1.4.0 | Direct test | Apache-2.0 |
| [styler](https://hex.pm/packages/styler/1.12.2) | 1.12.2 | Direct development/test | Apache-2.0 |
| [telemetry](https://hex.pm/packages/telemetry/1.4.2) | 1.4.2 | Direct runtime | Apache-2.0 |
| [text_diff](https://hex.pm/packages/text_diff/0.1.0) | 0.1.0 | Transitive | MIT |
| [tls_certificate_check](https://hex.pm/packages/tls_certificate_check/1.35.0) | 1.35.0 | Transitive | MIT |
| [usage_rules](https://hex.pm/packages/usage_rules/1.2.8) | 1.2.8 | Direct development/test | MIT |
| [yamerl](https://hex.pm/packages/yamerl/0.10.0) | 0.10.0 | Transitive | BSD-2-Clause |
| [yaml_elixir](https://hex.pm/packages/yaml_elixir/2.12.2) | 2.12.2 | Transitive | MIT |
| [zoi](https://hex.pm/packages/zoi/0.18.7) | 0.18.7 | Direct runtime | Apache-2.0 |

## Build inputs and external runtime

The Nix flake pins nixpkgs for the compiler, BEAM toolchain and build utilities.
Those packages retain their respective upstream licenses; the flake is not a
license for their source or generated binaries. GitHub Actions are separately
maintained CI tooling pinned by commit in the workflow.

Live pane operations use the separately distributed Orrisd runtime. The vendored
IPC fixtures are synthetic protocol examples shared with that project, described
in `test/fixtures/contracts/MANIFEST.org`; they are not a bundled daemon.

Binary releases, containers or vendored dependency bundles require a review of
the exact redistributed closure and its notices. This inventory does not certify
such a distribution.
