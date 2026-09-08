# Orris

Orris is a local-first Elixir workflow supervisor for coordinating agent work.
It combines an append-only journal, deterministic workflow reduction, owned
command execution, and a native process guardian for verification gates.

This is development source. The complete product is under construction.

## Contributing

This project is in an early stage of development. We are not currently accepting external code contributions while we establish the architecture, governance, and contribution policies for the project.

Bug reports, feature requests, design feedback, and discussions are welcome.

For now, please do not submit pull requests, patches, source-code implementations, or substantial code snippets through issues, discussions, email, or other channels. This includes unsolicited implementations of bug fixes or requested features.

You are welcome to clone, fork, modify, and experiment with the project in accordance with the Apache License 2.0.

We expect to revisit external code contributions as the project matures.

## Current capabilities

- Typed run specifications, plans and journal events, with validated replay.
- Actor-aware commands and per-run supervision with explicit effect ownership.
- Receipt-aware pane dispatch interfaces and deadline-bound gate execution.
- Deterministic contract fixtures and tests for refusal, recovery and process cleanup.

A LiveView control plane, shared hosting, the full application/MCP API, complete
session management, capacity management and enforced budgets remain product work.
Schema support or a test fixture does not imply a complete runtime feature.

## Development

The verification gate pins Elixir 1.20.4 and Erlang/OTP 29.0.5. With Nix and
flakes enabled, run from the repository root:

```sh
nix develop --command bin/verify
nix build --no-link .#gate-guardian
```

The verifier fetches locked dependencies and runs the test suite, static checks,
security audits, redaction checks and an escript integration probe. It requires
network access for dependency and advisory sources. Supported Nix outputs are
`aarch64-darwin`, `aarch64-linux` and `x86_64-linux`; declaring an output is not
a claim of measured runtime coverage on every platform.

## Runtime integration

Live pane operations require the separate Orrisd coordination runtime and an
`ap` executable supporting the capability and receipt protocol required by
[the protocol v2 dispatch contract](docs/contracts/ipc-v2.md), built on the
[base framing and command rules](docs/contracts/ipc-v1.md). This source snapshot does not
bundle that runtime or provide a verified external installation of the pair.
Real gate execution also requires configuring the built `gate_guardian` path.
See [runtime configuration](lib/ai_orchestrator/config/runtime.ex) and the
[guardian protocol](docs/contracts/gate-guardian-protocol.org).

Orris and Orrisd are the public project names. Existing `AiOrchestrator`,
`ai_orchestrator`, `ai-orchestrator`, `ai-pair` and `ap` identifiers in modules,
commands, paths and protocol contracts retain their technical meanings.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
