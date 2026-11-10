# Implementation Status

sipbridge is experimental. This page separates what the service does today from
test-only utilities, dated historical observations and proposed work. The
authoritative statements are in [Architecture](../spec/architecture.md#qualification-and-release-boundary),
[Fixtures](../fixtures.md) and [Verification](../verification.md).

## Implemented service behavior

The `cmd/sipbridge` executable, with memory (default) or explicit Redis state:

- Public SIP over UDP, TCP and TLS with real transport class preserved per context.
- Native transactions, dialogs and timers through the pinned sipgo reducers,
  using one schema 2 / reducer 9 / timers 1 compare-and-swap path.
- JSON control over mutual TLS WebSocket with exact URI SAN authorization,
  heartbeats, supersession and credential reload.
- Owner leases, native completion without a surviving controller, command-time
  admission and command budgets.
- Exact physical write evidence with `WRITTEN`, `NOT_WRITTEN` and `UNKNOWN`.
- Process-local REGISTER/SUBSCRIBE method policy and optional local initial
  digest challenges.
- Liveness, serving and readiness endpoints, counters, structured diagnostics and
  optional SIP capture.

## Fixture-only utilities

These programs and paths test isolated contracts. They are not service modes and
do not provide production evidence.

| Utility | Scope |
| --- | --- |
| `cmd/sipbridge-control` | Control transport only: hello, supersession, status, heartbeats, JSON validation, TLS reload. No SIP. |
| `cmd/sipbridge-fixture` | Two-ingress TLS native-wire fixture with its own resident protocol objects, driven by `test/run.py`. Requires `SIPBRIDGE_FIXTURE_STDIO=1`, set only by the runner. |
| Historical 1/7 relay fixtures | Shared-reducer assertions with trusted per-send grants and controller-relayed writes. Not linked into the service. |

## Local service checks

Go race tests, real Redis/Sentinel restart tests and service subprocess/socket
tests use synthetic controllers and disposable Redis. They show local contract
behavior. They do not qualify real account policy, media, handset or carrier
interoperability, load, version skew or safe production drain.

## Historical evidence

[Verification](../verification.md#historical-evidence-not-current-qualification)
records dated observations, including an isolated staging experiment. They
describe the builds tested at the time and are not current readiness. Rerun the
current gates for any candidate.

## Proposed or gated

| Item | State |
| --- | --- |
| Drain protocol (`drain.begin` / `status` / `complete`) | Proposed. No schema or implementation. |
| Context inventory and drain generations | Proposed. |
| Output queues and additional operation markers | Proposed bounds only. |
| Full conformance suite (two processes, two controllers, all test groups) | Required evidence, not complete. See [Protocol section 12](../spec/protocol.md#_12-required-conformance-evidence). |
| REFER/Replaces, PRACK, early UPDATE, forking, media/CDR and legacy paths | Require backend acceptance evidence. |
| Zero-disconnect SIP-core replacement | Not provided. |
| Production selection | Gated. The original platform's supported production SIP deployment is unchanged. |

No document in this repository grants deployment authorization.
