# Capabilities

Capabilities name negotiated behavior. They are not readiness signals; readiness
is carried by [`status`](./session.md#status). The negotiated set is the
intersection of the controller's offer and the service's set, and it never
changes during a session.

## Service capability set

| Capability | Required in `hello` | Meaning |
| --- | --- | --- |
| `sip.v1` | Yes | Typed SIP application family. Without it, no SIP message family is accepted. |
| `sip.state.v1` | Yes | Logical contexts, owner leases and compare-and-swap execution in the selected state store. Same capability for memory and Redis. |
| `sip.native-completion.v1` | Yes | Retained native completion, physical settlement and retirement without a surviving controller. The service issues an owner lease during the handshake. |
| `sip.directory-reconcile.v1` | Yes | Compatibility label still required by deployed controllers. Does **not** enable the removed reconciliation RPCs. |
| `sip.completion-relay.v1` | Yes | Compatibility label. Does **not** restore per-send grants, physical relay or settlement through the controller. |
| `sip.command-admission.v1` | No | Command-time policy and delivery admission for `respond`, `openUac` and `dialogRequest`. |
| `sip.command-budget.v1` | No | `command` envelopes may carry up to 40,000 ms of budget instead of 2,000 ms. |
| `sip.dialog-continuity.v1` | No | Required to send `ingest` with `TLS_DIALOG_CONTINUITY` or `TLS_TRANSACTION_CONTINUITY` admission. |
| `sip.local-challenge.v1` | Only with `--require-auth-methods` | Local initial digest challenges. Offered by the service only when configured; then required. |

"Required" means sipbridge refuses the handshake without it. Optional
capabilities change what the session may do. The original platform's controller
requires `sip.command-budget.v1` before it admits work, because some native
operations wait up to 32 seconds.

## Compatibility rules

- The major version identifies incompatible behavior. Only major `1` exists.
- Within a major, additions are optional and capability-gated. Unknown optional
  JSON fields are tolerated; unknown operations, enum values and capabilities
  are not guessed.
- JSON names and enum names are part of the contract. They are never renamed or
  reused with a different meaning; removed names are reserved.
- Compatibility labels and generated fields are removed only together with the
  controller code that depends on them. Decoding a historical family never
  authorizes its removed operation.

The rollout window, version-skew testing requirements and rollback constraints
are in [Protocol: compatibility, reload and drain](../spec/protocol.md#_11-compatibility-reload-and-drain).

## Session-only fixture

The separate `cmd/sipbridge-control` test program negotiates no SIP
capabilities and always reports SIP readiness as unavailable. It exists to test
control transport interoperability and is not a service mode. See
[Fixtures](../fixtures.md#session-only-executable).
