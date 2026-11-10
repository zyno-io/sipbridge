# Capabilities

A capability is a short name for a protocol feature, such as
`sip.command-budget.v1`. The controller lists the capabilities it supports in
`hello`, sipbridge keeps the ones it supports too, and the result, returned in
`helloAck`, is what both sides may use for the rest of the session. It does not
change while the session is open.

Capabilities say what a session *can* do. Whether a controller is *ready* for
work is a separate matter, reported with [`status`](./session.md#status).

Use the table as an implementation checklist: offer the required capabilities
and the optional features your controller supports. `sip.local-challenge.v1`
is needed only when sipbridge runs with `--require-auth-methods`.

## Service capability set

| Capability | Required in `hello` | Meaning |
| --- | --- | --- |
| `sip.v1` | Yes | The SIP messages of this protocol. Without it, sipbridge accepts no SIP-related message on the session. |
| `sip.state.v1` | Yes | SIP state kept in the state store, with leases for controllers. The same capability covers memory and Redis. |
| `sip.native-completion.v1` | Yes | sipbridge finishes SIP protocol work by itself (CANCEL, ACK, retransmissions, cleanup) even when the controller that owned it is gone, and issues the controller's lease during the handshake. |
| `sip.command-admission.v1` | No | `respond`, `openUac` and `dialogRequest` commands carry an `admission` block that sipbridge checks at the moment the command runs. See [Commands](./commands.md#admission). |
| `sip.command-budget.v1` | No | A `command` may wait up to 40,000 ms instead of 2,000 ms. |
| `sip.dialog-continuity.v1` | No | Allows `ingest` with `TLS_DIALOG_CONTINUITY` or `TLS_TRANSACTION_CONTINUITY`, which is how a dialog continues on a new TLS connection. |
| `sip.controller-reattach.v1` | No | Enables recovery tokens, attachment generations and owner-scoped inventory. A replacement process must prove ownership and reconcile before ready status. |
| `sip.local-challenge.v1` | Only with `--require-auth-methods` | [Local initial digest challenges](../guide/application.md#local-initial-digest-challenges). sipbridge offers it only when configured for them, and then requires it. |

"Required" means sipbridge closes the handshake if the capability is missing.
The optional ones are worth offering. `sip.command-budget.v1` in particular
matters in practice, because some SIP operations legitimately take up to 32
seconds and would not fit in a two-second budget; the reference controller
refuses to work without it.

## Contract rules

The initial control contract is major `1`, minor `0`. A session keeps the version
and capabilities selected by its handshake until it closes.

- Optional behavior requires a negotiated capability.
- Unknown optional JSON fields are ignored. Unknown operations and required enum
  values are refused.
- JSON field names, enum names and protobuf declarations are part of the same
  wire contract. Use the schema and generated bindings from this repository.

See [Protocol: compatibility, reload and drain](../spec/protocol.md#_11-compatibility-reload-and-drain)
for session negotiation, credential reload and the proposed drain behavior.

## Session-only fixture

The repository also contains `cmd/sipbridge-control`, a test program that
implements only the session part of the protocol. It agrees to no SIP
capabilities and always reports SIP as unavailable. It is useful for testing a
controller's connection and handshake code without a full sipbridge, and is not
a way to run the service. See [Fixtures](../fixtures.md#session-only-executable).
