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
| `sip.directory-reconcile.v1` | Yes | Kept for compatibility with existing controllers. It must be offered but enables nothing; the reconciliation messages it once referred to have been removed. |
| `sip.completion-relay.v1` | Yes | Kept for compatibility in the same way. It does not bring back the older design in which controllers approved and relayed each send. |
| `sip.command-admission.v1` | No | `respond`, `openUac` and `dialogRequest` commands carry an `admission` block that sipbridge checks at the moment the command runs. See [Commands](./commands.md#admission). |
| `sip.command-budget.v1` | No | A `command` may wait up to 40,000 ms instead of 2,000 ms. |
| `sip.dialog-continuity.v1` | No | Allows `ingest` with `TLS_DIALOG_CONTINUITY` or `TLS_TRANSACTION_CONTINUITY`, which is how a dialog continues on a new TLS connection. |
| `sip.local-challenge.v1` | Only with `--require-auth-methods` | [Local initial digest challenges](../guide/application.md#local-initial-digest-challenges). sipbridge offers it only when configured for them, and then requires it. |

"Required" means sipbridge closes the handshake if the capability is missing.
The optional ones are worth offering. `sip.command-budget.v1` in particular
matters in practice, because some SIP operations legitimately take up to 32
seconds and would not fit in a two-second budget; the reference controller
refuses to work without it.

## Compatibility rules

The protocol is meant to let sipbridge and controllers be upgraded
independently. These are the rules it follows, and that your controller can
rely on:

- The major version changes only for incompatible changes. Only major `1`
  exists.
- Within a major version, new behavior is optional and has its own capability.
  You never get new behavior you did not ask for.
- Unknown optional JSON fields are ignored. Unknown operations, enum values and
  capabilities are never guessed at.
- JSON field names and enum names are permanent. They are not renamed or given
  a new meaning, and the names of removed ones are not reused.
- The two compatibility capabilities above will be dropped only together with
  the controller code that still sends them. Being able to decode an older
  message type never means sipbridge will act on it.

How version differences are handled during a rollout, and what limits rollback,
is covered in
[Protocol: compatibility, reload and drain](../spec/protocol.md#_11-compatibility-reload-and-drain).

## Session-only fixture

The repository also contains `cmd/sipbridge-control`, a test program that
implements only the session part of the protocol. It agrees to no SIP
capabilities and always reports SIP as unavailable. It is useful for testing a
controller's connection and handshake code without a full sipbridge, and is not
a way to run the service. See [Fixtures](../fixtures.md#session-only-executable).
