---
description: The named protocol capabilities a controller offers in hello, which ones sipbridge requires, and the rules for using them.
---

# Capabilities

A capability is a short name for a protocol feature, such as
`sip.command-budget.v1`. The controller lists the capabilities it supports in
`hello`; sipbridge keeps the ones it also supports and returns them in
`helloAck`. That list is what both sides may use until the session closes.

Capabilities say what a session *can* do. Whether a controller is *ready* for
work is separate and is reported with [`status`](./session.md#status).

## What to offer

Offer all seven required capabilities. sipbridge closes the handshake if any
of them is missing, with either state backend.

| Capability | Required | What it means for your controller |
| --- | --- | --- |
| `sip.v1` | Yes | The SIP message families of this protocol. |
| `sip.state.v1` | Yes | sipbridge keeps the SIP state and issues your controller a lease (`applicationLease`) in `helloAck`. The same capability covers memory and Redis state. |
| `sip.native-completion.v1` | Yes | sipbridge finishes SIP work it has committed (ACK, CANCEL, retransmissions, cleanup) by itself, even after your controller is gone. |
| `sip.request-decision.v1` | Yes | New requests arrive as a one-time `request`, and you answer each with one `requestDecision`. See [Deciding a request](./messages.md#deciding-a-request). |
| `sip.command-admission.v1` | Yes | Decisions and commands carry an `admission` block. See [Admission](./commands.md#admission). |
| `sip.command-budget.v1` | Yes | A `command` may wait up to 40,000 ms instead of 2,000 ms. SIP operations can legitimately take up to 32 seconds. |
| `sip.dialog-continuity.v1` | Yes | A phone that reconnects over TLS can continue an existing dialog on its new connection, after sipbridge checks the dialog's identity. |
| `sip.local-challenge.v1` | Only with `--require-auth-methods` | [Local initial digest challenges](../guide/application.md#local-initial-digest-challenges). sipbridge offers it only when configured for them, and then requires it. |

The schema also names `sip.controller-reattach.v1`, which lets a new
controller process take over a previous one's calls. Neither serving state
backend offers it, so it is never negotiated; it exists for
[test fixtures](../controller-recovery.md) only.

## Rules

- The protocol version is major `1`, minor `0`. A session keeps the version
  and capabilities chosen in its handshake until it closes.
- Optional behavior needs its negotiated capability.
- Unknown JSON fields are ignored; unknown message types and enum values are
  refused.
- The protobuf schema, its field numbers and the generated codecs define
  compatibility. Each sipbridge image carries the schema receipt it was built
  from at `/usr/share/sipbridge/control-schema.json`; compare it with the one
  your controller was built against. The major version alone does not prove
  compatibility.

## Session-only test program

The repository also contains `cmd/sipbridge-control`, a test program that
implements only the session part of the protocol. It agrees to no SIP
capabilities and never reports SIP as ready. It is useful for testing a
controller's connection code, and is not a way to run the service. See
[Fixtures](../fixtures.md#session-only-executable).
