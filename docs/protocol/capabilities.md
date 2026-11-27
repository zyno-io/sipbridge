---
description: The named protocol capabilities a controller offers in hello, the serving baseline, and the rules for optional features.
---

# Capabilities

The serving protocol version defines the baseline. SIP message families,
logical state and leases, direct request decisions, command admission and
budgets, dialog IDs and inspection results, native completion, and validated
dialog continuity are available on every serving session. The Redis profile
also includes exact socket dispatch, prepared-operation resume and control
attachments. HelloAck identifies the active backend.

## Optional features

The controller offers optional features in `hello` and SIPbridge returns the
ones it also supports in `helloAck`. Readiness is reported separately with
[`status`](./session.md#status).

| Capability | When to offer it |
| --- | --- |
| `sip.local-challenge.v1` | Use [local initial digest challenges](../guide/application.md#local-initial-digest-challenges). A listener configured to issue them requires this capability. |
| `sip.reinvite-ack.v1` | Receive accepted peer re-INVITE ACKs as `ACKNOWLEDGED` with the SIP message and `inviteCseq` correlation value. |

## Rules

- The serving protocol version is major `1`, minor `1`. A session keeps the version
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
implements the standalone minor `0` session fixture. It agrees to no SIP
capabilities and never reports SIP as ready. It is useful for testing a
controller's connection code, and is not a way to run the service. See
[Fixtures](../fixtures.md#session-only-executable).
