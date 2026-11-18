---
description: How a controller connects over WebSocket and exchanges strict JSON envelopes, with the encoding rules, size limits and the basic message flow.
---

# Protocol Overview

The control protocol connects your controller to sipbridge. The controller
opens a WebSocket, and from then on the two exchange JSON messages: sipbridge
offers SIP requests and reports events, and the controller sends back
decisions and commands.

This section is for controller authors. This page covers the connection, the
message format and the overall flow; the following pages cover the
[session](./session.md), [capabilities](./capabilities.md), every
[message type](./messages.md) and [commands and their outcomes](./commands.md).
Field names and types come from the
[control schema](../../proto/sipbridge-control.proto); the complete rules are
in the [protocol specification](../spec/protocol.md).

## Connection

| Item | Value |
| --- | --- |
| URL | `wss://<control address>/v1/control`; `ws://` only on a loopback address |
| Subprotocol | `sipbridge.control.json.v1` |
| Authentication | Client certificate (default) or bearer token; see [Identities and TLS](../guide/security.md#control-connection) |
| `Origin` header | Must be absent |
| Compression | Disabled |
| Frames | Text only; a binary frame closes the session |
| Who connects | Always the controller |

## Envelope

Every WebSocket message is one JSON object, called an **envelope**. It has a
few common fields and exactly one payload field, whose name says what kind of
message it is:

| Field | Type | Meaning |
| --- | --- | --- |
| `correlationId` | string | Chosen by the sender of a request and repeated in the reply, so the two can be matched. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. Make it unique within one run of your process. It is a label and grants nothing. |
| `sessionId` | string | Issued in `helloAck`. Leave it out of `hello`; include it, exactly, in every later message. |
| `remainingBudgetMs` | integer | How many milliseconds the sender is still willing to wait for this operation. Required on `requestDecision`, `command` and `inspect`; left out of `hello`, `status`, `ping` and `pong`. At most 2,000, or 40,000 for a `command`. |
| one payload | object | Exactly one of `hello`, `helloAck`, `status`, `ping`, `pong`, `request`, `requestDecision`, `command`, `result`, `dialogEvent`, `inspect`, and the others listed in [Message Families](./messages.md). |

A heartbeat is a complete, minimal example:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841","ping":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

## JSON encoding

The JSON follows protobuf's standard
[ProtoJSON mapping](https://protobuf.dev/programming-guides/json/), applied
strictly. A protobuf JSON codec generated from the schema gives you most of
this for free. If you write JSON by hand, use these forms:

| Schema type | JSON form | Example |
| --- | --- | --- |
| field names | lowerCamelCase only; `snake_case` is rejected | `connectionAttempt` |
| `uint64` | Decimal **string**, `0` to `18446744073709551615`; no sign, leading zero or exponent | `"9007199254740993"` |
| `uint32` | JSON number | `1048576` |
| `bool` | JSON boolean | `true` |
| `bytes` | Padded standard base64 string | `"AP+AQQ=="` |
| enum | Symbolic name | `"ACCEPTED"` |
| oneof | The chosen variant as a named field; exactly one present | `"cancel":{}` |

sipbridge is strict about what it accepts, and a malformed message closes the
session. It rejects invalid UTF-8, duplicate keys, nesting deeper than 32
levels, `null` or a wrongly typed value in a known field, a missing or extra
payload, unknown enum names and anything after the closing brace. Unknown
**fields** are ignored, so that new fields can be added without breaking older
controllers. A field holding its default value (`0`, `""`, `false`) may be
left out, and sipbridge leaves such fields out of what it sends: treat a
missing field as its default.

In JavaScript, never parse a `uint64` into a `Number`; use `BigInt` or the
generated TypeScript codec. The repository's
[shared JSON cases](../../proto/sipbridge-control-json-cases.json) list
accepted and rejected examples you can use to test your codec.

## Size limits

A complete message, as UTF-8 text including base64 and JSON overhead, must fit
the envelope limit agreed in the handshake: at most 1 MiB and at least 512
bytes. A SIP message inside an envelope can be up to 64 KiB.

## Message flow

```text
Controller                                   sipbridge
    | ---- WebSocket upgrade -----------------> |
    | ---- hello ------------------------------> |
    | <--- helloAck ---------------------------- |
    | ---- status (ready) ---------------------> |
    | <--> ping / pong, both directions <------> |
    |                                            |  a SIP request arrives
    | <--- request ----------------------------- |  offered once
    | ---- requestDecision --------------------> |  exactly one answer
    | <--- result ------------------------------ |  decision recorded
    | ---- command (respond, dialogRequest...) > |  as the call proceeds
    | <--- result ------------------------------ |
    | <--- dialogEvent ------------------------- |
    | ---- result (ACCEPTED) ------------------> |  event received
```

Many operations are in flight on one session at once, so match replies to
requests by `correlationId`, not by arrival order. If you hold several
sessions, the order of messages on one says nothing about the order on
another.

[A Call, Step by Step](../guide/call-walkthrough.md) shows this flow with
complete messages.
