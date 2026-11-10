# Protocol Overview

A controller drives sipbridge through JSON messages on a mutually authenticated
WebSocket. This section explains the wire format and message flow. Normative
field behavior is in [PROTOCOL.md](../spec/protocol.md); field names and types
come from the [control schema](../../dependencies/sipbridge-control.proto)
(protobuf package `zyno.sipbridge.v1`).

## Connection

| Item | Value |
| --- | --- |
| URL | `wss://<sipbridge control endpoint>/v1/control` |
| Subprotocol | `sipbridge.control.json.v1` |
| Authentication | Mutual TLS; client leaf URI SAN must be on the allowlist |
| `Origin` header | Must be absent |
| Compression | Disabled |
| Frames | Text only; binary frames close the session |
| Initiator | Always the controller |

See [Identities and TLS](../guide/security.md#control-connection) for the
acceptance checks.

## Envelope

Every WebSocket message is one JSON `Envelope`:

| Field | Type | Meaning |
| --- | --- | --- |
| `correlationId` | string | Request ID, echoed by the reply. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. Opaque; unique within the issuing process epoch; not authority. |
| `sessionId` | string | Issued in `helloAck`. Absent on `hello`; required and exact on every later message in both directions. |
| `remainingBudgetMs` | uint32 | Remaining deadline budget for RPC-style messages. Must be absent (`0`) on `hello`, `status`, `ping` and `pong`. At most 2,000, or 40,000 for `command` with `sip.command-budget.v1`. |
| one payload field | object | Exactly one of `hello`, `helloAck`, `status`, `ping`, `pong`, `ingress`, `routeResult`, `ingest`, `request`, `command`, `result`, … |

```json
{"correlationId":"op-17","sessionId":"6f0d6a4e-1c1b-4f4e-9b52-3f8f2f6c2a10","remainingBudgetMs":1800,"inspect":{"context":{"id":"ctx-1"}}}
```

That example shows envelope shape only; real `inspect` requests carry the full
`ContextRef` received earlier.

## JSON encoding

The encoding is a strict profile of the protobuf
[ProtoJSON mapping](https://protobuf.dev/programming-guides/json/). It is not
binary protobuf and not a separate JSON contract.

| Schema type | JSON form | Example |
| --- | --- | --- |
| field names | lowerCamelCase only; `snake_case` names are rejected | `connectionAttempt` |
| `uint64` | Decimal **string** `0`–`18446744073709551615`; no sign, leading zero or exponent | `"9007199254740993"` |
| `uint32` | JSON integer | `1048576` |
| `bool` | JSON boolean | `true` |
| `bytes` | Padded standard base64 string | `"AP+AQQ=="` |
| enum | Symbolic name | `"ACCEPTED"` |
| oneof | The selected variant as a named field; exactly one present | `"cancel":{}` |

Receivers reject: invalid UTF-8; duplicate object keys, including escaped
equivalents; nesting deeper than 32 containers; `null` or wrong-typed known
fields; multiple or missing oneof variants; unknown enum names; trailing content.
Unknown optional fields are ignored for additive compatibility, but an unknown
payload cannot stand in for a recognized one. Fields at their default value may
be omitted, and sipbridge omits them in output. Whitespace in sipbridge output is
not significant.

Keep `uint64` values lossless. In JavaScript, use `BigInt` or a generated
codec, never `Number`.

The repository's [shared JSON cases](../../internal/control/testdata/json-cases.json)
list accepted and rejected examples used by the Go and TypeScript codecs.

## Size limits

The complete UTF-8 message, including base64 and JSON overhead, must fit the
negotiated envelope bound, which is at most 1 MiB and at least 512 bytes. SIP
messages inside envelopes are limited to 64 KiB.

## Message flow

```text
Controller                                   sipbridge
    | ---- TLS + WebSocket upgrade -----------> |
    | ---- hello ------------------------------> |
    | <--- helloAck ---------------------------- |
    | ---- status -----------------------------> |  session becomes ready
    | <--> ping / pong (both directions) <-----> |
    |                                            |  SIP request arrives
    | <--- ingress ----------------------------- |
    | ---- ingest -----------------------------> |  controller admits it natively
    | <--- result ------------------------------ |
    | ---- routeResult (ACCEPTED) -------------> |
    | <--- request ----------------------------- |  presented to the owning controller
    | ---- command (respond) ------------------> |
    | <--- result ------------------------------ |
    | <--- dialogEvent ------------------------- |
```

The flow is illustrative. An accepted `routeResult` follows the native ingest
commit, and presentation, results and events are asynchronous. A session
multiplexes independent operations. WebSocket ordering within one session is not
a global order across sessions, contexts or controllers.

## Pages in this section

- [Session](./session.md): `hello`, `helloAck`, `status`, heartbeats and supersession.
- [Capabilities](./capabilities.md): what each capability means and which are required.
- [Message Families](./messages.md): every payload, its direction and whether it is current, historical or proposed.
- [Commands and Outcomes](./commands.md): sequencing, certainty and delivery results.
