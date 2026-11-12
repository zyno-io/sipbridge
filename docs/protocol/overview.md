# Protocol Overview

The control protocol connects your application to sipbridge. The controller
opens a WebSocket to sipbridge, and from then on the two exchange JSON
messages: sipbridge reports SIP requests and events, and the controller sends
back routing decisions and commands.

This section explains the protocol for someone writing a controller. It starts
here with the connection, the message format and a typical exchange; the
following pages cover the session handshake, capabilities, each message type
and command outcomes. The complete rules for every field are in
[PROTOCOL.md](../spec/protocol.md), and the field names and types come from the
[control schema](../../proto/sipbridge-control.proto).

## Connection

| Item | Value |
| --- | --- |
| URL | `wss://<sipbridge control endpoint>/v1/control` |
| Subprotocol | `sipbridge.control.json.v1` |
| Authentication | mTLS by default, or explicit bearer mode; see [Identities and TLS](../guide/security.md#control-connection) |
| `Origin` header | Must be absent |
| Compression | Disabled |
| Frames | Text only; a binary frame closes the session |
| Who connects | Always the controller |

[Identities and TLS](../guide/security.md#control-connection) explains the
certificate checks, and [Getting Started](../guide/getting-started.md) has a
small Go program that connects.

## Envelope

Every WebSocket message is one JSON object, called an envelope. It has a few
common fields and exactly one payload field, whose name says what kind of
message it is:

| Field | Type | Meaning |
| --- | --- | --- |
| `correlationId` | string | An ID chosen by the sender of a request and repeated in the reply, so the two can be matched. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. Make it unique within one run of your process. It is only a label and grants nothing. |
| `sessionId` | string | Issued by sipbridge in `helloAck`. Leave it out of `hello`; include it, exactly, in every later message. |
| `remainingBudgetMs` | uint32 | How many milliseconds the sender is still willing to wait for this operation. Leave it out of `hello`, `status`, `ping` and `pong`. At most 2,000, or 40,000 for a `command` when `sip.command-budget.v1` was negotiated. |
| one payload field | object | Exactly one of `hello`, `helloAck`, `status`, `ping`, `pong`, `ingress`, `routeResult`, `ingest`, `request`, `command`, `result`, … |

```json
{"correlationId":"op-17","sessionId":"6f0d6a4e-1c1b-4f4e-9b52-3f8f2f6c2a10","remainingBudgetMs":1800,"inspect":{"context":{"id":"ctx-1"}}}
```

This example shows the shape of an envelope. A real `inspect` carries the whole
`ContextRef` you received earlier, not just an ID.

## JSON encoding

The JSON follows protobuf's standard
[ProtoJSON mapping](https://protobuf.dev/programming-guides/json/), applied
strictly. If you generate code from the schema with a protobuf JSON codec you
get most of this for free; if you write JSON by hand, these are the forms to
use:

| Schema type | JSON form | Example |
| --- | --- | --- |
| field names | lowerCamelCase only; `snake_case` names are rejected | `connectionAttempt` |
| `uint64` | Decimal **string** `0`–`18446744073709551615`; no sign, leading zero or exponent | `"9007199254740993"` |
| `uint32` | JSON integer | `1048576` |
| `bool` | JSON boolean | `true` |
| `bytes` | Padded standard base64 string | `"AP+AQQ=="` |
| enum | Symbolic name | `"ACCEPTED"` |
| oneof | The selected variant as a named field; exactly one present | `"cancel":{}` |

sipbridge is deliberately strict about what it accepts, and a malformed message
closes the session. It rejects invalid UTF-8, duplicate object keys, nesting
deeper than 32 levels, `null` or a value of the wrong type in a known field,
more or fewer than one payload, unknown enum names, and anything after the
closing brace. Unknown *fields* are ignored, so that fields can be added later
without breaking older software. A field holding its default value (`0`, `""`,
`false`) may be left out, and sipbridge leaves such fields out of what it sends.

Take care with `uint64` in JavaScript. These values are sent as strings because
a JavaScript `Number` cannot hold them exactly; parse them with `BigInt` or a
generated codec.

The repository's [shared JSON cases](../../internal/control/testdata/json-cases.json)
list accepted and rejected examples. They are handy as test input for your own
codec.

## Size limits

A whole message, as UTF-8 text, must fit the envelope limit agreed in the
handshake: at most 1 MiB and at least 512 bytes. A SIP message carried inside
an envelope can be up to 64 KiB.

## Message flow

A session begins with a handshake and then carries SIP work. This is an
incoming request being routed, accepted and answered:

```text
Controller                                   sipbridge
    | ---- TLS + WebSocket upgrade -----------> |
    | ---- hello ------------------------------> |
    | <--- helloAck ---------------------------- |
    | ---- status -----------------------------> |  session becomes ready
    | <--> ping / pong (both directions) <-----> |
    |                                            |  SIP request arrives
    | <--- ingress ----------------------------- |
    | ---- ingest -----------------------------> |  controller admits the request
    | <--- request ----------------------------- |  handle while ingest is pending
    | ---- command (respond) ------------------> |
    | <--- result ------------------------------ |  response command outcome
    | ---- result (ACCEPTED) ------------------> |  presentation acknowledgement
    | <--- result ------------------------------ |  original ingest outcome
    | ---- routeResult (ACCEPTED) -------------> |
    | <--- dialogEvent ------------------------- |
```

In words: sipbridge asks what to do with a new request (`ingress`). The
controller admits it into sipbridge's SIP state (`ingest`). A definite fresh
commit can present the request directly to its controller before returning the
ingest result. The controller handles that nested `request`, selects a response
with `command`/`respond`, and acknowledges presentation independently of physical
SIP delivery. It confirms routing with `routeResult` after ingest returns.
Presentation acknowledgement can precede the response command, and deferred flow
publication can change this example's ordering; neither creates a presentation
retry queue. sipbridge reports dialog progress separately through `dialogEvent`.

Two things to keep in mind when you implement this. Many operations are in
flight on one session at once, so match replies to requests by `correlationId`
instead of by order of arrival. And if you hold several sessions, the order of
messages on one session tells you nothing about the order on another.

## Pages in this section

- [Session](./session.md): the handshake, readiness, heartbeats and reconnecting.
- [Capabilities](./capabilities.md): the feature names exchanged in the
  handshake and which ones are required.
- [Message Families](./messages.md): every message type, who sends it and what
  it is for.
- [Commands and Outcomes](./commands.md): how to tell sipbridge to do something
  and how to read the result.
