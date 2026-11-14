# Protocol Overview

The control protocol connects your application to sipbridge. The controller
opens a WebSocket to sipbridge, and from then on the two exchange binary protobuf
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
| Subprotocol | `sipbridge.control.protobuf.v1` |
| Authentication | mTLS by default, or explicit bearer mode; see [Identities and TLS](../guide/security.md#control-connection) |
| `Origin` header | Must be absent |
| Compression | Disabled |
| Frames | Binary only; text messages are rejected |
| Who connects | Always the controller |

[Identities and TLS](../guide/security.md#control-connection) explains the
certificate checks, and [Getting Started](../guide/getting-started.md) has a
small Go program that connects.

## Envelope

Every binary WebSocket message contains one generated protobuf `Envelope`. It
has common fields and one recognized payload selected by its protobuf oneof:

| Field | Type | Meaning |
| --- | --- | --- |
| `correlationId` | string | An ID chosen by the sender of a request and repeated in the reply, so the two can be matched. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. Make it unique within one run of your process. It is only a label and grants nothing. |
| `sessionId` | string | Issued by sipbridge in `helloAck`. Leave it out of `hello`; include it, exactly, in every later message. |
| `remainingBudgetMs` | uint32 | How many milliseconds the sender is still willing to wait for this operation. Leave it out of `hello`, `status`, `ping` and `pong`. At most 2,000, or 40,000 for a `command` when `sip.command-budget.v1` was negotiated. |
| one payload field | message | Exactly one of `hello`, `helloAck`, `status`, `ping`, `pong`, `ingress`, `routeResult`, `ingest`, `request`, `command`, `result`, … |

The following JSON is a readable projection, not a text frame:

```json
{"correlationId":"op-17","sessionId":"6f0d6a4e-1c1b-4f4e-9b52-3f8f2f6c2a10","remainingBudgetMs":1800,"inspect":{"context":{"id":"ctx-1"}}}
```

This example shows the shape of an envelope. A real `inspect` carries the whole
`ContextRef` you received earlier, not just an ID.

## Binary encoding

Use the generated Go or TypeScript protobuf codec from the exact schema receipt
used by the service. A received message is bounded before decoding. Standard
protobuf defaults, unknown-field handling and oneof selection apply; sipbridge
then checks the recognized payload/action, required semantic enum values,
direction and authority. Unknown optional fields are discarded.

TypeScript codecs represent 64-bit counters as `bigint` and bytes as byte arrays.
Go uses the generated integer and byte types. Generated JSON conversions remain
useful for application storage and readable examples; never send those projections
as text control messages. There is no JSON fallback or legacy subprotocol.

The exact decoder rules, including the known-message recursion bound and standard
handling of skipped unknown groups, belong to
[PROTOCOL.md section 3](../spec/protocol.md#binary-websocket-framing).

## Size limits

The complete encoded protobuf message must fit the envelope limit agreed in the
handshake: at most 1 MiB and at least 512 bytes. A SIP message carried inside
an envelope can be up to 64 KiB.

## Message flow

A session begins with `hello`, `helloAck` and readiness `status`, then carries
heartbeats and SIP work. The default Memory runtime sends a `request` notification; the controller
submits `requestDecision` and receives the committed `result`. A committed
decision permits controller publication; physical SIP delivery has its own
outcome. The [direct-decision contract](../spec/protocol.md#direct-request-decision-memory)
owns the flow and exact admission/replay rules. Redis checkpoint execution retains this
routing/presentation exchange:

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
