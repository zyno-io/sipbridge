# Message Families

This page lists every message type in the control protocol: who sends it, what
it is for and what reply it expects. The payload field of an
[envelope](./overview.md#envelope) names the message type. Each type has a fixed
direction, and sipbridge rejects a message sent the wrong way, outside a
session, or without the capability it needs.

The messages are specific to SIP call control. There is no generic "send these
bytes" or "run this RPC" message. The complete rules are in
[PROTOCOL.md section 7](../spec/protocol.md#_7-message-families-and-execution-boundary).

In the tables, **C** is the controller and **S** is sipbridge.

## Current families

| Payload (reply) | Direction | Purpose |
| --- | --- | --- |
| `hello` (`helloAck`) | C → S | Opens the session. See [Session](./session.md). |
| `status` | C → S | Says whether the controller is ready, and optionally supplies the challenge policy. No reply. |
| `ping` (`pong`) | Both | Heartbeat. |
| `ingress` (`routeResult`) | S → C | "A SIP message arrived; what should happen to it?" Carries the original message, the connection it arrived on (`flow`) and, if it belongs to something sipbridge already tracks, the `context`. The controller has up to two seconds to answer. |
| `ingest` (`result`) | C → S | "Accept this message into SIP processing." The controller sends back the original bytes for a specific context, with the reason it may proceed (the admission basis, below). The result says whether sipbridge's SIP layer took the message, not whether anything was sent or the call succeeded. |
| `request` | S → C | An accepted SIP request, handed to the controller that owns it, with a snapshot of the context and a `requestId` to quote when answering. |
| `dialogEvent` | S → C | Something happened on a dialog: `PROVISIONAL`, `ANSWERED`, `ACKNOWLEDGED`, `CANCELLED`, `ENDED`, `FAILED` or `SUBSCRIPTION_EXPIRED`. |
| `command` (`result`) | C → S | Tells sipbridge to do something on a context: `respond`, `openUac`, `dialogRequest` or `cancel`. See [Commands and Outcomes](./commands.md). |
| `flowAdoption` (`flowAdoptionResult`) | S → C | A client continued a dialog on a new connection and sipbridge has validated it. The controller confirms, so that later messages for that dialog are delivered on the new connection. The reply carries a `ContextDelivery` receipt. |
| `credentialLookup` (`credentialResult`) | S → C | An outgoing request was challenged, and sipbridge asks for the credentials behind the reference the controller supplied. They are used for the retry and never stored. |
| `flowClosed` | S → C | A client connection closed. It says nothing about other connections or about who owns a call. |
| `inspect` (`result`) | C → S | Reads the current state of a context and its most recent command, including confirmation when the context has been retired. Read-only: it never creates or takes over anything. |

### Acknowledging requests and events

sipbridge wants to know that the controller received each `request` and
`dialogEvent`, so the controller acknowledges them with a `result` that repeats
the message's `correlationId`:

```json
{"correlationId":"effect-1","sessionId":"session-1","result":{"certainty":"ACCEPTED"}}
```

This only says "received". It does not answer the SIP request; that still takes
a `command`.

If the acknowledgement is lost, sipbridge delivers the message again with the
same `correlationId`. Your controller therefore has to recognize duplicates:
record each `correlationId` before acknowledging it, and when one arrives a
second time, acknowledge it again without repeating the work. Otherwise a lost
acknowledgement could make your application handle the same INVITE twice.
Acknowledge promptly, before starting any slow application logic.

### Routing outcomes

The controller answers `ingress` with a `routeResult` whose `outcome` is one of:

| Outcome | What sipbridge does |
| --- | --- |
| `ACCEPTED` | Nothing more at this stage; the controller has admitted the request with `ingest`. |
| `REDIRECT` | Asks the controller named in `routeResult.nexus` instead. That controller must have a session open, or the request gets `503`. A request can be redirected at most twice, never to a controller already asked, and the original deadline keeps running. |
| `REJECTED` | Sends the SIP status in `routeResult.sipStatus` to the client, where SIP allows a response. |
| `UNAVAILABLE` | Fails the request, normally with `503`. |

`REDIRECT` exists so that one controller can pass a request to the controller
that should handle it, for example the one that owns the call. It is
internal to your controllers: the SIP client sees no 3xx response, and no
connection or call moves. A request cannot be redirected once it has been
admitted.

### Ingest admission basis

When the controller sends `ingest`, it states why this message is allowed to
join the context:

| `admission` | Meaning |
| --- | --- |
| `EXISTING_FLOW` | The message arrived on the connection the context already uses. |
| `AUTHENTICATED_BINDING` | The controller has verified that the message comes from the authorized registration for this account. |
| `TLS_DIALOG_CONTINUITY` | The message arrived on a new TLS connection but carries the full identity of an existing TLS dialog. sipbridge verifies that before accepting. Requires `sip.dialog-continuity.v1`. |
| `TLS_TRANSACTION_CONTINUITY` | A SIP response that arrived on a new TLS connection and matches a client transaction sipbridge still has open. Requires `sip.dialog-continuity.v1`. |

These describe how a message attaches to existing SIP state. None of them
states that an account passed digest authentication, and none allows a new
INVITE, a different dialog or a registration change.

## Shared references

Messages refer to connections, contexts and processes with a handful of
structured references:

| Reference | Meaning |
| --- | --- |
| `ProcessRef` | A process: its `instanceId` plus the random `epoch` of its current run. |
| `FlowRef` | One client connection: the sipbridge process holding it, a never-reused `connectionId`, the observed `sourceIp` and `sourcePort`, the `listener` and the actual `transport` (`udp`, `tcp`, `tls`). It identifies a socket, not an account. |
| `StateAuthority` | The state store: `service` (the namespace), `generation`, `primaryId`, `schemaVersion`. |
| `StateRef` | Where a context's state lives: the authority plus `groupId`, `incarnation`, `callId` and `partition`. |
| `ContextRef` | One context: its `state` plus a never-reused `id`. The `owner` field names the sipbridge process that replied and is informational. |
| `ContextSnapshot` | The current picture of a context: phase, identity, tags, targets, SDP, revisions, latest command sequence and delivery receipt. |

You do not need to build or interpret these. Store each reference as you
received it and send it back unchanged.

## Historical families

The schema defines more message types than the table above: `egress`, `write`,
`delivery`, `sendPermit`, `deliverEffect`, `inspectEffect`, `settleEffect`,
`retireAuthority`, `probeApplication`, `applicationStatus`, `reconcileContext`,
`reconcileAuthority` and `authorityReceipt`.

Most of these belong to an earlier design, in which controllers approved and
relayed each individual send. The current service sends by itself and does not
use them; `reconcileContext` is refused outright. They stay in the schema so
that older software and tests can still decode them. The exception is
`probeApplication` / `applicationStatus`, which is still used as a check before
subscription processing is handed to another controller.

A new controller can ignore this group.

## Proposed, not implemented

PROTOCOL.md describes a drain protocol (`drain.begin`, `drain.status`,
`drain.complete`), a way to list an instance's contexts page by page, and
bounded output queues. These are designs only. They are not in the schema or
the service, so do not build against them yet.
