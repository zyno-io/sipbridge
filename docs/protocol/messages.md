# Message Families

JSON examples on this page are readable projections of generated protobuf
messages. Send binary envelopes using the [wire profile](./overview.md#binary-encoding), never JSON text frames.

This page lists the controller message families: who sends them, what
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
| `request` | S → C | Direct Memory pending-request notification, or checkpoint request presentation, with a snapshot, observed flow and exact request handle. |
| `requestDecision` (`result`) | C → S | One direct Memory policy/response decision; accepted SUBSCRIBE includes its required NOTIFY. The result proves native commitment, not physical delivery. |
| `dialogEvent` | S → C | Something happened on a dialog: `PROVISIONAL`, `ANSWERED`, `ACKNOWLEDGED`, `CANCELLED`, `ENDED`, `FAILED` or `SUBSCRIPTION_EXPIRED`. |
| `command` (`result`) | C → S | Tells sipbridge to do something on a context: `respond`, `openUac`, `dialogRequest` or `cancel`. See [Commands and Outcomes](./commands.md). |
| `flowAdoption` (`flowAdoptionResult`) | S → C | A client continued a dialog on a new connection and sipbridge has validated it. The controller confirms, so that later messages for that dialog are delivered on the new connection. The reply carries a `ContextDelivery` receipt. |
| `credentialLookup` (`credentialResult`) | S → C | An outgoing request was challenged, and sipbridge asks for the credentials behind the reference the controller supplied. They are used for the retry and never stored. |
| `flowClosed` | S → C | A client connection closed. It says nothing about other connections or about who owns a call. |
| `inspect` (`result`) | C → S | Reads the current state of a context and its most recent command, including confirmation when the context has been retired. Read-only: it never creates or takes over anything. |

### Acknowledging requests and events

Direct Memory `request` notifications are not separately acknowledged: the
controller sends the decision described in the
[native contract](../spec/protocol.md#direct-request-decision-memory).
Checkpoint request presentations and `dialogEvent` use a `result` repeating the
message's `correlationId`:

```json
{"correlationId":"effect-1","sessionId":"session-1","result":{"certainty":"ACCEPTED"}}
```

This only says "received". It does not answer the SIP request; that still takes
a `command`.

Lifecycle events can be delivered again with the same `correlationId` under their
original finite bound. Recognize those duplicates before repeating callbacks.
Fresh request presentation is one-shot and is not retried when its acknowledgement
is lost. Acknowledgement means receiver acceptance, not a completed business
handler or SIP response.

### Routing outcomes

This routing exchange and the following `ingest` apply to checkpoint peers.
Direct Memory uses the notification/decision contract above.

The controller answers `ingress` with a `routeResult` whose `outcome` is one of:

| Outcome | What sipbridge does |
| --- | --- |
| `ACCEPTED` | Nothing more at this stage; the controller has admitted the request with `ingest`. |
| `REDIRECT` | Asks the controller named in `routeResult.controller` instead. That controller must have a session open, or the request gets `503`. A request can be redirected at most twice, never to a controller already asked, and the original deadline keeps running. |
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

## Subscription liveness

`probeApplication` / `applicationStatus` check application liveness before
subscription processing is handed to another controller. This preflight is
separate from the native SIP timers and the application's recovery lease.

## Operational limits

The service has no drain command or global context inventory. Plan SIP-process
replacement as described in [Deployment](../guide/deployment.md#replacing-a-process).
The owner-scoped recovery inventory below is available to controllers that
negotiate recovery.

## Controller recovery inventory

Negotiating `sip.controller-reattach.v1` enables `controllerContexts` and its
`controllerContextsResult` reply. A request carries an optional opaque cursor and
a limit up to 64. Cursors are opaque, contain no whitespace, and are limited to
32,768 bytes; return the cursor unchanged. The reply contains at most 64 `ContextRef` values and the next
cursor. Listing is confined to the authenticated logical owner and is allowed
before ready status during recovery; it grants no new command authority.
Deduplicate results and inspect each context instead of treating pagination as
event replay. See [Controller Recovery](../guide/controller-recovery.md).
