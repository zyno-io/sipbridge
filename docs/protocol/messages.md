# Message Families

Each envelope carries one payload. Both sides validate direction, session,
negotiated capability and payload shape. Payload names do not imply a generic
RPC, an SRF-style tunnel or an arbitrary raw-wire interface. Normative semantics
are in [PROTOCOL.md section 7](../spec/protocol.md#_7-message-families-and-execution-boundary).

Direction legend: **C** controller, **S** sipbridge.

## Current families

| Payload (reply) | Direction | Purpose |
| --- | --- | --- |
| `hello` (`helloAck`) | C → S | Session negotiation. See [Session](./session.md). |
| `status` | C → S | Readiness and optional challenge policy. No reply. |
| `ping` (`pong`) | Both | Correlated heartbeat. |
| `ingress` (`routeResult`) | S → C | A SIP message that needs a business routing decision, with its observed `flow` and, when known, its logical `context`. Sent before native acceptance, within the 2-second route budget. |
| `ingest` (`result`) | C → S | The controller asks the native owner to admit the original bytes for an exact logical context, with an admission basis. A result is not proof of business success or a socket write. |
| `request` | S → C | A natively admitted request presented to its owning controller, with a context snapshot and a `requestId` handle. |
| `dialogEvent` | S → C | Lifecycle observation: `PROVISIONAL`, `ANSWERED`, `ACKNOWLEDGED`, `CANCELLED`, `ENDED`, `FAILED`, `SUBSCRIPTION_EXPIRED`. |
| `command` (`result`) | C → S | Sequenced action on a context: `respond`, `openUac`, `dialogRequest` or `cancel`. See [Commands and Outcomes](./commands.md). |
| `flowAdoption` (`flowAdoptionResult`) | S → C | After native validation of a continuing request on a new flow, asks the controller to publish ordered context delivery. The receipt is a `ContextDelivery`. |
| `credentialLookup` (`credentialResult`) | S → C | Transient lookup of an opaque outbound credential reference for a digest retry. The reply never enters retained state. |
| `flowClosed` | S → C | An exact ingress flow closed. Affects no other flow or logical owner. |
| `inspect` (`result`) | C → S | Read an existing logical context and its latest operation, with positive retirement proof when applicable. Never allocates or adopts. |

`request` and `dialogEvent` are delivered as correlated calls whose
`correlationId` is the retained effect ID. Redelivery after a lost reply reuses
that ID, so controllers must deduplicate on it.
Acknowledge a validated, recorded receipt with a `result` payload using the same
correlation and current session IDs before doing application policy work:

```json
{"correlationId":"effect-1","sessionId":"session-1","result":{"certainty":"ACCEPTED"}}
```

This acknowledges receipt, not a completed SIP response or dialog mutation.
Retain the deduplication result before acknowledging and reuse it after redelivery.
Application actions such as answering an INVITE still require a sequenced native
command; do not rerun business policy just because its receipt was redelivered.


### Routing outcomes

`routeResult.outcome` is one of:

| Outcome | sipbridge action |
| --- | --- |
| `ACCEPTED` | The request was admitted; nothing further at routing level. |
| `REDIRECT` | Retry with the controller named in `routeResult.nexus`, which must have an established session; otherwise `503`. At most two redirects, no repeated controller, same original deadline. |
| `REJECTED` | Send `routeResult.sipStatus` to the client where a response is legal. |
| `UNAVAILABLE` | Fail boundedly, normally with `503`. |

A redirect is an internal routing step, not a SIP 3xx. It moves no socket,
context or call. A redirect after native acceptance is forbidden.

### Ingest admission basis

| `admission` | Meaning |
| --- | --- |
| `EXISTING_FLOW` | Only the retained physical flow may continue. |
| `AUTHENTICATED_BINDING` | The controller verified the exact authorized account binding. |
| `TLS_DIALOG_CONTINUITY` | A native continuity attempt for one retained TLS dialog. Requires `sip.dialog-continuity.v1`. |
| `TLS_TRANSACTION_CONTINUITY` | A TLS response admitted only after the native library confirms its live client transaction. Requires `sip.dialog-continuity.v1`. |

None of these asserts digest authentication or authorizes a new INVITE, another
dialog or a registration change.

## Shared references

| Reference | Meaning |
| --- | --- |
| `ProcessRef` | `instanceId` plus unpredictable per-start `epoch`. |
| `FlowRef` | Ingress `ProcessRef`, never-reused `connectionId`, observed `sourceIp`/`sourcePort`, `listener` and actual `transport` (`udp`, `tcp`, `tls`). Identifies a socket, not an account. |
| `StateAuthority` | `service` (namespace), `generation`, `primaryId`, `schemaVersion`. |
| `StateRef` | Authority, `groupId`, `incarnation`, `callId`, `partition`. |
| `ContextRef` | Logical `state` plus never-reused `id`. `owner` is the replying executor, metadata only. |
| `ContextSnapshot` | Current native phase, identity, tags, targets, SDP, revisions, latest command sequence and delivery receipt for a context. |

Treat every reference as opaque and return it exactly as received.

## Historical families

The schema still defines `egress`, `write`, `delivery`, `sendPermit`,
`deliverEffect`, `inspectEffect`, `settleEffect`, `retireAuthority`,
`probeApplication`, `applicationStatus`, `reconcileContext`,
`reconcileAuthority` and `authorityReceipt`. They remain for compatible readers
and test fixtures. The current native producer does not use trusted per-send
grants, controller-relayed physical writes, settlement or authority-retirement
RPCs, and `reconcileContext` is refused. Subscription-controller probes
(`probeApplication`/`applicationStatus`) keep an explicit business preflight
role. Decoding a historical family never authorizes its removed operation.

## Proposed, not implemented

PROTOCOL.md describes `drain.begin`, `drain.status`, `drain.complete`, context
inventory pages and bounded output queues. None of these exist in the schema or
the service. They require canonical schema and capability design before use.
