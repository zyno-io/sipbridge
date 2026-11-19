---
description: Every control message a controller exchanges with sipbridge, who sends it, what reply it expects, and the exact rules for request decisions.
---

# Message Families

Messages use the [strict JSON encoding](./overview.md#json-encoding).

The payload field of an [envelope](./overview.md#envelope) names its message
type. Each type has a fixed direction; sipbridge closes a session that sends a
message the wrong way, without its capability, or outside a session. The
messages are specific to SIP call control: there is no generic "send these
bytes" message.

In the tables, **C** is the controller and **S** is sipbridge.

## Messages a controller uses

| Payload (reply) | Direction | Purpose |
| --- | --- | --- |
| `hello` (`helloAck`) | C → S | Opens the session. See [Session](./session.md). |
| `status` | C → S | Says whether the controller is ready, and optionally supplies the local challenge policy. No reply. |
| `ping` (`pong`) | Both | Heartbeat. |
| `request` | S → C | A SIP request that needs your decision. Offered once; answer with `requestDecision`, not with a `result`. |
| `requestDecision` (`result`) | C → S | Your one decision for that request. See [below](#deciding-a-request). |
| `command` (`result`) | C → S | Do something on a context: `respond`, `openUac`, `dialogRequest` or `cancel`. See [Commands and Outcomes](./commands.md). |
| `inspect` (`result`) | C → S | Read a context's current snapshot and latest command outcome. Read-only. |
| `dialogEvent` (`result`) | S → C | Something happened on a call or subscription. |
| `flowAdoption` (`flowAdoptionResult`) | S → C | A phone continued a subscription on a new connection; publish it if you agree. |
| `credentialLookup` (`credentialResult`) | S → C | An outgoing request was challenged; supply the credential behind your reference. |
| `flowClosed` | S → C | A phone connection closed. No reply. |

The schema contains further message types (`ingress`, `routeResult`,
`ingest`, `write`, `controllerContexts` and others). They belong to older
designs kept for [test fixtures](../fixtures.md). A serving sipbridge never
sends them, and sending them closes the session.

## Shared references

Messages refer to processes, connections and calls with a few structured
references. Store each one as you received it and send it back unchanged; you
never build or interpret them yourself.

| Reference | Meaning |
| --- | --- |
| `ProcessRef` | One run of a process: its `instanceId` plus the random `epoch` of that run. |
| `FlowRef` | One phone or trunk connection as sipbridge observed it: the sipbridge process holding it, a never-reused `connectionId`, the observed `sourceIp` and `sourcePort`, the `listener` and the actual `transport` (`udp`, `tcp` or `tls`). It identifies a socket, not an account. |
| `ContextRef` | One call, subscription or standalone transaction (a **context**). Its `owner` field names the process that replied and is informational. |
| `ContextSnapshot` | The current picture of a context: kind, tags, URIs, SDP, `answered` and `ended` flags, `revision`, `commandSequence`, owning controller, account names and current delivery receipt. |

## Deciding a request

A `request` carries a `snapshot` of its context, a `requestId`, the original
SIP `message` and the `flow` it arrived on. It may also carry a `flowAdoption`
proposal, described below. Answer it with exactly one `requestDecision`
before its deadline, normally about two seconds after arrival. The envelope
must carry a nonzero `remainingBudgetMs`.

A REGISTER accepted with `200`:

```json
{
  "correlationId": "decision-7",
  "sessionId": "0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841",
  "remainingBudgetMs": 2000,
  "requestDecision": {
    "context": {"owner":{"instanceId":"sipbridge-0","epoch":"6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30"},"id":"7a2c4e6f8b0d1f3a5c7e9b1d3f5a7c9e","state":{"authority":{"service":"sipbridge","generation":"default","primaryId":"a7c3e9f1-2b4d-4e6a-8c0f-1d3b5a7e9c21","schemaVersion":1},"groupId":"7a2c4e6f8b0d1f3a5c7e9b1d3f5a7c9e","incarnation":"d1f3a5c7-e9b1-4d3f-8a7c-9e2b4d6f8a10","callId":"5c1d8e2f@phone.example.test"}},
    "requestId": "r-2e4f6a8b0c1d3e5f7a9b1c2d3e4f5a6b7c8d9e0f1a2b3c4d",
    "sequence": "1",
    "admission": {"directoryEpoch":"directory-42","expiresAtMs":"1795017602000"},
    "basis": "AUTHENTICATED_BINDING",
    "principalId": "alice",
    "bindingId": "alice-desk-phone",
    "status": 200
  }
}
```

A SUBSCRIBE for BLF accepted with `200`, including the first NOTIFY:

```json
{
  "correlationId": "decision-8",
  "sessionId": "0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841",
  "remainingBudgetMs": 2000,
  "requestDecision": {
    "context": {"owner":{"instanceId":"sipbridge-0","epoch":"6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30"},"id":"9b1d3f5a7c9e2b4d6f8a0c1e3f5a7b9d","state":{"authority":{"service":"sipbridge","generation":"default","primaryId":"a7c3e9f1-2b4d-4e6a-8c0f-1d3b5a7e9c21","schemaVersion":1},"groupId":"9b1d3f5a7c9e2b4d6f8a0c1e3f5a7b9d","incarnation":"f3a5c7e9-b1d3-4f5a-9c7e-1b3d5f7a9c21","callId":"8e3f1a7b@phone.example.test"}},
    "requestId": "r-4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c4e6b8d0f",
    "sequence": "1",
    "admission": {"directoryEpoch":"directory-42","expiresAtMs":"1795017602000"},
    "basis": "AUTHENTICATED_BINDING",
    "principalId": "alice",
    "bindingId": "alice-desk-phone",
    "status": 200,
    "notify": {"method":"NOTIFY","headers":[{"name":"Content-Type","value":"application/dialog-info+xml"}],"body":"PD94bWwgdmVyc2lvbj0iMS4wIj8+CjxkaWFsb2ctaW5mbyB4bWxucz0idXJuOmlldGY6cGFyYW1zOnhtbDpuczpkaWFsb2ctaW5mbyIgdmVyc2lvbj0iMCIgc3RhdGU9ImZ1bGwiIGVudGl0eT0ic2lwOjEwMDBAZXhhbXBsZS50ZXN0Ii8+Cg=="}
  }
}
```

Use real values from the `request` you received, and compute `expiresAtMs`
from the current time.

### Fields

| Field | Rule |
| --- | --- |
| `context`, `requestId` | Copied from the `request`. |
| `sequence` | The snapshot's `commandSequence` plus one. A missing `commandSequence` means `0`. |
| `status` | `0`, or a final SIP status from 200 to 699. `0` means "accepted; I will respond later with `respond` commands" and cannot carry `headers` or `body`. REGISTER and SUBSCRIBE need a final status. |
| `headers`, `body` | Your part of the response: for example `Content-Type` and an SDP body. Headers that sipbridge owns, such as Via, To, Call-ID, CSeq, Contact, Route and Content-Length, are refused. |
| `admission` | `directoryEpoch` (your data version), `expiresAtMs` (absolute deadline) and, if the snapshot has a `delivery`, the same value as `expectedDelivery`. Required for a positive decision. |
| `basis` | Why a positive decision is allowed; see the next table. |
| `principalId`, `bindingId` | Your names for the account and the device registration. Must match any values already on the context. |
| `notify` | Required exactly when you accept a SUBSCRIBE (2xx), and forbidden otherwise. Its `method` must be `NOTIFY`. |
| `delivery` | Only when the `request` carried a `flowAdoption` proposal; see below. |

A rejection (status 300 to 699) carries no `basis`, `principalId`,
`bindingId` or `delivery`.

| `basis` | Use when |
| --- | --- |
| `AUTHENTICATED_BINDING` | You verified the account. `principalId` and `bindingId` are required. This is the usual choice for a new REGISTER, SUBSCRIBE or INVITE. |
| `EXISTING_FLOW` | The request arrived on the connection the context already uses, for example a request inside a call on the phone's original connection. |
| `TLS_DIALOG_CONTINUITY` | The request arrived over TLS on a new connection for an existing TLS dialog or subscription, and sipbridge has checked its full dialog identity. |

None of these means "this account passed digest authentication". That is
your controller's judgment, made before you choose a basis.

### NOTIFY content

In a `notify` block, and in later `dialogRequest` NOTIFY commands, you supply
only the body and its `Content-Type`. sipbridge adds the `Event` header from
the subscription and a `Subscription-State` of `active;expires=<seconds
left>`. To end a subscription, send a NOTIFY with a `Subscription-State`
header whose value starts with `terminated`. Supported event packages are
`dialog` (BLF) and `message-summary` (MWI).

### A request on a new connection

When a request continues an existing dialog on a different connection than
the one sipbridge last used, the `request` carries a `flowAdoption` proposal:
the new `flow` and a `nativeRevision`. To accept the request on that
connection, record it in your own data, then include the receipt in the
decision's `delivery` field: the proposal's `flow` and `nativeRevision`, your
current `directoryEpoch` (equal to `admission.directoryEpoch`) and a
nonempty `registrationTicket` from your registration ordering. Keep
`admission.expectedDelivery` set to the *previous* receipt from the snapshot.
To refuse, send a rejection status instead.

### The result

| `certainty` | Meaning |
| --- | --- |
| `ACCEPTED` | sipbridge recorded the decision and started sending any response and NOTIFY. Not proof that they arrived. |
| `NOT_ACCEPTED` | Refused, with an `error`. Nothing was sent for it. A refusal may still use up the sequence number; read the snapshot before trying again. |
| `UNKNOWN` | sipbridge cannot tell whether the decision took effect, for example because the context has since gone. Do not repeat it. |

If no valid decision arrives in time, sipbridge answers the caller with `503`
itself. A request is never offered a second time, and a lost result never
gives you permission to repeat the decision on another session.

## Events

`dialogEvent` reports progress on a context:

| `kind` | Meaning |
| --- | --- |
| `PROVISIONAL` | An outgoing call received a provisional response such as `180 Ringing`. |
| `ANSWERED` | An outgoing call was answered. |
| `ACKNOWLEDGED` | The phone acknowledged your `200 OK` to its INVITE. Initial ACKs carry only a snapshot. With `sip.reinvite-ack.v1`, re-INVITE ACKs carry `message` and an `inviteCseq` correlation value (which may be zero). Message presence distinguishes them from initial ACKs. |
| `CANCELLED` | The caller cancelled before the call was answered. |
| `ENDED` | The call ended, for example by a BYE from either side. |
| `FAILED` | An outgoing call failed, for example a final error response or a transport failure. |
| `SUBSCRIPTION_EXPIRED` | A subscription ended by expiry. |

Each event carries a frozen `snapshot` and, where there is one, the SIP
`message` that caused it. Reply with a `result` repeating its `correlationId`:

```json
{"correlationId":"ev-7f3a","sessionId":"0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841","result":{"certainty":"ACCEPTED"}}
```

This only says "received". Without that reply, sipbridge redelivers the same
event with the same `correlationId`, with growing pauses, until the event's
delivery deadline passes. Recognize those duplicates. A provisional event can
be dropped once the final outcome is known; the final events are what to rely
on.

## Connection changes

`flowAdoption` arrives when a phone refreshes a subscription from a new
connection (with Redis, also when a call continues on a new TLS connection).
It names the context, account, new `flow` and a `nativeRevision`. If you agree,
record the new connection and reply with a `flowAdoptionResult` whose
`receipt` holds that `flow`, the same `nativeRevision`, your current
`directoryEpoch` and a `registrationTicket`. Reply with an `error` to refuse.

`flowClosed` names a connection that has closed. It does not end calls by
itself, and it says nothing about other connections.

## Credentials

`credentialLookup` names the context and the `credential` reference you put in
`openUac.network.credential`. Reply with a `credentialResult` containing
`username`, `password` and `version`, or an `error`. sipbridge uses the
password for one digest computation and keeps nothing.
