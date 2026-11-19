---
description: How to send commands, how admission, sequence numbers and budgets work, what each result certainty means, and how to handle UNKNOWN.
---

# Commands and Outcomes

Messages use the [strict JSON encoding](./overview.md#json-encoding).

A `command` makes sipbridge do something on a context: answer a request, place
a call, send a request inside a dialog, or cancel. Every command gets a
`result`, and the most important part of a result is how *certain* sipbridge
is about what happened. This page explains how to send commands and how to
react to each result.

## Shape

A command names its context, a sequence number, an admission block and one
action. This one answers a request with `180 Ringing` (the values are made up;
copy the context reference and `requestId` from the `request` you received):

```json
{
  "correlationId": "cmd-2",
  "sessionId": "0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841",
  "remainingBudgetMs": 2000,
  "command": {
    "context": {"owner":{"instanceId":"sipbridge-0","epoch":"6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30"},"id":"4be1f0c27d9a3e5b6c8d0f1a2b3c4d5e","state":{"authority":{"service":"sipbridge","generation":"default","primaryId":"a7c3e9f1-2b4d-4e6a-8c0f-1d3b5a7e9c21","schemaVersion":1},"groupId":"4be1f0c27d9a3e5b6c8d0f1a2b3c4d5e","incarnation":"e3b8a1d4-5c6f-4a2b-9d0e-7f1c3b5a8e92","callId":"a84b4c76e66710@phone.example.test"}},
    "sequence": "2",
    "admission": {"directoryEpoch":"directory-42","expiresAtMs":"1795017604000"},
    "respond": {"requestId":"r-9c1e7a3f5b2d4e6a8c0f1b3d5e7a9c2e4f6a8b0d1c3e5f7a","status":180}
  }
}
```

Exactly one action is present:

| Action | Fields | Use |
| --- | --- | --- |
| `respond` | `requestId`, `status`, `headers`, `body`, `principalId`, `bindingId` | Answer a request you accepted with a `status: 0` decision. |
| `openUac` | `uri`, `method`, `headers`, `body`, `principalId`, `bindingId`, `network`, `transport` | Start an outgoing call (`method` empty or `INVITE`), or send a standalone `OPTIONS`, `MESSAGE`, `NOTIFY`, `REGISTER` or `PUBLISH`. |
| `dialogRequest` | `method`, `headers`, `body` | A request inside an existing call or subscription: `BYE`, re-`INVITE`, `INFO`, `REFER`, `UPDATE`, `NOTIFY`, or an `ACK` carrying your SDP answer. |
| `cancel` | none | Cancel an outgoing call that has not been answered yet. |

You supply content: the SDP body and your own headers. sipbridge supplies the
SIP plumbing (Via, Contact, Route, CSeq, tags and branch) and refuses headers
that would override it. In `openUac` you may set `From` and `To`; they default
to `sip:sipbridge@<advertised address>` and the request URI.

### Starting a new context

`openUac` creates a new context. Choose a new ID of your own (a reference
string that does not start with `in-`), send `{"id": "<your id>"}` as the
context and `"1"` as the sequence. The result's snapshot holds the full
context reference to use from then on. The destination must be explicit:

- **a registered phone:** set `principalId` and `bindingId`, and pass the
  phone's current connection as `admission.selectedFlow` (the `flow` from its
  REGISTER, as long as that connection is still open);
- **a carrier or server:** set `network`, with an optional `proxy` (next hop,
  without changing the request URI), `transport` and `credential` reference.

A SIP URI alone never makes sipbridge dial an address. A `sips:` target is
never downgraded to an unencrypted transport. With Redis state, the only
standalone requests a phone can send in (outside a call) are REGISTER and
SUBSCRIBE; with memory state, other standalone requests such as MESSAGE are
offered to you as well.

[A Call, Step by Step](../guide/call-walkthrough.md#part-3-an-outgoing-call)
has complete examples.

## Admission

`respond`, `openUac` and `dialogRequest` carry an `admission` block: your
statement that, right now, this action is allowed under your current data.

- `directoryEpoch`: the version of your account and routing data the decision
  is based on. If a context has recorded a different epoch, the command is
  refused.
- `expiresAtMs`: an absolute time, in Unix milliseconds, after which the
  command must not run.
- `expectedDelivery`: the context's current delivery receipt, exactly as in
  its latest snapshot. Leave it out when the snapshot has none.
- `selectedFlow`: the connection to send on, for requests to a registered
  phone.

sipbridge checks the block at the moment it runs the command. If the epoch,
the delivery or the connection has changed, or the time has passed, the
command is refused before it uses up a sequence number, so you can decide
again with fresh data. The block covers this command only and is limited to
4 KiB. `cancel` needs no admission block.

## Sequence numbers

Each context has its own command sequence. Every command (and the decision
that took the request) must use the next number: the latest snapshot's
`commandSequence` plus one.

- **Repeating the exact same command with the same sequence does not run it
  twice.** sipbridge answers with what it knows about the original.
- **A different command with an already used number does not run.** It
  returns `UNKNOWN`, because sipbridge will not pretend the earlier command
  with that number did not run.
- **A command refused by sipbridge's SIP layer uses up its number.** Send the
  corrected command with the next one. A refusal at the admission check does
  not use it up.

Cleanup and cancellation have capacity reserved for them, so a busy context
cannot stop you from ending a call.

## Budgets

`remainingBudgetMs` says how long you are prepared to wait:

| Message | Maximum `remainingBudgetMs` |
| --- | --- |
| `requestDecision`, `inspect` and other requests | 2,000 |
| `command` | 40,000 |

The budget is a deadline for the whole operation. SIP's own timers run
separately and are not changed by it. If the budget runs out after sipbridge
has started the work, the result is `UNKNOWN` (see below), not a failure.

## Result certainty

Every `result` has a `certainty`. Base your control flow on it, and use the
`error` text only for logs:

| Value | Meaning | What to do |
| --- | --- | --- |
| `NOT_ACCEPTED` | The command sent no SIP message. If a SIP rule caused the refusal, `sipStatus` gives its code, such as `491` when an INVITE is already pending on the dialog. For other refusals, `sipStatus` is `0`. | Use `sipStatus` to handle a SIP refusal. Follow the sequence rules above. |
| `ACCEPTED` | Started; the final SIP outcome comes later. | Wait for a `dialogEvent` or a later result. |
| `COMPLETED` | Finished; `sipStatus`, `request`, `response` and `snapshot` may be included. | Carry on. |
| `UNKNOWN` | sipbridge cannot tell whether the command took effect. | Do not repeat it. `inspect` the context. |

When each command reports:

| Command | Result |
| --- | --- |
| `openUac` INVITE | `ACCEPTED` once committed; ringing, answer and failure arrive as `dialogEvent`s. |
| `respond` | `COMPLETED` with the status once the response has been written to the connection; `UNKNOWN` if it could not be written with certainty. |
| `dialogRequest` BYE | `ACCEPTED` with the request once the BYE has been written; may later become `COMPLETED` with the peer's answer. |
| Other `dialogRequest`s, standalone `openUac` | `COMPLETED` when the peer's final response arrives or SIP times out; `UNKNOWN` if the request could not be written with certainty. |

If a re-INVITE was written and its SIP timer runs out, the result is
`COMPLETED` with `sipStatus` set to `0`. No final response came back. The
existing call stays up and you can still send a BYE. A re-INVITE can also be
refused with `491` while the previous INVITE's ACK is still being sent.

### Handling UNKNOWN

`UNKNOWN` arises when something failed after the point where the command might
already have taken effect: the budget ran out during a send, a connection
broke mid-write, or the result was lost with the session. The SIP message may
be on its way to the peer, or may never have left.

The safe reaction is always the same. **Do not send a new command that repeats
the action**; if the first one did go out, the peer sees it twice. Instead
`inspect` the context, see what state it is in, and decide from there. A
result that arrives later does not change that rule and never causes a
replay.

A dropped session is the same situation in disguise: any command without a
result is unknown from your side, even if sipbridge completed it. Remember too
that a dropped session ends your calls (see [Session](./session.md#when-a-session-ends)).

`inspect` reports what is true *now*. If a context no longer exists, that
does not prove that an earlier command was refused or that a response was not
sent; it only says the context has gone.

## Delivery outcomes

Underneath command results, every SIP message sipbridge sends has its own
delivery outcome. This covers messages you asked for and those sipbridge
sends by itself, such as retransmissions, ACKs, the `200` to a CANCEL and the
`487` that follows it:

| Outcome | Meaning |
| --- | --- |
| `WRITTEN` | The operating system accepted the complete message for sending. Not proof that the peer received it. |
| `NOT_WRITTEN` | Nothing was sent, with certainty: the connection was gone, there was no capacity, or the deadline had passed. |
| `UNKNOWN` | A send started and then failed or was cut short, or its record was lost. The peer may have received it. |

sipbridge does not paper over these. It never retries an uncertain send,
never switches to an older connection when the current one fails, and does
not claim exactly-once delivery. If a TCP or TLS write fails partway through a
message, that connection is closed so the next message cannot be corrupted by
the fragment.
