# Commands and Outcomes

A `command` asks the native owner of one context to perform one action. Each
command is sequenced, bounded by a deadline, and answered by a `result` whose
certainty must be honored exactly. The normative rules are in
[PROTOCOL.md sections 8–9](../spec/protocol.md#_9-commands-replay-and-failure)
and [Runtime: business selection](../runtime.md#business-selection-and-subscriptions).

## Shape

```json
{
  "correlationId": "cmd-0042",
  "sessionId": "<session UUID>",
  "remainingBudgetMs": 2000,
  "command": {
    "context": {
      "owner": { "instanceId": "sipbridge-0", "epoch": "<sipbridge process UUID>" },
      "id": "<context id>",
      "state": {
        "authority": { "service": "sipbridge", "generation": "default", "primaryId": "<state authority ID>", "schemaVersion": 2 },
        "groupId": "<group id>",
        "incarnation": "<incarnation>",
        "callId": "<Call-ID>",
        "partition": 7
      }
    },
    "sequence": "3",
    "admission": {
      "directoryEpoch": "<controller directory epoch>",
      "expiresAtMs": "<absolute uint64 milliseconds>"
    },
    "respond": {
      "requestId": "<requestId from the presented request>",
      "status": 180
    }
  }
}
```

Exactly one action is present:

| Action | Fields | Use |
| --- | --- | --- |
| `respond` | `requestId`, `status`, `headers`, `body`, `principalId`, `bindingId`, `authority` | Answer a presented request. `authority` carries the directory epoch, and for an initial INVITE 2xx, the original admitted scope. |
| `openUac` | `uri`, `method`, `body`, `headers`, `bindingId`, `principalId`, `network`, `transport` | Originate. Empty `method` or `INVITE` creates a call; `OPTIONS`, `MESSAGE` and `NOTIFY` run a standalone transaction and retire. `network` names an explicit target with optional `proxy`, `transport` and an opaque `credential` reference. |
| `dialogRequest` | `method`, `body`, `headers` | Send an in-dialog request, such as BYE, re-INVITE, INFO, REFER or NOTIFY. |
| `cancel` | none | Cancel pending setup. Allowed for the exact owner with a live term, without refreshing earlier setup admission. |

The controller supplies SDP bodies and permitted application headers.
sipbridge builds protocol identity: Via, Contact, Route, CSeq, tags and branch.
Headers may not override those fields, internal credentials or source evidence.
`network.username` and `network.password` are decodable compatibility fields;
the service refuses them. Supply a `credential` reference and answer
`credentialLookup` instead.

## Admission

With `sip.command-admission.v1`, `respond`, `openUac` and `dialogRequest` carry
`admission`: the directory epoch, an absolute semantic expiry, and optionally the
selected exact flow and expected delivery receipt. Admission is bound to the
session, owner term, context, sequence and action. A stale epoch, delivery or
expiry is refused before a new sequence is consumed; an exact replay keeps its
original result. The encoded admission is limited to 4 KiB. Later commands cannot
refresh earlier setup.

## Sequencing and replay

- Each context retains the highest accepted command sequence, its operation and
  latest outcome.
- Repeating the latest sequence returns its known or in-progress outcome. An
  older sequence is stale.
- A new command rejected during native execution consumes its sequence.
  Admission refusals (stale epoch, delivery or expiry) do not.
- Terminal cleanup and SIP cancellation have reserved capacity, so they are not
  blocked behind other work.

## Budgets

| Message | Maximum `remainingBudgetMs` |
| --- | --- |
| Any RPC envelope | 2,000 |
| `command` with `sip.command-budget.v1` | 40,000 |

The initiator owns the end-to-end deadline. Relays deduct elapsed time and never
reset it on redirect or reconnect. Native SIP timers are independent of these
budgets.

## Result certainty

`result.certainty`:

| Value | Meaning | What the controller should do |
| --- | --- | --- |
| `NOT_ACCEPTED` | Refused before execution. `error` explains. | Fix the cause; a new command needs a new sequence. |
| `ACCEPTED` | Execution started; the SIP outcome arrives later through events or snapshots. Used for initial INVITE setup, and for a BYE once its exact first write is proven `WRITTEN`. | Wait for `dialogEvent` or later results. A terminal result may replace the acceptance. |
| `COMPLETED` | Final result known. `sipStatus`, `request`, `response` and `snapshot` may be present. | Proceed. |
| `UNKNOWN` | Evidence was lost after possible acceptance. Dependent work on the context is fenced. | Do not resend. Use `inspect` on the same context to reconcile. |

Other in-dialog and standalone commands wait for their final result within the
budget. A timeout after admission stays `UNKNOWN`; a late result cannot clear the
fence or trigger a replay.

PROTOCOL.md defines the error categories (`invalid`, `unauthorized`,
`unsupported`, `staleGeneration`, `staleRevision`, `notFound`,
`ownerUnavailable`, `overloaded`, `deadlineExceeded`, `outcomeUnknown`), each
with execution certainty. Treat the `error` string as diagnostic and rely on
`certainty` for control flow. A transport error is never turned into a
fabricated SIP final response.

## Delivery outcomes

Every SIP message sent, whether a command result, a cached response, CANCEL
200, INVITE 487, ACK, retransmission or timer output, passes through the same
physical boundary:

| Outcome | Meaning |
| --- | --- |
| `WRITTEN` | The socket write call accepted the complete message. Not proof of receipt. |
| `NOT_WRITTEN` | Proven that no write started: stale flow, no capacity or expired budget. |
| `UNKNOWN` | A write began and failed or was partial, or evidence was lost. Delivery may have happened. |

There is no transparent retry, no fallback to an older flow and no exactly-once
claim. A TCP/TLS write that fails mid-message closes that exact stream before
another message can start on it.
