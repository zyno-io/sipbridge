# Commands and Outcomes

JSON examples on this page are readable projections of generated protobuf
messages. Send binary envelopes using the [wire profile](./overview.md#binary-encoding), never JSON text frames.

A `command` is how the controller makes sipbridge do something: answer a
request, place a call, send a request inside a dialog, or cancel. Every command
gets a `result`, and the most important part of a result is how *certain*
sipbridge is about what happened. This page explains how to send commands and,
above all, how to react to each kind of result.

The complete rules are in
[PROTOCOL.md sections 8–9](../spec/protocol.md#_9-commands-replay-and-failure)
and [Runtime: business selection](../runtime.md#business-selection-and-subscriptions).

## Shape

A command names the context it applies to, a sequence number, and one action.
This one answers a presented INVITE with `180 Ringing`:

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
        "authority": { "service": "sipbridge", "generation": "default", "primaryId": "<state authority ID>", "schemaVersion": 1 },
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

In this example, the `context` block is the `ContextRef` sipbridge gave you
in the `request`, copied back unchanged.

Exactly one action is present:

| Action | Fields | Use |
| --- | --- | --- |
| `respond` | `requestId`, `status`, `headers`, `body`, `principalId`, `bindingId`, `authority` | Answer a request sipbridge presented. `authority` carries the directory epoch and, for a 2xx to an initial INVITE, the scope under which the call was originally admitted. |
| `openUac` | `uri`, `method`, `body`, `headers`, `bindingId`, `principalId`, `network`, `transport` | Send a new request. An empty `method` or `INVITE` starts a call. `OPTIONS`, `MESSAGE` and `NOTIFY` send a single standalone request, after which the context is finished. `network` names an explicit target, with an optional `proxy`, `transport` and `credential` reference. |
| `dialogRequest` | `method`, `body`, `headers` | Send a request inside an existing dialog, such as BYE, re-INVITE, INFO, REFER or NOTIFY. |
| `cancel` | none | Cancel a call that is still being set up. |

You supply the content: the SDP body and any application headers. sipbridge
supplies the SIP plumbing: Via, Contact, Route, CSeq, tags and branch. Headers
you pass cannot override those.

To authenticate an outgoing request, put a `credential` reference in `network`
and answer the `credentialLookup` that follows. The schema still has
`network.username` and `network.password` fields for older software, but
sipbridge refuses commands that use them.

## Admission

With `sip.command-admission.v1`, the `respond`, `openUac` and `dialogRequest`
commands carry an `admission` block. It is the controller's statement that, as
of now, this action is still allowed under its current data:

- `directoryEpoch` is the version of the controller's routing and registration
  data the decision was based on.
- `expiresAtMs` is an absolute time after which the command must not run.
- Optionally, the exact connection to deliver on and the delivery receipt the
  controller expects.

sipbridge checks the block at the moment it runs the command. If the epoch is
out of date, the connection has changed or the time has passed, the command is
refused and its sequence number is not used up, so you can decide again with
fresh data. The block applies to this one command only; it cannot extend or
renew permission given to an earlier one. Its encoded size is limited to 4 KiB.

`cancel` needs no admission block. The controller that owns a call can cancel
it while its lease is live.

## Sequencing and replay

Each context has its own command sequence, and `sequence` must increase with
each new command. sipbridge remembers the highest sequence it accepted for a
context, what the command was, and its latest outcome. This is what stops a
duplicate from doing damage:

- **Repeating the latest sequence does not run the command twice.** If the
  identical command arrives again with the same sequence, sipbridge answers
  with what it knows about the original (finished, in progress or unknown).
  When you are missing a result, `inspect` is still the way to find out what
  happened.
- **An older sequence is rejected as stale.**
- **A command that sipbridge's SIP layer rejects uses up its sequence.** Send
  the corrected command with the next number. A refusal at the admission check
  (stale epoch, connection or expiry) does not use up the sequence.

Cleanup and cancellation have capacity set aside for them, so a busy context
cannot block you from ending a call.

## Budgets

`remainingBudgetMs` tells sipbridge how long you are prepared to wait:

| Message | Maximum `remainingBudgetMs` |
| --- | --- |
| Any request envelope | 2,000 |
| `command` with `sip.command-budget.v1` | 40,000 |

The budget is a deadline for the whole operation, set by whoever started it.
Time already spent is deducted as the operation is passed along, and a redirect
or reconnect does not reset it. SIP's own timers run separately and are not
affected by budgets.

## Result certainty

Every `result` has a `certainty`. Base your control flow on it:

| Value | Meaning | What the controller should do |
| --- | --- | --- |
| `NOT_ACCEPTED` | Refused before anything was done. `error` says why. | Fix the cause, then follow the sequence rules above for the kind of refusal. |
| `ACCEPTED` | Under way, with the SIP outcome still to come. You get this for an initial INVITE, and for a BYE once its first send is known to be `WRITTEN`. | Wait for a `dialogEvent` or a later result. A final result may follow. |
| `COMPLETED` | Finished. `sipStatus`, `request`, `response` and `snapshot` may be included. | Carry on. |
| `UNKNOWN` | sipbridge cannot tell whether the command took effect. | Do not send it again as a new command. Use `inspect` on the context to find out where things stand. |

Commands other than an initial INVITE and BYE wait for their final result
within the budget.

### Handling UNKNOWN

`UNKNOWN` is the case that needs care. It arises when something failed after
the point where the command might already have taken effect: the budget ran
out while a send was in progress, a connection broke mid-write, or the record
of the outcome was lost. The SIP message may be on its way to the peer or may
never have left.

The safe reaction is always the same. Do not issue a new command that repeats
the action, because if the first one did go out, the peer sees it twice.
Instead `inspect` the context, see what state it is actually in, and decide
from there. Until then, sipbridge blocks further work on that context that
depends on the uncertain command. A result that turns up late does not lift
that block or cause a replay.

A dropped session is the same situation in disguise: any command without a
result is unknown from your side, even if sipbridge completed it. After
reconnecting, inspect before you act.

Memory can retire a [completed reliable REGISTER procedure](../memory-core.md#completed-reliable-register-lifetime)
without a historical tombstone. A retry for that absent context returns UNKNOWN
without a snapshot. Inspection of current absence never proves that the old
command was unaccepted or its response was NOT_WRITTEN.

### Errors

A result that reports a problem carries an `error` in one of these categories:
`invalid`, `unauthorized`, `unsupported`, `staleGeneration`, `staleRevision`,
`notFound`, `ownerUnavailable`, `overloaded`, `deadlineExceeded`,
`outcomeUnknown`. Use the error text for logs and diagnosis, and `certainty`
for decisions. sipbridge never makes up a SIP response to stand in for a
transport error; if a send failed, you are told it failed.

## Delivery outcomes

Underneath command results, every individual SIP message sipbridge sends has a
delivery outcome. This covers messages you asked for and those sipbridge sends
by itself, such as retransmissions, ACKs, the `200` to a CANCEL and the `487`
that follows it:

| Outcome | Meaning |
| --- | --- |
| `WRITTEN` | The operating system accepted the complete message. This is not proof the peer received it. |
| `NOT_WRITTEN` | Nothing was sent, with certainty: the connection was gone, there was no capacity or the budget had expired. |
| `UNKNOWN` | A send started and then failed or was cut short, or the record of it was lost. The peer may have received it. |

sipbridge does not paper over these. It does not retry an uncertain send, it
does not switch to an older connection when the current one fails, and it does
not claim exactly-once delivery. If a TCP or TLS write fails partway through a
message, that connection is closed so that the next message cannot be corrupted
by the fragment.
