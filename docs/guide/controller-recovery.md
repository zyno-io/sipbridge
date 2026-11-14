# Controller Recovery

JSON examples on this page are readable projections of generated protobuf
messages. Send binary envelopes using the [wire profile](../protocol/overview.md#binary-encoding), never JSON text frames.

A controller can restart without immediately giving up its established SIP calls.
To use this, negotiate `sip.controller-reattach.v1`, save the recovery handle from
`helloAck`, and restore your own application state before the timeout. sipbridge
keeps the protocol state; it cannot rebuild your application's call logic.

The current resident Memory service does not offer this capability; it is an
option for Redis checkpoint execution. Controllers that do not offer the capability keep the ordinary
process-bound lease and cleanup behavior. The complete wire rules are in
[Controller recovery and authentication](../controller-recovery.md).

## The recovery window

The first release fixes recoverable-owner timing at a five-second lease plus a
ten-second reattach grace. Normal heartbeats renew a live lease. After the last
renewal, the old lease expires at five seconds and the recovery deadline is ten
seconds later. A replacement must finish restoration before that deadline.

| Point after the last renewal | Meaning |
| --- | --- |
| Before 5 seconds | The existing holder still owns a live lease. Another process cannot take it. |
| From 5 to 15 seconds | A replacement can prove ownership, attach, reconcile and send ready status. |
| At 15 seconds | Unfinished recovery is permanently fenced and normal SIP cleanup starts. |

The server's state-store clock decides these times. Reads, failed attempts,
reconnects and heartbeats during pending restoration do not restart the grace.
Use the deadline in the recovery receipt; do not assume you get a fresh window
when your replacement process starts.

This deadline covers the SIP state retained by sipbridge. Your controller restores
its own application state and coordinates any other resources it uses.

## Save a checkpoint

A successful recoverable hello returns these `applicationLease` fields:

| Field | Keep it for |
| --- | --- |
| `authority` | Selecting and verifying the same retained SIP state authority. |
| `application` | The original logical owner lineage. Keep it unchanged across holder processes. |
| `term` | The exact owner term. |
| `recoveryToken` | Secret proof that this application owns the retained calls. |
| `attachmentGeneration` | Which attachment the proof belongs to. Use the generated uint64 representation (`bigint` in TypeScript). JSON below is a readable projection. |
| `holder` | The actual current controller process, including its current epoch. |
| `leaseLifetimeMs`, `reattachTimeoutMs` | The negotiated timing, currently 5,000 and 10,000. |

Store the handle and your call metadata somewhere a replacement can read. That
metadata may include your call IDs, native `ContextRef` values, media backend and
session/endpoint IDs, routing state and references to credentials held by your
application. Keep the token out of logs and public APIs. Do not store account
credentials in sipbridge state.

A SIP snapshot is not an application checkpoint. Saving only a Call-ID or a
process name does not allow recovery.

## Attach a replacement process

1. Connect to the same state authority using the same authenticated principal.
   mTLS uses the authorized URI identity; bearer mode uses `--control-principal`.
   A different principal is denied even if it has the recovery token.
2. Give the new process its own real `hello.controller` identity and fresh epoch.
   Never impersonate the old process epoch.
3. Offer the recovery capability and send `hello.recovery` with the saved
   application, term, token and attachment generation, plus a fresh `attemptId`.
4. Read `helloAck`, verify the authority, and checkpoint the new receipt before
   sending business commands. A successful replacement changes `holder`, advances
   `attachmentGeneration` and rotates the token.

The recovery part of a hello looks like this, using values from your saved handle:

```json
{
  "correlationId": "recover-1",
  "hello": {
    "controller": { "instanceId": "controller-b", "epoch": "new-process-epoch" },
    "connectionAttempt": "1",
    "major": 1,
    "capabilities": [
      "sip.v1",
      "sip.state.v1",
      "sip.native-completion.v1",
      "sip.controller-reattach.v1"
    ],
    "bounds": { "maxEnvelopeBytes": 1048576 },
    "recovery": {
      "application": { "instanceId": "controller-a", "epoch": "original-process-epoch" },
      "term": "<saved term>",
      "token": "<saved recovery token>",
      "attemptId": "147221f7-9c48-4c9a-bfc5-39d917a4e33b",
      "attachmentGeneration": "1"
    }
  }
}
```

If the reply is lost, retry the exact proof with the same attempt ID and process
identity. The server returns the same attachment receipt instead of claiming the
owner twice. A different attempt cannot reuse a consumed proof, and a failed
attempt does not extend the original deadline. Current holders can reconnect to
other workers using the current proof without advancing the generation again.

## Reconcile before becoming ready

A recovered session starts non-admitting. It can list and inspect its own contexts
while the application restores state. Send `controllerContexts` with a limit of
at most 64; repeat using the returned cursor until it is empty:

```json
{"correlationId":"contexts-1","sessionId":"<new session ID>","controllerContexts":{"limit":64}}
```

The `controllerContextsResult` contains `contexts` and the next `cursor`. Return
the opaque cursor unchanged; cursors contain no whitespace and are limited to
32,768 bytes. This is
an owner-scoped list of references, not a global inventory or a history of missed
events. Native work may change the list while you read it, so deduplicate
references and inspect each one. The index holds at most 4,096 references per owner.

For each context, compare its current snapshot and command outcome with your
checkpoint. Restore your own call and media state, respect terminal results, and
leave uncertain mutations fenced. Do not place the call again or replay a SIP
command because its result was lost.

Only after that reconciliation send fresh `status` with `retainedReady` and
`bindingsReady` both true. Pending restoration must complete before the original
reattach deadline; heartbeats do not extend it. Old-holder frames and unstarted
writes cannot gain authority under the new attachment generation. Already-started
writes can still settle with their original outcome, including `UNKNOWN`.

## What remains running

Established dialogs are the initial recovery scope. SIP transaction timers,
ACK/CANCEL handling, retransmissions and bounded cleanup keep their original
deadlines while the application is away. An application decision cannot wait
indefinitely: pending setup and terminal calls are cleaned up rather than revived.
A peer or its session timer may still end a call during the window.

Memory state supports controller restart only while the same sipbridge process
stays alive. Redis requires the same compatible retained state authority. Neither
mode can recover a lost physical socket or recreate erased SIP state. A restarted
media process also loses its sessions independently.
