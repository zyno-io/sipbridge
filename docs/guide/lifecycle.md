# Lifecycle and Retention

This page explains how sipbridge thinks about connections, calls and
controllers over time: what each one is tied to, what happens when something
restarts or disconnects, and how long state is kept. It is background for
controller authors and operators. The exact rules and limits are in
[Runtime](../runtime.md).

## Four identities

Most of sipbridge's behavior follows from keeping four things apart. They have
different lifetimes, and none of them can stand in for another.

| Identity | What it is | Can it move? |
| --- | --- | --- |
| **Flow** (`FlowRef`) | One actual network connection: a TCP or TLS socket, or a UDP listener and peer address pair, in one run of a sipbridge process. sipbridge creates it from what it observes on the network, never from SIP headers. | No. When the socket closes or the process restarts, the flow is gone. |
| **Context** (`ContextRef`) | One SIP conversation that sipbridge tracks: a call leg, a subscription or a standalone transaction. It is identified by its Call-ID group plus an ID that is never reused. | With Redis, any instance sharing the state can continue it. With memory state it belongs to one process. |
| **Controller application** (`ApplicationLease.application`) | The stable application lineage and the process currently holding its lease. | A recoverable owner can attach a new holder within its deadline using its saved proof; the application restores its own call logic. Ordinary process-bound owners cannot transfer calls. |
| **Directory epoch** | The controller's version stamp for its own routing and registration data. | A change makes sipbridge refuse commands issued under the old version. |

The separation between flow and context is what lets a client reconnect: the
old flow is gone, but the context survives and can be attached to a new flow
once the request on it has been checked.

A Call-ID by itself never ties things together. Two dialogs, forked branches or
subscriptions that share a Call-ID remain separate contexts.

## Execution model

sipbridge handles every operation the same way. It loads the state for one
Call-ID from the state store, computes the next state and the messages to send,
and saves both in a single compare-and-swap write, which succeeds only if
nobody else changed that state in the meantime. Only after the save does it
send anything, and the send is done by the process that holds the target
socket.

Two consequences are worth knowing. First, sipbridge keeps no working copy of a
call in process memory between operations, so any instance sharing the store
can perform the next step. Second, every message was recorded before it was
sent, so sipbridge always knows what it intended to send and can report
truthfully on what happened:

| Outcome | Meaning | What sipbridge does next |
| --- | --- | --- |
| `WRITTEN` | The operating system accepted the complete message for sending. This does not prove the peer received or acted on it. | Nothing beyond normal SIP behavior. |
| `NOT_WRITTEN` | Nothing was sent, with certainty: the connection was gone, capacity was exhausted or the deadline had passed. | Reports the failure. sipbridge may retry the same bytes on the same connection within the original deadline. |
| `UNKNOWN` | A send started and then failed or was cut short, or the record of what happened was lost. The peer may or may not have received it. | Nothing. It is not retried on another connection and is never quietly reclassified. |

SIP's own retransmissions (for example repeating an INVITE over UDP) are part
of the protocol and happen on their normal schedule. They are not retries of a
controller command.

## Owner lease

A controller holds a time-limited lease for its calls. Answering heartbeats keeps
it live. Calls also keep the original application lineage, so a new process cannot
claim them just by choosing the same instance name.

| Owner mode | Live lease | What happens after expiry |
| --- | --- | --- |
| Ordinary process-bound owner | 30 seconds | Existing loss detection/recovery checks fence the owner and start cleanup, typically about 40 seconds after its last renewal. |
| Negotiated `sip.controller-reattach.v1` | 5 seconds | The same principal and saved recovery proof may attach a replacement holder for 10 seconds after the old lease expiry. |

For recovery, `applicationLease.application` is the original lineage,
`holder` is the actual current process and `attachmentGeneration` rejects work
from every older attachment. Save the recovery token and handle before restarting.
A new holder must inspect retained calls, restore its own application state and
send ready status before issuing fresh business work. Failed attempts and
heartbeats during restoration do not move the original deadline.

If no holder completes recovery before that deadline, sipbridge seals ownership
and runs its normal SIP cleanup. Pending outgoing setup is cancelled, unanswered
incoming requests receive `503`, and native ACK/CANCEL obligations still finish
within their own timers. Already-started writes may settle as written or UNKNOWN;
reattachment never retries them. [Controller recovery](./controller-recovery.md)
walks through the protocol.

## Commands and certainty

Commands for one context are numbered, and sipbridge processes them in order.
Each `result` says how sure sipbridge is about what happened:

| Certainty | Meaning |
| --- | --- |
| `NOT_ACCEPTED` | Refused. Nothing was done. |
| `ACCEPTED` | Started; the final SIP outcome will follow later. Used for an initial INVITE, and for a BYE once its first send is known to be `WRITTEN`. |
| `COMPLETED` | Finished; the final result is known. |
| `UNKNOWN` | The command may or may not have taken effect. Work on that context that depends on it is blocked; use `inspect` to see where the context stands. |

Replaying the identical command with its most recent sequence number returns
its known or in-progress outcome rather than running it twice. An older
sequence number is rejected as stale. A timeout or disconnect never causes
sipbridge to attempt something again by itself. See
[Commands and Outcomes](../protocol/commands.md).

## Retention and retirement

sipbridge keeps a context for a while after its call ends, because SIP expects
it to: late retransmissions and repeated requests still need the right answer.

- A finished context or transaction is kept for at least two minutes. Running
  timers, unsent messages or unresolved sends can keep it longer.
- A context is removed ("retired") only when all of its SIP obligations are
  done. The controller considering the call over is not enough.
- A send whose outcome is `UNKNOWN` continues to count against capacity. It is
  not written off to free a slot.
- After retirement, a small marker remains for the rest of the replay window so
  that late duplicates are recognized. Up to 128 markers are kept, separately
  from the capacity for live contexts.
- Live state is never expired just to make room. When capacity is full, new
  work is refused.

## Reconnect and restart

| Event | Result |
| --- | --- |
| Control session drops; both processes still running | Client connections and contexts remain. The controller reconnects and inspects; outcomes of commands that were in flight may be `UNKNOWN`. |
| Controller process restarts | With negotiated recovery and a saved checkpoint, the new holder may reattach before the fixed deadline. Without that proof, old calls end through normal cleanup. |
| sipbridge restarts, memory state | All connections, contexts and leases are lost. Clients must register again and calls must be set up again. |
| sipbridge restarts, Redis state | Its connections are lost. Contexts remain in Redis, and a client that reconnects can continue once its request passes the usual checks. |
| Redis failover | State that survived continues; operations on missing state fail; new work proceeds. |
| Certificate renewal | No restart needed. New handshakes use the new certificates. |
