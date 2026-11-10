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
| **Controller application** (`ApplicationRef`) | The controller process that set up a call and holds its logic. | No. A call does not move to another controller. The one exception is subscriptions, whose processing a controller can hand to another controller it approves. |
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

sipbridge needs to know when a controller process is gone for good, so that its
calls do not linger. It does this with a **lease**: when a controller connects,
sipbridge gives it a time-limited ownership token (the `applicationLease` in
`helloAck`), and every call the controller sets up is tied to that lease. The
controller keeps the lease alive simply by answering heartbeats.

| Parameter | Value |
| --- | --- |
| Lease lifetime | 30 seconds |
| Renewal | On each answered heartbeat, at most every 2 seconds per session |
| Grace period after the lease is first seen expired | Fixed 5 seconds |
| Typical time to detect a lost controller | About 40 seconds after the last renewal; longer during a state store outage |
| Time allowed for SIP cleanup afterwards | Fixed 64 seconds |

A controller that is still connected can resume its lease after a state store
outage, even if the lease expired in the meantime, as long as sipbridge has not
yet declared it lost.

Once a controller is declared lost, the decision is final. sipbridge stops
accepting commands for that controller's calls and then winds the calls down
itself: outgoing calls still being set up are cancelled, incoming requests
awaiting an answer get `503`, and in-progress ACK and CANCEL exchanges are
completed. A restarted controller gets a new lease and can take new calls,
but it cannot reclaim the old ones.

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
| Controller process restarts | It gets a new lease. Its old calls are not handed back; sipbridge ends them as described under [Owner lease](#owner-lease). |
| sipbridge restarts, memory state | All connections, contexts and leases are lost. Clients must register again and calls must be set up again. |
| sipbridge restarts, Redis state | Its connections are lost. Contexts remain in Redis, and a client that reconnects can continue once its request passes the usual checks. |
| Redis failover | State that survived continues; operations on missing state fail; new work proceeds. |
| Certificate renewal | No restart needed. New handshakes use the new certificates. |
