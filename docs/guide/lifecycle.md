# Lifecycle and Retention

This page summarizes how state is created, owned, retired and recovered. The
normative rules and exact bounds are in [Runtime](../runtime.md).

## Four identities

| Identity | Scope | Moves? |
| --- | --- | --- |
| **Physical flow** (`FlowRef`) | One socket, or one UDP listener/peer tuple, in one ingress process epoch. Only ingress creates it; SIP headers cannot. | Never. A restart loses it. |
| **Logical SIP context** (`ContextRef`) | State authority, Call-ID group, incarnation and a never-reused context ID. | Any compatible worker attached to the same Redis authority can execute its next transition. Memory contexts belong to one process. |
| **Controller application** (`ApplicationRef`) | The controller process/epoch that owns a call and its pinned owner lease term. | Never migrates. Subscription processor handoff approved by the controller's directory is the one explicit exception. |
| **Business directory epoch** | The controller's routing/binding authority. | A change refuses affected old work; it cannot rebuild old authority. |

Call-ID alone never merges directional dialogs, fork branches or subscriptions.

## Execution model

Every operation loads one bounded Call-ID group, reduces it with pure Go code and
commits the next value and its effects with compare-and-swap. No authoritative
copy is kept between operations or across external I/O. Messages are sent only
from committed effects, by the exact ingress process that owns the target socket.

Results of a socket write:

| Outcome | Meaning | Automatic action |
| --- | --- | --- |
| `WRITTEN` | The socket write call accepted the complete message. It does not prove the peer received or acted on it. | None beyond normal SIP behavior. |
| `NOT_WRITTEN` | Proven that no write started (stale flow, capacity, expired budget). | Typed failure; native recovery may retry the same bytes on the same flow within original bounds. |
| `UNKNOWN` | A write began and failed or was partial, or evidence was lost. Delivery may have happened. | None. Never retried on another flow, never relabeled. |

SIP's own retransmissions are separate native obligations with their original
transaction identity and deadlines, not control retries.

## Owner lease

Each authenticated controller process/epoch holds one opaque SIP-owner term in
the state store. Contexts pin the term that created them.

| Parameter | Value |
| --- | --- |
| Term lifetime | 30 seconds |
| Renewal | On validated, correlated control exchanges (heartbeats), at most every 2 seconds per session |
| Recovery window after first observed expiry | Fixed 5 seconds |
| Typical loss detection after last renewal | About 40 seconds, longer during outages |
| Protocol cleanup after the owner-loss fence | Fixed 64 seconds |

A connected session can resume its retained term after a datastore outage, even
after expiry, until a successor or owner-loss sealing wins. Owner loss fences
business commands permanently, then ends work natively: pending outbound setup is
cancelled, pending inbound requests get `503`, and ACK, CANCEL, late-fork and
uncertain physical obligations still complete. A successor controller process
gets a new term and cannot rebind old INVITE contexts.

## Commands and certainty

Each context serializes commands by sequence number. A result carries one of:

| Certainty | Meaning |
| --- | --- |
| `NOT_ACCEPTED` | Refused before execution. |
| `ACCEPTED` | Execution started; final SIP outcome follows asynchronously (initial INVITE, or a BYE whose first write is proven `WRITTEN`). |
| `COMPLETED` | Final result is known. |
| `UNKNOWN` | Evidence was lost after possible acceptance. Fences dependent work on that context. |

Replaying the latest sequence returns its known or in-progress outcome; an older
sequence is stale. Timeout or disconnect never creates a new attempt. See
[Commands and Outcomes](../protocol/commands.md).

## Retention and retirement

- Terminal native contexts and terminated transactions have a two-minute
  retention floor. Timers, replay evidence, pending effects or physical
  settlement can extend it; the floor alone never proves retirement.
- A context retires only after its native procedures, replay windows, timers and
  physical settlement complete. Terminal application state alone is not enough.
- `UNKNOWN` physical outcomes stay charged against capacity; they are never
  relabeled to free a slot.
- After retirement, a separate bounded quota of 128 retired-context markers
  covers the remaining replay window without consuming live-context capacity.
- Active records never expire just to recover capacity. When capacity is full,
  new work is refused.

## Reconnect and restart

| Event | Result |
| --- | --- |
| Control session drops, both processes alive | Session fenced. Sockets and native contexts remain. Controller reconnects and inspects; pending outcomes may be `UNKNOWN`. |
| Controller process restarts (new epoch) | New owner term. Old calls are not adopted; they end through owner-loss cleanup. |
| sipbridge process restarts, memory state | All contexts, sockets and leases are lost. Clients must re-register and re-establish calls. |
| sipbridge process restarts, Redis state | Its sockets are lost. Retained logical contexts remain; a replacement flow needs fresh native admission. |
| Redis failover | Compatible records resume; missing records fail old operations; fresh work proceeds. |
| Certificate rotation | No restart; new handshakes use the new credentials. |
