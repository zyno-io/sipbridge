---
description: What happens to phones, calls, subscriptions and registrations when a controller disconnects, sipbridge restarts or drains, or Redis fails over.
---

# Restarts and Disconnects

This page explains what each part of a running system is tied to, and what
happens to it when something restarts or disconnects. It is background for
controller authors and operators. The exact rules are in
[Service runtime](../runtime.md).

## What is tied to what

| Thing | Tied to | Can it move? |
| --- | --- | --- |
| **Connection** (`FlowRef`) | One TCP or TLS socket, or a UDP address pair, in one run of one sipbridge process. sipbridge creates it from what it observes on the network, never from SIP headers. | No. When the socket closes or the process restarts, it is gone. |
| **Call** (a context) | With memory state, the sipbridge process. With Redis, the shared state. | With Redis, any instance can continue it once a phone reconnects and its request passes the identity checks. |
| **Subscription** (BLF, MWI) | The sipbridge process and connection that accepted it, with either backend. | No. It ends when its TCP or TLS connection closes or it expires. |
| **Controller ownership** (`applicationLease`) | One controller process (instance and epoch) and a lease term renewed by heartbeats. | No. A new controller process, or a new session after a disconnect, gets a new term and does not inherit calls. |
| **Registration** | Your controller's own data. | It is yours to keep, move or expire. |

A Call-ID by itself never ties things together. Two dialogs, forked branches
or subscriptions that share a Call-ID remain separate contexts.

## Owner lease

When a controller completes its handshake, sipbridge issues it a lease: a
`term` that lasts 30 seconds and is renewed by heartbeats (at most every two
seconds). Each call and subscription a controller accepts records that
controller and term.

The lease ends when:

- the controller's session closes or misses six seconds of heartbeats: the
  term ends **immediately**;
- renewal keeps failing, for example during a long Redis outage, until the 30
  seconds run out.

When a term ends, sipbridge ends that controller's calls:

- calls that are set up get a cleanup BYE from sipbridge;
- an incoming call still waiting for an answer is refused;
- an outgoing call still being set up is cancelled;
- SIP work already committed still finishes on its normal timers: ACKs,
  retransmissions, responses already chosen, and a cleanup BYE only after the
  matching ACK is known to have been written;
- sends with an `UNKNOWN` outcome stay unknown; ending the call never retries
  them.

Calls are not handed to another controller, and reconnecting does not bring
them back. With Redis, all instances share one lease per controller process,
so one dropped session ends that controller's calls on every instance.

Subscriptions are different. When their controller's term ends, they keep
running on their phone connection until it closes or they expire. A new
controller can take them over: when the phone next refreshes the
subscription, the refresh is offered to a ready controller as a normal
`request`. A controller that kept the subscription's context reference can
also `inspect` it and send its next NOTIFY directly. NOTIFYs the old
controller had already committed still go out.

## Commands and certainty

Commands for one context are numbered and run in order, and every `result`
says how sure sipbridge is about the outcome: `NOT_ACCEPTED`, `ACCEPTED`,
`COMPLETED` or `UNKNOWN`. A timeout or disconnect never makes sipbridge run
anything again by itself. [Commands and Outcomes](../protocol/commands.md)
explains each value.

Delivery outcomes are separate and apply to every message sipbridge sends:

| Outcome | Meaning | What sipbridge does next |
| --- | --- | --- |
| `WRITTEN` | The operating system accepted the complete message for sending. Not proof the peer received it. | Nothing beyond normal SIP behavior. |
| `NOT_WRITTEN` | Nothing was sent, with certainty. | Reports the failure. |
| `UNKNOWN` | A send started and was cut short, or its record was lost. | Nothing. Never retried on another connection, never quietly reclassified. |

SIP's own retransmissions (for example repeating a response over UDP) are part
of the protocol and happen on their normal schedule. They are not retries of a
controller command.

## Retention

sipbridge keeps a context for a while after its call ends, because SIP expects
it to: late retransmissions and repeated requests still need the right
answer. A context is removed only when all of its SIP obligations are done:
timers finished, messages settled, events delivered. The controller considering
the call over is not enough. A send whose outcome is `UNKNOWN` keeps counting
against capacity until its fixed cleanup time. Live state is never discarded
to make room; when capacity is full, new work is refused.

## What happens when

| Event | Result |
| --- | --- |
| Controller session drops; controller still running | Its calls end as described above. Phone connections, subscriptions and registrations stay. The controller reconnects with a higher `connectionAttempt` and handles new work. |
| Controller process restarts | The same, plus anything the controller held in memory is gone. |
| Controller rolled out with old and new processes overlapping | Each process keeps its own calls until its sessions close. Close an old process's sessions only once its calls have ended. |
| sipbridge receives `SIGTERM` | Bounded drain: readiness goes to `503`, SIP continues during the withdrawal window, and then admitted work finishes within the shutdown deadline. See [Replacing a process](./deployment.md#replacing-a-process). |
| sipbridge restarts, memory state | All its connections, calls, subscriptions and leases are gone. Phones reconnect and register again; calls are set up again. |
| sipbridge restarts, Redis state | Its connections and local subscriptions are gone. Shared calls remain in Redis, and a phone that reconnects (to any instance) can continue its call while its controller's term is still live. |
| Redis failover | State that survived continues; operations on lost state fail; new work proceeds. See [Failure and recovery](./state-backends.md#failure-and-recovery). |
| Certificate renewal | No restart. New handshakes use the new certificates; existing connections stay. |
