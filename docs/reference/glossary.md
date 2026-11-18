---
description: Plain-language definitions of terms used across the guides, the control protocol and the specification documents.
---

# Glossary

Terms you will meet in the guides, the control protocol and the
specifications. The specifications use some of them more formally; these are
the plain-language versions.

**Admission.** The `admission` block on a decision or command: your data
version (`directoryEpoch`), an expiry and, where relevant, the expected
delivery receipt or the connection to use. sipbridge checks it at the moment
it acts.

**Basis.** The reason a decision gives for accepting a request:
`AUTHENTICATED_BINDING`, `EXISTING_FLOW` or `TLS_DIALOG_CONTINUITY`. None of
them means sipbridge authenticated an account; that is the controller's job.

**Binding.** Your record that a particular account and device is registered
and allowed, named by `bindingId`. sipbridge stores the name but never
interprets it.

**Context.** One SIP conversation sipbridge tracks: a call, a subscription or
a standalone transaction. A `ContextRef` identifies it; store it and send it
back unchanged.

**Controller.** Your application: the program that connects to sipbridge over
the control protocol and decides how requests are answered.

**Decision.** The controller's single answer (`requestDecision`) to a
`request`: reject, answer, or accept and answer later.

**Delivery receipt.** A `ContextDelivery`: your confirmation that later
messages for a context should use a particular connection. Required when a
phone continues a dialog or subscription on a new connection.

**Directory epoch.** A version label for your own account and routing data.
Decisions and commands carry it, and sipbridge refuses those made under an
out-of-date label.

**Drain.** What sipbridge does on `SIGTERM`: stop taking new connections, let
admitted work finish for a bounded time, then close what is left.

**Epoch.** A random ID generated each time a process starts. With an instance
ID it identifies one run of a process (a `ProcessRef`), so a restarted process
is never mistaken for its predecessor.

**Flow.** One network connection as sipbridge observed it: a TCP or TLS
socket, or a UDP address pair, in one run of one sipbridge process (a
`FlowRef`). A flow never moves to another process.

**Lease (term).** A controller's ownership of its calls, issued in `helloAck`
and renewed by heartbeats. It ends at once when the controller's session
closes, and the controller's calls end with it.

**Local phone domain.** With Redis state, the part of sipbridge that keeps
incoming REGISTER transactions and subscriptions in memory on the accepting
instance, rather than in Redis.

**Native.** Done by sipbridge's own SIP layer rather than by the controller,
for example answering a CANCEL or retransmitting a response.

**Request.** A `request` message: sipbridge offering one incoming SIP request
to your controller, once, with a `requestId`.

**Retire.** To remove a context once all of its SIP obligations are complete.

**Sequence.** The per-context command number. Each decision or command uses
the latest `commandSequence` plus one.

**Snapshot.** A `ContextSnapshot`: the current picture of a context, returned
in results and events.

**State backend.** Where sipbridge keeps SIP state: process memory by default,
or Redis for calls shared between instances. See
[State Backends](../guide/state-backends.md).

**UNKNOWN.** The outcome reported when sipbridge cannot tell whether a send or
a command took effect. It is never retried automatically and never quietly
changed. See [Handling UNKNOWN](../protocol/commands.md#handling-unknown).
