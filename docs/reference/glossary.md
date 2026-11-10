# Glossary

Terms you will meet in these guides, in the wire protocol and in the
specification documents. The specification uses some of them more formally
than the guides do; the definitions here are the plain-language versions.

**Admission basis.** The reason a controller gives, in an `ingest` message, for
letting a SIP message join an existing context: `EXISTING_FLOW`,
`AUTHENTICATED_BINDING`, `TLS_DIALOG_CONTINUITY` or
`TLS_TRANSACTION_CONTINUITY`. None of them means an account was authenticated.

**Application controller.** See *Controller*.

**Binding.** The controller's record that a particular account and device is
registered and authorized, with a revision number that only increases. It is
more than a Contact URI or a source IP address.

**Call-ID group.** All the state sipbridge holds for one SIP Call-ID. It is
loaded and saved as a unit, and memory capacity is counted in groups. A group
can contain several contexts.

**Committed effect.** Something sipbridge has decided to do, usually a SIP
message to send, recorded in the state store before it is carried out. Almost
everything sipbridge sends starts as one. The exceptions are the local `405`,
the local digest challenge and the best-effort `503` sent while the state store
is down.

**Compare-and-swap (CAS).** A way of saving that succeeds only if the data has
not changed since it was read. sipbridge saves all state this way, which is
what lets several instances share a store safely.

**Completion (native).** SIP work sipbridge can finish by itself, with no
controller involved: answering a CANCEL, handling an ACK, repeating a final
response, retransmissions and timer-driven messages.

**Context.** One SIP conversation that sipbridge tracks: a call leg, a
subscription or a standalone transaction. With Redis, any instance sharing the
state can continue it; with memory state it belongs to one process.

**Controller.** Your application: the program that connects to sipbridge over
the control protocol and decides how requests are routed, authenticated and
answered.

**Directory epoch.** A version stamp for the controller's own routing and
registration data. Commands carry it, and sipbridge refuses commands issued
under an out-of-date one.

**Epoch.** A random ID generated each time a process starts. Together with an
instance ID it identifies one particular run of a process (a `ProcessRef`), so a
restarted process is never mistaken for its predecessor.

**Fence.** To stop accepting work from, or for, something that can no longer
be trusted to be current, such as a superseded session or a lost controller.

**Fixture.** A test program or test data set in the repository. Fixtures are
not ways to run the service.

**Flow.** One actual network connection as observed by sipbridge: a TCP or TLS
socket, or a UDP listener and peer address pair, within one run of a sipbridge
process. A flow never moves to another process.

**Incarnation.** A generation number for a Call-ID group. If state is lost and
the same Call-ID is seen again, it gets a new incarnation, so that leftover
references to the old state cannot act on the new.

**Ingress.** The part of sipbridge that owns the public sockets: it receives
and parses messages, notes where they came from and performs the actual sends.

**Native.** Done by sipbridge's own SIP layer rather than by the controller.

**Owner lease (term).** A time-limited token that says a controller process is
alive and owns its calls. It is issued in `helloAck` and renewed by heartbeats.
Without recovery, permanent lease loss starts cleanup. A recoverable owner may
reattach with its saved proof before the fixed deadline; after that, cleanup runs. See
[Owner lease](../guide/lifecycle.md#owner-lease).

**Profile (3/9/1).** The version of sipbridge's state format: schema 3, reducer
9, timers 1. Instances sharing Redis must all use the same one. The current
service has only this one.

**Reducer.** A function that takes the current SIP state and an input (a
message, a command or a timer firing) and returns the next state and the
messages to send. sipbridge's SIP behavior is implemented this way in the
pinned sipgo library.

**Retire.** To remove a context once all of its SIP obligations are complete.

**State store.** Where sipbridge keeps its SIP state: process memory by
default, or Redis. See [State Backends](../guide/state-backends.md).

**UNKNOWN.** The outcome reported when sipbridge cannot tell whether a send or a
command took effect. It is never retried automatically and never quietly
changed to something else. See
[Handling UNKNOWN](../protocol/commands.md#handling-unknown).
