# Glossary

**Application controller.** The external service that owns business decisions
and drives sipbridge over the control protocol. Called Nexus in the normative
documents and in wire field names such as `hello.nexus`.

**Admission basis.** The reason an `ingest` may enter an existing context:
`EXISTING_FLOW`, `AUTHENTICATED_BINDING`, `TLS_DIALOG_CONTINUITY` or
`TLS_TRANSACTION_CONTINUITY`. None of them is account authentication.

**Binding.** An authorized account/device/registration scope with a monotonic
revision, owned by the controller. Not an address-of-record, Contact URI or
source IP.

**Call-ID group.** The unit of state loading and compare-and-swap. A group can
hold several logical contexts; Call-ID alone never merges them.

**Committed effect.** An output recorded in the state store before any I/O.
Every SIP message sipbridge sends comes from one, except the documented local
`405`, local challenge and best-effort datastore-unavailable `503` replies.

**Completion (native).** Retained SIP work sipbridge can finish without a
controller: CANCEL, ACK, cached finals, retransmissions, timer output.

**Context (logical SIP context).** State authority, Call-ID group, incarnation and
never-reused context ID. Portable between compatible workers sharing Redis;
process-local in memory mode.

**Directory epoch.** The controller's business-directory version. A change
refuses old business work.

**Drachtio.** The SIP server used by the original platform's supported
production deployment. sipbridge does not depend on it.

**Epoch.** An unpredictable per-start process identifier. Paired with an
instance ID in `ProcessRef`.

**Flow (physical flow).** One socket, or one UDP listener/peer tuple, in one
ingress process epoch. Created only by ingress. Never migrates.

**Incarnation.** A generation of a Call-ID group. Fresh work after state loss
gets a new incarnation, so old references cannot execute.

**Ingress.** The part of a sipbridge process that owns public sockets, framing,
observed source evidence and physical writes.

**Owner lease (term).** The controller process's SIP-owner authority in the state
store, renewed by heartbeats. Contexts pin the term that created them.

**Pilot.** The original platform's account-mutation backend, reached only through
the controller.

**Profile (2/9/1).** State schema 2, reducer 9, timers 1: the single execution
profile of the current service. Historical 1/7 values exist only in test fixtures.

**Reducer.** A pure function from current protocol state and input to next state
and effects. SIP semantics live in the pinned sipgo reducers.

**Talk.** The original platform repository that owns the canonical control schema
and generated bindings.

**UNKNOWN.** A physical write or operation outcome that cannot be proven. Never
retried automatically or relabeled.
