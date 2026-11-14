# Service runtime contract

This is the normative checkpoint execution and bounds supplement to
[ARCHITECTURE.md](../ARCHITECTURE.md), covering the schema 1 / reducer 1 /
timers 1 Redis service and explicit checkpoint fixtures. The resident Memory
service follows [Resident Memory core](memory-core.md); read/reduce/CAS and
shared scheduling descriptions below apply to checkpoint execution.
Historical claimed/relay execution is restricted to [test fixtures](fixtures.md).
[PROTOCOL.md](../PROTOCOL.md) owns wire and business-admission semantics.

## Configuration, identity and loss

`SIPBRIDGE_STATE_BACKEND` defaults to `memory`; `redis` must be explicit. Unknown
values fail startup. Selection is fixed for the process lifetime. Redis settings
without explicit Redis selection are rejected as inconsistent configuration.
Both services expose the same logical control state capability. Their
execution and storage mechanisms differ; Memory never opens `state.MemoryStore`
or enters checkpoint read/reduce/CAS execution.

Memory's process-local authority, native retention, deadline heap and failure
behavior are defined in [Resident Memory core](memory-core.md).
`SIPBRIDGE_MEMORY_MAX_GROUPS` limits resident contexts (default 128), and
`SIPBRIDGE_MEMORY_MAX_BYTES` limits accounted resident bytes plus admitted
completion headroom (default 1 GiB). Admission also reserves finite output slots.
The former encoded per-group reservation does not apply. These bounds are not an
RSS promise; runtime metadata and transient transport/codec values add overhead.
The encoded `state.MemoryStore` remains an explicit checkpoint test fixture.
Resident command-result error detail is limited to 4 KiB of UTF-8; longer detail
is truncated at a character boundary with an explicit suffix.

The `sip.state.v1` capability negotiates these logical state semantics for both
adapters. Controllers must negotiate this capability before state admission.
Backend selection does not change the control capability or the native profile.
No wire fields or schema/reducer/timer versions change.

The Redis store opens without waiting for Redis. Configuration supplies primary or
Sentinel endpoints, credentials, namespace, TLS trust and the service identity key.
Plaintext requires `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true`. Sentinel discovery is
metadata; actual data connections must be writable primaries. An empty namespace
is ordinary available empty state. Recovery needs no provisioning, partition marker
inventory or generation change. The configured generation is compatibility identity,
not an operator recovery step or evidence of surviving state.

Workers sharing a namespace must share the service identity key, partition mapping
and schema/reducer/timer profile, private storage encoding and descriptor identity.
The storage authority HMAC includes the encoding plus both the private storage
schema SHA-256 and canonical control schema SHA-256 from the verified build receipt.
The `protobuf-v1` label alone never establishes compatibility. A descriptor change
fences the preceding binary authority even when logical versions remain 1/1/1;
source-only changes do not change this descriptor identity. Deployment owns this consistency; readiness cannot
detect workers mapping one Call-ID to different partitions. Wire authority identifies
compatible configuration, not proof that data survived an outage. Every existing
record independently validates namespace, schema, incarnation and revision.
Transport identity changes invalidate in-flight computations. Retained compatible
records can resume after reconnect; fresh allocation alone can create missing state.
Old references, commands, timers and completion results cannot recreate it.
Asynchronous replication permits loss and undetectable structurally valid rollback.

Logical identity is authority, group, incarnation and context ID, independent of
executor metadata. Inbound Locate reserves a monotonically increasing, non-reused
ID in the group incarnation. A reservation binds exact initial bytes, expires after
two seconds without extension and counts against 32-context admission. An identical
lookup reuses only its live reservation; consumption pins the authenticated controller
term. Unmatched responses and in-dialog requests reserve nothing. Private refusal
uses separate lookup and releases any matching unconsumed reservation.

Outbound bootstrap groups have a separate namespace and one consumed ID per
incarnation. The stable HMAC derives the group from authenticated controller epoch and
placeholder ID on every replica. Lost first-reference replies can be inspected
without allocating or restarting OpenUac. Before first allocation, only whole-group
absence proves bootstrap retirement. Inspection distinguishes retained pending
UNKNOWN, positively proven absence and unavailable/changed authority.

## Controller route outcomes

A controller RouteResult of UNAVAILABLE is a valid unavailable route and yields
503 for a reply-eligible SIP request, with diagnostic outcome `route-unavailable`.
It never admits application work or creates native state. Unknown or unspecified
route outcomes remain malformed replies, diagnosed as `invalid-route-outcome`.
ACK and SIP response refusal exclusions and the original route deadline remain
unchanged. A controller cannot choose an arbitrary SIP status for UNAVAILABLE.

## Retained binary encoding

The binary profile stores each large retained payload as `protobuf-v1`, with an
explicit record encoding header and generated storage-only protobuf messages.
The immutable compatibility profile includes this encoding separately from the
schema/reducer/timer contract; changing the encoding changes native authority.
Old JSON records are not decoded, migrated or rewritten by a binary worker.
A format cutover drains and stops old workers before a fresh namespace is used.
The public controller schema and binary control framing are independently
versioned surfaces; changing control framing alone does not mandate a namespace reset.

The payload contains raw protocol-state and effect bytes; the engine owns their
typed binary codecs. No JSON/base64/hex wrapper belongs around those large values.
Encoding sizes each immutable generated graph once and reuses that size only
within the same marshal call; it does not retain a graph or size cache between
operations. Small Lua-inspected input/receipt/lease envelopes retain their bounded existing
representation. Record data, revision, scheduling and lease projections still
commit atomically. The encoded MemoryStore fixture uses the same codec and independent snapshot
ownership; the resident Memory service uses typed records directly.
Before generated decoding allocates, records are capped at 64 MiB, recursion at
32 levels, each repeated/map field at 4,096 entries and the total field count at
262,144. The `SIPB`, encoding-byte 1 and record-kind header are mandatory.
Encoded bytes, recursion and collection counts are bounded before allocation;
unknown fields, duplicate singular/map fields and unsupported encodings fail
closed before a reducer observes a partially interpreted value. Exact SIP wire,
integer values, explicit presence, replay identity, UNKNOWN outcomes and reserved
native completion capacity retain their existing semantics.

Effect metadata reads validate retained SIP identity with the same parser and
mandatory-header/branch checks as full observation, without constructing public
message headers, addresses or digest fields. Full message projections are
materialized only for native wire-fingerprint admission and outgoing controller
RPCs. Neither representation is cached across operations. Non-SIP effect wire
(such as a network-open target) keeps its existing typed decoding rules.
Within one classification loop, lifecycle and dispatch selection reuse the same
validated effect body. Native reservation likewise passes its owned selected body
and drain observation to the common authority checks rather than decoding it a
second time. These local values never survive the operation or bypass validation.
Already-settled physical evidence is not serialized again merely to set the same
settlement bit; lifecycle continuation still receives both preparation passes.

Effect encoding constructs only the envelope metadata that is retained. Its
request/egress/event projection omits the parsed SIP message before any copy;
it does not clone the parsed headers, addresses or body merely to discard them.
Exact wire and non-message metadata remain unchanged. This operation-local
projection shares immutable metadata only until synchronous encoding completes;
it neither mutates the caller's message nor introduces a retained cache.

Codec telemetry uses only fixed record-kind and encode/decode labels, with
operation/failure counts, cumulative bytes and elapsed seconds on `/metrics`.
It never scans Redis or exports application identifiers or retained bytes.
The offline `sipbridge-state-inspect` command reads a bounded raw `data` value,
checks the same binary and semantic bounds and emits counts, allowlisted native
phase/timer classes, fixed retirement/disposition cutoffs and retention only.
It does not contact Redis, restore state or print SIP wire,
addresses, credentials, Call-IDs, group/context/effect IDs or recovery tokens.
Authority/profile/revision are Redis envelope fields, not inferable from raw data;
operators must inspect them separately through their authorized Redis tooling.
The explicit [fixture completion projection](fixtures.md#offline-completion-projection)
adds allowlisted identifying protocol proofs for disposable qualification only;
it does not change default inspection redaction or provide a state import path.

A generated direct group reader consumes the existing `EngineStateGroup` protobuf
frame after the same complete strict preflight. It constructs independently owned
runtime values without first allocating a storage protobuf aggregate, and delegates
embedded public protobuf messages to their standard decoder. Semantic context and
transaction-policy checks still precede publication. The preceding generated
protobuf decoder and adapter remain the test parity oracle.
The reader preserves field numbers, presence wrappers, nullable map entries,
exact byte ownership and rejection bounds; it introduces no alternate format,
retained cache, fallback, migration or historical-data repair.

Generation-only reflection describes the retained Go graph. Checked-in storage
field numbers are stable; removed fields stay reserved. Runtime conversion is
typed generated Go, with explicit map/list presence wrappers and nullable map
values. Storage descriptors/adapters have a separate build receipt; changing
private storage never regenerates the public control schema. Completion proof
capacity includes conservative binary length-prefix growth through enclosing
messages and keeps the existing 1,600-byte per-transaction proof bound.

### Operation values and replay

CAS admission validates and serializes its immutable input envelope once. The
adapter retains only that operation's canonical encoded input, digest and compact
field/payload binding until CAS completes. Commit verifies that the supplied input
still matches the binding, then reuses the same canonical envelope and digest.
It does not repeat whole-envelope JSON/base64 encoding for validation and commit.
The operation fingerprint remains unchanged; mutating
any admitted identity, deadline, lease ID or payload still refuses the CAS.

The context's cached command result retains its outcome and exact request/response
wire, but no duplicate context snapshot or parsed SIP message graph. Public command
replay, inspection and waiters project a fresh snapshot from the canonical context
and reconstruct request/response fields from that wire at their boundary. Invalid
retained wire refuses the projection; it cannot supply send authority. Group decode
does not parse these messages. Persisted operation receipts and lifecycle event
snapshots retain their original independent meaning; this compaction does not
rewrite historical outcome evidence.

Terminal effect metadata is still decoded and validated before retention. Once
its wire and controller envelope have been compacted away, later reductions
reuse those encoded effect bytes rather than serialize the same metadata again.
This changes neither proof retention nor settlement or dispatch eligibility.
An effect whose controller message is already wire-only also needs no cloned
envelope merely to encode the same retained metadata.

Until their original result cutoff, operation receipts retain their original frozen snapshot and exact outcome, but
store request/response wire without duplicate parsed SIP fields. Result delivery
reconstructs those fields from the retained wire; it never substitutes a newer
context snapshot for a historical receipt. Physical reservation results remain
wire-only internal values, including their empty-response status sentinel.
Native input envelopes set the result cutoff to the original immutable input
admission deadline, including any earlier policy expiry. This governs generic
receipt replay, not an already-running command's wait for native completion or
observation of its current command result. At or after that cutoff, receipt
read/replay/inspection returns an explicit
discarded-outcome marker, independently of whether the committed transition
accepted or refused the operation. The caller receives UNKNOWN without a snapshot;
it cannot interpret that marker as absent, not admitted, not written, or retry
permission. Identity, fingerprint, input digest, revision and receipt expiry remain
until the existing marker-retention cutoff. A new retry budget cannot restore an
old result or renew its original cutoff. No current snapshot is substituted for
discarded historical evidence.

Successful commits lazily discard expired outcome payloads before capacity
accounting; read suppression performs no Redis mutation. There is no new cleanup
timer, key, mutex or background scan. Quiet groups can retain encoded expired
outcomes until their next successful commit or ordinary retirement, without
returning them as live results. Generic store callers may omit the optional result
cutoff to retain their outcome until receipt expiry; native callers set it for
every input. A deliberately empty outcome and a discarded outcome are distinct.
Compatible workers must all support discarded-outcome markers before writing them.
This replaces historical RPC-result reconciliation after its request deadline;
native transaction/disposition windows and physical UNKNOWN proofs do not shrink.
Definite fresh CAS success may return one transient typed result DTO, bounded to
1 MiB, to avoid decoding its own receipt and reparsing already projected messages.
The execution driver copies that DTO before returning it. Neither backend stores
it, and the caller consumes it within the operation. Replay, read-only results,
uncertain commits and larger outcomes use the retained receipt path. This is not
a result cache or a second protocol state value.

Context retirement blocked by a retained transaction's fixed replay or request
disposition cutoff schedules that actual cutoff, rather than a one-second retry.
Native transaction/dialog timers remain separate obligations. Other unresolved
retirement blockers retain their existing bounded retry behavior.

When a subscription ends at its terminal grace deadline or a terminal/481 NOTIFY
response, that same transition terminates any unanswered SUBSCRIBE server
transaction still in Trying with no selected response, active native timer,
pending/deferred procedure or unresolved effect. It retains the transaction and
request-disposition proof through the existing context retirement and original
request-disposition deadlines. This prevents an unanswered refresh from keeping
an ended subscription active indefinitely. Client NOTIFY transactions, selected
responses and uncertain physical writes retain their independent obligations.
There is no retrospective scan or repair of previously ended records.

All workers sharing an authority must support compact cached-result projection
and owner-expiry scheduling. Unchanged descriptors do not make preceding binaries
compatible with these omitted derived fields. Upgrade the worker cohort together;
an older binary is not a compatible rollback once compact records are written.
No schema migration or historical-format fallback is introduced.

### Derived identity within an operation

Hints grant no authority; each selected group reloads current state and timer generation.
Selector rebuilding uses the same parsed-wire identity and transaction-key rules
as full message observation without constructing controller header, address or
digest projections. One reduction may memoize a validated scalar identity and
direction-specific derived selectors under one owned exact-wire key, bounded to
256 entries and 1 MiB of retained key bytes. Completion budgeting reuses the
CSeq method/number, To tag and Via branch/sent-by projection instead of parsing
each retained transaction again. Selector validation and portable-message
validation remain distinct: the latter's transport and identity requirements
are checked before reuse, with the pinned parser as the fallback for ineligible
records. Neither terminated transactions nor their completion obligations are
excluded from budgeting. The memo contains no parsed mutable SIP object or
authorization, is discarded at
the operation boundary, and is cleared before a failed transition restores its
original aggregate. It never bypasses a fresh store read, native validation or
CAS fencing. Exhausting the memo budget falls back to ordinary reconstruction.

## CAS execution and admission

The driver performs one validated read, pure Go reduction and atomic CAS. Reads
include store time (Redis TIME or local memory-store time), revision, adapter CAS
observations, incarnation and lease observations.
Missing groups supply only a proposed incarnation until create CAS succeeds.
Commit validates original input deadline, current adapter authority (including the
writable primary in Redis mode), namespace/profile,
incarnation, revision/counter, capacity and current owner-term guards. Unexpected
legacy Pending fails closed without overwrite or Resume. Retained comparison-counter
and pending-field readers do not enable the historical claimed driver.

A native CAS reservation may update only physical-effect metadata when its exact
input is a proof, no pending input or retained timer is due, and every context has
a fresh, live, unsealed observation of its unchanged owner term and attachment
generation. Complete binary/schema and group semantic decoding still runs. The
existing selected-effect wire, authorization, permit, context, attempt, flow,
policy, owner, ACK dependency and deadline checks remain authoritative; reservation
changes only AUTHORIZED to DISPATCHING and removes that effect's duplicate wire
and envelope. Protocol bytes, selector indexes, timer deadlines, application
indexes and the conservative completion reservation remain unchanged. CAS still
validates the whole payload, capacity and selected owner guards. Transient result,
drain and dispatch projections belong only to this operation. Refusals, known
outcomes, uncertain evidence and ineligible records use normal reduction.

This reservation path defers opportunistic pruning, selector reconstruction,
completion-budget recomputation and unrelated effect compaction to normal protocol
or timer inputs; it does not extend their retained deadlines. Unrelated retained
SIP wire is parsed when consumed by protocol, lookup, timer or budget work, rather
than reparsed merely to reserve another transaction's already-authorized write.
Malformed selected write evidence still refuses reservation, and UNKNOWN never
authorizes a repeat physical attempt. There is no resident protocol cache or new
storage format.

CAS contention retries retain the original input/request deadline and five-ms pause;
there is no renewed budget or arbitrary low retry ceiling. A fixed 1,024 local
stripe set serializes the same partition/group before the global execution slot.
It includes incarnation-independent group identity; collisions reduce parallelism
without merging state. Stripes retain no snapshot/input, span only read/reduce/CAS
and release before preflight, dispatch or physical I/O. They promise no fairness
and supply no cross-process authority.

Stripe/slot admission uses the earlier of caller cancellation and immutable input
deadline. Caller cancellation performs no fallback. If only the input deadline
expires, an available global slot may perform one caller-bounded, at-most-two-second
ReadUpdate without stripe/reduce/CAS, returning only an exact retained receipt.
The admitted first read retains its caller store-I/O bound so it can return a
receipt after semantic expiry; no receipt after expiry means no reduction or CAS.
Exact live replay is checked before fresh admission expiry.

Read-only scheduling, physical-dispatch discovery, dispatch fallback and existing-
selector lookup, protocol-only control/controller preflight, native-completion
selection, ACK dependency checks, exact reference bootstrap, historical physical-
outcome inspection and completion observation for an already-admitted command use
a separate non-authorizing state view. The view includes the
exact group reference/incarnation, revision, versions, bounded validated binary
payload, same-read store time and pending-input presence. It does not read or
decode receipt/proof bodies, pending-input contents or application-lease
observations, and cannot supply a replay result, CAS admission or delivery permit.
It does not initialize owner-recovery deadlines as a side effect of observation.
Validation is scoped to those selected fields: malformed unselected history does
not poison the view, while full Load, ReadUpdate, replay and retirement retain
their complete validation and evidence requirements. A pending input still
blocks scheduler and existing-selector admission. Existing idempotent recovery
RPCs may use their retained effect identity from the view. Completion observation
still reconstructs the canonical private group and matches the exact context and
operation, including public-BYE write evidence; it does not replay admission or
gate on pending-input presence. Store-read failure and private-group corruption
retain their distinct result/error behavior. Historical write inspection retains
every effect/authorization/permit/flow/attempt comparison and reports absence or
unproven status as UNKNOWN; it supplies no new send permission. Physical sends still
require native reservation/permit proof. Native group decoding preserves versions
so server-transaction disposition checks cannot be bypassed. Returned values are
independently owned and discarded before socket/RPC I/O; there is no resident
snapshot or receipt cache.
Redis credentials must grant `HSTRLEN` on the already-scoped group hashes for the
pending-presence check; activate that command grant before replacing a worker with
this reader. It requires no additional key namespace or write permission.

Internal negative checks can refuse before native-payload decoding, only with no
legacy Pending and matching outer/decoded kinds: effect-start absent/already started/
physical; effect-result whose exact effect was collected; timer whose exact ID and
generation is absent or not due against that read's Redis time. These are read-only
refusals, never fabricated commit projections or evidence of delivery. Existing
results and exact due timers use normal reduction, including overdue timers.
Scheduler reload still discovers independently committed effects.

A reducer error restores original protocol value/effects before recording refusal.
A rejected new command deliberately consumes its sequence; rejected dialog ingest
cannot consume remote CSeq. No pure CAS input is persisted. Death before commit loses
only computation. External operations retain bounded commit markers and current-command
fingerprint/result; legacy equal-sequence replay without proof is refused. External
cleanup has a reserved bounded proof slot. Internal effects/timers use exact retained
identity, generation and status rather than generic receipt history. Uncertain
external commits are inspected by the same operation; unresolved/internal uncertainty
cannot invent another effect, physical attempt, CSeq or deadline.

Fresh proven CAS returns a transient bounded projection of its group reference and
committed effects. It is neither serialized nor retained as state. An empty fresh
projection is meaningful. Replay, refusal and uncertain-commit inspection use the
fresh-read fallback. Physical Complete dispatches its commit projection once before
releasing its local guard; lost replies leave durable scheduler recovery intact.

A fresh proven inbound transition projects one transient presentation DTO to its
executing worker, which calls the selected controller within the original two-second
control deadline before returning from ingress. A deferred owner proof or flow
adoption can produce that DTO on its completing worker, without extending the
original deadline. Presentation is never stored, indexed, replayed, recovered or
retried after an uncertain commit; process loss may lose the in-flight request.
No successful presentation result transition is recorded. A failed or expired
one-shot call attempts a bounded request-scoped native 503 transition; it can
select a final only if the same admitted server transaction is still unanswered.
A concurrent selected final wins through the same CAS guard. An older retained
deferred transaction without the original presentation deadline is refused; it
never receives a fresh budget. Inbound control handlers have their own bounded
48-slot admission so callback waits do not occupy the 64 execution slots needed
by controller commands and native completion. Lifecycle and pure
credential effects can dispatch directly from Pending without effect-start/executor
claim. Fixed receiver/envelope/deadline and controller receiver deduplication
remain mandatory for those effects. Result commit checks the exact
context/application/lease term and live native-owner guard; historical executor-bearing
results retain their recorded executor check. Network opens, send authorization and
physical writes keep their separate start/reservation boundaries. A collected effect
never authorizes another callback or SIP intent.

## Owner lease and permanent loss

The selected SIP-state adapter owns one opaque term per authenticated controller
process/epoch. Lifetime is 30 seconds; validated live control exchanges renew at most every two seconds per
session. The hello returns the current lease receipt. Live-term renewal never
shortens expiry; the receipt allows at most 500 ms additional remaining time for
bounded Redis clock tolerance. Session fencing and a correlated challenge/reply
precede renewal; the peer cannot name another process. Closing/draining one control
socket does not revoke the term. The trusted directory's ten-second process lease
is routing/business information, not SIP-owner-loss authority.

Ordinary renewal needs the exact live term. An already connected authenticated
session can resume that retained term after an outage, even after expiry, until
successor or owner-loss sealing wins. The first observation of an expired retained
unsealed term starts a shared fixed five-second recovery window. Other workers cannot
extend it or seal before it elapses. Successful renewal clears that episode; a later
independent expiration gets its own window. Terminal contexts can offer renewal to
their shared owner but never revive themselves. Cleanup and resume race atomically
through the seal. Only a fresh authenticated handshake acquires a successor term.

Native owner checks are deadline-driven. A live context's next check is the exact
expiration of its currently observed pinned lease, rather than a recurring
five-second context timer. If that lease has renewed when the check runs, the
next check follows its new expiration. An expired term inside its fixed recovery
window schedules that window's existing deadline; it never starts or extends a
window itself. Every transition still validates current native lease observations
and the original term. Earlier SIP transaction, physical and cleanup deadlines
remain independent. This removes otherwise empty read/reduce/CAS cycles for
healthy owners without retaining a second authoritative protocol state.

Referenced terms have bounded reference counts and no TTL. Releasing the final
reference restores the 120-second idle TTL. Bounded pruning rescores pinned expired
records to avoid starving reclaimable ones. Read/write operations validate current
primary/role, per-record authority and Redis clock. Store errors are unavailable,
never owner-death proof. CAS does not renew expired leases inside reduction;
connected-owner recovery precedes a fresh reduction and refreshes observations.

Context admission, business command commits and unstarted APPLICATION reservations
require the original live pinned term. Reservation checks term/expiry atomically;
a prior permit cannot outlive it. Commit racing expiry retries against current
observations. Without `sip.controller-reattach.v1`, a successor admits new contexts but cannot
rebind old INVITE contexts. Reconnect within a live term preserves application
calls; a non-recoverable successor must terminate or reconcile old-term local
calls rather than wait for callbacks to a fenced owner. Directory-approved
subscription processor adoption retains its separate rules.

Owner loss commits a permanent business fence before native termination. It abandons
dead-owner nonphysical policy/events/adoption, invalidates unstarted ordinary work,
cancels pending UAC setup and selects 503 for pending UAS requests. Existing ACK,
CANCEL, late-fork and physical DISPATCHING/UNKNOWN obligations remain. A UAS success
waits for matching ACK or native timeout. With healthy scheduling, the 30-second
lease and fixed five-second recovery window normally detect loss about 35 seconds
after renewal, plus bounded scheduling delay; outages can delay this. The fixed 64-second protocol
cleanup bound starts at the fence, without extending any earlier native deadline.
Unsendable cleanup can end locally; physical uncertainty remains retained/charged.
If an already-retained pending server request has an unsupported Via branch and
its owner is permanently lost, the pinned reducer terminates that unsendable
transaction locally instead of attempting a mismatched 503. The owner fence,
request-disposition proof and any earlier physical uncertainty are preserved;
no response or physical retry is inferred. Fresh unsupported branches are refused
at parsing, as specified in [Protocol](../PROTOCOL.md).

### Opt-in controller reattachment

`sip.controller-reattach.v1` creates a recoverable logical owner. Its original
application lineage remains stable, while holder and attachment generation name
the currently authenticated process. The initial release fixes its lease at five
seconds and its reattach grace at ten seconds. The orphan deadline is anchored to
the previous lease expiry, never first observation or a retry. Attachment proof
requires the same principal, owner term, generation and recovery token. One atomic
adapter operation changes the holder/generation and rotates the token. An exact
successful attempt can return its retained receipt after a lost reply.

The initial recovery scope is established INVITE dialogs. Fresh ready status is
required after reconciliation; heartbeat renewal cannot extend a pending
restoration deadline. Native SIP obligations keep their original timers, and
UNKNOWN physical evidence stays unknown and charged. Deadline expiry seals the
owner before normal cleanup. A bounded derivative owner index supplies at most 64
context references per page, at most 4,096 per owner, with no Redis key scan or
second resident FSM. See [Controller recovery](controller-recovery.md) for the
full attachment contract and [the recovery guide](guide/controller-recovery.md)
for controller steps.

## Ingress, selectors and completion

Stream framing reuses its parsed SIP message for the operation-local public
projection after the same mandatory identity/branch validation as ordinary
observation. It does not reparse the same frame or trust parsed objects supplied
over control. Exact wire bytes and message bodies remain independently owned.
Framing allocates its original-byte buffer on demand; after a frame larger than
8 KiB drains the stream, it releases that buffer and replaces the parser stream
so rare large messages do not pin their high-water capacity to a phone connection.

### Process-local method policy

`--allow-register` and `--allow-subscribe` default to true. Setting either to
false permanently disables that inbound request method for the process lifetime,
including expiry-zero and tagged requests. After parsing and before completion
probing, Redis access or controller dispatch, ingress returns a local 405 with an
Allow header excluding disabled methods. This static denial is an explicit
exception to Redis-committed output: the process never admits those requests,
so it cannot race a previously selected business final in that process.
Changing the policy requires deliberate process replacement and normal drain.

Refusals use the exact observed flow, bounded writes, actual received/rport
evidence, a deterministic To tag and no extra retention or request history.
Retransmissions produce the same response. Responses whose CSeq names a disabled
method are not blocked. No other method changes admission; in particular,
INVITE, ACK, CANCEL, BYE and in-dialog REFER/NOTIFY keep their normal
handling. The local 405 Allow header describes this process's enabled native
methods; it grants no business or account authority.

### Local OPTIONS

Physical ingress answers validated OPTIONS on the observed return flow before
native completion, context lookup, datastore access or controller dispatch.
Tagged and untagged requests use the same capability-response path; they do not
look up or change dialogs, CSeq watermarks, registrations, subscriptions or
application authority. No native transaction, output proof or reply history is
retained. This follows the OPTIONS-specific behavior in
[RFC 3261 sections 11.2 and 12.2.2](https://www.rfc-editor.org/rfc/rfc3261.html#section-11.2).

The ordinary response is 200 with this process's enabled methods in Allow and
no invented SDP or media capabilities. A draining process or absence of an
eligible ready control peer produces a local 503 using only cached control
status; OPTIONS never asks a controller or probes the datastore. This reports
local signaling availability, not account admission or guaranteed call success.
Unsupported Require option tags produce 420 with Unsupported; unsupported
required request content produces the normal 415 rejection. Optional unsupported
content may be ignored.

Responses preserve an existing To tag or derive a deterministic one, and apply
observed received/rport evidence. Identical UDP requests regenerate the same
reply under unchanged operational status, without a retransmission cache.
Availability changes do not create retained history merely to replay an old
status. Existing bounded writes and shutdown accounting apply; OPTIONS traffic
cannot extend the fixed drain bound or enter the stateful refusal path.

### Configured initial authentication

`--require-auth-methods` accepts a comma-separated subset of REGISTER, INVITE,
SUBSCRIBE; empty preserves existing behavior. Unknown/duplicate methods and
conflicts with disabled registration methods fail startup. REGISTER requests
and untagged initial INVITE/SUBSCRIBE without parseable digest credentials are
challenged after disabled-method rejection and before native completion, lookup
or control dispatch. No account lookup is performed.

The negotiated [challenge protocol](../PROTOCOL.md#local-initial-digest-challenges)
supplies bounded expiring issuance policy. Local 401 and temporary failure when
policy is unavailable are explicit local-output exceptions: they admit no
business state. Tagged requests, responses and credential-bearing requests retain
normal native/authentication checks. Required methods are process-immutable.

Challenge randomness is deterministically HMAC-derived from the exact request
wire, scoped key and a fixed 120-second time bucket. Retransmissions within a
bucket produce identical nonces. Response tags remain deterministic across bucket
boundaries for the same key and transaction. The timestamp is that bucket's
start, so issued challenges never gain validity beyond the normal 120 seconds.
The transaction tag carries an independently verifiable finite ACK proof. Proofs
bind exact flow and original transaction identity; changing buckets or refreshing
policy cannot create a call from a challenge ACK. Key retirement is bounded to
the challenge lifetime and cannot authorize issuance after policy expiry. No
request table or alternate authoritative native state machine is retained.

UDP ingress serializes admitted messages for one exact physical flow and
case-sensitive Call-ID through route-attempt completion. Accepted authenticated
controller reply follows native ingest commit, so an admitted ACK commits before the
next INVITE routes. Lost/expired replies cannot establish late-ingest order; absent
ACK proof can still produce native 491. Unrelated Call-IDs/flows remain independent.
Active and queued datagrams share 128 slots; overload drops without blocking the
reader or allocating a lane. Queue wait counts against the original two-second
route budget. Expired/cancelled work performs no fresh route and preserves predecessor
ordering while releasing admission. Location, redirects and ingest share that deadline;
The controller forwards remaining whole milliseconds. Stream handling is independent.

Tagged live-context lookup can derive current selectors from one validated read at
that Redis time without stripe acquisition, revision, retention or owner refresh.
Untagged requests, absent selectors, legacy pending, ended/owner-lost/private-refusal
contexts use atomic allocation. Fresh identities still need committed reservations.
Lookup grants neither authority nor a cached state copy. Policy/ingest revalidate
current state, lease and physical flow. Native lookup can refuse a fresh tagged
re-INVITE with 481 when its exact retiring/ended directional dialog proves refusal;
an occupied server-transaction key takes precedence. Other methods and live or
unmatched dialogs retain ordinary processing and rejection arbitration.

Before business routing, completion selection uses the same authoritative ReadUpdate
and timestamp as reduction. It never creates state; no match is a read-only result
without a receipt, and a CAS conflict repeats selection. Exactly one retained native
match is required; ambiguity is refused. REGISTER, SUBSCRIBE
and NOTIFY requests skip completion admission/reads; their responses still qualify
through exact native client-transaction proof. Admitted completion comprises:

- Responses: full retained origin, transport, Call-ID, CSeq/method, tags and Via.
- CANCEL/non-2xx ACK: exact INVITE server transaction and request/final identity.
- Successful INVITE ACK: exact directional dialog and remote INVITE procedure/CSeq,
  including already acknowledged replay. BYE: exact dialog state/sequence or retained
  server-transaction replay; no fresh context from foreign/expired dialog state.
- Tagged cached-final replay: owner-fenced call context only, exact server family,
  full native origin, byte-identical request and selected final at least 200.
  REGISTER/SUBSCRIBE/NOTIFY, subscription and out-of-dialog contexts are excluded.

No completion arrival authenticates an account, revives an owner, changes ordinary
delivery, presents business work or creates an application call. Matching ACK first commits
its actual acknowledgement and branch completion flow. Acknowledged replay cannot
republish, advance snapshot/event revision or displace newer delivery. Before owner
fencing a separate original-owner flow-adoption effect may proceed, without blocking
completion; owner loss abandons that proposal. Native callbacks retain the original
application identity, never that of a submitting relay.

TCP/TLS response continuity records only transaction/branch completion flow. Native
ACK/CANCEL/retransmission and each fork's cleanup use that proof, not ordinary
context delivery. Ordinary successful dialog-flow publication advances only the
selected branch's completion flow. Plaintext replacement needs exact current flow
or matching authenticated binding; TLS continuity needs full directional identity,
actual TLS and lifecycle proof. Retained transport class never changes.

## Business selection and subscriptions

Respond/OpenUac/DialogRequest require bounded command-time admission: directory
epoch, absolute semantic expiry, optional selected exact flow and expected delivery
receipt. Authenticated session, pinned term, context, operation, sequence and action
bind it. Absence of a receipt requires native delivery to remain absent. Stale epoch,
delivery or expiry refuses before a new sequence; exact replay retains its result.
The admission wrapper is at most 4 KiB, clamped to original control deadline and
retained only with the current/deferred transaction or initial open intent. Digest
inherits that selection. Later commands cannot refresh earlier setup/deferred work.
No response-scope history is retained.

Each APPLICATION emission matches its selected command/transaction, uses the exact
selected/response-return flow, and starts by the minimum of semantic expiry, Redis
time plus two seconds and owner expiry. Initial network open has a five-second cap
within semantic expiry. Existing physical effects preserve old command, flow, term,
fingerprint and deadline; completion may record their actual outcomes after selection
changes. Public Cancel checks exact owner/live term without refreshing setup.

Respond pins actual directory epoch; initial INVITE 2xx also pins original
policy-admitted INVITE scope/origin and selected-dialog scope. A later response
cannot replace that anchor. Changed epochs refuse business work. Native
response/completion uses independent retained SIP-state authority.

A public AD BYE's exact first WRITTEN outcome permits request-bearing ACCEPTED from
retained native wire, with a fresh snapshot. It proves the write, not a peer response
or ended snapshot. The transaction/timers/final result continue. Only its current
command, exact original BYE and transaction qualify; an out-of-dialog OpenUac BYE,
UNKNOWN or NOT_WRITTEN cannot. Later retransmission failure cannot revoke the proof;
peer-final/terminal processing may replace the command result. Other commands await
their normal final result. The controller handles intermediate acceptance separately from the final
peer result.

The controller and its account backend own account-version registration mutation and ordered flow publication.
No-op/older ambiguous unregister results preserve eligibility; explicit removal plus
unchanged-binding CAS withdraws it. Existing-context registration transport authority
has a four-hour lease, separate from new-call eligibility. Account identity, not
Contact or NAT port, scopes replacement. Subscription-only leases include terminal
grace. A standalone/subscription flow is not moved by another account's registration.

Subscriptions retain Event/id, fixed route headers, accepted Contact/flow/expiry,
independent local NOTIFY and remote request sequence. Native Dialog validates remote
CSeq; native client/server transactions own SUBSCRIBE/NOTIFY. Exact replay is response
only. Rejected refresh preserves accepted delivery fields and local NOTIFY sequence.
A refresh already admitted by ingest has consumed its remote request CSeq; rejecting
the controller response does not roll that sequence back. This differs from rejected
native ingest, whose partial mutation is rolled back. Failed response delivery does
not undo accepted business mutation. Every accepted renewal/unsubscribe, even unchanged
flow/controller, requires a new mutation/publication receipt. Initial bootstrap may
use its admitted unversioned delivery. Accepted Contact, flow, processor and expiry
commit together after receipt; failed publication leaves the old route and selects 503.

Controller preflight loads/discards current state before a pure trusted-directory
probe, then commits the approved controller before original SIP/command validation.
It changes no command sequence, accepted lease, target, flow, receipt or registration
watermark. Only an authoritative exact subscription-controller index and live term
prove adoption; an unrelated live process, missing index or unavailable read cannot.
Non-SUBSCRIBE request ingest can skip this probe only when retained context and
transactions prove it is not a subscription; ambiguous/pending/response/command paths
keep it. Native completion bypasses business preflight. CAS detects controller change
in its validated read and repeats bounded preflight without committing that computation.

Only accepted processor commands are admitted. Before publication the exact pending
renewal's processor may Respond to that request only; it cannot NOTIFY/cancel another
request. Old-processor refusal consumes no command/SIP sequence. A terminating native
NOTIFY guard returns completed 481 with consumed command sequence and no write, leaving
a later final NOTIFY usable. Zero expiry/timeout fences active NOTIFY before one final;
accepted unsubscribe/expiry retains five seconds for terminal delivery without renewing
the lease. Final completion/failure and NOTIFY 481 remove only that subscription.

## Bounded SIP connection drain

SIGTERM and interrupt withdraw SIP readiness before cancelling the runtime. New
SIP listener connections, outbound network opens and UDP peer allocations are
fenced immediately. Existing connections keep their readers, admitted completion,
control sessions and Redis authority available during drain. The
`--sip-withdrawal-grace` defaults to five seconds (zero through sixty seconds);
`--sip-drain-timeout` defaults to forty seconds, must exceed withdrawal grace, and
cannot exceed two minutes. These are measured from withdrawal, not renewed by
traffic. Deployment termination grace must also allow a two-second completion
reporting tail after the drain bound.

After withdrawal grace, idle streams close under their existing write lock.
Small process-local, hashed transaction hints distinguish an outstanding response
or INVITE ACK from completed transaction replay retention. They contain no SIP
wire or authoritative protocol graph and grant no admission or write permission.
Hints are bounded to 64 per stream and 8,192 per process; ambiguous or saturated
tracking conservatively holds a flow through the hard drain bound. Expiry and
reclamation use traffic or drain events, not a healthy background polling loop.
Pure L/M or application replay retention does not keep a settled socket open.

Existing responses, ACK/CANCEL and already admitted writes may finish; fresh
session starts on an existing connection are refused during withdrawal. The hard
deadline closes remaining sockets, including incomplete setup, without rewriting
logical dialog/subscription state or pretending uncertain writes did not start.
Closing and writing share the exact-flow fence. Completion reporting remains
available for at most two additional seconds; a missing report retains the
existing UNKNOWN semantics. Incoming traffic after reconnection still requires
ordinary shared-state admission and accepted-flow checks. Drain does not recreate
missing dialogs, transfer sockets, seal controller owners, or remove native
post-200/ACK, subscription or replay deadlines. Worker timer takeover remains
independent of connection closure.

## Exact physical evidence and response recovery

Every send retains immutable bytes and typed fingerprint. A physical write request
contains only the exact flow, effect and permit identities; bytes are loaded from
the committed reservation. Reservation commits before socket I/O and returns only
committed bytes; a terminal attempt cannot replay a
reservation into another start. Only the exact Flow.ingress process runs the local
Reserve/Complete writer. Other workers can compute/discover work, never use its socket.
Native permits/reservation/settlement share the SIP effect record, without another
ledger, native credential mesh or work record.

The physical writer publishes its exclusive local attempt memo before Reserve. A
failed/empty reservation proves only that invocation made no syscall; it records
NOT_WRITTEN completion without retrying Reserve/write. Lost reservation commit and
completion serialize by exact attempt. Native UNKNOWN grants no send; a new no-syscall
memo is removed without fabricating completion, while an original partial/uncertain
memo stays. A terminal known reservation also grants no write. Read-only inspection
can recover that exact stored terminal fact when the old acknowledged memo is gone,
including differing auxiliary error text; it cannot replace genuine uncertain evidence.
Atomic expiry of AUTHORIZED unreserved delivery records NOT_WRITTEN with its already
pinned attempt ID. DISPATCHING/UNKNOWN cannot expire into no-start proof.

Local memos retain the original monotonic start deadline. Completed memos can expire
then, even after lost acknowledgement; in-flight syscalls are never evicted. Inspection
then uses retained Redis evidence or UNKNOWN. Exact reservation forbids a second write;
pointer checks prevent a late acknowledgement deleting a successor memo. Losing local
evidence may fail a call rather than permanently consume healthy writer capacity.

A selected server response retains compact status, command/deadline, wire hash, exact
return flow, written proof and retry time beside native cached response. Native
termination, a historical Written bit, earlier provisional or another response's write
cannot acknowledge it. Superseded outcomes update only their own physical evidence.
Conclusive command results cannot be revoked by later retransmission failures.
Publication-failure selection preserves its 503/error until actual delivery failure
or write; it never acknowledges the originally requested acceptance.

Exact NOT_WRITTEN or atomic expired-unstarted invalidation can schedule recovery after
T1 with unchanged wire hash/flow and original Respond/native bounds. Existing work
coalesces. Any relevant final DISPATCHING/UNKNOWN/WRITTEN or unclassifiable historical
physical evidence blocks automatic final recovery. Provisional recovery requires its
exact selected command/status/hash and no such outcome for that response on any
admitted flow; the any-flow blocker survives compaction. Generic absence of effects or
transaction Written proves neither delivery nor no-start. Explicit validated request
replay may adopt an admitted response flow; automatic recovery never adopts another.

Provisional retry ends at the original Respond deadline and native retirement bound.
A proceeding INVITE's zero RetireMS neither removes nor supplies that command bound;
automatic 100 has no unbounded retry. Positive no-start proof with exhausted quota or
deadline and no uncertain work settles a definite completed delivery failure, preserving
a later final command. Uncertainty/expiry without conclusive proof stays UNKNOWN.
Non-INVITE finals use Timer J/retirement and Respond bounds; reliable non-2xx INVITE
finals, which lack periodic G, use the helper within original H/Respond. Other INVITE
finals retain native G/H or success/ACK obligations. Protocol-generated finals use their
own H/J/L bounds, never an earlier provisional command deadline. Native ACK/termination
ends applicable retries; blocked no-start work is reconsidered at T1 within finite quota.

Paired CANCEL commits both transactions, attributing CANCEL 200 evidence/quota only to
CANCEL and INVITE 487 only to INVITE. Exact replay preserves proof/quota. A written
CANCEL 200 never acknowledges INVITE 487. Rejected re-INVITE releases offer arbitration
without changing accepted SDP/target. An accepted INVITE response commits local/remote SDP, target and revision together
before writing. Non-INVITE responses never change media SDP; INFO,
REFER and in-dialog NOTIFY complete their own client transaction/command.

## Forks, cancellation and outbound setup

At most sixteen directional UAC branches are retained. First valid initial success
selects the application dialog once. Every other success, including after cancellation
or terminal setup, retains branch ACK/cleanup BYE without replacing the winner or
ending its snapshot. Repeated success replays cached ACK, never another logical BYE.
Re-INVITE/BYE completion updates its exact directional dialog.

UAC cleanup BYE cannot authorize/reserve delivery until the exact directional ACK
procedure is proven WRITTEN. A later identical native ACK retransmission can prove it
while an earlier attempt remains UNKNOWN. An expired unstarted BYE can re-emit its same
cached bytes only after atomic invalidation, within original Timer F and finite quota.
Exact physical NOT_WRITTEN can release that replacement earlier. DISPATCHING/UNKNOWN/
WRITTEN blocks replacement. UAS ACK-timeout cleanup has no local ACK prerequisite.

Cancellation commits its decision first, invalidating never-started initial INVITE/
open work. A late exact grant becomes no-start/settlement, never send permission;
reserved attempts retain original evidence and native cleanup duties. Failed initial
client INVITE emits FAILED once but retains late-success branch ACK/BYE until fixed
native retention. A client BYE ending without reply ends its exact dialog. Retained
reliable client non-2xx ACK replay uses exact transaction response flow, remote tag,
native bytes and original retirement/quota, independently of provisional branch flow.
An already-ended successful branch replays ACK without recancelling or allocating BYE.
Pending ACK wire capacity survives separately from live-dialog count.

Application-created client transactions, including local re-INVITE inside a UAS
dialog, pin their own outbound application scope. Their ACK uses that transaction
scope. Initial context/dialog cleanup scope remains separate; only an actual cleanup
transaction inherits it. Initial digest-retry and fork scopes never merge.

Local BYE fences new dialog work before write/wait. Concurrent re-INVITE gets native
481 without waiting behind it; ACK/crossed BYE remain native. Failure cannot reopen
admission. In-dialog REFER's successful response uses Contact from the exact retained
portable dialog, matching Call-ID, directional tags and transport. Missing/inconsistent
Contact fails closed rather than selecting preferred dialog/current worker advertisement.
REFER NOTIFY uses the INVITE dialog/client transaction without MWI/BLF subscription state.

OpenUac OPTIONS/MESSAGE/NOTIFY uses a standalone transaction and retires its context;
empty/INVITE creates the dialog path. The controller selects check-sync. URI remains the called
URI when proxy supplies next hop. Direct credentials are refused; Redis retains only an
opaque context-scoped versioned credential reference. A matched 401/407 commits a pure
digest effect with original request/challenge, fixed cnonce/count and new Via branch;
at most three retries, no duplicate-challenge allocation. Challenge To-tags do not pin
successful dialog tags; retry response matches live transaction/current CSeq.

Credential lookup through the original controller and sipgo's pure complete-Request-URI
digest builder occur outside reduction. Only derived SIP request enters transition,
never password, credential reply or credential-bearing errors. Fixed inputs permit
repeat lookup, but current owner guard and exact result identity admit only one
commit. The result admission cutoff is computation deadline plus 120 seconds, fixed
across executors; after computation expiry it can fail setup but start no new request.
Open/credential effects and conclusively pre-wire FAILED reporting end by their
original deadline plus 120 seconds only when no uncertain grant/physical evidence
requires retention. Missing/changed credentials fail setup, never adopt another call.

## Scheduling, results and reclamation

### Owned scheduling

A group has one derivative timer executor. Its earliest protocol, recovery or
retirement deadline is indexed in that worker's sorted queue. The worker retains
only the observed head/cursor and a conservative monotonic wake time. It reads a
group when work is due, then validates the current generation and commits the
transition. Neither a queue entry nor worker presence grants SIP authority.
A request on another worker does not move timers or recreate its old socket.

There are two worker indexes with different purposes: live dispatch presence for
all workers, and expiry hints for workers that actually own nonempty timer queues.
One presence renewal covers all of a worker's groups. A worker with no scheduled
group has no orphan-timer entry. Commits maintain timer ownership, queue heads and
earlier-deadline wakeups atomically; removing the last timed group removes that
owner's orphan hint. Healthy presence renewal does not read protocol records.
The owner hint uses its current presence expiry. Without live presence, preserve
existing expired evidence or make a new hint immediately due; an unrelated commit
cannot give a dead owner another lease. Takeover validates destination liveness
through dispatch presence, so a live worker with an empty timer queue is eligible.
Transfer atomically maintains both queues and their source/destination owner hints.

One namespace coordinator handles unowned work, application-owner expiry and
expired timer owners. It selects overdue classes fairly, otherwise the earliest
future deadline. It performs
bounded takeover only when an owner is due, transfers at most 64 groups per page,
and paces remaining pages at 20 ms. Transfer changes scheduling metadata, not
protocol revision, socket identity or application owner. A stale acknowledgement
cannot erase newer work; an already admitted timer still needs its exact native
generation and ordinary CAS checks. Overdue unowned, application-expiry and orphan
classes receive bounded round-robin progress; a continued takeover page cannot
starve the other classes. With no due class, use the earliest future deadline.

A contender receives either a live lease or the current holder's validated expiry
and handoff cursor from one atomic observation. On contention it waits for that
expiry or a lease-handoff notification instead of retrying on a fixed cadence.
Conditional release cannot erase a successor and wakes contenders only after its
own release commits. Renewal does not wake contenders. A lost wake is covered by
the remembered expiry; it cannot postpone acquisition indefinitely.
The follower watch lasts for the follower role, not one lease observation.
Healthy contention updates the acquisition deadline without cancelling an
in-flight notification read. The watch advances its explicit cursor as wakes
arrive; a new observation does not rewind it. Acquisition, a watch/authority
error or shutdown ends the watch. Cancel and join it before starting the leader
watch, retaining the acquired lease's original conservative deadline throughout.
A lease that expires during that join grants no usable leadership.
The handoff stream retains at most one constant marker with a 120-second idle
TTL. Its type and required command permissions are checked before lease mutation.

The coordinator renews independently of due-work and repair I/O. Lease uncertainty
cancels its admissions. Every store operation checks the actual token, primary and
lease; a cached deadline is never authority. After Redis/primary/watch failure the
worker discards its observation and obtains a fresh one under bounded backoff.

| Maintenance boundary | Bound |
| --- | --- |
| Coordinator lease / renewal | Two seconds / once per second, under the conservative original lease deadline |
| Dispatch presence / renewal | Two seconds / once per second; one renewal for all owned groups |
| Contended acquisition | Observed holder expiry or handoff wake; no periodic acquire loop |
| Orphan takeover | Actual earliest nonempty owner-queue expiry; no periodic takeover loop |
| Notification transport | Bounded one-second XREAD on a separate two-connection pool; timeout alone reloads no head or protocol record |
| Index repair | One incremental 128-key page per second while a pass is active; no repair timer after completion |

The notification transport's finite read permits cancellation and primary
validation. Worker and coordinator watches cannot occupy the execution pool.
Follower handoff and leader scheduling watches replace one another: cancel and
finish the former before starting the latter. Together with the independent
dispatch watch, they fit the two-connection notification pool.
Cancellation can wait for the remainder of the one-second blocking read.
That join contributes to failover latency and consumes the acquired lease's
original deadline; it never starts a new lease or extends permission.
Each watch reads from an explicit cursor without consuming another worker's wake.
A cached head may fire early after a deadline is extended; that causes one fresh
head read. Earlier work emits a wake in the same atomic mutation. Empty queues
leave no due-work timer. One finite repair pass starts after coordinator acquisition
or a corrupt hint; corruption cannot turn into a permanent healthy-state scan.
Both worker and coordinator observations atomically read head, cursor and validated
Redis time. Convert delays conservatively, including elapsed I/O. Creating the
first timer-owner hint is an earlier coordinator deadline and must publish its
wake in that same mutation, including commits made on another worker.

Unowned ready/due work retains bounded partition paging: ceil(partitions/8),
clamped to 1–8 partitions per 20 ms pass, separate eight-entry ready/due limits and
100 ms store-read budgets. Eight protocol passes cover 64 partitions: 160 ms of
nominal protocol-pass cadence, before I/O or intervening due classes. This is not
an overall recovery-latency bound. After an idle wait, start at the observed head's partition; continued work
rotates fairly. Timer eligibility comes from the selected group's validated store
clock. Outages do not extend SIP deadlines.

Definite fresh commits dispatch local effects directly. Their local backup index
gets a 100 ms eligibility grace; remote targets and repair remain immediate. A
continuing group/target keeps the earlier existing score. Earlier native timers
still win; the grace changes neither write permission nor original deadlines.
Exact revision acknowledgement and settlement remove the hint. Dropped direct
work near expiry has less backup time, never renewed permission.

Application-owner expiry has one shared deadline per owner. Renewal changes owner
metadata, not every dialog. Expiry retains its fixed grace, then uses bounded
resumable context pages. A new term or attachment generation fences old progress.
Initial setup may fail during a worker outage; established INVITE/ACK/fork and
subscription/NOTIFY obligations keep their separate shared-state rules.

This scheduling contract requires a coordinated worker-cohort upgrade. An older
writer does not publish every required coordinator wake, so mixed operation is
not qualified merely because protocol descriptors remain unchanged. Stop all old
workers in a namespace before starting this implementation. Retained compatible
protocol records can remain; no migration reader or historical cleanup loop is
introduced. Restore the complete cohort before testing continuity and measuring
idle behavior. Application-controller recovery remains separately optional.

### Command observation and dispatch progress

Dispatch chaining requires exact forward effect progress. A repeated/uncertain
receiver result returns to durable recovery, never tight RPC/result recursion. The
physical guard releases once; known physical results chain native settlement, uncertain
results return to recovery. Only earliest unresolved lifecycle event dispatches. Its
original two-second envelope deadline anchors a 120-second recovery horizon; each RPC
gets at most two seconds capped to that horizon. At the exact cutoff no new start/RPC
occurs. Prefix order, receiver ID, application/lease guards remain; event recovery never
extends SIP/command/physical authority. Notifications behind predecessors retain this
same horizon.

Commands that can wait register their bounded, exact operation waiter before
producer admission/CAS, after resolving the group and context. Every return path
unregisters it. An indexed writer can therefore complete immediately after the
producer commit without beating waiter registration. Generic group-change hints
do not wake identified command waiters: an intermediate socket completion or
unrelated ingest is not their result. A definite successful local CAS may hand its
matching waiter one compact result DTO containing exact message wire and the
committed snapshot. This transient projection is restricted to the exact group,
incarnation, context and operation; lower store revisions cannot replace higher
ones. Only completed outcomes or the existing proven written-public-BYE result
qualify. Uncertain CAS, replay and read-only paths cannot publish it. An ordinary
non-INVITE client final updates the current command result only when that
transaction's command identity still matches the current command. A final for an
older transaction still completes its native transaction and dialog duties; it
cannot populate a newer command's result slot. The reducer builds the projection
only for active local waiters; it is never persisted, is removed with the
waiter, and contains no authoritative protocol aggregate. Public message fields
are reconstructed only when returning it. The optional DTO is capped at 1 MiB;
larger replies use the store-read fallback. Missing local evidence, sibling-worker
completion and reconnect retain the validated store-read fallback. Fresh commits
wait for an exact result notification or 100 ms before first read. Replay/nil
projections read immediately. Subsequent fallback waits back off from 100 to
200 to at most 250 ms. Exact local results wake immediately at every interval;
remote or oversized results may wait up to the current fallback interval.
Each wait starts after read/decode, avoiding catch-up reads.
When the remaining original budget is no greater than the planned pause plus
100 ms, waits cap at 10 ms and remaining budget.
No Pub/Sub/background state copy exists; only latest bounded result survives. Cancel/
completion unregisters. An unstarted OpenUac allocated before sequence admission is
retired on budget expiry; expired queued commands never advance sequence or retire
an already active context.

`zt_sipbridge_operation_observations_total` has only the fixed `local` and `store`
source labels. It counts consumed exact local results and fallback ReadState
attempts, respectively, not unique commands. Two process-local atomic counters
retain no identifiers or state and require no extra store observation.

### Retirement and physical uncertainty

Retirement requires native procedures/replay windows, timers and exact physical
settlement complete; terminal application bits/time alone are insufficient. Native
settlement is local SIP-state work.
A transaction that actually enters the portable terminated phase discards its
stopped timer map. Its emitted timer cancellations retain their original values;
origin/final/ACK wire and request/physical proof keep their existing lifetimes.
An active timer in that result is refused. Already terminated records and dialog
timers are not rewritten by this transition rule.
A successful reliable subscription NOTIFY client transaction may retire during normal
reduction before the generic replay tail. This requires its actual terminated phase,
a matched retained 2xx, positive settled WRITTEN proof transferred from its physical
effect, no native timer or pending/digest/cleanup/request-disposition obligation, and
no retained effect for that context. Effect absence alone never proves a write.
The subscription sequence, accepted flow, current command result and commit markers
remain authoritative. Unmatched duplicate finals cannot emit a response or restart
that transaction. Unsettled, uncertain, UDP, unsuccessful, server and INVITE
transactions retain their existing bounds; no historical repair or polling is added.
True UNKNOWN stays charged. Terminal compaction can remove duplicate wire/snapshots
but preserves identity/hash/permit/outcome and causal written-ACK proof. Settled terminal
physical and receiver-acknowledged nonphysical effects alone free slots. Expired obsolete
non-native UNKNOWN in an ended context stops legacy recovery polling only after its
fixed windows and every live continuation expire; it is never relabeled WRITTEN or
NOT_WRITTEN. An ended, owner-lost native context also stops recurring retirement and
effect-recovery timers after its original retirement/replay bounds and each unresolved
physical delivery's deadline plus 120 seconds have elapsed, provided no transaction,
active dialog timer, owner cleanup or current request fence remains. Only expired
native DISPATCHING/UNKNOWN may remain unresolved; pending work and known unsettled
outcomes keep maintenance enabled. These cold records and their exact effect bytes,
attempts, reservations and UNKNOWN evidence remain charged and inspectable. This does
not prove retirement, change settlement or authorize another write. Exact-worker
physical dispatch and late completion remain independently available; worker registry
expiry is not death proof. Other contexts continue normally. Timer projections change
atomically, not by deleting rebuildable indexes.

Native context IDs never reuse. Backend cleanup of trusted permits/directory records
requires exact indexed metadata and native inspection under the same authority, then
raw-value/epoch CAS. Whole-group absence, obsolete incarnation or absence of non-reused
inbound context can prove retirement; live reservation/unresolved creating input cannot.
481, timeout, changed/unavailable authority and missing reply never prove it. Bounded
trusted indexes backpressure when proof is unavailable. A separate 128-entry retired-context marker quota covers remaining original replay/
reconciliation cutoff without consuming live-context slots. Redis time prunes expired
markers; replay never extends one. Repeated transaction contexts can leave room for
new work while another context keeps the group alive. Group retirement checks exact
incarnation/revision and empty live work; active records do not automatically expire.

### Work accounting

Metrics are bounded process counters/gauges with fixed kind, source, stage and
error-class labels. Collection reads no protocol state, retains no SIP identity
and forces no GC. Encode attempts, commits, callback attempts and physical outcomes
are distinct measurements. The [operation atlas](operations/index.md) owns their
implementation accounting; toolchain/thread changes require separate measurement.

## Datastore failure and refusal arbitration

Readiness coalesces bounded primary/clock checks, never scans SIP keys. A temporary
store/clock problem refuses current work and recovers on later healthy observations;
no operator repair latch exists. Redis TIME uses 250-ms tolerance against operation
send/receive interval and monotonic elapsed time. Pre-mutation script window uses the
original I/O/context bound; a post-mutation anomaly leaves UNKNOWN with no dispatch.

A positively failed preliminary retry/receipt read stops before mutation. Only proven
missing state permits first admission. Ingress then attempts native rejection with a
fresh 2.5-second arbitration budget, followed only on independently indicated datastore
unavailability by a fresh 100-ms stateless reply budget. Expired UDP work never started
is dropped. Routing budget exhaustion, caller expiry, no peer or a local health flag
alone does not prove outage. ACK/responses never receive failure replies.

The stateless exception applies to positively non-effectful read failure with transport
or detected authority/profile/clock/corrupt-state failure, not programming/ref/capacity
errors. A datastore I/O timeout in independent arbitration is best-effort evidence and
cannot distinguish overload from blackhole. Preliminary read failure cannot exclude a
concurrent/earlier acceptance: the requested 503/Retry-After can conflict with its final
when arbitration is impossible. It uses exact current flow, deterministic fallback To
tag and no dialog/allocation/business mutation. Uncertain rejection commits cannot
produce another alternate final. Known absent dialogs get normal 481. If a validated
native refusal read proves the Call-ID group absent, a request with both From and To tags
is refused without allocating a group or private transaction, including when no
controller peer is ready. An unknown ACK remains unanswered. Existing groups still
use normal CAS refusal arbitration; unavailable or uncertain reads are not absence.

Normal refusal is a bounded private native context binding full flow, exact bytes and
status, with no selectors. It cannot replace admitted return flow, dialog, cached final
or transaction. Status-independent refusal identity makes first selected final win.
Exact flow/request replay alone reuses it. Its negative-INVITE ACK validates original
flow, INVITE identity and final To-tag before stopping G/H, without ordinary admission.
Ordinary ingress and refusal arbitrate in the same group CAS. Occupied mismatched
transaction identity/bytes cannot be overwritten before dialog/subscription sequencing.

Each server transaction retains at most 22 complete-flow/request HMAC proofs. An admitted
proof suppresses competing private final; prior refusal handles late ingest without
business/flow/sequence mutation. Directory-approved controller-only adoption remains
independent. At quota an unproved flow gets neither cached output nor competing final,
even after lost handoff reply. Refusal cannot cancel another flow's initial reservation.
Proof retention floor is original input deadline plus 120 seconds, frozen on first
refusal and raised only by unseen validated admitted proofs. Pruning protects recovered
opposite dispositions before/after reduction. Retention never reopens native replay
past RetireMS. Tombstone/expired-no-new-work/saturated-flow hits return clean COMPLETED
without implying SIP acceptance or physical delivery.

## Service bounds and encoding

| Resource / operation | Bound |
| --- | --- |
| SIP frame | 64 KiB |
| Control envelope, store input and operation outcome | Each at most 1 MiB; negotiation can reduce envelope bound, never split a logical operation |
| Native group | 64 MiB; 32 contexts; 16 directional forks per context |
| Regular / paired CANCEL / cleanup procedures | 32 regular, 32 CANCEL, two BYEs per fork; rounded to 128 transactions |
| Per-transaction emissions | 22: server hard partitions of 11 provisional (including automatic 100/replays) and 11 final; client request/ACK classes unchanged |
| Effects / timers | Each 4,096 slots; derived effect demand 3,008 rounded up |
| Completion reserve | All future transaction/fork emissions, timers, immutable proofs, 64-KiB worst-case wires and maximum inheritable admission wrapper |
| Arbitration proof reserve | 1,600 bytes minus already encoded proof/floor metadata per retained server transaction, plus 1,600 per derived future procedure |
| Owner term / renewal / recovery | 30 s / at most every 2 s / fixed 5 s episode |
| External operation results | Until original operation deadline; later retained markers return UNKNOWN |
| External commit markers | 120 s reconciliation tail; internal native identities/status remain replay authority |
| Business command / ordinary RPC / physical delivery | 40 s with negotiated command budget / 2 s / 2 s |
| Initial network open | 5 s within original semantic expiry |
| UDP admitted active + queued | 128; original 2-s route window includes queueing |
| Command admission JSON | 4 KiB |
| Terminal subscription delivery | 5 s after accepted unsubscribe/expiry |

Ceilings are limits, not eager allocations. Fresh admission reserves completion space
before output and can refuse by actual bytes. Eleven-transmission classes follow capped
64*T1 native schedules. Excess provisional replay is ignored; an application response
with no emission capacity is a definite consumed-sequence refusal, never ACCEPTED.
Terminal native contexts and terminated transactions use two-minute retention
floors. Exact native timers, replay evidence, pending effects and physical settlement
can keep records longer; the floor alone never proves retirement. Five-minute
transport claim/late-reply retention is separate from native context retention.

Reads enforce actual retained encoded bytes without re-encoding for size. Writes use
exact canonical bytes, reserve completion before subtraction to avoid overflow, and
validate the full embedded document/nesting. Structured bytes require successful JSON
marshal exactly matching their original bytes; candidates through 64 KiB need no extra
pre-scan, larger ones validate before potentially expanding marshal. Noncanonical or
malformed candidates retain opaque representation, subject to normal corruption checks.
Custom marshal/validation failure preserves the outer encoder's rejection/wrapping.

Retained payload, native group, input, effect, digest and network-target values use
generated typed protobuf adapters behind a private `SIPB` frame with encoding and
root-kind bytes. Wire/SDP data are raw protobuf bytes, never hex or base64 wrappers.
The private field registry preserves field numbers. Exact physical worker targets,
including the wildcard controller-executor hint, roundtrip with each retained effect.
Control messages use the [binary protobuf wire profile](../PROTOCOL.md#binary-websocket-framing). Lua-inspected receipts, pending-input
envelopes, application guards and bounded dispatch-target/worker projections retain
their existing small JSON/scalar representation and atomic accounting.

The decoder preflights the complete graph before receiver assignment: 64 MiB total,
depth 32, 4,096 entries per collection and 262,144 total fields. Unknown fields,
duplicate singular/oneof/map entries, invalid enums and invalid UTF-8 are rejected.
Typed adapters preserve absent/empty collections and optional values; failed decode
leaves the receiver unchanged, and decoded values do not alias input or prior values.
Deterministic encoding preserves operation fingerprints. Runtime private payloads
have no JSON or preceding-binary fallback; diagnostic JSON helpers are fixture-only.
Each image must match its full public/private source receipts. Introducing this
persisted format requires a fresh storage namespace, without migration or mixed
JSON/binary writers. Source-only changes with unchanged descriptor identity need
explicit retained-state and mixed-worker qualification before rolling replacement;
they do not inherently require a namespace reset. The offline inspector returns
bounded redacted counts only and grants no restore or execution permission.
Dispatch validates the complete retained effect before physical/control I/O. No
encoding optimization adds a resident cache, skips capacity checks or changes
native transaction, owner, CAS or exact-write authority.

## Network advertisement and observability

TLS builders use configured TLS public endpoint; UDP/TCP use public plain endpoint.
Contact preserves user/URI parameters with selected host/port/actual transport; SIPS
Contact templates become SIP for new plaintext contexts. Plain defaults to TLS public
host plus actual plain-listen port; --advertise-plain overrides NAT/public host:port.
Direct engine configurations default to 5060. Existing context transport/flow never
changes through advertisement. Shared public advertisement is required for takeover.

--sip-interface selects exactly one existing global-unicast IPv4. SIP listeners and
TCP/TLS target dials bind it; UDP uses the bound listener. Private listeners are
independent. Missing/ambiguous address or conflicting listener host refuses startup.
Absent explicit advertisement, use that address/configured TLS port. No route/interface/
firewall changes or NET_ADMIN are needed; shared ingress advertises its VIP explicitly.

New outbound TCP/TLS streams have one immutable unused-transport lease: OpenNetwork's
original operation deadline (or entry time without one) plus two minutes, anchored to
entry monotonic time. Dial completion, cancellation, lost result and activity never
renew it. Complete parsed/observed inbound SIP and authorized write admission record
potential responsibility before routing/syscall under exact flow lock; expiry uses
that same lock and deadline. Partial frames/CRLF/connect/reservation alone do not count.
Expiry closes only an unused outbound stream and frees its exact flow/descriptor/slot;
responsibility ends the lease permanently, including physically uncertain writes.
Inbound streams/shared UDP retain their own lifecycle. No uncertain open is redialed.

Control failures log fixed local stages/categories, numeric close code and validated
process/session metadata, never peer text/raw errors/frames. Slow operations report
bounded timing/correlation/context and worker occupancy without extra reads. Call-ID
hashing trims whole value, validates printable non-whitespace ASCII through 512 bytes,
then hashes; invalid/missing IDs are omitted. Ingress emits one scalar timing report
on failure or at least 100 ms, separating completion/location/route/refusal and entry/
completion budget/hops. Refusal's separate budget can make total exceed route budget;
diagnostics never renew it or parse/read/change authority.

Optional SIP capture observes complete ingress frames and actual egress attempts as
plaintext at process boundary, with real socket endpoints. CRLF is excluded; distinct
retransmissions remain separate; partial/failed writes do not prove complete delivery.
A bounded nonblocking queue separates encoding/HTTP/batch/retry from signaling; drop,
collector failure and shutdown loss never block/change calls. Records go only to the
explicitly configured edge-local collector. The SIP process receives no trusted Loki
credential; collector/deployment establish provenance.

Capture records store `wire` as readable JSON text, preserving UTF-8 headers, bodies
and CRLF through JSON escaping. If the body after the first CRLF/CRLF separator is
not valid UTF-8, replace the entire body with `[NNN bytes]`, its original byte length.
Headers, including Content-Length, remain original; this display placeholder is not
a replacement SIP message. Invalid UTF-8 header bytes use JSON Unicode replacement
characters. `wireBytes` retains original message length and `bytesWritten` remains
the actual socket-write count. Display conversion never changes write-outcome proof
or signaling bytes. Ordinary logs contain operational failures and aggregate
drop/recovery information, without duplicate SIP payloads or per-message success noise.
Routine logs contain aggregate drops/recovery and operational failures, never a
duplicate payload or per-message success stream.

Capture v1 indexes request method or response's single valid CSeq method in INVITE, ACK,
BYE, CANCEL, OPTIONS, REGISTER, PRACK, SUBSCRIBE, NOTIFY, PUBLISH, INFO, REFER, MESSAGE,
UPDATE; other/missing/invalid/ambiguous use OTHER. Exact valid extension method remains
structured metadata. It is a search hint, never authorization or SIP validation.

## Recoverable controller owners

The recoverable attachment profile is specified in
[Controller recovery](controller-recovery.md). Recoverable owners use a five-second
lease and ten-second fixed post-expiry grace; ordinary owners retain the
process-bound lease profile described above. The memory and Redis adapters atomically guard
attachment generation together with term/expiry for fresh application commits and
write reservations. Owner-context metadata is indexed under a bounded quota with
no second authoritative FSM.
