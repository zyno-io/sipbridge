# Service runtime contract

This is the normative execution and bounds supplement to
[ARCHITECTURE.md](../ARCHITECTURE.md). It describes the schema 1 / reducer 1 /
timers 1 CAS service. Historical claimed/relay execution is restricted to
[test fixtures](fixtures.md); it is not a service configuration or recovery path.
[PROTOCOL.md](../PROTOCOL.md) owns wire and business-admission semantics.

## Configuration, identity and loss

`SIPBRIDGE_STATE_BACKEND` defaults to `memory`; `redis` must be explicit. Unknown
values fail startup. Selection is fixed for the process lifetime. Redis settings
without explicit Redis selection are rejected as inconsistent configuration.
Both adapters implement the same read/reduce/CAS, lease, effect and retirement
contract. Historical claimed execution remains fixture-only.

Memory requires no Redis connection or credentials. An omitted identity key is
randomly generated for that process; an explicitly supplied key uses normal key
validation. Each memory store also has a fresh authority identity, even when its
configuration/key matches another store. Independent processes and restarts cannot
execute old references. A controller remains required for business policy. Memory readiness
checks local lifecycle/clock health, not Redis; signaling readiness still requires
an eligible controller. Closing the store fences operations and releases records.

Memory uses independent snapshots and a context-cancellable mutex around store
operations; pure reduction and external I/O occur outside it. Group commit, lease
reference updates/sealing and scheduling indexes change atomically. Time is sampled
inside that operation; wall/monotonic discontinuities fail the affected operation.
Memory indexes are maintained atomically, so schedule repair is a bounded no-op.
Lease expiry, recovery grace, sealing and idle reclamation match the Redis profile.

Memory uses the native profile’s 64-MiB per-group encoded bound, a 128-group
ceiling and 1 GiB of reserved group capacity. The default byte reservation therefore
admits at most 16 concurrently retained groups; a group may contain multiple native
contexts. The full per-group bound is required before outbound INVITE admission
so all mandatory fork/transaction completion paths retain space. `SIPBRIDGE_MEMORY_MAX_GROUPS` and `SIPBRIDGE_MEMORY_MAX_BYTES` may
lower or raise the total reservation bounds. Every newly committed group reserves
its full group bound until exact retirement. This is encoded-state accounting,
not an RSS guarantee; Go objects and transient snapshots add overhead. Capacity
refuses new groups without eviction and cannot consume an existing group's reserved
completion capacity. Existing per-group admission/reserved-completion checks remain.
Active records never expire merely to recover capacity.

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
and schema/reducer/timer profile. Deployment owns this consistency; readiness cannot
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
only computation. External operations retain bounded receipts and current-command
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

Presentation, lifecycle and pure credential effects can dispatch directly from
Pending without effect-start/executor claim. Fixed receiver/envelope/deadline and
controller receiver deduplication remain mandatory. Result commit checks the exact
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
INVITE, ACK, CANCEL, BYE, OPTIONS and in-dialog REFER/NOTIFY keep their normal
handling. The local 405 Allow header describes this process's enabled native
methods; it grants no business or account authority.

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

Before business routing, read-only completion lookup never creates state. It needs
exactly one retained native match, checked again by reduction. REGISTER, SUBSCRIBE
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
only. Rejected refresh preserves accepted fields; failed response delivery does not
undo accepted business mutation. Every accepted renewal/unsubscribe, even unchanged
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

One group recovery timer replaces per-effect timers. One background scheduler per
state namespace holds a 250-ms Redis-time lease, renewed every 50 ms with a unique
leadership token. Followers retry acquisition every 100 ms and do not enumerate
queues or repair indexes. Conditional renewal/release cannot revive or delete a
successor's lease. A changed primary invalidates the lease. Leadership coordinates
background work only; both SIP workers remain active and all CAS, owner, deadline
and exact physical-write guards remain authoritative. An uncertain/expired local
lease stops new scheduler admissions and cancels that leader's background work.
Lease renewal runs independently of queue reads and maintenance. Every read uses
the latest conservative local lease deadline; renewal failure cancels the leader
and its admitted work. One repair page may run concurrently with due-work
discovery, with a two-second total budget and the same per-operation Redis lease
guards. A slow repair page cannot block renewal or native timer admission.

Native physical delivery has a separate, per-process dispatch index. Each commit
projects the opaque ingress process/epoch target of unsettled native delivery
effects into that worker's queue and emits a bounded wake marker. Every worker
listens for its own work independently of scheduler leadership; followers never
enumerate shared timer/ready partitions. They load only their indexed groups,
discard the aggregate before physical I/O, and acknowledge a hint only against
its exact observed revision. A newer commit cannot be erased by an older ack.
The index is a hint: exact native reservation, permit, process/epoch and socket
guards alone admit writes. Recovery timers can republish an unsuccessful attempt;
no process can take over a dead process's socket. Index entries remain bounded by
retained physical obligations and disappear on settlement/retirement. Blocking
wake reads use a separate bounded connection pool, never the execution pool.
Controller/credential/network effects are indexed to every currently registered
worker, so a missing control connection on the scheduler leader cannot suppress
an operation another worker can service. Registration is a two-second scheduling
hint renewed every 500 ms, bounded to 256 live workers and pruned on renewal; it
grants no application authority. Per-process acknowledgements are independent.
New/reconnected workers receive retained work on the next committed recovery
transition. Existing exact effect-start, receiver deduplication, owner and physical
guards still decide which operation may progress. Physical ingress targets remain
explicit and are never expanded through this registration. Blocking application
I/O has a separate group-admission memo from timer reduction.

Every index mutation atomically updates a sorted head projection containing at most
two entries per partition. A queue whose head advances appends a constant marker to
a wake stream, exactly trimmed to its latest entry. This is a bounded notification,
not an event/job history or a consumer group. Types and required command permissions
are checked before protocol mutation. The head, wake cursor and Redis time are read
atomically under the current scheduler lease. Only the next deadline and stream
cursor survive that read; neither is protocol authority. The deadline becomes a
local monotonic timer. A separate blocking XREAD from that explicit cursor wakes
earlier work without consuming another leader's notification. Timeout/lease renewal
alone does not reload queues or protocol records. A change between head-read and
XREAD remains visible; coalesced/trimmed notifications require one fresh head read.
The memory store supplies the same boundary with process-local notifications.

A new leader rebuilds the head projection from the ready/due queue heads before
waiting; it never adopts its predecessor's cached timestamp/cursor. Redis failure,
primary change or notification error discards the cache and reacquires leadership.
When work is due, every 20 ms the leader visits ceil(partitions/8), clamped to 1–8
partitions, with a 100-ms read budget. Through 64 partitions a healthy sweep takes
at most 160 ms, below 500-ms native T1. One bounded Lua call validates the current
lease and reads consecutive partitions at a common Redis TIME cutoff; ready/due
classes retain independent eight-entry score-ordered limits. The first admitted
batch after an idle wait or leader replacement starts at the cached head's partition;
continued due work follows the rotating cursor rather than repeatedly favoring a
hot head. Timer eligibility uses the validated store clock from that group's read.
The first admitted partition rotates per full cursor cycle, including failures. Index type/member
corruption isolates its class; mutation still refuses bad index types. This assumes
single-primary/Sentinel, not Redis Cluster cross-slot support. Lease handoff plus
healthy store/admission latency must be qualified against native deadlines; outages
do not extend SIP deadlines or promise uninterrupted retransmissions.

Hints grant no authority; each selected group reloads current state and timer generation.
Ready precedes due; overlapping work deduplicates. A newly elected leader, or a
leader observing corrupt hints, runs one bounded incremental metadata-repair scan,
at most one 128-key page per second alongside normal dispatch. It stops at the end
of that scan instead of continuously rescanning an idle namespace. Normal commits
maintain both indexes and the head atomically; scheduling never destructively pops
an obligation before its transition commits. Repair cannot recreate lost protocol
records or guarantee timely recovery from arbitrary external index corruption.
The admin metrics expose scheduler leadership, acquisitions, head reads, hint
batches, coalesced wakeups and repair pages as bounded process counters/gauges;
collecting them performs no store reads and includes no context/queue identity.
Scheduler failures expose fixed stage/error-class counters, including group
decode and timer-transition failures that would otherwise leave an overdue hint
unchanged. Raw errors, payloads and SIP identities never become metric labels.
A group with no eligible effects allocates no dispatcher/read. Prepared bounded effect
metadata goes to separate transport workers after dropping aggregate/scheduler admission.
Overdue timers get bounded processing time, never renewed semantic deadlines; hard
terminal timers precede obsolete retransmissions. Close stops admission before awaiting
workers. Local dispatch concurrency memos contain no protocol authority.

Dispatch chaining requires exact forward effect progress. A repeated/uncertain
receiver result returns to durable recovery, never tight RPC/result recursion. The
physical guard releases once; known physical results chain native settlement, uncertain
results return to recovery. Only earliest unresolved lifecycle event dispatches. Its
original two-second envelope deadline anchors a 120-second recovery horizon; each RPC
gets at most two seconds capped to that horizon. At the exact cutoff no new start/RPC
occurs. Prefix order, receiver ID, application/lease guards remain; event recovery never
extends SIP/command/physical authority. Notifications behind predecessors retain this
same horizon.

Command waits register a bounded local notification before fresh effect dispatch.
Notifications are coalesced hints; results always reload validated Redis. Fresh commits
wait for notification or 40 ms before first read. Replay/nil projections read immediately,
then back off 10/20/40 ms. Each wait starts after read/decode, avoiding catch-up reads.
In the last 100 ms of original deadline, waits cap at 10 ms and remaining budget.
No Pub/Sub/background state copy exists; only latest bounded result survives. Cancel/
completion unregisters. An unstarted OpenUac allocated before sequence admission is
retired on budget expiry; expired queued commands never advance sequence or retire
an already active context.

Retirement requires native procedures/replay windows, timers and exact physical
settlement complete; terminal application bits/time alone are insufficient. Native
settlement is local SIP-state work.
True UNKNOWN stays charged. Terminal compaction can remove duplicate wire/snapshots
but preserves identity/hash/permit/outcome and causal written-ACK proof. Settled terminal
physical and receiver-acknowledged nonphysical effects alone free slots. Expired obsolete
non-native UNKNOWN in an ended context stops legacy recovery polling only after its
fixed windows and every live continuation expire; it is never relabeled WRITTEN or
NOT_WRITTEN. Timer projections change atomically, not by deleting rebuildable indexes.

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
produce another alternate final. Known absent dialogs get normal 481.

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
| External receipt reconciliation | 120 s; internal native identities/status remain replay authority |
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

Retained typed input/effect decoding and protobuf JSON shadows validate complete values
before receiver assignment. Encoders preserve canonical bytes/fingerprints; ordinary
field duplicates/case variants retain Go typed-JSON semantics, while duplicate protobuf
fields choose the last complete value with strict protobuf validation. Lexical JSON-tag
order permits direct ordinary encoding only for the two private concrete input/effect
structs without valid protobuf shadows (including empty messages). Frozen-byte tests
protect that compatibility, not a second state representation.

Decoded fields never alias caller buffers, other fields or old receiver values. Owned
RawMessage buffers transfer once; copy final value if spare capacity exceeds max(4,096,
length/4), avoiding large duplicate-field retention. Failed decode is atomic; later decode
cannot overwrite earlier returned buffers. Structured, opaque and base64 representations
retain their exact decoding rules. Dispatch may select by native identity metadata, but the physical/
control dispatcher validates the full retained body before I/O. No encoding optimization
adds a resident cache, skips capacity checks or changes authority.

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
lease and ten-second fixed post-expiry grace; ordinary owners retain the legacy
lease profile described above. The memory and Redis adapters atomically guard
attachment generation together with term/expiry for fresh application commits and
write reservations. Owner-context metadata is indexed under a bounded quota with
no second authoritative FSM.
