---
description: Scope of the session-only, native-wire and Redis relay fixture programs, and what passing them does and does not show.
---

# Fixtures and evidence scope

This is the normative fixture supplement to [ARCHITECTURE.md](../ARCHITECTURE.md).
The service selects resident Memory or the selected-record binary Redis runtime. The programs and historical
test paths below isolate particular contracts; passing them is not full controller,
media, handset, load or production qualification. See [Verification](verification.md)
for commands and [Runtime](runtime.md) for the actual service contract.

## Session-only executable

`cmd/sipbridge-control` exposes private JSON/mTLS WebSocket sessions without a public
SIP listener. It implements hello/ack, transport supersession, status, heartbeats,
strict JSON decoding and semantic validation and TLS reload. It negotiates no SIP capabilities,
refuses SIP readiness and has no attached client socket/native context. It exercises
real Go/TypeScript transport interoperability, not a working call adapter.

Shared transport code validates certificate chain and exact workload URI SAN before
upgrade. It bounds admission including pending hello; higher accepted attempt fences
older connection immediately but replacement peer becomes discoverable/ready only
after successful HelloAck write and lease acquisition. Failed handshake never publishes
a usable peer. Different active epoch cannot supersede the current session. This
transport fencing is distinct from current service SIP-owner lease authority.

## Binary native inspection

`sipbridge-state-inspect` defaults to one bounded raw selected-record frame on
stdin (`--input` selects a file). It emits redacted record classes, phases and
counts through the canonical native codec; it never decodes a checkpoint by
accident. `--historical-checkpoint` explicitly selects the older offline reader.

For disposable qualification, `--fixture-native-redis` reads the configured
binary root without provisioning, acquiring/renewing leases, settling outputs or
writing any Redis state. It uses `SIPBRIDGE_REDIS_*` connection settings and
`SIPBRIDGE_STATE_PREFIX`, `NAMESPACE`, `GENERATION`, `IDENTITY_KEY` (the same root
construction as the service). Select `--context-id` plus `--incarnation`, or
`--call-id` with optional exact `--local-tag` and `--remote-tag`. Exact tags first
use the canonical dialog selector. Call-ID-only selection is an explicitly
fixture-only root-scoped administrative scan, limited to 4,096 candidate keys
and 32 matching contexts. It never becomes a service lookup path. An optional
`--application-instance` plus `--application-epoch` observes that exact process's
current application lease without acquiring it; it can be queried alone. An
expired term is reported only after a read-only, same-primary/profile observation
of its actual owner expiry and Redis TIME; a generic timeout/ErrExpired is never
evidence of lease expiry. This reads only the current process term, not history.

This inspector observes shared Redis records only. The composed runtime's inbound
REGISTER and phone subscription contexts live solely on their accepting process.
A reserved local context-ID query is refused; use the existing authenticated
public Inspect on that exact process/epoch. An empty offline Call-ID selection
proves only that no matching shared Redis context was selected, not local or
global context absence. The public schema is unchanged and no local private-state
projection or inspector endpoint is added.

The `sipbridge.fixture.native.v1` JSON projection contains provenance, Redis
checked time/current primary role, optional application lease, and actual
contexts. Each context reports its native identity, revision/assignment,
redacted canonical snapshot, flow, actual procedures/dialogs/outputs/events and
referenced results. Follow only the context's actual bounded associations:
at most 128 procedures, 16 dialogs, 32 outputs per procedure and three rooted
events per procedure; read at most 128 selected fields per batch and 32 MiB in
one projection. Recheck the same context revision and assignment after packet
proof reads; changed observations fail with conflict rather than mixing views.
Proof absence stays absence and does not assert WRITTEN or consumption.

Procedure evidence includes original active native deadlines, cancelled and
cancel-sent bits, cleanup state, exact message identity/hash proofs and actual
result/output/event references. Dialog evidence includes tags, sequence/offer
state and hashed target/routes. Output evidence contains actual proof presence,
state, settlement and acknowledgement when present. Lifecycle evidence contains
its actual root/owners, kind and operation identity. No former group/effects
shape is reconstructed. Times are Redis milliseconds; uint64 values are decimal
strings. Independent lease/proof observations do not create an atomic global
snapshot or authorize a state change.

The projection binds the compiled native descriptor, canonical native/storage/
control schema receipt, binary compatibility profile and build revision. Raw
SIP, SDP, URI/route text, credentials, permit/policy bytes and recovery handles
are excluded. Snapshot identifiers and flow references are identifying fixture
evidence; keep this mode private to synthetic environments. The normal offline
mode reports only classes/counts. Neither mode is an execution or import API.

## Historical offline completion projection

`sipbridge-state-inspect --historical-checkpoint --fixture-completion-projection` accepts one bounded raw
binary `data` value and emits `sipbridge.fixture.completion.v1` JSON for disposable
service qualification. It uses this repository's strict payload/protocol/effect
adapters, with no Redis connection, lease observation, state mutation or clock
sampling. Consumers copy the executable and storage receipt from the same pinned
image and verify the complete public/private receipts; they do not implement a
private storage decoder. The JSON is an inspection projection, never import data.

This explicit fixture mode exposes logical/context/process identities, native
lease and lifecycle timestamps, exact opaque completion scopes, flow references,
transaction/dialog/timer state and pending event kinds/operation identities.
Message proofs contain method/status, Call-ID/CSeq/tags and SHA-256 of the complete
original wire and From/To header values. URI targets/routes and registration tickets
are represented by hashes. Raw SIP, headers, bodies, SDP, credentials, effect permits,
command policies and recovery handles are excluded. Snapshot SDP is removed before
serialization; this historical inspector exposes only redacted counts/classes unless explicitly requested.
Use fixture mode only for isolated synthetic records and keep identifying output
private. Neither projection is authority to send, settle, adopt or retire anything.

## Independent stdio/native-wire executable

`cmd/sipbridge-fixture` reads bounded JSON from its parent; it is not service mode.
Only the runner sets `SIPBRIDGE_FIXTURE_STDIO=1`. Two independent test ingress
processes terminate verified TLS, and the owner uses a virtual TLS stream plus
first-read barrier to feed already decrypted SIP into sipgo's transaction layer.
There is no second TLS handshake or extra SIP Via/hop. Unique private source keys
separate logical flows in the native pool; observed client source remains explicit.
Private keys cannot escape into public SIP/source fields. This fixture has its own
resident protocol objects; those objects are not used by the Redis service.

Fixture device tokens stand in for controller authorization and are never production
authentication. Fresh REGISTER commits current binding; exact replay cannot restore
an older one. A bounded one-time marker covers reliable transaction retirement.
Closure invalidates only exact epoch/connection; unrelated devices cannot inject
retained contexts. Native cancellation also validates original request identity
after branch lookup, independently of account authorization.

The fixture supports bounded message-summary/dialog subscription packages with
native Dialog remote sequence validation and native SUBSCRIBE/NOTIFY transactions.
It retains Event/id, fixed route headers, accepted target/source/expiry and independent
local NOTIFY/remote request sequence. Exact replay is response-only; rejected refresh
preserves accepted fields. Expiry/unsubscribe fences active work before one final
NOTIFY, and completion/failure or 481 removes only that subscription. A scheduler
checks current lease under lock, so an old timer cannot end a renewed subscription;
subsecond live lease advertises rounded-up positive expiry.

| Fixture resource | Bound |
| --- | --- |
| IPC / SIP record | 64 KiB each |
| Output / delivery acknowledgement capacity | 128 each |
| Virtual ingress capacity | 32 |
| Owner virtual-flow retention | 64 |
| Delivery deadline | 2 seconds |
| Accepted subscription expiry | 300 seconds |
| Retained subscriptions | 128 |

The suite exercises real TLS on two ingress processes, cross-ingress CANCEL,
native final/cached/timer output, call signaling, subscription refresh/expiry/end,
identity isolation, write errors and uncertain delivery. Artifacts carry binary/source
hashes and traces. State is finite and disposable; owner death loses its contexts.
It does not establish Redis execution, full service control recovery, client UDP/TCP,
process replacement, production subscription policy/content or strict/proxy/forked
subscription routing. Keep this native-wire gate separate from service socket tests.

## Historical Redis relay fixtures

Historical schema 1 / reducer 7 / timers 1 values and broad reducer/rolling assertions
remain in test fixtures. Their constructor and claimed/CAS selection are test-only;
`internal/execution/fixture` provides the claimed driver, which the service does not
link. Shared store claimed methods are fixture-specific support, not selectable
service execution. The service constructor accepts only 1/1/1 CAS, and unexpected
retained Pending computations fail closed without Resume or overwrite.

These tests preserve historical trusted per-send authorization, controller-relayed physical
writes/inspection/settlement and directory-owner probes where an assertion specifically
needs them. These fixtures provide no current service API. Migrating their
assertions to native authority changes their premise;
use actual 1/1 process/socket tests for current production-path evidence.

The process fixture's controller RPC wait must end when its session closes,
matching the real control peer. Losing a session produces an unavailable result;
it never proves that an interrupted physical write did not happen. Tests that
specifically require a completed setup before killing a worker must wait for its
recorded write result, rather than inferring completion from a socket read.
Uncertain-write cases remain separate and must preserve UNKNOWN.

The claimed fixture driver loads one value and Redis-derived time, reduces and commits
before dispatch. Pending input recovery finishes that exact input before another;
claims never span external RPC/socket I/O, and uncertain commits inspect exact
operation identity. This describes only the fixture driver, never CAS crash recovery.

The removed resident `internal/engine.Engine` is not another service profile. Shared
observation/build helpers remain where used; preserved behavior is checked in native
CAS or independent fixture tests. No alternate resident service fallback is retained.

## Coverage retained when retiring the resident Engine

The service-path migration tests are in
[state_engine_resident_migration_test.go](../internal/engine/state_engine_resident_migration_test.go).
They use actual Redis CAS with native 1/1 leases and command admission. Historical
1/7 assertions below exercise shared reducers under their stated fixture authority;
they are explicitly separate evidence, not a claim that all old tests were ported.

| Removed resident assertion family | Retained check / scope |
| --- | --- |
| Full identity, directional tags, transport/binding admission | Native `TestStateMigratedTransportClassAndDirectionalAdmission`; refusal leaves route/presentation untouched. |
| Replacement publication before presentation | Native `TestStateMigratedReplacementPublicationOrdersApplicationWork`, UDP/TCP/TLS success/refusal; route/presentation waits for exact receipt, denial cannot revive through late success. Historical `TestStateEngineReconnectPublicationDefersPresentation` retains additional shared-reducer coverage. |
| Replacement-flow CANCEL identity | Native `TestStateMigratedPendingCancelRequiresExactIdentity`; exact native origin required, no account/route publication. |
| Same-flow initial replay | Native `TestStateMigratedSameFlowReplayToleratesResolvedPrincipal`; INVITE/REGISTER/SUBSCRIBE replay tolerates empty-to-resolved principal without new business admission. |
| Initial subscription bootstrap and same-flow renewal | Native `TestStateMigratedSameFlowSubscriptionPublication`, UDP/TCP/TLS success/refusal; bootstrap has no adoption, renewal commits accepted fields only after receipt. |
| Subscription grace and terminal NOTIFY | Native `TestStateMigratedSubscriptionGraceAndTerminalNotify` and `TestStateMigratedUnsubscribeAdmitsOnlyTerminalNotify`; five seconds, rejected active 481, terminal command remains usable. |
| Processor adoption before challenge/renewal | Native `TestStateMigratedSubscriptionProcessorChallengesBeforePublication` and `TestNativeOwnerSubscriptionAdoptionPinsOnlyApprovedSuccessor`; approved native successor only. Historical `TestStateOwnerSubscriptionControllerProofDoesNotRefreshLeaseOrFlow` retains detailed no-mutation checks. |
| Digest retry live transaction identity | Native `TestStateMigratedDigestRetryUsesLiveResponseIdentity`, 401/407; fixed retry identities and challenge tag distinct from final/fork dialog identity. Native process `TestStateNativeProcessCredentialLookupSurvivesExecutorDeath` checks immutable digest/ACK wire across worker death. |
| Response continuity without ordinary-route adoption | Native digest live-response checks and historical `TestStateResponseOnlyTLSReconnectACKUsesBWithoutChangingContextFlow`; transaction completion flow remains distinct. |
| ACK and exact cached-final replay | Native `TestNativeCompletionReplaysOwnerFencedAcceptedReinviteFinal` and process `TestStateNativeProcessRejectedInviteACKRequiresExactFlowAndFinalIdentity`; historical `TestStateCompletionRelayACKSeparatesOriginalOwnerPublicationFromCleanup` retains no-republication assertion. Resident ack-map cardinality is obsolete. |
| One remote BYE final/terminal event | Native `TestStateMigratedRemoteByeHasOneFinalAndOneEndAcrossWriteOutcomes`; WRITTEN/UNKNOWN cannot create an alternate final/event. |
| Retiring-dialog re-INVITE refusal | Native `TestStateRetiringDialogLookupRefusesResumeBeforeBusinessRouting` and `TestStateRetiredDialogRejectsFreshReinviteBeforeAdmission`. |
| Established REFER Contact | Historical `TestStateReferResponseUsesExactRetainedDialogContact` and `TestStateReferContactSurvivesCrossExecutorUACTakeover`; exact retained directional native dialog Contact. |
| Resident reaper/refcount/capacity internals | Removed with those data structures. Native `TestNativeAdmissionReservationIsSingleUseAndRetirementStable`, `TestNativeAdmissionReservationExpiresWithoutRevival`, `TestNativeAdmissionQuotaAndPendingCreator` and `TestNativeRetirementPreservesPhysicalUnknown` check actual reservation/retention contracts instead. |
| UA accept cannot overwrite logical pool | Obsolete resident-only mechanism removed. Service has no UA/pool; native advertisement tests and `TestStateMigratedSIPSNetworkCannotDowngrade` cover its actual public transport boundary. |

A resident test that rejected every second successful initial To-tag is superseded
by the native winner/loser fork contract; preserving that old assertion would contradict
current branch ACK/cleanup behavior. Further 1/7 migration must preserve or explicitly
retire historical grant/relay/settlement and pending-claim recovery premises before
removing their remaining shared reducer readers.

## Pinned dependency qualification

`third_party/sipgo` is the exact consolidated source recorded in
`dependencies/sipgo.json`, including license and separate patch provenance. Checksum
manifest/tree digest reject silent changes, additions and removals. Generate native
TLS test certificates only in the disposable copy made by `scripts/test-native.sh`.
That copy also corrects the upstream client-builder test's initial CSeq range
assertion to match its existing 15-bit random allocation (0 through 32767).
The former `> 1` assertion randomly rejected valid generated values. The checked-in
dependency and its runtime behavior remain unchanged; the fixture adjustment
fails closed if the pinned assertion changes.
Native transaction matching, timer, digest-URI and dialog semantics remain in those
pinned reducers; adapter cleanup does not authorize editing the snapshot.

The PR drafts in [upstream](upstream) describe separately reviewable proposals.
Fixture qualification and upstream/native test coverage do not establish complete
platform conformance or a deployable production rollout.
