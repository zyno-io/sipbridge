# sipbridge architecture

Status: experimental UDP/TCP/TLS service with an application-control protocol.
This document describes its components, state and routing ownership and operating invariants.
Local native, process/socket and backend E2E checks have different scopes, listed
in [Verification](docs/verification.md); they do not cover full platform
conformance, load qualification or safe SIP-core replacement.

## Reading map and contract ownership

This document owns the component boundaries and invariants below. Its normative
supplements own the details; a behavior change updates the owning document first.

| Document | Owns |
| --- | --- |
| [Controller recovery](docs/controller-recovery.md) | Recoverable logical owners, attachment fencing, authentication and bounded controller restart windows. |
| [Operation flows](docs/operations/index.md) | Source-to-operation traversal, flowcharts and per-operation work accounting. |
| [Runtime](docs/runtime.md) | State transitions, leases, completion, physical evidence, exact service bounds and reclamation. |
| [Protocol](PROTOCOL.md) | Control wire behavior, capability compatibility, identity, business admission and proposed conformance requirements. |
| [Fixtures](docs/fixtures.md) | Test-program behavior, limits and evidence scope. |
| [Verification](docs/verification.md) | Reproducible checks and explicitly dated historical evidence. |
| [README](README.md) | Build, publication and qualification entry points. |

`proto/sipbridge-control.proto` owns the wire declarations and shared JSON
fixtures in this repository. The pinned local generator produces Go bindings,
TypeScript bindings and strict JSON validation under a versioned receipt;
`dependencies/sipbridge-control.proto` is a distribution copy. Schema changes and
generated consumers stay in step; fixture JSON is never the service API. Current
service behavior is distinguished from proposed protocol and rollout requirements
throughout these documents.

## Components and authority

| Component | Authority | Principal implementation |
| --- | --- | --- |
| Physical ingress | UDP peers, TCP/TLS sockets, framing, observed source and exact socket-write evidence | `internal/engine/ingress.go`, `outbound_stream.go`, `state_writer.go` |
| Native execution | SIP transaction/dialog/timer values and finite committed effects | `internal/engine/state_engine*.go`, pinned `third_party/sipgo` reducers |
| Execution driver | Bounded read, pure reduction and store CAS; no engine-owned authoritative FSM | `internal/execution` |
| SIP state store | Logical group/incarnation, revisions, owner terms, committed effects and finite retention | `internal/state` |
| Control session | Configured authentication, transport fencing, bounded RPC, heartbeat and reload | `internal/control` |
| Application controller | Call behavior, authorization, registration policy, subscription content and trusted routing directory | Your application |
| Account backend | Account-scoped registration/business mutation, where the application uses one | Application-owned service |

`cmd/sipbridge` runs one schema 1 / reducer 1 / timers 1 CAS execution path.
`SIPBRIDGE_STATE_BACKEND` selects `memory` (the default) or explicit `redis` at
startup. The selected store is the sole protocol authority; the engine retains no
authoritative copy. Memory is process-local standalone state, lost on restart;
Redis supports compatible workers sharing retained state. There is no automatic
backend fallback, live migration or alternate execution mode. The separate
`cmd/sipbridge-control` and `cmd/sipbridge-fixture` are test programs described in
[Fixtures](docs/fixtures.md).

SIP transaction matching, dialog sequence rules, retransmissions and timers belong
to the pinned sipgo reducers. Subscription protocol metadata and finite effect
coordination belong to the adapter; BLF/MWI content and business decisions remain
in the controller. The SIP process has no business database, trusted-directory Redis,
account backend or Kubernetes credentials and requires no NET_ADMIN capability.

Privileged kernel enforcement stays separate. Registry, firewall
policy/scoring, PKI issuance, CNI/routing and collectors remain external. A helper
container sharing a Pod is still coupled to that Pod's rollout.

## Four distinct identities

A physical flow belongs to one ingress process epoch and identifies an exact
socket or UDP listener/peer tuple. Ingress alone creates observed source and
transport evidence. Via/Contact text cannot supply it. Stream connection IDs
are never reused within a process epoch.

A logical SIP context belongs to one compatible SIP-state authority, Call-ID
group, incarnation and non-reused context ID. A worker attached to that authority can execute its
next store transition; the replying executor is metadata, not logical ownership.
Call-ID alone never merges directional dialogs, fork branches or subscriptions.

Without recovery, an application call remains tied to its original controller
process and lease. The optional `sip.controller-reattach.v1` capability separates
that stable application lineage from the currently attached process: a new holder
proves the same principal and recovery token, then reconciles retained contexts
before sending ready status. Its attachment generation fences the old holder.
This does not reconstruct application memory. Subscription processor handoff still
requires its separate directory approval and accepted-flow admission. The exact
attachment rules are in [Controller recovery](docs/controller-recovery.md).

The trusted business directory and SIP-state Redis have separate authority. A
business directory epoch change refuses the affected operation; it cannot rebuild
old business authority. Native completion and retirement use retained SIP-state
authority independently of controller business policy.

## Request and write lifecycle

```mermaid
sequenceDiagram
    participant C as SIP peer
    participant I as Physical ingress
    participant N as controller
    participant W as Native worker
    participant R as SIP state store
    C->>I: SIP bytes on an observed flow
    I->>R: ReadUpdate and select retained completion
    alt Proven retained native completion
        I->>R: CAS exact native transition
    else Business admission required
        I->>N: Original bytes and observed flow
        N->>W: Authorized ingest or command
        W->>R: Read, pure reduce, CAS state and effects
    end
    R-->>I: Discover exact committed physical effect
    I->>R: Reserve exact one-attempt write
    I->>C: Retained bytes on exact socket
    I->>R: Complete exact physical outcome
```

The diagram shows roles, not extra services or permission to send arbitrary
bytes. Control connections are controller-initiated. Native dispatch uses committed
SIP-state effects; there is no trusted per-send grant or settlement round trip.

Ingress uses sipgo framing and parsing, retaining original bytes and actual
transport. Before business routing it can prove an exact retained native
completion without creating a group/context. REGISTER, SUBSCRIBE and NOTIFY
requests use normal policy admission when enabled. The process-local
[method policy](docs/runtime.md#process-local-method-policy) can reject disabled
REGISTER/SUBSCRIBE locally before any state or control access. Selection and reduction use the same authoritative update read; CAS validates
incarnation, revision, lease and deadline, and conflicts repeat selection.

Business routing selects an eligible controller, with bounded authenticated redirects
before acceptance. Existing calls prefer their connected original controller. A tagged
live-context lookup can use one validated read without a new revision; new inbound
contexts require a committed single-use reservation. Neither lookup nor an
internal redirect grants ownership, send permission or account authentication.

After native admission commits, request presentation is a one-shot controller
RPC on the worker handling that request. The bounded presentation DTO exists only
in the definite fresh commit result; it is not a retained effect, queue entry or
recovery timer. The worker calls the controller directly after releasing state
gates. A successful presentation acknowledgement requires no state mutation.
Controller-selected SIP responses still use Respond and the committed native
write path. Replay, uncertain commit inspection and a replacement worker cannot
recreate the presentation. Losing this in-flight exchange during a process failure
can fail the request; it does not discard already-established dialog state.
Deferred flow publication or owner proof can produce a one-shot presentation from
their completing transition. Lifecycle events and physical delivery obligations
retain their independently bounded recovery rules.

The native worker loads one bounded Call-ID group, reduces in Go and atomically
commits its next value and finite effects. It retains no authoritative snapshot
between operations or across external I/O. Store CAS, incarnation, revision,
comparison counter, lease and original-deadline guards supply serialization.
A retained unexpected pending computation fails closed; it is never resumed.

Every normal output, including native ACK/CANCEL/cached finals and timer output,
comes from a committed effect. Only the exact physical ingress can reserve and
write that effect. A known complete socket write is WRITTEN; proven no start is
NOT_WRITTEN; partial writes and missing evidence remain UNKNOWN. A successful
write proves neither peer receipt nor application success. Store atomicity does
not include sockets, controller or account backend operations.

UNKNOWN never authorizes an automatic physical retry or older-flow fallback.
Native retransmissions are distinct finite SIP obligations, with their original
transaction identity and deadlines. ACK-written proof precedes an automatic UAC
cleanup BYE. A best-effort stateless 503 during proven datastore unavailability is
the narrowly bounded exception to committed output; its race limitations are
specified in [Runtime](docs/runtime.md#datastore-failure-and-refusal-arbitration).
ACK and responses never receive such a reply. Static method denials are the
separate local-output exception documented above; they cannot admit business work.

Configured [initial digest challenges](PROTOCOL.md#local-initial-digest-challenges)
also run before state/control lookup. They use a bounded trusted nonce policy,
not account credentials, and never mark a request authenticated. The controller and its account backend
retain verification, replay admission and account authority. Their local 401 and
policy-unavailable response cannot create an application call or retained SIP context.

## Transport and business admission

UDP, TCP and TLS preserve their real transport class in FlowRef, native state,
Via/Contact advertisement and native timer selection. A retained context cannot
change class. Replacement plaintext flows need exact current-flow or matching
binding evidence; a TLS dialog tuple can permit only the explicitly negotiated,
fully validated context-continuity attempt. It never authenticates an account.
Transaction responses use their exact admitted request flow, independently of an
ordinary future delivery route.

The service uses sipgo portable transaction/dialog values and pure reducers,
returning messages as committed effects. It has no resident UA or native transport
pool that can dial around the physical-effect boundary. Builders use explicit
public advertisement and actual observed source/transport. Private implementation
addresses never escape into advertised headers or source evidence.

Controller commands carry bounded command-time policy admission. Respond, OpenUac and
DialogRequest require the exact owner term, operation, sequence, directory epoch,
semantic expiry and selected delivery evidence. Public Cancel retains its
exact-owner/live-term cleanup exemption. New business commands cannot renew an
earlier setup selection. Native completion uses its independent retained scope.

The controller and its account backend own registration mutation and ordered binding publication. A delayed
or replayed older result cannot restore a previous binding. An unregister removes
new-call eligibility only after explicit committed removal and unchanged-binding
CAS; existing-context transport authority has its own bounded lease.

Subscriptions retain accepted Contact, flow, Event identity, expiry and independent
NOTIFY/request sequence. A rejected refresh preserves the accepted delivery fields
and local NOTIFY sequence; its admitted remote request CSeq remains consumed.
Controller adoption changes processing authority first; only a policy-accepted refresh and
validated publication receipt can change accepted delivery. INVITE ownership
never changes through this path.

Unregistered outbound destinations require a controller-selected URI, optional proxy,
transport and opaque credential reference. All sends still use committed effects.
Digest retries use sipgo's complete serialized Request-URI and fixed retained
inputs. Clear credentials are ephemeral and never stored in transition inputs,
artifacts or ordinary logs. TLS verifies the peer name/chain; SIPS cannot downgrade.

## Recovery and process lifecycle

The Redis adapter opens from configured primary/Sentinel endpoints, namespace, credentials,
TLS trust and service identity key. An empty namespace is available empty state;
there is no provisioning command, namespace marker inventory or manual recovery
generation. Old references, results and timers cannot recreate erased state.
Fresh registrations and calls can establish new records immediately.

Redis is asynchronously replicated. Failover reconnects to the writable primary;
compatible retained records can resume, while missing/inconsistent records can lose
a call or require registration again. A structurally valid rollback may be
undetectable. Recovery provides no rollback or exactly-once guarantee and uses no
resident second copy to reconstruct lost state.

Authenticated control exchanges renew the controller process's SIP-state lease.
Datastore errors do not imply controller death or close healthy control/public sockets.
A non-recoverable connected session can resume its retained term after an outage
until sealing or successor evidence wins. Recoverable owners instead have a fixed
reattach deadline derived from their lease expiry; pending restoration and retries
cannot extend it. Once that deadline expires, owner loss permanently fences
business commands before bounded native termination. No relay arrival clears it.

`/sip-servingz` reports listeners, public TLS and process lifecycle.
`/sip-readyz` additionally requires datastore and signaling availability. Liveness
measures an internally broken process; loss of Redis, controller, registry or policy
must not trigger SIP-process restart. Readiness fences new work while retained
native duties and healthy sockets remain independent.

Public/private certificate, key and trust directories reload as validated atomic
contexts for new handshakes. Invalid updates retain the previous context; existing
SIP/control sessions remain open. Deploy directly projected directories without
subPath or init-time copies.

The image is independently digest-pinned and promoted. Ordinary controller,
account-service, policy, discovery and certificate changes must not alter the SIP
Pod template. Publication
never deploys a workload. Prefer explicit promotion and OnDelete updates when a
production chart is designed; an actual SIP-core restart still loses its sockets.
Fence admission, preserve retained paths, drain native/application work, withdraw
the backend and restart deliberately. Zero-disconnect core replacement remains
unprovided.

## Qualification and release boundary

The public project repository is [GitHub](https://github.com/zyno-io/sipbridge),
and the documentation is published with GitHub Pages. Publication verifies the
schema and dependency pins before building. The consolidated
`third_party/sipgo` snapshot is
immutable and verified against `dependencies/sipgo.json` and its checksum manifest.
Native fixture generation runs in a disposable copy. A dependency change requires
native branch tests and wire qualification.

CI requires source/schema pins, formatting/vet, Redis-enabled race tests, real
Redis/Sentinel restart tests, pinned-native tests and the independent TLS fixture
before image publication. Consumers verify the image's contract receipt and choose
its digest separately from backend releases. See [README](README.md) for commands
and [Verification](docs/verification.md) for evidence limits.

Local synthetic control/Redis/socket tests do not prove real account policy,
media/CDR, carrier/handset interoperability, load, version-skew acceptance or safe
production drain. Application integration tests and recorded deployment
experiments provide separate evidence for those boundaries.
