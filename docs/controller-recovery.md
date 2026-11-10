# Controller recovery and authentication

This document owns the v1 controller attachment contract and its authentication
and state-adapter boundaries.

## Public contract

sipbridge owns its schema, generated bindings, validators and wire examples in
this repository. Protocol v1 uses `hello.controller`, `routeResult.controller`,
`--initial-controller`, `/v1/control` and `sipbridge.control.json.v1`. The public
project and source links use https://github.com/zyno-io/sipbridge. The generated
contract receipt identifies the schema used by each build.

## Authentication and transport

Authentication runs before WebSocket upgrade or process-claim admission. The
default is mutual TLS with exact URI identity authorization. Explicit bearer
authentication uses a token file and stable configured principal. TLS transport
still verifies the server; explicit plain WebSocket control is limited to local
standalone use. Authentication is always required. A failed mTLS handshake never
falls back to a token or an unauthenticated session.

The authenticated principal is independent of the process ID asserted in hello.
Certificate renewal or bearer-token rotation can retain the principal. Controller
recovery needs both the same principal and the owner recovery token. Tokens are
sensitive and excluded from diagnostics, snapshots, catalogs and normal logs.
Public SIP TLS and its admitted-flow continuity proofs are separate from control
authentication and are unchanged by choosing bearer authentication.

## Logical ownership and timing

`ApplicationLease.application` identifies the original owner lineage.
`ApplicationLease.holder` identifies the actual currently attached process from
`hello.controller`; it never pretends a restarted process has the old epoch.
`attachmentGeneration` fences application mutations and unstarted application
writes against every earlier attachment. The selected state adapter alone owns
this metadata; the engine keeps no resident authoritative SIP state.

Negotiating `sip.controller-reattach.v1` creates a recoverable owner with a
five-second lease and a ten-second reattach grace. Normal control exchanges renew
the live lease. The orphan deadline is the old lease expiry plus the frozen grace,
not the time a worker notices expiry. The default loss/recovery budget is therefore
15 seconds from the last renewal. Recovery attempts, reads and failed attachment
never extend that original deadline.
Non-recoverable owners retain their existing lease and cleanup behavior.

The first successful hello returns a recovery token. A replacement process sends
`hello.recovery` with the saved application, term, token, attachment generation and
a new attempt ID. A different process can claim only an expired owner before its
orphan deadline. One atomic adapter operation chooses a single winner, increments
the generation and rotates the token. Retrying the exact successful attachment
with the same attempt ID returns the same receipt after a lost reply. A different
attempt cannot reuse the consumed proof. Authenticated sessions for the current
holder may reconnect to other workers using the current token without rotating
the generation again. If the same holder reconnects after expiry, it must still
finish restoration before the original deadline; its generation and token remain
unchanged.

A new or recovered session starts non-admitting. It may enumerate only its own
contexts and inspect them while restoring application state. A replacement keeps
the original orphan deadline in `reattachUntilMs` until restoration completes.
Heartbeats and another replacement cannot restart that deadline. Only validated
ready status with both `retainedReady` and `bindingsReady` clears the pending
restoration phase and permits fresh business commands. New application work checks the exact term,
generation, holder and expiry; old-holder frames and queued writes cannot acquire
fresh authority. Already-started physical writes retain their actual or UNKNOWN
outcomes and may still settle. Recovery never turns uncertainty into a retry.

## Recovery and SIP work

Replacement-process recovery initially covers established INVITE dialogs. A
reconnecting instance with the same process epoch keeps its existing in-memory
application state and may resume other still-valid work under its original native
deadlines; it does not transfer ownership. Native transaction,
ACK/CANCEL, retransmission and finite cleanup duties keep their existing deadlines.
An application-required request cannot wait indefinitely for a missing controller.
Pending setup and terminal calls are inspected and cleaned up rather than revived.
SIP state cannot reconstruct application business state: a recovering controller
must retain its recovery handle and restore its own call metadata.

`controllerContexts` is a bounded owner-scoped metadata listing, not a global SIP
inventory or a second FSM. Pages contain at most 64 references. A derivative index
is updated atomically with group commits and retirement, has a 4,096-reference
owner quota, and never scans Redis keys. Inspection supplies snapshots and command
outcomes. Pagination can repeat references while native work changes; clients
deduplicate and reconcile each context instead of assuming an event replay.

When the original recovery deadline expires, ownership is permanently fenced and
existing native termination runs. Expired, sealed, retired or missing state cannot
be recreated by an attachment request. Memory recovery requires the same sipbridge
process; Redis requires the same compatible retained state authority. Neither a
SIP-core restart that loses sockets nor a media-process restart is repaired by
controller reattachment.

## Controller responsibilities

The controller saves its recovery handle, checkpoints its application state and
reconciles retained SIP contexts before becoming ready. It also restores any
application-managed resources needed to continue a call. Recovery is optional
and available through the public v1 control contract.
