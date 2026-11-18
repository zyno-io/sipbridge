# Portable protocol values

This experimental package exposes native SIP transaction/dialog decisions as
serializable values. The driver supplies event time in milliseconds and all
random identifiers. Reducers clone their inputs and return values plus ordered
effects; they never open connections, start timers, read entropy, wait on
channels, call applications or access storage. `Message` is one opaque owned immutable value: exact wire, separately observed
transport/source/destination metadata, scalar identity/matching facts and selected
construction/projection material. It retains no mutable SIP request/response graph.
Copies share immutable evidence; reducer phase/profile/timer checks still run on
every transition.

Commit the resulting state and effects together before interpreting effects.
Timer notifications carry the exact saved name/generation and may be duplicated;
the saved absolute deadline is authoritative. Delivery results are later inputs.
An effect is an instruction, not evidence of a successful socket write. Storage
fences, effect IDs, exclusive dispatch, uncertainty, authorization and bounded
retention belong to the driver. A transaction key locates a candidate; full native
origin identity matching remains mandatory.

Four transaction families use explicit phases. Dialog values retain directional
identity, independent sequence spaces, fixed route sets, accepted target,
pending INVITE/offer/ACK procedures and absolute successful-final ACK deadlines.
Create a separate dialog value for each remote-tag fork. The transaction reducer
reports every accepted-transaction 2xx; the driver dispatches it to that child.
Transaction presentation effects must pass dialog admission before application
execution. A dialog `respond` effect is submitted to its server transaction, not
written directly; `start_transaction` creates a client transaction. Direct `send`
is reserved for stateless ACK or a transaction-owned wire effect.

The legacy goroutine/timer API remains unchanged in this intermediate extraction.
It must never run beside a portable FSM for the same transaction/dialog. Migrating
that convenience driver onto these reducers remains separate work. The package
does not itself provide a Redis runtime or qualify unsupported SIP extensions.

Deliberate corrections relative to the legacy storage/action implementation are
explicit non-INVITE E/F/K timers, a fixed TU ACK deadline and exponentially capped
TU retransmission interval, no sleeping ACK action, and Accepted-state retention
after transport failure. These are covered by deterministic event traces. Strict
and loose route construction use the complete persisted route set. PRACK/session
timers and proxy forking policy are outside this package's supported profile.

## Driver API

- `NewTransaction` and `StepTransaction` implement the four transaction families.
  `RequestCancel` records client INVITE cancellation, deferring its single
  `start_transaction` effect until a provisional response permits CANCEL.
  Emitting that CANCEL also commits a fixed `64*T1` cancellation watchdog for
  the original INVITE, following the UAC-core recommendation in
  [RFC 3261 section 9.1](https://www.rfc-editor.org/rfc/rfc3261#section-9.1).
  Repeated cancellation/provisional input cannot extend it; an INVITE final or
  termination stops it. CANCEL's own transaction remains independent.
  An elapsed B/F/H terminal deadline takes precedence over a delayed A/E/G
  retransmission notification, so scheduler recovery cannot emit fresh traffic
  after that procedure's original absolute timeout.
- `StepServerCancel` returns both INVITE and CANCEL transaction values. Commit
  them together; retain CANCEL's own non-INVITE replay state independently of
  INVITE retirement. Later CANCEL duplicates can use `StepTransaction` on the
  retained CANCEL value without the original INVITE being alive.
- `NewDialog` creates one tagged branch. `StepDialog` owns its local/remote
  sequence allocation, target refresh, fixed routes and successful-INVITE ACK
  procedure. `DialogCancel` marks the branch for cleanup; it does not create a
  second CANCEL alongside the client transaction's cancellation procedure.
- `BuildInitialRequest`, `BuildResponseWithHeaders` and
  `BuildDialogRequestWithHeaders` accept explicit committed identities and
  application content. Native identity/routing/framing headers cannot be
  overridden by the arbitrary header list. Initial From/To may be supplied in
  structured fields or once in the header list; a supplied From tag must match
  the reserved native tag. Dialog response Contact is an explicit URI or name-addr
  argument; bare URI parameters stay inside the URI rather than becoming header
  parameters.
  Successful REGISTER response Contacts are registration-policy content: validated
  ordered Contact headers (including expires parameters) come from the application
  header list, and the builder never adds its own dialog Contact to REGISTER.
- `BuildDigestRetry` requires the original complete URI, explicit cnonce/count
  and new branch. It is a transient credential-executor helper, not a durable
  reducer input. Password is excluded from JSON encoding and is never retained
  in transaction/dialog state.

`Dialog.Retiring` is monotonic. A late successful re-INVITE or its ACK cannot
reopen a dialog after local termination. `LocalBye` retains the one allocated
request; its final response ends the dialog even on failure. Native completion
and wire-result certainty remain separate from application acceptance.

Validate hydrated values before interpreting effects. Unknown versions, phases,
transaction keys or transport profiles fail rather than calling initialization.
The driver supplies bounded storage/concurrency and dispatches transaction
effects to the correct dialog branch. This package has no per-dialog actor,
timer goroutine or persistence implementation.

## Immutable message boundary

`ParseMessage` owns and parses raw input once. `FreezeMessage` owns facts from
an already parsed **same observed frame** and its exact bytes; callers must not
pair a parsed object with independently supplied wire. Native identity validation
is distinct from observed-ingress branch admission. A failed native freeze still
returns its owned value and original native error; local ingress policy must keep
its existing order before consuming native facts. Key accessors keep the existing
client/server and ACK/CANCEL normalization rules and separate key errors.

Matching and the same transaction/dialog reducers use frozen facts. Structured
response, ACK, CANCEL, dialog and digest builders construct privately and freeze
their final value directly; they do not render merely to parse the result. A
selected native final keeps its exact replay/conflict identity.
`WithObservedResponseFlow` creates a separate immutable physical variant with
observed Via received/rport, including replacement TLS flows; unchanged finalized
routing reuses the value. Physical dispatch writes that exact finalized evidence.

`ObservedFacts` and its observed header/body/Via/key projection companions
preserve the separate observation boundary: mandatory identity and the existing
request RFC3261 branch check, without portable transport or CSeq restrictions.
They share the same frozen facts and preserve the request-line method separately
from CSeq method. Parser errors remain parser errors; the default parser still
rejects an out-of-range CSeq. Native `Facts`, keys and every reducer always use
the strict validation result. Observed reads never grant native execution proof.

`Facts` exposes scalars. Header/body/Via/wire projections return owned copies;
wire length/hash/equality and append-to-owned-buffer avoid copying reads.
`RetainedBytes` includes private owned data and never prepares an unused restored
message; immutable sharing may be conservatively charged by each retaining record.
`Equal` compares serialized value identity, independently of lazy preparation.

`RestoreMessage` explicitly maps the same four serialized fields without parsing
unused wire. Its private lazy holder validates at most once when consumed in that
loaded operation; it is not a global cache, serialized pointer, or second FSM.
JSON retains the prior wire/transport/source/destination shape, including nil and
empty wire. Storage adapters must explicitly map that shape, not reflect private
fields or eagerly validate every message in a restored group.

The immutable tests cover ownership, concurrent lazy consumption, exact selected
versus flow emission identity, raw admission errors, native parse-free paths,
digest derivation and a 201-case exact wire/state golden captured from the
pre-change consolidated dependency. Additional correction oracles cover ordered
multi-Contact bindings, accepted duplicate singleton selection, structured method
normalization, padded/comma routes, observed/native admission separation, and
source-empty native response routing fallback. Repeated construction fields own
ordered typed values only when duplicated; their additional data is accounted.
Existing protocol regression traces remain
the semantic oracle; this representation does not change replay lifetimes,
UDP schedules, post-OK/fork/ACK duties, or required NOTIFY behavior.
