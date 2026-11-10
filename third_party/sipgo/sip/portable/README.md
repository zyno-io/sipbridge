# Portable protocol values

This experimental package exposes native SIP transaction/dialog decisions as
serializable values. The driver supplies event time in milliseconds and all
random identifiers. Reducers clone their inputs and return values plus ordered
effects; they never open connections, start timers, read entropy, wait on
channels, call applications or access storage. `Message` contains immutable wire
bytes and separately observed transport/source/destination metadata.

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
