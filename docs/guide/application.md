# Building a Controller

sipbridge runs SIP; your **controller** decides what should happen. This page
explains what a controller has to do, in the order you would build it. The
[Control Protocol](../protocol/overview.md) section shows the messages
themselves, and [PROTOCOL.md](../spec/protocol.md) holds the exact rules.

There is no client SDK. A controller is any program that speaks the
[binary protobuf profile](../protocol/overview.md#binary-encoding) over a WebSocket. Field
names and types are owned by the [schema](../../proto/sipbridge-control.proto)
in this repository. The pinned local generator produces Go/TypeScript contracts
and generated binary codecs, with hashes recorded in
[`control-schema.json`](../../dependencies/control-schema.json). You can also
generate types for another language from the same schema.

## Division of responsibility

| Concern | sipbridge | Controller |
| --- | --- | --- |
| Sockets, parsing, the real source address and transport of each message | Owns | Receives as facts it can rely on |
| SIP transactions, dialogs, CSeq, timers, retransmissions | Owns | Sees through events and snapshots |
| Via, Contact, Route, CSeq and tags on generated messages | Owns | Must not override |
| Which requests are accepted, redirected or rejected | Carries out | Decides |
| Account authentication and digest verification | Never | Owns |
| Registrations | Remembers only where to deliver for dialogs in progress | Owns, including their ordering |
| SDP, application headers, call logic, media | Carries the content you choose | Owns |
| Subscription content (for example BLF, MWI) | Tracks the subscription dialog and NOTIFY sequence numbers | Owns |
| Outbound credentials | Uses briefly, never stores | Stores; supplies through `credentialLookup` |

## Session duties

Everything starts with a control session, and the controller is always the one
that connects. [Getting Started](./getting-started.md) has a working example of
steps 1 to 5.

1. Authenticate with the configured client certificate or bearer token, and
   verify sipbridge's server certificate when TLS is enabled.
2. Send `hello` with a stable instance ID, a random `epoch` generated once per
   controller process start, a connection attempt number that increases on each
   reconnect, and the [required capabilities](../protocol/capabilities.md).
3. Read `helloAck` and keep its `sessionId`, negotiated capabilities, bounds,
   `stateAuthority` and `applicationLease`.
4. Send `status` to say you are ready. sipbridge routes requests to a session
   only when `retainedReady` and `bindingsReady` are both true.
5. Answer every `ping` with a matching `pong` within six seconds. You may send
   your own pings too.
6. If the session fails, reconnect with backoff and use `inspect` to find out
   what happened to any work that was in flight.

If you run several sipbridge instances, keep a session open to each of them,
including instances you have taken out of rotation but that are still finishing
existing calls.

## Request handling duties

### Memory: one decision

Offer `sip.request-decision.v1` together with the native state, completion,
admission, budget and continuity capabilities. A Memory `request` is a one-shot
notification; do not acknowledge it separately. Authenticate, select the owner and
policy, then submit one `requestDecision` for its exact handle and next sequence.
An accepted SUBSCRIBE decision includes its required NOTIFY content. The native
`ACCEPTED` result proves committed intent, not a socket write. Asynchronous call
work can start after admission and use ordinary commands for later responses.

When a notification includes a native-validated flow proposal, return its ordered
receipt in the same decision while preserving the prior delivery witness used
for policy. The [direct-decision contract](../spec/protocol.md#direct-request-decision-memory)
owns exact fields, ownership, deadlines and uncertainty rules.

### Checkpoint: route and present

**Route new requests.** For each `ingress` message, return a `routeResult`
before its budget runs out (two seconds at most): `ACCEPTED`, `REDIRECT` to
another connected controller (at most two redirects, never back to a controller
already tried), or `REJECTED` with a SIP status.

**Authenticate before you admit.** Decide who the caller is and whether they
are allowed before sending `ingest`. The `ingest` names the reason the request
may proceed, its *admission basis*: `EXISTING_FLOW`, `AUTHENTICATED_BINDING`,
`TLS_DIALOG_CONTINUITY` or `TLS_TRANSACTION_CONTINUITY`
([explained here](../protocol/messages.md#ingest-admission-basis)). Base the
decision on verified credentials. A TLS handshake, a From or Contact header, a
source address or a Call-ID does not prove an account.

**Answer requests.** sipbridge presents each admitted request as a `request`
message. Answer with a `command` carrying `respond`, quoting the request's
handle.

Process `request` while an `ingest` RPC is still pending: direct presentation
waits for the controller's acceptance result before returning that ingest result.
Reply to the presentation with an accepted/completed `result`; this acknowledges
application handling, not a physical SIP write. The response command has its own
correlation ID and write outcome. Presentation is one-shot under the original
deadline, without a durable queue or cross-worker retry after process loss.

### Commands and events

**Originate requests.** Use `openUac` for a new INVITE or a standalone OPTIONS,
MESSAGE or NOTIFY, and `dialogRequest` for requests inside a dialog such as BYE
or re-INVITE. You must name where the request goes: either a registration you
have authorized or an explicit network target. A SIP URI alone does not make
sipbridge dial an address.

**Confirm connection changes.** When a client continues a dialog on a new
connection, sipbridge sends `flowAdoption`. Reply with `flowAdoptionResult`, in
order for each dialog.

**Supply outbound credentials on request.** When an outgoing request is
challenged, sipbridge sends `credentialLookup` for the credential reference you
gave in `openUac`. Do not put a username and password in `openUac` itself;
sipbridge refuses them.

**Follow events.** Process `dialogEvent` and `flowClosed`, and take every
result's `certainty` (`NOT_ACCEPTED`, `ACCEPTED`, `COMPLETED`, `UNKNOWN`) at
face value.

## Failure duties

Networks fail in the middle of things, and sipbridge is designed never to guess
what happened. Your controller has to follow the same discipline.

**Do not resend on a missing or `UNKNOWN` result.** If a command times out, the
session drops, or the result says `UNKNOWN`, the SIP message may already have
gone out. Sending the command again could, for example, answer a call twice.
Use `inspect` to read the current state and continue from there. The same
applies to trying a different connection: don't.
[Commands and Outcomes](../protocol/commands.md) covers this in detail.

**Recover only calls your application owns.** With negotiated recovery, the
same principal and saved handle can attach a replacement process within the
fixed deadline; restore your application state before sending ready status.
Without recovery, sipbridge detects the lost owner and starts cleanup (see
[Owner lease](./lifecycle.md#owner-lease)). The SIP state sipbridge still holds
for those calls is not enough to claim them. A bare Call-ID, process name or
snapshot never substitutes for the [recovery proof](./controller-recovery.md).

**Start afresh when your own routing data changes.** Commands carry a
`directoryEpoch`, your controller's version stamp for its routing and
registration data. If that version changes, sipbridge refuses commands issued
under the old one. There is no call to reconcile old work; set up new work under
the current version.

**Keep registrations in order.** Registration ordering is yours to enforce. A
late or replayed result from an older registration must not replace a newer one.

## Local initial digest challenges

Normally the controller issues the `401` challenge for a request that arrives
without credentials. With `--require-auth-methods`, sipbridge sends that first
`401` itself for the listed methods, which saves a round trip to the controller
for every such request. The controller then has to:

- Offer `sip.local-challenge.v1` in `hello`. Without it the handshake fails.
- Send a `DigestChallengePolicy` in `status`: realm, authentication epoch, a
  32-byte signing key used only for challenges, and an expiry no more than 60
  seconds away. Refresh it before it expires (the reference controller sends a
  new one every 30 seconds).
- Verify credentials on the retry, including nonce format, time window, nonce
  counts and replay. sipbridge issues the challenge but never checks the answer.

Until a valid policy arrives, readiness reports `authentication policy
unavailable` and requests that would be challenged get a temporary failure.
Nonce construction and key derivation are specified in
[Protocol: local initial digest challenges](../spec/protocol.md#local-initial-digest-challenges).

## Not yet available

The protocol has no way to drain a sipbridge instance or list every owner's
contexts globally. Recovery exposes only the authenticated owner's bounded
list. Plan restarts as described in
[Deployment](./deployment.md#replacing-a-process).

Use the messages listed in [Message Families](../protocol/messages.md) when
building a controller.
