# Building a Controller

sipbridge runs SIP; your **controller** decides what should happen. This page
explains what a controller has to do, in the order you would build it. The
[Control Protocol](../protocol/overview.md) section shows the messages
themselves, and [PROTOCOL.md](../spec/protocol.md) holds the exact rules.

There is no client SDK. A controller is any program that speaks the
[JSON profile](../protocol/overview.md#json-encoding) over a WebSocket. Field
names and types come from a protobuf schema; the repository carries a read-only
[copy](../../dependencies/sipbridge-control.proto) of it, with hashes recorded
in [`control-schema.json`](../../dependencies/control-schema.json), which you
can use to generate types for your language. The first controller written for
sipbridge is called Nexus, which is why some wire fields are named `nexus`.

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

1. Connect with a client certificate carrying an allowed URI SAN, and verify
   sipbridge's server certificate.
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

**Do not take over another controller's calls.** A call's logic lives in the
controller process that set it up. If that process dies, sipbridge notices,
typically after about 40 seconds, and begins SIP cleanup for its calls (see
[Owner lease](./lifecycle.md#owner-lease)). The SIP state sipbridge still holds
for those calls is not an invitation for a different controller to carry on.

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

The protocol has no way to drain a sipbridge instance or list everything it
holds. PROTOCOL.md proposes messages for both, but they are not in the schema
or the service yet, so plan restarts as described in
[Deployment](./deployment.md#replacing-a-process).

The schema also contains older message types such as `sendPermit`,
`deliverEffect`, `settleEffect` and `retireAuthority`. They remain so that older
software can still decode them, but the service no longer acts on them. Build
against the current messages listed in
[Message Families](../protocol/messages.md).
