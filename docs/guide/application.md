---
description: "What a production controller must do, in build order: the session, deciding requests, commands and events, subscriptions, failures and local digest challenges."
---

# Building a Controller

sipbridge runs SIP; your **controller** decides what should happen. This page
lists what a controller has to do, in the order you would build it. If you
have not yet, run [Getting Started](./getting-started.md) and read
[A Call, Step by Step](./call-walkthrough.md) first; they show the messages
this page talks about.

There is no client SDK. A controller is any program that speaks
[strict JSON](../protocol/overview.md#json-encoding) over a WebSocket. Field
names and types come from the [schema](../../proto/sipbridge-control.proto) in
the repository, which also ships generated Go and TypeScript types and strict
JSON validation in [`generated/typescript`](../../generated/typescript). You
can generate types for other languages from the same schema.

## Who does what

| Concern | sipbridge | Your controller |
| --- | --- | --- |
| Sockets, parsing, the real source address and transport of each message | Does it | Receives them as facts |
| SIP transactions, dialogs, CSeq, timers, retransmissions, ACK and CANCEL handling | Does it | Sees results and events |
| Via, Contact, Route, CSeq and tags on generated messages | Does it | Cannot override them |
| Whether a request is accepted, and with which response | Carries it out | Decides |
| Account authentication and digest verification | Never | Does it |
| Registrations: who is registered where, and until when | Keeps nothing beyond the current request | Owns them |
| SDP, application headers, call logic, media | Carries what you choose | Owns them |
| Subscription content (BLF, MWI) | Runs the subscription dialog and NOTIFY delivery | Supplies the content |
| Outbound credentials | Uses once, never stores | Stores; supplies on request |

## 1. Hold a session

The controller always connects to sipbridge, never the other way round.

1. Open `wss://<control address>/v1/control` (or `ws://` for local loopback
   testing) with the subprotocol `sipbridge.control.json.v1`, no `Origin`
   header and compression off. Authenticate with your client certificate
   (default) or a bearer token. See [Identities and TLS](./security.md).
2. Send `hello` with a stable instance ID, a random `epoch` that you generate
   once per process start, a `connectionAttempt` that increases on every
   reconnect, and **all seven required capabilities** listed in
   [Capabilities](../protocol/capabilities.md). sipbridge closes the handshake
   if any is missing.
3. Read `helloAck`. Keep its `sessionId` (every later message must carry it),
   the negotiated capabilities and limits, and `stateAuthority`.
4. Send `status` with `retainedReady` and `bindingsReady` both `true`. Until
   you do, sipbridge sends you no work and reports itself not ready.
5. Answer every `ping` with a `pong` that echoes its `correlationId` and
   `nonce`, within six seconds. Answering heartbeats also keeps your ownership
   of your calls alive.

If you run several sipbridge instances, hold a session to each one.
[Session](../protocol/session.md) has the details.

## 2. Decide every request you are offered

When a SIP request needs a decision, sipbridge sends a `request`. Answer it
with exactly one `requestDecision` before the request's deadline (about two
seconds); if you miss it, sipbridge answers the caller with `503`.

Before deciding:

- **Authenticate.** Work out who sent the request. A TLS connection, a From or
  Contact header, a source address or a Call-ID does not prove an account. If
  you require digest authentication, answer the first request with `401` and a
  challenge, then verify the retry. sipbridge never checks credentials for you
  (it can send the first challenge for you; see
  [local challenges](#local-initial-digest-challenges)).
- **Apply your policy.** Is this account allowed to register, call this
  number, subscribe to this extension?

Then send the decision. Its main choices are:

| You want to | Send |
| --- | --- |
| Reject | A `status` of 300 to 699. No account or connection fields. |
| Answer at once | A final `status` (2xx), `basis`, `principalId`, `bindingId`, and optionally `headers` and `body`. |
| Take the request and answer later | `status: 0` with `basis`, `principalId`, `bindingId`. Then use `respond` commands. Not allowed for REGISTER and SUBSCRIBE. |
| Accept a SUBSCRIBE | A 2xx `status` plus the first NOTIFY's content in `notify`. |

Every positive decision also carries the context's next `sequence` and an
`admission` block (your data version and an expiry). The exact field rules are
in [Message Families](../protocol/messages.md#deciding-a-request).

The `result` to a decision is `ACCEPTED` when sipbridge has recorded it and
started sending any response. It does not mean the response reached the phone.

### Registrations

sipbridge does not keep a registration database. When you accept a REGISTER,
record in your own store which account and device it is, its expiry, and the
`flow` it arrived on. You need that `flow` to call the phone later (see
[outgoing calls](./call-walkthrough.md#part-3-an-outgoing-call)), and the
phone's connection identifies where it can be reached right now.

Keep registrations ordered. A late or replayed result for an older REGISTER
must never overwrite a newer one. With memory state, sipbridge forgets a
completed TCP or TLS REGISTER transaction as soon as its response is written,
so an identical retry can be offered to you again as a new request.

### Subscriptions (BLF and MWI)

sipbridge supports the `dialog` and `message-summary` event packages. To
accept a SUBSCRIBE, include in the decision the content of the first NOTIFY,
as a `notify` block whose method is `NOTIFY`. sipbridge sends that NOTIFY
right after the `200`. Later state changes go out as `dialogRequest` commands
with method `NOTIFY` on the subscription's context. sipbridge numbers and
delivers them; you supply the content. Refreshes and unsubscribes arrive as new
`request` messages and again need a decision with a NOTIFY.

A phone subscription stays tied to the sipbridge process and connection that
accepted it, with either state backend. It ends when its TCP or TLS connection
closes, when it expires, or when the phone unsubscribes. A UDP subscription
lasts until it expires or the phone unsubscribes.

## 3. Send commands

Commands make sipbridge do something on a context:

| Command | Use |
| --- | --- |
| `respond` | Answer a request you took with `status: 0`: provisional responses, then one final response. |
| `openUac` | Start an outgoing call, or send a standalone OPTIONS, MESSAGE, NOTIFY, REGISTER or PUBLISH. |
| `dialogRequest` | Send a request inside an existing call: BYE, re-INVITE, INFO, REFER, UPDATE, NOTIFY, or an ACK that carries your SDP answer. |
| `cancel` | Cancel an outgoing call that has not been answered. |

Each command carries the context reference, the next sequence number for that
context and an `admission` block. [Commands and Outcomes](../protocol/commands.md)
explains sequencing, budgets and every result.

An outgoing request needs an explicit destination: either a registered phone's
connection (`admission.selectedFlow` with the account's `bindingId`) or a
network target (`network`) for a carrier or server. A SIP URI on its own never
makes sipbridge dial an address. Put a credential reference, not a password,
in `network.credential`, and answer the `credentialLookup` that follows a
`401` or `407`.

## 4. Handle what sipbridge tells you

| Message | What to do |
| --- | --- |
| `dialogEvent` | Update your call state: `PROVISIONAL`, `ANSWERED`, `ACKNOWLEDGED`, `CANCELLED`, `ENDED`, `FAILED`, `SUBSCRIPTION_EXPIRED`. Reply with a `result` of `ACCEPTED`. A repeated `correlationId` is a duplicate. |
| `flowAdoption` | A phone continued a dialog on a new connection. If you agree, reply with a `flowAdoptionResult` receipt so later messages use that connection; otherwise reply with an `error`. |
| `credentialLookup` | Reply with a `credentialResult` (username, password) or an `error`. |
| `flowClosed` | A phone's connection closed. Registrations that pointed at it can no longer be reached through it. |

A `request` may also carry a `flowAdoption` proposal when the request arrived
on a new connection for an existing dialog. To accept the request on the new
connection, publish it in your own records and return the receipt in the
decision's `delivery` field.

## 5. Handle failure without guessing

Networks fail in the middle of things. sipbridge never guesses what happened,
and your controller must not either.

**Never resend after an `UNKNOWN` or a missing result.** If a command's result
is `UNKNOWN`, times out, or your session drops before it arrives, the SIP
message may already be out. Sending the command again could answer a call
twice or send a second BYE. Use `inspect` to read where the context stands and
continue from there. The same goes for a decision: never repeat it on another
session.

**Expect to lose calls when your session drops.** sipbridge ends the calls of
a controller whose session closes or stops answering heartbeats. It still
finishes SIP work it already committed (ACKs, cleanup BYEs, retransmissions),
but it does not hand the calls to your reconnected session or to another
controller process. Controller reattachment is not available. Plan controller
restarts accordingly; see [Controller Restarts](./controller-recovery.md).

**Treat your data version seriously.** Every command carries your
`directoryEpoch`. If you change it, sipbridge refuses commands made under the
old one; there is no reconciliation step. Set up new work under the current
version instead.

**Keep a session to every instance.** With memory state, everything a
sipbridge process knows lives in that process. With Redis, calls are shared
between instances, but registrations in progress and subscriptions still stay
on the process that accepted them. A controller with no session to that
process cannot act on them.

## Local initial digest challenges

Normally your controller answers an unauthenticated request with `401` itself.
With `--require-auth-methods`, sipbridge sends that first `401` for the listed
methods without asking you, which saves one round trip per registration or
call. Your controller then has to:

- offer `sip.local-challenge.v1` in `hello`; without it the handshake fails;
- send a `digestChallengePolicy` in `status`: realm, authentication epoch, a
  32-byte signing key used only for challenges, and an expiry at most 60
  seconds away. Refresh it well before it expires, for example every 30
  seconds;
- verify the credentials on the retry, including the nonce's format, its
  120-second window, nonce counts and replay. sipbridge issues challenges but
  never checks the answer.

Until a valid policy arrives, readiness reports `authentication policy
unavailable` and requests that would be challenged get a temporary failure.
The nonce format and key derivation are specified in
[Protocol: local initial digest challenges](../spec/protocol.md#local-initial-digest-challenges).

## Checklist

- [ ] Hello offers all required capabilities; epoch is random per start;
      connection attempts increase.
- [ ] `status` sent after the handshake; heartbeats answered.
- [ ] Every `request` gets exactly one decision within two seconds.
- [ ] Credentials verified before any positive decision.
- [ ] Sequence numbers tracked per context; admission expiry computed per
      command.
- [ ] `UNKNOWN` and missing results lead to `inspect`, never to a resend.
- [ ] Events acknowledged and duplicates ignored.
- [ ] Registrations, their flows and their ordering kept in your own store.
- [ ] Calls treated as lost when your session drops.
