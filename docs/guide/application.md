# Application Responsibilities

sipbridge executes SIP; the **application controller** decides what should
happen. This page lists what a controller must do to work with the current
service. The normative rules are in [PROTOCOL.md](../spec/protocol.md); field
shapes are in the [control schema](../../dependencies/sipbridge-control.proto).
The original platform's controller is called Nexus, which is why some wire
fields are named `nexus`.

There is no public client SDK. The canonical schema is owned by the original
platform repository, and this repository carries a read-only
[snapshot](../../dependencies/sipbridge-control.proto) with recorded hashes in
[`control-schema.json`](../../dependencies/control-schema.json). Any
implementation that follows the strict [JSON profile](../protocol/overview.md#json-encoding)
can act as a controller.

## Division of responsibility

| Concern | sipbridge | Controller |
| --- | --- | --- |
| Sockets, framing, observed source and transport | Owns | Receives as trusted evidence |
| SIP transactions, dialogs, CSeq, timers, retransmissions | Owns (pinned sipgo reducers) | Observes through events and snapshots |
| Via, Contact, Route, CSeq and tags on generated messages | Owns | Must not override |
| Which requests are accepted, redirected or rejected | Executes | Decides |
| Account authentication and digest verification | Never | Owns |
| Registration bindings and their ordering | Retains only delivery state for live contexts | Owns |
| SDP, application headers, call state, media | Carries chosen content | Owns |
| Subscription content (for example BLF, MWI) | Protocol metadata and NOTIFY sequencing | Owns |
| Outbound credentials | Uses transiently | Stores; supplies through `credentialLookup` |

## Session duties

A controller initiates every control session. It must:

1. Connect with a certificate carrying an allowed URI SAN and validate
   sipbridge's server certificate.
2. Send `hello` with a stable instance ID, an unpredictable epoch per controller
   process start, a connection attempt number that increases on each reconnect,
   and the [required capabilities](../protocol/capabilities.md).
3. Read `helloAck` and keep its `sessionId`, negotiated capabilities, bounds,
   `stateAuthority` and `applicationLease`.
4. Send `status` to declare readiness. sipbridge considers a session ready only
   when `retainedReady` and `bindingsReady` are both true.
5. Answer every `ping` with a matching `pong` within six seconds, and may send
   its own pings.
6. Reconnect with bounded backoff after a session failure, and use `inspect` to
   reconcile outstanding work.

Keep a session open to every sipbridge process it may need, including processes
that are only completing retained work.

## Request handling duties

- **Route new requests.** For each `ingress`, return a `routeResult` within the
  remaining budget: `ACCEPTED`, `REDIRECT` to another connected controller
  (at most two redirects, no repeats), or `REJECTED` with a SIP status.
- **Authorize before acting.** Decide account identity and policy before sending
  `ingest` with an admission basis (`EXISTING_FLOW`, `AUTHENTICATED_BINDING`,
  `TLS_DIALOG_CONTINUITY` or `TLS_TRANSACTION_CONTINUITY`). A TLS handshake,
  From/Contact, source address or Call-ID is never account proof.
- **Answer presented requests.** Respond to each `request` with a sequenced
  `command` carrying `respond`, using the request handle and current command-time
  admission.
- **Originate requests** with `openUac` (new INVITE, or a standalone OPTIONS,
  MESSAGE or NOTIFY) and `dialogRequest` (in-dialog requests). Destinations are
  an authorized registered binding or an explicit network target; a URI alone is
  not permission to dial an address.
- **Publish delivery changes.** Answer `flowAdoption` with a
  `flowAdoptionResult` receipt, keeping ordering per context.
- **Supply outbound credentials** when sipbridge sends `credentialLookup` for an
  opaque credential reference. Do not put clear credentials in `openUac`; the
  service refuses them.
- **Handle observations.** Process `dialogEvent` and `flowClosed`, and treat
  every result's certainty (`NOT_ACCEPTED`, `ACCEPTED`, `COMPLETED`, `UNKNOWN`)
  literally.

## Failure duties

- Never resend a command, or a request on a different flow, because a result is
  missing or `UNKNOWN`. Inspect the retained operation instead.
- Never adopt another controller's in-memory call because SIP state still exists.
  If a controller process dies, its calls end through cleanup policy.
- A changed business directory epoch refuses old business work. Establish fresh
  authority; there is no reconciliation RPC.
- Keep registration ordering in the controller: a delayed or replayed older
  result must not restore a superseded binding.

## Local initial digest challenges

With `--require-auth-methods`, sipbridge sends the initial `401` itself for
listed methods that carry no parseable credentials. The controller must then:

- Offer `sip.local-challenge.v1` in `hello`. Without it the handshake fails.
- Send a `DigestChallengePolicy` in `status`: realm, authentication epoch, a
  32-byte purpose-scoped signing key and an expiry no more than 60 seconds
  away. Refresh it (the reference controller uses 30 seconds).
- Verify credentials on the authenticated retry, including nonce format,
  window, nonce counts and replay.

Until a valid policy arrives, readiness reports `authentication policy
unavailable` and challengeable requests get a local temporary failure. Exact
nonce construction and key derivation are specified in
[Protocol: local initial digest challenges](../spec/protocol.md#local-initial-digest-challenges).

## Not yet available

The schema has no drain, context-inventory or drain-generation messages. These
are proposed requirements in PROTOCOL.md, not service APIs. Historical message
families such as `sendPermit`, `deliverEffect`, `settleEffect`,
`retireAuthority` and directory reconciliation remain decodable for
compatibility but grant no current authority. See
[Message Families](../protocol/messages.md).
