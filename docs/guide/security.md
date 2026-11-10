# Identities and TLS

sipbridge has two TLS surfaces with different trust models, plus an unauthenticated
admin listener. Keep them on separate networks where possible.

| Surface | Flag | Authentication |
| --- | --- | --- |
| Control | `--control-listen` | Mutual TLS. Client chain must verify against `--client-ca`, and the leaf must carry an allowed URI SAN. |
| Public SIP over TLS | `--sip-listen` | Server-only TLS. Phones and trunks are not asked for client certificates. |
| Public SIP over UDP/TCP | `--sip-plain-listen` | None at the transport layer. |
| Admin | `--admin-listen` | None. Plain HTTP; defaults to loopback. |

All TLS listeners require TLS 1.2 or later.

## Control connection

A controller opens:

```text
wss://<sipbridge control endpoint>/v1/control
Sec-WebSocket-Protocol: sipbridge.control.json.v1
```

sipbridge accepts the upgrade only when all of these hold:

1. The client presented a certificate whose chain verifies against `--client-ca`.
2. The leaf certificate has a URI SAN exactly equal to one entry of
   `--allowed-client-uri`. Common name, source IP, a trusted CA alone or a
   caller-supplied name are not enough.
3. The request is `GET /v1/control` with no query string.
4. The request has **no** `Origin` header. Browser-originated upgrades are refused.
5. The client offered the `sipbridge.control.json.v1` subprotocol.
6. Fewer than 64 control sessions are active.

WebSocket compression is disabled. The controller must validate sipbridge's
server chain and a DNS SAN for the endpoint it intended to reach.

### What the client certificate proves

A shared workload certificate authenticates a controller *workload*, not a
particular controller process. The `hello.nexus.instanceId` and `epoch` fields
are trusted-workload assertions checked against existing session claims; they
are not cryptographic per-process identities. Issue controller certificates only
to software you trust to make those assertions honestly.

Example identity alignment, using synthetic values:

| Setting | Value |
| --- | --- |
| Controller certificate URI SAN | `spiffe://example.test/sip-controller` |
| `--allowed-client-uri` | `spiffe://example.test/sip-controller` |
| sipbridge control certificate DNS SAN | `sipbridge-0.control.example.test` |
| Controller dial target / TLS server name | `sipbridge-0.control.example.test` |
| `--instance` | `sipbridge-0` |

Several URI identities can be allowed with a comma-separated list, for example
during controller identity rotation.

## Public SIP TLS

The public certificate authenticates sipbridge to SIP clients. A completed TLS
handshake does not authenticate a SIP account. Account authentication and
digest verification belong to the controller.

TLS continuity matters for one feature: a retained dialog may continue on a new
TLS connection when the request carries the full directional dialog identity and
passes native lifecycle checks. This relies on the secrecy of strong random
dialog identifiers over TLS. It never authenticates an account, never authorizes
a new INVITE or registration, and is not available for plain UDP/TCP. See
[Protocol: binding acceptance](../spec/protocol.md#_6-binding-acceptance-and-replacement-flow).

When sipbridge opens outbound TLS to a network target, it verifies the peer
name and chain using the image's system trust store, and a SIPS target cannot be
downgraded.

## Credential reload

Certificate, key and trust files are re-read every five seconds.

- A changed, valid set is installed atomically for **new** handshakes. Existing
  SIP and control connections stay open.
- An invalid replacement is rejected and the previous context is kept. sipbridge
  writes `private credentials invalid; retaining previous context` or
  `public credentials invalid; retaining previous context` to stderr.
- If the installed certificate leaves its validity period, readiness reports
  `503` (and `/sip-servingz` too, for the public certificate).

Routine leaf renewal is not revocation. Ending existing sessions after a key
compromise is an explicit operator action, which in practice means replacing the
process.

Mount credential directories directly, for example as projected volumes. Do not
use `subPath` mounts or init-time copies; they do not receive updates.

## Secrets the process holds

| Material | Source | Notes |
| --- | --- | --- |
| Control and public TLS keys | Files | Reloaded as above. |
| Redis credentials and state identity key | Environment | Redis mode only. Validation errors never echo these values. |
| Digest challenge signing key | Controller `status` message | Only with `--require-auth-methods`. Short-lived, purpose-scoped; never an account credential. |
| Outbound digest credentials | Controller `credentialLookup` reply | Transient. Never stored in state, transition inputs, artifacts or ordinary logs. |

sipbridge never receives account passwords or hashes for inbound authentication,
a business database, the controller's trusted-directory store or Kubernetes
credentials.

## Logging and capture

Control-failure diagnostics contain fixed stage names and validated identifiers
only, never frames, peer text or SIP content. Routine logs never include SIP
bodies, digest credentials, authorization headers or keys.

Optional SIP capture is different: it exports complete plaintext SIP messages,
including any `Authorization` headers and bodies, to the configured collector.
Treat the collector as sensitive. See [Observability](./observability.md#sip-capture).
