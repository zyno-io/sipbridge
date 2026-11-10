# Identities and TLS

sipbridge has listeners for three different audiences, and each is secured
differently. The control listener is for your controllers and is the one that
matters most: whoever can open a control session can direct calls. The public
SIP listeners are for phones and trunks. The admin listener serves health checks
and metrics. Put them on separate networks where you can.

| Surface | Flag | Authentication |
| --- | --- | --- |
| Control | `--control-listen` | Mutual TLS. Client chain must verify against `--client-ca`, and the leaf must carry an allowed URI SAN. |
| Public SIP over TLS | `--sip-listen` | Server-only TLS. Phones and trunks are not asked for client certificates. |
| Public SIP over UDP/TCP | `--sip-plain-listen` | None at the transport layer. |
| Admin | `--admin-listen` | None. Plain HTTP; defaults to loopback. |

All TLS listeners require TLS 1.2 or later.

## Control connection

The control connection uses mutual TLS: sipbridge proves its identity to the
controller with a server certificate, and the controller proves its identity to
sipbridge with a client certificate. A controller opens:

```text
wss://<sipbridge control endpoint>/v1/control
Sec-WebSocket-Protocol: sipbridge.control.json.v1
```

sipbridge accepts the connection only when all of these hold:

1. The client presented a certificate whose chain verifies against `--client-ca`.
2. That certificate has a URI subject alternative name (URI SAN) exactly equal
   to one entry of `--allowed-client-uri`. Being signed by the trusted CA is not
   enough by itself, and neither is a matching common name or source IP. This
   lets you share one CA across many services while allowing only your
   controllers to connect.
3. The request is `GET /v1/control` with no query string.
4. The request has **no** `Origin` header. Browser-originated upgrades are refused.
5. The client offered the `sipbridge.control.json.v1` subprotocol.
6. Fewer than 64 control sessions are active.

WebSocket compression is disabled. On its side, the controller must verify
sipbridge's certificate chain and check that the certificate names the endpoint
it meant to reach.

### What the client certificate proves

The certificate proves that the peer is one of your controllers. It does not
prove *which* controller process it is. If all your controller replicas share
one certificate, sipbridge tells them apart by the `instanceId` and `epoch` they
state in `hello`, and it takes their word for it (it only checks the claim
against sessions it already knows about). So give controller certificates only
to software you trust to identify itself honestly.

The identities have to line up as in this example, which uses made-up values:

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

The public certificate tells SIP clients they reached the right server. It
works in one direction only: phones and trunks are not asked for certificates,
so a completed TLS handshake says nothing about which account is on the other
end. Authenticating accounts, including verifying SIP digest credentials, is
the controller's job.

TLS does enable one feature. When a client's TLS connection drops and it
reconnects, a request on the new connection can continue an existing dialog if
it carries that dialog's full identity (Call-ID and both tags) and the dialog is
still alive. This is safe because those identifiers are long random values that
were only ever sent over TLS, so only the original client knows them. It applies
to continuing an existing dialog only. It does not authenticate an account or
permit a new INVITE or registration, and it is not available over plain UDP or
TCP. See
[Protocol: binding acceptance](../spec/protocol.md#_6-binding-acceptance-and-replacement-flow).

When sipbridge itself opens a TLS connection to a target the controller named,
it verifies the peer's name and certificate chain against the system trust
store in the image. A `sips:` target is never downgraded to an unencrypted
transport.

## Credential reload

You can renew certificates without restarting sipbridge. It re-reads its
certificate, key and CA files every five seconds.

- A changed, valid set takes effect for **new** handshakes. Existing SIP and
  control connections stay open and keep working.
- An invalid replacement is ignored and the previous certificates stay in use.
  sipbridge writes `private credentials invalid; retaining previous context` or
  `public credentials invalid; retaining previous context` to stderr.
- If the certificate in use expires, readiness reports `503` (and
  `/sip-servingz` does too, for the public certificate).

Because existing connections stay open, renewing a certificate does not cut off
anyone who connected with the old one. If a key is compromised and you need
those sessions gone, restart the process.

On Kubernetes, mount the certificate directories directly, for example as
projected volumes. `subPath` mounts and copies made by an init container never
see updates, so reload would silently do nothing.

## Secrets the process holds

| Material | Source | Notes |
| --- | --- | --- |
| Control and public TLS keys | Files | Reloaded as above. |
| Redis credentials and state identity key | Environment | Redis mode only. Validation errors never echo these values. |
| Digest challenge signing key | Controller `status` message | Only with `--require-auth-methods`. Short-lived, purpose-scoped; never an account credential. |
| Outbound digest credentials | Controller `credentialLookup` reply | Transient. Never stored in state, transition inputs, artifacts or ordinary logs. |

sipbridge is never given the passwords or hashes used to authenticate inbound
accounts, and it has no access to your application's database.

## Logging and capture

Routine logs contain fixed event names and validated identifiers. They never
include SIP bodies, digest credentials, `Authorization` headers, keys or raw
control frames.

Optional SIP capture is different: it exports complete plaintext SIP messages,
including any `Authorization` headers and bodies, to the configured collector.
Treat the collector as sensitive. See [Observability](./observability.md#sip-capture).
