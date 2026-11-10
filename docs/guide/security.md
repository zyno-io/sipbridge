# Identities and TLS

sipbridge has listeners for three different audiences, and each is secured
differently. The control listener is for your controllers and is the one that
matters most: whoever can open a control session can direct calls. The public
SIP listeners are for phones and trunks. The admin listener serves health checks
and metrics. Put them on separate networks where you can.

| Surface | Flag | Authentication |
| --- | --- | --- |
| Control | `--control-listen` | mTLS by default, or explicit bearer authentication. TLS is default; plain bearer control is loopback-only. |
| Public SIP over TLS | `--sip-listen` | Server-only TLS. Phones and trunks are not asked for client certificates. |
| Public SIP over UDP/TCP | `--sip-plain-listen` | None at the transport layer. |
| Admin | `--admin-listen` | None. Plain HTTP; defaults to loopback. |

All TLS listeners require TLS 1.2 or later.

## Control connection

The default is mutual TLS: sipbridge proves its server identity and checks the
controller's client certificate chain plus an exact URI SAN from
`--allowed-client-uri`. A shared CA alone, common name or IP address does not
identify an allowed controller.

```text
wss://<sipbridge control endpoint>/v1/control
Sec-WebSocket-Protocol: sipbridge.control.json.v1
```

All modes require `GET /v1/control` without a query string, the v1 subprotocol,
no `Origin` header and text frames. Authentication completes before the upgrade
or any process/owner claim. A failed mTLS handshake never falls back to bearer.

### Bearer authentication

Choose `--control-auth=bearer` with `--control-bearer-token-file` and a stable
`--control-principal`. The controller sends `Authorization: Bearer <token>` in
the upgrade request. TLS still verifies sipbridge's server certificate, but the
controller needs no client certificate. Do not supply `--client-ca` or
`--allowed-client-uri` in this mode.

Generate a token file without displaying it:

```sh
umask 077
openssl rand -hex 32 > control-token
```

The token is 32–4,096 printable non-space ASCII bytes. It may end with exactly one
LF or CRLF; additional newlines are rejected.
Keep the principal stable across token rotations and controller restarts. It is
the ownership identity, while each hello still carries the actual process epoch.

### Local plain WebSocket

A local standalone controller can use plain WebSocket with these control flags,
in addition to the SIP listener options from [Getting Started](./getting-started.md):

```sh
--control-auth=bearer \
--control-transport=plain \
--control-listen=127.0.0.1:8443 \
--control-bearer-token-file=control-token \
--control-principal=local-controller
```

Use `ws://127.0.0.1:8443/v1/control`. Control TLS flags must be omitted, and a
non-loopback listener is rejected. Bearer authentication remains required.
Public SIP TLS and account authentication are separate settings and rules.

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

You can renew control credentials without restarting sipbridge. Configured
certificate/key/CA files and bearer token files are re-read every five seconds.
A valid bearer replacement applies to new upgrades; an invalid replacement keeps
the previous token. Existing sessions retain their authenticated principal.

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
